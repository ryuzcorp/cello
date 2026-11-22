// Modified by Ryuz Corp (Cello): a per-isolate ArrayBuffer memory budget.
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// A black-box runner, not node code: it boots the binary, copies the
// fixture, and polls TCP with wall-clock deadlines, so the execution
// boundary's injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! An isolate's `ArrayBuffer` memory is capped, and the process stays bounded.
//!
//! Before this, V8's default ArrayBuffer allocator was `malloc`: a loop of
//! `new Uint8Array(8 MiB)` grew the shared process without limit while the
//! isolate's V8 heap stayed near empty, because backing stores are external
//! memory and `near_heap_limit` only extends the heap. Measured on the
//! unwired runtime: 640 MiB of buffers with a 128 MiB heap limit, no refusal,
//! RSS +643 MiB.
//!
//! The node now boots with `CELLO_V8_HEAP_LIMIT_MB=64` and
//! `CELLO_V8_EXTERNAL_LIMIT_MB=320`. The fixture grows to 512 MiB in 8 MiB
//! steps, so it has to be refused at 320 MiB — five heap limits, and *above*
//! the 256 MiB the default four-heap multiple would allow, which is what pins
//! the knob — with the `RangeError` V8 raises when its allocator returns null.
//! RSS is measured around that one request (on Linux, where `/proc` reports
//! it), so "bounded" is a property of the real process rather than of the
//! accounting alone. A second request then
//! allocates the legitimate per-request payloads (64 MiB body or `/tmp` read,
//! 32 MiB WebSocket message, 32 MiB tee budget and a clone) on the same
//! Durable Object, which only fits once the first request's buffers have been
//! released.
//!
//! Everything runs through one named Durable Object, so the isolate keeps the
//! memory a previous request charged it. Answers travel in headers: a response
//! body is itself a charged backing store, and an isolate at its ceiling cannot
//! allocate one, which is also why the fixture releases its buffers before
//! answering.

mod common;

use common::{announced, cello_command, forward_announce};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const MIB: u64 = 1024 * 1024;

/// What the fixture reports: status, headers, body.
type Reply = (u16, Vec<(String, String)>, String);

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/array-buffer-cap")
        .canonicalize()
        .expect("fixture exists")
}

fn fixture_tempdir(prefix: &str) -> tempfile::TempDir {
    let mut builder = tempfile::Builder::new();
    builder.prefix(prefix);
    match builder.tempdir_in("/dev/shm") {
        Ok(dir) => dir,
        Err(error) => {
            eprintln!("fixture tempdir fell back to disk (/dev/shm unusable: {error})");
            builder.tempdir().expect("fixture tempdir")
        }
    }
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    for entry in std::fs::read_dir(src).expect("read fixture dir") {
        let entry = entry.expect("fixture entry");
        let to = dst.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), to).expect("copy fixture file");
        }
    }
}

/// A booted `cello dev` and its public address. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(project: &Path) -> Self {
        let mut child = cello_command(
            project,
            [
                ("NO_COLOR", "1"),
                // Five heap limits of buffers, above the four-heap default: a
                // run that ignored the knob would refuse at 256 MiB instead.
                ("CELLO_V8_HEAP_LIMIT_MB", "64"),
                ("CELLO_V8_EXTERNAL_LIMIT_MB", "320"),
            ],
        )
        .arg("dev")
        .arg(project)
        .arg("--port")
        .arg("0")
        .arg("--no-watch")
        .arg("--clean")
        .arg("--logs")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cello dev");
        let mut stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = stdout.read_to_end(&mut sink);
        });
        let announced_lines: Arc<Mutex<Vec<String>>> = Arc::default();
        forward_announce(stderr, Arc::clone(&announced_lines));
        let mut node = Self {
            child,
            public: String::new(),
        };
        let deadline = Instant::now() + Duration::from_secs(180);
        let public = loop {
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!(
                    "cello dev exited during boot: {status}: {}",
                    announced_lines.lock().expect("announce lock").join("\n")
                );
            }
            if let Some(public) = announced(&announced_lines, "cello listening on ") {
                if probe(&public, "/health") {
                    break public;
                }
            }
            if Instant::now() > deadline {
                panic!(
                    "boot: no healthy node within the deadline: {}",
                    announced_lines.lock().expect("announce lock").join("\n")
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        node.public = public;
        node
    }

    /// Resident set size of the process that serves the isolate, in MiB.
    ///
    /// `cello dev` is a supervisor: it spawns the serving node as a child, and
    /// the isolate (and every `ArrayBuffer` it allocates) lives in that child.
    /// Sampling the supervisor alone measured its own few MiB and passed
    /// whatever the serving node did, so the whole descendant tree is walked
    /// and the largest RSS is taken. One serving node holds the isolate, so
    /// "largest" is that node rather than a sum that double-counts shared
    /// pages.
    ///
    /// The node is non-dumpable, but its own `/proc/<pid>/status` is readable
    /// by the same user that spawned it. `/proc` is Linux-only, so the RSS
    /// bound is asserted there only; the refusal, growth, churn and health
    /// assertions hold on every platform.
    #[cfg(target_os = "linux")]
    fn rss_mib(&self) -> u64 {
        process_tree(self.child.id())
            .into_iter()
            .filter_map(|pid| {
                let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
                let kib: u64 = status
                    .lines()
                    .find_map(|line| line.strip_prefix("VmRSS:"))
                    .and_then(|value| value.split_whitespace().next())
                    .and_then(|value| value.parse().ok())?;
                Some(kib / 1024)
            })
            .max()
            .expect("the serving node in the tree")
    }

    /// RSS of the `cello dev` supervisor itself, for the assertion that the
    /// tree walk is what observes the growth. Test-only diagnostic.
    #[cfg(target_os = "linux")]
    fn supervisor_rss_mib(&self) -> u64 {
        let status =
            std::fs::read_to_string(format!("/proc/{}/status", self.child.id())).expect("status");
        let kib: u64 = status
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:"))
            .and_then(|value| value.split_whitespace().next())
            .and_then(|value| value.parse().ok())
            .expect("VmRSS");
        kib / 1024
    }
}

/// Every process in a node's tree, the root included. `cello dev` forks the
/// serving node (and the node may fork isolate runners), so a bound on "the
/// node's memory" has to cover what it spawned.
#[cfg(target_os = "linux")]
fn process_tree(root: u32) -> Vec<u32> {
    let mut found = vec![root];
    let mut index = 0;
    while index < found.len() {
        let pid = found[index];
        index += 1;
        let Ok(tasks) = std::fs::read_dir(format!("/proc/{pid}/task")) else {
            continue;
        };
        for task in tasks.filter_map(|task| task.ok()) {
            let children = task.path().join("children");
            let list = std::fs::read_to_string(&children).unwrap_or_default();
            for child in list.split_whitespace() {
                if let Ok(child) = child.parse::<u32>() {
                    if !found.contains(&child) {
                        found.push(child);
                    }
                }
            }
        }
    }
    found.sort();
    found
}

impl Drop for TestNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// One HTTP/1.1 exchange. The headers are what the fixture reports through, so
/// they are returned beside the body.
fn request(addr: &str, path: &str, timeout: Duration) -> Result<Reply, String> {
    let mut stream = TcpStream::connect(addr).map_err(|error| format!("connect: {error}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|error| format!("timeout: {error}"))?;
    let wire =
        format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: */*\r\n\r\n");
    stream
        .write_all(wire.as_bytes())
        .map_err(|error| format!("write: {error}"))?;
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .map_err(|error| format!("read: {error}"))?;
    let raw = String::from_utf8(raw).map_err(|error| format!("utf8: {error}"))?;
    let (head, body) = raw
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("no header/body split in {raw:?}"))?;
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| format!("no status in {head:?}"))?;
    let headers = head
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    Ok((status, headers, body.to_string()))
}

fn get(node: &TestNode, path: &str) -> Result<Reply, String> {
    request(&node.public, path, Duration::from_secs(120))
}

fn probe(addr: &str, path: &str) -> bool {
    matches!(request(addr, path, Duration::from_secs(2)), Ok((200, _, _)))
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}

/// Poll one route until it answers without a refusal, which is how a freed
/// isolate recovers: `recover_heap` collects at the turn boundary, rate
/// limited to one collection a second.
fn await_clear(node: &TestNode, path: &str, deadline: Duration) -> (u64, String) {
    let end = Instant::now() + deadline;
    loop {
        let (status, headers, body) = get(node, path).expect("route answers");
        assert_eq!(status, 200, "{path}: {body}");
        let refused = header(&headers, "x-refused").unwrap_or("").to_string();
        let grown: u64 = header(&headers, "x-grown")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        if refused.is_empty() {
            return (grown, refused);
        }
        if Instant::now() > end {
            panic!("{path} still refused after the deadline: {refused}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The ceiling is the knob, not the four-heap default: 320 MiB with a 64 MiB
/// heap. A default run would refuse at 256 MiB, so this pins the setting.
#[test]
fn array_buffer_growth_is_refused_at_the_budget() {
    let dir = fixture_tempdir("cello-array-buffer-cap-");
    let project = dir.path().join("app");
    copy_dir(&fixture_dir(), &project);
    let node = TestNode::boot(&project);

    // 512 MiB asked for, with a 320 MiB budget and a 64 MiB heap limit, on an
    // isolate that has charged nothing yet: the refusal point is the budget,
    // not what a previous request happened to leave charged.
    #[cfg(target_os = "linux")]
    let before = node.rss_mib();
    #[cfg(target_os = "linux")]
    let supervisor_before = node.supervisor_rss_mib();
    let (status, headers, body) = get(&node, "/grow?target=536870912").expect("grow answers");
    assert_eq!(status, 200, "{body}");
    #[cfg(target_os = "linux")]
    let after = node.rss_mib();
    #[cfg(target_os = "linux")]
    let supervisor_after = node.supervisor_rss_mib();

    let refused = header(&headers, "x-refused").unwrap_or("");
    assert!(
        refused.starts_with("RangeError"),
        "growing past the budget was not refused with a RangeError: {refused:?}"
    );
    assert!(
        refused.contains("Array buffer allocation failed"),
        "the refusal names the failed allocation: {refused:?}"
    );
    let grown: u64 = header(&headers, "x-grown")
        .and_then(|value| value.parse().ok())
        .expect("x-grown");
    assert!(
        grown > 256 * MIB && grown <= 320 * MIB,
        "growth stopped at the knob's 320 MiB budget, not the default \
         multiple's 256: {grown} bytes"
    );
    // The charge is at most 320 MiB; the process may hold allocator and V8
    // overhead above it, but a run that kept growing would be 512 MiB higher.
    // `/proc` is where the RSS comes from, so this bound is Linux-only.
    #[cfg(target_os = "linux")]
    {
        let tree_grew = after.saturating_sub(before);
        let supervisor_grew = supervisor_after.saturating_sub(supervisor_before);
        assert!(
            tree_grew <= 416,
            "the serving process grew by {tree_grew} MiB for a 320 MiB budget \
             ({before} -> {after})"
        );
        // The tree walk is what observes the isolate: the serving node grew by
        // most of the budget while the `cello dev` supervisor it was spawned
        // by stayed small. Sampling the supervisor alone — what this test did
        // before — could not have failed whatever the serving node did.
        assert!(
            tree_grew > 160,
            "the serving process grew only {tree_grew} MiB, so the bound is \
             not observing the isolate ({before} -> {after})"
        );
        assert!(
            supervisor_grew < tree_grew / 2,
            "the supervisor grew {supervisor_grew} MiB of the tree's \
             {tree_grew} MiB; the sample would have to come from the tree \
             ({supervisor_before} -> {supervisor_after})"
        );
    }

    // Churn is not growth: allocate and drop 1 GiB in total, never refused.
    let (status, headers, body) = get(&node, "/churn?count=128").expect("churn answers");
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        header(&headers, "x-refused"),
        Some(""),
        "churn was refused: {headers:?}"
    );
    assert_eq!(
        header(&headers, "x-grown"),
        Some(&*format!("{}", 1024 * MIB))
    );

    // The node still serves, and the legitimate payloads fit: the refused
    // request's memory came back to the budget (the isolate recovered).
    let (grown, _) = await_clear(&node, "/legit", Duration::from_secs(30));
    assert_eq!(grown, 160 * MIB);
    assert!(
        probe(&node.public, "/health"),
        "the node stopped serving after the refusals"
    );
}

/// An RPC abort whose reason the receiving isolate cannot pay for still
/// aborts. The Durable Object holds 296 of its 320 MiB while it waits on a
/// signal its caller passed, and the caller aborts with a 40 MiB reason: the
/// reason is a host store the receiver's budget refuses. The live wait
/// rejects with the budget's `RangeError`, and the abort must reach the
/// signal as the standard `AbortError` instead of being dropped with the
/// payload, which left a cancelled operation running until its own timeout.
#[test]
fn unpayable_rpc_abort_reason_still_aborts() {
    let dir = fixture_tempdir("cello-array-buffer-abort-");
    let project = dir.path().join("app");
    copy_dir(&fixture_dir(), &project);
    let node = TestNode::boot(&project);

    let (status, _, body) = get(&node, "/unpayable-abort").expect("abort route answers");
    assert_eq!(status, 200, "{body}");
    let outcome: serde_json::Value = serde_json::from_str(&body).expect("json outcome");
    assert_eq!(
        outcome["aborted"], true,
        "the abort was dropped with its refused reason: {outcome}"
    );
    assert_eq!(outcome["name"], "AbortError", "{outcome}");
    assert_eq!(outcome["message"], "The operation was aborted", "{outcome}");
    assert!(
        probe(&node.public, "/health"),
        "the node stopped serving after the refused abort reason"
    );
}
