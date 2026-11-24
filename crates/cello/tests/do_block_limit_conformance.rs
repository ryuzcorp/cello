// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by Ryuz Corp (Cello): `blockConcurrencyWhile`'s limit (#261).

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines.
#![allow(clippy::disallowed_methods)]

//! #261: `blockConcurrencyWhile`'s limit had no coverage. A body that runs
//! past it is cancelled and the object reset, and the caller's promise
//! rejects with workerd's message:
//!
//! > A call to blockConcurrencyWhile() in a Durable Object waited for too
//! > long. The call was canceled and the Durable Object was reset.
//!
//! The limit is workerd's 30 s, which no test can wait out, so the node takes
//! a shortened one from `CELLO_TEST_BLOCK_LIMIT_MS` when `CELLO_TEST_WAIT=1`
//! is set -- the same seam every other test-only knob uses, absent on a
//! shipping node.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

mod common;
use common::{announced, cello_command, forward_announce};

/// The limit the node is given. Short enough that the test is quick, long
/// enough that an unloaded node does not beat it to a scheduled turn.
const LIMIT_MS: u64 = 100;

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/do-block-limit")
        .canonicalize()
        .expect("fixture exists")
}

fn fixture_tempdir(prefix: &str) -> tempfile::TempDir {
    let mut builder = tempfile::Builder::new();
    builder.prefix(prefix);
    match builder.tempdir_in("/dev/shm") {
        Ok(dir) => dir,
        Err(error) => {
            eprintln!("fixture tempdir fell back to disk (/dev/shm unusable: {error}");
            builder.tempdir().expect("fixture tempdir")
        }
    }
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    for file in ["wrangler.json", "worker.js"] {
        std::fs::copy(src.join(file), dst.join(file)).expect("copy fixture");
    }
}

/// A booted `cello dev` and its address. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(app_project: &Path) -> Self {
        let limit = LIMIT_MS.to_string();
        let mut child = cello_command(
            app_project,
            [
                ("NO_COLOR", "1"),
                ("CELLO_TEST_WAIT", "1"),
                ("CELLO_TEST_BLOCK_LIMIT_MS", &limit),
            ],
        )
        .arg("dev")
        .arg(app_project)
        .arg("--port")
        .arg("0")
        .arg("--no-watch")
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
        node.public = poll_until("boot", Duration::from_secs(120), || {
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!("cello dev exited during boot: {status}");
            }
            let addr = announced(&announced_lines, "cello listening on ")?;
            probe(&addr, "/health").then_some(addr)
        });
        node
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn get(addr: &str, path: &str) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect(addr).map_err(|error| format!("connect: {error}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .map_err(|error| format!("timeout: {error}"))?;
    let wire = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
    );
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
    Ok((status, body.to_string()))
}

fn body_json(addr: &str, path: &str) -> serde_json::Value {
    let (status, body) = get(addr, path).unwrap_or_else(|error| panic!("GET {path}: {error}"));
    assert_eq!(status, 200, "GET {path}: {body}");
    serde_json::from_str(&body).expect("JSON body")
}

fn probe(addr: &str, path: &str) -> bool {
    matches!(get(addr, path), Ok((200, _)))
}

fn poll_until(
    name: &str,
    deadline: Duration,
    mut condition: impl FnMut() -> Option<String>,
) -> String {
    let start = Instant::now();
    loop {
        if let Some(done) = condition() {
            return done;
        }
        if start.elapsed() > deadline {
            panic!("{name}: condition unmet within the deadline");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// workerd's own message, word for word: the caller's promise carries it and
/// the object it belonged to is gone.
const LIMIT_MESSAGE: &str = "A call to blockConcurrencyWhile() in a Durable Object waited for too long. The call was canceled and the Durable Object was reset.";

#[test]
fn a_block_past_the_limit_resets_the_object() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("cello-do-block-limit-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    // Inside the limit: the block settles and the object keeps its counters.
    let fast = body_json(&node.public, "/fast");
    assert_eq!(fast["value"], "fast", "{fast}");
    assert_eq!(fast["instance"], 1, "{fast}");
    assert_eq!(fast["events"], 1, "{fast}");
    assert!(
        fast["elapsed"].as_u64().expect("elapsed") < 5000,
        "a block under the limit settles at once: {fast}"
    );

    // Past it: the body never settles, so only the limit ends the block. The
    // rejection is the promise's, not a throw, and it carries workerd's
    // message.
    let started = Instant::now();
    let held = body_json(&node.public, "/hold");
    let waited = started.elapsed();
    assert_eq!(held["message"], LIMIT_MESSAGE, "{held}");
    assert_eq!(held["instance"], 1, "{held}");
    assert_eq!(held["events"], 2, "{held}");
    assert!(
        held["elapsed"].as_u64().expect("elapsed") >= LIMIT_MS - 1,
        "the block ended no earlier than the limit: {held}"
    );
    assert!(
        waited >= Duration::from_millis(LIMIT_MS - 1),
        "the request waited for the limit: {waited:?}"
    );

    // The reset dropped the instance: the next event runs on a new object
    // whose counters start over.
    let after = body_json(&node.public, "/state");
    assert_eq!(after["instance"], 2, "{after}");
    assert_eq!(after["events"], 1, "{after}");
}
