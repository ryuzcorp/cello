// Modified by Ryuz Corp (Cello): outbound fetch timeout conformance.
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// A black-box runner, not node code: it boots the binary, copies a fixture,
// serves three deterministic shape origins over loopback, and polls TCP with
// wall-clock deadlines.
#![allow(clippy::disallowed_methods)]

//! `CELLO_FETCH_TIMEOUT_S` bounds one outbound fetch: the connect, the
//! request body and the wait for the response head. It is not a total
//! deadline on the response body, so a Worker that streams an upstream body
//! (an SSE feed, a proxied download) keeps reading past that budget while
//! the upstream keeps sending. A body that goes silent still ends, at the
//! stream idle deadline (`CELLO_HTTP_STREAM_IDLE_S`), which every received
//! chunk refreshes: a black-holed upstream can never pin a connection.
//!
//! Both halves are load-bearing. A total deadline truncates a live stream
//! (a control plane proxying its own SSE through a Worker fetch died at
//! exactly the 120 s default); dropping every bound lets one dead peer park
//! a Worker's turn forever.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The fetch budget the fixture node runs with. The stream below lasts
/// almost twice as long, so a total deadline cannot pass the test.
const FETCH_BUDGET_S: u64 = 2;
/// The stream idle deadline the fixture node runs with, so a stalled body
/// test costs a couple of seconds instead of the 60 s default.
const IDLE_BUDGET_S: u64 = 2;
/// Chunks the slow origin sends, and their size: 24 x 150 ms ~= 3.6 s.
const STREAM_CHUNKS: usize = 24;
const STREAM_INTERVAL: Duration = Duration::from_millis(150);
const CHUNK_BODY: &[u8] = b"data: x\n\n";

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/fetch-timeout")
        .canonicalize()
        .expect("fixture exists")
}

mod common;
use common::{announced, cello_command, forward_announce};

/// Tempdir for one fixture copy: `/dev/shm` when usable, else the default
/// tempdir.
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

fn copy_dir(src: &Path, dst: &Path, origin_port: u16) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    std::fs::copy(src.join("wrangler.json"), dst.join("wrangler.json"))
        .expect("copy wrangler.json");
    let worker = std::fs::read_to_string(src.join("worker.js")).expect("read worker.js");
    std::fs::write(
        dst.join("worker.js"),
        worker.replace("__ORIGIN_PORT__", &origin_port.to_string()),
    )
    .expect("write worker.js");
}

/// A booted `cello dev` and its address. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(app_project: &Path) -> Self {
        let fetch_budget = FETCH_BUDGET_S.to_string();
        let idle_budget = IDLE_BUDGET_S.to_string();
        let mut child = cello_command(
            app_project,
            [
                ("NO_COLOR", "1".to_string()),
                ("CELLO_FETCH_TIMEOUT_S", fetch_budget),
                ("CELLO_HTTP_STREAM_IDLE_S", idle_budget),
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

fn read_head(stream: &mut TcpStream) -> Vec<u8> {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).expect("read request head");
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            return head;
        }
        assert!(head.len() < 8192, "request head too long");
    }
}

/// One chunk in `Transfer-Encoding: chunked` framing.
fn write_chunk(stream: &mut TcpStream, body: &[u8]) {
    let _ = stream.write_all(format!("{:x}\r\n", body.len()).as_bytes());
    let _ = stream.write_all(body);
    let _ = stream.write_all(b"\r\n");
}

/// The deterministic origin: `/stream` keeps sending past the fetch budget,
/// `/head` accepts and never answers the head, `/stall` answers the head and
/// one chunk and then goes silent. A silent peer is held open for longer
/// than every budget under test, so a scenario can only end through a
/// timeout, never through the peer closing.
fn serve_origin(listener: TcpListener, hold: Duration) {
    for stream in listener.incoming() {
        let Ok(mut stream) = stream else {
            continue;
        };
        std::thread::spawn(move || {
            let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
            let head = read_head(&mut stream);
            if head.starts_with(b"GET /stream ") {
                let _ = stream.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                );
                for _ in 0..STREAM_CHUNKS {
                    write_chunk(&mut stream, CHUNK_BODY);
                    let _ = stream.flush();
                    std::thread::sleep(STREAM_INTERVAL);
                }
                let _ = stream.write_all(b"0\r\n\r\n");
            } else if head.starts_with(b"GET /stall ") {
                let _ = stream.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                );
                write_chunk(&mut stream, CHUNK_BODY);
                let _ = stream.flush();
                std::thread::sleep(hold);
            } else {
                // `/head`: accept, read the request, answer nothing.
                std::thread::sleep(hold);
            }
        });
    }
}

fn json_of(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap_or_else(|error| panic!("result JSON ({error}): {body}"))
}

fn field<'a>(out: &'a serde_json::Value, name: &str) -> &'a serde_json::Value {
    out.get(name)
        .unwrap_or_else(|| panic!("{name} missing: {out}"))
}

fn millis(out: &serde_json::Value, name: &str) -> u64 {
    field(out, name)
        .as_u64()
        .unwrap_or_else(|| panic!("{name} is not a number: {out}"))
}

#[test]
fn fetch_budget_bounds_the_head_and_not_a_live_body() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind origin");
    let origin_port = listener.local_addr().expect("origin addr").port();
    // Held longer than any budget under test: a scenario can only end
    // through a timeout, never because the origin gave up and closed.
    std::thread::spawn(move || serve_origin(listener, Duration::from_secs(20)));

    let fixture = fixture_dir();
    let project = fixture_tempdir("cello-fetch-timeout-");
    copy_dir(&fixture, project.path(), origin_port);
    let node = TestNode::boot(project.path());

    // A head that never arrives still ends at the fetch budget: the anti-hang
    // bound the knob exists for.
    let (status, body) = get(&node.public, "/head").expect("GET /head");
    assert_eq!(status, 200, "GET /head: {body}");
    let out = json_of(&body);
    assert_eq!(
        field(&out, "ok").as_bool(),
        Some(false),
        "a fetch whose head never arrives must reject: {body}"
    );
    let ms = millis(&out, "ms");
    assert!(
        (FETCH_BUDGET_S * 500..=FETCH_BUDGET_S * 3000).contains(&ms),
        "the head bound fired at {ms} ms, not around {FETCH_BUDGET_S} s: {body}"
    );

    // A live body outlives the fetch budget: every chunk arrives, and the
    // body is not cut when the budget it took to get the head is spent.
    let (status, body) = get(&node.public, "/stream").expect("GET /stream");
    assert_eq!(status, 200, "GET /stream: {body}");
    let out = json_of(&body);
    assert_eq!(
        field(&out, "ok").as_bool(),
        Some(true),
        "a body that keeps sending must outlive CELLO_FETCH_TIMEOUT_S: {body}"
    );
    assert_eq!(
        field(&out, "bytes").as_u64(),
        Some((STREAM_CHUNKS * CHUNK_BODY.len()) as u64),
        "every chunk arrives: {body}"
    );
    assert!(
        millis(&out, "ms") >= FETCH_BUDGET_S * 1000,
        "the body ran longer than the fetch budget, so a total deadline would have cut it: {body}"
    );

    // A body that goes silent still ends, at the idle deadline: removing the
    // total deadline must not remove the bound on a black-holed peer.
    let (status, body) = get(&node.public, "/stall").expect("GET /stall");
    assert_eq!(status, 200, "GET /stall: {body}");
    let out = json_of(&body);
    assert!(
        field(&out, "firstBytes")
            .as_u64()
            .is_some_and(|bytes| bytes > 0),
        "the stalled body started before it stalled: {body}"
    );
    assert_eq!(
        field(&out, "ok").as_bool(),
        Some(false),
        "a body that stops sending must end: {body}"
    );
    let idle = millis(&out, "idleMs");
    assert!(
        (IDLE_BUDGET_S * 500..=IDLE_BUDGET_S * 3000).contains(&idle),
        "the idle deadline fired at {idle} ms, not around {IDLE_BUDGET_S} s: {body}"
    );
}
