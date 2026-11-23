// Modified by Ryuz Corp (Cello): streaming-response background-budget regression test (upstream #275).
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// A black-box runner, not node code: it boots the binary, copies the
// fixture, and reads TCP with wall-clock deadlines, so the execution
// boundary's injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! A streaming response body is response work, not `waitUntil` work
//! (upstream #275). `__bridgeResponseStream` used to register its pump with
//! `waitUntil`, so the post-response background budget charged the body: one
//! budget window after the response head, the driver sealed the `waitUntil`
//! window and dropped the pending `__response_stream_write`, and every SSE
//! stream died silently — the socket stayed open with no bytes, no close and
//! no error. The pump now holds its own response-body lifetime, so it streams
//! for as long as the client stays connected and the body keeps producing,
//! while `ctx.waitUntil()` work stays bounded by the budget as patch 0101
//! intends.
//!
//! The node runs `cello dev` with `CELLO_BACKGROUND_BUDGET_S=2`, so a body
//! that outlives the budget proves it is not charged to it. The fixture's
//! transforms tick every 100 ms:
//!
//! * the module worker's `/stream` (stateless) and the cell's `/cell/stream`
//!   must keep delivering frames several budget windows past the head;
//! * `/wait-forever` parks a never-settling `waitUntil` that the budget must
//!   still end — while a cell stream stays healthy;
//! * `/cell/stream-with-wait-forever` parks that claim in the same entry that
//!   streams, so the budget must cut the claim and leave the body flowing;
//! * dropping the client mid-stream must cancel the producer through the
//!   request signal, which is the existing disconnect path.
//!
//! There are no fixed sleeps: every wait polls an observable condition with a
//! deadline, and the server guard kills the child on every exit path.

mod common;

use common::{announced, cello_command, forward_announce};
use serde_json::Value;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The budget every node in this suite runs with. Two seconds is long enough
/// for the fixture's first ticks to cross the wire and short enough that the
/// read windows below stay cheap.
const BACKGROUND_BUDGET_S: u64 = 2;
const BUDGET: Duration = Duration::from_secs(BACKGROUND_BUDGET_S);

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/stream-budget")
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
    copy_dir_with_origin(src, dst, None);
}

/// Copy the fixture, substituting the drip origin's port in `index.js`. Every
/// other file is copied byte for byte.
fn copy_dir_with_origin(src: &Path, dst: &Path, drip_port: Option<u16>) {
    std::fs::create_dir_all(dst).expect("copy fixture dir");
    for entry in std::fs::read_dir(src).expect("read fixture dir") {
        let entry = entry.expect("fixture entry");
        let to = dst.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir_with_origin(&entry.path(), &to, drip_port);
        } else if drip_port.is_some() && entry.file_name() == "index.js" {
            let source = std::fs::read_to_string(entry.path()).expect("read fixture worker");
            std::fs::write(
                to,
                source.replace(
                    "__DRIP_PORT__",
                    &drip_port.expect("drip port is set").to_string(),
                ),
            )
            .expect("write fixture worker");
        } else {
            std::fs::copy(entry.path(), to).expect("copy fixture file");
        }
    }
}

/// A booted `cello dev` and its addresses. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
struct TestNode {
    child: Child,
    public: String,
    lines: Arc<Mutex<Vec<String>>>,
}

impl TestNode {
    fn boot(project: &Path) -> Self {
        Self::boot_with(project, &[])
    }

    fn boot_with(project: &Path, extra_env: &[(&str, String)]) -> Self {
        let mut env = vec![
            ("NO_COLOR".to_string(), "1".to_string()),
            (
                "CELLO_BACKGROUND_BUDGET_S".to_string(),
                BACKGROUND_BUDGET_S.to_string(),
            ),
        ];
        env.extend(
            extra_env
                .iter()
                .map(|(name, value)| ((*name).to_string(), value.clone())),
        );
        // Port zero: the OS assigns the worker port, so parallel boots never
        // claim the same one; discovery below learns it.
        let mut child = cello_command(project, env)
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
            lines: Arc::clone(&announced_lines),
        };
        let public = poll_until("boot", Duration::from_secs(180), || {
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!(
                    "cello dev exited during boot: {status}: {}",
                    announced_lines.lock().expect("announce lock").join("\n")
                );
            }
            let public = announced(&announced_lines, "cello listening on ")?;
            probe(&public, "/health").then_some(public)
        });
        node.public = public;
        node
    }

    fn log_lines(&self, needle: &str) -> Vec<String> {
        self.lines
            .lock()
            .expect("announce lock")
            .iter()
            .filter(|line| line.contains(needle))
            .cloned()
            .collect()
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// One complete HTTP/1.1 exchange, for the fixture's JSON routes.
fn request(
    addr: &str,
    method: &str,
    path: &str,
    timeout: Duration,
) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect(addr).map_err(|error| format!("connect: {error}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|error| format!("timeout: {error}"))?;
    let wire = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: */*\r\n\r\n"
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

fn get(addr: &str, path: &str) -> Result<(u16, String), String> {
    request(addr, "GET", path, Duration::from_secs(30))
}

fn probe(addr: &str, path: &str) -> bool {
    matches!(
        request(addr, "GET", path, Duration::from_secs(2)),
        Ok((200, _))
    )
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
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// An open SSE response, read incrementally.
struct SseStream {
    stream: TcpStream,
}

impl SseStream {
    fn open(addr: &str, path: &str) -> Self {
        let mut stream = TcpStream::connect(addr).expect("connect stream");
        // Short reads keep the deadline loop responsive without treating a
        // quiet window as the end of the body.
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("stream read timeout");
        let wire =
            format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nAccept: text/event-stream\r\n\r\n");
        stream
            .write_all(wire.as_bytes())
            .expect("write stream request");
        Self { stream }
    }

    /// Append every `data: tick <n>` frame read until `until`, with the
    /// instant each arrived. Stops early when the peer closes.
    fn read_ticks(&mut self, until: Instant, ticks: &mut Vec<(Instant, u64)>) {
        let mut pending = String::new();
        let mut buf = [0u8; 4096];
        while Instant::now() < until {
            match self.stream.read(&mut buf) {
                Ok(0) => return,
                Ok(read) => {
                    pending.push_str(&String::from_utf8_lossy(&buf[..read]));
                    while let Some(end) = pending.find('\n') {
                        let line: String = pending.drain(..=end).collect();
                        let Some(rest) = line.trim_end().strip_prefix("data: tick ") else {
                            continue;
                        };
                        if let Ok(tick) = rest.trim().parse::<u64>() {
                            ticks.push((Instant::now(), tick));
                        }
                    }
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(error) => panic!("stream read failed: {error}"),
            }
        }
    }
}

fn boot_node() -> (tempfile::TempDir, TestNode) {
    let dir = fixture_tempdir("cello-stream-budget-");
    let app_project = dir.path().join("app");
    copy_dir(&fixture_dir(), &app_project);
    let node = TestNode::boot(&app_project);
    (dir, node)
}

fn boot_node_with_origin(origin: &DripOrigin) -> (tempfile::TempDir, TestNode) {
    let dir = fixture_tempdir("cello-stream-budget-");
    let app_project = dir.path().join("app");
    copy_dir_with_origin(&fixture_dir(), &app_project, Some(origin.port));
    // The idle deadline must not be what ends the background read: every
    // drip refreshes it, and it is set well past the budget anyway.
    let node = TestNode::boot_with(
        &app_project,
        &[("CELLO_HTTP_STREAM_IDLE_S", IDLE_BUDGET_S.to_string())],
    );
    (dir, node)
}

/// How often the drip origin emits one byte. Longer than the background
/// budget, so a read that is only ended by the driver's next wake overruns
/// it visibly, and far shorter than the idle deadline.
const DRIP_INTERVAL: Duration = Duration::from_millis(1_200);
/// The stream idle deadline every drip node runs with.
const IDLE_BUDGET_S: u64 = 30;

/// A loopback origin that answers any GET with a chunked body that emits one
/// byte per [`DRIP_INTERVAL`] forever, and records when a client closes.
///
/// It is the upstream a `waitUntil` body read can hold: every byte refreshes
/// the HTTP stream idle deadline, so only the background budget can end it.
struct DripOrigin {
    port: u16,
    closed_at: Arc<Mutex<Option<Instant>>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    listener: Option<std::thread::JoinHandle<()>>,
}

impl DripOrigin {
    fn start() -> Self {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("bind drip origin to a free port");
        let port = listener.local_addr().expect("drip origin address").port();
        let closed_at: Arc<Mutex<Option<Instant>>> = Arc::default();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let accepted = Arc::clone(&closed_at);
        let stop_flag = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            for connection in listener.incoming() {
                if stop_flag.load(std::sync::atomic::Ordering::Acquire) {
                    return;
                }
                let Ok(mut stream) = connection else { continue };
                let closed = Arc::clone(&accepted);
                std::thread::spawn(move || {
                    let mut request = [0u8; 1024];
                    let _ = stream.read(&mut request);
                    let head = b"HTTP/1.1 200 OK\r\ncontent-type: application/octet-stream\r\n\
                                 transfer-encoding: chunked\r\n\r\n";
                    if stream.write_all(head).is_err() {
                        *closed.lock().expect("closed lock") = Some(Instant::now());
                        return;
                    }
                    loop {
                        if stream.write_all(b"1\r\nx\r\n").is_err() {
                            *closed.lock().expect("closed lock") = Some(Instant::now());
                            return;
                        }
                        std::thread::sleep(DRIP_INTERVAL);
                    }
                });
            }
        });
        Self {
            port,
            closed_at,
            stop,
            listener: Some(handle),
        }
    }

    /// When a client closed its connection, if one has.
    fn closed_at(&self) -> Option<Instant> {
        *self.closed_at.lock().expect("closed lock")
    }
}

impl Drop for DripOrigin {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        // Wake the blocking accept so the listener thread observes `stop`.
        let _ = TcpStream::connect(("127.0.0.1", self.port));
        if let Some(handle) = self.listener.take() {
            let _ = handle.join();
        }
    }
}

/// Span between the first and last frame read, and the count.
fn span(ticks: &[(Instant, u64)]) -> (Duration, usize) {
    match (ticks.first(), ticks.last()) {
        (Some((first, _)), Some((last, _))) => (last.duration_since(*first), ticks.len()),
        _ => (Duration::ZERO, ticks.len()),
    }
}

/// A streamed body keeps producing past the background budget, in a stateless
/// Worker and in a cell: before the fix the pump was `waitUntil` work, and the
/// driver sealed it one budget window after the response head.
#[test]
fn streamed_response_body_outlives_the_background_budget() {
    let (_dir, node) = boot_node();
    let windows = [
        ("/stream", "stateless Worker body"),
        ("/cell/stream", "cell body"),
    ];
    for (path, what) in windows {
        let mut sse = SseStream::open(&node.public, path);
        let mut ticks = Vec::new();
        sse.read_ticks(Instant::now() + BUDGET * 5, &mut ticks);
        let (elapsed, count) = span(&ticks);
        assert!(
            count >= 4,
            "{what}: the stream must deliver frames at all ({count} frames in {elapsed:?})"
        );
        assert!(
            elapsed >= BUDGET * 4,
            "{what}: frames must keep arriving past the {BACKGROUND_BUDGET_S}s background \
             budget, but the last {count} arrived within {elapsed:?} of the first"
        );
    }
}

/// `ctx.waitUntil()` work that never settles is still ended by the budget
/// while a streamed response stays healthy: the fix separates the two
/// lifetimes instead of exempting the whole entry.
#[test]
fn waituntil_is_cut_at_the_budget_while_a_stream_stays_healthy() {
    let (_dir, node) = boot_node();
    let mut sse = SseStream::open(&node.public, "/cell/stream");
    let mut ticks = Vec::new();
    sse.read_ticks(Instant::now() + BUDGET, &mut ticks);
    assert!(
        !ticks.is_empty(),
        "the cell stream must deliver frames before the waitUntil probe"
    );
    let (status, body) = get(&node.public, "/wait-forever").expect("waitUntil probe answers");
    assert_eq!((status, body.as_str()), (200, "ok"));
    // Keep reading while polling for the budget's log line: the stream has to
    // be observed healthy after the leftover background work is ended.
    let started = Instant::now();
    let deadline = started + Duration::from_secs(30);
    let mut ended_at = None;
    let mut ticks_after = Vec::new();
    while Instant::now() < deadline {
        sse.read_ticks(
            Instant::now() + Duration::from_millis(200),
            &mut ticks_after,
        );
        if ended_at.is_none()
            && !node
                .log_lines("post-response work outlived its budget; ending it")
                .is_empty()
        {
            ended_at = Some(Instant::now());
        }
        if let Some(ended_at) = ended_at {
            // Two more budget windows of frames after the waitUntil ended.
            sse.read_ticks(ended_at + BUDGET * 2, &mut ticks_after);
            break;
        }
    }
    assert!(
        ended_at.is_some(),
        "a never-settling waitUntil must be ended by the {}s background budget; \
         the stream instead kept the entry alive forever",
        BACKGROUND_BUDGET_S
    );
    assert!(
        ticks_after.len() >= 4,
        "the stream must stay healthy after the budget ended the leftover waitUntil \
         ({} frames)",
        ticks_after.len()
    );
    // The new generation of frames is what proves it: their arrival is after
    // the log line, not a backlog flushed once.
    assert!(
        ticks_after
            .last()
            .is_some_and(|(at, _)| *at > ended_at.expect("ended")
                && at.duration_since(ended_at.expect("ended")) >= BUDGET),
        "frames must keep arriving at least one budget past the waitUntil cutoff"
    );
}

/// The budget still ends a `waitUntil` claim the streaming entry itself
/// carries: the body and the background claim are separate lifetimes, so an
/// open stream must not postpone the claim's cutoff.
#[test]
fn same_entry_waituntil_is_cut_at_the_budget_while_its_body_streams() {
    let (_dir, node) = boot_node();
    let mut sse = SseStream::open(&node.public, "/cell/stream-with-wait-forever");
    let mut ticks = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut ended_at = None;
    while Instant::now() < deadline {
        sse.read_ticks(Instant::now() + Duration::from_millis(200), &mut ticks);
        if ended_at.is_none()
            && !node
                .log_lines("post-response work outlived its budget while a response body streams")
                .is_empty()
        {
            ended_at = Some(Instant::now());
        }
        if let Some(ended_at) = ended_at {
            // A budget window of frames after the claim ended.
            sse.read_ticks(ended_at + BUDGET * 2, &mut ticks);
            break;
        }
    }
    let ended_at = ended_at.unwrap_or_else(|| {
        panic!(
            "the waitUntil claim of the streaming entry was never ended by the \
             {}s background budget; an open body must not postpone it",
            BACKGROUND_BUDGET_S
        )
    });
    let after = ticks.iter().filter(|(at, _)| *at > ended_at).count();
    assert!(
        after >= 4,
        "the body must keep streaming after the budget ended its waitUntil claim \
         ({after} frames arrived in the next two budget windows)"
    );
}

/// Dropping the client mid-stream still cancels the producer through its
/// request signal: a stream that outlives the budget must not outlive its
/// client either.
#[test]
fn client_disconnect_cancels_the_stream_producer() {
    let (_dir, node) = boot_node();
    let mut sse = SseStream::open(&node.public, "/cell/stream");
    let mut ticks = Vec::new();
    sse.read_ticks(Instant::now() + BUDGET, &mut ticks);
    assert!(
        ticks.len() >= 2,
        "the stream must deliver frames before the disconnect ({} frames)",
        ticks.len()
    );
    drop(sse);
    let state = poll_until(
        "producer observes the disconnect",
        Duration::from_secs(20),
        || {
            let (status, body) = get(&node.public, "/cell/state").ok()?;
            let state: Value = serde_json::from_str(&body).ok()?;
            (status == 200 && state["aborted"].as_bool()?).then_some(body)
        },
    );
    let observed = serde_json::from_str::<Value>(&state).expect("state json")["ticks"]
        .as_u64()
        .expect("tick count");
    // The producer stops: a live loop would tick about twenty times in the
    // window below. One more frame can be in flight when the signal fires.
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut highest = observed;
    while Instant::now() < deadline {
        let (status, body) = get(&node.public, "/cell/state").expect("state answers");
        assert_eq!(status, 200);
        let state: Value = serde_json::from_str(&body).expect("state json");
        assert_eq!(
            state["aborted"].as_bool(),
            Some(true),
            "the disconnect must keep the request signal aborted"
        );
        highest = highest.max(state["ticks"].as_u64().expect("tick count"));
    }
    assert!(
        highest <= observed + 1,
        "the producer must stop after the disconnect, but its tick count went from \
         {observed} to {highest}"
    );
}

/// A body that never produces is ended by the 60 s idle deadline even though
/// the host holds its transfer lease and the client stays connected: taking
/// the body out of the background budget must not leave a silent stream
/// pinning its isolate for as long as the socket lives.
#[test]
fn idle_response_body_ends_at_the_stream_deadline() {
    let (_dir, node) = boot_node();
    let mut stream = TcpStream::connect(&node.public).expect("connect idle stream");
    stream
        .set_read_timeout(Some(Duration::from_millis(250)))
        .expect("idle stream read timeout");
    let wire = format!(
        "GET /idle HTTP/1.1\r\nHost: {}\r\nAccept: text/event-stream\r\n\r\n",
        node.public
    );
    stream
        .write_all(wire.as_bytes())
        .expect("write idle request");
    let opened = Instant::now();
    let mut buf = [0u8; 4096];
    let ended = loop {
        assert!(
            opened.elapsed() < Duration::from_secs(120),
            "an idle response body must end at the 60 s stream deadline"
        );
        match stream.read(&mut buf) {
            Ok(0) => break opened.elapsed(),
            Ok(_) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(_) => break opened.elapsed(),
        }
    };
    assert!(
        ended >= Duration::from_secs(55),
        "the idle body ended after {ended:?}, before the 60 s stream deadline"
    );
}

/// A client that takes the head and then stops reading leaves the pump's
/// write blocked on backpressure. The stream deadline must still end that
/// write, so the producer is cancelled and the cell event released; dropping
/// the registry's senders alone cannot wake a send whose receiver nobody
/// polls.
#[test]
fn stalled_client_ends_the_blocked_write_at_the_stream_deadline() {
    let (_dir, node) = boot_node();
    let mut stream = TcpStream::connect(&node.public).expect("connect stalled stream");
    let wire = format!(
        "GET /cell/big-stream HTTP/1.1\r\nHost: {}\r\nAccept: text/event-stream\r\n\r\n",
        node.public
    );
    stream
        .write_all(wire.as_bytes())
        .expect("write stalled request");
    // Never read: the frames fill the socket buffers and the pump's next
    // host write blocks. (The producer parks in `writer.write()`, which the
    // TransformStream holds until the pump reads, so its tick count is no
    // stall signal either way.) The stream deadline (60 s without progress)
    // must end that blocked write; the pump then cancels the producer
    // through its request signal.
    let started = Instant::now();
    poll_until(
        "the stalled stream's producer is cancelled",
        Duration::from_secs(150),
        || {
            std::thread::sleep(Duration::from_millis(400));
            let (status, body) = get(&node.public, "/cell/state").ok()?;
            let state: Value = serde_json::from_str(&body).ok()?;
            (status == 200 && state["aborted"].as_bool()?).then_some(body)
        },
    );
    let ended = started.elapsed();
    assert!(
        ended >= Duration::from_secs(55),
        "the stalled stream ended after {ended:?}, before the 60 s stream deadline"
    );
    drop(stream);
}

/// A fetch body read only by `ctx.waitUntil()` work outlives the idle
/// deadline (every drip byte refreshes it) but not the background budget
/// (#141). The read must be rejected with an error the Worker can observe,
/// and the upstream request must be cancelled: before the fix the driver's
/// abort pass dropped the read silently, leaving neither a settled promise
/// nor a closed upstream socket until a later drip woke it.
#[test]
fn waituntil_body_read_ends_at_the_background_budget() {
    let origin = DripOrigin::start();
    let (_dir, node) = boot_node_with_origin(&origin);
    let started = Instant::now();
    let (status, body) = get(&node.public, "/drip-waituntil").expect("drip route answers");
    assert_eq!((status, body.as_str()), (200, "ok"));
    let state = poll_until(
        "the waitUntil body read is rejected",
        Duration::from_secs(15),
        || {
            let (status, body) = get(&node.public, "/drip-state").ok()?;
            if status != 200 {
                return None;
            }
            let state: Value = serde_json::from_str(&body).ok()?;
            state["error"].is_string().then_some(body)
        },
    );
    let elapsed = started.elapsed();
    let state: Value = serde_json::from_str(&state).expect("state json");
    let error = state["error"].as_str().expect("error is a string");
    assert!(
        error.contains("background budget"),
        "the read must fail with the background-budget Error, got {error:?}"
    );
    // The Worker measures the read itself; the outer elapsed only guards the
    // HTTP round trip, so a loaded CI runner cannot turn this into a flake.
    let read_ms = state["elapsed"].as_u64().expect("the read measured itself");
    assert!(
        read_ms <= (BUDGET + Duration::from_secs(1)).as_millis() as u64,
        "the read must end at the {BACKGROUND_BUDGET_S}s background budget, not \
         {read_ms} ms after it started"
    );
    assert!(
        elapsed <= BUDGET + Duration::from_secs(5),
        "the rejection must reach the Worker promptly ({elapsed:?} after the request)"
    );
    poll_until(
        "the upstream request is cancelled",
        Duration::from_secs(5),
        || origin.closed_at().map(|_| String::new()),
    );
    let closed = origin.closed_at().expect("upstream connection closed");
    // The origin only observes the close when a later write hits the closed
    // socket, so allow a couple of drip intervals for that. The point is
    // that the connection ends while nothing is reading it, not that the
    // origin's next write is prompt.
    assert!(
        closed.duration_since(started) <= BUDGET + DRIP_INTERVAL * 3,
        "the upstream socket must close once the read is cut (closed {:?} after \
         the request)",
        closed.duration_since(started)
    );
}

/// A response body that reads an upstream body through a JS pump is response
/// work: the client bounds it, so the background budget must not cut it
/// (#139, and Noite's control-plane streams). Only a body read by
/// `waitUntil` work alone carries the background deadline.
#[test]
fn proxied_body_read_outlives_the_background_budget() {
    let origin = DripOrigin::start();
    let (_dir, node) = boot_node_with_origin(&origin);
    let mut stream = TcpStream::connect(&node.public).expect("connect proxy stream");
    stream
        .set_read_timeout(Some(Duration::from_millis(200)))
        .expect("proxy stream read timeout");
    let wire = format!(
        "GET /proxy-stream HTTP/1.1\r\nHost: {}\r\nAccept: */*\r\n\r\n",
        node.public
    );
    stream
        .write_all(wire.as_bytes())
        .expect("write proxy request");
    let started = Instant::now();
    let mut raw = Vec::new();
    let mut header_end = None;
    let mut body_arrivals = Vec::new();
    let mut buf = [0u8; 4096];
    while started.elapsed() < BUDGET * 6 {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(read) => {
                raw.extend_from_slice(&buf[..read]);
                if let Some(end) = header_end {
                    if raw.len() > end {
                        body_arrivals.push(Instant::now());
                    }
                } else if let Some(position) =
                    raw.windows(4).position(|window| window == b"\r\n\r\n")
                {
                    header_end = Some(position + 4);
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(error) => panic!("proxy stream read failed: {error}"),
        }
    }
    assert!(
        body_arrivals.len() >= 3,
        "the proxied body must keep producing ({})",
        body_arrivals.len()
    );
    let span = body_arrivals
        .last()
        .expect("body arrived")
        .duration_since(*body_arrivals.first().expect("body arrived"));
    assert!(
        span >= BUDGET * 4,
        "a proxied response body must outlive the {BACKGROUND_BUDGET_S}s background \
         budget while the client reads, but its bytes spanned only {span:?}"
    );
    drop(stream);
}
