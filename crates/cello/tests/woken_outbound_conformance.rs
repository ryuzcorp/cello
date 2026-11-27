// Modified by Ryuz Corp (Cello): woken-continuation outbound conformance (upstream #258); live-ambient adoption legs (#299, #303). Apache-2.0.

// A black-box runner, not node code: it boots the real `cello dev` binary
// and drives the issue's self-vs-woken outbound dance, so the execution
// boundary's injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for outbound calls made from a continuation woken by another
//! event (denoland/celld#258): request A starts async work and returns; a
//! later request resolves the promise it waits on; the continuation then
//! makes an outbound `fetch()`, a storage write, or a facet RPC. Under
//! live-ambient adoption (#299, #303) the woken continuation runs as the
//! live same-cell event that woke it, so every leg settles exactly the way
//! workerd settles it — including the finished-fetch facet loop and the
//! own-cell storage write the retired-event refusal used to forbid.
//!
//! The cancellation leg proves adopted work retires with its adopter: the
//! adopting event's background budget drops the adopted fetch, and the
//! server observes the client disconnect. The forgery leg pins the other
//! side: a live event claiming another cell's scope is still refused, so
//! adoption never charges one cell for another's work. A genuinely
//! orphaned continuation keeps the existing loud failure (pinned
//! white-box by `retired_event_attribution`).
//!
//! One `cello dev` boots the fixture per test. No fixed sleeps on the pass
//! path: each leg polls its result with a deadline, and the server guard
//! kills the child on every exit path.

mod common;

use common::{announced, cello_command, forward_announce, NO_EXTRA_ENV};
use std::ffi::OsStr;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/woken-outbound")
        .canonicalize()
        .expect("fixture exists")
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
    fn boot(project: &Path) -> Self {
        Self::boot_with_env(project, NO_EXTRA_ENV)
    }

    fn boot_with_env(
        project: &Path,
        extra_env: impl IntoIterator<Item = (impl AsRef<OsStr>, impl AsRef<OsStr>)>,
    ) -> Self {
        let mut child = cello_command(project, extra_env)
            .arg("dev")
            .arg(project)
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
        // Drain stdout so a chatty child can never block on a full pipe.
        // Stderr is collected line by line: it carries the node's announce
        // lines, the only channel to its bound addresses now that the node
        // is non-dumpable (patch 0053).
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = stdout.read_to_end(&mut sink);
        });
        let announced_lines: Arc<Mutex<Vec<String>>> = Arc::default();
        forward_announce(stderr, Arc::clone(&announced_lines));
        // The kill-on-drop guard exists before the readiness poll, so a
        // boot timeout or an early child exit cannot leak the server.
        let mut node = Self {
            child,
            public: String::new(),
        };
        // Readiness is the announced public address answering with the app
        // loaded. The port is ephemeral (`--port 0`): the announce line is
        // the node's own report of where it bound, so no probe-then-bind
        // race can hand us another test's node.
        node.public = poll_boot(&mut node, &announced_lines);
        node
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn poll_boot(node: &mut TestNode, announced_lines: &Arc<Mutex<Vec<String>>>) -> String {
    let start = std::time::Instant::now();
    loop {
        if let Some(status) = node.child.try_wait().expect("poll child") {
            panic!(
                "cello dev exited during boot: {status}: {}",
                announced_lines.lock().expect("announce lock").join("\n")
            );
        }
        if let Some(public) = announced(announced_lines, "cello listening on ") {
            if probe(&public) {
                return public;
            }
        }
        if start.elapsed() > Duration::from_secs(120) {
            panic!("boot: condition unmet within the deadline");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Readiness probe with a short timeout: a bound-but-not-ready node holds
/// connections open, so a long read would eat the boot deadline.
fn probe(addr: &str) -> bool {
    matches!(
        http_get(addr, "/health", Duration::from_secs(2)),
        Ok((200, _))
    )
}

fn http_get(addr: &str, path: &str, timeout: Duration) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect(addr).map_err(|error| format!("connect: {error}"))?;
    stream
        .set_read_timeout(Some(timeout))
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

/// Drive one leg of the dance and return its settled result, or fail when
/// the waiter is still `pending` at the deadline: that is the hang this
/// test pins against.
fn run_leg(node: &TestNode, begin: &str, woken: bool, name: &str) -> String {
    run_leg_with_deadline(node, begin, woken, name, Duration::from_secs(15))
}

/// The same dance with an explicit settle deadline, for legs whose
/// settlement waits on a budget (cancellation) or a facet first-open.
fn run_leg_with_deadline(
    node: &TestNode,
    begin: &str,
    woken: bool,
    name: &str,
    deadline: Duration,
) -> String {
    let (status, _) = http_get(&node.public, begin, Duration::from_secs(15)).expect("begin");
    assert_eq!(status, 200, "{begin}");
    if woken {
        let (status, _) = http_get(
            &node.public,
            &format!("/notify?name={name}"),
            Duration::from_secs(15),
        )
        .expect("notify");
        assert_eq!(status, 200);
    } else {
        // The self wake is a 200 ms timer; give it room without asserting
        // on timing.
        std::thread::sleep(Duration::from_millis(500));
    }
    let start = std::time::Instant::now();
    loop {
        let (status, body) = http_get(
            &node.public,
            &format!("/result?name={name}"),
            Duration::from_secs(15),
        )
        .expect("result answers");
        assert_eq!(status, 200, "{body}");
        let result = body.trim().to_string();
        if result != "pending" {
            return result;
        }
        if start.elapsed() > deadline {
            panic!("woken waiter never settled (still pending)");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

#[test]
fn woken_continuation_outbound_work_settles() {
    let dir = tempfile::Builder::new()
        .prefix("cello-woken-outbound-")
        .tempdir()
        .expect("tempdir");
    let project = dir.path().join("app");
    copy_dir(&fixture_dir(), &project);
    let node = TestNode::boot(&project);

    // Control first: woken by its own timer, everything succeeds.
    let self_fetch = run_leg(&node, "/self?how=fetch&name=sfetch", false, "sfetch");
    assert_eq!(self_fetch, "ok plain ok", "{self_fetch}");
    let self_store = run_leg(&node, "/self?how=storage&name=sstore", false, "sstore");
    assert_eq!(self_store, "ok stored", "{self_store}");
    let self_facet = run_leg_with_deadline(
        &node,
        "/self?how=facet&name=scoop",
        false,
        "scoop",
        Duration::from_secs(30),
    );
    assert_eq!(self_facet, "ok pong:x", "{self_facet}");

    // Woken by another request: under live-ambient adoption the
    // continuation runs as the live same-cell event that woke it, so every
    // leg settles exactly the way workerd settles it.
    let woken_fetch = run_leg(&node, "/start?how=fetch&name=wfetch", true, "wfetch");
    assert_eq!(woken_fetch, "ok plain ok", "{woken_fetch}");
    let woken_store = run_leg(&node, "/start?how=storage&name=wstore", true, "wstore");
    assert_eq!(woken_store, "ok stored", "{woken_store}");
    let woken_facet = run_leg_with_deadline(
        &node,
        "/start?how=facet&name=wcoop",
        true,
        "wcoop",
        Duration::from_secs(30),
    );
    assert_eq!(woken_facet, "ok pong:x", "{woken_facet}");
}

/// Adopted work retires with its adopter: the adopting event's background
/// budget drops the adopted fetch, and the server observes the client
/// disconnect. Before adoption the woken fetch threw immediately, so the
/// hanging endpoint was never reached and no cancellation was recorded.
#[test]
fn adopted_outbound_cancels_with_adopting_event() {
    let dir = tempfile::Builder::new()
        .prefix("cello-woken-cancel-")
        .tempdir()
        .expect("tempdir");
    let project = dir.path().join("app");
    copy_dir(&fixture_dir(), &project);
    let node = TestNode::boot_with_env(&project, [("CELLO_BACKGROUND_BUDGET_S", "10")]);

    let result = run_leg_with_deadline(
        &node,
        "/start-slow?name=wcancel",
        true,
        "wcancel",
        Duration::from_secs(40),
    );
    assert!(
        result.starts_with("cancelled:"),
        "adopted fetch was not cancelled with its adopter: {result}"
    );

    // The cell still serves: dropping the adopted op wedged nothing.
    let again = run_leg(&node, "/self?how=storage&name=safter", false, "safter");
    assert_eq!(again, "ok stored", "{again}");
}

/// A live event claiming another cell's scope is refused, adoption or not:
/// the scope check compares against the executing cell, so one cell's work
/// never charges another cell.
#[test]
fn executing_cell_scope_refuses_foreign_claim() {
    let dir = tempfile::Builder::new()
        .prefix("cello-woken-forge-")
        .tempdir()
        .expect("tempdir");
    let project = dir.path().join("app");
    copy_dir(&fixture_dir(), &project);
    let node = TestNode::boot(&project);

    let (status, scope) = http_get(
        &node.public,
        "/expose-scope?name=fa",
        Duration::from_secs(15),
    )
    .expect("expose scope");
    assert_eq!(status, 200);
    let scope = scope.trim().to_string();
    assert!(!scope.is_empty(), "empty scope");

    let (status, body) = http_get(
        &node.public,
        &format!("/forge?name=fb&scope={scope}&key=k"),
        Duration::from_secs(15),
    )
    .expect("forge answers");
    assert_eq!(status, 200, "{body}");
    let body = body.trim().to_string();
    assert!(
        body.starts_with("forged-refused:"),
        "foreign scope was not refused: {body}"
    );
    assert!(
        body.contains("not bound to the executing cell"),
        "unexpected refusal shape: {body}"
    );
}
