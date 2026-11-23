// Modified by Ryuz Corp (Cello): Queue capacity-refusal conformance. Apache-2.0.

// A black-box runner, not node code: it boots the binary, copies the
// fixture, reads the cells' SQLite files, and polls TCP with wall-clock
// deadlines, so the execution boundary's injected clock/filesystem/async
// rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! A Queue batch a full isolate refuses must not spend a delivery attempt.
//!
//! The per-isolate `ArrayBuffer` budget (`CELLO_V8_EXTERNAL_LIMIT_MB`, #110)
//! refuses a batch whose message bodies do not fit the isolate the consumer
//! script's pool admits the dispatch into. The consumer never runs, so the
//! delivery never happened -- but the broker's only way back from a dispatch
//! the host gave up on was a lease expiry, and an expiry spends an attempt.
//! Under memory pressure a message therefore walked to `max_retries` and the
//! dead-letter queue without the handler seeing it once.
//!
//! The fixture makes that reproducible: `/fill` retains buffers in module
//! scope on the warm isolate the pool reuses for the next dispatch, and
//! `refuse-src` runs with `max_retries: 0` plus a dead-letter queue, so the
//! first attempt charged for a refusal dead-letters the message on the spot.
//! `/free` drops the buffers; the dispatch then fits and the handler acks.
//!
//! Before the fix the released-row poll fails: no row is ever visible again
//! with `attempts = 0`, because the lease stays installed until it expires and
//! the expiry charges the attempt (and, with `max_retries: 0`, dead-letters
//! the message). After the fix a refused batch is released with the
//! settlement's own fence, an attempt-free backoff (recorded in the cell's
//! metadata) keeps it from being redelivered into the same full isolate in a
//! loop, and `attempts` reaches the consumer as `1`.
//!
//! The free release is bounded, and the bound is policy, not this file: a
//! stateless isolate that stays full is *reclaimed* by the pool once it is idle
//! (the next dispatch lands on a fresh one with a full budget), so a black-box
//! test cannot hold an isolate refusable for a minute without also pinning
//! `CELLO_MAX_STATELESS_ISOLATES` and keeping it busy. `cello_logic::queue`'s
//! unit tests cover the bound, the pressure ladder, the unfittable batch and
//! the reset; this file covers the part only a real node can show. The second
//! test is the other half of the classifier: a consumer that throws is a
//! delivery that failed, so it must keep spending attempts.

mod common;

use common::{announced, cello_command, forward_announce};
use rusqlite::Connection;
use serde_json::Value;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The isolate budget the fixture works against. `/fill` retains 60 MiB of it
/// in module scope, which leaves the 4 MiB below the cap for everything else:
/// a single 120,000-byte send fits, and a 40-message batch (4.8 MB) does not.
/// The isolate is never filled to the last byte, so no request in the test
/// depends on a collection running first.
const EXTERNAL_LIMIT_MB: u64 = 64;
const FILL_MIB: u64 = 60;
const BATCH_MESSAGES: usize = 40;
const MESSAGE_BYTES: u64 = 120_000;

/// The broker's first refusal delay, in milliseconds, as a fact about the
/// wire rather than an import: the fail-before run compiles against the code
/// that has no such policy, and the assertion still has to mean something.
const REFUSAL_BACKOFF_MS: i64 = 1_000;

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/queue-refusal")
        .canonicalize()
        .expect("fixture exists")
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    for file in ["wrangler.json", "worker.js"] {
        std::fs::copy(src.join(file), dst.join(file)).expect("copy fixture");
    }
}

/// A booted `cello dev` and its public address. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(app_project: &Path, data_dir: &Path) -> Self {
        Self::boot_with(app_project, data_dir, [])
    }

    fn boot_with<const N: usize>(
        app_project: &Path,
        data_dir: &Path,
        extra: [(&str, &str); N],
    ) -> Self {
        let mut child = cello_command(
            app_project,
            [
                (
                    "CELLO_TEST_DATA_DIR",
                    data_dir.to_str().expect("utf8 data dir"),
                ),
                ("CELLO_V8_HEAP_LIMIT_MB", "64"),
                ("CELLO_V8_EXTERNAL_LIMIT_MB", "64"),
            ]
            .into_iter()
            .chain(extra),
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
        // Drain stdout so a chatty child can never block on a full pipe.
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
                panic!(
                    "cello dev exited during boot: {status}: {}",
                    announced_lines.lock().expect("announce lock").join("\n")
                );
            }
            let public = announced(&announced_lines, "cello listening on ")?;
            probe(&public, "/health").then_some(public)
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

fn request_timeout(
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
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
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
    request_timeout(addr, "GET", path, Duration::from_secs(60))
}

/// Readiness probe with a short timeout: a bound-but-not-ready node holds
/// connections open, so a long read would eat the boot deadline.
fn probe(addr: &str, path: &str) -> bool {
    matches!(
        request_timeout(addr, "GET", path, Duration::from_secs(2)),
        Ok((200, _))
    )
}

fn get_json(node: &TestNode, path: &str) -> Value {
    let (status, body) = get(&node.public, path).expect("public GET");
    assert_eq!(status, 200, "GET {path}: {body}");
    serde_json::from_str(&body).expect("JSON body")
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

fn cell_db(data_dir: &Path, queue: &str) -> PathBuf {
    data_dir
        .join(cello::engine_api::queue_cell_scope(queue))
        .join("ltx/e1/db.sqlite")
}

fn open_db(path: &Path) -> Connection {
    let db = Connection::open(path).expect("open cell db");
    db.busy_timeout(Duration::from_secs(10))
        .expect("set busy timeout");
    db
}

/// What the source queue row state says about one refused batch: how often the
/// cell released a refused batch (its persisted streak), when the rows were
/// enqueued, when they become visible again, and the highest attempt count any
/// row has spent.
#[derive(Debug)]
struct RefusalState {
    streak: u16,
    enqueued_at: i64,
    visible_at: i64,
    max_attempts: i64,
}

fn read_refusal_state(db: &Connection) -> RefusalState {
    let streak: String = db
        .query_row(
            "SELECT value FROM __queue_meta WHERE name = 'refusal_count'",
            [],
            |row| row.get(0),
        )
        .unwrap_or_else(|_| "0".to_string());
    let (enqueued_at, visible_at, max_attempts): (i64, i64, i64) = db
        .query_row(
            "SELECT COALESCE(MAX(enqueued_at), 0), COALESCE(MAX(visible_at), 0),
                    COALESCE(MAX(attempts), 0)
               FROM __queue_messages",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("read queue rows");
    RefusalState {
        streak: streak.parse().expect("refusal streak is a number"),
        enqueued_at,
        visible_at,
        max_attempts,
    }
}

/// Wait for the cell to have released the refused batch `streak` times with
/// no attempt spent. The last observed state goes into the failure, so a
/// fail-before run names what actually happened (no release ever, or a spent
/// attempt).
fn wait_for_streak(db: &Connection, streak: u16, deadline: Duration) -> RefusalState {
    let start = Instant::now();
    loop {
        let state = read_refusal_state(db);
        if state.streak >= streak && state.max_attempts == 0 {
            return state;
        }
        assert!(
            start.elapsed() <= deadline,
            "a refused batch must be released without spending an attempt \
             ({streak} releases expected); last state {state:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn row_count(db: &Connection) -> i64 {
    db.query_row("SELECT COUNT(*) FROM __queue_messages", [], |row| {
        row.get(0)
    })
    .expect("count rows")
}

fn deliveries(node: &TestNode, queue: &str) -> Vec<Value> {
    let response = get_json(node, "/deliveries");
    response["result"]["deliveries"]
        .as_array()
        .expect("deliveries")
        .iter()
        .filter(|delivery| delivery["queue"] == queue)
        .cloned()
        .collect()
}

fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("cello-queue-refusal-")
        .tempdir()
        .expect("tempdir")
}

#[test]
fn a_refused_batch_is_delivered_after_the_isolate_frees_memory() {
    let dir = tempdir();
    let app_project = dir.path().join("app");
    copy_dir(&fixture_dir().join("app"), &app_project);
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let node = TestNode::boot(&app_project, &data_dir);

    // Fill 60 MiB of the isolate's 64 MiB budget, one step per request, and
    // keep the buffers in module scope so the queue dispatch meets them.
    const { assert!(FILL_MIB < EXTERNAL_LIMIT_MB) };
    for step in 0..FILL_MIB {
        let (status, body) = get(&node.public, "/fill?mib=1")
            .unwrap_or_else(|error| panic!("fill step {step}: {error}"));
        assert_eq!(status, 200, "fill step {step}: {body}");
    }

    let sent = get_json(
        &node,
        &format!("/send?queue=refuse-src&n={BATCH_MESSAGES}&size={MESSAGE_BYTES}"),
    );
    assert_eq!(sent["ok"], true, "{sent}");

    // The cell releases a refused batch: the rows become visible again in the
    // future and no attempt is spent. Before the fix this state never exists,
    // because the lease stays installed until it expires and the expiry
    // charges the attempt.
    let source = cell_db(&data_dir, "refuse-src");
    let db = open_db(&source);
    let first = wait_for_streak(&db, 1, Duration::from_secs(90));
    assert!(
        first.visible_at - first.enqueued_at >= REFUSAL_BACKOFF_MS,
        "a released batch must wait before it is visible again: {first:?}"
    );

    // Consecutive refusals are spaced by the backoff rather than redelivered
    // in a loop: the alarm can only fire at the first visible deadline, so the
    // second release lands at least one base delay after the first.
    let second = wait_for_streak(&db, 2, Duration::from_secs(90));
    assert!(
        second.visible_at - first.visible_at >= REFUSAL_BACKOFF_MS,
        "a refused batch must back off before it is redelivered: {first:?} then {second:?}"
    );

    // Free the isolate. The next dispatch collects the dropped buffers at its
    // turn boundary, so the batch fits.
    assert!(
        matches!(get(&node.public, "/free"), Ok((204, _))),
        "the fixture must answer /free with an empty 204"
    );

    let delivered = poll_until("delivery", Duration::from_secs(120), || {
        let delivered = deliveries(&node, "refuse-src");
        (delivered.len() >= BATCH_MESSAGES).then(|| format!("{} delivered", delivered.len()))
    });
    assert!(delivered.contains("delivered"), "{delivered}");

    // Every message reached the consumer exactly once, on its first real
    // attempt: the refusals were not charged.
    let delivered = deliveries(&node, "refuse-src");
    assert_eq!(delivered.len(), BATCH_MESSAGES, "{delivered:?}");
    for delivery in &delivered {
        assert_eq!(delivery["attempts"], 1, "{delivery}");
        assert_eq!(delivery["size"], MESSAGE_BYTES, "{delivery}");
    }

    // A consumer ran, so the streak is cleared: the next refusal is free
    // again rather than resuming the sustained ladder. Settlement is its own
    // host-to-cell trip, so poll rather than read once.
    poll_until("streak reset", Duration::from_secs(30), || {
        (!refusal_streak_present(&db)).then(|| "reset".to_string())
    });

    // Nothing was dead-lettered and the source drained.
    let current = get_json(&node, "/metrics");
    assert_eq!(current["result"]["refuse"]["backlog"], 0, "{current}");
    assert_eq!(current["result"]["refuseDlq"]["backlog"], 0, "{current}");
    let dead_letter = cell_db(&data_dir, "refuse-dst");
    if dead_letter.exists() {
        assert_eq!(
            row_count(&open_db(&dead_letter)),
            0,
            "a refusal was dead-lettered"
        );
    }
}

fn refusal_streak_present(db: &Connection) -> bool {
    db.query_row(
        "SELECT COUNT(*) FROM __queue_meta WHERE name LIKE 'refusal%'",
        [],
        |row| row.get::<_, i64>(0),
    )
    .expect("count refusal meta rows")
        != 0
}

#[test]
fn a_consumer_exception_still_spends_an_attempt() {
    let dir = tempdir();
    let app_project = dir.path().join("app");
    copy_dir(&fixture_dir().join("app"), &app_project);
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let node = TestNode::boot(&app_project, &data_dir);

    let sent = get_json(&node, "/send?queue=retry-src&n=1&size=32");
    assert_eq!(sent["ok"], true, "{sent}");

    // `retry-src` throws on every delivery with `max_retries: 1`, so the
    // message is delivered twice -- attempts one and two -- and then
    // dead-lettered. A thrown handler is a failed delivery, not a refusal.
    let delivered = poll_until("two deliveries", Duration::from_secs(120), || {
        let delivered = deliveries(&node, "retry-src");
        (delivered.len() >= 2).then(|| format!("{} delivered", delivered.len()))
    });
    assert!(delivered.contains("delivered"), "{delivered}");

    let mut attempts: Vec<u64> = deliveries(&node, "retry-src")
        .iter()
        .map(|delivery| delivery["attempts"].as_u64().expect("attempts"))
        .collect();
    attempts.sort_unstable();
    assert_eq!(attempts, vec![1, 2]);

    poll_until("dead-letter", Duration::from_secs(120), || {
        let current = get_json(&node, "/metrics");
        (current["result"]["retryDlq"]["backlog"] == 1).then(|| current.to_string())
    });
}
