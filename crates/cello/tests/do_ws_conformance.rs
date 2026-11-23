// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by Ryuz Corp (Cello): Durable Object and WebSocket workerd
// divergences (#149, #150, #151, #154, #181, #183, #185, #191, #192).

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and speaks HTTP over TCP with wall-clock deadlines.
#![allow(clippy::disallowed_methods)]

//! Durable Object and WebSocket behaviour that diverged from workerd and is
//! fixed for 0.2.9: alarm validation, send-before-accept, stub error
//! propagation, storage put/DataCloneError, acceptWebSocket and
//! auto-response validation, queue settle enumerability, SQL boolean binds
//! and cursor internals, and the standard `binaryType` default.
//!
//! One fixture route answers per behaviour with the exact text workerd
//! 1.20261121.1 produces. Every mismatch is collected so a pre-fix run
//! reports all of them at once, not just the first.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/do-ws")
        .canonicalize()
        .expect("fixture exists")
}

mod common;
use common::{announced, cello_command, forward_announce};

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

/// A booted `cello dev`; killed on drop so a failure leaks no port.
struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(app_project: &Path) -> Self {
        let mut child = cello_command(app_project, [("NO_COLOR", "1")])
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
            announced(&announced_lines, "cello listening on ")
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

/// One route's JSON answer; a non-200 or a non-JSON body is a failure the
/// caller records rather than a panic, so one broken route reports itself.
fn route(addr: &str, path: &str) -> Result<serde_json::Value, String> {
    let (status, body) = get(addr, path)?;
    if status != 200 {
        return Err(format!("GET {path} answered {status}: {body}"));
    }
    serde_json::from_str(&body).map_err(|error| format!("GET {path} is not JSON: {error}: {body}"))
}

fn text(value: &serde_json::Value, path: &[&str]) -> String {
    let mut cursor = value;
    for key in path {
        cursor = match key.parse::<usize>() {
            Ok(index) if cursor.is_array() => cursor.get(index).unwrap_or(&serde_json::Value::Null),
            _ => cursor.get(*key).unwrap_or(&serde_json::Value::Null),
        };
    }
    match cursor {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[test]
fn durable_object_and_websocket_match_workerd() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("cello-do-ws-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    let mut failures: Vec<String> = Vec::new();
    fn expect(failures: &mut Vec<String>, label: &str, actual: String, want: &str) {
        if actual != want {
            failures.push(format!("{label}: got {actual:?}, want {want:?}"));
        }
    }

    macro_rules! answer {
        ($path:expr) => {
            match route(&node.public, $path) {
                Ok(value) => value,
                Err(error) => {
                    failures.push(error);
                    serde_json::Value::Null
                }
            }
        };
    }

    // #149: setAlarm validates its argument, the instant, and the handler.
    let alarms = answer!("/alarms");
    expect(
        &mut failures,
        "alarms.set.name",
        text(&alarms, &["set", "name"]),
        "TypeError",
    );
    expect(
        &mut failures,
        "alarms.set.message",
        text(&alarms, &["set", "message"]),
        "Your Durable Object class must have an alarm() handler in order to call setAlarm()",
    );
    expect(&mut failures,
        "alarms.setString.message",
        text(&alarms, &["setString", "message"]),
        "Failed to execute 'setAlarm' on 'DurableObjectStorage': parameter 1 is not of type 'date'.",
    );
    expect(
        &mut failures,
        "alarms.setZero.message",
        text(&alarms, &["setZero", "message"]),
        "setAlarm() cannot be called with an alarm time <= 0",
    );
    expect(
        &mut failures,
        "alarms.setNaN.message",
        text(&alarms, &["setNaN", "message"]),
        "The value cannot be converted because it is not a valid Date.",
    );
    expect(
        &mut failures,
        "alarms.after.ok",
        text(&alarms, &["after", "ok"]),
        "true",
    );

    // #181: single put refuses undefined; clone failures are DataCloneError.
    let storage = answer!("/storage");
    expect(
        &mut failures,
        "storage.undef.message",
        text(&storage, &["undef", "message"]),
        "put() called with undefined value.",
    );
    expect(
        &mut failures,
        "storage.undef.name",
        text(&storage, &["undef", "name"]),
        "TypeError",
    );
    expect(
        &mut failures,
        "storage.fn.name",
        text(&storage, &["fn", "name"]),
        "DataCloneError",
    );
    expect(
        &mut failures,
        "storage.fn.message",
        text(&storage, &["fn", "message"]),
        "() => 1 could not be cloned.",
    );
    expect(
        &mut failures,
        "storage.sym.name",
        text(&storage, &["sym", "name"]),
        "DataCloneError",
    );
    expect(
        &mut failures,
        "storage.sym.message",
        text(&storage, &["sym", "message"]),
        "Symbol(s) could not be cloned.",
    );
    expect(
        &mut failures,
        "storage.proxy.name",
        text(&storage, &["proxy", "name"]),
        "DataCloneError",
    );
    expect(
        &mut failures,
        "storage.date.ok",
        text(&storage, &["date", "ok"]),
        "true",
    );

    // #150, #183, #192: accept/send validation, pair shape, binaryType default.
    let api = answer!("/ws-api");
    expect(
        &mut failures,
        "ws.pairKeys",
        text(&api, &["pairKeys"]),
        r#"["0","1"]"#,
    );
    expect(
        &mut failures,
        "ws.binaryType",
        text(&api, &["binaryType"]),
        "blob",
    );
    expect(
        &mut failures,
        "ws.acceptTwice.message",
        text(&api, &["acceptTwice", "message"]),
        "Cannot call `acceptWebSocket()` if the WebSocket was already accepted via `accept()`",
    );
    expect(&mut failures,
        "ws.autoNull.message",
        text(&api, &["autoNull", "message"]),
        "Failed to execute 'setWebSocketAutoResponse' on 'DurableObjectState': parameter 1 is not of type 'WebSocketRequestResponsePair'.",
    );
    expect(
        &mut failures,
        "ws.autoUndefined.ok",
        text(&api, &["autoUndefined", "ok"]),
        "true",
    );
    expect(&mut failures,
        "ws.sendBeforeAccept.message",
        text(&api, &["sendBeforeAccept", "message"]),
        "You must call one of accept() or state.acceptWebSocket() on this WebSocket before sending messages.",
    );
    expect(&mut failures,
        "ws.sendBeforeAcceptClient.message",
        text(&api, &["sendBeforeAcceptClient", "message"]),
        "You must call one of accept() or state.acceptWebSocket() on this WebSocket before sending messages.",
    );

    // #192: the message type follows binaryType on both ends of a pair.
    let types = answer!("/message-types");
    expect(
        &mut failures,
        "messageTypes.default.tag",
        text(&types, &["default", "0", "tag"]),
        "[object Blob]",
    );
    expect(
        &mut failures,
        "messageTypes.default.size",
        text(&types, &["default", "0", "size"]),
        "2",
    );
    expect(
        &mut failures,
        "messageTypes.defaultBinaryType",
        text(&types, &["defaultBinaryType"]),
        "blob",
    );
    expect(
        &mut failures,
        "messageTypes.switched.tag",
        text(&types, &["switched", "0", "tag"]),
        "[object ArrayBuffer]",
    );
    expect(
        &mut failures,
        "messageTypes.switched.bytes",
        text(&types, &["switched", "0", "bytes"]),
        "[7,8]",
    );
    expect(
        &mut failures,
        "messageTypes.switchedBinaryType",
        text(&types, &["switchedBinaryType"]),
        "arraybuffer",
    );

    // #151: a stub rejection is the handler's own error, with no host frame.
    let thrown = answer!("/throw");
    for (label, name, message) in [
        ("error", "Error", "handler exploded"),
        ("type", "TypeError", "bad type"),
        ("string", "Error", "plain string"),
        ("dom", "AbortError", "boom"),
    ] {
        expect(
            &mut failures,
            &format!("throw.{label}.name"),
            text(&thrown, &[label, "name"]),
            name,
        );
        expect(
            &mut failures,
            &format!("throw.{label}.message"),
            text(&thrown, &[label, "message"]),
            message,
        );
        expect(
            &mut failures,
            &format!("throw.{label}.hostFrames"),
            text(&thrown, &[label, "hostFrames"]),
            "false",
        );
    }

    // #154 and #191: a boolean bind is text, and a cursor keeps no own fields.
    let sql = answer!("/sql");
    expect(
        &mut failures,
        "sql.boolType",
        text(&sql, &["boolType"]),
        "text",
    );
    expect(
        &mut failures,
        "sql.boolValue",
        text(&sql, &["boolValue"]),
        "true",
    );
    expect(
        &mut failures,
        "sql.cursorKeys",
        text(&sql, &["cursorKeys"]),
        "[]",
    );
    expect(
        &mut failures,
        "sql.cursorOwnNames",
        text(&sql, &["cursorOwnNames"]),
        "0",
    );
    expect(
        &mut failures,
        "sql.cursorColumns",
        text(&sql, &["cursorColumns"]),
        r#"["v"]"#,
    );
    expect(
        &mut failures,
        "sql.cursorRowsWritten",
        text(&sql, &["cursorRowsWritten"]),
        "number",
    );

    // #185: settle methods are prototype members, invisible to iteration.
    let queue = answer!("/queue-shape");
    expect(
        &mut failures,
        "queue.missing",
        text(&queue, &["missing"]),
        "null",
    );
    expect(
        &mut failures,
        "queue.batchKeys",
        text(&queue, &["batchKeys"]),
        "messages,metadata,queue",
    );
    expect(
        &mut failures,
        "queue.batchSpreadKeys",
        text(&queue, &["batchSpreadKeys"]),
        "messages,metadata,queue",
    );
    expect(
        &mut failures,
        "queue.messageKeys",
        text(&queue, &["messageKeys"]),
        "attempts,body,id,timestamp",
    );
    expect(
        &mut failures,
        "queue.messageJsonKeys",
        text(&queue, &["messageJsonKeys"]),
        "attempts,body,id,timestamp",
    );
    expect(
        &mut failures,
        "queue.ackAllType",
        text(&queue, &["ackAllType"]),
        "function",
    );
    expect(
        &mut failures,
        "queue.ackType",
        text(&queue, &["ackType"]),
        "function",
    );
    expect(
        &mut failures,
        "queue.body",
        text(&queue, &["body"]),
        "shape-probe",
    );

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
