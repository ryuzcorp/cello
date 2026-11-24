// Modified by Ryuz Corp (Cello): cross-node handler-failure shape (#226).
// Apache-2.0.

// A black-box runner, not node code: it boots two real nodes, copies a
// fixture, and speaks HTTP over TCP with wall-clock deadlines.
#![allow(clippy::disallowed_methods)]

//! A Durable Object handler failure keeps its shape when the owner is on
//! another node (#226).
//!
//! The owner answers a forwarded fetch with `x-cells-handler-failure: 2` and a
//! versioned JSON body carrying the kind, name, message and storage-clone
//! bytes of a thrown object. The caller rebuilds the same `CellHandlerError`
//! the local path produces, so a stub caller sees the class, workerd's
//! `remote` marker and a first-line stack — not a host `Error` around the
//! owner's text.
//!
//! Two nodes share one development store, so they find each other through it
//! the way a fleet does. The top-level Worker runs on the node the client
//! dialled; the `Thrower` Durable Object is placed by ownership, so dialling
//! the node that does not own it makes every `stub.fetch()` in the route a
//! cross-node call. The route reports both node names, and the test picks an
//! instance name whose owner is the other node before it asserts anything.
//!
//! The oversize label is the one deliberate deviation: a handler failure
//! whose clone cannot fit the bounded peer body degrades to the primitive
//! shape (an `Error` with the message), which is what a clone the serializer
//! refuses already does locally. Everything else must match workerd, and the
//! same answers are asserted on both nodes so a local and a remote caller
//! agree.

mod common;

use common::{announced, cello_command, forward_announce};
use serde_json::Value;
use std::ffi::OsStr;
use std::io::{BufRead, Read, Write};
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

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    for file in ["wrangler.json", "worker.js"] {
        std::fs::copy(src.join(file), dst.join(file)).expect("copy fixture");
    }
}

/// A tempdir for the shared store and both nodes' state. Tmpfs keeps the
/// two nodes' SQLite commits off the box's disk queue: a stalled commit
/// self-fences a healthy node (the reason every two-node fixture uses it).
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

/// Deploy the fixture into the store both nodes will open, the way
/// `cello dev` deploys before it starts its node. One deploy makes both nodes
/// serve the same script; neither node runs a supervisor here.
fn deploy_fixture(project: &Path) {
    let state = project.join(".cello/dev");
    std::fs::create_dir_all(&state).expect("state dir");
    let database = state.join("objects.sqlite3");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    cello::asyncrt::set_host_handle(rt.handle().clone());
    rt.block_on(async {
        let bucket = cello::dev::open_local_bucket(&database).expect("open dev bucket");
        cello::fleet::validate_bucket(&bucket)
            .await
            .expect("validate bucket");
        bucket.probe_cas().await.expect("probe cas");
        let built = cello::deploy::build(&cello::deploy::Options {
            config: Some(project.to_path_buf()),
            bucket: None,
            endpoint: None,
            region: None,
            dry_run: false,
            json: false,
            vars: cello::deploy::VarOverrides::None,
            local_images: true,
            sourcemap: None,
            no_activate: false,
            read_roots: Vec::new(),
        })
        .expect("build fixture");
        cello::deploy::write(&bucket, &built)
            .await
            .expect("deploy fixture");
        cello::wake_format::ensure_ready(&bucket)
            .await
            .expect("ensure ready");
    });
    // The nodes load and serve in their own processes; this runtime only
    // carried the deploy.
}

/// One serving node on the shared store, killed on drop so a failure leaks
/// no port or store writer.
struct TestNode {
    child: Child,
    public: String,
    name: String,
}

impl TestNode {
    fn boot(project: &Path, store: &Path, name: &str) -> Self {
        let mut child = cello_command(
            project,
            [
                ("CELLO_INTERNAL_DEV_STORE", store.as_os_str()),
                ("CELLO_NODE", OsStr::new(name)),
                ("NO_COLOR", OsStr::new("1")),
            ],
        )
        .arg("--no-control-plane")
        .arg("--bucket")
        .arg("cello-dev")
        .arg("--listen")
        .arg("127.0.0.1:0")
        .arg("--internal-listen")
        .arg("127.0.0.1:0")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cello");
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let announced_lines: Arc<Mutex<Vec<String>>> = Arc::default();
        forward_announce(stderr, Arc::clone(&announced_lines));
        {
            // A serving node writes its announce lines to stdout, the data
            // stream; `forward_announce` covers stderr for the supervisor.
            let lines = Arc::clone(&announced_lines);
            std::thread::spawn(move || {
                let mut reader = std::io::BufReader::new(stdout);
                let mut line = String::new();
                while reader.read_line(&mut line).is_ok_and(|read| read > 0) {
                    lines
                        .lock()
                        .expect("announce lock")
                        .push(std::mem::take(&mut line));
                }
            });
        }
        let mut node = Self {
            child,
            public: String::new(),
            name: name.to_string(),
        };
        node.public = poll_until("boot", Duration::from_secs(180), || {
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!("cello exited during boot: {status}");
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
        .set_read_timeout(Some(Duration::from_secs(120)))
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
fn route(addr: &str, path: &str) -> Result<Value, String> {
    let (status, body) = get(addr, path)?;
    if status != 200 {
        return Err(format!("GET {path} answered {status}: {body}"));
    }
    serde_json::from_str(&body).map_err(|error| format!("GET {path} is not JSON: {error}: {body}"))
}

fn text(value: &Value, path: &[&str]) -> String {
    let mut cursor = value;
    for key in path {
        cursor = cursor.get(*key).unwrap_or(&Value::Null);
    }
    match cursor {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// workerd's shape for every value the handler throws, as
/// `do_ws_conformance` asserts it on one node. The cross-node caller must
/// produce the same answer; `oversize` is asserted separately because the
/// bounded peer body degrades it.
const THROWN: &[(&str, &str, &str, &str, &str)] = &[
    // label, ctor, name, message, first stack line
    (
        "error",
        "Error",
        "Error",
        "handler exploded",
        "Error: handler exploded",
    ),
    (
        "type",
        "TypeError",
        "TypeError",
        "bad type",
        "TypeError: bad type",
    ),
    (
        "string",
        "Error",
        "Error",
        "plain string",
        "Error: plain string",
    ),
    (
        "dom",
        "DOMException",
        "AbortError",
        "aborted!",
        "AbortError: aborted!",
    ),
    ("num", "Error", "Error", "42", "Error: 42"),
    ("null", "Error", "Error", "null", "Error: null"),
    ("empty", "TypeError", "TypeError", "", "TypeError"),
];

fn expect(failures: &mut Vec<String>, label: &str, actual: String, want: &str) {
    if actual != want {
        failures.push(format!("{label}: got {actual:?}, want {want:?}"));
    }
}

/// Assert the shapes every thrown value must keep, plus `remote: true`.
fn assert_shapes(failures: &mut Vec<String>, label_prefix: &str, answer: &Value) {
    for (label, ctor, name, message, stack) in THROWN {
        expect(
            failures,
            &format!("{label_prefix}.{label}.ctor"),
            text(answer, &[label, "ctor"]),
            ctor,
        );
        expect(
            failures,
            &format!("{label_prefix}.{label}.name"),
            text(answer, &[label, "name"]),
            name,
        );
        expect(
            failures,
            &format!("{label_prefix}.{label}.message"),
            text(answer, &[label, "message"]),
            message,
        );
        expect(
            failures,
            &format!("{label_prefix}.{label}.remote"),
            text(answer, &[label, "remote"]),
            "true",
        );
        expect(
            failures,
            &format!("{label_prefix}.{label}.stackFirst"),
            text(answer, &[label, "stack"])
                .split('\n')
                .next()
                .unwrap_or_default()
                .to_string(),
            stack,
        );
        expect(
            failures,
            &format!("{label_prefix}.{label}.stackLines"),
            text(answer, &[label, "stackLines"]),
            "1",
        );
    }
    expect(
        failures,
        &format!("{label_prefix}.dom.isDom"),
        text(answer, &["dom", "isDom"]),
        "true",
    );
    expect(
        failures,
        &format!("{label_prefix}.obj.ctor"),
        text(answer, &["obj", "ctor"]),
        "Object",
    );
    expect(
        failures,
        &format!("{label_prefix}.obj.isError"),
        text(answer, &["obj", "isError"]),
        "false",
    );
    expect(
        failures,
        &format!("{label_prefix}.obj.name"),
        text(answer, &["obj", "name"]),
        "ObjError",
    );
    expect(
        failures,
        &format!("{label_prefix}.obj.message"),
        text(answer, &["obj", "message"]),
        "obj msg",
    );
    expect(
        failures,
        &format!("{label_prefix}.obj.remote"),
        text(answer, &["obj", "remote"]),
        "true",
    );
    expect(
        failures,
        &format!("{label_prefix}.obj.stack"),
        text(answer, &["obj", "stack"]),
        "null",
    );
}

/// What an RPC method must reject with, as workerd's RPC surface does:
/// `(method, ctor, name, message)`. The stack is the first line plus the
/// caller's own frames in both directions, so only the first line is fixed
/// here and the local/remote answers are compared whole.
const RPC_THROWN: &[(&str, &str, &str, &str)] = &[
    ("dom", "DOMException", "AbortError", "aborted!"),
    ("range", "RangeError", "RangeError", "bad range"),
    ("str", "Error", "Error", "plain string"),
    ("empty", "TypeError", "TypeError", ""),
];

fn assert_rpc_shapes(failures: &mut Vec<String>, label_prefix: &str, answer: &Value) {
    for (label, ctor, name, message) in RPC_THROWN {
        expect(
            failures,
            &format!("{label_prefix}.{label}.ctor"),
            text(answer, &[label, "ctor"]),
            ctor,
        );
        expect(
            failures,
            &format!("{label_prefix}.{label}.name"),
            text(answer, &[label, "name"]),
            name,
        );
        expect(
            failures,
            &format!("{label_prefix}.{label}.message"),
            text(answer, &[label, "message"]),
            message,
        );
        expect(
            failures,
            &format!("{label_prefix}.{label}.remote"),
            text(answer, &[label, "remote"]),
            "true",
        );
        let stack = text(answer, &[label, "stack"]);
        expect(
            failures,
            &format!("{label_prefix}.{label}.stackFirst"),
            stack.split('\n').next().unwrap_or_default().to_string(),
            &if message.is_empty() {
                (*name).to_string()
            } else {
                format!("{name}: {message}")
            },
        );
    }
    expect(
        failures,
        &format!("{label_prefix}.dom.isDom"),
        text(answer, &["dom", "isDom"]),
        "true",
    );
    expect(
        failures,
        &format!("{label_prefix}.obj.ctor"),
        text(answer, &["obj", "ctor"]),
        "Object",
    );
    expect(
        failures,
        &format!("{label_prefix}.obj.isError"),
        text(answer, &["obj", "isError"]),
        "false",
    );
    expect(
        failures,
        &format!("{label_prefix}.obj.name"),
        text(answer, &["obj", "name"]),
        "ObjError",
    );
    expect(
        failures,
        &format!("{label_prefix}.obj.message"),
        text(answer, &["obj", "message"]),
        "obj msg",
    );
    expect(
        failures,
        &format!("{label_prefix}.obj.remote"),
        text(answer, &["obj", "remote"]),
        "true",
    );
    expect(
        failures,
        &format!("{label_prefix}.obj.stack"),
        text(answer, &["obj", "stack"]),
        "null",
    );
    // The reply envelope is not the 64 KiB handler-failure body, so this 70 KB
    // clone still arrives as the object.
    expect(
        failures,
        &format!("{label_prefix}.oversize.ctor"),
        text(answer, &["oversize", "ctor"]),
        "Object",
    );
    expect(
        failures,
        &format!("{label_prefix}.oversize.name"),
        text(answer, &["oversize", "name"]),
        "BigError",
    );
    expect(
        failures,
        &format!("{label_prefix}.oversize.message"),
        text(answer, &["oversize", "message"]),
        "too big",
    );
    expect(
        failures,
        &format!("{label_prefix}.oversize.remote"),
        text(answer, &["oversize", "remote"]),
        "true",
    );
    expect(
        failures,
        &format!("{label_prefix}.oversize.isError"),
        text(answer, &["oversize", "isError"]),
        "false",
    );
    // Not just the metadata: the whole 70,000-character clone crossed, so a
    // dropped or truncated payload fails here.
    expect(
        failures,
        &format!("{label_prefix}.oversize.payloadLength"),
        text(answer, &["oversize", "payloadLength"]),
        "70000",
    );
    expect(
        failures,
        &format!("{label_prefix}.oversize.payloadAllX"),
        text(answer, &["oversize", "payloadAllX"]),
        "true",
    );
}

/// The `(local, cross-node)` answers of one test route, from an instance
/// whose owner is the other node than the one the top-level Worker runs on.
///
/// Activation claims ownership, so the node that serves a new instance owns
/// it from then on. Create the instance on one node and dial the other: the
/// caller's top-level Worker then makes every stub call in the route cross
/// nodes, while the creator's own answer stays local. Both directions are
/// tried because either node can refuse the claim under load. `endpoint`
/// must accept `?name=` and report `caller`/`throwerNode`.
fn across_nodes(a: &TestNode, b: &TestNode, endpoint: &str) -> Result<(Value, Value), Vec<String>> {
    let mut see = Vec::new();
    for (creator, caller) in [(a, b), (b, a)] {
        for index in 0..8 {
            let path = format!("{endpoint}?name=probe-{index}");
            let local = match route(&creator.public, &path) {
                Ok(answer) => answer,
                Err(error) => {
                    see.push(error);
                    continue;
                }
            };
            if text(&local, &["throwerNode"]) != creator.name
                || text(&local, &["caller"]) != creator.name
            {
                see.push(format!(
                    "{path} on {}: caller={} owner={}",
                    creator.name,
                    text(&local, &["caller"]),
                    text(&local, &["throwerNode"])
                ));
                continue;
            }
            let cross_node = match route(&caller.public, &path) {
                Ok(answer) => answer,
                Err(error) => {
                    see.push(error);
                    continue;
                }
            };
            if text(&cross_node, &["caller"]) != caller.name
                || text(&cross_node, &["throwerNode"]) != creator.name
            {
                see.push(format!(
                    "{path} on {}: caller={} owner={}",
                    caller.name,
                    text(&cross_node, &["caller"]),
                    text(&cross_node, &["throwerNode"])
                ));
                continue;
            }
            return Ok((local, cross_node));
        }
    }
    Err(see)
}

#[test]
fn a_handler_failure_keeps_its_shape_across_nodes() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("cello-do-remote-");
    copy_dir(&fixture, project.path());
    deploy_fixture(project.path());
    let store = project.path().join(".cello/dev/objects.sqlite3");
    let a = TestNode::boot(project.path(), &store, "remote-test-a");
    let b = TestNode::boot(project.path(), &store, "remote-test-b");

    let (local, cross_node) = match across_nodes(&a, &b, "/throw") {
        Ok(pair) => pair,
        Err(see) => panic!("no instance crossed nodes; saw {see:?}"),
    };

    let mut failures = Vec::new();
    assert_shapes(&mut failures, "remote", &cross_node);
    assert_shapes(&mut failures, "local", &local);

    // The oversize label: 70 KB of clone bytes cannot cross the bounded peer
    // body, so the remote caller gets the documented degradation — an `Error`
    // carrying the message — while the local caller keeps the object. Both
    // still reject with a handler failure, never the owner's 500 response.
    expect(
        &mut failures,
        "remote.oversize.ctor",
        text(&cross_node, &["oversize", "ctor"]),
        "Error",
    );
    expect(
        &mut failures,
        "remote.oversize.name",
        text(&cross_node, &["oversize", "name"]),
        "Error",
    );
    expect(
        &mut failures,
        "remote.oversize.message",
        text(&cross_node, &["oversize", "message"]),
        "too big",
    );
    expect(
        &mut failures,
        "remote.oversize.remote",
        text(&cross_node, &["oversize", "remote"]),
        "true",
    );
    expect(
        &mut failures,
        "local.oversize.ctor",
        text(&local, &["oversize", "ctor"]),
        "Object",
    );
    expect(
        &mut failures,
        "local.oversize.name",
        text(&local, &["oversize", "name"]),
        "BigError",
    );
    expect(
        &mut failures,
        "local.oversize.message",
        text(&local, &["oversize", "message"]),
        "too big",
    );
    // The local caller keeps the whole 70 KB clone; the degraded remote answer
    // has no payload at all, which is the bound doing its job rather than a
    // truncation that silently lost data.
    expect(
        &mut failures,
        "local.oversize.payloadLength",
        text(&local, &["oversize", "payloadLength"]),
        "70000",
    );
    expect(
        &mut failures,
        "local.oversize.payloadAllX",
        text(&local, &["oversize", "payloadAllX"]),
        "true",
    );
    expect(
        &mut failures,
        "remote.oversize.payloadLength",
        text(&cross_node, &["oversize", "payloadLength"]),
        "null",
    );

    // RPC (`stub.method()`) is a different peer reply: the callee's exception
    // rides the structured-clone reply envelope as a 200 body, not the bounded
    // `x-cells-handler-failure` body, so the two-node hop must change nothing —
    // including a clone larger than that body bound (#238).
    let (rpc_local, rpc_cross) = match across_nodes(&a, &b, "/rpc-throw") {
        Ok(pair) => pair,
        Err(see) => panic!("no RPC instance crossed nodes; saw {see:?}"),
    };
    assert_rpc_shapes(&mut failures, "rpc.remote", &rpc_cross);
    assert_rpc_shapes(&mut failures, "rpc.local", &rpc_local);
    for label in ["dom", "range", "str", "obj", "empty", "oversize"] {
        for field in [
            "ctor",
            "name",
            "message",
            "isDom",
            "isError",
            "remote",
            "stack",
            "payloadLength",
            "payloadAllX",
        ] {
            let from_local = text(&rpc_local, &[label, field]);
            let from_cross = text(&rpc_cross, &[label, field]);
            if from_local != from_cross {
                failures.push(format!(
                    "rpc.{label}.{field}: local {from_local:?} != cross-node {from_cross:?}"
                ));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "shape mismatches:\n{}",
        failures.join("\n")
    );
}
