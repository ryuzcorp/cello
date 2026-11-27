// Modified by Ryuz Corp (Cello): non-ASCII ingress header regression (issue #295). Apache-2.0.

// A black-box runner, not node code: it boots the real `cello dev` binary
// (and, for the remote leg, two real serving nodes on a shared store) and
// speaks raw HTTP/1.1 over TCP, so the execution boundary's injected
// clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for header bytes across the whole boundary (issue #295,
//! upstream denoland/celld#327): inbound HTTP bytes decode as UTF-8 with
//! replacement, as workerd decodes a received value, and the way back out
//! re-encodes the Worker's string as UTF-8, so valid values round-trip
//! byte-for-byte.
//!
//! Before the fix `ingress_payload_parts` collected headers with
//! `value.to_str().ok()` in a `filter_map`, so any value that was not valid
//! UTF-8 dropped the whole header. For `Cookie` that dropped every cookie,
//! and a sibling subdomain could sign users out with no way for the app to
//! detect or repair it. The same lossy shape sat on the other inbound
//! conversions (`serve_do`, the tunneled reply). Every bytes-to-string
//! conversion now goes through `cello::header_bytes`: a valid non-ASCII
//! `Cookie` like `x=caf` + U+00E9 stays visible, and an undecodable byte
//! becomes U+FFFD instead of dropping the header.
//!
//! Four legs pin the boundary: a local request stays visible (`/echo`,
//! including a lone invalid byte that must read back U+FFFD), a reflected
//! response carries the exact bytes back on the raw wire (`/reflect`), a
//! subrequest delivers the exact bytes to the origin (`/proxy`, observed by
//! a test-owned helper origin), and a two-node remote-cell dispatch proves
//! neither the tunnel forward nor the reply drops them (`/remote-echo`).
//!
//! One `cello dev` boots the fixture per single-node test; the remote test
//! deploys the fixture to a shared store and boots two serving nodes. No
//! fixed sleeps: raw requests go out once the node answers `/health` (or
//! announces), and the server guards kill the children on every exit path.

mod common;

use common::{announced, cello_command, forward_announce, NO_EXTRA_ENV};
use std::ffi::OsStr;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/ingress-header-bytes")
        .canonicalize()
        .expect("fixture exists")
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
    fn boot(project: &Path) -> Self {
        let mut child = cello_command(project, NO_EXTRA_ENV)
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
    let wire =
        format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
    http_raw(addr, wire.as_bytes(), timeout)
}

fn http_raw(addr: &str, wire: &[u8], timeout: Duration) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect(addr).map_err(|error| format!("connect: {error}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|error| format!("timeout: {error}"))?;
    stream
        .write_all(wire)
        .map_err(|error| format!("write: {error}"))?;
    read_response(&mut stream)
}

fn read_response(stream: &mut TcpStream) -> Result<(u16, String), String> {
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

/// The raw request on the wire: the `Cookie` value carries the two
/// non-ASCII bytes 0xC3 0xA9 (`x=caf` + U+00E9 in UTF-8); `X-Broken` carries
/// a lone 0xE9 byte, which is not valid UTF-8 and must decode to U+FFFD;
/// `X-Split` ends at its CRLF, so the smuggled line parses as its own
/// header and must never appear inside a value the Worker sees.
fn request_wire(path: &str) -> Vec<u8> {
    let mut wire = format!("GET {path} HTTP/1.1\r\n").into_bytes();
    wire.extend_from_slice(b"Host: x\r\nConnection: close\r\n");
    wire.extend_from_slice(b"X-Probe: hello\r\nX-Split: a\r\nX-Smuggled: yes\r\n");
    wire.extend_from_slice(b"X-Broken: a");
    wire.extend_from_slice(&[0xE9]);
    wire.extend_from_slice(b"b\r\nCookie: x=caf");
    wire.extend_from_slice(&[0xC3, 0xA9]);
    wire.extend_from_slice(b"\r\nContent-Length: 0\r\n\r\n");
    wire
}

/// A raw response as status, head bytes and body bytes. The head can carry
/// non-UTF-8 header bytes, so it stays bytes until the assertions pick
/// their field.
fn read_raw_response(stream: &mut TcpStream) -> Result<(u16, Vec<u8>, Vec<u8>), String> {
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .map_err(|error| format!("read: {error}"))?;
    let split = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| format!("no header/body split in {} bytes", raw.len()))?;
    let head = raw[..split].to_vec();
    let body = raw[split + 4..].to_vec();
    let status_line = head
        .split(|byte| *byte == b'\n')
        .next()
        .ok_or_else(|| "empty response head".to_string())?;
    let status_line =
        std::str::from_utf8(status_line).map_err(|error| format!("status utf8: {error}"))?;
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| format!("no status in {status_line:?}"))?;
    Ok((status, head, body))
}

fn http_raw_bytes(
    addr: &str,
    wire: &[u8],
    timeout: Duration,
) -> Result<(u16, Vec<u8>, Vec<u8>), String> {
    let mut stream = TcpStream::connect(addr).map_err(|error| format!("connect: {error}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|error| format!("timeout: {error}"))?;
    stream
        .write_all(wire)
        .map_err(|error| format!("write: {error}"))?;
    read_raw_response(&mut stream)
}

/// A helper origin that reports the exact bytes of the `Cookie` header it
/// receives, as comma-separated lowercase hex, so the `/proxy` leg proves
/// the subrequest carried them.
fn boot_origin() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind origin");
    let port = listener.local_addr().expect("origin addr").port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else {
                continue;
            };
            std::thread::spawn(move || {
                let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
                let head = read_head(&mut stream);
                let hex = cookie_hex(&head);
                let body = hex.as_bytes();
                let _ = stream.write_all(b"HTTP/1.1 200 OK\r\n");
                let _ = stream.write_all(format!("Content-Length: {}\r\n", body.len()).as_bytes());
                let _ = stream.write_all(b"Connection: close\r\nContent-Type: text/plain\r\n\r\n");
                let _ = stream.write_all(body);
            });
        }
    });
    port
}

fn read_head(stream: &mut TcpStream) -> Vec<u8> {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        if stream.read_exact(&mut byte).is_err() {
            break;
        }
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") || head.len() > 65536 {
            break;
        }
    }
    head
}

fn cookie_hex(head: &[u8]) -> String {
    for line in head.split(|byte| *byte == b'\n') {
        let line = match line {
            [rest @ .., b'\r'] => rest,
            line => line,
        };
        if line.len() > 7 && line[..7].eq_ignore_ascii_case(b"cookie:") {
            let mut value = &line[7..];
            while value.starts_with(b" ") || value.starts_with(b"\t") {
                value = &value[1..];
            }
            while value.ends_with(b" ") || value.ends_with(b"\t") {
                value = &value[..value.len() - 1];
            }
            return value
                .iter()
                .map(|byte| format!("{byte:x}"))
                .collect::<Vec<_>>()
                .join(",");
        }
    }
    "MISSING".to_string()
}

fn boot_single(prefix: &str) -> (tempfile::TempDir, TestNode) {
    let origin_port = boot_origin();
    let dir = tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .expect("tempdir");
    let project = dir.path().join("app");
    copy_dir(&fixture_dir(), &project, origin_port);
    let node = TestNode::boot(&project);
    (dir, node)
}

#[test]
fn non_ascii_cookie_reaches_worker_byte_for_byte() {
    let (_dir, node) = boot_single("cello-ingress-header-bytes-");

    let (status, body) = http_raw(
        &node.public,
        &request_wire("/echo"),
        Duration::from_secs(15),
    )
    .expect("echo answers");
    assert_eq!(status, 200, "{body}");
    // Pre-fix the cookie read back `MISSING`: `to_str` refused the 0xC3
    // 0xA9 bytes and the whole header was skipped. Now the valid UTF-8
    // decodes to the single code point U+00E9 (`x=caf` + `e9`).
    assert!(
        body.contains("\"cookie\":\"78,3d,63,61,66,e9\""),
        "non-ASCII cookie visible to the Worker: {body}"
    );
    // An ASCII header crosses exactly as sent.
    assert!(
        body.contains("\"probe\":\"68,65,6c,6c,6f\""),
        "ASCII header unchanged: {body}"
    );
    // A lone 0xE9 byte is not valid UTF-8: like workerd, the Worker sees
    // U+FFFD instead of losing the header.
    assert!(
        body.contains("\"broken\":\"61,fffd,62\""),
        "invalid byte becomes U+FFFD: {body}"
    );
    // A CRLF ends the field: the smuggled line is its own header, and the
    // split value carries no CR or LF byte into the Worker.
    assert!(
        body.contains("\"split\":\"61\""),
        "no CR/LF smuggled into a value: {body}"
    );
}

#[test]
fn reflected_cookie_returns_exact_wire_bytes() {
    let (_dir, node) = boot_single("cello-ingress-header-bytes-reflect-");

    let (status, head, _body) = http_raw_bytes(
        &node.public,
        &request_wire("/reflect"),
        Duration::from_secs(15),
    )
    .expect("reflect answers");
    let head_lossy = String::from_utf8_lossy(&head);
    assert_eq!(status, 200, "{head_lossy}");
    // The response must carry the exact bytes. Pre-fix the header read back
    // `MISSING` at the ingress; now the Worker's U+00E9 re-encodes to the
    // exact C3 A9 pair on the wire.
    let exact = b"x-echo-cookie: x=caf\xc3\xa9";
    assert!(
        head.windows(exact.len()).any(|window| window == exact),
        "cookie bytes exact on the wire: {head_lossy}"
    );
    let doubled = b"\xc3\x83\xc2\xa9";
    assert!(
        !head.windows(doubled.len()).any(|window| window == doubled),
        "no UTF-8 doubling on the wire: {head_lossy}"
    );
}

#[test]
fn subrequest_carries_exact_cookie_bytes() {
    let (_dir, node) = boot_single("cello-ingress-header-bytes-proxy-");

    let (status, body) = http_raw(
        &node.public,
        &request_wire("/proxy"),
        Duration::from_secs(30),
    )
    .expect("proxy answers");
    assert_eq!(status, 200, "{body}");
    // The origin hex-reports the bytes it received. Pre-fix the cookie never
    // arrived (dropped at the ingress, so the fixture forwarded `MISSING`);
    // now the exact C3 A9 pair lands.
    assert_eq!(
        body, "78,3d,63,61,66,c3,a9",
        "origin saw the exact cookie bytes: {body:?}"
    );
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
/// `cello dev` deploys before it starts its node. One deploy makes both
/// nodes serve the same script; neither node runs a supervisor here.
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
struct RemoteNode {
    child: Child,
    public: String,
    name: String,
}

impl RemoteNode {
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
                while std::io::BufRead::read_line(&mut reader, &mut line).is_ok_and(|read| read > 0)
                {
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

impl Drop for RemoteNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
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

#[test]
fn remote_cell_dispatch_preserves_header_bytes() {
    let project_guard = fixture_tempdir("cello-ingress-header-bytes-remote-");
    let project = project_guard.path();
    copy_dir(&fixture_dir(), project, 0);
    deploy_fixture(project);
    let store = project.join(".cello/dev/objects.sqlite3");
    let a = RemoteNode::boot(project, &store, "hdr-a");
    let b = RemoteNode::boot(project, &store, "hdr-b");

    // Activation claims ownership, so the node that first serves a new
    // instance owns it from then on: touching it on the caller first made
    // the caller the owner every time and the route never crossed. Prime
    // each name on the creator, verify the creator owns it, then send the
    // byte-bearing request for the same name to the caller — that call
    // deterministically crosses. Both directions are tried because either
    // node can be slow to claim under load.
    let mut seen = Vec::new();
    let mut crossed: Option<(Vec<u8>, Vec<u8>)> = None;
    'outer: for (creator, caller) in [(&a, &b), (&b, &a)] {
        for index in 0..4u32 {
            let path = format!("/remote-echo?name={}-{index}", creator.name);
            let prime = match http_get(&creator.public, &path, Duration::from_secs(60)) {
                Ok(answer) => answer,
                Err(error) => {
                    seen.push(format!("prime {path} on {}: {error}", creator.name));
                    continue;
                }
            };
            if prime.0 != 200
                || !prime
                    .1
                    .contains(format!("\"owner\":\"{}\"", creator.name).as_str())
            {
                seen.push(format!("prime {path} on {}: {prime:?}", creator.name));
                continue;
            }
            let wire = request_wire(&path);
            let (status, head, body) =
                match http_raw_bytes(&caller.public, &wire, Duration::from_secs(60)) {
                    Ok(answer) => answer,
                    Err(error) => {
                        seen.push(format!("{path} on {}: {error}", caller.name));
                        continue;
                    }
                };
            if status != 200 {
                seen.push(format!("{path} on {}: status {status}", caller.name));
                continue;
            }
            let Ok(body) = String::from_utf8(body) else {
                seen.push(format!("{path} on {}: non-UTF-8 body", caller.name));
                continue;
            };
            let caller_here = format!("\"caller\":\"{}\"", caller.name);
            let owner_there = format!("\"owner\":\"{}\"", creator.name);
            if !body.contains(caller_here.as_str()) || !body.contains(owner_there.as_str()) {
                seen.push(format!("{path} on {}: {body}", caller.name));
                continue;
            }
            crossed = Some((head, body.into_bytes()));
            break 'outer;
        }
    }
    let Some((head, body)) = crossed else {
        panic!("no instance crossed nodes; saw {seen:?}");
    };
    let body = String::from_utf8(body).expect("crossed body is ASCII hex JSON");
    // The owner saw the decoded value: the tunnel forward carried the UTF-8
    // bytes and `serve_do` decoded them to U+00E9. Pre-fix the whole header
    // vanished (dropped at the ingress) and the cookie read back `MISSING`.
    assert!(
        body.contains("\"cookie\":\"78,3d,63,61,66,e9\""),
        "owner saw the decoded cookie: {body}"
    );
    // The owner's reply header returned the same way: the raw wire off the
    // caller carries the exact C3 A9 pair through ordinary UTF-8 re-encoding.
    let head_lossy = String::from_utf8_lossy(&head);
    let exact = b"x-echo-cookie: x=caf\xc3\xa9";
    assert!(
        head.windows(exact.len()).any(|window| window == exact),
        "return leg exact on the wire: {head_lossy}"
    );
    let doubled = b"\xc3\x83\xc2\xa9";
    assert!(
        !head.windows(doubled.len()).any(|window| window == doubled),
        "no UTF-8 doubling on the return leg: {head_lossy}"
    );
}
