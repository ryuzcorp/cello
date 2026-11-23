// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by Ryuz Corp (Cello): cross-isolate revived-Request bodyUsed.
// Apache-2.0.

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for Request body presence and bytes:
//!
//! - a Request that crossed an isolate boundary keeps body presence in the
//!   `__cello$req` marker, so a revived bodyless GET stays bodyless and a
//!   revived POST keeps its body (#127), and a `fetch()` on either consumes
//!   exactly what workerd consumes;
//! - `new Request(usedRequest)` throws workerd's reconstruction TypeError
//!   while `clone()` still copies (#140);
//! - a bodyless POST reports `body === null` and reading it leaves
//!   `bodyUsed` false, both for a constructed Request and for the ingress
//!   request with no Content-Length or Transfer-Encoding (#142);
//! - the response pump and body consumers read a view chunk through internal
//!   slots, so a guest's own `buffer`/`byteOffset`/`byteLength` cannot widen
//!   the window or turn the reply empty (#130).
//!
//! One `cello dev` boots the app fixture; the controller fixture is
//! pre-seeded into the dev bucket via the library (the same `build` + `write`
//! `dev` itself runs), so the node loads both scripts and the service binding
//! resolves cross-isolate. There are no fixed sleeps: every wait polls an
//! observable condition with a deadline, and the server guard kills the child
//! on every exit path.

use serde_json::Value;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/fetch-used-body")
        .canonicalize()
        .expect("fixture exists")
}

mod common;

use common::{announced, cello_command, forward_announce};

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    for file in ["wrangler.json", "worker.js"] {
        std::fs::copy(src.join(file), dst.join(file)).expect("copy fixture");
    }
}

/// Tempdir for one fixture copy: `/dev/shm` when usable, else the
/// default tempdir, as the other cross-isolate suites do.
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

/// Deploy the controller into the app project's dev bucket before `dev`
/// boots, so the node loads both scripts. `dev` deploys the app itself and
/// becomes primary (last write wins `deploy/current.json`); the controller's
/// named pointer stays for the service binding to resolve.
fn preseed_controller(app_project: &Path, controller_src: &Path) {
    let state = app_project.join(".cello/dev");
    std::fs::create_dir_all(state.join("runtime")).expect("state dir");
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
            config: Some(controller_src.to_path_buf()),
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
        .expect("build controller");
        cello::deploy::write(&bucket, &built)
            .await
            .expect("write controller");
        cello::wake_format::ensure_ready(&bucket)
            .await
            .expect("ensure ready");
    });
}

/// A booted `cello dev` and its address. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
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

/// A request that carries no body framing at all: no `Content-Length` and no
/// `Transfer-Encoding`, as a bare `curl -X POST` or `curl` GET sends. Workerd
/// reports `body === null` for it, unlike `Content-Length: 0` (#142, #174).
fn no_framing(addr: &str, method: &str, path: &str) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect(addr).map_err(|error| format!("connect: {error}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .map_err(|error| format!("timeout: {error}"))?;
    let wire = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
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

/// The body with any HTTP/1.1 chunk framing removed. A streamed reply is
/// chunked; a `Content-Length` reply is already plain, and both round-trip.
fn dechunked(body: &str) -> String {
    let mut at = 0;
    let mut out = String::new();
    loop {
        let Some(line_end) = body[at..].find("\r\n").map(|offset| at + offset) else {
            return body.to_string();
        };
        let size_text = &body[at..line_end];
        if size_text.is_empty()
            || size_text.len() > 8
            || !size_text.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return body.to_string();
        }
        let Ok(size) = usize::from_str_radix(size_text, 16) else {
            return body.to_string();
        };
        if size == 0 {
            return out;
        }
        let start = line_end + 2;
        let end = start + size;
        if end > body.len() {
            return body.to_string();
        }
        out.push_str(&body[start..end]);
        at = end + 2;
    }
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

fn post_json(node: &TestNode, path: &str) -> Value {
    let (status, body) =
        request_timeout(&node.public, "POST", path, Duration::from_secs(60)).expect("public POST");
    assert_eq!(status, 200, "POST {path}: {body}");
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
        std::thread::sleep(Duration::from_millis(200));
    }
}

#[test]
fn fetch_used_body_conformance() {
    let dir = fixture_tempdir("cello-fetch-used-body-");
    let app_project = dir.path().join("app");
    let controller_src = dir.path().join("controller");
    copy_dir(&fixture_dir().join("app"), &app_project);
    copy_dir(&fixture_dir().join("controller"), &controller_src);
    preseed_controller(&app_project, &controller_src);
    let node = TestNode::boot(&app_project);

    // The body crossed the boundary and is readable.
    let read = get_json(&node, "/revived-read");
    assert_eq!(read["ok"], true, "{read}");
    assert_eq!(read["result"]["text"], "abc", "{read}");
    assert_eq!(read["result"]["after"], true, "{read}");

    // A fetch consumes the revived POST once; a second fetch is refused with
    // the Request reconstruction error, never a silent resend.
    let post = get_json(&node, "/revived-post");
    assert_eq!(post["ok"], true, "{post}");
    let result = &post["result"];
    assert_eq!(result["before"], false, "{post}");
    assert_eq!(result["outcome"]["name"], "Error", "{post}");
    assert_eq!(result["outcome"]["message"], "stop", "{post}");
    assert_eq!(result["after"], true, "{post}");
    assert_eq!(result["reread"]["name"], "TypeError", "{post}");
    assert_eq!(
        result["second"]["message"], "Cannot reconstruct a Request with a used body.",
        "{post}"
    );

    // A revived bodyless GET stays bodyless: fetching it consumes nothing,
    // so it can be fetched again.
    let get_route = get_json(&node, "/revived-get");
    assert_eq!(get_route["ok"], true, "{get_route}");
    let result = &get_route["result"];
    assert_eq!(result["bodyIsNull"], true, "{get_route}");
    assert_eq!(result["outcome"]["message"], "stop", "{get_route}");
    assert_eq!(result["after"], false, "{get_route}");
    assert_eq!(result["second"]["message"], "stop", "{get_route}");

    // A refused init leaves the revived request unused and readable.
    let bad = get_json(&node, "/revived-bad-init");
    assert_eq!(bad["ok"], true, "{bad}");
    let result = &bad["result"];
    assert_eq!(result["outcome"]["name"], "TypeError", "{bad}");
    assert_eq!(
        result["outcome"]["message"], "Invalid HTTP method string: BAD METHOD",
        "{bad}"
    );
    assert_eq!(result["after"], false, "{bad}");
    assert_eq!(result["text"], "abc", "{bad}");

    // #140: a used body cannot be rebuilt into a second Request, through
    // either the buffered consumer or a disturbed stream, while clone()
    // still copies an unused body in both shapes.
    let copy = get_json(&node, "/request-copy-used");
    assert_eq!(copy["ok"], true, "{copy}");
    let result = &copy["result"];
    assert_eq!(result["bufferedCopy"]["name"], "TypeError", "{copy}");
    assert_eq!(
        result["bufferedCopy"]["message"], "Cannot reconstruct a Request with a used body.",
        "{copy}"
    );
    assert_eq!(result["disturbedCopy"]["name"], "TypeError", "{copy}");
    assert_eq!(
        result["disturbedCopy"]["message"], "Cannot reconstruct a Request with a used body.",
        "{copy}"
    );
    assert_eq!(result["cloneText"], "clone", "{copy}");
    assert_eq!(result["sourceText"], "clone", "{copy}");

    // #142: a bodyless POST has no body stream, and reading it does not mark
    // it used. An explicit zero-length body is still a body.
    let bodyless = get_json(&node, "/request-bodyless-post");
    assert_eq!(bodyless["ok"], true, "{bodyless}");
    let result = &bodyless["result"];
    assert_eq!(result["bodyIsNull"], true, "{bodyless}");
    assert_eq!(result["usedBefore"], false, "{bodyless}");
    assert_eq!(result["text"], "", "{bodyless}");
    assert_eq!(result["usedAfter"], false, "{bodyless}");
    assert_eq!(result["copyBodyIsNull"], true, "{bodyless}");
    assert_eq!(result["emptyBodyIsNull"], false, "{bodyless}");
    assert_eq!(result["emptyText"], "", "{bodyless}");
    assert_eq!(result["emptyUsedAfter"], true, "{bodyless}");

    // #142/#174 at ingress: presence is the HTTP framing rule, not the
    // method. No framing is `body === null` and stays unused; `Content-Length:
    // 0` frames an empty body that text() consumes once, for a POST and a GET
    // alike.
    for method in ["POST", "GET"] {
        let (status, body) = no_framing(&node.public, method, "/incoming-inspect")
            .unwrap_or_else(|error| panic!("unframed {method}: {error}"));
        assert_eq!(status, 200, "unframed {method}: {body}");
        let unframed: Value = serde_json::from_str(&body).expect("JSON body");
        let result = &unframed["result"];
        assert_eq!(result["bodyIsNull"], true, "{unframed}");
        assert_eq!(result["usedBefore"], false, "{unframed}");
        assert_eq!(result["text"], "", "{unframed}");
        assert_eq!(result["usedAfter"], false, "{unframed}");
        assert_eq!(result["textAgain"]["value"], "", "{unframed}");

        let framed = if method == "POST" {
            post_json(&node, "/incoming-inspect")
        } else {
            get_json(&node, "/incoming-inspect")
        };
        let result = &framed["result"];
        assert_eq!(result["bodyIsNull"], false, "{framed}");
        assert_eq!(result["usedBefore"], false, "{framed}");
        assert_eq!(result["usedAfter"], true, "{framed}");
        assert_eq!(result["textAgain"]["name"], "TypeError", "{framed}");
        assert_eq!(
            result["textAgain"]["message"],
            "Body has already been used. It can only be used once. \
             Use tee() first if you need to read it twice.",
            "{framed}"
        );
    }

    // #130: the response pump reads the view's real window even when the
    // guest shadows its properties or keeps the backing buffer tight.
    for route in ["/chunk-shadow-wide", "/chunk-shadow-tight"] {
        let (status, body) = get(&node.public, route).expect("chunk-shadow GET");
        assert_eq!(status, 200, "{route}: {body}");
        assert_eq!(dechunked(&body), "abc", "{route}: {body:?}");
    }

    // #130: a forged prototype is the bytes TypeError, the same as workerd;
    // it must never be silently sent as an empty body.
    let forged = get_json(&node, "/chunk-forged");
    assert_eq!(forged["ok"], true, "{forged}");
    let result = &forged["result"];
    for shape in ["typed", "buffer", "dataView"] {
        assert_eq!(result[shape]["name"], "TypeError", "{shape}: {forged}");
        assert_eq!(
            result[shape]["message"], "This ReadableStream did not return bytes.",
            "{shape}: {forged}"
        );
    }
}
