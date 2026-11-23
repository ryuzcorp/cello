// Modified by Ryuz Corp (Cello): Fetch API object-model conformance (workerd
// differential sweep).
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! The Fetch API object model answers what workerd 1.20261121.1 answers.
//!
//! Every test below is one group of divergences the differential harness found
//! and filed: a string body's default `content-type` (#159), cache modes
//! (#160), an incoming request's `manual` redirect mode (#161), `Response`
//! status validation (#163), a body on a null-body status (#165), CR/LF in
//! `statusText` (#167), immutable incoming headers (#169), the abort reason
//! wording (#172), a followed redirect's `url`/`redirected` (#173), relative
//! URLs in `new Request` (#175), a GET/HEAD body (#176), `integrity`/
//! `keepalive` defaults (#177), a non-signal `signal` and a non-empty
//! `integrity` (#178), `File.lastModified` (#179), the retained
//! `content-length` (#180), header validation messages (#182), URL /
//! `AbortSignal.any` / redirect error messages (#184), `blob().type` (#186), a
//! FormData Blob's name (#188) and TRACE (#190).
//!
//! One test per group, each booting its own `cello dev`, so a regression names
//! the behaviour it broke instead of stopping at the first assertion. The
//! boots are serialized: eight dev nodes at once would only fight for the
//! machine. The same Worker runs on workerd under `scripts/workerd-diff`, so
//! the transcripts are pinned on both sides.

use serde_json::Value;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

mod common;
use common::{announced, cello_command, forward_announce};

/// The gzip member for "hello gzip" (30 bytes), so the helper origin can
/// answer a compressed body with a `content-length` that does not describe
/// the decoded bytes.
const GZIP_BODY: [u8; 30] = [
    31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 203, 72, 205, 201, 201, 87, 72, 175, 202, 44, 0, 0, 25, 106,
    210, 223, 10, 0, 0, 0,
];

/// One boot at a time: every test needs a `cello dev`, and eight concurrent
/// nodes only contend for CPU. A panicking test poisons the lock, which the
/// next one must survive, so the poison is taken over rather than unwrapped.
fn serial() -> MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    SERIAL
        .lock()
        .unwrap_or_else(|poison: std::sync::PoisonError<_>| poison.into_inner())
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/fetch-api")
        .canonicalize()
        .expect("fixture exists")
}

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

/// A booted `cello dev`, its fixture copy and its helper origin. The child is
/// killed on drop, so a failing assertion cannot leak a server onto its port.
struct Booted {
    node: TestNode,
    /// Held only for its lifetime: dropping the tempdir removes the fixture
    /// copy the node booted from.
    _project: tempfile::TempDir,
}

struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(app_project: &Path) -> Self {
        let mut child = cello_command(app_project, [("NO_COLOR", "1".to_string())])
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

/// Boots one fixture copy and the helper origin its `fetch()` calls reach.
fn boot(prefix: &str) -> Booted {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind origin");
    let origin_port = listener.local_addr().expect("origin addr").port();
    std::thread::spawn(move || serve_origin(listener));
    let fixture = fixture_dir();
    let project = fixture_tempdir(prefix);
    copy_dir(&fixture, project.path(), origin_port);
    let node = TestNode::boot(project.path());
    Booted {
        node,
        _project: project,
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

/// The helper origin the fixture fetches, so its `fetch()` calls do not target
/// this Worker. It answers plain HTTP/1.1 on loopback, one response per
/// connection, always with an exact `Content-Length`.
fn serve_origin(listener: TcpListener) {
    for stream in listener.incoming() {
        let Ok(mut stream) = stream else {
            continue;
        };
        std::thread::spawn(move || {
            let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
            let head = read_head(&mut stream);
            let line = String::from_utf8_lossy(&head);
            let request = line.lines().next().unwrap_or_default().to_string();
            let mut parts = request.split_whitespace();
            let method = parts.next().unwrap_or_default();
            let target = parts.next().unwrap_or_default();
            if target.starts_with("/redirect") {
                let code = query(target, "code").unwrap_or("302");
                let to = query(target, "to").unwrap_or("/target");
                respond(
                    &mut stream,
                    &format!("{code} Found"),
                    &[("Location", to), ("Content-Length", "0")],
                    b"",
                );
            } else if target.starts_with("/gzip") {
                let length = GZIP_BODY.len().to_string();
                respond(
                    &mut stream,
                    "200 OK",
                    &[
                        ("Content-Type", "text/plain"),
                        ("Content-Encoding", "gzip"),
                        ("Content-Length", &length),
                    ],
                    &GZIP_BODY,
                );
            } else if target.starts_with("/no-content") {
                respond(&mut stream, "204 No Content", &[("x-helper", "yes")], b"");
            } else if target.starts_with("/reset-content") {
                respond(
                    &mut stream,
                    "205 Reset Content",
                    &[("Content-Length", "0")],
                    b"",
                );
            } else if target.starts_with("/not-modified") {
                respond(&mut stream, "304 Not Modified", &[("etag", "\"v1\"")], b"");
            } else if target.starts_with("/echo") {
                // Reproduce the request head and measure the body the host
                // framed, so a replayed `content-length` shows up as a short
                // read (#159, #180). The read is bounded: a wrong declared
                // length must fail the assertion, not hang the test.
                let declared = declared_length(&head);
                let mut received = Vec::new();
                let mut chunk = [0u8; 1024];
                let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                while received.len() < declared {
                    match stream.read(&mut chunk) {
                        Ok(0) | Err(_) => break,
                        Ok(read) => received.extend_from_slice(&chunk[..read]),
                    }
                }
                let mut headers = serde_json::Map::new();
                for header in line.lines().skip(1) {
                    if let Some((name, value)) = header.split_once(':') {
                        let name = name.trim().to_ascii_lowercase();
                        if name == "content-type" || name == "content-length" {
                            headers.insert(name, Value::String(value.trim().to_string()));
                        }
                    }
                }
                let body = serde_json::json!({
                    "method": method,
                    "headers": Value::Object(headers),
                    "declaredLength": declared,
                    "bodyBytes": received.len(),
                })
                .to_string();
                let length = body.len().to_string();
                respond(
                    &mut stream,
                    "200 OK",
                    &[
                        ("Content-Type", "application/json"),
                        ("Content-Length", &length),
                    ],
                    body.as_bytes(),
                );
            } else {
                respond(
                    &mut stream,
                    "200 OK",
                    &[
                        ("Content-Type", "text/plain"),
                        ("Content-Length", &"target-body".len().to_string()),
                    ],
                    b"target-body",
                );
            }
            let _ = stream.flush();
        });
    }
}

/// The request body length the head declares, or 0 when it names none.
fn declared_length(head: &[u8]) -> usize {
    String::from_utf8_lossy(head)
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())?
        })
        .unwrap_or(0)
}

fn query<'a>(target: &'a str, name: &str) -> Option<&'a str> {
    target
        .split('&')
        .find_map(|pair| pair.strip_prefix(&format!("{name}=")))
}

/// One HTTP/1.1 response, framed by an exact `Content-Length` and closed.
fn respond(stream: &mut TcpStream, status: &str, headers: &[(&str, &str)], body: &[u8]) {
    let mut head = format!("HTTP/1.1 {status}\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("Connection: close\r\n\r\n");
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body);
}

/// A raw HTTP/1.1 reply: status, the header block's pairs as received, and the
/// body.
type RawReply = (u16, Vec<(String, String)>, String);

/// One GET: status, the header block's pairs (names as received) and the body.
fn get_full(addr: &str, path: &str) -> Result<RawReply, String> {
    let mut stream = TcpStream::connect(addr).map_err(|error| format!("connect: {error}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .map_err(|error| format!("timeout: {error}"))?;
    let wire = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nx-sent: one\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
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
    let headers = head
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_string(), value.trim().to_string()))
        .collect();
    Ok((status, headers, body.to_string()))
}

fn get(addr: &str, path: &str) -> Result<(u16, String), String> {
    get_full(addr, path).map(|(status, _, body)| (status, body))
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

fn json_of(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|error| panic!("result JSON ({error}): {body}"))
}

/// Reads `/path` and returns the route's own report, so a failed route surfaces
/// its `uncaught` entry instead of a JSON parse error.
fn route(node: &TestNode, path: &str) -> Value {
    let (status, body) = get(&node.public, path).expect("request");
    assert_eq!(status, 200, "{path}: {body}");
    let out = json_of(&body);
    assert!(
        out.get("uncaught").is_none(),
        "{path} threw: {}",
        out["uncaught"]
    );
    out
}

/// The outcome of one `settle` attempt at `path`, either `.ok` or
/// `.threw.name`/`.threw.message`.
fn outcome(value: &Value) -> String {
    if let Some(ok) = value.get("ok") {
        return match ok {
            Value::String(text) => text.clone(),
            other => other.to_string(),
        };
    }
    let threw = &value["threw"];
    format!(
        "{}: {}",
        threw["name"].as_str().unwrap_or("?"),
        threw["message"].as_str().unwrap_or("?")
    )
}

fn assert_outcome(value: &Value, expected: &str, what: &str) {
    assert_eq!(outcome(value), expected, "{what}: {value}");
}

// #159 / #186: a string body's default type, bytes staying untyped, and the
// parsed MIME type of a Blob built from a response.
#[test]
fn response_body_types_match_workerd() {
    let _serial = serial();
    let booted = boot("cello-fetch-api-body-");
    let body = route(&booted.node, "/body-defaults");
    assert_eq!(
        body["string"],
        serde_json::json!([["content-type", "text/plain;charset=UTF-8"]]),
        "#159: a string Response body carries the Fetch default type: {body}"
    );
    assert_eq!(
        body["requestString"],
        serde_json::json!([["content-type", "text/plain;charset=UTF-8"]]),
        "#159: a string Request body carries the Fetch default type: {body}"
    );
    assert_eq!(
        body["bytes"],
        serde_json::json!([]),
        "#159: bytes stay untyped"
    );
    assert_eq!(
        body["requestBytes"],
        serde_json::json!([]),
        "#159: byte bodies stay untyped"
    );
    assert_eq!(
        body["number"],
        serde_json::json!([["content-type", "text/plain;charset=UTF-8"]]),
        "#159: a non-BodyInit body stringifies and carries the default type"
    );
    assert_eq!(
        body["explicit"],
        serde_json::json!([["content-type", "text/html"]]),
        "#159: an explicit content-type is never overwritten"
    );
    assert_eq!(
        body["blobTypes"]["spaced"], "text/plain;charset=utf-8",
        "#186: blob().type re-serializes the MIME type: {body}"
    );
    assert_eq!(
        body["blobTypes"]["upper"], "text/plain;charset=UTF-8",
        "#186"
    );
    assert_eq!(
        body["blobTypes"]["boundary"], "text/plain;charset=utf-8; boundary=abc",
        "#186: workerd keeps a space before boundary: {body}"
    );
    assert_eq!(
        body["blobTypes"]["bare"], "text/plain;charset=UTF-8",
        "#159/#186"
    );
    assert_eq!(
        body["blobTypes"]["invalid"], "",
        "#186: an unparsable type is empty"
    );
    assert_eq!(body["text"], "hello");
}

// #163: status range and integer conversion. #165: a null-body status with
// a body. #167: CR/LF in statusText.
#[test]
fn response_status_validation_matches_workerd() {
    let _serial = serial();
    let booted = boot("cello-fetch-api-status-");
    let status = route(&booted.node, "/response-status");
    let range = "RangeError: Responses may only be constructed with status codes in the range 200 to 599, inclusive.";
    for code in ["0", "99", "101", "600", "junk"] {
        assert_outcome(
            &status["status"][code],
            range,
            &format!("#163 status {code}"),
        );
    }
    assert_eq!(status["status"]["200"]["ok"], 200);
    assert_eq!(
        status["status"]["201.5"]["ok"], 201,
        "#163: a fractional status truncates"
    );
    assert_eq!(
        status["status"]["string"]["ok"], 200,
        "#163: a numeric string converts"
    );
    let null_body =
        "TypeError: Response with null body status (101, 204, 205, or 304) cannot have a body.";
    for case in ["204-string", "205-string", "304-stream"] {
        assert_outcome(
            &status["nullBody"][case],
            null_body,
            &format!("#165 {case}"),
        );
    }
    assert_eq!(status["nullBody"]["204-null"]["ok"], 204);
    assert_eq!(
        status["nullBody"]["204-empty"]["ok"], 204,
        "#165: an empty body is allowed"
    );
    assert_eq!(status["nullBody"]["304-null"]["ok"], 304);
    for case in ["newline", "cr", "tab"] {
        assert_outcome(
            &status["statusText"][case],
            "TypeError: Invalid statusText",
            &format!("#167 {case}"),
        );
    }
    assert_eq!(status["statusText"]["plain"]["ok"], "Made It");
    assert_eq!(status["statusText"]["unicode"]["ok"], "h\u{e9}llo");
    assert_eq!(
        status["plainWebSocket"]["status"], 101,
        "#163: a 101 with a WebSocket is valid"
    );
    assert_eq!(status["plainWebSocket"]["body"], Value::Null);
    assert_outcome(
        &status["plainWebSocket"]["mismatch"],
        "RangeError: Responses with a WebSocket must have status code 101.",
        "#163",
    );
}

// #165: a zero-length buffered body on a null-body status is no body at all.
// Before this it kept a non-null body and the #159 default-type rule added
// `content-type: text/plain;charset=UTF-8`, which workerd never does.
#[test]
fn null_body_zero_length_body_becomes_null() {
    let _serial = serial();
    let booted = boot("cello-fetch-api-null-body-empty-");
    let out = route(&booted.node, "/response/null-body-zero-length");
    for (case, status) in [
        ("emptyString", 204),
        ("emptyBytes", 204),
        ("emptyBlob", 204),
        ("emptyTypedBlob", 204),
        ("emptySearchParams", 204),
        ("emptyString304", 304),
    ] {
        let response = &out[case];
        assert_eq!(response["status"], status, "#165 {case}: {response}");
        assert_eq!(
            response["bodyIsNull"], true,
            "#165 {case}: a zero-length body is no body: {response}"
        );
        assert_eq!(
            response["headers"],
            serde_json::json!([]),
            "#165 {case}: no content-type is inferred: {response}"
        );
        assert_eq!(response["text"], "", "#165 {case}: {response}");
        assert_eq!(response["bodyUsed"], false, "#165 {case}: {response}");
    }
    assert_eq!(
        out["explicitType"]["headers"],
        serde_json::json!([["content-type", "application/custom"]]),
        "#165: an explicit content-type is kept"
    );
    assert_eq!(out["explicitType"]["bodyIsNull"], true);
    assert_outcome(
        &out["nonEmpty"],
        "TypeError: Response with null body status (101, 204, 205, or 304) cannot have a body.",
        "#165: a non-empty body is still refused",
    );
}

// #160 / #175 / #176 / #177 / #178 / #184 / #190.
#[test]
fn request_options_match_workerd() {
    let _serial = serial();
    let booted = boot("cello-fetch-api-request-");
    let options = route(&booted.node, "/request-options");
    assert_eq!(options["cache"]["no-store"]["ok"], "no-store", "#160");
    assert_eq!(options["cache"]["no-cache"]["ok"], "no-cache", "#160");
    assert_outcome(
        &options["cache"]["default"],
        "TypeError: Unsupported cache mode: default",
        "#160",
    );
    assert_outcome(
        &options["cache"]["bogus"],
        "TypeError: Unsupported cache mode: bogus",
        "#160",
    );
    assert_outcome(
        &options["url"]["relative"],
        "TypeError: Invalid URL: /x",
        "#175",
    );
    assert_outcome(
        &options["url"]["junk"],
        "TypeError: Invalid URL: not a url",
        "#175",
    );
    assert_eq!(
        options["url"]["normal"]["ok"], "https://example.com/a%20b",
        "#175: the stored url is the serialized absolute URL: {options}"
    );
    let get_body = "TypeError: Request with a GET or HEAD method cannot have a body.";
    assert_outcome(&options["body"]["get"], get_body, "#176");
    assert_outcome(&options["body"]["head"], get_body, "#176");
    assert_eq!(options["body"]["post"]["ok"], "POST");
    assert_eq!(options["defaults"]["integrity"], "", "#177");
    assert_eq!(options["defaults"]["keepalive"], false, "#177");
    assert_eq!(options["defaults"]["cache"], "<undefined>", "#177/#160");
    assert_outcome(
        &options["signal"]["junk"],
        "TypeError: Incorrect type for the 'signal' field on 'RequestInitializerDict': the provided value is not of type 'AbortSignal'.",
        "#178",
    );
    assert_eq!(
        options["signal"]["null"]["ok"], true,
        "#178: null is an absent signal"
    );
    assert_outcome(
        &options["integrity"]["value"],
        "TypeError: Subrequest integrity checking is not implemented. The integrity option must be either undefined or an empty string.",
        "#178",
    );
    assert_eq!(options["integrity"]["empty"]["ok"], "", "#178");
    let bad_redirect = "TypeError: Invalid redirect value, must be one of \"follow\" or \"manual\" (\"error\" won't be implemented since it does not make sense at the edge; use \"manual\" and check the response status code).";
    assert_outcome(&options["redirect"]["error"], bad_redirect, "#184");
    assert_eq!(
        options["redirect"]["upper"]["ok"], "follow",
        "#184: the mode is case-insensitive"
    );
    assert_outcome(&options["redirect"]["bogus"], bad_redirect, "#184");
    assert_eq!(options["method"]["trace"]["ok"], "TRACE", "#190");
    assert_eq!(options["method"]["TRACE"]["ok"], "TRACE", "#190");
    assert_outcome(
        &options["method"]["connect"],
        "TypeError: Invalid HTTP method string: connect",
        "#190",
    );
    // #169: `headers` is readonly, with workerd's assignment message.
    assert_outcome(
        &options["headersAssign"],
        "TypeError: Cannot assign to read only property 'headers' of object '#<Request>'",
        "#169 headers assignment",
    );
}

// #172 the default abort reason; #184 AbortSignal.any and the URL messages.
#[test]
fn abort_and_url_messages_match_workerd() {
    let _serial = serial();
    let booted = boot("cello-fetch-api-abort-");
    let abort = route(&booted.node, "/abort");
    assert_eq!(abort["name"], "AbortError", "#172");
    assert_eq!(abort["message"], "The operation was aborted", "#172");
    assert_outcome(
        &abort["thrown"],
        "AbortError: The operation was aborted",
        "#172",
    );
    assert_outcome(
        &abort["anyElement"],
        "TypeError: Incorrect type for array element 0: the provided value is not of type 'AbortSignal'.",
        "#184",
    );
    assert_outcome(
        &abort["anyElementIndex"],
        "TypeError: Incorrect type for array element 1: the provided value is not of type 'AbortSignal'.",
        "#184",
    );
    assert_outcome(
        &abort["anyNotArray"],
        "TypeError: Failed to execute 'any' on 'AbortSignal': parameter 1 is not of type 'Array'.",
        "#184",
    );

    let url = route(&booted.node, "/url");
    assert_outcome(
        &url["invalidPort"],
        "TypeError: Invalid URL string.",
        "#184",
    );
    assert_outcome(&url["invalid"], "TypeError: Invalid URL string.", "#184");
    assert_outcome(&url["relative"], "TypeError: Invalid URL string.", "#184");
    assert_outcome(
        &url["searchParams"],
        "TypeError: Cannot assign to read only property 'searchParams' of object '[object URL]'",
        "#184",
    );
}

// #179 File lastModified; #188 a Blob entry's name.
#[test]
fn file_and_formdata_match_workerd() {
    let _serial = serial();
    let booted = boot("cello-fetch-api-file-");
    let file = route(&booted.node, "/file-formdata");
    assert_eq!(
        file["lastModified"]["infinity"]["ok"],
        Value::Null,
        "#179: Infinity survives"
    );
    assert_eq!(file["lastModified"]["nan"]["ok"], 0, "#179");
    assert_eq!(file["lastModified"]["negative"]["ok"], -1, "#179");
    assert_eq!(file["names"]["blob"], "blobNoName", "#188");
    assert_eq!(
        file["names"]["explicit"], "explicit.bin",
        "#188: an explicit filename wins"
    );
    assert_eq!(file["names"]["file"], "own.txt", "#188");
    assert_eq!(
        file["names"]["set"], "setKey",
        "#188: set() names it after the key too"
    );
}

// #182 header validation messages and the names workerd accepts.
#[test]
fn header_validation_matches_workerd() {
    let _serial = serial();
    let booted = boot("cello-fetch-api-headers-");
    let headers = route(&booted.node, "/headers");
    assert_outcome(
        &headers["appendSpace"],
        "TypeError: Invalid header name.",
        "#182",
    );
    assert_outcome(
        &headers["valueNewline"],
        "TypeError: Invalid header value.",
        "#182",
    );
    assert_outcome(
        &headers["valueNul"],
        "TypeError: Invalid header value.",
        "#182",
    );
    assert_eq!(
        headers["emptyName"]["ok"],
        serde_json::json!([["", "v"]]),
        "#182: workerd accepts an empty header name (a deliberate deviation from the spec)"
    );
    assert_eq!(
        headers["unicodeName"]["ok"],
        serde_json::json!([["\u{2713}", "v"]]),
        "#182: workerd accepts a non-ASCII header name"
    );
    assert_outcome(
        &headers["sequenceEntry"],
        "TypeError: To initialize a Headers object from a sequence, each inner sequence must have exactly two elements.",
        "#182",
    );
    assert_outcome(
        &headers["primitiveInit"],
        "TypeError: Failed to construct 'Headers': constructor parameter 1 is not of type 'Headers or Sequence or object'.",
        "#182",
    );
    assert_outcome(
        &headers["stringInit"],
        "TypeError: Failed to construct 'Headers': constructor parameter 1 is not of type 'Headers or Sequence or object'.",
        "#182: a string is not a valid init",
    );
    assert_eq!(
        headers["functionInit"]["ok"],
        serde_json::json!([]),
        "#182: a function is a record"
    );
}

// #161 / #169 / #176 on the incoming request.
#[test]
fn incoming_request_guards_match_workerd() {
    let _serial = serial();
    let booted = boot("cello-fetch-api-incoming-");
    let incoming = route(&booted.node, "/incoming");
    assert_eq!(incoming["redirect"], "manual", "#161");
    assert_eq!(incoming["copyRedirect"], "manual", "#161");
    for case in ["set", "append", "delete"] {
        assert_outcome(
            &incoming["headers"][case],
            "TypeError: Can't modify immutable headers.",
            &format!("#169 incoming headers.{case}"),
        );
    }
    assert_eq!(
        incoming["headers"]["after"],
        Value::Null,
        "#169: the header was not added"
    );
    assert_eq!(incoming["headers"]["kept"], "one", "#169");
    assert_eq!(
        incoming["copyMutable"]["ok"], "1",
        "#169: a copy's headers are mutable"
    );
    assert_eq!(
        incoming["bodylessGet"]["clone"]["ok"], "GET",
        "#176: a host-built GET is not refused"
    );
}

// #173 / #161 / #180 / #159 (subrequest) / #178 (fetch init).
#[test]
fn fetch_semantics_match_workerd() {
    let _serial = serial();
    let booted = boot("cello-fetch-api-fetch-");
    let fetch = route(&booted.node, "/fetch");
    assert_eq!(fetch["follow"]["status"], 200, "#173: {fetch}");
    assert_eq!(
        fetch["follow"]["redirected"], true,
        "#173: a followed redirect is reported"
    );
    assert_eq!(
        fetch["follow"]["url"], "/target",
        "#173: response.url is the final URL"
    );
    assert_eq!(fetch["follow"]["body"], "target-body", "#173");
    assert_eq!(
        fetch["manual"]["status"], 302,
        "#173: manual does not follow"
    );
    assert_eq!(fetch["manual"]["redirected"], false, "#173");
    assert_eq!(fetch["manual"]["location"], "/target", "#173");
    assert_eq!(
        fetch["proxied"]["status"], 302,
        "#161: an incoming request proxies as manual"
    );
    assert_eq!(fetch["proxied"]["redirectMode"], "manual", "#161");
    assert_eq!(fetch["gzip"]["encoding"], "gzip", "#180");
    assert_eq!(
        fetch["gzip"]["length"], "30",
        "#180: content-length is retained"
    );
    assert_eq!(fetch["gzip"]["body"], "hello gzip", "#180");
    assert_eq!(fetch["echo"]["method"], "POST");
    assert_eq!(
        fetch["echo"]["contentType"], "text/plain;charset=UTF-8",
        "#159: a string body reaches the origin typed"
    );
    assert_eq!(
        fetch["fetchInit"]["cache"]["ok"], 200,
        "#160: fetch(url, {{ cache }}) works"
    );
    assert_outcome(
        &fetch["fetchInit"]["integrity"],
        "TypeError: Subrequest integrity checking is not implemented. The integrity option must be either undefined or an empty string.",
        "#178 fetch init",
    );
    assert_outcome(
        &fetch["fetchInit"]["redirectError"],
        "TypeError: Invalid redirect value, must be one of \"follow\" or \"manual\" (\"error\" won't be implemented since it does not make sense at the edge; use \"manual\" and check the response status code).",
        "#184 fetch init",
    );
}

// #165: a fetched null-body status has no body at all. Before the host stopped
// registering a stream for one, `fetch()` of a 204/205/304 threw the
// constructor's null-body TypeError -- which broke every void RPC that answers
// 204.
#[test]
fn fetch_null_body_statuses_match_workerd() {
    let _serial = serial();
    let booted = boot("cello-fetch-api-null-body-");
    let out = route(&booted.node, "/fetch/null-body");
    for (key, status) in [
        ("noContent", 204),
        ("resetContent", 205),
        ("notModified", 304),
    ] {
        let case = &out[key];
        assert_eq!(case["status"], status, "#165 {key}: {out}");
        assert_eq!(
            case["bodyIsNull"], true,
            "#165 {key}: a fetched null-body status has no body: {out}"
        );
        assert_eq!(
            case["contentType"],
            Value::Null,
            "#165 {key}: no content-type is inferred: {out}"
        );
        assert_eq!(case["text"], "", "#165 {key}");
    }
}

// #182: an odd header name must not destroy the whole response. Workerd keeps
// the response, drops the names its header layer cannot represent, and keeps
// the bytes a field value may carry.
#[test]
fn odd_response_headers_keep_the_response() {
    let _serial = serial();
    let booted = boot("cello-fetch-api-odd-headers-");
    let (status, headers, body) =
        get_full(&booted.node.public, "/raw/odd-headers").expect("request");
    assert_eq!(
        status, 200,
        "#182: the response survives an unrepresentable header name: {body}"
    );
    assert_eq!(body, "ok", "#182");
    let value = |name: &str| {
        headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    };
    assert_eq!(value("x-ok"), Some("1"), "#182: {headers:?}");
    assert_eq!(
        value("x-ctl"),
        Some("ab"),
        "#182: the byte a field value cannot carry is dropped, the value stays: {headers:?}"
    );
    assert!(
        headers.iter().all(|(name, _)| !name.is_empty()),
        "#182: an empty header name is not sent: {headers:?}"
    );
    assert!(
        headers.iter().all(|(name, _)| name.is_ascii()),
        "#182: a non-ASCII header name is not sent: {headers:?}"
    );
}

// #180: a forwarded response's `content-length` is kept on the Response and
// dropped from the wire, where the host frames the new body itself.
#[test]
fn subrequest_framing_is_the_hosts() {
    let _serial = serial();
    let booted = boot("cello-fetch-api-reframe-");
    let out = route(&booted.node, "/fetch/reframe");
    assert_eq!(out["responseEncoding"], "gzip", "#180");
    assert_eq!(
        out["responseLength"], "30",
        "#180: the fetched Response keeps the origin's length: {out}"
    );
    assert_eq!(out["body"], "hello gzip", "#180");
    assert_eq!(
        out["sentLength"], "10",
        "#180: the subrequest body is framed by the host, not by the replayed header: {out}"
    );
    assert_eq!(out["sentBytes"], 10, "#180: {out}");
}
