// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by Ryuz Corp (Cello): CryptoKey internal-slot regression test (#197).

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines.
#![allow(clippy::disallowed_methods)]

//! A `CryptoKey`'s usage, algorithm, extractability and material live only in
//! internal slots. Before the fix they were readable own properties:
//! `key.__celloMaterial.bytes` returned a non-extractable key's secret,
//! `new CryptoKey(...)` built a key from guest code, and a forged
//! `Object.create(CryptoKey.prototype)` signed with copied state. Every attack
//! the security review measured is asserted here, plus the property shape and
//! the structured-clone revival path that must keep working.
//!
//! The fixture is the differential gate's `corpus/crypto`, so the
//! cross-runtime half of the contract (`/isolation`) stays one copy.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../scripts/workerd-diff/corpus/crypto")
        .canonicalize()
        .expect("gate crypto fixture exists")
}

mod common;
use common::{announced, cello_command, forward_announce};

/// Tempdir for one fixture copy: `/dev/shm` when usable, else the
/// default tempdir.
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
    for file in ["wrangler.json", "index.js"] {
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
            probe(&addr, "/values").then_some(addr)
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

fn json(addr: &str, path: &str) -> serde_json::Value {
    let (status, body) = get(addr, path).expect("GET fixture route");
    assert_eq!(status, 200, "GET {path}: {body}");
    serde_json::from_str(&body).expect("route JSON")
}

fn error(value: &serde_json::Value) -> (&str, &str) {
    let name = value
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("<missing>");
    let message = value
        .get("message")
        .and_then(|v| v.as_str())
        .unwrap_or("<missing>");
    (name, message)
}

#[test]
fn crypto_key_state_lives_in_internal_slots() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("cello-crypto-key-isolation-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    // Cross-runtime contract: workerd answers each of these the same way, so
    // the differential gate pins them too.
    let isolation = json(&node.public, "/isolation");
    assert_eq!(
        isolation.get("materialType").and_then(|v| v.as_str()),
        Some("undefined"),
        "key material is reachable from guest code: {isolation}"
    );
    assert_eq!(
        isolation.get("ownNames"),
        Some(&serde_json::json!([
            "algorithm",
            "extractable",
            "type",
            "usages"
        ])),
        "unexpected CryptoKey own names: {isolation}"
    );
    assert_eq!(
        isolation.get("symbolFunctions"),
        Some(&serde_json::json!([])),
        "a prototype symbol is callable as a serializer: {isolation}"
    );
    for probe in ["constructor", "constructorCall"] {
        let result = isolation.get(probe).expect(probe);
        assert_eq!(
            result.get("ok").and_then(|v| v.as_bool()),
            Some(false),
            "{probe} returned a key: {isolation}"
        );
        assert_eq!(
            error(result.get("error").expect("error object")),
            ("TypeError", "Illegal constructor"),
            "{probe} must throw"
        );
    }
    assert_eq!(
        isolation.get("constructorLength").and_then(|v| v.as_u64()),
        Some(0),
        "CryptoKey.length: {isolation}"
    );
    assert_eq!(
        isolation.get("constructorTag").and_then(|v| v.as_str()),
        Some("[object CryptoKey]"),
        "CryptoKey tag: {isolation}"
    );
    for (probe, method) in [("forgedSign", "sign"), ("forgedWrap", "wrapKey")] {
        let result = isolation.get(probe).expect(probe);
        assert_eq!(result.get("ok").and_then(|v| v.as_bool()), Some(false));
        let (name, message) = error(result.get("error").expect("error object"));
        assert_eq!(name, "TypeError", "{probe}: {isolation}");
        assert_eq!(
            message,
            format!(
                "Failed to execute '{method}' on 'SubtleCrypto': parameter 2 is \
                 not of type 'CryptoKey'."
            ),
            "{probe}: {isolation}"
        );
    }
    let export = isolation.get("exportNonExtractable").expect("export probe");
    let (name, message) = error(export.get("error").expect("error object"));
    assert_eq!(
        (name, message),
        (
            "InvalidAccessError",
            "Attempt to export non-extractable HMAC key."
        ),
        "a non-extractable key exported: {isolation}"
    );
    let verify_sign = isolation.get("verifyOnlySign").expect("verify-only probe");
    let (name, message) = error(verify_sign.get("error").expect("error object"));
    assert_eq!(
        (name, message),
        (
            "InvalidAccessError",
            "Requested key usage \"sign\" does not match any usage listed in this CryptoKey."
        ),
        "a verify-only key signed: {isolation}"
    );

    // structuredClone never mints a host object from guest data.
    let clone_key = isolation.get("cloneKey").expect("clone key probe");
    assert_eq!(clone_key.get("ok").and_then(|v| v.as_bool()), Some(false));
    assert_eq!(
        error(clone_key.get("error").expect("error object")),
        (
            "DataCloneError",
            "Could not serialize object of type \"CryptoKey\". This type does not support serialization."
        ),
        "CryptoKey was cloned: {isolation}"
    );
    let clone_proto = isolation
        .get("cloneProto")
        .and_then(|v| v.get("value"))
        .expect("clone proto probe");
    assert_eq!(
        clone_proto.get("isKey").and_then(|v| v.as_bool()),
        Some(false),
        "a prototype-only forgery revived as a CryptoKey: {isolation}"
    );
    assert_eq!(
        clone_proto.get("tag").and_then(|v| v.as_str()),
        Some("[object Object]")
    );
    let clone_forged = isolation
        .get("cloneForged")
        .and_then(|v| v.get("value"))
        .expect("clone forged probe");
    assert_eq!(
        clone_forged.get("isKey").and_then(|v| v.as_bool()),
        Some(false),
        "key-shaped data revived as a CryptoKey: {isolation}"
    );
    let forged_blob = isolation
        .get("cloneForgedBlob")
        .and_then(|v| v.get("value"))
        .expect("forged blob probe");
    assert_eq!(
        forged_blob.get("isBlob").and_then(|v| v.as_bool()),
        Some(false),
        "a Blob prototype-only forgery revived as a Blob: {isolation}"
    );
    let forged_request = isolation
        .get("cloneForgedRequest")
        .and_then(|v| v.get("value"))
        .expect("forged request probe");
    assert_eq!(
        forged_request.get("isRequest").and_then(|v| v.as_bool()),
        Some(false),
        "a Request prototype-only forgery revived as a Request: {isolation}"
    );
    let key_object = isolation.get("keyObjectExport").expect("KeyObject probe");
    assert_eq!(
        error(key_object.get("error").expect("error object")),
        ("TypeError", "Unable to export non-extractable key"),
        "KeyObject.from(non-extractable).export() succeeded: {isolation}"
    );

    // Cello-only shape: the properties are frozen copies and operations read
    // slots, so a mutation attempt cannot change a signature.
    let strict = json(&node.public, "/isolation-strict");
    let mutation = strict.get("mutation").expect("mutation probe");
    assert_eq!(
        mutation.get("same").and_then(|v| v.as_bool()),
        Some(true),
        "mutating key.algorithm changed the HMAC hash: {strict}"
    );
    assert_eq!(
        mutation.get("reported").and_then(|v| v.as_str()),
        Some("SHA-256"),
        "the key reported the mutated hash: {strict}"
    );
    assert_eq!(
        strict.get("mutateThrew").and_then(|v| v.as_bool()),
        Some(true),
        "the algorithm copy is not frozen: {strict}"
    );
    assert_eq!(
        strict.get("pushThrew").and_then(|v| v.as_bool()),
        Some(true),
        "the usages copy is not frozen: {strict}"
    );
    assert_eq!(
        strict.get("usagesAfterPush"),
        Some(&serde_json::json!(["sign"])),
        "usages changed: {strict}"
    );
    assert_eq!(
        strict.get("algorithmFrozen").and_then(|v| v.as_bool()),
        Some(true),
        "algorithm dictionary is not frozen: {strict}"
    );
    assert_eq!(
        strict.get("usagesFrozen").and_then(|v| v.as_bool()),
        Some(true),
        "usages array is not frozen: {strict}"
    );
    let descriptor = strict.get("descriptor").expect("descriptor probe");
    assert_eq!(
        descriptor.get("accessor").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert_eq!(
        descriptor.get("enumerable").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert_eq!(
        descriptor.get("configurable").and_then(|v| v.as_bool()),
        Some(true)
    );

    // The clone registry leaves no symbol to steal, and copying a real
    // Blob's constructor brand cannot revive a Blob.
    assert_eq!(
        strict.get("legacySymbol"),
        Some(&serde_json::json!(["undefined", "undefined", "undefined"])),
        "a clone brand symbol is installed on a prototype: {strict}"
    );
    assert_eq!(
        strict.get("callableSymbols"),
        Some(&serde_json::json!([])),
        "a prototype symbol was callable: {strict}"
    );
    let copied_blob = strict
        .get("copiedBlobBrand")
        .expect("copied blob brand probe");
    assert_eq!(
        copied_blob.get("isBlob").and_then(|v| v.as_bool()),
        Some(false),
        "a forged Blob with a copied brand revived: {strict}"
    );
    let strict_clone = strict.get("cloneKey").expect("clone key probe");
    assert_eq!(
        strict_clone.get("ok").and_then(|v| v.as_bool()),
        Some(false),
        "CryptoKey was cloned: {strict}"
    );
    // A genuine File keeps its content, name and type through structuredClone
    // (the serializer must read the instance, not the registry entry).
    let file = strict.get("cloneFile").expect("file clone probe");
    assert_eq!(
        (
            file.get("isFile").and_then(|v| v.as_bool()),
            file.get("name").and_then(|v| v.as_str()),
            file.get("type").and_then(|v| v.as_str()),
            file.get("size").and_then(|v| v.as_u64()),
            file.get("text").and_then(|v| v.as_str()),
        ),
        (
            Some(true),
            Some("a.txt"),
            Some("text/plain"),
            Some(2),
            Some("hi")
        ),
        "a cloned File lost its content: {strict}"
    );
}
