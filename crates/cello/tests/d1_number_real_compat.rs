// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by Ryuz Corp (Cello): D1 number bind type (#250).

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and speaks HTTP over TCP with wall-clock deadlines.
#![allow(clippy::disallowed_methods)]

//! The `d1_number_real` compatibility gate (#250).
//!
//! Workerd's D1 binding stores a JavaScript number as SQLite REAL, so
//! `typeof(?)` is `'real'` and an untyped column keeps a REAL; Cello bound a
//! whole number as INTEGER. From compatibility date 2026-11-24 Cello matches
//! workerd, and an older date (or none) keeps INTEGER so rows a deployment
//! already wrote keep their meaning. The fixture reports the type SQLite
//! recorded beside each value, including what a column's affinity does to it
//! and what the documented migration rewrites.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/d1-number")
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

/// Copy the fixture into `project` and write its `wrangler.json` with the
/// given compatibility date (absent when `None`) and flags.
fn copy_fixture(src: &Path, project: &Path, date: Option<&str>, flags: &[&str]) {
    std::fs::create_dir_all(project).expect("create fixture dir");
    std::fs::copy(src.join("worker.js"), project.join("worker.js")).expect("copy worker");
    let mut config: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(src.join("wrangler.json")).expect("fixture config"),
    )
    .expect("fixture config is JSON");
    match date {
        Some(date) => config["compatibility_date"] = serde_json::json!(date),
        None => {
            config
                .as_object_mut()
                .expect("config object")
                .remove("compatibility_date");
        }
    }
    if flags.is_empty() {
        config
            .as_object_mut()
            .expect("config object")
            .remove("compatibility_flags");
    } else {
        config["compatibility_flags"] = serde_json::json!(flags);
    }
    std::fs::write(
        project.join("wrangler.json"),
        serde_json::to_string_pretty(&config).expect("serialize config"),
    )
    .expect("write config");
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

fn route(addr: &str, path: &str) -> serde_json::Value {
    let (status, body) = get(addr, path).unwrap_or_else(|error| panic!("GET {path}: {error}"));
    assert_eq!(status, 200, "GET {path} answered {status}: {body}");
    serde_json::from_str(&body)
        .unwrap_or_else(|error| panic!("GET {path} is not JSON: {error}: {body}"))
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

fn number(value: &serde_json::Value, path: &[&str]) -> f64 {
    let mut cursor = value;
    for key in path {
        cursor = match key.parse::<usize>() {
            Ok(index) if cursor.is_array() => cursor.get(index).unwrap_or(&serde_json::Value::Null),
            _ => cursor.get(*key).unwrap_or(&serde_json::Value::Null),
        };
    }
    cursor
        .as_f64()
        .unwrap_or_else(|| panic!("{:?} is not a number: {cursor}", path))
}

/// The type a whole-number bind keeps on this deployment: REAL after the
/// gate, INTEGER before it. A fraction is REAL either way.
fn whole(real: bool) -> &'static str {
    if real {
        "real"
    } else {
        "integer"
    }
}

fn assert_binds(answer: &serde_json::Value, real: bool) {
    for (name, expected) in [
        ("zero", 0.0),
        ("one", 1.0),
        ("negOne", -1.0),
        ("two53", 9_007_199_254_740_992.0),
    ] {
        assert_eq!(text(answer, &[name, "ty"]), whole(real), "{name}.ty");
        assert_eq!(number(answer, &[name, "v"]), expected, "{name}.v");
    }
    // A fraction is REAL on both sides of the gate.
    assert_eq!(text(answer, &["frac", "ty"]), "real", "frac.ty");
    assert_eq!(number(answer, &["frac", "v"]), 1.5, "frac.v");
    // A boolean is a number by the time it reaches the engine, and a
    // non-finite number is SQL NULL, as in workerd.
    assert_eq!(
        text(answer, &["boolTrue", "ty"]),
        whole(real),
        "boolTrue.ty"
    );
    assert_eq!(number(answer, &["boolTrue", "v"]), 1.0, "boolTrue.v");
    assert_eq!(
        text(answer, &["boolFalse", "ty"]),
        whole(real),
        "boolFalse.ty"
    );
    assert_eq!(number(answer, &["boolFalse", "v"]), 0.0, "boolFalse.v");
    for name in ["nan", "inf"] {
        assert_eq!(text(answer, &[name, "ty"]), "null", "{name}.ty");
        assert_eq!(text(answer, &[name, "v"]), "null", "{name}.v");
    }
    assert_eq!(text(answer, &["null", "ty"]), "null");
    assert_eq!(text(answer, &["text", "ty"]), "text", "text.ty");
    assert_eq!(text(answer, &["text", "v"]), "1", "text.v");
}

fn assert_affinity(answer: &serde_json::Value, real: bool) {
    // An untyped column keeps the bound type; INTEGER and NUMERIC affinity
    // convert an integral REAL back to INTEGER; REAL affinity accepts both;
    // TEXT affinity renders the value, and a REAL renders `1.0`.
    assert_eq!(text(answer, &["plain_ty"]), whole(real), "plain_ty");
    assert_eq!(text(answer, &["i_ty"]), "integer", "i_ty");
    assert_eq!(text(answer, &["r_ty"]), "real", "r_ty");
    assert_eq!(text(answer, &["t_ty"]), "text", "t_ty");
    assert_eq!(
        text(answer, &["t"]),
        if real { "1.0" } else { "1" },
        "t (TEXT affinity renders the bound type)"
    );
    assert_eq!(text(answer, &["n_ty"]), "integer", "n_ty");
}

fn assert_lookup(answer: &serde_json::Value, real: bool) {
    // A REAL whole number still finds an INTEGER PRIMARY KEY row.
    assert_eq!(text(answer, &["whereInt", "0", "name"]), "b", "whereInt");
    assert_eq!(text(answer, &["whereMiss"]), "[]", "whereMiss");
    assert_eq!(text(answer, &["limit", "0", "id"]), "1", "limit.0");
    assert_eq!(text(answer, &["limit", "1", "id"]), "2", "limit.1");
    assert_eq!(text(answer, &["offset", "0", "id"]), "2", "offset.0");
    assert_eq!(text(answer, &["offset", "1", "id"]), "3", "offset.1");
    // A non-integral LIMIT is a datatype mismatch in SQLite either way.
    assert!(
        text(answer, &["limitFraction", "error"]).contains("SQLITE_MISMATCH"),
        "limitFraction: {}",
        text(answer, &["limitFraction", "error"])
    );
    assert_eq!(text(answer, &["insert", "changes"]), "1", "insert.changes");
    assert_eq!(
        text(answer, &["insert", "last_row_id"]),
        "4",
        "insert.last_row_id"
    );
    assert_eq!(
        text(answer, &["batch", "0", "results", "0", "ty"]),
        whole(real)
    );
    assert_eq!(text(answer, &["batch", "1", "results", "0", "ty"]), "real");
    assert_eq!(text(answer, &["raw", "0", "0"]), "1", "raw value");
    assert_eq!(text(answer, &["raw", "0", "1"]), whole(real), "raw typeof");
    assert_eq!(text(answer, &["firstColumn"]), whole(real), "firstColumn");
    assert_eq!(
        text(answer, &["text", "rendered"]),
        if real { "1.0" } else { "1" },
        "a REAL renders as 1.0"
    );
}

fn boot_with(project: &Path, date: Option<&str>, flags: &[&str]) -> TestNode {
    let src = fixture_dir();
    copy_fixture(&src, project, date, flags);
    TestNode::boot(project)
}

#[test]
fn numbers_bind_real_from_the_compatibility_date() {
    let legacy_project = fixture_tempdir("cello-d1-number-legacy-");
    let legacy = boot_with(legacy_project.path(), Some("2026-11-23"), &[]);
    assert_binds(&route(&legacy.public, "/binds"), false);
    assert_affinity(&route(&legacy.public, "/affinity"), false);
    assert_lookup(&route(&legacy.public, "/lookup"), false);
    drop(legacy);

    let current_project = fixture_tempdir("cello-d1-number-current-");
    let current = boot_with(current_project.path(), Some("2026-11-24"), &[]);
    assert_binds(&route(&current.public, "/binds"), true);
    assert_affinity(&route(&current.public, "/affinity"), true);
    assert_lookup(&route(&current.public, "/lookup"), true);
}

#[test]
fn number_flags_override_the_date() {
    let enable_project = fixture_tempdir("cello-d1-number-enable-");
    let enable = boot_with(
        enable_project.path(),
        Some("2026-11-01"),
        &["d1_number_real"],
    );
    assert_binds(&route(&enable.public, "/binds"), true);
    drop(enable);

    let disable_project = fixture_tempdir("cello-d1-number-disable-");
    let disable = boot_with(
        disable_project.path(),
        Some("2027-01-01"),
        &["no_d1_number_real"],
    );
    assert_binds(&route(&disable.public, "/binds"), false);
}

#[test]
fn absent_compatibility_date_keeps_integer() {
    let project = fixture_tempdir("cello-d1-number-undated-");
    let node = boot_with(project.path(), None, &[]);
    assert_binds(&route(&node.public, "/binds"), false);
}

#[test]
fn rewriting_the_column_before_bumping_keeps_one_type() {
    // The migration in docs/services/d1.md: while the deployment still binds
    // INTEGER, rewrite the existing rows to REAL, then move the date.
    let project = fixture_tempdir("cello-d1-number-migration-");
    {
        let node = boot_with(project.path(), Some("2026-11-23"), &[]);
        let prepared = route(&node.public, "/prepare");
        assert_eq!(
            text(&prepared, &["rows", "0", "ty"]),
            "integer",
            "pre-bump rows"
        );
        assert_eq!(text(&prepared, &["rows", "0", "plain"]), "1");
        assert_eq!(text(&prepared, &["whereOne"]), "1");
        let rewritten = route(&node.public, "/rewrite");
        assert_eq!(
            text(&rewritten, &["rows", "0", "ty"]),
            "real",
            "the rewrite stores REAL"
        );
        assert_eq!(text(&rewritten, &["rows", "1", "ty"]), "real");
        assert_eq!(
            text(&rewritten, &["whereOne"]),
            "1",
            "REAL 1 still equals 1"
        );
        assert_eq!(text(&rewritten, &["whereZero"]), "1");
    }

    // Bump the date on the same state: every row in the column is REAL, and
    // a number predicate finds all of them.
    let bumped = boot_with(project.path(), Some("2026-11-24"), &[]);
    let read = route(&bumped.public, "/read");
    let types: Vec<String> = (0..4)
        .map(|index| text(&read, &["rows", &index.to_string(), "ty"]))
        .collect();
    assert_eq!(types, ["real", "real", "real", "real"], "every row is REAL");
    assert_eq!(
        text(&read, &["whereOne"]),
        "2",
        "the predicate finds both 1 rows"
    );
    assert_eq!(
        text(&read, &["whereZero"]),
        "2",
        "the predicate finds both 0 rows"
    );
}

#[test]
fn contradictory_flags_refuse_startup() {
    let project = fixture_tempdir("cello-d1-number-bad-flags-");
    let src = fixture_dir();
    copy_fixture(
        &src,
        project.path(),
        Some("2026-11-24"),
        &["d1_number_real", "no_d1_number_real"],
    );
    let mut child = cello_command(project.path(), [("NO_COLOR", "1")])
        .arg("dev")
        .arg(project.path())
        .arg("--port")
        .arg("0")
        .arg("--no-watch")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cello dev");
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll child") {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("cello dev booted with contradictory d1 number flags");
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .expect("piped stderr")
        .read_to_string(&mut stderr)
        .expect("read stderr");
    assert!(!status.success(), "startup must fail: {stderr}");
    assert!(
        stderr.contains(
            "Compatibility flags are mutually contradictory: \
             d1_number_real vs no_d1_number_real"
        ),
        "stderr: {stderr}"
    );
}
