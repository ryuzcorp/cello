// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by Ryuz Corp (Cello): Durable Object SQL boolean bind type (#154).

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and speaks HTTP over TCP with wall-clock deadlines.
#![allow(clippy::disallowed_methods)]

//! The `sql_boolean_text` compatibility gate (#154).
//!
//! Workerd stores a `true`/`false` bind into Durable Object SQL as the TEXT
//! `'true'`/`'false'`; Cello stored INTEGER `1`/`0` since its first DO SQL
//! release. From compatibility date 2026-11-24 Cello matches workerd, and an
//! older date (or none) keeps INTEGER so rows a deployment already stores
//! keep their meaning. Each route reports the type, the value, and what a
//! boolean predicate and `if (row.flag)` do with it.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/do-sql-boolean")
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

/// One `/binds` answer, checked against the type that deployment writes.
fn assert_binds(answer: &serde_json::Value, text_type: bool) {
    let (ty, true_value, false_value) = if text_type {
        ("text", "true", "false")
    } else {
        ("integer", "1", "0")
    };
    // A bind's own type, with no table in the way.
    assert_eq!(text(answer, &["selectTrue", "ty"]), ty, "selectTrue.ty");
    assert_eq!(
        text(answer, &["selectTrue", "v"]),
        true_value,
        "selectTrue.v"
    );
    assert_eq!(text(answer, &["selectFalse", "ty"]), ty, "selectFalse.ty");
    assert_eq!(
        text(answer, &["selectFalse", "v"]),
        false_value,
        "selectFalse.v"
    );
    // The column keeps the bind's type, and only that representation.
    assert_eq!(text(answer, &["rows", "0", "ty"]), ty, "rows.0.ty");
    assert_eq!(
        text(answer, &["rows", "0", "flag"]),
        true_value,
        "rows.0.flag"
    );
    assert_eq!(text(answer, &["rows", "1", "ty"]), ty, "rows.1.ty");
    assert_eq!(
        text(answer, &["rows", "1", "flag"]),
        false_value,
        "rows.1.flag"
    );
    // A boolean predicate matches the rows a boolean bind wrote, and the
    // other representation matches none of them.
    assert_eq!(text(answer, &["whereTrue"]), "1", "whereTrue");
    assert_eq!(text(answer, &["whereFalse"]), "1", "whereFalse");
    assert_eq!(
        text(answer, &["whereInt"]),
        if text_type { "0" } else { "1" },
        "whereInt"
    );
    assert_eq!(
        text(answer, &["whereText"]),
        if text_type { "1" } else { "0" },
        "whereText"
    );
    assert_eq!(
        text(answer, &["compare", "eqInt"]),
        if text_type { "0" } else { "1" },
        "compare.eqInt"
    );
    assert_eq!(
        text(answer, &["compare", "eqText"]),
        if text_type { "1" } else { "0" },
        "compare.eqText"
    );
    assert_eq!(text(answer, &["compare", "eqSelf"]), "0", "compare.eqSelf");
    // `if (row.flag)` reads the stored value directly: an INTEGER 0 is falsy,
    // the TEXT "false" is truthy.
    assert_eq!(text(answer, &["truthy", "0"]), "truthy", "truthy.0");
    assert_eq!(
        text(answer, &["truthy", "1"]),
        if text_type { "truthy" } else { "falsy" },
        "truthy.1"
    );
}

fn boot_with(project: &Path, date: Option<&str>, flags: &[&str]) -> TestNode {
    let src = fixture_dir();
    copy_fixture(&src, project, date, flags);
    TestNode::boot(project)
}

#[test]
fn boolean_binds_follow_the_compatibility_date() {
    // The gate's date and the day before it, on separate deployments.
    let legacy_project = fixture_tempdir("cello-do-sql-bool-legacy-");
    let legacy = boot_with(legacy_project.path(), Some("2026-11-23"), &[]);
    assert_binds(&route(&legacy.public, "/binds"), false);
    drop(legacy);

    let current_project = fixture_tempdir("cello-do-sql-bool-current-");
    let current = boot_with(current_project.path(), Some("2026-11-24"), &[]);
    assert_binds(&route(&current.public, "/binds"), true);
}

#[test]
fn boolean_flags_override_the_date() {
    // An explicit enable at an old date writes workerd's TEXT...
    let enable_project = fixture_tempdir("cello-do-sql-bool-enable-");
    let enable = boot_with(
        enable_project.path(),
        Some("2026-11-01"),
        &["sql_boolean_text"],
    );
    assert_binds(&route(&enable.public, "/binds"), true);
    drop(enable);

    // ...and the opt-out at a date past the gate keeps INTEGER.
    let disable_project = fixture_tempdir("cello-do-sql-bool-disable-");
    let disable = boot_with(
        disable_project.path(),
        Some("2027-01-01"),
        &["no_sql_boolean_text"],
    );
    assert_binds(&route(&disable.public, "/binds"), false);
}

#[test]
fn absent_compatibility_date_keeps_integer() {
    // No date means no version of the type was declared, so the deployment
    // keeps writing what it has always written.
    let project = fixture_tempdir("cello-do-sql-bool-undated-");
    let node = boot_with(project.path(), None, &[]);
    assert_binds(&route(&node.public, "/binds"), false);
}

#[test]
fn rewriting_the_column_before_bumping_keeps_one_type() {
    // The migration in docs/services/durable-objects.md: while the
    // deployment still writes INTEGER, rewrite the existing rows to TEXT,
    // then move the compatibility date.
    let project = fixture_tempdir("cello-do-sql-bool-migration-");
    {
        let node = boot_with(project.path(), Some("2026-11-23"), &[]);
        let rows = route(&node.public, "/prepare");
        assert_eq!(
            text(&rows, &["rows", "0", "ty"]),
            "integer",
            "pre-bump rows are INTEGER"
        );
        assert_eq!(text(&rows, &["rows", "0", "flag"]), "1");
        assert_eq!(text(&rows, &["rows", "1", "flag"]), "0");
        let rewritten = route(&node.public, "/rewrite");
        assert_eq!(
            text(&rewritten, &["after", "0", "ty"]),
            "text",
            "the rewrite stores TEXT"
        );
        assert_eq!(text(&rewritten, &["after", "0", "flag"]), "true");
        assert_eq!(text(&rewritten, &["after", "1", "ty"]), "text");
        assert_eq!(text(&rewritten, &["after", "1", "flag"]), "false");
    };

    // Bump the date on the same state, then write booleans again: every row
    // in the column is TEXT, and a boolean predicate finds all of them.
    let bumped = boot_with(project.path(), Some("2026-11-24"), &[]);
    let read = route(&bumped.public, "/read");
    let types: Vec<String> = (0..4)
        .map(|index| text(&read, &["rows", &index.to_string(), "ty"]))
        .collect();
    assert_eq!(types, ["text", "text", "text", "text"], "every row is TEXT");
    assert_eq!(
        text(&read, &["whereTrue"]),
        "2",
        "the boolean predicate finds both true rows"
    );
    assert_eq!(
        text(&read, &["whereFalse"]),
        "2",
        "the boolean predicate finds both false rows"
    );
    assert_eq!(
        text(&read, &["whereInt"]),
        "0",
        "no INTEGER row is left behind"
    );
}

#[test]
fn contradictory_flags_refuse_startup() {
    let project = fixture_tempdir("cello-do-sql-bool-bad-flags-");
    let src = fixture_dir();
    copy_fixture(
        &src,
        project.path(),
        Some("2026-11-24"),
        &["sql_boolean_text", "no_sql_boolean_text"],
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
            panic!("cello dev booted with contradictory sql boolean flags");
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
             sql_boolean_text vs no_sql_boolean_text"
        ),
        "stderr: {stderr}"
    );
}
