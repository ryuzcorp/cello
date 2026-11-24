// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by Ryuz Corp (Cello): released-writer and compression-stream
// close-after-error conformance (#193, #223).

// A white-box driver test, not node code: it builds a Worker in-process and
// drives one turn per request, so the execution boundary's injected
// clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Two workerd divergences measured on 1.20261121.1, both in the stream
//! globals. Nothing here needs an op: every route settles in microtasks, so
//! one turn is the whole request.
//!
//! - A released `WritableStreamDefaultWriter` (#193): workerd throws
//!   `TypeError: This WritableStream writer has been released.` from
//!   `desiredSize` in every stream state, and rejects `write`, `close` and
//!   `abort`. Releasing installs a fresh rejected `ready`, so a read after
//!   the release always reports the released error, while a handler attached
//!   before it keeps the old promise's settlement: still resolved, or still
//!   the stream's error. `closed` keeps its settlement once it has one.
//!   Cello reported the stream's size instead of throwing, and left `ready`
//!   carrying the stream's error after the release.
//! - `CompressionStream`/`DecompressionStream` after a failed write (#223):
//!   workerd's compression streams are internal streams, so the failure
//!   closes the writable side instead of erroring it. A later `close()` and
//!   `write()` report `TypeError: This WritableStream has been closed.`,
//!   `desiredSize` is 0 and `abort()` resolves, while `writer.closed`
//!   rejects with the failure and `writer.ready` resolves. Cello replayed
//!   the stored error from `close()`.
//!
//! The `scripts/workerd-diff` gate pins the same transcripts against a real
//! workerd (`writer-released`, `decompression-close-after-error`).

use cello::js::{Compat, RequestBody, Worker, WorkerConfig, WorkerConfigOptions};
use serde_json::Value;
use std::sync::OnceLock;
use std::time::Instant;

// V8's protection-key grant must land on the process's initial thread, before
// libtest creates a test thread; see `cello::init_v8_before_threads`.
cello::init_v8_before_threads!();

/// Install the asyncrt host handle once per test binary, on a leaked
/// current-thread runtime. V8 is already initialised by the process
/// constructor (see `cello::init_v8_before_threads`); the `init_v8` call
/// stays as an idempotent statement of the dependency.
fn ensure_engine() {
    static INIT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    INIT.get_or_init(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        cello::runtime::init_v8();
        cello::asyncrt::set_host_handle(runtime.handle().clone());
        runtime
    });
}

/// Load one module worker from `src` for a direct turn-driving test.
fn load_worker(src: &str, script_name: &str) -> Worker {
    let config = WorkerConfig::new(WorkerConfigOptions {
        src: src.to_string(),
        script_name: script_name.to_string(),
        do_classes: Vec::new(),
        bindings: Vec::new(),
        r2_bindings: Vec::new(),
        d1_bindings: Vec::new(),
        kv_bindings: Vec::new(),
        queue_bindings: Vec::new(),
        queue_consumers: Vec::new(),
        workflow_bindings: Vec::new(),
        vars: Vec::new(),
        node: String::new(),
        modules: Vec::new(),
        compat: Compat::default(),
    });
    Worker::load_config(std::sync::Arc::new(config)).expect("load worker")
}

/// Run one request's first turn to its sync answer and retire it, returning
/// the status and body. Every route answers without suspending, so one turn
/// is the whole request.
fn turn_once(worker: &mut Worker, url: &str) -> (u16, Vec<u8>) {
    let (reply, mut receive) = tokio::sync::oneshot::channel();
    let job = cello::WorkerJob::Fetch {
        queued_at: Instant::now(),
        entrypoint: None,
        invocation_limits: None,
        url: url.to_string(),
        method: "GET".to_string(),
        body: RequestBody::Bytes(bytes::Bytes::new()),
        headers: Vec::new(),
        request_id: None,
        tail_report: None,
        reply,
    };
    let (entry, started) = worker.turn_begin(job, None);
    drop(started);
    let mut entry = entry.expect("request starts");
    entry.abandon();
    drop(entry);
    let response = receive.try_recv().expect("sync answer").expect("ok answer");
    (response.status, response.body)
}

const WORKER: &str = r#"
const failures = [];
const check = (name, condition, detail) => {
  if (!condition) failures.push(`${name}: ${detail}`);
};
const errorOf = (error) => ({
  name: error && error.name ? error.name : String(error),
  message: error && error.message ? error.message : String(error),
});
const settles = (promise) => promise.then(() => "ok", errorOf);
// A released writer's desiredSize is a sync throw, so it cannot go through
// settles().
const desiredSizeOf = (writer) => {
  try {
    return `ok ${writer.desiredSize}`;
  } catch (error) {
    return errorOf(error);
  }
};
const isTypeError = (result, message) =>
  result && typeof result === "object" &&
  result.name === "TypeError" && result.message === message;
const RELEASED = "This WritableStream writer has been released.";
const CLOSED = "This WritableStream has been closed.";

async function writerReleased() {
  const flip = async () => {
    for (let i = 0; i < 16; i++) await null;
  };

  // Nothing pending: ready resolved at construction. A handler attached
  // before the release keeps the resolved promise; the reader the issue's
  // repro uses reads writer.ready again after the release, which workerd
  // answers with a fresh released rejection.
  const idleStream = new WritableStream({}, { highWaterMark: 2 });
  const idleWriter = idleStream.getWriter();
  const idleReadyBefore = settles(idleWriter.ready);
  idleWriter.releaseLock();
  const idleReady = settles(idleWriter.ready);
  const idleClosed = settles(idleWriter.closed);
  const idleDesired = desiredSizeOf(idleWriter);
  check("idle desiredSize throws", isTypeError(idleDesired, RELEASED), `${JSON.stringify(idleDesired)}`);
  check("idle ready keeps the pre-release settlement", (await idleReadyBefore) === "ok", "the pre-release read changed");
  check("idle ready read after the release rejects released", isTypeError(await idleReady, RELEASED), "ready did not reject with the released error");
  check("idle closed rejects released", isTypeError(await idleClosed, RELEASED), "closed did not reject with the released error");
  for (const [name, result] of [
    ["write", await settles(idleWriter.write("x"))],
    ["close", await settles(idleWriter.close())],
    ["abort", await settles(idleWriter.abort("why"))],
  ]) {
    check(`idle ${name} rejects released`, isTypeError(result, RELEASED), `${JSON.stringify(result)}`);
  }
  idleWriter.releaseLock();
  check("second releaseLock is a no-op", idleStream.locked === false, "stream stayed locked");
  const reused = idleStream.getWriter();
  check("the released stream can be re-locked", reused.desiredSize === 2, `desiredSize is ${reused.desiredSize}`);

  // Ready still pending at release: backpressure holds the write.
  let releaseWrite;
  const busyStream = new WritableStream(
    { write: () => new Promise((resolve) => (releaseWrite = resolve)) },
    { highWaterMark: 0 });
  const busyWriter = busyStream.getWriter();
  const busyWrite = settles(busyWriter.write("a"));
  await flip();
  const busyReady = settles(busyWriter.ready);
  const busyClosed = settles(busyWriter.closed);
  busyWriter.releaseLock();
  const busyDesired = desiredSizeOf(busyWriter);
  check("pending desiredSize throws", isTypeError(busyDesired, RELEASED), `${JSON.stringify(busyDesired)}`);
  check("pending ready rejects released", isTypeError(await busyReady, RELEASED), "ready did not reject with the released error");
  check("pending closed rejects released", isTypeError(await busyClosed, RELEASED), "closed did not reject with the released error");
  releaseWrite();
  check("the in-flight write still completes", (await busyWrite) === "ok", "the in-flight write rejected");
  check("release unlocks the stream", busyStream.locked === false, "stream stayed locked");

  // Released after the stream closed: closed keeps resolving, ready is the
  // fresh released rejection.
  const doneStream = new WritableStream({});
  const doneWriter = doneStream.getWriter();
  await doneWriter.close();
  const doneReadyBefore = settles(doneWriter.ready);
  const doneClosedBefore = settles(doneWriter.closed);
  doneWriter.releaseLock();
  const doneReady = settles(doneWriter.ready);
  const doneClosed = settles(doneWriter.closed);
  const doneDesired = desiredSizeOf(doneWriter);
  check("closed-stream desiredSize throws", isTypeError(doneDesired, RELEASED), `${JSON.stringify(doneDesired)}`);
  check("closed-stream pre-release ready stays resolved", (await doneReadyBefore) === "ok", "ready rejected");
  check("closed-stream pre-release closed stays resolved", (await doneClosedBefore) === "ok", "closed rejected");
  check("closed-stream ready read after the release rejects released", isTypeError(await doneReady, RELEASED), "ready did not reject with the released error");
  check("closed-stream closed stays resolved", (await doneClosed) === "ok", "closed rejected");

  // Released after the stream errored: closed keeps the stream's error, the
  // pre-release ready read keeps it too, and a later ready read is the
  // released rejection.
  const badStream = new WritableStream({ write() { throw new TypeError("boom"); } });
  const badWriter = badStream.getWriter();
  await settles(badWriter.write("a"));
  const badReadyBefore = settles(badWriter.ready);
  badWriter.releaseLock();
  const badReady = settles(badWriter.ready);
  const badClosed = settles(badWriter.closed);
  const badDesired = desiredSizeOf(badWriter);
  check("errored-stream desiredSize throws", isTypeError(badDesired, RELEASED), `${JSON.stringify(badDesired)}`);
  check("errored-stream pre-release ready keeps the error", isTypeError(await badReadyBefore, "boom"), "the pre-release read changed");
  check("errored-stream ready read after the release rejects released", isTypeError(await badReady, RELEASED), "ready changed on release");
  check("errored-stream closed keeps the error", isTypeError(await badClosed, "boom"), "closed changed on release");
}

async function decompressionCloseAfterError() {
  const stream = new DecompressionStream("gzip");
  const writer = stream.writable.getWriter();
  const ready = settles(writer.ready);
  const closed = settles(writer.closed);
  const read = new Response(stream.readable).arrayBuffer()
    .then(() => "ok", errorOf);
  const write = await settles(writer.write(new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])));
  check("write rejects decompression failed", isTypeError(write, "Decompression failed."), `${JSON.stringify(write)}`);
  check("readable rejects decompression failed", isTypeError(await read, "Decompression failed."), "readable did not error");
  const close = await settles(writer.close());
  check("close reports the closed stream", isTypeError(close, CLOSED), `${JSON.stringify(close)}`);
  const closeAgain = await settles(writer.close());
  check("second close reports the closed stream", isTypeError(closeAgain, CLOSED), `${JSON.stringify(closeAgain)}`);
  const writeAgain = await settles(writer.write(new Uint8Array([1])));
  check("write after the failure reports the closed stream", isTypeError(writeAgain, CLOSED), `${JSON.stringify(writeAgain)}`);
  const desired = desiredSizeOf(writer);
  check("desiredSize is 0", desired === "ok 0", desired);
  check("ready stays resolved", (await ready) === "ok", "ready rejected");
  check("closed rejects with the failure", isTypeError(await closed, "Decompression failed."), "closed did not reject with the failure");
  check("abort resolves", (await settles(writer.abort("stop"))) === "ok", "abort rejected");
  check("the writable stays locked", stream.writable.locked === true, "writable unlocked itself");

  // CompressionStream shares the internal-stream writable: a clean close
  // still resolves, and only the second close is the closed-stream error.
  const compress = new CompressionStream("gzip");
  const compressWriter = compress.writable.getWriter();
  const compressed = new Response(compress.readable).arrayBuffer()
    .then((buffer) => buffer.byteLength);
  check("compression write resolves", (await settles(compressWriter.write(new Uint8Array([1, 2, 3])))) === "ok", "write rejected");
  check("compression close resolves", (await settles(compressWriter.close())) === "ok", "close rejected");
  const compressCloseAgain = await settles(compressWriter.close());
  check("compression second close reports the closed stream", isTypeError(compressCloseAgain, CLOSED), `${JSON.stringify(compressCloseAgain)}`);
  check("compression round trip decodes", (await compressed) === 23, "unexpected compressed length");
}

// A readable cancel followed by an abort with the write still in flight: the
// abort is recorded while the stream is already erroring, so it stores no
// reason of its own. Settling the write must not overwrite the stored error
// with `undefined` (#230 review; workerd answers the abort reason, base Cello
// and this patch answer the cancel reason, and neither ever loses it).
const rejection = (promise) =>
  promise.then(
    () => "resolved",
    (error) =>
      error === undefined
        ? "undefined"
        : typeof error === "string"
          ? `string:${error}`
          : errorOf(error).message,
  );

async function compressionCancelThenAbort() {
  const stream = new CompressionStream("gzip");
  const writer = stream.writable.getWriter();
  const reader = stream.readable.getReader();
  reader.read().catch(() => {});
  await null;
  const write = settles(writer.write(new TextEncoder().encode("payload")));
  const cancel = settles(reader.cancel("cancel-reason"));
  const abort = rejection(writer.abort("abort-reason"));
  const closed = rejection(writer.closed);
  check("the parked write resolves", (await write) === "ok", "write failed");
  check("the cancel resolves", (await cancel) === "ok", "cancel failed");
  const closedResult = await closed;
  check("closed keeps the stored error", closedResult === "string:cancel-reason", `closed: ${closedResult}`);
  const abortResult = await abort;
  check("abort keeps the stored error", abortResult === "string:cancel-reason", `abort: ${abortResult}`);
}

export default {
  async fetch(request) {
    failures.length = 0;
    const { pathname } = new URL(request.url);
    if (pathname === "/writer-released") await writerReleased();
    else if (pathname === "/decompression-close-after-error") await decompressionCloseAfterError();
    else if (pathname === "/compression-cancel-then-abort") await compressionCancelThenAbort();
    else return Response.json({ ok: false, failures: [`no route ${pathname}`] }, { status: 404 });
    return Response.json({ ok: failures.length === 0, failures });
  },
};
"#;

fn check_route(worker: &mut Worker, path: &str) {
    let (status, body) = turn_once(worker, &format!("http://test{path}"));
    assert_eq!(status, 200, "{path}: {}", String::from_utf8_lossy(&body));
    let report: Value = serde_json::from_slice(&body).unwrap_or_else(|error| {
        panic!(
            "{path}: JSON body ({error}): {}",
            String::from_utf8_lossy(&body)
        )
    });
    assert_eq!(report["ok"], true, "{path}: {report}");
}

#[test]
fn released_writer_matches_workerd() {
    ensure_engine();
    let mut worker = load_worker(WORKER, "stream-edges");
    check_route(&mut worker, "/writer-released");
}

#[test]
fn decompression_close_after_error_matches_workerd() {
    ensure_engine();
    let mut worker = load_worker(WORKER, "stream-edges");
    check_route(&mut worker, "/decompression-close-after-error");
}

#[test]
fn compression_cancel_then_abort_keeps_the_stored_error() {
    ensure_engine();
    let mut worker = load_worker(WORKER, "stream-edges");
    check_route(&mut worker, "/compression-cancel-then-abort");
}
