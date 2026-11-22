// Modified by Ryuz Corp (Cello): deterministic virtual-clock stream conformance.
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// A white-box driver test, not node code: it builds a Worker in-process,
// runs its turns directly on a paused Tokio clock, and reads the response
// body stream the way the shell does. The execution boundary's
// clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Long stream timelines in virtual time.
//!
//! Every serious stream bug so far was a timing bug — a body cut at the
//! background budget (denoland/celld#275, shipped through five tags), a
//! `TransformStream` that ignored backpressure (upstream celld#105), a
//! tee-of-a-tee truncated under load (upstream celld#111) — and none of them
//! reproduced in a test that could afford a real-time stream. A real 5-minute
//! SSE costs 5 minutes of CI; a real 60-second idle body costs 60.
//!
//! These tests run the real driver (`cello::runtime::drive`) against a real
//! Worker isolate on a `start_paused` Tokio runtime. Every clock in the
//! runtime — the handler and background budgets, response-stream idle expiry,
//! and JavaScript's `setTimeout` — reads [`cello::asyncrt::mono_now`] or a
//! Tokio timer, so the paused clock advances all of them together and five
//! virtual minutes of stream run in milliseconds. The wall-time bound at the
//! end of each scenario is the assertion that time stayed virtual: if a
//! deadline ever goes back to `std::time::Instant` or a bare thread sleep,
//! the scenario hangs or takes real seconds, and fails.

use cello::js::{Compat, HttpResponse, RequestBody, Worker, WorkerConfig, WorkerConfigOptions};
use futures_util::StreamExt as _;
use std::future::Future;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};
use std::time::Duration;
use tokio::time::Instant;

cello::init_v8_before_threads!();

/// The paused clock the whole file shares. `start_paused` freezes it and
/// auto-advances to the next timer once every task is parked, so virtual time
/// only moves when the runtime has nothing to do. A current-thread runtime is
/// required: a multi-thread runtime has no single point at which every task
/// is parked, and Tokio refuses to pause it.
static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("paused test runtime");
    cello::runtime::init_v8();
    cello::asyncrt::set_host_handle(runtime.handle().clone());
    runtime
});

/// One paused runtime carries one clock, so scenarios cannot share it
/// concurrently without one advancing the other's deadlines. libtest runs
/// tests on parallel threads; this serialises the scenarios.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Run one scenario on the paused runtime and assert it cost no real time.
fn in_virtual_time<T>(scenario: impl Future<Output = T>) -> T {
    let _serial = serial();
    // Build the runtime (and initialise V8) before the clock starts: that
    // cost is process setup, not scenario time.
    LazyLock::force(&RUNTIME);
    let started = std::time::Instant::now();
    let out = RUNTIME.block_on(scenario);
    let wall = started.elapsed();
    assert!(
        wall < Duration::from_secs(1),
        "the scenario must run in virtual time: {wall:?} of wall time"
    );
    out
}

fn config(src: &str, script_name: &str) -> Arc<WorkerConfig> {
    Arc::new(WorkerConfig::new(WorkerConfigOptions {
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
    }))
}

fn pool(src: &str, script_name: &str) -> Arc<cello::pool::Pool> {
    let config = config(src, script_name);
    Arc::new(cello::pool::Pool::new(
        cello::runtime::pool_limits(),
        Duration::from_secs(5),
        Box::new(move || Worker::load_config(config.clone())),
    ))
}

type Reply = tokio::sync::oneshot::Receiver<Result<HttpResponse, anyhow::Error>>;

/// Admit one isolate and drive a request on the paused runtime. The
/// affiliation lives as long as the drive, exactly as the shell holds it for
/// the request's whole life.
fn start(pool: &Arc<cello::pool::Pool>, url: &str) -> (tokio::task::JoinHandle<()>, Reply) {
    let pool = pool.clone();
    let url = url.to_string();
    let (reply, receive) = tokio::sync::oneshot::channel();
    let driving = tokio::spawn(async move {
        let affiliation = pool
            .admit_or_wait(false)
            .await
            .expect("admit the test slot");
        let slot = affiliation.slot().clone();
        let job = cello::WorkerJob::Fetch {
            queued_at: std::time::Instant::now(),
            entrypoint: None,
            invocation_limits: None,
            url,
            method: "GET".to_string(),
            body: RequestBody::Bytes(bytes::Bytes::new()),
            headers: Vec::new(),
            request_id: None,
            tail_report: None,
            reply,
        };
        cello::runtime::drive(slot, job, None).await;
        drop(affiliation);
    });
    (driving, receive)
}

/// Take one response head (or a JSON body) from a completed request.
async fn answer(receive: &mut Reply) -> HttpResponse {
    receive
        .await
        .expect("the request answered")
        .expect("a response")
}

/// Let a driven request retire and prove it did, instead of detaching the
/// task: a detached drive keeps polling its isolate (and its timers) into
/// whatever scenario runs next, so one scenario's leftovers would perturb
/// another's clock.
async fn retire(driving: tokio::task::JoinHandle<()>) {
    tokio::time::timeout(Duration::from_secs(30), driving)
        .await
        .expect("the drive retires once its consumer is gone")
        .expect("the drive task joins");
}

/// Read the whole streamed body, returning its bytes and when each chunk
/// arrived on the virtual clock.
async fn drain(stream: &mut cello::js::HttpChunkStream) -> (Vec<u8>, Vec<Instant>) {
    let mut bytes = Vec::new();
    let mut at = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("a healthy body");
        bytes.extend_from_slice(&chunk);
        at.push(Instant::now());
    }
    (bytes, at)
}

/// An SSE body that emits one event every 10 virtual seconds, for as long as
/// its consumer keeps reading.
const SSE: &str = r#"
export default {
  async fetch() {
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    const encoder = new TextEncoder();
    (async () => {
      for (let tick = 1; ; tick++) {
        await new Promise((resolve) => setTimeout(resolve, 10000));
        await writer.write(encoder.encode(`data: ${tick}\n\n`));
      }
    })();
    return new Response(readable, {
      headers: { 'content-type': 'text/event-stream' },
    });
  },
};
"#;

/// A body that writes one chunk and then never produces again, without ever
/// closing. The client never disconnects, so only the idle deadline can end
/// it.
const SILENT: &str = r#"
export default {
  fetch() {
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    (async () => {
      await writer.write(new TextEncoder().encode('first\n'));
    })();
    return new Response(readable);
  },
};
"#;

/// An answered response with `waitUntil` work that keeps waking on a timer
/// and never settles. The budget must end it; nothing else ever will.
const BACKGROUND: &str = r#"
import { waitUntil } from 'cloudflare:workers';

export default {
  fetch() {
    waitUntil((async () => {
      for (;;) {
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    })());
    return new Response('ok');
  },
};
"#;

/// A producer that writes fixed-size chunks into a `TransformStream` whose
/// readable side is the response body, and records how many writes had
/// completed when the consumer had not read for a virtual second.
///
/// `stalled` is the observation: with backpressure a write only completes
/// when the body's consumer pulls, so a stalled producer sits at a couple of
/// accepted chunks; without it every write resolved into the readable's
/// queue and `stalled` reaches the whole body.
const TRANSFORM: &str = r#"
const CHUNKS = 400;
const CHUNK_BYTES = 4096;

export default {
  fetch() {
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    const encoder = new TextEncoder();
    let accepted = 0;
    let stalled = -1;
    setTimeout(() => { stalled = accepted; }, 1000);
    (async () => {
      const chunk = new Uint8Array(CHUNK_BYTES).fill(7);
      for (let i = 0; i < CHUNKS; i++) {
        await writer.write(chunk);
        accepted++;
      }
      await writer.write(encoder.encode(`stalled=${stalled} accepted=${accepted}\n`));
      await writer.close();
    })();
    return new Response(readable);
  },
};
"#;

/// A body cut at the background budget must still deliver every event.
///
/// denoland/celld#275 treated a streaming response body as `waitUntil` work,
/// so past `CELLO_BACKGROUND_BUDGET_S` (30 s, Cloudflare's limit) the driver
/// aborted the pump and the client saw a truncated stream — the bug that
/// shipped through five Cello tags. Five virtual minutes is ten times the
/// budget, so a single expiry anywhere in the window fails the test; a
/// real-time test would need the same five minutes of CI.
#[test]
fn sse_survives_five_virtual_minutes() {
    let pool = pool(SSE, "sse");
    let events = in_virtual_time(async move {
        let (driving, mut receive) = start(&pool, "http://test/sse");
        let response = answer(&mut receive).await;
        assert_eq!(response.status, 200);
        let mut stream = response.stream.expect("a streamed body");
        let opened = Instant::now();
        let mut events: Vec<String> = Vec::new();
        // Five virtual minutes plus a second of slack, so the thirtieth
        // event (at 300 s) is inside the window and a thirty-first would not
        // be missed silently.
        let close = opened + Duration::from_secs(301);
        loop {
            match tokio::time::timeout_at(close, stream.next()).await {
                Err(_) => break,
                Ok(Some(Ok(bytes))) => {
                    events.push(String::from_utf8(bytes).expect("utf-8 event"));
                }
                Ok(other) => panic!(
                    "the streamed body ended after {} events, {other:?}, {:?} into a \
                     five-minute stream",
                    events.len(),
                    Instant::now() - opened
                ),
            }
        }
        assert_eq!(
            Instant::now() - opened,
            Duration::from_secs(301),
            "the scenario covered five virtual minutes"
        );
        // The client goes away: the body pump must notice and the entry
        // retire, so the next scenario starts from a quiet isolate.
        drop(stream);
        retire(driving).await;
        events
    });
    let expected: Vec<String> = (1..=30).map(|tick| format!("data: {tick}\n\n")).collect();
    assert_eq!(
        events, expected,
        "every event of five virtual minutes arrived, in order, past the budget"
    );
}

/// A body with no progress ends at its 60-second idle deadline, and not
/// before.
///
/// The deadline is 60 s of silence (or of a consumer that stopped reading);
/// a live body refreshes it on every chunk. Both halves matter: ending early
/// truncates a slow producer, and never ending pins the isolate. Here the
/// client is connected and healthy, reads the one chunk, and then simply
/// stops: only the idle clock can end the body.
#[test]
fn silent_body_ends_at_its_idle_deadline() {
    let (ended_at, terminal) = in_virtual_time(async {
        let pool = pool(SILENT, "silent");
        let (driving, mut receive) = start(&pool, "http://test/silent");
        let response = answer(&mut receive).await;
        let mut stream = response.stream.expect("a streamed body");
        let first = stream
            .next()
            .await
            .expect("the first chunk")
            .expect("healthy");
        assert_eq!(first, b"first\n");
        let refreshed = Instant::now();
        // The producer is silent from here on. A healthy deadline must not
        // end the body early: one second short of 60 s it is still open.
        let early =
            tokio::time::timeout_at(refreshed + Duration::from_secs(59), stream.next()).await;
        assert!(
            early.is_err(),
            "the body ended 59 s into a 60 s idle window: {early:?}"
        );
        let terminal = tokio::time::timeout_at(refreshed + Duration::from_secs(61), stream.next())
            .await
            .expect("the idle deadline ends the body")
            .expect("a terminal item, not an open body");
        let ended_at = Instant::now() - refreshed;
        drop(stream);
        retire(driving).await;
        (ended_at, terminal)
    });
    assert!(
        ended_at >= Duration::from_secs(60),
        "the body must reach its 60 s deadline, ended at {ended_at:?}"
    );
    assert!(
        ended_at <= Duration::from_secs(60) + Duration::from_millis(100),
        "the body must end at its 60 s deadline, ended at {ended_at:?}"
    );
    let error = terminal.expect_err("an expired body ends with the idle error");
    assert!(
        error.contains("expired after 60 seconds of inactivity"),
        "the terminal error names the idle deadline: {error}"
    );
}

/// Post-response `waitUntil` work ends at the background budget.
///
/// An answered request keeps its isolate for the background work it left
/// behind, and the budget is what lets a retiring generation drain instead
/// of pinning the heap until the process exits. The work here wakes every
/// second and never settles: without the budget the drive would run to the
/// 300 s handler budget. The clock starts when the response is released, so
/// 30 s after the answer (plus the one-second stamp granularity) is the
/// bound.
#[test]
fn background_work_ends_at_its_budget() {
    in_virtual_time(async {
        let pool = pool(BACKGROUND, "background");
        let (driving, mut receive) = start(&pool, "http://test/background");
        let response = answer(&mut receive).await;
        assert_eq!(response.body, b"ok");
        let answered = Instant::now();
        // Alive one second short of the budget: the driver holds the entry
        // for its background work, exactly as the shell would. A timeout
        // here means the drive is still running, which is the assertion.
        let mut driving = driving;
        let early = tokio::time::timeout_at(answered + Duration::from_secs(29), &mut driving).await;
        assert!(
            early.is_err(),
            "the background work ended early, {:?} after the answer",
            Instant::now() - answered
        );
        let ended = tokio::time::timeout_at(answered + Duration::from_secs(32), &mut driving)
            .await
            .expect("the background budget ends the leftover work");
        ended.expect("the drive task joins");
        let ended_at = Instant::now() - answered;
        assert!(
            ended_at >= Duration::from_secs(30),
            "the budget is 30 s, the work ended at {ended_at:?}"
        );
    });
}

/// A `TransformStream` write waits for its consumer (upstream celld#105).
///
/// Its writable side used to resolve `write()` as soon as the transform
/// returned, whether or not anybody read the readable side, so a Worker
/// streaming a response out of a `TransformStream` buffered the whole body
/// in the isolate's heap while the HTTP client did not read. The spec
/// handshake makes the write wait, and a read releases it; both halves are
/// asserted here, through the real response-body pump.
#[test]
fn transform_stream_write_waits_for_its_consumer() {
    let (stalled, accepted, body_bytes) = in_virtual_time(async {
        let pool = pool(TRANSFORM, "transform");
        let (driving, mut receive) = start(&pool, "http://test/transform");
        let response = answer(&mut receive).await;
        let mut stream = response.stream.expect("a streamed body");
        // The consumer reads nothing for a virtual second: the producer must
        // stall instead of filling the isolate's heap.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let (bytes, _) = drain(&mut stream).await;
        drop(stream);
        retire(driving).await;
        let trailer_at = bytes
            .windows(b"stalled=".len())
            .position(|window| window == b"stalled=")
            .expect("the producer's trailer");
        let trailer = std::str::from_utf8(&bytes[trailer_at..])
            .expect("utf-8 trailer")
            .trim();
        let mut fields = trailer.split(' ');
        let stalled: i64 = fields.next().expect("a stalled field")["stalled=".len()..]
            .parse()
            .expect("a stalled count");
        let accepted: usize = fields.next().expect("an accepted field")["accepted=".len()..]
            .parse()
            .expect("an accepted count");
        (stalled, accepted, trailer_at)
    });
    assert_eq!(
        accepted, 400,
        "every write completed once the consumer read"
    );
    assert_eq!(
        body_bytes,
        400 * 4096,
        "every chunk of the 400-chunk body arrived"
    );
    assert!(
        stalled >= 0,
        "the producer wrote the whole body before the consumer read a chunk: nothing ever \
         parked it, so the one-second snapshot never ran (the pre-#105 behaviour)"
    );
    assert!(
        stalled <= 8,
        "a stalled consumer must park the producer near the queue, not accept the body: \
         {stalled} of {accepted} writes completed while nothing read"
    );
}

/// A tee-of-a-tee source whose chunk schedule comes from the request URL.
/// `digest` is FNV-1a over the branch's bytes, so the fixture reports what
/// each branch received rather than comparing branches to each other.
const TEE: &str = r#"
const digest = (bytes) => {
  let hash = 0x811c9dc5;
  for (let i = 0; i < bytes.length; i++) {
    hash ^= bytes[i];
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return `${bytes.length}:${hash.toString(16)}`;
};

const readAll = async (stream) => {
  const reader = stream.getReader();
  const parts = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    parts.push(value);
    total += value.length;
  }
  const all = new Uint8Array(total);
  let at = 0;
  for (const part of parts) {
    all.set(part, at);
    at += part.length;
  }
  return all;
};

// The order the three branches are drained in. A tee-of-a-tee's branches
// buffer differently depending on which one the consumer pulls first, which
// is the interleaving the pump's scheduling sees.
const drainIn = async (order, first, left, right) => {
  if (order === 'first') {
    const a = await readAll(first);
    const b = await readAll(left);
    const c = await readAll(right);
    return [a, b, c];
  }
  if (order === 'last') {
    const b = await readAll(left);
    const c = await readAll(right);
    const a = await readAll(first);
    return [a, b, c];
  }
  return Promise.all([readAll(first), readAll(left), readAll(right)]);
};

export default {
  async fetch(request) {
    const params = new URL(request.url).searchParams;
    const sizes = params.get('chunks').split(',').map(Number);
    const order = params.get('order');
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    (async () => {
      let at = 0;
      for (const size of sizes) {
        const chunk = new Uint8Array(size);
        for (let i = 0; i < size; i++) chunk[i] = (at + i) % 251;
        await writer.write(chunk);
        at += size;
      }
      await writer.close();
    })();
    const [first, second] = readable.tee();
    const [left, right] = second.tee();
    const [a, b, c] = await drainIn(order, first, left, right);
    return Response.json({ a: digest(a), b: digest(b), c: digest(c) });
  },
};
"#;

/// The same FNV-1a digest the tee fixture computes in JavaScript.
fn fnv1a_digest(bytes: &[u8]) -> String {
    let mut hash = 0x811c_9dc5u32;
    for &byte in bytes {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    format!("{}:{hash:x}", bytes.len())
}

/// The digest of the bytes a chunk schedule must produce: byte `i` is
/// `i % 251`, so a dropped, duplicated or reordered byte changes it.
fn schedule_digest(sizes: &[usize]) -> String {
    let total: usize = sizes.iter().sum();
    let bytes: Vec<u8> = (0..total).map(|at| (at % 251) as u8).collect();
    fnv1a_digest(&bytes)
}

/// One worker isolate serves every case: each fetch is a fresh request, and
/// the pool keeps the slot warm between cases.
static TEE_POOL: LazyLock<Arc<cello::pool::Pool>> = LazyLock::new(|| pool(TEE, "tee"));

/// Fetch one scheduled tee-of-a-tee and return each branch's digest.
fn tee_digests(sizes: &[usize], order: &str) -> (String, String, String) {
    let schedule = sizes
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let url = format!("http://test/tee?chunks={schedule}&order={order}");
    in_virtual_time(async {
        let (driving, mut receive) = start(&TEE_POOL, &url);
        let response = answer(&mut receive).await;
        let body: serde_json::Value =
            serde_json::from_slice(&response.body).expect("the fixture answered JSON");
        retire(driving).await;
        (
            body["a"].as_str().expect("branch a").to_string(),
            body["b"].as_str().expect("branch b").to_string(),
            body["c"].as_str().expect("branch c").to_string(),
        )
    })
}

/// The schedule proptest recorded while this property was being proven to
/// fail, pinned in every drain order. Seeds are random, so the historical
/// case needs its own test: one chunk of 1025 bytes, cross-drained.
#[test]
fn recorded_schedule_is_byte_identical() {
    let sizes = [1025usize];
    let want = schedule_digest(&sizes);
    for order in ["lockstep", "first", "last"] {
        let (a, b, c) = tee_digests(&sizes, order);
        assert_eq!(a, want, "branch a, order {order}");
        assert_eq!(b, want, "branch b, order {order}");
        assert_eq!(c, want, "branch c, order {order}");
    }
}

/// Tee branches must deliver byte-identical data under any chunk schedule
/// (upstream celld#111), including a tee of a tee.
///
/// Two things are the runtime's scheduling points a test can perturb
/// deterministically: the chunk sizes (where the pump yields, how much each
/// branch holds when the other catches up) and the order the branches are
/// drained in (which branch is "behind" at each send). Both are chosen by
/// proptest. Each branch's digest is checked against bytes computed in Rust,
/// not against another branch, so a branch that drops, duplicates or
/// reorders bytes fails on its own. proptest prints the failing schedule and
/// its seed, and replays it with that seed.
mod tee_property {
    use super::*;
    use proptest::prelude::*;
    use proptest::test_runner::FileFailurePersistence;

    proptest! {
        // Failures persist beside the crate, not in the workspace root: the
        // default resolves from the source path and a stray directory there
        // becomes a workspace member (`crates/*`) that has no manifest.
        #![proptest_config(ProptestConfig {
            cases: 48,
            failure_persistence: Some(Box::new(FileFailurePersistence::Direct(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/proptest-regressions/tee.txt"
            )))),
            ..ProptestConfig::default()
        })]
        #[test]
        fn tee_of_tee_is_byte_identical(
            sizes in prop::collection::vec(1usize..=4096, 1..=160),
            order in prop::sample::select(vec!["lockstep", "first", "last"]),
        ) {
            let want = schedule_digest(&sizes);
            let (a, b, c) = tee_digests(&sizes, order);
            prop_assert_eq!(&a, &want, "branch a of {}", sizes.len());
            prop_assert_eq!(&b, &want, "branch b of {}", sizes.len());
            prop_assert_eq!(&c, &want, "branch c of {}", sizes.len());
        }
    }
}
