---
title: Limitations
description: "The operational limits of a Cello fleet: one application, external TLS, credentials, and reconnects."
---

# Limitations

Cello v0.6.3 is a beta release with these operational limits. See
[Cloudflare compatibility](cloudflare-compat.md) for the supported
services, APIs, and Wrangler configuration.

## Run one application per fleet

- A fleet runs one application. Cello has no account service,
  multi-tenant scheduler, or managed ingress.
- A fleet stores its durable state in an S3-compatible bucket, a Google
  Cloud Storage bucket, or an Azure Blob Storage container. Only
  `cello dev` uses the local SQLite object store.
- Ownership balancing counts cells by node weight, not by CPU or memory
  use. It moves only hibernated cells, so a fleet without idle eviction
  balances only the cells that hibernate on their own.

## Terminate TLS outside Cello

- Cello does not terminate TLS. Terminate public TLS at an ingress proxy,
  and put the internal listener on a private network or an encrypted
  overlay.
- Peer traffic is plaintext HTTP. The fleet HMAC authenticates tunnel
  establishment and control requests but does not encrypt data, so your
  network must provide confidentiality.
- The fleet bucket controls the fleet. Give its credentials access to one
  fleet only. See [Security](../guides/security.md).

## Match credentials to your provider

- The credential methods differ by provider. See
  [Configure storage](../guides/storage.md).
- Azure identity works only in the public Azure cloud. A managed identity
  from Azure App Service or Azure Container Apps does not work. Use a
  workload identity or a storage account key there.

## Reconnect WebSockets after a move

- An outbound Durable Object WebSocket keeps its cell resident. It closes
  when the cell moves to another node, so store the connection intent and
  reconnect.
- A node limits the resident cells and outbound WebSockets.

## Stream a response for as long as the client listens

- A streaming response body is response work, not background work.
  `CELLO_BACKGROUND_BUDGET_S` ends `ctx.waitUntil()` work after the handler
  answers, but never a body the client is still reading. The body runs while
  the client stays connected and the body keeps producing. While a body
  streams, the budget ends the `waitUntil` claim; timers that work already
  started stop one budget window after the body ends.
- An open response still ends: when the body completes or errors, when the
  client disconnects (which cancels the producer), after `CELLO_HANDLER_BUDGET_S`
  if the handler has not answered, when the node shuts down, and after
  `CELLO_HTTP_STREAM_IDLE_S` (60 s by default) without progress: the body
  produces nothing, or the client stops reading.
- A Worker `fetch()` is bounded the same way, and not by a total deadline:
  `CELLO_FETCH_TIMEOUT_S` bounds the connect, a streamed request body, and
  the wait for the response head (both knobs accept 1–86400 seconds). The
  response body then streams for as long as the upstream keeps sending — an
  SSE feed or a proxied download is never truncated because it outlived that
  budget. A body the Worker reads is ended when no bytes arrive for
  `CELLO_HTTP_STREAM_IDLE_S` (every received chunk starts a fresh window), so
  a silent or black-holed upstream still ends. That idle timer runs only while
  the host is polling the body: a passthrough fetch body that the client stops
  reading is bounded by the response-body rule above, not by this one.
- A streamed body keeps its event, isolate, and cell resident until it ends,
  so a retiring generation waits for open streams before it frees its heaps.
- A `TransformStream` applies backpressure: while nobody reads the readable
  side, `await writer.write(...)` stays pending and `writer.ready` does not
  settle, so a response body built from one stops producing when the HTTP
  client stops reading instead of buffering in the isolate's heap. A
  cancelled readable, an aborted or errored writable, and
  `controller.error()` / `controller.terminate()` release a parked write
  instead of leaving it pending forever. A
  producer that writes without awaiting either, or that writes after
  `writer.desiredSize` says 0, still queues its chunks in memory — Cello
  cannot bound a stream you keep feeding.
- A `tee()` bounds the skew between its branches: the difference between
  what the two branches hold is measured just after a chunk is queued, and
  past 32 MiB — the budget the host tee of a platform body uses — the
  branch that is behind fails its next read with `tee branch fell more than
  32 MiB behind its sibling; read both branches concurrently`, while the
  branch being read keeps flowing to the end. The chunk size alone fails no
  branch, but a branch that has not read an oversize chunk yet holds all of
  it and fails for lag, so with chunks larger than the budget both branches
  must be read concurrently. A chunk is charged its size — a string 2 bytes
  per UTF-16 code unit, a buffer its length, a view its visible length and
  not the buffer behind it — and every chunk at least 64 bytes. That makes
  the budget a guard against accidental buffering rather than an isolate
  memory limit: a payload hidden in an object chunk, or a small view over a
  large buffer, is not counted. The budget is per `tee()`, so a chain of N
  clones can hold N × 32 MiB. Read both branches, or cancel the one you do
  not need.
- An isolate holds at most 128 live `CompressionStream`s and
  `DecompressionStream`s together. Constructing one more throws a
  `RangeError`. A stream stops counting once it finishes, is cancelled or
  aborted, errors, or is garbage-collected after it becomes unreachable.

## Cap memory per isolate

- An isolate holds at most `CELLO_V8_EXTERNAL_LIMIT_MB` of `ArrayBuffer`
  memory: four times its V8 heap limit by default, 512 MiB with the default
  128 MiB heap. The budget covers the backing stores V8 allocates through the
  isolate's allocator — plain `ArrayBuffer`s and typed arrays,
  `SharedArrayBuffer`s without `maxByteLength`, every `structuredClone` of
  one, and the buffers the host hands to JavaScript (response bodies,
  WebSocket messages, RPC arguments, Queue message bodies, `/tmp` reads, D1
  blobs).
- **A resizable `ArrayBuffer`, a growable `SharedArrayBuffer` and
  `WebAssembly.Memory` are not capped by this budget.** V8 backs them with
  the platform page allocator, which is outside every embedder hook: no
  allocator can refuse their construction or a `resize`/`grow`, and wasm
  memory is not even counted as external memory. They count against nothing
  here, and crossing the cap with them does not condemn the isolate. Each is
  still bounded on its own: `--wasm-max-mem-pages` fails every `grow` past the
  isolate's whole budget in pages (8192 pages of 64 KiB, 512 MiB, by default;
  clamped to V8's own 65536 pages, 4 GiB, for a larger budget), including a
  grow performed by wasm code; a declared `maximum` above that is
  accepted and simply cannot be reached. A resizable or growable buffer is
  limited only by V8's own `maxByteLength` range and the node's
  `CELLO_MAX_RSS_MB` pressure shedding.
  What bounds the *number* of such buffers is the same node pressure
  shedding, so do not rely on this budget against a Worker that loops
  `new ArrayBuffer(8 << 10, { maxByteLength: 1 << 30 })`. V8 does count a
  resized `ArrayBuffer` in `external_memory` (measured: +32 MiB; a growable
  `SharedArrayBuffer` and a wasm memory are not counted at all), so V8's own
  heap heuristics — including the heap limit's condemnation — can still react
  to one; the budget is not what reacts. A page-allocator hook in the V8
  binding is the change that would cover them; until then the gap is stated
  here rather than claimed.
- Past the cap an allocator-backed allocation is refused, and the failure
  reaches the guest as a `RangeError` whose message is V8's
  `Array buffer allocation failed`. A refused host store throws
  `Array buffer allocation failed: the isolate's ArrayBuffer memory budget is
  exhausted (..., see CELLO_V8_EXTERNAL_LIMIT_MB); free buffers before
  allocating more`. A `structuredClone` of a plain buffer that the allocator
  refuses fails inside V8's deserializer with `Unable to deserialize cloned
  data`.
- A refusal condemns the isolate the way a heap-limit hit does: it takes on
  no new hibernatable socket and no new SQL result set materializes, and it
  serves again once a collection finds both the heap and the budget under 75
  percent of their limits. Two kinds of refusal count as evidence: a refused
  host store (a response body, a WebSocket message, a `/tmp` read, a D1
  blob), and a plain allocation larger than the whole budget, which no
  collection can make room for. A smaller allocation that V8 collects and
  retries successfully never reaches the guest, so it does not count. A turn
  that begins with retained buffers over 75 percent of the budget is
  condemned too, after a rate-limited collection — so an isolate that merely
  asked for more than was free once, and dropped it, is not condemned. The
  isolate is not retired for memory it holds: the cell keeps serving on it at
  reduced admission, exactly as it does after a heap-limit hit.
- A refused host store is never presented as a partial value. A stream chunk
  that does not fit rejects the `read()` promise with the budget's
  `RangeError`; a host buffer for a synchronous host call throws at that
  call; a row whose BLOB does not fit throws one `RangeError` and its SQL
  cursor is closed, so the refused statement is not retained on the cell's
  connection. When the refused row came from a write statement
  (`INSERT`/`UPDATE`/`DELETE ... RETURNING`), the write was already applied
  before that row existed and closing the cursor does not undo it, so the
  message says so explicitly ("the statement had already executed ... do not
  retry it") — retrying after freeing buffers would apply it twice.
- Bytes come back when the collector frees the object: V8 frees a backing
  store through the allocator's `free`, which returns the bytes to the
  budget. Churn is therefore never refused — an allocate-and-drop loop is
  answered by a collection and a retry — however much it allocates in total.
- The limit covers the whole isolate, not one request. A `tee()` chain, a
  clone, a WebSocket message and a `.arrayBuffer()` call share it, so a
  handler that holds several 32 MiB payloads at once can reach it while
  every payload on its own is well under it. An isolate at the limit cannot
  allocate at all, including the buffer a response body is built from; free
  what you no longer need before you allocate more.
- The budget is per isolate, not per node: `CELLO_MAX_CELLS_PER_ISOLATE`
  cells share one, and a node runs as many isolates as its pools allow. Size
  it against `CELLO_MAX_RSS_MB` and the isolate count, not against the
  fleet's memory.

## Run on Linux or Apple Silicon

The installer supplies binaries for Linux x86-64, Linux ARM64, and Apple
Silicon. Windows is not supported.
