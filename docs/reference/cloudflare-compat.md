---
title: Cloudflare compatibility
description: Which Cloudflare services and runtime APIs Cello supports, and how each difference behaves.
---
<!-- Modified by Ryuz Corp (Cello): per-isolate ArrayBuffer memory cap; node:crypto scrypt; cloned streaming bodies; cross-isolate RPC stubs and streams; per-node Workers Cache API; receiver-held RPC streams; AbortSignal.timeout is unreferenced (upstream #306); loader 101 webSocket target (upstream #305); loader sibling cloudflare:workers imports (upstream #304); AES-KW and symmetric JWK (upstream #295); EC raw point format (upstream #271); writable request /tmp, node:url file helpers, and the CELLO_TMP_MAX_BYTES /tmp cap (upstream #280); workerd close codes (upstream #279); fetch gzip/deflate decoding (upstream #238); Symbol.dispose on RPC results (upstream #260); post-response DO dial (upstream #254); synchronous caller cancel (upstream #281); workerd process identity and Buffer shape, iterator helper flags, and reportError's describer. -->

This page lists only the differences from the linked Cloudflare APIs. An
entry without a note matches Cloudflare.

- **Yes**: implemented, except for the listed differences.
- **Partial**: a substantial part is unavailable.
- **Experimental**: can change without notice.
- **No**: not implemented.

Cello rejects an unsupported configuration or API at deployment or first
use. An unsupported feature that stays silent is a defect.

## Services

| Service | Status | Differences |
| --- | --- | --- |
| [Workers](../services/workers.md) | **Yes** | No custom domains or TLS termination; `passThroughOnException()` has no effect; no Workers AI binding |
| [Durable Objects](../services/durable-objects.md) | **Yes** | See the service page |
| [Durable Object Facets](../services/durable-object-facets.md) | **Yes** | See the service page |
| [Containers](../services/containers.md) | **Experimental** | See the service page |
| [Static assets](../services/static-assets.md) | **Yes** | See the service page |
| [Cron Triggers](../services/cron-triggers.md) | **Yes** | See the service page |
| [Dynamic Workers](../services/dynamic-workers.md) | **Yes** | See the service page |
| [KV](../services/kv.md) | **Yes** | See the service page |
| [Queues](../services/queues.md) | **Yes** | See the service page |
| [D1](../services/d1.md) | **Yes** | See the service page |
| [Workflows](../services/workflows.md) | **Yes** | See the service page |
| [R2](../services/r2.md) | **Yes** | See the service page |
| [Cache](../services/cache.md) | **Yes** | Per-node and in-memory; no `fetch()` integration; purge by URL, not by tag |
| Workers AI | **No** | Not implemented |
| Vectorize | **No** | Not implemented |
| Hyperdrive | **No** | Not implemented |
| Browser Rendering | **No** | Not implemented |
| Email Workers | **No** | Not implemented |
| [Python Workers](../services/workers.md#python-workers) | **Partial** | `fetch` handlers on the Pyodide 0.28 runtime line |

## Runtime APIs

| API | Status |
| --- | --- |
| [Fetch, Request, Response, and Headers](#fetch-request-response-and-headers) | **Yes** |
| [Bindings](#bindings) | **Yes** |
| [Context](#context) | **Yes** |
| [Handlers](#handlers) | **Yes** |
| [RPC](#rpc) | **Yes** |
| [Streams](#streams) | **Yes** |
| Encoding | **Yes** |
| [WebSockets](#websockets) | **Yes** |
| [Web Crypto](#web-crypto) | **Yes** |
| [Web standards](#web-standards) | **Yes** |
| [WebAssembly](#code-generation) | **Yes** |
| [Performance and timers](#performance-and-timers) | **Yes** |
| Console | **Yes** |
| [Node.js compatibility](#nodejs-compatibility) | **Partial** |
| [Cache](#cache) | **Yes** |
| HTMLRewriter | **Yes** |
| [TCP sockets](#tcp-sockets) | **Yes** |
| EventSource | **Yes** |
| MessageChannel | **Yes** |
| BroadcastChannel | **No** |

### [Fetch, Request, Response, and Headers](https://developers.cloudflare.com/workers/runtime-apis/fetch/)

- `Request` and `fetch()` accept `cache: "no-store"` and `cache: "no-cache"`
  and refuse every other mode with workerd's `TypeError: Unsupported cache
  mode: <value>`; the cache is not consulted. `redirect: "error"` is refused
  at construction, as in workerd, because the mode is not implemented.
- An incoming request's `redirect` is `manual` and its `headers` are
  immutable, so `fetch(request)` returns a 3xx instead of following it and a
  header rewrite throws `TypeError: Can't modify immutable headers.`
- A string body (or any other non-`BodyInit` value) carries
  `content-type: text/plain;charset=UTF-8` unless the init names one. Bytes,
  `Blob`, `FormData`, `URLSearchParams` and stream bodies keep their own rules.
- `Response` validates its status (`200`-`599`, except `101` with a
  `WebSocket`), refuses a non-empty or streaming body on a null-body status
  (`101`, `204`, `205`, `304`), and refuses a C0 control in `statusText`. An
  accepted zero-length buffered body there becomes no body at all: `body` is
  `null`, `text()` is empty and no `content-type` is inferred, as in workerd
  -- including the type an empty typed `Blob` or `URLSearchParams` would
  otherwise carry.
- `Response.blob().type` is the parsed and re-serialized `content-type`;
  `Blob.type` on a user-built `Blob` is the raw lower-cased string, as in
  workerd.
- A fetched `204`, `205` or `304` has `body === null` and an empty `text()`,
  as in workerd; no body stream crosses from the host.
- Header names are validated with workerd's rule rather than the spec's token
  production: an empty name and a non-ASCII name are accepted (workerd
  accepts them), and the outgoing HTTP layer refuses to send them. On the way
  out, a name the writer cannot represent is dropped and the response still
  goes out; a byte a field value may not carry is dropped from the value.
  Workerd writes such a byte raw -- not a legal field value -- so that single
  case is stricter here, and the response survives either way.
- A `Blob` appended to `FormData` without a filename is named after the entry
  key, which is workerd's behaviour and not the WHATWG spec's `"blob"`.
- `new Request(url, { method: "trace" })` normalizes to `TRACE`; `CONNECT`
  and `TRACK` are refused.
- A gzip or deflate response body decodes before delivery, unless your
  request names `accept-encoding` (then the body arrives raw, as the Fetch
  standard requires). `content-encoding` and the original `content-length`
  both stay on the response, as in workerd, even though neither describes the
  decoded body. Sending a body drops a `content-length` or
  `transfer-encoding` the init replayed (a forwarded response's headers), and
  the host frames the request itself, as workerd does.
- A followed redirect sets `response.url` to the URL that answered and
  `response.redirected` to `true`. A redirect chain that lands back on the
  exact request URL reports `redirected: false` (the hop count is not
  exposed by the HTTP client).
- An inbound `Request` carries a `cf` object without Cloudflare edge
  fields. Cello cannot prove the geolocation, colo, or TLS metadata.
- Cello removes `Content-Length` from a Worker response, except for a
  `HEAD` response.
- A cloned body that the platform streams (a `fetch()`, RPC, Durable
  Object, or R2 body) buffers for the slower copy up to 32 MiB of skew,
  shared by all of one request's tees. Past that a copy is failed: a copy
  nobody has read from goes first -- the one holding the most, if it gives
  back enough of the overrun on its own -- and only when no unread copy
  gives back enough does the copy holding the most fail. A copy that is
  itself teed, or that a subrequest is streaming, is spared once it has
  read a byte; before its first byte it is indistinguishable from an
  unread copy, so a fetch streaming a copy can fail with the skew error
  too. The failed copy's next
  read rejects with `tee branch fell more than 32 MiB behind its sibling;
  read both branches concurrently`, a read already in flight rejects with
  the same error rather than ending the body as if it were complete, and
  the other copies keep flowing to completion. That budget is charged with
  the whole send, so a single chunk larger than 32 MiB fails every copy that
  has not read it. Workerd buffers for the slower copy within the isolate's
  128 MB limit instead of failing it.
- An isolate's `ArrayBuffer` memory is capped (`CELLO_V8_EXTERNAL_LIMIT_MB`,
  four V8 heap limits by default) instead of Workerd's single 128 MB
  per-isolate memory limit covering the heap and every buffer together. The
  cap covers the stores V8 allocates through the isolate: plain buffers and
  typed arrays, `SharedArrayBuffer`s without `maxByteLength`, `structuredClone`
  of one, and buffers the host hands to JavaScript (response bodies, WebSocket
  messages, RPC arguments, Queue message bodies, `/tmp` reads, D1 blobs). Past
  it the allocation is refused with a `RangeError` (V8's
  `Array buffer allocation failed`; a budget
  message on host stores), and the isolate is condemned: it takes on no new
  hibernatable socket, exactly as after a heap-limit hit, and serves again once
  both limits are back under 75 percent. Cello's defaults are more generous
  than Workerd's (128 MiB heap plus 512 MiB of buffers); lower
  `CELLO_V8_HEAP_LIMIT_MB` and `CELLO_V8_EXTERNAL_LIMIT_MB` together to
  approximate Workerd. Below 128 MB an isolate behaves the same as on
  Cloudflare.
- A resizable `ArrayBuffer`, a growable `SharedArrayBuffer` and
  `WebAssembly.Memory` are **not** covered by that cap. V8 allocates them from
  platform pages, which no embedder hook sees, so Cello neither refuses nor
  charges them, and workerd's 128 MB isolate limit has no equivalent for them
  here: a `maxByteLength` of 1 GiB is accepted where Workerd would fail the
  isolate later. What remains is per memory, not per isolate:
  `--wasm-max-mem-pages` fails every `grow` of one wasm memory past the cap,
  and the node's `CELLO_MAX_RSS_MB` pressure shedding bounds the number of
  such buffers. Keep loops over `maxByteLength`/`grow` out of Workers that
  also serve traffic. See [Limitations](limits.md).
- A `tee()` of a Worker-constructed `ReadableStream` bounds the skew the
  same way, counted per tee. The difference between what the two branches
  hold is measured just after a chunk is queued; past 32 MiB the branch
  that is behind fails its next read with the same error, while the branch
  being read keeps flowing. The chunk size alone fails no branch, but a
  branch that has not read an oversize chunk yet holds all of it and fails
  for lag, so with chunks larger than the budget both branches must be read
  concurrently. A chunk is charged its size -- 2 bytes per UTF-16 code unit
  for a string, the length of a buffer, the visible length of a view and
  not the buffer behind it -- and every chunk at least 64 bytes. That makes
  the budget a guard against accidental buffering rather than an isolate
  memory limit, so a payload hidden in an object chunk is not counted and a
  chain of N clones can hold N × 32 MiB. Workerd does not bound these
  branches at all.
- A streamed response body that produces no bytes for 60 seconds ends,
  even while the client stays connected. Workerd keeps an idle body open.
  Send a keepalive (an SSE comment line such as `: ping`) at least once a
  minute. A body that keeps producing streams for as long as the client
  listens; `CELLO_BACKGROUND_BUDGET_S` never ends it.
- A remote Durable Object call cannot retry after request-body
  transmission starts, because Cello keeps no replay copy.
- A remote Durable Object call waits for a rejected owner generation to
  change for at most `CELLO_OPERATION_DEADLINE_MS`.

### Reflected object shape

Workerd keeps the state of its Web API objects out of the JavaScript object,
and Cello matches that reflection. A `Request`, `Response`, `Headers`,
`FormData`, `AbortSignal`, `Blob`/`File`, `URL`/`URLSearchParams`, a stream,
an event object, `DOMException`, `MessagePort`/`MessageChannel`, `WebSocket`,
`CryptoKey`, a KV/R2/D1 binding or a `Cache` has no own property of its own:
`Object.keys()` is empty, spread and `JSON.stringify()` see `{}` (a
`DOMException` serializes as `code`/`name`/`message`/`stack`, as workerd's
does), and `Object.getOwnPropertyNames()` lists no host field or host helper.
Every WebIDL attribute is an enumerable accessor on the prototype -- workerd's
reflection, not a data property per instance -- so assigning to a read-only
attribute no longer changes what the object reports or does. Each interface
carries its `Symbol.toStringTag`, so
`Object.prototype.toString.call(response)` is `"[object Response]"` and
`[object DOMException]` for an error. `Request` and `Response` share workerd's
`Body` prototype level (a global `Body`), and `D1Database` inherits workerd's
`WrappedBinding` level, which is the tag a D1 database reports.

Known reflection-only differences:

- An interface's `prototype` property is non-writable: V8 refuses to redefine
  a class's, so where workerd's interface objects have a writable `prototype`
  Cello's do not.
- Method `length` values and accessor function names follow V8's class
  syntax. The differential gate pins structure, enumerability and
  accessor-versus-data, not those incidental values.
- `D1DatabaseSession` is not pinned against workerd: workerd's own session
  reflects internal `WrappedBinding` members (`_queryOrThrow`, `_send`,
  `_updateBookmark`, ...). Cello exposes `prepare`, `batch` and
  `getBookmark`.
- `D1PreparedStatement` carries workerd's own `statement` and `params` data
  members. They are copies for reflection; the statement's behaviour reads
  internal slots.
- `Headers.getAll()` is workerd's Set-Cookie accessor: any other name throws
  its `TypeError: getAll() can only be used with the header name
  "Set-Cookie".`
- `Cache.add()`, `Cache.addAll()`, `Cache.matchAll()`, `Cache.keys()`,
  `CacheStorage.delete()`, `CacheStorage.match()`, `CacheStorage.has()` and
  `CacheStorage.keys()` exist with workerd's shapes and throw workerd's own
  synchronous "the method is not implemented" error: the pinned build does the
  same, and the per-node store exposes no operation that enumerates entries.
- `URL.createObjectURL()` and `URL.revokeObjectURL()` exist and throw
  workerd's own "is not implemented" error, as workerd's pinned build does.

### [Bindings](https://developers.cloudflare.com/workers/runtime-apis/bindings/)

Only the binding types in the [services table](#services) are available.

Every binding method converts its options argument the way workerd does: an
absent or `null` argument means no options, any object is read for its
fields, and another argument throws `parameter N is not of type
'XOptions'`. That covers R2 `get`/`put`/`list`/`createMultipartUpload`, KV
`put`/`list`, and Durable Object storage `get`/`put`/`delete`/`list`/
`setAlarm`/`deleteAlarm`. What a binding's *own* fields accept is per
service; the [KV](../services/kv.md), [R2](../services/r2.md) and
[D1](../services/d1.md) pages list them.

- **D1 `meta.rows_read`/`rows_written` are Cello's own counters.**
  `changes`, `last_row_id`, `changed_db`, `size_after`, `total_attempts` and
  `timings.sql_duration_ms` mean what they mean on Cloudflare: `last_row_id`
  is the last row id an application statement assigned (a read after a write
  still reports it), and `size_after` counts the application's pages only,
  because Cello's storage and replica control tables live in the same file
  and a D1 database on Cloudflare has none. Two deviations remain. The two row
  counters do not: workerd reads them from libSQL's btree layer, which
  charges a write for the rows it reads and a DDL statement for pages, so a
  `CREATE TABLE` reports `rows_read: 1, rows_written: 2` there and `0, 0`
  here. A statement whose row counts follow from its result (a `SELECT`
  returns the rows it read; an `INSERT`/`UPDATE`/`DELETE` reports the rows it
  wrote) agrees with workerd. `exec()` runs the whole input as one statement
  stream, where Cloudflare's local runtime sends each line as its own query
  (so a statement may not span lines there); its `count` follows Cloudflare's
  one-per-input-line rule in both.
- **Durable Object SQL boolean binds are TEXT from compatibility date
  2026-11-24.** Workerd stores a `true`/`false` bind as `'true'`/`'false'`;
  Cello wrote INTEGER `1`/`0` before that date and keeps doing so at an older
  date or with none, so the rows a deployment already holds keep their
  meaning. `sql_boolean_text` and `no_sql_boolean_text` force either type.
  Bumping an existing deployment's date mixes the two types in one column,
  where the string `'false'` is truthy; the [Durable
  Objects](../services/durable-objects.md#boolean-binds) page has the
  migration.
- **R2 cursors are opaque tokens.** A string outside the token format that
  names no object gets the empty page R2 answers instead of a listing
  restarted from the top. A plain key is still read as the cursor 0.2.9
  answered a page with, through 0.2.10, and only while the bucket holds an
  object at that key -- a rolling upgrade must not truncate a client's
  pagination; the plain-key path is removed in 0.2.11. A token is honoured as
  a position inside the binding's own bucket and prefix; it is unsigned, so a
  client can mint one, but it cannot read another bucket, escape the prefix's
  directory, or widen a page bound.

`env` additionally carries `CELLO_NODE`: the name of the node running your
code, injected by the host after every deployment binding. A bundle that
must refuse a credential minted for another region reads this, not
placement. No deployment may bind the name — `vars`, `.dev.vars`, and
every other binding kind are refused at deploy time — and a loaded
Worker's props cannot smuggle it past the host value of an isolate that
has one.

In a Worker Loader bundle (`env.LOADER`), a `cloudflare:workers` name
imported only by a non-entry chunk still resolves: external stubs cover
the union of the main module's and every sibling's imports.

### [Context](https://developers.cloudflare.com/workers/runtime-apis/context/)

- `passThroughOnException()` has no effect, because Cello has no CDN
  fallback.
- `ctx.facets` is available only inside a Durable Object.

### [Handlers](https://developers.cloudflare.com/workers/runtime-apis/handlers/)

The `tail` and `email` handlers are unavailable.

### [RPC](https://developers.cloudflare.com/workers/runtime-apis/rpc/)

- Durable Object RPC stubs cross isolate boundaries on the same node: a
  method you call on a stub returned by another isolate runs against the
  owning cell. A stub whose owner runs on another node rejects with a
  clear error (no cross-node forwarding yet); a stub whose owner was
  evicted or reloaded rejects like a released same-isolate stub. Old-shape
  markers (no host id) keep failing on use exactly as before.
- The caller's request owns a stub it receives: when the fetch, RPC,
  alarm, or queue invocation ends, Cello disposes every stub it still
  holds and routes every release to the owner. There is no idle expiry: a
  quiet-but-live handle stays usable however long nothing flows, and a
  validated operation pins its entry so a racing release cannot fail it.
  Owner eviction revokes synchronously, and isolate retirement releases
  whatever the freed isolate still held.
- A stream you receive over RPC stays usable while you hold it: an active
  reader (or writer) keeps the stream past the receiving call and its
  invocation, the way a ReadableStream sent over RPC stays open until its
  writer closes it. Reading to the end, closing, cancelling, or an
  explicit dispose releases the hold at once; a dropped stream frees when
  collected, or when its isolate retires.
- A caller-owned `ReadableStream` in call arguments crosses to the cell
  through an upload channel: your isolate pumps it host-side (bounded,
  backpressured) and the cell pulls. Cancellation propagates both ways,
  and the channel dies with the sender's invocation. A caller-owned
  `WritableStream` has no reverse path: it keeps a same-isolate marker and
  fails on use across isolates.
- Minting is capped per scope (4096 live stubs) and per process (65536
  stubs, 8192 uploads); calls past the cap fail with a clear error.
  `CELLO_STUB_SCOPE_CAP`, `CELLO_STUB_PROCESS_CAP`, and
  `CELLO_UPLOAD_CAP` override the caps. Stub ids are 128-bit bearer
  tokens bound to the isolate that revived them; a stolen id alone is
  useless, and ids never log or serialize observably. Upload ids are
  process-wide bearer tokens without holder binding (any isolate
  presenting one can pull, push, or cancel it), contained by the same
  unguessability, uniform errors, and non-observability: markers never
  surface to script, and forwarding moves the wrapper rather than the id.
  One Cello process is one fleet, so the bearer boundary is the process.
- An `AbortSignal` in a Durable Object RPC call crosses on the same node
  (live: the receiver observes an abort the sender fires later) but does
  not cross a node boundary. Cancelling publishes synchronously, so the
  abort reaches the callee even as the caller's entry retires.
- A remote RPC retries only when the failed peer attempt did not start
  the method. Use a stable operation ID for your own retry.
- A plain object or array returned over RPC carries a no-op
  `Symbol.dispose`, so a `using` declaration typechecks; only results that
  revive stubs or disposers release anything.
- Workerd stub lifetimes differ in three cases: a stub you use only from
  `waitUntil` work after the handler settles is already disposed; a stub
  a Durable Object stores on its instance does not survive into a later
  invocation; a stub does not survive a socket upgrade. Each fails closed
  with a disposed error instead of leaking.
- `globalThis.__celloStubStatsForTestOnly()` reports your isolate's stub
  counts for the conformance suite. It exists only on nodes booted with
  `CELLO_TEST_STUB_STATS=1` (a process environment variable, which tenant
  code cannot set); production isolates never define it. It is test
  tooling, not a runtime API.

### [Streams](https://developers.cloudflare.com/workers/runtime-apis/streams/)

- Cello expires an unclaimed and inactive HTTP stream after 60 seconds. A
  successful stream operation starts a new 60-second period.
- An expired or unknown stream reports an error instead of EOF.
- A `TransformStream` applies backpressure, as in Workerd: its readable side
  defaults to a highWaterMark of 0, and `writer.write()` / `writer.ready`
  stay pending until a consumer reads. A cancelled readable, `abort()`,
  `controller.error()`, and `controller.terminate()` release a parked write.
  `IdentityTransformStream` and
  `FixedLengthStream` resolve a write only once its bytes have been read.
- A pull source runs on Workerd's schedule, which differs from the WHATWG
  text: `pull()` is called when a read request is registered and again while
  an `enqueue` leaves the desired size positive, but not after a `read()`
  dequeues an already-buffered chunk (the spec's `[[PullSteps]]` pulls
  there too; V8 does, Workerd does not). Every `read()` -- a default reader,
  a byte stream's default reader, or a BYOB reader -- resolves one microtask
  later than a plain JS promise, as Workerd's native binding does, so a pull
  that fulfils a pending read and its follow-up pull both run before the
  awaiting `read()` resumes. A source that prefetches, charges quota, or
  advances a cursor in `pull` therefore sees the same pull count per read as
  on Workerd.
- `tee()` cancels its source with the cancellation reason of the branch
  that cancels last, passed through unchanged. The WHATWG text joins the
  two reasons into an array; Workerd does not, and neither does Cello. A
  source that inspects its cancellation reason (a client abort versus a
  shutdown) reads the same value on both runtimes.
- `CompressionStream` and `DecompressionStream` are capped at 128 live
  streams per isolate; Workerd has no such cap. The 129th constructor call
  throws a `RangeError`. Finish, cancel, or abort streams you no longer use.
  Their writable side is an internal stream: once it has closed,
  `releaseLock()` is a no-op and the stream stays locked, as on Workerd. A
  readable `cancel()` records its error on the writable instead of erroring
  it, so a write already in flight still completes and a later one fails
  with the closed-stream error; a pending `read()` rejects with an `Error`
  built from the cancel reason, and it settles after the isolate's microtask
  drain, so a `cancel()` or `abort()` in the same turn as the producing
  write wins over the chunk. `abort()` on that writable is an ordinary
  abort: it resolves, `writer.closed` rejects with the abort reason, and the
  writable ends in the closed shape (`desiredSize` 0, later `write()` and
  `close()` report the closed stream) instead of the errored one a JS sink
  gets. A
  string chunk is UTF-8 encoded and `undefined` is an empty chunk; any
  other non-`ArrayBuffer`/`ArrayBufferView` chunk fails the same way. A
  gzip header CRC (FHCRC) is verified, and the trailer's CRC-32 and ISIZE
  are checked by the push that completes each field; a member whose trailer
  never arrived reports the incomplete-data error on `close()`, and bytes
  after the member report the trailing-bytes error; a `deflate` or
  `deflate-raw` stream reports the same trailing-bytes error for input past
  its end. The encoders write Workerd's bytes: zlib level 7, gzip with
  mtime 0, XFL 0 and OS 3, and a `deflate` stream whose zlib header is
  `78 9c` with an Adler-32 over exactly the bytes the encoder consumed, so
  a byte comparison of compressed output agrees with Workerd, chunked or
  not.
- `releaseLock()` recomputes backpressure after dropping the released
  writer's queued writes and keeps the dispatched write's size in the queue
  total, so a new writer's `ready` and `desiredSize` settle exactly as on
  Workerd; `ready` follows backpressure alone, staying pending behind a
  queued close while the stream is over its high-water mark and resolving
  when the close lands. `desiredSize` keeps reporting the queue formula
  while the stream is erroring (it can go negative) instead of `null`. A
  locked stream reports `This WritableStream is currently locked to a
  writer.` or `This ReadableStream is currently locked to a reader.`, in
  `pipeTo`/`pipeThrough` as well (Workerd's `pipeTo` writable message has
  no trailing period; `pipeThrough`'s does).
- Byte streams cross isolate boundaries on the same node as part of
  Durable Object RPC, in both directions: a `ReadableStream` your cell
  returns reads chunk by chunk, and a `WritableStream` it returns accepts
  writes, closes, and aborts. A `ReadableStream` in call arguments
  crosses the other way, pumped caller-side through a bounded host
  channel the cell pulls (see RPC above). Only byte chunks cross that
  channel: a non-bytes chunk fails the stream with a disconnect rather
  than mangling it. Nothing here expires on a timer.

### [WebSockets](https://developers.cloudflare.com/workers/runtime-apis/websockets/)

- `binaryType` defaults to `"blob"` for a compatibility date on or after
  2026-03-17 and for a deployment with no `compatibility_date`, so a binary
  message arrives as a `Blob`; an older date keeps `"arraybuffer"`, and
  `websocket_standard_binary_type` / `no_websocket_standard_binary_type`
  forces either. Setting both flags refuses startup. Setting
  `"arraybuffer"` switches the next message. Hibernation does not follow it:
  `webSocketMessage(ws, message)` always receives an `ArrayBuffer`.
- `send()` on a socket that neither `accept()` nor
  `state.acceptWebSocket()` claimed throws, instead of dropping the frame.
- Accepting an already accepted socket with `acceptWebSocket()` throws, and
  `setWebSocketAutoResponse(null)` throws; call it with no argument to
  clear the pair. A `new WebSocketPair()` has no own `length`.
- An outbound Worker socket closes when its event and `waitUntil` work
  end. A socket returned in the response stays open.
- Each isolate-polled input queue has a 1 MiB budget for non-terminal
  frames. A message larger than 1 MiB uses the complete budget.
- If your isolate stops polling, Cello discards unread frames during
  cleanup. A later pull reports an abnormal close.
- Each pipe direction between two ends is bounded twice: at most 16 MiB
  may wait in its output channel (the admission cap), and at most 16 MiB
  may sit staged upstream of its destination (the staging cap) — another
  isolate's input queue, a cell dispatch, or the socket itself. Admission
  bytes release when the pipe's forwarder receives a frame, and staged
  bytes when its send completes, so a direction with a full staging queue
  can still admit up to another 16 MiB into its output channel: the two
  limits are separate, not one 16 MiB aggregate. Either cap polices only
  bytes already held ahead of each frame: one message up to the 32 MiB
  message cap below passes whatever small backlog precedes it, so a
  heartbeat staged ahead never dooms the large message behind it. A
  backlog already at either cap still ends the pipe loudly, as does a
  frame larger than one message. Each ledger therefore holds at most just
  under its cap plus one message, so the transient worst case per
  direction is just under twice that (about 96 MiB). A flood past either
  cap ends the pipe loudly instead of growing host memory without bound:
  both ends observe close code 1011 with `wasClean: false`, an abnormal
  host-originated close no peer sent. Only staged bytes count against the
  cap: handler runtime stays unbounded, so a slow `webSocketMessage`
  handler never trips it, and a slow but live peer still drains fully —
  ordinary data writes never time out. Only the closing writes run under
  a 5-second progress bound — the close flush, the automatic ping/pong and
  close replies, and the loud-1011 attempts — and each frame delivered
  restarts the wait.
- A single WebSocket message — one frame or fragments assembled — is
  bounded at 32 MiB, Cloudflare's own message size limit: a peer that
  fragments past it ends the socket loudly with close code 1009 (Message
  Too Big) instead of growing host memory without bound, and your cell
  observes the 1009 as an abnormal application close. Each frame is held
  to what its message has left, so a frame that would pass the cap is
  refused from its header before its payload is read. A single message up
  to this cap also passes each 16 MiB pipe stage above whatever small
  backlog precedes it. Bytes held while a message is read do not count
  against the 16 MiB staging cap above: up to 32 MiB of assembly plus the
  WebSocket parser's read buffer, which can keep about twice the largest
  frame (about 64 MiB). The worst case per socket is therefore under about
  100 MiB plus one staged budget and the message ahead of it (just under
  16 MiB plus one message).
- Socket delivery is best-effort: if a 1011 attempt times out, the local
  pipe still reports 1011 but the wire peer may see an abnormal
  disconnect with no Close frame. Likewise, a write that stops mid-frame
  poisons the socket: no further frame is ever written after it, and the
  transport drops instead — the peer sees a truncation, never a terminal
  frame parsed as a continuation of the unfinished one.
- A close queued behind data still delivers every frame first, in order,
  while the peer can make progress; a peer that stopped reading ends the
  pipe instead of blocking closure.
- A WebSocket transport cannot move to a new cell owner. Your client must
  reconnect with the same application operation ID.
- A tunneled connection forwards the owner's Close without an additional
  Close. If the owner connection fails between frames before a Close, the
  ingress sends code 1012. If a frame is incomplete, the ingress closes
  the transport instead.
- `acceptWebSocket()` throws above 90 percent of the V8 heap limit.
- A response with a WebSocket to a request without `Upgrade: websocket`
  fails, as in workerd. Your Durable Object's `stub.fetch()` call then
  rejects, and an HTTP client receives status 500. The server end of the
  socket receives a close event with code 1006.
- Your Durable Object's `stub.fetch()` call rejects when the handler
  fails, on the owner node and on any other node. A failure on another node
  reports the same value a local owner would: the class is kept (an `Error`
  subclass stays that class, a `DOMException` stays one, a thrown object
  crosses by value), every object carries the `remote` marker, and a stack
  is its first line only. Two bounded exceptions apply to a remote owner: a
  thrown object whose clone does not fit the 64 KiB peer handler-failure
  body arrives as an `Error` carrying its message, and an owner running an
  older release reports its text message as an `Error` instead. Both are
  rolling-upgrade or bound cases; neither turns the failure into an
  application response.
- A Worker Loader worker (`env.LOADER`) answers a WebSocket upgrade the
  same way: a `101` response carrying `webSocket` routes to its target
  whether the loaded worker made the socket itself or forwards the
  upgrade to a Durable Object.
- `close()` sends 1000–4999 except the reserved no-status codes 1004,
  1005, 1006, and 1015, which stay refused. Codes 1001–1014 that the old
  WHATWG-only rule rejected now close, so a 1008 policy-violation refusal
  reaches the peer instead of throwing.
- A Durable Object can open an outbound WebSocket after it answers: the
  dial routes by the object's cell, not by the dead request.

### [Web Crypto](https://developers.cloudflare.com/workers/runtime-apis/web-crypto/)

- HMAC accepts MD5, SHA-1, SHA-224, SHA-256, SHA-384, and SHA-512.
- ECDSA supports only the P-256 curve with SHA-256, so a request that
  names a different curve or hash for a P-256 key is refused with
  `NotSupportedError`. Workerd accepts a mismatched curve/hash pair on the
  request, so callers written against it may need the request corrected.
- AES-GCM accepts any non-empty IV length and the authentication tag
  lengths 32, 64, 96, 104, 112, 120, and 128 bits.
- RSA-OAEP accepts SHA-1, SHA-256, SHA-384, and SHA-512. A nonempty label
  must contain valid UTF-8.
- AES-KW wraps and unwraps (RFC 3394; an IV mismatch fails the unwrap). A
  symmetric key imports and exports as `raw` bytes and as `jwk` with
  `kty: "oct"`.
- Ed25519 signs and verifies, and `NODE-ED25519` names the same
  algorithm. A signature from one spelling verifies under the other.
- X25519 derives bits and keys. A peer key of a low order gives a shared
  secret of all zero bytes, so `deriveBits()` rejects that key instead.
- A `CryptoKey`'s `algorithm` and `usages` are fresh frozen copies on each
  read, and the key's state is held outside the JavaScript object. Assigning
  to the copy throws in strict mode, where workerd's socket copy accepts the
  assignment and ignores it; the key's behaviour is the same either way.
- An Ed25519 or X25519 public key uses its 32-byte point for the `raw`
  format, so `importKey()` and `exportKey()` both carry the point alone.
- An EC public key imports and exports the uncompressed point as `raw`.

### [Web standards](https://developers.cloudflare.com/workers/runtime-apis/web-standards/)

`AbortSignal.timeout()` never keeps its event (or its admission slot)
alive: the timer is unreferenced, so it fires or dies with the request,
whichever comes first, and carries no timer id or cancellation.

The global object reports `ServiceWorkerGlobalScope` as its
`Symbol.toStringTag`, `globalThis.origin` is the string `"null"`, and the
interface constructors workerd defines exist here too — `Crypto`,
`Navigator`, `Performance*`, `FetchEvent`, `ScheduledEvent`, `TailEvent`,
`TraceEvent`, `PromiseRejectionEvent`, `WorkerGlobalScope`,
`ServiceWorkerGlobalScope`, and `Body` — so feature detection and
`instanceof` agree. Only `PerformanceMark` and `PerformanceObserver` are
constructible from script; every other one throws `TypeError: Illegal
constructor`. `crypto` is a `Crypto` instance and `navigator` a `Navigator`
instance (with `platform` `""` and `sendBeacon`). `reportError` exists and
dispatches the global `error` event synchronously, with workerd's
`Uncaught <value>` message; `URL.createObjectURL` and `URL.revokeObjectURL`
exist and throw workerd's
`Error: URL.createObjectURL() is not implemented`, because it does not
implement them either.

Four globals Cello used to expose have been removed to match workerd:
`DigestStream` (it remains at `crypto.DigestStream`), `FileReader`,
`Temporal` (workerd's V8 build leaves it off, along with V8's
`Date.prototype.toTemporalInstant`), and the internal
`DurableObjectRoutingError`. `MessageEvent` is constructible with workerd's
`data`/`origin`/`lastEventId`/`source`/`ports` and its own non-writable
properties. A `CompressionStream`/`DecompressionStream` accepts `brotli`
(byte-identical to workerd's output), stringifies a present format argument,
and rejects an unknown format with workerd's own TypeError. A `structuredClone` of a compiled
`WebAssembly.Module` throws `Error: Unable to deserialize cloned data.`, as
on workerd, so a module never crosses the clone boundary.

### Code generation

A Worker has no code generator, as on Cloudflare. `eval()`, `new Function()`
and `Function(...)` throw `EvalError: Code generation from strings disallowed
for this context`, and compiling wasm from JavaScript bytes — `new
WebAssembly.Module(bytes)`, `WebAssembly.compile()` and
`WebAssembly.instantiate(bytes)` — throws `CompileError: Wasm code generation
disallowed by embedder`. A library that probes `eval` inside `try`/`catch`
just detects no support and keeps running.

`WebAssembly.validate()` reads the bytes without compiling them and still
answers. `WebAssembly.Memory`, and `WebAssembly.instantiate()` of an
already-compiled `WebAssembly.Module` are not code generation and still work.
A `.wasm` file imported by the bundle is compiled ahead of time (Wrangler's
`CompiledWasm` rule, or the `no_bundle` sibling scan), so its default export
is a `WebAssembly.Module` and a wasm-bindgen or workers-rs bundle runs
unchanged. See [WebAssembly](wasm.md).

### [Performance and timers](https://developers.cloudflare.com/workers/runtime-apis/performance/)

`performance.timeOrigin` is `0`, and `performance.now()` matches
`Date.now()`. Both clocks advance at an I/O boundary and stay fixed
during JavaScript execution.

`performance` is a `Performance` instance with workerd's interface: marks,
measures and their entry classes (`PerformanceEntry`, `PerformanceMark`,
`PerformanceMeasure`, `PerformanceResourceTiming`), `getEntries*`,
`clearMarks`/`clearMeasures`/`clearResourceTimings`,
`setResourceTimingBufferSize`, `markResourceTiming`, `toJSON`, `timerify`,
`eventLoopUtilization`, `eventCounts`, and `nodeTiming` (all-zero node
timings; there is no libuv loop). `PerformanceObserver` exists with
`supportedEntryTypes` `["measure", "mark"]`, but never delivers a callback
inside a request — measured on workerd 1.20261121.1, and `takeRecords()`
returns an empty array on both.

A timer returns a `Timeout` object rather than a number: `refresh()`,
`ref()`, `unref()`, `hasRef()`, `close()` (returning the handle), and a
numeric primitive. `clearTimeout`/`clearInterval` accept either the handle or
its numeric value. A non-function callback throws workerd's TypeError
synchronously. Mark and measure names are coerced with `String()`, and
`detail: null` is refused where `undefined` means absent.

### [Node.js compatibility](https://developers.cloudflare.com/workers/runtime-apis/nodejs/)

- Cello implements `node:assert`, `node:async_hooks`, `node:buffer`,
  `node:diagnostics_channel`, `node:events`, `node:fs`, `node:os`,
  `node:path`, `node:stream`, `node:timers/promises`, and `node:util`.
- `node:diagnostics_channel` does not export messages to a tail Worker.
- `node:crypto` does not implement Diffie-Hellman, streaming signatures,
  ciphers, RSA-PSS, or DSA signatures and key generation.
- `node:crypto` implements `scrypt`/`scryptSync` with Node's parameters
  and defaults (`N`/`cost` 16384, `r`/`blockSize` 8,
  `p`/`parallelization` 1, `maxmem` 32 MiB).
- `KeyObject.toCryptoKey()` applies the requested algorithm,
  extractability, and usages to an asymmetric key.
- `KeyObject.export()` refuses a non-extractable key
  (`TypeError: Unable to export non-extractable key`), matching workerd. Node
  exports the material with a `DEP0204` deprecation warning instead.
- `node:zlib` implements only the synchronous gzip and deflate functions.
- `node:fs` provides `access`, `mkdir`, `realpath`, `stat`, `lstat`,
  `readFile`, `writeFile`, `writeFileSync`, `appendFile`,
  `appendFileSync` (plus `fs.promises`), and read/write streams. `/tmp`
  is writable and request-local: `writeFile` truncates unless
  `{ flag: "a" }` appends, `createWriteStream` writes chunks as they
  arrive, and anything that would push a request past
  `CELLO_TMP_MAX_BYTES` (default 64 MiB) fails with `ENOSPC`, while an
  overwrite frees the replaced bytes. `/bundle` is read-only and holds
  the Worker modules, and it is also `process.cwd()`, so a relative path
  resolves onto them. The entry module is projected as `worker.js`, while
  workerd uses the configured `main` name.
- `node:url` provides `pathToFileURL`/`fileURLToPath` plus the WHATWG URL
  API, with POSIX path rules only.
- The global `process` matches workerd for each field that it defines,
  such as `process.execPath`, `process.argv`, `process.title`,
  `process.version` (`v22.23.4`), `process.versions` (the same 28 keys,
  only `node` carrying a value) and `process.cwd()` (`/bundle`). Other
  fields, such as `process.kill` and `process.features`, are undefined.
- `Buffer` matches workerd's shape: `Buffer.poolSize` is `0` and
  read-only, so `Buffer.from`/`allocUnsafe` never allocate out of a pool;
  `Buffer.length` is `3`; `Buffer.prototype.inspect` prints Node's 50-byte
  hex dump; and `offset`/`parent` exist as enumerable accessors.
- `Iterator.prototype.includes`, `Iterator.prototype.join`, `Iterator.zip`
  and `Iterator.zipKeyed` exist, as on workerd; V8 15.2 still gates them,
  so Cello enables the flags (`--js-iterator-includes`,
  `--js-iterator-join`, `--js-joint-iteration`) at startup.
- `reportError` describes its value with V8's own no-side-effects
  describer, never the guest `toString` and never a Proxy trap: a
  `Proxy` (revoked ones included) is described by V8, and
  `Object.create(null)` reports `Uncaught [object Object]` rather than
  throwing. The event's `error` member is the reported value itself. A
  `DOMException` reports its `name: message` where workerd's describer
  finds none (workerd exposes those members as accessors).
- The Cello bundler supports a synchronous CommonJS `require()` of a
  Node.js built-in module. A raw ESM Worker has no global `require()`.
- A Node.js built-in module object is writable, so a dependency such as
  `graceful-fs` can patch it at load. The patch stays in its isolate.
- An import of another Node.js module succeeds, but its first call throws
  an error.

### [Cache](https://developers.cloudflare.com/workers/runtime-apis/cache/)

See [Cache](../services/cache.md) for the full behavior, freshness
rules, and limits.

- The cache is per-node and in-memory: entries survive across isolates of
  one script on one node, never cross scripts, and never cross nodes.
- `fetch()` neither reads nor populates the cache, and `cf.cacheKey` is
  unsupported; the request URL is the whole key.
- No API purges by `Cache-Tag`; purge by URL with `delete`.
- `Cache.add`, `Cache.addAll`, `Cache.matchAll`, `Cache.keys`,
  `CacheStorage.delete`, `CacheStorage.match`, `CacheStorage.has`, and
  `CacheStorage.keys` exist as on workerd, but every call throws workerd's
  `Error: Failed to execute '…': the method is not implemented.` — workerd
  1.20261121.1 does not implement them, and matching that keeps a bundle's
  feature detection and failure path identical.

### [TCP sockets](https://developers.cloudflare.com/workers/runtime-apis/tcp-sockets/)

- A socket cannot outlive its event. Your Durable Object must reconnect
  during a later event.
- Cello verifies a TLS server against its bundled Mozilla root store.
- Cello does not block the destination ports that Cloudflare blocks. Your
  fleet network controls the egress policy.

### BroadcastChannel

The class exists so that your bundle loads, but its constructor throws.

## Compatibility flags

Cello honors these compatibility flags:

- `delete_all_deletes_alarm`
- `js_rpc`
- `fetcher_no_get_put_delete`
- `sqlite_vec`
- `websocket_standard_binary_type`, implied for compatibility dates on or
  after 2026-03-17; `no_websocket_standard_binary_type` opts out
- `sql_boolean_text`, implied for compatibility dates on or after
  2026-11-24; `no_sql_boolean_text` opts out. Setting both refuses startup.
- The static-assets navigation flags

Cello accepts every other flag without effect.
`Cloudflare.compatibilityFlags` reports only the honored flags.

## Wrangler configuration

`cello deploy` accepts `wrangler.jsonc` or `wrangler.json`, not
`wrangler.toml`.

Your `name` must contain 1 to 63 lowercase ASCII letters, digits, or
internal hyphens. It cannot start or end with a hyphen.

The accepted top-level keys:

- `$schema`, `name`, `main`, and `no_bundle`
- `compatibility_date` and `compatibility_flags`
- `durable_objects` and `migrations`
- `assets`, `services`, `triggers`, and `vars`
- `d1_databases`, `kv_namespaces`, `queues`, `workflows`, and
  `r2_buckets`
- `worker_loaders` and `containers`
- `define` and `rules`

Any other top-level key, including `routes`, stops the deployment.

`define` and `rules` go to the esbuild run. Each `define` value is a
JavaScript expression, as in Wrangler. A rule `type` must be `Text`,
`Data`, or `CompiledWasm`, and each glob must have the form `**/*.ext`
or `*.ext`, because esbuild selects a loader by extension. Two rules that
give one extension different types stop the deployment. Cello already
applies `CompiledWasm` to `**/*.wasm`, so only that type is valid for
`.wasm`. `no_bundle` skips esbuild, so `no_bundle` with `define` or
`rules` stops the deployment.

An asset-only project can omit `main`. `cello deploy` refuses an unsafe
asset path, and `.assetsignore` requires Wrangler.

See [Limitations](limits.md) for the operational limits.
