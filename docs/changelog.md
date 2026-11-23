---
title: Changelog
description: What shipped in each Cello release, from v0.6.3-ryuzcorp.4 to the unreleased changes.
---
## Unreleased

The missing-API workerd differences are fixed: every `MessageEvent`, the
`Cache`/`CacheStorage` method surface, brotli compression, `FixedLengthStream`
length failures, the `performance` entry/observer API, the global scope's
interface constructors, timer handles, and two error paths now match workerd
(#195, #196, #201-#204, #205, #208, #209, #214, #219, #220). Four globals are
removed because workerd does not define them: `DigestStream` (still at
`crypto.DigestStream`), `FileReader`, `Temporal`, and the internal
`DurableObjectRoutingError`. The differential gate gained nine cases. The
removals are listed again in the breaking-changes section below.

- **`new MessageEvent(...)` works for every init** (#195). Workerd's
  `MessageEvent` is constructible with `data`, `origin`, `lastEventId`,
  `source` and `ports`; Cello threw `TypeError` from the constructor for
  every init and had no `ports`/`source` members. The members are own,
  enumerable, non-writable properties in workerd's order, `ports` is a fresh
  array of `MessagePort`s, `data` defaults to `null`, and the constructor's
  validation messages match (`constructor parameter 1 is not of type
  'string'`, `Incorrect type for the 'ports' field on 'Initializer'`, …).
- **`Cache` and `CacheStorage` expose workerd's whole method surface** (#196).
  `Cache.add`, `Cache.addAll`, `Cache.matchAll`, `Cache.keys`,
  `CacheStorage.delete`, `CacheStorage.match`, `CacheStorage.has` and
  `CacheStorage.keys` were missing. workerd 1.20261121.1 defines them and
  throws `Error: Failed to execute '<method>' on '<interface>': the method is
  not implemented.` synchronously; Cello now does exactly that, and every
  method's `length` is 0 as JSG's are.
- **`CompressionStream`/`DecompressionStream` support `brotli`** (#201). The
  encoder uses quality 5 with a 19-bit window, which reproduces workerd's
  bytes exactly for the probed inputs, and the decoder validates the stream at
  close. An unknown format is rejected at construction with workerd's
  `TypeError: The compression format must be either 'deflate', 'deflate-raw',
  'gzip' or 'brotli'.`; a missing argument gets its
  `constructor parameter 1 is not of type 'string'.` message; a present
  argument is stringified first, so an object with a `toString` or a number
  gets the format message, as on workerd. `flate2` is
  bumped to 1.1.11 (patch) because 1.1.9 reports a truncated gzip stream as a
  checksum error, and only the newer release distinguishes it from corrupt
  input.
- **`FixedLengthStream` reports a length failure on the readable** (#202). An
  overflow write and an underflow close now resolve; the readable errors with
  workerd's `TypeError: Attempt to write too many bytes through a
  FixedLengthStream.` / `TypeError: FixedLengthStream did not see all expected
  bytes before close().` and a later close or write keeps rejecting with the
  stored error.
- **`performance` has workerd's entries, observers and timers** (#203). The
  object is a `Performance` instance with `getEntries`, `getEntriesByName`,
  `getEntriesByType`, `clearMarks`, `clearMeasures`, `clearResourceTimings`,
  `setResourceTimingBufferSize`, `markResourceTiming`, `toJSON`, `timerify`,
  `eventLoopUtilization`, `eventCounts` and `nodeTiming`, and the entry
  classes (`PerformanceEntry`, `PerformanceMark`, `PerformanceMeasure`,
  `PerformanceResourceTiming`, `PerformanceObserverEntryList`) and
  `PerformanceObserver` exist. `mark()` returns a `PerformanceMark`, a
  measure's `start`/`end` resolve mark names, and `JSON.stringify` produces
  workerd's fields. The clock is unchanged: `timeOrigin` is `0` and both
  clocks advance only at an I/O boundary. Mark and measure names are coerced
  with `String()`, only a missing name throws, and `detail: null` is refused
  as workerd does (only `undefined` means absent). `PerformanceObserver`
  exists but never delivers a callback inside a request, matching workerd.
- **The global scope has workerd's interface constructors and values** (#204,
  #205, #209). `reportError`, `origin` (`"null"`),
  `globalThis[Symbol.toStringTag]` (`"ServiceWorkerGlobalScope"`), the
  `Crypto`, `Navigator`, `Performance*`, `FetchEvent`, `ScheduledEvent`,
  `TailEvent`, `TraceEvent`, `PromiseRejectionEvent`, `WorkerGlobalScope`,
  `ServiceWorkerGlobalScope` and `Body` constructors, and
  `URL.createObjectURL`/`revokeObjectURL` (both throwing workerd's
  not-implemented `Error`) are present. `reportError` dispatches the `error`
  event synchronously with workerd's `Uncaught <value>` message. `crypto` is a
  `Crypto` instance and
  `navigator` a `Navigator` instance with `platform` `""` and `sendBeacon`.
  `console` has `context`, no `createTask`, and the `console` tag.
- **Timers behave like workerd's** (#208, #219). `setTimeout`/`setInterval`
  are named functions with workerd's arity, return a `Timeout` object with
  `refresh`/`ref`/`unref`/`hasRef`/`close` and a numeric primitive;
  `close()` returns the handle, and `clearTimeout`/`clearInterval` accept the
  handle or its numeric value. A non-function callback throws
  `TypeError: The "callback" argument must be of type function. Received …`
  synchronously instead of failing later as an unhandled rejection.
- **Error text matches workerd where the type already did** (#214). `atob`,
  `btoa`, an invalid `TextDecoder` label, a fatal decode, a decompression
  stream closed with incomplete data, and the default `AbortError` message
  now use workerd's wording.
- **`structuredClone` refuses a compiled wasm module** (#220). It throws
  `Error: Unable to deserialize cloned data.` as workerd does, instead of
  transferring the module.
- **Web API objects no longer expose host internals, and they report
  workerd's interface shape** (#170, #171, #206, #207). Every reflected
  object -- `Request`, `Response`, `Headers`, `FormData`,
  `AbortController`/`AbortSignal`, `Blob`/`File`, `URL`/`URLSearchParams`,
  `TextEncoder`/`TextDecoder`, every stream and its reader, writer and
  controller, `TransformStream` subclasses, the events, `DOMException`,
  `MessagePort`/`MessageChannel`, `WebSocket`, `CryptoKey`/`SubtleCrypto`,
  the KV/R2/D1 bindings and `Cache` -- keeps its state in a private slot.
  `Object.keys(obj)` is empty, `JSON.stringify(new Response("x"))` is `{}`,
  `JSON.stringify(new FixedLengthStream(3))` is `"{}"`, and spread copies
  nothing; `Object.getOwnPropertyNames()` lists no `_bodyBytes`,
  `__celloHost`, `_entries`, `_expectedLength` or similar field, and no
  `_cancel`, `_finishClose`, `_pump`, `__celloHeaderList` or other host
  helper is on a prototype. Attributes are enumerable prototype accessors as
  workerd's reflection has them, so assigning to a read-only attribute no
  longer changes what the object reports; each interface carries its
  `Symbol.toStringTag` (so `Object.prototype.toString.call(response)` is
  `"[object Response]"`), and `Request`/`Response` share workerd's global
  `Body` prototype level. `DOMException` is now shaped like workerd's: own
  enumerable `code`/`name`/`message`/`stack`, no `Error` statics, tag
  `DOMException`. `Headers.getAll()`, `Request.cf`/`integrity`/`keepalive`/
  `cache`/`fetcher`, `Response.cf`, `WebSocket.readyState`/
  `bufferedAmount`/`url`/`protocol`/`extensions`, and
  `ReadableStreamBYOBRequest.atLeast` now exist as workerd defines them;
  `Headers.getAll()` keeps its Cello signature but reports workerd's
  Set-Cookie-only rule for every other name.
  `R2Bucket` is a real interface whose construction is token-gated, so a
  forged `new R2Bucket(...)` throws instead of minting an object the host
  paths would accept, and `D1Database.query()` refuses with workerd's
  `d1_binding_jsrpc` message. **Breaking:** KV's Cello-only `deleteBulk()` is
  gone (workerd has no such method) and `DOMException.captureStackTrace`, and
  the other `Error` statics, are no longer reachable. The `shape-*` gate
  cases pin the reflected shape of every class above against workerd
  1.20261121.1; the remaining, reflection-only differences (interface
  `prototype` writability, V8's method `length`s and D1 sessions) are listed
  in the compatibility page.
- **A released writer and a failed compression write answer as workerd does**
  (#193, #223) after the stream state moved into private slots: a released
  `WritableStreamDefaultWriter` throws from `desiredSize` in every state and
  installs a fresh rejected `ready`, and a failed compression write closes the
  writable side instead of erroring it (`closesOnError` is a field of the
  private writable state, so the opt-in cannot be lost).
- **The differential gate now pins the reflected shape of every interface**
  (#170, #171, #206, #207). Nine `shape-*` cases compare own properties,
  prototype members and their enumerability, the interface tags, constructor
  names and lengths, `JSON.stringify` output and `Object.prototype.toString`
  between workerd 1.20261121.1 and Cello, for the Fetch types, events, the
  stream family, Web Crypto, the KV/R2/D1 bindings and `WebSocket`. Every case
  agrees -- 201 cases, including the stream-edge cases from #193/#223.
- **A host value can no longer cross RPC as inert data** (#170). The RPC
  projection marks a private-state host -- a signal, `Request`, `Response`,
  `Headers`, `Blob`, `CryptoKey` or a stream -- as a host value, so it takes
  the lift path instead of a structured clone that would have flattened it to
  `{}`. The queue, attachment, workflow and batched-storage paths refuse those
  values with workerd's `DataCloneError` text, and so does `storage.put()` on
  both the single-key and batch paths: the refusal sits in the host
  serializer's own delegate, driven by a module-private, brand-only predicate,
  so it stores nothing and runs no guest code. The predicate reads only
  intrinsics captured at bootstrap, so a Worker that patches
  `WeakMap.prototype.has`/`Reflect.apply` under a pollution prelude still gets
  the refusal (or the success) instead of running its own code from inside the
  serializer. A stream still crosses a caller boundary as a `DataCloneError`,
  exactly as before, and a client that disconnects mid-stream still aborts the
  producer's `request.signal` through the private response state.

Two more `WritableStream` divergences from workerd are fixed, and the
tee-skew conformance test no longer assumes which of a tee's copies the skew
budget ends. A third change is breaking: a Worker can no longer generate code
at run time (#194), described below. The differential gate gained six cases
from those fixes and fourteen Request/body cases from the changes below,
plus 25 binding cases, six Web Crypto cases, 23 Durable Object and
WebSocket cases, 66 Fetch API cases and nine missing-API cases (all below),
193 in total. No configuration or
bucket-format changes; the development store gains two nullable
columns, which is backward compatible (see below). The `eval`,
`Function` and wasm runtime-compilation changes are listed after the stream
fixes because they break bundles that relied on them.

- **A released `WritableStreamDefaultWriter` matches workerd** (#193). A
  released writer's `desiredSize` kept reporting the stream's size, and
  `null` once the stream had errored; workerd throws
  `TypeError: This WritableStream writer has been released.` in every
  state, and so does cello now. `releaseLock()` also left `writer.ready`
  carrying the stream's error, where workerd installs a freshly rejected
  ready: a read after the release is always the released error, while a
  handler attached before the release keeps the old settlement (still
  resolved, or still the stream's error). `writer.closed` keeps its own
  settlement once it has one, and still rejects with the released error
  while pending. `write()`, `close()` and `abort()` keep rejecting with the
  released error. The gate gained the `writer-released` case; both compared
  against workerd 1.20261121.1.
- **A compression stream's writable side closes after a failed write**
  (#223). `DecompressionStream` and `CompressionStream` are internal
  streams in workerd, and a failed push closes their writable side instead
  of erroring it: a later `close()` or `write()` rejects with `TypeError:
  This WritableStream has been closed.` instead of the stored
  `Decompression failed.`, `desiredSize` is `0` and `abort()` resolves,
  while `writer.closed` rejects with the failure and `writer.ready`
  resolves. A plain `TransformStream` whose transform throws still errors
  both sides, as before. The gate gained the
  `decompression-close-after-error` case.
- **A `WritableStream` refuses each state with workerd's own message**
  (#132). A second `writer.close()` on a closed stream reported the shared
  `TypeError: Stream already closed/errored` from the same early path as an
  errored stream; workerd answers `TypeError: This WritableStream has been
  closed.`. A `close()` while a close is already in flight now rejects with
  `TypeError: Cannot close a writer that is already being closed` instead of
  `Stream already closing`, and `writer.write()` no longer shares one
  `Cannot write to a closing/closed stream` for two states: it reports
  `TypeError: This WritableStream has been closed.` on a closed stream and
  workerd's `TypeError: This ReadableStream is closed.` while a close is in
  flight. `close()` and `write()` on an errored or erroring stream keep
  rejecting with the stored error, which #129 fixed and workerd agrees with.
  The gate gained the `writable-close-states` and `writable-write-states`
  cases; both compared against workerd 1.20261121.1.
- **`writer.abort()` on an errored stream resolves, and every call gets its
  own promise** (#133). The chunk header recorded an "abort() on an errored
  stream rejects with the stored error" deviation as workerd's behaviour, but
  workerd resolves it — the error already reached `writer.closed` and every
  other consumer, so aborting changes nothing. Repeated `abort()` also
  returned the request's own promise, while workerd hands each call a fresh
  promise (all of them settling alike, and the sink's `abort` still running
  at most once). Aborting while the stream is still erroring keeps rejecting
  with the stored error and still skips the sink abort, as measured. The
  header now states the measured behaviour instead of the old claim, and the
  gate gained the `writable-abort-states` case.
- **The tee-skew conformance test pins what survives, not which copy loses**
  (#134). The test uploaded 40 MiB through a request-body tee whose forwarded
  copy fed a subrequest, left the local copy unread, and asserted the local
  copy was the one the 32 MiB skew budget ends. The budget fails a copy whose
  consumer has taken no byte yet and cannot tell the forwarded copy from the
  local one before its first byte, so under load the forwarded `fetch()`
  sometimes rejected with the skew error; the fixture then returned before
  reading the rest of the request body and the half-duplex test client failed
  its upload with `Broken pipe` (5 of 10 runs under CPU load). The fixture now
  reports each copy's outcome and always reads the local copy to the end, the
  client reads the response while it uploads, and the test asserts the
  scheduling-independent contract: the handler answers, at least one copy
  reports the skew cap, and every copy that reports success carries the whole
  40 MiB.
- **A `fetch()` body read in `ctx.waitUntil()` is bounded by the background
  budget** (#141). Since #139 removed the total fetch deadline, a read that
  only `waitUntil` work drove was bounded only by
  `CELLO_HTTP_STREAM_IDLE_S`, which every received chunk refreshes: a
  1-byte drip kept the read running past `CELLO_BACKGROUND_BUDGET_S` and
  pinned the event, its isolate and the upstream socket until the idle
  deadline. The driver's abort pass did not end it, because it adopts the
  operations that own the `IoContext`. A read issued from post-response
  `waitUntil` work now carries the event's absolute background deadline on
  its HTTP stream source; at that deadline the HTTP stream registry cancels
  the upstream request and the read rejects with an `Error` naming the knob.
  A response body the client is still reading keeps its client-bound
  behaviour, and the `CELLO_BACKGROUND_BUDGET_S` wording now states what is
  enforced.

**Breaking:** a Worker can no longer generate code at run time (#194).
`eval()`, `new Function()` and `Function(...)` throw `EvalError: Code
generation from strings disallowed for this context`, and compiling wasm from
JavaScript bytes — `new WebAssembly.Module(bytes)`, `WebAssembly.compile()`
and `WebAssembly.instantiate(bytes)` — throws `CompileError: Wasm code
generation disallowed by embedder`, exactly as on workerd. A bundle that
evaluated strings on Cello must ship the code it generated. A `.wasm` import
still gives a compiled `WebAssembly.Module`, and
`WebAssembly.instantiate()` of an already-compiled module still works;
`WebAssembly.validate()` is unchanged.

Three Request/body divergences from workerd, all found by the differential
harness:

- **`new Request(usedRequest)` throws** (#140). The `Request` constructor
  rebuilt a used body -- a buffered body a consumer had taken, or a stream a
  reader had disturbed -- into a fresh, usable Request, so a Worker that
  accidentally rebuilt an already-read request silently re-sent the payload.
  It now throws workerd's `TypeError: Cannot reconstruct a Request with a
  used body.`; a body that is locked by a reader that has read nothing is
  refused too, with workerd's `TypeError: The ReadableStream has been locked
  to a reader.` `clone()` still copies an unused body and tees a live stream.
  The gate gained the `request-copy-used` and `request-copy-locked` cases.
- **A bodyless `Request` has `body === null`** (#142). A POST built without a
  body used to get an empty body stream: `request.body` was readable where
  workerd's is `null`, and `text()` marked it used so a second read threw.
  Construction, `clone()`, the cross-isolate revive marker and both ingress
  paths now keep "no body supplied" distinct from an empty body. The ingress
  rule is the HTTP framing rule, for every method: an unframed request is
  `null`, while `Content-Length: 0` frames a present empty body whose single
  read marks it used, as workerd reports (#174). A bodyless Request forwarded
  to a cell or a service binding crosses as an absent body, not an empty one.
  The gate gained `request-bodyless-post`, `incoming-bodyless`,
  `incoming-empty`, `incoming-get-framed` and `incoming-get-bodyless`.
- **Body bytes come from internal slots on the response pump** (#130). The
  pump read `buffer`/`byteOffset`/`byteLength` off a guest-supplied view, so
  a chunk with its own `byteLength` sent eight bytes where workerd sends
  three, and a tight backing buffer made the reply empty. It now reads the
  captured `%TypedArray.prototype`/`DataView` getters and maps a forged
  prototype to workerd's `TypeError: This ReadableStream did not return
  bytes.` The same slot reads back the constructors' body path, `Blob` parts,
  the KV value encoder and value-stream drain, and a queued `WebSocket.send`
  (which now snapshots the bytes, as workerd does). The gate gained
  `chunk-shadow-response`, `chunk-shadow-response-tight`,
  `chunk-shadow-response-dataview`, `blob-part-shadow`, `kv-string-chunk`,
  `kv-view-shadow` and `websocket-send-copy`.

The KV, R2, D1 and Cache bindings now answer the way workerd's local
implementation does on the paths a differential sweep compared. R2 reports an
object's content ETag — the MD5 of its bytes, or `md5(md5s)-N` for a multipart
upload — and a unique per-write `version`, so identical content answers one
ETag. R2 and KV refuse `.`/`..` object and key names. Range, metadata-size,
list-limit, expiration and D1 statement failures carry R2's error codes, KV's
`KV <OP> failed: <status> <text>` shape, and D1's SQLite detail instead of
Cello-specific text. Every fixed divergence has a case in the
`scripts/workerd-diff` gate, which now also runs the KV, R2, D1 and Cache
bindings (workerd gets `--experimental` for its binding config).

KV:

- `.` and `..` are refused as key names, on reads and writes, with
  workerd's `TypeError` (#147).
- `expiration` must be at least 60 seconds ahead, like `expirationTtl`
  (#146).
- A `list()` limit above 1000 is refused (#155).
- Input validation uses workerd's error class and message: a local check
  throws `TypeError`, and a request-level refusal reads
  `KV <OP> failed: <status> <text>` (#156).
- A bulk `getWithMetadata()` answers `null` for an absent key instead of an
  object whose value is null, so a caller can tell "absent" from "stored".

R2:

- `R2Object.etag` and `httpEtag` are the object's content ETag, not the
  development store's sequence number (#144). A single-request write is the
  hex MD5 of its bytes; a multipart object is `md5(md5s)-N`. `version`
  remains a unique id, one per write.
- A range that starts past the end of the object fails with
  `get: The requested range is not satisfiable (10039)` instead of the
  store's internal path (#145).
- `.` and `..` key segments are refused with `InvalidObjectName (10020)`
  before any write, on every method (#148).
- `customMetadata` is capped at 8 KiB of names plus values, which fails a
  put with `MetadataTooLarge (10012)` (#162).
- `list()` refuses a `limit` outside 1..1000 with `(10022)` (#164; the
  invalid-cursor behavior is unchanged).
- `onlyIf.etagMatches`/`etagDoesNotMatch` refuse the quoted `httpEtag`
  spelling with workerd's `TypeError` (#189).

D1:

- `batch([])`, `exec("")`, a comment-only `exec`, and a `prepare()` that
  holds no statement fail with `No SQL statements detected.` (#152).
- `exec()` reports SQLite's syntax error, including the token offset and the
  result code, instead of Cello's "incomplete statement" diagnosis (#153).
- `meta` carries `total_attempts`, always 1 (#157).

Cache:

- `ignoreSearch` and `ignoreVary` are refused with the error workerd
  throws rather than silently ignored (#166).
- A cached response keeps its `statusText`, including an empty one (#168).

Development store migration: the dev-store SQLite file gains two nullable
columns, `etag_md5` and `version`, created or upgraded in one immediate
transaction, so the `cello dev` supervisor and node cannot race the same
`ALTER TABLE`. The change is additive and backward compatible. A row the
older store wrote keeps its sequence number as its ETag — so a conditional
write that names it still matches — and takes that number as its `version`;
it earns a content MD5 only when this version rewrites it. A row the older
store rewrites *after* the upgrade is recognised by its `etag` and `version`
disagreeing and answered by the sequence that write recorded, so no repair
scan runs on open and no stale MD5 survives. No object data is rewritten,
and `cello dev` opens an existing store in place.

Web Crypto now enforces the usages a `CryptoKey` was created with, validates
its arguments with workerd's error types and text, and accepts the AES-GCM
IV and tag lengths workerd accepts. The security-relevant change is the
first: a key imported for `verify` can no longer sign, and a usage string an
algorithm does not define is refused at import instead of stored on the key.

- **A `CryptoKey`'s usages are enforced on every operation** (#197).
  `importKey`, `generateKey` and `deriveKey` now reject a usage the
  algorithm does not define with a `SyntaxError`, reject a secret or private
  key with no usage, and store the usages in the algorithm's own order. Every
  operation (`sign`, `verify`, `encrypt`, `decrypt`, `wrapKey`, `unwrapKey`,
  `deriveBits`, `deriveKey`) checks the key's algorithm name and its declared
  usage before doing any work, raising `InvalidAccessError` when either does
  not match. `sign()` on a verify-only key, `encrypt()` with an Ed25519 key
  and `unwrapKey()` with a wrap-only KEK now fail as they do on Cloudflare.
  `deriveKey` requires `deriveKey` on the base key without also requiring
  `deriveBits`. `NODE-ED25519` and `Ed25519` name one algorithm for the
  key-match check, so a key generated under one spelling signs under the
  other. A key's type, algorithm, extractability, usages and material now
  live in internal slots: `key.__celloMaterial` is gone, guest
  `new CryptoKey()` throws `TypeError: Illegal constructor`, a forged
  `Object.create(CryptoKey.prototype)` is not a `CryptoKey`, and the
  properties are frozen copies, so mutating `key.algorithm.hash` cannot
  change what `sign()` hashes with. `structuredClone()` refuses a
  `CryptoKey` with workerd's `DataCloneError: Could not serialize object of
  type "CryptoKey". This type does not support serialization.` The clone
  registry is module-private rather than a symbol on each prototype, so no
  guest code can call a serializer and a prototype-only forgery is cloned as
  plain data. `CryptoKey.call()` throws `Illegal constructor`, like workerd.
  `node:crypto`'s `KeyObject.export()` refuses a non-extractable key with
  workerd's `TypeError: Unable to export non-extractable key` (Node itself
  exports it with a DEP0204 warning).
- **Web Crypto argument validation matches workerd** (#198). An absent or
  `null` algorithm, a non-`CryptoKey` key, a non-`BufferSource` data,
  signature or IV, an absent `iv`/`counter`, a non-integer
  `getRandomValues` view and an unknown digest, import format or generation
  algorithm now raise the same `TypeError`, `NotSupportedError`,
  `TypeMismatchError` or `QuotaExceededError` with the same message text.
  The internal `Cannot read properties of undefined` paths in `sign`,
  `verify`, `encrypt`, `decrypt`, `wrapKey`, `unwrapKey`, `deriveBits` and
  `exportKey` are gone. That includes the `importKey` key-data conversion
  (`BufferSource or JsonWebKey`), the unknown import algorithm name, and a
  number supplied where an algorithm is expected. RSA-PSS signing and
  verification now take the caller's `saltLength`; a signature whose salt
  differs no longer verifies, and a missing salt raises workerd's
  `DataError`/`TypeError`.
- **AES-GCM accepts any non-empty IV length and the full tag-length set**
  (#199). The host implemented the AEAD with a fixed-type nonce, so only
  12- and 16-byte IVs worked; J0 is now derived per NIST SP 800-38D for any
  length, and 32- and 64-bit authentication tags are accepted alongside
  96-128 bits. A payload written on Cloudflare with a short IV now decrypts
  here. The 96-bit-IV path runs through the audited `aes-gcm` AEAD, with the
  tag truncated to the caller's length and compared in constant time.
- **The non-extractable export error and several `getRandomValues` and AES
  messages use workerd's wording** (#214). This is text only, apart from
  `exportKey` now checking extractability before an unknown format.

The differential gate gained six Web Crypto cases (`crypto-usages`,
`crypto-operations`, `crypto-validation`, `crypto-aes-gcm`, `crypto-values`,
`crypto-key-isolation`):
the usage tables measured against workerd 1.20261121.1, the operation
checks, the validation probes, the IV and tag ranges, the key-isolation
attacks, and the digest, HMAC, AES-GCM, ECDSA, Ed25519, `getRandomValues`,
`timingSafeEqual` and `DigestStream` values the earlier sweep already
measured as agreeing.

Durable Objects and WebSockets now match workerd 1.20261121.1 on the
divergences the differential harness found. **One change is breaking:** a
WebSocket's `binaryType` now defaults to `"blob"` for a compatibility date on
or after 2026-03-17, so a binary message arrives as a `Blob` and
`new Uint8Array(event.data)` reads nothing. A Worker that wants an
`ArrayBuffer` sets `socket.binaryType = "arraybuffer"`; a deployment with an
older compatibility date keeps the old `ArrayBuffer` default. The rest is
behaviour a Worker already wrote against and silently did not get: `setAlarm()`
without an `alarm()` handler or with a non-Date throws, `send()` before
`accept()` throws, a Durable Object stub rejection is the handler's own error
without `rejected:` or host stack frames, and Queue settle methods are no
longer own enumerable properties.

Durable Object and WebSocket fixes:

- **`binaryType` follows the standard default** (#192). A freshly accepted
  socket, whether a `WebSocketPair` end or one from a `fetch()` upgrade,
  reports `"blob"`, and a binary message is a `Blob` until the Worker sets
  `"arraybuffer"`; setting it switches the very next message. This is
  workerd's `websocket_standard_binary_type`, implied from compatibility date
  2026-03-17; `websocket_standard_binary_type` opts in early and
  `no_websocket_standard_binary_type` opts out, and a Worker that sets both
  flags is refused at startup. A deployment with no `compatibility_date`
  gets the standard default, as workerd's newest-date default does.
  Hibernation is unchanged: `webSocketMessage(ws, message)` always receives
  an `ArrayBuffer`, whatever the socket's `binaryType`.
- **`setAlarm()` validates like workerd** (#149). A class with no `alarm()`
  handler, a non-Date/number argument, a non-finite instant, and a time at or
  before the Unix epoch each throw workerd's `TypeError`; a call in the
  constructor is still allowed because the handler may be added later.
- **`send()` before `accept()` throws** (#150). An inbound socket that neither
  `accept()` nor `state.acceptWebSocket()` claimed no longer accepts a frame
  and drops it silently.
- **A stub rejection is the value the handler threw** (#151). Both
  `stub.fetch()` and stub RPC reject with the handler's own value: an
  `Error` subclass keeps its class, a `DOMException` stays a `DOMException`,
  a thrown object stays an object, and a primitive becomes an `Error` with
  its text. The class is decided against the runtime's own `DOMException`,
  captured before tenant code runs, so a Worker that replaces
  `globalThis.DOMException` and throws an instance gives its caller a plain
  `Error`. The error carries workerd's own `remote` marker, and its `stack`
  is the first line (plus the caller's frames over RPC), so no
  `rejected: Error: …` text or Cello host frame reaches the guest.
- **`put(key, undefined)` is refused and clone failures are
  `DataCloneError`** (#181). A single put no longer writes nothing for
  `undefined` and leaves an older value behind; a function, symbol, or proxy
  value fails with workerd's `DataCloneError`, and a batch put still accepts
  `undefined` values.
- **`acceptWebSocket()`, `setWebSocketAutoResponse()`, and
  `WebSocketPair` match workerd** (#183). Accepting an already accepted
  socket is an error; clearing the auto-response takes no argument, not
  `null`; and a pair is `{0, 1}` with no own `length`.
- **Queue settle methods are prototype members** (#185). `Object.keys`,
  spread, `JSON.stringify`, and `structuredClone` of a batch or message see
  only `queue`/`messages`/`metadata` and
  `id`/`timestamp`/`body`/`attempts`.
- **A SQL cursor has no own enumerable fields** (#191). `Object.keys(cursor)`
  is empty and its internals no longer leak. Its `rowsRead`/`rowsWritten`
  still count Cello's DML rows rather than libsql's b-tree rows, so a DDL
  statement reports 0/0 where workerd reports 1/2; see
  [Durable Objects](../services/durable-objects.md).

The differential gate gained 23 cases across the `do`, `ws`, `ws-legacy`,
`ws-hib`, and `queues` fixtures, including a raw-frame WebSocket probe.

One behaviour is deliberately **not** changed: a boolean bound into Durable
Object SQL is still stored as INTEGER `1`/`0`, not workerd's TEXT
`'true'`/`'false'`. Existing rows already hold integers, and mixing the two
types would make `if (row.flag)` truthy for a new `'false'` while an old `0`
stayed falsy, with no migration able to rewrite a stored value's type. See
[Durable Objects](../services/durable-objects.md).
The Fetch API object model now answers what workerd 1.20261121.1 answers. A
differential sweep filed 23 divergences (#159-#190); this release fixes the
High and Medium ones and the cheap Low ones. A string body carries its default
`content-type`, an incoming request's redirect mode is `manual` and its headers
are immutable, `Response` validates its status, status text and body, a
followed redirect reports where it landed, and `new Request` validates its URL,
method, body, cache mode, signal and integrity. The new
`fetch_api_conformance` suite pins each one, and `scripts/workerd-diff` gained
the sweep's agreeing Fetch probes (a second origin serves their `fetch()`
calls).

- **A string body carries `content-type: text/plain;charset=UTF-8`** (#159).
  `new Response("hello")` and `new Request(url, { body: "hello" })` left the
  header unset, so a returned string body reached the wire untyped and a
  proxied string body lost its type on the subrequest. A non-BodyInit body
  (a number, an object stringified) gets it too, an explicit `content-type`
  always wins, and byte, Blob, FormData, URLSearchParams and stream bodies
  keep their own rules.
- **Cache modes validate like workerd's** (#160). `cache: "no-store"` and
  `"no-cache"` are accepted and reported by `request.cache`; every other mode,
  including `"default"` and `"reload"`, is `TypeError: Unsupported cache mode:
  <value>`. Before this, every mode -- including the ubiquitous
  `fetch(url, { cache: "no-store" })` -- threw a blanket "not implemented"
  `Error`.
- **An incoming request's redirect mode is `manual`** (#161). A proxy that
  forwarded an incoming request with `fetch(request)` followed redirects and
  returned the target's 200; it now returns the 3xx, as on Cloudflare.
- **`Response` validates its status, status text and body** (#163, #165,
  #167). A status outside 200-599 (or not a number, or `0`) is a `RangeError`
  with workerd's message, a fractional status truncates (`201.5` is `201`), a
  `101` is allowed only alongside a `WebSocket` (and a `WebSocket` only with a
  `101`), a null-body status (`101`, `204`, `205`, `304`) refuses a non-empty
  or streaming body, and a C0 control in `statusText` is `TypeError: Invalid
  statusText`. A zero-length buffered body on a null-body status (an empty
  string, byte array, `Blob`, typed `Blob` or `URLSearchParams`) is accepted
  and then reported as no body at all, as workerd does: `body` is `null`, the
  read is empty, and no `content-type` is inferred. An explicit one is kept.
- **An incoming request's headers are immutable** (#169). `set`, `append` and
  `delete` on `request.headers` throw `TypeError: Can't modify immutable
  headers.` instead of silently doing nothing, and `Request.prototype.headers`
  is a readonly accessor. A copy made with `new Request(request)` keeps a
  mutable list.
- **The default abort reason reads `The operation was aborted`** (#172), as
  workerd's does.
- **A followed redirect reports its final URL** (#173). `response.url` is the
  URL that answered and `response.redirected` is true; before this the
  response claimed the original URL and `redirected: false`, so a proxy that
  resolved links against `response.url` produced wrong URLs. `clone()` copies
  both fields.
- **`new Request` validates its URL and body** (#175, #176). A relative or
  unparsable input is `TypeError: Invalid URL: <input>`, and the stored `url`
  is the serialized absolute URL; a GET or HEAD with a body is `TypeError:
  Request with a GET or HEAD method cannot have a body.` An incoming GET is
  not refused: the host builds its request through an internal path, as
  workerd's runtime does.
- **`integrity` and `keepalive` have their spec defaults** (#177): `""` and
  `false`. A non-empty `integrity` is refused (#178) with workerd's "not
  implemented" `TypeError`, and a non-`AbortSignal` `signal` is refused with
  the type error it names. `null` is an absent signal. The same validation
  covers `fetch(url, init)`.
- **`File.lastModified` keeps `Infinity`** (#179); only `NaN` becomes 0.
- **`fetch()` keeps `content-length` on a decompressed response** (#180), as
  workerd does, alongside the retained `content-encoding`.
- **Error messages match workerd's** (#184): `Invalid URL string.` for a URL
  that does not parse, workerd's read-only message for assigning
  `url.searchParams`, `Unable to parse URL: <input>` from `Response.redirect`,
  the element-typed `TypeError` from `AbortSignal.any`, and the invalid-redirect
  message for `redirect: "error"`, which is now refused at construction
  (previously accepted and failed later).
- **`Response.blob()` re-serializes the MIME type** (#186). `blob().type` is
  the parsed and re-serialized `content-type` (`text/plain;charset=UTF-8`, not
  the raw lower-cased header), so a MIME equality check on a blob's type
  matches workerd's.
- **Header validation follows workerd** (#182): the messages are workerd's,
  and workerd's *acceptance* of an empty header name and a non-ASCII one is
  reproduced deliberately -- both deviate from the spec's token production,
  and a name workerd accepts must not throw here. The outgoing HTTP layer
  still refuses to put such a name on the wire.
- **A `Blob` appended to `FormData` without a filename is named after the
  entry key** (#188), which is workerd's behaviour; the WHATWG spec says
  `"blob"`, so this deliberately follows workerd.
- **`TRACE` is a known method again** (#190). `new Request(url, { method:
  "trace" })` normalizes to `"TRACE"` instead of throwing. `CONNECT` and
  `TRACK` are still refused. (The filed issue expected workerd to refuse
  `"TRACE"`; workerd 1.20261121.1 accepts it, and the gate pins that.)
- **A fetched `204`/`205`/`304` has no body** (#165). The host registers no
  body stream for a null-body status and the harness hands the Worker `null`,
  so the response reports `body === null` and `text()` is `""`, as workerd's
  does. Before this, the new null-body guard refused the empty stream the host
  always registered and `fetch()` of a void response threw -- every runner RPC
  that answers 204 with it.
- **An unrepresentable response header no longer destroys the response**
  (#182). A name the HTTP layer cannot represent (empty, non-ASCII, a
  separator) is dropped and the response still goes out, and a byte a field
  value may not carry is dropped from the value, instead of the whole
  response becoming `500 invalid Worker headers`. Workerd writes such a byte
  raw, which is not a legal field value and hyper refuses, so that one case
  deviates deliberately in cello's favour.
- **The host frames subrequest bodies** (#180). A Worker-supplied
  `content-length` or `transfer-encoding` describes the body it already has --
  a forwarded response's header list -- so it is dropped when sending, while
  the fetched `Response` keeps both. Before this, forwarding a decompressed
  gzip body replayed the compressed length and left the origin waiting.
- **`request.headers = …` reports workerd's message** (#169):
  `TypeError: Cannot assign to read only property 'headers' of object
  '#<Request>'`.

Still divergent and left for the slices that own them: the internal-own-key and
`[object Object]` tag differences (#170, #171) and the incoming GET body stream
(#174).

**Breaking:** four globals workerd does not define are removed (#204):
`DigestStream` (a global alias; `crypto.DigestStream` remains), `FileReader`
(construction only ever threw), `Temporal` (workerd's V8 build leaves it off,
including V8's `Date.prototype.toTemporalInstant` entry point), and
`DurableObjectRoutingError` (an internal class that was never documented).
A bundle that named one of them relied on Cello-only surface and gets
`undefined` now, as on Cloudflare.

## v0.2.8

A Worker's outbound `fetch()` no longer cuts a live response body at
`CELLO_FETCH_TIMEOUT_S`. That budget now covers the connection, the upload and
the response head. A new idle deadline, `CELLO_HTTP_STREAM_IDLE_S`, ends a body
that goes silent. Before this, a control plane that proxied its own SSE through
a Worker was cut off at 120 s. A Queue batch that a full consumer isolate
refuses no longer spends a delivery attempt. Four more divergences from
workerd, found by the differential harness, are fixed: `TransformStream`
close after cancel, lazy iterable bodies, single-use bodies (including
through `fetch()`), and `cache.put()` with string chunks. The only
configuration change is the new `CELLO_HTTP_STREAM_IDLE_S` knob. Both stream
timeouts are refused at startup above 86400 s. There are no data or
bucket-format changes.

The `TransformStream` and body divergences:

- **`writer.close()` after a readable cancel rejects with the cancel
  reason** (#129). Cancelling a `TransformStream`'s readable errors its
  writable with the cancel reason, and `writer.closed` already reported it,
  but `close()` took the errored early path and rejected with
  `TypeError: Stream already closed/errored`, so a producer's teardown could
  not tell a consumer abort from a real close failure. It now rejects with
  the stream's stored error, which is workerd's behaviour (the WHATWG text
  says a fresh `TypeError`). An `IdentityTransformStream` or
  `FixedLengthStream` cancel reaches the same path with workerd's converted
  `Error`. The differential gate gained the `transform-close-after-cancel`
  case.
- **An iterable body is a lazy stream of its own chunks** (#126).
  `new Response(["alpha", "beta"])` (or the same on a `Request`, a `Set`, or
  any other sync or async iterable) stringified to `"alpha,beta"` because the
  constructor fell through to the string-coercion path whenever a chunk was
  not bytes; a byte iterable was concatenated eagerly, which pulled the whole
  producer inside the constructor and hung on an infinite one. Every iterable
  that reaches the body path -- an array, an async iterable, or a sync
  iterable with no custom `toString` -- is now workerd's body stream: the
  iterable becomes the body whatever its chunk types, a chunk is validated at
  consumption, the constructor pulls nothing, each read pulls one value, and
  no `Content-Length` is set.
  `new Response(["alpha", "beta"]).text()` therefore rejects with
  `TypeError: This ReadableStream did not return bytes.`, an infinite
  generator constructs and reads a bounded prefix, and a non-iterable object
  with its own `toString` still stringifies. Cancelling the body pulls the
  producer's cleanup and adopts its outcome, so a rejecting `finally` or
  `return()` rejects `cancel()` with that reason, as workerd's does.
- **A body can be consumed once** (#127). A buffered body re-served its bytes
  on every `text()`, `json()`, `formData()`, `blob()`, `arrayBuffer()`, or
  `bytes()` call, so a Worker that read a body twice (or through two
  consumers) silently worked, while workerd throws
  `TypeError: Body has already been used. It can only be used once. Use tee()
  first if you need to read it twice.` Both `Response` and `Request` now
  reject a second read with workerd's error, whether the first read was a
  buffered consumer or a reader on the body stream. `fetch(request)` consumes
  the request's body too, even when the fetch never sends it — an aborted
  signal still leaves `bodyUsed` true and a second `fetch` refused with
  `TypeError: Cannot reconstruct a Request with a used body.` — while a
  request with no body, an `init` carrying its own body and a fetch whose
  `init` is refused (an invalid method or redirect) leave the input alone,
  and an unsupported `redirect` value reports workerd's message. A Request
  handed to another isolate (a Durable Object call) carries body presence in
  its marker, so a revived bodyless GET stays bodyless. A `null` body carries
  nothing and stays readable, as in workerd.
- **`cache.put()` takes a string chunk as its UTF-8 bytes** (#128). The capped
  drain summed `chunk.byteLength`, which is `NaN` for a string, then copied
  into an empty target and threw `RangeError: offset is out of bounds` without
  storing anything. A `put()` now consumes the body the way the host does: a
  string chunk is encoded as UTF-8, any other chunk goes through the same
  window-safe byte check every Body consumer uses, and each chunk is copied as
  it is read, so a producer that reuses one buffer across pulls cannot corrupt
  the stored entry.

- **`fetch()` no longer puts a live response body on a total deadline**
  (#136). `CELLO_FETCH_TIMEOUT_S` was applied as reqwest's total request
  timeout, which covers the response body, so a Worker streaming an
  upstream body — an SSE feed, a proxied download — had its body truncated
  at exactly the budget while chunks were still arriving (a control plane
  proxying its own SSE through a Worker `fetch` died at 120 s). The budget
  now bounds the connect, a streamed request body and the wait for the
  response head; the response body keeps streaming, and a body that makes
  no progress still ends at the HTTP stream idle deadline. That deadline is
  now the documented `CELLO_HTTP_STREAM_IDLE_S` knob (default 60 s, every
  received chunk refreshes it), so a black-holed upstream can never pin a
  connection. No data-format or behaviour change for short fetches.
- **The HTTP stream idle deadline is settable** (#136).
  `CELLO_HTTP_STREAM_IDLE_S` defaults to 60 and is validated at startup; a
  non-positive or malformed value stops the node instead of silently
  disabling the bound.

A Queue batch that a full consumer isolate refuses no longer spends a
delivery attempt. Under `CELLO_V8_EXTERNAL_LIMIT_MB` pressure the broker
counted each refusal as a delivery the consumer never ran, so a message
walked towards `max_retries` and the dead-letter queue untouched by
application code. The batch is now released with the settlement's own fence
and retried after a bounded backoff that ends in a charged attempt. The
refusal streak lives in the queue cell's existing metadata table:

- **A budget-refused Queue batch is released without spending an attempt**
  (#110). A dispatch whose batch does not fit the consumer isolate's
  `ArrayBuffer` budget — or that finds no stateless slot to run in — never
  executes a line of the handler, so it is no longer a failed delivery. The
  host reports the refusal back to the queue cell, which clears the lease in
  one transaction, leaves `attempts` untouched, and makes the rows visible
  again after a delay that doubles per consecutive refusal (one second up to
  five minutes) and starts over after a quiet spell longer than one maximum
  backoff, or a successful delivery. That backoff is what keeps a full
  isolate from being dispatched to in a loop, and it puts the refused rows
  behind whatever is already waiting. A handler that throws,
  `message.retry()`, `batch.retryAll()`, and a lease that expires unsettled
  still spend an attempt. If the release itself is lost, the lease expires
  and spends the attempt exactly as before, so no message is lost. The free
  release is bounded -- six consecutive refusals, or about a minute after the
  streak began -- so a batch that keeps being refused reaches `max_retries`
  and its dead-letter queue instead of cycling until retention deletes it. A
  batch whose bodies exceed the whole budget cannot be dispatched at all: the
  queue cell materializes the same bodies under the same cap, so the
  producer's `send` fails instead. A node that
  is out of capacity, rather than one isolate at its budget, waits from five
  seconds and on the same ladder, which now grows to five minutes instead of
  five seconds.

## v0.2.7

Five stream and body divergences from workerd are fixed, all found by the
workerd differential harness: `bodyUsed` reports a disturbed or consumed
body, `clone()` reports workerd's message, a body stream that yields a
non-byte chunk fails its consumers with a `TypeError`, a pull source runs
on workerd's schedule, and `tee()` cancels its source with workerd's reason.
A failed release attestation can now be re-run on the same tag. No
configuration, data, or bucket-format changes.

- **`request.bodyUsed` / `response.bodyUsed` observe the body stream**
  (#115). The flag was plain state that only the runtime's own consumers set,
  so a Worker that held a reader, or had read part of a live request body,
  still saw `false` -- and any guard that re-checks `bodyUsed` mid-stream
  before re-reading was bypassed. It is now the Fetch observation it should
  be: true once the body stream is disturbed (a read or a cancel through any
  reader, mounted mid-stream), or once the runtime has taken the body through
  `text()`, `arrayBuffer()`, `bytes()`, a subrequest forward, or
  `cache.put()`. Merely holding a reader is still not a disturbance, which is
  what workerd reports.
- **`clone()` of a used body reports workerd's `TypeError` message** (#116).
  Both `Response.clone()` and `Request.clone()` still refuse a consumed,
  disturbed or locked body, but the message is now workerd's
  `This ReadableStream is currently locked to a reader,` instead of
  `Body has already been consumed`, so a Worker that surfaces
  `error.message` reports what the same Worker reports on Cloudflare. Cello
  keeps refusing after a manual read plus `releaseLock()`, where workerd lets
  the clone through and hands both sides empty bytes.
- **A body stream that yields a non-byte chunk rejects with workerd's
  `TypeError`** (#117). `text()`, `json()`, `formData()`, `blob()`,
  `arrayBuffer()`, `bytes()`, or a subrequest body all funnel through one
  drain; a string (or any other non-`ArrayBuffer`/view)
  chunk there failed inside the byte arithmetic with
  `RangeError: offset is out of bounds`, which names neither the cause nor the
  fix. It now rejects with
  `TypeError: This ReadableStream did not return bytes.` before any data is
  returned, as workerd does. `ArrayBuffer` and every `ArrayBufferView` chunk
  are accepted, and the response/host path keeps streaming string chunks as
  their UTF-8 bytes, which is also what workerd does.
- **`Request.prototype.bytes()` and `Response.prototype.bytes()` exist**
  (#117). Fetch's byte accessor was missing; it returns the undecoded body as
  fresh `Uint8Array` and marks the body used, like the other consumers.

A failed `publish` run is retryable on the same tag, so a release no longer
costs a version when an attestation flakes. The pipeline now publishes the
immutable `<version>` image first, attests the image and the release files, and
only then moves `beta`/`latest` and creates the GitHub Release, so a failure
before the attestations leaves the channel tags and the Release untouched (the
`<version>` image is pullable as soon as it is published). `gh run rerun <run-id>
--failed` accepts an already-published `<version>` only when it is exactly the
index the run stitches from its own per-arch digests; a different image is
refused, a channel tag that already holds a newer version is never rolled back,
and a published `<version>` or Release is still never replaced. The publish
shell moved to `scripts/publish-release.sh`, with a stubbed-registry test for
the retry decisions.

A pull source runs on workerd's schedule, and a `tee()` cancels its source
with workerd's reason:

- **A pull source runs Workerd's pull schedule** (#113). After the first
  `read()` dequeued a buffered chunk, Cello had already called `pull()` a
  second time, so a source that prefetches, charges quota, or advances a
  cursor did that work one chunk earlier than the same code on Workerd.
  Cello no longer refills after a dequeue: `pull()` runs for a registered
  read request and from an `enqueue` that leaves the desired size positive,
  which is what Workerd's controller does (the WHATWG text also pulls after
  a dequeue; V8 does, Workerd does not). `read()` also settles one
  microtask later, mirroring Workerd's native read-result binding, so the
  pull that fulfils a pending read and its follow-up pull both run before
  the awaiting `read()` continues. The hop covers a byte stream's default
  reader and BYOB reads too, not just the default stream. The pull count and
  ordering now match Workerd, down to the per-read `pull()` counts a source
  observes.
- **`tee()` cancels its source with the last branch's reason** (#114). The
  source's `cancel` used to receive the spec's joined `[reason1, reason2]`
  array, so a source that stringified it saw `"first,second"` where Workerd
  passes the last branch's reason (`"second"`). The last cancelling branch's
  reason is now passed through unchanged, byte for byte, and the tee's
  internal error path no longer cancels with a duplicate array either.

## v0.2.6

The same code as v0.2.5, released again with complete provenance: the v0.2.5
publish run attested its image, but attesting the release tarballs failed
(the Sigstore transparency log answered 404), and a published version is
never re-run. Use v0.2.6.

## v0.2.5

An application WebSocket close now reaches a peer that is still sending, and
the release smoke gate's 32 MiB check is stable. No configuration, data, or
bucket-format changes.

- **A loud WebSocket close survives a peer that is still sending** (#120). When the
  edge closed a socket after writing its own 1009 (a message past the 32 MiB
  cap) or 1011 (a flooded pipe) while the peer's bytes sat unread in the
  receive buffer, dropping the transport made Linux answer with RST instead of
  FIN, and an RST can discard data the peer has not read yet -- including the
  close frame. A real client then saw an unclean 1006 or `ECONNRESET` instead
  of the close the edge sent; `check 4` of the release smoke gate failed this
  way in about 3 of 20 runs, and so did v0.2.3. Every path that drops a
  WebSocket transport after a close frame -- the loud 1009 and 1011, a cell's
  own close, and the echo of a peer's close -- now flushes the final frame,
  half-closes the write side (`shutdown(Write)`), and reads and discards what
  the peer still sends until EOF or a 2 s / 4 MiB bound, so the peer reads
  the close frame and then a clean EOF. The drain runs after the pump rather
  than in it; the flush, the half-close, and the drain share one 2 s deadline,
  every byte is discarded instead of buffered, at most 256 sockets drain at
  once node-wide (past that the frame is flushed and half-closed, then the
  transport drops at once), and a draining node skips the drain entirely, so
  no close dispatch or shutdown waits longer than that bound.
  The smoke client also no longer fails on the `EPIPE` that its own refused
  oversize write expects, while an `ECONNRESET` stays fatal.

## v0.2.4

A `TransformStream` now applies backpressure, a `tee()` bounds the skew
between its branches, compression streams no longer leak native memory, and
an isolate's `ArrayBuffer` memory is capped. No data or bucket-format
changes; `CELLO_V8_EXTERNAL_LIMIT_MB` is new and optional.

- **`TransformStream` applies backpressure** (#105). Its writable side used to
  resolve `write()` as soon as the transform returned, whether or not
  anything read the readable side, so a Worker streaming a
  `TransformStream` into a response buffered the whole body in the
  isolate's heap while its HTTP client did not read -- and `near_heap_limit`
  grows the heap rather than failing, so one non-reading client could run
  the process out of memory. The writable side now waits for the readable
  side to be read, as Workerd does: `await writer.write(...)` and
  `writer.ready` stay pending until a consumer pulls, end to end through
  the response-body pump. Cancelling the readable, aborting the writable,
  `controller.error()`, and `controller.terminate()` release a write parked
  on the handshake (with the stored error, or a `TypeError` for
  `terminate`), and an error or terminate issued from inside `start()` is
  applied as soon as the writable exists, so a later write rejects instead
  of parking. A producer that writes without awaiting either,
  or that writes past `writer.desiredSize` 0, still queues in memory;
  `writer.ready` is the signal to honor.
- **Compression streams release their native state** (#106). Every
  `CompressionStream` and `DecompressionStream` held its zlib state in one
  process-wide table that only a finished, cancelled, or aborted stream left.
  An abandoned stream, a stream whose input was not bytes, or a torn-down
  isolate kept its entry forever, so one tenant constructing streams in a loop
  could leak native memory for every tenant on the node. The table is now
  per isolate and goes with it. A stream left unreachable mid-flight is
  released when it is garbage-collected, and a transform error releases it
  too. An isolate holds at most 128 live compression streams; the next
  constructor call throws a `RangeError`.
- **A `tee()` bounds the skew between its branches** (#107). A Worker that
  read one branch of `response.body.tee()` -- or of a `Response.clone()` or
  `Request.clone()` of a streaming body it constructed -- to the end while
  the other branch went unread buffered the whole body in the isolate's
  heap, and `near_heap_limit` grows the heap rather than failing, so one
  tenant could run the shared process out of memory. The tee now charges
  each chunk to both branches; the difference between what they hold is
  measured just after a chunk is queued, and past 32 MiB -- the host tee's
  budget for platform bodies -- the branch that is behind fails its next
  read with `tee branch fell more than 32 MiB behind its sibling; read both
  branches concurrently`, while the branch being read keeps flowing to the
  end with the source uncancelled. The chunk size alone fails no branch,
  but a branch that has not read an oversize chunk yet holds all of it and
  fails for lag, so with chunks larger than the budget both branches must
  be read concurrently. A chunk is charged its size (2 bytes per UTF-16
  code unit for a string, its length for a buffer, its visible length for a
  view) and at least 64 bytes. That makes the budget a guard against
  accidental buffering rather than an isolate memory limit: a payload
  hidden in an object chunk is not counted, and the budget is per `tee()`,
  so a chain of N clones can hold N × 32 MiB.
- **A tee of a tee keeps its read path under the shared budget** (#111). The
  32 MiB budget one request's tees share picked its victim by buffered bytes
  alone, and the `HashMap` iteration order broke ties. In a chain that tie is
  the common case -- the inner `tee()`'s source and an unread copy hold
  exactly the same bytes until the inner pump runs -- and once the chain
  charges the same bytes at two stages the inner tee's source is often the
  largest holder outright, so the budget kept failing the copy its own tee
  was draining. The inner tee then treated that as its source ending and
  closed both of its copies: a Worker's `await copy.arrayBuffer()` (or a
  subrequest streaming a copy) got an empty or truncated body under load and
  no error at all, while the other copies carried on. The budget now ends a
  copy nobody has read from -- the one holding the most, when it gives back
  enough on its own -- and fails a copy a consumer is draining only when no
  unread copy does. A copy is spared once its consumer has taken a byte, and
  a copy an inner tee is draining is spared from the moment it is teed;
  before its first byte a copy a subrequest is streaming is
  indistinguishable from an unread copy, so a fetch streaming one can itself
  be the victim. Either way the failed copy rejects with `tee branch fell
  more than 32 MiB behind its sibling; read both branches concurrently` --
  including a read already in flight, and a send that a source termination
  races -- instead of ending the body as if it were complete, so a Worker
  never receives a truncated body without an error. The other copies keep
  flowing.
- **A failed `fetch()` reports its root cause** (#111). The error message used
  to be reqwest's wrapper (`error sending request for url (…)`); it now
  names the cause, such as `tcp connect error: Connection refused`,
  `operation timed out`, or the tee skew message when a streamed request body
  was a tee copy the budget ended. Code that matched the old wrapper text
  should match the cause instead.
- **An isolate's `ArrayBuffer` memory is capped** (#110). V8's default
  ArrayBuffer allocator is `malloc`, so `new ArrayBuffer(n)` in a Worker was
  an unbounded allocation inside the shared process: `near_heap_limit`
  extends the V8 heap limit and never fails the allocation, and backing
  stores are external memory rather than heap. One tenant looping
  `new Uint8Array(8 << 20)` grew the node by 640 MiB with a 128 MiB heap
  limit and was never refused (measured: RSS +643 MiB, isolate not
  condemned). Every isolate now has an allocator of its own that charges
  each backing store to a per-isolate budget and returns null past it, which
  V8 turns into a catchable `RangeError: Array buffer allocation failed`.
  The budget is `CELLO_V8_EXTERNAL_LIMIT_MB`, four heap limits by default
  (512 MiB), above the 64 MiB `/tmp` read, 32 MiB WebSocket message, 32 MiB
  tee skew and clone that one request legitimately holds. A refusal condemns
  the isolate the way a heap-limit hit does: it takes on no new retained
  state and serves again once it is back under 75 percent of both limits.
  Two refusals are evidence: a refused host store, and an allocation larger
  than the whole budget (which no collection can make room for). A smaller
  allocation that V8 collects and retries successfully never fails the
  guest, so it does not condemn. A collection returns freed buffers to the
  budget, so churn past the limit is never refused. Buffers the host hands
  to JavaScript -- response bodies, WebSocket messages, RPC arguments, Queue
  message bodies, `/tmp` reads, D1 blobs -- are charged to the same budget
  and refused the same way: a refused stream chunk rejects the read promise
  with the budget's `RangeError`, and a SQL row whose BLOB does not fit throws
  one `RangeError` and closes its native cursor, so the refused statement is not
  retained; when that row came from a write statement, the write had already
  been applied and the message says not to retry it. Backing stores are
  charged for the memory they retain, not the length the guest sees: a host
  `Vec` with spare capacity is charged its whole allocation. Resizable
  `ArrayBuffer`s, growable `SharedArrayBuffer`s and `WebAssembly.Memory` are
  **not** capped: V8 backs them with the platform page allocator, which no
  embedder hook can see, so Cello neither charges nor refuses them. A wasm
  memory is bounded per memory by
  `--wasm-max-mem-pages` (the whole budget in pages: every `grow` past it
  fails, including one performed by wasm code; a declared `maximum` above it
  is accepted but unreachable); the number of wasm memories, and every
  resizable or growable buffer, are bounded only by the node's memory
  pressure shedding.
  See [Limitations](reference/limits.md).

## v0.2.3

Streamed responses no longer die after 30 seconds. No configuration,
data, or bucket-format changes.

- **A streamed response body outlives the background budget** (#103).
  Since v0.1.0, `CELLO_BACKGROUND_BUDGET_S` (30 s) also ended the response
  body pump, so an SSE stream or any long download went silent about 30 s
  after its handler answered, with the socket left open. A body now streams
  for as long as the client stays connected and the body keeps producing.
  `ctx.waitUntil()` work is still ended at the budget, also while its
  entry streams.
- **A stalled stream ends after 60 seconds without progress.** That covers
  a body that produces nothing and a client that stops reading; the
  producer is cancelled through its request signal. Send a keepalive (an
  SSE comment) at least once a minute. Workerd keeps an idle body open.
- **A client disconnect no longer waits on the body's `cancel()`.** The
  host stream closes first, so a `cancel()` that never settles cannot pin
  the request.

## v0.2.2

Stream and WebSocket fixes, faster CI, and reorganized docs. No
configuration, data, or bucket-format changes.

- **A teed body no longer deadlocks** (#93). A Worker that read one
  `tee()` branch of a platform-streamed body (a `fetch()`, RPC, Durable
  Object, or R2 body) to the end before the other used to hang once 16
  chunks piled up on the unread side. The copies now buffer for each other
  up to 32 MiB of skew, shared by all of one request's tees. Past that, the
  lagging copy's next read rejects with `tee branch fell more than 32 MiB
  behind its sibling; read both branches concurrently`, and the node's
  memory stays bounded.
- **WebSocket closes report what happened** (#90). A peer close that
  arrives while a send is cut short reports `wasClean: false`. A backlog
  that tripped the 16 MiB budget wins over a racing close and ends with
  1011.
- **Faster CI** (#91, #94). Lint and the full suite run natively on amd64
  and arm64 under the same hostile env as before, in about 12 minutes
  instead of about 20. Docs-only pull requests skip the Rust jobs. A
  release tag publishes only after the `main` CI run on its commit has
  succeeded, so the publish job no longer re-runs the tests. The flaky
  `shutdown_during_parked_send_ends_pump` test is now deterministic.
- **Docs** (#92, #95–#100). The site is reorganized into Guides,
  Services, Concepts, and Reference, with a new quickstart, configuration
  and CLI references, and one page per service. Old URLs redirect.
- **Test harness** (#89). `CELLO_ESBUILD` reaches tests that run under
  the hermetic env.

## v0.2.1

WebSocket robustness. No configuration, data, or bucket-format changes.

- **Backpressure-safe WebSocket pipes** (#83, #87). The upgrade pump and
  the cell pipes no longer block on a full queue. A peer that stops reading
  is held to a 16 MiB backlog per direction, and the socket then ends
  loudly with 1011. A single message of up to 32 MiB still passes, whatever
  smaller backlog is ahead of it. Closes stay observable while a send is
  parked, and teardown is bounded. A slow but live peer or a slow handler
  is never timed out.
- **WebSocket messages are capped at 32 MiB** (#85), the Cloudflare
  Workers limit, whether sent as one frame or as fragments. A peer that
  streams continuation frames past the cap gets close code 1009 instead
  of growing the node's memory without bound, and the cell sees
  `(1009, reason, false)`. An over-cap frame is refused from its header.
  The reader holds under about 100 MiB per socket while messages are read;
  `docs/cloudflare-compat.md` states the full per-direction bounds.

## v0.2.0

Simple backward compatibility with
[denoland/celld](https://github.com/denoland/celld): swap the binary or
image on the same bucket. Stop every old node first — the peer protocol
is new (`cello-tunnel`, `x-cello-overload`, `cello-log-stream`, new
overload marker), so a mixed fleet is unsupported — then start 0.2.0.
Rename every `CELLD_*` variable to `CELLO_*`; the node refuses to start
otherwise, fail-closed.

Persisted identifiers are now written with the `cello` spelling and read
with both: `__cello-runtime`, the `__cello$*` stub envelopes (rows
written before the binding-isolation patches still decode as plain
data), `cello-r2` metadata and `cello-r2-cursor-*` cursors,
`cello-schema` / `cello-retention`, and the `cello-image:` prefix. Data
an older node wrote carries over with no migration step and no rewrite
pass. In-process dunders and the wire identifiers were renamed outright.
See [Compatibility](guides/upgrade.md).


## v0.1.0

First Cello release. The renamed product: repo, crate, binary, and CLI
are `cello`; `CELLD_*` is `CELLO_*`; `.celld/dev` is `.cello/dev`; the
health path, telemetry names, and prose say Cello. Bucket objects, cell
SQLite, stored values, and the node-to-node wire stay `celld`-spelled
(see [Compatibility with celld](guides/upgrade.md)). Includes everything
below; no behavior change beyond the rename.

Releases `v0.6.3-ryuzcorp.4` through `.13` shipped as celld, so their
entries below use the historical spellings they shipped with —
`CELLD_*`, `.celld/dev`, and the `celld` binary — not the current
`CELLO_*` names.

## v0.6.3-ryuzcorp.13

Adds `0079`–`0136`. No bucket format bump and no wire protocol change
between nodes, so `.12` and `.13` nodes can share a bucket; the only new
bucket-side artifact is the `log/<node>.loss.json` loss record, which
older nodes read past, and a downgrade restores everything `.13`
wrote. Default behavior changes: the node-session id is pinned in the
data dir (`0092`), `service.version` carries the operator's application
version instead of Cello's build (`0132`), and WebSocket close codes
1001–1014 (other than the reserved no-status codes) now close instead
of throwing (`0113`).

New CLI: `celld versions [SCRIPT]` lists deployed versions (`0123`);
`celld d1 --local` runs execute, export, and migrations against the
running `celld dev` (`0121`); `celld dev --state DIR` (or
`CELLD_DEV_STATE`) keeps local state outside the project (`0124`).

- Runtime and cancellation: `0079`, `0080`, `0081`, `0083`, `0084`,
  `0085`, `0086`, `0087`, `0118` settle shared, waiter-owned, and
  background work loudly instead of hanging; `CELLD_MAX_CELLS_PER_ISOLATE`
  caps cells per isolate (default 32) (`0135`);
  `CELLD_BACKGROUND_BUDGET_S` ends post-response background work past
  the budget (default 30 s) without failing the answered request
  (`0101`); a poisoned cell restarts on a fresh isolate (`0104`);
  `AbortSignal.timeout` frees its slot (`0106`); the budget-orphaned
  workflow drive drops on retry (`0125`); failed covered alarms disarm
  while uncovered ones re-arm at scan pace (`0127`).
- Fleet and durability: `0088`–`0096` bound seal proof, renewal
  readbacks, alarm deadlines, tail streaming, lease conflicts, loss
  declaration, ensemble resumes, and wake timing; `0103` collects
  record-less fragments; `0105` prunes live epochs after unclean
  restarts; `0126` retries transient bucket write conflicts; `0129`
  routes dead-owner takeovers with capacity.
- Storage: `0098`, `0105`, `0099`, `0122`, `0100` cover compaction
  retention, unclean-restart pruning, the single-writer dev store,
  indexed local LISTs, and the repaired facet-image import;
  `CELLD_SQL_STATEMENT_MS` bounds per-statement time (unset means
  unbounded) (`0128`); hydration counting is O(1) (`0102`).
- Workers compatibility: `0109`–`0117`, `0108`, `0107`, `0130`, `0113`;
  `0119` refuses foreign-`script_name` bindings at deploy time; `0131`
  reserves the host-injected `CELLD_NODE` name.
- Telemetry: `0132`–`0134`, `0136`.
- New env knobs and their defaults: `CELLD_BACKGROUND_BUDGET_S` (30),
  `CELLD_MAX_CELLS_PER_ISOLATE` (32), `CELLD_SQL_STATEMENT_MS` (unset),
  `CELLD_TMP_MAX_BYTES` (67108864, 64 MiB), `CELLD_OTEL_LOGS` (unset),
  `OTEL_RESOURCE_ATTRIBUTES` (unset), `CELLD_DEV_STATE`
  (`PROJECT/.celld/dev`), `CELLD_NODE` (generated and pinned to
  `node-id` in the data dir).

## v0.6.3-ryuzcorp.12

Adds `0076`–`0078`. No API, CLI, or bucket format change, so `.11` and
`.12` nodes share a bucket and a downgrade restores everything `.12`
wrote.

- An eviction skips the full-database handoff snapshot when the cell's
  restorable chain was born in this epoch, every acked row is already
  per-cell, and the chain holds at most 16 objects (`0078`, modelled on
  ClickHouse MergeTree's lazy merges). Measured on 200 agents × 6
  writes, bucket posture, release builds: Class A 1934 → 1734 (−10%),
  PUT 1400 → 1200, write p50 unchanged at 210 ms. On 50 agents: Class A
  582 → 532. Restored-from-older-epoch cells, group-commit rows not yet
  per-cell, and longer chains still snapshot as before.
- `tools/s3bench` defaults to the release binary and refuses to start
  while another run is up (`0076`).
- Test nodes boot on ephemeral ports, removing a port-reuse flake
  (`0077`).

## v0.6.3-ryuzcorp.11

Adds `0058`–`0067`, `0069`–`0072`, and `0075` (`0068`, `0073`, `0074`
parked unmerged). No API or CLI change. A `.11` node publishes bucket
format 3, which `.10` nodes read, so `.10` and `.11` nodes share a
bucket.

- Bucket-posture group commit (`0059`–`0064`, `0070`, `0071`) is in the
  binary but off by default (`0075`); `CELLD_BUCKET_BUNDLES=1` turns it
  on. It needs every live lease to read format 3 (every node on `.11`)
  and falls back to per-cell uploads at once otherwise. Measured on 200
  agents × 6 writes: Class A 1934 → 1140 with grouping on, but write-ack
  p50 210 → 445 ms; on a hot cell, p50 235 → 432 ms. Enable it only
  where request cost matters more than write latency. `.10` recovers a
  bucket `.11` wrote bundles into (whole-prefix fold, no loss
  declared), so a downgrade is safe.
- An eviction proves a journaled epoch with one HEAD instead of a
  prefix LIST (`0061`).
- The C heap is trimmed sooner after evictions (`0065`,
  `CELLD_C_TRIM_MB`, default 8). Post-drain RSS fell ~10 MiB on the
  agent-50 shape.
- `CELLD_SQLITE_CACHE_KB` caps the per-connection SQLite page cache
  (`0066`; default 2000 KiB is unchanged).
- A deployment generation projects its `/bundle` tree once instead of
  once per isolate (`0067`).
- `tools/s3bench` (`0058`, `0069`, `0072`) measures bucket request
  counts and per-write latency against a local S3 for cost claims.

## v0.6.3-ryuzcorp.10

Adds `0054`–`0057`, all bucket-request and storage savings; no format
or API change, so `.9` and `.10` nodes share a bucket.

- A bucket-posture eviction makes fewer requests (`0054`). An eviction
  that publishes a handoff snapshot then deletes, in the background,
  the per-cell L0/L1 objects the snapshot supersedes — one single-key
  `DeleteObject` per key (`0057`). The bucket credential needs
  `s3:DeleteObject` on `cells/`, which the read-write state grant
  already covers; a failed delete only leaves garbage behind and never
  fails the eviction.
- Concurrent bucket-posture writes on one cell share ownership reads
  (`0056`): fewer `cells/<cell>/own.json` GETs on hot cells at the same
  acknowledgement rule.
- A fleet-posture node with no peer it could ever recruit backs its
  `nodes/` scan off to the 30 s maintenance tick (`0055`), so a node
  that starts next to it is recruited within ~30 s instead of ~1 s (the
  new node recruits the old one itself at once).

## v0.6.3-ryuzcorp.9

Adds `0052`–`0053`.

- No behavior change with `CELLD_DEPLOY_AWS_*` unset. A platform that
  sets the trio must give the deployment credential read access to both
  `deploy/` and `deploy-blobs/`, and keep the ordinary `AWS_*` grant
  read-write over the state prefixes (`cells/`, `log/`, `nodes/`,
  `fleet/`, `wake/`, `drain/`, `kv/`, `r2/`, `probe/`, `telemetry/`).
  The runtime also GETs `wake-format.json` and LISTs `wake-v2/` and
  `wake-retired-v2/` (one read-only prefix grant over `wake-` covers
  it), and lists and deletes the `node-cells/` markers (that prefix
  stays read-write). The deploy command keeps the ordinary credential
  with write access to the deployment prefixes. A control-plane node,
  managed (`CELLD_CLOUD`) or BYO bucket alike, must not set the trio:
  it publishes `deploy/` itself, so it refuses the trio at boot.
- A serving process is non-dumpable: no core dump comes out of a node,
  and a non-root debugger or profiler cannot attach (`ptrace`
  refused); root tools are unaffected. Anything reading the node's
  `/proc/<pid>/environ`, `mem`, or `maps` as the fleet uid now gets
  `EACCES` (`0053`). The window between `execve` and `main` setting the
  flag is still dumpable, and spawned children start with an emptied
  environment (`PATH`, `HOME`, `TMPDIR` only), so no credential reaches
  them.

## v0.6.3-ryuzcorp.8

Adds `0051`: `celld deploy --read-root DIR` (repeatable) names
directories beyond the project that esbuild's inputs and
`containers[].image` Dockerfiles with their build contexts may come
from. A platform that deploys a built subdirectory of a checkout, or a
monorepo app whose packages are hoisted above its config, names the
checkout. Without the flag, `.7`'s project-only bounds are unchanged.

## v0.6.3-ryuzcorp.7

Adds `0043`–`0050`.

- `celld deploy` and `celld d1` refuse a project path that resolves
  outside the project directory — the config file's own directory
  (`0050`). That covers the config, `main`, assets, Dockerfiles and
  build contexts, `migrations_dir`, Python sources, and every file
  esbuild bundles. A monorepo that hoists `node_modules` above that
  directory no longer bundles: install the packages inside it.
  `migrations_dir` entries must be regular files, and a
  `containers[].image` that is an absolute or `..` path is refused.
- On Linux, esbuild runs under Landlock with reads confined to the
  project, so `CELLD_ESBUILD` (or `esbuild` on `PATH`) must be the
  self-contained release binary, not a wrapper script (`0050`). A
  kernel or seccomp profile without Landlock runs it unconfined with a
  warning; Docker's, containerd's, and podman's default profiles allow
  it.
- `celld r2 head` and `celld r2 get --json` print a
  `{"error":"not_found",…}` object on stdout for an absent key (`0044`);
  exit code and stderr unchanged.
- `celld --version` reports the full build version (`0046`).
- New, opt-in: `CELLD_CACHE_SCOPE=deployment` (`0045`); `celld invoke`
  runs one request against a staged version before `celld activate`
  (`0048`), with `--header-env` for secret headers (`0049`);
  `celld deploy --sourcemap` collects a `no_bundle` entry's sibling map
  (`0047`).
- `celld invoke` needs every node on this release. A stream received
  over RPC now stays open while the receiver holds it (`0043`), ending
  the spurious aborts of signal-carrying container execs.

## v0.6.3-ryuzcorp.6

Adds `0009`–`0042`.

- Upgrade every node of a fleet together. A rolling upgrade moves cells
  with no migration (`0037`), but cross-version traffic fails until
  every node runs this release: operator calls through an old node fail
  closed on new nodes, Queue DLQ and redrive forwards fail in both
  directions, and stubs sent from an old node revive as plain data on a
  new one (`0040`, `0041`). The Queue alarm replays the durable DLQ and
  redrive marks once the fleet converges.
- Storage rows holding stubs written before the upgrade read back as
  plain data (Durable Object stubs as null). Get each stub again from
  its namespace or binding and store it again to write the new envelope
  (`0040`); storing the read-back value does not restore a stub.
- Workers now fail when they reach a resource their script never
  bound, touch another cell's storage, call D1, KV, or Queue operator
  methods through stubs, or use a socket id outside its owning cell or
  request (`0022`, `0040`, `0041`, `0042`). Declare every binding a
  Worker uses, and run operator actions through the `cello` CLIs.
- The Workers Cache API takes up to 64 MiB of memory per node by
  default; `CELLD_CACHE_MAX_BYTES=0` turns it off (`0018`).
- Shared D1, KV, and Queue cells move to a runtime pool that `/state`
  reports as `__celld-runtime` (`0037`).
- Opt-in hardening stays off by default: `CELLD_EGRESS` is `open`
  (`0021`), `CELLD_FENCE_NODE_NETNS` is `off` (`0014`),
  `CELLD_MEMORY_SCOPE` is `container` (`0011`), and
  `CELLD_OPERATOR_TOKEN` is unset (`0013`).
- Set `CELLD_OPERATOR_TOKEN` only once every node runs this release:
  older nodes send no token, so a mixed fleet with the token set fails
  internal calls with 401 (`0013`). Set it in the environment of every
  `cello` CLI too; liveness and readiness probes use the public health
  endpoint.
- Queue rows whose dead-letter source was forged before the upgrade can
  still be redriven to the name they carry; planting such rows is
  closed by `0041`, redrive is operator-only, and retention ages them
  out.

## v0.6.3-ryuzcorp.5

Adds `0007`–`0008` (durable Object RPC across isolates; container exec
`exitCode` survival).

## v0.6.3-ryuzcorp.4

Patches `0001`–`0006`: per-container image selection, exec waiting on
its start, `node:crypto` scrypt, streaming-body `Response.clone()`,
service-binding RPC stubs, and container exec pty. First release
published by the fork.
