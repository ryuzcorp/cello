(function() {
// ReadableStream (WHATWG subset)

// Shadow Object.prototype.then with a non-enumerable
// undefined on every read result so user overrides of
// Object.prototype.then can't intercept `{ value, done }`
// when it flows through user code's await. Non-enumerable
// keeps it invisible to for..in + assert_object_equals.
function _readResult(value, done) {
  const r = { value, done };
  Object.defineProperty(r, 'then', {
    value: undefined,
    enumerable: false,
    writable: true,
    configurable: true,
  });
  return r;
}

// Workerd hands a read result to JS across its native binding, so the
// promise `read()` returns settles one microtask after the stream produced
// the value: the binding's promise conversion is an extra `.then`. That hop
// is observable whenever a pull fulfils the pending read: the pull's own
// continuation (which re-runs CallPullIfNeeded via [[pullAgain]]) is queued
// before the awaiting read resumes, so a source that enqueues from `pull`
// sees two pulls by the time the second `read()` continues (issue #113).
// Mirror the hop on every read result.
function _readSettled(promise) {
  return promise.then((result) => result);
}

// Brand-check sentinel for internal construction.
const _rsc_brand = Symbol("ReadableStreamController");

const RELEASED =
  "This ReadableStream reader has been released.";

// The skew budget one tee applies: bytes its branches may hold queued and
// unconsumed between them before the lagging one fails. It is the host
// tee's budget and message (HTTP_TEE_MAX_SKEW_BYTES and
// HTTP_TEE_SKEW_MESSAGE in http_streams.rs), so a body exposes one bound
// however it was teed; a unit test there pins the pair to this file, since
// a prelude script cannot import a Rust constant.
const _TEE_MAX_SKEW_BYTES = 33554432;
const _TEE_SKEW_MESSAGE =
  "tee branch fell more than 32 MiB behind its sibling; read both branches concurrently";

// What one queued chunk costs the budget. Every chunk is charged, and no
// chunk type is free:
//  * a view (typed array or DataView): its visible `byteLength` -- the
//    window it presents, not the backing buffer it keeps alive;
//  * ArrayBuffer / SharedArrayBuffer: its `byteLength`;
//  * string: 2 bytes per UTF-16 code unit, which is what V8 stores;
//  * anything else (an object, an array, a number, ...): a fixed floor
//    standing in for V8's object header plus the queue slots. Its contents
//    are deliberately not sized: a tee must not run a chunk's getters or
//    proxy traps, and a producer that wants to buffer a payload inside
//    objects needs no tee to do it. The floor still caps a queue of small
//    objects and primitives, and it applies to every chunk type, so empty
//    strings and zero-length buffers are not free.
//
// The budget is a guard against accidental buffering by honest code, not
// an isolate memory limit: this charge can be smaller than what a chunk
// actually retains (a 1 MiB view over a 64 MiB buffer costs 1 MiB -- and a
// 1-byte view the 64-byte floor -- and a payload hidden in an object costs
// the floor).
const _TEE_OBJECT_BYTES = 64;

// The probe must never run chunk code: a chunk may be a Proxy, or an
// object whose prototype is ArrayBuffer.prototype, and a native getter on
// the wrong receiver throws. Capture the intrinsics once, at prelude
// evaluation before any user code runs, and call them with the captured
// Reflect.apply inside try/catch, so the measurement is total and no trap
// or Get runs on the chunk.
const _teeApply = Reflect.apply;
const _teeIsView = ArrayBuffer.isView;
const _teeArrayBufferByteLength = Object.getOwnPropertyDescriptor(
  ArrayBuffer.prototype, 'byteLength').get;
const _teeTypedArrayByteLength = Object.getOwnPropertyDescriptor(
  Object.getPrototypeOf(Uint8Array.prototype), 'byteLength').get;
const _teeDataViewByteLength = Object.getOwnPropertyDescriptor(
  DataView.prototype, 'byteLength').get;
const _teeSharedArrayBufferByteLength =
  typeof SharedArrayBuffer === 'undefined' ? undefined :
    Object.getOwnPropertyDescriptor(
      SharedArrayBuffer.prototype, 'byteLength').get;

// The length a captured getter reports for `value`, or undefined when the
// getter rejects the receiver (a fake ArrayBuffer, a Proxy, a revoked
// Proxy) or reports something that is not a non-negative finite number.
function _teeMeasuredBytes(getter, value) {
  if (getter === undefined) return undefined;
  try {
    const n = _teeApply(getter, value, []);
    if (typeof n === 'number' && Number.isFinite(n) && n >= 0)
      return n;
  } catch (e) {
    // Not an instance of that builtin: fall through to the floor.
  }
  return undefined;
}

function _teeChunkBytes(chunk) {
  let bytes = _TEE_OBJECT_BYTES;
  if (typeof chunk === 'string') {
    bytes = chunk.length * 2;
  } else if (chunk !== null && typeof chunk === 'object') {
    let measured;
    try {
      if (_teeApply(_teeIsView, undefined, [chunk])) {
        // A typed array or DataView. A Proxy of one is not a view, so it
        // never reaches these getters.
        measured = _teeMeasuredBytes(_teeTypedArrayByteLength, chunk);
        if (measured === undefined)
          measured = _teeMeasuredBytes(_teeDataViewByteLength, chunk);
      } else {
        measured = _teeMeasuredBytes(_teeArrayBufferByteLength, chunk);
        if (measured === undefined)
          measured = _teeMeasuredBytes(
            _teeSharedArrayBufferByteLength, chunk);
      }
    } catch (e) {
      measured = undefined;
    }
    if (measured !== undefined) bytes = measured;
  }
  return bytes < _TEE_OBJECT_BYTES ? _TEE_OBJECT_BYTES : bytes;
}

// ---- private stream state -------------------------------------------------
// Every field of a ReadableStream lives in a per-instance state object held
// only by this module's WeakMap: the stream's own properties and every
// internal operation stay out of reach of tenant code (#170, #206). The
// other stream preludes (byte_streams.js, identity_transform_stream.js) and
// the harness read the same state through `__cello.$$readableState`, which
// no Worker can reach.
class ReadableStreamState {
  constructor() {
    this.queue = [];
    // Parallel array of chunk sizes (always 1 when no
    // strategy.size is set) and the running total, for
    // desiredSize accounting per spec.
    this.queueSizes = [];
    this.queueTotalSize = 0;
    this.closed = false;
    this.closeRequested = false;
    this.errored = false;
    this.error = null;
    this.locked = false;
    this.source = null;
    this.controller = null;
    this.reader = null;
    this.readRequests = [];
    this.hwm = 1;
    this.pulling = false;
    this.pullAgain = false;
    this.started = false;
    // Optional observer run after a read dequeues a queued chunk (a layer
    // such as TransformStream releases its writable-side backpressure).
    this.onDequeue = null;
    // Closed promise + resolver.
    this.closedPromise = undefined;
    this.closedResolve = undefined;
    this.closedReject = undefined;
    this.sizeFn = undefined;
    this.pullFn = undefined;
    this.startFn = undefined;
    this.cancelFn = undefined;
    // Workerd extension: a source may declare its total byte length;
    // fetch-style consumers read it for Content-Length.
    this.expectedLength = undefined;
    this.disturbed = false;
    // Workerd's internal (native) streams reject a pending read() when the
    // stream is cancelled instead of resolving {done: true} (compression
    // streams opt in).
    this.cancelRejectsReads = false;
    // Workerd's internal streams settle a pending read() only after the
    // isolate drains its microtasks, so a cancel() called synchronously
    // after the write that produced the chunk still rejects the read. JS
    // streams settle it inside the enqueue (compression streams opt in,
    // #252).
    this.deferReads = false;
    this.readDrainQueued = false;
    // The byte-stream controller (byte_streams.js) or internal-transform
    // controller (identity_transform_stream.js) layered on this stream, and
    // the materialized-body cursor the BYOB reader turns into a controller.
    this.byteCtl = undefined;
    this.ictl = undefined;
    this.buffered = undefined;
    // Per-kind overrides for getReader/cancel, installed by the module that
    // owns the controller so the shared prototype path stays unbranched.
    this.readerHook = null;
    this.cancelHook = null;
  }
}

const _readableStates = new WeakMap();
const __rsState = (stream) => {
  const state = _readableStates.get(stream);
  // A forged receiver (a plain object, or a Proxy of a stream) has no state
  // and must fail like the private slot it stands in for.
  if (state === undefined)
    throw new TypeError("Illegal invocation");
  return state;
};
// Captured: the host storage predicate calls this while a guest may have
// patched `WeakMap.prototype.has` or `Reflect.apply`.
const __rsStatesHas = WeakMap.prototype.has;
const __rsBrandApply = Reflect.apply;
const __rsIs = (value) =>
  ((typeof value === 'object' && value !== null) ||
   typeof value === 'function')
    ? __rsBrandApply(__rsStatesHas, _readableStates, [value]) : false;

// Resolve one pending read request from queue.
function __resolveRead(st) {
  // An internal stream settles its reads on the microtask drain, so a
  // cancel() or an error() in the same turn wins over the chunk just
  // enqueued (#252).
  if (st.deferReads) {
    __settleReadsLater(st);
    return;
  }
  __drainReads(st);
}

function __settleReadsLater(st) {
  if (st.readDrainQueued) return;
  st.readDrainQueued = true;
  Promise.resolve().then(() => {
    st.readDrainQueued = false;
    __drainReads(st);
  });
}

function __drainReads(st) {
  if (st.errored) {
    // Workerd's internal streams reject the read with an Error built from
    // the reason the stream died of (#252).
    const error = st.cancelRejectsReads ? __cancelError(st.error) : st.error;
    for (const r of st.readRequests)
      r.reject(error);
    st.readRequests = [];
    return;
  }
  while (st.readRequests.length > 0 &&
         st.queue.length > 0) {
    const r = st.readRequests.shift();
    const chunk = st.queue.shift();
    const sz = st.queueSizes.shift() ?? 0;
    st.queueTotalSize -= sz;
    if (st.queueTotalSize < 0) st.queueTotalSize = 0;
    r.resolve(_readResult(chunk, false));
  }
  if (st.queue.length === 0 &&
      st.closeRequested)
    __finishClose(st);
}

function __finishClose(st) {
  if (st.closed) return;
  st.closed = true;
  // Resolve pending reads with done.
  for (const r of st.readRequests)
    r.resolve(_readResult(undefined, true));
  st.readRequests = [];
  if (st.closedResolve) {
    st.closedResolve();
    st.closedResolve = null;
  }
}

function __rejectClosed(st, e) {
  if (st.closedReject) {
    st.closedReject(e);
    st.closedReject = null;
    st.closedResolve = null;
  }
}

function __maybePull(st) {
  if (!st.started || st.closed ||
      st.errored || st.closeRequested)
    return;
  if (!st.pullFn) return;
  // Spec ShouldCallPull: a registered read request pulls regardless of
  // backpressure, then a positive desiredSize does. Workerd checks the
  // same two signals. desiredSize is the strategy highWaterMark minus
  // the queued *bytes*, not the queued chunk count: a strategy.size
  // (tee branches charge real chunk bytes) must hold back a pull that
  // still leaves the queue over its mark.
  if (st.readRequests.length === 0 &&
      st.hwm - st.queueTotalSize <= 0)
    return;
  if (st.pulling) {
    st.pullAgain = true;
    return;
  }
  st.pulling = true;
  let result;
  try {
    result = st.pullFn.call(
      st.source, st.controller);
  } catch (e) {
    st.pulling = false;
    st.controller.error(e);
    return;
  }
  Promise.resolve(result).then(
    () => {
      st.pulling = false;
      if (st.pullAgain) {
        st.pullAgain = false;
        __maybePull(st);
      }
    },
    (e) => {
      st.pulling = false;
      st.controller.error(e);
    }
  );
}

// Workerd's internal streams reject a pending read with an `Error` built
// from the cancellation reason: an `Error` reason keeps its class, message
// and own enumerable properties on a fresh instance, `undefined` gets
// workerd's own text, and anything else is stringified (#252).
function __cancelError(reason) {
  if (reason instanceof Error) {
    let copy;
    try {
      copy = new reason.constructor(reason.message);
    } catch (e) {
      copy = new Error(reason.message);
    }
    // The reason is tenant code: a Proxy may throw from `ownKeys`, and a
    // copied property may be read-only on the copy.
    let keys = [];
    try {
      keys = Object.keys(reason);
    } catch (e) {}
    for (const key of keys) {
      try {
        copy[key] = reason[key];
      } catch (e) {}
    }
    return copy;
  }
  if (reason === undefined) return new Error("Stream was cancelled.");
  return new Error(String(reason));
}

// Discard queued chunks and transition to closed. The shared tail of
// cancel() and the internal-stream/byte-stream cancel paths.
function __cancelStream(stream, st, reason) {
  // Spec section 4.2.6: cancel discards queued chunks and
  // transitions to closed. Without this, read() hits
  // the `queue.length > 0` branch before the `closed`
  // check and keeps yielding stale values.
  st.queue = [];
  st.queueSizes = [];
  st.queueTotalSize = 0;
  st.disturbed = true;
  // Workerd's internal (native) streams reject a pending
  // read() when the stream is cancelled; spec JS streams
  // resolve it {done: true}. Compression streams opt in
  // to the internal behavior (compression_streams.js), and
  // the rejection carries an Error built from the reason
  // (#252).
  if (st.cancelRejectsReads) {
    const error = __cancelError(reason);
    for (const r of st.readRequests)
      r.reject(error);
    st.readRequests = [];
  }
  __finishClose(st);
  if (st.cancelFn) {
    let result;
    try {
      result = st.cancelFn.call(
        st.source, reason);
    } catch (e) {
      return Promise.reject(e);
    }
    return Promise.resolve(result)
      .then(() => undefined);
  }
  return Promise.resolve(undefined);
}

// The internal cancel path shared by the reader and the stream: the byte and
// internal-stream modules own their own queue and install a hook.
function __cancelReadable(stream, reason) {
  const st = __rsState(stream);
  return st.cancelHook !== null
    ? st.cancelHook(stream, reason)
    : __cancelStream(stream, st, reason);
}

class ReadableStreamDefaultController {
  #stream;
  #closeRequested = false;
  #errored = false;
  constructor(brand, stream) {
    if (brand !== _rsc_brand)
      throw new TypeError(
        "ReadableStreamDefaultController cannot be " +
        "constructed directly");
    this.#stream = stream;
  }
  enqueue(chunk) {
    const st = __rsState(this.#stream);
    if (this.#closeRequested || this.#errored ||
        st.closed)
      throw new TypeError(
        "Cannot enqueue a closed/errored stream");
    // Run size() if the strategy provides one. A throw
    // or invalid return (non-finite, negative) errors
    // the stream and rethrows from enqueue. Size's own
    // controller.error(...) wins if it was called first.
    let size = 1;
    if (st.sizeFn) {
      try { size = st.sizeFn(chunk); }
      catch (e) { this.error(e); throw e; }
      // Workerd's message for a non-number size --
      // e.g. an async size() returning a promise.
      if (typeof size !== 'number') {
        const err = new TypeError(
          "The value cannot be converted because " +
          "it is not an integer.");
        this.error(err);
        throw err;
      }
      if (!Number.isFinite(size) || size < 0) {
        const err = new RangeError(
          "invalid strategy size: " + size);
        this.error(err);
        throw err;
      }
    }
    st.queue.push(chunk);
    st.queueSizes.push(size);
    st.queueTotalSize += size;
    __resolveRead(st);
    __maybePull(st);
  }
  close() {
    const st = __rsState(this.#stream);
    if (this.#closeRequested || this.#errored ||
        st.closed)
      throw new TypeError("Already closed");
    this.#closeRequested = true;
    st.closeRequested = true;
    if (st.queue.length === 0)
      __finishClose(st);
    else
      __resolveRead(st);
  }
  error(e) {
    if (this.#errored) return;
    this.#errored = true;
    const st = __rsState(this.#stream);
    st.error = e;
    st.errored = true;
    st.queue = [];
    st.queueSizes = [];
    st.queueTotalSize = 0;
    // Reject closed first (spec order).
    __rejectClosed(st, e);
    // Then reject pending reads. An internal stream defers the rejection to
    // the microtask drain so a cancel() later in the same turn rejects the
    // read with its own Error instead (#252).
    if (st.deferReads) {
      __settleReadsLater(st);
      return;
    }
    for (const r of st.readRequests)
      r.reject(e);
    st.readRequests = [];
  }
  get desiredSize() {
    if (this.#errored) return null;
    if (this.#closeRequested) return 0;
    const st = __rsState(this.#stream);
    return st.hwm -
           st.queueTotalSize;
  }
}

class ReadableStreamDefaultReader {
  #stream;
  #released = false;

  constructor(stream) {
    if (!__rsIs(stream))
      throw new TypeError(
        "ReadableStreamDefaultReader requires a " +
        "ReadableStream argument");
    const st = __rsState(stream);
    if (st.locked)
      throw new TypeError(
        "This ReadableStream is currently locked to a reader.");
    this.#stream = stream;
    st.locked = true;
    st.reader = this;
  }

  read() {
    if (this.#released)
      return _readSettled(Promise.reject(
        new TypeError(RELEASED)));
    const s = __rsState(this.#stream);
    s.disturbed = true;
    if (s.errored)
      return _readSettled(Promise.reject(s.error));
    // Check closed first: a stream that was closed
    // re-entrantly (e.g. close() called from sizeFn) may
    // still have a stuck chunk in its queue which must
    // never be emitted. Only drain the queue while the
    // stream is still readable.
    if (s.closed)
      return _readSettled(Promise.resolve(
        _readResult(undefined, true)));
    if (s.queue.length > 0) {
      const chunk = s.queue.shift();
      const sz = s.queueSizes.shift() ?? 0;
      s.queueTotalSize -= sz;
      if (s.queueTotalSize < 0) s.queueTotalSize = 0;
      if (s.queue.length === 0 && s.closeRequested)
        __finishClose(s);
      // A stream layered on this one can need the desired-size change a
      // dequeue causes even though no pull runs. TransformStream's readable
      // side is such a layer: workerd's transform controller updates its
      // writable-side backpressure from the queue itself (a native
      // backpressure listener), so a read that frees a queue slot must
      // release a parked write here too (see writable_stream.js).
      if (s.onDequeue) s.onDequeue();
      // Workerd does not refill after dequeuing a buffered chunk: a pull
      // runs only for a registered read request (below) or from an
      // enqueue. The spec's [[PullSteps]] calls CallPullIfNeeded here,
      // which is why V8 reports one more pull per read. Matching workerd
      // keeps a source's side effects (a prefetch, a quota charge, a
      // cursor advance) on workerd's schedule -- issue #113.
      return _readSettled(Promise.resolve(
        _readResult(chunk, false)));
    }
    // Queue a read request.
    return _readSettled(new Promise((resolve, reject) => {
      s.readRequests.push({ resolve, reject });
      __maybePull(s);
    }));
  }

  releaseLock() {
    if (!this.#released) {
      const s = __rsState(this.#stream);
      // Reject pending read requests per spec.
      for (const r of s.readRequests)
        r.reject(new TypeError(RELEASED));
      s.readRequests = [];
      s.locked = false;
      s.reader = null;
      this.#released = true;
    }
  }

  get closed() {
    if (this.#released)
      return Promise.reject(
        new TypeError(RELEASED));
    return __rsState(this.#stream).closedPromise;
  }

  cancel(reason) {
    if (this.#released)
      return Promise.reject(
        new TypeError(RELEASED));
    return __cancelReadable(this.#stream, reason);
  }
}

class ReadableStream {
  #s;

  constructor(underlyingSource, queuingStrategy) {
    const st = this.#s = new ReadableStreamState();
    _readableStates.set(this, st);
    st.closedPromise = new Promise((res, rej) => {
      st.closedResolve = res;
      st.closedReject = rej;
    });
    // WHATWG Streams treats [[closedPromise]] as internal
    // state that only surfaces when the user queries
    // reader.closed / stream's closedPromise. Our
    // Promise is a real JS Promise, so rejecting it when
    // no one has attached a .catch fires V8's unhandled-
    // rejection signal. Attach a no-op sink to mark it
    // handled at the engine level; user `await` on
    // reader.closed still sees the rejection because the
    // promise is shared.
    st.closedPromise.catch(() => {});
    // Spec: extract strategy fields first. Size is read
    // and validated before highWaterMark; an invalid size
    // must beat an invalid highWaterMark. hwm stays
    // undefined when defaulted: byte streams default to 0
    // where default streams use 1.
    let hwm;
    let sizeFn;
    if (queuingStrategy !== undefined) {
      const s = queuingStrategy.size;
      if (s !== undefined && typeof s !== 'function')
        throw new TypeError(
          'strategy.size must be a function');
      sizeFn = s;
      const h = queuingStrategy.highWaterMark;
      if (h !== undefined) {
        const n = Number(h);
        // Workerd throws TypeError here, not the spec's
        // RangeError.
        if (Number.isNaN(n) || n < 0)
          throw new TypeError(
            'highWaterMark must be non-negative');
        hwm = n;
      }
    }
    st.hwm = hwm === undefined ? 1 : hwm;
    st.sizeFn = sizeFn;
    if (underlyingSource === null ||
        (underlyingSource !== undefined &&
         typeof underlyingSource !== 'object'))
      throw new TypeError("Invalid source");
    const src = underlyingSource || {};
    // Workerd extension: a source may declare its total
    // byte length; fetch-style consumers read it for
    // Content-Length (see also FixedLengthStream).
    if (src.expectedLength !== undefined)
      st.expectedLength = Number(src.expectedLength);
    if (src.type !== undefined) {
      const t = String(src.type);
      if (t !== 'bytes')
        throw new TypeError(
          `Invalid type '${t}'`);
      // Byte-stream machinery lives in a lazy prelude;
      // naming the global compiles it on first use. It
      // installs the byte controller on this stream, so
      // nothing below (default controller, pull wiring)
      // runs.
      ReadableByteStreamController;
      __cello.$$setupByteStream(this, src, hwm);
      return;
    }
    // Read each getter once per spec -- extracting
    // pull/start/cancel algorithms happens at
    // construction, not on every invocation.
    const pullFn = src.pull;
    const startFn = src.start;
    const cancelFn = src.cancel;
    if (cancelFn !== undefined &&
        typeof cancelFn !== 'function')
      throw new TypeError(
        "cancel must be a function");
    if (pullFn !== undefined &&
        typeof pullFn !== 'function')
      throw new TypeError(
        "pull must be a function");
    if (startFn !== undefined &&
        typeof startFn !== 'function')
      throw new TypeError(
        "start must be a function");
    st.source = src;
    st.startFn = startFn;
    st.pullFn = pullFn;
    st.cancelFn = cancelFn;
    st.controller =
      new ReadableStreamDefaultController(
        _rsc_brand, this);
    if (!st.startFn) {
      // Workerd starts a source with no start()
      // synchronously (mirrors the byte controller and
      // the writable's sink), so a strategy size() runs
      // before the constructor returns.
      st.started = true;
      __maybePull(st);
      return;
    }
    // A sync start() throw must error the stream, not
    // escape the constructor.
    let startResult;
    try {
      startResult =
        st.startFn.call(st.source, st.controller);
    } catch (e) {
      startResult = Promise.reject(e);
    }
    Promise.resolve(startResult).then(
      () => {
        st.started = true;
        __maybePull(st);
      },
      (e) => {
        st.controller.error(e);
      }
    );
  }

  getReader(options) {
    const st = this.#s;
    if (st.readerHook !== null) return st.readerHook(this, options);
    if (options !== undefined && options !== null) {
      if (options.mode !== undefined)
        throw new TypeError(
          `Invalid reader mode '${options.mode}'`);
    }
    return new ReadableStreamDefaultReader(this);
  }

  get locked() { return this.#s.locked; }

  // The internal cancel path: the byte and internal-stream modules install
  // their own implementation because they own the queue that has to be
  // discarded.
  #cancel(reason) {
    const st = this.#s;
    if (st.cancelHook !== null) return st.cancelHook(this, reason);
    return __cancelStream(this, st, reason);
  }

  cancel(reason) {
    // Workerd names the locked stream the same way pipeTo and pipeThrough
    // do (#269).
    if (this.#s.locked)
      return Promise.reject(new TypeError(
        "This ReadableStream is currently locked to a reader."));
    return this.#cancel(reason);
  }

  tee() {
    // Branches of a byte stream are byte streams, so
    // they support BYOB reads; the source is still read
    // through a default reader (coalesced chunks).
    const isBytes = this.#s.byteCtl !== undefined;
    const reader = this.getReader();
    let closed = false;
    let canceled1 = false, canceled2 = false;
    // A branch the skew budget failed. It is not cancelled: its reads
    // reject with the budget error, the other branch keeps flowing, and
    // the source is not cancelled while that one is live.
    let failed1 = false, failed2 = false;
    let reason1, reason2;
    let reading = false;
    // The charge of the chunk the in-flight pull is enqueuing, computed
    // once per chunk before either enqueue (which may transfer a buffer).
    // A default branch's size strategy reads it back, so the budget and
    // the branch's `queueTotalSize` agree and a chunk is never measured
    // twice.
    let chunkCharge = _TEE_OBJECT_BYTES;
    const live1 = () => !canceled1 && !failed1;
    const live2 = () => !canceled2 && !failed2;

    // Both branches are gone (cancelled or failed), so nothing can read
    // this tee again: cancel the source with the reason the last removed
    // branch used, instead of holding its reader forever. Workerd's own
    // tee passes that last reason through unchanged, not the spec's
    // joined `[reason1, reason2]` array -- a source that branches on the
    // cancellation reason would otherwise read "first,second" here and
    // "second" there. Workerd's cancel settles a branch immediately
    // rather than waiting for the other, and the source is cancelled only
    // once both are gone -- the same here.
    function release(reason) {
      if (closed || live1() || live2()) return;
      closed = true;
      return reader.cancel(reason);
    }

    const makeBranch = (first) => {
      const src = {
        pull: pullAlgorithm,
        cancel: (reason) => {
          if (first) { canceled1 = true; reason1 = reason; }
          else { canceled2 = true; reason2 = reason; }
          return release(reason);
        },
      };
      if (isBytes) {
        src.type = 'bytes';
        return new ReadableStream(src);
      }
      // Size a default branch's chunks in the budget's own bytes, so its
      // `queueTotalSize` -- which the reader maintains on every dequeue
      // -- is exactly what the branch holds unconsumed. A bytes strategy
      // may not carry size(), and a byte branch already counts bytes.
      return new ReadableStream(src, { size: () => chunkCharge });
    };
    const branch1 = makeBranch(true);
    const branch2 = makeBranch(false);
    const controllers = [
      branch1.#s.controller,
      branch2.#s.controller,
    ];
    // Bytes a branch holds queued and unconsumed: a byte branch keeps the
    // total on its controller, a default branch on the stream.
    const held = (branch) => {
      const st = branch.#s;
      return st.byteCtl !== undefined
        ? __cello.$$byteControllerState(st.byteCtl).queueTotalSize
        : st.queueTotalSize;
    };

    function failBranch(first) {
      if (first) { failed1 = true; reason1 = _TEE_SKEW_MESSAGE; }
      else { failed2 = true; reason2 = _TEE_SKEW_MESSAGE; }
      // The read that fails is the branch nobody drained, not the code
      // that teed, so the message names the fix. Same `Error` and text as
      // the host tee's rejection.
      (first ? controllers[0] : controllers[1])
        .error(new Error(_TEE_SKEW_MESSAGE));
    }

    // Fail a branch only for actual lag: it holds more queued bytes than a
    // live sibling and that difference, measured just after the chunk is
    // queued, is past the budget. The leading branch is never failed and
    // one chunk can fail at most one branch. The chunk size alone fails no
    // branch, but a branch that has not read an oversize chunk yet holds
    // all of it and fails for lag, so with chunks larger than the budget
    // both branches must be read concurrently -- the host tee's total-based
    // check would fail both regardless (see docs/reference/limits.md).
    // Returns whether a branch is still live; with one branch left there is
    // no sibling to be behind, so it keeps everything and the source is not
    // cancelled.
    function failLagging() {
      for (;;) {
        if (!live1() && !live2()) {
          const released = release(
            reason2 !== undefined ? reason2 : reason1);
          if (released !== undefined) released.catch(() => {});
          return false;
        }
        if (!live1() || !live2()) return true;
        const firstHeld = held(branch1);
        const secondHeld = held(branch2);
        if (firstHeld > secondHeld &&
            firstHeld - secondHeld > _TEE_MAX_SKEW_BYTES) {
          failBranch(true);
          continue;
        }
        if (secondHeld > firstHeld &&
            secondHeld - firstHeld > _TEE_MAX_SKEW_BYTES) {
          failBranch(false);
          continue;
        }
        return true;
      }
    }

    function pullAlgorithm() {
      if (reading || closed) return Promise.resolve();
      reading = true;
      return reader.read().then(
        ({ value, done }) => {
          reading = false;
          try {
            if (done) {
              if (closed) return;
              // The source has ended: the tee is finished whatever a
              // branch does next, and its `cancel` must not run.
              closed = true;
              // A close can throw for a branch only. A byte branch whose
              // BYOB read left a partial element on an odd-length tail
              // throws from close() (per spec); the sibling must still
              // close, and only the throwing branch is errored.
              try {
                if (live1()) controllers[0].close();
              } catch (e) {
                if (live1()) controllers[0].error(e);
              }
              try {
                if (live2()) controllers[1].close();
              } catch (e) {
                if (live2()) controllers[1].error(e);
              }
              return;
            }
            chunkCharge = _teeChunkBytes(value);
            // Byte enqueue transfers the chunk's buffer,
            // so when both branches are live the first
            // gets a copy.
            if (live1())
              controllers[0].enqueue(isBytes && live2()
                ? new Uint8Array(value) : value);
            if (live2()) controllers[1].enqueue(value);
            // Measure the lag after both enqueues: a branch whose reader
            // was already waiting has drained the chunk synchronously, so
            // the difference between the two totals is what the branches
            // really hold, and an unread branch fails once it is past the
            // budget even when the chunk that got it there is oversize.
            failLagging();
          } catch (e) {
            // No chunk and no stream state may strand a branch: whatever
            // threw (a byte enqueue rejecting a chunk, a controller
            // closed underneath the tee, a failing strategy), both
            // branches get the error, the tee is done, and the source
            // reader is released.
            if (closed) return;
            closed = true;
            if (live1()) controllers[0].error(e);
            if (live2()) controllers[1].error(e);
            // One reason, as workerd's tee does: no composite array.
            const released = reader.cancel(e);
            if (released !== undefined) released.catch(() => {});
          }
        },
        (e) => {
          reading = false;
          if (!closed) {
            closed = true;
            if (live1()) controllers[0].error(e);
            if (live2()) controllers[1].error(e);
          }
        }
      );
    }

    // Error propagation from original stream.
    reader.closed.catch(e => {
      if (!closed) {
        closed = true;
        if (live1()) controllers[0].error(e);
        if (live2()) controllers[1].error(e);
      }
    });

    return [branch1, branch2];
  }

  pipeTo(destination, options) {
    if (!__rsIs(this))
      return Promise.reject(new TypeError(
        "pipeTo called on non-ReadableStream"));
    if (destination === null ||
        typeof destination !== 'object' ||
        !(destination instanceof WritableStream))
      return Promise.reject(new TypeError(
        "pipeTo destination must be WritableStream"));
    // Read options eagerly (getters may have
    // side effects).
    let preventClose = false;
    let preventAbort = false;
    let preventCancel = false;
    let signal;
    if (options != null) {
      preventClose = !!options.preventClose;
      preventAbort = !!options.preventAbort;
      preventCancel = !!options.preventCancel;
      signal = options.signal;
    }
    const st = this.#s;
    // Workerd's pipeTo texts, including its writable one without the
    // trailing period that getWriter uses (#245).
    if (st.locked)
      return Promise.reject(new TypeError(
        "This ReadableStream is currently locked to a reader."));
    if (__cello.$$writableStreamState(destination).locked)
      return Promise.reject(new TypeError(
        "This WritableStream is currently locked to a writer"));
    // A destination a previous pipe already closed is refused up front
    // with workerd's message, instead of failing the first write (#269).
    if (__cello.$$writableStreamState(destination).state === 'closed')
      return Promise.reject(new TypeError(
        "This destination writable stream is closed."));
    if (signal && signal.aborted) {
      const err = signal.reason || new DOMException(
        "The operation was aborted", "AbortError");
      if (!preventCancel) this.#cancel(err);
      if (!preventAbort) destination.abort(err);
      return Promise.reject(err);
    }
    // Per spec, pipeTo immediately marks the source as
    // disturbed so bodyUsed / similar observers flip
    // synchronously even before the first read fires.
    st.disturbed = true;
    const reader = this.getReader();
    const writer = destination.getWriter();
    let shuttingDown = false;
    let currentWrite = Promise.resolve();
    // The first write failure the pipe saw (#272): the pipe rejects with
    // it once the destination's closed promise settles, instead of with
    // the destination's error. Undefined while every write succeeded.
    let writeError;
    // The pipe's own close failure on source-done (#272): the closed
    // watcher below turns it into the pipe's rejection.
    let closeError;
    return new Promise((resolve, reject) => {
      let abortListener;
      if (signal) {
        abortListener = () => {
          const err = signal.reason ||
            new DOMException(
              "The operation was aborted",
              "AbortError");
          shutdownWith(err, !preventAbort,
            !preventCancel);
        };
        signal.addEventListener(
          'abort', abortListener);
      }
      function finish(isError, error) {
        reader.releaseLock();
        writer.releaseLock();
        if (signal && abortListener)
          signal.removeEventListener(
            'abort', abortListener);
        if (isError) reject(error);
        else resolve(undefined);
      }
      function shutdownWith(
        err, doAbort, doCancel) {
        if (shuttingDown) return;
        shuttingDown = true;
        // Wait for in-flight write to complete.
        currentWrite.then(doActions, doActions);
        function doActions() {
          const actions = [];
          if (doAbort)
            actions.push(writer.abort(err).then(
              () => {}, ae => { err = ae; }));
          if (doCancel)
            actions.push(reader.cancel(err)
              .catch(() => {}));
          Promise.all(actions)
            .then(() => finish(true, err));
        }
      }
      // A failed write rejects the pipe with the write's own error, while
      // the source is still cancelled with the destination's error (#272):
      // a readable cancel fails a parked TransformStream write with a
      // TypeError naming the readable side, but the source sees the cancel
      // reason. The destination's error is only known once its closed
      // promise rejects, which always follows a failed write.
      function shutdownWithWrite(
        cancelErr, rejectErr, doCancel) {
        if (shuttingDown) return;
        shuttingDown = true;
        currentWrite.then(doActions, doActions);
        function doActions() {
          if (!doCancel) {
            finish(true, rejectErr);
            return;
          }
          reader.cancel(cancelErr).catch(() => {}).then(
            () => finish(true, rejectErr));
        }
      }
      // A destination that errors while the pipe has no write outstanding
      // cancels the source with the error and fulfills, as Workerd does
      // (#272): a TransformStream whose readable is cancelled after its
      // writes drained, or a controller.error() on an idle pipe. (With
      // preventCancel there is nothing to wait on, so the pipe fulfills
      // once the write settles; Workerd itself leaves that corner pending,
      // which this implementation deliberately does not copy.)
      function shutdownFulfill(cancelErr, doCancel) {
        if (shuttingDown) return;
        shuttingDown = true;
        currentWrite.then(doActions, doActions);
        function doActions() {
          if (!doCancel) {
            finish(false);
            return;
          }
          reader.cancel(cancelErr).then(
            () => finish(false),
            () => finish(false));
        }
      }
      // Watch for source error.
      reader.closed.catch(re => {
        if (shuttingDown) return;
        shutdownWith(re, !preventAbort, false);
      });
      // Watch for dest error.
      writer.closed.catch(we => {
        if (shuttingDown) return;
        if (closeError !== undefined)
          shutdownWith(closeError, false, !preventCancel);
        else if (writeError !== undefined)
          shutdownWithWrite(we, writeError, !preventCancel);
        else
          shutdownFulfill(we, !preventCancel);
      });
      function waitForReady() {
        if (shuttingDown) return;
        writer.ready.then(pumpRead, () => {});
      }
      function pumpRead() {
        if (shuttingDown) return;
        reader.read().then(({ value, done }) => {
          if (shuttingDown) return;
          if (done) {
            if (!preventClose) {
              // A failed close rejects the pipe with the close's own
              // error; the destination's closed promise always settles
              // after it, and its watcher above does the shutdown.
              writer.close().then(
                () => finish(false),
                e => { closeError = e; });
            } else {
              finish(false);
            }
            return;
          }
          // A failed write does not pump again: the destination errored,
          // so its closed promise (and the watcher above) will shut the
          // pipe down with this write's own error (#272). Only the first
          // failure is recorded, so the pipe reports the write that the
          // destination failed first.
          const writeP = writer.write(value).then(
            () => {},
            e => { if (writeError === undefined) writeError = e; });
          currentWrite = writeP;
          writeP.then(() => {
            if (shuttingDown) return;
            waitForReady();
          });
        }, () => {});
      }
      waitForReady();
    });
  }

  pipeThrough(transform, options) {
    if (!__rsIs(this))
      throw new TypeError(
        "pipeThrough called on non-ReadableStream");
    if (typeof transform !== 'object' ||
        transform === null)
      throw new TypeError(
        "pipeThrough argument must be an object");
    if (!(transform.readable instanceof
          ReadableStream))
      throw new TypeError(
        "transform.readable must be ReadableStream");
    if (!(transform.writable instanceof
          WritableStream))
      throw new TypeError(
        "transform.writable must be WritableStream");
    if (this.#s.locked)
      throw new TypeError(
        "This ReadableStream is currently locked to a reader.");
    if (__cello.$$writableStreamState(transform.writable).locked)
      throw new TypeError(
        "This WritableStream is currently locked to a writer.");
    // Spec: mark the pipeTo promise as handled --
    // pipeTo already mirrors errors between the two
    // streams via the readable's error state, so a
    // rejection here would otherwise surface as an
    // unhandled rejection.
    this.pipeTo(transform.writable, options)
      .catch(() => {});
    return transform.readable;
  }

  async *values(options) {
    const preventCancel =
      !!(options && options.preventCancel);
    const reader = this.getReader();
    let drained = false;
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) { drained = true; return; }
        yield value;
      }
    } finally {
      // WHATWG Streams 4.5.3: the default async
      // iterator's return() must cancel the stream
      // before releasing the reader lock -- early
      // termination of the loop (break / return /
      // throw inside `for await`) was leaking the
      // upstream source. Cancel is a no-op once the
      // stream's `done` has been observed, so we
      // skip it on natural drain.
      if (!drained && !preventCancel) {
        try { await reader.cancel(); } catch { /* swallow -- cancel errors don't block release */ }
      }
      reader.releaseLock();
    }
  }
}

// Spec: @@asyncIterator is the same function object as
// values().
ReadableStream.prototype[Symbol.asyncIterator] =
  ReadableStream.prototype.values;

ReadableStream.from = function(asyncIterable) {
  let iterator, nextMethod, isAsync;
  // Check async iterator first, then sync.
  if (asyncIterable != null &&
      typeof asyncIterable[Symbol.asyncIterator]
        === 'function') {
    iterator =
      asyncIterable[Symbol.asyncIterator]();
    if (typeof iterator !== 'object' ||
        iterator === null)
      throw new TypeError(
        "@@asyncIterator must return an object");
    nextMethod = iterator.next;
    isAsync = true;
  } else if (asyncIterable != null &&
             typeof asyncIterable[Symbol.iterator]
               === 'function') {
    iterator = asyncIterable[Symbol.iterator]();
    if (typeof iterator !== 'object' ||
        iterator === null)
      throw new TypeError(
        "@@iterator must return an object");
    nextMethod = iterator.next;
    isAsync = false;
  } else {
    throw new TypeError(
      "ReadableStream.from() requires an iterable");
  }

  let cancelInProgress = false;
  return new ReadableStream({
    async pull(controller) {
      let resultP;
      try {
        resultP = nextMethod.call(iterator);
      } catch (e) {
        controller.error(e);
        throw e;
      }
      const result = isAsync
        ? await resultP : resultP;
      if (typeof result !== 'object' ||
          result === null) {
        controller.error(new TypeError(
          "iterator result must be an object"));
        throw new TypeError(
          "iterator result must be an object");
      }
      if (result.done) {
        controller.close();
        return;
      }
      // Await value even for sync iterators
      // (handles promise-valued sync iterables).
      const v = isAsync
        ? result.value : await result.value;
      controller.enqueue(v);
    },
    cancel(reason) {
      if (cancelInProgress)
        return Promise.resolve();
      cancelInProgress = true;
      if (iterator.return === undefined)
        return Promise.resolve();
      if (typeof iterator.return !== 'function')
        return Promise.reject(new TypeError(
          "iterator.return is not a function"));
      let result;
      try {
        result = iterator.return(reason);
      } catch (e) {
        return Promise.reject(e);
      }
      return Promise.resolve(result).then(r => {
        if (typeof r !== 'object' || r === null)
          throw new TypeError(
            "return() must return an object");
      });
    }
  }, { highWaterMark: 0 });
};

// Workerd reflects both strategies with an enumerable `highWaterMark`
// accessor and an enumerable `size` accessor that returns the algorithm
// (a prototype `size()` method would report the wrong descriptor kind).
const _countSize = function() { return 1; };
// Optional chain: size(undefined) returns undefined
// rather than throwing (Workerd behavior).
const _byteLengthSize = function(chunk) { return chunk?.byteLength; };

class CountQueuingStrategy {
  #hwm;
  constructor({ highWaterMark }) {
    this.#hwm = highWaterMark;
  }
  get highWaterMark() { return this.#hwm; }
  get size() { return _countSize; }
}

class ByteLengthQueuingStrategy {
  #hwm;
  constructor({ highWaterMark }) {
    this.#hwm = highWaterMark;
  }
  get highWaterMark() { return this.#hwm; }
  get size() { return _byteLengthSize; }
}

globalThis.ReadableStream = ReadableStream;
globalThis.ReadableStreamDefaultReader =
  ReadableStreamDefaultReader;
globalThis.ReadableStreamDefaultController =
  ReadableStreamDefaultController;
globalThis.CountQueuingStrategy = CountQueuingStrategy;
globalThis.ByteLengthQueuingStrategy =
  ByteLengthQueuingStrategy;

// The runtime's own modules read a stream's private state and close it out
// through these hooks; tenant code cannot reach `__cello`.
__cello.$$readableState = __rsState;
// Brand for the clone codec: a constructed stream, not a forged prototype.
__cello.$$isReadableStream = __rsIs;
__cello.$$finishCloseStream = (stream) =>
  __finishClose(__rsState(stream));
__cello.$$rejectClosedStream = (stream, e) =>
  __rejectClosed(__rsState(stream), e);
__cello.$$cancelReadableStream = (stream, reason) =>
  __cancelReadable(stream, reason);
// The Error an internal stream's pending read rejects with when it is
// cancelled; writable_stream.js records it for a compression stream's
// writable (#252).
__cello.$$cancelError = __cancelError;

// WebIDL reflection (enumerable members, data tag, hidden constructor) for
// every interface this file defines (#171, #206).
for (const [cls, name] of [
  [ReadableStream, 'ReadableStream'],
  [ReadableStreamDefaultReader,
    'ReadableStreamDefaultReader'],
  [ReadableStreamDefaultController,
    'ReadableStreamDefaultController'],
  [CountQueuingStrategy, 'CountQueuingStrategy'],
  [ByteLengthQueuingStrategy,
    'ByteLengthQueuingStrategy'],
]) {
  __cello.$$webidl(cls, name);
}
})();
