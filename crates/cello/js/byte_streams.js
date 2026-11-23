// Readable byte streams (WHATWG subset): ReadableByteStreamController,
// ReadableStreamBYOBReader, ReadableStreamBYOBRequest.
//
// Compiled on first access (see LAZY_GLOBALS); streams.js names the
// ReadableByteStreamController global when a source declares
// `type: 'bytes'`, so a bundle with no byte streams never pays for this
// file. Every field lives in a private state object (this module's WeakMap)
// and streams.js reaches it through `__cello.$$byteControllerState`; the
// stream itself keeps a controller backref in its own private state, so no
// guest-visible object carries an internal field or method (#170, #206).
//
// Three deliberate Workerd deviations from current spec text: a read on
// a default reader drains the whole byte queue as one coalesced chunk,
// a source with no start() is started synchronously (mirroring the
// writable's sink handling), and close() settles parked BYOB reads (see
// closeCommit) instead of leaving them for respond(0).
return (() => {

const _brand = Symbol('ReadableByteStreamController');
const INVALIDATED =
  'This ReadableStreamBYOBRequest has been invalidated.';
const RELEASED =
  'This ReadableStream reader has been released.';

// Same shape streams.js returns from reads: shadow `then` so a user
// override of Object.prototype.then cannot intercept the result.
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

// A byte read settles one microtask after the stream produced the value,
// exactly like the default reader's: workerd's native binding converts the
// read result through an extra promise hop, so the pull that fulfilled the
// read can run its follow-up before the awaiting read resumes. This module
// is a separate prelude file, so the helper is repeated rather than
// imported (see `_readSettled` in streams.js).
function _readSettled(promise) {
  return promise.then((result) => result);
}

const __rsState = (stream) => __cello.$$readableState(stream);

// ---- controller state -----------------------------------------------------
class ByteControllerState {
  constructor(stream) {
    this.stream = stream;
    // Byte queue: {buffer, byteOffset, byteLength} slices.
    this.queue = [];
    this.queueTotalSize = 0;
    this.closeRequested = false;
    // Pull-into descriptors. `readerType` is 'byob' (req carries the
    // read's resolve/reject) or 'default' (an autoAllocate read whose
    // request sits in the stream's readRequests).
    this.pendingPullIntos = [];
    this.byobRequest = null;
    this.autoAlloc = undefined;
    this.hwm = 0;
    this.source = null;
    this.pullFn = undefined;
    this.cancelFn = undefined;
    this.started = false;
    this.pulling = false;
    this.pullAgain = false;
    // The ReadableByteStreamController a source's pull/start sees; the state
    // itself is private and has no methods.
    this.self = null;
  }
}

const _controllerStates = new WeakMap();
const __bcState = (controller) => {
  const state = _controllerStates.get(controller);
  if (state === undefined)
    throw new TypeError("Illegal invocation");
  return state;
};

// ---- BYOB request state ---------------------------------------------------
const _requestStates = new WeakMap();

// ---- readers --------------------------------------------------------------
const _byobReaderStates = new WeakMap();

class ReadableStreamBYOBRequest {
  #s;
  constructor(brand, controller, view) {
    if (brand !== _brand)
      throw new TypeError(
        'ReadableStreamBYOBRequest cannot be constructed directly');
    this.#s = { controller, view };
    _requestStates.set(this, this.#s);
  }
  get view() { return this.#s.view; }
  // Workerd extension: the remaining bytes the pending read still needs.
  get atLeast() {
    const state = this.#s;
    if (state.controller === null) return 0;
    const d = __bcState(state.controller).pendingPullIntos[0];
    if (d === undefined) return 0;
    const remaining = d.minFill - d.bytesFilled;
    return remaining > 0 ? remaining : 0;
  }
  respond(bytesWritten) {
    const state = this.#s;
    if (state.controller === null) throw new TypeError(INVALIDATED);
    const n = Number(bytesWritten);
    if (!Number.isInteger(n) || n < 0)
      throw new TypeError('invalid bytesWritten');
    __respond(__bcState(state.controller), n);
  }
  respondWithNewView(view) {
    const state = this.#s;
    if (state.controller === null) throw new TypeError(INVALIDATED);
    if (!ArrayBuffer.isView(view))
      throw new TypeError('view must be an ArrayBufferView');
    __respondWithNewView(__bcState(state.controller), view);
  }
}

function __invalidateRequest(state) {
  const r = state.byobRequest;
  if (r === null) return;
  const request = _requestStates.get(r);
  request.controller = null;
  request.view = null;
  state.byobRequest = null;
}

// Workerd: a default-reader read drains the whole byte queue as one chunk
// (a single entry is handed back without a copy).
function __drain(state) {
  const q = state.queue;
  let out;
  if (q.length === 1) {
    const d = q[0];
    out = new Uint8Array(d.buffer, d.byteOffset, d.byteLength);
  } else {
    out = new Uint8Array(state.queueTotalSize);
    let off = 0;
    for (const d of q) {
      out.set(
        new Uint8Array(d.buffer, d.byteOffset, d.byteLength), off);
      off += d.byteLength;
    }
  }
  state.queue = [];
  state.queueTotalSize = 0;
  __handleDrain(state);
  return out;
}

function __handleDrain(state) {
  if (state.queueTotalSize === 0 && state.closeRequested)
    __closeCommit(state);
}

// Workerd settles parked BYOB reads when the stream closes: a partially
// filled (min/readAtLeast) descriptor commits what it got with done still
// false, an empty one hands its buffer back as a zero-length done view.
// (Spec would leave them for respond(0); a respond after this sees no
// request and is a no-op.)
function __closeCommit(state) {
  const pending = state.pendingPullIntos;
  state.pendingPullIntos = [];
  __invalidateRequest(state);
  __cello.$$finishCloseStream(state.stream);
  for (const d of pending)
    if (d.req) d.req.resolve(_readResult(
      new d.ctor(d.buffer, d.byteOffset,
                 d.bytesFilled / d.elementSize),
      d.bytesFilled === 0));
}

function __error(state, e) {
  const s = __rsState(state.stream);
  if (s.closed || s.errored) return;
  state.queue = [];
  state.queueTotalSize = 0;
  const pending = state.pendingPullIntos;
  state.pendingPullIntos = [];
  __invalidateRequest(state);
  s.error = e;
  s.errored = true;
  // Reject closed first (spec order), then reads.
  __cello.$$rejectClosedStream(state.stream, e);
  for (const r of s.readRequests) r.reject(e);
  s.readRequests = [];
  for (const d of pending) if (d.req) d.req.reject(e);
}

// FillPullIntoDescriptorFromQueue: copies up to the descriptor's
// capacity, committing only element-aligned bytes once minFill is
// reachable. Returns whether the descriptor is ready to commit.
function __fill(state, d) {
  const max = Math.min(state.queueTotalSize,
                       d.byteLength - d.bytesFilled);
  const maxFilled = d.bytesFilled + max;
  const aligned = maxFilled - (maxFilled % d.elementSize);
  let remaining = max;
  let ready = false;
  if (aligned >= d.minFill) {
    remaining = aligned - d.bytesFilled;
    ready = true;
  }
  const dest = new Uint8Array(d.buffer);
  while (remaining > 0) {
    const head = state.queue[0];
    const n = Math.min(remaining, head.byteLength);
    dest.set(new Uint8Array(head.buffer, head.byteOffset, n),
             d.byteOffset + d.bytesFilled);
    if (n === head.byteLength) state.queue.shift();
    else { head.byteOffset += n; head.byteLength -= n; }
    state.queueTotalSize -= n;
    d.bytesFilled += n;
    remaining -= n;
  }
  return ready;
}

function __commit(state, d, done) {
  const view = new d.ctor(d.buffer, d.byteOffset,
                          d.bytesFilled / d.elementSize);
  if (d.readerType === 'default') {
    const r = __rsState(state.stream).readRequests.shift();
    if (r) r.resolve(_readResult(view, done));
  } else {
    d.req.resolve(_readResult(view, done));
  }
}

function __process(state) {
  while (state.pendingPullIntos.length > 0 &&
         state.queueTotalSize > 0) {
    const d = state.pendingPullIntos[0];
    if (!__fill(state, d)) return;
    state.pendingPullIntos.shift();
    __commit(state, d, false);
  }
}

function __shouldPull(state) {
  const s = __rsState(state.stream);
  if (!state.started || !state.pullFn || s.closed || s.errored ||
      state.closeRequested)
    return false;
  if (s.readRequests.length > 0) return true;
  if (state.pendingPullIntos.length > 0) return true;
  return state.hwm - state.queueTotalSize > 0;
}

function __pull(state) {
  if (!__shouldPull(state)) return;
  if (state.pulling) {
    state.pullAgain = true;
    return;
  }
  state.pulling = true;
  let result;
  try { result = state.pullFn.call(state.source, state.self); }
  catch (e) {
    state.pulling = false;
    __error(state, e);
    return;
  }
  Promise.resolve(result).then(
    () => {
      state.pulling = false;
      if (state.pullAgain) {
        state.pullAgain = false;
        __pull(state);
      }
    },
    (e) => {
      state.pulling = false;
      __error(state, e);
    });
}

function __pullInto(state, view, min) {
  const s = __rsState(state.stream);
  const elementSize = view.BYTES_PER_ELEMENT ?? 1; // DataView: 1
  const d = {
    buffer: null,
    bufferLen: 0,
    byteOffset: view.byteOffset,
    byteLength: view.byteLength,
    bytesFilled: 0,
    elementSize,
    minFill: min * elementSize,
    ctor: view.constructor,
    readerType: 'byob',
    req: null,
  };
  try { d.buffer = view.buffer.transfer(); }
  catch (e) { return Promise.reject(e); }
  d.bufferLen = d.buffer.byteLength;
  if (state.pendingPullIntos.length > 0)
    return new Promise((resolve, reject) => {
      d.req = { resolve, reject };
      state.pendingPullIntos.push(d);
    });
  if (s.closed)
    return Promise.resolve(_readResult(
      new d.ctor(d.buffer, d.byteOffset, 0), true));
  if (state.queueTotalSize > 0) {
    if (__fill(state, d)) {
      const filled = new d.ctor(d.buffer, d.byteOffset,
                                d.bytesFilled / elementSize);
      __handleDrain(state);
      return Promise.resolve(_readResult(filled, false));
    }
    if (state.closeRequested) {
      const e = new TypeError('Insufficient bytes to fill elements');
      __error(state, e);
      return Promise.reject(e);
    }
  }
  const p = new Promise((resolve, reject) => {
    d.req = { resolve, reject };
    state.pendingPullIntos.push(d);
  });
  __pull(state);
  return p;
}

function __respond(state, n) {
  const s = __rsState(state.stream);
  const d = state.pendingPullIntos[0];
  if (s.closed) {
    if (n !== 0)
      throw new TypeError(
        'bytesWritten must be zero on a closed stream');
  } else {
    if (n === 0)
      throw new TypeError('bytesWritten must be non-zero');
    if (d.bytesFilled + n > d.byteLength)
      throw new RangeError('bytesWritten exceeds the view');
  }
  __respondInternal(state, d, n);
}

function __respondWithNewView(state, view) {
  const s = __rsState(state.stream);
  const d = state.pendingPullIntos[0];
  if (!(view.buffer instanceof ArrayBuffer))
    throw new TypeError(
      'view must be over a detachable ArrayBuffer');
  if (s.closed) {
    if (view.byteLength !== 0)
      throw new TypeError(
        'view must be zero-length on a closed stream');
  } else if (view.byteLength === 0) {
    throw new TypeError('view must be non-zero-length');
  }
  if (d.byteOffset + d.bytesFilled !== view.byteOffset)
    throw new RangeError(
      'view byteOffset does not match the request');
  if (d.bufferLen !== view.buffer.byteLength)
    throw new RangeError('view buffer does not match the request');
  if (d.bytesFilled + view.byteLength > d.byteLength)
    throw new RangeError('view is larger than the request');
  const n = view.byteLength;
  d.buffer = view.buffer.transfer();
  __respondInternal(state, d, n);
}

function __respondInternal(state, d, n) {
  __invalidateRequest(state);
  if (__rsState(state.stream).closed) {
    // respond(0) after close: every pending descriptor commits as
    // done with a zero-length view over its (filled) buffer.
    while (state.pendingPullIntos.length > 0)
      __commit(state, state.pendingPullIntos.shift(), true);
    return;
  }
  d.bytesFilled += n;
  if (d.bytesFilled < d.minFill) { __pull(state); return; }
  state.pendingPullIntos.shift();
  const remainder = d.bytesFilled % d.elementSize;
  if (remainder > 0) {
    // Unaligned tail goes back on the queue for the next read.
    const end = d.byteOffset + d.bytesFilled;
    state.queue.push({
      buffer: d.buffer.slice(end - remainder, end),
      byteOffset: 0,
      byteLength: remainder,
    });
    state.queueTotalSize += remainder;
    d.bytesFilled -= remainder;
  }
  __commit(state, d, false);
  __process(state);
  __pull(state);
}

function __releasePending(state, err) {
  for (const d of state.pendingPullIntos)
    if (d.req) d.req.reject(err);
  state.pendingPullIntos = [];
  __invalidateRequest(state);
}

// Wires a `type: 'bytes'` stream: called by the ReadableStream constructor,
// which returns immediately after (so none of the default controller/pull
// machinery is set up). `hwm` is undefined when the strategy did not
// provide one; byte streams default to 0.
function __setupByteStream(stream, src, hwm) {
  const startFn = src.start;
  const pullFn = src.pull;
  const cancelFn = src.cancel;
  for (const [name, fn] of [['start', startFn], ['pull', pullFn],
                            ['cancel', cancelFn]])
    if (fn !== undefined && typeof fn !== 'function')
      throw new TypeError(`${name} must be a function`);
  let auto = src.autoAllocateChunkSize;
  if (auto !== undefined) {
    auto = Number(auto);
    if (!Number.isInteger(auto) || auto <= 0)
      throw new TypeError(
        'autoAllocateChunkSize must be a positive integer');
  }
  const c = new ReadableByteStreamController(_brand, stream);
  const state = __bcState(c);
  state.source = src;
  state.pullFn = pullFn;
  state.cancelFn = cancelFn;
  state.autoAlloc = auto;
  state.hwm = hwm === undefined ? 0 : hwm;
  const s = __rsState(stream);
  s.byteCtl = c;
  s.controller = c;
  // The shared prototype path stays unbranched: the stream dispatches to
  // the module that owns its controller.
  s.readerHook = byteGetReader;
  s.cancelHook = byteCancel;
  if (startFn) {
    let result;
    try { result = startFn.call(src, c); }
    catch (e) { result = Promise.reject(e); }
    Promise.resolve(result).then(
      () => { state.started = true; __pull(state); },
      (e) => __error(state, e));
  } else {
    // Workerd starts a source with no start() synchronously.
    state.started = true;
    __pull(state);
  }
}

class ReadableByteStreamController {
  constructor(brand, stream) {
    if (brand !== _brand)
      throw new TypeError(
        'ReadableByteStreamController cannot be constructed directly');
    const state = new ByteControllerState(stream);
    state.self = this;
    _controllerStates.set(this, state);
  }

  get byobRequest() {
    const state = __bcState(this);
    if (state.byobRequest === null &&
        state.pendingPullIntos.length > 0) {
      const d = state.pendingPullIntos[0];
      const view = new Uint8Array(d.buffer,
        d.byteOffset + d.bytesFilled, d.byteLength - d.bytesFilled);
      state.byobRequest =
        new ReadableStreamBYOBRequest(_brand, this, view);
    }
    return state.byobRequest;
  }

  get desiredSize() {
    const state = __bcState(this);
    const s = __rsState(state.stream);
    if (s.errored) return null;
    if (s.closed || state.closeRequested) return 0;
    return state.hwm - state.queueTotalSize;
  }

  enqueue(chunk) {
    const state = __bcState(this);
    const s = __rsState(state.stream);
    if (!ArrayBuffer.isView(chunk) || chunk.byteLength === 0 ||
        chunk.buffer.byteLength === 0)
      throw new TypeError(
        'chunk must be a non-empty ArrayBufferView');
    if (state.closeRequested || s.closed || s.errored)
      throw new TypeError('Cannot enqueue a closed/errored stream');
    const { byteOffset, byteLength } = chunk;
    const buffer = chunk.buffer.transfer();
    const first = state.pendingPullIntos[0];
    if (first) {
      __invalidateRequest(state);
      if (first.readerType === 'default') {
        // A pending autoAllocate descriptor is dropped; salvage any
        // bytes it already holds ahead of the new chunk.
        state.pendingPullIntos.shift();
        if (first.bytesFilled > 0) {
          state.queue.push({ buffer: first.buffer,
            byteOffset: first.byteOffset,
            byteLength: first.bytesFilled });
          state.queueTotalSize += first.bytesFilled;
        }
      }
    }
    const reader = s.reader;
    if (reader instanceof ReadableStreamDefaultReader) {
      while (s.readRequests.length > 0 && state.queueTotalSize > 0)
        s.readRequests.shift()
          .resolve(_readResult(__drain(state), false));
      if (s.readRequests.length > 0) {
        s.readRequests.shift().resolve(_readResult(
          new Uint8Array(buffer, byteOffset, byteLength), false));
      } else {
        state.queue.push({ buffer, byteOffset, byteLength });
        state.queueTotalSize += byteLength;
      }
    } else {
      state.queue.push({ buffer, byteOffset, byteLength });
      state.queueTotalSize += byteLength;
      if (reader) __process(state);
    }
    __pull(state);
  }

  close() {
    const state = __bcState(this);
    const s = __rsState(state.stream);
    if (state.closeRequested || s.closed || s.errored)
      throw new TypeError('Cannot close a closed/errored stream');
    if (state.queueTotalSize > 0) {
      state.closeRequested = true;
      return;
    }
    const d = state.pendingPullIntos[0];
    if (d && d.bytesFilled % d.elementSize !== 0) {
      const e = new TypeError('Insufficient bytes to fill elements');
      __error(state, e);
      throw e;
    }
    __closeCommit(state);
  }

  error(e) {
    __error(__bcState(this), e);
  }
}

// Default reader over a byte stream. Subclassing keeps the shared
// ReadableStreamDefaultReader.read() free of byte-stream branches;
// releaseLock/cancel/closed are inherited (release nulls the stream's
// reader, which is this class's released signal).
class ByteDefaultReader extends ReadableStreamDefaultReader {
  #stream;
  constructor(stream) {
    super(stream);
    this.#stream = stream;
  }
  read() {
    const s = __rsState(this.#stream);
    if (s.reader !== this)
      return _readSettled(Promise.reject(new TypeError(RELEASED)));
    s.disturbed = true;
    if (s.errored) return _readSettled(Promise.reject(s.error));
    const c = __bcState(s.byteCtl);
    if (c.queueTotalSize > 0)
      return _readSettled(Promise.resolve(_readResult(__drain(c), false)));
    if (s.closed)
      return _readSettled(Promise.resolve(_readResult(undefined, true)));
    const p = new Promise((resolve, reject) =>
      s.readRequests.push({ resolve, reject }));
    const auto = c.autoAlloc;
    if (auto !== undefined)
      c.pendingPullIntos.push({
        buffer: new ArrayBuffer(auto),
        bufferLen: auto,
        byteOffset: 0,
        byteLength: auto,
        bytesFilled: 0,
        elementSize: 1,
        minFill: 1,
        ctor: Uint8Array,
        readerType: 'default',
        req: null,
      });
    __pull(c);
    return _readSettled(p);
  }
}

class ReadableStreamBYOBReader {
  #s;

  // Accepts byte streams (a byte controller in the stream state) and
  // internal streams (an `ictl`: IdentityTransformStream readables and
  // materialized body streams), both of which implement the `_pullInto`
  // protocol.
  constructor(stream) {
    const st = __cello.$$isReadableStream(stream)
      ? __rsState(stream) : undefined;
    const c = st === undefined
      ? undefined : (st.byteCtl ?? st.ictl);
    if (c === undefined)
      throw new TypeError(
        'ReadableStreamBYOBReader requires a byte ReadableStream');
    if (st.locked)
      throw new TypeError('ReadableStream is locked');
    this.#s = { stream, byteCtl: st.byteCtl, ictl: c, released: false };
    _byobReaderStates.set(this, this.#s);
    st.locked = true;
    st.reader = this;
  }

  read(view, options) {
    const state = this.#s;
    if (state.released)
      return _readSettled(Promise.reject(new TypeError(RELEASED)));
    if (!ArrayBuffer.isView(view) || view.byteLength === 0 ||
        view.buffer.byteLength === 0)
      return _readSettled(Promise.reject(new TypeError(
        'read() requires a non-empty ArrayBufferView')));
    let min = 1;
    if (options !== undefined && options !== null &&
        options.min !== undefined) {
      min = Number(options.min);
      const elements = view.length ?? view.byteLength; // DataView
      if (!Number.isInteger(min) || min < 1 || min > elements)
        return _readSettled(Promise.reject(new TypeError('invalid min')));
    }
    const s = __rsState(state.stream);
    s.disturbed = true;
    if (s.errored) return _readSettled(Promise.reject(s.error));
    return _readSettled(state.byteCtl !== undefined
      ? __pullInto(__bcState(state.byteCtl), view, min)
      : state.ictl._pullInto(view, min));
  }

  // Workerd extension: resolve only once at least `min` bytes are in
  // `view` (or the stream ends first -- then with what arrived).
  readAtLeast(min, view) {
    return this.read(view, { min });
  }

  cancel(reason) {
    const state = this.#s;
    if (state.released)
      return Promise.reject(new TypeError(RELEASED));
    return __cello.$$cancelReadableStream(state.stream, reason);
  }

  get closed() {
    const state = this.#s;
    if (state.released)
      return Promise.reject(new TypeError(RELEASED));
    return __rsState(state.stream).closedPromise;
  }

  releaseLock() {
    const state = this.#s;
    if (state.released) return;
    const err = new TypeError(RELEASED);
    if (state.byteCtl !== undefined)
      __releasePending(__bcState(state.byteCtl), err);
    else
      state.ictl._releasePending(err);
    const s = __rsState(state.stream);
    s.locked = false;
    s.reader = null;
    state.released = true;
  }
}

// Internal controller over an already-materialized body: BYOB reads
// fill from the remaining bytes of the shared {bytes, off} cursor and
// never park. Fewer bytes than asked (or than `min`) means the body is
// ending; done flips only on the empty read after it, like Workerd's
// internal sources.
class BufferedInternalController {
  #st;
  _pendingPullIntos = [];  // for releaseLock; never populated

  constructor(st) { this.#st = st; }

  _releasePending(_err) {}

  _pullInto(view, _min) {
    let buffer;
    try { buffer = view.buffer.transfer(); }
    catch (e) { return Promise.reject(e); }
    const st = this.#st;
    const elementSize = view.BYTES_PER_ELEMENT ?? 1;
    let n = Math.min(st.bytes.byteLength - st.off, view.byteLength);
    n -= n % elementSize;
    if (n > 0) {
      new Uint8Array(buffer, view.byteOffset, n)
        .set(st.bytes.subarray(st.off, st.off + n));
      st.off += n;
    }
    return Promise.resolve(_readResult(
      new view.constructor(buffer, view.byteOffset, n / elementSize),
      n === 0));
  }
}

// Installed per byte-stream instance by the ReadableStream constructor.
function byteGetReader(stream, options) {
  if (options !== undefined && options !== null &&
      options.mode !== undefined) {
    if (String(options.mode) !== 'byob')
      throw new TypeError(`Invalid reader mode '${options.mode}'`);
    return new ReadableStreamBYOBReader(stream);
  }
  return new ByteDefaultReader(stream);
}

function byteCancel(stream, reason) {
  const s = __rsState(stream);
  const c = __bcState(s.byteCtl);
  if (s.closed) return Promise.resolve();
  if (s.errored) return Promise.reject(s.error);
  s.disturbed = true;
  c.queue = [];
  c.queueTotalSize = 0;
  const pending = c.pendingPullIntos;
  c.pendingPullIntos = [];
  __invalidateRequest(c);
  // Spec: cancel resolves pending BYOB reads with no value.
  for (const d of pending)
    if (d.req) d.req.resolve(_readResult(undefined, true));
  __cello.$$finishCloseStream(stream);
  if (!c.cancelFn) return Promise.resolve();
  let result;
  try { result = c.cancelFn.call(c.source, reason); }
  catch (e) { return Promise.reject(e); }
  return Promise.resolve(result).then(() => undefined);
}

// The runtime's own modules read and drive byte-stream state through these
// hooks; tenant code cannot reach `__cello`.
__cello.$$setupByteStream = __setupByteStream;
__cello.$$byteControllerState = __bcState;
__cello.$$bufferedByobController = (st) =>
  new BufferedInternalController(st);

// WebIDL reflection (#171, #206).
for (const [cls, name] of [
  [ReadableByteStreamController, 'ReadableByteStreamController'],
  [ReadableStreamBYOBReader, 'ReadableStreamBYOBReader'],
  [ReadableStreamBYOBRequest, 'ReadableStreamBYOBRequest'],
]) {
  __cello.$$webidl(cls, name);
}

return {
  ReadableByteStreamController,
  ReadableStreamBYOBReader,
  ReadableStreamBYOBRequest,
};
})()
