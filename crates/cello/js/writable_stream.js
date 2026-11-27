// WritableStream (WHATWG subset)
//
// The stream follows the spec state machine: 'writable' | 'erroring' |
// 'errored' | 'closed'. An abort or controller error while a sink
// write/close is in flight (or before start settles) parks the stream in
// 'erroring'; the in-flight operation completes normally, then
// finishErroring rejects the queued writes in order, runs the sink
// abort, and rejects close/closed. One deliberate Workerd deviation from
// the current spec text: close() on an errored stream rejects with the
// stored error (spec rejects with a fresh TypeError). Everything else
// here is measured workerd 1.20261121.1 behaviour, not spec text: abort()
// on an errored stream resolves (spec resolves too; this file used to
// reject), every abort() call gets its own promise although all of them
// follow the request's single abort (spec returns the same promise
// again), and the close()/write() rejections name each state the way
// workerd's messages do.
//
// Every field lives in a private state object (this module's WeakMap), so
// a WritableStream, writer, controller or TransformStream exposes no own
// properties and no internal prototype methods; other preludes read the
// same state through `__cello.$$...` (#170, #206).

// Brand-check sentinel for internal construction.
const _wsc_brand = Symbol("WritableStreamController");

// A fresh promise following `p`'s outcome. Workerd hands every abort() call
// its own promise instead of returning the request's promise again, while
// all of them settle alike (#133, measured on 1.20261121.1). The no-op
// rejection sink keeps a dropped call from firing the unhandled-rejection
// hook, the same reason the abort request carries one.
function _followAbort(p) {
  const follower = p.then((value) => value);
  follower.catch(() => {});
  return follower;
}

const __rsState = (stream) => __cello.$$readableState(stream);

// ---- writable stream state -------------------------------------------------
class WritableStreamState {
  constructor() {
    this.sink = null;
    this.locked = false;
    this.state = 'writable'; // writable|erroring|errored|closed
    this.storedError = undefined;
    this.controller = null;
    this.writer = null;
    this.queue = []; // of {chunk, size, resolve, reject}
    this.queueSize = 0;
    this.inFlightWrite = null;
    this.inFlightClose = null;
    this.closeRequest = null;
    this.pendingAbort = null;
    this.abortPromise = null;
    this.backpressure = false;
    this.hwm = 1;
    this.started = false;
    this.sizeFn = undefined;
    this.startFn = undefined;
    this.writeFn = undefined;
    this.closeFn = undefined;
    this.abortFn = undefined;
    // Spec: [[signal]] is an AbortSignal that fires when the stream is
    // aborted. Installed by the controller's constructor.
    this.signalAbort = undefined;
    // Per-stream hook a TransformStream installs on its writable side: its
    // sink write parks on the readable's backpressure promise, so entering
    // 'erroring' has to release it (the write then rejects with the stored
    // error) or an abort, a readable cancel, or a controller error could
    // never settle. Kept in the private state rather than an own field: a
    // tenant that shadows a public property must not be able to make
    // abort()/cancel throw before the sink's abort algorithm runs.
    this.unblockWrite = undefined;
    // Runs after every abort() request has been recorded (IdentityTransform
    // Stream unparks a write that is waiting on a read the abort cancels).
    this.abortHook = undefined;
    // Workerd's compression streams are internal streams: a failed push or
    // flush closes the writable side instead of erroring it (#223).
    // compression_streams.js opts in through the private writable state
    // (`__cello.$$writableStreamState(...).closesOnError`).
    this.closesOnError = false;
    // A plain TransformStream's readable cancel fails the writes it lands
    // on with a TypeError naming the readable side, while the stored error
    // stays the cancel reason (#272). Set only for writes outstanding when
    // the cancel arrives that never started transforming; cleared when the
    // error settles.
    this.writeError = null;
    // A byte transform validates the chunk when the sink accepts the
    // write, not when the transform runs (#276): an invalid chunk rejects
    // only that write, leaving both sides usable. Compression streams opt
    // in through the private writable state, the same way `closesOnError`
    // does; every other stream leaves this null.
    this.validateChunk = null;
    // Workerd's compression streams are also internal on the read side: a
    // readable cancel does not error the writable. It records the cancel
    // error here instead, so a write the writable accepted afterwards
    // refuses with it (and errors the stream), while a write accepted
    // before it still completes (#252, #257). The native compressor stays
    // alive only until the accepted writes drain: a cancel with nothing
    // accepted or in flight ends it right away, otherwise the last
    // accepted write's settle does, and the outcome is stashed so a close
    // that follows replays the member's true end -- success for a complete
    // member, the incomplete-data error otherwise -- instead of failing
    // with `zlib stream already closed` (#270). The flush consumes it, an
    // abort drops it, and abandonment is covered by the finalizer.
    this.cancelled = null;
    // Hook a compression stream installs here: run once its readable is
    // cancelled and no accepted write is still outstanding, so a
    // cancel-only stream frees its native slot with no close, no abort,
    // and no dependency on the collector (#270). Kept in the private
    // state for the same reason as `unblockWrite` above.
    this.cancelDrainHook = undefined;
  }
}

const _writableStates = new WeakMap();
const __wsState = (stream) => {
  const state = _writableStates.get(stream);
  if (state === undefined)
    throw new TypeError("Illegal invocation");
  return state;
};

// ---- writer state ----------------------------------------------------------
class WriterState {
  constructor(stream) {
    this.stream = stream;
    this.released = false;
    this.closedPromise = null;
    this.closedResolve = null;
    this.closedReject = null;
    this.readyPromise = null;
    this.readyResolve = null;
    this.readyReject = null;
  }
}

const _writerStates = new WeakMap();
const __wtrState = (writer) => {
  const state = _writerStates.get(writer);
  if (state === undefined)
    throw new TypeError("Illegal invocation");
  return state;
};

function __resetReady(state) {
  state.readyPromise = new Promise((res, rej) => {
    state.readyResolve = res;
    state.readyReject = rej;
  });
  // Same pattern the constructor uses for the
  // closed promise: attach a no-op rejection sink
  // so V8 doesn't report an unhandled rejection
  // when ready is rejected by the erroring path and
  // the user never touched `writer.ready`. The
  // user awaiting writer.ready still sees the
  // rejection -- the promise is shared. Without
  // this, a writable whose sink throws on the
  // first write produced both the user-visible
  // `await writer.write(...)` rejection AND a
  // ghost "Uncaught (in promise)" reported on
  // the freshly-reset ready promise (the
  // queueSize-just-hit-hwm case).
  state.readyPromise.catch(() => {});
}

function __resolveReady(state) {
  if (state.readyResolve) state.readyResolve();
}

function __rejectReady(state, e) {
  if (state.readyReject) {
    state.readyReject(e);
    // Replace with a rejected promise so subsequent
    // reads of writer.ready get the error.
    state.readyPromise = Promise.reject(e);
    // Prevent unhandled rejection.
    state.readyPromise.catch(() => {});
    state.readyResolve = null;
    state.readyReject = null;
  }
}

function __resolveClosed(state) {
  if (state.closedResolve) state.closedResolve();
}

function __rejectClosedWriter(state, e) {
  if (state.closedReject) {
    state.closedReject(e);
    state.closedResolve = null;
    state.closedReject = null;
  }
}

class WritableStreamDefaultController {
  #stream;
  #abortController;

  constructor(brand, stream) {
    if (brand !== _wsc_brand)
      throw new TypeError(
        "WritableStreamDefaultController cannot be " +
        "constructed directly");
    this.#stream = stream;
    // Spec: [[signal]] is an AbortSignal that fires
    // when the stream is aborted, letting sinks cancel
    // in-flight work. Lazily pair with an internal
    // AbortController so the signal survives the full
    // lifetime of the controller.
    this.#abortController = new AbortController();
    __wsState(stream).signalAbort = (reason) => {
      if (!this.#abortController.signal.aborted) {
        this.#abortController.abort(reason);
      }
    };
  }

  error(e) {
    __errorStream(__wsState(this.#stream), e);
  }

  get signal() {
    return this.#abortController.signal;
  }
}

class WritableStreamDefaultWriter {
  #s;

  constructor(stream) {
    if (!(stream instanceof WritableStream))
      throw new TypeError(
        "WritableStreamDefaultWriter requires a " +
        "WritableStream argument");
    const st = __wsState(stream);
    if (st.locked)
      throw new TypeError(
        "This WritableStream is currently locked to a writer.");
    const state = this.#s = new WriterState(stream);
    _writerStates.set(this, state);
    st.locked = true;
    st.writer = this;

    // Closed promise. Same pattern as ReadableStream
    // (#105): attach a no-op sink so V8 doesn't fire
    // unhandled-rejection when the writer is aborted
    // and the user never touched writer.closed. User
    // `await writer.closed` still sees the rejection
    // because the promise is shared.
    state.closedPromise = new Promise((res, rej) => {
      state.closedResolve = res;
      state.closedReject = rej;
    });
    state.closedPromise.catch(() => {});

    __resetReady(state);
    if (st.state === 'writable') {
      // Workerd resolves a fresh writer's ready from the stream's
      // backpressure alone: a queued close does not resolve it while the
      // stream is still over its high-water mark (#245).
      if (!st.backpressure) __resolveReady(state);
    } else if (st.state === 'closed') {
      __resolveReady(state);
      __resolveClosed(state);
    } else if (st.state === 'erroring') {
      __rejectReady(state, st.storedError);
    } else { // errored
      __rejectReady(state, st.storedError);
      __rejectClosedWriter(state, st.storedError);
    }
  }

  write(chunk) {
    const state = this.#s;
    if (state.released)
      return Promise.reject(
        new TypeError("This WritableStream writer has been released."));
    return __write(__wsState(state.stream), chunk);
  }

  close() {
    const state = this.#s;
    if (state.released)
      return Promise.reject(
        new TypeError("This WritableStream writer has been released."));
    return __close(__wsState(state.stream));
  }

  abort(reason) {
    const state = this.#s;
    if (state.released)
      return Promise.reject(
        new TypeError("This WritableStream writer has been released."));
    return __abort(__wsState(state.stream), reason);
  }

  releaseLock() {
    const state = this.#s;
    if (state.released) return;
    const s = __wsState(state.stream);
    // Workerd's compression streams are internal streams: once their
    // writable side has closed -- cleanly, or through a failed push or
    // flush -- releaseLock() is a no-op. The writer keeps the stream
    // locked, desiredSize stays 0, and its earlier promises keep their
    // settlement (#230).
    if (s.closesOnError && s.state === 'closed') return;
    // Spec section 4.6.9: releasing rejects the writer's ready and
    // (still-pending) closed promises with a fresh TypeError, even while
    // an abort is in flight. For ready that means a *new* rejected promise
    // even when the old one had settled, so a read after the release sees
    // the released error; a handler attached before the release keeps the
    // old settlement -- still resolved, or still the stream's error, as
    // workerd measured (#193). closed keeps its own settlement once it has
    // one (__rejectClosedWriter only acts while it is pending).
    const err = new TypeError(
      "This WritableStream writer has been released.");
    if (state.readyReject) state.readyReject(err);
    state.readyPromise = Promise.reject(err);
    state.readyPromise.catch(() => {});
    state.readyResolve = null;
    state.readyReject = null;
    __rejectClosedWriter(state, err);
    // Workerd also rejects queued (not in-flight) writes
    // with the same error; the chunks are dropped. The
    // write already dispatched keeps its size in the queue
    // total until it settles, so a new writer's desiredSize
    // and ready still see that reservation (#245).
    for (const entry of s.queue) {
      entry.reject(err);
      s.queueSize -= entry.size;
    }
    if (s.queueSize < 0) s.queueSize = 0;
    s.queue = [];
    // Recompute rather than leave the stale flag: a new writer's ready
    // resolves on construction only when backpressure is clear, and nothing
    // else would re-run the check while the start gate is still open
    // (#232, #245).
    s.backpressure = s.queueSize >= s.hwm;
    s.locked = false;
    s.writer = null;
    state.released = true;
  }

  get desiredSize() {
    const state = this.#s;
    // Workerd throws for a released writer in every stream state (#193),
    // instead of reporting the stream's size; `null` for an errored or
    // closed stream would otherwise leak through.
    if (state.released)
      throw new TypeError(
        "This WritableStream writer has been released.");
    const s = __wsState(state.stream);
    // Only an *errored* stream reports null; while it is still erroring
    // workerd keeps reporting the queue formula, which can go negative
    // (a queued write past the high-water mark, measured -1 with
    // highWaterMark 1 and two writes outstanding) (#232).
    if (s.state === 'errored') return null;
    if (s.state === 'closed') return 0;
    return s.hwm - s.queueSize;
  }

  get ready() { return this.#s.readyPromise; }
  get closed() { return this.#s.closedPromise; }
}

function __closing(st) {
  return st.closeRequest !== null ||
         st.inFlightClose !== null;
}

// Backpressure flips only on a threshold crossing, so
// ready is reset (or resolved) once per transition
// rather than on every write.
function __updateBackpressure(st) {
  const bp = st.queueSize >= st.hwm;
  if (bp === st.backpressure) return;
  st.backpressure = bp;
  const w = st.writer;
  if (!w) return;
  if (bp) __resetReady(__wtrState(w));
  else __resolveReady(__wtrState(w));
}

function __write(st, chunk) {
  // Spec: the size algorithm runs before the state
  // checks; a throw or invalid size errors the stream
  // and the checks below then reject with it.
  let size = 1;
  if (st.sizeFn) {
    try {
      size = st.sizeFn(chunk);
      if (!Number.isFinite(size) || size < 0)
        throw new RangeError(
          "invalid strategy size: " + size);
    } catch (e) {
      __errorStream(st, e);
    }
  }
  // A write landing between abort() and its settlement follows the abort's
  // lifecycle on internal (compression/identity/fixed) streams: the abort
  // is still settling, so the state below has not flipped yet, and the
  // write settles once the abort does, reporting the closed stream the
  // settled abort leaves behind (#283). Chaining on the pending request --
  // rather than rejecting now -- is what orders the abort first, then the
  // writes, then closed. The settled abort flips an internal stream to
  // 'closed', which the check below keeps reporting; anything else with a
  // request still recorded is still settling.
  if (st.closesOnError && st.abortPromise !== null && st.state !== 'closed')
    return st.abortPromise.then(() => {
      // Throw, rather than returning a rejected promise: adopting a
      // thenable costs a microtask, and closed must settle after these
      // writes, not before them (#283).
      throw new TypeError("This WritableStream has been closed.");
    });
  if (st.state === 'errored')
    return Promise.reject(st.storedError);
  // Workerd names each state separately here (#132), and its message
  // for a closing stream says "ReadableStream": measured on
  // 1.20261121.1, not a typo of ours.
  if (st.state === 'closed')
    return Promise.reject(new TypeError(
      "This WritableStream has been closed."));
  // Workerd's internal (compression/identity/fixed) streams report a
  // pending close as the closed stream for every later write (#281): once
  // close() was called the writable is already closing, whether the close
  // has dispatched or is still queued. Plain streams keep the
  // "ReadableStream is closed" message in both positions, as on workerd.
  if (__closing(st)) {
    if (st.closesOnError)
      return Promise.reject(new TypeError(
        "This WritableStream has been closed."));
    return Promise.reject(new TypeError(
      "This ReadableStream is closed."));
  }
  if (st.state === 'erroring')
    return Promise.reject(st.storedError);
  // A byte transform validates the chunk when the write is accepted, not
  // when the transform runs: an invalid chunk rejects only that write,
  // leaving both sides usable (#276, #280). The state errors above win,
  // so a write to a dead stream reports the state error, as on Workerd.
  if (st.validateChunk !== null && st.validateChunk !== undefined) {
    try { st.validateChunk(chunk); }
    catch (e) { return Promise.reject(e); }
  }

  return new Promise((resolve, reject) => {
    // `cancelled` is the readable-cancel error live when the write was
    // accepted. Workerd's internal (compression) writable completes a write
    // it accepted before the cancel and refuses one accepted after it
    // (#257).
    st.queue.push({ chunk, size, resolve, reject, cancelled: st.cancelled });
    st.queueSize += size;
    __updateBackpressure(st);
    __advanceQueue(st);
  });
}

function __close(st) {
  // Workerd rejects close() on an errored stream with its stored error
  // (the spec text rejects with a fresh TypeError), the reason
  // writer.closed already reports. A producer whose consumer cancelled
  // the readable therefore observes the cancel reason instead of a
  // generic message (#129). The other two states have their own
  // workerd messages (#132), measured on 1.20261121.1.
  if (st.state === 'errored')
    return Promise.reject(st.storedError);
  if (st.state === 'closed')
    return Promise.reject(new TypeError(
      "This WritableStream has been closed."));
  // Same closed shape for close(): a close() that arrives after abort()
  // was called reports the closed stream, even while the first close is
  // still queued behind the start gate or the abort is still settling
  // (#270). Plain streams keep queueing here and reject with the abort
  // reason when it lands, as workerd does.
  if (st.closesOnError && st.pendingAbort !== null)
    return Promise.reject(new TypeError(
      "This WritableStream has been closed."));
  if (__closing(st))
    return Promise.reject(new TypeError(
      "Cannot close a writer that is already being closed"));
  return new Promise((resolve, reject) => {
    st.closeRequest = { resolve, reject };
    if (st.backpressure &&
        st.state === 'writable' && st.writer)
      __resolveReady(__wtrState(st.writer));
    __advanceQueue(st);
  });
}

function __abort(st, reason) {
  const p = __abortRequest(st, reason);
  // IdentityTransformStream observes every abort request to unpark a write
  // that is waiting on a read this abort cancels.
  if (st.abortHook) st.abortHook(reason);
  return p;
}

function __abortRequest(st, reason) {
  // Workerd: every abort() call gets its own promise, and all of them
  // follow the request's single abort, settled or not (#133).
  if (st.abortPromise) return _followAbort(st.abortPromise);
  if (st.state === 'closed')
    return Promise.resolve();
  // Workerd resolves abort() on a stream that already errored: the
  // error reached every consumer already, so aborting changes nothing
  // (the spec text resolves too; this path used to reject with the
  // stored error, which the old header misrecorded as workerd's).
  // Aborting while the stream is still *erroring* rejects with the
  // stored error from finishErroring: the request is recorded before
  // the state settles.
  if (st.state === 'errored')
    return Promise.resolve();
  // Fire the controller's AbortSignal so sinks watching
  // controller.signal can cancel in-flight work.
  if (st.signalAbort) st.signalAbort(reason);
  const wasErroring = st.state === 'erroring';
  if (wasErroring) reason = undefined;
  const p = new Promise((resolve, reject) => {
    st.pendingAbort =
      { resolve, reject, reason, wasErroring };
  });
  st.abortPromise = p;
  // No-op sink: a dropped abort() that ends up rejecting
  // must not fire the unhandled-rejection hook.
  p.catch(() => {});
  if (!wasErroring) {
    // A compression stream's abort waits for the write in flight (#230):
    // workerd lets the transform run, and a failed push is the error the
    // stream keeps -- closed rejects with "Decompression failed." and
    // abort() resolves -- instead of the abort reason poisoning the parked
    // write. Release the write from the readable handshake so it runs; the
    // settle path (__startWrite or __closeAfterError) ends the request.
    // Only a sink parked on that handshake can wait this way. An internal
    // byte stream's parked write has no reader coming, so it takes the
    // erroring path below and the abort hook unparks it with the abort
    // reason instead of deadlocking behind it (#283).
    if (st.closesOnError && st.state === 'writable' &&
        st.unblockWrite !== undefined &&
        (st.inFlightWrite !== null || st.queue.length > 0)) {
      st.unblockWrite();
      return p;
    }
    __startErroring(st, reason);
  }
  return p;
}

// WritableStreamDefaultControllerErrorIfNeeded.
function __errorStream(st, e) {
  if (st.state === 'writable') __startErroring(st, e);
}

function __startErroring(st, reason) {
  st.storedError = reason;
  st.state = 'erroring';
  const unblockWrite = st.unblockWrite;
  if (unblockWrite) unblockWrite();
  if (st.writer) __rejectReady(__wtrState(st.writer), reason);
  if (st.started && !st.inFlightWrite &&
      !st.inFlightClose)
    __finishErroring(st);
}

function __finishErroring(st) {
  st.state = 'errored';
  const err = st.storedError;
  // Reject queued writes in order, then settle abort,
  // then reject close/closed -- the spec's promise
  // resolution order, except on a plain TransformStream where workerd
  // rejects close/closed before the abort promise settles (#290). A
  // plain TransformStream's readable cancel fails the
  // writes it lands on with its own TypeError instead (#272).
  const writeErr = st.writeError ?? err;
  st.writeError = null;
  for (const entry of st.queue) entry.reject(writeErr);
  st.queue = [];
  st.queueSize = 0;
  const abortReq = st.pendingAbort;
  st.pendingAbort = null;
  if (!abortReq) {
    __rejectCloseAndClosed(st);
    return;
  }
  if (abortReq.wasErroring) {
    // The stream was already erroring when abort() was
    // called: the sink abort is not run.
    abortReq.reject(err);
    __rejectCloseAndClosed(st);
    return;
  }
  // A plain TransformStream's writable runs the pair's sink abort, and
  // workerd rejects writer.closed before that abort settles. Every other
  // shape keeps the abort-first order: an idle plain WritableStream and a
  // gated-sink in-flight abort already agree, and internal streams end in
  // the closed shape with their own deferral below. The writable side of
  // a plain TransformStream is the only one with the backpressure
  // handshake and no closed-shape flag.
  const plainTransformFirst =
    !st.closesOnError && st.unblockWrite !== undefined;
  let result;
  try {
    result = st.abortFn
      ? st.abortFn.call(st.sink, abortReq.reason)
      : undefined;
  } catch (e) { result = Promise.reject(e); }
  Promise.resolve(result).then(
    () => {
      if (plainTransformFirst) {
        __rejectCloseAndClosed(st);
        abortReq.resolve();
        return;
      }
      abortReq.resolve();
      // Workerd's internal streams end an aborted writable in the closed
      // shape (desiredSize 0, later write()/close() report "has been
      // closed"), not the errored shape a JS sink gets (#252). Aborting
      // is not backpressure, so the pending ready fulfills with it: the
      // erroring path already rejected it above, so install a resolved
      // promise rather than resolving the spent deferred (#281).
      if (st.closesOnError) {
        st.state = 'closed';
        if (st.writer) {
          const writerState = __wtrState(st.writer);
          writerState.readyPromise = Promise.resolve();
          writerState.readyResolve = null;
          writerState.readyReject = null;
        }
        // Writes that landed between abort() and its settlement chain on
        // the abort request, so they settle right after it; closed settles
        // after them. Defer the close/closed rejections past those chained
        // writes instead of rejecting them in the same tick (#283).
        Promise.resolve().then(() => __rejectCloseAndClosed(st));
      } else {
        __rejectCloseAndClosed(st);
      }
    },
    (e) => {
      if (plainTransformFirst) {
        __rejectCloseAndClosed(st);
        abortReq.reject(e);
        return;
      }
      abortReq.reject(e);
      // Same ordering as the fulfilling arm: chained writes settle before
      // closed does (#283).
      if (st.closesOnError)
        Promise.resolve().then(() => __rejectCloseAndClosed(st));
      else
        __rejectCloseAndClosed(st);
    },
  );
}

function __rejectCloseAndClosed(st) {
  if (st.closeRequest) {
    st.closeRequest.reject(st.storedError);
    st.closeRequest = null;
  }
  if (st.writer)
    __rejectClosedWriter(__wtrState(st.writer), st.storedError);
}

function __dealWithRejection(st, e) {
  if (st.state === 'writable') {
    __startErroring(st, e);
    return;
  }
  if (st.state === 'erroring') __finishErroring(st);
}

// A compression stream whose readable was cancelled ends its native stream
// as soon as no accepted write is still outstanding: nothing queued, none
// in flight, and no close running its flush (the flush owns the end then).
// A queued close does not block the end -- no transform can run between it
// and the flush, so the flush replays the stashed outcome unchanged (#270).
function __maybeEndCancelled(st) {
  if (!st.closesOnError || st.cancelled === null ||
      st.cancelDrainHook === undefined)
    return;
  if (st.inFlightWrite !== null || st.queue.length > 0 ||
      st.inFlightClose !== null)
    return;
  st.cancelDrainHook();
}

function __advanceQueue(st) {
  if (!st.started) return;
  if (st.inFlightWrite || st.inFlightClose) return;
  const state = st.state;
  if (state === 'errored' || state === 'closed') return;
  if (state === 'erroring') {
    __finishErroring(st);
    return;
  }
  if (st.queue.length > 0)
    __startWrite(st, st.queue.shift());
  else if (st.closeRequest)
    __startClose(st);
}

function __startWrite(st, entry) {
  st.inFlightWrite = entry;
  // A sync throw is deferred into a rejection so an
  // abort() issued between write() and the microtask
  // still sees the write as in flight (Workerd runs the
  // sink abort in that case; a sync error path would
  // reach the errored state first and skip it).
  let result;
  try {
    // A write the readable's cancel accepted before it ran still completes
    // on workerd's internal streams; only one accepted after the cancel
    // refuses, with the cancel error (#257).
    if (entry.cancelled !== null) throw entry.cancelled;
    result = st.writeFn
      ? st.writeFn.call(
          st.sink, entry.chunk, st.controller)
      : undefined;
  } catch (e) { result = Promise.reject(e); }
  Promise.resolve(result).then(
    () => {
      st.inFlightWrite = null;
      entry.resolve(undefined);
      st.queueSize -= entry.size;
      if (st.queueSize < 0) st.queueSize = 0;
      // A compression stream's abort deferred to this write (#230): the
      // chunk was written, so the abort now takes the normal path. Starting
      // the error here rejects the writes still queued, then finishErroring
      // runs the sink abort and resolves the request. Only while the stream
      // is still writable: an abort that arrived while it was already
      // erroring stores no reason of its own (`wasErroring`), and starting
      // the error again would overwrite the stored one with `undefined`.
      if (st.closesOnError && st.state === 'writable' && st.pendingAbort)
        __startErroring(st, st.pendingAbort.reason);
      else if (st.state === 'writable' && !__closing(st))
        __updateBackpressure(st);
      __advanceQueue(st);
      // The last accepted write settled behind a readable cancel: end the
      // native stream now if no close owns that end instead (#270).
      __maybeEndCancelled(st);
    },
    (e) => {
      st.inFlightWrite = null;
      st.queueSize -= entry.size;
      if (st.queueSize < 0) st.queueSize = 0;
      entry.reject(e);
      if (st.closesOnError) __closeAfterError(st, e);
      else __dealWithRejection(st, e);
      // A failed push already dropped the native stream, so this is a
      // guarded no-op; a cancel-error refusal on an otherwise idle stream
      // ends one a clean drain left behind (#270).
      __maybeEndCancelled(st);
    },
  );
}

function __startClose(st) {
  st.inFlightClose = st.closeRequest;
  st.closeRequest = null;
  let result;
  try {
    result = st.closeFn
      ? st.closeFn.call(st.sink)
      : undefined;
  } catch (e) { result = Promise.reject(e); }
  Promise.resolve(result).then(
    () => __finishClose(st),
    (e) => __finishCloseWithError(st, e),
  );
}

function __finishClose(st) {
  const abortReq = st.pendingAbort;
  if (st.state === 'erroring' && abortReq !== null &&
      st.closesOnError && !abortReq.wasErroring) {
    // The abort() that errored the stream raced a close() that already
    // started its flush: on workerd's internal (compression) streams the
    // abort wins (#270). The close rejects with the abort reason, the
    // sink abort still runs -- erroring the readable with that reason and
    // releasing the native stream (already consumed by the flush, so a
    // no-op) -- and the writable ends in the closed shape: the abort
    // fulfills, writer.closed rejects with the abort reason, and later
    // write()/close() calls report the closed stream. Plain streams keep
    // the close-wins outcome below, as workerd does.
    st.inFlightClose.reject(st.storedError);
    st.inFlightClose = null;
    st.pendingAbort = null;
    let result;
    try {
      result = st.abortFn
        ? st.abortFn.call(st.sink, abortReq.reason)
        : undefined;
    } catch (e) { result = Promise.reject(e); }
    Promise.resolve(result).then(
      () => {
        abortReq.resolve();
        st.state = 'closed';
        if (st.writer) {
          const writerState = __wtrState(st.writer);
          __resolveReady(writerState);
          __rejectClosedWriter(writerState, st.storedError);
        }
      },
      (e) => {
        abortReq.reject(e);
        __rejectCloseAndClosed(st);
      },
    );
    return;
  }
  st.inFlightClose.resolve();
  st.inFlightClose = null;
  if (st.state === 'erroring') {
    // An abort raced the close and lost: the close wins,
    // the abort fulfills, the stream closes cleanly.
    st.storedError = undefined;
    if (abortReq) {
      st.pendingAbort = null;
      abortReq.resolve();
    }
  }
  st.state = 'closed';
  if (st.writer) {
    const writerState = __wtrState(st.writer);
    // A close queued while the stream was over its high-water mark left
    // ready pending (the write-completion path skips backpressure while
    // closing); workerd resolves it when the close lands (#245).
    __resolveReady(writerState);
    __resolveClosed(writerState);
  }
}

function __finishCloseWithError(st, e) {
  const abortReq = st.pendingAbort;
  if (st.state === 'erroring' && abortReq !== null &&
      st.closesOnError && !abortReq.wasErroring) {
    // The abort() that errored the stream raced a close() whose flush is
    // now failing: on workerd's internal (compression) streams the abort
    // still wins (#270). The close rejects with the abort reason, the
    // sink abort still runs, and the writable ends in the closed shape:
    // the abort fulfills and writer.closed rejects with the abort reason.
    // The pending read keeps the flush failure -- the sink's close
    // algorithm errored the readable with it before this ran -- while
    // later write()/close() calls report the closed stream. Plain streams
    // keep the close-wins outcome below, as workerd does.
    st.inFlightClose.reject(st.storedError);
    st.inFlightClose = null;
    st.pendingAbort = null;
    let result;
    try {
      result = st.abortFn
        ? st.abortFn.call(st.sink, abortReq.reason)
        : undefined;
    } catch (e2) { result = Promise.reject(e2); }
    Promise.resolve(result).then(
      () => {
        abortReq.resolve();
        st.state = 'closed';
        if (st.writer) {
          const writerState = __wtrState(st.writer);
          __resolveReady(writerState);
          __rejectClosedWriter(writerState, st.storedError);
        }
      },
      (e2) => {
        abortReq.reject(e2);
        __rejectCloseAndClosed(st);
      },
    );
    return;
  }
  st.inFlightClose.reject(e);
  st.inFlightClose = null;
  // A pending abort is rejected with the close error;
  // the sink abort is not run.
  if (abortReq) {
    st.pendingAbort = null;
    abortReq.reject(e);
  }
  if (st.closesOnError) __closeAfterError(st, e);
  else __dealWithRejection(st, e);
}

// A failing write or flush on an internal stream (workerd's compression
// streams) settles the writable side as *closed*, not errored (#223): later
// write()/close() calls report "This WritableStream has been closed.",
// desiredSize is 0 and abort() resolves, while writer.closed rejects with the
// failure and writer.ready resolves. Queued writes and a queued close reject
// with the closed-stream TypeError, as workerd does, instead of the stored
// error.
function __closeAfterError(st, e) {
  // An abort already moved the stream to 'erroring': that path owns the
  // settle (and rejects the abort with the stored error).
  if (st.state !== 'writable') {
    __dealWithRejection(st, e);
    return;
  }
  st.state = 'closed';
  const closedError = new TypeError(
    "This WritableStream has been closed.");
  for (const entry of st.queue) entry.reject(closedError);
  st.queue = [];
  st.queueSize = 0;
  if (st.closeRequest) {
    st.closeRequest.reject(closedError);
    st.closeRequest = null;
  }
  // An abort racing this push lost: workerd keeps the push failure (closed
  // rejects with it) and resolves abort() (#230).
  const abortReq = st.pendingAbort;
  if (abortReq) {
    st.pendingAbort = null;
    abortReq.resolve();
  }
  const writerState = st.writer === null ? undefined : __wtrState(st.writer);
  if (writerState !== undefined) {
    __resolveReady(writerState);
    __rejectClosedWriter(writerState, e);
  }
}

class WritableStream {
  #s;


  constructor(underlyingSink, queuingStrategy) {
    const st = this.#s = new WritableStreamState();
    _writableStates.set(this, st);
    // Per spec: extract strategy fields first. Size is
    // read and validated before highWaterMark; invalid
    // size must beat invalid highWaterMark.
    if (queuingStrategy !== undefined) {
      const size = queuingStrategy.size;
      if (size !== undefined
          && typeof size !== 'function')
        throw new TypeError(
          'strategy.size must be a function');
      st.sizeFn = size;
      const hwm = queuingStrategy.highWaterMark;
      if (hwm !== undefined) {
        const n = Number(hwm);
        // Workerd throws TypeError here, not the spec's
        // RangeError.
        if (Number.isNaN(n) || n < 0)
          throw new TypeError(
            'highWaterMark must be non-negative');
        st.hwm = n;
      }
    }
    const sink = underlyingSink || {};
    if (sink.type !== undefined)
      throw new RangeError("Invalid type");
    st.sink = sink;
    // Cache sink algorithms once per spec + bind this.
    st.startFn = sink.start;
    st.writeFn = sink.write;
    st.closeFn = sink.close;
    st.abortFn = sink.abort;
    st.controller = new WritableStreamDefaultController(
      _wsc_brand, this);
    st.backpressure = st.queueSize >= st.hwm;
    if (st.startFn) {
      // A sync start throw becomes a rejection rather
      // than escaping the constructor; per spec it
      // errors the stream once started is recorded.
      let result;
      try {
        result = st.startFn.call(sink, st.controller);
      } catch (e) { result = Promise.reject(e); }
      Promise.resolve(result).then(
        () => { st.started = true; __advanceQueue(st); },
        (e) => { st.started = true; __dealWithRejection(st, e); },
      );
    } else {
      // Workerd starts a sink with no start() synchronously:
      // a write dispatched before the next microtask is
      // already in flight when an abort arrives, so it
      // completes instead of being rejected with the queue.
      st.started = true;
    }
  }

  getWriter() {
    return new WritableStreamDefaultWriter(this);
  }

  get locked() { return this.#s.locked; }

  close() {
    if (this.#s.locked)
      return Promise.reject(new TypeError(
        "Cannot close a locked stream"));
    return __close(this.#s);
  }

  abort(reason) {
    if (this.#s.locked)
      return Promise.reject(new TypeError(
        "Cannot abort a locked stream"));
    return __abort(this.#s, reason);
  }
}

// Spec'd controller surface passed to transformer.start /
// transform / flush. Methods forward into the readable
// side's controller; `error()` also captures the first
// error so writer.close() can replay it (see the close
// path below for why that matters).
class TransformStreamDefaultController {
  #rsc;
  #onError;
  #onEnqueue;
  #onTerminate;
  constructor(rsc, onError, onEnqueue, onTerminate) {
    this.#rsc = rsc;
    this.#onError = onError;
    this.#onEnqueue = onEnqueue;
    this.#onTerminate = onTerminate;
  }
  enqueue(chunk) {
    try {
      this.#rsc.enqueue(chunk);
    } catch (e) {
      // Spec TransformStreamDefaultControllerEnqueue: a strategy size()
      // that throws (or returns a bad value) errors the readable; the
      // writable must follow, or a write parked on the handshake waits
      // for a pull the dead readable will never make. An enqueue on an
      // already closed or errored readable leaves desiredSize as it was.
      if (this.#rsc.desiredSize === null) this.#onError(e);
      throw e;
    }
    // Per spec the backpressure flag follows the enqueue, not
    // precedes it: the chunk may have satisfied a pending read.
    this.#onEnqueue();
  }
  terminate() {
    // Spec ReadableStreamDefaultControllerClose returns quietly when the
    // readable can no longer close (already closed or errored); the
    // writable side is still errored below.
    try { this.#rsc.close(); } catch {}
    // Spec: terminate also errors the writable side. Without it a write
    // the closed readable will never pull again would park forever.
    this.#onTerminate(
      new TypeError('The transform stream was terminated.'));
  }
  error(e) {
    // Spec TransformStreamError: error the readable side first, then error
    // the writable side and unblock a write parked on the handshake.
    this.#rsc.error(e);
    this.#onError(e);
  }
  get desiredSize() { return this.#rsc.desiredSize; }
}

// ---- transform pair state --------------------------------------------------
const _transformStates = new WeakMap();

function __pairOf(ts) {
  const pair = _transformStates.get(ts);
  if (pair === undefined)
    throw new TypeError("Illegal invocation");
  return pair;
}

// TransformStream -- pairs a readable + writable
//
// The pair runs the spec's backpressure handshake: the readable
// defaults to a highWaterMark of 0, so it holds the writable side
// back until a consumer pulls. A queued write waits on
// [[backpressureChangePromise]] before its transform runs, which is
// what keeps `await writer.write(...)` (and `writer.ready`) pending
// while nobody reads -- and, end to end, keeps a Worker writing a
// streamed Response, whose HTTP client stopped reading, from
// buffering the body in the isolate's heap.
class TransformStream {
  #s;

  constructor(transformer = {}, writableStrategy,
              readableStrategy) {
    const pair = this.#s = { readable: null, writable: null };
    _transformStates.set(this, pair);
    // Cache transformer methods once (may include proto
    // chain); call with .call(transformer, ...) so `this`
    // binds per spec.
    const startFn = transformer.start;
    const transformFn = transformer.transform;
    const flushFn = transformer.flush;
    const cancelFn = transformer.cancel;
    let rsc;
    // State for cross-side coordination. cancelled ->
    // transformer.cancel already ran (or started). flushing
    // -> writable.close triggered flush(). Whichever fires
    // first wins; the other side becomes a no-op.
    let cancelled = false;
    let flushing = false;
    let writable;
    // [[backpressure]] and [[backpressureChangePromise]]. Flipping the
    // flag settles the promise the other side is parked on and installs
    // the one it must wait on next: a parked write waits for a pull, and
    // a pull waits for the write that stalls the readable again.
    let backpressure = false;
    let backpressureChangeResolve = null;
    let backpressureChangePromise = null;
    const setBackpressure = (value) => {
      if (backpressure === value) return;
      backpressure = value;
      if (backpressureChangeResolve) backpressureChangeResolve();
      backpressureChangePromise = new Promise((resolve) => {
        backpressureChangeResolve = resolve;
      });
    };
    // Spec: TransformStreamSetBackpressure(stream, true) before the
    // writable side exists, so the first write waits for a consumer.
    setBackpressure(true);
    const runCancel = (reason) => {
      if (cancelled) return Promise.resolve();
      cancelled = true;
      if (!cancelFn) return Promise.resolve();
      return Promise.resolve(
        cancelFn.call(transformer, reason));
    };
    const readable = new ReadableStream({
      start(c) { rsc = c; },
      pull() {
        // The consumer wants data: release the parked write and hand
        // the readable the promise for the next stall. A second read
        // while a write is in flight reuses that promise.
        setBackpressure(false);
        return backpressureChangePromise;
      },
      async cancel(reason) {
        const wst = writable ? __wsState(writable) : null;
        if (wst !== null && wst.closesOnError) {
          // Workerd's compression streams are internal on the read side: a
          // cancel records its error on the writable and leaves it
          // writable, so a write it already accepted still completes (the
          // transform runs, its output is dropped), a write accepted after
          // it refuses with that error, and an `abort()` is an ordinary
          // abort (#252, #257). Recording the cancel before the `flushing`
          // check is what makes a close that already started its flush skip
          // the enqueue (#262). The native stream ends once nothing
          // accepted is still outstanding -- right here when the cancel
          // finds the writable idle, otherwise when the last accepted
          // write settles -- and the outcome is stashed so a close that
          // follows replays the member's true end instead of failing with
          // `zlib stream already closed` or resolving a close workerd
          // rejects (#270). The flush consumes the id, an abort drops it,
          // and abandonment is covered by the finalizer.
          wst.cancelled = __cello.$$cancelError(reason);
          if (wst.unblockWrite) wst.unblockWrite();
          __maybeEndCancelled(wst);
          return;
        }
        if (flushing) return;
        if (writable) {
          const target = __wsState(writable);
          // Workerd fails the writes a readable cancel lands on with a
          // TypeError naming the readable side -- but only the ones that
          // never started transforming. A write whose transform is already
          // running still completes (its output is dropped), and so does a
          // write queued behind it: the queue is resolved and dropped here
          // while the in-flight write finishes on its own. Writes parked
          // on the backpressure handshake, or queued behind one, fail with
          // the TypeError instead; the stored error stays the cancel reason
          // either way (#272).
          if (target.state === 'writable' && !target.started &&
              target.queue.length > 0) {
            // The cancel landed before the writable's start gate opened,
            // so no write was ever dispatched: workerd resolves the queued
            // writes (and the pending ready) instead of failing them,
            // while closed still rejects with the cancel reason (#283).
            // Settle closed first, then ready, then the writes -- the
            // observed order -- before erroring the stream for the gate.
            const writerState = target.writer
              ? __wtrState(target.writer)
              : null;
            if (writerState) __rejectClosedWriter(writerState, reason);
            if (writerState && writerState.readyResolve) {
              writerState.readyResolve();
              writerState.readyResolve = null;
              writerState.readyReject = null;
            }
            for (const entry of target.queue) {
              target.queueSize -= entry.size;
              entry.resolve();
            }
            if (target.queueSize < 0) target.queueSize = 0;
            target.queue = [];
            target.backpressure = false;
          } else if (target.state === 'writable' && target.started &&
              (target.inFlightWrite !== null || target.queue.length > 0)) {
            if (transformRunning) {
              for (const entry of target.queue) {
                target.queueSize -= entry.size;
                entry.resolve();
              }
              if (target.queueSize < 0) target.queueSize = 0;
              target.queue = [];
              // The erroring path below rejects the pending ready with the
              // stored error; detach it first so it stays fulfilled, as on
              // Workerd.
              if (target.writer) {
                const ws = __wtrState(target.writer);
                if (ws.readyResolve) ws.readyResolve();
                ws.readyResolve = null;
                ws.readyReject = null;
              }
            } else {
              target.writeError = new TypeError(
                "The readable side of this TransformStream is no longer " +
                "readable.");
              // The erroring path below would reject the pending ready
              // with the stored error; detach it first so it stays
              // fulfilled, as on Workerd (like the running branch above).
              if (target.writer) {
                const ws = __wtrState(target.writer);
                if (ws.readyResolve) ws.readyResolve();
                ws.readyResolve = null;
                ws.readyReject = null;
              }
            }
          }
          __errorStream(target, reason);
        }
        await runCancel(reason);
      },
    }, {
      // Spec: the readable side defaults to a highWaterMark of 0,
      // unlike a bare ReadableStream's 1.
      highWaterMark: readableStrategy?.highWaterMark ?? 0,
      size: readableStrategy?.size,
    });
    // Workerd's transform readable updates the writable side's backpressure
    // from its queue directly (a native backpressure listener), so a read
    // that frees a queue slot releases a parked write. This readable is a
    // generic ReadableStream, which no longer pulls on a dequeue (workerd's
    // generic controller does not either), so observe the dequeue instead:
    // with highWaterMark > 0 a read can leave desiredSize positive without
    // any pull running, and the parked write must resume.
    __rsState(readable).onDequeue = () => {
      setBackpressure(!(rsc.desiredSize > 0));
    };
    // Workerd extension: expectedLength on the
    // transformer feeds Content-Length when the
    // readable becomes a fetch body.
    if (transformer.expectedLength !== undefined)
      __rsState(readable).expectedLength =
        Number(transformer.expectedLength);
    // A readable cancel closes the readable side itself, so a compression
    // flush that still ran must not close it a second time (#262). A pending
    // abort owns the readable the same way: the abort errors it with the
    // abort reason, so the flush must not close it first, or a read would
    // resolve with bytes workerd rejects with (#270). Plain transforms
    // never have a pending abort here, so only the cancel arm applies.
    const readableCancelled = () => {
      if (writable === undefined) return false;
      const wst = __wsState(writable);
      return wst.cancelled !== null ||
        (wst.closesOnError && wst.pendingAbort !== null);
    };
    // Track the first controller.error() so that
    // transformer.flush()'s side effect can be replayed
    // on writer.close() (rsc's error state is private).
    let readableError = null;
    // start() runs before the writable exists (the readable side is
    // constructed first), so a controller error or terminate from inside
    // start has nowhere to go yet. Stash it and apply it as soon as the
    // writable is constructed: otherwise the write that later arrives parks
    // on a handshake nothing will ever release.
    let startWindowError = null;
    const errorWritable = (e) => {
      if (writable) __errorStream(__wsState(writable), e);
      else if (startWindowError === null) startWindowError = e;
    };
    const ctrl = new TransformStreamDefaultController(rsc, (e) => {
      if (readableError === null) readableError = e;
      // Spec: TransformStreamErrorWritableAndUnblockWrite.
      errorWritable(e);
    }, () => {
      // Spec: TransformStreamDefaultControllerEnqueue sets the
      // backpressure flag when the readable no longer needs data.
      setBackpressure(!(rsc.desiredSize > 0));
    }, (e) => { errorWritable(e); });
    // start runs once at construction per spec. A throw -- sync or
    // async -- must error both sides rather than escape the
    // constructor, so wrap it: the executor turns a sync throw into
    // a rejection, and handing that to the writable's own start gate
    // rejects every queued write and the close.
    const started = startFn
      ? new Promise((res) => res(startFn.call(transformer, ctrl)))
      : undefined;
    started?.catch((e) => rsc.error(e));
    // Whether the in-flight write has started transforming. A readable
    // cancel fails the writes it lands on with a TypeError naming the
    // readable side, but only the ones that never started transforming;
    // one whose transform is running still completes, and so does a write
    // queued behind it (#272).
    let transformRunning = false;
    // The transform itself, once the readable has asked for it. A throw
    // or rejection errors both sides and settles the write.
    const performTransform = (chunk) => {
      let result;
      try {
        transformRunning = true;
        result = transformFn
          ? transformFn.call(transformer, chunk, ctrl)
          : ctrl.enqueue(chunk);
      } catch (e) {
        rsc.error(e);
        throw e;
      }
      return Promise.resolve(result).catch((e) => {
        rsc.error(e);
        throw e;
      });
    };
    writable = new WritableStream({
      start() { return started; },
      write(chunk) {
        // The default sink-write algorithm: stay parked while the
        // readable is full, so the producer's write -- and with it the
        // producer's `await` -- waits for the consumer to pull.
        transformRunning = false;
        if (!backpressure) return performTransform(chunk);
        const change = backpressureChangePromise;
        return change.then(() => {
          // Abort, controller.error(), or a failed flush may have
          // errored the stream while this write was parked: fail it
          // with the stored error instead of transforming into a dead
          // readable. A readable cancel fails it with its own TypeError
          // naming the readable side instead (#272).
          const parked = __wsState(writable);
          if (parked.state === 'erroring')
            throw parked.writeError ?? parked.storedError;
          return performTransform(chunk);
        });
      },
      close() {
        if (cancelled) return;
        flushing = true;
        if (!flushFn) {
          rsc.close();
          return;
        }
        // Important: the sync throw case. A flush that
        // throws synchronously skips .then(..., onRejected)
        // on a Promise.resolve()-wrapped return, so any
        // rsc.error() handler attached that way never
        // fires. Defer flushFn into a thenable so sync
        // throws become async rejections that reach the
        // error arm below.
        return Promise.resolve().then(
          () => flushFn.call(transformer, ctrl),
        ).then(() => {
          // flush may have called controller.error;
          // propagate that to writer.close() so it
          // rejects with the original error.
          if (readableError !== null) throw readableError;
          if (!readableCancelled()) rsc.close();
        }, (e) => {
          rsc.error(e);
          throw e;
        });
      },
      async abort(reason) {
        try {
          await runCancel(reason);
          rsc.error(reason);
        } catch (e) {
          rsc.error(e);
          throw e;
        }
      },
    }, writableStrategy);
    // An abort or a readable failure enters 'erroring'; the hook wakes a
    // write parked on the handshake so it can reject with the stored
    // error instead of waiting for a pull that will never come. Module
    // private so tenant code cannot shadow it.
    __wsState(writable).unblockWrite = () => setBackpressure(false);
    // A controller.error()/terminate() from start() ran before the
    // writable existed; apply it now, before any write can queue.
    if (startWindowError !== null)
      __errorStream(__wsState(writable), startWindowError);
    pair.readable = readable;
    pair.writable = writable;
  }

  get readable() { return __pairOf(this).readable; }
  get writable() { return __pairOf(this).writable; }
}

globalThis.WritableStream = WritableStream;
globalThis.WritableStreamDefaultWriter =
  WritableStreamDefaultWriter;
globalThis.WritableStreamDefaultController =
  WritableStreamDefaultController;
globalThis.TransformStream = TransformStream;
globalThis.TransformStreamDefaultController =
  TransformStreamDefaultController;

// The runtime's own modules drive writable state and adopt a
// TransformStream's pair through these hooks; tenant code cannot reach
// `__cello`.
__cello.$$writableStreamState = __wsState;
// Brands for the clone codec: a constructed stream, not a forged prototype.
// The intrinsics are captured: the host storage predicate calls both brands
// while a guest may have patched the prototype or `Reflect.apply`.
const _writableStatesHas = WeakMap.prototype.has;
const _transformStatesHas = WeakMap.prototype.has;
const _transformStatesGet = WeakMap.prototype.get;
const _streamsBrandApply = Reflect.apply;
__cello.$$isWritableStream = (value) =>
  value !== null && typeof value === "object" &&
  _streamsBrandApply(_writableStatesHas, _writableStates, [value]);
__cello.$$isTransformStream = (value) =>
  value !== null && typeof value === "object" &&
  _streamsBrandApply(_transformStatesHas, _transformStates, [value]);
__cello.$$errorStream = (stream, e) =>
  __errorStream(__wsState(stream), e);
__cello.$$setTransformPair = (target, readable, writable, typeName) => {
  let pair = _transformStates.get(target);
  if (pair === undefined) {
    pair = {
      readable: null,
      writable: null,
      typeName: typeName ?? "TransformStream",
    };
    _transformStates.set(target, pair);
  }
  pair.readable = readable;
  pair.writable = writable;
  if (typeName !== undefined) pair.typeName = typeName;
};
// The interface a clone refusal names: workerd reports the concrete
// subclass, and the name is state, not a property a guest could answer.
__cello.$$transformTypeName = (value) =>
  _streamsBrandApply(_transformStatesGet, _transformStates, [value])
    ?.typeName ?? "TransformStream";

// WebIDL reflection (#171, #206).
for (const [cls, name] of [
  [WritableStream, 'WritableStream'],
  [WritableStreamDefaultWriter,
    'WritableStreamDefaultWriter'],
  [WritableStreamDefaultController,
    'WritableStreamDefaultController'],
  [TransformStream, 'TransformStream'],
  [TransformStreamDefaultController,
    'TransformStreamDefaultController'],
]) {
  __cello.$$webidl(cls, name);
}
