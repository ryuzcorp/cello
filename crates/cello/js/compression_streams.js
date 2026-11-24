// CompressionStream / DecompressionStream -- WHATWG Compression Streams,
// each a TransformStream over one stateful native zlib stream
// (__zlib_stream_new / push / end / drop). Output streams chunk by chunk
// as input is written; the terminal block arrives from flush() when the
// writable side closes. The native state is released on that flush, on
// cancel or abort, on a push error, and -- for a stream abandoned
// mid-flight -- by a FinalizationRegistry once the transformer, which
// both sides keep alive, is collected. The isolate's table also goes
// when the isolate does, and caps how many streams can be live at once.
//
// Workerd's CompressionStream is a TransformStream subclass (readable and
// writable are inherited accessors), so the pair lives in the
// TransformStream private state, never as own fields (#170, #206).
//
// Compiled on first access (see LAZY_GLOBALS); a bundle that never names
// either global never pays for this file.
return (() => {

// Per spec the chunk must be a BufferSource; anything else is a
// TypeError, which errors both sides of the transform. Workerd's byte
// transforms also take a string (encoded as UTF-8, measured on
// 1.20261121.1) and treat undefined as an empty chunk, and its TypeError
// names the stream class rather than the value (#230).
const utf8 = new TextEncoder();

function bytes(chunk) {
  if (typeof chunk === 'string') return utf8.encode(chunk);
  if (chunk === undefined) return new Uint8Array(0);
  if (ArrayBuffer.isView(chunk))
    return new Uint8Array(chunk.buffer, chunk.byteOffset,
                          chunk.byteLength);
  if (chunk instanceof ArrayBuffer) return new Uint8Array(chunk);
  throw new TypeError(
    'This TransformStream is being used as a byte stream, but received ' +
    'an object of non-ArrayBuffer/ArrayBufferView type on its writable ' +
    'side.');
}

// Releases the native stream of a collected transformer. Ids are never
// reused, so a release after flush or cancel already freed it is a
// no-op. Created with the first stream, not when this file compiles.
let registry;

function make(format, decompress, name, missing) {
  // Workerd stringifies a present argument and rejects only a missing one
  // (measured: `new CompressionStream(5)` and a `toString` object both reach
  // the format message, an explicit undefined does too).
  if (missing)
    throw new TypeError(
      `Failed to construct '${name}': constructor parameter 1 is not ` +
      'of type \'string\'.');
  format = String(format);
  if (format !== 'gzip' && format !== 'deflate' &&
      format !== 'deflate-raw' && format !== 'brotli')
    throw new TypeError(
      "The compression format must be either 'deflate', " +
      "'deflate-raw', 'gzip' or 'brotli'.");
  let id;
  // The TransformStream's readable and writable sides both close over
  // this object, so it is collectable exactly when neither side is
  // reachable any more.
  let ts;
  // A readable cancel drops the output -- the readable is gone -- but not
  // the compressor: workerd's internal streams keep the writable open, so a
  // write it already accepted still runs and its push failure is what the
  // stream keeps. The native state is freed by `flush()` (a later close),
  // by the writable's abort, or by the finalizer (#252, #257).
  //
  // A pending abort drops the output too: the abort owns the readable from
  // here on, erroring it with the abort reason, so a flush that still runs
  // must not enqueue into it (nor close it, see the TransformStream close
  // path) -- otherwise a read would resolve with bytes workerd rejects
  // with (#270).
  const readableCancelled = () => {
    if (ts === undefined) return false;
    const wst = __cello.$$writableStreamState(ts.writable);
    return wst.cancelled !== null ||
      wst.pendingAbort !== null;
  };
  const transformer = {
    transform(chunk, controller) {
      let out;
      try {
        out = __zlib_stream_push(id, bytes(chunk));
        if (out.byteLength && !readableCancelled())
          controller.enqueue(out);
      } catch (e) {
        __zlib_stream_drop(id);
        throw e;
      }
    },
    flush(controller) {
      const out = __zlib_stream_end(id);
      if (out.byteLength && !readableCancelled())
        controller.enqueue(out);
    },
    cancel() { __zlib_stream_drop(id); },
  };
  ts = new TransformStream(transformer);
  // Workerd's compression streams are internal streams: a
  // pending read() rejects with the cancel reason instead
  // of resolving {done: true}, and it settles only after
  // the microtask drain, so a cancel() called right after
  // the producing write still rejects it (#252).
  const rst = __cello.$$readableState(ts.readable);
  rst.cancelRejectsReads = true;
  rst.deferReads = true;
  // ... and a failed push or flush closes the writable side
  // instead of erroring it (#223).
  __cello.$$writableStreamState(ts.writable).closesOnError = true;
  // Last, so nothing after it can throw and strand the native stream.
  // Throws a RangeError when the isolate holds too many.
  id = __zlib_stream_new(format, decompress);
  try {
    registry ??= new FinalizationRegistry(__zlib_stream_drop);
    registry.register(transformer, id);
  } catch (e) {
    __zlib_stream_drop(id);
    throw e;
  }
  return ts;
}

class CompressionStream {
  constructor(format) {
    const ts = make(format, false, 'CompressionStream',
                    arguments.length === 0);
    __cello.$$setTransformPair(this, ts.readable, ts.writable,
                               'CompressionStream');
  }
}

class DecompressionStream {
  constructor(format) {
    const ts = make(format, true, 'DecompressionStream',
                    arguments.length === 0);
    __cello.$$setTransformPair(this, ts.readable, ts.writable,
                               'DecompressionStream');
  }
}

// Workerd wires both as TransformStream subclasses; the base constructor
// must never run, so the chains are set directly.
Object.setPrototypeOf(CompressionStream.prototype,
                      TransformStream.prototype);
Object.setPrototypeOf(CompressionStream, TransformStream);
Object.setPrototypeOf(DecompressionStream.prototype,
                      TransformStream.prototype);
Object.setPrototypeOf(DecompressionStream, TransformStream);

// WebIDL reflection (#171, #206).
__cello.$$webidl(CompressionStream, 'CompressionStream');
__cello.$$webidl(DecompressionStream, 'DecompressionStream');

return { CompressionStream, DecompressionStream };
})()
