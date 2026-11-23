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
// TypeError, which errors both sides of the transform.
function bytes(chunk) {
  if (ArrayBuffer.isView(chunk))
    return new Uint8Array(chunk.buffer, chunk.byteOffset,
                          chunk.byteLength);
  if (chunk instanceof ArrayBuffer) return new Uint8Array(chunk);
  throw new TypeError(
    'The provided value is not of type (ArrayBuffer or ' +
    'ArrayBufferView).');
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
  const transformer = {
    transform(chunk, controller) {
      let out;
      try {
        out = __zlib_stream_push(id, bytes(chunk));
        if (out.byteLength) controller.enqueue(out);
      } catch (e) {
        __zlib_stream_drop(id);
        throw e;
      }
    },
    flush(controller) {
      const out = __zlib_stream_end(id);
      if (out.byteLength) controller.enqueue(out);
    },
    cancel() { __zlib_stream_drop(id); },
  };
  const ts = new TransformStream(transformer);
  // Workerd's compression streams are internal streams: a
  // pending read() rejects with the cancel reason instead
  // of resolving {done: true}.
  __cello.$$readableState(ts.readable).cancelRejectsReads = true;
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
