// Every case returns a body that already contains whatever order the Worker
// observed, so the harness never looks at wall time. Response bodies use byte
// chunks: Workers only accepts `Uint8Array`/`ArrayBuffer`.

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const text = (body) => new Response(body, { headers: { "content-type": "text/plain; charset=utf-8" } });
const report = (value) => text(JSON.stringify(value));
const errorInfo = (error) => ({ name: error?.name ?? String(error), message: error?.message ?? String(error) });
const attempt = (fn) => {
  try { return fn(); } catch (error) { return errorInfo(error); }
};
const checksum = (value) => {
  let sum = 0;
  for (const char of value) sum = (sum * 31 + char.codePointAt(0)) % 2147483647;
  return sum;
};

const readAll = async (stream) => {
  const reader = stream.getReader();
  const parts = [];
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) return parts;
      parts.push(value);
    }
  } finally {
    reader.releaseLock();
  }
};
const readText = async (stream) => (await readAll(stream)).map((chunk) => decoder.decode(chunk)).join("");

const byteStream = (chunks) =>
  new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      controller.close();
    },
  });

/** CRC-32 (gzip's polynomial), so a member can be built by hand. */
const crc32 = (bytes) => {
  let crc = 0xffffffff;
  for (const byte of bytes) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit += 1) crc = (crc >>> 1) ^ (0xedb88320 & -(crc & 1));
  }
  return (crc ^ 0xffffffff) >>> 0;
};

const rawDeflate = async (bytes) =>
  new Uint8Array(await new Response(new Response(bytes).body.pipeThrough(new CompressionStream("deflate-raw"))).arrayBuffer());

/**
 * A single gzip member built field by field, so the header CRC, the trailer's
 * CRC-32 and ISIZE can each be made right or wrong on purpose. `headerCrc`
 * is "none" (no FHCRC field), "valid", or "wrong".
 */
async function gzipMember(payload, { headerCrc = "none", crc = crc32(payload), isize = payload.length } = {}) {
  const body = await rawDeflate(payload);
  const head = [0x1f, 0x8b, 8, headerCrc === "none" ? 0 : 2, 0, 0, 0, 0, 0, 0xff];
  if (headerCrc !== "none") {
    const value = crc32(new Uint8Array(head));
    const low = headerCrc === "wrong" ? value ^ 0xffff : value;
    head.push(low & 0xff, (low >>> 8) & 0xff);
  }
  const trailer = new Uint8Array(new Uint32Array([crc, isize]).buffer);
  return new Uint8Array([...head, ...body, ...trailer]);
}

/** Compresses, then decompresses, and reports what came back out. */
async function roundTrip(format, payload) {
  const compressed = await new Response(new Response(payload).body.pipeThrough(new CompressionStream(format))).arrayBuffer();
  const decompressed = await new Response(new Response(compressed).body.pipeThrough(new DecompressionStream(format))).text();
  return { compressed: new Uint8Array(compressed), decompressed };
}

const routes = {
  async "/readable-basics"() {
    const rs = new ReadableStream({ start: (c) => (c.enqueue("a"), c.enqueue("b"), c.close()) });
    const reader = rs.getReader();
    const seen = [];
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      seen.push(value);
    }
    return report({ seen, locked: rs.locked, end: await reader.read() });
  },

  async "/readable-pull-order"() {
    // Issue #113: a pull source runs one pull per read request, and the tee/
    // enqueue pull chain advances in microtasks. Cello used to refill after
    // the first `read()` dequeued, so it reported a second pull already.
    let pulls = 0;
    const rs = new ReadableStream({
      pull(controller) {
        pulls += 1;
        controller.enqueue(`c${pulls}`);
        if (pulls === 3) controller.close();
      },
    });
    const reader = rs.getReader();
    const order = [];
    for (let index = 0; index < 3; index += 1) {
      const { value } = await reader.read();
      order.push([value, pulls]);
    }
    return report({ order, pulls, end: (await reader.read()).done });
  },

  async "/readable-byob"() {
    const rs = new ReadableStream({
      type: "bytes",
      start: (c) => (c.enqueue(new Uint8Array([1, 2, 3])), c.enqueue(new Uint8Array([4, 5])), c.close()),
    });
    const reader = rs.getReader({ mode: "byob" });
    const sizes = [];
    const bytes = [];
    let view = new Uint8Array(2);
    for (;;) {
      const { value, done } = await reader.read(view);
      if (done) break;
      sizes.push(value.byteLength);
      bytes.push(...value);
      view = new Uint8Array(4);
    }

    return report({ sizes, bytes });
  },

  async "/byte-read-order"() {
    // A byte-stream read settles one microtask after the pull enqueued its
    // chunk, like a default read: workerd's native binding converts the
    // read result through an extra promise hop. Both reader kinds hop.
    const run = async (byob) => {
      const order = [];
      let pulls = 0;
      const rs = new ReadableStream({
        type: "bytes",
        pull(c) {
          pulls += 1;
          order.push(`pull${pulls}`);
          c.enqueue(new Uint8Array([pulls]));
        },
      });
      const reader = byob ? rs.getReader({ mode: "byob" }) : rs.getReader();
      const pending = byob ? reader.read(new Uint8Array(1)) : reader.read();
      pending.then(() => order.push("read"));
      Promise.resolve().then(() => order.push("m1"));
      Promise.resolve().then(() => order.push("m2"));
      order.push("sync");
      // Let every microtask, and the read, finish before reporting.
      await new Promise((resolve) => setTimeout(resolve, 5));
      return order;
    };
    return report({ defaultByte: await run(false), byob: await run(true) });
  },

  async "/identity-read-order"() {
    // IdentityTransformStream is a native byte stream in workerd, and its
    // reads settle through the same binding hop as any other reader's:
    // after the microtasks queued at read() time.
    const run = async (byob) => {
      const ts = new IdentityTransformStream();
      const writer = ts.writable.getWriter();
      const reader = byob ? ts.readable.getReader({ mode: "byob" }) : ts.readable.getReader();
      const write = writer.write(new Uint8Array([1, 2, 3]));
      write.catch(() => {});
      const order = [];
      const pending = byob ? reader.read(new Uint8Array(1)) : reader.read();
      pending.then(() => order.push("read"));
      Promise.resolve().then(() => order.push("m1"));
      Promise.resolve().then(() => order.push("m2"));
      order.push("sync");
      await new Promise((resolve) => setTimeout(resolve, 5));
      return order;
    };
    return report({ identityDefault: await run(false), identityByob: await run(true) });
  },

  async "/writable-order"() {
    const order = [];
    const ws = new WritableStream({
      start: () => order.push("start"),
      write: (chunk) => order.push(`write:${chunk}`),
      close: () => order.push("close"),
    });
    const writer = ws.getWriter();
    const writes = [writer.write("a"), writer.write("b")];
    order.push("queued");
    await Promise.all(writes);
    order.push("written");
    await writer.close();
    return report({ order, done: await writer.closed.then(() => "closed", (error) => errorInfo(error)) });
  },

  async "/writable-close-states"() {
    // Issue #132: workerd names every state of a second close() itself.
    // closed -> "This WritableStream has been closed."; a close already in
    // flight -> "Cannot close a writer that is already being closed";
    // errored and erroring -> the stream's stored error (issue #129).
    // `flip` is a bounded microtask drain, never wall time: it lets a
    // queued close reach the sink so the state is provably 'closing'.
    const flip = async () => {
      for (let i = 0; i < 16; i++) await null;
    };
    const out = {};

    const closedStream = new WritableStream({});
    const closedWriter = closedStream.getWriter();
    await closedWriter.close();
    out.closed = {
      second: await closedWriter.close().then(() => "ok", errorInfo),
      third: await closedWriter.close().then(() => "ok", errorInfo),
    };

    let releaseClose;
    const closingStream = new WritableStream({ close: () => new Promise((resolve) => (releaseClose = resolve)) });
    const closingWriter = closingStream.getWriter();
    const firstClose = closingWriter.close();
    await flip();
    out.closing = {
      second: await closingWriter.close().then(() => "ok", errorInfo),
      third: await closingWriter.close().then(() => "ok", errorInfo),
    };
    releaseClose();
    out.closing.first = await firstClose.then(() => "ok", errorInfo);

    const erroredStream = new WritableStream({ write() { throw new Error("sink"); } });
    const erroredWriter = erroredStream.getWriter();
    out.errored = {
      write: await erroredWriter.write("x").then(() => "ok", errorInfo),
      close: await erroredWriter.close().then(() => "ok", errorInfo),
      closeAgain: await erroredWriter.close().then(() => "ok", errorInfo),
    };

    // 'erroring': controller.error() while a write is in flight parks the
    // stream; close() then queues behind the write and rejects with the
    // stored error once the write settles.
    let releaseWrite;
    let controller;
    const erroringStream = new WritableStream({
      start: (c) => (controller = c),
      write: () => new Promise((resolve) => (releaseWrite = resolve)),
    });
    const erroringWriter = erroringStream.getWriter();
    const inFlightWrite = erroringWriter.write("x").then(() => "ok", errorInfo);
    await flip();
    controller.error(new Error("boom"));
    await flip();
    const pendingClose = erroringWriter.close().then(() => "ok", errorInfo);
    releaseWrite();
    out.erroring = { close: await pendingClose, write: await inFlightWrite };

    return report(out);
  },

  async "/writable-abort-states"() {
    // Issue #133: workerd resolves abort() on a stream that already errored
    // and gives every abort() call its own promise, while all of them
    // follow the request's one abort. Aborting while the stream is still
    // erroring rejects with the stored error.
    const flip = async () => {
      for (let i = 0; i < 16; i++) await null;
    };
    const out = {};

    const writableStream = new WritableStream({});
    const writableWriter = writableStream.getWriter();
    const first = writableWriter.abort(new Error("why"));
    const second = writableWriter.abort(new Error("again"));
    out.writable = {
      same: first === second,
      first: await first.then(() => "ok", errorInfo),
      second: await second.then(() => "ok", errorInfo),
      third: await writableWriter.abort(new Error("third")).then(() => "ok", errorInfo),
      closed: await writableWriter.closed.then(() => "closed", errorInfo),
    };

    const erroredStream = new WritableStream({ write() { throw new Error("sink"); } });
    const erroredWriter = erroredStream.getWriter();
    out.errored = {
      write: await erroredWriter.write("x").then(() => "ok", errorInfo),
      abort: await erroredWriter.abort(new Error("why")).then(() => "ok", errorInfo),
      again: await erroredWriter.abort(new Error("again")).then(() => "ok", errorInfo),
      closed: await erroredWriter.closed.then(() => "closed", errorInfo),
    };

    const closedStream = new WritableStream({});
    const closedWriter = closedStream.getWriter();
    await closedWriter.close();
    const closedFirst = closedWriter.abort(new Error("why"));
    const closedSecond = closedWriter.abort(new Error("again"));
    out.closed = {
      same: closedFirst === closedSecond,
      first: await closedFirst.then(() => "ok", errorInfo),
      second: await closedSecond.then(() => "ok", errorInfo),
    };

    // A TransformStream whose readable a consumer cancelled: the writable
    // errored with the cancel reason (issue #129), and abort() on it
    // resolves rather than replaying that reason.
    const transformed = new TransformStream();
    const transformWriter = transformed.writable.getWriter();
    await transformed.readable.getReader().cancel("stop");
    out.transformCancel = {
      abort: await transformWriter.abort(new Error("why")).then(() => "ok", errorInfo),
      closed: await transformWriter.closed.then(() => "closed", errorInfo),
    };

    // A sink abort that throws rejects this call only; the stream stays
    // errored and writer.closed carries the reason.
    const throwingStream = new WritableStream({ abort() { throw new Error("nope"); } });
    const throwingWriter = throwingStream.getWriter();
    out.sinkAbortThrows = {
      abort: await throwingWriter.abort(new Error("why")).then(() => "ok", errorInfo),
      again: await throwingWriter.abort(new Error("again")).then(() => "ok", errorInfo),
      closed: await throwingWriter.closed.then(() => "closed", errorInfo),
    };

    // Aborting while 'erroring' rejects both calls with the stored error and
    // does not run the sink abort, which is what workerd does.
    let releaseWrite;
    let controller;
    let sinkAbort = "not-run";
    const erroringStream = new WritableStream({
      start: (c) => (controller = c),
      write: () => new Promise((resolve) => (releaseWrite = resolve)),
      abort: () => (sinkAbort = "ran"),
    });
    const erroringWriter = erroringStream.getWriter();
    const inFlightWrite = erroringWriter.write("x").then(() => "ok", errorInfo);
    await flip();
    controller.error(new Error("boom"));
    await flip();
    const aborting = erroringWriter.abort(new Error("why"));
    const abortingAgain = erroringWriter.abort(new Error("again"));
    releaseWrite();
    out.erroring = {
      same: aborting === abortingAgain,
      abort: await aborting.then(() => "ok", errorInfo),
      again: await abortingAgain.then(() => "ok", errorInfo),
      write: await inFlightWrite,
      sinkAbort,
    };

    return report(out);
  },

  async "/writable-write-states"() {
    // The write() side of issue #132: workerd's messages name the state,
    // and its closing-stream message says "ReadableStream".
    const flip = async () => {
      for (let i = 0; i < 16; i++) await null;
    };
    const out = {};

    const closedStream = new WritableStream({});
    const closedWriter = closedStream.getWriter();
    await closedWriter.close();
    out.closed = await closedWriter.write("x").then(() => "ok", errorInfo);

    let releaseClose;
    const closingStream = new WritableStream({ close: () => new Promise((resolve) => (releaseClose = resolve)) });
    const closingWriter = closingStream.getWriter();
    const pendingClose = closingWriter.close();
    await flip();
    out.closing = await closingWriter.write("x").then(() => "ok", errorInfo);
    releaseClose();
    await pendingClose;

    const erroredStream = new WritableStream({ write() { throw new Error("sink"); } });
    const erroredWriter = erroredStream.getWriter();
    out.errored = await erroredWriter.write("x").then(() => "ok", errorInfo);

    let releaseWrite;
    let controller;
    const erroringStream = new WritableStream({
      start: (c) => (controller = c),
      write: () => new Promise((resolve) => (releaseWrite = resolve)),
    });
    const erroringWriter = erroringStream.getWriter();
    const inFlightWrite = erroringWriter.write("x").then(() => "ok", errorInfo);
    await flip();
    controller.error(new Error("boom"));
    await flip();
    const blockedWrite = erroringWriter.write("y").then(() => "ok", errorInfo);
    releaseWrite();
    out.erroring = await blockedWrite;
    await inFlightWrite;

    return report(out);
  },

  async "/writer-released"() {
    // Issue #193: a released writer's surface. workerd throws the released
    // TypeError from desiredSize in every state (a cello writer used to keep
    // reporting the stream's size, and null once the stream errored),
    // rejects write/close/abort, and installs a fresh rejected ready: a read
    // after the release is the released error, while a handler attached
    // before it keeps the old settlement. closed keeps its settlement once
    // it has one. `flip` is a bounded microtask drain, never wall time: it
    // gets the write into the sink so ready is provably pending at release.
    const flip = async () => {
      for (let i = 0; i < 16; i++) await null;
    };
    const attempt = (fn) => {
      try { return String(fn()); } catch (error) { return errorInfo(error); }
    };
    const out = {};

    // Nothing pending: ready resolved at construction. `*Before` keeps a
    // handler on the pre-release promise; the plain reads come after.
    const idleStream = new WritableStream({}, { highWaterMark: 2 });
    const idleWriter = idleStream.getWriter();
    const idleReadyBefore = idleWriter.ready.then(() => "ok", errorInfo);
    idleWriter.releaseLock();
    const idleReady = idleWriter.ready.then(() => "ok", errorInfo);
    const idleClosed = idleWriter.closed.then(() => "ok", errorInfo);
    out.idle = {
      desiredSize: attempt(() => idleWriter.desiredSize),
      readyBefore: await idleReadyBefore,
      ready: await idleReady,
      closed: await idleClosed,
      write: await idleWriter.write("x").then(() => "ok", errorInfo),
      close: await idleWriter.close().then(() => "ok", errorInfo),
      abort: await idleWriter.abort("why").then(() => "ok", errorInfo),
      releaseAgain: attempt(() => idleWriter.releaseLock()),
      unlocked: idleStream.locked,
    };
    out.idle.reuse = attempt(() => idleStream.getWriter().desiredSize);

    // Ready still pending at release: backpressure holds the write.
    let releaseWrite;
    const busyStream = new WritableStream(
      { write: () => new Promise((resolve) => (releaseWrite = resolve)) },
      { highWaterMark: 0 });
    const busyWriter = busyStream.getWriter();
    const busyWrite = busyWriter.write("a").then(() => "ok", errorInfo);
    await flip();
    const busyReady = busyWriter.ready.then(() => "ok", errorInfo);
    const busyClosed = busyWriter.closed.then(() => "ok", errorInfo);
    busyWriter.releaseLock();
    out.pending = {
      desiredSize: attempt(() => busyWriter.desiredSize),
      ready: await busyReady,
      closed: await busyClosed,
    };
    releaseWrite();
    out.pending.write = await busyWrite;
    out.pending.unlocked = busyStream.locked;

    // Released after the stream closed: both promises stay resolved.
    const doneStream = new WritableStream({});
    const doneWriter = doneStream.getWriter();
    await doneWriter.close();
    const doneReadyBefore = doneWriter.ready.then(() => "ok", errorInfo);
    const doneClosedBefore = doneWriter.closed.then(() => "ok", errorInfo);
    doneWriter.releaseLock();
    const doneReady = doneWriter.ready.then(() => "ok", errorInfo);
    const doneClosed = doneWriter.closed.then(() => "ok", errorInfo);
    out.closedStream = {
      desiredSize: attempt(() => doneWriter.desiredSize),
      readyBefore: await doneReadyBefore,
      closedBefore: await doneClosedBefore,
      ready: await doneReady,
      closed: await doneClosed,
    };

    // Released after the stream errored: ready and closed keep the error.
    const badStream = new WritableStream({ write() { throw new TypeError("boom"); } });
    const badWriter = badStream.getWriter();
    await badWriter.write("a").then(() => "ok", errorInfo);
    const badReadyBefore = badWriter.ready.then(() => "ok", errorInfo);
    badWriter.releaseLock();
    const badReady = badWriter.ready.then(() => "ok", errorInfo);
    const badClosed = badWriter.closed.then(() => "ok", errorInfo);
    out.erroredStream = {
      desiredSize: attempt(() => badWriter.desiredSize),
      readyBefore: await badReadyBefore,
      ready: await badReady,
      closed: await badClosed,
    };

    return report(out);
  },

  async "/decompression-close-after-error"() {
    // Issue #223: workerd's compression streams are internal streams, so a
    // failed push closes the writable side instead of erroring it: a later
    // close()/write() reports the closed-stream TypeError, desiredSize is 0
    // and abort() resolves, while writer.closed rejects with the failure and
    // writer.ready resolves. A plain TransformStream rejects all three with
    // the thrown error instead, so this is the compression stream alone.
    const attempt = (fn) => {
      try { return String(fn()); } catch (error) { return errorInfo(error); }
    };
    const out = {};

    const stream = new DecompressionStream("gzip");
    const writer = stream.writable.getWriter();
    const ready = writer.ready.then(() => "ok", errorInfo);
    const closed = writer.closed.then(() => "ok", errorInfo);
    const read = new Response(stream.readable).arrayBuffer()
      .then((buffer) => `ok ${buffer.byteLength}`, errorInfo);
    out.write = await writer.write(new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))
      .then(() => "ok", errorInfo);
    out.readAfterWrite = await read;
    out.close = await writer.close().then(() => "ok", errorInfo);
    out.closeAgain = await writer.close().then(() => "ok", errorInfo);
    out.writeAgain = await writer.write(new Uint8Array([1])).then(() => "ok", errorInfo);
    out.desiredSize = attempt(() => writer.desiredSize);
    out.ready = await ready;
    out.closed = await closed;
    out.abort = await writer.abort("stop").then(() => "ok", errorInfo);
    out.locked = stream.writable.locked;

    // CompressionStream shares the internal-stream writable: a clean close
    // still resolves, and only the second close is the closed-stream error.
    const compress = new CompressionStream("gzip");
    const compressWriter = compress.writable.getWriter();
    const compressed = new Response(compress.readable).arrayBuffer()
      .then((buffer) => buffer.byteLength);
    out.compress = {
      write: await compressWriter.write(new Uint8Array([1, 2, 3])).then(() => "ok", errorInfo),
      close: await compressWriter.close().then(() => "ok", errorInfo),
      closeAgain: await compressWriter.close().then(() => "ok", errorInfo),
      read: await compressed,
    };

    return report(out);
  },

  async "/release-before-start"() {
    // Issue #232: releaseLock() while the writable's start gate is still
    // open. Workerd drops the released writer's queued write with the
    // released TypeError but leaves the stream usable: clearing the queue
    // must clear backpressure too, or the next writer's ready() never
    // resolves.
    const log = [];
    let release;
    const ws = new WritableStream({
      start() {
        log.push("start");
        return new Promise((resolve) => (release = resolve));
      },
      write(chunk) { log.push(`write:${chunk}`); },
    });
    const writer = ws.getWriter();
    const queued = writer.write("a").then(() => "ok", errorInfo);
    writer.releaseLock();
    log.push("released");
    const next = ws.getWriter();
    let ready = "pending";
    next.ready.then(() => (ready = "resolved"), (error) => (ready = errorInfo(error).message));
    for (let index = 0; index < 5; index += 1) await null;
    const beforeStart = { ready, desiredSize: next.desiredSize, locked: ws.locked };
    release();
    const write = await queued;
    for (let index = 0; index < 5; index += 1) await null;
    return report({ beforeStart, write, ready, desiredSize: next.desiredSize, log });
  },

  async "/erroring-desired-size"() {
    // Issue #232: a writer that is not released still reports the queue
    // formula while the stream is erroring, even when that goes negative;
    // only the fully errored stream answers null.
    let controller;
    let releaseWrite;
    const ws = new WritableStream({
      start(c) { controller = c; },
      write(chunk) {
        if (chunk === "a") return new Promise((resolve) => (releaseWrite = resolve));
      },
    }, { highWaterMark: 1 });
    const writer = ws.getWriter();
    const a = writer.write("a").then(() => "ok", errorInfo);
    const b = writer.write("b").then(() => "ok", errorInfo);
    await null;
    await null;
    controller.error("boom");
    const erroring = attempt(() => writer.desiredSize);
    const ready = await writer.ready.then(() => "ok", errorInfo);
    releaseWrite();
    const out = { erroring, ready, a: await a, b: await b };
    out.closed = await writer.closed.then(() => "ok", errorInfo);
    out.errored = attempt(() => writer.desiredSize);
    return report(out);
  },

  async "/compression-string-chunks"() {
    // Issue #230: a string chunk is UTF-8 encoded (and undefined is an
    // empty chunk) instead of failing the byte-stream type check, and the
    // type check names the stream class the way workerd does.
    const compress = new CompressionStream("gzip");
    const writer = compress.writable.getWriter();
    const packed = new Response(compress.readable).arrayBuffer();
    const empty = await writer.write(undefined).then(() => "ok", errorInfo);
    const text = await writer.write("héllo").then(() => "ok", errorInfo);
    const close = await writer.close().then(() => "ok", errorInfo);
    const bytes = new Uint8Array(await packed);
    const back = await new Response(new Response(bytes).body.pipeThrough(new DecompressionStream("gzip"))).text();

    const decompress = new DecompressionStream("gzip");
    const dWriter = decompress.writable.getWriter();
    const dRead = new Response(decompress.readable).arrayBuffer().then(() => "ok", errorInfo);
    const dWrite = await dWriter.write("not gzip").then(() => "ok", errorInfo);
    const dClose = await dWriter.close().then(() => "ok", errorInfo);

    const typed = await (async () => {
      const stream = new CompressionStream("gzip");
      const w = stream.writable.getWriter();
      // A pending read keeps the transform running. The chunk check fails
      // the write, and the readable is then cancelled: which way that
      // parking read settles depends on ordering, so it is not asserted.
      const reader = stream.readable.getReader();
      reader.read().catch(() => {});
      const result = await w.write(123).then(() => "ok", errorInfo);
      const cancel = await reader.cancel("done").then(() => "ok", errorInfo);
      return { result, cancel };
    })();

    return report({ empty, text, close, dWrite, dClose, dRead: await dRead, back, typed, length: bytes.length });
  },

  async "/compression-truncated-close"() {
    // Issue #230: a gzip member whose trailer never arrived reports the
    // incomplete-data error on close(), while a trailer that arrived but
    // does not check out fails the push that carried it, and bytes past
    // the member are their own error.
    const compress = async (bytes) =>
      new Uint8Array(await new Response(new Response(bytes).body.pipeThrough(new CompressionStream("gzip"))).arrayBuffer());
    const through = async (bytes) => {
      const stream = new DecompressionStream("gzip");
      const writer = stream.writable.getWriter();
      const read = new Response(stream.readable).arrayBuffer().then(() => "ok", errorInfo);
      return {
        write: await writer.write(bytes).then(() => "ok", errorInfo),
        close: await writer.close().then(() => "ok", errorInfo),
        read: await read,
      };
    };
    const whole = await compress(new TextEncoder().encode("hello world hello world"));
    const flipped = whole.slice();
    flipped[flipped.length - 1] ^= 0xff;
    const trailing = new Uint8Array([...whole, 1, 2, 3, 4]);
    return report({
      missingTrailer: await through(whole.slice(0, whole.length - 8)),
      cutTrailer: await through(whole.slice(0, whole.length - 4)),
      normal: await through(whole),
      badChecksum: await through(flipped),
      trailing: await through(trailing),
    });
  },

  async "/compression-abort-racing-push"() {
    // Issue #230: an abort() racing a compression push loses to it. The
    // parked write still runs, its push failure is the error the stream
    // keeps, and abort() resolves.
    const compress = async () =>
      new Uint8Array(await new Response(new Response(new TextEncoder().encode("payload payload")).body.pipeThrough(new CompressionStream("gzip"))).arrayBuffer());
    const whole = await compress();
    const bad = whole.slice(0, 14);
    bad[10] = 0xff;
    bad[11] = 0xff;
    bad[12] = 0xff;
    bad[13] = 0xff;
    const stream = new DecompressionStream("gzip");
    const writer = stream.writable.getWriter();
    const ready = writer.ready.then(() => "ok", errorInfo);
    const closed = writer.closed.then(() => "ok", errorInfo);
    const write = writer.write(bad).then(() => "ok", errorInfo);
    const abort = writer.abort("my-reason").then(() => "ok", errorInfo);
    const out = { abort: await abort, write: await write };
    await null;
    out.ready = await ready;
    out.closed = await closed;
    out.locked = stream.writable.locked;
    stream.readable.cancel("done");
    return report(out);
  },

  async "/compression-release-after-failure"() {
    // Issue #230: the writable of a compression stream is an internal
    // stream, so releaseLock() on it is a no-op once it has closed -- the
    // stream stays locked and desiredSize stays 0 -- instead of admitting
    // a new writer. A gzip member with a valid header and a corrupt deflate
    // body fails the push, which closes the writable side.
    const compress = async () =>
      new Uint8Array(await new Response(new Response(new TextEncoder().encode("payload payload")).body.pipeThrough(new CompressionStream("gzip"))).arrayBuffer());
    const whole = await compress();
    const bad = whole.slice(0, 14);
    bad[10] = 0xff;
    bad[11] = 0xff;
    bad[12] = 0xff;
    bad[13] = 0xff;
    const failed = new DecompressionStream("gzip");
    const failedWriter = failed.writable.getWriter();
    const failedClosed = failedWriter.closed.then(() => "ok", errorInfo);
    const failedRead = new Response(failed.readable).arrayBuffer().then(() => "ok", errorInfo);
    const failedWrite = await failedWriter.write(bad).then(() => "ok", errorInfo);
    const read = await failedRead;
    failedWriter.releaseLock();
    const failedOut = {
      write: failedWrite,
      read,
      locked: failed.writable.locked,
      desiredSize: attempt(() => failedWriter.desiredSize),
      getWriter: (() => {
        try { return typeof failed.writable.getWriter(); } catch (error) { return error.name; }
      })(),
      closed: await failedClosed,
    };

    const clean = new CompressionStream("gzip");
    const cleanWriter = clean.writable.getWriter();
    const cleanRead = new Response(clean.readable).arrayBuffer().then(() => "ok", errorInfo);
    await cleanWriter.write(new Uint8Array([1, 2, 3])).then(() => "ok", errorInfo);
    await cleanWriter.close().then(() => "ok", errorInfo);
    const cleanBytes = await cleanRead;
    cleanWriter.releaseLock();
    const cleanOut = {
      read: cleanBytes,
      locked: clean.writable.locked,
      desiredSize: attempt(() => cleanWriter.desiredSize),
      write: await cleanWriter.write(new Uint8Array([4])).then(() => "ok", errorInfo),
    };

    return report({ failed: failedOut, clean: cleanOut });
  },

  async "/compression-gzip-header-crc"() {
    // Issue #230 review: FHCRC is validated, not skipped. A correct header
    // CRC is accepted; a wrong one fails the push that carries it.
    const payload = encoder.encode("fhcrc payload fhcrc payload");
    const through = async (bytes) => {
      const stream = new DecompressionStream("gzip");
      const writer = stream.writable.getWriter();
      const read = new Response(stream.readable).arrayBuffer().then((buffer) => `ok ${buffer.byteLength}`, errorInfo);
      return {
        write: await writer.write(bytes).then(() => "ok", errorInfo),
        close: await writer.close().then(() => "ok", errorInfo),
        read: await read,
      };
    };
    return report({
      valid: await through(await gzipMember(payload, { headerCrc: "valid" })),
      wrong: await through(await gzipMember(payload, { headerCrc: "wrong" })),
      absent: await through(await gzipMember(payload)),
    });
  },

  async "/compression-trailer-split"() {
    // Issue #230 review: the trailer's CRC-32 is checked as soon as its four
    // bytes arrive, so a bad checksum fails its own push even when ISIZE is
    // still to come.
    const payload = encoder.encode("split trailer payload");
    const member = await gzipMember(payload);
    const badCrc = member.slice();
    badCrc[badCrc.length - 8] ^= 0xff;
    const badSize = member.slice();
    badSize[badSize.length - 1] ^= 0xff;
    const split = (bytes) => [bytes.slice(0, bytes.length - 8), bytes.slice(bytes.length - 8, bytes.length - 4), bytes.slice(bytes.length - 4)];
    const feed = async (pieces) => {
      const stream = new DecompressionStream("gzip");
      const writer = stream.writable.getWriter();
      const read = new Response(stream.readable).arrayBuffer().then((buffer) => `ok ${buffer.byteLength}`, errorInfo);
      const writes = [];
      for (const piece of pieces) writes.push(await writer.write(piece).then(() => "ok", errorInfo));
      return {
        writes,
        close: await writer.close().then(() => "ok", errorInfo),
        read: await read,
      };
    };
    return report({
      badCrc: await feed(split(badCrc)),
      badSize: await feed(split(badSize)),
      good: await feed([member]),
    });
  },

  async "/compression-cancel-then-abort"() {
    // Issue #230 review, tightened by #252: a readable cancel followed by an
    // abort with a write in flight and a read pending. Workerd's compression
    // writable is an internal stream, so the abort is an ordinary abort that
    // resolves, `closed` rejects with the abort reason (never `undefined`
    // and never the cancel reason), and the pending read rejects with an
    // `Error` carrying the cancel reason.
    const stream = new CompressionStream("gzip");
    const writer = stream.writable.getWriter();
    const reader = stream.readable.getReader();
    const pull = reader.read().then(({ value }) => value.byteLength, errorInfo);
    await null;
    const write = writer.write(encoder.encode("payload")).then(() => "ok", errorInfo);
    const cancel = reader.cancel("cancel-reason").then(() => "ok", errorInfo);
    const abort = writer.abort("abort-reason").then(() => "ok", errorInfo);
    const closed = writer.closed.then(() => "ok", (error) => (error === undefined ? "undefined" : errorInfo(error)));
    const out = {
      write: await write,
      cancel: await cancel,
      abort: await abort,
      closed: await closed,
      pull: await pull,
    };
    // The writable ends in workerd's closed shape: still locked, nothing
    // left to write into, and a later write reports the closed stream.
    out.locked = stream.writable.locked;
    out.desiredSize = attempt(() => writer.desiredSize);
    out.getWriter = attempt(() => typeof stream.writable.getWriter());
    out.writeAfter = await writer.write(encoder.encode("later")).then(() => "ok", errorInfo);
    return report(out);
  },

  async "/compression-queued-write-cancel"() {
    // Issue #257: with no await between them, the write is still queued
    // behind the writable's start gate when the cancel and the abort arrive.
    // Workerd completes a write it accepted before the cancel, so the
    // transform still runs: a valid member resolves and `closed` reports the
    // abort reason, an invalid one fails and `closed` keeps the failure.
    const member = async (text) =>
      new Uint8Array(await new Response(new Response(encoder.encode(text)).body
        .pipeThrough(new CompressionStream("gzip"))).arrayBuffer());
    const valid = await member("payload payload payload");
    const shape = async (chunk) => {
      const stream = new DecompressionStream("gzip");
      const writer = stream.writable.getWriter();
      const reader = stream.readable.getReader();
      const pull = reader.read().then(({ value }) => value.byteLength, errorInfo);
      const write = writer.write(chunk).then(() => "ok", errorInfo);
      const cancel = reader.cancel("cancel-reason").then(() => "ok", errorInfo);
      const abort = writer.abort("abort-reason").then(() => "ok", errorInfo);
      const closed = writer.closed.then(() => "ok", (error) => (error === undefined ? "undefined" : errorInfo(error)));
      const out = {
        write: await write,
        cancel: await cancel,
        abort: await abort,
        closed: await closed,
        pull: await pull,
        desiredSize: attempt(() => writer.desiredSize),
        locked: stream.writable.locked,
        writeAfter: await writer.write(chunk).then(() => "ok", errorInfo),
      };
      out.closedAfter = await writer.closed.then(() => "ok", errorInfo);
      return out;
    };
    return report({ valid: await shape(valid), invalid: await shape(new Uint8Array([1, 2, 3, 4])) });
  },

  async "/compression-invalid-chunk"() {
    // Issue #276: writing a chunk that is not a BufferSource (nor a string
    // nor undefined) rejects only that write with a TypeError, leaving both
    // sides usable -- the conversion throws before any native call. Cello
    // used to park the write until a read pulled it through the transform,
    // which errored the readable and closed the writable with the chunk
    // error. Shaped both ways round the first read without any timing: the
    // drain loop is already reading when the invalid write lands
    // (readFirst), or the invalid write is issued first and the drain
    // starts right after (invalidFirst). Every promise settles on both
    // runtimes, and the transcripts differ on the unfixed one.
    const hex = (bytes) => [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
    const shape = async (readFirst) => {
      const stream = new CompressionStream("gzip");
      const writer = stream.writable.getWriter();
      const reader = stream.readable.getReader();
      const chunks = [];
      let drained = null;
      const startDrain = () => {
        drained = (async () => {
          for (;;) {
            try {
              const { value, done } = await reader.read();
              if (done) return { done: true };
              chunks.push({ len: value.byteLength, head: hex(value.slice(0, 8)) });
            } catch (error) { return errorInfo(error); }
          }
        })();
      };
      let invalid;
      if (readFirst) {
        startDrain();
        invalid = writer.write(42).then(() => "ok", errorInfo);
      } else {
        invalid = writer.write(42).then(() => "ok", errorInfo);
        startDrain();
      }
      const invalidOut = await invalid;
      const valid = writer.write(encoder.encode("payload")).then(() => "ok", errorInfo);
      const close = writer.close().then(() => "ok", errorInfo);
      const closed = writer.closed.then(() => "ok", errorInfo);
      const out = {
        invalid: invalidOut,
        valid: await valid,
        close: await close,
        closed: await closed,
        drain: await drained,
        chunks,
        total: chunks.reduce((sum, chunk) => sum + chunk.len, 0),
      };
      reader.releaseLock();
      writer.releaseLock();
      return out;
    };
    return report({
      invalidFirst: await shape(false),
      readFirst: await shape(true),
    });
  },

  async "/decompression-invalid-chunk"() {
    // Issue #276, the decompression side: an invalid chunk rejects only
    // that write; a valid member written after it still decompresses and
    // the close resolves. The member is built by compressing a payload
    // through a pipe, the same way the round-trip cases do. The drain
    // joins every chunk, so output splits cannot flake the transcript.
    const member = new Uint8Array(await new Response(
      new Response(encoder.encode("payload payload payload")).body
        .pipeThrough(new CompressionStream("gzip"))).arrayBuffer());
    const shape = async (readFirst) => {
      const stream = new DecompressionStream("gzip");
      const writer = stream.writable.getWriter();
      const reader = stream.readable.getReader();
      const parts = [];
      let drained = null;
      const startDrain = () => {
        drained = (async () => {
          for (;;) {
            try {
              const { value, done } = await reader.read();
              if (done) return { done: true };
              parts.push(decoder.decode(value));
            } catch (error) { return errorInfo(error); }
          }
        })();
      };
      let invalid;
      if (readFirst) {
        startDrain();
        invalid = writer.write(42).then(() => "ok", errorInfo);
      } else {
        invalid = writer.write(42).then(() => "ok", errorInfo);
        startDrain();
      }
      const invalidOut = await invalid;
      const valid = writer.write(member).then(() => "ok", errorInfo);
      const close = writer.close().then(() => "ok", errorInfo);
      const closed = writer.closed.then(() => "ok", errorInfo);
      const out = {
        invalid: invalidOut,
        valid: await valid,
        close: await close,
        closed: await closed,
        drain: await drained,
        text: parts.join(""),
      };
      reader.releaseLock();
      writer.releaseLock();
      return out;
    };
    return report({
      invalidFirst: await shape(false),
      readFirst: await shape(true),
    });
  },

  async "/compression-close-cancel"(request) {
    // Issue #262: writer.close() around a same-turn readable cancel. Workerd
    // resolves both `close()` and `closed`; the flush that close starts must
    // not enqueue into the cancelled readable, and the compressor it uses
    // must not be released ahead of it. Probed for both orderings and for
    // gzip and deflate, with the pending read the cancel settles, the
    // `desiredSize` left behind, and a write and a close attempted after.
    const only = new URL(request.url).searchParams.get("only");
    const shape = async (format, mode) => {
      const stream = new CompressionStream(format);
      const writer = stream.writable.getWriter();
      const reader = stream.readable.getReader();
      const pull = reader.read().then(
        ({ value, done }) => ({ ok: true, done, length: value?.byteLength }),
        errorInfo);
      if (mode !== "queued") await null;
      const write = mode === "no-write"
        ? "skipped"
        : writer.write(new Uint8Array(64).fill(0x61)).then(() => "ok", errorInfo);
      if (mode === "cancel-first") {
        // The cancel drains before any close: the compressor is gone by the
        // time close() starts its flush.
        const cancelled = reader.cancel("cancel-reason").then(() => "ok", errorInfo);
        const early = { write: await write, cancel: await cancelled };
        const close = writer.close().then(() => "ok", errorInfo);
        const closed = writer.closed.then(() => "ok", (error) => (error === undefined ? "undefined" : errorInfo(error)));
        const out = {
          ...early,
          close: await close,
          closed: await closed,
          pull: await pull,
        };
        out.desiredSize = attempt(() => writer.desiredSize);
        out.locked = stream.writable.locked;
        out.writeAfter = await writer.write(new Uint8Array(1)).then(() => "ok", errorInfo);
        out.closeAfter = await writer.close().then(() => "ok", errorInfo);
        out.closedAfter = await writer.closed.then(() => "ok", (error) => (error === undefined ? "undefined" : errorInfo(error)));
        return out;
      }
      const close = writer.close().then(() => "ok", errorInfo);
      const closed = writer.closed.then(() => "ok", (error) => (error === undefined ? "undefined" : errorInfo(error)));
      const cancel = reader.cancel("cancel-reason").then(() => "ok", errorInfo);
      const out = {
        write: await write,
        close: await close,
        closed: await closed,
        cancel: await cancel,
        pull: await pull,
      };
      out.desiredSize = attempt(() => writer.desiredSize);
      out.locked = stream.writable.locked;
      out.writeAfter = await writer.write(new Uint8Array(1)).then(() => "ok", errorInfo);
      out.closeAfter = await writer.close().then(() => "ok", errorInfo);
      out.closedAfter = await writer.closed.then(() => "ok", (error) => (error === undefined ? "undefined" : errorInfo(error)));
      return out;
    };
    const out = {};
    for (const format of ["gzip", "deflate"]) {
      for (const mode of ["queued", "inflight", "no-write", "cancel-first"]) {
        if (only !== null && only !== mode) continue;
        out[`${format}:${mode}`] = await shape(format, mode);
      }
    }
    return report(out);
  },
  async "/pipe-to-destination-states"() {
    // Issue #269: pipeTo onto a destination it cannot take rejects instead
    // of hanging. A destination locked by a held writer or by another pipe
    // in flight rejects with the lock message and leaves the source
    // unlocked; one a previous pipe already closed is refused up front
    // with the closed-destination message; an errored one rejects with the
    // stored error. A destination whose writer was released pipes fine.
    const out = {};
    {
      const dest = new WritableStream({ write() {} });
      dest.getWriter();
      const source = byteStream(["a", "b"]);
      out.lockedByWriter = await source.pipeTo(dest).then(() => "ok", errorInfo);
      out.lockedByWriterSource = source.locked;
    }
    {
      let release;
      const gate = new Promise((done) => (release = done));
      const dest = new WritableStream({ write() { return gate; } });
      const first = byteStream(["a"]).pipeTo(dest).then(() => "ok", errorInfo);
      out.lockedByPipe = await byteStream(["b"]).pipeTo(dest).then(() => "ok", errorInfo);
      release();
      out.lockedByPipeFirst = await first;
    }
    {
      const dest = new WritableStream({});
      await byteStream(["a"]).pipeTo(dest);
      out.closedDestination = await byteStream(["b"]).pipeTo(dest).then(() => "ok", errorInfo);
    }
    {
      const dest = new WritableStream({ write() { throw new Error("sink-boom"); } });
      const writer = dest.getWriter();
      await writer.write(encoder.encode("x")).catch(() => {});
      writer.releaseLock();
      out.erroredDestination = await byteStream(["a"]).pipeTo(dest).then(() => "ok", errorInfo);
    }
    {
      const dest = new WritableStream({ write() {} });
      dest.getWriter().releaseLock();
      out.releasedDestination = await byteStream(["a"]).pipeTo(dest).then(() => "ok", errorInfo);
      out.releasedDestinationLocked = dest.locked;
    }
    return report(out);
  },

  async "/pipe-through-locked"() {
    // Issue #269: pipeThrough throws synchronously when either side is
    // taken, however the writable got locked, and pipeTo onto the writable
    // of a live pipeThrough chain rejects instead of hanging.
    const out = {};
    {
      const source = byteStream(["a"]);
      source.getReader();
      out.lockedSource = attempt(() => {
        source.pipeThrough(new TransformStream());
        return "ok";
      });
    }
    {
      const transform = new TransformStream();
      transform.writable.getWriter();
      out.lockedWritable = attempt(() => {
        byteStream(["a"]).pipeThrough(transform);
        return "ok";
      });
    }
    {
      const transform = new TransformStream();
      const feeder = byteStream(["f"]).pipeTo(transform.writable);
      feeder.catch(() => {});
      out.lockedByPipe = attempt(() => {
        byteStream(["a"]).pipeThrough(transform);
        return "ok";
      });
      // Ending the feeder is cleanup, not pinned: once the readable is
      // cancelled the feeder pipe settles fulfilled on workerd and rejects
      // with the cancel reason on cello (plain-transform cancel races the
      // parked write differently). Both settle, so neither hangs.
      await transform.readable.cancel("done").catch(() => {});
      await feeder.then(() => {}, () => {});
    }
    {
      const transform = new TransformStream({
        transform(chunk, controller) { controller.enqueue(chunk); },
      });
      const readable = byteStream(["a", "b"]).pipeThrough(transform);
      out.livePipeThrough = await byteStream(["z"]).pipeTo(transform.writable).then(() => "ok", errorInfo);
      out.drained = await readText(readable);
    }
    return report(out);
  },

  async "/compression-close-abort"() {
    // Issue #270: close() racing writer.abort() with no readable cancel.
    // The abort wins over a queued or in-flight close on workerd's internal
    // streams: the close and closed reject with the abort reason, the abort
    // fulfills, and the readable errors with it too. An abort with no close
    // racing it ends the writable in the closed shape, so a close or write
    // that follows it reports the closed stream instead of the abort reason.
    const out = {};
    for (const format of ["gzip", "deflate", "deflate-raw"]) {
      {
        // Write, close, abort, close, abort with no await between: the
        // start gate is still shut, so everything queues behind it.
        const stream = new CompressionStream(format);
        const writer = stream.writable.getWriter();
        const read = new Response(stream.readable).arrayBuffer().then(
          (buffer) => buffer.byteLength, errorInfo);
        const write = writer.write(encoder.encode("payload")).then(() => "written", errorInfo);
        const close = writer.close().then(() => "closed-ok", errorInfo);
        const abort = writer.abort("abort-reason").then(() => "aborted", errorInfo);
        const closed = writer.closed.then(() => "closed", errorInfo);
        const close2 = writer.close().then(() => "closed-ok", errorInfo);
        const abort2 = writer.abort("other-reason").then(() => "aborted", errorInfo);
        out[`${format}:queued`] = {
          write: await write,
          close: await close,
          abort: await abort,
          closed: await closed,
          close2: await close2,
          abort2: await abort2,
          read: await read,
          locked: stream.writable.locked,
        };
      }
      {
        // The write finished, so the close's flush is in flight when the
        // abort lands: the abort still wins.
        const stream = new CompressionStream(format);
        const writer = stream.writable.getWriter();
        const read = new Response(stream.readable).arrayBuffer().then(
          (buffer) => buffer.byteLength, errorInfo);
        await writer.write(encoder.encode("payload payload"));
        const closed = writer.closed.then(() => "closed", errorInfo);
        const close = writer.close().then(() => "closed-ok", errorInfo);
        const abort = writer.abort("abort-reason").then(() => "aborted", errorInfo);
        out[`${format}:flush`] = {
          close: await close,
          abort: await abort,
          closed: await closed,
          read: await read,
        };
      }
      {
        // Abort first, then close and write: the writable is already in
        // the closed shape, so both report the closed stream.
        const stream = new CompressionStream(format);
        const writer = stream.writable.getWriter();
        const abort = await writer.abort("abort-reason").then(() => "aborted", errorInfo);
        const close = await writer.close().then(() => "closed-ok", errorInfo);
        const write = await writer.write(encoder.encode("x")).then(() => "written", errorInfo);
        out[`${format}:abort-first`] = {
          abort,
          close,
          write,
          closed: await writer.closed.then(() => "closed", errorInfo),
          desiredSize: attempt(() => writer.desiredSize),
        };
      }
    }
    return report(out);
  },

  async "/decompression-empty-close"() {
    // Issue #270: closing a DecompressionStream whose member never
    // completed rejects close() and closed with the incomplete-data error,
    // however the close lines up with the readable cancel. A complete
    // member still closes cleanly.
    const member = await gzipMember(encoder.encode("payload payload payload"));
    const truncated = member.slice(0, 10);
    const out = {};
    {
      const stream = new DecompressionStream("gzip");
      const writer = stream.writable.getWriter();
      const read = new Response(stream.readable).arrayBuffer().then(
        (buffer) => buffer.byteLength, errorInfo);
      out.emptyCloseOnly = {
        close: await writer.close().then(() => "closed-ok", errorInfo),
        closed: await writer.closed.then(() => "closed", errorInfo),
        read: await read,
      };
    }
    {
      // No data at all: no read is started, since a pending one never
      // settles on workerd once the readable is cancelled with nothing to
      // deliver.
      const stream = new DecompressionStream("gzip");
      const writer = stream.writable.getWriter();
      const reader = stream.readable.getReader();
      const cancel = reader.cancel("cancel-reason").then(() => "cancelled", errorInfo);
      const close = writer.close().then(() => "closed-ok", errorInfo);
      out.emptyCancelFirst = {
        cancel: await cancel,
        close: await close,
        closed: await writer.closed.then(() => "closed", errorInfo),
      };
    }
    {
      // A read has to be pending before the write: without a consumer the
      // write parks on backpressure and the route would hang on both
      // runtimes. The pending read rejects with the cancel reason on both.
      const stream = new DecompressionStream("gzip");
      const writer = stream.writable.getWriter();
      const reader = stream.readable.getReader();
      const pull = reader.read().then(
        ({ value, done }) => ({ bytes: value?.byteLength ?? 0, done }), errorInfo);
      await writer.write(truncated);
      const cancel = reader.cancel("cancel-reason").then(() => "cancelled", errorInfo);
      const close = writer.close().then(() => "closed-ok", errorInfo);
      out.truncatedCancelFirst = {
        cancel: await cancel,
        close: await close,
        closed: await writer.closed.then(() => "closed", errorInfo),
        pull: await pull,
      };
    }
    for (const [label, chunk] of [["empty", null], ["truncated", truncated]]) {
      const stream = new DecompressionStream("gzip");
      const writer = stream.writable.getWriter();
      // A pending read keeps a written chunk flowing; it settles the same
      // only in the cancel-first shape above, so here it is just a
      // consumer with its outcome ignored.
      if (chunk) stream.readable.getReader().read().then(() => {}, () => {});
      if (chunk) await writer.write(chunk);
      const close = writer.close().then(() => "closed-ok", errorInfo);
      const cancel = stream.readable.cancel("cancel-reason").then(() => "cancelled", errorInfo);
      out[`${label}CloseFirst`] = {
        close: await close,
        cancel: await cancel,
        closed: await writer.closed.then(() => "closed", errorInfo),
      };
    }
    {
      // A complete member closes cleanly even across the cancel. The
      // pending read is only a consumer here: it settles differently on
      // the two runtimes (delivery timing), so it is awaited but not pinned.
      const stream = new DecompressionStream("gzip");
      const writer = stream.writable.getWriter();
      const reader = stream.readable.getReader();
      const pull = reader.read().then(() => "read", errorInfo);
      const write = await writer.write(member).then(() => "written", errorInfo);
      const cancel = await reader.cancel("cancel-reason").then(() => "cancelled", errorInfo);
      await pull;
      out.completeCancelFirst = {
        write,
        cancel,
        close: await writer.close().then(() => "closed-ok", errorInfo),
        closed: await writer.closed.then(() => "closed", errorInfo),
      };
    }
    return report(out);
  },

  async "/compression-encoder-chunks"() {
    // Issue #257: workerd enqueues one chunk per push with whatever the
    // compressor emitted -- the gzip/zlib header on the first push, nothing
    // more until deflate has a block or the stream ends -- instead of
    // holding output until 32 KiB or close. The concatenation is the same
    // byte stream either way (`compression-encoder-bytes` pins that).
    const hex = (bytes) => [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
    const out = {};
    for (const format of ["gzip", "deflate", "deflate-raw", "brotli"]) {
      const stream = new CompressionStream(format);
      const writer = stream.writable.getWriter();
      const reader = stream.readable.getReader();
      const chunks = [];
      const drained = (async () => {
        for (;;) {
          const { value, done } = await reader.read();
          if (done) return;
          chunks.push({ len: value.byteLength, head: hex(value.slice(0, 8)) });
        }
      })();
      for (const size of [3, 3, 1000, 40000]) {
        await writer.write(new Uint8Array(size).fill(0x61));
      }
      const close = await writer.close().then(() => "ok", errorInfo);
      await drained;
      out[format] = { close, chunks, total: chunks.reduce((sum, chunk) => sum + chunk.len, 0) };
    }
    return report(out);
  },

  async "/locked-stream-messages"() {
    // Issue #245: workerd names the interface and the holder when a stream
    // is already locked.
    const out = {};
    const writable = new WritableStream({});
    writable.getWriter();
    out.lockedWriter = attempt(() => writable.getWriter());
    const compress = new CompressionStream("gzip");
    const compressWriter = compress.writable.getWriter();
    out.lockedCompression = attempt(() => compress.writable.getWriter());
    compressWriter.releaseLock();
    out.compressionReacquired = (() => {
      try { return typeof compress.writable.getWriter(); } catch (error) { return errorInfo(error); }
    })();
    compress.readable.cancel("done").catch(() => {});
    const readable = new ReadableStream({});
    readable.getReader();
    out.lockedReader = attempt(() => readable.getReader());
    // pipeTo rejects and pipeThrough throws; workerd's writable text has no
    // trailing period in pipeTo but does in pipeThrough.
    out.pipeToLockedSource = await readable.pipeTo(new WritableStream({})).then(() => "ok", errorInfo);
    const lockedDestination = new WritableStream({});
    lockedDestination.getWriter();
    out.pipeToLockedDestination = await new ReadableStream({ start: (controller) => controller.close() })
      .pipeTo(lockedDestination).then(() => "ok", errorInfo);
    const throughSource = new ReadableStream({});
    throughSource.getReader();
    out.pipeThroughLockedSource = attempt(() => {
      throughSource.pipeThrough(new TransformStream());
      return "ok";
    });
    const lockedTransform = new TransformStream();
    lockedTransform.writable.getWriter();
    out.pipeThroughLockedWritable = attempt(() => {
      new ReadableStream({ start: (controller) => controller.close() }).pipeThrough(lockedTransform);
      return "ok";
    });
    return report(out);
  },

  async "/writable-ready-queued-close"() {
    // Issue #245: a release leaves the dispatched write's size in the queue,
    // and a queued close does not resolve a fresh writer's ready while the
    // stream is over its high-water mark. Workerd resolves it when the close
    // lands.
    let releaseWrite, releaseClose;
    const ws = new WritableStream({
      write() { return new Promise((resolve) => (releaseWrite = resolve)); },
      close() { return new Promise((resolve) => (releaseClose = resolve)); },
    }, { highWaterMark: 1 });
    const writer = ws.getWriter();
    const write = writer.write("a").then(() => "ok", errorInfo);
    for (let index = 0; index < 4; index += 1) await null;
    const close = writer.close().then(() => "ok", errorInfo);
    const released = writer.desiredSize;
    writer.releaseLock();
    const next = ws.getWriter();
    let ready = "pending";
    next.ready.then(() => (ready = "resolved"), (error) => (ready = `rejected ${errorInfo(error).message}`));
    for (let index = 0; index < 4; index += 1) await null;
    const out = { released, atRelease: { ready, desiredSize: next.desiredSize } };
    releaseWrite();
    out.write = await write;
    for (let index = 0; index < 4; index += 1) await null;
    out.afterWrite = { ready, desiredSize: next.desiredSize };
    releaseClose();
    out.close = await close;
    for (let index = 0; index < 4; index += 1) await null;
    out.afterClose = { ready, desiredSize: next.desiredSize, locked: ws.locked };
    return report(out);
  },

  async "/compression-encoder-bytes"() {
    // Issue #245: workerd's gzip/deflate bytes are zlib's level 7 with mtime
    // 0 and OS 3, and the deflate stream's header is 78 9c (FLEVEL 2) even
    // though the body is level 7. The chunked case is the one that splits a
    // write inside flate2's 32 KiB output buffer, where the Adler-32 must
    // count only the bytes the encoder consumed.
    const payload = encoder.encode("the quick brown fox jumps over the lazy dog. ".repeat(50));
    const hex = (bytes) => [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
    const out = {};
    for (const format of ["gzip", "deflate", "deflate-raw"]) {
      out[format] = hex(new Uint8Array(await new Response(new Response(payload).body.pipeThrough(new CompressionStream(format))).arrayBuffer()));
    }
    const random = new Uint8Array(100 * 1024);
    let state = 123456789;
    for (let index = 0; index < random.length; index += 1) {
      state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
      random[index] = (state >>> 24) & 0xff;
    }
    const stream = new CompressionStream("deflate");
    const writer = stream.writable.getWriter();
    const read = readAll(stream.readable);
    for (let offset = 0; offset < random.length; offset += 65536) {
      await writer.write(random.subarray(offset, offset + 65536));
    }
    await writer.close();
    const parts = await read;
    const total = parts.reduce((sum, part) => sum + part.byteLength, 0);
    const joined = new Uint8Array(total);
    let at = 0;
    for (const part of parts) { joined.set(part, at); at += part.byteLength; }
    out.deflateChunked64k = hex(joined);
    out.deflateChunkedLength = total;
    return report(out);
  },

  async "/gzip-member-names"() {
    // Issue #245: the header's optional FNAME field. A terminated name is a
    // valid member; a name whose NUL only appears inside the body re-aligns
    // the parse and fails that push; a name that never terminates waits and
    // reports incomplete data at close.
    const header = (flags) => [0x1f, 0x8b, 8, flags, 0, 0, 0, 0, 0, 0xff];
    const decoder = new TextDecoder();
    const through = async (pieces) => {
      const stream = new DecompressionStream("gzip");
      const writer = stream.writable.getWriter();
      const read = new Response(stream.readable).arrayBuffer()
        .then((buffer) => `ok ${decoder.decode(buffer)}`, errorInfo);
      const writes = [];
      for (const piece of pieces) writes.push(await writer.write(piece).then(() => "ok", errorInfo));
      return { writes, close: await writer.close().then(() => "ok", errorInfo), read: await read };
    };
    const payload = encoder.encode("fname payload");
    const body = await rawDeflate(payload);
    const trailer = new Uint8Array(new Uint32Array([crc32(payload), payload.length]).buffer);
    const name = new Uint8Array(4096).fill(0x61);
    // A stored deflate block of 64 "a"s, built by hand so both runtimes see
    // the same bytes: its LEN high byte is the first NUL after a short name.
    const stored = new Uint8Array([0x01, 0x40, 0x00, 0xbf, 0xff, ...new Uint8Array(64).fill(0x61)]);
    return report({
      terminated: await through([new Uint8Array([...header(8), ...name, 0x00, ...body, ...trailer])]),
      terminatedNoTrailer: await through([new Uint8Array([...header(8), ...name, 0x00, ...body])]),
      nameIntoBody: await through([new Uint8Array([...header(8), 0x61, 0x62, 0x63, ...stored])]),
      unterminated: await through([new Uint8Array([...header(8), ...name])]),
      unterminatedSplit: await through([new Uint8Array([...header(8), ...name.slice(0, 2048)]), name.slice(2048)]),
    });
  },

  async "/compression-trailing-bytes"() {
    // Issue #245 review: bytes past the end of a deflate or deflate-raw
    // stream are their own error, whichever push carries them.
    const payload = encoder.encode("trailing payload trailing payload");
    const out = {};
    for (const format of ["deflate", "deflate-raw"]) {
      const packed = new Uint8Array(await new Response(new Response(payload).body.pipeThrough(new CompressionStream(format))).arrayBuffer());
      const feed = async (pieces) => {
        const stream = new DecompressionStream(format);
        const writer = stream.writable.getWriter();
        const read = new Response(stream.readable).arrayBuffer().then((buffer) => `ok ${buffer.byteLength}`, errorInfo);
        const writes = [];
        for (const piece of pieces) writes.push(await writer.write(piece).then(() => "ok", errorInfo));
        return { writes, close: await writer.close().then(() => "ok", errorInfo), read: await read };
      };
      out[format] = {
        samePush: await feed([new Uint8Array([...packed, 1, 2, 3])]),
        later: await feed([packed, new Uint8Array([1, 2, 3])]),
        secondStream: await feed([new Uint8Array([...packed, ...packed])]),
        clean: await feed([packed]),
      };
    }
    return report(out);
  },

  async "/writable-backpressure"() {
    const order = [];
    let release;
    const ws = new WritableStream(
      {
        write(chunk) {
          order.push(`write:${chunk}`);
          return new Promise((resolve) => {
            release = resolve;
          });
        },
        close: () => order.push("close"),
      },
      { highWaterMark: 1, size: (chunk) => chunk.length },
    );
    const writer = ws.getWriter();
    const first = writer.write("x").then(() => order.push("x:done"));
    order.push(`desired:${writer.desiredSize}`);
    await null;
    order.push(`stalled:${writer.desiredSize}`);
    release();
    await first;
    order.push(`after:${writer.desiredSize}`);
    await writer.close();
    return report(order);
  },

  async "/tee-basic"() {
    const [a, b] = byteStream(["one", "two"]).tee();
    return report({ first: await readText(a), second: await readText(b) });
  },

  async "/tee-of-tee"() {
    const [a, b] = byteStream(["chunk-0", "chunk-1", "chunk-2", "chunk-3"]).tee();
    const [b1, b2] = b.tee();
    return report({ a: await readText(a), b1: await readText(b1), b2: await readText(b2) });
  },

  async "/tee-a-then-b"() {
    const payload = "x".repeat(1024);
    const source = byteStream(Array.from({ length: 32 }, (_, index) => `${index}:${payload}`));
    const [a, b] = source.tee();
    const first = await readText(a);
    const second = await readText(b);
    return report({ length: second.length, equal: first === second, tail: second.slice(-12), checksum: checksum(second) });
  },

  async "/tee-cancel-branch"() {
    let sourceCancelled = null;
    const source = new ReadableStream({
      start: (c) => {
        for (const chunk of ["first", "second", "third"]) c.enqueue(encoder.encode(chunk));
        c.close();
      },
      cancel: (reason) => (sourceCancelled = String(reason)),
    });
    const [a, b] = source.tee();
    const readerA = a.getReader();
    const head = decoder.decode((await readerA.read()).value);
    // A tee may hold a branch's cancel until the sibling finishes, so start it
    // before draining the sibling instead of awaiting it here.
    const cancellingA = readerA.cancel("a done");
    const rest = (await readAll(b)).map((chunk) => decoder.decode(chunk));
    const cancelA = await cancellingA.then(() => "ok", (error) => errorInfo(error));
    const cancelB = await b.cancel("b done").then(() => "ok", (error) => errorInfo(error));
    return report({ head, cancelA, rest, sourceCancelled, cancelB });
  },

  async "/tee-cancel-reason"() {
    // Issue #114: the source is cancelled with the reason of the last branch
    // that cancels, not the spec's joined [reason1, reason2] composite.
    let cancelled = null;
    const source = new ReadableStream({
      pull: (c) => c.enqueue(new TextEncoder().encode("chunk")),
      cancel: (reason) => (cancelled = String(reason)),
    });
    const [a, b] = source.tee();
    const cancelA = await a.cancel("first").then(() => "ok", (error) => errorInfo(error));
    const cancelB = await b.cancel("second").then(() => "ok", (error) => errorInfo(error));
    return report({ cancelA, cancelB, cancelled });
  },

  async "/clone-both"() {
    const response = new Response(byteStream(["alpha", "beta"]));
    const clone = response.clone();
    return report({ first: await response.text(), second: await clone.text() });
  },

  async "/transform-order"() {
    const order = [];
    const ts = new TransformStream({
      start: () => order.push("start"),
      transform: (chunk, controller) => {
        order.push(`transform:${chunk}`);
        controller.enqueue(chunk.toUpperCase());
      },
      flush: (controller) => (order.push("flush"), controller.enqueue("!")),
    });
    const writer = ts.writable.getWriter();
    const reader = ts.readable.getReader();
    // The readable side starts at high-water mark 0, so a write only settles
    // once a read pulls: start the write, then read, then await it.
    const writeA = writer.write("a");
    order.push(`read:${(await reader.read()).value}`);
    await writeA;
    const writeB = writer.write("b");
    order.push(`read:${(await reader.read()).value}`);
    await writeB;
    const closing = writer.close();
    order.push(`read:${(await reader.read()).value}`);
    await closing;
    order.push(`end:${(await reader.read()).done}`);
    return report(order);
  },

  async "/transform-error"() {
    const ts = new TransformStream({
      transform: (chunk, controller) => {
        if (chunk === "bad") throw new Error("transform failed");
        controller.enqueue(chunk);
      },
    });
    const writer = ts.writable.getWriter();
    const reader = ts.readable.getReader();
    const writing = writer.write("bad").then(() => "accepted", (error) => errorInfo(error));
    const reading = reader.read().then(() => "resolved", (error) => errorInfo(error));
    const closed = writer.closed.then(() => "closed", (error) => errorInfo(error));
    return report({ writing: await writing, reading: await reading, closed: await closed });
  },

  async "/transform-close-after-cancel"() {
    // Issue #129: cancelling the readable errors the writable with the
    // cancel reason, so a producer's later close() rejects with that reason
    // instead of a generic TypeError.
    const ts = new TransformStream();
    const writer = ts.writable.getWriter();
    const reader = ts.readable.getReader();
    await reader.cancel("bye");
    const describe = (error) =>
      `${error?.name ?? "value"}: ${error?.message ?? String(error)}`;
    return report({
      close: await writer.close().then(() => "ok", describe),
      closed: await writer.closed.then(() => "closed", describe),
    });
  },

  async "/transform-pipe-cancel-idle"() {
    // Issue #272: a plain TransformStream readable cancelled while a pipeTo
    // is feeding its writable, with no write outstanding -- both chunks
    // drained before the cancel -- fulfills the feeder pipe on Workerd
    // (the source is still cancelled with the reason), where Cello used
    // to reject it with the cancel reason. Two reads prove the pipe is
    // past the parked state without any timing: the second read resolves
    // once the second chunk was enqueued, so the cancel after it lands
    // while that write is finishing or done -- either way nothing is
    // parked on the backpressure handshake, and both shapes fulfill.
    let cancelled = null;
    const source = new ReadableStream({
      start: (c) => (c.enqueue("a"), c.enqueue("b")),
      cancel: (reason) => (cancelled = reason),
    });
    const ts = new TransformStream();
    const outcome = source.pipeTo(ts.writable).then(() => "done", errorInfo);
    const reader = ts.readable.getReader();
    const first = await reader.read().then(
      ({ value, done }) => ({ value: String(value), done }),
      (error) => errorInfo(error));
    const second = await reader.read().then(
      ({ value, done }) => ({ value: String(value), done }),
      (error) => errorInfo(error));
    reader.releaseLock();
    await ts.readable.cancel("bye");
    const settled = await outcome;
    const writer = ts.writable.getWriter();
    const post = {
      closed: await writer.closed.then(() => "closed", errorInfo),
      writeAfter: await writer.write("z").then(() => "ok", errorInfo),
      closeAfter: await writer.close().then(() => "ok", errorInfo),
      abortAfter: await writer.abort("again").then(() => "ok", errorInfo),
    };
    writer.releaseLock();
    const reread = ts.readable.getReader();
    post.readAfter = await reread.read().then(
      ({ done }) => ({ done }),
      (error) => errorInfo(error));
    reread.releaseLock();
    return report({
      first,
      second,
      outcome: settled,
      cancelled: cancelled === null ? null : String(cancelled),
      sourceLocked: source.locked,
      post,
    });
  },

  async "/transform-write-cancel-running"() {
    // Issue #272: the cancel lands while the first write's transform is
    // already running (the entered gate proves it without timing), so both
    // writes still complete on Workerd -- the queued second one included --
    // and the same writer's ready stays fulfilled, while writer.closed
    // rejects with the cancel reason. Cello used to reject the queued
    // write and ready with the cancel reason.
    let entered;
    const enteredPromise = new Promise((resolve) => (entered = resolve));
    const ts = new TransformStream({
      transform(chunk, controller) {
        entered();
        controller.enqueue(chunk);
      },
    });
    const writer = ts.writable.getWriter();
    const reader = ts.readable.getReader();
    const read = reader.read().then(
      ({ value, done }) => ({ value: String(value), done }),
      (error) => errorInfo(error));
    const first = writer.write("a").then(() => "done", errorInfo);
    const second = writer.write("b").then(() => "done", errorInfo);
    await enteredPromise;
    await reader.cancel("bye");
    const readOut = await read;
    reader.releaseLock();
    const ready = await writer.ready.then(() => "done", errorInfo);
    const closed = await writer.closed.then(() => "closed", errorInfo);
    return report({
      readOut,
      first: await first,
      second: await second,
      ready,
      closed,
    });
  },

  async "/pipe-dest-error-completing"() {
    // Issue #272: a controller.error() while a write is still running does
    // not fail that write, so the pipe has no failed write and fulfills on
    // Workerd (the source is still cancelled with the error), where Cello
    // used to reject with the destination's error.
    let cancelled = null;
    const source = new ReadableStream({
      start: (c) => c.enqueue("a"),
      cancel: (reason) => (cancelled = reason),
    });
    let controller;
    const dest = new WritableStream({
      start(c) { controller = c; },
      write() { controller.error(new Error("dest-broken")); },
    });
    const outcome = await source.pipeTo(dest).then(() => "done", errorInfo);
    return report({
      outcome,
      cancelled: cancelled === null ? null : errorInfo(cancelled),
      sourceLocked: source.locked,
    });
  },

  async "/transform-flush-cancel-drain"() {
    // A readable cancel from inside the writable's own flush, while a
    // reader drains: the chunk arrives, the pipe fulfills, and the source
    // -- already closed -- is never cancelled. Agrees on both runtimes.
    let cancelled = false;
    const source = new ReadableStream({
      start: (c) => (c.enqueue("a"), c.close()),
      cancel: () => (cancelled = true),
    });
    let ts;
    ts = new TransformStream({
      transform(chunk, controller) { controller.enqueue(chunk); },
      flush() { ts.readable.cancel("bye"); },
    });
    const seen = [];
    const reader = ts.readable.getReader();
    const drained = (async () => {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) return seen;
        seen.push(String(value));
      }
    })().then((value) => ({ out: value }), (error) => errorInfo(error));
    const outcome = await source.pipeTo(ts.writable).then(() => "done", errorInfo);
    return report({ drained: await drained, outcome, cancelled });
  },

  async "/transform-backpressure"() {
    const order = [];
    const ts = new TransformStream(
      {
        transform: (chunk, controller) => (order.push(`t:${chunk}`), controller.enqueue(chunk)),
        flush: () => order.push("flush"),
      },
      { highWaterMark: 0, size: () => 1 },
      { highWaterMark: 0 },
    );
    const writer = ts.writable.getWriter();
    const reader = ts.readable.getReader();
    const writeA = writer.write("a").then(() => order.push("w:a"));
    order.push(`desired:${writer.desiredSize}`);
    order.push(`read:${(await reader.read()).value}`);
    await writeA;
    const writeB = writer.write("b").then(() => order.push("w:b"));
    order.push(`read:${(await reader.read()).value}`);
    await writeB;
    await writer.close();
    order.push(`end:${(await reader.read()).done}`);
    return report(order);
  },

  async "/identity-transform"() {
    const ts = new IdentityTransformStream();
    const writer = ts.writable.getWriter();
    const drained = readAll(ts.readable);
    await writer.write(encoder.encode("abc"));
    await writer.write(encoder.encode("dé"));
    await writer.close();
    const chunks = await drained;
    const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
    return report({ text: chunks.map((chunk) => decoder.decode(chunk)).join(""), total });
  },

  async "/fixed-length"() {
    const ts = new FixedLengthStream(5);
    const writer = ts.writable.getWriter();
    const drained = new Response(ts.readable).arrayBuffer();
    await writer.write(new Uint8Array([1, 2, 3]));
    await writer.write(new Uint8Array([4, 5]));
    await writer.close();
    return report({ bytes: [...new Uint8Array(await drained)] });
  },

  async "/gzip-roundtrip"() {
    const { compressed, decompressed } = await roundTrip("gzip", "hello gzip é");
    return report({ magic: [...compressed.slice(0, 2)], decompressed });
  },

  async "/deflate-roundtrip"() {
    const { compressed, decompressed } = await roundTrip("deflate", "hello deflate é");
    return report({ head: [...compressed.slice(0, 1)], decompressed });
  },

  async "/gzip-large"() {
    const payload = "the quick brown fox jumps over the lazy dog. ".repeat(3000);
    const { compressed, decompressed } = await roundTrip("gzip", payload);
    return report({ payloadLength: payload.length, decodedLength: decompressed.length, equal: decompressed === payload, checksum: checksum(decompressed), compressedUnder: compressed.byteLength < payload.length });
  },

  async "/text-encoder-stream"() {
    const ts = new TextEncoderStream();
    const writer = ts.writable.getWriter();
    const drained = readAll(ts.readable);
    await writer.write("a");
    await writer.write("é");
    await writer.write("€");
    await writer.close();
    const bytes = (await drained).flatMap((chunk) => [...chunk]);
    return report({ hex: bytes.map((byte) => byte.toString(16).padStart(2, "0")).join("") });
  },

  async "/text-decoder-stream"() {
    const ts = new TextDecoderStream();
    const writer = ts.writable.getWriter();
    const drained = readAll(ts.readable).then((chunks) => chunks.join(""));
    const bytes = encoder.encode("héllo €");
    await writer.write(bytes.slice(0, 2));
    await writer.write(bytes.slice(2, 5));
    await writer.write(bytes.slice(5));
    await writer.close();
    return report({ text: await drained });
  },

  async "/pipe-to-abort"() {
    let cancelled = null;
    const controller = new AbortController();
    const source = new ReadableStream({
      start: (c) => c.enqueue("x"),
      pull: (c) => c.enqueue("y"),
      cancel: (reason) => (cancelled = String(reason)),
    });
    const sink = new WritableStream({
      write: (chunk) => chunk === "x" && controller.abort("stop"),
    });
    const outcome = await source.pipeTo(sink, { signal: controller.signal }).then(() => "done", errorInfo);
    return report({ outcome, cancelled, locked: source.locked });
  },

  async "/pipe-through-chain"() {
    const exclaim = new TransformStream({
      transform: (chunk, controller) => controller.enqueue(encoder.encode(`${decoder.decode(chunk)}!`)),
    });
    const upper = new TransformStream({ transform: (chunk, c) => c.enqueue(encoder.encode(decoder.decode(chunk).toUpperCase())) });
    const out = await new Response(byteStream(["a", "b", "c"]).pipeThrough(upper).pipeThrough(exclaim)).text();
    return report({ out });
  },

  async "/pipe-to-error"() {
    let cancelled = false;
    const source = new ReadableStream({
      start: (c) => (c.enqueue("a"), c.enqueue("b")),
      cancel: () => (cancelled = true),
    });
    const sink = new WritableStream({
      write: (chunk) => {
        if (chunk === "a") throw new Error("sink failed");
      },
    });
    const outcome = await source.pipeTo(sink).then(() => "done", errorInfo);
    return report({ outcome, cancelled });
  },

  async "/streamed-request-body"(request) {
    const chunks = await readAll(request.body);
    return report({ text: chunks.map((chunk) => decoder.decode(chunk)).join(""), chunks: chunks.length });
  },

  async "/body-used-reader"(request) {
    // bodyUsed observes the body stream: a reader that takes bytes disturbs
    // it, and a guard re-checking mid-stream must see that. Merely holding a
    // reader (locked, nothing read) is not yet a disturbance.
    const before = request.bodyUsed;
    const reader = request.body.getReader();
    const lockedOnly = request.bodyUsed;
    const first = await reader.read();
    const during = request.bodyUsed;
    // Drain the rest: the host may split the upload, and the transcript must
    // name the whole body however the chunks arrived.
    const parts = [decoder.decode(first.value)];
    for (;;) {
      const next = await reader.read();
      if (next.done) break;
      parts.push(decoder.decode(next.value));
    }
    reader.releaseLock();
    const after = request.bodyUsed;
    return report({ before, lockedOnly, during, after, chunk: parts.join("") });
  },

  async "/res-body-used"() {
    const buffered = new Response("abc");
    const bufferedInit = buffered.bodyUsed;
    const bufferedReader = buffered.body.getReader();
    const bufferedLocked = buffered.bodyUsed;
    const bufferedFirst = await bufferedReader.read();
    const bufferedDuring = buffered.bodyUsed;
    bufferedReader.releaseLock();
    const bufferedAfter = buffered.bodyUsed;

    const streamed = new Response(byteStream(["one", "two"]));
    const streamedReader = streamed.body.getReader();
    const streamedLocked = streamed.bodyUsed;
    const streamedFirst = await streamedReader.read();
    const streamedDuring = streamed.bodyUsed;
    streamedReader.releaseLock();
    const streamedAfter = streamed.bodyUsed;

    const consumed = new Response("abc");
    await consumed.text();
    const consumedUsed = consumed.bodyUsed;

    return report({
      buffered: {
        init: bufferedInit, locked: bufferedLocked, during: bufferedDuring,
        after: bufferedAfter, chunk: decoder.decode(bufferedFirst.value),
      },
      streamed: {
        locked: streamedLocked, during: streamedDuring, after: streamedAfter,
        chunk: decoder.decode(streamedFirst.value),
      },
      consumed: consumedUsed,
    });
  },

  async "/clone-consumed"() {
    const settle = (fn) => Promise.resolve()
      .then(() => fn())
      .then((value) => value, errorInfo);
    const consumedResponse = new Response("consumed");
    await consumedResponse.text();
    const responseClone = await settle(() => consumedResponse.clone());
    const consumedRequest = new Request("https://diff.test/", { method: "POST", body: "consumed" });
    await consumedRequest.text();
    const requestClone = await settle(() => consumedRequest.clone());
    // A body held (not read) by its own reader is refused too, with the same
    // message: workerd reports the locked stream whatever the consumer did.
    const lockedResponse = new Response("locked");
    lockedResponse.body.getReader();
    const lockedClone = await settle(() => lockedResponse.clone());
    const empty = new Response(null).clone();
    return report({
      responseClone, requestClone, lockedClone, emptyStatus: empty.status,
      emptyBodyUsed: empty.bodyUsed,
    });
  },

  async "/string-chunks"() {
    // A body stream must yield bytes. Every Body consumer rejects a string
    // chunk with workerd's TypeError, and none of them hands back partial
    // data.
    const strings = () => new ReadableStream({
      start: (c) => (c.enqueue("alpha"), c.enqueue("beta"), c.close()),
    });
    const settle = (promise) => promise.then((value) => value, errorInfo);
    const requestOf = (body) => new Request("https://diff.test/", { method: "POST", body });
    const text = await settle(new Response(strings()).text());
    const arrayBuffer = await settle(new Response(strings()).arrayBuffer());
    const json = await settle(new Response(strings()).json());
    const formData = await settle(new Response(strings()).formData());
    const blob = await settle(new Response(strings()).blob());
    const bytes = await settle(new Response(strings()).bytes());
    const requestText = await settle(requestOf(strings()).text());
    const mixed = await settle(new Response(new ReadableStream({
      start: (c) => (c.enqueue(encoder.encode("ok")), c.enqueue("bad"), c.close()),
    })).text());
    // Bytes are accepted in every shape workerd accepts them.
    const buffer = await settle(new Response(new ReadableStream({
      start: (c) => (c.enqueue(encoder.encode("ab").buffer), c.close()),
    })).text());
    const view = await settle(new Response(new ReadableStream({
      start: (c) => (c.enqueue(encoder.encode("ab")), c.close()),
    })).bytes().then((value) => [...value]));
    const zero = await settle(new Response(new ReadableStream({
      start: (c) => { c.close(); },
    })).text());
    return report({ text, arrayBuffer, json, formData, blob, bytes, requestText, mixed, buffer, view, zero });
  },

  async "/body-chunk-shadows"() {
    // A chunk's window must come from internal slots. A guest can define own
    // `buffer`, `byteOffset` and `byteLength` properties on a view; reading
    // them off the chunk pads the body, reads another window, or throws.
    const shadowed = (view, fake) => {
      Object.defineProperty(view, "buffer", { value: fake });
      Object.defineProperty(view, "byteOffset", { value: 1 });
      Object.defineProperty(view, "byteLength", { value: 4096 });
      return view;
    };
    const decode = (chunks) => {
      const stream = new ReadableStream({
        start: (c) => {
          for (const chunk of chunks) c.enqueue(chunk);
          c.close();
        },
      });
      return new Response(stream).text().then((value) => value, errorInfo);
    };
    const filler = () => new ArrayBuffer(8192);
    const typed = await decode([shadowed(new Uint8Array(encoder.encode("abc")), filler())]);
    const dataView = await decode([shadowed(new DataView(encoder.encode("xyz").buffer), filler())]);
    const small = await decode([shadowed(new DataView(encoder.encode("qrs").buffer), new ArrayBuffer(2))]);
    const wide = await decode([shadowed(new Uint16Array([0x6261]), filler())]);
    return report({ typed, dataView, small, wide });
  },

  async "/chunk-shadow-response"() {
    // #130: the host response pump must read a chunk's window from its
    // internal slots too. Workerd writes the view's real three bytes; a pump
    // that reads the guest's own `byteLength` writes eight.
    const backing = new ArrayBuffer(8192);
    const view = new Uint8Array(backing, 0, 3);
    view.set(encoder.encode("abc"));
    Object.defineProperty(view, "byteLength", { value: 8 });
    return new Response(new ReadableStream({
      start: (c) => (c.enqueue(view), c.close()),
    }));
  },

  async "/chunk-shadow-response-tight"() {
    // The same forgery with a backing buffer no larger than the real window:
    // reading the own properties throws inside the pump, and the client sees
    // an empty reply instead of workerd's "abc".
    const view = new Uint8Array(encoder.encode("abc"));
    Object.defineProperty(view, "buffer", { value: new ArrayBuffer(2) });
    Object.defineProperty(view, "byteOffset", { value: 1 });
    Object.defineProperty(view, "byteLength", { value: 8 });
    return new Response(new ReadableStream({
      start: (c) => (c.enqueue(view), c.close()),
    }));
  },

  async "/chunk-shadow-response-dataview"() {
    // A DataView takes the DataView getters; the same shadowing must not
    // change the window.
    const data = new DataView(encoder.encode("xyz").buffer);
    Object.defineProperty(data, "buffer", { value: new ArrayBuffer(1) });
    Object.defineProperty(data, "byteOffset", { value: 1 });
    Object.defineProperty(data, "byteLength", { value: 4096 });
    return new Response(new ReadableStream({
      start: (c) => (c.enqueue(data), c.close()),
    }));
  },

  async "/request-copy-used"() {
    // #140: `new Request(usedRequest)` refuses, exactly as `fetch(used)` does;
    // `clone()` of an unused body still works in both shapes.
    const settle = (fn) => Promise.resolve().then(fn).then((value) => value, errorInfo);
    const buffered = new Request("https://diff.test/z", { method: "POST", body: "abc" });
    await buffered.text();
    const bufferedCopy = await settle(() => new Request(buffered));

    const streamOf = (chunk) => new ReadableStream({
      start: (c) => (c.enqueue(chunk), c.close()),
    });
    const disturbed = new Request("https://diff.test/s", {
      method: "POST", body: streamOf(encoder.encode("a")),
    });
    const reader = disturbed.body.getReader();
    await reader.read();
    reader.releaseLock();
    const disturbedCopy = await settle(() => new Request(disturbed));

    const cloneSource = new Request("https://diff.test/c", { method: "POST", body: "clone" });
    const bufferedClone = cloneSource.clone();
    const bufferedCloneText = await bufferedClone.text();
    const bufferedSourceText = await cloneSource.text();

    const streamedSource = new Request("https://diff.test/t", {
      method: "POST", body: streamOf(encoder.encode("tee")),
    });
    const streamedClone = streamedSource.clone();
    const streamedBoth = await Promise.all([
      streamedSource.text(), streamedClone.text(),
    ]);
    return report({
      bufferedCopy, disturbedCopy,
      bufferedCloneText, bufferedSourceText, streamedBoth,
    });
  },

  async "/request-bodyless-post"() {
    // #142: no body supplied is `body === null`, not an empty stream, and
    // reading it leaves `bodyUsed` false. `new Request(request)` copies that.
    const request = new Request("https://diff.test/", { method: "POST" });
    const bodyIsNull = request.body === null;
    const usedBefore = request.bodyUsed;
    const value = await request.text();
    const usedAfter = request.bodyUsed;
    const copy = new Request(request);
    const explicitNull = new Request("https://diff.test/null", {
      method: "POST", body: null,
    });
    const empty = new Request("https://diff.test/empty", {
      method: "POST", body: new Uint8Array(0),
    });
    const emptyText = await empty.text();
    const get = new Request("https://diff.test/get");
    return report({
      bodyIsNull, usedBefore, value, usedAfter,
      copyBodyIsNull: copy.body === null,
      explicitNullBodyIsNull: explicitNull.body === null,
      emptyBodyIsNull: empty.body === null,
      emptyText, emptyUsedAfter: empty.bodyUsed,
      getBodyIsNull: get.body === null,
      getCopyBodyIsNull: new Request(get).body === null,
    });
  },

  async "/incoming-bodyless"(request) {
    // #142: the ingress request with no body framing. A POST without
    // Content-Length/Transfer-Encoding reports `body === null`, and text()
    // does not mark it used.
    const bodyIsNull = request.body === null;
    const usedBefore = request.bodyUsed;
    const value = await request.text();
    return report({
      method: request.method, bodyIsNull, usedBefore, value,
      usedAfter: request.bodyUsed,
    });
  },

  async "/incoming-empty"(request) {
    // #142: `Content-Length: 0` frames a body, so workerd reports an empty
    // stream that text() consumes; only an unframed request is null.
    const bodyIsNull = request.body === null;
    const usedBefore = request.bodyUsed;
    const value = await request.text();
    return report({
      method: request.method, bodyIsNull, usedBefore, value,
      usedAfter: request.bodyUsed,
    });
  },

  async "/blob-part-shadow"() {
    // #130: a Blob part's window comes from the view's internal slots, so an
    // own `byteLength` cannot pad the Blob.
    const view = new Uint8Array([97, 98, 99]);
    Object.defineProperty(view, "byteLength", { value: 5 });
    const blob = new Blob([view]);
    return report({ size: blob.size, text: await blob.text() });
  },

  async "/request-copy-locked"() {
    // #140: a body locked by a reader that has read nothing is unused but
    // still cannot be stolen into a second Request; clone() keeps its own
    // locked message.
    const settle = (fn) => Promise.resolve().then(fn).then((value) => value, errorInfo);
    const request = new Request("https://diff.test/locked", {
      method: "POST", body: "abc",
    });
    request.body.getReader();
    return report({
      used: request.bodyUsed,
      locked: request.body.locked,
      copy: await settle(() => new Request(request)),
      clone: await settle(() => request.clone()),
    });
  },

  async "/websocket-send-copy"() {
    // #130: send snapshots the bytes, as workerd does; a mutation after
    // `send()` must not change the delivered frame.
    const pair = new WebSocketPair();
    const sender = pair[0];
    const receiver = pair[1];
    // Accept only the sender: the frame waits in its queue, exactly the path
    // where a retained view would later read the caller's mutated buffer.
    sender.accept();
    receiver.binaryType = "arraybuffer";
    const received = new Promise((resolve) => {
      receiver.addEventListener("message", (event) => {
        const data = event.data;
        resolve({
          tag: Object.prototype.toString.call(data),
          bytes: data instanceof ArrayBuffer
            ? Array.from(new Uint8Array(data))
            : Array.from(new Uint8Array(data.buffer ?? data)),
        });
      });
    });
    const view = new Uint8Array([97, 98, 99]);
    sender.send(view);
    view[0] = 122;
    receiver.accept();
    return report({ received: await received });
  },

  async "/string-chunks-response"() {
    // The response/host path takes the same stream: both runtimes write a
    // string chunk out as its UTF-8 bytes.
    return new Response(new ReadableStream({
      start: (c) => (c.enqueue("alpha"), c.enqueue("beta"), c.close()),
    }));
  },

  async "/iterable-string-body"() {
    // Issue #126: an iterable becomes the body stream whatever its chunks
    // are -- the chunk type is checked by the consumer, not the constructor --
    // so a string chunk rejects every read with the body-stream TypeError
    // instead of being stringified into "alpha,beta".
    const settle = (fn) => Promise.resolve().then(fn).then((value) => value, errorInfo);
    const response = await settle(() => new Response(["alpha", "beta"]).text());
    const request = await settle(() => new Request("https://diff.test/", {
      method: "POST", body: ["alpha", "beta"],
    }).text());
    const set = await settle(() => new Response(new Set(["alpha", "beta"])).text());
    // An async iterable is a body stream too, so a string chunk rejects the
    // same way rather than failing with a cello-only message.
    const asyncIterable = await settle(() => new Response((async function* () {
      yield "alpha";
      yield "beta";
    })()).text());
    const asyncBytes = await settle(() => new Response((async function* () {
      yield encoder.encode("ab");
      yield encoder.encode("cd");
    })()).text());
    // An iterable of bytes is still concatenated eagerly.
    const bytes = await settle(() => new Response([
      encoder.encode("ab"), encoder.encode("cd"),
    ]).text());
    // Precedence is unchanged: a non-iterable object with its own toString
    // stringifies, and so does a primitive.
    const custom = await settle(() => new Response({ toString: () => "custom" }).text());
    const primitive = await settle(() => new Response(42).text());
    return report({
      response, request, set, asyncIterable, asyncBytes, bytes, custom,
      primitive,
    });
  },

  async "/body-read-twice"() {
    // Issue #127: a body can be consumed once, so a second read throws
    // workerd's TypeError rather than serving the buffered bytes again.
    const settle = (promise) => promise.then((value) => value, errorInfo);
    const response = new Response("abc");
    const responseFirst = await response.text();
    const responseSecond = await settle(response.text());
    const request = new Request("https://diff.test/", { method: "POST", body: "abc" });
    const requestFirst = await request.text();
    const requestSecond = await settle(request.text());
    // json() and bytes() are the same consumer.
    const json = new Response('{"a":1}');
    const jsonFirst = await json.json();
    const jsonSecond = await settle(json.json());
    const byteBody = new Response("abc");
    const bytesFirst = [...(await byteBody.bytes())];
    const bytesSecond = await settle(byteBody.bytes());
    // A streaming body is consumed the same way.
    const streamed = new Response(byteStream(["abc"]));
    const streamedFirst = await streamed.text();
    const streamedSecond = await settle(streamed.text());
    // A null body carries nothing, so it stays readable and unused.
    const empty = new Response(null);
    const emptyFirst = await empty.text();
    const emptySecond = await empty.text();
    return report({
      responseFirst, responseSecond, responseBodyUsed: response.bodyUsed,
      requestFirst, requestSecond, requestBodyUsed: request.bodyUsed,
      jsonFirst, jsonSecond, bytesFirst, bytesSecond,
      streamedFirst, streamedSecond,
      emptyFirst, emptySecond, emptyBodyUsed: empty.bodyUsed,
    });
  },

  async "/body-used-fetch-aborted"() {
    // Issue #127: `fetch(request)` consumes the request's body even when the
    // fetch never sends it, so the input reports bodyUsed and a second use is
    // refused with Request's reconstruction error. A request with no body and
    // an `init` carrying its own body leave the input alone.
    const settle = (promise) => promise.then((value) => value, errorInfo);
    const aborted = () => {
      const controller = new AbortController();
      controller.abort(new Error("stop"));
      return controller.signal;
    };
    const request = new Request("https://diff.test/", {
      method: "POST", body: "abc", signal: aborted(),
    });
    const fetchOutcome = await settle(fetch(request));
    const bodyUsed = request.bodyUsed;
    const reread = await settle(request.text());
    const secondFetch = await settle(fetch(request));
    const get = new Request("https://diff.test/get");
    const getOutcome = await settle(fetch(get, { signal: aborted() }));
    const getUsed = get.bodyUsed;
    const getSecond = await settle(fetch(get, { signal: aborted() }));
    const replaced = new Request("https://diff.test/replaced", {
      method: "POST", body: "abc",
    });
    const replacedOutcome = await settle(fetch(replaced, {
      method: "POST", body: "zzz", signal: aborted(),
    }));
    const replacedUsed = replaced.bodyUsed;
    // A streaming body is adopted by the Request copy too, not refused.
    const streamed = new Request("https://diff.test/streamed", {
      method: "POST",
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(encoder.encode("abc"));
          controller.close();
        },
      }),
      signal: aborted(),
    });
    const streamedOutcome = await settle(fetch(streamed));
    const streamedUsed = streamed.bodyUsed;
    const streamedReread = await settle(streamed.text());
    // A POST with no body supplied carries nothing to consume: workerd's
    // `body` is null there, so a fetch leaves the request unused.
    const bodyless = new Request("https://diff.test/bodyless", { method: "POST" });
    const bodylessOutcome = await settle(fetch(bodyless, { signal: aborted() }));
    const bodylessUsed = bodyless.bodyUsed;
    const bodylessSecond = await settle(fetch(bodyless, { signal: aborted() }));
    // A fetch that refuses its init must not burn the input: workerd copies
    // the Request only after validation succeeds.
    const badMethod = new Request("https://diff.test/bad-method", {
      method: "POST", body: "abc",
    });
    const badMethodOutcome = await settle(fetch(badMethod, { method: "BAD METHOD" }));
    const badMethodUsed = badMethod.bodyUsed;
    const badMethodText = await settle(badMethod.text());
    const badRedirect = new Request("https://diff.test/bad-redirect", {
      method: "POST", body: "abc",
    });
    const badRedirectOutcome = await settle(fetch(badRedirect, { redirect: "bogus" }));
    const badRedirectUsed = badRedirect.bodyUsed;
    const badRedirectText = await settle(badRedirect.text());
    return report({
      fetchOutcome, bodyUsed, reread, secondFetch,
      getOutcome, getUsed, getSecond,
      replacedOutcome, replacedUsed,
      streamedOutcome, streamedUsed, streamedReread,
      bodylessOutcome, bodylessUsed, bodylessSecond,
      badMethodOutcome, badMethodUsed, badMethodText,
      badRedirectOutcome, badRedirectUsed, badRedirectText,
    });
  },

  async "/iterable-body-lazy"() {
    // Issue #126: an iterable body is pulled lazily, one value per read. The
    // constructor pulls nothing, an all-byte iterable is a stream too, and no
    // Content-Length is set for either (measured against workerd).
    const count = { pulls: 0 };
    function* strings(n) {
      for (let i = 0; i < n; i += 1) {
        count.pulls += 1;
        yield String(i);
      }
    }
    const response = new Response(strings(5000));
    const afterCtor = count.pulls;
    const contentLength = response.headers.get("content-length");
    const reader = response.body.getReader();
    const first = await reader.read();
    const afterFirstRead = count.pulls;
    const rest = [];
    for (;;) {
      const step = await reader.read();
      if (step.done) break;
      rest.push(step.value);
    }
    const byteCount = { pulls: 0 };
    function* bytes() {
      byteCount.pulls += 1;
      yield encoder.encode("a");
      byteCount.pulls += 1;
      yield encoder.encode("b");
    }
    const byteResponse = new Response(bytes());
    const byteAfterCtor = byteCount.pulls;
    const byteContentLength = byteResponse.headers.get("content-length");
    const byteText = await byteResponse.text();
    return report({
      afterCtor, contentLength, first: first.value, afterFirstRead,
      chunks: rest.length, total: count.pulls,
      byteAfterCtor, byteContentLength, byteText, bytePulls: byteCount.pulls,
    });
  },

  async "/iterable-infinite-body"() {
    // Issue #126: an infinite generator must not be drained by the
    // constructor; the body is read as a bounded prefix and cancelled.
    function* infinite() {
      let i = 0;
      for (;;) {
        yield String(i);
        i += 1;
      }
    }
    const response = new Response(infinite());
    const reader = response.body.getReader();
    const prefix = [];
    for (let i = 0; i < 3; i += 1) prefix.push((await reader.read()).value);
    await reader.cancel();
    // A cancel pulls the producer's cleanup and adopts its outcome: a
    // rejecting finally or return() rejects cancel(), as workerd's does.
    const asyncCleanup = new Response((async function* () {
      try {
        yield "a";
      } finally {
        throw new Error("async cleanup failed");
      }
    })());
    const asyncCleanupReader = asyncCleanup.body.getReader();
    await asyncCleanupReader.read();
    const asyncCleanupCancel = await asyncCleanupReader.cancel()
      .then(() => "resolved", errorInfo);
    const rejectingReturn = {
      [Symbol.iterator]() {
        let first = true;
        return {
          next() {
            if (!first) return { value: undefined, done: true };
            first = false;
            return { value: "a", done: false };
          },
          return() { return Promise.reject(new Error("return rejected")); },
        };
      },
    };
    const rejectingReturnReader = new Response(rejectingReturn).body.getReader();
    await rejectingReturnReader.read();
    const rejectingReturnCancel = await rejectingReturnReader.cancel()
      .then(() => "resolved", errorInfo);
    return report({
      prefix, bodyUsed: response.bodyUsed,
      asyncCleanupCancel, rejectingReturnCancel,
    });
  },

  async "/sse"() {
    const body = new ReadableStream({
      start: (c) => {
        for (let index = 0; index < 4; index += 1) c.enqueue(encoder.encode(`data: ${index}\n\n`));
        c.enqueue(encoder.encode("event: end\ndata: bye\n\n"));
        c.close();
      },
    });
    return new Response(body, { headers: { "content-type": "text/event-stream" } });
  },

  async "/error-in-stream"() {
    let controller;
    const rs = new ReadableStream({
      start: (c) => {
        controller = c;
        c.enqueue("partial");
      },
    });
    const reader = rs.getReader();
    const first = await reader.read();
    controller.error(new Error("boom"));
    const second = await reader.read().then(() => "resolved", (error) => errorInfo(error));
    return report({ first: first.value, second });
  },

  async "/compression-cancel-releases-slot"() {
    // Issue #270: a readable cancel with no close or abort still frees the
    // native compressor slot once nothing accepted is outstanding. Every
    // wrapper stays held, so the collector cannot help: 300 rounds must
    // construct cleanly and the final probe must too.
    const held = [];
    let constructed = 0;
    let rangeErrors = 0;
    for (let i = 0; i < 300; i += 1) {
      try {
        const stream = new CompressionStream("gzip");
        held.push(stream);
        constructed += 1;
        await stream.readable.cancel("done");
      } catch (e) {
        if (e?.name === "RangeError") rangeErrors += 1;
        else return report({ uncaught: errorInfo(e) });
      }
    }
    let final;
    try {
      held.push(new CompressionStream("gzip"));
      final = "ok";
    } catch (e) { final = errorInfo(e); }
    return report({ constructed, rangeErrors, final, held: held.length });
  },

  async "/compression-response-cancel-releases-slot"() {
    // Issue #270: the same release through a Response body cancel.
    const held = [];
    let constructed = 0;
    let rangeErrors = 0;
    for (let i = 0; i < 300; i += 1) {
      try {
        const stream = new CompressionStream("gzip");
        held.push(stream);
        constructed += 1;
        await new Response(stream.readable).body.cancel("done");
      } catch (e) {
        if (e?.name === "RangeError") rangeErrors += 1;
        else return report({ uncaught: errorInfo(e) });
      }
    }
    let final;
    try {
      held.push(new CompressionStream("gzip"));
      final = "ok";
    } catch (e) { final = errorInfo(e); }
    return report({ constructed, rangeErrors, final, held: held.length });
  },

  async "/compression-pipe-fail-releases-slot"() {
    // Issue #270: a pipeTo that fails its destination cancels the
    // compression source; the accepted write still runs, then the drain
    // frees the slot. Wrappers stay held, so only a prompt release keeps
    // 300 rounds under the cap.
    const held = [];
    let constructed = 0;
    let rangeErrors = 0;
    for (let i = 0; i < 300; i += 1) {
      try {
        const stream = new CompressionStream("gzip");
        constructed += 1;
        held.push(stream);
        const writer = stream.writable.getWriter();
        held.push(writer);
        const write = writer.write(encoder.encode("hello hello hello"));
        write.then(() => {}, () => {});
        await stream.readable.pipeTo(new WritableStream({
          write() { throw new Error("dest-boom"); },
        })).then(() => "ok", errorInfo);
        await write.then(() => "written", errorInfo);
      } catch (e) {
        if (e?.name === "RangeError") rangeErrors += 1;
        else return report({ uncaught: errorInfo(e) });
      }
    }
    let final;
    try {
      held.push(new CompressionStream("gzip"));
      final = "ok";
    } catch (e) { final = errorInfo(e); }
    return report({ constructed, rangeErrors, final, held: held.length });
  },

  async "/compression-abort-failing-flush"() {
    // Issue #270: a truncated member with a pending read, so the close
    // starts a flush that fails; an abort that lands before it settles
    // still wins. Close and closed reject with the abort reason, the
    // abort fulfills, and the pending read keeps the incomplete-data
    // error from the flush.
    const member = await gzipMember(encoder.encode("payload payload payload"));
    const truncated = member.slice(0, 10);
    const out = {};
    for (const when of ["same-turn", "next-tick"]) {
      const stream = new DecompressionStream("gzip");
      const writer = stream.writable.getWriter();
      const reader = stream.readable.getReader();
      const pull = reader.read().then(
        ({ value, done }) => ({ bytes: value?.byteLength ?? 0, done }), errorInfo);
      await writer.write(truncated);
      const close = writer.close().then(() => "closed-ok", errorInfo);
      let abort;
      if (when === "same-turn") abort = writer.abort("abort-reason").then(() => "aborted", errorInfo);
      else { await null; abort = writer.abort("abort-reason").then(() => "aborted", errorInfo); }
      out[when] = {
        close: await close,
        abort: await abort,
        closed: await writer.closed.then(() => "closed", errorInfo),
        pull: await pull,
      };
    }
    return report(out);
  },
};

export default {
  async fetch(request) {
    const { pathname } = new URL(request.url);
    const route = routes[pathname];
    if (!route) return new Response("not found", { status: 404 });
    try {
      return await route(request);
    } catch (error) {
      // A case that throws unexpectedly must fail: the runner only accepts 200.
      return new Response(JSON.stringify({ uncaught: errorInfo(error) }), { status: 500 });
    }
  },
};
