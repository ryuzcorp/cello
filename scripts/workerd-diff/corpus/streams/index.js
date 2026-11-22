// Every case returns a body that already contains whatever order the Worker
// observed, so the harness never looks at wall time. Response bodies use byte
// chunks: Workers only accepts `Uint8Array`/`ArrayBuffer`.

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const text = (body) => new Response(body, { headers: { "content-type": "text/plain; charset=utf-8" } });
const report = (value) => text(JSON.stringify(value));
const errorInfo = (error) => ({ name: error?.name ?? String(error), message: error?.message ?? String(error) });
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
