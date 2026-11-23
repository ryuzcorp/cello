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
    return report({
      fetchOutcome, bodyUsed, reread, secondFetch,
      getOutcome, getUsed, getSecond,
      replacedOutcome, replacedUsed,
      streamedOutcome, streamedUsed, streamedReread,
      bodylessOutcome, bodylessUsed, bodylessSecond,
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
    return report({ prefix, bodyUsed: response.bodyUsed });
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
