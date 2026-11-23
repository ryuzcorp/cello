// Modified by Ryuz Corp (Cello): cross-isolate revived-Request fixture. Apache-2.0.
const J = (o, st = 200) => Response.json(o, { status: st });
const settle = (fn) => Promise.resolve().then(fn).then(
  (value) => value,
  (e) => ({ name: e?.name ?? String(e), message: e?.message ?? String(e) }),
);
const streamOf = (chunk) => new ReadableStream({
  start(c) { c.enqueue(chunk); c.close(); },
});

const makePost = () => new Request('https://revive.test/post', {
  method: 'POST', body: 'abc',
});

// The response pump reads a chunk's window from internal slots (#130): own
// `buffer`/`byteOffset`/`byteLength` must not widen it, and a tight backing
// buffer must not throw the pump into an empty reply.
const shadowedTight = () => {
  const view = new Uint8Array([97, 98, 99]);
  Object.defineProperty(view, 'buffer', { value: new ArrayBuffer(2) });
  Object.defineProperty(view, 'byteOffset', { value: 1 });
  Object.defineProperty(view, 'byteLength', { value: 8 });
  return view;
};
const shadowedWide = () => {
  const view = new Uint8Array(new ArrayBuffer(8192), 0, 3);
  view.set([97, 98, 99]);
  Object.defineProperty(view, 'byteLength', { value: 8 });
  return view;
};

// Workerd refuses a forged prototype view with the bytes TypeError; cello
// must not silently send an empty body (#130).
const forgedChunks = async () => ({
  typed: await settle(() => new Response(streamOf(Object.create(Uint8Array.prototype))).text()),
  buffer: await settle(() => new Response(streamOf(Object.create(ArrayBuffer.prototype))).bytes()),
  dataView: await settle(() => new Response(streamOf(Object.create(DataView.prototype))).text()),
});

// `new Request(used)` throws and `clone()` still copies (#140).
const requestCopyUsed = async () => {
  const buffered = new Request('https://revive.test/z', { method: 'POST', body: 'abc' });
  await buffered.text();
  const bufferedCopy = await settle(() => new Request(buffered));
  const streamed = new Request('https://revive.test/s', {
    method: 'POST', body: streamOf(new Uint8Array([97])),
  });
  const reader = streamed.body.getReader();
  await reader.read();
  reader.releaseLock();
  const disturbedCopy = await settle(() => new Request(streamed));
  const source = new Request('https://revive.test/c', { method: 'POST', body: 'clone' });
  const clone = source.clone();
  return {
    bufferedCopy, disturbedCopy,
    cloneText: await clone.text(), sourceText: await source.text(),
  };
};

// No body supplied is `body === null`, and reading it leaves `bodyUsed`
// false (#142).
const requestBodyless = async () => {
  const request = new Request('https://revive.test/', { method: 'POST' });
  const bodyIsNull = request.body === null;
  const usedBefore = request.bodyUsed;
  const text = await request.text();
  const usedAfter = request.bodyUsed;
  const copy = new Request(request);
  const empty = new Request('https://revive.test/e', {
    method: 'POST', body: new Uint8Array(0),
  });
  const emptyText = await empty.text();
  return {
    bodyIsNull, usedBefore, text, usedAfter,
    copyBodyIsNull: copy.body === null,
    emptyBodyIsNull: empty.body === null,
    emptyUsedAfter: empty.bodyUsed, emptyText,
  };
};

// What the Worker sees for its own incoming request (#142).
const incomingInspect = async (request) => {
  const bodyIsNull = request.body === null;
  const usedBefore = request.bodyUsed;
  const text = await request.text();
  const textAgain = await request.text().then(
    (value) => ({ value }),
    (e) => ({ name: e?.name ?? String(e), message: e?.message ?? String(e) }),
  );
  return {
    method: request.method, bodyIsNull, usedBefore, text,
    usedAfter: request.bodyUsed, textAgain,
  };
};

export default {
  async fetch(req, env) {
    const t = new URL(req.url).pathname.slice(1);
    // Routes that answer with a stream or inspect the ingress request never
    // touch the controller fixture.
    if (t === 'chunk-shadow-tight') return new Response(streamOf(shadowedTight()));
    if (t === 'chunk-shadow-wide') return new Response(streamOf(shadowedWide()));
    const run = async () => {
      if (t === 'health') return { ok: true };
      switch (t) {
        case 'request-copy-used': return requestCopyUsed();
        case 'request-bodyless-post': return requestBodyless();
        case 'chunk-forged': return forgedChunks();
        case 'incoming-inspect': return incomingInspect(req);
      }
      const stub = await env.SB.getBodyStub('probe');
      switch (t) {
        // A Request built here is handed to a Durable Object in another
        // isolate: the DO sees the revived copy.
        case 'revived-post':
          return await stub.probePost(makePost());
        case 'revived-get':
          return await stub.probeGet(new Request('https://revive.test/get'));
        case 'revived-bad-init':
          return await stub.probeBadInit(makePost());
        case 'revived-read':
          return await stub.probeRead(makePost());
        default: return { error: `unknown ${t}` };
      }
    };
    try {
      return J({ test: t, ok: true, result: await run() });
    } catch (e) {
      return J({ test: t, ok: false, error: `${e.name}: ${e.message}` });
    }
  },
};
