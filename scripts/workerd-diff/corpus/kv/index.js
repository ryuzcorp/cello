// A KV binding's byte encoding, measured against workerd with a minimal KV
// service (see `kv.js`/`workerd.capnp`). The value a `put()` stores must be
// read from a view's internal slots (#130), and a string chunk in a value
// stream must be its UTF-8 bytes, not zero bytes.
const errorInfo = (error) => ({
  name: error?.name ?? String(error),
  message: error?.message ?? String(error),
});
const settle = (fn) => Promise.resolve().then(fn).then(
  (value) => value,
  (error) => errorInfo(error),
);
const streamOf = (chunk) => new ReadableStream({
  start(c) { c.enqueue(chunk); c.close(); },
});
const read = async (env, key) => {
  const value = await env.KV.get(key, "arrayBuffer");
  if (value === null) return null;
  return Array.from(value instanceof ArrayBuffer ? new Uint8Array(value) : value);
};

export default {
  async fetch(request, env) {
    const path = new URL(request.url).pathname;
    if (path === "/kv-string-chunk") {
      const outcome = await settle(async () => {
        await env.KV.put("s", streamOf("abc"));
        return await read(env, "s");
      });
      return Response.json({ outcome });
    }
    if (path === "/kv-view-shadow") {
      const outcome = await settle(async () => {
        const view = new Uint8Array(new ArrayBuffer(8), 0, 3);
        view.set([97, 98, 99]);
        Object.defineProperty(view, "byteLength", { value: 5 });
        await env.KV.put("v", view);
        return await read(env, "v");
      });
      return Response.json({ outcome });
    }
    return Response.json({ ok: true });
  },
};
