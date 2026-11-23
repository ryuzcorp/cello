// The Cache API fixture. Its `workerd.capnp` gives workerd a cache service,
// which a deployment config cannot define; cello reads `wrangler.json` and
// provides `caches` without a binding. Every case returns a body that already
// carries what the Worker observed, so a transcript never depends on wall time.
const report = (value) => new Response(JSON.stringify(value), {
  headers: { "content-type": "text/plain; charset=utf-8" },
});
const errorInfo = (error) => ({
  name: error?.name ?? String(error),
  message: error?.message ?? String(error),
});

const stringStream = () => new ReadableStream({
  start(controller) {
    controller.enqueue("alpha");
    controller.enqueue("beta");
    controller.close();
  },
});

const routes = {
  async "/cache-put-string-chunks"() {
    // Issue #128: `cache.put` consumes the body host-side, so a string chunk
    // is taken as its UTF-8 bytes and the entry stores "alphabeta". Cello's
    // capped drain used to throw RangeError and store nothing.
    const key = new Request("https://cache.test/string");
    const response = new Response(stringStream(), {
      headers: { "cache-control": "max-age=60" },
    });
    const put = await caches.default.put(key, response).then(
      () => "ok",
      errorInfo,
    );
    const hit = await caches.default.match(key);
    const text = hit === undefined ? "miss" : await hit.text();
    return report({ put, text, bodyUsed: response.bodyUsed });
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
