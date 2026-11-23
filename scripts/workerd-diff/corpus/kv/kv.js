// The KV namespace service workerd's `kvNamespace` binding speaks to, for the
// differential fixture only: one in-memory map keyed by the request path.
const store = new Map();
export default {
  async fetch(request) {
    const key = new URL(request.url).pathname;
    if (request.method === "PUT" || request.method === "POST") {
      store.set(key, new Uint8Array(await request.arrayBuffer()));
      return new Response(null, { status: 200 });
    }
    if (request.method === "GET") {
      const bytes = store.get(key);
      return bytes
        ? new Response(bytes, { status: 200 })
        : new Response(null, { status: 404 });
    }
    if (request.method === "DELETE") {
      store.delete(key);
      return new Response(null, { status: 200 });
    }
    return new Response(null, { status: 405 });
  },
};
