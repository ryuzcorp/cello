// KV binding fixture. Every route reports what the binding answered, so a
// transcript never depends on wall time: the one probe that needs a clock
// (an expiration 59 seconds out) normalizes the timestamp it echoed back.
const encoder = new TextEncoder();
const report = (value) =>
  new Response(JSON.stringify(value), {
    headers: { "content-type": "text/plain; charset=utf-8" },
  });
const errorInfo = (error) => ({
  name: error?.name ?? String(error),
  message: error?.message ?? String(error),
});
const attempt = async (fn) => {
  try {
    return { ok: await fn() };
  } catch (error) {
    return { error: errorInfo(error) };
  }
};
const hex = (buffer) =>
  [...new Uint8Array(buffer)].map((b) => b.toString(16).padStart(2, "0")).join("");

let KV;
const routes = {
  // Every read type, the missing-key answer for each, metadata round-trip,
  // and delete (including an absent key and an empty one).
  async "/get-types"() {
    await KV.put("types", JSON.stringify({ a: 1, b: ["x", 2] }), {
      metadata: { n: 7, s: "m" },
    });
    const text = await KV.get("types");
    const buffer = await KV.get("types", "arrayBuffer");
    const stream = await KV.get("types", "stream");
    const withMeta = await KV.getWithMetadata("types");
    const withMetaJson = await KV.getWithMetadata("types", "json");
    const missing = {};
    for (const type of [undefined, "text", "json", "arrayBuffer", "stream"]) {
      missing[type ?? "default"] = await KV.get("absent", type);
    }
    const missingMeta = await KV.getWithMetadata("absent");
    await KV.delete("types");
    return report({
      text,
      json: await KV.get("types", "json"),
      bufferBytes: buffer.byteLength,
      bufferHex: hex(buffer).slice(0, 16),
      streamText: await new Response(stream).text(),
      withMeta: {
        value: withMeta.value,
        metadata: withMeta.metadata,
        json: withMetaJson.value,
        jsonMetadata: withMetaJson.metadata,
      },
      missing,
      missingMeta: { value: missingMeta.value, metadata: missingMeta.metadata },
      afterDelete: await KV.get("types"),
      deleteMissing: await attempt(() => KV.delete("absent")),
      deleteEmpty: await attempt(() => KV.delete("")),
    });
  },

  // Key rules: an empty key, the two dot names, the 512-byte bound, and the
  // same rules on reads. Upstream's exact classes and messages.
  async "/keys"() {
    const out = {};
    for (const [name, key] of [
      ["empty", ""],
      ["dot", "."],
      ["dotdot", ".."],
      ["slashy", "a/../b"],
      ["atLimit", "k".repeat(512)],
      ["overLimit", "k".repeat(513)],
    ]) {
      out[`put:${name}`] = await attempt(async () => {
        await KV.put(key, "v");
        return "ok";
      });
      out[`get:${name}`] = await attempt(() => KV.get(key));
      out[`delete:${name}`] = await attempt(() => KV.delete(key));
    }
    return report(out);
  },

  // Validation bounds: the value and metadata limits, the expiration floor,
  // the bulk-get size, the cursor and the list limit.
  async "/validation"() {
    const out = {};
    out.valueAt = await attempt(async () => {
      await KV.put("big", new Uint8Array(25 * 1024 * 1024));
      return (await KV.get("big", "arrayBuffer")).byteLength;
    });
    out.valueOver = await attempt(() =>
      KV.put("too-big", new Uint8Array(25 * 1024 * 1024 + 1)));
    out.metadataAt = await attempt(async () => {
      await KV.put("meta-at", "v", { metadata: "x".repeat(1022) });
      return "ok";
    });
    out.metadataOver = await attempt(() =>
      KV.put("meta-over", "v", { metadata: "x".repeat(1023) }));
    out.valueObject = await attempt(() => KV.put("object", { a: 1 }));
    out.valueDate = await attempt(() => KV.put("date", new Date(0)));
    out.ttl59 = await attempt(() => KV.put("ttl", "v", { expirationTtl: 59 }));
    out.ttl60 = await attempt(() => KV.put("ttl60", "v", { expirationTtl: 60 }));
    out.expirationPast = await attempt(() =>
      KV.put("past", "v", { expiration: 1000000000 }));
    const at = Math.floor(Date.now() / 1000) + 59;
    out.expiration59 = await attempt(() => KV.put("soon", "v", { expiration: at }))
      .then((outcome) => JSON.parse(
        JSON.stringify(outcome).replaceAll(String(at), "<expiration>"),
      ));
    out.expirationOk = await attempt(() =>
      KV.put("later", "v", { expiration: Math.floor(Date.now() / 1000) + 120 }));
    out.bulkEmpty = await attempt(() => KV.get([]));
    out.bulkTooMany = await attempt(() =>
      KV.get(Array.from({ length: 101 }, (_, i) => `bulk/${i}`)));
    out.cursor = await attempt(() => KV.list({ cursor: "not-a-cursor" }));
    out.listLimit1000 = await attempt(async () => {
      for (const key of ["limit/a", "limit/b"]) await KV.put(key, "v");
      return (await KV.list({ prefix: "limit/", limit: 1000 })).keys.length;
    });
    out.listLimit1001 = await attempt(() =>
      KV.list({ prefix: "limit/", limit: 1001 }));
    out.listLimit5000 = await attempt(() =>
      KV.list({ prefix: "limit/", limit: 5000 }));
    return report(out);
  },

  // List shape and paging: prefixes, the cursor round-trip, key metadata,
  // and the absent cursor when a page is complete.
  async "/list"() {
    for (const key of ["list/a", "list/b", "list/c", "other/d"]) {
      await KV.put(key, "v");
    }
    await KV.put("list/meta", "v", { metadata: { k: 1 } });
    const first = await KV.list({ prefix: "list/" });
    const page = await KV.list({ prefix: "list/", limit: 2 });
    const second = await KV.list({
      prefix: "list/",
      limit: 2,
      cursor: page.cursor,
    });
    return report({
      firstNames: first.keys.map((key) => key.name),
      firstComplete: first.list_complete,
      firstCursor: first.cursor === undefined,
      keysShape: first.keys.map((key) => ({
        name: key.name,
        expiration: key.expiration ?? null,
        metadata: key.metadata ?? null,
      })),
      pageNames: page.keys.map((key) => key.name),
      pageComplete: page.list_complete,
      pageCursorSet: page.cursor !== undefined,
      secondNames: second.keys.map((key) => key.name),
      secondComplete: second.list_complete,
    });
  },

  // Bulk reads answer a Map with a null hole, and carry metadata.
  async "/bulk"() {
    await KV.put("bulk/a", "1");
    await KV.put("bulk/b", "2");
    const plain = await KV.get(["bulk/a", "bulk/b", "bulk/missing"]);
    const typed = await KV.get(["bulk/a"], "json");
    const withMeta = await KV.getWithMetadata(["bulk/a", "bulk/missing"]);
    return report({
      plain: [...plain],
      typed: [...typed],
      withMeta: [...withMeta],
    });
  },
};

export default {
  async fetch(request, env) {
    KV = env.KV;
    const { pathname } = new URL(request.url);
    const route = routes[pathname];
    if (!route) return new Response("not found", { status: 404 });
    try {
      return await route(request);
    } catch (error) {
      return new Response(JSON.stringify({ uncaught: errorInfo(error) }), {
        status: 500,
      });
    }
  },
};
