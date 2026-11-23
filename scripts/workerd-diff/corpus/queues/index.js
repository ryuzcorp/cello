// Queue probes: producer validation and the consumer batch shape.
const canonical = (value) => {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.keys(value).sort().map((k) => [k, canonical(value[k])]));
  }
  return value;
};
const report = (value) => new Response(JSON.stringify(canonical(value)), {
  headers: { "content-type": "text/plain; charset=utf-8" },
});
const errorInfo = (error) => ({
  name: error?.name ?? String(error),
  message: error?.message ?? String(error),
});
const attempt = async (fn) => {
  try { return { ok: await fn() }; } catch (error) { return { error: errorInfo(error) }; }
};
let Q;
let LOG;
const routes = {
  async "/send-validation"() {
    const out = {};
    out.text = await attempt(() => Q.send("hello"));
    out.object = await attempt(() => Q.send({ a: 1 }));
    out.nullBody = await attempt(() => Q.send(null));
    out.number = await attempt(() => Q.send(5));
    out.contentTypeText = await attempt(() => Q.send("t", { contentType: "text" }));
    out.contentTypeJson = await attempt(() => Q.send({ j: 1 }, { contentType: "json" }));
    out.contentTypeV8 = await attempt(() => Q.send({ v: 1 }, { contentType: "v8" }));
    out.contentTypeBytes = await attempt(() => Q.send(new Uint8Array([1]), { contentType: "bytes" }));
    out.contentTypeBad = await attempt(() => Q.send("x", { contentType: "bogus" }));
    out.delayOk = await attempt(() => Q.send("d", { delaySeconds: 86400 }));
    out.delayTooBig = await attempt(() => Q.send("d", { delaySeconds: 86401 }));
    out.delayNegative = await attempt(() => Q.send("d", { delaySeconds: -1 }));
    out.overSize = await attempt(() => Q.send("x".repeat(128_001)));
    out.atLimit = await attempt(() => Q.send("x".repeat(128_000)));
    return report(out);
  },
  async "/send-batch"() {
    const out = {};
    out.empty = await attempt(() => Q.sendBatch([]));
    out.two = await attempt(async () => {
      const r = await Q.sendBatch([{ body: "a" }, { body: { b: 1 }, contentType: "json" }]);
      return { metadata: r.metadata, keys: Object.keys(r).sort() };
    });
    out.tooMany = await attempt(() => Q.sendBatch(Array.from({ length: 101 }, (_, i) => ({ body: `m${i}` }))));
    out.shapeBad = await attempt(() => Q.sendBatch([{ body: "a", contentType: "bogus" }]));
    out.overBytes = await attempt(() => {
      const body = "y".repeat(200_000);
      return Q.sendBatch([{ body }, { body }]);
    });
    out.mixedContentType = await attempt(() => Q.sendBatch([{ body: "s", contentType: "text" }, { body: { j: 1 } }]));
    return report(out);
  },
  async "/shape"() {
    await LOG.delete("shape");
    await Q.send("shape-probe", { contentType: "text" });
    for (let i = 0; i < 600; i++) {
      const seen = await LOG.get("shape", "json");
      if (seen !== null) return report({ seen });
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    return report({ seen: null, timedOut: true });
  },
  async "/ack-retry"() {
    await LOG.delete("retried");
    await Q.send("retry-me", { contentType: "text" });
    const started = Date.now();
    for (let i = 0; i < 600; i++) {
      const seen = await LOG.get("retried", "json");
      if (Array.isArray(seen) && seen.length >= 2) {
        return report({ seen, waitedMs: Date.now() - started });
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    return report({ seen: await LOG.get("retried", "json"), timedOut: true });
  },
};

async function record(key, entry) {
  const seen = (await LOG.get(key, "json")) ?? [];
  seen.push(entry);
  await LOG.put(key, JSON.stringify(seen));
}

export default {
  async fetch(request, env) {
    Q = env.Q;
    LOG = env.LOG;
    const { pathname } = new URL(request.url);
    const route = routes[pathname];
    if (!route) return new Response("not found", { status: 404 });
    try {
      return await route(request);
    } catch (error) {
      return new Response(JSON.stringify(canonical({ uncaught: errorInfo(error) })), { status: 500 });
    }
  },
  async queue(batch, env) {
    LOG = env.LOG;
    // The settle methods are prototype members in workerd, so nothing a
    // handler iterates, spreads or serializes sees a function.
    const messageShape = (m) => ({
      keys: Object.keys(m).sort(),
      spreadKeys: Object.keys({ ...m }).sort(),
      jsonKeys: Object.keys(JSON.parse(JSON.stringify(m))).sort(),
      idType: typeof m.id,
      timestampType: typeof m.timestamp,
      attempts: m.attempts,
      body: m.body,
      contentType: m.body instanceof Uint8Array ? "bytes" : typeof m.body,
    });
    const shape = {
      queue: batch.queue,
      batchKeys: Object.keys(batch).sort(),
      batchSpreadKeys: Object.keys({ ...batch }).sort(),
      batchJsonKeys: Object.keys(JSON.parse(JSON.stringify(batch))).sort(),
      metadataJsonKeys: Object.keys(JSON.parse(JSON.stringify(batch.metadata))).sort(),
      messages: batch.messages.map(messageShape),
    };
    if (batch.messages.some((m) => m.body === "shape-probe")) {
      await record("shape", shape);
      batch.ackAll();
      return;
    }
    if (batch.messages.some((m) => m.body === "retry-me")) {
      await record("retried", shape);
      for (const message of batch.messages) {
        message.retry({ delaySeconds: 0 });
      }
      return;
    }
    await record("received", shape);
    batch.ackAll();
  },
};
