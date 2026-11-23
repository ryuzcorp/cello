// The Cache API fixture. Its `workerd.capnp` gives workerd a cache service,
// which a deployment config cannot define; cello reads `wrangler.json` and
// provides `caches` without a binding. Every case returns a body that already
// carries what the Worker observed, so a transcript never depends on wall time.
const encoder = new TextEncoder();
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

const attempt = async (fn) => {
  try {
    return { ok: await fn() };
  } catch (error) {
    return { error: errorInfo(error) };
  }
};
const CT = { "content-type": "text/plain" };

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
    // A producer that reuses one buffer across pulls: the chunk must be
    // copied when it is read, as the host copies it, or the stored entry
    // holds the later bytes.
    const reuseKey = new Request("https://cache.test/reuse");
    const buffer = new Uint8Array(8);
    let step = 0;
    const reuse = new Response(new ReadableStream({
      pull(controller) {
        if (step === 2) {
          controller.close();
          return;
        }
        buffer.set(encoder.encode(step === 0 ? "alp" : "bra"));
        controller.enqueue(buffer.subarray(0, 3));
        step += 1;
      },
    }), { headers: { "cache-control": "max-age=60" } });
    const reusePut = await caches.default.put(reuseKey, reuse).then(
      () => "ok",
      errorInfo,
    );
    const reuseHit = await caches.default.match(reuseKey);
    const reuseText = reuseHit === undefined ? "miss" : await reuseHit.text();
    return report({
      put, text, bodyUsed: response.bodyUsed, reusePut, reuseText,
      reuseBodyUsed: reuse.bodyUsed,
    });
  },

  // A store refusal is a TypeError with the same text in both runtimes.
  async "/put-refusals"() {
    const cache = caches.default;
    return report({
      post: await attempt(() => cache.put(
        new Request("https://cache.test/post", { method: "POST" }),
        new Response("x", { headers: CT }))),
      partial: await attempt(() => cache.put(
        new Request("https://cache.test/206"),
        new Response("x", { status: 206, headers: CT }))),
      varyStar: await attempt(() => cache.put(
        new Request("https://cache.test/vary"),
        new Response("x", { headers: { ...CT, vary: "*" } }))),
    });
  },

  // match options: ignoreMethod is honored; ignoreSearch and ignoreVary are
  // refused rather than silently answering a different question.
  async "/match-options"() {
    const cache = caches.default;
    const key = new Request("https://cache.test/match");
    await cache.put(
      key,
      new Response("body", { headers: { ...CT, "cache-control": "max-age=60" } }),
    );
    const hit = async (request, options) => {
      const found = await cache.match(request, options);
      return found === undefined ? "miss" : "hit";
    };
    return report({
      get: await attempt(() => hit(key)),
      headWithoutIgnore: await attempt(() =>
        hit(new Request(key.url, { method: "HEAD" }))),
      headWithIgnore: await attempt(() =>
        hit(new Request(key.url, { method: "HEAD" }), { ignoreMethod: true })),
      ignoreSearch: await attempt(() =>
        hit(new Request("https://cache.test/match?x=1"), { ignoreSearch: true })),
      ignoreSearchFalse: await attempt(() =>
        hit(new Request("https://cache.test/match?x=1"), { ignoreSearch: false })),
      ignoreVary: await attempt(() => hit(key, { ignoreVary: true })),
      ignoreVaryFalse: await attempt(() => hit(key, { ignoreVary: false })),
      unknownOption: await attempt(() => hit(key, { bogus: true })),
      ifNoneMatch: await attempt(async () => {
        const found = await cache.match(new Request(key.url, {
          headers: { "if-none-match": "\"nope\"" },
        }));
        return found === undefined
          ? "miss"
          : { status: found.status, text: await found.text() };
      }),
      range: await attempt(async () => {
        const found = await cache.match(new Request(key.url, {
          headers: { range: "bytes=1-2" },
        }));
        return found === undefined
          ? "miss"
          : {
            status: found.status,
            text: await found.text(),
            range: found.headers.get("content-range"),
          };
      }),
      missing: await attempt(() => hit(new Request("https://cache.test/absent"))),
    });
  },

  // delete and namespaces.
  async "/delete"() {
    const cache = caches.default;
    const key = new Request("https://cache.test/del");
    await cache.put(
      key,
      new Response("d", { headers: { ...CT, "cache-control": "max-age=60" } }),
    );
    const out = {};
    out.deletePresent = await attempt(async () => typeof await cache.delete(key));
    out.after = await attempt(async () =>
      (await cache.match(key)) === undefined ? "miss" : "hit");
    out.deleteMissing = await attempt(async () =>
      typeof await cache.delete(new Request("https://cache.test/del-missing")));
    out.deletePost = await attempt(async () =>
      typeof await cache.delete(
        new Request("https://cache.test/del", { method: "POST" }),
      ));
    out.deletePostIgnore = await attempt(async () =>
      typeof await cache.delete(
        new Request("https://cache.test/del", { method: "POST" }),
        { ignoreMethod: true },
      ));
    return report(out);
  },

  async "/open"() {
    const out = {};
    const named = await caches.open("probe");
    const def = await caches.open("default");
    out.sameAsDefault = named === caches.default;
    out.defaultOpenSame = def === caches.default;
    await named.put(
      new Request("https://cache.test/open"),
      new Response("n", { headers: { ...CT, "cache-control": "max-age=60" } }),
    );
    out.namedHit = (await named.match(new Request("https://cache.test/open")))
      === undefined ? "miss" : "hit";
    out.defaultMiss = (await caches.default.match(
      new Request("https://cache.test/open"),
    )) === undefined ? "miss" : "hit";
    out.openAgainSame = (await caches.open("probe")) === named;
    return report(out);
  },

  // put consumes the body; each match builds a fresh response.
  async "/put-consumes"() {
    const cache = caches.default;
    const response = new Response("consumed", {
      headers: { ...CT, "cache-control": "max-age=60" },
    });
    const put = await attempt(() =>
      cache.put(new Request("https://cache.test/consume"), response));
    return report({
      put,
      bodyUsed: response.bodyUsed,
      match: await attempt(async () => {
        const found = await cache.match(new Request("https://cache.test/consume"));
        return found === undefined ? "miss" : await found.text();
      }),
    });
  },

  async "/match-twice"() {
    const cache = caches.default;
    const key = new Request("https://cache.test/twice");
    await cache.put(
      key,
      new Response("twice", {
        headers: {
          ...CT,
          "cache-control": "max-age=60",
          "cache-tag": "t1",
        },
      }),
    );
    const a = await cache.match(key);
    const b = await cache.match(key);
    return report({
      sameObject: a === b,
      aText: await a.text(),
      bText: await b.text(),
      status: a.status,
    });
  },

  // The reason phrase is part of the stored response.
  async "/status-text"() {
    const cache = caches.default;
    const out = {};
    // Only statuses upstream's Cache stores: an uncacheable status is a
    // documented Cello difference, not a reason phrase question.
    for (const [name, status, phrase] of [
      ["plain404", 404, ""],
      ["fishing", 404, "Gone Fishing"],
      ["empty200", 200, ""],
    ]) {
      const url = `https://cache.test/status/${name}`;
      await cache.put(
        new Request(url),
        new Response("d", {
          status,
          statusText: phrase,
          headers: { ...CT, "cache-control": "max-age=60" },
        }),
      );
      const found = await cache.match(new Request(url));
      out[name] = found === undefined
        ? "miss"
        : { status: found.status, statusText: found.statusText };
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
