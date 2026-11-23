// Fetch API object-model conformance fixture. Every route answers 200 with a
// JSON transcript of what the runtime observed; the test asserts the values
// workerd 1.20261121.1 produces for the same code (see
// scripts/workerd-diff/corpus/fetchapi, which gates them against workerd).
//
// `__ORIGIN_PORT__` is replaced by the test with a loopback helper origin the
// same thread answers, so `fetch()` has somewhere to go that is not this
// Worker.

const HELPER = `http://127.0.0.1:__ORIGIN_PORT__`;
const tag = (value) => Object.prototype.toString.call(value);
const settle = (fn) => {
  try {
    return { ok: fn() };
  } catch (error) {
    return { threw: { name: error?.name ?? null, message: error?.message ?? null } };
  }
};
const settleAsync = async (fn) => {
  try {
    return { ok: await fn() };
  } catch (error) {
    return { threw: { name: error?.name ?? null, message: error?.message ?? null } };
  }
};
const report = (value) =>
  new Response(JSON.stringify(value), { headers: { "content-type": "application/json" } });

const routes = {
  // #159: a string body carries text/plain;charset=UTF-8, bytes carry nothing,
  // an explicit header wins. #186: blob().type is the serialized MIME type.
  "/body-defaults": async () => ({
    string: [...new Response("hello").headers],
    bytes: [...new Response(new Uint8Array([104, 105])).headers],
    number: [...new Response(42).headers],
    explicit: [...new Response("x", { headers: { "content-type": "text/html" } }).headers],
    json: [...Response.json({ a: 1 }).headers],
    requestString: [...new Request("https://e.com/", { method: "POST", body: "hi" }).headers],
    requestBytes: [...new Request("https://e.com/", { method: "POST", body: new Uint8Array([104]) }).headers],
    blobTypes: {
      spaced: (await new Response("x", { headers: { "content-type": "text/plain; charset=utf-8" } }).blob()).type,
      upper: (await new Response("x", { headers: { "content-type": "Text/Plain; Charset=UTF-8" } }).blob()).type,
      boundary: (await new Response("x", { headers: { "content-type": "text/plain; charset=utf-8; boundary=abc" } }).blob()).type,
      bare: (await new Response("hello").blob()).type,
      invalid: (await new Response("x", { headers: { "content-type": "not a type" } }).blob()).type,
    },
    text: await new Response("hello").text(),
  }),

  // #163: status range and integer conversion. #165: a null-body status with
  // a body. #167: CR/LF in statusText.
  "/response-status": () => ({
    status: {
      0: settle(() => new Response(null, { status: 0 }).status),
      99: settle(() => new Response(null, { status: 99 }).status),
      101: settle(() => new Response(null, { status: 101 }).status),
      200: settle(() => new Response(null, { status: 200 }).status),
      201.5: settle(() => new Response(null, { status: 201.5 }).status),
      299: settle(() => new Response(null, { status: 299 }).status),
      600: settle(() => new Response(null, { status: 600 }).status),
      string: settle(() => new Response(null, { status: "200" }).status),
      junk: settle(() => new Response(null, { status: "abc" }).status),
    },
    nullBody: {
      "204-string": settle(() => new Response("x", { status: 204 }).status),
      "204-empty": settle(() => new Response("", { status: 204 }).status),
      "204-null": settle(() => new Response(null, { status: 204 }).status),
      "205-string": settle(() => new Response("x", { status: 205 }).status),
      "304-stream": settle(() => new Response(new ReadableStream(), { status: 304 }).status),
      "304-null": settle(() => new Response(null, { status: 304 }).status),
    },
    statusText: {
      newline: settle(() => new Response(null, { statusText: "a\nb" }).statusText),
      cr: settle(() => new Response(null, { statusText: "a\rb" }).statusText),
      tab: settle(() => new Response(null, { statusText: "a\tb" }).statusText),
      plain: settle(() => new Response(null, { statusText: "Made It" }).statusText),
      unicode: settle(() => new Response(null, { statusText: "h\u00e9llo" }).statusText),
    },
    // The runtime's own WebSocket upgrade path builds a 101 with a socket.
    plainWebSocket: (() => {
      const pair = new WebSocketPair();
      pair[1].accept();
      return {
        status: new Response(null, { status: 101, webSocket: pair[1] }).status,
        body: new Response(null, { status: 101, webSocket: pair[1] }).body,
        mismatch: settle(() => new Response("x", { status: 204, webSocket: pair[1] }).status),
      };
    })(),
  }),

  // #160 cache modes, #175 relative URL, #176 GET/HEAD body, #177 defaults,
  // #178 signal/integrity, #184 redirect value, #190 TRACE.
  "/request-options": () => ({
    cache: {
      "no-store": settle(() => new Request("https://e.com/", { cache: "no-store" }).cache),
      "no-cache": settle(() => new Request("https://e.com/", { cache: "no-cache" }).cache),
      default: settle(() => new Request("https://e.com/", { cache: "default" }).cache),
      bogus: settle(() => new Request("https://e.com/", { cache: "bogus" }).cache),
    },
    url: {
      relative: settle(() => new Request("/x").url),
      junk: settle(() => new Request("not a url").url),
      normal: settle(() => new Request("HTTPS://EXAMPLE.COM/a b").url),
    },
    body: {
      get: settle(() => new Request("https://e.com/", { method: "GET", body: "x" }).method),
      head: settle(() => new Request("https://e.com/", { method: "HEAD", body: "x" }).method),
      post: settle(() => new Request("https://e.com/", { method: "POST", body: "x" }).method),
    },
    defaults: (() => {
      const request = new Request("https://e.com/");
      return {
        integrity: request.integrity,
        keepalive: request.keepalive,
        cache: request.cache === undefined ? "<undefined>" : request.cache,
      };
    })(),
    signal: {
      junk: settle(() => new Request("https://e.com/", { signal: {} }).signal.aborted),
      null: settle(() => new Request("https://e.com/", { signal: null }).signal instanceof AbortSignal),
    },
    integrity: {
      value: settle(() => new Request("https://e.com/", { integrity: "sha256-x" }).integrity),
      empty: settle(() => new Request("https://e.com/", { integrity: "" }).integrity),
    },
    redirect: {
      error: settle(() => new Request("https://e.com/", { redirect: "error" }).redirect),
      upper: settle(() => new Request("https://e.com/", { redirect: "FOLLOW" }).redirect),
      bogus: settle(() => new Request("https://e.com/", { redirect: "bogus" }).redirect),
    },
    method: {
      trace: settle(() => new Request("https://e.com/", { method: "trace" }).method),
      TRACE: settle(() => new Request("https://e.com/", { method: "TRACE" }).method),
      connect: settle(() => new Request("https://e.com/", { method: "connect" }).method),
    },
  }),

  // #172 the default abort reason; #184 AbortSignal.any's element check.
  "/abort": () => {
    const controller = new AbortController();
    controller.abort();
    return {
      name: controller.signal.reason.name,
      message: controller.signal.reason.message,
      thrown: settle(() => controller.signal.throwIfAborted()),
      anyElement: settle(() => AbortSignal.any([{}]).aborted),
      anyElementIndex: settle(() => AbortSignal.any([new AbortController().signal, {}]).aborted),
      anyNotArray: settle(() => AbortSignal.any(7).aborted),
    };
  },

  // #184 URL parse message and the read-only searchParams.
  "/url": () => ({
    invalidPort: settle(() => new URL("https://example.com:99999/").href),
    invalid: settle(() => new URL("not a url").href),
    relative: settle(() => new URL("/x").href),
    searchParams: settle(() => {
      const url = new URL("https://e.com/");
      url.searchParams = "?m=2";
      return url.href;
    }),
  }),

  // #179 File lastModified; #188 a Blob entry's name.
  "/file-formdata": () => {
    const form = new FormData();
    form.append("blobNoName", new Blob(["q"]));
    form.append("withName", new Blob(["q"]), "explicit.bin");
    form.append("file", new File(["q"], "own.txt"));
    form.set("setKey", new Blob(["q"]));
    return {
      lastModified: {
        infinity: settle(() => new File([], "a", { lastModified: Infinity }).lastModified),
        nan: settle(() => new File([], "b", { lastModified: NaN }).lastModified),
        negative: settle(() => new File([], "c", { lastModified: -1 }).lastModified),
      },
      names: {
        blob: form.get("blobNoName").name,
        explicit: form.get("withName").name,
        file: form.get("file").name,
        set: form.get("setKey").name,
      },
    };
  },

  // #182 header validation messages and the names workerd accepts.
  "/headers": () => {
    const headers = new Headers();
    return {
      appendSpace: settle(() => headers.append("a b", "v")),
      valueNewline: settle(() => headers.append("a", "line\ninject")),
      valueNul: settle(() => headers.set("c", "x\u0000y")),
      emptyName: settle(() => {
        const own = new Headers();
        own.append("", "v");
        return [...own];
      }),
      unicodeName: settle(() => {
        const own = new Headers();
        own.append("\u2713", "v");
        return [...own];
      }),
      sequenceEntry: settle(() => new Headers([["a"]])),
      primitiveInit: settle(() => new Headers(7)),
      stringInit: settle(() => new Headers("a: b")),
      functionInit: settle(() => [...new Headers(() => {})]),
      after: [...headers],
    };
  },

  // #161 an incoming request's redirect mode and #169 its immutable headers.
  "/incoming": (request) => ({
    redirect: request.redirect,
    copyRedirect: new Request("https://diff.test/", request).redirect,
    headers: {
      set: settle(() => request.headers.set("x-added", "1")),
      append: settle(() => request.headers.append("x-added", "1")),
      delete: settle(() => request.headers.delete("x-added")),
      after: request.headers.get("x-added"),
      kept: request.headers.get("x-sent"),
    },
    // The copied request's header list is mutable.
    copyMutable: settle(() => {
      const copy = new Request(request);
      copy.headers.set("x-copied", "1");
      return copy.headers.get("x-copied");
    }),
    bodylessGet: {
      // #176 must not refuse the host's own GET request, which carries an
      // empty body on the wire.
      clone: settle(() => new Request(request).method),
    },
  }),

  // #173 a followed redirect reports its final URL; #161 the same through a
  // request copy; #180 content-length survives decompression.
  "/fetch": async (request) => {
    const follow = await fetch(`${HELPER}/redirect?code=302&to=/target`);
    const manual = await fetch(`${HELPER}/redirect?code=302&to=/target`, { redirect: "manual" });
    const proxied = await fetch(new Request(`${HELPER}/redirect?code=302&to=/target`, request));
    const gzip = await fetch(`${HELPER}/gzip`);
    const echo = await fetch(`${HELPER}/echo`, { method: "POST", body: "payload" });
    const echoed = await echo.json();
    return {
      follow: {
        status: follow.status,
        redirected: follow.redirected,
        url: new URL(follow.url).pathname,
        body: await follow.text(),
      },
      manual: {
        status: manual.status,
        redirected: manual.redirected,
        location: manual.headers.get("location"),
      },
      proxied: {
        status: proxied.status,
        redirected: proxied.redirected,
        // The incoming request's `manual` mode is what the copy carries.
        redirectMode: new Request(`${HELPER}/x`, request).redirect,
      },
      gzip: {
        encoding: gzip.headers.get("content-encoding"),
        length: gzip.headers.get("content-length"),
        body: await gzip.text(),
      },
      echo: {
        method: echoed.method,
        contentType: echoed.headers["content-type"],
      },
      // #178: the fetch init's signal and integrity go through the same
      // Request validation as `new Request`.
      fetchInit: {
        cache: await settleAsync(async () => {
          const response = await fetch(`${HELPER}/target`, { cache: "no-store" });
          return response.status;
        }),
        integrity: await settleAsync(async () => {
          await fetch(`${HELPER}/target`, { integrity: "sha256-x" });
          return "resolved";
        }),
        redirectError: await settleAsync(async () => {
          await fetch(`${HELPER}/target`, { redirect: "error" });
          return "resolved";
        }),
      },
    };
  },
};

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const route = routes[url.pathname];
    if (!route) return report({ unknownPath: url.pathname });
    try {
      const result = await route(request);
      return result instanceof Response ? result : report(result);
    } catch (error) {
      return report({ uncaught: { name: error?.name ?? null, message: error?.message ?? null } });
    }
  },
};
