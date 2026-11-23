// Fetch API object-model probes. Every route returns a 200 JSON transcript of
// what the runtime observed, so the differential harness can compare them.
import { DurableObject } from "cloudflare:workers";


const text = (body) => new Response(body, { headers: { "content-type": "text/plain; charset=utf-8" } });
const tag = (value) => Object.prototype.toString.call(value);
const replacer = (_key, value) => {
  if (value === undefined) return "<undefined>";
  if (typeof value === "function") return "<function>";
  if (typeof value === "symbol") return String(value);
  if (typeof value === "bigint") return `${value}n`;
  return value;
};
const report = (value) => text(JSON.stringify(value, replacer));

const errInfo = (error) => ({
  name: error?.name ?? null,
  message: error?.message ?? null,
  tag: error && typeof error === "object" ? tag(error) : null,
  typeof: typeof error,
});

const settle = (fn) => {
  try {
    return { ok: fn() };
  } catch (error) {
    return { threw: errInfo(error) };
  }
};

const fileInfo = (value) =>
  typeof value === "string"
    ? value
    : { name: value.name, type: value.type, size: value.size, lastModifiedPositive: value.lastModified > 0, tag: tag(value) };

/** Runs a table of label -> thunk, recording each result or thrown error. */
const attempts = (table) => {
  const out = {};
  for (const [label, fn] of Object.entries(table)) out[label] = settle(fn);
  return out;
};

const routes = {
  // ---------------------------------------------------------------- Headers
  "/headers/basics": () => {
    const h = new Headers();
    h.append("X-B", "1");
    h.append("x-b", "2");
    h.set("A", "3");
    h.append("a", "4");
    h.append("Empty", "");
    return {
      entries: [...h],
      keys: [...h.keys()],
      values: [...h.values()],
      get: h.get("X-B"),
      getA: h.get("a"),
      getMissing: h.get("nope"),
      has: [h.has("A"), h.has("a"), h.has("missing")],
      afterDelete: (() => {
        h.delete("A");
        return { entries: [...h], has: h.has("a") };
      })(),
      tag: tag(h),
      proto: Object.getPrototypeOf(h) === Headers.prototype,
    };
  },

  "/headers/validate": () => {
    const h = new Headers();
    return attempts({
      "append empty name": () => h.append("", "v"),
      "append space in name": () => h.append("a b", "v"),
      "append invalid char name": () => h.append("a()", "v"),
      "append CRLF value": () => h.append("a", "line\r\ninject"),
      "append LF only value": () => h.append("a", "line\ninject"),
      "append tab value": () => h.append("a", "tab\there"),
      "append non-ascii value": () => h.append("a", "héllo"),
      "append number name": () => h.append(1, "v"),
      "append null value": () => h.append("n", null),
      "get invalid name": () => h.get("a b"),
      "delete invalid name": () => h.delete("a b"),
      "set NUL value": () => h.set("c", "x\u0000y"),
      "ctor array bad entry": () => new Headers([["a"]]) && "made",
      "ctor record undefined": () => new Headers({ a: undefined }).get("a"),
      "ctor array of 3": () => new Headers([["a", "1", "2"]]).get("a"),
      "ctor number": () => new Headers(7).get("a"),
      "ctor sequence record": () => new Headers({ "X-A": "1", "x-b": "2" }).get("x-a"),
      "ctor empty name": () => new Headers({ "": "v" }) && "made",
      "after all": () => [...h],
    });
  },

  "/headers/set-cookie": () => {
    const h = new Headers();
    h.append("Set-Cookie", "a=1; Path=/");
    h.append("set-cookie", "b=2; HttpOnly");
    h.set("X", "y");
    return {
      get: h.get("set-cookie"),
      getSetCookie: typeof h.getSetCookie === "function" ? h.getSetCookie() : "<missing>",
      entries: [...h],
      combined: [...h].find(([name]) => name === "set-cookie")?.[1],
    };
  },

  "/headers/immutable-redirect": () => {
    const r = Response.redirect("https://example.com/", 302);
    const out = { status: r.status, headers: [...r.headers], location: r.headers.get("location"), type: r.type };
    Object.assign(
      out,
      attempts({
        set: () => r.headers.set("x", "1"),
        append: () => r.headers.append("location", "/next"),
        delete: () => r.headers.delete("location"),
      }),
    );
    return out;
  },

  "/headers/forbidden-request-names": () => {
    const out = {};
    for (const name of ["Host", "Connection", "Content-Length", "Cookie", "Origin", "Referer", "Proxy-Foo", "Sec-Foo", "X-Ok"]) {
      const r = new Request("https://example.com/");
      out[`request:${name}`] = settle(() => {
        r.headers.set(name, "v");
        return r.headers.get(name);
      });
    }
    for (const name of ["Host", "Connection", "Content-Length", "Cookie", "X-Ok"]) {
      const h = new Headers();
      out[`plain:${name}`] = settle(() => {
        h.set(name, "v");
        return h.get(name);
      });
    }
    return out;
  },

  "/headers/incoming-immutable": (request) => {
    const out = { headers: [...request.headers].filter(([name]) => name !== "host") };
    Object.assign(
      out,
      attempts({
        set: () => request.headers.set("x-added", "1"),
        append: () => request.headers.append("x-added", "1"),
        delete: () => request.headers.delete("x-added"),
      }),
    );
    out.after = [...request.headers].filter(([name]) => name !== "host");
    return out;
  },

  // ---------------------------------------------------------------- Request
  "/request/defaults": () => {
    const r = new Request("https://example.com/a/b?q=1");
    return {
      method: r.method,
      url: r.url,
      redirect: r.redirect,
      mode: r.mode,
      credentials: r.credentials,
      cache: r.cache,
      integrity: r.integrity,
      destination: r.destination,
      referrer: r.referrer,
      referrerPolicy: r.referrerPolicy,
      keepalive: r.keepalive,
      bodyUsed: r.bodyUsed,
      body: r.body,
      headers: [...r.headers],
      signalTag: tag(r.signal),
      signalAborted: r.signal.aborted,
      signalReason: r.signal.reason,
      cf: r.cf,
      duplex: r.duplex,
      tag: tag(r),
      proto: Object.getPrototypeOf(r) === Request.prototype,
      isRequest: r instanceof Request,
    };
  },

  "/request/method": () => {
    const out = {};
    for (const method of ["get", "post", "Patch", "DELETE", "options", "head"]) {
      out[`ok:${method}`] = settle(() => new Request("https://e.com/", { method }).method);
    }
    for (const method of ["CONNECT", "trace", "track", "", "GE T", "M-SEARCH"]) {
      out[`bad:${method}`] = settle(() => new Request("https://e.com/", { method }).method);
    }
    return out;
  },

  "/request/options": () => {
    const stream = () => new ReadableStream({ start: (c) => { c.enqueue(new TextEncoder().encode("x")); c.close(); } });
    return attempts({
      "redirect-invalid": () => new Request("https://e.com/", { redirect: "bogus" }).redirect,
      "mode-invalid": () => new Request("https://e.com/", { mode: "bogus" }).mode,
      "mode-navigate": () => new Request("https://e.com/", { mode: "navigate" }).mode,
      "mode-same-origin": () => new Request("https://e.com/", { mode: "same-origin" }).mode,
      "credentials-invalid": () => new Request("https://e.com/", { credentials: "bogus" }).credentials,
      "cache-invalid": () => new Request("https://e.com/", { cache: "bogus" }).cache,
      "cache-only-if-cached": () => new Request("https://e.com/", { cache: "only-if-cached" }).cache,
      "get-with-body": () => new Request("https://e.com/", { method: "GET", body: "x" }).method,
      "head-with-body": () => new Request("https://e.com/", { method: "HEAD", body: "x" }).method,
      "post-with-body": () => new Request("https://e.com/", { method: "POST", body: "x" }).method,
      "duplex-half": () => new Request("https://e.com/", { method: "POST", body: "x", duplex: "half" }).duplex,
      "duplex-full": () => new Request("https://e.com/", { method: "POST", body: "x", duplex: "full" }).duplex,
      "stream-no-duplex": () => new Request("https://e.com/", { method: "POST", body: stream() }).duplex,
      "stream-half": () => new Request("https://e.com/", { method: "POST", body: stream(), duplex: "half" }).duplex,
      "signal-not-signal": () => new Request("https://e.com/", { signal: {} }).signal,
      "invalid-url": () => new Request("not a url").url,
      "relative-url": () => new Request("/x").url,
      integrity: () => new Request("https://e.com/", { integrity: "sha256-x" }).integrity,
      // #169: `headers` is a readonly attribute.
      headersAssign: () => {
        const request = new Request("https://e.com/");
        request.headers = new Headers({ x: "1" });
        return "assigned";
      },
      referrer: () => new Request("https://e.com/", { referrer: "https://r.com/" }).referrer,
      "referrer-empty": () => new Request("https://e.com/", { referrer: "" }).referrer,
      "referrer-junk": () => new Request("https://e.com/", { referrer: "junk" }).referrer,
      cf: () => new Request("https://e.com/", { cf: { a: 1 } }).cf,
      "url-userinfo": () => new Request("https://user:pass@e.com/").url,
    });
  },

  "/request/signal-abort": () => {
    const controller = new AbortController();
    const r = new Request("https://e.com/", { signal: controller.signal });
    const before = r.signal.aborted;
    controller.abort("stop");
    return {
      before,
      after: r.signal.aborted,
      reason: r.signal.reason,
      sameSignal: r.signal === controller.signal,
      ownSignal: new Request("https://e.com/").signal === new Request("https://e.com/").signal,
    };
  },

  "/request/clone": async () => {
    const r = new Request("https://e.com/x", { method: "POST", body: "hello", headers: { "x-a": "1" } });
    const c = r.clone();
    return {
      url: c.url,
      method: c.method,
      headers: [...c.headers],
      bodyUsed: [r.bodyUsed, c.bodyUsed],
      texts: [await r.text(), await c.text()],
      bodyUsedAfter: [r.bodyUsed, c.bodyUsed],
      cloneAfterUse: settle(() => c.clone()),
    };
  },

  "/request/copy-constructor": async () => {
    const original = new Request("https://e.com/x", { method: "POST", body: "hello", headers: { "x-a": "1" } });
    const copy = new Request(original);
    const out = { url: copy.url, method: copy.method, headers: [...copy.headers], cf: copy.cf };
    out.texts = [await copy.text(), await original.text()];
    return out;
  },

  // --------------------------------------------------------------- Response
  "/response/defaults": () => {
    const r = new Response();
    return {
      status: r.status,
      statusText: r.statusText,
      type: r.type,
      url: r.url,
      redirected: r.redirected,
      headers: [...r.headers],
      headersMap: { ct: r.headers.get("content-type") },
      body: r.body,
      ok: r.ok,
      tag: tag(r),
      proto: Object.getPrototypeOf(r) === Response.prototype,
      isResponse: r instanceof Response,
      webSocket: r.webSocket,
    };
  },

  "/response/status": () => {
    const stream = new ReadableStream();
    return attempts({
      "99": () => new Response(null, { status: 99 }).status,
      "101": () => new Response(null, { status: 101 }).status,
      "199": () => new Response(null, { status: 199 }).status,
      "200": () => new Response(null, { status: 200 }).status,
      "299": () => new Response(null, { status: 299 }).status,
      "600": () => new Response(null, { status: 600 }).status,
      "0": () => new Response(null, { status: 0 }).status,
      "string": () => new Response(null, { status: "abc" }).status,
      "201.5": () => new Response(null, { status: 201.5 }).status,
      "statusText-newline": () => new Response(null, { status: 200, statusText: "a\nb" }).statusText,
      "statusText-cr": () => new Response(null, { status: 200, statusText: "a\rb" }).statusText,
      "statusText-unicode": () => new Response(null, { status: 200, statusText: "héllo" }).statusText,
      "statusText-plain": () => new Response(null, { status: 201, statusText: "Made It" }).statusText,
      "204-with-string": () => new Response("x", { status: 204 }).status,
      "204-with-null": () => new Response(null, { status: 204 }).status,
      "205-with-string": () => new Response("x", { status: 205 }).status,
      "304-with-string": () => new Response("x", { status: 304 }).status,
      "304-with-null": () => new Response(null, { status: 304 }).status,
      "304-with-stream": () => new Response(stream, { status: 304 }).status,
      "200-with-stream": () => new Response(stream, { status: 200 }).status,
      "ok-201": () => new Response(null, { status: 201 }).ok,
      "ok-404": () => new Response(null, { status: 404 }).ok,
    });
  },

  "/response/static": async () => {
    const out = {};
    const json = Response.json({ a: 1, b: [true, null] });
    out.json = { status: json.status, headers: [...json.headers], type: json.type, text: await json.text() };
    const custom = Response.json("hello", { headers: { "x-y": "z" }, status: 200 });
    out.jsonCustom = { headers: [...custom.headers], text: await custom.text() };
    out.jsonUndefined = settle(() => Response.json(undefined));
    const circular = {};
    circular.self = circular;
    out.jsonCircular = settle(() => Response.json(circular));
    const redirect = Response.redirect("https://example.com/path?a=1", 307);
    out.redirect = {
      status: redirect.status,
      type: redirect.type,
      headers: [...redirect.headers],
      url: redirect.url,
      redirected: redirect.redirected,
      body: redirect.body,
    };
    out.redirect301 = Response.redirect("https://example.com/", 301).status;
    out.redirect308 = Response.redirect("https://example.com/", 308).status;
    out.redirect200 = settle(() => Response.redirect("https://example.com/", 200));
    out.redirect302Default = settle(() => Response.redirect("https://example.com/").status);
    out.redirectBadUrl = settle(() => Response.redirect("not a url", 302));
    out.redirectRelative = settle(() => Response.redirect("/x", 302).headers.get("location"));
    const error = Response.error();
    out.error = {
      status: error.status,
      statusText: error.statusText,
      type: error.type,
      url: error.url,
      headers: [...error.headers],
      body: error.body,
      ok: error.ok,
      redirected: error.redirected,
    };
    return out;
  },

  "/response/webSocket": () => {
    const pair = new WebSocketPair();
    pair[1].accept();
    const response = new Response(null, { status: 101, webSocket: pair[1] });
    return {
      status: response.status,
      hasWebSocket: response.webSocket === pair[1],
      webSocketTag: tag(response.webSocket),
      type: response.type,
      body: response.body,
      plainWebSocket: new Response("x").webSocket,
    };
  },

  // ----------------------------------------------------------- Body mixins
  "/body/invalid-json": async () => {
    const out = {};
    const read = async (label, body) => {
      try {
        out[label] = await new Response(body).json();
      } catch (error) {
        out[label] = errInfo(error);
      }
    };
    await read("invalid", "not json");
    await read("empty", "");
    await read("truncated", '{"a":');
    await read("valid", '{"a":1}');
    await read("scalar", "42x");
    return out;
  },

  "/body/formdata-urlencoded": async () => {
    const res = new Response("a=1&b=hello+world&c=%26&a=2&empty=&d=%E2%9C%93", {
      headers: { "content-type": "application/x-www-form-urlencoded" },
    });
    const fd = await res.formData();
    return { entries: [...fd].map(([name, value]) => [name, fileInfo(value)]), a: fd.getAll("a") };
  },

  "/body/formdata-multipart": async () => {
    const fd = new FormData();
    fd.append("a", "1");
    fd.append("with space", "v");
    fd.append("f", new File(["file-bytes"], "x.txt", { type: "text/plain" }));
    fd.append("blob", new Blob(["blob-bytes"], { type: "application/octet-stream" }), "b.bin");
    const request = new Request("https://e.com/", { method: "POST", body: fd });
    const contentType = request.headers.get("content-type");
    const back = await new Response(await request.arrayBuffer(), { headers: { "content-type": contentType } }).formData();
    return {
      contentTypeKind: contentType.split(";")[0],
      hasBoundary: /boundary=/.test(contentType),
      entries: [...back].map(([name, value]) => [name, fileInfo(value)]),
      roundTrip: [await back.get("a"), await back.get("with space").text()],
    };
  },

  "/body/formdata-wrong-type": () => {
    return attempts({
      json: () => new Response("{}", { headers: { "content-type": "application/json" } }).formData(),
      missing: () => new Response("a=1").formData(),
      plain: () => new Response("a=1", { headers: { "content-type": "text/plain" } }).formData(),
    });
  },

  "/body/blob": async () => {
    const res = new Response("hello", { headers: { "content-type": "Text/Plain; charset=UTF-8" } });
    const blob = await res.blob();
    const bare = await new Response("hello").blob();
    const json = await new Response('{"a":1}', { headers: { "content-type": "application/json" } }).blob();
    return {
      type: blob.type,
      size: blob.size,
      text: await blob.text(),
      tag: tag(blob),
      bareType: bare.type,
      jsonType: json.type,
    };
  },

  "/body/buffers": async () => {
    const one = await new Response("abc").arrayBuffer();
    const two = new Response("abc");
    const bytesValue = typeof two.bytes === "function" ? await two.bytes() : "<missing>";
    return {
      arrayBufferTag: tag(one),
      arrayBufferBytes: one.byteLength,
      bytesTag: tag(bytesValue),
      bytesLength: bytesValue === "<missing>" ? null : bytesValue.length,
      bytesCtor: bytesValue === "<missing>" ? null : bytesValue.constructor?.name ?? null,
      bytesIsUint8: bytesValue === "<missing>" ? null : bytesValue instanceof Uint8Array,
      textEmpty: await new Response("").text(),
      textCrlf: await new Response("a\r\nb").text(),
    };
  },

  "/body/used": async () => {
    const res = new Response("abc");
    const first = await res.text();
    return {
      first,
      second: await res.text().then((v) => ({ ok: v }), (e) => errInfo(e)),
      clone: settle(() => res.clone()),
    };
  },

  // ------------------------------------------------------------------- URL
  "/url/parse": () => {
    const inputs = [
      "https://example.com",
      "https://example.com:443/",
      "HTTP://EXAMPLE.COM/A",
      "https://example.com/a/../b/./c",
      "https://user:pass@example.com/",
      "https://example.com/?",
      "https://example.com/#",
      "https://example.com/path?q=a b",
      "file:///tmp/x",
      "https://[::1]:8080/x",
      "https://example.com/ü",
      "https://example.com:99999/",
      "https:example.com/x",
      "//example.com/x",
      "https://example.com/%7euser",
      "https://example.com/?a=1&a=2#frag",
    ];
    const out = {};
    for (const input of inputs) {
      try {
        const u = new URL(input, "https://base.test/dir/");
        out[input] = {
          href: u.href,
          protocol: u.protocol,
          host: u.host,
          hostname: u.hostname,
          port: u.port,
          pathname: u.pathname,
          search: u.search,
          hash: u.hash,
          origin: u.origin,
          username: u.username,
          password: u.password,
          tag: tag(u),
        };
      } catch (error) {
        out[input] = errInfo(error);
      }
    }
    return out;
  },

  "/url/canparse": () => {
    const out = {};
    for (const input of ["https://a", "not a url", "/rel", "https://exa mple.com", ""]) {
      out[input] = settle(() => ({ can: URL.canParse(input), canBase: URL.canParse(input, "https://base.com/") }));
    }
    out.parse = {
      ok: settle(() => URL.parse("https://a/b")?.href),
      bad: settle(() => URL.parse("nope")),
      rel: settle(() => URL.parse("/x", "https://base.com/dir/")?.href),
      relBad: settle(() => URL.parse("/x")),
    };
    return out;
  },

  "/url/searchparams": () => {
    const u = new URL("https://e.com/p?a=1&b=2&a=3");
    const sp = u.searchParams;
    const out = { initial: [...sp], str: sp.toString(), href: u.href, tag: tag(sp) };
    sp.append("c", "x y");
    sp.set("a", "9");
    sp.delete("b");
    out.after = { entries: [...sp], str: sp.toString(), href: u.href };
    sp.sort();
    out.sorted = { str: sp.toString(), href: u.href };
    u.search = "?z=1";
    out.reassigned = [...sp];
    u.searchParams = "?m=2";
    out.assignedParams = [...sp];
    return out;
  },

  "/url/idna": () => {
    const out = {};
    for (const input of ["\u2603.com", "xn--n3h.com", "\u00d6BB.at", "fa\u00df.de", "127.0.0.1", "0x7f.1", "EXAMPLE.COM"]) {
      out[input] = settle(() => {
        const u = new URL(`https://${input}/`);
        return { hostname: u.hostname, href: u.href };
      });
    }
    return out;
  },

  "/url/relative": () => {
    const out = {};
    for (const [input, base] of [
      ["../x", "https://e.com/a/b/c"],
      ["?q=2", "https://e.com/a?q=1"],
      ["#f", "https://e.com/a?q=1"],
      ["//other.com/x", "https://e.com/"],
      ["x", "https://e.com/a/b"],
      ["", "https://e.com/a/b"],
    ]) {
      out[`${input}@${base}`] = settle(() => new URL(input, base).href);
    }
    return out;
  },

  // ------------------------------------------------- FormData / Blob / File
  "/formdata/basics": async () => {
    const fd = new FormData();
    fd.append("a", "1");
    fd.append("a", "2");
    fd.append("f", new File(["x"], "x.txt", { type: "text/plain" }));
    fd.set("a", "3");
    fd.append("b", new Blob(["zz"], { type: "application/octet-stream" }), "b.bin");
    const out = {
      entries: [...fd].map(([name, value]) => [name, fileInfo(value)]),
      has: [fd.has("a"), fd.has("f"), fd.has("b")],
      getAll: fd.getAll("a"),
      tag: tag(fd),
      proto: Object.getPrototypeOf(fd) === FormData.prototype,
      emptyFileName: null,
    };
    fd.delete("f");
    out.afterDelete = [...fd.keys()];
    fd.append("n", 123);
    out.numberAsString = fd.get("n");
    fd.append("blobNoName", new Blob(["q"]));
    out.anonymousBlobName = fileInfo(fd.get("blobNoName")).name;
    const implicit = new FormData();
    implicit.append("x", new Blob());

    // A form element is not available; a plain record is not accepted.
    out.fromRecord = settle(() => new FormData({ a: "1" }));
    return out;
  },

  "/blob/slice": async () => {
    const b = new Blob(["abcdef"], { type: "TEXT/Plain; charset=utf-8" });
    const s = b.slice(1, 4, "IMAGE/PNG");
    return {
      type: b.type,
      size: b.size,
      slice: { type: s.type, size: s.size, text: await s.text(), tag: tag(s) },
      negSlice: { text: await b.slice(-2).text(), size: b.slice(-2).size },
      bigSlice: { size: b.slice(2, 100).size, text: await b.slice(2, 100).text() },
      emptySlice: { size: b.slice(3, 3).size, type: b.slice(3, 3, "x/y;z").type },
      stream: await new Response(b.stream()).text(),
      arrayBuffer: (await b.arrayBuffer()).byteLength,
      text: await b.text(),
      badType: new Blob([], { type: "t\u00e9xt/plain" }).type,
      plainType: new Blob([], { type: "text/plain; charset=utf-8" }).type,
    };
  },

  "/blob/from-parts": async () => {
    const inner = new Blob(["xy"], { type: "text/inner" });
    const combined = new Blob(["a", inner, new Uint8Array([0x62]), "\u00e9"], { type: "application/x-test" });
    return {
      size: combined.size,
      type: combined.type,
      text: await combined.text(),
      bytes: [...new Uint8Array(await combined.arrayBuffer())],
      json: new Blob(["{bad"]).type,
    };
  },

  "/file/basics": async () => {
    const f = new File(["hi"], "a.txt", { type: "text/plain" });
    const f2 = new File([], "b.txt", { lastModified: 0 });
    return {
      name: f.name,
      size: f.size,
      type: f.type,
      lastModifiedPositive: f.lastModified > 0,
      lastModifiedZero: f2.lastModified,
      text: await f.text(),
      tag: tag(f),
      isBlob: f instanceof Blob,
      isFile: f instanceof File,
      emptyName: new File([], "").name,
      infinity: settle(() => new File([], "c.txt", { lastModified: Infinity }).lastModified),
      nan: settle(() => new File([], "d.txt", { lastModified: NaN }).lastModified),
      negative: settle(() => new File([], "e.txt", { lastModified: -1 }).lastModified),
    };
  },

  // ------------------------------------------------- AbortController/Signal
  "/abort/defaults": () => {
    const controller = new AbortController();
    const before = { aborted: controller.signal.aborted, reason: controller.signal.reason, tag: tag(controller.signal) };
    controller.abort();
    const reason = controller.signal.reason;
    const out = {
      before,
      aborted: controller.signal.aborted,
      reason: { name: reason?.name, message: reason?.message, tag: tag(reason), isDOMException: reason instanceof DOMException },
      abortAgain: (() => {
        controller.abort("ignored");
        return { aborted: controller.signal.aborted, reasonName: controller.signal.reason?.name };
      })(),
      throwIfAborted: settle(() => controller.signal.throwIfAborted()),
      freshThrow: settle(() => new AbortController().signal.throwIfAborted()),
    };
    const custom = new AbortController();
    custom.abort("custom");
    out.custom = custom.signal.reason;
    const number = new AbortController();
    number.abort(42);
    out.number = number.signal.reason;
    const undef = new AbortController();
    undef.abort(undefined);
    out.undefinedReason = { name: undef.signal.reason?.name, tag: tag(undef.signal.reason) };
    return out;
  },

  "/abort/static": async () => {
    const out = {};
    const timeout = AbortSignal.timeout(0);
    await new Promise((resolve) => (timeout.aborted ? resolve() : timeout.addEventListener("abort", resolve, { once: true })));
    out.timeout = { aborted: timeout.aborted, reason: { name: timeout.reason?.name, message: timeout.reason?.message, tag: tag(timeout.reason) } };
    const c1 = new AbortController();
    const c2 = new AbortController();
    const any = AbortSignal.any([c1.signal, c2.signal]);
    out.anyBefore = any.aborted;
    c2.abort("second");
    out.any = { aborted: any.aborted, reason: any.reason };
    out.anyEmpty = settle(() => AbortSignal.any([]).aborted);
    const c3 = new AbortController();
    c3.abort("third");
    const any2 = AbortSignal.any([c3.signal, new AbortController().signal]);
    out.anyAlreadyAborted = { aborted: any2.aborted, reason: any2.reason };
    out.anyBadElement = settle(() => AbortSignal.any([{}]));
    return out;
  },

  // ----------------------------------------------------------------- fetch
  "/fetch/self": async (_request, _url, env) => {
    const base = env.HELPER;
    const response = await fetch(`${base}/target`);
    return {
      status: response.status,
      ok: response.ok,
      type: response.type,
      path: new URL(response.url).pathname,
      redirected: response.redirected,
      contentType: response.headers.get("content-type"),
      helperHeader: response.headers.get("x-helper"),
      text: await response.text(),
    };
  },

  "/fetch/redirect-modes": async (_request, _url, env) => {
    const base = env.HELPER;
    const follow = await fetch(`${base}/redirect?code=302&to=/target`);
    const manual = await fetch(`${base}/redirect?code=302&to=/target`, { redirect: "manual" });
    const manualBody = await manual.text();
    const follow301 = await fetch(`${base}/redirect?code=301&to=/target`);
    return {
      follow: {
        status: follow.status,
        redirected: follow.redirected,
        path: new URL(follow.url).pathname,
        body: await follow.text(),
      },
      manual: {
        status: manual.status,
        type: manual.type,
        redirected: manual.redirected,
        location: manual.headers.get("location"),
        body: manualBody,
      },
      error: await fetch(`${base}/redirect?code=302&to=/target`, { redirect: "error" }).then(
        () => "resolved",
        (e) => errInfo(e),
      ),
      follow301: { status: follow301.status, path: new URL(follow301.url).pathname },
      bare: { status: (await fetch(`${base}/redirect?code=302&to=/target`, { redirect: "manual" })).status },
    };
  },

  "/fetch/cf-option": async (_request, _url, env) => {
    const base = env.HELPER;
    return {
      withCf: await fetch(`${base}/target`, { cf: { cacheTtl: 30 } }).then(
        async (r) => ({ status: r.status, text: await r.text() }),
        (e) => errInfo(e),
      ),
      withUnknownOption: await fetch(`${base}/target`, { bogusOption: 1 }).then(
        async (r) => ({ status: r.status }),
        (e) => errInfo(e),
      ),
    };
  },

  "/fetch/gzip": async (_request, _url, env) => {
    const base = env.HELPER;
    const response = await fetch(`${base}/gzip`);
    return {
      status: response.status,
      contentEncoding: response.headers.get("content-encoding"),
      contentLength: response.headers.get("content-length"),
      contentType: response.headers.get("content-type"),
      body: await response.text(),
    };
  },

  "/fetch/not-found": async (_request, _url, env) => {
    const base = env.HELPER;
    const response = await fetch(`${base}/not-found`);
    return { status: response.status, ok: response.ok, text: await response.text(), type: response.type };
  },

  "/fetch/echo": async (_request, _url, env) => {
    const base = env.HELPER;
    const response = await fetch(`${base}/echo`, { method: "POST", body: "payload", headers: { "x-sent": "one" } });
    const echoed = await response.json();
    const headers = echoed.headers;
    return {
      method: echoed.method,
      url: echoed.url,
      headers: Object.fromEntries(
        Object.entries(headers).filter(([name]) => !["host", "connection", "content-length", "user-agent", "accept", "accept-encoding", "accept-language", "content-type"].includes(name)),
      ),
      contentLength: headers["content-length"],
      contentType: headers["content-type"],
      bodyBytes: echoed.bodyBytes,
    };
  },

  // #165: a fetched 204/205/304 has no body at all (workerd's `body` is
  // null), so the dictionary guard must not see one.
  "/fetch/null-body": async (_request, _url, env) => {
    const base = env.HELPER;
    const one = async (path) => {
      const response = await fetch(`${base}${path}`);
      return {
        status: response.status,
        bodyIsNull: response.body === null,
        contentType: response.headers.get("content-type"),
        text: await response.text(),
        ok: response.ok,
      };
    };
    return {
      noContent: await one("/no-content"),
      resetContent: await one("/reset-content"),
      notModified: await one("/not-modified"),
    };
  },

  // #180: a forwarded response's `content-length` must not frame the new
  // subrequest body. The origin reports what it actually received.
  "/fetch/reframe": async (_request, _url, env) => {
    const base = env.HELPER;
    const compressed = await fetch(`${base}/gzip`);
    const text = await compressed.text();
    const echoed = await fetch(`${base}/echo`, {
      method: "POST",
      headers: compressed.headers,
      body: text,
    }).then((response) => response.json());
    return {
      // The response object keeps both headers (#180).
      responseEncoding: compressed.headers.get("content-encoding"),
      responseLength: compressed.headers.get("content-length"),
      // The origin frames the body the host sent, not the replayed length.
      sentLength: echoed.headers["content-length"],
      sentBytes: echoed.bodyBytes,
      body: text,
    };
  },

  // #182: a Worker may replay an odd header name; workerd drops what its
  // header layer cannot represent and keeps the response. A control byte in a
  // value is not compared here: workerd writes it raw, which is not a legal
  // field value and hyper refuses it, so cello drops the byte (pinned by
  // `fetch_api_conformance` instead).
  "/raw/odd-headers": () =>
    new Response("ok", {
      headers: {
        "": "v",
        "x-ok": "1",
        "\u2713": "v",
      },
    }),

  // -------------------------------------------------------------- incoming
  "/incoming/request": (request) => {
    const base = new URL(request.url).origin;
    return {
      url: request.url.replace(base, "<base>"),
      method: request.method,
      headers: [...request.headers].filter(([name]) => name !== "host"),
      mode: request.mode,
      redirect: request.redirect,
      credentials: request.credentials,
      cache: request.cache,
      destination: request.destination,
      referrer: request.referrer,
      referrerPolicy: request.referrerPolicy,
      signalAborted: request.signal.aborted,
      body: request.body,
      cfUndefined: request.cf === undefined,
      cfKeysSorted: request.cf ? Object.keys(request.cf).sort() : null,
      type: tag(request),
      urlHasTrailing: new URL(request.url).pathname,
    };
  },


  // ------------------------------------------------ string-body content type
  "/body/string-headers": async () => {
    const stringBody = new Response("hello");
    const bytesBody = new Response(new Uint8Array([104, 105]));
    const streamBody = new Response(new Blob(["hi"]).stream());
    const paramsBody = new Response(new URLSearchParams("a=1"));
    const requestString = new Request("https://e.com/", { method: "POST", body: "hello" });
    const requestBytes = new Request("https://e.com/", { method: "POST", body: new Uint8Array([104]) });
    const stringBlob = await new Response("hello").blob();
    const bytesBlob = await new Response(new Uint8Array([104])).blob();
    return {
      string: [...stringBody.headers],
      bytes: [...bytesBody.headers],
      stream: [...streamBody.headers],
      params: [...paramsBody.headers],
      requestString: [...requestString.headers],
      requestBytes: [...requestBytes.headers],
      stringBlobType: stringBlob.type,
      bytesBlobType: bytesBlob.type,
      stringText: await stringBody.text(),
    };
  },

  "/raw/string-response": () => new Response("hello"),
  "/raw/json-response": () => Response.json({ a: 1, b: [1, 2] }),
  "/raw/bytes-response": () => new Response(new Uint8Array([104, 105])),

  // ------------------------------------------- redirect mode propagation
  "/fetch/request-redirect-propagation": async (request, _url, env) => {
    const target = new Request(`${env.HELPER}/redirect?code=302&to=/target`, request);
    const response = await fetch(target);
    return {
      incomingRedirect: request.redirect,
      constructedRedirect: target.redirect,
      status: response.status,
      redirected: response.redirected,
      path: new URL(response.url).pathname,
      body: await response.text(),
    };
  },

  "/fetch/request-redirect-manual-override": async (request, _url, env) => {
    const manual = new Request(request, { redirect: "manual" });
    const response = await fetch(new Request(`${env.HELPER}/redirect?code=302&to=/target`, manual));
    return {
      redirect: manual.redirect,
      status: response.status,
      redirected: response.redirected,
      path: new URL(response.url).pathname,
    };
  },

  // --------------------------------------------------- object-model tags
  "/incoming/instanceof": (request) => ({
    isRequest: request instanceof Request,
    tag: tag(request),
    headersTag: tag(request.headers),
    signalTag: tag(request.signal),
    bodyTag: request.body === null ? null : tag(request.body),
    cfTag: tag(request.cf),
    urlTag: tag(new URL(request.url)),
    bodyUsed: request.bodyUsed,
    bodyIsNull: request.body === null,
  }),

  // -------------------------------------------------- request body mixins
  "/request/formdata-body": async (request) => {
    const form = await request.formData();
    return { entries: [...form].map(([name, value]) => [name, fileInfo(value)]) };
  },

  "/request/bytes-body": async (request) => {
    const bytes = await request.clone().bytes();
    const buffer = await request.arrayBuffer();
    return {
      bytes: [...bytes],
      arrayBuffer: [...new Uint8Array(buffer)],
      ctor: bytes.constructor?.name ?? null,
      isUint8: bytes instanceof Uint8Array,
      tag: tag(bytes),
    };
  },

  // -------------------------------------------------------- immutability
  "/response/error-immutable": () => {
    const error = Response.error();
    const json = Response.json({});
    const out = {
      errorHeaders: [...error.headers],
      jsonHeaders: [...json.headers],
      errorGuard: attempts({
        set: () => error.headers.set("x", "1"),
        append: () => error.headers.append("x", "1"),
        delete: () => error.headers.delete("content-type"),
      }),
      jsonGuard: attempts({ set: () => json.headers.set("x", "1") }),
    };
    return out;
  },

  "/request/cache-valid": () => {
    const out = {};
    for (const cache of ["no-store", "reload", "no-cache", "force-cache", "default"]) {
      out[cache] = settle(() => new Request("https://e.com/", { cache }).cache);
    }
    return out;
  },

  "/request/header-array-record": () => {
    const request = new Request("https://e.com/", { headers: { a: ["x", "y"] } });
    const headers = new Headers({ a: ["x", "y"] });
    return { request: [...request.headers], headers: [...headers] };
  },

  "/fetch/cache-option": async (_request, _url, env) => {
    const base = env.HELPER;
    const run = async (init) =>
      fetch(`${base}/target`, init).then(
        async (r) => ({ status: r.status, text: await r.text() }),
        (e) => errInfo(e),
      );
    return {
      noStore: await run({ cache: "no-store" }),
      bogus: await run({ cache: "bogus" }),
      integrity: await run({ integrity: "sha256-x" }),
      duplex: await run({ duplex: "full" }),
    };
  },

  "/request/null-body-read": async (request) => ({
    text: await request.text(),
    bodyUsed: request.bodyUsed,
    clonedFirst: request.clone() instanceof Request,
  }),

  "/request/body-used-incoming": async (request) => {
    const out = { initialBodyUsed: request.bodyUsed, bodyNull: request.body === null };
    out.cloneBefore = settle(() => request.clone() instanceof Request);
    out.text = await request.text();
    out.afterBodyUsed = request.bodyUsed;
    out.cloneAfter = settle(() => request.clone() instanceof Request);
    out.textAgain = await request.text().then((value) => ({ ok: value }), (e) => errInfo(e));
    return out;
  },

  "/raw/mixed-headers": () =>
    new Response("x", { headers: { "content-type": "Text/Plain; Charset=UTF-8", "x-weird": "MiXeD  Value" } }),

  "/body/blob-type-explicit": async () => {
    const a = await new Response("x", { headers: { "content-type": "text/plain;charset=UTF-8" } }).blob();
    const b = await new Response("x", { headers: { "content-type": "application/json" } }).blob();
    const c = await new Response("x", { headers: { "content-type": "text/plain; charset=utf-8; boundary=abc" } }).blob();
    return { a: a.type, b: b.type, c: c.type };
  },

  "/internals/own-keys": async () => {
    const response = new Response("x");
    const request = new Request("https://e.com/");
    const headers = new Headers({ a: "1" });
    const signal = new AbortController().signal;
    const form = new FormData();
    return {
      response: Object.keys(response),
      responseStringifyKeys: Object.keys(JSON.parse(JSON.stringify(response))),
      request: Object.keys(request),
      headers: Object.keys(headers),
      signal: Object.keys(signal),
      form: Object.keys(form),
      responseOwnNames: Object.getOwnPropertyNames(response).filter((name) => name !== "constructor"),
    };
  },

  "/issue/request-init-validation": () => {
    const out = {};
    const run = (label, fn) => {
      try { out[label] = { ok: fn() }; }
      catch (error) { out[label] = { name: error.name, message: error.message }; }
    };
    run("signal", () => new Request("https://diff.test/", { signal: {} }).signal.aborted);
    run("integrity", () => new Request("https://diff.test/", { integrity: "sha256-x" }).integrity);
    run("integrityEmpty", () => new Request("https://diff.test/", { integrity: "" }).integrity);
    return out;
  },

  "/issue/request-url-validation": () => {
    const out = {};
    for (const input of ["/x", "not a url", "example.com/x"]) {
      try { out[input] = { url: new Request(input).url }; }
      catch (error) { out[input] = { name: error.name, message: error.message }; }
    }
    return out;
  },

  "/issue/file-last-modified": () => {
    const out = {};
    for (const lastModified of [Infinity, NaN, -1, 0, 1795441252227]) {
      try { out[String(lastModified)] = { lastModified: new File([], "a", { lastModified }).lastModified }; }
      catch (error) { out[String(lastModified)] = { name: error.name, message: error.message }; }
    }
    return out;
  },

  "/issue/status-matrix": () => {
    const out = {};
    for (const status of [0, 99, 101, 199, 200, 201.5, 299, 600, "abc"]) {
      try { out[String(status)] = { status: new Response(null, { status }).status }; }
      catch (error) { out[String(status)] = { name: error.name, message: error.message }; }
    }
    return out;
  },

  "/issue/null-body-status": () => {
    const out = {};
    for (const [label, body, status] of [
      ["204-string", "x", 204], ["205-string", "x", 205], ["304-string", "x", 304],
      ["304-stream", new ReadableStream(), 304], ["204-null", null, 204], ["304-null", null, 304],
    ]) {
      try { out[label] = { status: new Response(body, { status }).status }; }
      catch (error) { out[label] = { name: error.name, message: error.message }; }
    }
    return out;
  },

  "/issue/status-text": () => {
    const out = {};
    for (const statusText of ["a\nb", "a\rb", "h\u00e9llo", "Made It"]) {
      try { out[JSON.stringify(statusText)] = { statusText: new Response(null, { status: 200, statusText }).statusText }; }
      catch (error) { out[JSON.stringify(statusText)] = { name: error.name, message: error.message }; }
    }
    return out;
  },

  "/issue/incoming-redirect": (request) => ({
    redirect: request.redirect,
    copy: new Request("https://diff.test/", request).redirect,
  }),

  "/issue/incoming-headers-mutation": (request) => {
    const out = {};
    const run = (label, fn) => {
      try { fn(); out[label] = "no-throw"; }
      catch (error) { out[label] = { name: error.name, message: error.message }; }
    };
    run("set", () => request.headers.set("x-added", "1"));
    run("append", () => request.headers.append("x-added", "1"));
    run("delete", () => request.headers.delete("x-added"));
    out.after = request.headers.get("x-added");
    return out;
  },

  "/issue/get-with-body": async () => {
    const out = {};
    for (const method of ["GET", "HEAD"]) {
      try {
        const request = new Request("https://diff.test/", { method, body: "x" });
        out[method] = { method: request.method, text: await request.text(), bodyIsNull: request.body === null };
      } catch (error) {
        out[method] = { name: error.name, message: error.message };
      }
    }
    return out;
  },

  "/issue/incoming-body": async (request) => {
    const out = {
      bodyIsNull: request.body === null,
      bodyTag: request.body === null ? null : tag(request.body),
      bodyUsedBefore: request.bodyUsed,
    };
    out.text = await request.text();
    out.bodyUsedAfter = request.bodyUsed;
    out.textAgain = await request.text().then((value) => ({ value }), (e) => ({ name: e.name, message: e.message }));
    return out;
  },

  "/issue/request-defaults-fields": () => {
    const request = new Request("https://diff.test/");
    return {
      integrity: request.integrity,
      integrityType: typeof request.integrity,
      keepalive: request.keepalive,
      keepaliveType: typeof request.keepalive,
    };
  },

  "/issue/method-matrix": () => {
    const out = {};
    for (const method of ["trace", "TRACE", "connect", "track", "CONNECT", "TRACK"]) {
      try { out[method] = { ok: new Request("https://diff.test/", { method }).method }; }
      catch (error) { out[method] = { name: error.name, message: error.message }; }
    }
    return out;
  },

  "/issue/blob-body-content-type": () => {
    const typed = new Response(new Blob(["x"], { type: "image/png" }));
    const plain = new Response(new Blob(["x"]));
    const file = new Response(new File(["x"], "a.txt", { type: "text/csv" }));
    const arrayBuffer = new Response(new Blob(["x"], { type: "image/png" }).stream());
    return {
      typed: [...typed.headers],
      plain: [...plain.headers],
      file: [...file.headers],
      streamOfTyped: [...arrayBuffer.headers],
    };
  },

  "/issue/header-latin1": () => {
    const attempt = (label, fn) => {
      try { return { label, ok: fn() }; }
      catch (error) { return { label, name: error.name, message: error.message }; }
    };
    return [
      attempt("checkmark value", () => { const h = new Headers(); h.append("a", "\u2713"); return h.get("a"); }),
      attempt("emoji value", () => { const h = new Headers(); h.append("a", "\u{1f600}"); return h.get("a"); }),
      attempt("checkmark name", () => { const h = new Headers(); h.append("\u2713", "v"); return [...h]; }),
      attempt("emoji in response", () => [...new Response("x", { headers: { a: "\u{1f600}" } }).headers]),
      attempt("latin1 boundary", () => { const h = new Headers(); h.append("a", "\u00ff\u0100"); return h.get("a"); }),
    ];
  },

  // A zero-length buffered body on a null-body status: workerd accepts it and
  // then reports no body at all (`body === null`) and infers no content-type.
  // The hop through a Durable Object proves the caller sees the same shape,
  // because the object's Response travels through the service boundary.
  "/issue/null-body-zero-length": async (request, url, env) => {
    const build = async (body, init = {}) => {
      try {
        const response = new Response(body, { status: 204, ...init });
        return {
          status: response.status,
          bodyIsNull: response.body === null,
          headers: [...response.headers],
          text: await response.text(),
          bodyUsed: response.bodyUsed,
        };
      } catch (error) {
        return { threw: error.name, message: error.message };
      }
    };
    const out = {
      emptyString: await build(""),
      emptyBytes: await build(new Uint8Array(0)),
      emptyBlob: await build(new Blob([])),
      explicitType: await build("", { headers: { "content-type": "application/custom" } }),
      emptyString304: await build("", { status: 304 }),
      nonEmpty: await build("x"),
    };
    const stub = env.EMPTY_BODY.get(env.EMPTY_BODY.idFromName("only"));
    const hop = await stub.fetch("https://do.test/empty");
    out.hop = {
      status: hop.status,
      bodyIsNull: hop.body === null,
      headers: [...hop.headers],
      text: await hop.text(),
    };
    return out;
  },

};

export class NullBodyObject extends DurableObject {
  async fetch() {
    return new Response("", { status: 204 });
  }
}


export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const route = routes[url.pathname];
    if (!route) return report({ unknownPath: url.pathname });
    try {
      const result = await route(request, url, env);
      return result instanceof Response ? result : report(result);
    } catch (error) {
      return report({ uncaught: errInfo(error) });
    }
  },
};
