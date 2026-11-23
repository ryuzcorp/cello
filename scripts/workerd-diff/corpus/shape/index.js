// The observable shape of every Web API object the runtime defines: own
// properties, prototype members and their enumerability, JSON serialization
// and Symbol.toStringTag. Workerd is the reference; a class that reports a
// different shape here leaks host internals to tenant code (#170, #206) or
// fails a cross-realm type check (#171).
//
// `/streams` also covers the whole stream family -- controllers, readers,
// BYOB requests and the transform/identity/compression subclasses -- plus
// the runtime values the ticket pins (a pending BYOB request's `atLeast`,
// the queuing strategies' `size` accessor, and `JSON.stringify` of every
// stream, which must not throw).
const report = (value) =>
  new Response(JSON.stringify(value, null, 0), {
    headers: { "content-type": "text/plain; charset=utf-8" },
  });

const attempt = (fn) => {
  try {
    return fn();
  } catch (error) {
    return { threw: { name: error?.name ?? String(error), message: String(error?.message ?? error) } };
  }
};

const describe = (target, key, kind) => {
  const descriptor = Object.getOwnPropertyDescriptor(target, key);
  if (descriptor === undefined) return `${key}:missing`;
  const enumerable = descriptor.enumerable ? "enum" : "hidden";
  // Writability is not part of the pinned shape: the ticket is about
  // enumerability, attribute kind and the interface tag, and a class's
  // non-writable `prototype` cannot be redefined in V8.
  return `${key}:${kind}:${enumerable}:${"value" in descriptor ? "data" : "accessor"}`;
};

// One string, not an array: the diff harness compares leaf values, and an
// array would report every later member as a separate divergence whenever
// one member is missing.
const members = (target) =>
  target === null || target === undefined
    ? null
    : Object.getOwnPropertyNames(target).map((key) => describe(target, key, "own"))
        .sort().join(" | ");

// The prototype's own members, one descriptor each. This is where workerd
// keeps every WebIDL attribute as an enumerable accessor (WebIDL makes
// prototype members enumerable) and a host-only helper method shows up as a
// divergence.
const level = (proto) => {
  if (proto === null || proto === undefined) return null;
  const out = Object.getOwnPropertyNames(proto)
    .map((key) => describe(proto, key, "proto")).sort().join(" | ");
  const tag = Object.getOwnPropertyDescriptor(proto, Symbol.toStringTag);
  return {
    members: out,
    toStringTag: tag === undefined
      ? null
      : { value: tag.value, enumerable: tag.enumerable, writable: tag.writable },
  };
};

// Every level of the prototype chain, so an intermediate interface level
// (workerd keeps the Body mixin between Request/Response and Object) is
// visible instead of hidden behind the first level.
const protoMembers = (value) => {
  const chain = [];
  let proto = typeof value === "function" ? value.prototype : Object.getPrototypeOf(value);
  while (proto !== null && proto !== Object.prototype) {
    chain.push(level(proto));
    proto = Object.getPrototypeOf(proto);
  }
  chain.push(level(Object.prototype));
  return chain;
};

const constructorOf = (value) => {
  const ctor = typeof value === "function" ? value : Object.getPrototypeOf(value)?.constructor;
  if (typeof ctor !== "function") return null;
  return {
    own: Object.getOwnPropertyNames(ctor)
      .map((key) => describe(ctor, key, "ctor")).sort().join(" | "),
    enumerable: Object.keys(ctor)
      .map((key) => `${key}:${describe(ctor, key, "ctor")}`).sort()
      .join(" | "),
  };
};

const shape = (value) => {
  if (value === null || (typeof value !== "object" && typeof value !== "function"))
    return { primitive: typeof value };
  // A stack string names the runtime's own frames, so only its presence is
  // comparable; DOMException's JSON shape is what the pin is about.
  const normalize = (key, entry) => (key === "stack" ? "<stack>" : entry);
  const json = attempt(() => JSON.stringify(value, normalize));
  return {
    tag: attempt(() => Object.prototype.toString.call(value)),
    keys: attempt(() => Object.keys(value)
      .map((key) => describe(value, key, "own")).sort().join(" | ")),
    own: attempt(() => members(value)),
    json,
    symbols: Object.getOwnPropertySymbols(value).map(String).join(" | "),
    ctor: attempt(() => constructorOf(value)),
    proto: attempt(() => protoMembers(value)),
  };
};

const shapes = (values) => {
  const out = {};
  for (const [name, value] of Object.entries(values)) out[name] = shape(value);
  return out;
};

const noop = () => {};

const routes = {
  "/fetch"() {
    const signal = new AbortController().signal;
    return report(shapes({
      Headers: new Headers({ "x-a": "1" }),
      Request: new Request("https://diff.test/"),
      RequestPost: new Request("https://diff.test/", { method: "POST", body: "abc" }),
      Response: new Response("x"),
      ResponseNull: new Response(null),
      FormData: new FormData(),
      AbortController: new AbortController(),
      AbortSignal: signal,
      AbortSignalAny: AbortSignal.any([signal]),
    }));
  },

  "/binary"() {
    return report(shapes({
      Blob: new Blob(["a"], { type: "text/plain" }),
      File: new File(["a"], "a.txt", { type: "text/plain" }),
      URL: new URL("https://diff.test/a?b=1"),
      URLSearchParams: new URLSearchParams("a=1&b=2"),
      TextEncoder: new TextEncoder(),
      TextDecoder: new TextDecoder("utf-8"),
    }));
  },

  "/streams"() {
    let defaultController;
    const readable = new ReadableStream({
      start(controller) { defaultController = controller; },
    });
    let byteController;
    const byteReadable = new ReadableStream({
      type: "bytes",
      start(controller) { byteController = controller; },
    });
    const byobReader = byteReadable.getReader({ mode: "byob" });
    // A pending BYOB read materializes the controller's byobRequest.
    const byobPending = byobReader.read(new Uint8Array(8));
    const byobRequest = byteController.byobRequest;
    let writableController;
    new WritableStream({
      start(controller) { writableController = controller; },
    });
    let transformController;
    new TransformStream({
      start(controller) { transformController = controller; },
    });
    // Every pending promise must settle or the isolate's unhandled-rejection
    // hook fires for the fixture's own objects; this route reports shapes,
    // so take the reads out of the transcript.
    byobPending.catch(noop);
    const fixed = new FixedLengthStream(3);
    const countStrategy = new CountQueuingStrategy({ highWaterMark: 1 });
    return report({
      values: {
        // The pinned runtime values, not just descriptors: a pending BYOB
        // request's remaining minimum, the strategies' `size` function, and
        // the JSON/tag shape of the internal transform streams.
        byobAtLeast: byteController.byobRequest.atLeast,
        byobView: byteController.byobRequest.view instanceof Uint8Array,
        byobViewLength: byteController.byobRequest.view.byteLength,
        countSizeType: typeof countStrategy.size,
        countSizeLength: countStrategy.size.length,
        countHwm: countStrategy.highWaterMark,
        byteLengthSizeType: typeof new ByteLengthQueuingStrategy({ highWaterMark: 1 }).size,
        fixedKeys: Object.keys(fixed).length,
        fixedJson: JSON.stringify(fixed),
        fixedTag: Object.prototype.toString.call(fixed),
        byobReaderClosed: typeof byobReader.closed,
      },
      json: {
        ReadableStream: attempt(() => JSON.stringify(readable)),
        WritableStream: attempt(() => JSON.stringify(new WritableStream())),
        TransformStream: attempt(() => JSON.stringify(new TransformStream())),
        IdentityTransformStream: attempt(() => JSON.stringify(new IdentityTransformStream())),
        FixedLengthStream: attempt(() => JSON.stringify(fixed)),
        CompressionStream: attempt(() => JSON.stringify(new CompressionStream("gzip"))),
        DigestStream: attempt(() => JSON.stringify(new crypto.DigestStream("SHA-256"))),
      },
      shapes: shapes({
        ReadableStream: readable,
        ReadableStreamDefaultController: defaultController,
        ReadableStreamReader: readable.getReader(),
        ReadableByteStreamController: byteController,
        ReadableStreamBYOBReader: byobReader,
        ReadableStreamBYOBRequest: byobRequest,
        WritableStream: new WritableStream(),
        WritableStreamDefaultController: writableController,
        WritableStreamWriter: new WritableStream().getWriter(),
        TransformStream: new TransformStream(),
        TransformStreamDefaultController: transformController,
        CountQueuingStrategy: new CountQueuingStrategy({ highWaterMark: 1 }),
        ByteLengthQueuingStrategy: new ByteLengthQueuingStrategy({ highWaterMark: 1 }),
        IdentityTransformStream: new IdentityTransformStream(),
        FixedLengthStream: fixed,
        CompressionStream: new CompressionStream("gzip"),
        DecompressionStream: new DecompressionStream("gzip"),
        TextEncoderStream: new TextEncoderStream(),
        TextDecoderStream: new TextDecoderStream(),
        DigestStream: new crypto.DigestStream("SHA-256"),
      }),
    });
  },

  "/events"() {
    return report(shapes({
      Event: new Event("x"),
      CustomEvent: new CustomEvent("x", { detail: { a: 1 } }),
      MessageEvent: new MessageEvent("x", { data: "d", origin: "o", lastEventId: "1" }),
      CloseEvent: new CloseEvent("x", { code: 1000, reason: "r", wasClean: true }),
      ErrorEvent: new ErrorEvent("x", { message: "m" }),
      DOMException: new DOMException("message", "AbortError"),
      MessageChannel: new MessageChannel(),
      MessagePort: new MessageChannel().port1,
    }));
  },

  async "/crypto"() {
    const pair = await crypto.subtle.generateKey(
      { name: "ECDSA", namedCurve: "P-256" }, true, ["sign", "verify"],
    );
    const aes = await crypto.subtle.generateKey(
      { name: "AES-GCM", length: 128 }, true, ["encrypt", "decrypt"],
    );
    return report(shapes({
      CryptoKeyPrivate: pair.privateKey,
      CryptoKeySecret: aes,
      SubtleCrypto: crypto.subtle,
    }));
  },

  async "/details"() {
    const request = new Request("https://diff.test/");
    const response = new Response(null);
    const reqProto = Object.getPrototypeOf(request);
    const resProto = Object.getPrototypeOf(response);
    const bodyProto = Object.getPrototypeOf(reqProto);
    // Structural facts only: accessor/method identity (a getter's name and
    // arity are engine-chosen) is not part of the pinned shape.
    const d = (target, key) => {
      const desc = Object.getOwnPropertyDescriptor(target, key);
      if (!desc) return null;
      return {
        enumerable: desc.enumerable, configurable: desc.configurable,
        kind: "value" in desc ? "data" : "accessor",
        hasGet: desc.get !== undefined, hasSet: desc.set !== undefined,
      };
    };
    const urlProbe = (fn) => {
      try {
        return { ok: fn() };
      } catch (error) {
        return { threw: String(error?.message ?? error) };
      }
    };
    const objectUrl = urlProbe(() =>
      URL.createObjectURL(new Blob(["hi"], { type: "text/plain" })));
    const revokeUrl = urlProbe(() => URL.revokeObjectURL("blob:x"));
    return report({
      requestCache: attempt(() => request.cache),
      requestIntegrity: attempt(() => request.integrity),
      requestKeepalive: attempt(() => request.keepalive),
      requestFetcher: attempt(() => request.fetcher),
      requestCf: attempt(() => request.cf),
      requestUrl: request.url,
      responseCf: attempt(() => response.cf),
      bodyProtoConstructor: bodyProto.constructor === Object ? "Object" : String(bodyProto.constructor?.name),
      sameText: reqProto.text === resProto.text || bodyProto.text === Object.getPrototypeOf(resProto).text,
      sameBodyProto: Object.getPrototypeOf(reqProto) === Object.getPrototypeOf(resProto),
      bodyTagDescriptor: d(bodyProto, Symbol.toStringTag),
      bodyConstructorDescriptor: d(bodyProto, "constructor"),
      headersTagDescriptor: d(Headers.prototype, Symbol.toStringTag),
      headersGetDescriptor: d(Headers.prototype, "get"),
      reqCloneDescriptor: d(reqProto, "clone"),
      reqMethodDescriptor: d(reqProto, "method"),
      responseStatusDescriptor: d(resProto, "status"),
      bodyIsGlobal: typeof globalThis.Body,
      // The global must name the mixin that is really in the prototype
      // chain, or `instanceof Body` is false and its members vanish (#206).
      requestIsBody: request instanceof Body,
      responseIsBody: response instanceof Body,
      bodyProtoMembers: Object.getOwnPropertyNames(Body.prototype).sort(),
      bodyCtorProto: String(Object.getPrototypeOf(bodyProto.constructor).name),
      domExceptionIsError: new DOMException("m", "AbortError") instanceof Error,
      domExceptionCtorOwn: Object.getOwnPropertyNames(DOMException).join(" | "),
      domExceptionProtoCtor: Object.getPrototypeOf(DOMException.prototype) === Error.prototype,
      domExceptionCtorProto: Object.getPrototypeOf(DOMException) === Function.prototype,
      ctorNames: {
        Request: [Request.name, Request.length],
        Response: [Response.name, Response.length],
        Headers: [Headers.name, Headers.length],
        FormData: [FormData.name, FormData.length],
        AbortController: [AbortController.name, AbortController.length],
        AbortSignal: [AbortSignal.name, AbortSignal.length],
        WebSocket: [WebSocket.name, WebSocket.length],
        DOMException: [DOMException.name, DOMException.length],
      },
      responseJson: Response.json({ a: 1 }).headers.get("content-type"),
      // getAll() is workerd's Set-Cookie accessor: any other name is a
      // TypeError, and the refusal text is part of the pinned shape.
      headersGetAll: (() => {
        const headers = new Headers([["set-cookie", "a=1"]]);
        headers.append("set-cookie", "b=2");
        headers.append("x", "3");
        return {
          cookies: headers.getAll("set-cookie"),
          other: new Headers().getAll("set-cookie"),
          get: headers.get("set-cookie"),
          refused: attempt(() => headers.getAll("x")),
        };
      })(),
      objectUrl,
      revokeUrl,
    });
  },

  "/bindings"(_request, env) {
    const statement = env.DB.prepare("select 1");
    return report(shapes({
      KVNamespace: env.KV,
      R2Bucket: env.R2,
      D1Database: env.DB,
      D1PreparedStatement: statement,
      // D1DatabaseSession is deliberately not pinned: workerd's session
      // reflects its own internal WrappedBinding members (_queryOrThrow,
      // _send, _updateBookmark, ...), which cello's clean surface does not
      // reproduce; see cloudflare-compat.md.
    }));
  },

  "/websocket"() {
    const pair = new WebSocketPair();
    return report(shapes({
      WebSocket: pair[0],
    }));
  },
};

export default {
  async fetch(request, env) {
    const path = new URL(request.url).pathname;
    const route = routes[path];
    if (!route) return new Response("not found", { status: 404 });
    return await route(request, env);
  },
};
