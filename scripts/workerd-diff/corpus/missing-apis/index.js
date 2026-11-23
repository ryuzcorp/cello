// The missing-API surface from the workerd sweep (#195, #196, #201-#204,
// #214, #219, #220): every observation is a value, never wall time or live
// object ordering, so the transcript is stable on both runtimes.
import wasmModule from "./answer.wasm";

const text = (body) => new Response(body, { headers: { "content-type": "text/plain; charset=utf-8" } });
const report = (value) => text(JSON.stringify(value));

const error = (e) => `${e?.name}: ${e?.message}`;
const sync = (fn) => { try { return { ok: true, value: fn() }; } catch (e) { return { ok: false, error: error(e) }; } };
const async_ = async (fn) => { try { return { ok: true, value: await fn() }; } catch (e) { return { ok: false, error: error(e) }; } };
const hex = (bytes) => [...new Uint8Array(bytes)].map((b) => b.toString(16).padStart(2, "0")).join("");
const protoNames = (value) => Object.getOwnPropertyNames(value).sort();
const describe = (value) => ({
  type: typeof value,
  name: typeof value === "function" ? value.name : undefined,
  length: typeof value === "function" ? value.length : undefined,
});

const routes = {
  // #195: constructible MessageEvent with data/origin/lastEventId/source/ports.
  "/message-event"() {
    const descriptor = (object, key) => {
      const d = Object.getOwnPropertyDescriptor(object, key);
      return d && { writable: d.writable, enumerable: d.enumerable, configurable: d.configurable };
    };
    return report({
      minimal: sync(() => {
        const event = new MessageEvent("message");
        return {
          data: event.data, origin: event.origin, lastEventId: event.lastEventId,
          source: event.source, ports: event.ports.length,
          portsIsArray: Array.isArray(event.ports),
          tag: String(event[Symbol.toStringTag]),
        };
      }),
      ownNames: new MessageEvent("m", { data: "d" }).constructor === MessageEvent
        ? Object.getOwnPropertyNames(new MessageEvent("m", { data: "d" }))
        : null,
      descriptors: Object.fromEntries(
        ["ports", "source", "lastEventId", "origin", "data"].map(
          (key) => [key, descriptor(new MessageEvent("m"), key)])),
      json: JSON.stringify(new MessageEvent("m", { data: "d", origin: "o", lastEventId: "l" })),
      dataOnly: sync(() => new MessageEvent("m", { data: 5 }).data),
      ports: sync(() => {
        const channel = new MessageChannel();
        const source = [channel.port1];
        const event = new MessageEvent("m", { data: "d", ports: source });
        source.push(channel.port2);
        return {
          length: event.ports.length,
          same: event.ports[0] === channel.port1,
        };
      }),
      validation: {
        none: sync(() => new MessageEvent()),
        type: sync(() => new MessageEvent(5)),
        init: sync(() => new MessageEvent("m", 5)),
        ports: sync(() => new MessageEvent("m", { ports: null })),
        portElement: sync(() => new MessageEvent("m", { ports: [1] })),
        source: sync(() => new MessageEvent("m", { source: 5 })),
        initNull: sync(() => new MessageEvent("m", null).type),
        originCoerce: sync(() => new MessageEvent("m", { origin: {} }).origin),
      },
      proto: { names: protoNames(MessageEvent.prototype), tag: String(MessageEvent.prototype[Symbol.toStringTag]) },
      ctor: describe(MessageEvent),
      subclass: new MessageEvent("m") instanceof Event,
      dispatch: sync(() => {
        const seen = [];
        const target = new EventTarget();
        target.addEventListener("message", (event) => seen.push(event.data));
        target.dispatchEvent(new MessageEvent("message", { data: "seen" }));
        return seen;
      }),
    });
  },

  // #196: the method surface workerd exposes. Its unimplemented methods throw
  // synchronously; the implemented ones are probed by typeof only, because
  // workerd only implements them behind an instance-only cache service.
  "/cache-surface"() {
    const cache = caches.default;
    const storage = caches;
    const call = (object, name, iface, argument) => {
      const fn = object[name];
      const shape = {
        type: typeof fn,
        name: fn?.name,
        length: fn?.length,
        own: Object.prototype.hasOwnProperty.call(object, name),
      };
      try {
        const value = fn.call(object, argument);
        if (value instanceof Promise) value.catch(() => {});
        shape.sync = "resolved";
      } catch (e) {
        shape.sync = error(e);
      }
      return shape;
    };
    return report({
      cacheMethods: Object.fromEntries(
        ["add", "addAll", "delete", "match", "put", "matchAll", "keys"].map(
          (name) => [name, call(cache, name, "Cache", "https://example.com/")])),
      storageMethods: Object.fromEntries(
        ["open", "delete", "match", "has", "keys"].map(
          (name) => [name, call(storage, name, "CacheStorage", "name")])),
      implemented: Object.fromEntries(
        ["delete", "match", "put"].map((name) => [name, typeof cache[name]])),
      tags: [String(cache[Symbol.toStringTag]), String(storage[Symbol.toStringTag])],
      ctor: [cache.constructor.name, storage.constructor.name],
    });
  },

  // #201 + #214: brotli, format validation, and decompression close messages.
  async "/compression"() {
    const roundTrip = async (format, body) =>
      new Response(new Response(body).body.pipeThrough(new CompressionStream(format)));
    const brotli = async (body) => {
      const packed = new Uint8Array(await (await roundTrip("brotli", body)).arrayBuffer());
      const back = await new Response(
        new Response(packed).body.pipeThrough(new DecompressionStream("brotli"))).text();
      return { bytes: packed.length, hex: hex(packed), back };
    };
    const feed = (format, bytes, closeStream = true) => async_ (async () => {
      const stream = new DecompressionStream(format);
      const writer = stream.writable.getWriter();
      const read = new Response(stream.readable).arrayBuffer()
        .then((buffer) => buffer.byteLength, (e) => error(e));
      const write = await writer.write(bytes).then(() => "resolved", error);
      const close = closeStream
        ? await writer.close().then(() => "resolved", error)
        : undefined;
      return { write, close, read: await read };
    });
    const gzip = new Uint8Array(await (await roundTrip("gzip", "hello".repeat(50))).arrayBuffer());
    return report({
      brotli: {
        empty: await brotli(""),
        short: await brotli("hello"),
        text: await brotli("hello hello hello hello"),
      },
      construct: {
        noArgument: sync(() => new CompressionStream()),
        unknown: sync(() => new CompressionStream("bogus")),
        upper: sync(() => new CompressionStream("BROTLI")),
        decompressUnknown: sync(() => new DecompressionStream("bogus")),
        // A present argument is stringified; only a missing one is the
        // "parameter 1" error.
        objectFormat: typeof new CompressionStream({ toString() { return "gzip"; } }),
        boxedString: typeof new CompressionStream(new String("gzip")),
        number: sync(() => new CompressionStream(5)),
        explicitUndefined: sync(() => new CompressionStream(undefined)),
        nullFormat: sync(() => new CompressionStream(null)),
        decompressNumber: sync(() => new DecompressionStream(5)),
      },
      formats: Object.fromEntries(["gzip", "deflate", "deflate-raw"].map(
        (format) => [format, typeof new CompressionStream(format)])),
      incomplete: {
        gzip: await feed("gzip", gzip.slice(0, 10)),
        brotli: await feed("brotli", new Uint8Array([0x1b, 0x02])),
      },
      garbage: {
        gzip: await feed("gzip", new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), false),
        deflateRaw: await feed("deflate-raw", new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]), false),
      },
    });
  },

  // #202: a length failure resolves the write or close and errors the readable.
  async "/fixed-length"() {
    const run = async (bytes) => {
      const stream = new FixedLengthStream(3);
      const writer = stream.writable.getWriter();
      const drained = new Response(stream.readable).text()
        .then((value) => ({ text: value }), (e) => ({ error: error(e) }));
      const write = await writer.write(bytes).then(() => "resolved", error);
      const close = await writer.close().then(() => "resolved", error);
      return { write, close, read: await drained };
    };
    return report({
      exact: await run(new TextEncoder().encode("abc")),
      overflow: await run(new TextEncoder().encode("abcd")),
      underflow: await run(new TextEncoder().encode("ab")),
      empty: await run(new Uint8Array(0)),
    });
  },

  // #203: the Performance interface, its entries, and the observer surface.
  "/performance"() {
    const mark = performance.mark("alpha", { startTime: 3 });
    const detailed = performance.mark("beta", { startTime: 4, detail: { x: 1 } });
    const measured = performance.measure("gamma", { start: 0, end: 10 });
    const byName = performance.measure("delta", "alpha", "beta");
    const fn = performance.timerify((a, b) => a + b);
    const observer = new PerformanceObserver(() => {});
    observer.observe({ entryTypes: ["mark"] });
    return report({
      instance: {
        ownCount: Object.getOwnPropertyNames(performance).length,
        ctor: performance.constructor.name,
        isPerformance: performance instanceof Performance,
        isEventTarget: performance instanceof EventTarget,
        tag: String(performance[Symbol.toStringTag]),
        proto: protoNames(Performance.prototype),
      },
      timeOrigin: performance.timeOrigin,
      toJSON: performance.toJSON(),
      stringified: JSON.stringify(performance),
      entries: {
        all: performance.getEntries().map((entry) => [entry.entryType, entry.name]),
        marks: performance.getEntriesByType("mark").map((entry) => [entry.name, entry.startTime]),
        byName: performance.getEntriesByName("alpha").length,
        byNameType: performance.getEntriesByName("alpha", "measure").length,
      },
      mark: {
        ctor: mark.constructor.name,
        isEntry: mark instanceof PerformanceEntry,
        isMeasure: mark instanceof PerformanceMeasure,
        json: JSON.stringify(mark),
        detail: mark.detail,
        own: Object.getOwnPropertyNames(mark).length,
      },
      detailed: { json: JSON.stringify(detailed), detail: detailed.detail },
      measured: { json: JSON.stringify(measured), ctor: measured.constructor.name },
      byName: { startTime: byName.startTime, duration: byName.duration },
      timerify: {
        type: typeof fn,
        name: fn.name,
        length: fn.length,
        result: fn(1, 2),
        entriesAfter: performance.getEntriesByType("function").length,
      },
      observer: {
        supported: PerformanceObserver.supportedEntryTypes,
        tag: String(observer[Symbol.toStringTag]),
        proto: protoNames(PerformanceObserver.prototype),
        own: Object.getOwnPropertyNames(PerformanceObserver),
        takeRecords: observer.takeRecords().length,
        constructsNonFunction: sync(() => typeof new PerformanceObserver(1)),
      },
      nodeTiming: {
        json: JSON.stringify(performance.nodeTiming),
        ctor: performance.nodeTiming.constructor.name,
        isEntry: performance.nodeTiming instanceof PerformanceEntry,
      },
      eventLoopUtilization: performance.eventLoopUtilization(),
      eventCounts: {
        tag: String(performance.eventCounts[Symbol.toStringTag]),
        proto: protoNames(performance.eventCounts.constructor.prototype),
        size: performance.eventCounts.size,
      },
      surface: Object.fromEntries(["getEntries", "getEntriesByName", "getEntriesByType",
        "clearMarks", "clearMeasures", "timerify", "eventLoopUtilization", "toJSON",
        "markResourceTiming", "clearResourceTimings", "setResourceTimingBufferSize",
      ].map((name) => [name, typeof performance[name]])),
      markResourceTiming: performance.markResourceTiming({}, "https://x/", "fetch", 0, 0, 0, 0) === undefined,
      constructors: Object.fromEntries(
        ["PerformanceEntry", "PerformanceMark", "PerformanceMeasure", "PerformanceObserver",
          "PerformanceObserverEntryList", "PerformanceResourceTiming",
        ].map((name) => [name, { type: typeof globalThis[name], length: globalThis[name]?.length,
          tag: String(globalThis[name]?.prototype?.[Symbol.toStringTag]) }])),
      illegal: {
        performance: sync(() => new Performance()),
        entry: sync(() => new PerformanceEntry()),
        measure: sync(() => new PerformanceMeasure("x")),
        list: sync(() => new PerformanceObserverEntryList()),
      },
      markErrors: {
        none: sync(() => performance.mark()),
        measureNone: sync(() => performance.measure()),
        detail: sync(() => performance.mark("x", { detail: "s" })),
        // Only undefined is the absent detail; null is not an Object.
        detailNull: sync(() => performance.mark("x", { detail: null })),
        measureDetailNull: sync(() => performance.measure("x", { detail: null })),
        detailUndefined: performance.mark("x", { detail: undefined }).detail,
      },
      // DOMString coercion: only a missing name throws.
      nameCoercion: {
        markNumber: performance.mark(5).name,
        markBoolean: performance.mark(true).name,
        markNull: performance.mark(null).name,
        measureNumber: performance.measure(5).name,
        constructNumber: new PerformanceMark(5).name,
        constructNull: new PerformanceMark(null).name,
      },
      observeAcceptsAnything: sync(() => {
        const any = new PerformanceObserver(() => {});
        any.observe({ type: "bogus" });
        any.observe({});
        any.observe();
        any.disconnect();
        return "ok";
      }),
    });
  },

  // #204, #205, #208, #209: globals, interface constructors, navigator,
  // console, timers, and the Cello-only extras that must not exist.
  "/globals"() {
    const illegal = (name) => sync(() => new globalThis[name]() );
    return report({
      tag: {
        value: String(globalThis[Symbol.toStringTag]),
        objectToString: Object.prototype.toString.call(globalThis),
      },
      origin: { type: typeof origin, value: origin },
      reportError: {
        type: typeof reportError,
        name: reportError.name,
        length: reportError.length,
        returns: reportError(new Error("probe")) === undefined,
        noArgument: sync(() => reportError()),
        // Workerd dispatches synchronously, message "Uncaught " + String(v).
        dispatch: (() => {
          const events = [];
          const listener = (event) => events.push({
            type: event.type,
            message: event.message,
            error: event.error && event.error.message,
            cancelable: event.cancelable,
          });
          addEventListener("error", listener);
          reportError(new Error("x"));
          const afterError = events.length;
          reportError("plain");
          removeEventListener("error", listener);
          return { afterError, events };
        })(),
      },
      constructors: Object.fromEntries([
        "Body", "Crypto", "FetchEvent", "Navigator", "Performance", "PromiseRejectionEvent",
        "ScheduledEvent", "ServiceWorkerGlobalScope", "TailEvent", "TraceEvent",
        "WorkerGlobalScope",
      ].map((name) => [name, { type: typeof globalThis[name], length: globalThis[name]?.length,
        name: globalThis[name]?.name, tag: String(globalThis[name]?.prototype?.[Symbol.toStringTag]) }])),
      illegalConstructors: Object.fromEntries([
        "Body", "Crypto", "FetchEvent", "Navigator", "Performance", "PromiseRejectionEvent",
        "ScheduledEvent", "ServiceWorkerGlobalScope", "TailEvent", "TraceEvent",
        "WorkerGlobalScope",
      ].map((name) => [name, illegal(name)])),
      fetchEventProto: protoNames(FetchEvent.prototype),
      scheduledEventProto: protoNames(ScheduledEvent.prototype),
      navigator: {
        platform: navigator.platform,
        sendBeacon: typeof navigator.sendBeacon,
        ctor: navigator.constructor.name,
        tag: String(Object.getPrototypeOf(navigator)[Symbol.toStringTag]),
        own: Object.getOwnPropertyNames(navigator),
        proto: protoNames(Navigator.prototype),
        values: [navigator.userAgent, navigator.hardwareConcurrency, navigator.language, navigator.languages],
        beacon: navigator.sendBeacon("data:text/plain,probe"),
        beaconNoArgument: sync(() => navigator.sendBeacon()),
      },
      console: {
        context: typeof console.context,
        createTask: typeof console.createTask,
        tag: String(console[Symbol.toStringTag]),
        names: Object.keys(console).sort(),
        contextObject: sync(() => {
          const scoped = console.context("probe");
          return { type: typeof scoped, log: typeof scoped.log, tag: String(scoped[Symbol.toStringTag]) };
        }),
      },
      objectUrls: {
        create: typeof URL.createObjectURL,
        revoke: typeof URL.revokeObjectURL,
        createCall: sync(() => URL.createObjectURL(new Blob(["x"]))),
        revokeCall: sync(() => URL.revokeObjectURL("blob:x")),
      },
      extras: Object.fromEntries(["DigestStream", "DurableObjectRoutingError", "FileReader",
        "Temporal"].map((name) => [name, typeof globalThis[name]])),
      // Removing the namespace must remove V8's Date entry point too.
      temporalEntryPoint: typeof Date.prototype.toTemporalInstant,
      digestStream: typeof crypto.DigestStream,
      cryptoInstance: {
        ctor: crypto.constructor.name,
        isCrypto: crypto instanceof Crypto,
        ownCount: Object.getOwnPropertyNames(crypto).length,
        proto: protoNames(Crypto.prototype),
        tag: String(crypto[Symbol.toStringTag]),
      },
    });
  },

  // #208, #219: timer shapes, the Timeout handle, and the string-callback
  // TypeError. Ordering is the part that already agreed (kept here).
  async "/timers"() {
    const order = [];
    const handle = setInterval(() => {}, 100000);
    const proto = protoNames(Object.getPrototypeOf(handle));
    const asString = String(handle);
    const asNumber = +handle;
    const hasRef = handle.hasRef();
    const refReturns = handle.ref() === handle;
    const unrefReturns = handle.unref() === handle;
    const closeHandle = setTimeout(() => {}, 100000);
    const closeReturns = closeHandle.close() === closeHandle;
    clearInterval(handle);
    clearInterval(handle);
    const base = await new Promise((resolve) => {
      order.push("sync");
      const timeout = setTimeout(() => order.push("timeout"), 0);
      Promise.resolve().then(() => order.push("microtask"));
      order.push("after-schedule");
      const callback = (value) => {
        clearTimeout(timeout);
        resolve({
          order,
          value,
          shapes: {
            setTimeout: [setTimeout.name, setTimeout.length, typeof setTimeout.prototype],
            clearTimeout: [clearTimeout.name, clearTimeout.length, typeof clearTimeout.prototype],
            setInterval: [setInterval.name, setInterval.length],
          },
          handle: {
            type: typeof handle,
            ctor: handle.constructor.name,
            proto,
            stringMatchesPrimitive: asString === String(asNumber),
            hasRef,
            refReturns,
            unrefReturns,
            closeReturns,
          },
          clearedTwice: true,
          clearNumber: clearTimeout(1) === undefined,
          clearObject: clearTimeout({}) === undefined,
          stringCallbacks: {
            setTimeout: sync(() => setTimeout("x", 0)),
            setInterval: sync(() => setInterval("x", 0)),
            nullCallback: sync(() => setTimeout(null, 0)),
            objectCallback: sync(() => setTimeout({}, 0)),
          },
        });
      };
      setTimeout(callback, 0, "delivered");
    });
    // A numeric id is a valid clear handle (a bundle may persist `+handle`).
    const numberClear = await new Promise((resolve) => {
      let fired = false;
      const timeout = setTimeout(() => { fired = true; }, 0);
      clearTimeout(Number(timeout));
      setTimeout(() => resolve({ fired }), 0);
    });
    const numberClearInterval = await new Promise((resolve) => {
      let ticks = 0;
      const interval = setInterval(() => { ticks++; }, 0);
      clearInterval(Number(interval));
      setTimeout(() => resolve({ ticks }), 0);
    });
    // A fired one-shot's id is spent: clearing it must not cancel the
    // refreshed arm, which got a new id.
    const staleId = await new Promise((resolve) => {
      let count = 0;
      const timeout = setTimeout(() => { count++; }, 0);
      const stale = Number(timeout);
      setTimeout(() => {
        timeout.refresh();
        clearTimeout(stale);
        setTimeout(() => resolve({ count }), 0);
      }, 0);
    });
    return report({ ...base, numberClear, numberClearInterval, staleId });
  },

  // #214: error text where the type and code already agreed.
  "/messages"() {
    const decoderError = (label) => sync(() => new TextDecoder(label));
    const fatal = sync(() => new TextDecoder("utf-8", { fatal: true }).decode(new Uint8Array([0xff])));
    return report({
      atob: sync(() => atob("!!!!")),
      atobLength: sync(() => atob("abcde")),
      btoa: sync(() => btoa("\u20ac")),
      decoderLabel: decoderError("bogus"),
      decoderFatal: fatal,
      abortStatic: sync(() => AbortSignal.abort().reason.message),
      abortSignal: sync(() => AbortSignal.abort().reason.name),
      timeoutReason: sync(() => AbortSignal.timeout(0).reason.message),
    });
  },

  // #220: a compiled module never crosses the clone boundary.
  "/clone-wasm"() {
    return report({
      module: sync(() => structuredClone(wasmModule)),
      driver: sync(() => new WebAssembly.Instance(wasmModule) instanceof WebAssembly.Instance),
      memory: sync(() => structuredClone(new WebAssembly.Memory({ initial: 1 }))
        instanceof WebAssembly.Memory),
      global: sync(() => structuredClone(new WebAssembly.Global({ value: "i32" }))
        instanceof WebAssembly.Global),
    });
  },
};

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const route = routes[url.pathname];
    if (!route) return text("no route");
    return route();
  },
};
