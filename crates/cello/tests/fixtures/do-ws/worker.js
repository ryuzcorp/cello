// Durable Object and WebSocket divergences fixed for Cello 0.2.9. Every route
// answers JSON with the exact shape workerd 1.20261121.1 produces, so the
// test asserts observable wire results, not internals.

import { DurableObject } from "cloudflare:workers";

const attempt = async (fn, describe) => {
  try {
    await fn();
    return { ok: true };
  } catch (error) {
    return describe(error);
  }
};

const shape = (error) => ({
  ctor: error === null || error === undefined
    ? String(error)
    : (error.constructor?.name ?? typeof error),
  name: error?.name ?? null,
  message: error?.message ?? null,
  isDom: error instanceof DOMException,
  isError: error instanceof Error,
  remote: error !== null && typeof error === "object" ? error.remote : null,
  stack: typeof error?.stack === "string" ? error.stack : null,
  stackLines: typeof error?.stack === "string" ? error.stack.split("\n").length : null,
});

const settle = () => new Promise((resolve) => setTimeout(resolve, 5));

const describe = (data) => ({
  tag: Object.prototype.toString.call(data),
  isBlob: data instanceof Blob,
  isBuffer: data instanceof ArrayBuffer,
  size: data?.size ?? data?.byteLength ?? null,
  bytes: data instanceof ArrayBuffer ? [...new Uint8Array(data)] : null,
});

export class Plain {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }
  async fetch(request) {
    const route = new URL(request.url).pathname;
    if (route === "/alarms") {
      const s = this.state.storage;
      const out = {
        set: await attempt(() => s.setAlarm(Date.now() + 60_000), shape),
        setString: await attempt(() => s.setAlarm("tomorrow"), shape),
        setZero: await attempt(() => s.setAlarm(0), shape),
        setNaN: await attempt(() => s.setAlarm(NaN), shape),
      };
      out.after = await attempt(() => s.deleteAlarm(), shape);
      return Response.json(out);
    }
    if (route === "/storage") {
      const s = this.state.storage;
      // A private-state host must be refused, not stored as `{}`: the value's
      // state is in a slot now, so V8's serializer would write an empty
      // object (a regression from the own-field era). `back` proves the write
      // did not land either.
      const host = async (name, make) => {
        const out = await attempt(() => s.put("host-" + name, make()), shape);
        out.back = (await s.get("host-" + name)) ?? null;
        return out;
      };
      return Response.json({
        undef: await attempt(() => s.put("undef", undefined), shape),
        fn: await attempt(() => s.put("fn", () => 1), shape),
        sym: await attempt(() => s.put("sym", Symbol("s")), shape),
        proxy: await attempt(() => s.put("proxy", new Proxy({}, {})), shape),
        date: await attempt(() => s.put("date", new Date(0)), shape),
        stream: await host("stream",
          () => new ReadableStream({ start(c) { c.close(); } })),
        headers: await host("headers", () => new Headers({ a: "1" })),
        blob: await host("blob", () => new Blob(["x"])),
        response: await host("response", () => new Response("x")),
        bucket: await host("bucket", () => this.env.BUCKET),
        batch: await attempt(() => s.put({ "host-b1": new Headers({ a: "1" }) }), shape),
        batchBack: (await s.get("host-b1")) ?? null,
      });
    }
    if (route === "/ws-api") {
      const out = {};
      out.pairKeys = Object.keys(new WebSocketPair()).sort();
      out.binaryType = new WebSocketPair()[1].binaryType;
      const pair = new WebSocketPair();
      this.state.acceptWebSocket(pair[1]);
      out.acceptTwice = await attempt(
        () => this.state.acceptWebSocket(pair[1]), shape);
      out.autoNull = await attempt(
        () => this.state.setWebSocketAutoResponse(null), shape);
      out.autoUndefined = await attempt(
        () => this.state.setWebSocketAutoResponse(), shape);
      const unaccepted = new WebSocketPair();
      out.sendBeforeAccept = await attempt(() => unaccepted[1].send("x"), shape);
      out.sendBeforeAcceptClient = await attempt(() => unaccepted[0].send("x"), shape);
      return Response.json(out);
    }
    if (route === "/message-types") {
      const out = { default: [], switched: [] };
      const pair = new WebSocketPair();
      pair[0].accept();
      pair[1].accept();
      pair[1].addEventListener("message", (event) => out.default.push(describe(event.data)));
      pair[0].send(new Uint8Array([5, 6]).buffer);
      await settle();
      out.defaultBinaryType = pair[1].binaryType;
      const switched = new WebSocketPair();
      switched[0].accept();
      switched[1].accept();
      switched[1].binaryType = "arraybuffer";
      switched[1].addEventListener("message", (event) => out.switched.push(describe(event.data)));
      switched[0].send(new Uint8Array([7, 8]).buffer);
      await settle();
      out.switchedBinaryType = switched[1].binaryType;
      return Response.json(out);
    }
    return new Response("not found", { status: 404 });
  }
}

export class Timer {
  constructor(state) {
    this.state = state;
  }
  async fetch() {
    return new Response("unused");
  }
  async alarm() {}
}

export class Thrower {
  async fetch(request) {
    const kind = new URL(request.url).pathname;
    if (kind === "/type") throw new TypeError("bad type");
    if (kind === "/string") throw "plain string";
    if (kind === "/dom") throw new DOMException("aborted!", "AbortError");
    if (kind === "/obj") throw { name: "ObjError", message: "obj msg" };
    if (kind === "/num") throw 42;
    if (kind === "/null") throw null;
    if (kind === "/empty") throw new TypeError("");
    if (kind === "/replaced-dom") {
      // A Worker that replaces the global class owns the instance it throws;
      // workerd treats it as a plain Error, never a DOMException.
      globalThis.DOMException = class DOMException extends Error {};
      throw new globalThis.DOMException("replaced!");
    }
    throw new Error("handler exploded");
  }
}

export class RpcThrow extends DurableObject {
  dom() { throw new DOMException("aborted!", "AbortError"); }
  str() { throw "plain string"; }
  obj() { throw { name: "ObjError", message: "obj msg" }; }
  empty() { throw new TypeError(""); }
}

export class CtorAlarm {
  constructor(state) {
    this.state = state;
    // Workerd's Initializing state allows this: the class may add alarm()
    // later, so the check must not fire mid-constructor.
    this.armed = state.storage.setAlarm(Date.now() + 60_000);
  }
  async fetch() {
    const out = { threw: null };
    try {
      await this.armed;
    } catch (error) {
      out.threw = { name: error.name, message: error.message };
    }
    const alarm = await this.state.storage.getAlarm();
    out.alarmType = typeof alarm;
    out.alarmSet = alarm !== null;
    return Response.json(out);
  }
}

export class Shaper {
  constructor(state) {
    this.state = state;
  }
  async fetch(request) {
    if (new URL(request.url).pathname !== "/sql") return new Response("not found", { status: 404 });
    const sql = this.state.storage.sql;
    sql.exec("DROP TABLE IF EXISTS t");
    sql.exec("CREATE TABLE t (i INTEGER)");
    sql.exec("INSERT INTO t (i) VALUES (?)", true);
    const row = [...sql.exec("SELECT typeof(i) AS type, i FROM t")][0];
    const cursor = sql.exec("SELECT 1 AS v");
    return Response.json({
      boolType: row.type,
      boolValue: row.i,
      cursorKeys: Object.keys(cursor),
      cursorOwnNames: Object.getOwnPropertyNames(cursor).length,
      cursorColumns: cursor.columnNames,
      cursorRowsWritten: typeof cursor.rowsWritten,
    });
  }
}

let LOG;
let Q;

export default {
  async fetch(request, env) {
    LOG = env.LOG;
    Q = env.Q;
    const route = new URL(request.url).pathname;
    if (route === "/alarms") return env.PLAIN.getByName("plain").fetch("https://do.test/alarms");
    if (route === "/storage") return env.PLAIN.getByName("plain").fetch("https://do.test/storage");
    if (route === "/ws-api") return env.PLAIN.getByName("plain").fetch("https://do.test/ws-api");
    if (route === "/message-types")
      return env.PLAIN.getByName("plain").fetch("https://do.test/message-types");
    if (route === "/sql") return env.SHAPER.getByName("shaper").fetch("https://do.test/sql");
    if (route === "/alarm-ctor")
      return env.CTOR.getByName("ctor").fetch("https://do.test/alarm-ctor");
    if (route === "/throw") {
      const stub = env.THROWER.getByName("thrower");
      const out = {};
      for (const [label, kind] of [["error", ""], ["type", "/type"], ["string", "/string"],
        ["dom", "/dom"], ["obj", "/obj"], ["num", "/num"], ["null", "/null"], ["empty", "/empty"]]) {
        out[label] = await attempt(
          () => stub.fetch(`https://do.test${kind}`), shape);
      }
      return Response.json(out);
    }
    if (route === "/throw-replaced") {
      // Must run after every other route: replacing the global persists in
      // the isolate. A Worker that replaces the class owns the instance it
      // throws; workerd hands its caller a plain Error, never a DOMException.
      const stub = env.THROWER.getByName("thrower-replaced");
      return Response.json(await attempt(
        () => stub.fetch("https://do.test/replaced-dom"), shape));
    }
    if (route === "/rpc-throw") {
      const stub = env.RPC.getByName("rpc");
      const out = {};
      for (const method of ["dom", "str", "obj", "empty"]) {
        out[method] = await attempt(() => stub[method](), shape);
      }
      return Response.json(out);
    }
    if (route === "/queue-shape") {
      await LOG.delete("shape");
      await Q.send("shape-probe", { contentType: "text" });
      for (let i = 0; i < 200; i++) {
        const recorded = await LOG.get("shape", "json");
        if (recorded !== null) return Response.json(recorded);
        await new Promise((resolve) => setTimeout(resolve, 25));
      }
      return Response.json({ missing: true });
    }
    return new Response("not found", { status: 404 });
  },
  async queue(batch, env) {
    LOG = env.LOG;
    const message = batch.messages[0];
    await LOG.put("shape", JSON.stringify({
      batchKeys: Object.keys(batch).sort().join(","),
      batchSpreadKeys: Object.keys({ ...batch }).sort().join(","),
      messageKeys: Object.keys(message).sort().join(","),
      messageJsonKeys: Object.keys(JSON.parse(JSON.stringify(message))).sort().join(","),
      ackAllType: typeof batch.ackAll,
      ackType: typeof message.ack,
      body: message.body,
    }));
    batch.ackAll();
  },
};
