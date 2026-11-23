// Durable Object and WebSocket divergences fixed for Cello 0.2.9. Every route
// answers JSON with the exact strings workerd 1.20261121.1 produces, so the
// test asserts wire text, not internals.

const attempt = async (fn) => {
  try {
    await fn();
    return { ok: true };
  } catch (error) {
    return { name: error?.name ?? String(error), message: error?.message ?? String(error) };
  }
};

const settle = () => new Promise((resolve) => setTimeout(resolve, 5));

const describe = (data) => ({
  tag: Object.prototype.toString.call(data),
  isBlob: data instanceof Blob,
  isBuffer: data instanceof ArrayBuffer,
  size: data?.size ?? data?.byteLength ?? null,
  bytes: data instanceof ArrayBuffer ? [...new Uint8Array(data)] : null,
});

export class Plain {
  constructor(state) {
    this.state = state;
  }
  async fetch(request) {
    const route = new URL(request.url).pathname;
    if (route === "/alarms") {
      const s = this.state.storage;
      const out = {
        set: await attempt(() => s.setAlarm(Date.now() + 60_000)),
        setString: await attempt(() => s.setAlarm("tomorrow")),
        setZero: await attempt(() => s.setAlarm(0)),
        setNaN: await attempt(() => s.setAlarm(NaN)),
      };
      out.after = await attempt(() => s.deleteAlarm());
      return Response.json(out);
    }
    if (route === "/storage") {
      const s = this.state.storage;
      return Response.json({
        undef: await attempt(() => s.put("undef", undefined)),
        fn: await attempt(() => s.put("fn", () => 1)),
        sym: await attempt(() => s.put("sym", Symbol("s"))),
        proxy: await attempt(() => s.put("proxy", new Proxy({}, {}))),
        date: await attempt(() => s.put("date", new Date(0))),
      });
    }
    if (route === "/ws-api") {
      const out = {};
      out.pairKeys = Object.keys(new WebSocketPair()).sort();
      out.binaryType = new WebSocketPair()[1].binaryType;
      const pair = new WebSocketPair();
      this.state.acceptWebSocket(pair[1]);
      out.acceptTwice = await attempt(() => this.state.acceptWebSocket(pair[1]));
      out.autoNull = await attempt(() => this.state.setWebSocketAutoResponse(null));
      out.autoUndefined = await attempt(() => this.state.setWebSocketAutoResponse());
      const unaccepted = new WebSocketPair();
      out.sendBeforeAccept = await attempt(() => unaccepted[1].send("x"));
      out.sendBeforeAcceptClient = await attempt(() => unaccepted[0].send("x"));
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
    if (kind === "/dom") throw new DOMException("boom", "AbortError");
    throw new Error("handler exploded");
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
    if (route === "/throw") {
      const stub = env.THROWER.getByName("thrower");
      const out = {};
      for (const [label, kind] of [["error", ""], ["type", "/type"], ["string", "/string"], ["dom", "/dom"]]) {
        const failure = await attempt(() => stub.fetch(`https://do.test${kind}`));
        out[label] = {
          name: failure.name,
          message: failure.message,
          hostFrames:
            failure.message?.includes("rejected:") ||
            failure.message?.includes(" <- ") ||
            failure.message?.includes("<anonymous>"),
        };
      }
      return Response.json(out);
    }
    if (route === "/queue-shape") {
      await LOG.delete("shape");
      await Q.send("shape-probe", { contentType: "text" });
      for (let i = 0; i < 200; i++) {
        const shape = await LOG.get("shape", "json");
        if (shape !== null) return Response.json(shape);
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
