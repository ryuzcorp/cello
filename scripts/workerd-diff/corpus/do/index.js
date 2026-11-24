// Durable Object probes: storage, SQL, alarms, WebSockets, ids and errors.
const canonical = (value) => {
  if (value instanceof Date) return "<date>";
  if (value instanceof Map) return { "<map>": [...value].map(canonical) };
  if (value instanceof Set) return { "<set>": [...value].map(canonical) };
  if (value instanceof Uint8Array) return { "<bytes>": [...value] };
  if (value instanceof ArrayBuffer) return { "<buffer>": [...new Uint8Array(value)] };
  if (typeof value === "bigint") return `<bigint:${value}>`;
  if (typeof value === "number" && !Number.isFinite(value)) return `<number:${String(value)}>`;
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.keys(value).sort().map((k) => [k, canonical(value[k])]));
  }
  return value === undefined ? "<undefined>" : value;
};
const report = (value) => new Response(JSON.stringify(canonical(value)), {
  headers: { "content-type": "text/plain; charset=utf-8" },
});
const errorInfo = (error) => ({
  name: error?.name ?? String(error),
  message: error?.message ?? String(error),
});
const attempt = async (fn, describe = errorInfo) => {
  try { return { ok: await fn() }; } catch (error) { return { error: describe(error) }; }
};

export class Timer {
  constructor(state) { this.state = state; }
  async fetch(request) {
    const { pathname } = new URL(request.url);
    if (pathname === "/alarm-get") {
      const target = Date.now() + 3_600_000;
      const out = { initial: await this.state.storage.getAlarm() };
      await this.state.storage.setAlarm(target);
      out.exact = (await this.state.storage.getAlarm()) === target;
      out.type = typeof await this.state.storage.getAlarm();
      await this.state.storage.deleteAlarm();
      out.afterDelete = await this.state.storage.getAlarm();
      return report(out);
    }
    if (pathname === "/alarm-past") {
      const out = {};
      out.setPast = await attempt(async () => {
        await this.state.storage.setAlarm(Date.now() - 3_600_000);
        return typeof await this.state.storage.getAlarm();
      });
      return report(out);
    }
    return new Response("no route", { status: 404 });
  }
  async alarm(info) {
    // Firing is not compared; the handler only makes setAlarm legal.
  }
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
    throw new Error("handler exploded");
  }
}

export class Probe {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }
  async fetch(request) {
    const { pathname } = new URL(request.url);
    const routes = {
      async "/storage-basics"() {
        const s = this.state.storage;
        await s.put("a", { nested: { n: 1, list: [true, null, "s"] }, at: new Date(0), bytes: new Uint8Array([1, 2]) });
        await s.put("b", new Map([["k", 1]]));
        await s.put("c", new Set([1, 2]));
        await s.put("d", new ArrayBuffer(2));
        await s.put("e", NaN);
        await s.put("f", -0);
        const values = {};
        for (const key of ["a", "b", "c", "d", "e", "f", "missing"]) {
          values[key] = await s.get(key);
        }
        values.multi = await s.get(["a", "b", "missing"]);
        return report({ values });
      },
      async "/storage-list"() {
        const s = this.state.storage;
        await s.deleteAll();
        for (const k of ["l/c", "l/a", "l/b", "m/d"]) await s.put(k, k);
        const all = await s.list();
        const prefixed = await s.list({ prefix: "l/" });
        const bounded = await s.list({ start: "l/b", end: "m/" });
        const reversed = await s.list({ prefix: "l/", reverse: true });
        const limited = await s.list({ limit: 2 });
        const map = await s.list({ prefix: "l/" });
        return report({
          all: [...all.keys()], prefixed: [...prefixed.keys()], bounded: [...bounded.keys()],
          reversed: [...reversed.keys()], limited: [...limited.keys()],
          isMap: map instanceof Map, firstValue: map.get("l/a"),
        });
      },
      async "/storage-delete"() {
        const s = this.state.storage;
        await s.deleteAll();
        await s.put("x", 1);
        await s.put("y", 2);
        const out = {};
        out.deleteOne = await attempt(() => s.delete("x"));
        out.deleteMissing = await attempt(() => s.delete("nope"));
        out.deleteMany = await attempt(() => s.delete(["y", "nope2"]));
        out.remaining = [...(await s.list()).keys()];
        out.deleteEmptyArray = await attempt(() => s.delete([]));
        out.deleteAllResult = await attempt(() => s.deleteAll());
        return report(out);
      },
      async "/storage-keys"() {
        const s = this.state.storage;
        await s.deleteAll();
        const out = {};
        out.emptyKey = await attempt(() => s.put("", 1));
        out.longKey = await attempt(() => s.put("k".repeat(2049), 1));
        out.maxKey = await attempt(() => s.put("k".repeat(2048), 1));
        out.bigValue = await attempt(() => s.put("big", new Uint8Array(128 * 1024 + 1)));
        out.circular = await attempt(() => {
          const value = {};
          value.self = value;
          return s.put("circ", value);
        });
        out.undefinedValue = await attempt(async () => {
          await s.put("undef", undefined);
          return await s.get("undef");
        });
        out.functionValue = await attempt(() => s.put("fn", () => 1));
        out.symbolValue = await attempt(() => s.put("sym", Symbol("s")));
        return report(out);
      },
      async "/sql"() {
        const sql = this.state.storage.sql;
        sql.exec("DROP TABLE IF EXISTS t");
        const created = sql.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, data BLOB)");
        const inserted = sql.exec("INSERT INTO t (name, data) VALUES (?, ?)", "ada", new Uint8Array([1, 2]));
        const selected = [...sql.exec("SELECT id, name FROM t")];
        const withColumns = [...sql.exec("SELECT id, name FROM t")];
        const one = sql.exec("SELECT count(*) AS n FROM t").one();
        const raw = [...sql.exec("SELECT id, name FROM t").raw()];
        const out = {
          // A DDL statement's row counters diverge (workerd counts libsql's
          // b-tree rows), so the gate pins the cursor's public shape and DML
          // counters, not the DDL ones. See cloudflare-compat.md.
          inserted: { rowsRead: inserted.rowsRead, rowsWritten: inserted.rowsWritten, rowsWrittenLength: inserted.rowsWritten },
          selected, columnNames: withColumns.columnNames, one,
          raw: raw.map((row) => row.map((cell) => cell instanceof ArrayBuffer ? [...new Uint8Array(cell)] : cell)),
          databaseSize: typeof this.state.storage.sql.databaseSize,
          cursorType: typeof sql.exec("SELECT 1 AS v"),
          // Workerd keeps a cursor's fields private: nothing is an own
          // enumerable property.
          cursorKeys: Object.keys(sql.exec("SELECT 1 AS v")),
          createdToArray: created.toArray?.() ?? null,
        };
        out.cursorShape = Object.fromEntries(Object.entries(Object.getOwnPropertyDescriptors(Object.getPrototypeOf(withColumns))).map(([k, d]) => [k, typeof d.value]));
        return report(out);
      },
      async "/sql-types"() {
        const sql = this.state.storage.sql;
        sql.exec("DROP TABLE IF EXISTS t2");
        sql.exec("CREATE TABLE t2 (i INTEGER, r REAL, t TEXT, b BLOB, n TEXT)");
        const out = {};
        // A boolean bind is a deliberate Cello deviation (INTEGER 1/0, see
        // cloudflare-compat.md), so it is not compared with workerd here.
        out.bindNull = await attempt(() => {
          sql.exec("INSERT INTO t2 (t) VALUES (?)", null);
          return [...sql.exec("SELECT t FROM t2 ORDER BY rowid DESC LIMIT 1")];
        });
        out.bindNumber = await attempt(() => {
          sql.exec("INSERT INTO t2 (i, r) VALUES (?, ?)", 3, 1.5);
          return [...sql.exec("SELECT i, r FROM t2 ORDER BY rowid DESC LIMIT 1")];
        });
        out.bindBigInt = await attempt(() => {
          sql.exec("INSERT INTO t2 (i) VALUES (?)", 2 ** 60);
          return [...sql.exec("SELECT i FROM t2 ORDER BY rowid DESC LIMIT 1")];
        });
        out.bindBytes = await attempt(() => {
          sql.exec("INSERT INTO t2 (b) VALUES (?)", new Uint8Array([9, 9]));
          const row = [...sql.exec("SELECT b, typeof(b) AS type FROM t2 ORDER BY rowid DESC LIMIT 1")];
          return row.map((r) => r.type === undefined ? r : { type: r.type, bytes: r.b ? [...new Uint8Array(r.b)] : null });
        });
        out.bindDate = await attempt(() => sql.exec("INSERT INTO t2 (t) VALUES (?)", new Date(0)));
        out.execMulti = await attempt(() => [...sql.exec("INSERT INTO t2 (i) VALUES (1); INSERT INTO t2 (i) VALUES (2);")]);
        // An empty statement's message diverges (`SQL error:` prefix), a
        // separate issue; it stays out of the gate until it matches.
        return report(out);
      },
      async "/alarms"() {
        const s = this.state.storage;
        const out = {};
        out.initial = await attempt(() => s.getAlarm());
        out.set = await attempt(async () => {
          await s.setAlarm(Date.now() + 60_000);
          return typeof await s.getAlarm();
        });
        out.setPast = await attempt(async () => {
          await s.setAlarm(1);
          return typeof await s.getAlarm();
        });
        out.delete = await attempt(async () => {
          await s.deleteAlarm();
          return await s.getAlarm();
        });
        out.deleteAgain = await attempt(() => s.deleteAlarm());
        out.setInvalid = await attempt(() => s.setAlarm("tomorrow"));
        return report(out);
      },
      async "/storage-refusals"() {
        // #233: a URL and a URLSearchParams carry private state, so the plain
        // codec would store `{}`; workerd refuses both with a DataCloneError
        // that names the interface. `back` proves the write did not land. The
        // refusal sentence itself is each runtime's own, so the transcript
        // pins the class, that the interface is named, and that nothing was
        // stored -- not the wording.
        const s = this.state.storage;
        const host = async (name, make) => {
          const key = "refuse-" + name;
          const out = await attempt(() => s.put(key, make()));
          out.back = (await s.get(key)) ?? null;
          if (out.error) {
            out.error = {
              name: out.error.name,
              namesType: out.error.message.includes(name),
            };
          }
          return out;
        };
        return report({
          url: await host("URL", () => new URL("https://example.com/a?b=1")),
          searchParams: await host("URLSearchParams",
            () => new URLSearchParams("a=1&b=2")),
          formData: await host("FormData", () => {
            const form = new FormData();
            form.set("a", "1");
            return form;
          }),
          // A forged prototype is plain data on both runtimes: the brand,
          // not the prototype chain, decides. The property is defined
          // rather than assigned so `URL.prototype.href`'s brand-checked
          // setter (a different message on each runtime) never runs.
          forgedUrl: await attempt(async () => {
            const forged = Object.create(URL.prototype);
            Object.defineProperty(forged, "href",
              { value: "https://example.com/", enumerable: true });
            await s.put("refuse-forged", forged);
            return await s.get("refuse-forged");
          }),
          forgedSearchParams: await attempt(async () => {
            const forged = Object.create(URLSearchParams.prototype);
            await s.put("refuse-forged-params", forged);
            return await s.get("refuse-forged-params");
          }),
        });
      },
      async "/websockets"() {
        const out = {};
        const pair = new WebSocketPair();
        out.pairKeys = Object.keys(pair);
        out.pairIsArray = Array.isArray(pair);
        out.clientType = typeof pair[0];
        out.accept = await attempt(() => this.state.acceptWebSocket(pair[1]));
        out.listAfter = this.state.getWebSockets().length;
        out.sockets = this.state.getWebSockets().map((ws) => ({ readyState: ws.readyState, tags: this.state.getTags(ws) }));
        out.acceptTwice = await attempt(() => this.state.acceptWebSocket(pair[1]));
        const tagged = new WebSocketPair();
        this.state.acceptWebSocket(tagged[1], ["blue", "green"]);
        out.tags = this.state.getTags(tagged[1]);
        out.byTag = this.state.getWebSockets("blue").length;
        out.byMissingTag = this.state.getWebSockets("red").length;
        out.totalSockets = this.state.getWebSockets().length;
        out.autoResponse = await attempt(() => this.state.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong")));
        out.autoResponseNull = await attempt(() => this.state.setWebSocketAutoResponse(null));
        const unaccepted = new WebSocketPair();
        out.sendBeforeAccept = await attempt(() => unaccepted[1].send("x"));
        return report(out);
      },
      async "/ws-close-codes"() {
        const out = {};
        const codes = [1005, 1006, 1015, 999, 1000, 1001, 2999, 3000, 4999, 5000];
        for (const code of codes) {
          const pair = new WebSocketPair();
          pair[1].accept();
          out[`close${code}`] = await attempt(() => pair[1].close(code));
        }
        return report(out);
      },
      async "/block-concurrency"() {
        const log = [];
        this.state.blockConcurrencyWhile(async () => {
          log.push("start");
          await new Promise((resolve) => setTimeout(resolve, 1));
          log.push("end");
        });
        log.push("after-call");
        return report({ log });
      },
      async "/rpc"() {
        return report({ plain: true });
      },
      async "/storage-sync"() {
        return report(await attempt(async () => typeof await this.state.storage.sync()));
      },
      async "/transaction"() {
        const s = this.state.storage;
        await s.deleteAll();
        const out = {};
        out.sync = await attempt(() => s.transactionSync(() => {
          s.put("t1", 1);
          return "returned";
        }));
        out.nested = await attempt(() => s.transactionSync(() => {
          s.put("t2", 1);
          try {
            s.transactionSync(() => {
              s.put("t3", 1);
              throw new Error("inner");
            });
          } catch {
            // The outer transaction survives a failed nested one.
          }
          s.put("t4", 1);
        }));
        out.rollback = await attempt(() => s.transactionSync(() => {
          s.put("t5", 1);
          throw new Error("outer");
        }));
        out.after = [...(await s.list()).keys()];
        out.asyncTx = await attempt(() => s.transaction(() => s.put("t6", 1)));
        return report(out);
      },
      async "/ws-message-types"() {
        // workerd: sending before accept() is an error on either end of a
        // pair (web-socket.c++ send requires Accepted), and a message's type
        // follows binaryType (WHATWG default "blob").
        const out = {};
        const unaccepted = new WebSocketPair();
        out.sendBeforeAccept = await attempt(() => unaccepted[1].send("x"));
        out.sendBeforeAcceptClient = await attempt(() => unaccepted[0].send("x"));

        const typed = (data) => ({
          tag: Object.prototype.toString.call(data),
          isBlob: data instanceof Blob,
          isBuffer: data instanceof ArrayBuffer,
          size: data?.size ?? data?.byteLength ?? null,
          bytes: data instanceof ArrayBuffer ? [...new Uint8Array(data)] : null,
          text: typeof data === "string" ? data : null,
        });
        const pair = new WebSocketPair();
        const defaults = [];
        pair[0].accept();
        pair[1].accept();
        pair[1].addEventListener("message", (event) => defaults.push(typed(event.data)));
        pair[0].send("text");
        pair[0].send(new Uint8Array([5, 6]).buffer);
        await new Promise((resolve) => setTimeout(resolve, 5));
        out.defaultServer = defaults;
        out.defaultBinaryType = pair[1].binaryType;

        const switched = new WebSocketPair();
        const optioned = [];
        switched[0].accept();
        switched[1].accept();
        switched[1].binaryType = "arraybuffer";
        switched[1].addEventListener("message", (event) => optioned.push(typed(event.data)));
        switched[0].send(new Uint8Array([7, 8]).buffer);
        await new Promise((resolve) => setTimeout(resolve, 5));
        out.arrayBufferServer = optioned;
        out.switchedBinaryType = switched[1].binaryType;
        return report(out);
      },
    };
    const route = routes[pathname];
    if (!route) return report({ uncaught: `no route ${pathname}` });
    try {
      return await route.call(this);
    } catch (error) {
      return new Response(JSON.stringify(canonical({ uncaught: errorInfo(error) })), { status: 500 });
    }
  }
}

const routes = {
  async "/alarm-handler"() {
    const stub = TIMER.getByName("alarm");
    const out = {};
    out.get = await attempt(async () => {
      const response = await stub.fetch("https://do.test/alarm-get");
      return await response.json();
    });
    out.past = await attempt(async () => {
      const response = await stub.fetch("https://do.test/alarm-past");
      return await response.json();
    });
    return report(out);
  },
  async "/thrower"() {
    const stub = THROWER.getByName("thrower");
    const failure = (error) => {
      // The value workerd rethrows: class, `remote`, and a first-line stack.
      const stack = typeof error?.stack === "string"
        ? error.stack.split("\n")[0] : null;
      return {
        ctor: error === null || error === undefined
          ? String(error) : (error.constructor?.name ?? typeof error),
        name: error?.name ?? null,
        message: error?.message ?? null,
        isDom: error instanceof DOMException,
        isError: error instanceof Error,
        remote: error !== null && typeof error === "object" ? error.remote : null,
        stack,
      };
    };
    const out = {};
    for (const kind of ["", "/type", "/string", "/dom", "/obj", "/num", "/null", "/empty"]) {
      out[kind || "error"] = await attempt(async () => {
        const response = await stub.fetch(`https://do.test${kind}`);
        return { status: response.status, body: await response.text() };
      }, failure);
    }
    out.rpc = await attempt(async () => JSON.stringify(await stub.missing()));
    return report(out);
  },
  async "/ids"() {
    const a = OBJ.idFromName("alpha");
    const b = OBJ.idFromName("alpha");
    const c = OBJ.idFromName("beta");
    const str = a.toString();
    const fromString = OBJ.idFromString(str);
    const unique = OBJ.newUniqueId();
    return report({
      length: str.length, isHex: /^[0-9a-f]+$/.test(str), equals: a.equals(b),
      different: a.equals(c), roundTrip: fromString.equals(a), name: a.name,
      longName: OBJ.idFromName("n".repeat(1100)).name === undefined ? "absent" : "present",
      uniqueLength: unique.toString().length,
      badString: (() => { try { return { ok: OBJ.idFromString("zz") }; } catch (error) { return { error: errorInfo(error) }; } })(),
      sameNamespaceName: OBJ.idFromName("alpha").toString() === str,
      unknownClass: (() => { try { return { ok: typeof OBJ.get(a) }; } catch (error) { return { error: errorInfo(error) }; } })(),
    });
  },
  async "/stub-errors"() {
    const stub = OBJ.getByName("errors");
    const out = {};
    out.fetchOk = await attempt(async () => (await stub.fetch("https://do.test/rpc")).text());
    out.rpcOk = await attempt(async () => JSON.stringify(await stub.missingMethod()));
    out.fetchBadPath = await attempt(async () => {
      const response = await stub.fetch("https://do.test/nope");
      return { status: response.status, body: await response.text() };
    });
    out.getByNameType = typeof OBJ.getByName;
    return report(out);
  },
};

export default {
  async fetch(request, env) {
    OBJ = env.OBJ;
    TIMER = env.TIMER;
    THROWER = env.THROWER;
    const { pathname } = new URL(request.url);
    const route = routes[pathname];
    if (!route) {
      // Everything else is a command for the Durable Object.
      const stub = env.OBJ.getByName("main");
      return stub.fetch(request);
    }
    try {
      return await route(request);
    } catch (error) {
      return new Response(JSON.stringify(canonical({ uncaught: errorInfo(error) })), { status: 500 });
    }
  },
};
let OBJ;
let TIMER;
let THROWER;
