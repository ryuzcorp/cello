import { DurableObject } from "cloudflare:workers";
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
// #251: how a binding object reflects. Own properties keep their order -- a
// Worker can read it -- while a prototype's members are sorted, because a
// native prototype's definition order is not a contract. An accessor reports
// itself rather than the value it returns.
const surfaceDescriptors = (names, value) => names.map((name) => {
  const descriptor = Object.getOwnPropertyDescriptor(value, name);
  return [name, descriptor.get ? "getter" : typeof descriptor.value,
    descriptor.enumerable, descriptor.writable ?? null,
    descriptor.configurable];
});
// The own properties a Worker walks with `Object.keys` or `JSON.stringify`.
const surfaceKeys = (value) => surfaceDescriptors(Object.keys(value), value);
// Every own property name, for an object whose internals are slots rather
// than properties: workerd's has none, so neither may this one.
const surfaceOwn = (value) =>
  surfaceDescriptors(Object.getOwnPropertyNames(value), value);
const surfaceChain = (value) => {
  const out = [];
  for (let proto = Object.getPrototypeOf(value);
       proto && proto !== Object.prototype;
       proto = Object.getPrototypeOf(proto)) {
    const ctor = Object.getOwnPropertyDescriptor(proto, "constructor");
    const members = Object.getOwnPropertyNames(proto).map((name) => {
      const descriptor = Object.getOwnPropertyDescriptor(proto, name);
      return [name, descriptor.get ? "getter" : typeof descriptor.value,
        descriptor.enumerable];
    });
    members.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
    out.push([ctor?.value?.name ?? null, members]);
  }
  return out;
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

// The shape a stub caller sees when the call fails: class, name, message
// and workerd's `remote` marker. The gate needs the class and the marker,
// which the plain `errorInfo` below does not record.
const rpcFailure = (error) => ({
  ctor: error?.constructor?.name ?? String(error),
  name: error?.name ?? null,
  message: error?.message ?? null,
  remote: error !== null && typeof error === "object" ? error.remote ?? null : null,
});

// #271 P2: RPC callers observe a block abort the way fetch callers do. The
// class extends `DurableObject` so its methods serve RPC; plain classes do
// not resolve methods on either runtime.
export class Rpc extends DurableObject {
  constructor(state, env) {
    super(state, env);
    this.state = state;
    this.env = env;
  }
  async rpcBoom(kind) {
    const make = () => kind === "type" ? new TypeError("rpc-boom")
      : kind === "string" ? "plain rpc string"
      : new Error("rpc-boom");
    // A `catch` here would run on the old code and never on the new: the
    // abort breaks the event before the handler can answer.
    await this.state.blockConcurrencyWhile(() => { throw make(); });
    return "settled";
  }
  async pong() {
    return "pong";
  }
  async viaRpc(name) {
    const stub = this.env.RPCDO.get(this.env.RPCDO.idFromName(name));
    try {
      return { ok: await stub.rpcBoom("error") };
    } catch (error) {
      return { error: rpcFailure(error) };
    }
  }
}

// How many Probe objects this isolate built. A block failure resets the
// object, so the next event runs on a fresh instance and the counter moves.
// The count is read as a delta across the failing event, never as an
// absolute number: earlier cases build different counts of objects on the
// two runtimes.
let instances = 0;

export class Probe {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    // #267: the construction order pins the reset a block failure performs.
    this.instance = ++instances;
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
      async "/storage-surface"() {
        // #251: the storage, state, SQL and cursor objects keep their
        // internals private and report workerd's own surface. Ordered facts
        // are arrays, because `canonical` sorts an object's keys.
        const s = this.state.storage;
        const state = this.state;
        await s.deleteAll();
        const cursor = s.sql.exec("SELECT 1 AS v");
        const out = {
          props: [
            typeof state.props, Array.isArray(state.props),
            Object.keys(state.props ?? {}).length,
            state.props === null,
            Object.getPrototypeOf(state.props ?? {}) === Object.prototype,
          ],
          storageTag: Object.prototype.toString.call(s),
          stateTag: Object.prototype.toString.call(state),
          sqlTag: Object.prototype.toString.call(s.sql),
          storageKeys: Object.keys(s),
          storageOwn: surfaceKeys(s),
          storageChain: surfaceChain(s),
          stateKeys: Object.keys(state),
          stateOwn: surfaceKeys(state),
          // Cello's state keeps host-internal `_` members on its prototype
          // (a separate leftover); the public members and their existence are
          // what this case pins.
          statePublic: [
            "waitUntil", "blockConcurrencyWhile", "acceptWebSocket",
            "getWebSockets", "getWebSocketAutoResponse",
            "getWebSocketAutoResponseTimestamp", "setWebSocketAutoResponse",
            "setHibernatableWebSocketEventTimeout",
            "getHibernatableWebSocketEventTimeout", "getTags", "abort",
          ].map((name) => [name, typeof state[name]]),
          sqlKeys: Object.keys(s.sql),
          sqlOwn: surfaceOwn(s.sql),
          sqlChain: surfaceChain(s.sql),
          sqlStatics: Object.getOwnPropertyNames(
            Object.getPrototypeOf(s.sql).constructor),
          // Cello's own seams are test-only: a shipping node has none.
          sqlPrepare: "prepare" in s.sql,
          sqlIngest: "ingest" in s.sql,
          sqlFaultHook: "setWriteFaultForTest" in s.sql,
          cursorKeys: Object.keys(cursor),
          cursorOwn: surfaceOwn(cursor),
          cursorChain: surfaceChain(cursor),
          cursorTag: Object.prototype.toString.call(cursor),
          cursorString: String(cursor),
          cursorInstance: await attempt(() => cursor instanceof s.sql.Cursor),
          cursorCtorSame: s.sql.Cursor === undefined
            ? "absent"
            : Object.getPrototypeOf(cursor) === s.sql.Cursor.prototype,
          cursorCtorName: cursor.constructor?.name ?? null,
          cursorColumns: "columns" in cursor,
          cursorCtor: typeof s.sql.Cursor,
          statementCtor: typeof s.sql.Statement,
          cursorNew: await attempt(() => new s.sql.Cursor()),
          statementNew: await attempt(() => new s.sql.Statement()),
          setterType: typeof state.setHibernatableWebSocketEventTimeout,
          getterType: typeof state.getHibernatableWebSocketEventTimeout,
          initialTimeout: await attempt(() =>
            state.getHibernatableWebSocketEventTimeout()),
          kvKeys: Object.keys(s.kv),
          kvOwn: surfaceOwn(s.kv),
          kvChain: surfaceChain(s.kv),
          kvTag: Object.prototype.toString.call(s.kv),
          kvList: (() => {
            const iterator = s.kv.list();
            return {
              keys: Object.keys(iterator),
              own: surfaceOwn(iterator),
              chain: surfaceChain(iterator),
              tag: Object.prototype.toString.call(iterator),
              next: typeof iterator.next,
              selfIterable: iterator[Symbol.iterator]() === iterator,
            };
          })(),
        };
        // The hibernatable WebSocket event timeout: workerd's conversion, and
        // the value it reports back.
        const timeout = async (value) => {
          try { state.setHibernatableWebSocketEventTimeout(value); }
          catch (error) { return errorInfo(error); }
          return await state.getHibernatableWebSocketEventTimeout();
        };
        for (const [name, value] of Object.entries({
          unset: undefined, nullTimeout: null, zero: 0, number: 1500,
          string: "250", negative: -1, float: 1.5, nan: NaN,
          infinity: Infinity, bigint: 5n, week: 604800000,
          overWeek: 604800001,
        })) out["timeout:" + name] = await timeout(value);
        // Bookmarks: what a local storage back-end reports, and the messages
        // the point-in-time-recovery methods refuse with.
        out.bookmarkFirst = await attempt(() => s.getCurrentBookmark());
        out.bookmarkSecond = await attempt(() => s.getCurrentBookmark());
        out.bookmarkAfterWrite = await attempt(async () => {
          await s.put("marker", 1);
          return await s.getCurrentBookmark();
        });
        await s.deleteAll();
        out.bookmarkAfterDeleteAll = await attempt(() =>
          s.getCurrentBookmark());
        out.bookmarkForTime = await attempt(() =>
          s.getBookmarkForTime(Date.now()));
        out.bookmarkForTimeNumber = await attempt(() => s.getBookmarkForTime(1));
        out.bookmarkForTimeZero = await attempt(() => s.getBookmarkForTime(0));
        out.bookmarkForTimeNegative = await attempt(() =>
          s.getBookmarkForTime(-1));
        out.bookmarkForTimeDate = await attempt(() =>
          s.getBookmarkForTime(new Date(0)));
        out.bookmarkForTimeNaN = await attempt(() =>
          s.getBookmarkForTime(NaN));
        out.bookmarkForTimeInfinity = await attempt(() =>
          s.getBookmarkForTime(Infinity));
        out.bookmarkForTimeBigint = await attempt(() =>
          s.getBookmarkForTime(5n));
        out.bookmarkForTimeSymbol = await attempt(() =>
          s.getBookmarkForTime(Symbol("s")));
        out.bookmarkForTimeString = await attempt(() =>
          s.getBookmarkForTime("today"));
        out.bookmarkForTimeNone = await attempt(() => s.getBookmarkForTime());
        out.bookmarkRestore = await attempt(() =>
          s.onNextSessionRestoreBookmark("abc"));
        out.bookmarkRestoreNone = await attempt(() =>
          s.onNextSessionRestoreBookmark());
        out.bookmarkRestoreSymbol = await attempt(() =>
          s.onNextSessionRestoreBookmark(Symbol("s")));
        return report(out);
      },
      async "/storage-options"() {
        // The options argument of each method, converted as workerd's JSG
        // converts a dictionary: null and any object are dictionaries (an
        // array or a Date has none of the members), a primitive is the
        // refusal. Every probe starts from the same two keys, so one
        // argument's outcome cannot change the next one's answer.
        const s = this.state.storage;
        const out = {};
        const reset = async () => {
          await s.deleteAll();
          await s.put("k", 1);
          await s.put("second", 2);
        };
        const values = [
          ["string", "nope"],
          ["number", 1],
          ["true", true],
          ["null", null],
          ["array", []],
          ["arrayValues", ["x"]],
          ["function", () => {}],
          ["date", new Date(0)],
          ["undefined", undefined],
          ["object", {}],
        ];
        for (const [name, value] of values) {
          await reset();
          out[`get:${name}`] = await attempt(async () => {
            const got = await s.get("k", value);
            return got === undefined ? "<undefined>" : got;
          });
          out[`getMany:${name}`] = await attempt(async () => {
            const got = await s.get(["k", "second"], value);
            return got instanceof Map ? got.size : String(got);
          });
          out[`put:${name}`] = await attempt(async () => {
            await s.put("k", 2, value);
            return await s.get("k");
          });
          out[`delete:${name}`] = await attempt(async () => {
            const count = await s.delete("absent", value);
            return count === undefined ? "<undefined>" : count;
          });
          out[`deleteMany:${name}`] = await attempt(async () => {
            const count = await s.delete(["absent"], value);
            return count === undefined ? "<undefined>" : count;
          });
          out[`list:${name}`] = await attempt(async () => {
            const listed = await s.list(value);
            return listed instanceof Map ? listed.size : String(listed);
          });
        }
        // The entries form is `put(entries, options)`: an argument list that
        // matches no overload is refused with its own message. What the write
        // stores is not compared here: workerd takes only a plain object as
        // `entries` and silently writes nothing for a Map, which is a separate
        // divergence from this one.
        await reset();
        const entries = { k: 3 };
        for (const [name, value] of [
          ["string", "nope"],
          ["number", 1],
          ["true", true],
          ["object", {}],
          ["undefined", undefined],
        ]) {
          out[`putEntries:${name}`] = await attempt(async () => {
            await s.put(entries, value);
            return "ok";
          });
        }
        // Alarm options convert the same way; `null` and `{}` pass the
        // conversion and fail (or not) on the alarm itself.
        for (const [name, value] of [
          ["string", "nope"],
          ["number", 1],
          ["null", null],
          ["object", {}],
          ["undefined", undefined],
        ]) {
          out[`setAlarm:${name}`] = await attempt(async () => {
            await s.setAlarm(Date.now() + 60000, value);
            return "ok";
          });
          out[`deleteAlarm:${name}`] = await attempt(async () => {
            await s.deleteAlarm(value);
            return "ok";
          });
        }
        await s.deleteAlarm();
        await s.deleteAll();
        return report(out);
      },
      async "/storage-put-entries"() {
        // workerd's `put(entries)` takes a plain object: its own enumerable
        // string keys are the pairs, and an undefined value leaves the key
        // out. A Map, a Set or a Date has none of those, so it writes nothing
        // and resolves; an array is not a dictionary, so it falls to the
        // single-pair form, where the key is coerced and a missing value is
        // refused. Every probe starts from empty storage, and what it stored
        // is read back, so a shape that writes nothing cannot hide behind the
        // next one (#239).
        const s = this.state.storage;
        class Entries {
          constructor() {
            this.a = 1;
            this.b = "two";
          }
        }
        const hidden = { visible: 1 };
        Object.defineProperty(hidden, "secret", { value: 2, enumerable: false });
        const inherited = Object.create({ inherited: 9 });
        inherited.own = 1;
        const asFunction = () => {};
        asFunction.prop = 1;
        // Sequential: each probe clears the storage its predecessor filled.
        const probes = [
          ["plain", () => s.put({ a: 1, b: "two" })],
          ["map", () => s.put(new Map([["a", 1], ["b", 2]]))],
          ["set", () => s.put(new Set([1, 2]))],
          ["date", () => s.put(new Date(0))],
          ["instance", () => s.put(new Entries())],
          ["function", () => s.put(asFunction)],
          ["hidden", () => s.put(hidden)],
          ["inherited", () => s.put(inherited)],
          ["undefinedEntry", () => s.put({ undef: undefined, keep: 1 })],
          ["symbolKey", () => s.put({ [Symbol("s")]: 1, a: 2 })],
          ["arrayPairs", () => s.put([["a", 1]])],
          ["arrayEmpty", () => s.put([])],
          ["noArgs", () => s.put()],
          ["stringOnly", () => s.put("k")],
          ["stringUndefined", () => s.put("k", undefined)],
          ["numberKey", () => s.put(5, "five")],
          ["booleanKey", () => s.put(true, "yes")],
          ["bigintKey", () => s.put(7n, "seven")],
          ["nullKey", () => s.put(null, 1)],
          ["symbolArgument", () => s.put(Symbol("s"))],
          ["arrayWithValue", () => s.put([["r", 1]], { allowUnconfirmed: true })],
          ["arrayWithOptions", () => s.put([], {})],
          ["mapWithOptions", () => s.put(new Map([["m", 1]]), {})],
          ["batchWithUnknownOption", () => s.put({ u: 1 }, { bogus: true })],
          ["batchWithNullOption", () => s.put({ n: 1 }, null)],
          ["batchWithBadOption", () => s.put({ x: 1 }, "nope")],
          // An array in the options slot is a dictionary too: workerd reads
          // it positionally as PutOptions, so every shape leaves the fields
          // at their defaults and the entries land (#249 review).
          ["batchWithArrayOption", () => s.put({ a: 1 }, [])],
          ["batchWithArrayElement", () => s.put({ a: 1 }, [1])],
          ["batchWithArrayBooleans", () => s.put({ a: 1 }, [true, false, true])],
          ["batchWithArrayObject", () => s.put({ a: 1 }, [{ allowUnconfirmed: true }])],
          ["batchWithArrayHole", () => s.put({ a: 1 }, [,])],
        ];
        const out = {};
        for (const [name, run] of probes) {
          await s.deleteAll();
          const result = await attempt(run);
          result.stored = [...(await s.list()).entries()];
          out[name] = result;
        }
        return report(out);
      },
      async "/storage-list-fields"() {
        // A ListOptions field converts the way workerd's JSG converts it:
        // the string fields take any value (ToString, so `start: 5` reads
        // "5"), `reverse` is a boolean coercion, and `limit` is an `int`
        // with the WebIDL range and positivity checks. Nothing here is a
        // several-JSON error (#239).
        const s = this.state.storage;
        await s.deleteAll();
        for (const key of ["c", "a", "b"]) await s.put(key, key.toUpperCase());
        const list = async (options) => [...(await s.list(options)).keys()];
        const out = {};
        for (const [name, options] of Object.entries({
          limitString: { limit: "5" },
          limitFloat: { limit: 1.9 },
          limitNegative: { limit: -1 },
          limitHuge: { limit: 2 ** 31 },
          limitBigint: { limit: 5n },
          limitNull: { limit: null },
          limitObject: { limit: {} },
          limitUndefined: { limit: undefined },
          limitZero: { limit: 0 },
          prefixNumber: { prefix: 5 },
          prefixNull: { prefix: null },
          prefixBigint: { prefix: 5n },
          startNumber: { start: 5 },
          startAfterNumber: { startAfter: 5 },
          endNumber: { end: 5 },
          reverseString: { reverse: "yes" },
          reverseZeroString: { reverse: "0" },
          reverseOne: { reverse: 1 },
          reverseNull: { reverse: null },
          reverseUndefined: { reverse: undefined },
          unknownField: { bogus: 1 },
          bothStart: { start: "a", startAfter: "b" },
          padded: { prefix: "", start: "a", end: "c" },
        })) {
          out[name] = await attempt(() => list(options));
        }
        return report(out);
      },
      async "/method-lengths"() {
        // A binding's methods are native in workerd, and V8 publishes a
        // native function's `length` as the zero parameters its template
        // carries, so every JSG interface reports 0 whatever its signature
        // reads. The D1 interface is a TypeScript class there and keeps its
        // declared arities, which is what makes it the exception here (#239).
        const arities = (owner, names) =>
          names.map((name) => [name, typeof owner[name], owner[name]?.length]);
        const statement = this.env.DB.prepare("SELECT 1");
        return report({
          storage: arities(this.state.storage, [
            "get", "list", "put", "delete", "deleteAll", "transaction",
            "transactionSync", "getAlarm", "setAlarm", "deleteAlarm", "sync",
          ]),
          sql: arities(this.state.storage.sql, ["exec"]),
          state: arities(this.state, [
            "waitUntil", "blockConcurrencyWhile", "acceptWebSocket",
            "getWebSockets", "getTags", "setWebSocketAutoResponse",
            "getWebSocketAutoResponse", "abort",
          ]),
          namespace: arities(this.env.OBJ, [
            "newUniqueId", "idFromName", "idFromString", "get", "getByName",
            "jurisdiction",
          ]),
          id: arities(this.state.id, ["toString", "equals"]),
          kv: arities(this.env.KV, [
            "get", "list", "put", "getWithMetadata", "delete",
          ]),
          r2: arities(this.env.BUCKET, [
            "head", "get", "put", "delete", "list", "createMultipartUpload",
            "resumeMultipartUpload",
          ]),
          queue: arities(this.env.QUEUE, ["send", "sendBatch", "metrics"]),
          caches: arities(caches, ["open", "delete", "match", "has", "keys"]),
          cache: arities(caches.default, [
            "put", "match", "delete", "add", "addAll", "matchAll", "keys",
          ]),
          // D1 is a TypeScript class in workerd: these arities are real.
          db: arities(this.env.DB, [
            "prepare", "batch", "exec", "withSession", "query", "dump",
          ]),
          prepared: arities(statement, ["bind", "first", "run", "all", "raw"]),
        });
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
        // A cursor's `next()` answers both members even when it is done, and
        // both `one()` refusals end with workerd's period (#251 review).
        const tail = sql.exec("SELECT 1 AS v");
        tail.next();
        const tailDone = tail.next();
        const emptyDone = sql.exec("SELECT 1 AS v WHERE 0").next();
        out.tailDoneKeys = Object.keys(tailDone);
        out.tailDoneValue = "value" in tailDone ? String(tailDone.value) : "<absent>";
        out.emptyDoneKeys = Object.keys(emptyDone);
        out.emptyDoneValue = "value" in emptyDone ? String(emptyDone.value) : "<absent>";
        out.oneEmpty = await attempt(() => sql.exec("SELECT 1 WHERE 0").one());
        out.oneMany = await attempt(() => sql.exec("SELECT 1 UNION ALL SELECT 2").one());
        return report(out);
      },
      async "/sql-types"() {
        const sql = this.state.storage.sql;
        sql.exec("DROP TABLE IF EXISTS t2");
        sql.exec("CREATE TABLE t2 (i INTEGER, r REAL, t TEXT, b BLOB, n TEXT)");
        const out = {};
        // Booleans bind as workerd's TEXT 'true'/'false' from compatibility
        // date 2026-11-24, which this fixture runs at; an older date keeps
        // Cello's historical INTEGER 1/0 (cloudflare-compat.md).
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
        out.bindTrue = await attempt(() => sql.exec("SELECT ? AS v, typeof(?) AS ty", true, true).one());
        out.bindFalse = await attempt(() => sql.exec("SELECT ? AS v, typeof(?) AS ty", false, false).one());
        out.bindBoolColumn = await attempt(() => {
          sql.exec("DELETE FROM t2");
          sql.exec("INSERT INTO t2 (t) VALUES (?)", true);
          sql.exec("INSERT INTO t2 (t) VALUES (?)", false);
          const rows = [...sql.exec("SELECT t, typeof(t) AS ty FROM t2 ORDER BY rowid")];
          const where = sql.exec("SELECT count(*) AS n FROM t2 WHERE t = ?", true).one();
          return { rows, whereTrue: where.n };
        });
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
      // #261: the section outlives the promise the guest holds, so a nested
      // block still runs after the response -- and can still write -- and the
      // gate releases only once it has. The second request takes a fresh
      // block, which queues behind that section, and only then reads what the
      // nested body wrote, so the pair is ordered without a timer.
      async "/nested-block-post-response"() {
        const log = [];
        await this.state.blockConcurrencyWhile(() => {
          log.push("outer-body");
          this.state.blockConcurrencyWhile(async () => {
            log.push("inner-body");
            await this.state.storage.put("post-response-inner", "ran");
            log.push("inner-wrote");
          });
          log.push("outer-returned");
          return "outer-value";
        });
        log.push("outer-awaited");
        return report({ log });
      },
      async "/nested-block-after-response"() {
        const log = [];
        log.push("block:" + await this.state.blockConcurrencyWhile(() => "ran"));
        log.push("inner:" + ((await this.state.storage.get("post-response-inner")) ?? "missing"));
        return report({ log });
      },
      // #261: `blockConcurrencyWhile` converts its argument as WebIDL
      // `Function` before it touches the gate, and a non-function is refused
      // by rejecting the promise the member answered -- the caller keeps
      // running -- with WebIDL's message naming the member and the class. A
      // refused section holds nothing, so a real block after five refusals
      // still runs.
      async "/block-refusals"() {
        const refusal = async (value) => {
          const log = [];
          let promise;
          try {
            promise = this.state.blockConcurrencyWhile(value);
            log.push("returned:" + Object.prototype.toString.call(promise));
          } catch (error) {
            log.push({ threw: errorInfo(error) });
            return log;
          }
          log.push(await attempt(() => promise));
          log.push("after");
          return log;
        };
        const out = {
          number: await refusal(5),
          string: await refusal("x"),
          object: await refusal({}),
          null: await refusal(null),
          undefined: await refusal(undefined),
        };
        out.noArgument = await attempt(async () => {
          await this.state.blockConcurrencyWhile();
        });
        out.realBlock = await attempt(() => this.state.blockConcurrencyWhile(() => "ran"));
        return report(out);
      },
      // #261: a nested block opens its section at the call, but workerd
      // invokes the callback on a later turn: after the calling body's
      // microtasks, and after the promise the enclosing block answered has
      // settled and run its reactions. The `deep` log pins the same schedule
      // one level down, where each level's callback waits for the one above.
      async "/nested-block-order"() {
        const order = [];
        let innerPromise;
        const outerPromise = this.state.blockConcurrencyWhile(() => {
          order.push("outer-body");
          Promise.resolve().then(() => order.push("mt-in-outer"));
          innerPromise = this.state.blockConcurrencyWhile(() => {
            order.push("inner-body");
            Promise.resolve().then(() => order.push("mt-in-inner"));
            return "inner-value";
          });
          order.push("inner-returned:" + Object.prototype.toString.call(innerPromise));
          innerPromise.then((value) => order.push("inner-then:" + value));
          order.push("inner-then-attached");
          return "outer-value";
        });
        order.push("outer-returned:" + Object.prototype.toString.call(outerPromise));
        outerPromise.then((value) => order.push("outer-then:" + value));
        order.push("outer-then-attached");
        await outerPromise;
        order.push("outer-awaited");

        const deep = [];
        const d0 = this.state.blockConcurrencyWhile(() => {
          deep.push("d0-body");
          const d1 = this.state.blockConcurrencyWhile(() => {
            deep.push("d1-body");
            this.state.blockConcurrencyWhile(() => { deep.push("d2-body"); return "d2"; })
              .then(() => deep.push("d2-then"));
            deep.push("d1-inner-returned");
            return "d1";
          });
          d1.then(() => deep.push("d1-then"));
          deep.push("d0-inner-returned");
          return "d0";
        });
        d0.then(() => deep.push("d0-then"));
        await d0;
        deep.push("d0-awaited");
        return report({ order, deep });
      },
      // #267: a throw or reject inside a top-level or nested block breaks
      // the whole event on workerd (500 Internal Server Error), and the stub
      // caller receives the value the block threw -- a `catch` around the
      // block never runs. Each route catches where the old code would carry
      // on, so old and new transcripts differ exactly where the behavior
      // does: the old code answers 200 with what the catch saw, the new
      // code never answers and the stub call rejects.
      async "/block-fail-top"() {
        try {
          await this.state.blockConcurrencyWhile(() => { throw new Error("top-boom"); });
          return report({ settled: "resolved" });
        } catch (error) {
          return report({ caught: errorInfo(error) });
        }
      },
      async "/block-fail-reject"() {
        try {
          await this.state.blockConcurrencyWhile(async () => { throw new Error("top-boom"); });
          return report({ settled: "resolved" });
        } catch (error) {
          return report({ caught: errorInfo(error) });
        }
      },
      async "/block-fail-nested"() {
        const outer = await this.state.blockConcurrencyWhile(async () => {
          try {
            await this.state.blockConcurrencyWhile(() => { throw new Error("inner-boom"); });
            return "inner-resolved";
          } catch (error) {
            return `inner-caught:${error?.message ?? error}`;
          }
        });
        return report({ outer });
      },
      async "/block-fail-unawaited"() {
        const outer = await this.state.blockConcurrencyWhile(() => {
          const inner = this.state.blockConcurrencyWhile(() => { throw new Error("inner-boom"); });
          inner.catch(() => {});
          return "outer-value";
        });
        return report({ outer });
      },
      async "/block-fail-depth2"() {
        const d0 = await this.state.blockConcurrencyWhile(async () => {
          try {
            await this.state.blockConcurrencyWhile(async () => {
              try {
                await this.state.blockConcurrencyWhile(() => { throw new Error("d2-boom"); });
                return "d2-resolved";
              } catch (error) {
                return `d2-caught:${error?.message ?? error}`;
              }
            });
            return "d1-resolved";
          } catch (error) {
            return `d1-caught:${error?.message ?? error}`;
          }
        });
        return report({ d0 });
      },
      async "/block-fail-limit"() {
        const nest = (depth) => depth === 0 ? "bottom"
          : this.state.blockConcurrencyWhile(() => nest(depth - 1));
        try {
          return report({ top: await nest(70) });
        } catch (error) {
          return report({ caught: errorInfo(error) });
        }
      },
      async "/block-fail-kind-string"() {
        try {
          await this.state.blockConcurrencyWhile(() => { throw "plain string"; });
          return report({ settled: "resolved" });
        } catch (error) {
          return report({ caught: errorInfo(error) });
        }
      },
      async "/block-fail-kind-type"() {
        try {
          await this.state.blockConcurrencyWhile(() => { throw new TypeError("type-msg"); });
          return report({ settled: "resolved" });
        } catch (error) {
          return report({ caught: errorInfo(error) });
        }
      },
      // #271 P1: a block failure over an open transaction breaks the event
      // and rolls the savepoint back: the uncommitted write stays invisible
      // and the next transaction starts clean. The `catch` below runs on
      // the old code only.
      async "/block-fail-open-tx"() {
        try {
          await this.state.blockConcurrencyWhile(async () => {
            await this.state.storage.transaction(async (tx) => {
              tx.put("txkey", "held");
              await this.state.blockConcurrencyWhile(() => { throw new Error("tx-boom"); });
              tx.put("txkey", "unreached");
            });
          });
          return report({ settled: "resolved" });
        } catch (error) {
          return report({ caught: errorInfo(error) });
        }
      },
      async "/block-tx-state"() {
        const txkey = await this.state.storage.get("txkey");
        let txOk = false;
        try {
          await this.state.storage.transaction(async (tx) => { tx.put("txkey2", "v"); });
          txOk = true;
        } catch {
          txOk = false;
        }
        return report({ txkey: txkey ?? null, txOk });
      },
      async "/block-instance"() {
        return report({ instance: this.instance });
      },
      async "/state-surface"() {
        // #256: workerd's `DurableObjectState` publishes only its documented
        // members, all enumerable, and no host-internal name at all. Cello
        // used to carry `_ready`, `_socket`, `_blockConcurrencyWhile`,
        // `_runConcurrencyBlock` and `_resetAfterConcurrencyFailure` on the
        // prototype, where guest code could call them.
        const state = this.state;
        const prototype = Object.getPrototypeOf(state);
        const out = {
          tag: Object.prototype.toString.call(state),
          keys: Object.keys(state),
          own: surfaceKeys(state),
          chain: surfaceChain(state),
          internals: Object.getOwnPropertyNames(prototype)
            .filter((name) => name.startsWith("_")),
          internalCalls: [
            "_ready", "_socket", "_blockConcurrencyWhile",
            "_runConcurrencyBlock", "_resetAfterConcurrencyFailure",
          ].map((name) => [name, typeof state[name]]),
          // A native prototype's definition order is not a contract, so the
          // member set is sorted; the chain above sorts too.
          prototypeEnumerable: Object.keys(prototype).sort(),
        };
        // The documented block path still works, from the same object and
        // through a nested block, which is the path the internals served.
        out.block = await attempt(async () => {
          const log = [];
          await this.state.blockConcurrencyWhile(async () => {
            log.push("outer");
            await this.state.blockConcurrencyWhile(() => log.push("inner"));
          });
          return log;
        });
        return report(out);
      },
      async "/exports-surface"() {
        // #259: a Durable Object's `ctx.exports` is the module's export map,
        // as a stateless entrypoint's is: every export is an own, enumerable,
        // writable handle whose `typeof` is "function". A Durable Object
        // class handle is a loopback namespace as well (so it carries the
        // namespace methods and specializes when called), a class with no
        // namespace is a loopback class handle, and a plain handler is a
        // loopback service stub.
        const state = this.state;
        const exports = state.exports;
        const names = Object.keys(exports).sort();
        // Each handle kind keeps workerd's two prototype levels: one holding
        // only an own `constructor` and the interface's tag, over the level
        // carrying the members. Both are reflected here, with each member's
        // enumerability, because that is what a Worker reads.
        //
        // One name is deliberately left out: workerd names the service stub's
        // `Fetcher` level from whichever of its `Fetcher`/`ServiceStub`
        // interfaces the isolate instantiates first, so this deployment's D1
        // binding makes it `Fetcher` here and a deployment without one reports
        // `ServiceStub` (measured on 1.20261121.1, same class otherwise). Only
        // that level's descriptor is compared; its tag is the stable identity.
        const chain = (value) => {
          const out = [];
          for (let proto = Object.getPrototypeOf(value);
               proto !== null && proto !== Object.prototype;
               proto = Object.getPrototypeOf(proto)) {
            const tag = Object.prototype.toString.call(proto);
            const ctor = Object.getOwnPropertyDescriptor(proto, "constructor");
            out.push({
              tag,
              ctor: ctor
                ? [tag === "[object Fetcher]" ? null : ctor.value?.name ?? null,
                  ctor.enumerable]
                : null,
              own: Object.getOwnPropertyNames(proto).sort().map((name) => {
                const descriptor = Object.getOwnPropertyDescriptor(proto, name);
                return [name, descriptor.get ? "getter" : typeof descriptor.value,
                  descriptor.enumerable, descriptor.writable ?? null,
                  descriptor.configurable];
              }),
            });
          }
          return out;
        };
        const entry = (name) => {
          const value = exports[name];
          return {
            type: typeof value,
            keys: Object.keys(value),
            own: Object.getOwnPropertyNames(value).sort(),
            tag: Object.prototype.toString.call(value),
            chain: chain(value),
            get: typeof value?.get,
            idFromName: typeof value?.idFromName,
            fetch: typeof value?.fetch,
          };
        };
        const out = {
          names,
          // The key set, not the module's definition order.
          own: Object.keys(exports).sort(),
          ownFlags: Object.keys(exports).sort().map((name) => {
            const descriptor = Object.getOwnPropertyDescriptor(exports, name);
            return [name, descriptor.enumerable, descriptor.writable,
              descriptor.configurable];
          }),
          entries: {},
          stable: exports === state.exports,
        };
        for (const name of names) out.entries[name] = entry(name);
        // The default handler answers through its handle, exactly as a
        // service binding would.
        out.defaultFetch = await attempt(async () => {
          const response = await exports.default.fetch("https://do.test/rpc");
          return { status: response.status, body: await response.text() };
        });
        // A class handle reaches its own namespace and specializes for a
        // facet.
        out.classStub = await attempt(async () => {
          const response = await exports.Probe.getByName("handle")
            .fetch("https://do.test/rpc");
          return response.status;
        });
        out.specialize = await attempt(() => {
          const specialized = exports.Probe({ props: { a: 1 } });
          return [typeof specialized, Object.getOwnPropertyNames(specialized).sort(),
            Object.prototype.toString.call(specialized),
            specialized.constructor?.name ?? null, chain(specialized)];
        });
        // A loopback class handle specializes the same way, without a
        // namespace behind it.
        out.classSpecialize = await attempt(() => {
          const specialized = exports.NoNamespace({ props: { a: 1 } });
          return [typeof specialized, Object.prototype.toString.call(specialized),
            specialized.constructor?.name ?? null];
        });
        // Calling a handle converts workerd's required `Options` dictionary:
        // a missing or non-object argument is the interface's own refusal,
        // `null` and `undefined` are the empty dictionary, and the `props`
        // field has its own refusal. A loopback class handle names itself in
        // both.
        const refusal = (fn) => attempt(fn);
        out.calls = {
          noArgs: await refusal(() => exports.Probe()),
          number: await refusal(() => exports.Probe(5)),
          badProps: await refusal(() => exports.Probe({ props: 5 })),
          nullArg: await refusal(() => typeof exports.Probe(null)),
          undefinedArg: await refusal(() => typeof exports.Probe(undefined)),
          classNoArgs: await refusal(() => exports.NoNamespace()),
          classNumber: await refusal(() => exports.NoNamespace(5)),
          classBadProps: await refusal(() => exports.NoNamespace({ props: 5 })),
          handlerNoArgs: await refusal(() => exports.helper()),
          handlerBadProps: await refusal(() => exports.helper({ props: 5 })),
        };
        // `new` on a handle specializes exactly as calling it does, through
        // the construct path: valid arguments answer the specialized class,
        // bad ones the call path's refusal. A specialized class is not a
        // constructor, as workerd's is not.
        out.construct = {
          ns: await attempt(() => {
            const value = new exports.Probe({ props: { a: 1 } });
            return [typeof value, Object.prototype.toString.call(value),
              value.constructor?.name ?? null,
              Object.getOwnPropertyNames(value).sort()];
          }),
          nsNoArgs: await refusal(() => new exports.Probe()),
          nsNumber: await refusal(() => new exports.Probe(5)),
          nsBadProps: await refusal(() => new exports.Probe({ props: 5 })),
          class: await attempt(() => {
            const value = new exports.NoNamespace({ props: { a: 1 } });
            return [typeof value, Object.prototype.toString.call(value),
              value.constructor?.name ?? null,
              Object.getOwnPropertyNames(value).sort()];
          }),
          classNoArgs: await refusal(() => new exports.NoNamespace()),
          specialized: await refusal(() => {
            const specialized = exports.Probe({ props: { a: 1 } });
            return new specialized();
          }),
          handler: await refusal(() => new exports.helper()),
          handlerArgs: await attempt(() => {
            const value = new exports.helper({ props: { a: 1 } });
            return [typeof value, Object.prototype.toString.call(value),
              value.constructor?.name ?? null];
          }),
        };
        // `jurisdiction()` answers a fresh namespace object on every call --
        // never the handle, never the binding -- over the same instance
        // store, on the loopback and the real-namespace paths alike: an id
        // minted either side reads from the other. A named jurisdiction stays
        // refused on both paths (each runtime's own sentence, so only the
        // error name is compared).
        const refusalName = (fn) => {
          try { fn(); return "<no throw>"; } catch (e) { return e?.name ?? null; }
        };
        out.jurisdiction = await attempt(() => {
          const scoped = exports.Probe.jurisdiction();
          const scopedAgain = exports.Probe.jurisdiction();
          const envScoped = this.env.OBJ.jurisdiction();
          const envScopedAgain = this.env.OBJ.jurisdiction();
          const plainId = exports.Probe.idFromName("juris-x");
          const plainIdString = plainId.toString();
          return {
            freshLoop: scoped !== scopedAgain,
            freshEnv: envScoped !== envScopedAgain,
            loopIsEnv: scoped === envScoped,
            loopIsHandle: scoped === exports.Probe,
            envIsBinding: envScoped === this.env.OBJ,
            shape: [
              typeof scoped, Object.prototype.toString.call(scoped),
              scoped.constructor?.name ?? null,
              Object.getOwnPropertyNames(scoped).sort(),
            ],
            envShape: [
              typeof envScoped, Object.prototype.toString.call(envScoped),
              envScoped.constructor?.name ?? null,
              Object.getOwnPropertyNames(envScoped).sort(),
            ],
            chain: chain(scoped),
            envChain: chain(envScoped),
            sharedFn: scoped.get === exports.Probe.get &&
              scoped.get === this.env.OBJ.get &&
              envScoped.get === this.env.OBJ.get,
            sameCtor: scoped.constructor === envScoped.constructor,
            sameIdString:
              scoped.idFromName("juris-x").toString() === plainIdString,
            toScoped: scoped.get(plainId).id.toString() === plainIdString,
            toPlain: exports.Probe.get(scoped.idFromName("juris-y")).id
              .toString() === scoped.idFromName("juris-y").toString(),
            envToScoped:
              this.env.OBJ.get(plainId).id.toString() === plainIdString,
            scopedUid: typeof scoped.newUniqueId(),
            euHandle: refusalName(() => exports.Probe.jurisdiction("eu")),
            euEnv: refusalName(() => this.env.OBJ.jurisdiction("eu")),
          };
        });
        // An id minted by another class is refused by `idFromString` with
        // workerd's sentence (trailing period included), on the handle and
        // the binding paths alike.
        out.crossId = {
          handle: await refusal(() => exports.Probe.idFromString(
            exports.Thrower.idFromName("z").toString())),
          env: await refusal(() => this.env.OBJ.idFromString(
            exports.Thrower.idFromName("z").toString())),
        };
        // A namespace method reached off its handle is not a capability of
        // its own, and refuses with workerd's full message.
        out.detached = {};
        for (const name of ["get", "getByName", "idFromName", "idFromString",
          "newUniqueId", "jurisdiction"]) {
          const method = exports.Probe[name];
          out.detached[name] = [
            await refusal(() => method.call({})),
            await refusal(() => method()),
          ];
        }
        return report(out);
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

// #259: an exported class the config declares no namespace for, and a plain
// function export. workerd reads both as `ctx.exports` handles: a loopback
// class handle and, for the function, the same (it is a handler with no
// entrypoint base). Neither is a Durable Object binding.
export class NoNamespace {
  async fetch() {
    return new Response("no namespace", { status: 404 });
  }
}

export function helper() {
  return "helper";
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
  // #267: a throw or reject in a block, the 64-level limit, and the thrown
  // value shapes all break the event on workerd: the stub call rejects with
  // the value the block threw, and the object resets. A handle that observed
  // an aborted object stays broken, while a fresh stub to the reset object
  // works. The reset reads as a counter delta, never an absolute number:
  // earlier cases build different counts of objects on the two runtimes.
  async "/block-fail-top"() {
    const out = {};
    const top = OBJ.getByName("block-fail-top");
    const before = await (await top.fetch("https://do.test/block-instance")).json();
    out.throw = await attempt(async () => {
      const response = await top.fetch("https://do.test/block-fail-top");
      return { status: response.status, body: await response.text() };
    });
    const revived = OBJ.getByName("block-fail-top");
    const after = await (await revived.fetch("https://do.test/block-instance")).json();
    // The failure reset the object: the next event runs on a fresh instance.
    out.reset = after.instance - before.instance;
    const reject = OBJ.getByName("block-fail-reject");
    out.reject = await attempt(async () => {
      const response = await reject.fetch("https://do.test/block-fail-reject");
      return { status: response.status, body: await response.text() };
    });
    return report(out);
  },
  async "/block-fail-nested"() {
    const out = {};
    const nested = OBJ.getByName("block-fail-nested");
    out.nested = await attempt(async () => {
      const response = await nested.fetch("https://do.test/block-fail-nested");
      return { status: response.status, body: await response.text() };
    });
    const unawaited = OBJ.getByName("block-fail-unawaited");
    out.unawaited = await attempt(async () => {
      const response = await unawaited.fetch("https://do.test/block-fail-unawaited");
      return { status: response.status, body: await response.text() };
    });
    const depth2 = OBJ.getByName("block-fail-depth2");
    out.depth2 = await attempt(async () => {
      const response = await depth2.fetch("https://do.test/block-fail-depth2");
      return { status: response.status, body: await response.text() };
    });
    const broken = OBJ.getByName("block-fail-broken");
    const live = await (await broken.fetch("https://do.test/block-instance")).json();
    out.first = await attempt(async () => {
      const response = await broken.fetch("https://do.test/block-fail-top");
      return { status: response.status, body: await response.text() };
    }, rpcFailure);
    out.second = await attempt(async () => {
      const response = await broken.fetch("https://do.test/block-instance");
      return { status: response.status, body: await response.text() };
    }, rpcFailure);
    const fresh = OBJ.getByName("block-fail-broken");
    const revived = await (await fresh.fetch("https://do.test/block-instance")).json();
    // The failure reset the object, so a fresh stub runs on a new instance;
    // the broken stub above keeps rejecting with the abort instead.
    out.revived = revived.instance - live.instance;
    // P3: a repeat on the broken stub replays the value's class with no
    // `remote` marker, for non-Error throws too.
    const brokenStr = OBJ.getByName("block-fail-broken-string");
    out.strFirst = await attempt(async () => {
      const response = await brokenStr.fetch("https://do.test/block-fail-kind-string");
      return { status: response.status, body: await response.text() };
    }, rpcFailure);
    out.strSecond = await attempt(async () => {
      const response = await brokenStr.fetch("https://do.test/block-instance");
      return { status: response.status, body: await response.text() };
    }, rpcFailure);
    const brokenType = OBJ.getByName("block-fail-broken-type");
    out.typeFirst = await attempt(async () => {
      const response = await brokenType.fetch("https://do.test/block-fail-kind-type");
      return { status: response.status, body: await response.text() };
    }, rpcFailure);
    out.typeSecond = await attempt(async () => {
      const response = await brokenType.fetch("https://do.test/block-instance");
      return { status: response.status, body: await response.text() };
    }, rpcFailure);
    return report(out);
  },
  async "/block-fail-limit"() {
    const out = {};
    const limit = OBJ.getByName("block-fail-limit");
    out.limit = await attempt(async () => {
      const response = await limit.fetch("https://do.test/block-fail-limit");
      return { status: response.status, body: await response.text() };
    });
    const string = OBJ.getByName("block-fail-kind-string");
    out.string = await attempt(async () => {
      const response = await string.fetch("https://do.test/block-fail-kind-string");
      return { status: response.status, body: await response.text() };
    });
    const type = OBJ.getByName("block-fail-kind-type");
    out.type = await attempt(async () => {
      const response = await type.fetch("https://do.test/block-fail-kind-type");
      return { status: response.status, body: await response.text() };
    });
    return report(out);
  },
  // #271 P2: an RPC call observes a block abort the way a fetch does: the
  // first call rejects with the value the block threw (class intact, no
  // `remote` marker), the same stub stays broken, and a fresh stub works.
  // The cell leg covers a caller in another cell. P3: repeats keep the
  // class and still carry no marker.
  async "/block-fail-rpc"() {
    const out = {};
    const stub = RPCDO.getByName("block-fail-rpc");
    out.first = await attempt(() => stub.rpcBoom("error"), rpcFailure);
    out.second = await attempt(() => stub.rpcBoom("error"), rpcFailure);
    out.pongOnBroken = await attempt(() => stub.pong(), rpcFailure);
    const fresh = RPCDO.getByName("block-fail-rpc");
    out.revived = await attempt(() => fresh.pong());
    const typeStub = RPCDO.getByName("block-fail-rpc-type");
    out.typeFirst = await attempt(() => typeStub.rpcBoom("type"), rpcFailure);
    out.typeSecond = await attempt(() => typeStub.rpcBoom("type"), rpcFailure);
    const stringStub = RPCDO.getByName("block-fail-rpc-string");
    out.stringFirst = await attempt(() => stringStub.rpcBoom("string"), rpcFailure);
    out.stringSecond = await attempt(() => stringStub.rpcBoom("string"), rpcFailure);
    const caller = RPCDO.getByName("block-fail-rpc-caller");
    out.cell = await caller.viaRpc("block-fail-rpc-target");
    return report(out);
  },
  // #271 P1: the failing event rolls an open transaction back, and the stub
  // that saw the abort stays broken while a later event on the reset object
  // sees no uncommitted write and starts a fresh transaction cleanly.
  async "/block-fail-open-tx"() {
    const out = {};
    const tx = OBJ.getByName("block-fail-open-tx");
    out.first = await attempt(async () => {
      const response = await tx.fetch("https://do.test/block-fail-open-tx");
      return { status: response.status, body: await response.text() };
    }, rpcFailure);
    out.second = await attempt(async () => {
      const response = await tx.fetch("https://do.test/block-instance");
      return { status: response.status, body: await response.text() };
    }, rpcFailure);
    const state = OBJ.getByName("block-fail-open-tx");
    out.state = await (await state.fetch("https://do.test/block-tx-state")).json();
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
  async "/stub-surface"() {
    // #251: a Durable Object stub is a DurableObject over Fetcher, with its
    // routing private: `fetch`/`connect` have a native function's arity, a
    // detached call is refused, and the id and namespace keep their state.
    const stub = OBJ.getByName("stub");
    const out = {
      keys: Object.keys(stub),
      own: surfaceOwn(stub),
      chain: surfaceChain(stub),
      tag: Object.prototype.toString.call(stub),
      json: JSON.stringify(stub),
      hasThen: "then" in stub,
      fetchType: typeof stub.fetch,
      fetchLength: stub.fetch.length,
      connectType: typeof stub.connect,
      connectLength: stub.connect?.length ?? null,
      getType: typeof stub.get,
      name: stub.name,
      idKeys: Object.keys(stub.id),
      idOwn: surfaceOwn(stub.id),
      idChain: surfaceChain(stub.id),
      idTag: Object.prototype.toString.call(stub.id),
      // The hex identity is derived from a per-namespace key the two runtimes
      // seed differently, so its shape and round trip are what compare.
      idShape: [
        String(stub.id).length, /^[0-9a-f]*$/.test(String(stub.id)),
        String(stub.id) === OBJ.idFromName("stub").toString(),
      ],
      idHasName: "name" in stub.id,
      idJurisdiction: stub.id.jurisdiction ?? "<absent>",
      nsKeys: Object.keys(OBJ),
      nsOwn: surfaceOwn(OBJ),
      nsChain: surfaceChain(OBJ),
      nsTag: Object.prototype.toString.call(OBJ),
    };
    // A method reached off the stub is not a capability of its own.
    const detached = stub.fetch;
    out.detachedFetch = await attempt(async () => {
      const response = await detached("https://do.test/rpc");
      return { status: response.status };
    });
    out.foreignFetch = await attempt(async () => {
      const response = await stub.fetch.call({}, "https://do.test/rpc");
      return { status: response.status };
    });
    // `connect` converts its argument as workerd's `AnySocketAddress` union
    // does before the transport is reached: an object is the SocketAddress
    // dictionary, every primitive is a `host:port` string the URL parser
    // splits, and a converted address must be a valid host. Only the
    // conversion is compared: workerd dials and answers a Socket where Cello
    // refuses at its missing transport, so any non-TypeError reads as
    // "converted" on both, while every conversion refusal is a TypeError
    // whose name and message are compared. Every row was probed against
    // workerd 1.20261121.1 (#251 review).
    const connect = (value) => attempt(async () => {
      try {
        await stub.connect(value);
      } catch (error) {
        if (error instanceof TypeError) throw error;
      }
      return "<converted>";
    });
    out.connectNoArg = await attempt(() => {
      stub.connect();
      return "<converted>";
    });
    for (const [name, value] of Object.entries({
      empty: "", blank: " ", hostOnly: "127.0.0.1", emptyPort: "127.0.0.1:",
      emptyHost: ":80", nameHost: "host:80", valid: "127.0.0.1:80",
      port0: "127.0.0.1:0", portMax: "127.0.0.1:65535",
      portOver: "127.0.0.1:65536", portHuge: "127.0.0.1:99999",
      portFloat: "127.0.0.1:1.5", portWord: "127.0.0.1:http",
      scheme: "https://do.test/", portNegative: "127.0.0.1:-1",
      ipv6: "[::1]:80", ipv6Bare: "::1:80", ipv6NoPort: "[::1]",
      ipv6BareOnly: "::1", hostSpace: " 127.0.0.1:80",
      portSpace: "127.0.0.1: 80", trailingSpace: "127.0.0.1:80 ",
      twoColons: "a:b:80", leadingZeroPort: "a:080", underscorePort: "a:8_0",
      suffixPort: "a:80a", hexPort: "a:0x50", ipv4: "1.2.3.4:80",
      dottedHost: "a.b:80", spaceInHost: "a b:80", urlPort: "http://a:80",
      doubleColonTail: "a::", leadingSpaceString: " a:80",
      plusPort: "a:+80", dashHost: "-a:80", slashHost: "a/b:80",
      backslashHost: "a\\b:80", unicodeHost: "\u00e9:80",
      newlinePort: "a:80\n", tabPort: "a:\t80",
      number: 123, boolean: true, nan: NaN, bigint: 5n,
    })) out["connect:string:" + name] = await connect(value);
    for (const [name, value] of Object.entries({
      nullValue: null, explicitUndefined: undefined, object: {},
      portOnly: { port: 80 }, hostOnlyObject: { hostname: "127.0.0.1" },
      validObject: { hostname: "127.0.0.1", port: 80 },
      stringPort: { hostname: "127.0.0.1", port: "80" },
      floatPort: { hostname: "127.0.0.1", port: 1.5 },
      booleanPort: { hostname: "a", port: true },
      overPort: { hostname: "127.0.0.1", port: 65536 },
      int32Port: { hostname: "a", port: 2 ** 31 },
      negativePort: { hostname: "a", port: -1 },
      textPort: { hostname: "a", port: "80x" },
      nanPort: { hostname: "a", port: NaN },
      infinitePort: { hostname: "a", port: Infinity },
      bigintPort: { hostname: "a", port: 80n },
      emptyHostObject: { hostname: "", port: 80 },
      nullHostObject: { hostname: null, port: 80 },
      numericHost: { hostname: 127, port: 80 },
      shortHostObject: { hostname: "a", port: 80 },
      spaceHostObject: { hostname: " ", port: 80 },
      spaceInHostObject: { hostname: "a b", port: 80 },
      longHostObject: { hostname: "a".repeat(300), port: 80 },
      unicodeHostObject: { hostname: "\u00e9", port: 80 },
      arrayOne: ["a:1"], arrayTwo: ["a", 1], dateArg: new Date(0),
      functionArg: () => 1,
    })) out["connect:object:" + name] = await connect(value);
    out.connectSymbol = await connect(Symbol("s"));
    // Each member is read once, in workerd's order, so a getter's reads show.
    const reads = [];
    let hostReads = 0;
    out.connectGetterHost = await connect({
      get hostname() {
        reads.push("hostname");
        return ++hostReads === 1 ? "127.0.0.1" : "bad host";
      },
      get port() { reads.push("port"); return 80; },
    });
    out.connectGetterReads = reads.join(",");
    // The options dictionary converts too: a primitive is the WebIDL refusal,
    // an object is a dictionary whose unknown members are ignored, and only
    // `secureTransport` has values the transport validates.
    const connectOptions = (options) => attempt(async () => {
      try {
        await stub.connect("127.0.0.1:80", options);
      } catch (error) {
        if (error instanceof TypeError) throw error;
      }
      return "<converted>";
    });
    out.connectOptionsString = await connectOptions("bad");
    out.connectOptionsNumber = await connectOptions(5);
    out.connectOptionsTrue = await connectOptions(true);
    out.connectOptionsNull = await connectOptions(null);
    out.connectOptionsUndefined = await connectOptions(undefined);
    out.connectOptionsArray = await connectOptions([]);
    out.connectOptionsUnknown = await connectOptions({ bogus: 1 });
    out.connectOptionsSecure = await connectOptions({ secureTransport: "bogus" });
    out.connectOptionsSecureOff = await connectOptions({ secureTransport: "off" });
    out.connectOptionsHalfOpen = await connectOptions({ allowHalfOpen: "yes" });
    out.connectOptionsHighWater = await connectOptions({ highWaterMark: "9" });
    for (const [name, value] of Object.entries({
      nan: NaN, object: {}, negative: -1, symbol: Symbol("h"), bigint: 9n,
      infinite: Infinity, fraction: 1.5, nullValue: null, big: 2 ** 31,
      negativeFraction: -0.5, negativeBigint: -1n, hugeBigint: 2n ** 70n,
      negativeZero: -0, maxBigint: 2n ** 64n - 1n, edgeBigint: 2n ** 64n,
      hugeNumber: 2 ** 70, edgeNumber: 2 ** 64, belowEdgeNumber: 2 ** 63,
    })) {
      out["connectOptionsHighWater:" + name] =
        await connectOptions({ highWaterMark: value });
    }
    const optionReads = [];
    out.connectOptionsOrder = await connectOptions({
      get secureTransport() { optionReads.push("secureTransport"); return "bogus"; },
      get allowHalfOpen() { optionReads.push("allowHalfOpen"); return true; },
      get highWaterMark() { optionReads.push("highWaterMark"); return NaN; },
    });
    out.connectOptionsReads = optionReads.join(",");
    return report(out);
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
    RPCDO = env.RPCDO;
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
let RPCDO;
