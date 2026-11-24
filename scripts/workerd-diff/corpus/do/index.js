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
