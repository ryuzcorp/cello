// D1 binding fixture: result shapes, binds and the exact error surfaces.
// `duration` and `timings` are wall time and are dropped; `served_by`,
// `served_by_region` and `size_after` are environment facts.
const report = (value) =>
  new Response(JSON.stringify(value), {
    headers: { "content-type": "text/plain; charset=utf-8" },
  });
const errorInfo = (error) => ({
  name: error?.name ?? String(error),
  message: error?.message ?? String(error),
  cause: error?.cause ? String(error.cause.message ?? error.cause) : null,
});
const attempt = async (fn) => {
  try {
    return { ok: await fn() };
  } catch (error) {
    return { error: errorInfo(error) };
  }
};
const hex = (value) => {
  const bytes = value instanceof ArrayBuffer ? new Uint8Array(value) : new Uint8Array(value);
  return [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
};
// The comparable part of a D1Meta: retry bookkeeping and row accounting.
// `last_row_id` on a read-only statement and `timings`/`served_by`/
// `size_after` are documented Cello differences or environment facts.
const meta = (value) => ({
  changed_db: value.changed_db,
  changes: value.changes,
  rows_read: value.rows_read,
  rows_written: value.rows_written,
  total_attempts: value.total_attempts,
});

let DB;
const routes = {
  // Result shapes: first, first(column), all, raw, raw({columnNames}), run.
  async "/shape"() {
    await DB.exec(
      "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, name TEXT, n REAL, b BLOB, flag INTEGER)",
    );
    await DB.prepare("DELETE FROM t").run();
    const inserted = await DB.prepare(
      "INSERT INTO t (name, n, b, flag) VALUES (?, ?, ?, ?)",
    ).bind("ada", 1.5, new Uint8Array([1, 2]), true).run();
    const all = await DB.prepare("SELECT id, name, n, b, flag FROM t").all();
    const raw = await DB.prepare("SELECT id, name FROM t").raw();
    const rawNamed = await DB.prepare("SELECT id, name FROM t").raw({
      columnNames: true,
    });
    return report({
      inserted: { meta: meta(inserted.meta), results: inserted.results, success: inserted.success },
      all: {
        results: all.results.map((row) => ({
          id: row.id,
          name: row.name,
          n: row.n,
          b: hex(row.b),
          flag: row.flag,
        })),
        meta: meta(all.meta),
        success: all.success,
      },
      first: await DB.prepare("SELECT id, name FROM t").first(),
      firstColumn: await DB.prepare("SELECT name FROM t").first("name"),
      firstMissingColumn: await attempt(() =>
        DB.prepare("SELECT name FROM t").first("nope")),
      firstEmpty: await DB.prepare("SELECT name FROM t WHERE 0").first(),
      raw: raw.map((row) => row.map((cell) =>
        cell instanceof ArrayBuffer ? hex(cell) : cell)),
      rawNamed: rawNamed.map((row) => row.map((cell) =>
        cell instanceof ArrayBuffer ? hex(cell) : cell)),
      rawEmpty: await DB.prepare("SELECT name FROM t WHERE 0").raw(),
      emptyAll: await attempt(async () =>
        (await DB.prepare("SELECT name FROM t WHERE 0").all()).results),
    });
  },

  // Binds: what each JavaScript value becomes in the statement.
  async "/bind-types"() {
    await DB.exec("CREATE TABLE IF NOT EXISTS bt (id INTEGER PRIMARY KEY, name TEXT)");
    const insert = (value) =>
      DB.prepare("INSERT INTO bt (name) VALUES (?)").bind(value).run();
    const last = () =>
      DB.prepare("SELECT name FROM bt ORDER BY id DESC LIMIT 1").first();
    const out = {};
    out.number = await attempt(async () => (await insert(5)).meta.changes);
    out.float = await attempt(async () => (await insert(1.25)).meta.changes);
    out.string = await attempt(async () => (await insert("s")).meta.changes);
    out.null = await attempt(async () => {
      await insert(null);
      return (await last()).name;
    });
    out.arrayBuffer = await attempt(async () =>
      (await insert(new Uint8Array([3, 4]).buffer)).meta.changes);
    out.typedArray = await attempt(async () =>
      (await insert(new Uint8Array([5, 6]))).meta.changes);
    out.byteArray = await attempt(async () => (await insert([7, 8])).meta.changes);
    out.bigInt = await attempt(() => insert(2n ** 60n));
    out.bigNumber = await attempt(async () => (await insert(2 ** 60)).meta.changes);
    out.date = await attempt(() => insert(new Date(0)));
    out.undefinedValue = await attempt(() => insert(undefined));
    out.noBind = await attempt(() => DB.prepare("SELECT ? AS v").first());
    out.tooManyParams = await attempt(() =>
      DB.prepare("SELECT ? AS v").bind(
        ...Array.from({ length: 101 }, (_, i) => i),
      ).first());
    return report(out);
  },

  // exec: an empty statement set is an error, a syntax error is SQLite's
  // own (with the offset and the code), and success reports the count.
  async "/exec-errors"() {
    await DB.exec("CREATE TABLE IF NOT EXISTS e (id INTEGER PRIMARY KEY, v TEXT)");
    const out = {};
    for (const [name, sql] of [
      ["empty", ""],
      ["spaces", "  \n\t "],
      ["semicolon", ";"],
      ["semicolons", ";;"],
      ["comment", "-- nothing"],
      ["blockComment", "/* nothing */"],
      ["garbage", "NOT SQL AT ALL"],
      ["typo", "SELEC 1"],
      ["openQuote", "SELECT 'x"],
      ["openParen", "SELECT (1"],
      ["sameLine", "SELECT 1; NOT SQL AT ALL"],
      ["multiLine", "SELECT 1;\nSELEC 2"],
      ["multiLineFirst", "SELECT 1;\nSELECT 2;\nNOT SQL"],
      ["missingTable", "SELECT * FROM nope"],
    ]) {
      out[name] = await attempt(() => DB.exec(sql));
    }
    out.twoStatements = await attempt(async () => {
      await DB.exec(
        "INSERT INTO e (v) VALUES ('x'); INSERT INTO e (v) VALUES ('y');",
      );
      return "ran";
    });
    out.rows = await attempt(async () =>
      (await DB.prepare("SELECT COUNT(*) AS n FROM e").first()).n);
    return report(out);
  },

  // An empty batch and an empty prepared statement are refused the same
  // way; a batch result keeps its per-statement shape and rolls back whole.
  async "/batch"() {
    await DB.exec("CREATE TABLE IF NOT EXISTS b (id INTEGER PRIMARY KEY, v TEXT)");
    await DB.prepare("DELETE FROM b").run();
    const out = {};
    out.empty = await attempt(() => DB.batch([]));
    out.emptyStatement = await attempt(() => DB.batch([DB.prepare("")]));
    out.commentStatement = await attempt(() => DB.batch([DB.prepare("-- x")]));
    out.prepareEmpty = await attempt(() => DB.prepare("").first());
    out.ok = await attempt(async () => {
      const results = await DB.batch([
        DB.prepare("INSERT INTO b (v) VALUES ('a')"),
        DB.prepare("INSERT INTO b (v) VALUES ('b')"),
        DB.prepare("SELECT v FROM b ORDER BY id"),
      ]);
      return results.map((result) => ({
        meta: meta(result.meta),
        results: result.results,
        success: result.success,
      }));
    });
    out.failed = await attempt(() =>
      DB.batch([
        DB.prepare("INSERT INTO b (v) VALUES ('c')"),
        DB.prepare("INSERT INTO no_such_table (v) VALUES ('d')"),
      ]));
    out.after = await attempt(async () =>
      (await DB.prepare("SELECT v FROM b ORDER BY id").all()).results);
    out.unique = await attempt(async () => {
      await DB.exec("CREATE TABLE IF NOT EXISTS u (id INTEGER PRIMARY KEY)");
      await DB.prepare("DELETE FROM u").run();
      await DB.prepare("INSERT INTO u (id) VALUES (1)").run();
      return await DB.prepare("INSERT INTO u (id) VALUES (1)").run();
    });
    return report(out);
  },

  // The retry accounting a Worker reads off `meta`.
  async "/meta"() {
    await DB.exec("CREATE TABLE IF NOT EXISTS m (id INTEGER PRIMARY KEY, v TEXT)");
    const run = await DB.prepare("INSERT INTO m (v) VALUES (?)").bind("x").run();
    const read = await DB.prepare("SELECT COUNT(*) AS n FROM m").first();
    return report({
      total_attempts: run.meta.total_attempts,
      changed_db: run.meta.changed_db,
      changes: run.meta.changes,
      rows_written: run.meta.rows_written,
      read: read.n,
    });
  },
};

export default {
  async fetch(request, env) {
    DB = env.DB;
    const { pathname } = new URL(request.url);
    const route = routes[pathname];
    if (!route) return new Response("not found", { status: 404 });
    try {
      return await route(request);
    } catch (error) {
      return new Response(JSON.stringify({ uncaught: errorInfo(error) }), {
        status: 500,
      });
    }
  },
};
