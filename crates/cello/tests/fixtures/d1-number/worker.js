// What a D1 bind stores, and what a column keeps of it. Every answer is the
// type SQLite recorded plus the value, so the test compares JSON text.

const TYPES = [
  ["zero", 0],
  ["one", 1],
  ["negOne", -1],
  ["two53", 2 ** 53],
  ["frac", 1.5],
  ["nan", NaN],
  ["inf", Infinity],
  ["boolTrue", true],
  ["boolFalse", false],
  ["null", null],
  ["text", "1"],
];

export default {
  async fetch(request, env) {
    const db = env.DB;
    const route = new URL(request.url).pathname;
    if (route === "/binds") return Response.json(await binds(db));
    if (route === "/affinity") return Response.json(await affinity(db));
    if (route === "/lookup") return Response.json(await lookup(db));
    if (route === "/prepare") return Response.json(await prepare(db));
    if (route === "/rewrite") return Response.json(await rewrite(db));
    if (route === "/read") return Response.json(await read(db));
    return new Response("not found", { status: 404 });
  },
};

// A bind's own type, with no table in the way.
async function binds(db) {
  const out = {};
  for (const [name, value] of TYPES) {
    const row = await db
      .prepare("SELECT ? AS v, typeof(?) AS ty")
      .bind(value, value)
      .first();
    out[name] = row;
  }
  return out;
}

// The type a column keeps of a bound 1, by column type.
async function affinity(db) {
  await db.exec("DROP TABLE IF EXISTS aff");
  await db.exec("CREATE TABLE aff (plain, i INTEGER, r REAL, t TEXT, n NUMERIC)");
  await db.prepare("INSERT INTO aff (plain, i, r, t, n) VALUES (?, ?, ?, ?, ?)")
    .bind(1, 1, 1, 1, 1)
    .run();
  return await db.prepare(
    "SELECT plain, typeof(plain) AS plain_ty, i, typeof(i) AS i_ty, r, " +
    "typeof(r) AS r_ty, t, typeof(t) AS t_ty, n, typeof(n) AS n_ty FROM aff",
  ).first();
}

// A whole number may bind REAL, but a predicate on it still finds INTEGER
// rows, and the pagination clauses keep working.
async function lookup(db) {
  await db.exec("DROP TABLE IF EXISTS ids");
  await db.exec("CREATE TABLE ids (id INTEGER PRIMARY KEY, name TEXT)");
  await db.prepare("INSERT INTO ids (name) VALUES ('a'), ('b'), ('c')").run();
  const out = {};
  out.whereInt = (await db.prepare("SELECT name FROM ids WHERE id = ?").bind(2).all()).results;
  out.whereMiss = (await db.prepare("SELECT name FROM ids WHERE id = ?").bind(2.5).all()).results;
  out.limit = (await db.prepare("SELECT id FROM ids ORDER BY id LIMIT ?").bind(2).all()).results;
  out.offset = (await db.prepare("SELECT id FROM ids ORDER BY id LIMIT 2 OFFSET ?").bind(1).all()).results;
  out.limitFraction = await attempt(async () =>
    (await db.prepare("SELECT id FROM ids LIMIT ?").bind(2.5).all()).results);
  const inserted = await db.prepare("INSERT INTO ids (name) VALUES (?)").bind("d").run();
  out.insert = { changes: inserted.meta.changes, last_row_id: inserted.meta.last_row_id };
  const stmt = db.prepare("SELECT ? AS v, typeof(?) AS ty");
  out.batch = await db.batch([stmt.bind(3, 3), stmt.bind(3.5, 3.5)]);
  out.raw = await db.prepare("SELECT ? AS v, typeof(?) AS ty").bind(1, 1).raw();
  out.firstColumn = await db.prepare("SELECT ? AS v, typeof(?) AS ty").bind(7, 7).first("ty");
  out.text = await db.prepare("SELECT ? || '' AS rendered").bind(1).first();
  return out;
}

// The migration in docs/services/d1.md: rows written before the gate hold
// INTEGER; the rewrite normalizes them to REAL before the date moves.
async function prepare(db) {
  await db.exec("DROP TABLE IF EXISTS m");
  await db.exec("CREATE TABLE m (id INTEGER PRIMARY KEY, plain)");
  await db.prepare("INSERT INTO m (plain) VALUES (?)").bind(1).run();
  await db.prepare("INSERT INTO m (plain) VALUES (?)").bind(0).run();
  return await rows(db);
}

async function rewrite(db) {
  await db.exec(
    "UPDATE m SET plain = CAST(plain AS REAL) WHERE typeof(plain) = 'integer'",
  );
  return await rows(db);
}

// Rows written after the bump join the rewritten ones in one column.
async function read(db) {
  await db.prepare("INSERT INTO m (plain) VALUES (?)").bind(1).run();
  await db.prepare("INSERT INTO m (plain) VALUES (?)").bind(0).run();
  return await rows(db);
}

async function rows(db) {
  const result = await db
    .prepare("SELECT plain, typeof(plain) AS ty FROM m ORDER BY id")
    .all();
  const found = async (value) =>
    (await db.prepare("SELECT count(*) AS n FROM m WHERE plain = ?").bind(value).all())
      .results[0].n;
  return { rows: result.results, whereOne: await found(1), whereZero: await found(0) };
}

const attempt = async (fn) => {
  try {
    return await fn();
  } catch (error) {
    return { error: String(error && error.message) };
  }
};
