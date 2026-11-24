// A Durable Object that reports what type a boolean SQL bind is stored as,
// and what the documented migration from INTEGER to TEXT does to a column.
// Every answer is JSON with no timing in it, so the test compares text.

export class Store {
  constructor(state) {
    this.state = state;
  }

  async fetch(request) {
    const route = new URL(request.url).pathname;
    const sql = this.state.storage.sql;
    if (route === "/binds") return Response.json(this.binds(sql));
    if (route === "/prepare") return Response.json(this.prepareMigration(sql));
    if (route === "/rewrite") return Response.json(this.rewriteMigration(sql));
    if (route === "/read") return Response.json(this.readMigration(sql));
    return new Response("not found", { status: 404 });
  }

  binds(sql) {
    const out = {};
    // A bind's own type, with no table in the way.
    out.selectTrue = [...sql.exec("SELECT ? AS v, typeof(?) AS ty", true, true)][0];
    out.selectFalse = [...sql.exec("SELECT ? AS v, typeof(?) AS ty", false, false)][0];
    // The type a column keeps once the bind lands in it.
    sql.exec("DROP TABLE IF EXISTS flags");
    sql.exec("CREATE TABLE flags (id INTEGER PRIMARY KEY, flag)");
    sql.exec("INSERT INTO flags (flag) VALUES (?)", true);
    sql.exec("INSERT INTO flags (flag) VALUES (?)", false);
    out.rows = [...sql.exec("SELECT flag, typeof(flag) AS ty FROM flags ORDER BY id")];
    // A boolean bind in a predicate finds what that bind wrote, and nothing
    // the other representation wrote.
    out.whereTrue = sql.exec("SELECT count(*) AS n FROM flags WHERE flag = ?", true).one().n;
    out.whereFalse = sql.exec("SELECT count(*) AS n FROM flags WHERE flag = ?", false).one().n;
    out.whereInt = sql.exec("SELECT count(*) AS n FROM flags WHERE flag = 1").one().n;
    out.whereText = sql.exec("SELECT count(*) AS n FROM flags WHERE flag = 'true'").one().n;
    out.compare = sql.exec(
      "SELECT ? = 1 AS eqInt, ? = 'true' AS eqText, ? = ? AS eqSelf",
      true, true, true, false,
    ).one();
    // The hazard the migration note warns about: `if (row.flag)` is truthy
    // for the string "false".
    out.truthy = out.rows.map((row) => (row.flag ? "truthy" : "falsy"));
    return out;
  }

  // The rows a deployment at a date before the gate writes: boolean binds
  // that stay INTEGER 1/0.
  prepareMigration(sql) {
    sql.exec("DROP TABLE IF EXISTS m");
    sql.exec("CREATE TABLE m (id INTEGER PRIMARY KEY, flag)");
    sql.exec("INSERT INTO m (flag) VALUES (?)", true);
    sql.exec("INSERT INTO m (flag) VALUES (?)", false);
    return { rows: [...sql.exec("SELECT flag, typeof(flag) AS ty FROM m ORDER BY id")] };
  }

  // The documented rewrite, run before the compatibility date is bumped.
  rewriteMigration(sql) {
    const before = [...sql.exec("SELECT flag, typeof(flag) AS ty FROM m ORDER BY id")];
    sql.exec(
      "UPDATE m SET flag = CASE flag WHEN 1 THEN 'true' WHEN 0 THEN 'false' ELSE flag END",
    );
    const after = [...sql.exec("SELECT flag, typeof(flag) AS ty FROM m ORDER BY id")];
    return { before, after };
  }

  // Rows written after the bump join the rewritten ones in one column.
  readMigration(sql) {
    sql.exec("INSERT INTO m (flag) VALUES (?)", true);
    sql.exec("INSERT INTO m (flag) VALUES (?)", false);
    return {
      rows: [...sql.exec("SELECT flag, typeof(flag) AS ty FROM m ORDER BY id")],
      whereTrue: sql.exec("SELECT count(*) AS n FROM m WHERE flag = ?", true).one().n,
      whereFalse: sql.exec("SELECT count(*) AS n FROM m WHERE flag = ?", false).one().n,
      whereInt: sql.exec("SELECT count(*) AS n FROM m WHERE flag = 1").one().n,
    };
  }
}

export default {
  async fetch(request, env) {
    return env.STORE.getByName("store").fetch(request);
  },
};
