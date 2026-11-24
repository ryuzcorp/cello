---
title: Durable Objects
description: Keep per-name state in a cell, and reach it from any node by stub.
---
<!-- Modified by Ryuz Corp (Cello): stored-stub upgrade note. -->

A Durable Object is a single-threaded actor with its own SQLite database. Cello calls each running Durable Object a **cell**: exactly one node serves it at a time. Read the [Cloudflare Durable Objects documentation](https://developers.cloudflare.com/durable-objects/) for the standard API.

## Run a minimal counter

This example keeps one counter per name, and `idFromName()` gives each name a separate counter. It runs unchanged on `cello dev` (verified with cello 0.2.1).

The full runnable version is in [`examples/counter`](https://github.com/ryuzcorp/cello/tree/main/examples/counter).

<!-- cello-example: counter -->

```jsonc
// wrangler.jsonc
{
  "name": "counter",
  "main": "index.js",
  "compatibility_date": "2026-01-01",
  "durable_objects": {
    "bindings": [{ "name": "COUNTER", "class_name": "Counter" }]
  },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["Counter"] }]
}
```

```js
// index.js
export class Counter {
  constructor(state) {
    this.state = state;
  }
  async fetch(request) {
    const n = ((await this.state.storage.get("n")) ?? 0) + 1;
    await this.state.storage.put("n", n);
    return Response.json({ n });
  }
}

export default {
  async fetch(request, env) {
    const name = new URL(request.url).searchParams.get("name") ?? "default";
    const id = env.COUNTER.idFromName(name);
    return env.COUNTER.get(id).fetch(request);
  },
};
```

```sh
cello dev .
curl "http://127.0.0.1:9876/?name=a"   # {"n":1}
curl "http://127.0.0.1:9876/?name=a"   # {"n":2}
curl "http://127.0.0.1:9876/?name=b"   # {"n":1}
```

## Use the supported API

- `env.COUNTER.idFromName(name)` returns an id, and `env.COUNTER.get(id)` returns a stub. `getByName(name)` performs both steps.
- `newUniqueId()` and `idFromString()` create and parse a random id.
- `ctx.storage.get()`, `put()`, `delete()`, `list()`, and `deleteAll()` are the key-value methods. `put(entries, options)` takes a plain object whose own enumerable string keys are the pairs, as Cloudflare's single `put(key, value, options)` signature resolves it: an `undefined` value leaves the key out, a `Map` or a `Set` writes nothing, and an array is not a dictionary -- it reads as the single-pair form, with its key coerced to a string. `list()` converts its options the way Cloudflare does: the string fields take any value, `reverse` is a boolean coercion, `limit` is a positive `int` of at most 2,147,483,647, and `start` with `startAfter` is refused.
- `ctx.storage.sql.exec()` runs SQL on the database of the object.
- `transaction()` and `transactionSync()` group several writes.
- `storage.setAlarm()`, `getAlarm()`, and `deleteAlarm()` manage one alarm per object, and Cello calls the `alarm(alarmInfo)` handler.
- `ctx.acceptWebSocket()` accepts a hibernatable WebSocket, and Cello calls `webSocketMessage()` for each frame. `ctx.setHibernatableWebSocketEventTimeout()` sets the hibernatable WebSocket event timeout with workerd's conversion, and `ctx.getHibernatableWebSocketEventTimeout()` reports it.
 - `blockConcurrencyWhile()` closes the input gate, and a non-function argument is refused the way workerd refuses it: the call answers a rejected promise carrying WebIDL's message instead of throwing. A block taken inside another block's body is a nested section: its callback starts on a later turn, after the calling body's microtasks and after the enclosing block's promise has settled, and the outer section stays open over it until it drains. The promise `blockConcurrencyWhile()` answers settles when the body settles, before a nested block runs. A throw or reject in the body -- top-level or nested -- the 64-level nesting limit and the 30-second limit break the whole event instead: a `catch` around the block never runs, the stub caller receives what the block threw, over `fetch` and over RPC alike: an Error keeps its class, name and message (a `TypeError` stays a `TypeError`, a `DOMException` keeps its name), and a non-Error throw arrives as an `Error` carrying the value as its message. The first `fetch` call carries workerd's `remote` marker; repeats on that stub, and every RPC rejection, carry none. The object resets: a stub that observed the aborted object stays broken for later calls, while a fresh stub to the reset object works, as on workerd. A failure over an open transaction rolls it back, so uncommitted writes stay invisible and the next transaction starts clean.
- `storage.sync()` waits for the durability of the earlier committed writes.
- `storage.getCurrentBookmark()`, `getBookmarkForTime()`, and `onNextSessionRestoreBookmark()` are the session-bookmark methods. A local storage back-end has no point-in-time recovery, so the last two refuse with workerd's own error, and `getCurrentBookmark()` answers a per-object bookmark: see [Bookmarks](#bookmarks).
- `ctx.props` is a facet's props, and `{}` for a class declared without props.
- `ctx.exports` holds one callable handle per module export, as it does in a stateless entrypoint: a Durable Object class handle is also its namespace (`getByName`, `get`, `idFromName`, ...) and specializes for a facet when called, and `ctx.exports.default.fetch()` reaches the default export. See the [Cloudflare compatibility](../reference/cloudflare-compat.md#context) page for the handle shapes.

## Address a cell

Cello creates the object at the first call on a stub, not at `get()`.

`idFromName()` derives a 64-digit hexadecimal id with HMAC-SHA-256 over the name, under a key that Cello builds from the script name and the class name. One name therefore reaches one object from every node. `ctx.id.name` holds the name when it is 1024 UTF-8 bytes or less, as on Cloudflare; a longer name still routes correctly.

`newUniqueId()` draws random bytes, so keep its `toString()` form to reach the object again. `idFromString()` verifies the HMAC, so Cello refuses an id from a different namespace. A rename of the Worker script reaches new, empty objects, and the old objects keep their storage under the old ids.

## Rely on one owner at a time

Exactly one node serves a cell at a time. A node claims a cell with a conditional write of an ownership record to the fleet bucket. Each activation advances a fencing epoch in the storage prefix, so a node that lost the cell writes only into a superseded prefix. The [durability](../concepts/durability.md) page gives the full mechanism. Cello forwards each stub call to the owner node.

Inside the cell, one synchronous turn runs at a time. An event that awaits can overlap another unless `blockConcurrencyWhile()` closes the input gate. Cello holds a response until a durability proof covers every write the response can reveal, as the Cloudflare [output gate](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/) does, so you do not have to `await` a `put()`.

A cell keeps no in-memory state across an eviction, so the constructor runs again at the next event. Ownership moves when a node stops or drains, or when idle rebalancing moves a hibernated cell. A hibernatable WebSocket survives hibernation on the same node, and it closes when the cell moves, so the client reconnects.

## Store durably and fire alarms

A class that a `new_sqlite_classes` migration declares can use the key-value methods and `ctx.storage.sql.exec()`. The `transactionSync()` callback receives no argument, as in workerd, and a throw rolls it back. A transaction can start a nested transaction; a failed nested transaction discards only its own writes, and the enclosing transaction can still commit.

### Bookmarks

Cloudflare's session-bookmark methods describe a point-in-time-recovery
position. A local storage back-end has none, and workerd's own local back-end
answers the same way Cello does, so code that probes the API behaves
identically on both.

`storage.getCurrentBookmark()` resolves to
`00000000-<sequence>-00000000-<32 zero digits>`. The sequence is the object's
own counter, and what advances it is a call to `getCurrentBookmark()`: a write
does not, and `deleteAll()` starts it over at 0. Two consecutive calls
therefore answer different bookmarks, and a bookmark read after a write says
nothing about that write. `getBookmarkForTime()` and
`onNextSessionRestoreBookmark()` convert their argument and then reject with
`This Durable Object's storage back-end does not implement point-in-time
recovery.` An argument of the wrong type is refused first, with workerd's
`parameter 1 is not of type 'date'` or `'string'` message; a `NaN` or
`Infinity` time is refused with `The value cannot be converted because it is
not a valid Date.`

Nothing in Cello restores an object to a bookmark: take backups at the bucket,
where the cell's SQLite file lives, rather than through this API.

### Boolean binds

Workerd stores a `true`/`false` bind as the TEXT `'true'`/`'false'`. Cello wrote INTEGER `1`/`0` up to 0.2.9. From compatibility date `2026-11-24` Cello matches workerd, so `typeof(flag)` is `'text'`, `flag = 'true'` is true, and a `WHERE flag = ?` with a boolean finds the rows a boolean wrote. A compatibility date before that, or none at all, keeps INTEGER, so the rows a deployment already stores keep their meaning. `sql_boolean_text` forces the TEXT type and `no_sql_boolean_text` forces INTEGER, at any date; setting both stops the deployment at startup. The switch is fixed when a script's isolate loads, so one deployment never changes the type under a running instance.

**Bumping an existing deployment's date changes the type of the rows it writes from then on.** Old rows stay INTEGER `0`/`1`, new rows are TEXT `'false'`/`'true'`, and the two representations disagree in both languages: `'false'` is truthy, so `if (row.flag)` runs for it while an old `0` does not, and `flag = 1` and `flag = ?` (with a boolean) each match only their own rows. Before you bump the date, either:

- pin `no_sql_boolean_text` and keep reading INTEGER, or
- rewrite the column while the deployment still writes INTEGER:

  ```sql
  UPDATE flags SET flag = CASE flag WHEN 1 THEN 'true' WHEN 0 THEN 'false' ELSE flag END;
  ```

  Every `1`/`0` becomes `'true'`/`'false'`; a `NULL`, a text value or another number keeps its value. A `CHECK` constraint that allows only `0`/`1` makes the `UPDATE` fail instead of leaving a half-typed column. Cello's SQLite cannot drop such a constraint in place, so rebuild the table without it, copy the rows, and re-create the `CHECK` for the new values. Bump the date after the rewrite, so the binds that follow write TEXT too.

  The previous version keeps serving until the new one owns the cell, and every boolean it writes in that window is still INTEGER. Run the same `UPDATE` once more after the bumped version owns the cell; it is idempotent, because TEXT rows fall through `ELSE flag`. Alternatively, keep the cell idle from the rewrite until the takeover.

`tests/do_sql_boolean_compat.rs` runs that sequence against a real cell: INTEGER rows written at an old date, the rewrite, then the bumped date, ending with every row TEXT and a boolean predicate finding all of them.

Cello captures each write as an LTX segment and replicates it to `cells/<cell>/ltx/e<epoch>/` in the fleet bucket. In a fleet of two or more nodes, the owner answers when each of its one or two followers holds the write on disk, and the bucket upload follows. On a single node, every write waits for the object store.

Cello does not answer a successful `setAlarm()` until a durable wake entry in the bucket covers the alarm. A hibernated cell fires its alarm on its owner node. One node holds the waker role, and it wakes only a cell whose owner stopped.

Stubs kept in storage (`ctx.storage.put()` of a Durable Object stub) use a versioned envelope. Rows written before the binding-hardening release read back as plain data once; storing the stub again writes the current envelope and restores the live stub.

A value whose state lives in a host object is refused rather than stored as `{}`: a `URL`, `URLSearchParams`, `Request`, `Response`, `Headers`, `FormData`, `Blob`, `ReadableStream` (and its writable/transform relatives), `CryptoKey`, `MessagePort`, `AbortSignal` or an R2 bucket binding throws a `DataCloneError` naming the interface. Workerd refuses the `URL`, `URLSearchParams`, `FormData` and stream values too; it accepts `Headers` and `Blob` and stores a value, so Cello is the stricter of the two there. A plain object that merely inherits one of those prototypes is data, as on workerd, and storing one never runs its getters.

## Handle the differences from Cloudflare

| Area | Cello behavior |
| --- | --- |
| Placement and jurisdiction | None. `newUniqueId({ jurisdiction })` and `namespace.jurisdiction()` throw. |
| Namespace identity | The key contains the script name, so a script rename changes every `idFromName()` id. |
| Migrations | A `migrations` entry accepts `tag` and `new_sqlite_classes` only. A class rename, delete, or transfer stops the deployment. |
| Background work | A Durable Object event keeps pending I/O active after the handler returns; no `ctx.waitUntil()` needed. `waitUntil()` can add work while earlier work stays active. |
| RPC stubs | A stub cannot cross an isolate boundary. See [RPC](../reference/cloudflare-compat.md#rpc). |
| Outbound WebSockets | Do not continue after the object moves to another node. |
| `SqlStorage.Cursor.toArray()` | Gives a Cello-specific error near the V8 heap limit. |
| `storage.sync()` | Waits for the object store or the fleet ensemble to hold all earlier committed writes, within the shorter of the 10-second `CELLO_LTX_DURABILITY_TIMEOUT_SECS` default and the 15-second `CELLO_OPERATION_DEADLINE_MS` default. It rejects in an open transaction and after an abort; without an object store it resolves after the local commit. |
 | Limits | A transaction and `blockConcurrencyWhile()` have a 30-second limit. A timeout, or a block failure over an open transaction, resets the object and rolls the transaction back; a block past its limit also breaks the event, so the stub caller receives the limit rejection instead of the handler's answer. |
| Failed handlers | Still wait for the durability of any exposed value or write. A Cello-generated failure that exposes no object value returns immediately. |
| Session bookmarks | `getCurrentBookmark()` answers a per-object bookmark that a call to itself advances and `deleteAll()` resets; `getBookmarkForTime()` and `onNextSessionRestoreBookmark()` reject, because Cello has no point-in-time recovery, as workerd's local back-end also reports. |
| Hibernatable event timeout | `setHibernatableWebSocketEventTimeout()` stores and reports the value with workerd's conversion and one-week bound, but Cello does not cut a hibernatable WebSocket event short when the timeout passes. |
| Stub `connect()` | `stub.connect()` converts its address and options exactly as workerd does -- the `SocketAddress` dictionary or a `host:port` string split by the URL parser, with workerd's refusal messages -- and then fails: Cello has no Fetcher TCP transport for a Durable Object stub, so no socket opens. |
| Binding reflection | `Object.keys()`, `JSON.stringify()` and every enumerable member of a storage, state, id, namespace, stub, SQL storage, cursor or synchronous KV view answer what workerd's do, and no host-internal member is on those prototypes: `ctx` publishes workerd's own state members only. Cello keeps its internals as non-enumerable properties, so `Object.getOwnPropertyNames()` of a binding is not identical. |
| SQL cursors | Outside a transaction, a SQL write cursor must finish before a response, an outbound effect, or `storage.sync()`. An unfinished `RETURNING` cursor holds uncommitted writes, so Cello rejects that output. A read cursor can stay open. |
| SQL cursor counters | `rowsRead`/`rowsWritten` count Cello's rows read by the cursor and rows changed by DML. Workerd counts libsql's b-tree rows, so a DDL statement reports 1 read and 2 writes there and 0/0 here. A cursor has no own property, its `columnNames` is an enumerable accessor, and it has no `columns` member, as on workerd. |

The [Cloudflare compatibility](../reference/cloudflare-compat.md#services) page lists the runtime APIs and the unsupported services.

## Operate it

- `cello dev .` runs the project locally with persistent state in `.cello/dev`. Pass `--clean` to start empty.
- `cello cell list` shows the cells of a deployed fleet. Durability timeouts come from `CELLO_LTX_DURABILITY_TIMEOUT_SECS` and `CELLO_OPERATION_DEADLINE_MS`.
