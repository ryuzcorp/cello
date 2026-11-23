// Modified by Ryuz Corp (Cello): Queue capacity-refusal fixture. Apache-2.0.
// A Worker whose Queue consumer a full isolate can refuse.
//
// `/fill` retains ArrayBuffer memory in module scope, so it outlives the
// request that made it: the stateless pool reuses that warm isolate for the
// next queue dispatch, and the batch bodies then do not fit the isolate's
// `CELLO_V8_EXTERNAL_LIMIT_MB` budget until `/free` drops the buffers. The
// dispatch never reaches the handler, so the message was never delivered.
//
// `refuse-src` runs with `max_retries: 0` and a dead-letter queue: a refusal
// wrongly charged as a delivery attempt exhausts the message on the spot and
// moves it to `refuse-dst` without the handler ever running. `retry-src`
// always throws, which is a consumer failure and must keep spending attempts.
const J = (o, st = 200) => Response.json(o, { status: st });

const held = [];
const deliveries = [];

export default {
  async queue(batch) {
    for (const message of batch.messages) {
      const body = message.body;
      deliveries.push({
        queue: batch.queue,
        id: message.id,
        attempts: message.attempts,
        size: typeof body === "string" ? body.length : body.byteLength,
      });
    }
    if (batch.queue === "retry-src") {
      throw new Error("retry-src fails on purpose");
    }
    batch.ackAll();
  },

  async fetch(req, env) {
    const url = new URL(req.url);
    const t = url.pathname.slice(1);
    // `/free` answers with no body on purpose: the retained buffers are
    // released by the collector at a later turn boundary, so at this instant
    // the isolate is still at its ceiling and even a small response body is
    // refused.
    if (t === "free") {
      held.length = 0;
      return new Response(null, { status: 204 });
    }
    const run = async () => {
      switch (t) {
        case "health":
          return { ok: true };
        case "fill": {
          const mib = Number(url.searchParams.get("mib") ?? "1");
          held.push(new Uint8Array(mib * 1024 * 1024));
          return { held: held.length, mib };
        }
        case "send": {
          const queue = url.searchParams.get("queue") ?? "refuse-src";
          const binding = queue === "retry-src" ? env.Q_RETRY : env.Q_SRC;
          const n = Number(url.searchParams.get("n") ?? "1");
          const size = Number(url.searchParams.get("size") ?? "16");
          const body = "x".repeat(size);
          for (let i = 0; i < n; i++) {
            await binding.send(body, { contentType: "text" });
          }
          return { sent: n, size };
        }
        case "deliveries":
          return { deliveries };
        case "metrics": {
          const refuse = await env.Q_SRC.metrics();
          const refuseDlq = await env.Q_DST.metrics();
          const retryDlq = await env.Q_RETRY_DST.metrics();
          return {
            refuse: { backlog: refuse.backlogCount },
            refuseDlq: { backlog: refuseDlq.backlogCount },
            retryDlq: { backlog: retryDlq.backlogCount },
          };
        }
        default:
          return { error: `unknown ${t}` };
      }
    };
    try {
      return J({ test: t, ok: true, result: await run() });
    } catch (e) {
      return J({ test: t, ok: false, error: `${e.name}: ${e.message}` }, 500);
    }
  },
};
