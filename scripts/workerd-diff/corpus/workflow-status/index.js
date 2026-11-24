import { WorkflowEntrypoint } from "cloudflare:workers";

export class Quick extends WorkflowEntrypoint {
  async run(_event, step) {
    return await step.do("one", async () => ({ done: 1 }));
  }
}

export class Boom extends WorkflowEntrypoint {
  async run(_event, _step) {
    throw new Error("bang");
  }
}

export class Blocker extends WorkflowEntrypoint {
  async run(_event, step) {
    const ev = await step.waitForEvent("hold", { type: "go" });
    return await step.do("finish", async () => `got-${ev.payload?.n ?? "?"}`);
  }
}

const J = (o) => Response.json(o);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const attempt = async (fn) => {
  try {
    return { ok: await fn() };
  } catch (e) {
    return { error: String((e && e.message) || e) };
  }
};
// Both runtimes now answer the same error text on the covered paths, so the
// gate compares the message itself, not just the verdict. Anything still
// cello-strict (malformed sendEvent payloads, restart options) stays out.
const verdict = async (fn) => {
  try {
    await fn();
    return "ok";
  } catch (e) {
    return String((e && e.message) || e);
  }
};
const poll = async (fn, cond, tries = 150, ms = 200) => {
  let v;
  for (let i = 0; i < tries; i++) {
    v = await fn();
    if (cond(v)) return v;
    await sleep(ms);
  }
  return v;
};
const settled = (s) => s !== undefined && s.status !== "queued" && s.status !== "running";
const mark = (s) => ({
  keys: Object.keys(s).sort(),
  status: s.status,
  errorState: s.error === undefined ? "undef" : s.error === null ? "null" : "value",
  error: s.error ?? null,
  outputState: s.output === undefined ? "undef" : s.output === null ? "null" : "value",
  output: s.output ?? null,
});
const snap = async (instance) => mark(await instance.status());
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/shape-terminal") {
      const q = await env.QUICK.create({ id: "st-q", params: {} });
      const b = await env.BOOM.create({ id: "st-b", params: {} });
      const complete = await poll(() => q.status().then(mark), (v) => v.status === "complete");
      const errored = await poll(() => b.status().then(mark), (v) => v.status === "errored");
      return J({ complete, errored });
    }
    if (url.pathname === "/shape-live") {
      const w = await env.BLOCK.create({ id: "sl-w", params: {} });
      const p = await env.BLOCK.create({ id: "sl-p", params: {} });
      await poll(() => w.status(), (v) => settled(v) && v.status === "waiting");
      await poll(() => p.status(), (v) => settled(v) && v.status === "waiting");
      await p.pause();
      return J({ waiting: await snap(w), paused: await snap(p) });
    }
    if (url.pathname === "/send-terminal") {
      const q = await env.QUICK.create({ id: "se-q", params: {} });
      const b = await env.BOOM.create({ id: "se-b", params: {} });
      const t = await env.BLOCK.create({ id: "se-t", params: {} });
      await poll(() => q.status(), (v) => v.status === "complete");
      await poll(() => b.status(), (v) => v.status === "errored");
      await poll(() => t.status(), (v) => settled(v) && v.status === "waiting");
      await t.terminate();
      const sendComplete = await verdict(() => q.sendEvent({ type: "go", payload: { n: 1 } }));
      const sendErrored = await verdict(() => b.sendEvent({ type: "go", payload: { n: 1 } }));
      const sendTerminated = await verdict(() => t.sendEvent({ type: "go", payload: { n: 1 } }));
      return J({
        sendComplete,
        afterComplete: await snap(q),
        sendErrored,
        afterErrored: await snap(b),
        sendTerminated,
        afterTerminated: await snap(t),
      });
    }
    if (url.pathname === "/lifecycle-a") {
      const a1 = await env.BLOCK.create({ id: "lc-a1", params: {} });
      const a2 = await env.BLOCK.create({ id: "lc-a2", params: {} });
      const q = await env.QUICK.create({ id: "lc-q", params: {} });
      const e = await env.BOOM.create({ id: "lc-e", params: {} });
      await poll(() => a1.status(), (v) => settled(v) && v.status === "waiting");
      await poll(() => a2.status(), (v) => settled(v) && v.status === "waiting");
      await poll(() => q.status(), (v) => v.status === "complete");
      await poll(() => e.status(), (v) => v.status === "errored");
      const out = {};
      out.pauseWaiting = await verdict(() => a1.pause());
      out.pausedShape = await snap(a1);
      out.resumePaused = await verdict(() => a1.resume());
      await poll(() => a1.status(), (v) => settled(v) && v.status === "waiting");
      out.waitingAfterResume = await snap(a1);
      out.terminateWaiting = await verdict(() => a2.terminate());
      out.terminatedShape = await snap(a2);
      out.pauseTerminated = await verdict(() => a2.pause());
      out.resumeTerminated = await verdict(() => a2.resume());
      out.restartTerminated = await verdict(() => a2.restart());
      await poll(() => a2.status(), (v) => settled(v) && v.status === "waiting");
      out.waitingAfterRestart = await snap(a2);
      out.restartComplete = await verdict(() => q.restart());
      out.completeAfterRestart = await poll(() => q.status().then(mark), (v) => v.status === "complete");
      out.pauseComplete = await verdict(() => q.pause());
      out.resumeComplete = await verdict(() => q.resume());
      out.terminateComplete = await verdict(() => q.terminate());
      out.pauseErrored = await verdict(() => e.pause());
      out.resumeErrored = await verdict(() => e.resume());
      out.terminateErrored = await verdict(() => e.terminate());
      out.restartErrored = await verdict(() => e.restart());
      out.erroredAfterRestart = await poll(() => e.status().then(mark), (v) => v.status === "errored");
      return J(out);
    }
    if (url.pathname === "/lifecycle-b") {
      const out = {};
      const s1 = await env.BLOCK.create({ id: "lc-s1", params: {} });
      await poll(() => s1.status(), (v) => settled(v) && v.status === "waiting");
      await s1.pause();
      out.sendPaused = await verdict(() => s1.sendEvent({ type: "go", payload: { n: 5 } }));
      out.pausedAfterSend = await snap(s1);
      out.resumeBuffered = await verdict(() => s1.resume());
      out.bufferedResult = await poll(() => s1.status().then(mark), (v) => v.status === "complete");
      const s2 = await env.BLOCK.create({ id: "lc-s2", params: {} });
      out.sendEarly = await verdict(() => s2.sendEvent({ type: "go", payload: { n: 6 } }));
      out.earlyResult = await poll(() => s2.status().then(mark), (v) => v.status === "complete");
      const p = await env.BLOCK.create({ id: "lc-p", params: {} });
      await poll(() => p.status(), (v) => settled(v) && v.status === "waiting");
      await p.pause();
      out.pausePaused = await verdict(() => p.pause());
      out.restartPaused = await verdict(() => p.restart());
      out.waitingAfterRestartPaused = await poll(() => p.status().then(mark), (v) => settled(v) && v.status === "waiting");
      out.terminatePaused = await verdict(() => p.terminate());
      out.terminatedAfterTerminatePaused = await snap(p);
      const w2 = await env.BLOCK.create({ id: "lc-w2", params: {} });
      await poll(() => w2.status(), (v) => settled(v) && v.status === "waiting");
      out.resumeWaiting = await verdict(() => w2.resume());
      out.restartWaiting = await verdict(() => w2.restart());
      out.waitingAfterRestartWaiting = await poll(() => w2.status().then(mark), (v) => settled(v) && v.status === "waiting");
      const t2 = await env.BLOCK.create({ id: "lc-t2", params: {} });
      await poll(() => t2.status(), (v) => settled(v) && v.status === "waiting");
      await t2.terminate();
      out.terminateTerminated = await verdict(() => t2.terminate());
      return J(out);
    }
    if (url.pathname === "/binding") {
      const out = {};
      out.getMissing = await verdict(() => env.QUICK.get("bd-absent"));
      out.createInvalidId = await verdict(() => env.QUICK.create({ id: "-bad" }));
      const d = await env.QUICK.create({ id: "bd-dup", params: {} });
      await poll(() => d.status(), (v) => v.status === "complete");
      out.createDup = await verdict(() => env.QUICK.create({ id: "bd-dup", params: {} }));
      out.createBatchEmpty = await verdict(() => env.QUICK.createBatch([]));
      const big = Array.from({ length: 101 }, (_, i) => ({ id: `bd-big-${i}` }));
      out.createBatch101 = await verdict(() => env.QUICK.createBatch(big));
      out.deleteBatchNonArray = await verdict(() => env.QUICK.deleteBatch("bd-x"));
      const bigDel = Array.from({ length: 101 }, (_, i) => `bd-del-${i}`);
      out.deleteBatch101 = await verdict(() => env.QUICK.deleteBatch(bigDel));
      out.createBatchDupInBatch = await attempt(async () =>
        (await env.QUICK.createBatch([{ id: "bd-d0" }, { id: "bd-d0" }])).map((i) => i.id),
      );
      out.createBatch2 = await attempt(async () =>
        (await env.QUICK.createBatch([{ id: "bd-c0" }, { id: "bd-c1" }])).map((i) => i.id),
      );
      out.getBatchCreated = await attempt(() => env.QUICK.get("bd-c0").then((i) => i.status()).then(mark));
      out.deleteBatch = await attempt(() =>
        env.QUICK.deleteBatch(["bd-c0", "bd-c1", "bd-never-here", "-bad", "bd-c0"]),
      );
      out.getAfterDelete = await verdict(() => env.QUICK.get("bd-c0"));
      out.getMalformed = await verdict(() => env.QUICK.get("-bad"));
      out.getEmpty = await verdict(() => env.QUICK.get(""));
      const gone = await env.QUICK.create({ id: "bd-gone", params: {} });
      await poll(() => gone.status(), (v) => v.status === "complete");
      await env.QUICK.deleteBatch(["bd-gone"]);
      out.pauseDeleted = await verdict(() => gone.pause());
      out.terminateDeleted = await verdict(() => gone.terminate());
      out.sendEventDeleted = await verdict(() => gone.sendEvent({ type: "x", payload: 1 }));
      out.statusDeleted = await verdict(() => gone.status());
      return J(out);
    }
    return new Response("not found", { status: 404 });
  },
};
