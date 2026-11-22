// The guest side of the ArrayBuffer budget gate.
//
// The node boots with `CELLO_V8_HEAP_LIMIT_MB=64` and
// `CELLO_V8_EXTERNAL_LIMIT_MB=320`, so a loop of `new Uint8Array(8 MiB)` must
// stop at 320 MiB -- five times the heap limit, above the 256 MiB the default
// four-heap multiple would allow, which is what proves the knob is read -- with
// a `RangeError`, instead of growing the process. Every route runs in one
// Durable Object so a later request sees the memory an earlier one left behind.
//
// The outcome travels in headers, not a body: a response body is itself a
// charged backing store, and an isolate at its ceiling cannot allocate one.
import { DurableObject } from "cloudflare:workers";

const MIB = 1024 * 1024;
const held = [];

export class Cap extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.held = [];
    this.filled = new Promise((resolve) => {
      this.markFilled = resolve;
    });
  }

  // Fill this isolate's budget to `fill` bytes, then wait up to `ms` for
  // `signal` to abort. The caller aborts with a reason this isolate cannot
  // pay for, so the abort has to arrive as a standard AbortError instead of
  // being dropped with the refused payload.
  async holdUntilAborted(signal, fill, ms) {
    const step = 8 * MIB;
    const keep = [];
    for (let got = 0; got + step <= fill; got += step) {
      keep.push(new Uint8Array(step).fill(1));
    }
    this.markFilled(true);
    const outcome = await new Promise((resolve) => {
      const timer = setTimeout(() => resolve({ aborted: false }), ms);
      const done = () => {
        clearTimeout(timer);
        const reason = signal.reason;
        resolve({
          aborted: true,
          name: reason instanceof Error ? reason.name : typeof reason,
          message: reason instanceof Error ? reason.message : "",
        });
      };
      if (signal.aborted) done();
      else signal.addEventListener("abort", done, { once: true });
    });
    keep.length = 0;
    return outcome;
  }

  // Resolves once `holdUntilAborted` holds its memory and is subscribed.
  whenFilled() {
    return this.filled;
  }

  answer() {
    return new Response(null, {
      status: 200,
      headers: {
        "x-grown": String(this.grown ?? 0),
        "x-refused": this.refused ?? "",
      },
    });
  }

  async fetch(request) {
    const url = new URL(request.url);
    switch (url.pathname) {
      // Grow in 8 MiB steps to `target`, reporting the first refusal. The
      // buffers are released before answering so the node is left as the
      // request found it; a collection returns them to the budget.
      case "/grow": {
        const step = 8 * MIB;
        const target = Number(url.searchParams.get("target") ?? 0);
        this.grown = 0;
        this.refused = "";
        while (this.grown + step <= target) {
          try {
            const view = new Uint8Array(step);
            view.fill(1);
            this.held.push(view);
            this.grown += step;
          } catch (error) {
            this.refused = `${error.name}: ${error.message}`;
            break;
          }
        }
        this.held.length = 0;
        return this.answer();
      }
      // Allocate and drop `count` times: churn must never be refused.
      case "/churn": {
        const step = 8 * MIB;
        const count = Number(url.searchParams.get("count") ?? 0);
        this.grown = 0;
        this.refused = "";
        for (let index = 0; index < count; index++) {
          try {
            const view = new Uint8Array(step);
            view.fill(1);
            this.grown += step;
          } catch (error) {
            this.refused = `${error.name}: ${error.message}`;
            break;
          }
        }
        return this.answer();
      }
      // Every legitimate per-request payload together: a 64 MiB body or /tmp
      // read, a 32 MiB WebSocket message, a 32 MiB tee budget and a clone.
      case "/legit": {
        this.grown = 0;
        this.refused = "";
        try {
          const body = new Uint8Array(64 * MIB).fill(1);
          const message = new Uint8Array(32 * MIB).fill(2);
          const tee = new Uint8Array(32 * MIB).fill(3);
          const cloned = structuredClone(tee);
          this.held.push(body, message, tee, cloned);
          this.grown =
            body.byteLength + message.byteLength + tee.byteLength +
            cloned.byteLength;
        } catch (error) {
          this.refused = `${error.name}: ${error.message}`;
        }
        this.held.length = 0;
        return this.answer();
      }
    }
    return new Response("not found", { status: 404 });
  }
}

export default {
  fetch(request, env) {
    const url = new URL(request.url);
    // The node has no `/health` of its own; every black-box suite serves one
    // from its fixture, which is what boot waits for.
    if (url.pathname === "/health") return new Response("ok");
    // An abort whose reason the receiving isolate cannot pay for: the Durable
    // Object holds most of its budget while it waits on the signal, and the
    // reason is larger than what is left.
    if (url.pathname === "/unpayable-abort") return unpayableAbort(env);
    return env.CAP.get(env.CAP.idFromName("budget")).fetch(request);
  },
};

async function unpayableAbort(env) {
  const stub = env.CAP.get(env.CAP.idFromName("signal"));
  const controller = new AbortController();
  const pending = stub.holdUntilAborted(controller.signal, 296 * MIB, 10000);
  await stub.whenFilled();
  controller.abort(new Uint8Array(40 * MIB));
  return Response.json(await pending);
}
