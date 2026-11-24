// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by Ryuz Corp (Cello): `blockConcurrencyWhile`'s limit fixture (#261).

// A cell that holds a critical section past the limit, and one that returns
// well inside it. `instance` counts the objects the node built for this
// class, so the reset the timeout performs is visible: the next event runs on
// a fresh instance with its own counters.
import { DurableObject } from "cloudflare:workers";

let instances = 0;

export class Holder extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.instance = ++instances;
    this.events = 0;
  }
  async fetch(request) {
    const url = new URL(request.url);
    this.events += 1;
    if (url.pathname === "/hold") {
      const started = Date.now();
      // A body that never settles: only the limit ends it, and the caller
      // learns about it through the promise rather than a throw.
      const outcome = await this.ctx.blockConcurrencyWhile(async () => {
        await new Promise(() => {});
      }).then(() => ({ value: "settled" }), (error) => ({ message: error.message }));
      return Response.json({
        message: outcome.message ?? null,
        value: outcome.value ?? null,
        instance: this.instance,
        events: this.events,
        elapsed: Date.now() - started,
      });
    }
    if (url.pathname === "/fast") {
      const started = Date.now();
      const value = await this.ctx.blockConcurrencyWhile(async () => {
        await new Promise((resolve) => setTimeout(resolve, 1));
        return "fast";
      });
      return Response.json({
        value,
        instance: this.instance,
        events: this.events,
        elapsed: Date.now() - started,
      });
    }
    return Response.json({ instance: this.instance, events: this.events });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    // Readiness without creating the object, so the counters below start at
    // the first test request.
    if (url.pathname === "/health") return new Response("ok");
    const stub = env.HOLDER.get(env.HOLDER.idFromName("limit"));
    return stub.fetch(new URL(url.pathname, "http://obj"));
  },
};
