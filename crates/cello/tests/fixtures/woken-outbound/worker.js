// Modified by Ryuz Corp (Cello): woken-continuation outbound regression fixture (upstream #258); live-ambient adoption legs (#299, #303). Apache-2.0.
import { DurableObject } from 'cloudflare:workers';

// Async work started by one request, woken by another: the continuation
// resumes after its origin event retired. Under live-ambient adoption the
// woken continuation runs as the live same-cell event that woke it, so
// own-cell storage and outbound/facet work settle the way workerd settles
// them, and adopted work cancels with the adopting event. A continuation
// with no live same-cell ambient still fails loudly, and one cell's work
// never charges another cell.
export class EchoFacet extends DurableObject {
  async ping(x) {
    return `pong:${x}`;
  }
}

export class Cell extends DurableObject {
  async op(how, origin, name) {
    if (how === 'fetch') return (await fetch(`${origin}/plain`)).text();
    if (how === 'hangfetch') return (await fetch(`${origin}/hang?name=${name}`)).text();
    if (how === 'facet') {
      const facet = this.ctx.facets.get('echo', () => ({
        class: this.ctx.exports.EchoFacet({ props: {} }),
        id: 'echo-id',
      }));
      return await facet.ping('x');
    }
    await this.ctx.storage.put('k', 1);
    return 'stored';
  }
  async fetch(request) {
    const url = new URL(request.url);
    const how = url.searchParams.get('how');
    const settle = (p) => p.then(
      (v) => { this.result = `ok ${v}`; },
      (e) => { this.result = `threw ${e.message}`; },
    );
    if (url.pathname === '/self') {
      // Control: woken by its own timer, no other event involved.
      this.result = 'pending';
      settle(new Promise((r) => setTimeout(r, 200)).then(() => this.op(how, url.origin)));
      return new Response('started\n');
    }
    if (url.pathname === '/start') {
      // Woken by a later request: the bare promise keeps no host op, so
      // the origin event retires fully before `/notify` resumes it.
      this.result = 'pending';
      settle(new Promise((r) => { this.notify = r; }).then(() => this.op(how, url.origin)));
      return new Response('started\n');
    }
    if (url.pathname === '/start-slow') {
      // Cancellation leg: the waiter settles only through the server side.
      // The adopted fetch hangs until the adopting event's background
      // budget drops it; the server observes the client disconnect and
      // records it here, which is what `/result` reports.
      this.result = 'pending';
      settle(new Promise((r) => { this.notify = r; }).then(() => this.op('hangfetch', url.origin, url.searchParams.get('name'))));
      return new Response('started\n');
    }
    if (url.pathname === '/notify') {
      this.notify();
      return new Response('notified\n');
    }
    if (url.pathname === '/hang') {
      // Never answers on its own: the client is an adopted fetch the
      // adopting event's budget expiry drops. The disconnect aborts this
      // request's signal (workerd: a bare sleep is not itself aborted —
      // the timer still resolves at 60s — so the catch below cannot run).
      // The signal listener is the observable proof the server really saw
      // the adopter's disconnect.
      request.signal.addEventListener('abort', () => {
        this.result = 'cancelled:signal';
      }, { once: true });
      try {
        await new Promise((r) => setTimeout(r, 60000));
        return new Response('hung\n');
      } catch (e) {
        this.result = `cancelled:${e.message}`;
        throw e;
      }
    }
    if (url.pathname === '/expose-scope') {
      return new Response(`${this.ctx.storage._scope}\n`);
    }
    if (url.pathname === '/forge') {
      // A live event claiming another cell's scope through the facet
      // manager: the parent scope it passes is harness-held but mutable
      // here, so a forged scope reaches the op — which must refuse it as
      // foreign to the executing cell. Never adopted, always refused.
      const foreign = url.searchParams.get('scope');
      const facets = this.ctx.facets;
      const real = facets._state;
      try {
        facets._state = { _scope: foreign };
        const facet = facets.get('forge-echo', () => ({
          class: this.ctx.exports.EchoFacet({ props: {} }),
          id: 'forge-id',
        }));
        await facet.ping('x');
        return new Response('forged-ok\n');
      } catch (e) {
        return new Response(`forged-refused:${e.message}\n`);
      } finally {
        facets._state = real;
      }
    }
    if (url.pathname === '/result') return new Response(`${this.result}\n`);
    return new Response('not found', { status: 404 });
  }
}

export default {
  fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/health') return new Response('ok');
    if (url.pathname === '/plain') return new Response('plain ok');
    return env.CELL.get(env.CELL.idFromName(url.searchParams.get('name') ?? 'one')).fetch(request);
  },
};
