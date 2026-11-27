// Modified by Ryuz Corp (Cello): non-ASCII ingress header regression fixture (issue #295). Apache-2.0.
import { DurableObject } from 'cloudflare:workers';

// Hex code-point lists, so the runner can compare bytes exactly: the body
// stays ASCII no matter what bytes the headers carried.
const codes = (value) =>
  value === null
    ? 'MISSING'
    : [...value].map((c) => c.codePointAt(0).toString(16)).join(',');

const ORIGIN_PORT = '__ORIGIN_PORT__';

export class Echo {
  constructor(state, env) {
    this.env = env;
  }
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === '/remote-echo') {
      const cookie = request.headers.get('cookie');
      return new Response(
        JSON.stringify({ owner: this.env.CELLO_NODE ?? null, cookie: codes(cookie) }),
        { headers: { 'x-echo-cookie': cookie ?? 'MISSING' } },
      );
    }
    return new Response('not found', { status: 404 });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/health') return new Response('ok');
    if (url.pathname === '/echo') {
      return new Response(JSON.stringify({
        cookie: codes(request.headers.get('cookie')),
        probe: codes(request.headers.get('x-probe')),
        split: codes(request.headers.get('x-split')),
        broken: codes(request.headers.get('x-broken')),
      }));
    }
    if (url.pathname === '/reflect') {
      // The request's cookie back on the response: the raw wire must carry
      // the exact bytes through ordinary UTF-8 re-encoding.
      const cookie = request.headers.get('cookie');
      return new Response('reflected', {
        headers: { 'x-echo-cookie': cookie ?? 'MISSING' },
      });
    }
    if (url.pathname === '/proxy') {
      // The cookie out through a subrequest: the origin observes the bytes.
      const cookie = request.headers.get('cookie') ?? 'MISSING';
      const res = await fetch(`http://127.0.0.1:${ORIGIN_PORT}/observe`, {
        headers: { cookie },
      });
      return new Response(await res.text());
    }
    if (url.pathname === '/remote-echo') {
      // The cookie across nodes to a Durable Object that may live on a peer:
      // the forward leg carries the UTF-8 bytes through the tunnel, the owner
      // decodes them back, and the owner's reply header returns the same way.
      const name = url.searchParams.get('name') ?? 'one';
      const cookie = request.headers.get('cookie');
      const upstream = await env.ECHO.get(env.ECHO.idFromName(name)).fetch(
        new Request('https://echo.invalid/remote-echo', {
          headers: { cookie: cookie ?? '' },
        }),
      );
      const data = await upstream.json();
      return new Response(
        JSON.stringify({
          caller: env.CELLO_NODE ?? null,
          owner: data.owner ?? null,
          cookie: data.cookie ?? null,
        }),
        {
          headers: {
            'x-echo-cookie': upstream.headers.get('x-echo-cookie') ?? 'MISSING',
          },
        },
      );
    }
    return new Response('not found', { status: 404 });
  },
};
