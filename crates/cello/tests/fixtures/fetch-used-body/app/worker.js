// Modified by Ryuz Corp (Cello): cross-isolate revived-Request fixture. Apache-2.0.
const J = (o, st = 200) => Response.json(o, { status: st });

const makePost = () => new Request('https://revive.test/post', {
  method: 'POST', body: 'abc',
});

export default {
  async fetch(req, env) {
    const t = new URL(req.url).pathname.slice(1);
    const run = async () => {
      if (t === 'health') return { ok: true };
      const stub = await env.SB.getBodyStub('probe');
      switch (t) {
        // A Request built here is handed to a Durable Object in another
        // isolate: the DO sees the revived copy.
        case 'revived-post':
          return await stub.probePost(makePost());
        case 'revived-get':
          return await stub.probeGet(new Request('https://revive.test/get'));
        case 'revived-bad-init':
          return await stub.probeBadInit(makePost());
        case 'revived-read':
          return await stub.probeRead(makePost());
        default: return { error: `unknown ${t}` };
      }
    };
    try {
      return J({ test: t, ok: true, result: await run() });
    } catch (e) {
      return J({ test: t, ok: false, error: `${e.name}: ${e.message}` });
    }
  },
};
