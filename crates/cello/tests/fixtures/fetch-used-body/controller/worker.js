// Modified by Ryuz Corp (Cello): cross-isolate revived-Request fixture. Apache-2.0.
import { DurableObject, WorkerEntrypoint } from 'cloudflare:workers';

const errorInfo = (error) => ({
  name: error?.name ?? String(error),
  message: error?.message ?? String(error),
});

const aborted = () => {
  const controller = new AbortController();
  controller.abort(new Error('stop'));
  return controller.signal;
};

const settle = (promise) => promise.then((value) => value, errorInfo);

// A Request handed to a Durable Object crosses the isolate boundary through
// the `__cello$req` marker and revives here. Which revived requests gained a
// body decides whether a later `fetch()` consumes them (#127).
export class BodyDO extends DurableObject {
  async probePost(request) {
    const before = request.bodyUsed;
    const outcome = await settle(fetch(request, { signal: aborted() }));
    const after = request.bodyUsed;
    const reread = await settle(request.text());
    const second = await settle(fetch(request, { signal: aborted() }));
    return { before, outcome, after, reread, second };
  }
  async probeGet(request) {
    const bodyIsNull = request.body === null;
    const outcome = await settle(fetch(request, { signal: aborted() }));
    const after = request.bodyUsed;
    const second = await settle(fetch(request, { signal: aborted() }));
    return { bodyIsNull, outcome, after, second };
  }
  async probeBadInit(request) {
    const outcome = await settle(fetch(request, { method: 'BAD METHOD' }));
    const after = request.bodyUsed;
    const text = await settle(request.text());
    return { outcome, after, text };
  }
  async probeRead(request) {
    const text = await request.text();
    return { text, after: request.bodyUsed };
  }
}

export default class ReviveService extends WorkerEntrypoint {
  getBodyStub(name) {
    return this.env.BODIES.getByName(name);
  }
}
