// Modified by Ryuz Corp (Cello): outbound fetch timeout fixture. Apache-2.0.
const ORIGIN = "http://127.0.0.1:__ORIGIN_PORT__";

const message = (error) => String(error && (error.message || error));

/// A head that never arrives: the fetch must reject instead of parking the
/// Worker's turn forever.
const neverEnds = async (path) => {
  const started = Date.now();
  try {
    const res = await fetch(ORIGIN + path);
    return { ok: true, status: res.status, ms: Date.now() - started };
  } catch (error) {
    return { ok: false, ms: Date.now() - started, error: message(error) };
  }
};

/// A body that keeps sending past the fetch budget: every chunk has to
/// arrive, so the request must not be on a total deadline.
const wholeStream = async () => {
  const started = Date.now();
  let bytes = 0;
  let reads = 0;
  try {
    const res = await fetch(ORIGIN + "/stream");
    const reader = res.body.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      bytes += value.length;
      reads += 1;
    }
    return { ok: true, bytes, reads, ms: Date.now() - started };
  } catch (error) {
    return { ok: false, bytes, reads, ms: Date.now() - started, error: message(error) };
  }
};

/// A body that starts and then stalls: the first chunk arrives, then the
/// read has to end at the stream idle deadline instead of pinning the
/// isolate.
const stalls = async () => {
  let firstBytes = 0;
  let t0 = Date.now();
  try {
    const res = await fetch(ORIGIN + "/stall");
    const reader = res.body.getReader();
    const first = await reader.read();
    firstBytes = first.done ? 0 : first.value.length;
    t0 = Date.now();
    for (;;) {
      const { done } = await reader.read();
      if (done) {
        return { ok: true, firstBytes, idleMs: Date.now() - t0 };
      }
    }
  } catch (error) {
    return { ok: false, firstBytes, idleMs: Date.now() - t0, error: message(error) };
  }
};

export default {
  async fetch(req) {
    const p = new URL(req.url).pathname;
    if (p === "/health") {
      return new Response("ok");
    }
    if (p === "/head") {
      return Response.json(await neverEnds("/head"));
    }
    if (p === "/stream") {
      return Response.json(await wholeStream());
    }
    if (p === "/stall") {
      return Response.json(await stalls());
    }
    return Response.json({ ok: false, error: `unknown path ${p}` });
  },
};
