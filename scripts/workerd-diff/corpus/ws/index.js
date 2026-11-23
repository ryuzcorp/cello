// A WebSocket that reports how each message arrived, switches binaryType
// after the first binary frame, and (on /pair) drives both ends of a pair
// inside the isolate so a client-side socket's binaryType is covered too.
const describe = (socket, data) => ({
  binaryType: socket.binaryType,
  tag: Object.prototype.toString.call(data),
  isBlob: data instanceof Blob,
  byteLength: data?.byteLength ?? data?.size ?? null,
  text: typeof data === "string" ? data : null,
  bytes: data instanceof ArrayBuffer ? [...new Uint8Array(data)] : null,
});

const settle = () => new Promise((resolve) => setTimeout(resolve, 5));

export default {
  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (path === "/pair") {
      const out = {};
      const pair = new WebSocketPair();
      const seenClient = [];
      const seenServer = [];
      pair[0].accept();
      pair[1].accept();
      pair[0].addEventListener("message", (event) => seenClient.push(describe(pair[0], event.data)));
      pair[1].addEventListener("message", (event) => seenServer.push(describe(pair[1], event.data)));
      pair[0].send("from-client");
      pair[0].send(new Uint8Array([1, 2, 250]).buffer);
      pair[1].send("from-server");
      pair[1].send(new Uint8Array([3, 4]).buffer);
      await settle();
      out.client = seenClient;
      out.server = seenServer;
      out.clientBinaryType = pair[0].binaryType;
      out.serverBinaryType = pair[1].binaryType;
      const switched = new WebSocketPair();
      const switchedSeen = [];
      switched[0].accept();
      switched[1].accept();
      switched[0].binaryType = "arraybuffer";
      switched[0].addEventListener("message", (event) => switchedSeen.push(describe(switched[0], event.data)));
      switched[1].send(new Uint8Array([5]).buffer);
      await settle();
      out.switched = { binaryType: switched[0].binaryType, seen: switchedSeen };
      return Response.json(out);
    }
    if (path !== "/ws") return new Response("not found", { status: 404 });
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    let binaries = 0;
    server.addEventListener("message", (event) => {
      const data = event.data;
      let view = null;
      try { view = [...new Uint8Array(data)]; } catch (error) { view = `throw ${error.name}`; }
      server.send(JSON.stringify({
        binaryType: server.binaryType,
        typeofData: typeof data,
        tag: Object.prototype.toString.call(data),
        instanceOfArrayBuffer: data instanceof ArrayBuffer,
        byteLength: data?.byteLength ?? data?.size ?? null,
        view,
        text: typeof data === "string" ? data : null,
      }));
      if (typeof data !== "string" && binaries++ === 0) server.binaryType = "arraybuffer";
    });
    return new Response(null, { status: 101, webSocket: client });
  },
};
