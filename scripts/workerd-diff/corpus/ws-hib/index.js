// A hibernatable Durable Object WebSocket: the binary message type workerd
// passes to webSocketMessage() is independent of binaryType, and the socket
// reports the standard default.
export class Hib {
  constructor(state) {
    this.state = state;
  }
  async fetch(request) {
    if (new URL(request.url).pathname !== "/ws")
      return new Response("not found", { status: 404 });
    const pair = new WebSocketPair();
    this.state.acceptWebSocket(pair[1], ["probe"]);
    return new Response(null, { status: 101, webSocket: pair[0] });
  }
  async webSocketMessage(ws, message) {
    ws.send(JSON.stringify({
      binaryType: ws.binaryType,
      tag: Object.prototype.toString.call(message),
      isBlob: message instanceof Blob,
      isBuffer: message instanceof ArrayBuffer,
      byteLength: message?.byteLength ?? null,
      text: typeof message === "string" ? message : null,
      bytes: message instanceof ArrayBuffer ? [...new Uint8Array(message)] : null,
    }));
  }
}

export default {
  async fetch(request, env) {
    return env.HIB.getByName("hibernator").fetch(request);
  },
};
