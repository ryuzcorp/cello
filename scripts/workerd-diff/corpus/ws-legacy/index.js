// A compatibility date before the standard binaryType default: a pair end
// reports "arraybuffer" and a binary message is an ArrayBuffer.
export default {
  async fetch() {
    const pair = new WebSocketPair();
    pair[0].accept();
    pair[1].accept();
    const seen = [];
    pair[1].addEventListener("message", (event) => seen.push({
      tag: Object.prototype.toString.call(event.data),
      isBuffer: event.data instanceof ArrayBuffer,
      bytes: event.data instanceof ArrayBuffer ? [...new Uint8Array(event.data)] : null,
    }));
    pair[0].send(new Uint8Array([1, 2]).buffer);
    await new Promise((resolve) => setTimeout(resolve, 5));
    return Response.json({
      binaryType: pair[1].binaryType,
      clientBinaryType: pair[0].binaryType,
      seen,
    });
  },
};
