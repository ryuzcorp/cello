// Modified by Ryuz Corp (Cello): ctx.container.images; clone() of streaming and disturbed bodies; bodyUsed observes the body stream; body streams reject non-byte chunks (#115, #117); a body is consumed once, including by fetch() (#127); iterable bodies are lazy chunk streams (#126); __cello$do on the service-binding RPC path; container exec pty/resize; cross-isolate RPC stubs and streams; container exec AbortSignal; test-only fake kill completion; exec reads survive their subscribing event; context end aborts finished uploads; tenant egress policy; D1 export; D1 export operator gate; D1 batch; test-only fake close/signal completion; binding-forgery enforcement; Queue/KV operator fetch gate; Queue/KV stub-RPC allowlists; Queue DLQ host-op forward; Queue send/DLQ cell-side validation; trusted-RPC target pin; Queue operator internal re-checks; receiver-held RPC streams.; dropped timer ops fire their callback early so sleepers settle (upstream #317); unrefed AbortSignal.timeout timer (upstream #306); loader-fetch 101 webSocket join (upstream #305); nodejs process.env population (upstream #296); writable request /tmp files (upstream #280); Worker WebSocket close codes (upstream #279); Symbol.dispose on plain RPC results (upstream #260); workflow alarm forgets budget-orphaned drives (upstream #299); env.CELLO_NODE host-wins re-assertion on loader env (upstream #219); getCurrentTraceparent handler trace context (upstream #313).
// The DO object model and the Cloudflare-compatible runtime surface, run
// once per isolate by `js.rs::install_harness` after the Web-API prelude.
// Lived as a 4,000-line raw string inside js.rs until 2026-07-29; it is a
// JavaScript program and belongs in a .js file, like the rest of src/js/.
// Grant-path primordials, captured before tenant code runs: the lifts,
// projection, and table install below use only these, so a tenant that
// replaces `Array.prototype.push`, `WeakMap.prototype.get`,
// `Uint8Array.prototype.set`, `Map`, `Object.keys`, and kin cannot break
// legitimate stub transfers (availability) -- and forgery is closed
// host-side regardless (the brand check, table, and tag live in Rust).
const __primReflectApply = Reflect.apply;
const __primConstruct = Reflect.construct;
const __primArrayIsArray = Array.isArray;
const __primObjectKeys = Object.keys;
const __primGetProto = Object.getPrototypeOf;
const __primHasOwn = Object.hasOwn;
const __primDefineProp = Object.defineProperty;
const __primMap = Map;
const __primSet = Set;
const __primWeakMap = WeakMap;
const __primWeakSet = WeakSet;
const __primMapGet = Map.prototype.get;
const __primMapSet = Map.prototype.set;
const __primSetHas = Set.prototype.has;
const __primSetAdd = Set.prototype.add;
const __primWeakMapGet = WeakMap.prototype.get;
const __primWeakMapHas = WeakMap.prototype.has;
const __primWeakMapSet = WeakMap.prototype.set;
const __primWeakSetHas = WeakSet.prototype.has;
const __primWeakSetAdd = WeakSet.prototype.add;
const __primInstanceOf = Function.prototype[Symbol.hasInstance];
const __primArrPush = Array.prototype.push;
const __primArrSlice = Array.prototype.slice;
const __primArrConcat = Array.prototype.concat;
const __primStartsWith = String.prototype.startsWith;
const __mapGet = (m, k) => __primReflectApply(__primMapGet, m, [k]);
const __mapSet = (m, k, v) => __primReflectApply(__primMapSet, m, [k, v]);
const __setHas = (s, v) => __primReflectApply(__primSetHas, s, [v]);
const __setAdd = (s, v) => __primReflectApply(__primSetAdd, s, [v]);
const __weakGet = (m, k) => __primReflectApply(__primWeakMapGet, m, [k]);
const __weakHas = (m, k) => __primReflectApply(__primWeakMapHas, m, [k]);
const __weakSet = (m, k, v) => __primReflectApply(__primWeakMapSet, m, [k, v]);
const __weakSetHas = (s, v) => __primReflectApply(__primWeakSetHas, s, [v]);
const __weakSetAdd = (s, v) => __primReflectApply(__primWeakSetAdd, s, [v]);
const __instanceOf = (value, constructor) =>
  __primReflectApply(__primInstanceOf, constructor, [value]);
const __arrPush = (a, ...items) => __primReflectApply(__primArrPush, a, items);
const __arrSlice = (a, ...args) => __primReflectApply(__primArrSlice, a, args);
const __arrConcat = (a, ...args) => __primReflectApply(__primArrConcat, a, args);
const __startsWith = (s, prefix) =>
  __primReflectApply(__primStartsWith, s, [prefix]);
function __bodyBytes(body) {
  if (body == null) return new __primUint8Array();
  // A materialized body stream carries its bytes in a private slot. The
  // accessor is installed with CelloBodyStream below, so it is looked up when
  // this runs, never captured: the bytes are not an own property (#170).
  const streamBytes = typeof __cello.$$bodyStreamBytes === "function"
    ? __cello.$$bodyStreamBytes(body) : undefined;
  if (streamBytes instanceof __primUint8Array)
    return streamBytes.slice();
  // A view or buffer takes its window from internal slots only (#130): its
  // own `buffer`/`byteOffset`/`byteLength` are guest-writable, and a forged
  // prototype passes an `instanceof` check. `__bodyChunk` reads through the
  // captured getters and maps a forged receiver to the bytes TypeError.
  if (typeof body === "object" && __bodyChunkKind(body) !== 0)
    return __bodyChunk(body).slice();
  // A template literal (ToString), not String(): a Symbol body is a
  // TypeError in workerd's USVString conversion, and String() would accept
  // it.
  return new TextEncoder().encode(`${body}`);
}
// Bodies that carry their own Content-Type. A Blob's is its `type`, a
// FormData serializes to multipart with a generated boundary, and a
// URLSearchParams to urlencoded. Returns null for every other body so the
// caller falls through to __bodyBytes. Blob, File, FormData and
// URLSearchParams are all installed later in this file; this runs per
// request, long after that.
function __typedBody(body) {
  if (body == null || typeof body !== "object") return null;
  if (globalThis.Blob && body instanceof Blob)
    return { bytes: __cello.$$blobBytes(body).slice(), type: body.type };
  if (globalThis.FormData && body instanceof FormData)
    return __multipartBody(body);
  if (globalThis.URLSearchParams && body instanceof URLSearchParams)
    return {
      bytes: new TextEncoder().encode(String(body)),
      type: "application/x-www-form-urlencoded;charset=UTF-8",
    };
  return null;
}
// The WHATWG escape for a multipart field name or filename. It is one-way:
// a parser does not undo it, so a name containing a quote round-trips as
// `%22` rather than the original character.
function __mimeEscape(value) {
  value = String(value);
  // A trailing backslash would escape the closing quote of the parameter and
  // run the header into the part body, so it is refused rather than encoded.
  // Workerd refuses it with this exact message.
  if (value.endsWith("\\"))
    throw new TypeError("Name or filename can't end with backslash");
  return value
    .replace(/\n/g, "%0A").replace(/\r/g, "%0D").replace(/"/g, "%22");
}
// Serialize a FormData into a multipart/form-data body. The boundary must
// not occur in any part; 16 random bytes make that collision unreachable,
// and the delimiter stays unquoted so a `boundary=` parameter needs no
// quoting.
function __multipartBody(form) {
  const random = new Uint8Array(16);
  if (globalThis.crypto && crypto.getRandomValues) crypto.getRandomValues(random);
  else for (let i = 0; i < random.length; i++)
    random[i] = Math.floor(Math.random() * 256);
  const boundary = "----celloFormBoundary" +
    Array.from(random, (byte) => byte.toString(16).padStart(2, "0")).join("");
  const encoder = new TextEncoder();
  const chunks = [];
  for (const [name, value] of form) {
    let header = `--${boundary}\r\nContent-Disposition: form-data; name="${
      __mimeEscape(name)
    }"`;
    if (value instanceof Blob) {
      header += `; filename="${__mimeEscape(value.name)}"`;
      header += `\r\nContent-Type: ${value.type || "application/octet-stream"}`;
    }
    chunks.push(encoder.encode(`${header}\r\n\r\n`));
    chunks.push(value instanceof Blob
      ? __cello.$$blobBytes(value) : encoder.encode(value));
    chunks.push(encoder.encode("\r\n"));
  }
  chunks.push(encoder.encode(`--${boundary}--\r\n`));
  let total = 0;
  for (const chunk of chunks) total += chunk.byteLength;
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return { bytes, type: `multipart/form-data; boundary=${boundary}` };
}
// Workerd's "gen" bodies: every iterable -- sync or async -- becomes a body
// stream that yields its values lazily, one pull per read, so the constructor
// pulls nothing and no Content-Length is set. Measured against workerd: a sync
// generator of 5000 strings reports 0 pulls after the constructor and 1 after
// the first read, an infinite generator constructs, and an all-byte iterable
// is a stream too (no Content-Length), so nothing is concatenated eagerly.
// Called only for object bodies that are not streams or Blobs -- an object
// with a custom toString/@@toPrimitive stringifies instead, unless it is an
// array or async-iterable (Workerd's precedence). The iterable wins whatever
// its chunks are: the constructor does not look at a chunk's type, so an
// iterable holding a string is a body whose every read rejects with the
// body-stream TypeError, not a value stringified into "alpha,beta" (#126).
// Returns null for the string-coercion path.
function __iterableBody(body) {
  if (body instanceof ArrayBuffer || ArrayBuffer.isView(body))
    return null;
  const asyncIter = body[Symbol.asyncIterator];
  if (typeof asyncIter === "function")
    return __chunkBodyStream(asyncIter.call(body));
  if (!Array.isArray(body) &&
      (body.toString !== Object.prototype.toString ||
       body[Symbol.toPrimitive] !== undefined))
    return null;
  if (typeof body[Symbol.iterator] !== "function") return null;
  return __chunkBodyStream(body[Symbol.iterator]());
}
// A body stream over an iterator's raw values, pulled one per read: the shape
// workerd gives an iterable body. The value is enqueued unwrapped, so the
// first consumer validates the chunk (#117, #126), and a cancel returns the
// producer through `iterator.return`.
function __chunkBodyStream(iterator) {
  return new ReadableStream({
    async pull(controller) {
      const result = await iterator.next();
      if (result.done) { controller.close(); return; }
      controller.enqueue(result.value);
    },
    cancel(reason) {
      // Returned, not dropped: a cleanup that rejects must reject cancel()
      // with its reason, as workerd's does.
      if (typeof iterator.return === "function")
        return iterator.return(reason);
    },
  }, { highWaterMark: 0 });
}
// Workerd's message for a body stream that yields a chunk which is not
// bytes. Every Body consumer rejects with this TypeError; without the check
// a string chunk reaches the byte math below as `length +=
// chunk.byteLength` (NaN, so the target array is empty) and fails with
// "RangeError: offset is out of bounds", which names neither the cause nor
// the fix. An ArrayBuffer and every view are bytes, as in workerd (a view
// contributes its visible window, not its backing buffer).
const __BODY_NOT_BYTES = "This ReadableStream did not return bytes.";
// Workerd's message for a Body consumer that takes an already-used body. A
// body is consumed once; the buffered path used to serve `_bodyBytes` again,
// so a second `text()`/`json()`/`arrayBuffer()`/`bytes()` silently returned
// the payload while workerd throws (#127).
const __BODY_USED =
  "Body has already been used. It can only be used once. " +
  "Use tee() first if you need to read it twice.";
// A chunk's window is read from internal slots only: a guest can define own
// `buffer`, `byteOffset` and `byteLength` properties on a view, and reading
// them off the chunk pads the body, reads the wrong window, or throws from
// the byte math. The intrinsic getters brand-check the receiver, and the
// prototype walk classifies it without running guest code (a real view cannot
// be a Proxy). The Uint8Array fast path is deliberately not `subarray(0)`:
// subarray runs SpeciesConstructor over `chunk.constructor`, which a guest
// can shadow. Building a view here is the same window over the same buffer
// with no copy.
const __primUint8Array = Uint8Array;
const __primTypedArrayProto = __primGetProto(__primUint8Array.prototype);
const __primDataViewProto = DataView.prototype;
const __primArrayBufferProto = ArrayBuffer.prototype;
const __primAccess = (proto, name) =>
  Object.getOwnPropertyDescriptor(proto, name).get;
const __primTypedArrayBuffer = __primAccess(__primTypedArrayProto, "buffer");
const __primTypedArrayByteOffset =
  __primAccess(__primTypedArrayProto, "byteOffset");
const __primTypedArrayByteLength =
  __primAccess(__primTypedArrayProto, "byteLength");
const __primDataViewBuffer = __primAccess(__primDataViewProto, "buffer");
const __primDataViewByteOffset = __primAccess(__primDataViewProto, "byteOffset");
const __primDataViewByteLength = __primAccess(__primDataViewProto, "byteLength");
// ArrayBuffer's `byteLength` is brand-checked, so it both reads the length and
// proves the receiver is a real buffer: `Object.create(ArrayBuffer.prototype)`
// passes the prototype walk but has no internal slot.
const __primArrayBufferByteLength =
  __primAccess(__primArrayBufferProto, "byteLength");
const __BODY_CHUNK_TYPED = 1;
const __BODY_CHUNK_DATA_VIEW = 2;
const __BODY_CHUNK_BUFFER = 3;
const __bodyChunkKind = (chunk) => {
  if (typeof chunk !== "object" || chunk === null) return 0;
  for (let proto = __primGetProto(chunk); proto !== null;
       proto = __primGetProto(proto)) {
    if (proto === __primTypedArrayProto) return __BODY_CHUNK_TYPED;
    if (proto === __primDataViewProto) return __BODY_CHUNK_DATA_VIEW;
    if (proto === __primArrayBufferProto) return __BODY_CHUNK_BUFFER;
  }
  return 0;
};
// A guest-supplied BufferSource as a Uint8Array over its real window, read
// through the internal slots. Every host-facing API that takes bytes -- a Body
// chunk, an R2 or Queue value, a socket frame, a file write -- goes through
// this: an own `buffer`/`byteOffset`/`byteLength` is guest-writable and would
// otherwise widen the value into neighbouring memory or throw inside the host
// operation. A forged prototype (`Object.create(Uint8Array.prototype)` or
// `Object.create(ArrayBuffer.prototype)`) passes the prototype walk but has no
// internal slot; the brand-checked getter throws, and that maps to the API's
// own TypeError. `message` is the caller's, because "did not return bytes" is
// the Body-stream wording, not R2's or WebSocket's. Returns null for a value
// that is not a buffer or view, so the caller keeps its own fallback.
const __hostChunk = (value, message) => {
  try {
    switch (__bodyChunkKind(value)) {
      case __BODY_CHUNK_TYPED:
        return new __primUint8Array(
          __primTypedArrayBuffer.call(value),
          __primTypedArrayByteOffset.call(value),
          __primTypedArrayByteLength.call(value),
        );
      case __BODY_CHUNK_DATA_VIEW:
        return new __primUint8Array(
          __primDataViewBuffer.call(value),
          __primDataViewByteOffset.call(value),
          __primDataViewByteLength.call(value),
        );
      case __BODY_CHUNK_BUFFER:
        // Read the slot first: `new Uint8Array(forged)` would otherwise
        // succeed with zero bytes.
        __primArrayBufferByteLength.call(value);
        return new __primUint8Array(value);
      default:
        return null;
    }
  } catch {
    throw new TypeError(message);
  }
};
const __bodyChunk = (chunk) => {
  const bytes = __hostChunk(chunk, __BODY_NOT_BYTES);
  if (bytes === null) throw new TypeError(__BODY_NOT_BYTES);
  return bytes;
};
// The Cache API's own body drain reads guest chunks through the same slot
// rules; the export is internal (`__cello`), never a guest member.
__cello.$$bodyChunk = __bodyChunk;
// A guest chunk's window from its internal slots (#130): the socket and KV
// preludes run in their own scope and reach it through `__cello`.
__cello.__hostChunk = __hostChunk;
// The host response pump's chunk reader. A string chunk is UTF-8, as workerd
// writes it; every other chunk must be bytes, read through the internal slots.
// `__bodyBytes` is not used here: its String() fallback would turn a number or
// plain object chunk into text where workerd fails the body, and #130 fixed
// the slot reads so a guest's own `byteLength` cannot widen or break the write.
const __bodyPumpChunk = (chunk) => typeof chunk === "string"
  ? new TextEncoder().encode(chunk)
  : __bodyChunk(chunk);
// Drain a Request/Response body stream, caching the result on the
// instance so later consumers see the materialized bytes.
async function __drainBody(target) {
  const chunks = [];
  let length = 0;
  const reader = target.body.getReader();
  for (;;) {
    const result = await reader.read();
    if (result.done) break;
    const chunk = __bodyChunk(result.value);
    chunks.push(chunk);
    length += chunk.byteLength;
  }
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  const state = __bodyOwnerState(target);
  state.bytes = bytes;
  // text() memoizes into the slot on demand. Decoding here charges every
  // binary passthrough the cost of a string it never reads.
  state.text = undefined;
  return bytes;
}
class CelloBodyStream extends ReadableStream {
  // The coupled bytes, read offset, BYOB buffer and expected length live in
  // a private slot: a body stream is a Web API object, so a worker holding
  // `response.body` must see the ReadableStream surface only (#170, #206).
  #s;
  // Runs at class definition: an arrow body would never execute.
  static #expose = (() => {
    __cello.$$bodyStream = (stream) => stream.#s;
    __cello.$$bodyStreamBytes = (value) => {
      if (value === null || value === undefined) return undefined;
      return __primGetProto(value) === CelloBodyStream.prototype
        ? value.#s.bytes : undefined;
    };
    __cello.$$bodyStreamText = (stream) =>
      new TextDecoder().decode(stream.#s.bytes);
  })();
  constructor(owner) {
    // Couple the stream to its buffered owner, so the bytes and the stream
    // cannot be installed from different arrays. Copying the owner's bytes
    // again here doubles the external memory held for every constructed body.
    const ownerState = __bodyOwnerState(owner);
    if (ownerState === null)
      throw new TypeError("CelloBodyStream requires a buffered body owner");
    const bytes = ownerState.bytes;
    if (!(bytes instanceof Uint8Array))
      throw new TypeError("CelloBodyStream owner requires buffered body bytes");
    const st = { bytes, off: 0 };
    super({
      pull(controller) {
        if (st.off >= st.bytes.byteLength) {
          controller.close();
          return;
        }
        const end = Math.min(st.off + 64 * 1024, st.bytes.byteLength);
        controller.enqueue(st.bytes.subarray(st.off, end));
        st.off = end;
      },
    }, { highWaterMark: 0 });
    // `expectedLength` lets a re-used stream advertise Content-Length; both
    // it and the BYOB cursor live in the stream's private state (#170), so a
    // body stream carries no own fields.
    this.#s = { bytes, st };
    const streamState = __cello.$$readableState(this);
    streamState.expectedLength = bytes.byteLength;
    streamState.buffered = st;
  }
  // Workerd body streams are internal streams, so BYOB readers
  // work on them. Naming ReadableStreamBYOBReader compiles the
  // lazy byte-stream prelude; a worker that never asks for byob
  // mode never pays for it.
  getReader(options) {
    if (options !== undefined && options !== null &&
        options.mode !== undefined) {
      if (String(options.mode) !== "byob")
        throw new TypeError(`Invalid reader mode '${options.mode}'`);
      const state = __cello.$$readableState(this);
      if (state.ictl === undefined) {
        // Reading the global materializes the lazy prelude, which installs
        // the controller factory the next line calls.
        void ReadableStreamBYOBReader;
        state.ictl = __cello.$$bufferedByobController(state.buffered);
      }
      return new ReadableStreamBYOBReader(this);
    }
    return super.getReader(options);
  }
}
class CelloHttpBodyStream extends ReadableStream {
  #s;
  static #expose = (() => {
    __cello.$$httpStreamId = (stream) => {
      if (stream === null || stream === undefined) return undefined;
      return __primGetProto(stream) === CelloHttpBodyStream.prototype
        ? stream.#s.id : undefined;
    };
  })();
  constructor(streamId) {
    const id = Number(streamId);
    super({
      async pull(controller) {
        // A chunk arrives as a Uint8Array. The host moves it in without
        // a copy. The end of the stream arrives as a string.
        const chunk = await __http_stream_read(id);
        if (typeof chunk === "string") controller.close();
        else controller.enqueue(chunk);
      },
      cancel() { __http_stream_cancel(id); },
    }, { highWaterMark: 0 });
    this.#s = { id };
  }
  tee() {
    if (this.locked) throw new TypeError("ReadableStream is locked");
    const state = __cello.$$readableState(this);
    state.locked = true;
    state.disturbed = true;
    const [left, right] = JSON.parse(
      __http_stream_tee(this.#s.id),
    );
    return [
      new CelloHttpBodyStream(left),
      new CelloHttpBodyStream(right),
    ];
  }
}
// Body streams expose the standard constructor to consumers. In particular,
// the Python Workers SDK validates JS body types by constructor.name.
// Keep the internal prototypes and their specialized read/tee behavior.
for (const bodyStream of [CelloBodyStream, CelloHttpBodyStream]) {
  Object.defineProperty(bodyStream.prototype, "constructor", {
    value: ReadableStream, writable: true, configurable: true,
  });
}
// Fetch's clone() refuses a disturbed or locked body (Workerd too): a body
// read through its own reader, even partly, cannot be cloned. Workerd
// reports every refusal with this one message -- its Body consumers leave
// the stream locked to an internal reader, so a consumed body is locked
// there too -- and `scripts/workerd-diff` pins the text. Cello keeps the
// stricter gate rather than copying workerd's behaviour for a manual read
// followed by releaseLock(): workerd lets that clone through and hands both
// sides empty bytes, dropping the payload already read.
const __BODY_LOCKED =
  "This ReadableStream is currently locked to a reader,";
// A reader that holds a body but has read nothing leaves it unused, yet the
// stream cannot be stolen into a second Request. Workerd reports that state
// separately from the used-body error (#140).
const __BODY_LOCKED_REQUEST =
  "The ReadableStream has been locked to a reader.";
const __bodyDisturbed = (message) => message.bodyUsed ||
  (message.body != null &&
    (message.body.locked ||
      __cello.$$readableState(message.body).disturbed));
// The Fetch standard's "parse a MIME type" plus "serialize a MIME type", which
// `blob()` applies to a `content-type` header (workerd calls it through
// MimeType). It is not header copying: the essence is lower-cased, parameters
// are re-serialized, a duplicate parameter keeps its first value, and a value
// that is not a token is quoted. workerd writes `charset` as `;charset=` and
// `boundary` as `; boundary=` (a quirk of its serializer, pinned by
// scripts/workerd-diff), and every other parameter as `;name=`. An unparsable
// type is the empty string. Returning the raw header (lower-cased by Blob)
// made `(await new Response("x", { headers: { "content-type":
// "text/plain; charset=utf-8" } }).blob()).type` differ from workerd's
// (issues #186, #159).
const __MIME_TOKEN = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;
const __MIME_TYPE =
  /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+\/[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;
const __MIME_SPACE = /[\t\n\r ]/;
function __serializeContentType(value) {
  if (value == null) return "";
  const input = String(value);
  let at = input.indexOf(";");
  const essence = (at === -1 ? input : input.slice(0, at))
    .replace(/^[\t\n\r ]+|[\t\n\r ]+$/g, "").toLowerCase();
  if (!__MIME_TYPE.test(essence)) return "";
  const names = [];
  const values = [];
  const seen = new Set();
  while (at !== -1 && at < input.length) {
    at++; // the ";"
    while (at < input.length && __MIME_SPACE.test(input[at])) at++;
    if (at < input.length && input[at] === ";") continue;
    const nameStart = at;
    while (at < input.length && input[at] !== "=" && input[at] !== ";")
      at++;
    // A parameter with no "=" ends the type, as in workerd: the parameters
    // collected so far stay, everything after is dropped.
    if (at >= input.length || input[at] === ";") break;
    const name = input.slice(nameStart, at)
      .replace(/[\t\n\r ]+$/, "").toLowerCase();
    at++; // the "="
    if (!__MIME_TOKEN.test(name)) break;
    let parameter;
    if (input[at] === '"') {
      at++;
      parameter = "";
      while (at < input.length) {
        const c = input[at++];
        if (c === "\\" && at < input.length) parameter += input[at++];
        else if (c === '"') break;
        else parameter += c;
      }
      // Once the quoted string ends, everything up to the next ";" is
      // ignored, as the spec's parameter parser does.
      while (at < input.length && input[at] !== ";") at++;
    } else {
      const valueStart = at;
      while (at < input.length && input[at] !== ";") at++;
      parameter = input.slice(valueStart, at).replace(/[\t\n\r ]+$/, "");
    }
    if (!seen.has(name)) {
      seen.add(name);
      names.push(name);
      values.push(parameter);
    }
  }
  let out = essence;
  for (let i = 0; i < names.length; i++) {
    const name = names[i];
    const parameter = values[i];
    const quoted = __MIME_TOKEN.test(parameter) ? parameter
      : '"' + parameter.replace(/\\/g, "\\\\").replace(/"/g, '\\"') + '"';
    out += (name === "charset" ? ";charset="
      : name === "boundary" ? "; boundary="
      : ";" + name + "=") + quoted;
  }
  return out;
}
// A Blob's own `type` is the raw string lower-cased (no parsing), so a Blob
// built from a serialized MIME type must skip that normalization.
const __blobOfType = (bytes, type) => {
  const blob = new Blob([bytes]);
  if (type !== "") __cello.$$setBlobType(blob, type);
  return blob;
};
// A response status that carries no body at all. Workerd's Response reports
// `body === null` for one, and its Content-Type is whatever the init or the
// peer headers said (nothing is inferred), so every path that turns a host
// reply into a Response drops the payload rather than wrapping it (#165).
const __nullBodyStatus = (status) =>
  status === 101 || status === 204 || status === 205 || status === 304;
// A Response body from a host reply: null for a null-body status (cancelling
// a stream handle the host registered anyway), otherwise the stream, the
// inline body or the bytes the reply carries.
const __hostReplyBody = (status, reply) => {
  if (__nullBodyStatus(status)) {
    if (reply.streamId !== undefined) __http_stream_cancel(reply.streamId);
    return null;
  }
  return reply.streamId !== undefined
    ? new CelloHttpBodyStream(reply.streamId)
    : reply.body !== undefined ? reply.body
      : Uint8Array.from(reply.bodyBytes || []);
};

// ---- WebIDL reflection helpers ---------------------------------------------
// WebIDL makes every interface prototype member enumerable, which class syntax
// does not; workerd follows the IDL and so must the shape a Worker reflects
// (#206). The constructor stays hidden, and the tag stays non-enumerable.
const __webidlPrototype = (ctor) => {
  for (const key of Object.getOwnPropertyNames(ctor.prototype)) {
    if (key === "constructor") continue;
    const descriptor = Object.getOwnPropertyDescriptor(ctor.prototype, key);
    if (descriptor === undefined || descriptor.enumerable ||
        !descriptor.configurable) continue;
    descriptor.enumerable = true;
    Object.defineProperty(ctor.prototype, key, descriptor);
  }
};
const __webidlStatics = (ctor) => {
  for (const key of Object.getOwnPropertyNames(ctor)) {
    if (key === "length" || key === "name" || key === "prototype") continue;
    const descriptor = Object.getOwnPropertyDescriptor(ctor, key);
    if (descriptor === undefined || descriptor.enumerable ||
        !descriptor.configurable) continue;
    descriptor.enumerable = true;
    Object.defineProperty(ctor, key, descriptor);
  }
};
const __webidlTag = (ctor, name) =>
  Object.defineProperty(ctor.prototype, Symbol.toStringTag, {
    value: name, configurable: true, writable: true, enumerable: false,
  });
// WebIDL reports an interface's `length` as its number of required arguments;
// every Fetch interface in workerd defines none. (A class's `prototype` stays
// non-writable; V8 refuses to redefine it, and no Worker branch depends on it.)
const __webidlLength = (ctor) =>
  Object.defineProperty(ctor, "length", {
    value: 0, configurable: true, writable: false,
  });
// A binding's methods are native functions in workerd, and V8 publishes a
// native function's `length` as the zero parameters its template carries --
// 0 for every JSG interface, whatever its C++ signature reads out of the
// arguments (#239). A JS class body would publish its declared parameters
// instead, so each binding whose interface workerd defines in C++ restates
// the native descriptor. Not applied to an interface workerd defines in
// TypeScript: the D1 binding keeps its declared arities there.
const __nativeMethodArities = (ctor) => {
  for (const key of Object.getOwnPropertyNames(ctor.prototype)) {
    if (key === "constructor") continue;
    const descriptor = Object.getOwnPropertyDescriptor(ctor.prototype, key);
    if (descriptor === undefined || typeof descriptor.value !== "function")
      continue;
    Object.defineProperty(descriptor.value, "length", {
      value: 0, configurable: true, writable: false,
    });
  }
};

// ---- request/response state ------------------------------------------------
// Every field of a Request or Response lives in a private slot; the runtime's
// own modules read it through `__cello.$$...`, which a Worker cannot reach.
// `_bodyUsed` used to be an own field for hidden-class stability; the WeakMap
// alone is the flag now, so a guest cannot un-use a body by assignment (#130).
const __bodyUsedTargets = new __primWeakMap();
const __bodyUsedFlag = (target) => __weakGet(__bodyUsedTargets, target) === true;
const __markBodyUsed = (target) => {
  __weakSet(__bodyUsedTargets, target, true);
};
// The Cache API marks a body it drains host-side; internal only.
__cello.$$markBodyUsed = __markBodyUsed;
let __requestState;
let __responseState;
const __bodyOwnerState = (owner) =>
  owner instanceof globalThis.Request ? __requestState(owner)
    : owner instanceof globalThis.Response ? __responseState(owner)
      : null;
const __bodyProtocol = (receiver) => {
  const state = __bodyOwnerState(receiver);
  if (state === null) throw new TypeError("Illegal invocation");
  return state;
};
// A body can be taken once. `bodyUsed` covers both halves: a buffered body a
// consumer already took and a stream a reader disturbed, including one
// cancelled. A null body has nothing to consume and stays unused, as workerd's
// does.
const __bodyConsume = async (receiver) => {
  const state = __bodyProtocol(receiver);
  if (state.body !== null) {
    if (__bodyUsedFlag(receiver) || __bodyDisturbed(receiver))
      throw new TypeError(__BODY_USED);
    __markBodyUsed(receiver);
  }
  if (state.bytes !== null) return state.bytes;
  return __drainBody(receiver);
};
const __bodyText = async (receiver) => {
  const state = __bodyProtocol(receiver);
  const bytes = await __bodyConsume(receiver);
  // Only Request memoizes: a Response decodes fresh, so a large binary body
  // never keeps a second, string-shaped copy alive. Both classes keep the
  // slot so their hidden class stays stable.
  if (state.memoText) return state.text ??= new TextDecoder().decode(bytes);
  return new TextDecoder().decode(bytes);
};
const __bodyFormData = async (receiver) => {
  // The undecoded bytes, not `text()`: a UTF-8 decode replaces each invalid
  // sequence in a binary file part with one U+FFFD, so both the bytes and
  // the length change.
  const bytes = await __bodyConsume(receiver);
  return __cello.__parseFormData(bytes, receiver.headers.get("content-type"));
};
const __bodyArrayBuffer = async (receiver) =>
  (await __bodyConsume(receiver)).slice().buffer;
// The parsed-and-reserialized MIME type, not the raw header (#186). The
// serializer is workerd's own: lower-cased essence, re-serialized parameters.
const __bodyBlob = async (receiver) =>
  __blobOfType((await __bodyConsume(receiver)).slice().buffer,
    __serializeContentType(receiver.headers.get("content-type")));
// Workerd's Body interface is a global and the shared prototype level under
// Request and Response; its members are the Body mixin's.
globalThis.Body = function Body() {
  throw new TypeError("Illegal constructor");
};
{
  const proto = globalThis.Body.prototype;
  __webidlTag(globalThis.Body, "Body");
  // Method shorthand, not the arrow helpers by reference: a method receives
  // its receiver as `this`, while an arrow closes over the module's (here
  // undefined) `this`.
  const methods = {
    // Each helper takes the owner explicitly; a method must hand it its
    // receiver, which the helper-style arrow definitions above cannot see.
    arrayBuffer: function () { return __bodyArrayBuffer(this); },
    bytes: function () {
      return __bodyConsume(this).then((bytes) => bytes.slice());
    },
    text: function () { return __bodyText(this); },
    json: async function () {
      return JSON.parse(await __bodyText(this));
    },
    formData: function () { return __bodyFormData(this); },
    blob: function () { return __bodyBlob(this); },
  };
  for (const [name, value] of Object.entries(methods))
    __primDefineProp(proto, name, {
      value, writable: true, enumerable: true, configurable: true,
    });
  __primDefineProp(proto, "body", {
    get() { return __bodyProtocol(this).body; },
    enumerable: true, configurable: true,
  });
  // Fetch's `bodyUsed` observes the body stream; it is not stored state. The
  // getter turns true as soon as the stream is disturbed -- a read or a
  // cancel through any reader, including a Worker's own half-read of a live
  // request body -- which is what workerd reports. The explicit half -- a
  // buffered body or a host-stream transfer -- is the WeakMap flag.
  __primDefineProp(proto, "bodyUsed", {
    get() {
      const state = __bodyProtocol(this);
      return __bodyUsedFlag(this) ||
        (state.body != null &&
          __cello.$$readableState(state.body).disturbed);
    },
    enumerable: true, configurable: true,
  });
}
__cello.$$consumeBody = __bodyConsume;
__cello.$$setResponseType = (response, type) => {
  __responseState(response).type = type;
};
__cello.$$responseState = (response) => __responseState(response);
__cello.$$requestState = (request) => __requestState(request);
globalThis.Response = class Response {
  #s;
  static #expose = (__responseState = (response) => response.#s);
  constructor(body, init = {}) {
    // Streaming/iterable detection runs only for object bodies, so
    // the common string path skips every instanceof below.
    let stream = null;
    let typed = null;
    if (body !== null && typeof body === "object") {
      if (body instanceof ReadableStream) stream = body;
      else if ((typed = __typedBody(body)) !== null) { /* bytes and type */ }
      else stream = __iterableBody(body);
    }
    // Workerd's status rules: ToNumber then truncate; 200-599, except that a
    // 101 is reachable only alongside a WebSocket and a WebSocket only
    // alongside a 101. A refused status used to be stored as-is (`"abc"`
    // became null, `201.5` stayed fractional, 0 and 600 left the isolate),
    // which reaches the host as an unrepresentable response instead of the
    // error that names the mistake (#163).
    const webSocket = init.webSocket ?? null;
    if (webSocket !== null &&
        !(globalThis.WebSocket && webSocket instanceof globalThis.WebSocket))
      throw new TypeError(
        "Incorrect type for the 'webSocket' field on 'InitializerDict': " +
        "the provided value is not of type 'WebSocket'.");
    let status = init.status === undefined ? 200 : Number(init.status);
    // NaN and a fractional status both fail the range check / truncate, which
    // is what workerd does: 201.5 is 201, "abc" is a RangeError.
    status = status !== status ? 0 : Math.trunc(status);
    const wsTarget = init.__wsTarget || init._wsTarget || null;
    const __STATUS_RANGE =
      "Responses may only be constructed with status codes in the " +
      "range 200 to 599, inclusive.";
    if (webSocket !== null && status !== 101)
      throw new RangeError("Responses with a WebSocket must have status code 101.");
    if (status === 101) {
      // A 101 needs a WebSocket, or an internal cross-isolate upgrade target:
      // the socket lives in the answering isolate and __wrapServiceResponse
      // binds the caller end, so that response is built before its WebSocket
      // exists.
      if (webSocket === null && wsTarget === null)
        throw new RangeError(__STATUS_RANGE);
    } else if (status < 200 || status > 599) {
      throw new RangeError(__STATUS_RANGE);
    }
    // The status line is written verbatim, so a C0 control (CR and LF above
    // all) must be refused here rather than escaped downstream (#167).
    // Workerd's check is every C0 code point; DEL and above pass.
    const statusText = init.statusText === undefined
      ? "" : String(init.statusText);
    if (/[\u0000-\u001f]/.test(statusText))
      throw new TypeError("Invalid statusText");
    const nullBody = __nullBodyStatus(status);
    const headers = new Headers(init.headers);
    // A null-body status infers nothing from the body: workerd reports no body
    // at all, so an empty typed Blob or URLSearchParams carries no
    // `content-type` either. An explicit header is still kept.
    if (typed && typed.type && !nullBody && !headers.has("content-type"))
      headers.set("content-type", typed.type);
    // A USVString (or any other non-BodyInit value) body carries the
    // Fetch-standard default type. Without it a string body reached the wire
    // and a subrequest with no `content-type` at all (#159). Bytes and
    // buffers must stay header-free, so they are excluded here -- by internal
    // slot, not `instanceof` -- as is every body that became a stream or
    // carries its own type, and a null-body status, which reports no body at
    // all.
    if (body != null && stream === null && typed === null &&
        __bodyChunkKind(body) === 0 && !nullBody &&
        !headers.has("content-type"))
      headers.set("content-type", "text/plain;charset=UTF-8");
    const bytes = stream !== null
      ? null
      : typed ? typed.bytes : __bodyBytes(body);
    // A null-body status refuses a body. Workerd allows a zero-length
    // buffered one (byte-array, string or Blob) and refuses a stream, whose
    // length it cannot know; the internal 101/204 constructors in this file
    // pass either null or an empty body, so they keep working (#165).
    if (nullBody && body != null && (stream !== null || bytes.length !== 0))
      throw new TypeError(
        "Response with null body status (101, 204, 205, or 304) cannot " +
        "have a body.");
    this.#s = {
      // Never decoded eagerly: a whole-body UTF-8 decode costs seconds for a
      // large binary body (an archive, a git pack) and can exceed V8's string
      // length. Workerd accepts a zero-length body on a null-body status and
      // then reports none: an empty read and `body === null`, so a zero-length
      // body is indistinguishable from an absent one, as on Cloudflare.
      bytes: nullBody && body != null ? new Uint8Array(0) : bytes,
      text: undefined,
      // A Response decodes fresh; only a Request memoizes the string.
      memoText: false,
      body: null,
      status,
      statusText,
      headers,
      webSocket,
      wsTarget,
      ok: status >= 200 && status <= 299,
      redirected: false,
      type: "default",
      url: "",
      cf: init.cf,
      // Set by __attachResponseRequestCancellation: the hop's request
      // controllers and whether its body was already wrapped.
      requestControllers: undefined,
      cancellationWrapped: false,
    };
    __weakSetAdd(__responseInstances, this);
    // Hono rebuilds a response after middleware with
    // `new Response(response.body, response)`; the held stream (or a
    // fresh CelloBodyStream) preserves the payload across that
    // standard clone shape. A null-body status owns no stream at all.
    this.#s.body = nullBody || body == null
      ? null
      : stream !== null
        ? stream
        : new CelloBodyStream(this);
  }
  get status() { return this.#s.status; }
  get statusText() { return this.#s.statusText; }
  get headers() { return this.#s.headers; }
  get ok() { return this.#s.ok; }
  get redirected() { return this.#s.redirected; }
  get url() { return this.#s.url; }
  get webSocket() { return this.#s.webSocket; }
  get cf() { return this.#s.cf; }
  get type() { return this.#s.type; }
  static json(data, init = {}) {
    const headers = new Headers(init.headers || {});
    if (!headers.has("content-type"))
      headers.set("content-type", "application/json");
    return new Response(JSON.stringify(data), { ...init, headers });
  }
  static redirect(url, status = 302) {
    const input = String(url);
    const statusCode = Number(status) | 0;
    if (![301, 302, 303, 307, 308].includes(statusCode)) {
      throw new RangeError(
        `${statusCode} is not a redirect status code. ` +
        "It must be one of: 301, 302, 303, 307, or 308.");
    }
    // Workerd's wording for an unparsable target, which is all a relative
    // URL is here (there is no base to resolve against) (#184).
    let target;
    try { target = new URL(input).href; }
    catch { throw new TypeError("Unable to parse URL: " + input); }
    const response = new Response(null, {
      status: statusCode,
      headers: { Location: target },
    });
    // A redirect response owns a guarded header list. Without the guard, an
    // application can rewrite Location after construction, which differs
    // from both Fetch and Workerd even though the initial response is valid.
    __cello.$$sealHeaders(response.headers);
    return response;
  }
  static error() {
    const response = new Response(null);
    response.#s.status = 0;
    response.#s.ok = false;
    response.#s.type = "error";
    return response;
  }

  clone() {
    if (__bodyDisturbed(this))
      throw new TypeError(__BODY_LOCKED);
    // Streaming body: tee, keep one branch, clone the other (as
    // Request.clone does, and as Workerd does for both).
    const state = this.#s;
    let body = state.body === null ? null : state.bytes;
    if (state.bytes === null) {
      const [left, right] = state.body.tee();
      state.body = left;
      body = right;
    }
    // A network-error response has status 0, which the constructor refuses;
    // build the copy at 200 and put the error status back, preserving every
    // other field.
    const error = this.status === 0;
    const response = new Response(
      body,
      {
        status: error ? 200 : state.status,
        statusText: state.statusText, headers: state.headers,
        webSocket: state.webSocket, __wsTarget: state.wsTarget, cf: state.cf,
      },
    );
    if (error) {
      response.#s.status = 0;
      response.#s.ok = false;
    }
    response.#s.type = state.type;
    response.#s.url = state.url;
    response.#s.redirected = state.redirected;
    return response;
  }
};
__webidlPrototype(globalThis.Response);
__webidlStatics(globalThis.Response);
__webidlTag(globalThis.Response, "Response");
__webidlLength(globalThis.Response);
// The Body mixin sits between Request/Response and Object, as workerd's
// interface hierarchy does, so the body members are shared rather than
// duplicated on each prototype.
Object.setPrototypeOf(globalThis.Response.prototype, globalThis.Body.prototype);
// Hoisted: this would otherwise allocate an array and scan it on
// every Request construction, which is the HTTP hot path. TRACE is here
// because workerd normalizes `trace` into it (#190); CONNECT and TRACK are
// not methods this runtime accepts.
const __HTTP_METHODS = new Set(
  ["GET", "HEAD", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "TRACE"]);
// Server-side Requests that entered over the authenticated operator route.
// The host marks them at dispatch; operator-only cell branches demand
// membership. Script never observes the set itself, only the refusals.
const __operatorIngressRequests = new WeakSet();
// Internal init markers as module-private Symbols, not string keys: tenant
// script cannot spell them, so neither guard is bypassable. `__INCOMING` says
// the host built this request (skip the script-facing URL parse and the
// GET/HEAD body refusal); `__PRIOR_BODY` says the body is a clone's tee'd
// branch or a revived one, not a body a script supplied with GET/HEAD.
const __INCOMING = Symbol("celloIncoming");
const __PRIOR_BODY = Symbol("celloPriorBody");
globalThis.Request = class Request {
  #s;
  static #expose = (__requestState = (request) => request.#s);
  constructor(input, init = {}) {
    const prior = input instanceof Request ? input : null;
    const priorState = prior === null ? null : __requestState(prior);
    // Host-built incoming requests (HTTP ingress and a service-binding
    // target's view of a call) skip the script-facing URL and GET/HEAD body
    // checks: the host guarantees an absolute URL, and workerd hands an
    // incoming GET a body stream. Their headers are immutable.
    const incoming = init[__INCOMING] === true;
    const suppliedSignal = init.signal !== undefined && init.signal !== null;
    if (suppliedSignal && !(init.signal instanceof globalThis.AbortSignal))
      throw new TypeError(
        "Incorrect type for the 'signal' field on 'RequestInitializerDict': " +
        "the provided value is not of type 'AbortSignal'.");
    if (init.integrity !== undefined && String(init.integrity) !== "")
      throw new TypeError(
        "Subrequest integrity checking is not implemented. The integrity " +
        "option must be either undefined or an empty string.");
    // Workerd's cache-disabled surface: `no-store` and `no-cache` are
    // accepted, and every other mode is a TypeError rather than the blanket
    // "not implemented" Error this used to throw for all of them -- which
    // made the ubiquitous `fetch(url, { cache: "no-store" })` fail (#160).
    let callCache;
    if (init.cache !== undefined) {
      const cache = String(init.cache);
      if (cache !== "no-store" && cache !== "no-cache")
        throw new TypeError("Unsupported cache mode: " + cache);
      callCache = cache;
    } else {
      callCache = prior ? priorState.cache : undefined;
    }
    let url;
    if (prior) {
      url = priorState.url;
    } else if (incoming) {
      url = String(input);
    } else {
      // A Request's url is a serialized absolute URL. A relative or
      // unparsable input used to be stored verbatim, so `new URL(r.url)`,
      // `fetch(r)` and `new Request(r.url)` each failed somewhere else
      // (#175).
      const raw = `${input}`;
      let parsed;
      try { parsed = new URL(raw); }
      catch { throw new TypeError("Invalid URL: " + raw); }
      url = parsed.href;
    }
    let method;
    {
      // Workers upper-cases every method (the upper_case_all_http_methods
      // flag) but rejects anything outside the known set, reporting the
      // original casing. Workerd's set includes TRACE and excludes CONNECT
      // and TRACK (#190).
      const raw = String(init.method === undefined
        ? (prior ? priorState.method : "GET") : init.method);
      const upper = raw.toUpperCase();
      if (!__HTTP_METHODS.has(upper))
        throw new TypeError(`Invalid HTTP method string: ${raw}`);
      method = upper;
    }
    const hasBody =
      Object.prototype.hasOwnProperty.call(init, "body");
    let body = hasBody ? init.body : null;
    // The refusal is for a body a script *supplies* with a GET/HEAD. A
    // Request used as the init dict carries its own `body` as an own
    // property, and `new Request(url, request)` is the standard proxy shape
    // -- workerd accepts it for an incoming GET, whose body presence comes
    // from the wire, not from the init. A clone's tee'd branch
    // (`__PRIOR_BODY`) is the same case.
    if (hasBody && body != null && !(init instanceof Request) &&
        init[__PRIOR_BODY] !== true &&
        (method === "GET" || method === "HEAD") && !incoming)
      throw new TypeError(
        "Request with a GET or HEAD method cannot have a body.");
    let stream = null;
    let typed = null;
    if (hasBody && body !== null && typeof body === "object") {
      if (body instanceof ReadableStream) stream = body;
      else if ((typed = __typedBody(body)) !== null) { /* bytes and type */ }
      else stream = __iterableBody(body);
    }
    // Workerd's copy steals the input's body, so a Request whose buffered
    // body a consumer took, or whose stream a reader disturbed, cannot be
    // handed to a second Request (#140). The check sits after `hasBody` so an
    // explicit `init.body` still replaces the prior's, and before the adopt
    // below, which itself disturbs the prior; `clone()` tees a live stream
    // and copies an unused buffered body, so it never reaches here disturbed.
    if (!hasBody && prior) {
      if (prior.bodyUsed) throw new TypeError(__FETCH_USED);
      // Locked but unread: a reader holds the stream, so the copy cannot
      // steal it, with workerd's own message.
      if (priorState.body != null && priorState.body.locked)
        throw new TypeError(__BODY_LOCKED_REQUEST);
    }
    if (!hasBody && prior && priorState.bytes === null) {
      // Adopt the prior request's stream; per spec the prior body
      // is disturbed by the new request.
      stream = priorState.body;
      __markBodyUsed(prior);
    }
    const bytes = stream !== null
      ? null
      : hasBody
        ? (typed ? typed.bytes : __bodyBytes(body))
        : (prior ? priorState.bytes.slice() : new __primUint8Array());
    // Headers are stored raw and built on the first `.headers` access. The
    // hot paths -- a hello world, a Worker that only forwards to a cell --
    // read none, so `new Headers` and the JSON.parse behind it never run.
    let headersJson;
    let headersInit;
    if (init.__headersJson !== undefined) {
      headersJson = init.__headersJson;
      headersInit = undefined;
    } else if (init.headers === undefined && prior) {
      headersJson = priorState.headersJson;
      // Per spec `new Request(request)` copies the header list. The prior
      // request's `Headers` object cannot stand in for it: `new Headers(h)`
      // reads `h` through its iterator, which sorts, lower-cases and
      // combines, so forwarding a request whose headers were read would
      // undo the wire shape the outbound paths preserve.
      headersInit = priorState.headers !== null
        ? __cello.$$headerList(priorState.headers)
        : priorState.headersInit;
    } else {
      headersJson = undefined;
      headersInit = init.headers;
    }
    // Whether the request carries a body at all. Workerd's `body` is null
    // when none was supplied: a bodyless POST and a GET both report null, a
    // Content-Length: 0 request reports an empty body stream, and
    // `new Request(request)` copies the distinction (#142).
    const hasBodyFlag = stream !== null ||
      (hasBody ? body !== null : prior ? priorState.hasBody : false);
    // Workerd compares the redirect mode case-insensitively ("FOLLOW" is
    // "follow") and accepts only the two modes it implements; `error` is
    // refused with the message its text describes (#184).
    const redirect = String(init.redirect === undefined
      ? (prior ? priorState.redirect : "follow")
      : init.redirect).toLowerCase();
    if (redirect !== "follow" && redirect !== "manual") {
      // Workerd's text for an unsupported redirect value, verbatim.
      throw new TypeError(
        'Invalid redirect value, must be one of "follow" or "manual" ' +
        '("error" won\'t be implemented since it does not make sense at ' +
        'the edge; use "manual" and check the response status code).');
    }
    const signal = suppliedSignal
      ? init.signal
      : (prior ? priorState.signal : new AbortController().signal);
    __weakSetAdd(__requestInstances, this);
    this.#s = {
      url,
      method,
      // Never decoded eagerly: a request whose body is never read never
      // pays the decode.
      bytes,
      text: undefined,
      memoText: true,
      body: null,
      headersJson, headersInit, headers: null,
      // An incoming request's header list is immutable: a middleware that
      // rewrites it must be told, not silently ignored (#169).
      immutableHeaders: incoming,
      hasBody: hasBodyFlag,
      redirect,
      callCache,
      signal,
      signalForSubrequests: init.__celloIncomingSignal
        ? null
        : suppliedSignal
          ? signal
          : (prior ? priorState.signalForSubrequests : null),
      cf: init.cf === undefined
        ? (prior ? priorState.cf : undefined) : init.cf,
    };
    // A typed body's default content-type must land now; only user-built
    // typed bodies reach this, never the incoming request path. A string
    // body gets the Fetch-standard text/plain too (#159): without it a
    // proxied string body reached the origin untyped. Bytes and buffers
    // stay header-free; `__bodyChunkKind` classifies them by internal slot,
    // so a forged view is not mistaken for a string body.
    if (hasBody && body != null && stream === null && typed === null &&
        __bodyChunkKind(body) === 0 &&
        !this.headers.has("content-type")) {
      this.headers.set("content-type", "text/plain;charset=UTF-8");
    } else if (typed && typed.type && !this.headers.has("content-type")) {
      this.headers.set("content-type", typed.type);
    }
    // The body stream stays eager in the private slot: the RPC and storage
    // serializers fall back to the lift-into-marker path only when a value is
    // not directly structured-cloneable, and a Request's un-cloneable body
    // stream (or AbortSignal) is what triggers that. A lazy getter let
    // `__sc_encode` clone the Request into a broken plain object and skip the
    // lift -- the serializeHttpTypes conformance failure.
    // `body === null` is the one rule for "no body was supplied" (#142), so
    // a bodyless POST reports null like a GET; the GET/HEAD body refusal
    // above is about a script-supplied body, and an incoming request is
    // exempt because the host's request can legitimately carry one.
    this.#s.body = hasBodyFlag
      ? stream !== null ? stream : new CelloBodyStream(this)
      : null;
    // Operator ingress lives in `__operatorIngressRequests`, which only the
    // host's `__makeRequest` adds to when it builds the server-side Request
    // for a call that entered over the authenticated `/runtime/` route. The
    // constructor honors no init key for it, so tenant script cannot mark
    // even its own local Request objects, and `new Request(prior)` never
    // inherits membership.
  }
  get url() { return this.#s.url; }
  get method() { return this.#s.method; }
  get headers() {
    const state = this.#s;
    if (state.headers === null) {
      const source = state.headersJson !== undefined
        ? JSON.parse(state.headersJson)
        : state.headersInit;
      state.headers = new Headers(source);
      if (state.immutableHeaders) __cello.$$sealHeaders(state.headers);
      state.headersJson = undefined;
      state.headersInit = undefined;
    }
    return state.headers;
  }
  // `headers` is a readonly attribute in workerd and in the Fetch standard. A
  // setter here was also the one way around an incoming request's immutable
  // header list (#169). Workerd reports V8's read-only-property message (its
  // attribute is a read-only data property), not the getter-only accessor
  // text, so the setter throws that instead of leaving V8 to say it.
  set headers(_value) {
    throw new TypeError(
      "Cannot assign to read only property 'headers' of object '#<Request>'");
  }
  get redirect() { return this.#s.redirect; }
  get signal() { return this.#s.signal; }
  get cf() { return this.#s.cf; }
  // Workerd defines these four attributes; a cell has no fetcher, cannot
  // honour an integrity digest, and runs with the cache option disabled, so
  // they carry workerd's own defaults instead of being absent.
  get fetcher() { return null; }
  get integrity() { return ""; }
  get keepalive() { return false; }
  get cache() { return this.#s.callCache; }
  clone() {
    if (__bodyDisturbed(this))
      throw new TypeError(__BODY_LOCKED);
    const state = this.#s;
    if (state.bytes === null) {
      // Streaming body: tee, keep one branch, clone the other. The new
      // branch is not a script-supplied body, so it skips the GET/HEAD
      // body rule -- an incoming GET legitimately carries a body stream.
      const [left, right] = state.body.tee();
      state.body = left;
      return new Request(this, { body: right, [__PRIOR_BODY]: true });
    }
    return new Request(this);
  }
};
__webidlPrototype(globalThis.Request);
__webidlStatics(globalThis.Request);
__webidlTag(globalThis.Request, "Request");
__webidlLength(globalThis.Request);
Object.setPrototypeOf(globalThis.Request.prototype, globalThis.Body.prototype);
// Clone-refusal brands: a WeakSet proves a constructed instance, so a forged
// prototype clones as plain data exactly as workerd's does (#221).
const __requestInstances = new __primWeakSet();
const __responseInstances = new __primWeakSet();
__cello.$$isRequestInstance = (value) => __weakSetHas(__requestInstances, value);
__cello.$$isResponseInstance = (value) => __weakSetHas(__responseInstances, value);
// Response attributes are read-only on the prototype (workerd's are too), so
// the two internal transitions go through the state object.
// The persisted codec (storage, queue v8 bodies, socket attachments, signal
// reasons) is plain V8 structured clone with no host-object support: a value
// whose state is all private would be stored as `{}` where it used to be
// refused by an uncloneable function brand. Walk the graph and refuse the
// host instances this codec cannot round-trip.
// Brand-only: every test is a WeakSet/WeakMap membership read through a
// captured intrinsic, so it runs no guest code -- not a Proxy trap, not a
// patched `has`. The host serializer calls this too (through
// `__cello.__storageHostName`), which is why it must never read a property.
const __brandCheck = (name) => (value) =>
  typeof __cello[name] === "function" && __cello[name](value) === true;
const __cloneHostName = (value) => {
  if (value === null || typeof value !== "object") return null;
  // Brands only: WeakSet/WeakMap membership keyed by identity. A stub is not
  // a host here (it crosses through its own envelope) and probing it would
  // run its traps, so nothing below reads a property or a symbol.
  if (__cello.$$isRequestInstance(value)) return "Request";
  if (__cello.$$isResponseInstance(value)) return "Response";
  if (__cello.$$isHeadersInstance(value)) return "Headers";
  if (__cello.$$isFormDataInstance(value)) return "FormData";
  // The URL and URLSearchParams brands are private-field checks, so a forged
  // prototype stays plain data while a real instance is refused instead of
  // stored as `{}` (#233). Workerd's storage refuses both.
  if (__brandCheck("__isUrlInstance")(value)) return "URL";
  if (__brandCheck("__isUrlSearchParamsInstance")(value)) return "URLSearchParams";
  if (__cello.$$isAbortSignalInstance(value)) return "AbortSignal";
  if (__brandCheck("$$isMessagePortInstance")(value)) return "MessagePort";
  if (__cello.__isBlobInstance(value)) return "Blob";
  if (__brandCheck("$$isCryptoKey")(value)) return "CryptoKey";
  if (__brandCheck("$$isReadableStream")(value)) return "ReadableStream";
  if (__brandCheck("$$isWritableStream")(value)) return "WritableStream";
  // The pair state carries the concrete interface name, so a clone refusal
  // reports `FixedLengthStream` as itself, not its `TransformStream` base.
  if (__brandCheck("$$isTransformStream")(value)) {
    return __cello.$$transformTypeName(value);
  }
  if (__brandCheck("$$isR2Bucket")(value)) return "R2Bucket";
  return null;
};
// The storage serializer's host-object predicate: module-private, brand-only,
// and reachable from Rust only through `internal_value` on the harness's own
// internals object. A guest can neither call it nor replace it.
__cello.__storageHostName = __cloneHostName;
// Refuse the host instances the plain codec cannot round-trip, before any
// queueing: `put()`'s single-key path hands the value straight to the Rust
// serializer, which would store `{}` for a private-slot object instead of
// raising workerd's DataCloneError.
const __scRefuseHost = (value) => {
  const seen = new __primSet();
  const stack = [value];
  while (stack.length > 0) {
    const v = stack.pop();
    if (v === null || typeof v !== "object") continue;
    if (__setHas(seen, v)) continue;
    __setAdd(seen, v);
    const host = __cloneHostName(v);
    if (host !== null) {
      // The captured class, not the global: a Worker that replaced
      // `DOMException` must not get a call inside the serializer path, and
      // the caller must still see `name === "DataCloneError"` (#233). This is
      // the batch/queue/attachment site; the single-key path throws the same
      // class from `throw_storage_data_clone_error`.
      throw new __primDOMException(
        'Could not serialize object of type "' + host +
        '". This type does not support serialization.', "DataCloneError");
    }
    // A stub crosses through its own envelope, so neither its traps nor its
    // captured handles are walked here: `instanceof` on one runs guest-visible
    // code inside the storage path.
    if (typeof __stubMeta !== "undefined" &&
        (__weakHas(__stubMeta, v) ||
         __weakHas(__svcMeta, v) ||
         __do_is_stub(v))) continue;
    if (v instanceof Date || v instanceof RegExp || v instanceof Error ||
        v instanceof ArrayBuffer || ArrayBuffer.isView(v) ||
        (typeof SharedArrayBuffer !== "undefined" &&
          v instanceof SharedArrayBuffer) ||
        (typeof WebAssembly !== "undefined" &&
          v instanceof WebAssembly.Module)) continue;
    if (v instanceof Map) {
      for (const [key, entry] of v) stack.push(key, entry);
      continue;
    }
    if (v instanceof Set) {
      for (const entry of v) stack.push(entry);
      continue;
    }
    const keys = __primObjectKeys(v);
    for (let i = 0; i < keys.length; i++) stack.push(v[keys[i]]);
  }
};
const __scEncodeStrict = (value) => {
  __scRefuseHost(value);
  return __sc_encode(value);
};
__cello.$$setResponseUrl = (response, url) => {
  __responseState(response).url = url;
};
const __makeRequest = __cello.__makeRequest = (
  url, method, body, headersJson = "[]", signal = undefined,
  incomingSignal = false, operatorIngress = false,
) => {
  const request = new Request(url, {
    // The raw header JSON, not a parsed object: an incoming request whose
    // handler never reads `.headers` (a hello world, or a Worker that only
    // routes to a cell) then never parses it. See `get headers()`.
    // Cello cannot prove Cloudflare edge metadata, so the object stays empty.
    // In particular, no client-controlled header becomes a country or a colo;
    // the forwarded-authority trust flag does not establish geolocation trust.
    method, body, __headersJson: headersJson, signal, cf: {},
    // An incoming request's redirect mode is `manual`: a Worker that
    // forwards it with `fetch(request)` must see the 3xx, not the target
    // (#161). A deployment config cannot change it.
    redirect: "manual",
    [__INCOMING]: true,
    __celloIncomingSignal: incomingSignal,
  });
  // Marked here, on the host-built server-side Request only: no
  // tenant-visible init field exists, so tenant script cannot forge it.
  if (operatorIngress === true) __operatorIngressRequests.add(request);
  return request;
};
const __fmt = (a) => a.map((x) => {
  if (typeof x === "string") return x;
  if (x instanceof Error) return x.stack || (x.name + ": " + x.message);
  try { return JSON.stringify(x); } catch { return String(x); }
}).join(" ");
const __consoleNoop = () => {};
// Workerd's console: the methods are own properties of one object, tagged
// `console` for Object.prototype.toString, and `context` returns another
// console-like object. Deno's `createTask` has no workerd counterpart and
// is gone, so feature detection agrees.
const __makeConsole = () => ({
  debug: (...a) => __log("debug", __fmt(a)),
  error: (...a) => __log("error", __fmt(a)),
  info: (...a) => __log("info", __fmt(a)),
  log: (...a) => __log("log", __fmt(a)),
  warn: (...a) => __log("warn", __fmt(a)),
  clear: __consoleNoop,
  count: __consoleNoop,
  group: __consoleNoop,
  table: __consoleNoop,
  trace: __consoleNoop,
  assert: __consoleNoop,
  countReset: __consoleNoop,
  dir: __consoleNoop,
  dirxml: __consoleNoop,
  groupCollapsed: __consoleNoop,
  groupEnd: __consoleNoop,
  profile: __consoleNoop,
  profileEnd: __consoleNoop,
  time: __consoleNoop,
  timeEnd: __consoleNoop,
  timeLog: __consoleNoop,
  timeStamp: __consoleNoop,
  context: () => __makeConsole(),
});
globalThis.console = __makeConsole();
Object.defineProperty(globalThis.console, Symbol.toStringTag, {
  value: "console", configurable: true,
});

// async-op shims: outbound fetch + timers, driven by the host event loop.
// Move one request body through a host subrequest seam. A body that already
// names a host stream keeps that stream incremental. An ordinary JavaScript
// stream has no host owner, so it uses the existing byte fallback.
const __subrequestBody = async (req, absent = false) => {
  // `body === null` is the Request's own statement that none was supplied
  // (#142): a bodyless POST must cross as an absent body, not as the empty
  // stream it used to carry, or the target sees a present zero-length body.
  if (absent || req.body === null)
    return { body: undefined, streamId: undefined };
  const state = __requestState(req);
  if (state.bytes !== null)
    return { body: state.bytes, streamId: undefined };
  const streamId = __cello.$$httpStreamId(req.body);
  if (streamId === undefined)
    return { body: await __bodyConsume(req), streamId: undefined };
  // The receiver now owns the one host source. Lock the wrapper so a later
  // read fails instead of racing the receiver, and expose the transfer through
  // the standard Body flag.
  __markBodyUsed(req);
  req.body.getReader();
  return { body: new Uint8Array(), streamId };
};
// A data: URL carries its response inline; RFC 2397 percent-decoding
// happens byte-wise, so a payload that is deliberately invalid UTF-8
// survives to the consumer (HTMLRewriter's own suite depends on it).
const __dataUrlResponse = (url) => {
  // The URL parser strips leading and trailing C0 controls and spaces
  // before doing anything else; a literal trailing space in a data: URL
  // is therefore not part of the payload, while an encoded %20 is.
  url = url.replace(/^[\x00-\x20]+/, "").replace(/[\x00-\x20]+$/, "");
  const match = /^data:([^,]*),([\s\S]*)$/.exec(url);
  if (!match) throw new TypeError("Invalid data: URL");
  let [, meta, payload] = match;
  const base64 = /;base64$/i.test(meta);
  if (base64) meta = meta.replace(/;base64$/i, "");
  let bytes;
  if (base64) {
    // The data: URL processor percent-decodes the body before it decodes the
    // base64, so `data:text/plain;base64,YQ%3D%3D` carries the padding of
    // "YQ==". Decode byte-wise, as the non-base64 branch below does: a `%`
    // that does not start a valid escape stays literal and the base64 decode
    // rejects it.
    payload = payload.replace(
      /%([0-9a-f]{2})/gi,
      (_, hex) => String.fromCharCode(parseInt(hex, 16)),
    );
    const bin = atob(payload);
    bytes = Uint8Array.from(bin, (c) => c.charCodeAt(0));
  } else {
    const out = [];
    for (let i = 0; i < payload.length; i++) {
      const c = payload[i];
      if (c === "%" && /^[0-9A-Fa-f]{2}$/.test(payload.slice(i + 1, i + 3))) {
        out.push(parseInt(payload.slice(i + 1, i + 3), 16));
        i += 2;
      } else {
        out.push(payload.charCodeAt(i) & 0xff);
      }
    }
    bytes = Uint8Array.from(out);
  }
  return new Response(bytes, {
    headers: { "content-type": meta || "text/plain;charset=US-ASCII" },
  });
};
// Must match the host's `egress::FETCH_REFUSAL_MARKER` exactly.
const __EGRESS_REFUSAL_MARKER = "CELLO_EGRESS_REFUSED:";
// `fetch(request)` consumes the request's body: workerd's Request copy steals
// it, so the input reports `bodyUsed` afterwards and a second fetch is
// refused with Request's reconstruction error. The Request constructor itself
// stays non-marking -- `clone()` and `new Request(r)` rebuild an unused body
// from the bytes -- so every fetch surface that forwards the body takes it
// here (#127). A request with no body (a GET) is not consumed, and an `init`
// with its own `body` leaves the input alone, both as workerd behaves.
const __FETCH_USED = "Cannot reconstruct a Request with a used body.";
const __fetchAdoptsBody = (input, init) =>
  input instanceof Request && __requestState(input).hasBody === true &&
  !(init !== null && typeof init === "object" &&
    Object.prototype.hasOwnProperty.call(init, "body"));
// `usedBefore` is snapshotted before the Request copy: constructing it marks a
// streaming prior as it adopts its stream, and a copy that throws on an
// invalid init must leave the input untouched, as workerd's does.
const __fetchTakeBody = (input, usedBefore) => {
  if (usedBefore) throw new TypeError(__FETCH_USED);
  __markBodyUsed(input);
};
globalThis.fetch = async (input, init) => {
  const rawUrl = typeof input === "string" ? input : input?.url;
  const adoptsBody = __fetchAdoptsBody(input, init);
  const usedBefore = adoptsBody && input.bodyUsed;
  if (typeof rawUrl === "string" && rawUrl.startsWith("data:")) {
    if (adoptsBody) __fetchTakeBody(input, usedBefore);
    return __dataUrlResponse(rawUrl);
  }
  const req = new Request(input, init);
  if (adoptsBody) __fetchTakeBody(input, usedBefore);
  // `fetch(url, { headers: { Upgrade: "websocket" } })` is the other way
  // Cloudflare opens an outbound socket, and the one most examples use.
  if ((req.headers.get("upgrade") ?? "").toLowerCase() === "websocket") {
    return await __fetchWebSocketUpgrade(req);
  }
  // The caller's signal, never the incoming request's own.
  const signal = __requestState(req).signalForSubrequests;
  if (signal?.aborted) throw signal.reason;
  // The bytes go to the host as a typed array, the same way every other
  // subrequest op takes a body. Encoding them as a JSON number array cost
  // roughly ten times the body in transient allocation, and it corrupted
  // nothing only because the host decoded the same shape back.
  const { body, streamId } = await __subrequestBody(
    req, ["GET", "HEAD"].includes(req.method));
  // The header list, not the iterator: the iterator sorts, lower-cases and
  // combines, so `Array.from(req.headers)` would send one `X-Trace: a, b`
  // for two appends and put cello's casing on the wire in place of the
  // author's. `op_fetch` builds the request with `RequestBuilder::header`,
  // which appends, so repeats survive.
  let raw;
  try {
    const dispatch = __op_fetch(
      req.method, req.url, body,
      JSON.stringify(__cello.$$headerList(req.headers)), req.redirect,
      streamId, signal != null,
    );
    if (signal) {
      // An abort and the response can cross, leaving a body stream nobody
      // reads holding its upstream connection until the sweeper expires it.
      // Every arm is silenced because the caller already owns both outcomes.
      dispatch.then((late) => {
        if (!signal.aborted) return;
        const abandoned = JSON.parse(late).streamId;
        if (abandoned !== undefined) __http_stream_cancel(abandoned);
      }, () => {}).catch(() => {});
    }
    raw = await (signal ? __awaitCancellableDoCall(dispatch, signal) : dispatch);
  } catch (error) {
    // An abort is the caller's reason, not a network error, so it skips
    // every translation below.
    if (signal?.aborted && error === signal.reason) throw error;
    // A tenant-egress refusal rejects as TypeError, the way workerd
    // surfaces a network failure. The marker comes from the host
    // (`egress::FETCH_REFUSAL_MARKER`); only the detail crosses to JS.
    const refusal = error?.message ?? String(error);
    if (typeof refusal === "string" && refusal.startsWith(__EGRESS_REFUSAL_MARKER)) {
      throw new TypeError(refusal.slice(__EGRESS_REFUSAL_MARKER.length));
    }
    throw error;
  }
  const r = JSON.parse(raw);
  // Direct network responses always carry a stream id. A transferred
  // globalOutbound Fetcher can answer with an inline body, as service
  // bindings do, so preserve both response representations -- except for a
  // null-body status, where workerd's response has no body at all (#165).
  const responseBody = __hostReplyBody(r.status, r);
  const response = new Response(
    responseBody,
    { status: r.status, headers: r.headers },
  );
  // A followed redirect reports where it landed (#173). The host compares
  // its final URL with the one it sent; a manual response keeps the
  // requested URL, as workerd's does.
  __cello.$$responseState(response).url =
    r.url !== undefined ? r.url : __requestState(req).url;
  __cello.$$responseState(response).redirected = r.redirected === true;
  return response;
};
const __fetchWebSocketUpgrade = __cello.__fetchWebSocketUpgrade =
  async (req) => {
  const scope = __currentActorScope();
  const id = __ws_alloc();
  const socket = __makeSocket(id);
  const socketState = __wsState(socket);
  socketState.outbound = true;
  socketState.polled = !scope;
  socketState.url = req.url;
  socketState.readyState = WebSocket.READY_STATE_CONNECTING;
  __sockets.set(id, socket);
  let raw;
  try {
    raw = JSON.parse(await __ws_upgrade(
      id,
      scope,
      req.url,
      // The iterator, not the header list, unlike every other outbound
      // path. The handshake builder on the host side puts these into a
      // `HeaderMap` with `insert`, which replaces, and it reads the
      // offered subprotocols with a first-match `find`. Both would drop
      // every repeat but one. Sending the combined form keeps
      // `Sec-WebSocket-Protocol: a` plus `: b` meaning `a, b`, which is
      // what the peer must see. Moving this to the header list needs the
      // host to append instead, so it is a separate change.
      JSON.stringify(Array.from(req.headers)),
    ));
  } catch (error) {
    __sockets.delete(id);
    throw error;
  }
  if (!raw.upgraded) {
    // The server answered without upgrading. That is an ordinary response,
    // not a connection error, and it is returned unchanged.
    __sockets.delete(id);
    const response = new Response(new Uint8Array(raw.body), {
      status: raw.status,
      headers: raw.headers,
    });
    __cello.$$setResponseUrl(response, req.url);
    return response;
  }
  socketState.protocol = raw.protocol ?? "";
  // The host decides who drains the socket: it registered a pull queue only
  // for a socket no cell owns, and recovered the dial's cell for a
  // post-response continuation (#254). Adopt that answer instead of the
  // actor-scope guess, which is empty exactly there.
  socketState.polled = raw.pull === true;
  // The socket is already bound to the host id, so it can ride the
  // constructor: a 101 without a WebSocket (or an internal upgrade target)
  // is refused, and setting it afterwards threw there (#165).
  const response = new Response(null, { status: 101, webSocket: socket });
  __cello.$$setResponseUrl(response, req.url);
  return response;
};
__cello.__makeAssetsBinding = (script) => ({
  async fetch(input, init) {
    const req = input instanceof Request ? new Request(input, init) : new Request(input, init);
    const r = JSON.parse(await __asset_fetch(
      script,
      req.method,
      req.url,
      JSON.stringify(Array.from(req.headers)),
    ));
    // An asset 304 answers with no body, as workerd's does (#165).
    const responseBody = __hostReplyBody(r.status, r);
    const response = new Response(
      responseBody,
      { status: r.status, headers: r.headers },
    );
    __cello.$$setResponseUrl(response, req.url);
    return response;
  },
});
// An `r2_buckets` binding, served out of the fleet bucket under the
// reserved `r2/<bucketName>/` prefix. cello runs on blob storage rather
// than providing it, so a binding gets the store the node already holds
// credentials for; a node started without a bucket has nowhere to put a
// blob and every method says so.
//
// The whole `R2Bucket` surface is here: `head`, `get`, `put`, `delete`,
// `list`, `createMultipartUpload` and `resumeMultipartUpload`, with
// `httpMetadata`, `customMetadata`, `checksums`, `storageClass`, `onlyIf`
// and every range spelling. What Cello cannot honor still fails loudly
// rather than pretending -- a silent gap is the failure mode the binding
// is written to avoid. Those are `ssecKey` (cello has no customer-key
// encryption), a conditional `put` of a body too big for one request, and
// a multipart upload resumed on a node other than the one that opened it.
const __r2Reject = (bucket, method, detail) => {
  throw new Error(
    `R2 ${method} is not implemented in Cello (binding ${bucket})` +
      (detail ? `: ${detail}` : ""),
  );
};
// R2's own error shape: `<method>: <text> (<code>)`. A Worker matches the
// code and the text, so an error R2 defines is rendered exactly, not in
// Cello's own words.
const __r2Failure = (method, text, code) =>
  new Error(`${method}: ${text} (${code})`);
// An options dictionary, converted as workerd's JSG converts one: an absent
// or null argument is the empty dictionary, an object (a plain one, an array,
// a Date, a function) is the dictionary its own properties spell, and any
// other argument is the WebIDL refusal. Interpreting a string or a number as
// "no options" turned a typo into a silently different read or write.
const __bindingOptions = (value, method, target, position, type) => {
  if (value === undefined || value === null) return {};
  const kind = typeof value;
  if (kind === "object" || kind === "function") return value;
  throw new TypeError(
    `Failed to execute '${method}' on '${target}': parameter ${position} ` +
      `is not of type '${type}'.`,
  );
};
// One field of an options dictionary, converted as JSG converts a
// non-coercible field: a present value of another type is refused, and a
// missing one leaves the default.
const __boundStringField = (options, field, type, absent) => {
  const value = options[field];
  if (value === undefined) return absent;
  if (typeof value !== "string") throw __boundFieldType(field, type);
  return value;
};
const __boundFieldType = (field, type) =>
  new TypeError(
    `Incorrect type for the '${field}' field on 'ListOptions': the provided ` +
      `value is not of type '${type}'.`,
  );
// One numeric field of an options dictionary, converted as JSG converts an
// `int`: a non-finite value becomes 0 (a JS bitwise conversion would), a
// finite value outside int32 is refused, and a fractional one truncates.
const __boundInt32 = (value) => {
  // WebIDL's number conversion refuses a BigInt; `Number(5n)` would answer 5.
  if (typeof value === "bigint")
    throw new TypeError("Cannot convert a BigInt value to a number");
  const number = Number(value);
  if (!Number.isFinite(number)) return 0;
  if (number < -2147483648 || number > 2147483647) {
    throw new TypeError(
      "Value out of range. Must be between -2147483648 and 2147483647 " +
        "(inclusive).",
    );
  }
  return Math.trunc(number);
};
// R2's key rules, checked here so a key the real thing refuses is refused
// here too rather than quietly written.
const __r2Key = (binding, method, key) => {
  const text = String(key);
  // A `.` or `..` segment is not an object name to R2: the key is mapped
  // into the store as `r2/<bucket>/<key>`, so a segment the store would
  // resolve is refused here, before any write. Checked before the length,
  // because R2 reports a long key holding one as an invalid name too.
  if (text.split("/").some((segment) => segment === "." || segment === "..")) {
    throw __r2Failure(method, "The specified object name is not valid.", 10020);
  }
  const length = new TextEncoder().encode(text).length;
  if (length === 0 || length > 1024) {
    throw new Error(
      `R2 ${method} (binding ${binding}): a key is 1 to 1024 bytes, not ${length}`,
    );
  }
  return text;
};
const __r2Hex = (value) => {
  if (typeof value === "string") return value.trim().toLowerCase();
  const bytes = __hostChunk(
    value, "an R2 checksum is an ArrayBuffer or a typed array");
  if (!bytes) return null;
  let out = "";
  for (const byte of bytes) out += byte.toString(16).padStart(2, "0");
  return out;
};
const __r2Bytes = (hex) => {
  const out = new Uint8Array(hex.length >> 1);
  for (let at = 0; at < out.length; at++) {
    out[at] = parseInt(hex.substr(at * 2, 2), 16);
  }
  return out.buffer;
};
// Read a `ReadableStream` to one `Uint8Array`. Bodies drained this way
// are the ones the caller asked to have whole.
const __r2Drain = async (stream) => {
  const reader = stream.getReader();
  const chunks = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
    total += value.length;
  }
  const out = new Uint8Array(total);
  let at = 0;
  for (const chunk of chunks) {
    out.set(chunk, at);
    at += chunk.length;
  }
  return out;
};
const __r2Ms = (value) => {
  if (value === undefined || value === null) return undefined;
  const ms = value instanceof Date ? value.getTime() : Number(value);
  return Number.isFinite(ms) ? ms : undefined;
};
// `httpMetadata` is either an `R2HTTPMetadata` or a `Headers`, and R2
// takes both on the way in.
const __r2Http = (value) => {
  if (value === undefined || value === null) return {};
  const from = (name, key) =>
    typeof value.get === "function" ? value.get(name) ?? undefined : value[key];
  const http = {
    contentType: from("content-type", "contentType"),
    contentLanguage: from("content-language", "contentLanguage"),
    contentDisposition: from("content-disposition", "contentDisposition"),
    contentEncoding: from("content-encoding", "contentEncoding"),
    cacheControl: from("cache-control", "cacheControl"),
    cacheExpiry: __r2Ms(
      typeof value.get === "function" ? value.get("expires") : value.cacheExpiry,
    ),
  };
  for (const [name, entry] of Object.entries(http)) {
    if (entry === undefined || entry === null) delete http[name];
    else if (name !== "cacheExpiry") http[name] = String(entry);
  }
  return http;
};
// `onlyIf` is either an `R2Conditional` or a `Headers`; the header
// spelling of "only if it has not changed since" is If-Unmodified-Since,
// which is `uploadedBefore`.
const __r2OnlyIf = (value) => {
  if (value === undefined || value === null) return undefined;
  const only = typeof value.get === "function"
    ? {
      etagMatches: value.get("if-match") ?? undefined,
      etagDoesNotMatch: value.get("if-none-match") ?? undefined,
      uploadedAfter: __r2Ms(Date.parse(value.get("if-modified-since") ?? "")),
      uploadedBefore: __r2Ms(Date.parse(value.get("if-unmodified-since") ?? "")),
    }
    : {
      etagMatches: value.etagMatches,
      etagDoesNotMatch: value.etagDoesNotMatch,
      uploadedAfter: __r2Ms(value.uploadedAfter),
      uploadedBefore: __r2Ms(value.uploadedBefore),
    };
  for (const [name, entry] of Object.entries(only)) {
    if (entry === undefined || entry === null || Number.isNaN(entry)) {
      delete only[name];
    } else if (name.startsWith("etag")) {
      const etag = String(entry);
      // `R2Object.etag` is unquoted; `httpEtag` is the quoted spelling for
      // HTTP headers, which a conditional takes unquoted. R2 refuses the
      // quoted form rather than quietly accepting a value a deployed node
      // rejects.
      if (etag.length >= 2 && etag.startsWith('"') && etag.endsWith('"')) {
        throw new TypeError(
          `Conditional ETag should not be wrapped in quotes (${etag}).`,
        );
      }
      only[name] = etag;
    }
  }
  return Object.keys(only).length ? only : undefined;
};
// A range is an `R2Range` or a `Headers` carrying a Range header. R2
// takes `offset` alone, `length` alone, both, or `suffix`.
const __r2Range = (binding, value) => {
  if (value === undefined || value === null) return undefined;
  if (typeof value.get === "function") {
    const header = value.get("range");
    if (!header) return undefined;
    const parsed = /^bytes=(\d*)-(\d*)$/.exec(header.trim());
    if (!parsed) {
      __r2Reject(binding, "get(range)", `cello cannot parse ${header}`);
    }
    const [, first, last] = parsed;
    if (first === "") return { suffix: Number(last) };
    if (last === "") return { offset: Number(first) };
    return { offset: Number(first), length: Number(last) - Number(first) + 1 };
  }
  const range = {};
  if (value.suffix !== undefined && value.suffix !== null) {
    range.suffix = Number(value.suffix);
  }
  if (value.offset !== undefined && value.offset !== null) {
    range.offset = Number(value.offset);
  }
  if (value.length !== undefined && value.length !== null) {
    range.length = Number(value.length);
  }
  for (const [name, entry] of Object.entries(range)) {
    if (!Number.isFinite(entry) || entry < 0) {
      __r2Reject(binding, "get(range)", `\`${name}\` must be a whole number`);
    }
  }
  return Object.keys(range).length ? range : undefined;
};
// R2 hashes an object's bytes and hands the digests back as
// `ArrayBuffer`s, with a `toJSON()` that spells them in hex.
const __r2Checksums = (hex) => {
  const checksums = {
    toJSON() {
      const json = {};
      for (const [name, digest] of Object.entries(hex)) json[name] = digest;
      return json;
    },
  };
  for (const [name, digest] of Object.entries(hex)) {
    checksums[name] = __r2Bytes(digest);
  }
  return checksums;
};
// One object as R2 describes it. `object` is the host's record; `body`,
// when there is one, makes this an `R2ObjectBody`.
const __r2Object = (object, body) => {
  const http = { ...object.http };
  if (http.cacheExpiry !== undefined) {
    http.cacheExpiry = new Date(http.cacheExpiry);
  }
  const etag = object.etag ? String(object.etag).replace(/^"|"$/g, "") : "";
  const r2 = {
    key: object.key,
    version: object.version ?? etag,
    size: object.size,
    etag,
    httpEtag: etag ? `"${etag}"` : "",
    uploaded: new Date(object.uploaded),
    httpMetadata: http,
    customMetadata: object.custom ?? {},
    checksums: __r2Checksums(object.checksums ?? {}),
    storageClass: object.storageClass ?? "Standard",
    // R2 fills the response headers from the object's own metadata, which
    // is what makes `return new Response(object.body, { headers })` serve
    // a stored file correctly.
    writeHttpMetadata(headers) {
      const write = (name, value) => {
        if (value !== undefined && value !== null) headers.set(name, value);
      };
      write("content-type", http.contentType);
      write("content-language", http.contentLanguage);
      write("content-disposition", http.contentDisposition);
      write("content-encoding", http.contentEncoding);
      write("cache-control", http.cacheControl);
      if (http.cacheExpiry !== undefined) {
        headers.set("expires", http.cacheExpiry.toUTCString());
      }
    },
  };
  if (object.range) r2.range = { ...object.range };
  if (!body) return r2;
  const drain = () => __r2Drain(body);
  r2.body = body;
  Object.defineProperty(r2, "bodyUsed", {
    get: () => body.locked ||
      __cello.$$readableState(body).disturbed,
  });
  r2.arrayBuffer = async () => (await drain()).buffer;
  r2.bytes = drain;
  r2.text = async () => new TextDecoder().decode(await drain());
  r2.json = async () => JSON.parse(new TextDecoder().decode(await drain()));
  r2.blob = async () => new Blob([await drain()]);
  return r2;
};
// The write-side options every writing method shares, in the shape the
// host ops read.
const __r2WriteOptions = (binding, method, options) => {
  const o = options ?? {};
  if (o.ssecKey !== undefined) {
    __r2Reject(
      binding,
      `${method}(ssecKey)`,
      "cello has no customer-key encryption",
    );
  }
  const custom = {};
  let customBytes = 0;
  const encoder = new TextEncoder();
  for (const [name, value] of Object.entries(o.customMetadata ?? {})) {
    custom[name] = String(value);
    customBytes += encoder.encode(name).length +
      encoder.encode(custom[name]).length;
  }
  // R2 caps the user metadata headers; past the cap a real bucket refuses
  // the write, so the local store must too rather than answer a put that
  // only ever worked in development.
  if (customBytes > 8192) {
    throw __r2Failure(
      method,
      "Your metadata headers exceed the maximum allowed metadata size.",
      10012,
    );
  }
  const verify = {};
  for (const name of ["md5", "sha1", "sha256", "sha384", "sha512"]) {
    if (o[name] === undefined || o[name] === null) continue;
    const hex = __r2Hex(o[name]);
    if (hex === null) {
      __r2Reject(
        binding,
        `${method}(${name})`,
        "a checksum is hex or an ArrayBuffer",
      );
    }
    verify[name] = hex;
  }
  const storageClass = o.storageClass === undefined || o.storageClass === null
    ? undefined
    : String(o.storageClass);
  if (storageClass !== undefined &&
    !["Standard", "InfrequentAccess"].includes(storageClass)
  ) {
    __r2Reject(
      binding,
      `${method}(storageClass)`,
      `${storageClass} is not an R2 storage class`,
    );
  }
  return {
    http: __r2Http(o.httpMetadata),
    custom,
    storageClass,
    onlyIf: __r2OnlyIf(o.onlyIf),
    verify,
  };
};
// A `put` body, as either the bytes to send in one request or the stream
// to feed the host chunk by chunk. R2 takes all of these.
const __r2Value = async (binding, value) => {
  if (value === undefined || value === null) return new Uint8Array(0);
  if (typeof value === "string") return new TextEncoder().encode(value);
  const bytes = __hostChunk(
    value, "an R2 body is an ArrayBuffer or a typed array");
  if (bytes !== null) return bytes;
  if (typeof Blob !== "undefined" && value instanceof Blob) {
    return new Uint8Array(await value.arrayBuffer());
  }
  if (value && typeof value.getReader === "function") return value;
  return __r2Reject(
    binding,
    "put",
    "a body is a string, an ArrayBuffer, a view, a Blob, a ReadableStream, or null",
  );
};
// Feed a `ReadableStream` body to the host one chunk at a time. The host
// writes one request when the whole body fits in a part and a multipart
// upload when it does not, so a Worker can stream a body far larger than
// its heap.
const __r2PutStream = async (bucketName, key, stream, options) => {
  const id = Number(await __r2_put_begin(bucketName, key, JSON.stringify(options)));
  const reader = stream.getReader();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      const bytes = __hostChunk(
        value, "an R2 body stream must yield bytes");
      if (bytes === null)
        throw new TypeError("an R2 body stream must yield bytes");
      if (bytes.byteLength === 0) continue;
      await __r2_put_chunk(id, bytes);
    }
  } catch (error) {
    await __r2_put_end(id, true);
    throw error;
  }
  return JSON.parse(await __r2_put_end(id, false));
};
// One open multipart upload. `id` resolves to the host's upload id, so
// `resumeMultipartUpload` can hand back a handle without awaiting -- as R2
// does -- and surface a bad id on the first method that uses it.
const __r2Multipart = (binding, bucketName, key, id) => ({
  key,
  get uploadId() {
    return String(id.value ?? "");
  },
  async uploadPart(partNumber, value, options) {
    if (options?.ssecKey !== undefined) {
      __r2Reject(binding, "uploadPart(ssecKey)", "cello has no customer-key encryption");
    }
    const body = await __r2Value(binding, value);
    const bytes = body instanceof Uint8Array ? body : await __r2Drain(body);
    const r = JSON.parse(await __r2_mp_part(await id.host, partNumber | 0, bytes));
    // The object store owns the part list this completion acts on, so it
    // never hands back a part etag to name one with.
    return { partNumber: r.partNumber, etag: "" };
  },
  async complete(parts) {
    const numbers = (parts ?? []).map((part) => part.partNumber | 0);
    return __r2Object(JSON.parse(
      await __r2_mp_complete(await id.host, JSON.stringify(numbers)),
    ));
  },
  async abort() {
    await __r2_mp_abort(await id.host);
  },
});
// R2 bucket state is a module-private WeakMap, and the seven R2 operations
// are prototype members: workerd's binding has no own property and no host
// helper in its shape (#206).
const __r2BucketStates = new __primWeakMap();
// A module-private token: the interface is guest-constructible by name (as
// workerd's is), but only the host's factory can mint one, so a tenant cannot
// forge a bucket for a name its script was never granted.
const __r2Create = Symbol("cello.R2Bucket.create");
class R2Bucket {
  constructor(token, binding, bucketName) {
    if (token !== __r2Create) throw new TypeError("Illegal constructor");
    __weakSet(__r2BucketStates, this, { binding, bucketName });
  }
  #state() {
    const state = __weakGet(__r2BucketStates, this);
    if (state === undefined) throw new TypeError("Illegal invocation");
    return state;
  }
  async head(key) {
    const { binding, bucketName } = this.#state();
    const name = __r2Key(binding, "head", key);
    const r = JSON.parse(await __r2_head(bucketName, name));
    return r.state === "miss" ? null : __r2Object(r.object);
  }
  async get(key, options) {
    const { binding, bucketName } = this.#state();
    const name = __r2Key(binding, "get", key);
    const o = __bindingOptions(options, "get", "R2Bucket", 2, "GetOptions");
    if (o.ssecKey !== undefined) {
      __r2Reject(binding, "get(ssecKey)", "cello has no customer-key encryption");
    }
    const request = {
      range: __r2Range(binding, o.range),
      onlyIf: __r2OnlyIf(o.onlyIf),
    };
    const r = JSON.parse(await __r2_get(bucketName, name, JSON.stringify(request)));
    if (r.state === "miss") return null;
    // A refused `onlyIf` answers the object without its body, which is how
    // a caller tells "not modified" from "not there".
    if (r.state === "unmet") return __r2Object(r.object);
    return __r2Object(r.object, new CelloHttpBodyStream(r.streamId));
  }
  async put(key, value, options) {
    const { binding, bucketName } = this.#state();
    const name = __r2Key(binding, "put", key);
    const write = __r2WriteOptions(
      binding,
      "put",
      __bindingOptions(options, "put", "R2Bucket", 3, "PutOptions"),
    );
    const body = await __r2Value(binding, value);
    const r = body instanceof Uint8Array
      ? JSON.parse(await __r2_put(bucketName, name, body, JSON.stringify(write)))
      : await __r2PutStream(bucketName, name, body, write);
    // R2 answers a refused `onlyIf` with `null` rather than throwing.
    return r.stored ? __r2Object(r.object) : null;
  }
  async delete(keys) {
    const { binding, bucketName } = this.#state();
    const list = (Array.isArray(keys) ? keys : [keys]).map((key) =>
      __r2Key(binding, "delete", key)
    );
    if (list.length > 1000) {
      throw new Error(
        `R2 delete (binding ${binding}) takes at most 1000 keys, not ${list.length}`,
      );
    }
    if (list.length) await __r2_delete(bucketName, JSON.stringify(list));
  }
  async list(options) {
    const { binding, bucketName } = this.#state();
    const o = __bindingOptions(options, "list", "R2Bucket", 1, "ListOptions");
    // `limit` is an int: a non-finite value is 0, a fractional one truncates,
    // and R2's MaxKeys rule refuses a page bound that converts to zero or is
    // over 1000. Only -1 is R2's "use the default page" sentinel -- workerd
    // answers the 1000-key default page for it and refuses every other
    // negative -- and an absent one is that same page.
    let limit = 0;
    if (o.limit !== undefined) {
      const requested = __boundInt32(o.limit);
      if (requested === 0 || requested > 1000 || requested < -1) {
        throw __r2Failure(
          "list",
          "MaxKeys params must be positive integer <= 1000.",
          10022,
        );
      }
      limit = requested > 0 ? requested : 0;
    }
    const include = o.include === undefined ? [] : o.include;
    if (!Array.isArray(include)) throw __boundFieldType("include", "Array");
    for (const entry of include) {
      if (!["httpMetadata", "customMetadata"].includes(entry)) {
        __r2Reject(binding, "list(include)", `${entry} is not an R2 listing extra`);
      }
    }
    const request = {
      prefix: __boundStringField(o, "prefix", "string", ""),
      cursor: __boundStringField(o, "cursor", "string", null),
      startAfter: __boundStringField(o, "startAfter", "string", null),
      limit: limit > 0 ? limit : 0,
      delimiter: __boundStringField(o, "delimiter", "string", null),
      include: include.length > 0,
    };
    const r = JSON.parse(await __r2_list(bucketName, JSON.stringify(request)));
    // A cursor this node did not issue is not a listing position. R2 answers
    // the empty page for one and Cello holds to the same answer; restarting
    // from the first page instead would loop a paginating client forever.
    if (r.cursor_error !== undefined) {
      return {
        objects: [], truncated: false, delimitedPrefixes: [], cursor: undefined,
      };
    }
    const objects = r.objects.map((object) => {
      // A listing carries the extras it was asked for and nothing else,
      // so a caller cannot mistake an empty map for an empty object.
      if (!include.includes("httpMetadata")) object.http = {};
      if (!include.includes("customMetadata")) object.custom = {};
      return __r2Object(object);
    });
    // Workerd's `R2Objects` carries every member: `cursor` is present and
    // undefined on a complete listing, and callers test `truncated` before
    // reading it.
    return {
      objects,
      truncated: r.truncated,
      delimitedPrefixes: r.prefixes,
      cursor: r.truncated && r.cursor ? r.cursor : undefined,
    };
  }
  async createMultipartUpload(key, options) {
    const { binding, bucketName } = this.#state();
    const name = __r2Key(binding, "createMultipartUpload", key);
    const write = __r2WriteOptions(
      binding,
      "createMultipartUpload",
      __bindingOptions(
        options, "createMultipartUpload", "R2Bucket", 2, "MultipartOptions",
      ),
    );
    if (write.onlyIf) {
      __r2Reject(
        binding,
        "createMultipartUpload(onlyIf)",
        "a multipart completion takes no precondition",
      );
    }
    // R2 takes a checksum on `put`, which sees the whole body, and not on a
    // multipart upload, which never does.
    for (const name of Object.keys(write.verify)) {
      __r2Reject(binding, `createMultipartUpload(${name})`, "R2 takes a checksum on `put`");
    }
    const uploadId = await __r2_mp_begin(bucketName, name, JSON.stringify(write));
    return __r2Multipart(binding, bucketName, name, {
      value: uploadId,
      host: Promise.resolve(Number(uploadId)),
    });
  }
  // R2 hands back a handle without checking it; so does this, and the
  // first method that uses it says whether the upload is still open.
  resumeMultipartUpload(key, uploadId) {
    const { binding, bucketName } = this.#state();
    const name = __r2Key(binding, "resumeMultipartUpload", key);
    const id = String(uploadId);
    const host = __r2_mp_resume(bucketName, name, id).then(Number);
    // A handle nobody uses must not become an unhandled rejection; the
    // first method that awaits `host` still sees the failure.
    host.catch(() => {});
    return __r2Multipart(binding, bucketName, name, { value: id, host });
  }
}
__webidlTag(R2Bucket, "R2Bucket");
__webidlPrototype(R2Bucket);
__nativeMethodArities(R2Bucket);
globalThis.R2Bucket = R2Bucket;
// Brand for the clone codec: a bucket minted by the host, never a forged one.
__cello.$$isR2Bucket = (value) => __weakGet(__r2BucketStates, value) !== undefined;
__cello.__makeR2Bucket = (binding, bucketName) =>
  new R2Bucket(__r2Create, binding, bucketName);
// A host timer op resolves once, so an interval arms a new one after every
// round. Workerd hands back a `Timeout` object: its primitive value is the
// handle's numeric id, `refresh` re-arms it with a fresh id, and
// `clearTimeout`/`clearInterval` take the object, so ids never leave the
// runtime. The armed op is remembered per handle, and a resolve from an op
// that is no longer the armed one is a cancelled round: the op still
// settles, but the callback does not run.
const __invalidTimerCallback = (callback) => {
  const type = typeof callback;
  let received;
  if (callback === null) received = "Received null";
  else if (callback === undefined) received = "Received undefined";
  else if (type === "object")
    received = "Received an instance of " +
      (callback.constructor?.name || "Object");
  else if (type === "string") received = `Received type string ('${callback}')`;
  else if (type === "number") received = `Received type number (${callback})`;
  else if (type === "boolean") received = `Received type boolean (${callback})`;
  else if (type === "bigint") received = `Received type bigint (${callback}n)`;
  else if (type === "symbol") received = `Received type symbol (${String(callback)})`;
  else received = "Received type " + type;
  return new TypeError(
    'The "callback" argument must be of type function. ' + received);
};
// Numeric ids back to their handle, so `clearTimeout(Number(handle))` cancels
// as workerd does (a bundle that persists or does arithmetic on the handle
// loses the object but keeps the id).
const __timerHandles = new Map();
class Timeout {
  #id;
  #op = 0;
  #callback;
  #delay;
  #args;
  #repeat;
  #live = false;
  constructor(id, callback, delay, args, repeat) {
    this.#id = id;
    this.#callback = callback;
    this.#delay = delay;
    this.#args = args;
    this.#repeat = repeat;
    this.#arm();
    __timerHandles.set(id, this);
  }
  #arm() {
    const op = __timer_alloc();
    this.#op = op;
    this.#live = true;
    // A rejected op died with its event (client disconnect): the deadline
    // will never arrive, but the callback still runs so promises built on it
    // settle instead of hanging to their budget (denoland/celld#294). The
    // value the callback computes is the same one a firing would produce;
    // only the timing is early, and timing guarantees are already void for a
    // dead event.
    const fire = () => {
      // Cancelling or refreshing replaces the armed op, so a round whose op
      // is no longer the armed one is dead and stops here.
      if (!this.#live || this.#op !== op) return;
      this.#op = 0;
      // Arm an interval's next round before the callback runs. A
      // `clearInterval` inside the callback must end the interval, and
      // arming after the callback would undo it.
      if (this.#repeat) this.#arm();
      else {
        this.#live = false;
        // A fired one-shot is spent: a stale numeric id must not find it.
        if (__timerHandles.get(this.#id) === this) {
          __timerHandles.delete(this.#id);
        }
      }
      this.#callback(...this.#args);
    };
    __op_timer(op, this.#delay).then(fire, fire);
  }
  refresh() {
    if (this.#live) this.#cancel();
    this.#id = __timer_alloc();
    __timerHandles.set(this.#id, this);
    this.#arm();
    return this;
  }
  #cancel() {
    this.#live = false;
    if (__timerHandles.get(this.#id) === this) {
      __timerHandles.delete(this.#id);
    }
    if (this.#op !== 0) {
      __timer_cancel(this.#op);
      this.#op = 0;
    }
  }
  ref() { return this; }
  unref() { return this; }
  hasRef() { return true; }
  close() { this.#cancel(); return this; }
  [Symbol.toPrimitive]() { return this.#id; }
}
globalThis.setTimeout = function setTimeout(callback, ms, ...args) {
  if (typeof callback !== "function")
    throw __invalidTimerCallback(callback);
  return new Timeout(__timer_alloc(), callback, ms | 0, args, false);
};
globalThis.setInterval = function setInterval(callback, ms, ...args) {
  if (typeof callback !== "function")
    throw __invalidTimerCallback(callback);
  return new Timeout(__timer_alloc(), callback, ms | 0, args, true);
};
// One id space and one cancel, because a caller can cross the two.
const __closeTimer = (handle) => {
  if (handle instanceof Timeout) handle.close();
  else if (typeof handle === "number")
    __timerHandles.get(handle)?.close();
};
globalThis.clearTimeout = function clearTimeout(handle) {
  __closeTimer(handle);
};
globalThis.clearInterval = function clearInterval(handle) {
  __closeTimer(handle);
};

const __sqlCursorFinalizer = typeof FinalizationRegistry === "function"
  ? new FinalizationRegistry((cursorId) => __sql_cursor_close_finalizer(cursorId))
  : null;

// A `SqlStorageCursor` over one exec's result (Cloudflare DO SQL API).
//
// Workerd keeps every field of a cursor in private C++ storage: `Object.keys`,
// spread and `JSON.stringify` of a cursor see nothing, and its prototype
// carries only the accessors and methods below. Private fields give Cello the
// same shape, so the host-side cursor id and the prefetched row never leak
// into tenant data.
class SqlCursor {
  // `res` is what `__sql_cursor_start` returns: V8 values throughout, with the
  // first row already stepped. A failing query throws out of the op, so there
  // is no in-band error field to test here.
  #columns;
  #rowsWritten;
  #deferredError = null;
  #cursorId;
  #prefetched;
  #rowsRead;
  #finalizerToken = null;
  constructor(res) {
    this.#columns = res.columns;
    this.#rowsWritten = Number(res.rowsWritten || 0);
    this.#cursorId = Number(res.cursorId || 0);
    this.#prefetched = res.row;
    this.#rowsRead = this.#prefetched === null ? 0 : 1;
    if (this.#cursorId && __sqlCursorFinalizer) {
      this.#finalizerToken = {};
      __sqlCursorFinalizer.register(
        this, this.#cursorId, this.#finalizerToken,
      );
    }
  }
  get rowsRead() { return this.#rowsRead; }
  get rowsWritten() { return this.#rowsWritten; }
  #obj(r) { const o = {}; for (let i = 0; i < this.#columns.length; i++) o[this.#columns[i]] = r[i]; return o; }
  #finishNative() {
    if (this.#finalizerToken && __sqlCursorFinalizer)
      __sqlCursorFinalizer.unregister(this.#finalizerToken);
    this.#finalizerToken = null;
    this.#cursorId = 0;
  }
  #advanceNative() {
    if (!this.#cursorId) {
      this.#prefetched = null;
      return;
    }
    // The op answers with V8 values, not JSON: an array is a row, a number is
    // the cursor's final rowsWritten, and a failure throws. The throw is
    // caught and deferred, because a consumer must still receive the last good
    // row -- the error belongs to the call after it, not to this one.
    let result;
    try {
      result = __sql_cursor_next(this.#cursorId);
    } catch (error) {
      this.#deferredError = error;
      this.#prefetched = null;
      this.#finishNative();
      return;
    }
    if (Array.isArray(result)) {
      this.#prefetched = result;
      this.#rowsRead++;
      return;
    }
    this.#rowsWritten = Number(result || this.#rowsWritten);
    this.#prefetched = null;
    this.#finishNative();
  }
  #nextRaw() {
    if (this.#deferredError) {
      const error = this.#deferredError;
      this.#deferredError = null;
      throw error;
    }
    // workerd answers both members even when done, so a spread, destructure
    // or `Object.keys` of the result matches it (#251 review).
    if (this.#prefetched === null) return { done: true, value: undefined };
    const value = this.#prefetched;
    this.#advanceNative();
    return { done: false, value };
  }
  toArray() {
    const rows = [];
    while (true) {
      const result = this.next();
      if (result.done) return rows;
      rows.push(result.value);
      // `#advanceNative` prefetches the next row after this one; when that
      // prefetch is refused, the refusal is deferred to the next `next()` so
      // this row still arrives. Throwing the generic message here would
      // replace it, so leave the check for the heap when a refusal is
      // already pending.
      if (this.#deferredError === null && __heap_limit_excessively_exceeded())
        throw new Error(
          "the isolate is over its " +
          (__heap_limit_external_exceeded()
            ? "ArrayBuffer memory limit (see CELLO_V8_EXTERNAL_LIMIT_MB)"
            : "V8 heap limit (see CELLO_V8_HEAP_LIMIT_MB)") +
          ", so it stopped materializing a result set");
    }
  }
  one() {
    const first = this.next();
    if (first.done)
      throw new Error(
        "Expected exactly one result from SQL query, but got no results.");
    if (!this.next().done)
      throw new Error(
        "Expected exactly one result from SQL query, but got multiple results.");
    return first.value;
  }
  *raw() {
    while (true) {
      const result = this.#nextRaw();
      if (result.done) return;
      yield result.value;
    }
  }
  get columnNames() { return this.#columns; }
  next() {
    const result = this.#nextRaw();
    return result.done ? result : { done: false, value: this.#obj(result.value) };
  }
  [Symbol.iterator]() { return this; }
}
// workerd's cursor reports its own interface, and every prototype member a
// Worker can walk is enumerable, as a JSG interface's are (#251).
Object.defineProperty(SqlCursor.prototype, Symbol.toStringTag, {
  value: "Cursor", configurable: true, writable: true, enumerable: false,
});
__webidlPrototype(SqlCursor);
// The cell the SQL storage speaks for, for the runtime's own code (the D1
// cell runs its ops against it). A private slot has no reader outside the
// class, and a static block is the one place that may hand one out (#251).
let __sqlStorageScope;
class SqlStorage {
  static {
    __sqlStorageScope = (sql) => sql.#scope;
  }
  // The state is what the abort guard in `exec` reads; a storage built
  // without one would bypass the guard silently. All three live in private
  // slots: workerd's object has no own property (#251).
  #scope;
  #state;
  #storage;
  constructor(scope, state, storage) {
    if (!state) throw new TypeError("SqlStorage requires its DurableObjectState");
    this.#scope = scope;
    this.#state = state;
    this.#storage = storage;
  }
  prepare(query) {
    __storageAssertActive(this.#storage, "sql.prepare");
    const storage = this;
    const source = String(query);
    return (...binds) => storage.exec(source, ...binds);
  }
  exec(query, ...binds) {
    __storageAssertActive(this.#storage, "sql.exec");
    // A callback that outlived its block runs on an object that was reset,
    // whose replacement uses the same connection: a statement from it
    // would land in the replacement's state. The KV surface drops such
    // writes silently; SQL refuses aloud, since a cursor can carry reads.
    if (this.#state._aborted) {
      throw new Error("the Durable Object was reset; this event's storage is closed");
    }
    const jsonBinds = [];
    const byteBinds = [];
    for (let index = 0; index < binds.length; index++) {
      const value = binds[index];
      const bytes = __hostChunk(
        value, "a SQL bind is an ArrayBuffer or a typed array");
      if (bytes === null) {
        jsonBinds.push(value);
      } else {
        // Copy every buffer before JSON.stringify can call a later value's
        // toJSON(). That callback can resize, detach, or mutate this backing
        // buffer, but exec() has already accepted the bytes at this point.
        byteBinds.push([index, new Uint8Array(bytes)]);
        jsonBinds.push(null);
      }
    }
    const json = JSON.stringify(jsonBinds);
    return new SqlCursor(byteBinds.length === 0
      ? __sql_cursor_start(this.#scope, query, json)
      : __sql_cursor_start(this.#scope, query, json, byteBinds));
  }
  ingest(input) {
    __storageAssertActive(this.#storage, "sql.ingest");
    const result = JSON.parse(__sql_ingest(this.#scope, String(input)));
    if (result.error) throw new Error("SQL error: " + result.error);
    return result;
  }
  get databaseSize() {
    __storageAssertActive(this.#storage, "sql.databaseSize");
    return __sql_database_size(this.#scope);
  }
  setMaxPageCountForTest(pages) {
    if (typeof __sql_set_max_page_count_for_test !== "function")
      throw new Error("setMaxPageCountForTest is only available in tests");
    __sql_set_max_page_count_for_test(this.#scope, Number(pages));
  }
  setWriteFaultForTest(enabled) {
    if (typeof __sql_set_write_fault_for_test !== "function")
      throw new Error("setWriteFaultForTest is only available in tests");
    __sql_set_write_fault_for_test(!!enabled);
  }
  setCacheSizeForTest(pages) {
    if (typeof __sql_set_cache_size_for_test !== "function")
      throw new Error("setCacheSizeForTest is only available in tests");
    __sql_set_cache_size_for_test(this.#scope, Number(pages));
  }
  setInterruptFaultForTest(enabled) {
    if (typeof __sql_set_interrupt_fault_for_test !== "function")
      throw new Error(
        "setInterruptFaultForTest is only available in tests");
    __sql_set_interrupt_fault_for_test(this.#scope, !!enabled);
  }
  registerNomemFunctionForTest() {
    if (typeof __sql_register_nomem_function_for_test !== "function")
      throw new Error(
        "registerNomemFunctionForTest is only available in tests");
    __sql_register_nomem_function_for_test(this.#scope);
  }
}
// workerd's SqlStorage surface is `exec`, the `databaseSize` getter and two
// nested constructors (`Cursor` for the cursor `exec` answers, `Statement` for
// the prepared form); Cello's own seams -- a bound `prepare`, the
// multi-statement `ingest` and the fault hooks -- are not part of it, so they
// exist only on a node booted with CELLO_TEST_WAIT=1, like every other test
// seam (#231, #251).
function Cursor() { throw new TypeError("Illegal constructor"); }
// The nested constructor is the cursor's own prototype, so `exec()`'s result
// is an instance of it and reports it as `constructor`; only the runtime can
// build one (#251).
Cursor.prototype = SqlCursor.prototype;
Object.defineProperty(SqlCursor.prototype, "constructor", {
  value: Cursor, configurable: true, writable: true, enumerable: false,
});
function Statement() { throw new TypeError("Illegal constructor"); }
SqlStorage.prototype.Cursor = Cursor;
SqlStorage.prototype.Statement = Statement;
__webidlTag(SqlStorage, "SqlStorage");
__webidlPrototype(SqlStorage);
if (!__test_wait_enabled()) {
  for (const name of [
    "prepare", "ingest", "setMaxPageCountForTest", "setWriteFaultForTest",
    "setCacheSizeForTest", "setInterruptFaultForTest",
    "registerNomemFunctionForTest",
  ]) {
    delete SqlStorage.prototype[name];
  }
}
__nativeMethodArities(SqlStorage);
function __readStoredValue(scope, key, read) {
  try {
    return read();
  } catch (error) {
    const message = String(error && error.message || error);
    if (!message.toLowerCase().includes("deserialize cloned data"))
      throw error;
    const contextual = new Error(
      "actor storage deserialization failed; actorId = " +
      scope + "; key = " + key + "; " + message,
    );
    contextual.cause = error;
    throw contextual;
  }
}
// workerd's listing iterator is its own interface: no own property, `next` on
// an enumerable prototype, V8's iterator helpers inherited through
// `Iterator`, and one iterator at a time per storage (#251).
class ListIterator extends Iterator {
  #root;
  #generation;
  #cursor;
  constructor(root, generation, cursor) {
    super();
    this.#root = root;
    this.#generation = generation;
    this.#cursor = cursor;
  }
  next() {
    if (this.#generation !== this.#root._syncKvListGeneration) {
      throw new Error(
        "kv.list() iterator was invalidated because a new call to kv.list() was started. " +
        "Only one kv.list() iterator can exist at a time.",
      );
    }
    const value =
      __storage_sync_list_next(this.#cursor, __storedSentinel);
    if (value === null) return { done: true, value: undefined };
    value[1] = __unwrapStored(value[1]);
    return { done: false, value };
  }
}
__webidlTag(ListIterator, "ListIterator");
__webidlPrototype(ListIterator);
class SyncKvStorage {
  // workerd's own object; its state is private, so a Worker reflecting it sees
  // no property at all (#251).
  #storage;
  #root;
  constructor(storage) {
    this.#storage = storage;
    this.#root = storage._transactionRoot;
  }
  get(key) {
    __storageAssertActive(this.#storage, "kv.get");
    key = String(key);
    return __unwrapStored(__readStoredValue(
      this.#storage._scope, key,
      () => __storage_get(this.#storage._scope, key,
                          __storedSentinel),
    ));
  }
  put(key, value) {
    __storageAssertActive(this.#storage, "kv.put");
    key = String(key);
    try {
      __storage_put(this.#storage._scope, key, value);
    } catch (error) {
      __storage_put_serialized(this.#storage._scope, key,
                               __storedBytes(value, error));
    }
  }
  delete(key) {
    __storageAssertActive(this.#storage, "kv.delete");
    return __storage_delete(this.#storage._scope, String(key));
  }
  list(options) {
    __storageAssertActive(this.#storage, "kv.list");
    const request = __storageListRequest(options, "list", "SyncKvStorage");
    const generation = ++this.#root._syncKvListGeneration;
    const cursor = __storage_sync_list_start(
      this.#storage._scope, JSON.stringify(request),
    );
    return new ListIterator(
      this.#root, generation, cursor,
    );
  }
}
__webidlTag(SyncKvStorage, "SyncKvStorage");
__webidlPrototype(SyncKvStorage);
__nativeMethodArities(SyncKvStorage);
// A value as JSG's `kj::String` conversion reads it: V8's ToString, which
// takes any value but refuses a symbol, because a symbol has no string form.
// `String(symbol)` would instead answer "Symbol(...)" and let a typo through.
const __jsString = (value) => {
  if (typeof value === "symbol")
    throw new TypeError("Cannot convert a Symbol value to a string");
  return `${value}`;
};
// One listing's request, converted as workerd's JSG converts the ListOptions
// struct (#239): the four string fields read any value (an absent or
// `undefined` field is absent), `reverse` is the boolean coercion, `limit` is
// an `int` whose positivity is checked when the listing compiles, and the two
// start fields are mutually exclusive. Reading the raw JSON into the store
// reported serde's own text for a mistyped field, which no Worker matches on.
const __storageListRequest = (options, method, target) => {
  const o = __bindingOptions(options, method, target, 1, "ListOptions");
  const request = {};
  const start = o.start === undefined ? undefined : __jsString(o.start);
  const startAfter = o.startAfter === undefined
    ? undefined : __jsString(o.startAfter);
  if (start !== undefined && startAfter !== undefined)
    throw new TypeError(
      "list() cannot be called with both start and startAfter values.",
    );
  if (start !== undefined) request.start = start;
  if (startAfter !== undefined) request.startAfter = startAfter;
  if (o.end !== undefined) request.end = __jsString(o.end);
  if (o.prefix !== undefined) request.prefix = __jsString(o.prefix);
  if (o.reverse !== undefined) request.reverse = !!o.reverse;
  if (o.limit !== undefined) {
    const limit = __boundInt32(o.limit);
    if (limit <= 0) throw new TypeError("List limit must be positive.");
    request.limit = limit;
  }
  return request;
};
// Workerd's `jsg::Date` argument conversion: a Date or a number, and a finite
// value. Every other value is the WebIDL argument refusal, which names the
// method being called. `setAlarm` and `getBookmarkForTime` share it.
const __storageDate = (value, method) => {
  let ms;
  if (value instanceof Date) ms = value.getTime();
  else if (typeof value === "number") ms = value;
  else
    throw new TypeError(
      "Failed to execute '" + method + "' on 'DurableObjectStorage': " +
      "parameter 1 is not of type 'date'.");
  if (!Number.isFinite(ms))
    throw new TypeError(
      "The value cannot be converted because it is not a valid Date.");
  return ms;
};
// `setAlarm` additionally requires a time strictly after the Unix epoch.
const __alarmTime = (value) => {
  const ms = __storageDate(value, "setAlarm");
  if (ms <= 0)
    throw new TypeError("setAlarm() cannot be called with an alarm time <= 0");
  return ms;
};
// A local storage back-end has no point-in-time recovery, and workerd's own
// says so instead of inventing a bookmark: the one answer both methods give
// once their arguments convert (#251).
const __noPointInTimeRecovery = () =>
  new Error(
    "This Durable Object's storage back-end does not implement " +
      "point-in-time recovery.",
  );
// A bookmark's shape, as the local back-end reports it: a zero prefix, the
// cell's own sequence in hex, another zero field and 32 zero hex digits.
// Calling `getCurrentBookmark()` is what advances the sequence (a write does
// not), and `deleteAll()` starts it over, which is what a fixture can observe
// (#251).
const __storageBookmark = (sequence) =>
  "00000000-" + sequence.toString(16).padStart(8, "0") + "-00000000-" +
  "0".repeat(32);
// `setHibernatableWebSocketEventTimeout`'s conversion: `undefined` and `null`
// clear it, any other value is a number (a BigInt keeps V8's own refusal), a
// non-finite one is "not an integer", a negative one is refused as negative,
// and anything past a week is refused by the back-end's own limit.
const __hibernationTimeout = (value) => {
  if (value === undefined || value === null) return null;
  if (typeof value === "bigint")
    throw new TypeError("Cannot convert a BigInt value to a number");
  const number = Number(value);
  if (!Number.isFinite(number))
    throw new TypeError(
      "The value cannot be converted because it is not an integer.");
  const ms = Math.trunc(number);
  if (ms < 0)
    throw new TypeError(
      "The value cannot be converted because it is negative and this API " +
        "expects a positive number.");
  if (ms > 604800000)
    throw new Error("Event timeout should not exceed 604800000 ms.");
  return ms === 0 ? null : ms;
};
// The storage guard `SqlStorage` and the synchronous KV view share with the
// storage class: a static private method is reachable from a static block and
// from nowhere else, so neither the prototype nor the class object publishes
// it (#251).
let __storageAssertActive;
class DurableObjectStorage {
  static {
    __storageAssertActive = (storage, operation) =>
      DurableObjectStorage.#assertTransactionActive(storage, operation);
  }
  constructor(
    scope,
    state = null,
    transactionRoot = null,
    transactionDepth = 0,
    transactionControl = null,
  ) {
    // The host's own fields stay off reflection: workerd's storage exposes
    // `kv` and `sql` as its only own, enumerable properties, so `Object.keys`
    // and `JSON.stringify` of a storage answer exactly that much, and no
    // `_`-prefixed member is left to find (#251). They are defined rather
    // than assigned because assignment is what makes a property enumerable.
    // The scope field is non-writable too: it reaches host ops, and a
    // reassignment must not retarget them (H6).
    __primDefineProp(this, "_scope", {
      value: scope, enumerable: false, configurable: false, writable: false,
    });
    __primDefineProp(this, "_state", {
      value: state, enumerable: false, configurable: true, writable: true,
    });
    __primDefineProp(this, "_transactionRoot", {
      value: transactionRoot || this,
      enumerable: false, configurable: true, writable: true,
    });
    __primDefineProp(this, "_transactionDepth", {
      value: transactionDepth, enumerable: false, configurable: true, writable: true,
    });
    __primDefineProp(this, "_transactionControl", {
      value: transactionControl, enumerable: false, configurable: true, writable: true,
    });
    if (!transactionRoot) {
      __primDefineProp(this, "_transactionSerial", {
        value: 0, enumerable: false, configurable: true, writable: true,
      });
      __primDefineProp(this, "_transactionTail", {
        value: Promise.resolve(),
        enumerable: false, configurable: true, writable: true,
      });
      __primDefineProp(this, "_transactionOwner", {
        value: "", enumerable: false, configurable: true, writable: true,
      });
      __primDefineProp(this, "_activeTransaction", {
        value: null, enumerable: false, configurable: true, writable: true,
      });
      __primDefineProp(this, "_activeSyncTransaction", {
        value: null, enumerable: false, configurable: true, writable: true,
      });
      __primDefineProp(this, "_syncKvListGeneration", {
        value: 0, enumerable: false, configurable: true, writable: true,
      });
      __primDefineProp(this, "_bookmarkSequence", {
        value: 0, enumerable: false, configurable: true, writable: true,
      });
    }
    // workerd's own enumerable properties, in its order.
    this.kv = new SyncKvStorage(this);
    this.sql = new SqlStorage(scope, state, this);
  }

  static #transactionStatus(self) {
    let status = null;
    for (let control = self._transactionControl; control; control = control.parent) {
      if (control.rolledBack) return "rolled back";
      if (control.committed) status = "committed";
    }
    return status;
  }

  static #assertTransactionActive(self, operation) {
    const status = DurableObjectStorage.#transactionStatus(self);
    // workerd's wording, which applications match on.
    if (status === "committed")
      throw new Error(
        "Cannot call " + operation + "() on transaction that has already committed: " +
          "did you move `txn` outside of the closure?",
      );
    if (status) throw new Error("Cannot " + operation + "() on " + status + " transaction");
  }

  static #flushPendingPuts(self) {
    // An admitted put can resume after its boundary flushed and ended the
    // transaction. Its promise completes without flushing a later scope.
    if (DurableObjectStorage.#transactionStatus(self)) return false;
    if (self._state && self._state._aborted) return false;
    __storage_flush_pending_puts(self._scope);
    return true;
  }
  async get(k, options) {
    DurableObjectStorage.#assertTransactionActive(this, "get");
    // `allowConcurrency`/`noCache` reach the host's own scheduling, which
    // this cell answers from its own storage; the argument is still
    // converted, because a typo in it must not read as "no options".
    __bindingOptions(options, "get", "DurableObjectStorage", 2, "GetOptions");
    // Transaction operations reach SQLite before their caller can roll back.
    if (!this._transactionControl) await Promise.resolve();
    if (!DurableObjectStorage.#flushPendingPuts(this)) return;
    const key = Array.isArray(k) ? JSON.stringify(k) : String(k);
    return __readStoredValue(this._scope, key, () => {
      if (Array.isArray(k))
        return __unwrapStoredMap(
          __storage_get_many(this._scope, k, __storedSentinel));
      return __unwrapStored(
        __storage_get(this._scope, k, __storedSentinel));
    });
  }
  async put(k, val, options) {
    DurableObjectStorage.#assertTransactionActive(this, "put");
    // Workerd's put has one signature, `put(key: string or object, value,
    // options)`, and its WebIDL union decides the form: a non-array object is
    // the batch dictionary (the second argument is then its options), and
    // everything else -- including an array, which the dictionary conversion
    // refuses -- is the single-pair form with a ToString key. The refusal for
    // a call with no argument names that union, not the entries form.
    if (arguments.length === 0)
      throw new TypeError(
        "Failed to execute 'put' on 'DurableObjectStorage': parameter 1 is " +
          "not of type 'string or object'.",
      );
    const kind = typeof k;
    if (kind === "object" || kind === "function") {
      if (k !== null && !Array.isArray(k)) {
        DurableObjectStorage.#putEntries(this, k, val);
        await Promise.resolve();
        DurableObjectStorage.#flushPendingPuts(this);
        return;
      }
    }
    // A symbol has no string form: V8's ToString throws, where String(symbol)
    // would answer "Symbol(...)" and store a key the caller never wrote.
    const key = __jsString(k);
    __bindingOptions(options, "put", "DurableObjectStorage", 3, "PutOptions");
    // Workerd (actor-state.c++ putOne) refuses an undefined value on a
    // single put: it would store nothing and leave any earlier value in
    // place.
    if (val === undefined)
      throw new TypeError("put() called with undefined value.");
    try {
      __storage_queue_put(this._scope, key, val);
    } catch (error) {
      __storage_queue_put_serialized(this._scope, key,
                                     __storedBytes(val, error));
    }
    await Promise.resolve();
    DurableObjectStorage.#flushPendingPuts(this);
  }
  // The batch form's writes: the dictionary conversion reads every own
  // enumerable string key, in property order, and drops a pair whose value is
  // undefined (`putMultiple` skips it rather than deleting the key). A Map, a
  // Set or a Date has no such key, so it writes nothing at all.
  static #putEntries(self, entries, options) {
    // Any object is the options dictionary, an array included: workerd's
    // struct conversion reads one positionally, and an array of any shape
    // leaves every PutOptions field at its default, so the write is the same
    // as with `{}` (#249 review).
    const kind = typeof options;
    if (options !== undefined && options !== null && kind !== "object" &&
        kind !== "function") {
      throw new TypeError(
        "put() may only be called with a single key-value pair and optional " +
          "options as put(key, value, options) or with multiple key-value " +
          "pairs and optional options as put(entries, options)",
      );
    }
    const pairs = [];
    for (const key of Object.keys(entries)) {
      const value = entries[key];
      if (value === undefined) continue;
      pairs.push([key, value]);
    }
    if (pairs.length === 0) return;
    try {
      __storage_queue_put_many(self._scope, pairs);
    } catch (error) {
      // A batch with a stub entry: encode every entry first (plain
      // entries keep their native clone bytes, stub entries take
      // the stored-stub envelope), then queue -- so a genuinely
      // uncloneable entry still queues nothing, like the batch op,
      // which serializes fully before queueing.
      const encoded = pairs.map(([key, value]) => {
        try {
          return [key, __scEncodeStrict(value)];
        } catch (error_) {
          return [key, __storedBytes(value, error_)];
        }
      });
      for (const [key, bytes] of encoded)
        __storage_queue_put_serialized(self._scope, key, bytes);
    }
  }
  async delete(k, options) {
    DurableObjectStorage.#assertTransactionActive(this, "delete");
    // Workerd's delete names PutOptions in its refusal: the two dictionaries
    // carry the same flags (`allowUnconfirmed`).
    __bindingOptions(options, "delete", "DurableObjectStorage", 2, "PutOptions");
    if (!this._transactionControl) await Promise.resolve();
    if (!DurableObjectStorage.#flushPendingPuts(this)) return;
    if (Array.isArray(k)) {
      if (k.length === 0) return 0;
      return __storage_delete_many(this._scope, k);
    }
    return __storage_delete(this._scope, k);
  }
  async list(options) {
    DurableObjectStorage.#assertTransactionActive(this, "list");
    const request = __storageListRequest(options, "list", "DurableObjectStorage");
    if (!this._transactionControl) await Promise.resolve();
    if (!DurableObjectStorage.#flushPendingPuts(this)) return new Map();
    return __unwrapStoredMap(__storage_list(
      this._scope, JSON.stringify(request), __storedSentinel));
  }
  async deleteAll() {
    DurableObjectStorage.#assertTransactionActive(this, "deleteAll");
    if (!this._transactionControl) await Promise.resolve();
    if (!DurableObjectStorage.#flushPendingPuts(this)) return;
    __storage_delete_all(this._scope, __cell.deleteAllDeletesAlarm);
    // `getCurrentBookmark` describes the storage it is called on, and starts
    // over with it: the sequence advances per call, never per write (#251).
    this._transactionRoot._bookmarkSequence = 0;
  }
  // Resolves once every write this cell committed before the call is
  // proven durable, by the proof the output gate requires before it releases
  // an egress. Workerd resolves when the pending writes have completed; on
  // Cello the local file is a cache, so "completed" is "proven". The host op
  // rejects when that proof fails or times out, so an application never
  // treats a lost write as safe.
  async sync() {
    DurableObjectStorage.#assertTransactionActive(this, "sync");
    await Promise.resolve();
    const status = DurableObjectStorage.#transactionStatus(this);
    // An admitted sync can resume after transactionSync commits or rolls
    // back. Its boundary settled the queued puts, so prove the surviving
    // state without flushing a later transaction through this handle. A
    // new call after that boundary still fails the entry guard above.
    if (this._state?._aborted || (status === null && !DurableObjectStorage.#flushPendingPuts(this)))
      throw new Error(
        "storage.sync: " + this._scope + " was aborted; the object is no " +
        "longer running");
    await __storage_sync(this._scope);
  }
  async setAlarm(t, options) {
    // Arguments convert before the call, so the options refusal precedes the
    // instant's validation and the alarm-handler check.
    __bindingOptions(options, "setAlarm", "DurableObjectStorage", 2, "SetAlarmOptions");
    // Captured synchronously, before the first await: a call from the
    // constructor runs while the instance is not registered yet, and that is
    // exactly workerd's Initializing state, where setAlarm is allowed
    // because the class may still produce an alarm() handler.
    const instance = __cell.instances[this._scope];
    DurableObjectStorage.#assertTransactionActive(this, "setAlarm");
    if (!this._transactionControl) await Promise.resolve();
    if (!DurableObjectStorage.#flushPendingPuts(this)) return;
    if (this._state?._facetDepth > 0)
      throw new Error("Facets currently cannot set alarms.");
    // Workerd validates the argument, then the instant, then whether the
    // class can ever receive the wake (worker.c++ assertCanSetAlarm).
    const at = __alarmTime(t);
    if (instance !== undefined && typeof instance.alarm !== "function")
      throw new TypeError(
        "Your Durable Object class must have an alarm() handler in order " +
        "to call setAlarm()");
    __alarm_set(this._scope, at);
  }
  async getAlarm() {
    DurableObjectStorage.#assertTransactionActive(this, "getAlarm");
    if (!this._transactionControl) await Promise.resolve();
    if (!DurableObjectStorage.#flushPendingPuts(this)) return null;
    const v = __alarm_get(this._scope);
    return v === null ? null : v;
  }
  async deleteAlarm(options) {
    __bindingOptions(options, "deleteAlarm", "DurableObjectStorage", 1, "SetAlarmOptions");
    DurableObjectStorage.#assertTransactionActive(this, "deleteAlarm");
    if (!this._transactionControl) await Promise.resolve();
    if (!DurableObjectStorage.#flushPendingPuts(this)) return;
    __alarm_delete(this._scope);
  }

  static #transactionStart(self) {
    const root = self._transactionRoot;
    const savepoint = "cells_tx_" + (++root._transactionSerial);
    __storage_transaction_control(
      self._scope, "start", self._transactionDepth > 0, savepoint,
    );
    return savepoint;
  }

  static #transactionCommit(self, savepoint) {
    __storage_transaction_control(
      self._scope, "commit", self._transactionDepth > 0, savepoint,
    );
  }

  static #transactionRollback(self, savepoint, explicit = false) {
    __storage_transaction_control(
      self._scope,
      explicit ? "rollback_explicit" : "rollback",
      self._transactionDepth > 0,
      savepoint,
    );
  }

  static #transactionView(self, transactionControl) {
    return new DurableObjectStorage(
      self._scope,
      self._state,
      self._transactionRoot,
      self._transactionDepth + 1,
      transactionControl,
    );
  }
  transactionSync(f) {
    DurableObjectStorage.#assertTransactionActive(this, "transactionSync");
    const root = this._transactionRoot;
    if (this._transactionDepth === 0) {
      // A callback can call a helper that reaches the root `ctx.storage`
      // handle instead of the transaction view, and the helper starts its own
      // synchronous transaction. Workerd nests that one under the open
      // transaction; cello attempted a second top-level BEGIN and failed with
      // "cannot start a transaction within a transaction"
      // (denoland/celld#226). The open view takes the call instead, so it
      // becomes a savepoint: an inner rollback then discards only the inner
      // writes, and an outer rollback discards both layers.
      //
      // The synchronous transaction is preferred over an asynchronous one,
      // because it is the innermost open layer whenever both are open.
      if (root._activeSyncTransaction !== null)
        return root._activeSyncTransaction.transactionSync(f);
      const owner = String(__io_context_id());
      if (owner !== "" && root._transactionOwner === owner &&
          root._activeTransaction !== null)
        return root._activeTransaction.transactionSync(f);
    }
    const savepoint = DurableObjectStorage.#transactionStart(this);
    const control = DurableObjectStorage.#newTransactionControl(this, savepoint);
    const view = DurableObjectStorage.#transactionView(this, control);
    // No owner check guards this slot, unlike `_activeTransaction`, which an
    // asynchronous transaction publishes across awaits and must therefore keep
    // with the event that opened it. The callback below runs to its end before
    // any other event starts, so only that event can read the slot, and the
    // restore below runs at exactly the point the savepoint ends.
    const previousSync = root._activeSyncTransaction;
    root._activeSyncTransaction = view;
    try {
      // workerd hands the callback nothing: the root's own methods write
      // inside the transaction, and a throw rolls it back.
      const value = f();
      if (!control.rolledBack) {
        DurableObjectStorage.#transactionCommit(this, savepoint);
        control.committed = true;
      }
      return value;
    } catch (error) {
      if (!control.rolledBack) {
        try {
          DurableObjectStorage.#transactionRollback(this, savepoint);
          control.rolledBack = true;
        } catch (rollbackError) {
          DurableObjectStorage.#abortAfterFailedRollback(this, rollbackError, error);
        }
      }
      throw error;
    } finally {
      root._activeSyncTransaction = previousSync;
    }
  }

  static #newTransactionControl(self, savepoint) {
    const control = {
      rolledBack: false,
      rollbackPerformed: false,
      committed: false,
      parent: self._transactionControl,
      pending: new Set(),
      rollback: () => {
        DurableObjectStorage.#transactionRollback(self, savepoint, true);
        control.rollbackPerformed = true;
      },
    };
    return control;
  }

  static async #drainNestedTransactions(self, control) {
    while (control.pending.size !== 0)
      await Promise.allSettled(Array.from(control.pending));
  }

  static async #runTransactionWith(self, f, savepoint, control) {
    const view = DurableObjectStorage.#transactionView(self, control);
    const root = self._transactionRoot;
    const owner = String(__io_context_id());
    const installsActive = owner !== "" && root._transactionOwner === owner;
    const previousActive = root._activeTransaction;
    if (installsActive) root._activeTransaction = view;
    try {
      const value = await f(view);
      // A helper can start a reentrant transaction through the root storage
      // handle and omit the returned promise. That child already owns a
      // savepoint, so ending this layer first would make SQLite release the
      // child's savepoint and let its continuation run against a false owner.
      await DurableObjectStorage.#drainNestedTransactions(self, control);
      if (!control.rolledBack) {
        DurableObjectStorage.#transactionCommit(self, savepoint);
        control.committed = true;
      } else if (!control.rollbackPerformed) {
        control.rollback();
      }
      return value;
    } catch (error) {
      await DurableObjectStorage.#drainNestedTransactions(self, control);
      if (!control.rollbackPerformed) {
        // The flag records a rollback that happened, not one that was
        // attempted: a rollback that fails leaves the savepoint open, and a
        // later transaction on the same connection would commit its writes.
        // Nothing can be said about the connection then, so the object is
        // aborted rather than left to serve from it.
        try {
          DurableObjectStorage.#transactionRollback(self, savepoint);
          control.rolledBack = true;
          control.rollbackPerformed = true;
        } catch (rollbackError) {
          DurableObjectStorage.#abortAfterFailedRollback(self, rollbackError, error);
        }
      }
      throw error;
    } finally {
      if (installsActive && root._activeTransaction === view)
        root._activeTransaction = previousActive;
    }
  }
  // `cause` is the failure that led to the rollback; the abort's error keeps
  // it, so a debugger can walk back to the callback's own throw. The
  // rollback failure is reported even when the object is already aborted,
  // as it is after a block's limit: the abort then changes nothing, and the
  // log line is the only trace the failure leaves.

  static #abortAfterFailedRollback(self, rollbackError, cause) {
    // Inside a catch, so the log must not become the throw that leaves it:
    // the slot release after the caller's rethrow depends on that.
    try { console.error("transaction rollback failed", rollbackError); } catch {}
    const state = self._transactionRoot._state;
    if (state && !state._aborted) {
      state.abort(
        new Error(
          "a transaction rollback failed: " + __describeFailure(rollbackError),
          { cause },
        ),
      );
    }
  }

  static async #runTransaction(self, f) {
    const savepoint = DurableObjectStorage.#transactionStart(self);
    return DurableObjectStorage.#runTransactionWith(self, f, savepoint, DurableObjectStorage.#newTransactionControl(self, savepoint));
  }
  // The session bookmarks workerd's storage reports. A local back-end has no
  // point-in-time recovery, and workerd's own says so rather than inventing a
  // value, so the two methods that would need it refuse with its message after
  // their arguments convert. `getCurrentBookmark` needs none of that: the
  // local back-end answers a sequence that advances once per call, and
  // `deleteAll` starts it over (#251).
  async getCurrentBookmark() {
    DurableObjectStorage.#assertTransactionActive(this, "getCurrentBookmark");
    const root = this._transactionRoot;
    const sequence = root._bookmarkSequence;
    root._bookmarkSequence = sequence + 1;
    return __storageBookmark(sequence);
  }
  async getBookmarkForTime() {
    DurableObjectStorage.#assertTransactionActive(this, "getBookmarkForTime");
    __storageDate(arguments[0], "getBookmarkForTime");
    throw __noPointInTimeRecovery();
  }
  async onNextSessionRestoreBookmark() {
    DurableObjectStorage.#assertTransactionActive(
      this, "onNextSessionRestoreBookmark");
    // A required argument that is not a string is the WebIDL refusal; every
    // other value coerces and then meets the same missing back-end.
    if (arguments.length === 0)
      throw new TypeError(
        "Failed to execute 'onNextSessionRestoreBookmark' on " +
          "'DurableObjectStorage': parameter 1 is not of type 'string'.",
      );
    __jsString(arguments[0]);
    throw __noPointInTimeRecovery();
  }
  async transaction(f) {
    DurableObjectStorage.#assertTransactionActive(this, "transaction");
    if (this._transactionDepth > 0) {
      const nested = DurableObjectStorage.#runTransaction(this, f);
      const pending = this._transactionControl.pending;
      pending.add(nested);
      nested.then(
        () => pending.delete(nested),
        () => pending.delete(nested),
      );
      return nested;
    }
    const root = this._transactionRoot;
    const owner = String(__io_context_id());
    if (owner !== "" && root._transactionOwner === owner &&
        root._activeTransaction !== null)
      return root._activeTransaction.transaction(f);
    // Workerd runs the callback under blockConcurrencyWhile: the input gate
    // shuts, so no other event starts in the object while the transaction is
    // open. Without it another event's reads saw uncommitted rows, its
    // writes joined the open transaction on the same connection, and a
    // rollback discarded a write cello had already acknowledged (#714). The
    // callback's failure is caught inside the block: a failed transaction
    // rolls back and rejects, and must not reset the object the way a failed
    // critical section does.
    //
    // The tail orders transactions one event starts concurrently, which the
    // gate does not separate. Its slot is taken here, in call order: taken
    // inside the block it would be taken in the order the gate woke the
    // callers, which is not theirs to keep. It is released inside the block,
    // when the callback has settled, and not when the block has: a second
    // transaction the same event starts while the first runs rides the
    // outer hold as a nested block, and the outer block waits for nested
    // ones before it releases, so a slot held until the block's end would
    // wait on itself.
    const previous = root._transactionTail;
    let release;
    root._transactionTail = new Promise((resolve) => { release = resolve; });
    let control = null;
    let started = false;
    let abandoned = null;
    const guarded = async () => {
      started = true;
      let ownsTransaction = false;
      try {
        await previous;
        // The block ended while this waited for its slot, or the object was
        // reset under it by an enclosing block that failed: the caller has
        // its rejection and the gate is open again, so starting the
        // transaction now would run it beside whatever the object serves.
        if (abandoned !== null) return { error: abandoned };
        if (root._state._aborted) {
          return { error: new Error("the Durable Object was reset before the transaction started") };
        }
        // A callback can compose a helper which reaches the root
        // `ctx.storage` handle instead of the transaction view. The input
        // gate excludes other events, but an event which entered before the
        // gate closed can still be waiting for this slot. Keep the owner with
        // the open transaction so only the callback's event takes the
        // savepoint path; another event still waits on the tail.
        root._transactionOwner = owner;
        ownsTransaction = true;
        // The start is a host op that can fail like any other statement, and
        // a failure there is the transaction's to report, not the block's:
        // a throw that escaped here would reset the object.
        try {
          const savepoint = DurableObjectStorage.#transactionStart(this);
          control = DurableObjectStorage.#newTransactionControl(this, savepoint);
          return { value: await DurableObjectStorage.#runTransactionWith(this, f, savepoint, control) };
        } catch (error) {
          return { error };
        }
      } finally {
        if (ownsTransaction && root._transactionOwner === owner) {
          root._activeTransaction = null;
          root._transactionOwner = "";
        }
        release();
      }
    };
    let outcome;
    try {
      outcome = await root._state.blockConcurrencyWhile(guarded);
    } catch (error) {
      // The block itself failed: its limit ended the callback while it still
      // ran, or the block ahead of it failed and this one never started. A
      // transaction the callback still holds would otherwise stay open on
      // the connection the object's replacement uses, and the callback's
      // late commit would carry the replacement's writes with it. Roll it
      // back now, unless the callback's continuation committed in the same
      // tick the limit won; the callback's own commit is skipped from here
      // on. The slot stays with the callback until it settles: the callback
      // still runs on the connection, and a transaction started beside it
      // would interleave with its remaining statements. A routed child must
      // release its savepoint before this layer rolls back, so a live child
      // leaves the rollback for `_runTransactionWith`. A block that failed
      // before the callback started holds nothing, so its slot is released
      // here or nothing ever would.
      abandoned = error;
      if (control && !control.rolledBack && !control.committed) {
        try {
          if (control.pending.size === 0) control.rollback();
          control.rolledBack = true;
        } catch (rollbackError) {
          DurableObjectStorage.#abortAfterFailedRollback(this, rollbackError, error);
        }
      }
      if (!started) release();
      throw error;
    }
    if ("error" in outcome) throw outcome.error;
    return outcome.value;
  }
}
// The two parts a namespace and the host need from an id, which workerd keeps
// to itself: a private slot has no reader outside the class, and a static block
// is the one place that may hand one out (#251).
let __idClassName;
let __idScope;
__webidlTag(DurableObjectStorage, "DurableObjectStorage");
__webidlPrototype(DurableObjectStorage);
__nativeMethodArities(DurableObjectStorage);
class DurableObjectId {
  static {
    __idClassName = (id) => id.#className;
    __idScope = (id) => id.#className + ":" + id.#value;
  }
  #className;
  #value;
  constructor(className, value, name = undefined) {
    this.#className = className;
    this.#value = value;
    // workerd's own enumerable properties, in its order and read-only: a
    // Worker reads an id's name and jurisdiction, never writes them (#251).
    __primDefineProp(this, "jurisdiction", {
      value: undefined, enumerable: true, configurable: true, writable: false,
    });
    __primDefineProp(this, "name", {
      value: name, enumerable: true, configurable: true, writable: false,
    });
  }
  // `String(id)` reads the hex identity through `toString`, and the tag names
  // the interface, as workerd's does.
  toString() { return this.#value; }
  equals(other) {
    return other instanceof DurableObjectId && other.#value === this.#value;
  }
}
__webidlTag(DurableObjectId, "DurableObjectId");
__webidlPrototype(DurableObjectId);
__nativeMethodArities(DurableObjectId);
// Internal to the runtime: workerd has no such global, and a bundle that
// catches it sees the message and code, not this class.
const DurableObjectRoutingError = class DurableObjectRoutingError
  extends Error {
  constructor(detail = {}) {
    super("The Durable Object owner is currently unreachable");
    this.name = "DurableObjectRoutingError";
    this.code = "owner_unreachable";
    this.retryable = true;
    if (typeof detail.scope === "string") this.scope = detail.scope;
    if (typeof detail.owner === "string") this.owner = detail.owner;
  }
};
const __durableObjectRoutingError = (error) => {
  const marker = "__CELLO_DO_ROUTING_ERROR__:";
  const message = String(error && error.message || error);
  const offset = message.indexOf(marker);
  if (offset < 0) return null;
  try {
    return new DurableObjectRoutingError(
      JSON.parse(message.slice(offset + marker.length)),
    );
  } catch {
    return new DurableObjectRoutingError();
  }
};
// A Durable Object handler's uncaught exception reaches a stub caller as the
// value the handler threw, the way workerd propagates it: a standard error
// class is rebuilt as itself, a DOMException stays one, a non-Error object
// crosses by value, and a primitive becomes an Error with its text. Every
// object answer carries workerd's own `remote` marker, and an error's stack
// is the first line only -- no Cello frames reach the guest.
const __cellErrorCtors = Object.freeze({
  Error, TypeError, RangeError, SyntaxError, ReferenceError, EvalError,
  URIError, AggregateError,
});
const __remoteStackLine = (name, message) =>
  message === "" || message === undefined ? String(name) : `${name}: ${message}`;
// The caller's own frames, without harness frames: workerd's remote stack is
// the guest's call site, and an internal `<anonymous>:...` line would leak the
// host's layout to the guest.
const __callerFrames = () => {
  const stack = new Error().stack;
  if (typeof stack !== "string") return "";
  const frames = stack.split("\n").slice(1)
    .filter((line) => !line.includes("<anonymous>"));
  return frames.length === 0 ? "" : "\n" + frames.join("\n");
};
// Rebuild the value a stub call failed with. The first failure of an event
// crosses as the handler's own error with workerd's `remote` marker; a
// broken stub replays the same class and message for later calls without
// the marker, and an RPC failure never carries it -- that is what workerd
// reports for both.
const __cellHandlerThrow = (details) => __cellHandlerBuild(details, true);
const __cellHandlerReplay = (details) => __cellHandlerBuild(details, false);
const __cellHandlerBuild = (details, remote) => {
  const name = String(details?.name ?? "Error");
  const message = details?.message === undefined ? "" : String(details.message);
  let value;
  switch (details?.kind) {
    case "dom":
      value = new __primDOMException(message, name);
      break;
    case "object":
      try {
        value = __sc_decode(new Uint8Array(details.value ?? []));
      } catch {
        value = new Error(message);
        value.name = name;
      }
      break;
    case "primitive":
      value = new Error(message);
      break;
    default: {
      const ctor = __primHasOwn(__cellErrorCtors, name)
        ? __cellErrorCtors[name] : undefined;
      if (ctor !== undefined) {
        value = new ctor(message);
      } else {
        value = new Error(message);
        value.name = name;
      }
    }
  }
  if (remote && value !== null && typeof value === "object") {
    __primDefineProp(value, "remote", {
      value: true, enumerable: true, configurable: true, writable: true,
    });
    if (value instanceof Error)
      value.stack = __remoteStackLine(value.name, value.message);
  }
  return value;
};
// The marked RPC reply the callee sends instead of a result when the handler
// failed: the same JSON the fetch path sends, prefixed so the caller can
// tell it apart from structured-clone bytes. Must match
// `engine_api::RPC_FAILURE_PREFIX`.
const __rpcFailure = (raw) => {
  let text;
  try {
    text = typeof raw === "string" ? raw : new TextDecoder().decode(raw);
  } catch {
    return undefined;
  }
  const prefix = "__celloRpcFailure:";
  if (!text.startsWith(prefix)) return undefined;
  try {
    return JSON.parse(text.slice(prefix.length));
  } catch {
    return undefined;
  }
};
// A cell's input gate as the isolate sees it, for the one delivery
// point that is inside the isolate rather than in front of it: an
// RPC stub op. `events` records every block that has asked for the
// gate, so no window exists in which a block has started and the
// isolate still thinks the cell is open. `holder` names the block
// actually running; another event can still be queued.
//
// A stub minted while a block runs carries that block's token and
// re-enters it instead of queueing behind it. That is Workerd's
// `IoContext::makeReentryCallback`, and without it a callback the
// block itself sent out could never come back.
const __cellBlocks = new Map();
const __blockEnter = (scope, event, owner) => {
  let settleReady;
  let rejectReady;
  const ready = new Promise((resolve, reject) => {
    settleReady = resolve;
    rejectReady = reject;
  });
  const record = {
    owner,
    reaction: String(__io_context_id()),
    actorEvent: __currentActorEvent(),
    retired: false,
    ready,
    settleReady,
    rejectReady,
  };
  const block = __cellBlocks.get(scope);
  if (block !== undefined) {
    block.events.set(event, record);
    return { block, record };
  }
  const fresh = {
    holder: null,
    events: new Map([[event, record]]),
  };
  __cellBlocks.set(scope, fresh);
  return { block: fresh, record };
};
const __blockLeave = (scope, block, event) => {
  if (!block.events.delete(event)) return;
  if (block.holder === event) block.holder = null;
  if (block.events.size === 0 && __cellBlocks.get(scope) === block)
    __cellBlocks.delete(scope);
};
const __durableClassMeta = new WeakMap();
let __nextFacetOwner = 1;
// workerd's message for a method whose receiver is not what its interface
// requires, verbatim. One constant: `Fetcher`, a loopback namespace and a
// service stub all refuse this way, and a second copy is how they drift
// (#259).
const __ILLEGAL_INVOCATION =
  "Illegal invocation: function called with incorrect `this` reference. " +
  "See https://developers.cloudflare.com/workers/observability/" +
  "errors/#illegal-invocation-errors for details.";
const __illegalInvocation = () => new TypeError(__ILLEGAL_INVOCATION);
// ---- ctx.exports handle interfaces (#259) -----------------------------------
// workerd gives each handle kind two prototype levels: one holding only an
// own non-enumerable `constructor` and the interface's tag, over the level
// carrying the members. A Worker reflects both, so they are reproduced
// rather than collapsed onto the handle.
//
// Built on first use, because the interfaces they sit over --
// `DurableObjectNamespace` and `__cf.ServiceStub` -- are defined further down
// this file.
const __interfaceConstructor = (name) => ({ [name]: function () {
  throw new TypeError("Illegal constructor");
} })[name];
const __interfaceTag = (proto, tag) =>
  Object.defineProperty(proto, Symbol.toStringTag, {
    value: tag, configurable: true, writable: true, enumerable: false,
  });
let __handlePrototypes;
const __handleProto = (kind) => (__handlePrototypes ??= (() => {
  const specialized = __interfaceConstructor("DurableObjectClass");
  __interfaceTag(specialized.prototype, "DurableObjectClass");
  const plainClass = __interfaceConstructor("LoopbackDurableObjectClass");
  Object.setPrototypeOf(plainClass.prototype, specialized.prototype);
  __interfaceTag(plainClass.prototype, "LoopbackDurableObjectClass");
  const namespace = __interfaceConstructor("LoopbackDurableObjectNamespace");
  Object.setPrototypeOf(namespace.prototype, DurableObjectNamespace.prototype);
  __interfaceTag(namespace.prototype, "LoopbackDurableObjectNamespace");
  const service = __interfaceConstructor("LoopbackServiceStub");
  Object.setPrototypeOf(service.prototype, __cf.ServiceStub.prototype);
  __interfaceTag(service.prototype, "LoopbackServiceStub");
  return {
    namespace: namespace.prototype,
    class: plainClass.prototype,
    specialized: specialized.prototype,
    service: service.prototype,
  };
})())[kind];
// Takes encoded props, not the caller's options bag: an options bag reaching
// here is a bag nothing validated, and reading one key out of it is how every
// other key came to be dropped in silence. The loader stub validates the bag
// and encodes its value before it creates the class.
const __makeDurableObjectClass = (loadPromise, name, propsSc) => {
  // workerd's specialized handle is a `DurableObjectClass` instance: nothing
  // of its own, and its tag and `constructor` come from that level.
  const value = Object.create(__handleProto("specialized"));
  __durableClassMeta.set(value, {
    loadPromise,
    name: name === null || name === undefined ? "default" : String(name),
    propsSc,
  });
  return value;
};
// Loopback classes are callable, and `new` on one specializes exactly as a
// call does: workerd's handle is constructible with no own properties, which
// an arrow (never a constructor) and a plain function (always carrying a
// non-configurable own `prototype`) cannot both be. A bound function forwards
// `new` to its target -- returning the specialized object or throwing the
// call path's refusal -- while carrying no `prototype` of its own. The
// refusals are workerd's own (`callImpl`'s required `Options` dictionary, then
// the `props` field's conversion): `null` and `undefined` are the empty
// dictionary as WebIDL converts them, and `props` is read once. Capture props
// now, so later mutation of the caller's options cannot change startup.
// A specialized class is only a handle: `new` on it stays "not a constructor".
const __loopbackOptionsRefusal = (kind) => new TypeError(
  "Failed to execute '(called as function)' on '" + kind +
  "': parameter 1 is not of type 'Options'.");
const __loopbackPropsRefusal = () => new TypeError(
  "Incorrect type for the 'props' field on 'Options': the provided value " +
  "is not of type 'Object'.");
const __makeLoopbackDurableObjectClass = (name, kind) => {
  const loadPromise = Promise.resolve({ id: 0 });
  const specialize = (function (...args) {
    // A missing argument is refused before the dictionary converts, and
    // calling a loopback handle is the only way it is ever invoked: `new`
    // runs this same body through the bound function's construct trap, so it
    // reports the same refusal or answers the same specialized object.
    if (args.length === 0) throw __loopbackOptionsRefusal(kind);
    const options = args[0];
    if (options !== undefined && options !== null &&
        typeof options !== "object" && typeof options !== "function")
      throw __loopbackOptionsRefusal(kind);
    const props = options == null ? undefined : options.props;
    if (props !== undefined &&
        (props === null ||
         (typeof props !== "object" && typeof props !== "function")))
      throw __loopbackPropsRefusal();
    return __makeDurableObjectClass(
      loadPromise, name, __rpcOut(props === undefined ? {} : props, false));
  }).bind(null);
  // workerd's loopback handles carry no own `name`/`length`/`prototype`, and
  // the interface's tag comes from the prototype level there (#259). The
  // namespace's callable keeps `Function.prototype`: the handle above it
  // carries the level a Worker reflects.
  delete specialize.name;
  delete specialize.length;
  if (kind === "LoopbackDurableObjectClass")
    Object.setPrototypeOf(specialize, __handleProto("class"));
  __durableClassMeta.set(specialize, {
    loadPromise, name, propsSc: __rpcOut({}, false),
  });
  return specialize;
};
// workerd's `ctx.exports` entry for a Durable Object class is its loopback
// namespace: callable (calling it specializes the class for a facet, as a
// DurableObjectClass handle does) and carrying the namespace methods one level
// down, on `DurableObjectNamespace.prototype` itself -- the same function
// objects a real namespace answers with, which is why workerd routes either
// receiver through either method. The namespace's state stays in private
// slots, so the shared method reads a loopback handle's namespace out of a
// WeakMap and lets the class's own brand check accept a genuine one (#259).
let __loopbackNamespaceHandles;
const __loopbackNamespace = (receiver) => {
  const handles = __loopbackNamespaceHandles;
  const namespace = handles === undefined ? undefined : __weakGet(handles, receiver);
  if (namespace !== undefined) return namespace;
  if (receiver instanceof DurableObjectNamespace) return receiver;
  throw __illegalInvocation();
};
const __makeLoopbackNamespace = (name) => {
  __loopbackNamespaceHandles ??= new __primWeakMap();
  const namespace = __cell.makeNamespace(name);
  const specialize = __makeLoopbackDurableObjectClass(
    name, "LoopbackDurableObjectNamespace");
  // A bound function, so `new` on the handle specializes through the same
  // call path: a plain function would carry an own `prototype` workerd's
  // handle does not have.
  const handle = (function (...args) { return specialize(...args); }).bind(null);
  delete handle.name;
  delete handle.length;
  Object.setPrototypeOf(handle, __handleProto("namespace"));
  __weakSet(__loopbackNamespaceHandles, handle, namespace);
  // A facet accepts this handle where it accepts a specialized class.
  __durableClassMeta.set(handle, __durableClassMeta.get(specialize));
  return handle;
};
class DurableObjectFacets {
  constructor(state) {
    this._state = state;
    this._owner = String(__nextFacetOwner++);
    this._next = 1;
    this._running = new Map();
    this._barriers = new Map();
  }
  _validName(name) {
    name = String(name);
    if (new TextEncoder().encode(name).length > 256)
      throw new TypeError("Facet name is too long (max 256 characters).");
    return name;
  }
  get(name, getStartupOptions) {
    name = this._validName(name);
    if (this._state._facetDepth >= 3)
      throw new Error(
        "Facet nesting depth limit exceeded. The maximum depth including the root Durable Object is 4.");
    if (typeof getStartupOptions !== "function")
      throw new TypeError("The facet startup callback must be a function.");
    let record = this._running.get(name);
    if (record !== undefined) return record.stub;
    // Each run gets a distinct host scope. An aborted request can still have
    // a pending continuation, so reusing its scope would let that continuation
    // enter the replacement instance after the abort barrier opens.
    record = {
      aborted: false,
      error: undefined,
      owner: this._owner + ":" + this._next++,
      started: null,
      stub: null,
    };
    const barrier = this._barriers.get(name);
    const start = () => record.started ??= Promise.resolve(barrier)
      .then(getStartupOptions)
      .then(async (options) => {
        const meta = __durableClassMeta.get(options?.class);
        if (meta === undefined)
          throw new TypeError(
            "FacetStartupOptions.class must be a DurableObjectClass.");
        const { id: loader } = await meta.loadPromise;
        const id = options.id === undefined ? this._state.id : options.id;
        // Keep the ID kind and name in one clone. A string round-trip erases
        // named IDs and turns a literal string ID into a DurableObjectId.
        const idSc = __sc_encode(id instanceof DurableObjectId
          ? [__idClassName(id), id.toString(), id.name] : String(id));
        // Keep the load alive after the startup callback and class are gone.
        return [loader, meta.name, idSc, meta.propsSc, meta];
      });
    const invoke = async (operation) => {
      if (record.aborted) throw record.error;
      return operation(await start());
    };
    const target = {};
    // Methods share the loaded-worker RPC transport. A facet stub is not
    // awaitable, and a property is a single callable pipeline node.
    const session = {
      get: () => Promise.reject(new Error(
        "Awaitable properties on facets are not supported yet.")),
      call: (path, args) => invoke(async ([loader, className, id, propsSc]) => {
        if (path.length !== 1)
          throw new Error(
            "Pipelined property paths on facets are not supported yet.");
        return __rpcDes(await __facet_rpc(
          loader, className, this._state._scope, record.owner, name, id,
          propsSc, path[0], __rpcOut(args, false)));
      }),
    };
    // Arrow closures retain the manager because `target.fetch`'s method
    // receiver is the target object, not the DurableObjectFacets instance.
    const manager = this;
    target.fetch = async function(input, init) {
      return invoke(async ([loader, className, id, propsSc]) => {
        const adoptsBody = __fetchAdoptsBody(input, init);
        const usedBefore = adoptsBody && input.bodyUsed;
        const req = new Request(input, init);
        if (adoptsBody) __fetchTakeBody(input, usedBefore);
        const headers = JSON.stringify(__cello.$$headerList(req.headers));
        const { body, streamId } = await __subrequestBody(req);
        const response = JSON.parse(await __facet_fetch(
          loader, className, manager._state._scope, record.owner, name, id,
          propsSc, req.url, req.method, body, headers, streamId));
        const responseBody = __hostReplyBody(response.status, response);
        // The facet's socket lives in the facet's isolate. The target lets
        // the root return the upgrade, or accept the client end that the
        // wrapper binds to the facet's socket.
        return __wrapServiceResponse(new Response(responseBody, {
          status: response.status,
          headers: response.headers,
          __wsTarget: response.wsTarget,
        }), req.url);
      });
    };
    record.stub = new Proxy(target, {
      get(base, prop) {
        if (prop === "then") return undefined;
        if (Reflect.has(base, prop)) return Reflect.get(base, prop);
        if (typeof prop !== "string") return undefined;
        return __makeNode(session, [prop], null);
      },
    });
    this._running.set(name, record);
    return record.stub;
  }
  _abort(name, reason) {
    name = this._validName(name);
    const record = this._running.get(name);
    if (record === undefined) return Promise.resolve();
    record.aborted = true;
    record.error = reason;
    this._running.delete(name);
    const barrier = record.started === null ? Promise.resolve() : record.started
      .then(([loader, className]) =>
        __facet_abort(
          loader, className, this._state._scope, record.owner, name), () => {});
    this._barriers.set(name, barrier);
    const clear = () => {
      if (this._barriers.get(name) === barrier) this._barriers.delete(name);
    };
    barrier.then(clear, clear);
    return barrier;
  }
  abort(name, reason) {
    this._abort(name, reason);
  }
  delete(name) {
    name = this._validName(name);
    const barrier = this._abort(name, new Error("Facet was deleted.")).then(() =>
      __facet_delete(this._state._scope, name));
    this._barriers.set(name, barrier);
    const clear = () => {
      if (this._barriers.get(name) === barrier) this._barriers.delete(name);
    };
    // Keep a failed delete as the name's barrier. Clearing it would let the
    // next get reload the image which the failed operation did not delete.
    barrier.then(clear, () => {});
  }
  _release() {
    // The host closes these facets during give-back. Queuing _abort here
    // leaves an undriven op whose promise retains the manager, its root,
    // and the loaded classes after the root has stopped.
    const reason = new Error("The parent Durable Object was released.");
    for (const record of this._running.values()) {
      record.aborted = true;
      record.error = reason;
    }
    this._running.clear();
    this._barriers.clear();
  }
}
// The reason a failed critical section gives its waiters. It must not throw:
// it runs where a throw would skip the gate's release, and a value with no
// string form, such as `Object.create(null)`, is a legal thing to throw.
function __describeFailure(error) {
  try {
    return String((error && error.message) || error);
  } catch {
    return "critical section failed";
  }
}
// ---- Containers -----------------------------------------------------------
// `ctx.container` for a class named in the deployment's `containers`. The
// surface and every message follow workerd's api/container.c++; the host
// ops perform the effects against the node's container engine, and the
// scope names the cell the way the storage ops do.
const __containerEnvList = (env, what) => {
  const list = [];
  if (env === undefined || env === null) return list;
  for (const [name, value] of Object.entries(env)) {
    const text = String(value);
    if (name.includes("="))
      throw new Error(`Environment variable names cannot contain '=': ${name}`);
    if (name.includes("\0"))
      throw new Error(`Environment variable names cannot contain '\\0': ${name}`);
    if (text.includes("\0"))
      throw new Error(`Environment variable values cannot contain '\\0': ${name}`);
    list.push([name, text]);
  }
  return list;
};
const __execOutputMode = (mode, kind) => {
  const value = mode === undefined ? "pipe" : String(mode);
  if (value === "pipe" || value === "ignore" ||
      (kind === "stderr" && value === "combined")) return value;
  throw new TypeError(`Invalid ${kind} option: ${value}`);
};
const __validSignal = (signo) => Number.isInteger(signo) && signo > 0 && signo <= 64;
const __execStream = (id, which) => new ReadableStream({
  type: "bytes",
  async pull(controller) {
    // Retained across native-event retirements: a read issued in one
    // event and consumed in another (a read-ahead, a terminal frame)
    // re-subscribes instead of hanging on the dropped op.
    const chunk = await __retainedExecRead(id, which);
    if (chunk.byteLength === 0) controller.close();
    else controller.enqueue(new Uint8Array(chunk));
  },
});
class ContainerExecOutput {
  constructor(stdout, stderr, exitCode) {
    this.stdout = stdout;
    this.stderr = stderr;
    this.exitCode = exitCode;
  }
}
class ContainerExecProcess {
  // The host process id is private: ids are process-global sequential
  // numbers, and a mutable `this._id` let any container-DO code retarget
  // wait/drop/kill/resize at another cell's exec. Streams already pin the
  // construction-time id via closure.
  #id;
  constructor(id, pid, stdinMode, stdoutMode, stderrMode, isPty = false, signal = undefined) {
    this.#id = id;
    this.pid = pid;
    this.isPty = !!isPty;
    this._outputCalled = false;
    this._exitCode = null;
    // workerd sends SIGKILL when the exec signal aborts
    // (api/container.c++ `ExecProcess`, `kSigKill = 9`; the Durable
    // Object Container API documents "Aborting the signal sends SIGKILL
    // to the process"). An abort that lands while the exec op is in
    // flight kills the fresh process at once; otherwise the listener
    // below does it, and is removed when the process exits, so a later
    // abort cannot signal a dead process and the signal cannot retain
    // this process.
    if (signal !== undefined && signal !== null) {
      if (signal.aborted) {
        this.kill(9);
      } else {
        const onAbort = () => this.kill(9);
        signal.addEventListener("abort", onAbort, { once: true });
        const unwire = () => signal.removeEventListener("abort", onAbort);
        this.exitCode.then(unwire, unwire);
      }
    }
    this.stdout = stdoutMode === "pipe" ? __execStream(id, 1) : null;
    // A PTY merges stderr into stdout; there is no separate stream.
    this.stderr = this.isPty || stderrMode !== "pipe"
      ? null : __execStream(id, 2);
    this.stdin = stdinMode === "pipe"
      ? new WritableStream({
        write: (chunk) => __container_exec_write(id, __bytesOf(chunk)),
        close: () => __container_exec_close(id),
        abort: () => __container_exec_close(id),
      })
      : null;
  }
  get exitCode() {
    if (this._exitCode === null) {
      // Retained across native-event retirements: the backing wait is
      // unrefed (it dies with its subscribing event), so the stable
      // promise re-subscribes in each later event instead of hanging
      // on the first event's dropped op.
      const id = this.#id;
      this._exitCode =
        __retainedWait(() => __container_exec_wait(id).then(Number));
    }
    return this._exitCode;
  }
  async output() {
    if (this._outputCalled) throw new TypeError("output() can only be called once.");
    this._outputCalled = true;
    for (const [stream, name] of [[this.stdout, "stdout"], [this.stderr, "stderr"]]) {
      if (stream && stream.locked)
        throw new TypeError(`Cannot call output() after ${name} has started being consumed.`);
    }
    const all = (stream) => stream
      ? new Response(stream).arrayBuffer()
      : Promise.resolve(new ArrayBuffer(0));
    const [stdout, stderr] = await Promise.all([all(this.stdout), all(this.stderr)]);
    const exitCode = await this.exitCode;
    // Nothing else can read the process now, so the host can forget it.
    __container_exec_drop(this.#id);
    return new ContainerExecOutput(stdout, stderr, exitCode);
  }
  kill(signal = 15) {
    if (!__validSignal(signal)) throw new RangeError("Invalid signal number.");
    __container_exec_kill(this.#id, signal).catch(() => {});
  }
  async resize(cols, rows) {
    if (!this.isPty)
      throw new Error("resize() requires a PTY process.");
    if (!Number.isInteger(cols) || cols <= 0 || cols > 65535 ||
        !Number.isInteger(rows) || rows <= 0 || rows > 65535)
      throw new TypeError("resize() cols and rows must be 1..=65535.");
    await __container_exec_resize(this.#id, cols, rows);
  }
}
const __bytesOf = (chunk) => {
  const bytes = __hostChunk(chunk, "stdin accepts bytes");
  if (bytes === null) throw new TypeError("stdin accepts bytes");
  return bytes;
};
// The Fetcher `getTcpPort()` returns: `fetch()` speaks HTTP to the port over
// the node's route to the container, and `connect()` is the raw socket.
class ContainerPort {
  constructor(scope, port) {
    this._scope = scope;
    this._port = port;
  }
  async fetch(input, init) {
    const adoptsBody = __fetchAdoptsBody(input, init);
    const usedBefore = adoptsBody && input.bodyUsed;
    const request = new Request(input, init);
    const url = new URL(request.url);
    if (url.protocol === "https:")
      throw new Error(
        "Connecting to a container using HTTPS is not currently supported; use HTTP " +
        "instead. TLS is unnecessary anyway, as the connection is already secure by default.",
      );
    if (url.protocol !== "http:") throw new TypeError(`Fetch API cannot load: ${url}`);
    // The body is taken only once the request is actually sent: the protocol
    // refusals above leave the input untouched.
    if (adoptsBody) __fetchTakeBody(input, usedBefore);
    const authority = url.host;
    url.host = __container_address(this._scope, this._port);
    const forwarded = new Request(url.toString(), request);
    forwarded.headers.set("host", authority);
    try {
      return await fetch(forwarded);
    } catch (error) {
      // A refused, reset or timed-out connection is the port not listening
      // yet, which the `@cloudflare/containers` start loop recognizes by this
      // phrase and retries; any other failure keeps its own message. The
      // fetch error carries its root cause, not reqwest's wrapper text.
      const message = error?.message ?? String(error);
      if (/error sending request|connection|connect|timed out/i.test(message)) {
        throw new Error(
          `The container is not listening on port ${this._port}: ${message}`,
        );
      }
      throw error;
    }
  }
  connect(address, options) {
    if (options?.secureTransport === "on")
      throw new Error(
        "Connencting to a container using TLS is not currently supported. It is " +
        "unnecessary anyway, as the connection is already secure by default.",
      );
    return __cello.__cfSockets.connect(
      __container_address(this._scope, this._port), options);
  }
}
const CONTAINER_IMAGES = Object.freeze({ default: "default" });

class Container {
  constructor(scope) {
    this._scope = scope;
    this._destroyReason = undefined;
  }
  // The host's answer, not a JavaScript flag: a monitor() promise can be
  // dropped with its event, so the flag it would maintain could go stale.
  get running() { return __container_running(this._scope); }
  // The class's images by name, as on Cloudflare (`ctx.container.images`).
  // Cello deploys one image per class, so `default` is the only entry, and
  // it is what `start({ image })` takes.
  get images() { return CONTAINER_IMAGES; }
  start(options) {
    if (this.running)
      throw new Error("start() cannot be called on a container that is already running.");
    const o = options ?? {};
    if (o.image !== undefined && o.image !== CONTAINER_IMAGES.default)
      throw new TypeError(`start(): unknown image ${JSON.stringify(String(o.image))}; this class has only ctx.container.images.default`);
    const params = { enableInternet: !!o.enableInternet, labels: [] };
    if (o.entrypoint !== undefined) params.entrypoint = Array.from(o.entrypoint, String);
    params.env = __containerEnvList(o.env);
    if (o.labels !== undefined) {
      for (const [name, value] of Object.entries(o.labels)) {
        if (name.length === 0) throw new Error("Label names cannot be empty");
        for (const text of [name, String(value)]) {
          for (let i = 0; i < text.length; i++) {
            if (text.charCodeAt(i) < 0x20)
              throw new Error(`Label names cannot contain control characters (index ${i})`);
          }
        }
        params.labels.push([name, String(value)]);
      }
    }
    if (o.hardTimeout !== undefined && !(o.hardTimeout > 0))
      throw new RangeError("Hard timeout must be greater than 0");
    // Fire and forget, as on Cloudflare: a start that fails reports
    // through monitor(). The host marks the run as running at once.
    this._destroyReason = undefined;
    __container_start(this._scope, JSON.stringify(params)).catch(() => {});
  }
  async monitor() {
    if (!this.running)
      throw new Error("monitor() cannot be called on a container that is not running.");
    let raw;
    try {
      raw = await __container_monitor(this._scope);
    } catch (error) {
      this._destroyReason = undefined;
      throw error;
    }
    const exitCode = Number(raw);
    if (this._destroyReason !== undefined) {
      const reason = this._destroyReason;
      this._destroyReason = undefined;
      throw reason;
    }
    if (exitCode !== 0) {
      const error = new Error(`Container exited with unexpected exit code: ${exitCode}`);
      error.exitCode = exitCode;
      throw error;
    }
  }
  async destroy(error) {
    if (!this.running) return;
    if (this._destroyReason === undefined) this._destroyReason = error;
    await __container_destroy(this._scope);
  }
  signal(signo) {
    if (!__validSignal(signo)) throw new RangeError("Invalid signal number.");
    if (!this.running)
      throw new Error("signal() cannot be called on a container that is not running.");
    __container_signal(this._scope, signo).catch(() => {});
  }
  getTcpPort(port) {
    if (!(Number.isInteger(port) && port > 0 && port < 65536))
      throw new TypeError(`Invalid port number: ${port}`);
    return new ContainerPort(this._scope, port);
  }
  setInactivityTimeout(durationMs) {
    if (!(durationMs > 0))
      throw new Error(`setInactivityTimeout() requires durationMs > 0, got ${durationMs}`);
    return __container_inactivity(this._scope, durationMs);
  }
  async exec(cmd, options) {
    if (!this.running)
      throw new Error("exec() cannot be called on a container that is not running.");
    if (!Array.isArray(cmd) || cmd.length === 0)
      throw new TypeError("exec() requires a non-empty command array.");
    const o = options ?? {};
    const stdoutMode = __execOutputMode(o.stdout, "stdout");
    const stderrMode = __execOutputMode(o.stderr, "stderr");
    const combined = stderrMode === "combined";
    if (combined && stdoutMode !== "pipe")
      throw new TypeError('stderr: "combined" requires stdout to be "pipe".');
    if (o.cwd !== undefined && String(o.cwd).includes("\0"))
      throw new TypeError("cwd cannot contain '\\0' characters.");
    if (o.user !== undefined && String(o.user).includes("\0"))
      throw new TypeError("user cannot contain '\\0' characters.");
    let stdinMode = "none";
    if (o.stdin !== undefined) {
      if (o.stdin === "pipe") stdinMode = "pipe";
      else if (o.stdin instanceof ReadableStream) stdinMode = "stream";
      else throw new TypeError('stdin must be a ReadableStream or the string "pipe".');
    }
    // `pty` is `true` for the daemon default size or `{ cols, rows }` for an
    // initial size (either dimension defaults to 80x24 when omitted).
    let pty = false;
    let cols;
    let rows;
    if (o.pty !== undefined && o.pty !== false && o.pty !== null) {
      if (o.pty === true) {
        pty = true;
      } else if (typeof o.pty === "object" && !Array.isArray(o.pty)) {
        pty = true;
        cols = o.pty.cols ?? 80;
        rows = o.pty.rows ?? 24;
        if (!Number.isInteger(cols) || cols <= 0 || cols > 65535 ||
            !Number.isInteger(rows) || rows <= 0 || rows > 65535)
          throw new TypeError("pty cols and rows must be 1..=65535.");
      } else {
        throw new TypeError('pty must be true or { cols, rows }.');
      }
    }
    if (pty && combined)
      throw new TypeError('stderr: "combined" requires a non-PTY exec.');
    const signal = o.signal ?? undefined;
    if (signal !== undefined && !(signal instanceof AbortSignal))
      throw new TypeError("signal must be an AbortSignal.");
    // workerd refuses an already-aborted signal before spawning
    // anything, with the signal's reason (`throwIfAborted`; an
    // AbortError for abort()). The signal stays in JavaScript: only the
    // params below cross into the native op.
    if (signal !== undefined && signal.aborted) throw signal.reason;
    const params = {
      cmd: Array.from(cmd, String),
      env: __containerEnvList(o.env),
      combined,
      pty,
    };
    if (o.cwd !== undefined) params.cwd = String(o.cwd);
    if (o.user !== undefined) params.user = String(o.user);
    if (cols !== undefined) { params.cols = cols; params.rows = rows; }
    const { id, pid, isPty } = JSON.parse(
      await __container_exec(this._scope, JSON.stringify(params)),
    );
    if (stdinMode === "stream") {
      // Pump the caller's stream to the process; the pump is pending work
      // of the event, like a subrequest body.
      (async () => {
        const reader = o.stdin.getReader();
        try {
          for (;;) {
            const { value, done } = await reader.read();
            if (done) break;
            await __container_exec_write(id, __bytesOf(value));
          }
        } finally {
          await __container_exec_close(id);
        }
      })().catch(() => {});
    } else if (stdinMode === "none") {
      __container_exec_close(id).catch(() => {});
    }
    return new ContainerExecProcess(
      id, pid, stdinMode, stdoutMode, stderrMode, isPty, signal);
  }
  inspect() {
    return Promise.reject(new Error("inspect() is not implemented in cello"));
  }
  snapshotDirectory() {
    return Promise.reject(new Error("snapshotDirectory() is not implemented in cello"));
  }
  snapshotContainer() {
    return Promise.reject(new Error("snapshotContainer() is not implemented in cello"));
  }
  interceptOutboundHttp() {
    return Promise.reject(new Error("interceptOutboundHttp() is not implemented in cello"));
  }
  interceptOutboundHttps() {
    return Promise.reject(new Error("interceptOutboundHttps() is not implemented in cello"));
  }
  interceptAllOutboundHttp() {
    return Promise.reject(new Error("interceptAllOutboundHttp() is not implemented in cello"));
  }
  interceptOutboundTcp() {
    return Promise.reject(new Error("interceptOutboundTcp() is not implemented in cello"));
  }
}
// How long a `blockConcurrencyWhile` body may run before the block is
// cancelled and the object reset: workerd's 30 s. `CELLO_TEST_WAIT=1` takes
// the value from `CELLO_TEST_BLOCK_LIMIT_MS` instead, so the conformance
// suite reaches a timeout without waiting one out.
const __blockLimitMs = __test_wait_enabled() ? __test_block_limit_ms() : 30_000;
// The state's parts the runtime reaches from outside the class. A static
// private method cannot be named outside the class and a static block is the
// one place that may hand it out, so these three closures are the runtime's
// whole view: the prototype and the class object publish none of them (#256).
let __stateReady;
let __stateSocket;
let __stateFailBlock;
class DurableObjectState {
  static {
    __stateReady = (state) => state.#ready();
    __stateSocket = (state, id) => state.#socket(id);
    __stateFailBlock = (state, error) =>
      state.#resetAfterConcurrencyFailure(error);
  }
  constructor(scope) {
    // The host's own fields stay off reflection, and the six workerd exposes
    // are own, enumerable and in its order: `facets`, `container`, `storage`,
    // `id`, `props`, `exports`. A Worker keying or stringifying `ctx` then
    // sees exactly what it sees on workerd (#251).
    //
    // The scope is non-writable like storage above (H6): `abort()` and the
    // reset path drop pending puts for this scope, and the host derives it
    // from the executing cell as well, so a forged scope reaches neither.
    __primDefineProp(this, "_scope", {
      value: scope, enumerable: false, configurable: false, writable: false,
    });
    __primDefineProp(this, "_aborted", {
      value: false, enumerable: false, configurable: true, writable: true,
    });
    __primDefineProp(this, "_gate", {
      value: Promise.resolve(),
      enumerable: false, configurable: true, writable: true,
    });
    __primDefineProp(this, "_blockDepth", {
      value: 0, enumerable: false, configurable: true, writable: true,
    });
    __primDefineProp(this, "_hibernationTimeout", {
      value: null, enumerable: false, configurable: true, writable: true,
    });
    // `blockConcurrencyWhile` records nested blocks here; defining the slot
    // up front keeps that bookkeeping off `Object.keys(ctx)` after the first
    // nested block (#251).
    __primDefineProp(this, "_nestedBlocks", {
      value: undefined, enumerable: false, configurable: true, writable: true,
    });
    const facet = __cell.facetConfigs[scope];
    __primDefineProp(this, "_facetDepth", {
      value: facet?.depth ?? 0,
      enumerable: false, configurable: true, writable: true,
    });
    const separator = scope.indexOf(":");
    const className = separator < 0 ? scope : scope.slice(0, separator);
    this.facets = new DurableObjectFacets(this);
    // A class outside `containers` has no `ctx.container`; the property is
    // still own and enumerable, as workerd's is, so a key list matches.
    this.container = __cell.containers[className] ? new Container(scope) : undefined;
    this.storage = new DurableObjectStorage(scope, this);
    this.id = facet === undefined
      ? new DurableObjectId(
        className, separator < 0 ? scope : scope.slice(separator + 1),
        __cell.idNames[scope])
      : typeof facet.id === "string" ? facet.id : new DurableObjectId(...facet.id);
    // workerd's `ctx.props` is a plain object even for a class declared
    // without props; only a facet's own props replace it.
    this.props = facet?.props ?? {};
    this.exports = __ctxExports();
  }
  blockConcurrencyWhile(f) {
    // The native context binds the gate and its operations to one driver.
    // Its async continuations keep that driver even inside a foreign turn.
    return __with_input_gate_context(() => this.#blockConcurrencyWhile(f));
  }
  // The block path is the state's own: workerd has no member for it, and a
  // guest cannot name a private method, so only `blockConcurrencyWhile` and
  // the runtime's closures above reach it (#256).
  #blockConcurrencyWhile(f) {
    // workerd converts the argument as WebIDL `Function` before anything
    // else, and refuses a non-function by rejecting, not by throwing: the
    // caller keeps running and the promise it got back carries the refusal
    // (#261). The message is WebIDL's own, naming the member and the class.
    if (typeof f !== "function")
      return Promise.reject(new TypeError(
        "Failed to execute 'blockConcurrencyWhile' on 'DurableObjectState': " +
        "parameter 1 is not of type 'function'.",
      ));
    if (this._blockDepth >= 64)
      throw new Error(
        "blockConcurrencyWhile() calls are nested too deeply.",
      );
    if (this._blockDepth > 0) {
      // A stale reaction can run while an unrelated live block makes this
      // actor's depth nonzero. It cannot ride that hold when either half of
      // its retained [origin, driver] pair has already retired.
      if (__io_context_id() === "" || __io_context_id(true) === "") {
        return Promise.reject(new Error(
          "the cell event ended before it could acquire an input gate",
        ));
      }
      // A nested block rides the outer hold. One the outer body starts and
      // does not await would outlive that hold, and a `transaction()`
      // started that way is the shape that mattered: the outer block
      // returned, the gate opened, and another event wrote into the still
      // open transaction (#714). The outer block drains these before it
      // releases, so the hold lasts as long as any block under it. A nested
      // block that fails resets the object as any block does, and the outer
      // release must carry that failure, or the waiters would be admitted to
      // an object that no longer exists: the drain records it.
      //
      // The section opens here but workerd invokes the callback on a later
      // turn, never inside the call that made it (#261).
      const nested = this.#scheduleConcurrencyBlock(f);
      // A body that failed reset the object and rides with the release. A
      // block whose deferred turn was cancelled never ran, so it has no
      // failure to carry and must not refuse the section's waiters the way a
      // reset does (#267).
      (this._nestedBlocks ||= []).push(nested.then(
        () => null,
        (error) => this._aborted ? __describeFailure(error) : null,
      ));
      return nested;
    }
    // The host owns the gate (cello_logic::gate::InputGate). It replaces a
    // promise chain that could only order blocks against each other; the
    // gate sits at the delivery points, so nothing else is delivered to this
    // cell at all while the block runs.
    //
    // The acquire waits. It used to be synchronous, and had to be: a cell's
    // events came off one channel, so yielding even one microtask reopened a
    // window in which this cell delivered an event that then waited on a
    // gate nothing would release. Events are independent tasks now, so two
    // can reach a block together and the second has to queue -- and nothing
    // nests, so waiting cannot deadlock on an event suspended beneath it.
    // Asked for synchronously, awaited asynchronously, and the split is the
    // whole point. The op shuts the gate in this very call, so no event can
    // be delivered to this cell from here on -- that immediacy is what makes
    // `failCriticalSection()` reject a `ping()` issued right after it.
    // Waiting for the *ticket* is separate: events are independent tasks
    // now, so two of them can reach a block together and the second has to
    // queue behind the first. Calling this inside the async body instead
    // costs one microtask, and an event delivered in that window walks
    // straight into the critical section.
    const [eventText, owner, acquired] = __gate_acquire(this._scope);
    // An empty owner is an immediate refusal. Return its already-rejected
    // promise before recording the block, because no driver exists to settle
    // native work or to repair a readiness gate installed for stale work.
    if (owner === "") return acquired;
    const event = Number(eventText);
    const { block, record } = __blockEnter(this._scope, event, owner);
    // The promise the caller holds settles when the block's body settles, as
    // workerd's does. The section around it -- the drain of nested blocks and
    // the release -- outlives it, and that is what a nested block's callback
    // waits for: it runs after the enclosing promise has settled and its
    // reactions have run (#261).
    let settleGuest;
    let rejectGuest;
    const guest = new Promise((resolve, reject) => {
      settleGuest = resolve;
      rejectGuest = reject;
    });
    const next = (async () => {
      try {
        await acquired;
        // A shutdown can retire the promise reaction while another event
        // owns this queued acquisition. The owner still has to consume its
        // ticket, but the retired callback must not run.
        if (record.retired) {
          __gate_release(this._scope, event, owner, null);
          settleGuest(undefined);
          return;
        }
        block.holder = event;
        // A critical section that fails resets the actor, so whatever queued
        // behind its gate must be refused rather than handed to the reset
        // one -- it was sent to a cell whose state no longer exists. The
        // failure rides with the release for that: waiters are woken with it
        // instead of merely woken.
        let failure = null;
        let value;
        let thrown = null;
        try {
          value = await this.#runConcurrencyBlock(f);
        } catch (error) {
          thrown = error;
          failure = __describeFailure(error);
        }
        if (thrown === null) settleGuest(value);
        else rejectGuest(thrown);
        // Nested blocks are drained only after a body that returned: a
        // body that failed has reset the object, its waiters are refused
        // whatever the nested blocks do, and waiting for them would only
        // delay the refusal and a transaction's rollback behind it. Each
        // nested block has its own limit, so a drain is bounded by the
        // last one started, as workerd's nested critical sections are.
        while (failure === null && this._nestedBlocks && this._nestedBlocks.length > 0) {
          for (const nestedFailure of await Promise.all(this._nestedBlocks.splice(0))) {
            if (nestedFailure !== null && failure === null) failure = nestedFailure;
          }
        }
        this._nestedBlocks = undefined;
        __gate_release(this._scope, event, owner, failure);
        if (thrown !== null) throw thrown;
        return value;
      } catch (error) {
        // A block that never reached its body -- the acquisition refused, or
        // the block ahead failed -- rejects the caller's promise too.
        rejectGuest(error);
        throw error;
      } finally {
        // Also on a rejected `acquired`: the block ahead failed, this one
        // never ran, and the cell must not stay shut on its behalf.
        __blockLeave(this._scope, block, event);
      }
    })();
    // The section's own rejection is the readiness answer; the caller's copy
    // is kept handled here, as it was when the two were one promise, so a
    // guest that ignores the promise sees no unhandled rejection.
    guest.catch(() => {});
    // `__stateReady` awaits this. The host gate stops *other* events, but a
    // block taken in the constructor runs inside the very event that is
    // being delivered, so nothing external can hold that event back -- the
    // promise does. Concurrent blocks within one event go through
    // `_blockDepth` above and never reach here.
    this._gate = Promise.all([this._gate, record.ready]).then(() => undefined);
    // `__stateReady` observes the original promise. This extra handler prevents
    // a constructor-time rejection from becoming unhandled before an event
    // reaches `__stateReady`.
    this._gate.catch(() => {});
    next.then(record.settleReady, record.rejectReady);
    // The event's answer waits for this section (#267). A nested block rides
    // the section above it, so one promise per top-level block is the whole
    // set: `next` settles after the drain and the gate release.
    const actorEvent = __currentActorEvent();
    if (actorEvent !== undefined) actorEvent.sections.push(next);
    return guest;
  }
  #runConcurrencyBlock(f) {
    this._blockDepth++;
    return this.#startConcurrencyBlock(f);
  }
  // A nested block's section is open from the call, so a block a body started
  // from it counts as nested too, but the callback itself starts on a later
  // turn: workerd's nested critical section is entered by the enclosing
  // one's turn order, not by the caller's (#261). Rising above the caller's
  // microtasks is what a guest can see, and it is what keeps a body that
  // awaits its own nested block from running that block inside the call.
  #scheduleConcurrencyBlock(f) {
    this._blockDepth++;
    return __block_turn().then(() => this.#startConcurrencyBlock(f), (error) => {
      // The deferred turn never arrived -- the event ended and its ops were
      // rejected -- so the body never ran and the depth its section reserved
      // must come back (#267). Left behind, it makes the next block read as
      // nested and ride a hold nobody took.
      this._blockDepth--;
      throw error;
    });
  }
  // The body and the limit around it. A synchronous throw resets the object
  // and rejects, as a body that throws any other way does.
  #startConcurrencyBlock(f) {
    let result;
    try {
      result = f();
    } catch (error) {
      this._blockDepth--;
      this.#resetAfterConcurrencyFailure(error);
      throw error;
    }
    const timerId = __timer_alloc();
    let settled = false;
    const timeout = __op_timer(
      timerId,
      __blockLimitMs,
    ).then(() => {
      if (!settled) {
        throw new Error(
          "A call to blockConcurrencyWhile() in a Durable Object waited for " +
          "too long. The call was canceled and the Durable Object was reset.",
        );
      }
    });
    return Promise.race([Promise.resolve(result), timeout]).then(
      (value) => {
        settled = true;
        __timer_cancel(timerId);
        this._blockDepth--;
        return value;
      },
      (error) => {
        settled = true;
        __timer_cancel(timerId);
        this._blockDepth--;
        this.#resetAfterConcurrencyFailure(error);
        throw error;
      },
    );
  }
  #resetAfterConcurrencyFailure(error) {
    if (this._aborted) return;
    this._aborted = true;
    // Drop this scope's queued puts, not the executing cell's: under a
    // stub-mediated caller the executing cell is the caller, whose puts
    // must survive. The host verifies this scope is a resetting cell of
    // this isolate before dropping.
    if (__currentActorScope() === this._scope)
      __storage_drop_pending_puts_own();
    else
      __storage_drop_pending_puts_reset(this._scope);
    const instance = __cell.instances[this._scope];
    if (instance && instance.__celloState === this)
      delete __cell.instances[this._scope];
    // workerd does not hand a block's failure to the code that called it:
    // the event is broken, so a `catch` around the block never runs, and the
    // stub caller receives the value the block threw (#267). Terminating the
    // executing cell is the only way to stop its continuation.
    if (__currentActorScope() === this._scope) {
      __actor_fail(this._scope, error);
      return;
    }
    // A block that failed while this scope was not the executing cell -- a
    // stale reaction, or one a previous event started -- cannot break an
    // event. It breaks the actor, as Workerd joins it into the on-abort
    // promise.
    __actorBreak(this._scope, error);
  }
  abort(reason) {
    if (this._aborted) return;
    this._aborted = true;
    // Drop this scope's queued puts, not the executing cell's: under a
    // stub-mediated caller the executing cell is the caller, whose puts
    // must survive. The host verifies this scope is a resetting cell of
    // this isolate before dropping.
    if (__currentActorScope() === this._scope)
      __storage_drop_pending_puts_own();
    else
      __storage_drop_pending_puts_reset(this._scope);
    const instance = __cell.instances[this._scope];
    if (instance && instance.__celloState === this)
      delete __cell.instances[this._scope];
    const message = reason instanceof Error
      ? reason.message
      : String(reason);
    // Direct host-dispatched event on this actor: the uncatchable
    // terminate_execution path. Under a same-isolate stub caller,
    // termination would unwind the caller too, so break in JS.
    if (__currentActorScope() === this._scope) {
      __actor_abort(this._scope, message);
      return;
    }
    __actorBreak(this._scope,
      reason instanceof Error ? reason : new Error(message));
  }
  #ready() { return this._gate; }
  getWebSockets(tag) {
    return JSON.parse(__ws_list(this._scope, tag)).map((row) => __socketFromRow(row));
  }
  // Workerd's hibernatable WebSocket event timeout: the value it reports, with
  // its own conversion (an integer number of milliseconds, zero clearing it,
  // at most a week) and its own two refusal messages. Cello stores and reports
  // it; the hibernation driver here does not cut an event short on it, which
  // the Durable Objects page records (#251).
  setHibernatableWebSocketEventTimeout() {
    this._hibernationTimeout = __hibernationTimeout(arguments[0]);
  }
  getHibernatableWebSocketEventTimeout() {
    return this._hibernationTimeout;
  }
  getTags(ws) {
    if (!(ws instanceof WebSocket) || !__wsState(ws).hibernatable) {
      throw new Error(
        "you must call 'acceptWebSocket()' before attempting to access " +
        "the tags of a WebSocket.");
    }
    return Array.from(__wsState(ws).tags);
  }
  acceptWebSocket(ws, tags = []) {
    const accepted = __wsState(ws).accepted ||
      __wsState(ws).hibernationAccepted;
    if (accepted)
      throw new Error(
        "Cannot call `acceptWebSocket()` if the WebSocket was already " +
        "accepted via `accept()`");
    if (__heap_over_admission_share())
      throw new Error(
        "the isolate is near its V8 heap limit, so it refused a " +
        "WebSocket (see CELLO_V8_HEAP_LIMIT_MB)");
    const acceptedTags = Array.from(tags, String);
    const wsState = __wsState(ws);
    __ws_accept(wsState.id, this._scope, JSON.stringify(acceptedTags));
    wsState.target = { id: wsState.id, scope: this._scope };
    if (wsState.peer) __wsState(wsState.peer).target = wsState.target;
    wsState.hibernatable = true;
    // Accepting for hibernation is a second, distinct acceptance: workerd
    // refuses `acceptWebSocket()` once either form ran.
    wsState.hibernationAccepted = true;
    if (wsState.peer) __wsState(wsState.peer).hibernatable = true;
    wsState.tags = acceptedTags;
    __sockets.set(wsState.id, ws);
  }
  #socket(id) {
    return __sockets.get(Number(id)) ||
      this.getWebSockets().find((ws) => __wsState(ws).id === id) ||
      __wsStub(id);
  }
  // Workerd actor-state.c++ setWebSocketAutoResponse: no argument unsets,
  // `null` is not a pair and is refused, and each side is capped at 2048
  // UTF-8 bytes. The pair itself lives in the shell -- a matched message is
  // answered without waking this cell.
  setWebSocketAutoResponse(pair) {
    if (pair === undefined) {
      __ws_auto_response_set(this._scope, null, null);
      return;
    }
    if (!(pair instanceof WebSocketRequestResponsePair))
      throw new TypeError(
        "Failed to execute 'setWebSocketAutoResponse' on " +
        "'DurableObjectState': parameter 1 is not of type " +
        "'WebSocketRequestResponsePair'.",
      );
    const max = 2048;
    const bytes = (s) => new TextEncoder().encode(s).length;
    const requestSize = bytes(pair.request);
    if (requestSize > max)
      throw new RangeError(
        `Request cannot be larger than ${max} bytes. ` +
        `A request of size ${requestSize} was provided.`,
      );
    const responseSize = bytes(pair.response);
    if (responseSize > max)
      throw new RangeError(
        `Response cannot be larger than ${max} bytes. ` +
        `A response of size ${responseSize} was provided.`,
      );
    __ws_auto_response_set(this._scope, pair.request, pair.response);
  }
  getWebSocketAutoResponse() {
    const pair = JSON.parse(__ws_auto_response_get(this._scope));
    if (pair === null) return null;
    return new WebSocketRequestResponsePair(pair[0], pair[1]);
  }
  getWebSocketAutoResponseTimestamp(ws) {
    const ms = __ws_auto_response_ts(__wsState(ws).id);
    return ms === null ? null : new Date(ms);
  }
  waitUntil(promise) {
    __registerWaitUntil(promise);
  }
}
// Workerd's interface: its tag names the state, and every prototype member a
// Worker can walk is enumerable, as a JSG interface's are. No host-internal
// member is left on the prototype (#251, #256).
__webidlTag(DurableObjectState, "DurableObjectState");
__webidlPrototype(DurableObjectState);
__nativeMethodArities(DurableObjectState);
function _instance(scope) {
  let inst = __cell.instances[scope];
  if (!inst) {
    const className = scope.split(":")[0];
    const cls = __cell.classes[className];
    if (!cls) throw new Error("no DO class " + className);
    const state = new DurableObjectState(scope);
    inst = new cls(state, __cell.env);
    Object.defineProperty(inst, "__celloState", { value: state });
    // Workerd worker-rpc.c++ getTargetInfo(): RPC needs `extends
    // DurableObject` unless the js_rpc compat flag is on. Decided
    // once here; dispatch reads a boolean.
    __primDefineProp(state, "_rpcOk", {
      value: __cell.compat.jsRpc || __cell.doExports[className] === true,
      enumerable: false, configurable: true, writable: true,
    });
    __cell.instances[scope] = inst;
  }
  return inst;
}
async function _readyInstance(scope) {
  const inst = _instance(scope);
  await __stateReady(inst.__celloState);
  return inst;
}
const __actorEventStack = [];
const __currentActorEvent = () => {
  const context = String(__io_context_id());
  if (context === "") return undefined;
  for (let index = __actorEventStack.length - 1; index >= 0; index--) {
    const event = __actorEventStack[index];
    if (event.context === context) return event;
  }
  return undefined;
};
const __currentActorScope = () => __currentActorEvent()?.scope ?? "";
const __beginActorEvent = (scope) => {
  __rpcSignalRefresh();
  __refreshRetainedWaits(scope);
  // `sections` is every critical section this event opened. workerd does not
  // answer an event until they have drained: a block that fails afterwards
  // fails the event, not just the object, so the answer waits (#267).
  const event = { scope, context: String(__io_context_id()), sections: [] };
  __actorEventStack.push(event);
  return event;
};
// Wait out the sections an event opened, then let the answer go. A section
// never started (its deferred turn was cancelled) answers like a drained one:
// there is nothing left to wait for, and it must not fail the event.
const __awaitActorSections = async (actorEvent) => {
  const sections = actorEvent.sections;
  while (sections.length > 0) {
    try {
      await sections.shift();
    } catch {
      // The section failed. It aborted the cell, so the host already carries
      // the failure and the answer this event would produce is gone.
    }
  }
};
const __endActorEvent = (event) => {
  const index = __actorEventStack.lastIndexOf(event);
  if (index >= 0) __actorEventStack.splice(index, 1);
};
const __endActorEventsForContext = (context) => {
  for (let index = __actorEventStack.length - 1; index >= 0; index--) {
    if (__actorEventStack[index].context === context)
      __actorEventStack.splice(index, 1);
  }
};
const __incomingRequestSignals = new Map();
// The slots belong to AbortSignal, so the class installs the implementation
// when it is defined; this indirection stays callable from module-level code.
let __abortSignalImpl;
const __abortSignal = (signal, reason) => __abortSignalImpl(signal, reason);
const __abortIncomingRequest = __cello.__abortIncomingRequest = (requestId) => {
  const signal = __incomingRequestSignals.get(String(requestId));
  if (signal && !signal.aborted) {
    __abortSignal(signal, new Error("The client has disconnected"));
    return true;
  }
  return false;
};
__cello.__registerIncomingRequest = (requestId, request) => {
  __incomingRequestSignals.set(String(requestId), request.signal);
};
__cello.__finishIncomingRequest = (requestId) => {
  __incomingRequestSignals.delete(String(requestId));
};
__cello.__retireInputGateContext = (context) => {
  for (const [scope, block] of __cellBlocks) {
    const retired = [...block.events].filter(([, record]) =>
      record.owner === context || record.reaction === context
    );
    if (retired.length === 0) continue;
    const holder = retired.find(([event]) => block.holder === event);
    if (holder === undefined) {
      // A queued callback has not changed the actor. Remove only that
      // callback, and let an unrelated holder continue with the same state.
      for (const [event, record] of retired) {
        record.retired = true;
        record.settleReady();
        block.events.delete(event);
        if (record.actorEvent !== undefined)
          __endActorEvent(record.actorEvent);
      }
      if (block.events.size === 0 && __cellBlocks.get(scope) === block)
        __cellBlocks.delete(scope);
      continue;
    }
    // A running callback changed this actor and cannot reach its own
    // `finally`. Reset the actor, and release the host gate when the context
    // that started the callback differs from the turn that owns its work.
    const [holderEvent, holderRecord] = holder;
    __gate_release(
      scope,
      holderEvent,
      holderRecord.owner,
      "the cell's critical section ended without releasing",
    );
    const instance = __cell.instances[scope];
    if (instance !== undefined) instance.__celloState._aborted = true;
    __storage_drop_pending_puts(scope);
    if (instance !== undefined && __cell.instances[scope] === instance)
      delete __cell.instances[scope];
    for (const record of block.events.values()) {
      record.retired = true;
      record.settleReady();
      if (record.actorEvent !== undefined)
        __endActorEvent(record.actorEvent);
    }
    // The old actor cannot run any block `finally`. A later event creates an
    // instance whose ready promise contains none of this actor's work.
    if (__cellBlocks.get(scope) === block) __cellBlocks.delete(scope);
  }
  __endActorEventsForContext(context);
};
// Race a pending dispatch against the caller's abort signal: on abort,
// run `abandon` (which reaches the target's side) and reject with the
// caller's reason. The dispatch keeps its settle handlers, so a late
// settlement after abandonment cannot trip the unhandled-rejection
// signal.
const __raceCallerAbort = (dispatch, signal, abandon) =>
  new Promise((resolve, reject) => {
    let settled = false;
    const onAbort = () => {
      if (settled) return;
      settled = true;
      abandon();
      reject(signal.reason);
    };
    signal.addEventListener("abort", onAbort, { once: true });
    dispatch.then(
      (value) => {
        if (settled) return;
        settled = true;
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error) => {
        if (settled) return;
        settled = true;
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
const __awaitCancellableDoCall = (operation, signal) => {
  if (signal.aborted) {
    __do_call_cancel(operation.__celloCancelId);
    return Promise.reject(signal.reason);
  }
  return __raceCallerAbort(operation, signal,
    () => __do_call_cancel(operation.__celloCancelId));
};
// Workerd semantics: a Response crossing a service binding is a fresh
// fetch-shaped Response -- immutable headers, default reason phrase,
// the request URL, and a real body stream -- never the callee's own
// mutable object.
const __STATUS_TEXT = {
  100: "Continue", 101: "Switching Protocols", 102: "Processing",
  103: "Early Hints", 200: "OK", 201: "Created", 202: "Accepted",
  203: "Non-Authoritative Information", 204: "No Content",
  205: "Reset Content", 206: "Partial Content", 207: "Multi-Status",
  208: "Already Reported", 226: "IM Used", 300: "Multiple Choices",
  301: "Moved Permanently", 302: "Found", 303: "See Other",
  304: "Not Modified", 305: "Use Proxy", 307: "Temporary Redirect",
  308: "Permanent Redirect", 400: "Bad Request", 401: "Unauthorized",
  402: "Payment Required", 403: "Forbidden", 404: "Not Found",
  405: "Method Not Allowed", 406: "Not Acceptable",
  407: "Proxy Authentication Required", 408: "Request Timeout",
  409: "Conflict", 410: "Gone", 411: "Length Required",
  412: "Precondition Failed", 413: "Payload Too Large",
  414: "URI Too Long", 415: "Unsupported Media Type",
  416: "Range Not Satisfiable", 417: "Expectation Failed",
  418: "I'm a teapot", 421: "Misdirected Request",
  422: "Unprocessable Entity", 423: "Locked", 424: "Failed Dependency",
  425: "Too Early", 426: "Upgrade Required",
  428: "Precondition Required", 429: "Too Many Requests",
  431: "Request Header Fields Too Large",
  451: "Unavailable For Legal Reasons", 500: "Internal Server Error",
  501: "Not Implemented", 502: "Bad Gateway",
  503: "Service Unavailable", 504: "Gateway Timeout",
  505: "HTTP Version Not Supported", 506: "Variant Also Negotiates",
  507: "Insufficient Storage", 508: "Loop Detected",
  510: "Not Extended", 511: "Network Authentication Required",
};
const __ERROR_RESPONSE_MESSAGE =
  "Return value from serve handler must not be an error " +
  "response (like Response.error())";
const __wrapServiceResponse = (res, url) => {
  // Response.error() represents a Fetch network error, not an HTTP response.
  // Letting status 0 cross this seam turns it into an ordinary response and
  // makes callers continue after the target explicitly reported failure.
  if (res.type === "error" || res.status === 0) {
    throw new TypeError(__ERROR_RESPONSE_MESSAGE);
  }
  // A 101 that crossed isolates carries a target rather than a socket: the
  // pair's other end is in the isolate that answered, and the two cannot be
  // linked the way the loopback below links them. Give the caller a client
  // end the host can join to that target, on the same terms as a Durable
  // Object subrequest -- nothing binds until `accept()`, so a response
  // passed straight back out keeps the direct route to the real client.
  const bound = !res.webSocket && __responseState(res).wsTarget && res.status === 101
    ? new WebSocket(undefined, [], __responseState(res).wsTarget)
    : undefined;
  const wrapped = new Response(
    // Buffered bodies re-stream (an empty buffer still yields a
    // stream, as over real HTTP); a streaming body passes through. A
    // null-body status keeps no body at all: the hop's `""` payload must not
    // become an empty stream (or, with #159, a `content-type`).
    __nullBodyStatus(res.status)
      ? null
      : __responseState(res).bytes !== null
        ? __responseState(res).bytes : res.body,
    {
      status: res.status,
      statusText:
        res.statusText || __STATUS_TEXT[res.status] || "",
      headers: res.headers,
      webSocket: res.webSocket || bound,
      __wsTarget: __responseState(res).wsTarget,
    },
  );
  // The constructor copies headers, so seal the copy.
  __cello.$$sealHeaders(wrapped.headers);
  __cello.$$setResponseUrl(wrapped, url);
  // An upgraded pair whose both ends live in this isolate: link them
  // directly -- the host connection seam only reaches external
  // clients. Frames the handler queued before linking flush first.
  const client = wrapped.webSocket;
  if (wrapped.status === 101 && client && __wsState(client).peer) {
    const clientState = __wsState(client);
    const server = clientState.peer;
    const serverState = __wsState(server);
    clientState.loopback = server;
    serverState.loopback = client;
    for (const frame of serverState.pending.splice(0)) {
      // "send-binary" joined this queue with outbound sockets; without a case
      // of its own it fell through to the close branch and tore the pair down
      // instead of delivering the frame.
      if (frame[0] === "send")
        queueMicrotask(() => __wsHooks.dispatchMessage(client, frame[1]));
      else if (frame[0] === "send-binary")
        queueMicrotask(() => {
          const data = frame[1];
          __wsHooks.dispatchMessage(
            client,
            data instanceof ArrayBuffer ? data : data.buffer.slice(
              data.byteOffset,
              data.byteOffset + data.byteLength,
            ),
          );
        });
      else
        queueMicrotask(
          () => __wsHooks.dispatchClose(client, frame[1], frame[2], true));
    }
  }
  return wrapped;
};
const __SERVICE_REDIRECT_STATUSES = new Set([301, 302, 303, 307, 308]);
const __REQUEST_BODY_HEADERS = [
  "content-encoding", "content-language", "content-length",
  "content-location", "content-type", "transfer-encoding",
];
const __CROSS_ORIGIN_SENSITIVE_HEADERS = [
  "authorization", "cookie", "proxy-authorization",
];
// A service target only produces the first response. Fetch redirect handling
// belongs to the caller, because the Location leaves through the caller's
// ambient outbound policy rather than gaining the target service's authority.
// Capture the replayable request state before dispatch: a same-isolate target
// receives the Request itself and can consume or mutate it before it redirects.
const __serviceRedirectState = (req) => ({
  method: req.method,
  headers: __cello.$$headerList(req.headers),
  body: req.body === null ? undefined
    : __requestState(req).bytes === null ? null : __requestState(req).bytes.slice(),
  signal: __requestState(req).signalForSubrequests,
});
const __finishServiceFetch = async (res, req, replay) => {
  const response = __wrapServiceResponse(res, req.url);
  if (!__SERVICE_REDIRECT_STATUSES.has(response.status)) return response;
  const location = response.headers.get("location");
  if (location === null) return response;
  if (req.redirect === "manual") return response;

  // Nobody can consume the redirect response after this branch. Cancelling it
  // releases a cross-isolate body stream instead of waiting for its idle TTL.
  if (response.body !== null) {
    try { await response.body.cancel(); } catch { /* the redirect still wins */ }
  }
  if (req.redirect === "error")
    throw new TypeError("A redirect was returned for a request with redirect mode error.");

  const url = new URL(location, req.url).href;
  let method = replay.method;
  let body = replay.body;
  const headers = new Headers(replay.headers);
  if ((response.status === 301 || response.status === 302) && method === "POST" ||
      response.status === 303 && method !== "GET" && method !== "HEAD") {
    method = "GET";
    body = undefined;
    for (const name of __REQUEST_BODY_HEADERS) headers.delete(name);
  } else if (body === null) {
    // A transferred or live JavaScript stream has no replay copy. Fetch treats
    // the redirect as a network error instead of sending an empty replacement.
    throw new TypeError("Cannot follow a redirect with a non-replayable request body.");
  }
  // The ambient fetch starts a new native request, so its HTTP client cannot
  // know that these credentials came from another origin and remove them. Do
  // that at the service-to-network boundary before they can leak to Location.
  if (new URL(req.url).origin !== new URL(url).origin) {
    for (const name of __CROSS_ORIGIN_SENSITIVE_HEADERS) headers.delete(name);
  }
  const init = { method, headers, redirect: "follow" };
  if (body !== undefined) init.body = body;
  if (replay.signal !== null) init.signal = replay.signal;
  const followed = await globalThis.fetch(url, init);
  __cello.$$responseState(followed).redirected = true;
  if (followed.url === "") __cello.$$setResponseUrl(followed, url);
  return followed;
};
// `[[services]]` binding: a Fetcher pointed at another Worker in this
// process. No identity to resolve, so it goes straight to __svc_call.
// Worker Loader (Code Mode): spawn a fresh isolate from supplied code and
// invoke its default or named entrypoint through the cross-isolate request and
// RPC paths.
// A Fetcher can cross into a loaded isolate only as this host-minted route.
// Reading properties from a Proxy is not a trustworthy brand check, so the
// metadata lives in a side table that application objects cannot forge.
const __outboundMeta = new WeakMap();
const __loaderServiceMarker = "__cello$loaderSvc";
__cello.__makeLoader = () => {
  // A name weakly caches a shared load; `load()` is anonymous. Keep the load
  // promise itself on every derived capability, including a running facet,
  // so collection cannot evict a Worker while that capability still uses it.
  const byName = new Map();
  const loaderOptions = (method, options, supportsLimits) => {
    if (options === null || options === undefined) return undefined;
    if (typeof options !== "object")
      throw new TypeError(method + "() options must be an object.");
    const extra = Object.keys(options).filter(
      (key) => key !== "props" && !(supportsLimits && key === "limits"));
    if (extra.length !== 0)
      throw new TypeError(
        method + '() does not support the option "' + extra.join('", "') +
        '".');
    return options;
  };
  // `props` cross to the loaded isolate by structured clone, on the same
  // transport that already carries the call's arguments (`__rpcOut` below).
  // JSON is the obvious alternative and it is wrong: JSON.stringify turns a
  // Map, a Set, a Date, a typed array, and a RegExp into a plain object or a
  // string and reports no error, so the callee reads a mangled value that the
  // same call's arguments would have carried intact. Encoding here, once per
  // loader API call, also rejects an unclonable value at that call rather than
  // at the first operation.
  const loaderPropsSc = (method, options) => {
    const props = loaderOptions(method, options, false)?.props;
    if (props === undefined) return undefined;
    // `lift` false: the props cross an isolate boundary, so a stub inside them
    // is a DataCloneError, exactly as a stub inside an argument of the same
    // call is.
    return __rpcOut(props, false);
  };
  const loaderEntrypointOptions = (options) => {
    options = loaderOptions("getEntrypoint", options, true);
    const props = options?.props;
    return {
      propsSc: props === undefined ? undefined : __rpcOut(props, false),
      limitsJson: JSON.stringify(options?.limits ?? null),
    };
  };
  const makeEntrypoint = (loadPromise, entrypoint, propsSc, limitsJson) => {
    const target = {
      async fetch(input, init) {
        const { id, tails } = await loadPromise;
        const adoptsBody = __fetchAdoptsBody(input, init);
        const usedBefore = adoptsBody && input.bodyUsed;
        const req = new Request(input, init);
        if (adoptsBody) __fetchTakeBody(input, usedBefore);
        // The caller's signal, never the incoming request's own, as for a
        // service binding.
        const signal = __requestState(req).signalForSubrequests;
        if (signal?.aborted) throw signal.reason;
        // The verbatim header list; see the note on the outbound `fetch`.
        // The loaded worker rebuilds a `Headers` from these pairs, so a
        // repeat and its casing survive the isolate boundary.
        const headers = JSON.stringify(__cello.$$headerList(req.headers));
        const { body, streamId } = await __subrequestBody(req);
        const loaded = __loader_fetch(
          id, req.url, req.method, body, headers, streamId, entrypoint,
          propsSc === undefined ? new Uint8Array() : propsSc,
          limitsJson, tails.length !== 0, signal != null);
        const dispatch = tails.length === 0 ? loaded : loaded[0];
        const response = signal
          ? __awaitCancellableDoCall(dispatch, signal)
          : dispatch;
        if (tails.length !== 0) {
          const delivery = loaded[1]
            .then((report) => JSON.parse(report))
            .then((events) => Promise.allSettled(
              tails.map((tail) => tail.tail(events))))
            .then((results) => {
              for (const result of results) {
                if (result.status === "rejected")
                  console.error("Tail Worker failed:", result.reason);
              }
            })
            .catch((error) => console.error("Tail delivery failed:", error));
          // The loaded fetch answers before its report because the child can
          // still have waitUntil work. Keep this chain on the loader event so
          // its IoContext cannot retire and abort the report operation.
          __registerWaitUntil(delivery);
        }
        const r = JSON.parse(await response);
        const responseBody = __hostReplyBody(r.status, r);
        // A 101 from the loaded worker carries its socket as a join target;
        // `__wrapServiceResponse` binds the caller end to it, as for a
        // Durable Object upgrade.
        return __wrapServiceResponse(
          new Response(responseBody, {
            status: r.status, headers: r.headers, __wsTarget: r.wsTarget,
          }),
          req.url);
      },
    };
    // Both the default and named entrypoints expose RPC: property access is a
    // callable pipeline node dispatched via __loader_rpc to that entrypoint in
    // the loaded worker (default -> the "default" export, which
    // register_entrypoints registers like any other). fetch stays on the
    // target. Only single-method calls are supported for now.
    const session = {
      get: () => Promise.reject(new Error(
        "Awaitable properties on loaded workers are not supported yet.")),
      call: (path, args) => (async () => {
        if (path.length !== 1)
          throw new Error(
            "Pipelined property paths on loaded workers are not supported " +
            "yet.");
        const { id } = await loadPromise;
        return __rpcDes(
          await __loader_rpc(id, entrypoint, path[0], __rpcOut(args, false),
            propsSc, limitsJson));
      })(),
    };
    return new Proxy(target, {
      get: (base, prop) => {
        if (prop === "then") return undefined;
        if (Reflect.has(base, prop)) return Reflect.get(base, prop);
        if (typeof prop !== "string") return undefined;
        return __makeNode(session, [prop], null);
      },
    });
  };
  // The load promise is the shared lifetime, retained by stubs, entrypoints,
  // classes, and running facets. Finalizing only the original stub can evict
  // a Worker that one of its derived capabilities still uses.
  const finalizer = new FinalizationRegistry(({ id, name, ref }) => {
    if (ref !== undefined && byName.get(name) === ref) byName.delete(name);
    if (id !== undefined) __loader_drop(id);
  });
  const track = (loadPromise, name, ref) => {
    // Register before settlement, so failed loads also release their names.
    const held = { id: undefined, name, ref };
    finalizer.register(loadPromise, held, loadPromise);
    loadPromise.then(({ id }) => { held.id = id; }, () => {});
    return loadPromise;
  };
  const makeStub = (loadPromise, name, ref) => {
    const drop = () => {
      if (ref !== undefined && byName.get(name) === ref) byName.delete(name);
      finalizer.unregister(loadPromise);
      loadPromise.then(({ id }) => __loader_drop(id), () => {});
    };
    const stub = {
      getEntrypoint(name = null, options = {}) {
        const { propsSc, limitsJson } = loaderEntrypointOptions(options);
        return makeEntrypoint(
          loadPromise, name === null ? "default" : String(name),
          propsSc, limitsJson);
      },
      getDurableObjectClass(name = null, options = {}) {
        return __makeDurableObjectClass(
          loadPromise, name, loaderPropsSc("getDurableObjectClass", options));
      },
      dispose: drop,
    };
    if (typeof Symbol.dispose === "symbol") stub[Symbol.dispose] = drop;
    return stub;
  };
  // JSON.stringify silently drops binary values, so each non-string module --
  // an explicit module shape such as `{ wasm: bytes }` -- is normalized
  // first. JavaScript modules become plain strings, and wasm moves into a
  // side-band `[name, Uint8Array]` list so multi-MB blobs never take a
  // base64/JSON round-trip.
  const toBytes = (v) => __hostChunk(
    v, "a module's wasm bytes must be an ArrayBuffer or a typed array");
  const moduleKinds = [
    "js", "cjs", "py", "text", "data", "json", "wasm",
  ];
  const encodeLoaderEnv = (value) => {
    if (value === undefined || value === null)
      return { env: undefined, envRoutes: [] };
    const envRoutes = [];
    const seen = new Map();
    const project = (v) => {
      if (v === null ||
          (typeof v !== "object" && typeof v !== "function")) return v;
      const cached = seen.get(v);
      if (cached !== undefined) return cached;
      const route = __outboundMeta.get(v);
      if (route !== undefined) {
        const marker = { [__loaderServiceMarker]: envRoutes.length };
        seen.set(v, marker);
        envRoutes.push([
          route.script,
          route.entrypoint,
          route.propsSc ?? (route.props === undefined
            ? new Uint8Array() : __rpcOut(route.props, false)),
        ]);
        return marker;
      }
      if (Object.prototype.hasOwnProperty.call(v, __loaderServiceMarker))
        throw new DOMException(
          `Worker Loader env contains the reserved key ` +
          `${JSON.stringify(__loaderServiceMarker)}.`,
          "DataCloneError");
      if (typeof v === "function" || v instanceof __cf.RpcTarget ||
          __weakHas(__stubMeta, v) || __do_is_stub(v))
        throw new DOMException(
          "Worker Loader env supports only Service Binding capabilities.",
          "DataCloneError");
      if (v instanceof Map) {
        const out = new Map();
        seen.set(v, out);
        for (const [key, item] of v) out.set(project(key), project(item));
        return out;
      }
      if (v instanceof Set) {
        const out = new Set();
        seen.set(v, out);
        for (const item of v) out.add(project(item));
        return out;
      }
      const proto = Object.getPrototypeOf(v);
      if (!Array.isArray(v) && proto !== Object.prototype && proto !== null)
        return v;
      const out = Array.isArray(v) ? [] : {};
      seen.set(v, out);
      for (const key of Object.keys(v)) out[key] = project(v[key]);
      return out;
    };
    return { env: __sc_encode(project(value)), envRoutes };
  };
  const encodeModules = (c) => {
    if (c === null || typeof c !== "object" || c.modules === null
        || typeof c.modules !== "object")
      return { config: c, wasm: [], outbound: undefined, tails: [] };
    // workerd requires the date, and names the missing value as it does.
    if (typeof c.compatibilityDate !== "string")
      throw new Error(`Invalid compatibility date: ${c.compatibilityDate}`);
    const modules = {};
    const wasm = [];
    for (const [name, value] of Object.entries(c.modules)) {
      const wrapped = value !== null && typeof value === "object" ? value : {};
      const explicitKinds = moduleKinds.filter((kind) =>
        Object.prototype.hasOwnProperty.call(wrapped, kind)
      );
      // An object is a module shape with one kind, as in workerd; bare bytes
      // are not a module.
      if (value !== null && typeof value === "object" &&
          !(value instanceof WebAssembly.Module) && explicitKinds.length !== 1)
        throw new TypeError(
          "Each module must contain exactly one of 'js', 'cjs', 'text', " +
          `'data', 'json', 'py', or 'wasm'. Module '${name}' contained ` +
          `${explicitKinds.length} properties.`);
      const kind = explicitKinds[0];
      if (kind === "js") {
        const source = wrapped[kind];
        if (typeof source !== "string")
          throw new TypeError(
            `Worker Loader module ${JSON.stringify(name)} field ` +
            `${JSON.stringify(kind)} must be a string.`);
        modules[name] = source;
        continue;
      }
      const bytes = kind === "wasm" ? toBytes(wrapped.wasm) : null;
      if (bytes !== null) wasm.push([name, bytes]);
      else modules[name] = value;
    }
    let outbound;
    let outboundProps;
    if (Object.prototype.hasOwnProperty.call(c, "globalOutbound")) {
      if (c.globalOutbound === undefined) {
        outbound = undefined;
      } else if (c.globalOutbound === null) {
        outbound = null;
      } else {
        const meta = __outboundMeta.get(c.globalOutbound);
        if (meta === undefined)
          throw new TypeError("globalOutbound must be a Fetcher or null.");
        // JSON carries only the host-minted route. Props use V8 structured
        // clone bytes, so their types and the cross-isolate clone boundary
        // match the other parameterized ctx.exports transports.
        outboundProps = meta.propsSc ?? (meta.props === undefined
          ? new Uint8Array()
          : __rpcOut(meta.props, false));
        outbound = JSON.stringify({
          script: meta.script,
          entrypoint: meta.entrypoint,
        });
      }
    }
    const { env, envRoutes } = encodeLoaderEnv(c.env);
    let tails = [];
    if (Object.prototype.hasOwnProperty.call(c, "tails")) {
      if (!Array.isArray(c.tails))
        throw new TypeError("tails must be an array of Fetchers.");
      tails = c.tails.map((tail) => {
        if (!__outboundMeta.has(tail))
          throw new TypeError("tails must be an array of Fetchers.");
        return tail;
      });
    }
    const {
      globalOutbound: _globalOutbound,
      env: _env,
      tails: _tails,
      ...rest
    } = c;
    return {
      config: { ...rest, modules }, wasm, outbound, outboundProps,
      env, envRoutes, tails,
    };
  };
  // getCode is deferred into a microtask so a throw (or async getCode)
  // surfaces as a rejection when the worker is first used, not at get()/load().
  const loadFrom = (getCode) =>
    Promise.resolve().then(getCode)
      .then((c) => {
        const {
          config, wasm, outbound, outboundProps, env, envRoutes, tails,
        } = encodeModules(c);
        const id = __loader_load(
          JSON.stringify(config), wasm, outbound, outboundProps, env,
          envRoutes, tails.length !== 0);
        return { id, tails };
      });
  return {
    load(code) { return makeStub(track(loadFrom(() => code))); },
    get(name, getCode) {
      let ref = byName.get(name);
      let loadPromise = ref?.deref();
      if (loadPromise === undefined) {
        loadPromise = loadFrom(getCode);
        ref = new WeakRef(loadPromise);
        byName.set(name, ref);
        track(loadPromise, name, ref);
      }
      return makeStub(loadPromise, name, ref);
    },
  };
};

// Each service stub's own `fetch`, keyed by the stub a Worker holds: the
// interface's `fetch` and `connect` live on `ServiceStub.prototype` and read
// it, so both stub kinds share one implementation surface (#259).
const __serviceStubFetchers = new __primWeakMap();
const __makeServiceBinding = __cello.__makeServiceBinding =
  (script, entrypoint = null, propsSc = undefined) => {
  const target = {
  async fetch(input, init) {
    const adoptsBody = __fetchAdoptsBody(input, init);
    const usedBefore = adoptsBody && input.bodyUsed;
    const req = new Request(input, init);
    if (adoptsBody) __fetchTakeBody(input, usedBefore);
    const signal = __requestState(req).signalForSubrequests;
    if (signal?.aborted) throw signal.reason;
    // A stream body advertises its length the way the HTTP layer
    // would: known length -> Content-Length, unknown -> chunked.
    if (__requestState(req).bytes === null &&
        !req.headers.has("content-length") &&
        !req.headers.has("transfer-encoding")) {
      const length = __cello.$$readableState(req.body).expectedLength;
      if (length === undefined)
        req.headers.set("transfer-encoding", "chunked");
      else req.headers.set("content-length", String(length));
    }
    const redirectState = __serviceRedirectState(req);
    // Fast path: the target is this same script, so its handler lives in
    // this isolate. Skip the op + pool-thread hop and call it directly,
    // inside its own event so the target's waitUntil does not attach to
    // the caller's. Cross-script targets still need their own isolate.
    if (script === __cell.script && entrypoint !== null &&
        propsSc === undefined) {
      // Mirror __dispatchTo: the target sees a fresh incoming signal,
      // aborted with "The client has disconnected" when the caller
      // abandons the call or cancels the response body; the caller's
      // own signal races the dispatch so an abandoned call rejects
      // immediately instead of pinning on a hung target.
      const requestController = new AbortController();
      const dispatch = __dispatchEntrypointFetch(
        entrypoint,
        new Request(req, {
          signal: requestController.signal,
          __celloIncomingSignal: true,
          // The target's view of a service-binding call is an incoming
          // request: its headers are immutable, as on HTTP ingress (#169).
          [__INCOMING]: true,
        }));
      const response = signal
        ? await __raceCallerAbort(dispatch, signal, () =>
            requestController.abort(
              new Error("The client has disconnected")))
        : await dispatch;
      __attachResponseRequestCancellation(
        response, requestController, true);
      return __finishServiceFetch(response, req, redirectState);
    }
    if (script === __cell.script && entrypoint === null &&
        typeof __cell.selfFetch === "function") {
      if (__cell.svcDepth >= 8)
        throw new Error(
          "Service binding recursion limit exceeded (8)");
      __cell.svcDepth = (__cell.svcDepth || 0) + 1;
      const ctx = __beginEvent();
      // The sub-dispatch runs under the caller's context, so stubs and
      // streams its handler revives register into the live invocation
      // and stay usable (a fresh never-live scope would dispose them
      // on arrival). They die with the caller, like its own; a caller
      // past its end still takes the dead path through __ctxAdopt.
      const callerCtx = __ctxNow();
      try {
        return __finishServiceFetch(
          await __ctxRun(callerCtx,
            () => __cell.selfFetch(req, __cell.env, ctx)),
          req, redirectState);
      } finally {
        __cell.svcDepth--;
        __endEvent();
      }
    }
    // The verbatim header list; see the note on the outbound `fetch`. The
    // target isolate rebuilds a `Headers` from these pairs, so a repeat and
    // its casing survive the crossing.
    const headers = JSON.stringify(__cello.$$headerList(req.headers));
    // Cross-isolate calls carry exact bytes. A host-backed stream transfers
    // its one source to the target, and another stream uses the byte fallback.
    const { body, streamId } = await __subrequestBody(req);
    const r = JSON.parse(await (signal
      ? __awaitCancellableDoCall(
        __svc_call_cancellable(
          script, req.url, req.method, body, headers, streamId, entrypoint,
          propsSc),
        signal)
      : __svc_call(
        script, req.url, req.method, body, headers, streamId, entrypoint,
        propsSc)));
    const responseBody = __hostReplyBody(r.status, r);
    return __finishServiceFetch(
      new Response(responseBody, {
        status: r.status, headers: r.headers, __wsTarget: r.wsTarget,
      }),
      req, redirectState);
  },
  // Workerd's test-visible Fetcher.scheduled(): invoke the target's
  // scheduled handler and report the outcome.
  async scheduled(options = {}) {
    if (script !== __cell.script || entrypoint !== null ||
        typeof __cell.selfScheduled !== "function")
      throw new Error(
        "scheduled() is only implemented for same-script service " +
        "bindings whose target has a scheduled handler");
    let noRetry = false;
    const ctrl = {
      scheduledTime: options.scheduledTime === undefined
        ? Date.now() : Number(options.scheduledTime),
      cron: options.cron === undefined ? "" : String(options.cron),
      noRetry() { noRetry = true; },
    };
    const ctx = __beginEvent();
    try {
      await __cell.selfScheduled(ctrl, __cell.env, ctx);
      return { outcome: "ok", noRetry };
    } finally {
      const drain = __endEvent();
      // scheduled() reports the event outcome, so it cannot return while the
      // work that defines that event is still pending. Fetch replies differ:
      // a streamed response can depend on waitUntil work and must return first.
      if (drain !== null) await drain;
    }
  },
  };
  // An omitted entrypoint selects the module's default export, which may be a
  // WorkerEntrypoint or an object handler. `entrypoint = "Name"` selects that
  // named class. Any property other than a reserved Fetcher method is an
  // awaitable/callable pipeline node: awaiting
  // resolves the property remotely (a property-GET wire op), calling
  // invokes it, and deeper access extends the path, resolved on the
  // receiver side in one op. Same-script dispatch stays in this
  // isolate, so stub-able values may cross. A property path rooted
  // directly at the binding is context-free (ctx null): awaiting it
  // starts a fresh session, as in Workerd.
  const rpcEntrypoint = entrypoint === null ? "default" : entrypoint;
  const session = __entrypointSession(
    rpcEntrypoint, script === __cell.script, script, undefined, propsSc);
  const binding = new Proxy(target, {
    getPrototypeOf: () => __cf.ServiceStub.prototype,
    get: (base, prop) => {
    if (prop === "then") return undefined; // a stub is not a thenable
    if (Reflect.has(base, prop)) return Reflect.get(base, prop);
    if (typeof prop !== "string") return undefined;
    // Workerd's test hook: the named method as a callable handle,
    // resolved -- and refused -- on the receiver side.
    if (prop === "getRpcMethodForTestOnly")
      return (name) => __makeNode(session, [String(name)], null);
    return __makeNode(session, [prop], null);
  }});
  __outboundMeta.set(binding, { script, entrypoint, propsSc });
  // The interface's own `fetch` reads this, so a Worker that reaches the
  // member through `ServiceStub.prototype` gets the same request as through
  // the binding itself (#259).
  __weakSet(__serviceStubFetchers, binding, (input, init) =>
    target.fetch(input, init));
  return binding;
};

// Copy loader-supplied strings into `process.env` without installing the
// loader env itself. `build_env` runs after the user module evaluates, when
// `__installLoaderEnv` installs `__cell.env` too; this entry runs before
// evaluation so module-scope reads agree with the handler's `env`. The copy
// follows the same compat gate `populate_process_env` checks, carries only
// string entries, and never carries the reserved host-owned name
// (denoland/celld#296).
__cello.__installLoaderProcessEnv = (bytes, reservedName) => {
  const env = __sc_decode(bytes);
  if (env === null || typeof env !== "object") return;
  if (__cello.__cell.compat?.nodejsProcessEnv &&
      globalThis.process && globalThis.process.env) {
    for (const key of Object.keys(env)) {
      if (key === reservedName) continue;
      // `__proto__` assigns through the prototype setter and would
      // re-prototype `process.env`; `constructor`/`prototype` would shadow
      // the inherited members. None of them is a real variable.
      if (key === "__proto__" || key === "constructor" || key === "prototype") continue;
      const value = env[key];
      if (typeof value === "string") globalThis.process.env[key] = value;
    }
  }
};

__cello.__installLoaderEnv = (bytes, routes) => {
  const env = __sc_decode(bytes);
  const seen = new Set();
  const revive = (v) => {
    if (v === null || typeof v !== "object") return v;
    const routeIndex = Object.prototype.hasOwnProperty.call(
        v, __loaderServiceMarker) ? v[__loaderServiceMarker] : undefined;
    if (Number.isSafeInteger(routeIndex) && routeIndex >= 0) {
      // Rust supplies flat [script, entrypoint, props] triples, and the marker
      // stores the triple index rather than an offset into that array.
      const routeOffset = routeIndex * 3;
      if (!Number.isSafeInteger(routeOffset) || routeOffset + 2 >= routes.length)
        throw new DOMException(
          "Worker Loader env contains an invalid capability.",
          "DataCloneError");
      return __makeServiceBinding(
        routes[routeOffset], routes[routeOffset + 1], routes[routeOffset + 2]);
    }
    if (seen.has(v)) return v;
    seen.add(v);
    if (v instanceof Map) {
      const entries = [...v];
      v.clear();
      for (const [key, item] of entries) v.set(revive(key), revive(item));
      return v;
    }
    if (v instanceof Set) {
      const items = [...v];
      v.clear();
      for (const item of items) v.add(revive(item));
      return v;
    }
    if (Array.isArray(v) || Object.getPrototypeOf(v) === Object.prototype ||
        Object.getPrototypeOf(v) === null)
      for (const key of Object.keys(v)) v[key] = revive(v[key]);
    return v;
  };
  Object.assign(__cell.env, revive(env));
  // The host value wins over anything the loader's props smuggled in: the
  // node identity names where this isolate runs, and a loaded Worker that
  // could set it could report any region it likes (denoland/celld#219).
  // `__cell.node` is the same internal routing identity `build_env`
  // installs from, so this re-assertion cannot drift from it. Isolates the
  // host built without an identity (dynamic Workers) keep what the loader
  // passed: they run wherever their loader runs, so its value is already
  // the honest one.
  if (__cello.__cell.node !== undefined && __cello.__cell.node !== "") {
    __cell.env[__cello.__nodeIdentityEnv] = __cello.__cell.node;
  }
  // Handler-time parity for the pre-evaluation copy: the same strings land
  // in `process.env` after `__cell.env` installs.
  __cello.__installLoaderProcessEnv(bytes, __cello.__nodeIdentityEnv);
};
// RPC marshalling: V8 structured clone (Workerd js-rpc semantics), so
// undefined, Date, Map, Set, BigInt, typed arrays, and cycles survive.
// A value V8 cannot clone throws DataCloneError, as in Workerd.
//
// The envelope's first byte tags the payload. 0xff (the clone version
// header) = plain clone, decoded as-is. 0x01 = clone of a lifted tree in
// which RpcTarget instances, functions, and Durable Object stubs were
// replaced by stub markers; the lift runs only after a plain clone
// already threw. A projection walk happens first because V8 silently erases
// custom and null prototypes. 0x02 = a lifted tree carrying only by-value
// host types (Blob/File, Headers,
// Request/Response) and no capabilities -- decoded like 0x01, but a
// reply so tagged does not root its callee context. 0x00 = clone of
// [error, ownProps] -- a callee exception crossing as a real Error.
const __dataCloneError = (error) => new DOMException(
  String(error && error.message || error), "DataCloneError");
const __rpcUnsupportedType = (value) => new DOMException(
  'Could not serialize object of type "' +
  (__util_constructor_name(value) || "Object") +
  '". This type does not support serialization.',
  "DataCloneError");
// V8 clones a user-class instance and a null-prototype object as plain data,
// but Workerd refuses both over RPC. Project the value graph before V8 can
// erase that distinction. The projection also snapshots enumerable getters,
// so validation and serialization cannot invoke one getter twice. Native
// clone types and RPC host types keep their own serializers.
const __rpcProject = (value) => {
  const seen = new __primMap();
  const originals = new __primWeakMap();
  // A host value (a signal, Request/Response/Headers/Blob, a stream, a stub)
  // must take the lift path. It used to force that by carrying an enumerable
  // function brand, which made the plain V8 clone throw; with the state in
  // private slots the clone would flatten it to `{}` instead, so the
  // projection says outright that the graph is not plain data.
  let liftable = false;
  const set = (target, key, value) => __primDefineProp(target, key, {
    value, enumerable: true, configurable: true, writable: true,
  });
  const project = (v) => {
    if (v === null || (typeof v !== "object" && typeof v !== "function"))
      return v;
    const cached = __mapGet(seen, v);
    if (cached !== undefined) return cached;
    // Only the interfaces whose state is entirely private can reach the clone
    // as `{}`: everything else below keeps enough function-valued own state
    // that V8 refuses it (a stub Proxy), which selects the lift path on its
    // own. A stream used to qualify too -- its source, controller and reader
    // were own fields -- so it is listed here now that they live in the
    // private state (#170).
    const privateStateHost =
      v instanceof Headers || v instanceof Blob || v instanceof Request ||
      v instanceof Response || v instanceof AbortSignal ||
      v instanceof ReadableStream || v instanceof WritableStream ||
      (typeof CryptoKey !== "undefined" && v instanceof CryptoKey);
    if (typeof v === "function" || v instanceof __cf.RpcTarget ||
        __weakHas(__stubMeta, v) || __weakHas(__svcMeta, v) || __do_is_stub(v) ||
        privateStateHost ||
        v instanceof Date || v instanceof RegExp || v instanceof Error ||
        v instanceof ArrayBuffer || ArrayBuffer.isView(v) ||
        (typeof SharedArrayBuffer !== "undefined" &&
          v instanceof SharedArrayBuffer) ||
        (typeof WebAssembly !== "undefined" &&
          v instanceof WebAssembly.Module) ||
        __util_proxy_details(v) !== undefined) {
      if (privateStateHost) liftable = true;
      return v;
    }
    if (v instanceof __primMap) {
      const out = new __primMap();
      __mapSet(seen, v, out);
      for (const [key, entry] of v) {
        __mapSet(out, project(key), project(entry));
      }
      return out;
    }
    if (v instanceof __primSet) {
      const out = new __primSet();
      __mapSet(seen, v, out);
      for (const entry of v) __setAdd(out, project(entry));
      return out;
    }
    if (__primArrayIsArray(v)) {
      const out = new Array(v.length);
      __mapSet(seen, v, out);
      __weakSet(originals, out, v);
      const keys = __primObjectKeys(v);
      for (let i = 0; i < keys.length; i++) {
        const key = keys[i];
        set(out, key, project(v[key]));
      }
      return out;
    }
    if (__primGetProto(v) !== Object.prototype)
      throw __rpcUnsupportedType(v);
    const out = {};
    __mapSet(seen, v, out);
    __weakSet(originals, out, v);
    const keys = __primObjectKeys(v);
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      set(out, key, project(v[key]));
    }
    return out;
  };
  return { tree: project(value), originals, liftable };
};
// ---- request-context confinement -------------------------------
// Workerd ties I/O objects to the IoContext of the event that made
// them. Cells' equivalent: each dispatched event enters an
// async-context frame (the same CPED the ALS rides, so it survives
// awaits) carrying a context id. Stubs remember their owning
// context and refuse to serialize elsewhere; pipeline nodes refuse
// foreign awaits and calls -- each with Workerd's exact error.
let __nextCtxId = 1;
const __ctxKey = Symbol("cello.ctx");
// The innermost `__ctxRun` id for synchronous execution. V8 snapshots
// CPED when a promise reaction is registered and restores it while the
// reaction runs -- but synchronous execution runs under no reaction, so
// the frame reads undefined there (e.g. an argument lift in a handler's
// sync prefix, before its first await). `__ctxRun` therefore also
// records its id here for the sync span; continuations keep using the
// CPED frame, which wins whenever it is defined.
let __ctxSync = undefined;
const __ctxNow = () => {
  const frame = __als_get();
  if (frame !== undefined) {
    const id = frame.get(__ctxKey);
    if (id !== undefined) return id;
  }
  return __ctxSync;
};
// Run `fn` under context `id` (undefined = a fresh one). An async
// fn started inside keeps the frame across its awaits. The prior
// frame is cloned so user AsyncLocalStorage stores still flow into
// same-isolate callees, as they did before contexts existed.
const __ctxRun = (id, fn) => {
  const prior = __als_get();
  const syncPrior = __ctxSync;
  const frame = new Map(prior);
  const ctxId = id ?? __nextCtxId++;
  frame.set(__ctxKey, ctxId);
  __als_set(frame);
  __ctxSync = ctxId;
  try {
    return fn();
  } finally {
    __als_set(prior);
    __ctxSync = syncPrior;
  }
};
const __ctxError = (kind) => new Error(
  "Cannot perform I/O on behalf of a different request. I/O " +
  "objects (such as streams, request/response bodies, and others) " +
  "created in the context of one request handler cannot be " +
  "accessed from a different request's handler. This is a " +
  "limitation of Cloudflare Workers which allows us to improve " +
  "overall performance. (I/O type: " + kind + ")");
// ---- abortable request contexts --------------------------------
// Workerd's ctx.abort(reason) on an entrypoint aborts the request's
// IoContext: the in-flight call rejects with the reason, and stubs
// the context holds are disposed (their disposal callbacks fire).
// State is allocated only when a context first holds a stub handle
// or aborts, so plain-data dispatch pays nothing; call paths gate
// every abort lookup on `__abortedCtxs.size` (one field read).
// Aborted ids stay recorded: a stub rooted in a dead context must
// keep rejecting with the reason.
const __abortedCtxs = new Map();
// An actor-flavored abort reason: in-flight ops reject with the
// wrapped reason, later ops with Workerd's post-abort message
// (upstream TODO(bug): should propagate the reason, but doesn't).
class __ActorAbort { constructor(reason) { this.reason = reason; } }
const __postAbortError = (aborted) => aborted instanceof __ActorAbort
  ? new Error("The execution context which hosts this callback " +
      "is no longer running.")
  : aborted;
// ctx id -> Set of live stub handles (metas) the context holds.
const __ctxStubs = new Map();
// Live invocation contexts: every dispatch site mints its id through
// __ctxNext (count 1), and __ctxEnd drops it. A drive on a stub rooted
// in an ended context runs under a fresh per-drive rescue id instead,
// so nested revives land in a live context again and each drive's end
// disposes exactly what that drive registered -- concurrent drives
// never dispose each other's handles. Revives landing with no live
// context -- a floating call settling after response end, a dup() or
// an upload pump in post-end work -- take the dead path instead of
// registering: host holds release immediately, same-isolate handles
// stay usable but unowned. `__ctxRun(undefined)` fresh ids are op
// scopes, never invocations, and are intentionally not live.
const __ctxLive = new Map();
// Mint one invocation context id and mark it live.
const __ctxNext = () => {
  const id = __nextCtxId++;
  __ctxLive.set(id, 1);
  return id;
};
// The context a revive/dup/pump registers into: the current frame's id
// when that invocation is live, else undefined (the dead path). A dead id
// never re-registers.
const __ctxAdopt = () => {
  const now = __ctxNow();
  return now !== undefined && __ctxLive.has(now) ? now : undefined;
};
// ctx id -> Set of reject callbacks for stub ops in flight against
// that context. A hung callee never settles, so a context abort
// must reject its pending callers directly.
const __ctxPendingOps = new Map();
const __opTrack = (ctx, reject) => {
  let set = __ctxPendingOps.get(ctx);
  if (set === undefined) __ctxPendingOps.set(ctx, set = new Set());
  set.add(reject);
  return () => {
    set.delete(reject);
    if (set.size === 0) __ctxPendingOps.delete(ctx);
  };
};
const __ctxRegister = (meta) => {
  // Revive sites adopt a live context first; this stays total so a
  // direct caller can never re-create an ended context's set.
  if (meta.ctx === undefined || !__ctxLive.has(meta.ctx)) return;
  let set = __ctxStubs.get(meta.ctx);
  if (set === undefined) __ctxStubs.set(meta.ctx, set = new Set());
  set.add(meta);
};
const __ctxUnregister = (meta) => {
  const set = __ctxStubs.get(meta.ctx);
  if (set === undefined) return;
  set.delete(meta);
  if (set.size === 0) __ctxStubs.delete(meta.ctx);
};
// Tear down a context: drop it from the live set, stop every upload pump
// it owns and dispose every handle it still holds, so a dup() leaked
// into it reaches the sender's disposal callback. Runs at invocation end
// (fetch/RPC/alarm/queue), which is what makes a received stub owned by
// its request: the caller side dies with the call, and every release
// routes to the owner. A received stream with a live reader is spared:
// the reader holds the stream past its invocation, and closing it here
// would fire a carrier the sender never closed.
const __ctxEnd = (id) => {
  __ctxLive.delete(id);
  __ctxEndUploads(id);
  // Worker isolates never release cells, so entrypoint-ctx.abort records
  // would accumulate one per aborted ctx until heap retirement: prune
  // past a threshold. Steady-state cost is one field read.
  if (__abortedCtxs.size > 1024) __pruneAbortedCtxs();
  const set = __ctxStubs.get(id);
  if (set === undefined) return;
  __ctxStubs.delete(id);
  for (const meta of set) {
    if (!__streamHeld(meta)) __disposeStub(meta);
  }
};
// Drop abort records no live entry or handle references anymore. A stub
// rooted in a dead context must keep rejecting with its reason, but every
// check reads through a live entry or handle context: with none left,
// the record is unobservable.
const __pruneAbortedCtxs = () => {
  const live = new Set();
  for (const entry of __stubEntries.values()) live.add(entry.ctx);
  for (const id of __ctxStubs.keys()) live.add(id);
  for (const id of [...__abortedCtxs.keys()]) {
    if (!live.has(id)) __abortedCtxs.delete(id);
  }
};
const __ctxAbort = (id, reason) => {
  if (id === undefined || __abortedCtxs.has(id)) return;
  const stored = reason === undefined
    ? new Error("The execution context has been aborted.")
    : reason;
  __abortedCtxs.set(id, stored);
  __ctxEnd(id);
  const pending = __ctxPendingOps.get(id);
  if (pending === undefined) return;
  __ctxPendingOps.delete(id);
  const live =
    stored instanceof __ActorAbort ? stored.reason : stored;
  for (const reject of pending) reject(live);
};
// The entrypoint ctx.abort. Instances are cached across calls, so
// the construction-time ctx cannot pin a request id (Workerd
// constructs per request); the current frame's id at abort() time
// is the request being served, which matches Workerd observably.
const __ctxAbortCurrent =
  (reason) => __ctxAbort(__ctxNow(), reason);
// A fetch response awaiting its read: the invocation context that produced
// it. The response reader ends the context once the body no longer needs
// it (immediately for bytes bodies, when the bridge pump settles for live
// ones), so received stubs die with the fetch that received them without
// breaking a body still streaming. A side table, not a property: the
// response object stays exactly what the handler returned.
const __fetchCtxs = new WeakMap();
// ctx id -> Set of live upload pumps (caller-pushed streams) the context
// owns. Context end/abort stops every pump: the reader cancels (the origin
// sees it) and the channel aborts (the receiver observes the disconnect),
// so an upload never outlives its sender's invocation.
const __ctxUploads = new Map();
// ctx id -> Set of every upload id the context created. A pump that
// already finished unregisters from __ctxUploads, but its host entry
// still needs the sender-gone terminal at context end: without this,
// the entry lingers with sender_alive until isolate retirement, and
// the channels do not drain with the request.
const __ctxUploadIds = new Map();
const __ctxEndUploads = (id) => {
  const pumps = __ctxUploads.get(id);
  if (pumps !== undefined) {
    __ctxUploads.delete(id);
    for (const pump of pumps) {
      try {
        pump.reader.cancel().catch(() => {});
      } catch {}
    }
  }
  const ids = __ctxUploadIds.get(id);
  if (ids === undefined) return;
  __ctxUploadIds.delete(id);
  for (const uid of ids) {
    try {
      __upload_abort(uid);
    } catch {}
  }
};
// ---- same-process RPC stubs ------------------------------------
// A stub entry owns a local target; `refs` counts live handles
// across dup()s. When the last handle is disposed the target's own
// Symbol.dispose runs (async, matching Workerd's disposal callback).
// Stubs cross same-isolate transports only (same-script entrypoint
// RPC and same-process routed dispatch); a marker revived elsewhere fails
// loudly on use instead of aliasing an unrelated local entry.
// `ctx` records the owning request context: the entry's for
// running its target, the handle's for the serialize-elsewhere
// check.
const __stubEntries = new Map();
const __stubMeta = new WeakMap();
// Cross-isolate RPC stubs: the owning isolate maps each host-routable id
// back to its local entry, and the inverse for release. The host table is
// authoritative for routing; these only find the entry to run.
const __hostStubs = new Map();
const __hostStubLocals = new Map();
// Durable Object stubs carry a host `v8::Private` brand (set by
// `__do_brand_stub` in `get()`), read only by host ops (`__do_is_stub`,
// `__do_lift_record`): no JS side table to pollute, and the brand
// snapshots the id and dispatch name at mint, so a later `stub.name =`
// cannot pollute the receiver's idNames.
let __nextStubId = 1;
const __stubIsolate = Math.random().toString(36).slice(2);
const __newEntry = (target) => {
  // `scope` records the actor event that minted the entry (top of
  // the event stack at lift time), so an actor breakage can find
  // and abort the contexts hosting its exported stubs, and an op on
  // the stub can queue on that cell's input gate. `section` is the
  // critical section running at lift time, if any -- an op on this
  // stub re-enters it rather than queueing behind it.
  const scope = __currentActorScope() || undefined;
  const entry = {
    id: __nextStubId++, target, refs: 1, ctx: __ctxNow(), scope,
    section: __cellBlocks.get(scope)?.holder ?? null,
  };
  __stubEntries.set(entry.id, entry);
  return entry;
};
// Actors broken in JS: scope -> reason. Workerd's DO ctx.abort
// rejects in-flight and future calls; when the abort fires under a
// same-isolate stub caller, terminate_execution would unwind that
// caller too (the routed dispatch re-enters the isolate), so the
// breakage is recorded here instead: contexts hosting the actor's
// exported stubs abort (in-flight ops reject with the reason,
// later ops with the post-abort message) and RPC dispatch to the
// scope rejects with the reason. Direct host-dispatched DO events
// keep the uncatchable terminate_execution path.
const __brokenActors = new Map();
const __actorBreak = (scope, reason) => {
  if (__brokenActors.has(scope)) return;
  __brokenActors.set(scope, reason);
  for (const entry of __stubEntries.values())
    if (entry.scope === scope)
      __ctxAbort(entry.ctx, new __ActorAbort(reason));
};
// Everything this isolate holds for one cell scope that belongs to the
// cell's residency rather than to the isolate, so a fresh instance
// inherits none of it. `adopt_cell` calls this on both edges: the
// give-back frees it, the take-in guarantees that no epoch spans.
// Storage is not here -- `stop_cell` closes it host-side -- and sockets
// live in the host registry, so a hibernated cell keeps them.
const __cellRelease = (scope) => {
  __cell.instances[scope]?.ctx?.facets?._release();
  delete __cell.instances[scope];
  delete __cell.idNames[scope];
  delete __cell.facetConfigs[scope];
  __brokenActors.delete(scope);
  // An abandoned block can never run `__blockLeave`. A normal eviction
  // cannot abandon a block because its event keeps the cell in flight. A
  // stop that waits for nobody can, so the release drops every block record
  // together with the other state for this residency.
  __cellBlocks.delete(scope);
  // An entry holds `target`, which can be an object the released
  // instance owns. Dropping it ends that reachability; `released` is
  // what an already-revived handle reads, since it holds its entry
  // directly and never looks in this map again.
  for (const entry of __stubEntries.values()) {
    if (entry.scope !== scope) continue;
    entry.released = true;
    __stubEntries.delete(entry.id);
    // Revoke every host id this residency exported for the entry, so
    // cross-isolate handles fail instead of reaching a new residency.
    // Synchronous: the isolate may be going away with the residency.
    const exported = __hostStubLocals.get(entry.id);
    if (exported !== undefined) {
      __hostStubLocals.delete(entry.id);
      for (const id of exported) {
        __hostStubs.delete(id);
        __stub_unexport(id);
      }
    }
    // Break in-flight ops as an abort would, but not through
    // `__actorBreak`: that records the scope broken for the life of the
    // isolate, which is the state this function drops.
    __ctxAbort(entry.ctx, new __ActorAbort(__releasedStubError()));
  }
  // Drop abort records no live handle references anymore, so aborted
  // ids do not grow by one per aborted context for the life of the
  // isolate. Context end prunes the same way past a threshold, which is
  // what bounds Worker isolates that never release cells.
  if (__abortedCtxs.size !== 0) __pruneAbortedCtxs();
};
const __releasedStubError = () => new Error(
  "The Durable Object that returned this RPC stub no longer runs on " +
  "this node.");
const __disposeStub = (meta) => {
  if (meta.disposed) return;
  meta.disposed = true;
  __ctxUnregister(meta);
  const entry = meta.entry;
  // Host-backed handles each own one host hold (revive, dup, unlift
  // restore), so every dispose drops exactly one. A host entry holds no
  // other local state: it is never in `__stubEntries` and carries no
  // upload or target, so only the shared refcount still ticks.
  if (entry.host !== undefined) {
    __stub_release(entry.host.id).catch(() => {});
    --entry.refs;
    return;
  }
  if (--entry.refs > 0) return;
  __stubEntries.delete(entry.id);
  // A received upload's dispose cancels it: the sender's pump stops and
  // the channel frees. Fire-and-forget, like the disposal below.
  if (entry.upload !== undefined) {
    try {
      __upload_cancel(entry.upload.id);
    } catch {}
    return;
  }
  const disposer = entry.target?.[Symbol.dispose];
  if (typeof disposer === "function")
    Promise.resolve().then(() => disposer.call(entry.target));
};
// Workerd's param-disposal rule: params received by the target are
// disposed when the op ends -- except a received stream with a live
// reader, which the receiver holds past the call.
const __disposeReceived = (received) => {
  for (const handle of received) {
    if (!__streamHeld(handle)) __disposeStub(handle);
  }
};
// A mint-cap refusal from `__stub_export`/`__upload_new`. Caps fail the
// call with a clear error; every other export throw falls back to an
// upload channel or a same-isolate marker.
const __isCapExceeded = (error) =>
  String(error?.message ?? error).startsWith("cello_cap_exceeded:");
const __capError = (error) =>
  new Error(String(error?.message ?? error).slice("cello_cap_exceeded:".length).trimStart());
// Mint one host-routable id for `scope`, or null when the scope has no
// live residency here (a stateless sender). A mint cap throws through.
const __exportHostScope = (scope, kind) => {
  try {
    return JSON.parse(__stub_export(scope, kind));
  } catch (error) {
    if (__isCapExceeded(error)) throw __capError(error);
    return null;
  }
};
// Record one minted id against its local entry.
const __recordHostStub = (entry, kind, coords) => {
  entry.hostKind = kind;
  __hostStubs.set(coords.id, entry.id);
  let locals = __hostStubLocals.get(entry.id);
  if (locals === undefined)
    __hostStubLocals.set(entry.id, locals = new Set());
  locals.add(coords.id);
  return coords;
};
// Export one local entry under a host-routable id, or null when the
// entry's scope has no live residency here (a Worker-owned entry); the
// lifter then pushes the value through an upload channel or keeps a
// same-isolate marker instead.
const __exportHostStub = (entry, kind) => {
  const coords = __exportHostScope(entry.scope ?? "", kind);
  if (coords === null) return null;
  return __recordHostStub(entry, kind, coords);
};
// Undo a host lift whose envelope never sent (a mint cap threw, or the
// marker tree failed to encode): restore moved handles to the sender
// (re-acquiring handed-off holds) and drop fresh entries (unexporting
// their host ids, cancelling fresh bridges whose origins never reached
// a receiver). A failed send keeps the sender's handles and leaks
// nothing on either side; without this a cap-hit would strand table
// entries toward the same cap it enforces.
const __unliftFailed = (collected) => {
  if (collected === null) return;
  for (const { entry, bridge } of collected.entries) {
    const exported = __hostStubLocals.get(entry.id);
    if (exported !== undefined) {
      __hostStubLocals.delete(entry.id);
      for (const id of exported) {
        __hostStubs.delete(id);
        try {
          __stub_unexport(id);
        } catch {}
      }
    }
    __stubEntries.delete(entry.id);
    if (bridge) {
      try {
        entry.target[Symbol.dispose]?.();
      } catch {}
    }
  }
  for (const meta of collected.handles) {
    meta.disposed = false;
    __ctxRegister(meta);
    if (meta.entry.host !== undefined) {
      try {
        __stub_acquire(meta.entry.host.id);
      } catch {}
    }
  }
};
// A revived host stub's local entry: not in `__stubEntries` (its id is the
// host id, meaningless here), but refcounted like one, so dup() shares the
// release and the last dispose frees it.
const __hostStubEntry = (id, marker) => ({
  id: "h:" + id,
  host: { id, scope: marker.scope, epoch: marker.epoch, gen: marker.gen },
  refs: 1,
});
const __stubDisposedError = () =>
  new Error("RPC stub used after being disposed.");
// Dispose host-backed handles revived into `ctx` since `before` (a snapshot
// Set of its handles, or null when it held none): a drive's nested revives
// that its reply did not move out. Same-isolate handles keep their
// lifetime; only what crosses isolates shortens. A nested stream with a
// live reader is spared like at context end; a dropped one frees when
// collected.
const __disposeNewHostHandles = (ctx, before) => {
  if (ctx === undefined) return;
  const set = __ctxStubs.get(ctx);
  if (set === undefined) return;
  for (const meta of [...set]) {
    if (before !== null && before.has(meta)) continue;
    if (meta.entry.host === undefined && meta.entry.upload === undefined)
      continue;
    if (!__streamHeld(meta)) __disposeStub(meta);
  }
};
// Shared brand value for RpcTarget instances; see the RpcTarget
// constructor.
const __rpcNoClone = () => {};
// Workerd method-visibility rules by target kind: an RpcTarget
// exposes inherited methods/accessors (never own instance state,
// never Object.prototype); a plain object or function exposes own
// properties only. Property reads go through normal [[Get]] so
// Proxy handlers participate, as in Workerd.
const __rpcNoSuchMethod = (prop) => new TypeError(
  'The RPC receiver does not implement the method "' + prop + '".');
// Bind through the intrinsic because a callable proxy can implement `.bind`
// as a remote property. Reading that property can turn a valid named RPC
// method into a non-function. Keep this rule shared by every RPC binding site.
const __rpcBindMethod = (value, receiver) =>
  Reflect.apply(Function.prototype.bind, value, [receiver]);
const __stubResolve = (target, prop) => {
  if (target instanceof __cf.RpcTarget) {
    if (Object.hasOwn(target, prop) || !(prop in target) ||
        prop in Object.prototype) throw __rpcNoSuchMethod(prop);
  } else if (!Object.hasOwn(target, prop)) {
    throw __rpcNoSuchMethod(prop);
  }
  const value = target[prop];
  // Keep cello's own stubs and pipeline nodes unwrapped because the wrapper
  // would lose their metadata.
  return typeof value === "function" && !__stubMeta.has(value) &&
      !(value instanceof __cf.RpcPromise) &&
      !(value instanceof __cf.RpcProperty)
    ? __rpcBindMethod(value, target) : value;
};
// ---- streams over RPC ------------------------------------------
// A live stream crosses as a handle in the caps table: the sender
// locks the origin (reader/writer acquired at lift) behind a
// bridge entry, and the receiver's endpoint is an ordinary
// ReadableStream/WritableStream whose pulls and writes are stub
// ops against the bridge -- one bounded reverse call per chunk,
// the clone being the one wire copy. Backpressure is the op in
// flight: hwm 0 pulls only on demand, and the writable carries
// one chunk per op. EOF, close, and errors cross as op results;
// teardown (param disposal, context end, context abort) runs the
// bridge disposer, which cancels or aborts an unfinished origin
// with Workerd's generic disconnect errors -- reasons do not
// propagate, matching Workerd's own TODOs (and its verbatim
// "endeded" typo).
const __wsDisconnect = () => new Error(
  "WritableStream received over RPC was disconnected because " +
  "the remote execution context has endeded.");
const __rsDisconnect = () => new Error(
  "ReadableStream received over RPC disconnected prematurely.");
// Receiver wrapper stream -> its handle meta, for forwarding.
const __rpcStreamMeta = new WeakMap();
// A stream received over RPC stays usable until the receiver releases
// it: an active reader holds the stream past the receiving call and
// its invocation, the way a ReadableStream sent over RPC stays open
// until its writer closes it. Collection -- not context end -- frees
// a wrapper the receiver dropped without closing. The held value is
// the meta, which points back at the wrapper only weakly (see
// `__reviveStream`), so the wrapper still collects; disposal is
// idempotent, so an explicit dispose first makes this a no-op.
const __rpcStreamFinalizer = new FinalizationRegistry((meta) => {
  try {
    __disposeStub(meta);
  } catch {}
});
// Whether an automatic disposal (param disposal, context end, drive
// end) must spare this handle: a received stream whose wrapper is
// still locked to a live reader. Anything else -- stubs, unlocked or
// collected wrappers -- disposes as before, so a dropped stream still
// frees its hold promptly at request end.
const __streamHeld = (meta) =>
  meta.wrapper !== undefined && meta.wrapper.deref()?.locked === true;
const __readableBridge = (reader) => {
  const bridge = {
    done: false,
    async read() {
      let result;
      try {
        result = await reader.read();
      } catch (error) {
        bridge.done = true;
        throw error;
      }
      if (result.done) bridge.done = true;
      return result;
    },
    cancel() {
      if (bridge.done) return;
      bridge.done = true;
      reader.cancel(__rsDisconnect()).catch(() => {});
    },
    [Symbol.dispose]() { bridge.cancel(); },
  };
  return bridge;
};
const __writableBridge = (writer) => {
  const bridge = {
    done: false,
    write: (chunk) => writer.write(chunk),
    close() {
      bridge.done = true;
      return writer.close();
    },
    abort() {
      bridge.done = true;
      return writer.abort(__wsDisconnect());
    },
    [Symbol.dispose]() {
      if (!bridge.done) bridge.abort().catch(() => {});
    },
  };
  return bridge;
};
const __liftStream = (v, host = false, collector = null) => {
  const readable = v instanceof ReadableStream;
  const key = readable ? "__cello$rs" : "__cello$ws";
  const meta = __rpcStreamMeta.get(v);
  if (meta !== undefined && !meta.disposed && !v.locked) {
    // Re-serializing a received, untouched stream forwards the
    // original handle: the reference moves (like a stub) and
    // the local wrapper is dead -- a round trip stays one hop.
    meta.disposed = true;
    __ctxUnregister(meta);
    if (collector !== null) collector.handles.push(meta);
    // A received host stream forwards its host id, so the round
    // trip stays one hop across isolates too. This isolate's hold
    // moves with the reference; the receiver acquires on revive.
    if (meta.entry.host !== undefined) {
      const coords = meta.entry.host;
      try {
        __stub_handoff(coords.id);
      } catch {}
      return { [key]: coords.id, t: __stubIsolate, h: 1,
               scope: coords.scope, epoch: coords.epoch, gen: coords.gen };
    }
    // A received upload forwards its channel id the same way. Uploads
    // track no reader holds, so nothing else moves.
    if (meta.entry.upload !== undefined) {
      return { [key]: meta.entry.upload.id, t: __stubIsolate, h: 2 };
    }
    return { [key]: meta.entry.id, t: __stubIsolate };
  }
  if (v.locked)
    throw new TypeError(readable
      ? "The ReadableStream has been locked to a reader."
      : "The WritableStream has been locked to a writer.");
  const kind = readable ? "readable" : "writable";
  if (host) {
    // A cell-owned stream mints a routed id; a stream no cell owns (a
    // stateless sender lifting call arguments) pushes a readable through
    // an upload channel instead. The mint runs before any lock: the
    // bridge locks the origin, and an upload pumps the origin directly.
    // A caller-owned writable has no reverse path: it keeps a
    // same-isolate marker and fails on use across isolates, as before.
    const coords = __exportHostScope(__currentActorScope() || "", kind);
    if (coords !== null) {
      const entry = __newEntry(readable
        ? __readableBridge(v.getReader())
        : __writableBridge(v.getWriter()));
      __recordHostStub(entry, kind, coords);
      if (collector !== null) collector.entries.push({ entry, bridge: true });
      return { [key]: coords.id, t: __stubIsolate, h: 1,
               scope: coords.scope, epoch: coords.epoch,
               gen: coords.generation };
    }
    if (readable) {
      const upload = __exportUpload(v);
      if (upload !== null)
        return { [key]: upload, t: __stubIsolate, h: 2 };
    }
  }
  const bridge = readable
    ? __readableBridge(v.getReader())
    : __writableBridge(v.getWriter());
  const entry = __newEntry(bridge);
  if (collector !== null) collector.entries.push({ entry, bridge: true });
  return { [key]: entry.id, t: __stubIsolate };
};
// A received upload's local entry: not in `__stubEntries` (its id is the
// channel id, meaningless here). Disposing it cancels the channel; the
// sender's pump stops and the channel frees.
const __uploadEntry = (id) => ({
  id: "u:" + id,
  upload: { id },
  refs: 1,
});
// Open an upload channel for a caller-owned readable and start its pump:
// the sender reads the origin and pushes each chunk host-side, where the
// receiver pulls. Returns the upload id, or null when no sender context
// can own it (the caller keeps a same-isolate marker). A mint cap throws
// through: caps fail the call with a clear error. Only byte chunks cross:
// a non-bytes chunk fails the stream rather than mangling it.
const __exportUpload = (v) => {
  // No live sender context owns the pump (or none at all): refuse, so the
  // caller keeps a same-isolate marker instead of a channel no context
  // end will ever abort.
  const ctx = __ctxAdopt();
  if (ctx === undefined) return null;
  let id;
  try {
    id = JSON.parse(__upload_new()).id;
  } catch (error) {
    if (__isCapExceeded(error)) throw __capError(error);
    return null;
  }
  const reader = v.getReader();
  let pumps = __ctxUploads.get(ctx);
  if (pumps === undefined) __ctxUploads.set(ctx, pumps = new Set());
  const pump = { id, reader };
  pumps.add(pump);
  let ids = __ctxUploadIds.get(ctx);
  if (ids === undefined) __ctxUploadIds.set(ctx, ids = new Set());
  ids.add(id);
  // The pump is floating work: each push awaits buffer room (the receiver
  // pulls), so a fast producer cannot outrun a slow consumer. Context end
  // cancels the reader and aborts the channel, which rejects a parked
  // push; receiver cancel rejects the next push, which cancels the
  // origin. Every exit unregisters the pump, and every unclean exit
  // cancels the origin: a finished origin completed, but anything else
  // must observe the teardown (like a bridge disposer).
  (async () => {
    let clean = false;
    try {
      for (;;) {
        const next = await reader.read();
        if (next.done) {
          __upload_close(id);
          clean = true;
          return;
        }
        const value = next.value;
        const kind = __bodyChunkKind(value);
        if (kind !== __BODY_CHUNK_TYPED && kind !== __BODY_CHUNK_DATA_VIEW) {
          __upload_fail(id);
          return;
        }
        // Throws on a forged view; the catch below fails the upload rather
        // than pushing a window the guest only claimed.
        const bytes = __hostChunk(value, "an upload body must yield bytes");
        for (let offset = 0; offset < bytes.byteLength; offset += 64 * 1024) {
          await __upload_push(id, bytes.subarray(offset, offset + 64 * 1024));
        }
        if (bytes.byteLength === 0) await __upload_push(id, bytes);
      }
    } catch {
      // Cancelled by context end, rejected at a terminal, or unreadable:
      // the channel already carries (or, for a bare read error, now gets)
      // the terminal state the receiver observes.
      try {
        __upload_fail(id);
      } catch {}
    } finally {
      pumps.delete(pump);
      if (!clean) {
        try {
          await reader.cancel();
        } catch {}
      }
      try {
        reader.releaseLock();
      } catch {}
    }
  })();
  return id;
};
// A signal is not a snapshot on the RPC wire. The marker carries a
// process-wide identity, and the host forwards the source's single state
// transition to every receiving isolate. A received signal keeps that
// identity when an RPC forwards it again, so a chain does not add relays.
const __rpcSignalMeta = new WeakMap();
const __rpcSignalReceivers = new Map();
const __rpcSignalSourceFinalizer = new FinalizationRegistry(
  (id) => __rpc_signal_release(id));
const __rpcSignalReceiverFinalizer = new FinalizationRegistry(
  ({ id, subscription }) =>
    __rpc_signal_unsubscribe(id, subscription));
const __rpcSignalReasonOut = (reason) => {
  if (reason instanceof DOMException)
    return __scEncodeStrict({ d: true, n: reason.name, m: reason.message });
  try {
    return __scEncodeStrict({ v: reason });
  } catch {
    return __scEncodeStrict({ v: new Error(String(reason)) });
  }
};
const __rpcSignalReasonIn = (bytes) => {
  const reason = __sc_decode(bytes);
  return reason.d
    ? new DOMException(reason.m, reason.n)
    : reason.v;
};
const __rpcSignalSubscribe = (receiver) => {
  const signal = receiver.signal.deref();
  if (signal === undefined) return false;
  const pending = __rpc_signal_wait(receiver.id);
  receiver.subscription = pending.__celloRpcSignalSubscription;
  // A reason the isolate's budget cannot pay for rejects with a RangeError
  // (see __rpcSignalRefresh): the abort is real, only its payload is
  // unreadable, so deliver the standard AbortError instead of dropping it.
  pending.then((reason) => {
    const target = receiver.signal.deref();
    if (target !== undefined && reason.byteLength !== 0)
      __abortSignal(target, __rpcSignalReasonIn(reason));
  }, (error) => {
    if (!(error instanceof RangeError)) throw error;
    const target = receiver.signal.deref();
    if (target !== undefined)
      __abortSignal(target,
        new DOMException("The operation was aborted", "AbortError"));
  });
  // A wait belongs to the request that installed it. A retained signal needs
  // a fresh wait in each later request, and its finalizer must unsubscribe the
  // newest one rather than the dead request's subscription.
  __rpcSignalReceiverFinalizer.unregister(receiver);
  __rpcSignalReceiverFinalizer.register(signal, receiver, receiver);
  return true;
};
const __liftSignal = (signal) => {
  let meta = __rpcSignalMeta.get(signal);
  if (meta === undefined) {
    const id = __rpc_signal_new();
    meta = { id };
    __rpcSignalMeta.set(signal, meta);
    __rpcSignalSourceFinalizer.register(signal, id);
    if (signal.aborted) {
      __rpc_signal_abort(id, __rpcSignalReasonOut(signal.reason));
    } else {
      signal.addEventListener("abort", (event) => {
        __rpc_signal_abort(id,
          __rpcSignalReasonOut(event.target.reason));
      }, { once: true });
    }
  }
  const marker = { "__cello$sig": meta.id, a: signal.aborted };
  if (signal.aborted) marker.r = __rpcSignalReasonOut(signal.reason);
  return marker;
};
const __rpcSignalRefresh = () => {
  for (const [id, receivers] of __rpcSignalReceivers) {
    // A poll answers the encoded abort reason (a Uint8Array), null when the
    // source is gone, or undefined while it is still live. A reason the
    // isolate's budget cannot pay for throws a RangeError instead: the abort
    // is real, only its payload is unreadable. Deliver an AbortError with the
    // standard message and drop the record -- failing here would fail an
    // unrelated request before its handler ran, and the next request would
    // fail the same way.
    let state;
    let unpayable = false;
    try {
      state = __rpc_signal_poll(id);
    } catch (error) {
      if (!(error instanceof RangeError)) throw error;
      unpayable = true;
    }
    const live = new Set();
    for (const receiver of receivers) {
      const signal = receiver.signal.deref();
      if (signal === undefined) continue;
      if (state instanceof Uint8Array)
        __abortSignal(signal, __rpcSignalReasonIn(state));
      else if (unpayable)
        __abortSignal(signal,
          new DOMException("The operation was aborted", "AbortError"));
      else if (state !== null && __rpcSignalSubscribe(receiver))
        live.add(receiver);
    }
    if (unpayable || live.size === 0 || state === null || state instanceof Uint8Array)
      __rpcSignalReceivers.delete(id);
    else
      __rpcSignalReceivers.set(id, live);
  }
};
// ---- retained waits --------------------------------------------
// A stable promise across native-event retirements for a value a
// host-side waiter produces (a container exec's exit code). The backing
// subscription is unrefed: it belongs to the subscribing event and
// dies with it, so a promise held across events needs a fresh
// subscription in each later event -- the same rule retained RPC
// signals follow above. A waiter that kept its first subscription
// would hang every later await: the op's resolver is purged with its
// event and the promise never settles. Each refresh subscribes at
// most once per native event; the first settlement wins and
// unregisters, and a collected promise unregisters through the
// finalizer, so an un-awaited waiter retains only its record.
const __retainedWaits = new Map();
let __nextRetainedWaitId = 1;
const __retainedWaitFinalizer = new FinalizationRegistry((record) => {
  if (__retainedWaits.get(record.id) === record)
    __retainedWaits.delete(record.id);
});
const __settleRetainedWait = (record, ok, value) => {
  if (record.settled) return;
  record.settled = true;
  if (__retainedWaits.get(record.id) === record)
    __retainedWaits.delete(record.id);
  __retainedWaitFinalizer.unregister(record);
  if (ok) record.resolve(value);
  else record.reject(value);
};
const __subscribeRetainedWait = (record) => {
  let sub;
  try {
    sub = record.subscribe();
  } catch (error) {
    __settleRetainedWait(record, false, error);
    return;
  }
  sub.then(
    (value) => __settleRetainedWait(record, true, value),
    (error) => __settleRetainedWait(record, false, error),
  );
};
// `subscribe` issues one backing wait for the current event and answers
// its promise. Subscribes eagerly: the creating event is live, so an
// await in the same event resolves through its own subscription. The
// record keeps the owning cell scope: a refresh from another cell's
// event must not re-subscribe it, or the owner check fails and settles
// the wait with a refusal that has nothing to do with the waiter.
const __retainedWait = (subscribe) => {
  const record = {
    id: __nextRetainedWaitId++,
    subscribe,
    settled: false,
    ctx: String(__io_context_id()),
    scope: __currentActorScope(),
  };
  const promise = new Promise((resolve, reject) => {
    record.resolve = resolve;
    record.reject = reject;
  });
  __retainedWaits.set(record.id, record);
  __retainedWaitFinalizer.register(promise, record, record);
  __subscribeRetainedWait(record);
  return promise;
};
const __refreshRetainedWaits = (scope) => {
  if (__retainedWaits.size === 0) return;
  const ctx = String(__io_context_id());
  for (const record of __retainedWaits.values()) {
    if (record.settled || record.ctx === ctx) continue;
    if (record.scope !== scope) continue;
    record.ctx = ctx;
    __subscribeRetainedWait(record);
  }
};
// One exec output chunk that survives its subscribing event: the
// backing read is unrefed (it dies with its subscribing event), so
// the stable promise re-subscribes in each later event -- the same
// rule retained waits follow above. Subscriptions chain: each names
// the previous one, so a re-subscription after a dropped op takes
// the consumed bytes from the pipe stash instead of reading ahead,
// and a late pile-up comes back superseded (swallowed below: another
// subscription owns the read) instead of settling.
const __EXEC_READ_SUPERSEDED = "exec read superseded by a newer subscription";
const __execReadNever = new Promise(() => {});
const __retainedExecRead = (id, which) => {
  const lease = __exec_read_seq();
  let prev = 0;
  return __retainedWait(() => {
    const my = __exec_read_seq();
    const take = prev;
    prev = my;
    return __container_exec_read(id, which, lease, take, my).then(
      (bytes) => bytes,
      (error) => {
        if (String((error && error.message) || error) === __EXEC_READ_SUPERSEDED)
          return __execReadNever;
        throw error;
      });
  });
};
// A revived Durable Object stub holds one grant reference on its scope;
// collection drops it, so the grant set tracks live stubs instead of
// every scope ever received. Never throws: the release op is a no-op
// for unknown scopes.
const __doGrantFinalizer = typeof FinalizationRegistry === "function"
  ? new FinalizationRegistry((cell) => {
      try {
        __do_release(cell);
      } catch {}
    })
  : null;
// Build a live stub for a side-table entry and grant its scope. The
// entry's scope is what the lift checked before recording it, so the
// grant mints only for host-checked scopes. A cross-script entry names
// a class this isolate never declared; it revives with a placeholder
// key (get() routes by the id value alone and never reads the key),
// so a legitimately received stub survives the round trip.
const __makeDoStubFromTable = (entry) => {
  const doClass = entry.c;
  let namespace;
  try {
    namespace = __cell.makeNamespace(doClass);
  } catch {
    namespace = new DurableObjectNamespace(doClass, "foreign");
  }
  const stub = namespace.get(
    new DurableObjectId(doClass, entry.v, entry.n ?? undefined));
  const cell = doClass + ":" + entry.v;
  __do_grant(cell);
  if (__doGrantFinalizer) __doGrantFinalizer.register(stub, cell);
  return stub;
};
// Install side-table stubs into a decoded tree. Each entry names an
// absolute path from the envelope root; only listed paths become stubs,
// and any marker-shaped object in the payload stays inert data. The path
// is validated before the grant mints (H5): a broken path -- which a
// host-built envelope never produces -- installs nothing and grants
// nothing, so no refcount leaks.
const __installDoTable = (root, table) => {
  if (!__primArrayIsArray(table) || table.length === 0) return root;
  let out = root;
  for (let t = 0; t < table.length; t++) {
    const entry = table[t];
    if (entry === null || typeof entry !== "object") continue;
    const path = entry.p;
    if (!__primArrayIsArray(path)) continue;
    if (typeof entry.c !== "string" || typeof entry.v !== "string") continue;
    // Prototype-chain segments install nothing and grant nothing, as
    // defense in depth for rows written before the record-time check.
    let refused = false;
    for (let r = 0; r < path.length; r++) {
      const seg = path[r];
      if (seg === "__proto__" ||
          seg === "constructor" || seg === "prototype") {
        refused = true;
        break;
      }
    }
    if (refused) continue;
    if (path.length === 0) {
      out = __makeDoStubFromTable(entry);
      continue;
    }
    let node = out;
    let ok = true;
    for (let i = 0; i < path.length - 1; i++) {
      if (node === null || typeof node !== "object") {
        ok = false;
        break;
      }
      // Own-property walk only: a forged intermediate has no own
      // property here (a `__proto__` placeholder is lost in the clone),
      // so the entry installs nothing and grants nothing.
      if (!__primHasOwn(node, path[i])) {
        ok = false;
        break;
      }
      node = node[path[i]];
    }
    if (!ok || node === null || typeof node !== "object") continue;
    const last = path[path.length - 1];
    if (!__primHasOwn(node, last)) continue;
    // Own-property write through the captured primordial, never a plain
    // assignment through a possibly-inherited setter.
    __primDefineProp(node, last, {
      value: __makeDoStubFromTable(entry),
      writable: true, enumerable: true, configurable: true,
    });
  }
  return out;
};
// Replace stub-able values with wire markers. Runs only after a
// plain clone failed, so plain-data serialization never pays for
// it. Passing an existing stub transfers its reference: the
// sender's handle is disposed (dup() first to keep one) and the
// receiver adopts it. Returns null when nothing was liftable.
// `allowCapabilities` is also the lift mode: truthy lifts, and
// "host" mints host-routable ids for entries a cell owns (Durable
// Object RPC across isolates), falling back to same-isolate markers
// for values no cell owns. "do" is the cross-isolate
// service-binding mode: only Durable Object stubs cross (by value,
// no caps), every other capability stays a DataCloneError as with
// false.
//
// Durable Object stubs do not mint shape markers in the tree. The lift
// records each host-checked scope in a host-side table outside the tenant
// payload and leaves an inert placeholder behind; revive installs stubs
// only for listed paths, so a marker-shaped object a tenant smuggles
// into the payload -- through a class instance the lift leaves to the
// clone, or any future opaque type -- revives as data and grants nothing.
// The table lives host-side under `token` (no JS array to inject into),
// and the envelope tag is written host-side at finish.
const __stubLift = (value, allowCapabilities = true, originals, collector = null) => {
  let lifted = false;
  // Capabilities (stubs, disposers) root the callee context;
  // by-value host types do not -- they pick the bare envelope.
  let caps = false;
  const seen = new __primMap();
  const token = __do_lift_begin();
  let hasDo = false;
  const ctx = __ctxNow();
  const doOnly = allowCapabilities === "do";
  const allowAll = allowCapabilities === true;
  const host = allowCapabilities === "host";
  // A buffered body crosses as its bytes; a live-stream body
  // crosses as a stream handle (a capability, so the reply
  // roots its callee context and the pulls keep working).
  const liftBody = (v) => {
    // Owner-aware: `#s` is a distinct private brand on Request and Response,
    // so a Request's body state must be read through its own class.
    const state = __bodyOwnerState(v);
    if (state.bytes !== null) return state.bytes;
    caps = true;
    return __liftStream(v.body, host, collector);
  };
  const lift = (v, path) => {
    if (v === null ||
        (typeof v !== "object" && typeof v !== "function")) return v;
    const cached = __mapGet(seen, v);
    if (cached !== undefined) {
      // The same stub at a second path needs its own host-table entry.
      if (cached === null) {
        try {
          __do_lift_record(token, v, __arrSlice(path));
        } catch (error) {
          throw new DOMException(
            String((error && error.message) || error), "DataCloneError");
        }
      }
      return cached;
    }
    // Host-branded stubs record first, before any WeakMap brand check a
    // tenant could pollute. The host reads the brand (no JS), refuses
    // what the sender could not dispatch to, and records host-side.
    if (__do_is_stub(v)) {
      if (!allowAll && !doOnly && !host) return v;
      try {
        __do_lift_record(token, v, __arrSlice(path));
      } catch (error) {
        throw new DOMException(
          String((error && error.message) || error), "DataCloneError");
      }
      lifted = true;
      if (allowAll || host) caps = true;
      hasDo = true;
      __mapSet(seen, v, null);
      return null;
    }
    const meta = __weakGet(__stubMeta, v);
    if (meta) {
      if (!allowAll && !host) return v;
      lifted = true;
      caps = true;
      if (meta.disposed) throw __stubDisposedError();
      // A stub belongs to the request that received it; another
      // request cannot serialize it (Workerd's IoContext rule).
      if (meta.ctx !== ctx) throw __ctxError("Client");
      meta.disposed = true; // the ref moves to the receiver
      __ctxUnregister(meta);
      if (collector !== null) __arrPush(collector.handles, meta);
      // A received host stub forwards its host id: the reference moves
      // and the round trip stays one hop. This isolate's hold moves with
      // it; the receiver acquires on revive.
      if (meta.entry.host !== undefined) {
        const coords = meta.entry.host;
        try {
          __stub_handoff(coords.id);
        } catch {}
        const marker = { "__cello$stub": coords.id,
                         t: __stubIsolate, h: 1,
                         scope: coords.scope, epoch: coords.epoch,
                         gen: coords.gen, c: meta.callable };
        __mapSet(seen, v, marker);
        return marker;
      }
      if (host) {
        const coords = __exportHostStub(meta.entry, "target");
        if (coords !== null) {
          const marker = { "__cello$stub": coords.id,
                           t: __stubIsolate, h: 1,
                           scope: coords.scope, epoch: coords.epoch,
                           gen: coords.generation, c: meta.callable };
          __mapSet(seen, v, marker);
          return marker;
        }
      }
      const marker = { "__cello$stub": meta.entry.id,
                       t: __stubIsolate, c: meta.callable };
      __mapSet(seen, v, marker);
      return marker;
    }
    const svc = __weakGet(__svcMeta, v);
    if (svc !== undefined) {
      if (!allowAll && !host) return v;
      // A loopback service stub (ctx.exports): name + props cross
      // as plain data and revive as a fresh loopback stub. Props
      // are lifted too -- they may nest further stubs (Workerd's
      // nested channel tokens).
      lifted = true;
      caps = true;
      const marker = { "__cello$svc": svc.name, t: __stubIsolate };
      __mapSet(seen, v, marker);
      if (svc.props !== undefined) marker.p = lift(svc.props, __arrConcat(path, "p"));
      return marker;
    }
    // Workerd refuses to serialize its promise/property handles.
    if (v instanceof __cf.RpcPromise || v instanceof __cf.RpcProperty)
      throw new DOMException(
        'Could not serialize object of type "' +
        (v instanceof __cf.RpcPromise ? "RpcPromise" : "RpcProperty") +
        '". This type does not support serialization.',
        "DataCloneError");
    if (typeof v === "function" || v instanceof __cf.RpcTarget) {
      if (!allowAll && !host) return v;
      lifted = true;
      caps = true;
      const entry = __newEntry(v);
      if (collector !== null) __arrPush(collector.entries, { entry, bridge: false });
      const callable = typeof v === "function";
      if (host) {
        const coords = __exportHostStub(entry, "target");
        if (coords !== null) {
          const marker = { "__cello$stub": coords.id,
                           t: __stubIsolate, h: 1,
                           scope: coords.scope, epoch: coords.epoch,
                           gen: coords.generation, c: callable };
          __mapSet(seen, v, marker);
          return marker;
        }
      }
      const marker = { "__cello$stub": entry.id,
                       t: __stubIsolate,
                       c: callable };
      __mapSet(seen, v, marker);
      return marker;
    }
    // HTTP host types cross by value, as in Workerd's RPC
    // serialization: entry lists for Headers, the buffered bytes
    // for bodies (the marker aliases the live buffer; the clone
    // is the one wire copy), and no signal -- the receiver mints
    // a fresh one. A live stream body cannot cross yet.
    let marker;
    if (v instanceof Headers) {
      if (!allowAll && !host) return v;
      marker = { "__cello$hdr": [...v] };
    } else if (v instanceof Blob) {
      if (!allowAll && !host) return v;
      marker = { "__cello$blob": __cello.$$blobBytes(v), y: v.type };
      if (v instanceof File) {
        marker.n = v.name;
        marker.m = v.lastModified;
      }
    } else if (v instanceof Request) {
      if (!allowAll && !host) return v;
      marker = { "__cello$req": v.url, m: v.method,
                 h: [...v.headers], r: v.redirect, c: v.cf,
                 hb: __requestState(v).hasBody === true, b: liftBody(v) };
    } else if (v instanceof Response) {
      if (!allowAll && !host) return v;
      marker = { "__cello$res": v.status,
                 t: v.statusText ||
                    __STATUS_TEXT[v.status] || "",
                 h: [...v.headers], c: v.cf,
                 e: v.type === "error",
                 b: v.body === null ? null : liftBody(v) };
    } else if (v instanceof ReadableStream ||
               v instanceof WritableStream) {
      if (!allowAll && !host) return v;
      caps = true;
      marker = __liftStream(v, host, collector);
    } else if (v instanceof AbortSignal) {
      marker = __liftSignal(v);
    }
    if (marker !== undefined) {
      lifted = true;
      __mapSet(seen, v, marker);
      return marker;
    }
    const proto = __primGetProto(v);
    if (!__primArrayIsArray(v) &&
        proto !== Object.prototype && proto !== null) {
      // A Proxy emulating neither a plain object nor an RpcTarget
      // is Workerd's canonical proxy serialization error.
      if (__util_proxy_details(v) !== undefined)
        throw new DOMException(
          "Proxy could not be serialized because it is not a " +
          "valid RPC receiver type. The Proxy must emulate either " +
          "a plain object or an RpcTarget, as indicated by the " +
          "Proxy's prototype chain.", "DataCloneError");
      return v; // host/other: leave to the clone
    }
    const out = __primArrayIsArray(v) ? [] : {};
    __mapSet(seen, v, out);
    const keys = __primObjectKeys(v);
    for (let ki = 0; ki < keys.length; ki++) {
      const key = keys[ki];
      // A user value shaped like a wire marker would revive as a live
      // handle. Refuse it on every lift path, including lift=false and
      // storage, so only harness-minted markers (from stubs the sender
      // could already dispatch to -- the lift checks) ever reach the
      // revive path. Durable Object stubs additionally record in the
      // host-side table, so even a shape that reaches the tree grants
      // nothing.
      if (__startsWith(key, "__cello$"))
        throw new DOMException(
          'Could not serialize object with reserved key "' + key + '".',
          "DataCloneError");
      out[key] = lift(v[key], __arrConcat(path, key));
    }
    const orig = originals == null ? undefined : __weakGet(originals, v);
    const disposerSource = orig ?? v;
    const disposer = disposerSource[Symbol.dispose];
    if ((allowAll || host) && !__primArrayIsArray(v) &&
        typeof disposer === "function") {
      lifted = true;
      caps = true;
      const entry =
        __newEntry(__rpcBindMethod(disposer, disposerSource));
      if (collector !== null) __arrPush(collector.entries, { entry, bridge: false });
      // A host disposer crosses as its coordinates; an old reader looks
      // a bare id up and drops a shape it cannot use (fail-closed).
      if (host) {
        const coords = __exportHostStub(entry, "disposer");
        out["__cello$disp"] = coords === null ? entry.id
          : { h: 1, id: coords.id,
              scope: coords.scope, epoch: coords.epoch,
              gen: coords.generation };
      } else {
        out["__cello$disp"] = entry.id;
      }
    }
    return out;
  };
  let tree;
  try {
    tree = lift(value, []);
  } catch (error) {
    __do_lift_abort(token);
    throw error;
  }
  if (!lifted) {
    __do_lift_abort(token);
    return null;
  }
  return { tree, caps, token, hasDo };
};
// Revive host-type markers into real instances, adopting the wire
// bytes directly -- no copy beyond the clone's own.
const __reviveBlob = (marker, bytes) => {
  const blob = marker.n !== undefined
    ? new File([], marker.n,
        { type: marker.y, lastModified: marker.m })
    : new Blob([], { type: marker.y });
  __cello.$$setBlobBytes(blob, bytes);
  return blob;
};
const __adoptBody = (target, bytes) => {
  const state = __bodyOwnerState(target);
  state.bytes = bytes;
  state.text = undefined; // decoded lazily on the first text()/json()
  // A revived Request is built with no `init.body`, so its constructor left
  // `body === null`; presence arrives separately in the marker. Install the
  // coupled stream here when the marker says the body exists (#142).
  if (state.body === null && state.hasBody === true)
    state.body = new CelloBodyStream(target);
  if (state.body !== null) {
    const streamState = __cello.$$bodyStream(state.body);
    streamState.st.bytes = bytes;
    streamState.bytes = bytes;
    __cello.$$readableState(state.body).expectedLength = bytes.byteLength;
  }
};
// A non-bytes `b` is a live-stream body marker: revive it and
// let the constructor adopt it as a streaming body. The body arrives from
// the wire, not from script, so it is exempt from the GET/HEAD body rule an
// incoming GET's body stream would otherwise trip.
const __reviveRequest = (marker, url, revive) => {
  if (!(marker.b instanceof __primUint8Array))
    return new Request(url, { method: marker.m,
      headers: marker.h, redirect: marker.r, cf: marker.c,
      [__PRIOR_BODY]: true,
      body: revive(marker.b) });
  const req = new Request(url, { method: marker.m,
    headers: marker.h, redirect: marker.r, cf: marker.c });
  // The marker carries body presence: an unframed bodyless GET or POST
  // revives with `body === null`, and a Content-Length: 0 request revives
  // with an empty stream. A fetch on the former consumes nothing (#127,
  // #142).
  const reqState = __requestState(req);
  reqState.hasBody = marker.hb === true;
  if (reqState.hasBody) __adoptBody(req, marker.b);
  return req;
};
const __reviveResponse = (marker, revive) => {
  const bytes = marker.b instanceof __primUint8Array;
  // A network-error response carries status 0, which the constructor
  // refuses; revive it at 200 and put the error status back.
  const error = marker.e === true;
  const res = new Response(
    marker.b === null ? null : bytes ? "" : revive(marker.b), {
      status: error ? 200 : marker["__cello$res"], statusText: marker.t,
      headers: marker.h, cf: marker.c });
  if (error) {
    const state = __responseState(res);
    state.status = 0;
    state.ok = false;
    state.type = "error";
  }
  if (bytes) __adoptBody(res, marker.b);
  return res;
};
// Wire a received stream handle to a local endpoint built with
// the ordinary constructors -- nothing new threads through the
// stream hot paths. Read errors surface as Workerd's generic
// premature-disconnect; write errors propagate (Workerd sends
// real errors back through the write loop).
const __foreignStreamOp = () => Promise.reject(new Error(
  "RPC streams cannot cross isolate boundaries yet."));
const __reviveStream = (marker, id, readable, handles) => {
  // A host marker revives over the stub channel, whichever isolate this
  // is -- including the minting one. Same wrappers as below, same
  // disconnect errors, one bounded op per chunk. The revive acquires
  // this isolate's hold; dispose, handoff, and context end drop it,
  // except a stream with a live reader, which the receiver holds past
  // its call. A finished stream drops its hold at once (EOF, cancel,
  // close, abort, or a failed read); a dropped one frees when collected.
  if (marker.h === 1) {
    // The sender moved its hold with the reference, so the revive
    // acquires first; a revive after its invocation ended releases
    // immediately and arrives disposed instead of registering.
    const rctx = __ctxAdopt();
    try {
      __stub_acquire(id);
    } catch {}
    const meta = {
      entry: __hostStubEntry(id, marker),
      disposed: rctx === undefined, ctx: rctx,
    };
    if (rctx === undefined) __stub_release(id).catch(() => {});
    else __ctxRegister(meta);
    handles.push(meta);
    const stream = readable
      ? new ReadableStream({
          async pull(controller) {
            let result;
            try {
              result = await __hostStubOp(meta, ["read"], []);
            } catch {
              __disposeStub(meta);
              throw __rsDisconnect();
            }
            if (result.done) {
              controller.close();
              __disposeStub(meta);
            } else controller.enqueue(result.value);
          },
          // The hold drops once the cancel lands, like close below:
          // releasing first races the owner dropping its entry, and the
          // cancel then never reaches the producer.
          cancel: () => __hostStubOp(meta, ["cancel"], [])
            .catch(() => {})
            .finally(() => __disposeStub(meta)),
        }, { highWaterMark: 0 })
      : new WritableStream({
          write: (chunk) => __hostStubOp(meta, ["write"], [chunk]),
          // The hold drops once the close or abort lands: releasing
          // first could dispose the owner's entry before the op
          // validates.
          close: () => __hostStubOp(meta, ["close"], [])
            .finally(() => __disposeStub(meta)),
          abort: () => __hostStubOp(meta, ["abort"], [])
            .catch(() => {})
            .finally(() => __disposeStub(meta)),
        });
    __rpcStreamMeta.set(stream, meta);
    if (rctx !== undefined) {
      meta.wrapper = new WeakRef(stream);
      __rpcStreamFinalizer.register(stream, meta);
    }
    return stream;
  }
  // A caller-pushed upload revives as a pull reader over the sender's
  // host channel, whichever isolate this is. Same disconnect errors,
  // one bounded op per chunk. Old readers see no `h: 1` and fail
  // closed as foreign, as before.
  if (marker.h === 2 && readable) {
    // A revive after its invocation ended cancels the channel (the
    // sender's pump stops) and arrives disposed, reading EOF. A live
    // revive records this isolate as the holder for the retirement
    // backstop, and holds the channel past its call while a reader is
    // live; a finished read drops it at once, a dropped wrapper when
    // collected.
    const rctx = __ctxAdopt();
    const meta = {
      entry: __uploadEntry(id),
      disposed: rctx === undefined, ctx: rctx,
    };
    if (rctx === undefined) {
      try {
        __upload_cancel(id);
      } catch {}
    } else {
      try {
        __upload_acquire(id);
      } catch {}
      __ctxRegister(meta);
    }
    handles.push(meta);
    const stream = new ReadableStream({
      async pull(controller) {
        let bytes;
        try {
          bytes = await __upload_pull(id);
        } catch {
          __disposeStub(meta);
          throw __rsDisconnect();
        }
        if (bytes[0] === 1) {
          controller.close();
          __disposeStub(meta);
        } else controller.enqueue(bytes.subarray(1));
      },
      cancel: () => {
        try {
          __upload_cancel(id);
        } catch {}
        __disposeStub(meta);
      },
    }, { highWaterMark: 0 });
    __rpcStreamMeta.set(stream, meta);
    if (rctx !== undefined) {
      meta.wrapper = new WeakRef(stream);
      __rpcStreamFinalizer.register(stream, meta);
    }
    return stream;
  }
  const entry = marker.t === __stubIsolate
    ? __stubEntries.get(id) : undefined;
  if (entry === undefined)
    return readable
      ? new ReadableStream({ pull: __foreignStreamOp })
      : new WritableStream({ write: __foreignStreamOp,
          close: __foreignStreamOp, abort: __foreignStreamOp });
  const meta = { entry, disposed: false, ctx: __ctxAdopt() };
  __ctxRegister(meta);
  handles.push(meta);
  const stream = readable
    ? new ReadableStream({
        async pull(controller) {
          let result;
          try {
            result = await __stubOp(meta, ["read"], []);
          } catch {
            __disposeStub(meta);
            throw __rsDisconnect();
          }
          if (result.done) {
            controller.close();
            __disposeStub(meta);
          } else controller.enqueue(result.value);
        },
        // The entry drops once the cancel lands, like close below:
        // disposing first could abort the origin before the cancel walk
        // runs.
        cancel: () => __stubOp(meta, ["cancel"], [])
          .catch(() => {})
          .finally(() => __disposeStub(meta)),
      }, { highWaterMark: 0 })
    : new WritableStream({
        write: (chunk) => __stubOp(meta, ["write"], [chunk]),
        // The entry drops once the close or abort lands: disposing first
        // could abort the origin before the walk runs.
        close: () => __stubOp(meta, ["close"], [])
          .finally(() => __disposeStub(meta)),
        abort: () => __stubOp(meta, ["abort"], [])
          .catch(() => {})
          .finally(() => __disposeStub(meta)),
      });
  __rpcStreamMeta.set(stream, meta);
  meta.wrapper = new WeakRef(stream);
  __rpcStreamFinalizer.register(stream, meta);
  return stream;
};
const __reviveSignal = (marker, id) => {
  const controller = new AbortController();
  const signal = controller.signal;
  __rpcSignalMeta.set(signal, { id });
  if (marker.a) {
    controller.abort(__rpcSignalReasonIn(marker.r));
    return controller.signal;
  }
  let receivers = __rpcSignalReceivers.get(id);
  if (receivers === undefined) {
    receivers = new Set();
    __rpcSignalReceivers.set(id, receivers);
  }
  const receiver = { id, signal: new WeakRef(signal), subscription: null };
  receivers.add(receiver);
  __rpcSignalSubscribe(receiver);
  return signal;
};
// The inverse: markers become live handles. `handles` collects the
// revived stubs (for call-end param disposal or result-tree
// disposal); `disposers` collects remote Symbol.dispose entries.
// `doTable` is the lift's side table: Durable Object stubs install only
// from listed paths before shape revival runs. A `__cello$do`-shaped
// object in the payload is inert data in every path -- including legacy
// envelopes from unpatched senders, which grant nothing.
const __stubRevive = (value, doTable) => {
  const handles = [];
  const disposers = [];
  const seen = new __primSet();
  value = __installDoTable(value, doTable);
  const revive = (v) => {
    if (v === null || typeof v !== "object") return v;
    // Side-table stubs installed above are live handles, not markers:
    // reading a marker key on one would invoke its proxy trap.
    if (__do_is_stub(v) || __weakHas(__stubMeta, v)) return v;
    // Own-property shape reads only: an inherited marker (smuggled via
    // the shared prototype) must not revive -- plain data falls through.
    const stubId =
      __primHasOwn(v, "__cello$stub") ? v["__cello$stub"] : undefined;
    if (stubId !== undefined) {
      // A host marker revives over the stub channel, whichever isolate
      // this is -- including the minting one. The revive acquires this
      // isolate's hold; dispose, handoff, and context end drop it.
      if (v.h === 1) {
        // Like the host stream revive above: acquire the moved hold, but
        // a revive after its invocation ended releases immediately and
        // arrives disposed instead of registering.
        const rctx = __ctxAdopt();
        try {
          __stub_acquire(stubId);
        } catch {}
        const stub = __makeStub(__hostStubEntry(stubId, v), v.c, rctx);
        const meta = __weakGet(__stubMeta, stub);
        if (meta) {
          if (rctx === undefined) {
            meta.disposed = true;
            __stub_release(stubId).catch(() => {});
          }
          __arrPush(handles, meta);
        }
        return stub;
      }
      const entry = v.t === __stubIsolate
        ? __stubEntries.get(stubId) : undefined;
      const stub = entry === undefined
        ? __foreignStub()
        : __makeStub(entry, v.c);
      const meta = __weakGet(__stubMeta, stub);
      if (meta) __arrPush(handles, meta);
      return stub;
    }
    const svcName =
      __primHasOwn(v, "__cello$svc") ? v["__cello$svc"] : undefined;
    if (svcName !== undefined)
      return v.t === __stubIsolate
        ? __entrypointStub(svcName, revive(v.p))
        : __foreignStub();
    // No `__cello$do` shape branch: Durable Object stubs install only
    // from the side table above. Marker-shaped payload objects fall
    // through to plain-data revival and grant nothing.
    const hdr =
      __primHasOwn(v, "__cello$hdr") ? v["__cello$hdr"] : undefined;
    if (hdr !== undefined) return new Headers(hdr);
    const blobBytes =
      __primHasOwn(v, "__cello$blob") ? v["__cello$blob"] : undefined;
    if (blobBytes !== undefined) return __reviveBlob(v, blobBytes);
    const reqUrl =
      __primHasOwn(v, "__cello$req") ? v["__cello$req"] : undefined;
    if (reqUrl !== undefined)
      return __reviveRequest(v, reqUrl, revive);
    const resMarker =
      __primHasOwn(v, "__cello$res") ? v["__cello$res"] : undefined;
    if (resMarker !== undefined)
      return __reviveResponse(v, revive);
    const rsId =
      __primHasOwn(v, "__cello$rs") ? v["__cello$rs"] : undefined;
    if (rsId !== undefined)
      return __reviveStream(v, rsId, true, handles);
    const wsId =
      __primHasOwn(v, "__cello$ws") ? v["__cello$ws"] : undefined;
    if (wsId !== undefined)
      return __reviveStream(v, wsId, false, handles);
    const sigId =
      __primHasOwn(v, "__cello$sig") ? v["__cello$sig"] : undefined;
    if (sigId !== undefined) return __reviveSignal(v, sigId);
    if (__setHas(seen, v)) return v;
    __setAdd(seen, v);
    if (__primArrayIsArray(v)) {
      for (let i = 0; i < v.length; i++) v[i] = revive(v[i]);
      return v;
    }
    const proto = __primGetProto(v);
    if (proto !== Object.prototype && proto !== null) return v;
    const keys = __primObjectKeys(v);
    for (let ki = 0; ki < keys.length; ki++) {
      const key = keys[ki];
      if (key === "__cello$disp") {
        const disp = v[key];
        // A host disposer crosses as its coordinates; invoking it is a
        // stub op with an empty path, then a release. The revive
        // acquires the hold the release drops.
        if (disp !== null && typeof disp === "object" && disp.h === 1) {
          // A post-end revive cannot own the disposer: acquire the moved
          // hold and release it immediately, so the handoff completes
          // and the owner entry frees without running anything.
          const rctx = __ctxAdopt();
          try {
            __stub_acquire(disp.id);
          } catch {}
          if (rctx === undefined) __stub_release(disp.id).catch(() => {});
          else __arrPush(disposers, __hostStubEntry(disp.id, disp));
        } else {
          const entry = __stubEntries.get(disp);
          if (entry) __arrPush(disposers, entry);
        }
        delete v[key];
        continue;
      }
      v[key] = revive(v[key]);
    }
    return v;
  };
  return { value: revive(value), handles, disposers };
};
// A marker that crossed an isolate boundary: fail on use, loudly.
const __foreignStub = () => new Proxy(function () {}, {
  get: (_b, prop) => {
    if (prop === "then" || typeof prop !== "string") return undefined;
    return () => Promise.reject(new Error(
      "RPC stubs cannot cross isolate boundaries yet."));
  },
  apply: () => Promise.reject(new Error(
    "RPC stubs cannot cross isolate boundaries yet.")),
});
// Workerd's entrypoint method-visibility rules (worker-rpc.c++):
// reserved lifecycle names are refused outright; only prototype
// methods and accessors are visible -- never own instance state
// (env/ctx live there) and never Object.prototype.
const __entrypointReserved = new Set([
  "constructor", "fetch", "connect", "alarm", "scheduled",
  "webSocketMessage", "webSocketClose", "webSocketError", "dup",
]);
const __entrypointResolve = (inst, prop) => {
  if (__entrypointReserved.has(prop))
    throw new TypeError("'" + prop +
      "' is a reserved method and cannot be called over RPC.");
  if (Object.hasOwn(inst, prop) || !(prop in inst) ||
      prop in Object.prototype)
    throw __rpcNoSuchMethod(prop);
  const value = inst[prop];
  return typeof value === "function" && !__stubMeta.has(value) &&
      !(value instanceof __cf.RpcPromise) &&
      !(value instanceof __cf.RpcProperty)
    ? __rpcBindMethod(value, inst) : value;
};
// A pipeline hop may continue only through plain data, functions,
// RpcTargets, and stubs -- never through an RPC promise/property
// handle or a class instance (Workerd's receiver rules).
const __walkable = (v) =>
  v instanceof __cf.RpcPromise || v instanceof __cf.RpcProperty
    ? false
    : typeof v === "function" || v instanceof __cf.RpcTarget ||
      (typeof v === "object" && v !== null &&
        (Object.getPrototypeOf(v) === Object.prototype ||
          Array.isArray(v)));
// Receiver-side pipeline walk: resolve `path` from `root`, then
// GET the final member (`args` null) or CALL it. Errors name the
// path walked so far, as Workerd's do. A stub mid-walk continues
// against its own target, in the stub's owning context.
const __rpcWalk = async (root, path, args, entrypointRoot) => {
  let value = root;
  for (let i = 0; i < path.length; i++) {
    const meta = __stubMeta.get(value);
    if (meta) {
      if (meta.disposed) throw __stubDisposedError();
      // A host stub mid-walk continues over the stub channel.
      if (meta.entry.host !== undefined)
        return await __hostStubOp(meta, path.slice(i), args);
      const rest = path.slice(i);
      // A mid-walk stub rooted in an ended context continues in the
      // ambient (drive) context instead: reviving under the dead id would
      // take the dead path and break a nested capability the walk returns
      // for the drive's reply lift.
      const mid = meta.entry.ctx;
      const runCtx = mid !== undefined && __ctxLive.has(mid) ? mid : __ctxNow();
      return await __ctxRun(runCtx,
        () => __rpcWalk(meta.entry.target, rest, args, false));
    }
    if (i > 0 && !__walkable(value))
      throw __rpcNoSuchMethod(path[i - 1]);
    const prop = path[i];
    const next = entrypointRoot && i === 0
      ? __entrypointResolve(value, prop)
      : __stubResolve(value, prop);
    if (i === path.length - 1) {
      if (args === null) return next;
      if (typeof next !== "function")
        throw new TypeError(
          '"' + path.join(".") + '" is not a function.');
      return next(...args);
    }
    if (next instanceof __cf.RpcPromise ||
        next instanceof __cf.RpcProperty)
      throw __rpcNoSuchMethod(prop);
    value = next;
  }
  // An empty path applies the root itself (a callable stub).
  if (args === null) return value;
  if (typeof value !== "function")
    throw new TypeError("The RPC stub is not callable.");
  return value(...args);
};
// One stub-mediated op: clone through the RPC envelope both ways,
// so nested stubs, errors, and uncloneables behave exactly as a
// dispatched call. Args serialize in the caller's context (its
// stubs must be its own to transfer); the decode, the walk, and
// the reply serialization run in the stub's owning context, so
// stubs minted by the target belong to the target's context.
// Params received by the target are disposed when the op ends
// (Workerd's param-disposal rule), except a received stream with a
// live reader, which the receiver holds past the call.
const __stubOp = (meta, path, args) => {
  if (meta.disposed) return Promise.reject(__stubDisposedError());
  const entry = meta.entry;
  // The cell that minted this stub left residency, so `entry.target`
  // belongs to an instance this node released. The stub fails here
  // rather than calling into it.
  if (entry.released) return Promise.reject(__releasedStubError());
  // A stub rooted in an aborted context is broken: reject with the
  // abort reason, before and after the dispatch (the target may
  // abort its own context mid-call). Gated on one field read.
  if (__abortedCtxs.size !== 0) {
    const aborted = __abortedCtxs.get(entry.ctx);
    if (aborted !== undefined)
      return Promise.reject(__postAbortError(aborted));
  }
  const argsSc = args === null ? null : __rpcOut(args, true);
  // An entry outlives its minting invocation, so a drive usually runs
  // under an ended context: run it under a fresh per-drive rescue
  // instead, so nested revives register again and concurrent drives
  // never dispose each other's handles at their ends. A drive under a
  // still-live entry context shares that invocation (its owner). The
  // drive ends its rescue after its reply lift; abort tracking below
  // still keys on the entry's own context, exactly as before.
  const entryCtx = entry.ctx;
  const rescue = entryCtx === undefined || !__ctxLive.has(entryCtx)
    ? __nextCtxId++ : null;
  if (rescue !== null) __ctxLive.set(rescue, 1);
  const runCtx = rescue ?? entryCtx;
  const dispatch = (async () => {
    try {
    // Workerd delivers RPC asynchronously: the callee must not run
    // before the caller's synchronous code. A call made just after
    // a not-yet-delivered ctx.abort counts as in flight (rejects
    // with the reason, not the post-abort message). Bodies queue
    // in call order, so e-order holds.
    await null;
    // The cell that minted this stub gates it like any other event.
    // The op is dispatched inside the isolate and never becomes a
    // drive, so it is the one delivery point that has to ask here
    // rather than in Rust. A stub carrying the running critical
    // section re-enters it; everything else queues, and a section
    // that failed refuses what queued.
    const cell = entry.scope;
    if (cell !== undefined) {
      const block = __cellBlocks.get(cell);
      if (block !== undefined && (entry.section === null ||
          entry.section !== block.holder))
        await __gate_wait(cell);
    }
    const reply = await __ctxRun(runCtx,
      () => __rpcRun(async () => {
        const decoded =
          argsSc === null ? null : __rpcDesArgs(argsSc);
        try {
          return await __rpcWalk(entry.target, path,
            decoded === null ? null : decoded.args, false);
        } finally {
          if (decoded !== null) __disposeReceived(decoded.received);
        }
      }, true));
    if (__abortedCtxs.size !== 0) {
      const aborted = __abortedCtxs.get(entry.ctx);
      if (aborted !== undefined)
        throw aborted instanceof __ActorAbort
          ? aborted.reason : aborted;
    }
    return __rpcDes(reply);
    } finally {
      if (rescue !== null) __ctxEnd(rescue);
    }
  })();
  if (entry.ctx === undefined) return dispatch;
  // Race the op against its hosting context's abort: a hung callee
  // never settles, so the settlement check above cannot reach it.
  // A late settlement lands on an already-settled promise (no-op).
  return new Promise((resolve, reject) => {
    const untrack = __opTrack(entry.ctx, reject);
    dispatch.then(
      (value) => { untrack(); resolve(value); },
      (error) => { untrack(); reject(error); });
  });
};
// One stub-mediated op against another isolate: the caller's half of
// `__dispatchStubOp`. Args serialize with host lift (a Durable Object
// caller mints host ids back; a Worker-owned value keeps a same-isolate
// marker and fails on use, as before); the reply decodes like any RPC
// reply, so nested capabilities revive. Races the caller's context abort
// the way `__stubOp` races its hosting context.
const __hostStubOp = (meta, path, args) => {
  if (meta.disposed) return Promise.reject(__stubDisposedError());
  const host = meta.entry.host;
  if (__abortedCtxs.size !== 0 && meta.ctx !== undefined) {
    const aborted = __abortedCtxs.get(meta.ctx);
    if (aborted !== undefined)
      return Promise.reject(__postAbortError(aborted));
  }
  const argsSc = args === null ? null : __rpcOut(args, "host");
  const dispatch = (async () =>
    __rpcDes(await __stub_call(
      host.id, host.scope, JSON.stringify(path), argsSc)))();
  if (meta.ctx === undefined) return dispatch;
  return new Promise((resolve, reject) => {
    const untrack = __opTrack(meta.ctx, reject);
    dispatch.then(
      (value) => { untrack(); resolve(value); },
      (error) => { untrack(); reject(error); });
  });
};
// Resolve a path against a local, already-revived value. A hop
// landing on a same-isolate stub delegates the rest of the path
// to the stub's target; everything else is a plain [[Get]] so
// Durable Object stubs and Proxies participate.
const __walkLocal = (parent, path, args) => {
  for (let i = 0; i < path.length; i++) {
    const meta = __stubMeta.get(parent);
    if (meta) {
      if (meta.entry.host !== undefined)
        return __hostStubOp(meta, path.slice(i), args);
      return __stubOp(meta, path.slice(i), args);
    }
    const prop = path[i];
    const member = parent == null ? undefined : parent[prop];
    if (i < path.length - 1) {
      parent = member;
      continue;
    }
    if (args === null) return member;
    if (typeof member !== "function") {
      if (member === undefined) throw __rpcNoSuchMethod(prop);
      throw new TypeError('"' + prop + '" is not a function.');
    }
    // [[Call]] directly: `.apply` on a stub proxy would be a
    // remote property access, not an invocation.
    return Reflect.apply(member, parent, args);
  }
  if (args === null) return parent;
  const meta = __stubMeta.get(parent);
  if (meta) {
    if (meta.entry.host !== undefined)
      return __hostStubOp(meta, [], args);
    return __stubOp(meta, [], args);
  }
  if (typeof parent !== "function")
    throw new TypeError("The RPC value is not callable.");
  return parent(...args);
};
// A session is one place pipeline ops resolve: the local value a
// call returned, a same-isolate stub's target, or a named
// entrypoint (whose paths resolve receiver-side in one op).
const __valueSession = (promise) => ({
  root: () => promise,
  get: (path) => promise.then((v) => __walkLocal(v, path, null)),
  call: (path, args) =>
    promise.then((v) => __walkLocal(v, path, args)),
});
const __stubSession = (meta) => meta.entry.host !== undefined
  ? {
      get: (path) => __hostStubOp(meta, path, null),
      call: (path, args) => __hostStubOp(meta, path, args),
    }
  : {
      get: (path) => __stubOp(meta, path, null),
      call: (path, args) => __stubOp(meta, path, args),
    };
const __entrypointSession = (name, local, script, makeInst, propsSc) => ({
  get: (path) => local
    ? (async () => __rpcDes(
        await __entrypointOp(name, path, null, true, makeInst)))()
    : (async () => __rpcDes(
        await __svc_rpc(
          script, name, JSON.stringify(path), null, propsSc)))(),
  call: (path, args) => (async () => {
    const argsSc = __rpcOut(args, local ? true : "do");
    if (local)
      return __rpcDes(await __entrypointOp(
        name, path, argsSc, true, makeInst));
    return __rpcDes(
      await __svc_rpc(
        script, name, JSON.stringify(path), argsSc, propsSc));
  })(),
});
// Workerd's JsRpcPromise/JsRpcProperty: awaitable, callable, and
// property access extends a path resolved at the far end, so
// intermediates obey Workerd's receiver rules. `ctx` is the node's
// owning request context -- null marks a context-free node (a
// property path rooted directly at a service binding, which
// starts a fresh session per await); a foreign context awaiting a
// property or calling through the node gets Workerd's
// cross-context error. Depth is capped like Workerd's
// MAX_PROPERTY_DEPTH.
const __makeNode = (session, path, ctx) => {
  let promise;
  const value = () => promise ??= (() => {
    if (path.length === 0) return session.root();
    if (ctx !== null && ctx !== __ctxNow())
      return Promise.reject(__ctxError("Pipeline"));
    return session.get(path);
  })();
  const brand =
    path.length === 0 ? __cf.RpcPromise : __cf.RpcProperty;
  return new Proxy(function () {}, {
    getPrototypeOf: () => brand.prototype,
    get: (_b, p) => {
      if (p === "then")
        return (onOk, onErr) => value().then(onOk, onErr);
      if (p === "catch") return (onErr) => value().catch(onErr);
      if (p === "finally")
        return (onDone) => value().finally(onDone);
      if (typeof p !== "string") return undefined;
      if (path.length >= 5120)
        throw new TypeError(
          "RPC pipelined property chain is too deep.");
      return __makeNode(session, [...path, p], ctx);
    },
    apply: (_b, _this, args) => {
      let call;
      if (ctx !== null && ctx !== __ctxNow()) {
        call = Promise.reject(__ctxError("JsRpcPromise"));
        call.catch(() => {});
      } else {
        // Eager, so concurrent calls keep e-order.
        call = session.call(path, args);
      }
      return __makeNode(
        __valueSession(call), [], ctx ?? __ctxNow());
    },
  });
};
const __makeStub = (entry, callable, rctx = __ctxAdopt()) => {
  const meta =
    { entry, callable, disposed: false, ctx: rctx };
  __ctxRegister(meta);
  const stub = new Proxy(function () {}, {
    getPrototypeOf: () => __cf.RpcStub.prototype,
    get: (_b, prop) => {
      if (prop === "then") return undefined;
      if (prop === Symbol.dispose) return () => __disposeStub(meta);
      if (prop === "dup") return () => {
        if (meta.disposed) throw __stubDisposedError();
        const dupCtx = __ctxAdopt();
        // A dup after its invocation ended is already disposed (like a
        // post-end revive): it takes no hold and no ref, and fails on
        // use. Same-isolate dups keep today's behavior.
        if (dupCtx === undefined && entry.host !== undefined) {
          const dead = __makeStub(entry, callable, dupCtx);
          __stubMeta.get(dead).disposed = true;
          return dead;
        }
        entry.refs++;
        // A dup'd host handle is a second live handle: it acquires its
        // own hold, and each dispose drops one.
        if (entry.host !== undefined) {
          try {
            __stub_acquire(entry.host.id);
          } catch {}
        }
        return __makeStub(entry, callable, dupCtx);
      };
      if (typeof prop !== "string") return undefined;
      return __makeNode(__stubSession(meta), [prop], __ctxNow());
    },
    apply: (_b, _this, args) => __makeNode(
      __valueSession(meta.entry.host !== undefined
        ? __hostStubOp(meta, [], args)
        : __stubOp(meta, [], args)), [], __ctxNow()),
  });
  __stubMeta.set(stub, meta);
  return stub;
};
// workerd's service-stub members live on `ServiceStub.prototype` (tagged
// `[object Fetcher]`, with `fetch` and `connect`), and a loopback stub gets a
// `LoopbackServiceStub` level of its own above it. One WeakMap holds each
// stub's fetch, so the member a Worker reads off the prototype is the one
// that serves every stub kind, exactly as workerd's shared method does (#259).
const __serviceStubFetch = Object.defineProperty(function () {
  const fetch = __weakGet(__serviceStubFetchers, this);
  if (fetch === undefined) throw __illegalInvocation();
  return fetch(arguments[0], arguments[1]);
}, "name", { value: "fetch", configurable: true });
const __serviceStubConnect = Object.defineProperty(function () {
  // The receiver check comes first, as a native method's does: a detached
  // `connect` is not a capability either.
  if (!__weakHas(__serviceStubFetchers, this)) throw __illegalInvocation();
  // WebIDL refuses a missing argument before the union converts, and an
  // explicit `undefined` is the dictionary's required-members refusal, both
  // as `Fetcher.connect` reports them.
  if (arguments.length === 0)
    throw new TypeError(
      "Failed to execute 'connect' on 'Fetcher': parameter 1 is not of " +
        "type 'SocketAddress or string'.");
  __socketAddress(arguments[0]);
  __socketOptions(arguments[1]);
  throw new Error(
    "connect() is not implemented in cello: a service stub has no TCP " +
      "transport");
}, "name", { value: "connect", configurable: true });
// A loopback service stub for one of this worker's own entrypoints -- the
// ctx.exports surface. Calling the stub itself returns a new stub carrying
// per-instance props, delivered to the class constructor as ctx.props
// (Workerd's ctx.exports.Name({ props })).
const __svcMeta = new WeakMap();
const __entrypointStub = (name, props) => {
  let inst;
  const makeInst = props === undefined ? undefined : () => {
    if (inst !== undefined) return inst;
    const cls = __cell.entrypoints[name];
    if (typeof cls !== "function")
      throw new TypeError(
        "The entrypoint " + name + " cannot carry props.");
    // Construction gets its own event, like a top-level invocation, and hands
    // that event's drain to the enclosing one. This runs inside the event of
    // the first method call on the stub, so a ctx.waitUntil() call in the
    // constructor drains before that call replies.
    inst = __constructEntrypoint(cls, props);
    return inst;
  };
  const session = __entrypointSession(name, true, null, makeInst);
  const bare = () => {};
  // workerd's loopback handles carry no own `name`/`length`, and the service
  // stub's tag comes from its prototype level (#259).
  delete bare.name;
  delete bare.length;
  const stub = new Proxy(bare, {
    getPrototypeOf: () => __handleProto("service"),
    get: (_b, prop) => {
      if (prop === "then") return undefined;
      if (prop === "fetch") return __serviceStubFetch;
      if (prop === "connect") return __serviceStubConnect;
      // A symbol read still belongs to the interface: the tag is the level's.
      if (typeof prop !== "string") return __handleProto("service")[prop];
      if (prop === "getRpcMethodForTestOnly")
        return (n) => __makeNode(session, [String(n)], null);
      return __makeNode(session, [prop], null);
    },
    apply: (_b, _this, args) =>
      __entrypointStub(name, args[0]?.props),
  });
  // A Fetcher's fetch() is an HTTP request, as a service binding's is.
  __weakSet(__serviceStubFetchers, stub, (input, init) => __makeServiceBinding(
    __cell.script, name === "default" ? null : name,
    props === undefined ? undefined : __rpcOut(props, false),
  ).fetch(input, init));
  __svcMeta.set(stub, { name, props });
  __outboundMeta.set(stub, {
    script: __cell.script,
    entrypoint: name,
    props,
  });
  return stub;
};
// ---- stored stubs ----------------------------------------------
// Durable Object storage accepts only stubs with durable identity:
// loopback service stubs (entrypoint name + props re-mint a fresh
// stub on read) and Durable Object stubs (HMAC'd id + class revive
// in any isolate). A transient handle -- a received RPC stub, a
// function, an RpcTarget -- dies with its isolate; persisting its
// entry id would revive garbage after a restart, so it is refused.
// A stub-bearing row is written as 0x01 + clone(marker tree), off
// the plain-clone fast path (the lift runs only after the plain
// clone already threw, exactly like the RPC envelope). Reads hand
// tagged rows to JS as [sentinel, tree]; the sentinel never leaves
// this script and no user value can decode to it, so revival
// cannot be spoofed by data shaped like a marker.
const __storedSentinel = {};
const __storedLift = (value) => {
  let lifted = false;
  const seen = new __primMap();
  const token = __do_lift_begin();
  const lift = (v, path) => {
    if (v === null ||
        (typeof v !== "object" && typeof v !== "function")) return v;
    const cached = __mapGet(seen, v);
    if (cached !== undefined) {
      if (cached === null) {
        try {
          __do_lift_record(token, v, __arrSlice(path));
        } catch (error) {
          throw new DOMException(
            String((error && error.message) || error), "DataCloneError");
        }
      }
      return cached;
    }
    // Host-branded stubs record first, before any other brand check.
    if (__do_is_stub(v)) {
      try {
        __do_lift_record(token, v, __arrSlice(path));
      } catch (error) {
        throw new DOMException(
          String((error && error.message) || error), "DataCloneError");
      }
      lifted = true;
      __mapSet(seen, v, null);
      return null;
    }
    // Ordering mirrors __stubLift: identify proxies by their
    // side tables and brands before touching any property -- a
    // stub or pipeline proxy answers every property read with a
    // fresh RpcProperty node.
    if (__weakHas(__stubMeta, v) || v instanceof __cf.RpcTarget)
      throw new DOMException(
        "Durable Object storage can only store stubs with " +
        "durable identity: service stubs from ctx.exports and " +
        "Durable Object stubs. This value is a transient RPC " +
        "handle that would not survive a restart.",
        "DataCloneError");
    const svc = __weakGet(__svcMeta, v);
    if (svc !== undefined) {
      lifted = true;
      const marker = { "__cello$svc": svc.name };
      __mapSet(seen, v, marker);
      if (svc.props !== undefined) marker.p = lift(svc.props, __arrConcat(path, "p"));
      return marker;
    }
    if (v instanceof __cf.RpcPromise ||
        v instanceof __cf.RpcProperty)
      throw new DOMException(
        'Could not serialize object of type "' +
        (v instanceof __cf.RpcPromise ? "RpcPromise" : "RpcProperty") +
        '". This type does not support serialization.',
        "DataCloneError");
    if (typeof v === "function") return v; // leave to the clone
    if (__primArrayIsArray(v)) {
      const out = [];
      __mapSet(seen, v, out);
      // Indices copy below; extras are dropped, but a forged marker in an
      // extra must still be refused, not silently kept as inert data.
      const ekeys = __primObjectKeys(v);
      for (let ei = 0; ei < ekeys.length; ei++) {
        const key = ekeys[ei];
        if (__startsWith(key, "__cello$"))
          throw new DOMException(
            'Could not serialize object with reserved key "' + key + '".',
            "DataCloneError");
      }
      for (let i = 0; i < v.length; i++) out[i] = lift(v[i], __arrConcat(path, i));
      return out;
    }
    const proto = __primGetProto(v);
    if (proto !== Object.prototype && proto !== null)
      return v; // host/other: leave to the clone
    const out = {};
    __mapSet(seen, v, out);
    const keys = __primObjectKeys(v);
    for (let ki = 0; ki < keys.length; ki++) {
      const key = keys[ki];
      // As in __stubLift: a user value shaped like a wire marker would
      // revive as a live stub on read. Refuse it while lifting.
      if (__startsWith(key, "__cello$"))
        throw new DOMException(
          'Could not serialize object with reserved key "' + key + '".',
          "DataCloneError");
      out[key] = lift(v[key], __arrConcat(path, key));
    }
    return out;
  };
  let tree;
  try {
    tree = lift(value, []);
  } catch (error) {
    __do_lift_abort(token);
    throw error;
  }
  if (!lifted) {
    __do_lift_abort(token);
    return null;
  }
  return { tree, token };
};
// Encode one stub-bearing value for storage. Rethrows the original
// clone error when nothing was liftable, so plain uncloneables
// fail exactly as they always did. Tag 0x03 carries the host-table
// `[tree, sideTable]` envelope (table and tag written host-side);
// pre-host-table 0x01/0x02 rows read back as plain data with no revive.
const __storedBytes = (value, error) => {
  const lifted = __storedLift(value);
  if (lifted === null) throw error;
  try {
    return __do_lift_finish_stored(lifted.token, lifted.tree);
  } catch (error_) {
    throw new DOMException(
      String((error_ && error_.message) || error_), "DataCloneError");
  }
};
// Test-only: write a pre-gate 0x01 envelope with an arbitrary tree, so
// the upgrade path (old rows grant nothing) is exercisable. Defined only
// on nodes booted with CELLO_TEST_WAIT=1, like the other test-only
// globals; production isolates never define it (H4). Planting 0x01 is
// safe by construction -- those rows decode as plain data.
if (__test_wait_enabled())
  globalThis.__celloPlantStoredV1ForTestOnly = (scope, key, tree) => {
    __storage_put_serialized(String(scope), String(key),
      __bytes_tagged(1, __sc_encode(tree)));
  };
const __storedRevive = (pair) => {
  const seen = new __primSet();
  let value = __primArrayIsArray(pair) ? pair[0] : pair;
  const table = __primArrayIsArray(pair) ? pair[1] : undefined;
  value = __installDoTable(value, table);
  const revive = (v) => {
    if (v === null || typeof v !== "object") return v;
    if (__do_is_stub(v)) return v;
    const svcName =
      __primHasOwn(v, "__cello$svc") ? v["__cello$svc"] : undefined;
    if (svcName !== undefined)
      return __entrypointStub(svcName, revive(v.p));
    // No `__cello$do` shape branch: stored stubs install only from the
    // side table above. Marker-shaped payload objects -- including any
    // smuggled through a class instance -- stay inert data.
    if (__setHas(seen, v)) return v;
    __setAdd(seen, v);
    if (__primArrayIsArray(v)) {
      for (let i = 0; i < v.length; i++) v[i] = revive(v[i]);
      return v;
    }
    const proto = __primGetProto(v);
    if (proto !== Object.prototype && proto !== null) return v;
    const keys = __primObjectKeys(v);
    for (let ki = 0; ki < keys.length; ki++) {
      const key = keys[ki];
      v[key] = revive(v[key]);
    }
    return v;
  };
  return revive(value);
};
const __unwrapStored = (v) =>
  __primArrayIsArray(v) && v[0] === __storedSentinel
    ? __storedRevive(v[1]) : v;
// A map result is wrapped as a whole only when it holds at least
// one tagged row, so the per-entry walk never runs on plain data.
const __unwrapStoredMap = (v) => {
  if (!__primArrayIsArray(v) || v[0] !== __storedSentinel) return v;
  const map = v[1];
  for (const [key, value] of map)
    map.set(key, __unwrapStored(value));
  return map;
};
// ctx.exports, as workerd's server builds it: a loopback stub for
// `default` and each other entrypoint, a namespace for each exported
// Durable Object class with storage, and a class, for facets, for
// each other exported one. Built once, on first access -- ctx
// construction itself only carries the getter.
let __ctxExportsCache;
const __ctxExports = () => __ctxExportsCache ??= (() => {
  const out = {};
  for (const name of Object.keys(__cell.entrypoints))
    out[name] = __entrypointStub(name, undefined);
  for (const name of Object.keys(__cell.objectEntrypoints))
    out[name] = __entrypointStub(name, undefined);
  // Loader 0 is the script's own isolate: a facet of such a class runs
  // beside the object that holds it. A class with a namespace is its loopback
  // namespace handle; one without is a loopback class handle.
  const actor = (name) => Object.hasOwn(__cell.namespaceKeys, name)
    ? __makeLoopbackNamespace(name)
    : __makeLoopbackDurableObjectClass(name, "LoopbackDurableObjectClass");
  // A runtime-supplied class is never a Worker's export, whatever registered
  // it: the host injects the exact list -- `deploy::is_reserved_class`'s four
  // fleet classes plus this script's workflow alias, which nothing here could
  // name by shape without a second copy of that rule (#259).
  const reserved = (name) => __cell.reservedClasses.includes(name);
  for (const name of Object.keys(__cell.doExports)) {
    if (reserved(name)) continue;
    out[name] = actor(name);
  }
  // Every other export is one too: workerd treats a class that inherits no
  // entrypoint base as a Durable Object class (missingSuperclass), and a
  // plain function export is a handler, both of which reach `ctx.exports`
  // (#259).
  for (const name of Object.keys(__cell.classes)) {
    if (reserved(name) || Object.hasOwn(out, name)) continue;
    // A module export name cannot begin with a dot, so the reserved `.cron`
    // cell is the runtime's, never a handle.
    if (name.startsWith(".")) continue;
    out[name] = actor(name);
  }
  // A module's `WorkflowEntrypoint` classes are the Workflows it declares, and
  // workerd keys each one by its class name, not its binding name: the entry
  // is the workflow binding (`[object WrappedBinding]`), a distinct object over
  // the same instance store `env.<binding>` reaches (#266). A workflow class
  // never reaches `__cell.classes`, so no earlier loop has claimed the name.
  for (const workflow of Object.keys(__cell.workflows))
    out[__cell.workflows[workflow]] = __makeWorkflow(workflow);
  return out;
})();
// ---- RPC envelope ----------------------------------------------
// Serialize one payload. `lift` marks a transport where stub-able
// values may cross as markers: `true` mints same-isolate markers,
// "host" mints host-routable ids for entries a cell owns (Durable
// Object RPC across isolates), "do" is the cross-isolate
// service-binding mode (DO stubs plus signals only); elsewhere they
// stay a DataCloneError, exactly as before stubs existed.
//
// Tags 1/2 carry a bare marker tree with no Durable Object stubs. Tags
// 5/6 carry `[tree, hostTable]` with the host-side table; the tag, never
// the payload shape, tells the two apart, so a legacy tree that happens
// to look like a pair cannot smuggle a table. Legacy 3/4 (JS-built
// tables) revive the tree with no table and grant nothing. Every tag is
// written host-side, so a polluted `Uint8Array.prototype.set` cannot
// flip it (H3).
const __rpcOut = (value, lift) => {
  const projected = __rpcProject(value);
  try {
    if (projected.liftable) throw new TypeError("host value");
    return __sc_encode(projected.tree);
  } catch (error) {
    // A cross-isolate call lifts only a live signal (plus DO stubs in "do"
    // mode). A same-isolate call can also transfer the existing host values
    // and JS capability handles.
    // A host lift that throws (a mint cap) or whose marker tree fails to
    // encode restores what it moved and drops what it minted: a failed
    // send keeps the sender's handles and leaks nothing on either side.
    const collector =
      lift === "host" ? { entries: [], handles: [] } : null;
    let lifted;
    try {
      lifted =
        __stubLift(projected.tree, lift, projected.originals, collector);
    } catch (liftError) {
      __unliftFailed(collector);
      throw liftError;
    }
    if (lifted === null) throw __dataCloneError(error);
    try {
      if (lifted.hasDo) {
        return __do_lift_finish_rpc(lifted.token, lifted.tree, lifted.caps);
      }
      __do_lift_abort(lifted.token);
      return __rpc_encode_bare(lifted.tree, lifted.caps);
    } catch (error_) {
      __unliftFailed(collector);
      throw __dataCloneError(error_);
    }
  }
};
// A callee exception as tagged bytes: the Error crosses by value
// (V8 serializes Error natively), custom own properties beside it. The
// tag is written host-side with the other envelope tags.
const __rpcErrOut = (error) => {
  // Workerd rethrows the callee's value: an Error subclass keeps its class,
  // a DOMException stays one, a non-Error object stays an object, and a
  // primitive becomes an Error with its text. `name` rides in the props
  // because V8 only round-trips the standard Error subclass names.
  const isDom = error instanceof Error && error instanceof __primDOMException;
  let value = error;
  let meta;
  if (isDom) {
    meta = { kind: "dom" };
  } else if (error instanceof Error) {
    meta = { kind: "error" };
  } else if (error !== null && typeof error === "object") {
    meta = { kind: "object" };
  } else {
    value = new Error(String(error));
    meta = { kind: "primitive" };
  }
  const props = value instanceof Error
    ? { ...value, name: value.name } : {};
  let sc;
  try {
    sc = __sc_encode([value, props, meta]);
  } catch {
    const error_ = new Error(String(error?.message ?? error));
    sc = __sc_encode([
      error_, { name: String(error?.name ?? "Error") }, { kind: "primitive" },
    ]);
  }
  return __bytes_tagged(0, sc);
};
// The callee half of one RPC: run `body`, answer tagged bytes.
const __rpcRun = async (body, lift) => {
  try {
    return __rpcOut(await body(), lift);
  } catch (error) {
    return __rpcErrOut(error);
  }
};
const __rpcDesArgs = (bytes) => {
  const [tag, decoded] = __rpc_decode_bytes(bytes);
  if (tag === 0xff)
    return { args: decoded, received: [] };
  if (tag === 5 || tag === 6) {
    const revived = __stubRevive(decoded[0], decoded[1]);
    return { args: revived.value, received: revived.handles };
  }
  const revived = __stubRevive(decoded);
  return { args: revived.value, received: revived.handles };
};
// The caller half: decode a reply, rebuilding stubs and rethrowing
// callee exceptions as real Errors with the callee's own
// properties, `.remote`, and a local (caller-side) stack. The tag is
// checked and sliced host-side; legacy 3/4 arrive as the tree with no
// table and grant nothing.
// A plain object or array RPC result is disposable even when it carries no
// stubs: `using result = await service.method()` must work for pure data,
// as in workerd (upstream #260). Anything else that revives as an object --
// a stub, a Response, a stream -- keeps exactly the disposal it already had,
// so only the plain-data shapes matched here gain one. Attaching at the top
// result alone is enough: nested stubs are still collected into this
// result's handles and released together.
const __isPlainRpcResult = (v) =>
  __primArrayIsArray(v) ||
  ((proto) => proto === Object.prototype || proto === null)(
    __primGetProto(v));
const __rpcDes = (bytes) => {
  const [tag, decoded] = __rpc_decode_bytes(bytes);
  if (tag === 0) {
    const [value, props, meta] = decoded;
    let thrown = value;
    if (meta !== undefined && meta.kind === "dom") {
      // V8 clones the Error base, so the class comes back from the meta and
      // the DOMException name from the props.
      thrown = new __primDOMException(
        value?.message ?? "", props?.name ?? value?.name ?? "Error");
      Object.assign(thrown, props);
    } else if (value instanceof Error) {
      Object.assign(value, props);
    }
    if (thrown !== null && typeof thrown === "object") {
      thrown.remote = true;
      if (thrown instanceof Error) {
        thrown.stack = __remoteStackLine(thrown.name, thrown.message) +
          __callerFrames();
      }
    }
    throw thrown;
  }
  {
    // A pure-data reply crosses as a bare clone with no table to revive:
    // the value is fresh, so a no-op dispose makes it `using`-compatible
    // without touching the callee's object.
    if (tag === 0xff) {
      if (decoded !== null && typeof decoded === "object" &&
          __isPlainRpcResult(decoded)) {
        __primDefineProp(decoded, Symbol.dispose, {
          configurable: true,
          value: () => {},
        });
      }
      return decoded;
    }
    const { value, handles, disposers } =
      (tag === 5 || tag === 6)
        ? __stubRevive(decoded[0], decoded[1])
        : __stubRevive(decoded);
    if (value !== null && typeof value === "object" &&
        !__weakHas(__stubMeta, value) &&
        (handles.length > 0 || disposers.length > 0 ||
          __isPlainRpcResult(value))) {
      __primDefineProp(value, Symbol.dispose, {
        configurable: true,
        value: () => {
          for (const handle of handles) __disposeStub(handle);
          for (const entry of disposers) {
            // A host disposer runs remotely, then releases; the release
            // waits for the invocation so the id stays valid for it.
            if (entry.host !== undefined) {
              __hostStubOp(
                { entry, disposed: false, ctx: undefined }, [], [])
                .catch(() => {})
                .finally(() => {
                  __stub_release(entry.host.id).catch(() => {});
                });
              continue;
            }
            __stubEntries.delete(entry.id);
            Promise.resolve().then(() => entry.target());
          }
        },
      });
    }
    return value;
  }
};
// Deprecated Fetcher `get()`/`put()`/`delete()` HTTP helpers, kept by
// the fetcher_has_get_put_delete compat flag (Workerd http.c++):
// shortcuts for fetch() with the corresponding method.
const __fetcherStatus = (res, method) => {
  if (res.status >= 200 && res.status < 300) return;
  throw new Error("HTTP " + method + " request failed: " + res.status +
    " " + (res.statusText || __STATUS_TEXT[res.status] || ""));
};
const __fetcherHelper = (fetch, prop) => {
  if (prop === "get") return async (url, type) => {
    const res = await fetch(url, { method: "GET" });
    if (res.status === 404 || res.status === 410) return null;
    __fetcherStatus(res, "GET");
    if (type === "stream")
      return res.body ?? new ReadableStream({ start(c) { c.close(); } });
    if (type === "arrayBuffer") return res.arrayBuffer();
    if (type === "json") return res.json();
    return res.text();
  };
  if (prop === "put") return async (url, body, options) => {
    const { expiration, expirationTtl } = options ?? {};
    if (expiration !== undefined || expirationTtl !== undefined) {
      const url_ = new URL(url);
      if (expiration !== undefined)
        url_.searchParams.append("expiration", expiration);
      if (expirationTtl !== undefined)
        url_.searchParams.append("expiration_ttl", expirationTtl);
      url = url_.toString();
    }
    __fetcherStatus(await fetch(url, { method: "PUT", body }), "PUT");
  };
  return async (url) => {
    __fetcherStatus(await fetch(url, { method: "DELETE" }), "DELETE");
  };
};
function makeNamespace(className) {
  const namespaceKey = __cell.namespaceKeys[className];
  if (typeof namespaceKey !== "string")
    throw new Error("no Durable Object namespace key for " + className);
  return new DurableObjectNamespace(className, namespaceKey);
}
// A Durable Object stub's interface, as workerd reflects it: a `Fetcher` with
// `fetch` and `connect`, and a `DurableObject` level above it with no member of
// its own, whose instances carry `name` and `id` as own, enumerable data
// properties (#251). A method reads its stub's routing through a private slot
// and refuses any other receiver with the interface's own message, so a
// detached `fetch` is not a capability.
let __stubRoutes;
const __stubRoute = (receiver) => {
  const route = __weakGet(__stubRoutes, receiver);
  if (route === undefined) throw __illegalInvocation();
  return route;
};
// workerd's `Fetcher.connect(address)` opens a TCP socket the cell reads and
// writes, and its argument converts as workerd's `AnySocketAddress` union
// (`api/sockets.c++` `connectImpl`) before any transport is involved: an
// object is the `SocketAddress` dictionary (a required `uint16` port, an
// optional hostname), every primitive is a `host:port` string the standard
// URL parser splits, so IPv6 works, and the address must then be a valid host.
// Cello has no such transport, so only a fully converted address reaches the
// refusal below instead of routing a cell RPC that a class with a method
// named `connect` would answer by accident (#251).
const __addressError = (message) => new TypeError(message);
const __addressNotParsed = () =>
  __addressError("Specified address could not be parsed.");
const __addressMissingHostname = () =>
  __addressError("Specified address is missing hostname.");
const __addressMissingPort = () =>
  __addressError("Specified address is missing port.");
const __addressInvalid = () =>
  __addressError(
    "Specified address is empty string, contains unsupported characters or " +
      "is too long.");
// workerd's isValidHost: 1 to 255 octets of ASCII letters, digits and the
// punctuation a hostname or an IPv6 literal may carry. Only ASCII can pass,
// so a code unit above 127 is refused without computing a byte length.
const __validHost = (host) => {
  if (host.length === 0) return false;
  for (let i = 0; i < host.length; i++) {
    const c = host.charCodeAt(i);
    if ((c >= 97 && c <= 122) || (c >= 65 && c <= 90) || (c >= 48 && c <= 57) ||
        c === 45 || c === 46 || c === 95 || c === 91 || c === 93 || c === 58)
      continue;
    return false;
  }
  return host.length <= 255;
};
// The captured constructor: a guest that replaces `globalThis.URL` must not
// choose how an address is read.
const __primURL = URL;
const __socketAddress = (address) => {
  // The union dispatches on the value's type, as JSG's does: `null`,
  // `undefined` and every object (an array, a function, a Date) are the
  // dictionary, and a primitive is the string. `undefined` is here only for
  // an explicit argument; a missing one is the caller's WebIDL refusal.
  if (address === undefined || address === null)
    throw __addressError(
      "Cannot initialize SocketAddress with required members from an " +
        "undefined or null value.");
  if (typeof address === "object" || typeof address === "function") {
    // One read per member, in workerd's order, so a getter runs once.
    const rawHost = address.hostname;
    const hostname = rawHost === undefined ? "" : __jsString(rawHost);
    const raw = address.port;
    if (typeof raw === "bigint")
      throw new TypeError("Cannot convert a BigInt value to a number");
    const number = Number(raw);
    if (!Number.isFinite(number))
      throw __addressError(
        "The value cannot be converted because it is not an integer.");
    const port = Math.trunc(number);
    if (port < 0)
      throw __addressError(
        "The value cannot be converted because it is negative and this API " +
          "expects a positive number.");
    if (port > 65535)
      throw __addressError(
        "Value out of range. Must be less than or equal to 65535.");
    const converted = hostname + ":" + port;
    if (!__validHost(converted)) throw __addressInvalid();
    return converted;
  }
  const text = __jsString(address);
  let url;
  try {
    url = new __primURL("fake://" + text);
  } catch {
    throw __addressNotParsed();
  }
  if (url.hostname === "") throw __addressMissingHostname();
  if (url.port === "") throw __addressMissingPort();
  if (!__validHost(text)) throw __addressInvalid();
  return text;
};
// workerd's `SocketOptions` conversion: a primitive is the WebIDL refusal and
// `null` or an object is the dictionary, whose unknown members are ignored.
// Members are read and converted in workerd's order (`secureTransport`,
// `allowHalfOpen`, `highWaterMark`) before the transport checks
// `secureTransport`'s three names (#251).
const __socketOptions = (options) => {
  if (options === undefined) return;
  if (options === null)
    throw new TypeError(
      "Cannot initialize SocketOptions with required members from an " +
        "undefined or null value.");
  if (typeof options !== "object" && typeof options !== "function")
    throw new TypeError(
      "Failed to execute 'connect' on 'Fetcher': parameter 2 is not of " +
        "type 'SocketOptions'.");
  const secure = options.secureTransport;
  const name = secure === undefined ? undefined : __jsString(secure);
  void options.allowHalfOpen;
  const highWaterMark = options.highWaterMark;
  if (highWaterMark !== undefined) {
    // A `uint64_t`: a BigInt must fit losslessly, and any negative number,
    // a fraction above -1 included, is refused before truncation.
    if (typeof highWaterMark === "bigint") {
      if (highWaterMark < 0n || highWaterMark >= 2n ** 64n)
        throw new TypeError(
          "The value cannot be converted because it is either negative and " +
            "this API expects a positive bigint, or the value would be " +
            "truncated.");
    } else {
      const number = Number(highWaterMark);
      if (!Number.isFinite(number))
        throw new TypeError(
          "The value cannot be converted because it is not an integer.");
      if (number < 0)
        throw new TypeError(
          "The value cannot be converted because it is negative and this " +
            "API expects a positive bigint.");
      if (number >= 2 ** 64)
        throw new TypeError(
          "Value out of range. Must be less than or equal to " +
            "18446744073709551615.");
    }
  }
  if (name !== undefined && name !== "off" && name !== "starttls" &&
      name !== "on")
    throw new TypeError(
      "Unsupported value in secureTransport socket option: " + name);
};
class Fetcher {
  static {
    // The captured constructor and primitives: a guest that pollutes
    // `WeakMap.prototype` must not see or steer the stub registry.
    __stubRoutes = new __primWeakMap();
  }
  fetch() {
    return __stubRoute(this).fetch(arguments[0], arguments[1]);
  }
  connect() {
    // The receiver check comes first, as a native method's does: a detached
    // `connect` is not a capability either.
    __stubRoute(this);
    // WebIDL refuses a missing argument before the union converts, and an
    // explicit `undefined` is the dictionary's required-members refusal.
    if (arguments.length === 0)
      throw new TypeError(
        "Failed to execute 'connect' on 'Fetcher': parameter 1 is not of " +
          "type 'SocketAddress or string'.");
    __socketAddress(arguments[0]);
    __socketOptions(arguments[1]);
    throw new Error(
      "connect() is not implemented in cello: a Durable Object stub has no " +
        "TCP transport");
  }
}
__webidlTag(Fetcher, "Fetcher");
__webidlPrototype(Fetcher);
class DurableObject extends Fetcher {}
__webidlTag(DurableObject, "DurableObject");
// A named class: SDKs sniff bindings by constructor name (workers-rs
// EnvBinding requires `constructor.name === "DurableObjectNamespace"`).
class DurableObjectNamespace {
  #className;
  #namespaceKey;
  constructor(className, namespaceKey) {
    this.#className = className;
    this.#namespaceKey = namespaceKey;
  }
  idFromName(name) {
    name = String(name);
    return new DurableObjectId(
      this.#className, __do_id(this.#namespaceKey, "name", name), name,
    );
  }
  idFromString(value) {
    return new DurableObjectId(
      this.#className, __do_id(this.#namespaceKey, "validate", String(value)),
    );
  }
  newUniqueId(options = {}) {
    const jurisdiction = options == null ? undefined : options.jurisdiction;
    if (jurisdiction != null)
      throw new Error("Jurisdiction restrictions are not implemented");
    return new DurableObjectId(
      this.#className, __do_id(this.#namespaceKey, "unique", ""),
    );
  }
  // workerd answers a fresh `[object DurableObjectNamespace]` object on every
  // call -- never the handle, never the binding -- scoped to the requested
  // jurisdiction over the same instance store, so an id minted either side
  // reads from the other. With no jurisdiction the scope is the namespace's
  // own. A named jurisdiction stays refused, as workerd refuses it locally.
  jurisdiction(value) {
    if (value == null)
      return new DurableObjectNamespace(this.#className, this.#namespaceKey);
    throw new Error("Jurisdiction restrictions are not implemented");
  }
  getByName(name, options) {
    return this.get(this.idFromName(name), options);
  }
  get(id) {
    const className = this.#className;
    if (!(id instanceof DurableObjectId) || __idClassName(id) !== className)
      throw new TypeError("Durable Object ID is not valid for this namespace");
    const scope = __idScope(id);
    // Emulate production: the actor recovers its name only when it is
    // <= 1024 UTF-8 bytes; longer names are dropped so ctx.id.name is
    // undefined. The full name still seeds the routing hash, so
    // dispatch is unchanged. Short names skip the byte count (< 256
    // chars is always <= 1020 bytes) to keep the hot path alloc-free.
    const nm = id.name;
    const dispatchName = nm === undefined ? undefined
      : nm.length < 256 || new TextEncoder().encode(nm).length <= 1024
        ? nm : undefined;
    if (dispatchName !== undefined) __cell.idNames[scope] = dispatchName;
    // Fetch and native RPC use the same host routing/activation seam.
    // Never expose `.then`: a DO stub is not itself a promise. `name` and `id`
    // are own, enumerable and read-only, as workerd's fields are (#251).
    const target = new DurableObject();
    __primDefineProp(target, "name", {
      value: dispatchName, enumerable: true, configurable: true, writable: false,
    });
    __primDefineProp(target, "id", {
      value: id, enumerable: true, configurable: true, writable: false,
    });
    const abortMarker = "__CELLO_ACTOR_ABORT__:";
    const processExitMarker = "__CELLO_PROCESS_EXIT__:";
    // The details of the abort a call observed, so every later call on this
    // handle replays the same class and message. The replay carries no
    // `remote` marker, and an RPC failure never does either -- that is what
    // workerd reports for both (#271 P2, P3).
    let brokenFailure = null;
    const invoke = async (operation) => {
      if (brokenFailure !== null) throw __cellHandlerReplay(brokenFailure);
      try {
        return await operation();
      } catch (error) {
        const routingError = __durableObjectRoutingError(error);
        if (routingError !== null) throw routingError;
        const message = String(error && error.message || error);
        const marker = [abortMarker, processExitMarker]
          .find((candidate) => message.includes(candidate));
        if (!marker) throw error;
        brokenFailure = {
          kind: "primitive",
          name: "Error",
          message: message.slice(message.indexOf(marker) + marker.length),
        };
        throw __cellHandlerReplay(brokenFailure);
      }
    };
    const doFetch = async (input, init) => {
        const adoptsBody = __fetchAdoptsBody(input, init);
        const usedBefore = adoptsBody && input.bodyUsed;
        const req = new Request(input, init);
        if (adoptsBody) __fetchTakeBody(input, usedBefore);
        const signal = __requestState(req).signalForSubrequests;
        if (signal?.aborted) throw signal.reason;
        // A body already backed by a host stream forwards to the cell as its
        // stream id, so a proxied upload to a cell on this node costs a chunk
        // rather than the whole body (#156). The host keeps it a stream for a
        // local cell and collects it only to sign a cross-node frame. A held
        // body -- or a stream with no host id -- crosses as its bytes.
        const hostStreamId = __cello.$$httpStreamId(req.body);
        const bodyStreamId =
          __requestState(req).bytes === null && hostStreamId !== undefined
            ? hostStreamId
            : undefined;
        // A null body forwards as absent, not as an empty typed array: the
        // cell's revived request must also report `body === null` (#142).
        const body_ = req.body === null
          ? undefined
          : bodyStreamId !== undefined
            ? new Uint8Array()
            : __requestState(req).bytes === null
              ? await __bodyConsume(req)
              : __requestState(req).bytes;
        if (bodyStreamId !== undefined) {
          // The cell reads this upload through the same host stream id, so the
          // Worker's wrapper must not also read it: lock it (a late read throws
          // instead of racing the cell over one host stream) and mark it used,
          // which is what awaiting the old _consume() did and what workerd
          // reports as request.bodyUsed after the forward.
          __markBodyUsed(req);
          req.body.getReader();
        }
        // Forward the request's headers to the cell as JSON. When they were
        // never materialized -- a Worker that only routes to a cell reads
        // none -- pass the raw header string straight through, so the whole
        // dispatch never builds a Headers object on either side.
        //
        // Both branches now carry the same shape. The raw string is the
        // wire list the host handed in, so the materialized branch must send
        // the header list too, not the iterator; see the note on the
        // outbound `fetch`. Reading `.headers` on a forwarded request used
        // to change what the cell received.
        const headersJson = __requestState(req).headersJson !== undefined
          ? __requestState(req).headersJson
          : JSON.stringify(__cello.$$headerList(req.headers));
        const r = JSON.parse(await invoke(() => {
          if (!signal) return __do_call(
            scope, dispatchName ?? null, req.url, req.method, body_,
            headersJson, bodyStreamId,
          );
          return __awaitCancellableDoCall(
            __do_call_cancellable(
              scope, dispatchName ?? null, req.url, req.method, body_,
              headersJson, bodyStreamId,
            ),
            signal,
          );
        }));
        // The host answers a handler exception with a marked body instead of
        // a failed transport, so the caller rejects with the handler's own
        // error (name and message) rather than a host-formatted string.
        if (r.__celloCellError !== undefined) {
          const failure = __cellHandlerThrow(r.__celloCellError);
          // An aborted object is not one that merely failed an event: workerd
          // keeps this handle broken and answers every later call with the
          // same class and message (but no `remote` marker), while a handler
          // exception leaves the next call working.
          if (r.__celloCellError.aborted === true)
            brokenFailure = r.__celloCellError;
          throw failure;
        }
        const body = __hostReplyBody(r.status, r);
        // An upgrade the cell answered with. The pair straddles two isolates,
        // so the client end cannot be the peer object itself -- it is a new
        // socket the host joins to the cell's on `accept()`. A caller that
        // returns this response instead of accepting keeps the direct route
        // `__wsTarget` already describes, and binds nothing.
        return new Response(body, {
          status: r.status,
          headers: r.headers,
          __wsTarget: r.wsTarget,
          webSocket: r.status === 101 && r.wsTarget
            ? new WebSocket(undefined, [], r.wsTarget)
            : undefined,
        });
    };
    const stub = new Proxy(target, { get: (_target, prop) => {
      if (prop === "then") return undefined;
      if (Reflect.has(_target, prop)) return Reflect.get(_target, prop);
      if (typeof prop !== "string") return undefined;
      // `JSON.stringify` asks for `toJSON` on anything it serializes, and a
      // stub answers it with the object it is (workerd's own interceptor gets
      // a serialization of `name`/`id` the same way); answering with an RPC
      // function would serialize the stub as `{}` (#251).
      if (prop === "toJSON") return undefined;
      if (__cell.compat.fetcherGetPutDelete &&
          (prop === "get" || prop === "put" || prop === "delete"))
        return __fetcherHelper(doFetch, prop);
      // Every cell RPC goes out through the host, whichever node owns the
      // target. Same-process dispatch re-enters this isolate, where the
      // abort and exit markers revive. Arguments lift with host ids where
      // a cell owns them; anything else keeps a same-isolate marker and
      // fails on use across isolates, as before.
      return async (...args) => invoke(async () => {
        const raw = await __rpc_call(
          scope, dispatchName ?? null, prop, __rpcOut(args, "host"),
        );
        // The callee answers a handler failure as marked bytes instead of a
        // result, the same JSON the fetch path sends. It rebuilds to the
        // value the handler threw with no `remote` marker, and an abort
        // breaks this handle for later calls, exactly as workerd reports
        // both (#271 P2).
        const failure = __rpcFailure(raw);
        if (failure !== undefined) {
          if (failure.aborted === true) brokenFailure = failure;
          throw __cellHandlerReplay(failure);
        }
        return __rpcDes(raw);
      });
    }});
    const routes = { fetch: doFetch };
    __weakSet(__stubRoutes, target, routes);
    __weakSet(__stubRoutes, stub, routes);
    __do_brand_stub(stub, __idClassName(id), id.toString(), dispatchName ?? null);
    return stub;
  }
}
__webidlTag(DurableObjectNamespace, "DurableObjectNamespace");
// One set of six functions, shared by a real namespace and a loopback handle
// whose own level sits directly above this one -- which is what makes
// workerd's `namespace.get === exports.Name.get` true and lets either
// receiver call either method (#259). Each wrapper resolves the receiver to
// the namespace behind it, refusing anything else with the interface's own
// message before the class's own method -- and its private slots -- is
// reached. `idFromName` first, the class body's definition order, which the
// reflection a Worker reads keeps.
for (const name of ["idFromName", "idFromString", "newUniqueId",
  "jurisdiction", "getByName", "get"]) {
  const impl = DurableObjectNamespace.prototype[name];
  const wrapper = { [name]: function () {
    const namespace = __loopbackNamespace(this);
    const result = __primReflectApply(impl, namespace, arguments);
    // A method answering its own receiver (none does today; `jurisdiction()`
    // used to) hands a loopback handle's caller the handle back rather than
    // the namespace behind it.
    return result === namespace ? this : result;
  } }[name];
  Object.defineProperty(DurableObjectNamespace.prototype, name, {
    value: wrapper, enumerable: true, configurable: true, writable: true,
  });
}
const __attachResponseRequestCancellation = (
  response,
  requestController,
  wrapBody,
) => {
  if (!(response instanceof Response) ||
      __responseState(response).bytes !== null ||
      response.body === null) {
    return;
  }
  // The bookkeeping lives in the response's private state: an own field is
  // guest-visible, and `body` is a prototype accessor now (#206).
  const state = __responseState(response);
  let requestControllers = state.requestControllers;
  if (requestControllers === undefined) {
    requestControllers = state.requestControllers = [requestController];
  } else {
    requestControllers.push(requestController);
  }
  if (!wrapBody || state.cancellationWrapped === true) return;
  const reader = state.body.getReader();
  state.body = new ReadableStream({
    async pull(controller) {
      const result = await reader.read();
      if (result.done) controller.close();
      else controller.enqueue(result.value);
    },
    async cancel() {
      const reason = new Error("The client has disconnected");
      for (const controller of requestControllers) {
        if (!controller.signal.aborted) controller.abort(reason);
      }
      try {
        await reader.cancel(reason);
      } catch {}
    },
  }, { highWaterMark: 0 });
  state.cancellationWrapped = true;
};
// called on the owner node by the /__do/<scope> endpoint: run one object.
// Returns the Response; Rust's read_response unwraps it (don't double-wrap).
// A top-level (non-actor) request whose signal can be aborted by id, so a
// disconnected HTTP or service-binding caller reaches the target's
// `request.signal`.
// A large body, or a body of unknown length, arrives as a stream id and
// not as bytes. The handler then pulls the body off the socket as it
// reads, so the whole body is never resident. `request.body` is a stream
// in both cases.
__cello.__makeIncomingRequest = (
  url, method, body, headersJson, streamId,
) => __makeRequest(
  url, method,
  streamId === undefined ? body : new CelloHttpBodyStream(streamId),
  headersJson, undefined, true);
const __dispatchTo = __cello.__dispatchTo = async (
  scope, url, method, body, headersJson, requestId = null, bodyStreamId = null,
  operatorIngress = false,
) => {
  // A routed body that must not be collected crosses as a host stream id; the
  // handler here reads it in parts. A held body arrives as its bytes.
  if (bodyStreamId !== null) body = new CelloHttpBodyStream(bodyStreamId);
  // Request already allocates a default controller for an absent signal.
  // Retain that same allocation so a streamed response can report reader
  // cancellation after the handler has returned.
  const requestController = new AbortController();
  if (requestId !== null)
    __incomingRequestSignals.set(
      String(requestId), requestController.signal);
  const actorEvent = __beginActorEvent(scope);
  // A stub received by this invocation dies with it: the response reader
  // ends the context once the body no longer needs it. A non-Response
  // answer has no body, and a throw has no answer: end those here.
  const ctxId = __ctxNext();
  try {
    const dispatch = (async () => {
      const inst = await _readyInstance(scope);
      return await __ctxRun(ctxId, () => inst.fetch(
        __makeRequest(
          url,
          method,
          body,
          headersJson,
          requestController.signal,
          true,
          operatorIngress,
        )));
    })();
    // The caller's abandonment reaches the target through
    // __do_call_cancel/__abortIncomingRequest, which aborts the fresh incoming
    // signal made above; the host dispatcher passes no caller signal here.
    const response = await dispatch;
    // #267: an event is not finished when its handler answers. The answer
    // waits for the critical sections this event opened, so a block that
    // fails after the handler returned fails the event instead of leaving a
    // caller with a response the object no longer stands behind.
    await __awaitActorSections(actorEvent);
    // Not wrapped: the host holds this response and gates it, and the caller
    // that would cancel the body reaches it through the routed channel's own
    // controller rather than this one.
    __attachResponseRequestCancellation(response, requestController, false);
    if (response instanceof Response && response.status === 101 &&
        response.webSocket && !__wsState(response.webSocket).target) {
      const responseSocketState = __wsState(response.webSocket);
      const socket = responseSocketState.peer;
      const target = { id: responseSocketState.id, scope };
      responseSocketState.target = target;
      if (socket) {
        const socketState = __wsState(socket);
        socketState.target = target;
        if (!socketState.hibernatable)
          __ws_accept_regular(socketState.id, scope);
        __sockets.set(socketState.id, socket);
        __wsHooks.flushPending(socket);
      }
    }
    if (response instanceof Response) __fetchCtxs.set(response, ctxId);
    else __ctxEnd(ctxId);
    return response;
  } catch (error) {
    __ctxEnd(ctxId);
    throw error;
  } finally {
    if (requestId !== null)
      __incomingRequestSignals.delete(String(requestId));
    __endActorEvent(actorEvent);
  }
};
// What a Worker stub may invoke on a runtime-supplied cell, by class.
// Resolving any method name here used to expose operator-only and
// internal methods (__d1Migrate, __kvSweep, ...) to every stub holder,
// so runtime classes answer an allowlist while user classes keep open
// dispatch. The methods below are exactly the ones the binding clients
// call; `fetch` travels the doFetch path, not this one, and is
// unaffected.
// - D1: the database's private query/batch and `exec`. Export and migrate are
//   operator-only and enter over `fetch` with the operator-ingress marker.
// - KV: `get`/`getWithMetadata` (single and bulk) over `__kvGet`, `put`
//   over `__kvPut`, `delete` over `__kvDelete`, and `list`
//   (cursors included) over `__kvList`. `__kvSweep` runs inside the
//   cell's own alarm and `__kvMetrics` inside the marker-gated fetch
//   `info` branch.
// - Queue: `send`/`sendBatch` over `__queueSend` and `metrics()` over
//   `__queueMetrics`. The cell's own DLQ forward rides the
//   `__queue_dlq_forward` host op, so `__queueDlqAcceptBatch` resolves
//   without this allowlist and needs no entry; `_purge`, `_peek`,
//   `_setPaused`, and `_redrive` are operator-only over marker-gated
//   `fetch`, and delivery hands off over the `__queue_dispatch` host op.
// Any other method on these classes refuses as if it did not exist.
const __D1_RPC_METHODS = new Set(["__d1Query", "__d1Batch", "__d1Exec"]);
const __KV_RPC_METHODS = new Set(["__kvGet", "__kvPut", "__kvDelete", "__kvList"]);
const __QUEUE_RPC_METHODS = new Set(["__queueSend", "__queueMetrics"]);
const __WORKFLOW_RPC_METHODS = new Set([
  "__wfCreate",
  "__wfStatus",
  "__wfDelete",
  "__wfPause",
  "__wfResume",
  "__wfRestart",
  "__wfTerminate",
  "__wfSendEvent",
]);
const __NO_RPC_METHODS = new Set();
const __rpcMethodAllowlist = (className) => {
  switch (className) {
    case "__D1Database": return __D1_RPC_METHODS;
    case "__KvNamespace": return __KV_RPC_METHODS;
    case "__Queue": return __QUEUE_RPC_METHODS;
    case ".cron": return __NO_RPC_METHODS;
    default:
      if (className.startsWith("__Workflow.")) return __WORKFLOW_RPC_METHODS;
      return null;
  }
};
const __rpcResolveInstance = async (scope) => {
  const inst = await _readyInstance(scope);
  if (!inst.__celloState._rpcOk)
    throw new TypeError(
      "The receiving Durable Object does not support RPC, because " +
      "its class was not declared with `extends DurableObject`. In " +
      "order to enable RPC, make sure your class extends the " +
      "special class `DurableObject`, which can be imported from " +
      "the module \"cloudflare:workers\".");
  return inst;
};
const __rpcTargetMethod = async (scope, method) => {
  const inst = await __rpcResolveInstance(scope);
  const className = scope.slice(0, scope.indexOf(":"));
  const allowed = __rpcMethodAllowlist(className);
  if (allowed !== null && !allowed.has(method))
    throw new TypeError(method + " is not a function");
  const fn = inst[method];
  if (typeof fn !== "function")
    throw new TypeError(method + " is not a function");
  return [inst, fn];
};
// A call a harness host op built (today only the Queue DLQ forward):
// the same resolution without the tenant allowlist. The op is a
// harness parameter a bundle cannot name, and the host op authenticated
// its caller before the call routed, so reaching here passed the host
// gate already.
const __rpcTrustedTargetMethod = async (scope, method) => {
  // Pinned at the target as well as the caller: the sole trusted caller
  // (the Queue DLQ forward host op) targets only this method, and anything
  // else refuses here, so a future trusted caller, a header confusion, or a
  // bug cannot reach operator methods over trusted RPC.
  if (!scope.startsWith("__Queue:") || method !== "__queueDlqAcceptBatch")
    throw new TypeError(method + " is not a function");
  const inst = await __rpcResolveInstance(scope);
  const fn = inst[method];
  if (typeof fn !== "function")
    throw new TypeError(method + " is not a function");
  return [inst, fn];
};
// The byte path always lifts stub-able values into the reply, with
// host-routable ids: they revive over the stub channel in any isolate
// on this node, including this one. Callee exceptions cross in
// the error envelope on every flavor.
__cello.__dispatchRpc = async (scope, method, args, trusted) => {
  const resolve = (scope, method) =>
    trusted === true
      ? __rpcTrustedTargetMethod(scope, method)
      : __rpcTargetMethod(scope, method);
  const actorEvent = __beginActorEvent(scope);
  // A stub received by this invocation dies with it: the reply lift
  // already moved what the call returned, so ending the context only
  // disposes what the handler dropped. Every release routes to the
  // owner; a quiet-but-open handle never dies on a timer. A received
  // stream with a live reader is spared instead: the reader holds the
  // stream past its invocation.
  const ctxId = __ctxNext();
  try {
    try {
      // A string is the legacy JSON flavor; bytes are V8 structured clone.
      // Answer in kind.
      if (typeof args === "string") {
        const answer = await __ctxRun(ctxId, async () => {
          const [inst, fn] = await resolve(scope, method);
          const result = await fn.apply(inst, JSON.parse(args));
          return JSON.stringify(result) ?? "null";
        });
        await __awaitActorSections(actorEvent);
        return answer;
      }
      const answer = await __ctxRun(ctxId, () => (async () => {
        const decoded = __rpcDesArgs(args);
        try {
          return await __rpcRun(async () => {
            // A broken actor (JS-flavored ctx.abort) rejects every
            // later call with the reason. One gated field read.
            if (__brokenActors.size !== 0) {
              const broken = __brokenActors.get(scope);
              if (broken !== undefined) throw broken;
            }
            const [inst, fn] = await resolve(scope, method);
            return fn.apply(inst, decoded.args);
          }, "host");
        } finally {
          __disposeReceived(decoded.received);
        }
      })());
      await __awaitActorSections(actorEvent);
      return answer;
    } finally {
      __ctxEnd(ctxId);
    }
  } finally {
    __endActorEvent(actorEvent);
  }
};
// The owner half of one cross-isolate stub op: the `__stubOp` dispatch
// body as a drive. Looks the host id up, gates on the minting cell the
// way `__stubOp` does (the drive already joined the input gate), and runs
// the walk in the entry's context. Answers envelope bytes, never throws:
// errors cross in the 0x00 tag, so the caller decodes them like any RPC
// reply.
__cello.__dispatchStubOp = async (scope, hostId, path, args) => {
  const actorEvent = __beginActorEvent(scope);
  try {
    const local = __hostStubs.get(hostId);
    const entry = local === undefined ? undefined : __stubEntries.get(local);
    // The cell that minted this stub left residency, or the caller raced
    // a release: fail here rather than calling into a foreign instance.
    if (entry === undefined || entry.released)
      return __rpcErrOut(__releasedStubError());
    if (__abortedCtxs.size !== 0) {
      const aborted = __abortedCtxs.get(entry.ctx);
      if (aborted !== undefined)
        return __rpcErrOut(__postAbortError(aborted));
    }
    // Nested revives (an op whose target calls out and drops the result)
    // land in the drive's context; the reply lift moves what the op
    // returned, so ending the op disposes the host-backed rest. Without
    // this a dropped nested handle lingers for the entry's lifetime.
    // Every drive runs under a fresh per-drive rescue (like the
    // same-isolate drive), so concurrent drives never dispose each
    // other's handles -- sharing a still-live entry context would let
    // the first drive to end dispose revives a sibling registered
    // after its snapshot. Abort tracking below still keys on the
    // entry's own context, exactly as before.
    const rescue = __nextCtxId++;
    __ctxLive.set(rescue, 1);
    const runCtx = rescue;
    const run = async () => {
      const cell = entry.scope;
      if (cell !== undefined) {
        const block = __cellBlocks.get(cell);
        if (block !== undefined && (entry.section === null ||
            entry.section !== block.holder))
          await __gate_wait(cell);
      }
      const reply = await __ctxRun(runCtx,
        () => __rpcRun(async () => {
          const decoded = args === null ? null : __rpcDesArgs(args);
          try {
            if (__brokenActors.size !== 0) {
              const broken = __brokenActors.get(scope);
              if (broken !== undefined) throw broken;
            }
            return await __rpcWalk(entry.target, path,
              decoded === null ? null : decoded.args, false);
          } finally {
            if (decoded !== null) __disposeReceived(decoded.received);
          }
        }, "host"));
      if (__abortedCtxs.size !== 0) {
        const aborted = __abortedCtxs.get(entry.ctx);
        if (aborted !== undefined)
          return __rpcErrOut(aborted instanceof __ActorAbort
            ? aborted.reason : aborted);
      }
      await __awaitActorSections(actorEvent);
      return reply;
    };
    // Race the op against its hosting context's abort, as `__stubOp`
    // does: a hung callee never settles, so the settlement check above
    // cannot reach it. A late settlement lands on an already-settled
    // promise (no-op). Abort reasons and gate failures envelope like
    // any other error, so the caller decodes them like any RPC reply.
    try {
      if (entry.ctx === undefined) return await run();
      try {
        return await new Promise((resolve, reject) => {
          const untrack = __opTrack(entry.ctx, reject);
          run().then(
            (value) => { untrack(); resolve(value); },
            (error) => { untrack(); reject(error); });
        });
      } catch (error) {
        return __rpcErrOut(error);
      }
    } finally {
      __ctxEnd(rescue);
    }
  } finally {
    __endActorEvent(actorEvent);
  }
};
// The owner half of a cross-isolate stub release: forget the host id and
// dispose the local entry. A disposer entry is deleted without running its
// target -- the caller already invoked it remotely -- mirroring the local
// disposer path, which deletes the entry and calls the target directly.
__cello.__disposeHostStub = async (scope, hostId) => {
  const actorEvent = __beginActorEvent(scope);
  try {
    const local = __hostStubs.get(hostId);
    if (local === undefined) return;
    __hostStubs.delete(hostId);
    const exported = __hostStubLocals.get(local);
    if (exported !== undefined) {
      exported.delete(hostId);
      if (exported.size === 0) __hostStubLocals.delete(local);
    }
    const entry = __stubEntries.get(local);
    if (entry === undefined || entry.released) return;
    if (entry.hostKind === "disposer") {
      if (--entry.refs <= 0) __stubEntries.delete(entry.id);
      return;
    }
    __disposeStub({ entry, disposed: false, ctx: undefined });
  } finally {
    __endActorEvent(actorEvent);
  }
};
// Test-only observability for cross-isolate stub lifetime: counts of owner
// entries, host mappings, live caller handles, sender pumps, and live
// invocation contexts held by THIS isolate. Counts only -- ids never
// surface. The global exists only
// on nodes booted with CELLO_TEST_STUB_STATS=1 (the conformance suites);
// production isolates never define it, so tenant code cannot probe
// neighbor-invocation liveness on a shared heap. The fixture tolerates
// its absence and reports { unavailable: true }.
__cello.__stubStats = () => {
  let ctxHandles = 0;
  for (const set of __ctxStubs.values()) ctxHandles += set.size;
  let ctxUploads = 0;
  for (const set of __ctxUploads.values()) ctxUploads += set.size;
  return {
    ownerEntries: __stubEntries.size,
    hostIds: __hostStubs.size,
    hostLocalEntries: __hostStubLocals.size,
    ctxHandles,
    ctxUploads,
    abortedCtxs: __abortedCtxs.size,
    liveCtxs: __ctxLive.size,
  };
};
if (__test_stub_stats_enabled())
  globalThis.__celloStubStatsForTestOnly = __cello.__stubStats;
// Test-only: a stream's private state, so a conformance fixture can drive an
// internal invariant that tenant code can no longer reach (a tee branch's
// controller closed underneath the tee, #170). Defined only on nodes booted
// with CELLO_TEST_WAIT=1, like the other test-only globals; production
// isolates never define it.
if (__test_wait_enabled())
  globalThis.__celloStreamStateForTestOnly = (stream) =>
    __cello.$$readableState(stream);
// Test-only: the host id a socket allocated. The allocator's unguessability
// is a security property a conformance fixture must still observe now that
// the id lives in the private slot (#206). Defined only with
// CELLO_TEST_WAIT=1; production isolates never define it.
if (__test_wait_enabled())
  globalThis.__celloSocketIdForTestOnly = (socket) => __wsState(socket).id;
// Test-only retained waits: `makeWait` holds one native unrefed wait
// across native-event retirements (standing in for a container exec
// wait, which needs an engine the test environment has not got), and
// `fire` resolves it. Defined only on nodes booted with
// CELLO_TEST_WAIT=1 (the cross-event wait conformance suite).
if (__test_wait_enabled())
  globalThis.__celloTestWaitForTestOnly = {
    makeWait(id) {
      return { promise: __retainedWait(() => __testwait_await(id)), id };
    },
    fire(id, value) { __testwait_fire(id, value); },
  };
// Test-only fake container exec driver: the test environment has no
// container engine, so suites attach a scope and drive the real
// `Container.prototype.exec` (validation, the already-aborted refusal,
// param building, and the process abort wiring) against the fake exec
// table beneath the ops. Defined only on nodes booted with
// CELLO_TEST_WAIT=1 (the exec-signal conformance suite).
if (__test_wait_enabled())
  globalThis.__celloTestExecForTestOnly = {
    attach(scope) { __testexec_attach(scope); },
    exec(scope, cmd, options) {
      // The real exec on a scope-only Container: the receiver inherits
      // the prototype (so `this.running` resolves) and carries only
      // `_scope`, which is all `running` and the ops read.
      const self = Object.create(Container.prototype);
      self._scope = scope;
      return self.exec(cmd, options);
    },
    ids(scope) { return JSON.parse(__testexec_ids(scope)); },
    exit(id, code) { __testexec_exit(id, code); },
    kills(id) { return JSON.parse(__testexec_kills(id)); },
    killsCompleted(id) { return JSON.parse(__testexec_kills_completed(id)); },
    closes(id) { return JSON.parse(__testexec_closes(id)); },
    closesCompleted(id) { return JSON.parse(__testexec_closes_completed(id)); },
    signal(scope, signo) {
      // The real signal on a scope-only Container, like exec above.
      const self = Object.create(Container.prototype);
      self._scope = scope;
      return self.signal(signo);
    },
    signals(scope) { return JSON.parse(__testexec_signals(scope)); },
    signalsCompleted(scope) { return JSON.parse(__testexec_signals_completed(scope)); },
    params(id) { return JSON.parse(__testexec_params(id)); },
  };
// Test-only in-memory exec processes: `spawn` mints a real process-table
// entry, `emit`/`eof` feed its pipes, and `stream` is the production
// exec output stream, so a test drives the real cross-event read path
// where no container engine exists. Defined only on nodes booted with
// CELLO_TEST_WAIT=1 (the cross-event conformance suites).
if (__test_wait_enabled())
  globalThis.__celloTestProcForTestOnly = {
    spawn() { return __testproc_spawn(); },
    emit(id, which, bytes) { __testproc_emit(id, which, bytes); },
    eof(id, which) { __testproc_eof(id, which); },
    stream(id, which) { return __execStream(id, which); },
    drop(id) { __container_exec_drop(id); },
  };
const __entrypointClass = (name) => {
  const cls = __cell.entrypoints[name];
  if (typeof cls !== "function") {
    // Workerd getExportedHandler(): distinguish a Durable Object class
    // misused as a stateless entrypoint from a name that resolves to
    // nothing. Error path only; both lookups are O(1).
    if (__cell.classes[name] !== undefined || __cell.doExports[name])
      throw new TypeError(
        `The entrypoint name ${name} refers to a Durable Object ` +
        "class, but the incoming request is trying to invoke it as " +
        "a stateless worker.");
    throw new TypeError(
      `The entrypoint name ${name} was not found in this worker. ` +
      "Ensure the worker exports an entrypoint with that name.");
  }
  return cls;
};
// End the construction event immediately: ctx.waitUntil registers into
// whichever event is current at call time, so leaving this event on the
// stack would swallow every later registration in the isolate. The drain that
// the event yields moves to the enclosing event, because a ctx.waitUntil()
// call inside a constructor is otherwise fire-and-forget: the RPC reply can
// answer and the request context can tear down before that work runs.
// __wait_until drops the drain when no event encloses this one, which is the
// isolate-startup case and matches the older behavior.
const __constructEntrypoint = (cls, props) => {
  const ctx = __beginEvent(props);
  try {
    return new cls(ctx, __cell.env);
  } finally {
    const drain = __endEvent();
    if (drain !== null) __wait_until(drain);
  }
};
// A stateless Worker invocation owns one class instance and its constructor
// context. Reusing either across invocations leaks class fields and request
// authority, while constructing more than once inside one invocation splits
// work that Cloudflare delivers to one receiver.
const __newEntrypointInstance = (name, props) =>
  __constructEntrypoint(__entrypointClass(name), props);
// `env.NAME.fetch()` where NAME is bound with `entrypoint = "..."` goes
// to that class's fetch, not the module's default export. A plain
// object export (Workerd's non-class entrypoint) dispatches its
// handler functions as fn(arg, env, ctx).
// A missing fetch reports workerd's own text, the same one the loader
// installs for a Worker with no default export, so the two paths agree.
const __noHandler = (name, method) =>
  method === "fetch" && name === "default"
    ? new Error("Handler does not export a fetch() function.")
    : new TypeError(
      `Entrypoint ${JSON.stringify(name)} has no ${method} handler`);

const __dispatchEntrypointMethod = async (name, method, arg, props) => {
  // A stub received by this invocation dies with it. A fetch answer may
  // carry a live body, so the response reader ends its context once the
  // body no longer needs it; every other method ends here, and a throw
  // ends before it propagates.
  const ctxId = __ctxNext();
  const endValue = (value) => {
    if (method === "fetch" && value instanceof Response)
      __fetchCtxs.set(value, ctxId);
    else
      __ctxEnd(ctxId);
    return value;
  };
  const handler = __cell.objectEntrypoints[name];
  if (handler !== undefined) {
    if (typeof handler[method] !== "function")
      throw __noHandler(name, method);
    const ctx = __beginEvent(props);
    try {
      return endValue(await __ctxRun(ctxId,
        () => handler[method](arg, __cell.env, ctx)));
    } catch (error) {
      __ctxEnd(ctxId);
      throw error;
    } finally {
      __endEvent();
    }
  }
  // A class entrypoint's methods get env and ctx from its constructor,
  // not as arguments.
  const inst = __newEntrypointInstance(name, props);
  if (typeof inst[method] !== "function")
    throw __noHandler(name, method);
  try {
    return endValue(await __ctxRun(ctxId, () => inst[method](arg)));
  } catch (error) {
    __ctxEnd(ctxId);
    throw error;
  }
};
// Invoke a handler inside the event frame that the host already opened. A
// Queue dispatch uses this form because its host driver must receive and keep
// driving the handler's waitUntil aggregate after the settlement is returned.
const __dispatchEntrypointMethodInCurrentEvent = async (name, method, arg) => {
  // Queue answers carry no body: a stub received by this invocation dies
  // with it, here.
  const ctxId = __ctxNext();
  try {
    const handler = __cell.objectEntrypoints[name];
    if (handler !== undefined) {
      if (typeof handler[method] !== "function")
        throw __noHandler(name, method);
      return await __ctxRun(ctxId,
        () => handler[method](arg, __cell.env, __entrypointContext()));
    }
    const inst = __newEntrypointInstance(name, undefined);
    if (typeof inst[method] !== "function")
      throw __noHandler(name, method);
    return await __ctxRun(ctxId, () => inst[method](arg));
  } finally {
    __ctxEnd(ctxId);
  }
};
const __dispatchEntrypointFetch = __cello.__dispatchEntrypointFetch =
  (name, request, propsSc = null) =>
  __dispatchEntrypointMethod(
    name, "fetch", request,
    propsSc === null || propsSc.length === 0
      ? undefined : __sc_decode(propsSc));
// The plain-object-handler entry the drive calls directly with the
// handler and its request/env/context. Establishes the invocation's
// stub context (like `__dispatchEntrypointMethod` does for named
// entrypoints) and ends it when the answer settles: a Response hands
// its context to the response reader (which ends it once the body no
// longer needs it), anything else ends here, and a throw ends before
// it propagates.
__cello.__dispatchDefaultFetch = (fetchFn, req, env, executionCtx) => {
  const ctxId = __ctxNext();
  const endValue = (value) => {
    if (value instanceof Response) __fetchCtxs.set(value, ctxId);
    else __ctxEnd(ctxId);
    return value;
  };
  let result;
  try {
    result = __ctxRun(ctxId, () => fetchFn(req, env, executionCtx));
  } catch (error) {
    __ctxEnd(ctxId);
    throw error;
  }
  if (result === null ||
      (typeof result !== "object" && typeof result !== "function") ||
      typeof result.then !== "function") {
    return endValue(result);
  }
  return result.then(
    (value) => endValue(value),
    (error) => {
      __ctxEnd(ctxId);
      throw error;
    },
  );
};
__cello.__dispatchEntrypointScheduled = (name, ctrl) =>
  __dispatchEntrypointMethod(name, "scheduled", ctrl);
// A queue batch owns mutable settlement state only while its handler runs.
// Keeping a batch alive after dispatch must not keep authority to settle its
// lease, because that lease can have expired and been handed out again.
__cello.__dispatchEntrypointQueue = async (name, incoming) => {
  let active = true;
  let ackAll = false;
  const explicitAcks = new Set();
  const retries = new Map();
  const retryBatch = { retry: false, delaySeconds: undefined };
  const checkActive = () => {
    if (!active)
      throw new Error("Queue event methods cannot be called after the handler returns.");
  };
  const retryDelay = (options) => {
    const value = options?.delaySeconds;
    if (value === undefined) return undefined;
    if (!Number.isInteger(value) || value < -0x8000_0000 || value > 0x7fff_ffff)
      throw new TypeError("delaySeconds must be a 32-bit integer");
    return value;
  };
  const decode = (message) => {
    switch (message.contentType) {
      case "text": return new TextDecoder().decode(message.body);
      case "bytes": return message.body;
      case "json": return JSON.parse(new TextDecoder().decode(message.body));
      case "v8": return __sc_decode(message.body);
      default: throw new TypeError(`Unknown queue content type: ${message.contentType}`);
    }
  };
  // Workerd's QueueMessage/QueueBatch (queue.c++): the settle methods live on
  // the prototype, so Object.keys, spread, JSON.stringify and
  // structuredClone of a batch or message see only the data fields. The
  // mutable settlement state stays in the closure, keyed by private fields
  // that never appear as own properties.
  class QueueMessage {
    #id;
    constructor(message) {
      this.id = message.id;
      this.timestamp = new Date(message.timestamp);
      this.body = decode(message);
      this.attempts = message.attempts;
      this.#id = message.id;
    }
    ack() {
      checkActive();
      if (ackAll || retryBatch.retry || retries.has(this.#id)) return;
      explicitAcks.add(this.#id);
    }
    retry(options) {
      checkActive();
      if (ackAll || explicitAcks.has(this.#id)) return;
      const delaySeconds = retryDelay(options);
      if (delaySeconds !== undefined || !retries.has(this.#id))
        retries.set(this.#id, delaySeconds);
    }
  }
  const messages = incoming.messages.map((message) => new QueueMessage(message));
  const oldest = incoming.metrics.oldestMessageTimestamp;
  class QueueBatch {
    constructor() {
      this.queue = incoming.queue;
      this.messages = messages;
      this.metadata = {
        metrics: {
          backlogCount: incoming.metrics.backlogCount,
          backlogBytes: incoming.metrics.backlogBytes,
          oldestMessageTimestamp: oldest === undefined ? undefined : new Date(oldest),
        },
      };
    }
    ackAll() {
      checkActive();
      if (!retryBatch.retry) ackAll = true;
    }
    retryAll(options) {
      checkActive();
      if (ackAll) return;
      retryBatch.retry = true;
      const delaySeconds = retryDelay(options);
      if (delaySeconds !== undefined) retryBatch.delaySeconds = delaySeconds;
    }
  }
  const batch = new QueueBatch();
  let outcome = "ok";
  let errorText;
  try {
    await __dispatchEntrypointMethodInCurrentEvent(name, "queue", batch);
  } catch (error) {
    // A handler exception retries the batch but keeps explicit acks that the
    // handler made before it threw. The caller needs both facts together.
    outcome = "exception";
    try {
      errorText = String(error);
    } catch {
      errorText = "queue handler rejected";
    }
  } finally {
    active = false;
  }
  return {
    outcome,
    error: errorText,
    ackAll,
    retryBatch,
    explicitAcks: [...explicitAcks],
    retryMessages: [...retries].map(([msgId, delaySeconds]) => ({
      msgId,
      delaySeconds,
    })),
  };
};
// Workerd's simple-handler RPC rules (worker-rpc.c++): a non-class
// handler method is called as fn(arg, env, ctx), the client must send
// exactly one argument, and the handler must not declare more than
// (arg, env, ctx). The messages are Workerd's, verbatim.
const __callObjectEntrypoint = (handler, method, args, props) => {
  const fn = handler[method];
  if (typeof fn !== "function")
    throw new TypeError(
      'The RPC receiver does not implement the method "' + method +
      '".');
  if (fn.length > 3)
    throw new TypeError(
      'Cannot call handler function "' + method + '" over RPC ' +
      "because it has the wrong number of arguments. A simple " +
      "function handler can only be called over RPC if it has " +
      "exactly the arguments (arg, env, ctx), where only the first " +
      "argument comes from the client. To support multi-argument " +
      "RPC functions, use class-based syntax (extending " +
      "WorkerEntrypoint) instead.");
  if (args.length !== 1)
    throw new TypeError(
      'Attempted to call RPC function "' + method + '" with the ' +
      "wrong number of arguments. When calling a top-level handler " +
      "function that is not declared as part of a class, you must " +
      "always send exactly one argument. In order to support " +
      "variable numbers of arguments, the server must use " +
      "class-based syntax (extending WorkerEntrypoint) instead.");
  return fn.call(handler, args[0], __cell.env, __entrypointContext(props));
};
let __nextPendingEventId = 1;
const __pendingEvents = new Map();
const __pendingEventError = () => new Error(
  "The Workers runtime canceled this request because it detected that " +
  "your Worker's code had hung and would never generate a response. " +
  "Refer to: https://developers.cloudflare.com/workers/observability/errors/");
// The host calls this only when the request has no native operation that can
// resume JavaScript. Reject the entrypoint call itself, so its caller can
// catch the cancellation and continue the enclosing event.
__cello.__cancelPendingEvents = (ids) => {
  for (const id of ids) __pendingEvents.get(id)?.(__pendingEventError());
};
// One entrypoint op (a call, or a property GET when argsSc is
// null), inside a fresh request context -- the callee owns stubs
// revived from its params, and stubs it mints belong to it.
// `props` is the caller's per-stub props (Workerd's
// getEntrypoint(name, { props })); undefined leaves the entrypoint's
// default empty props.
const __entrypointOp = (name, path, argsSc, local, makeInst, props) => {
  const id = __ctxNext();
  return __ctxRun(id, () => {
  const pendingId = __nextPendingEventId++;
  let cancel;
  const cancellation = new Promise((_, reject) => { cancel = reject; });
  __pendingEvents.set(pendingId, cancel);
  __pending_event_begin(pendingId);
  const running = (async () => {
  // Decoded inside the try: a corrupt-args throw rejects the call after
  // the outer finally ends the context (below), instead of escaping
  // with the fresh id still live plus any partial revives.
  let decoded = null;
  let drain = null;
  try {
    decoded = argsSc === null ? null : __rpcDesArgs(argsSc);
    const reply = await __rpcRun(async () => {
      // The handler's synchronous part runs inside its own event so
      // ctx.waitUntil and the imported waitUntil have a target. The
      // event pops before the first await -- the event stack is
      // strictly LIFO and an event held across an await would be
      // popped by whichever event settles next -- and its registered
      // work drains before a plain reply (below).
      __beginEvent();
      let result;
      try {
        const handler = __cell.objectEntrypoints[name];
        if (handler !== undefined && makeInst === undefined) {
          // Simple-handler entrypoints expose single-method calls
          // only; property GETs and deeper paths are refused.
          if (argsSc === null || path.length !== 1)
            throw __rpcNoSuchMethod(path[0]);
          result = __callObjectEntrypoint(
            handler, path[0], decoded.args, props);
        } else {
          // A loopback stub with props retains one instance for that RPC
          // session. Each ordinary top-level operation constructs once here,
          // so neither its fields nor its constructor context reach the next
          // invocation.
          const inst = makeInst !== undefined ? makeInst()
            : __newEntrypointInstance(name, props);
          result = __rpcWalk(inst, path,
            decoded === null ? null : decoded.args, true);
        }
      } finally {
        drain = __endEvent();
      }
      return await result;
    }, local ? true : "do");
    // The call ended: tear its context down in a finally, so a drain
    // or abort throw below cannot skip it and leak a live context. A
    // service callee that called out and dropped the result holds host
    // handles here; the reply lift already moved what it returned, so
    // what remains is dropped: dispose it. Same-isolate handles keep
    // their lifetime below. Returned capabilities survive through their
    // entries' refs (the reply lift already moved them out), the way a
    // DO RPC callee's do: that path ends its context unconditionally
    // too. An upload pump the callee started for a nested call's stream
    // argument stops here; the nested call already replied, so its
    // reader drained or disposed.
    try {
      __disposeNewHostHandles(id, null);
      // Registered work drains before a plain reply. A
      // capability-bearing reply (tags 1 and 5) must not wait: a returned
      // stream's chunks may be produced by that very work, which
      // cannot finish until the caller pulls (returnReadableStream's
      // waitUntil writer would deadlock behind its own reply).
      if (reply[0] !== 1 && reply[0] !== 5 && drain !== null) await drain;
      // ctx.abort() during the call supersedes its result; the raw
      // reason rejects the caller (same isolate -- identity holds).
      if (__abortedCtxs.size !== 0) {
        const reason = __abortedCtxs.get(id);
        if (reason !== undefined) throw reason;
      }
    } finally {
      __ctxEnd(id);
    }
    return reply;
  } finally {
    // Idempotent with the inner end above: a decode throw (or any future
    // early throw) still tears the context down, disposing partial
    // revives with it.
    __ctxEnd(id);
    if (decoded !== null) __disposeReceived(decoded.received);
  }
  })();
  return Promise.race([running, cancellation]).finally(() => {
    __pendingEvents.delete(pendingId);
    __pending_event_end(pendingId);
  });
  });
};
// Cross-isolate service callers pass a complete property path. Older host
// callers can still pass one method name, which becomes a one-part path.
// `propsSc` carries getEntrypoint(name, {props}) as structured-clone bytes,
// the same encoding as `argsSc`. It is empty when the caller sent no props,
// which no encoded value can be, so an empty buffer is an unambiguous absent.
__cello.__dispatchEntrypointRpc =
  (name, path, argsSc, local = false, propsSc = null) => __entrypointOp(
    name, typeof path === "string" ? [path] : path, argsSc, local,
    undefined,
    propsSc === null || propsSc.length === 0
      ? undefined : __sc_decode(propsSc));
// WebSocket: the host holds the socket; these deliver events into the DO.
// `ws` is a lightweight stub whose send/close route back to the host task
// by wsId -- so the isolate can be hibernated between messages.
// A hibernatable socket this isolate does not hold: a real WebSocket bound
// to the id, accepted and targeted at the host queue, so send/close and the
// shape are the interface's own (no own `_hibernatable` marker, #206).
const __wsStub = __cello.__wsStub = (wsId) => {
  const socket = new WebSocket(Number(wsId));
  const state = __wsState(socket);
  state.accepted = true;
  state.target = { id: state.id, scope: "" };
  state.hibernatable = true;
  return socket;
};
__cello.__wsOpen = async (scope, wsId, protocol) => {
  const actorEvent = __beginActorEvent(scope);
  // Socket events are invocations too: a stub the handler revives dies
  // with the event. Without a context the handle would register
  // nowhere and its release would never route.
  const ctxId = __ctxNext();
  try {
    const inst = await _readyInstance(scope);
    const socket = __stateSocket(inst.__celloState, wsId);
    if (socket.readyState !== WebSocket.READY_STATE_CONNECTING) return;
    await __ctxRun(ctxId, async () => {
      // Both are prototype accessors now, so the host writes the private
      // socket state (#206).
      const socketState = __wsState(socket);
      socketState.protocol = protocol;
      socketState.readyState = WebSocket.READY_STATE_OPEN;
      socket.dispatchEvent(new Event("open"));
    });
  } finally {
    __ctxEnd(ctxId);
    __endActorEvent(actorEvent);
  }
};
__cello.__wsMessage = async (scope, wsId, msg) => {
  const actorEvent = __beginActorEvent(scope);
  const ctxId = __ctxNext();
  try {
    const inst = await _readyInstance(scope);
    const socket = __stateSocket(inst.__celloState, wsId);
    await __ctxRun(ctxId, async () => {
      if (!__wsState(socket).hibernatable && socket instanceof WebSocket)
        __wsHooks.dispatchMessage(socket, msg);
      else if (typeof inst.webSocketMessage === "function")
        await inst.webSocketMessage(socket, msg);
    });
  } finally {
    __ctxEnd(ctxId);
    __endActorEvent(actorEvent);
  }
};
__cello.__wsBinary = async (scope, wsId, data) => {
  const actorEvent = __beginActorEvent(scope);
  const ctxId = __ctxNext();
  try {
    const inst = await _readyInstance(scope);
    const socket = __stateSocket(inst.__celloState, wsId);
    const bytes = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
    await __ctxRun(ctxId, async () => {
      if (!__wsState(socket).hibernatable && socket instanceof WebSocket)
        __wsHooks.dispatchMessage(socket, bytes);
      else if (typeof inst.webSocketMessage === "function")
        await inst.webSocketMessage(socket, bytes);
    });
  } finally {
    __ctxEnd(ctxId);
    __endActorEvent(actorEvent);
  }
};
__cello.__wsClosed = async (scope, wsId, code, reason, wasClean) => {
  const actorEvent = __beginActorEvent(scope);
  const ctxId = __ctxNext();
  try {
    const inst = await _readyInstance(scope);
    const socket = __stateSocket(inst.__celloState, wsId);
    await __ctxRun(ctxId, async () => {
      if (!__wsState(socket).hibernatable && socket instanceof WebSocket)
        __wsHooks.dispatchClose(socket, code, reason, wasClean);
      else if (!wasClean && typeof inst.webSocketError === "function")
        // A hibernatable socket reports an abnormal closure through
        // webSocketError, which cello listed as a handler name and never
        // called.
        await inst.webSocketError(
          socket,
          new Error(reason ? `WebSocket closed abnormally: ${reason}` : "WebSocket closed abnormally"),
        );
      else if (typeof inst.webSocketClose === "function")
        await inst.webSocketClose(socket, code, reason, wasClean);
    });
  } finally {
    __ctxEnd(ctxId);
    __endActorEvent(actorEvent);
  }
};
// called by cello's scheduler when an alarm is due. Returns a promise.
__cello.__fireAlarm = async (scope, scheduledTime, retryCount, isRedrive) => {
  const actorEvent = __beginActorEvent(scope);
  // An alarm is an invocation: a stub the handler revives dies with it.
  const ctxId = __ctxNext();
  try {
    const inst = await _readyInstance(scope);
    if (typeof inst.alarm !== "function") return;
    await __ctxRun(ctxId, () =>
      inst.alarm({ scheduledTime, retryCount, isRetry: retryCount > 0, isRedrive: isRedrive === true }));
  } finally {
    __ctxEnd(ctxId);
    __endActorEvent(actorEvent);
  }
};
// The reserved cron cell. It is not a user class: cello registers it under
// `.cron` for every deployment that declares `triggers.crons`, and one cell
// per script carries that script's whole schedule. Ownership CAS on that one
// name is what makes a cron trigger fire once per fleet rather than once per
// node -- the same arbiter an alarm already relies on, with nothing added.
//
// The schedule itself is never stored here. `__cell.crons` comes from the
// deployment the cell is running under, so changing a cron expression takes
// effect on the next activation and needs no migration of an armed alarm.
class CelloCronSchedule {
  constructor(state) {
    this._state = state;
    // Failures within one occurrence. Held in memory on purpose: losing the
    // count to an eviction only makes the next retry sooner, and every retry
    // is capped at the next occurrence anyway, so it need not be durable.
    this._retry = 0;
    // What a pending retry owes: the occurrence, the expressions of it that
    // failed, and the deadline the retry was armed for. A retry deadline is a
    // backoff instant and not an occurrence, so nothing about it can be
    // recovered from the deadline itself -- without this record the wake
    // matches no expression, runs nothing, and drops the failed tick in
    // silence. Held in memory beside `_retry` and for the same reason: an
    // eviction costs the retry, never the next occurrence.
    this._owed = null;
  }
  // Cello calls this once per node after a deployment loads, because a cron
  // cell has no client to wake it. Arming is idempotent -- the same schedule
  // and the same clock give the same answer -- so it does not matter which
  // node wins the cell.
  async fetch() {
    // A pending retry is a deadline this cell already owes, and re-arming
    // would move it to the next occurrence and lose the tick it owes with it.
    if (this._owed !== null) return new Response(null, { status: 204 });
    const armed = await this._state.storage.getAlarm();
    // An alarm already due is a tick that is late, not one that is wrong.
    // Leaving it alone is what makes a fleet that was down fire the missed
    // occurrence once, instead of skipping it by re-arming into the future.
    if (armed === null || armed > Date.now()) await this._arm(null, [], false);
    return new Response(null, { status: 204 });
  }
  async alarm(info) {
    const crons = __cell.crons || [];
    // A retry wake carries the backoff instant as its deadline, so what to run
    // and what time to report both come from the record the failure left. Only
    // a deadline that is not a retry is an occurrence to match against.
    const owed = this._owed !== null && this._owed.armedFor === info.scheduledTime
      ? this._owed
      : null;
    const occurrence = owed === null ? info.scheduledTime : owed.at;
    // One deadline can belong to several expressions, and each is its own
    // invocation with its own controller.cron, as on Cloudflare. A retry
    // repeats only the expressions that failed, because the ones that
    // succeeded already ran for this occurrence.
    const due = owed === null
      ? __cron_plan(crons, info.scheduledTime, Date.now(), 0, false).matching
      : owed.indices;
    const failed = [];
    for (const index of due) {
      let noRetry = false;
      const controller = {
        // The occurrence, never the instant this attempt started, so a late
        // run and a retry both report the minute they were scheduled for.
        scheduledTime: occurrence,
        cron: crons[index],
        noRetry() { noRetry = true; },
      };
      const ctx = __beginEvent();
      try {
        if (typeof __cell.selfScheduled !== "function")
          throw new Error(
            "a cron trigger needs the Worker to export a `scheduled` handler");
        await __cell.selfScheduled(controller, __cell.env, ctx);
      } catch (error) {
        // Swallowed on purpose: this handler owns the re-arm below, so
        // throwing would hand the deadline to the generic alarm retry and
        // lose the next occurrence with it.
        console.error(
          `scheduled handler for cron ${JSON.stringify(crons[index])} failed:`,
          error);
        if (!noRetry) failed.push(index);
      } finally {
        const drain = __endEvent();
        // The alarm turn is the only host owner for this scheduled event.
        // Dropping its drain lets the actor turn end with waitUntil work still
        // pending, so an I/O promise can remain unresolved forever.
        if (drain !== null) await drain;
      }
    }
    await this._arm(occurrence, failed, owed !== null);
  }
  // `occurrence` is the tick just handled, or null when the cell is only
  // arming, `failed` holds the expressions of it still owed, and `retried`
  // says the deadline just handled was a backoff rather than an occurrence.
  async _arm(occurrence, failed, retried) {
    const crons = __cell.crons || [];
    // The count is failures within one occurrence, so a fresh occurrence
    // starts at one however the last one ended. Carrying it across would
    // spend `alarm_retry`'s ceiling once and leave every later occurrence
    // with no retry at all, which is a budget for the schedule and not for
    // the tick.
    this._retry = failed.length ? (retried ? this._retry + 1 : 1) : 0;
    // A negative first argument means "arming only, nothing fired".
    const plan = __cron_plan(crons, -1, Date.now(), this._retry, failed.length > 0);
    // The next occurrence beats the backoff whenever it is sooner, and it then
    // cancels the retry rather than queueing behind it, so the record the
    // retry needed goes when the retry does.
    this._owed = plan.armIsRetry
      ? { at: occurrence, indices: failed, armedFor: plan.armAt }
      : null;
    // No expression matches again and nothing is owed: the deployment dropped
    // its crons, so the cell retires rather than waking forever.
    if (plan.armAt === null) await this._state.storage.deleteAlarm();
    else await this._state.storage.setAlarm(plan.armAt);
  }
}
const __cell = __cello.__cell = {
  entrypoints: {},
  objectEntrypoints: {},
  doExports: {},
  classes: { ".cron": CelloCronSchedule },
  crons: [],
  workflows: {},
  containers: {},
  instances: {},
  facetConfigs: {},
  env: {},
  idNames: {},
  namespaceKeys: {},
  node: "",
  deleteAllDeletesAlarm: false,
  compat: { jsRpc: false, fetcherGetPutDelete: false, queueJsonMessages: false, nodejsProcessEnv: false },
  makeNamespace,
  release: __cellRelease,
};
// A class the runtime supplies. RPC on a stub needs `extends DurableObject`
// or the js_rpc flag; the class is the runtime's own, so grant it here rather
// than make every user set a flag. It is not the application's export, so it
// stays out of ctx.exports -- which asks the host's reserved list rather than
// keeping a second copy of it (#259).
const __runtimeClass = (name, cls) => {
  __cell.classes[name] = cls;
  __cell.doExports[name] = true;
};
// ---- KV ------------------------------------------------------------------
// The namespace cell and the binding's client are `services/kv.js`, which the
// workerd engine runs too; this is the host they run against here.
const {
  KvNamespaceCell: __KvNamespaceCell,
  KvNamespace: __KvNamespace,
  cellSweepBatch: __cellSweepBatch,
} = kvService({
  // RPC on a runtime class is granted by `__runtimeClass` below.
  DurableObject: class {
    constructor(ctx) {
      this.ctx = ctx;
    }
  },
  limits: () => __cell.kvLimits,
  // One op for the whole large-value path, following `__d1_run`.
  blob: async (_cell, request, value) => {
    const reply = await __kv_blob(JSON.stringify(request), value);
    return reply instanceof Uint8Array
      ? { found: true, value: reply }
      : JSON.parse(reply);
  },
  // The raw base64 ops accept typed views for this internal wire form; the
  // public atob() and btoa() wrappers keep their standard string-only
  // behaviour.
  btoa: (bytes) => $$btoa(bytes),
  atob: (text) => $$atob(text, true),
});

__runtimeClass("__KvNamespace", __KvNamespaceCell);

const __makeKvNamespace = __cello.__makeKvNamespace = (id, cellName) =>
  new __KvNamespace(() => __cell.makeNamespace("__KvNamespace"), cellName);

// ---- Queues --------------------------------------------------------------
// A Queue is one runtime-supplied Durable Object. The producer binding writes
// bytes and a content tag through RPC, and the cell owns the durable order,
// deadlines, leases, and alarms. `cello_logic::queue` owns every transition
// that can invalidate a lease or choose a deadline; this code supplies SQL and
// the Cloudflare-compatible presentation.

const __QUEUE_MESSAGES = "__queue_messages";
const __QUEUE_META = "__queue_meta";
const __QUEUE_STATS = "__queue_stats";
const __QUEUE_TRANSFER_RECEIPTS = "__queue_transfer_receipts";
const __queueEncoder = new TextEncoder();

// Independent send() calls share the same economics as sendBatch() without
// changing the public promise boundary. The first call opens a four-millisecond
// owner-local group by default; later cell events append their already-copied
// entries, and park on their own promise. One event commits the bounded group,
// and every response still trails that commit through the ordinary output
// gate. The three caps bound heap retention and keep the persisted transaction
// inside the public sendBatch limits.
const __QUEUE_PRODUCER_GROUP_CALLS = 64;
const __QUEUE_PRODUCER_GROUP_MESSAGES = 100;
const __QUEUE_PRODUCER_GROUP_BYTES = 256_000;

const __queueError = (message) => new Error("QUEUE_ERROR: " + String(message));
// A Queue message id is a UUIDv7 (RFC 9562): 48 bits of enqueue time, a
// 12-bit sequence inside that millisecond, and 62 random bits. It keeps the
// UUID shape a consumer can parse, and it sorts in enqueue order, so the
// UNIQUE index on `id` appends. A random v4 id made every grouped send
// transaction dirty one index leaf per message (about 1.6 WAL frames per
// 64-byte message) and turned every passive checkpoint into a random write
// across the whole backlog, which is what made a deep Queue's checkpoint
// cost grow with its backlog. The sequence never moves backwards inside one
// process, so a clock step only widens the gap between consecutive ids.
let __queueIdMs = 0;
let __queueIdSeq = 0;
function __queueMessageId(now) {
  if (now > __queueIdMs) {
    __queueIdMs = now;
    __queueIdSeq = 0;
  } else {
    __queueIdSeq += 1;
    if (__queueIdSeq > 0xfff) {
      __queueIdMs += 1;
      __queueIdSeq = 0;
    }
  }
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  const ms = __queueIdMs;
  bytes[0] = Math.floor(ms / 0x10000000000) & 0xff;
  bytes[1] = Math.floor(ms / 0x100000000) & 0xff;
  bytes[2] = (ms >>> 24) & 0xff;
  bytes[3] = (ms >>> 16) & 0xff;
  bytes[4] = (ms >>> 8) & 0xff;
  bytes[5] = ms & 0xff;
  bytes[6] = 0x70 | (__queueIdSeq >> 8);
  bytes[7] = __queueIdSeq & 0xff;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  let hex = "";
  for (let i = 0; i < 16; i++) hex += bytes[i].toString(16).padStart(2, "0");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${
    hex.slice(16, 20)
  }-${hex.slice(20)}`;
}
const __queueLimits = () => __cell.queueLimits;
const __queuePolicy = (request) =>
  JSON.parse(__queue_policy(JSON.stringify(request)));

const __queueDelay = (value, fallback = 0) => {
  if (value === undefined || value === null) return fallback;
  const seconds = Number(value);
  if (!Number.isInteger(seconds) || seconds < 0 ||
      seconds > __queueLimits().maxDelaySeconds) {
    throw __queueError(
      `delaySeconds must be an integer from 0 to ${__queueLimits().maxDelaySeconds}`,
    );
  }
  return seconds;
};

const __queueBytes = (value) => {
  const view = __hostChunk(
    value, "a bytes message must be an ArrayBuffer or a typed array");
  if (view === null)
    throw __queueError("a bytes message must be an ArrayBuffer or a typed array");
  // Copy: a caller must not be able to resize or reuse the backing buffer
  // while the durable write awaits.
  return new Uint8Array(view);
};

// Encode before entering the cell. The copy makes `sendBatch()` immune to a
// caller resizing or reusing a backing buffer while the durable write awaits.
const __queueEncode = (body, requestedType) => {
  const contentType = requestedType ??
    (__cell.compat.queueJsonMessages ? "json" : "v8");
  let bytes;
  switch (contentType) {
    case "text":
      bytes = __queueEncoder.encode(String(body));
      break;
    case "bytes":
      bytes = __queueBytes(body);
      break;
    case "json": {
      let json;
      try {
        json = JSON.stringify(body);
      } catch (error) {
        throw __queueError("the JSON message body cannot be serialized", error);
      }
      if (json === undefined) {
        throw __queueError("the JSON message body cannot be undefined");
      }
      bytes = __queueEncoder.encode(json);
      break;
    }
    case "v8":
      try {
        bytes = __scEncodeStrict(body);
      } catch (error) {
        throw __queueError("the message body cannot be cloned", error);
      }
      break;
    default:
      throw __queueError(
        `contentType must be "text", "bytes", "json", or "v8", got ${JSON.stringify(contentType)}`,
      );
  }
  if (bytes.byteLength > __queueLimits().maxMessageBytes) {
    throw __queueError(
      `a message is at most ${__queueLimits().maxMessageBytes} bytes, got ${bytes.byteLength}`,
    );
  }
  return { body: bytes, contentType, size: bytes.byteLength };
};

// The cell mistrusts every entry that crosses RPC: the binding encodes
// correctly, but a stub holder calls `__queueSend` directly with
// hand-built entries. `__queueSend` and the DLQ accept funnel through
// these checks, so a forged size, content type, or oversize body never
// reaches storage. Ids are minted at the insert, never read off the wire.
const __QUEUE_CONTENT_TYPES = new Set(["text", "bytes", "json", "v8"]);

const __queueEntryBody = (body) => {
  const view = __hostChunk(
    body, "a message body must be an ArrayBuffer or a typed array");
  if (view === null)
    throw __queueError("a message body must be an ArrayBuffer or a typed array");
  return new Uint8Array(view);
};

const __queueCheckContentType = (contentType) => {
  if (!__QUEUE_CONTENT_TYPES.has(contentType)) {
    throw __queueError(
      `contentType must be "text", "bytes", "json", or "v8", got ${JSON.stringify(contentType)}`,
    );
  }
  return contentType;
};

const __queueCheckBodySize = (body) => {
  if (body.byteLength > __queueLimits().maxMessageBytes) {
    throw __queueError(
      `a message is at most ${__queueLimits().maxMessageBytes} bytes, got ${body.byteLength}`,
    );
  }
  return body.byteLength;
};

// A lease-scoped read must seek, not walk the backlog. The plan is the only
// evidence that says which, and it is cheap to ask for, so a gated test seam
// reads it from the shipped query rather than from a copy that can drift.
const __queueReportLeaseLookupPlan = (sql, query, leaseId) => {
  if (typeof __test_queue_lease_lookup_plan !== "function") return;
  const plan = sql.exec(`EXPLAIN QUERY PLAN ${query}`, leaseId)
    .toArray()
    .map((row) => row.detail)
    .join("; ");
  __test_queue_lease_lookup_plan(plan);
};

const __queueMetricsView = (metrics) => ({
  backlogCount: metrics.backlogCount,
  backlogBytes: metrics.backlogBytes,
  oldestMessageTimestamp: metrics.oldestMessageTimestamp === null ||
      metrics.oldestMessageTimestamp === undefined
    ? undefined
    : new Date(metrics.oldestMessageTimestamp),
});

class __QueueCell {
  constructor(ctx) {
    this.ctx = ctx;
    this.queue = ctx.id.name;
    this._ready = false;
    this._producerGroup = null;
  }

  _open() {
    if (this._ready) return this.ctx.storage.sql;
    const sql = this.ctx.storage.sql;
    sql.exec(
      `CREATE TABLE IF NOT EXISTS ${__QUEUE_MESSAGES} (
         seq INTEGER PRIMARY KEY AUTOINCREMENT,
         id TEXT NOT NULL UNIQUE,
         body BLOB NOT NULL,
         content_type TEXT NOT NULL,
         size INTEGER NOT NULL,
         enqueued_at INTEGER NOT NULL,
         visible_at INTEGER NOT NULL,
         attempts INTEGER NOT NULL DEFAULT 0,
         lease_id TEXT,
         lease_generation INTEGER NOT NULL DEFAULT 0,
         leased_until INTEGER,
         purge_on_settle INTEGER NOT NULL DEFAULT 0,
         dlq_target TEXT,
         dlq_transfer_id TEXT,
         transfer_kind TEXT,
         dead_letter_source TEXT,
         CHECK ((dlq_target IS NULL AND transfer_kind IS NULL)
             OR (dlq_target IS NOT NULL
                 AND transfer_kind IN ('dead-letter', 'redrive')))
       )`,
    );
    sql.exec(
      `CREATE INDEX IF NOT EXISTS ${__QUEUE_MESSAGES}_visible
         ON ${__QUEUE_MESSAGES} (visible_at, seq)`,
    );
    // Rearming is on every producer acknowledgement and settlement. Each
    // scheduling lookup must therefore stop at one deadline or one batch;
    // an aggregate over the backlog makes a large Queue quadratic to fill.
    sql.exec(
      `CREATE INDEX IF NOT EXISTS ${__QUEUE_MESSAGES}_ready
         ON ${__QUEUE_MESSAGES} (visible_at, seq)
         WHERE purge_on_settle = 0 AND dlq_target IS NULL
           AND lease_id IS NULL`,
    );
    sql.exec(
      `CREATE INDEX IF NOT EXISTS ${__QUEUE_MESSAGES}_enqueued
         ON ${__QUEUE_MESSAGES} (enqueued_at, seq)`,
    );
    sql.exec(
      `CREATE INDEX IF NOT EXISTS ${__QUEUE_MESSAGES}_retained
         ON ${__QUEUE_MESSAGES} (enqueued_at, seq)
         WHERE purge_on_settle = 0 AND dlq_target IS NULL`,
    );
    sql.exec(
      `CREATE INDEX IF NOT EXISTS ${__QUEUE_MESSAGES}_lease
         ON ${__QUEUE_MESSAGES} (leased_until, seq)
         WHERE lease_id IS NOT NULL`,
    );
    sql.exec(
      `CREATE INDEX IF NOT EXISTS ${__QUEUE_MESSAGES}_moving
         ON ${__QUEUE_MESSAGES} (seq)
         WHERE dlq_target IS NOT NULL`,
    );
    // Dispatch and settlement both select a whole batch by its lease. Without
    // this index SQLite answers them by walking every row: the dispatch
    // readback scans `_visible` and the settlement scan walks the table, so
    // each delivered batch costs two passes over the backlog. The `_lease`
    // index cannot serve them because it is keyed on `leased_until`.
    // The partial predicate keeps only leased rows, which `max_concurrency`
    // times `max_batch_size` bounds, and an inserted message has a null
    // `lease_id`, so `send` pays nothing for it.
    sql.exec(
      `CREATE INDEX IF NOT EXISTS ${__QUEUE_MESSAGES}_lease_batch
         ON ${__QUEUE_MESSAGES} (lease_id, visible_at, seq)
         WHERE lease_id IS NOT NULL`,
    );
    // The live-row index catches an impossible duplicate while a received
    // message remains queued. The receipt below is the durable deduplication
    // record after a consumer or purge removes that message.
    sql.exec(
      `CREATE UNIQUE INDEX IF NOT EXISTS ${__QUEUE_MESSAGES}_dlq_transfer
         ON ${__QUEUE_MESSAGES} (dlq_transfer_id)
         WHERE dlq_transfer_id IS NOT NULL AND dlq_target IS NULL`,
    );
    sql.exec(
      `CREATE TABLE IF NOT EXISTS ${__QUEUE_META} (
         name TEXT PRIMARY KEY,
         value TEXT NOT NULL
       ) WITHOUT ROWID`,
    );
    // The producer result and operator metrics are on hot paths. SQLite
    // triggers keep the three values in the same transaction as every row
    // transition, so a caller cannot observe a new message with old totals.
    sql.exec(
      `CREATE TABLE IF NOT EXISTS ${__QUEUE_STATS} (
         id INTEGER PRIMARY KEY CHECK (id = 1),
         stored_count INTEGER NOT NULL,
         backlog_count INTEGER NOT NULL,
         backlog_bytes INTEGER NOT NULL,
         oldest_message_timestamp INTEGER
       )`,
    );
    sql.exec(
      `INSERT INTO ${__QUEUE_STATS}
         (id, stored_count, backlog_count, backlog_bytes,
          oldest_message_timestamp)
       SELECT 1,
              COUNT(*),
              COALESCE(SUM(purge_on_settle = 0), 0),
              COALESCE(SUM(CASE WHEN purge_on_settle = 0 THEN size ELSE 0 END), 0),
              MIN(CASE WHEN purge_on_settle = 0 THEN enqueued_at END)
         FROM ${__QUEUE_MESSAGES}
        WHERE true
       ON CONFLICT(id) DO NOTHING`,
    );
    sql.exec(
      `CREATE TRIGGER IF NOT EXISTS ${__QUEUE_MESSAGES}_stored_insert
       AFTER INSERT ON ${__QUEUE_MESSAGES}
       BEGIN
         UPDATE ${__QUEUE_STATS}
            SET stored_count = stored_count + 1
          WHERE id = 1;
       END`,
    );
    sql.exec(
      `CREATE TRIGGER IF NOT EXISTS ${__QUEUE_MESSAGES}_stored_delete
       AFTER DELETE ON ${__QUEUE_MESSAGES}
       BEGIN
         UPDATE ${__QUEUE_STATS}
            SET stored_count = stored_count - 1
          WHERE id = 1;
       END`,
    );
    sql.exec(
      `CREATE TRIGGER IF NOT EXISTS ${__QUEUE_MESSAGES}_stats_insert
       AFTER INSERT ON ${__QUEUE_MESSAGES}
       WHEN new.purge_on_settle = 0
       BEGIN
         UPDATE ${__QUEUE_STATS}
            SET backlog_count = backlog_count + 1,
                backlog_bytes = backlog_bytes + new.size,
                oldest_message_timestamp = CASE
                  WHEN oldest_message_timestamp IS NULL
                    OR new.enqueued_at < oldest_message_timestamp
                  THEN new.enqueued_at ELSE oldest_message_timestamp END
          WHERE id = 1;
       END`,
    );
    sql.exec(
      `CREATE TRIGGER IF NOT EXISTS ${__QUEUE_MESSAGES}_stats_delete
       AFTER DELETE ON ${__QUEUE_MESSAGES}
       WHEN old.purge_on_settle = 0
       BEGIN
         UPDATE ${__QUEUE_STATS}
            SET backlog_count = backlog_count - 1,
                backlog_bytes = backlog_bytes - old.size,
                oldest_message_timestamp = CASE
                  WHEN backlog_count <= 1 THEN NULL
                  WHEN oldest_message_timestamp = old.enqueued_at
                  THEN (SELECT MIN(enqueued_at) FROM ${__QUEUE_MESSAGES}
                         WHERE purge_on_settle = 0)
                  ELSE oldest_message_timestamp END
          WHERE id = 1;
       END`,
    );
    sql.exec(
      `CREATE TRIGGER IF NOT EXISTS ${__QUEUE_MESSAGES}_stats_exclude
       AFTER UPDATE OF purge_on_settle ON ${__QUEUE_MESSAGES}
       WHEN old.purge_on_settle = 0 AND new.purge_on_settle != 0
       BEGIN
         UPDATE ${__QUEUE_STATS}
            SET backlog_count = backlog_count - 1,
                backlog_bytes = backlog_bytes - old.size,
                oldest_message_timestamp = CASE
                  WHEN backlog_count <= 1 THEN NULL
                  WHEN oldest_message_timestamp = old.enqueued_at
                  THEN (SELECT MIN(enqueued_at) FROM ${__QUEUE_MESSAGES}
                         WHERE purge_on_settle = 0)
                  ELSE oldest_message_timestamp END
          WHERE id = 1;
       END`,
    );
    sql.exec(
      `CREATE TRIGGER IF NOT EXISTS ${__QUEUE_MESSAGES}_stats_include
       AFTER UPDATE OF purge_on_settle ON ${__QUEUE_MESSAGES}
       WHEN old.purge_on_settle != 0 AND new.purge_on_settle = 0
       BEGIN
         UPDATE ${__QUEUE_STATS}
            SET backlog_count = backlog_count + 1,
                backlog_bytes = backlog_bytes + new.size,
                oldest_message_timestamp = CASE
                  WHEN oldest_message_timestamp IS NULL
                    OR new.enqueued_at < oldest_message_timestamp
                  THEN new.enqueued_at ELSE oldest_message_timestamp END
          WHERE id = 1;
       END`,
    );
    sql.exec(
      `CREATE TABLE IF NOT EXISTS ${__QUEUE_TRANSFER_RECEIPTS} (
         transfer_id TEXT PRIMARY KEY,
         message_id TEXT NOT NULL,
         dead_letter_source TEXT,
         expires_at INTEGER NOT NULL
       ) WITHOUT ROWID`,
    );
    sql.exec(
      `CREATE INDEX IF NOT EXISTS ${__QUEUE_TRANSFER_RECEIPTS}_expires
         ON ${__QUEUE_TRANSFER_RECEIPTS} (expires_at, transfer_id)`,
    );
    this._ready = true;
    return sql;
  }

  _meta(name) {
    return this._open().exec(
      `SELECT value FROM ${__QUEUE_META} WHERE name = ?`, name,
    ).toArray()[0]?.value;
  }

  _setMeta(name, value) {
    if (value === null || value === undefined) {
      this._open().exec(`DELETE FROM ${__QUEUE_META} WHERE name = ?`, name);
      return;
    }
    this._open().exec(
      `INSERT INTO ${__QUEUE_META} (name, value) VALUES (?, ?)
       ON CONFLICT(name) DO UPDATE SET value = excluded.value`,
      name,
      String(value),
    );
  }

  _paused() {
    return this._meta("paused") === "1";
  }

  _metrics(now = Date.now(), waitingOnly = false) {
    const expiresBefore = now - __queueLimits().retentionMs;
    const sql = this._open();
    const stats = sql.exec(
      `SELECT backlog_count, backlog_bytes, oldest_message_timestamp
         FROM ${__QUEUE_STATS} WHERE id = 1`,
    ).toArray()[0];
    // Retention is a clock transition, not a row transition. Until the
    // bounded sweep catches up, use the exact query only at that boundary;
    // the normal hot path remains one materialized row at every depth.
    if (stats.oldest_message_timestamp !== null &&
        stats.oldest_message_timestamp <= expiresBefore) {
      const row = sql.exec(
        `SELECT COUNT(*) AS backlog_count,
                COALESCE(SUM(size), 0) AS backlog_bytes,
                MIN(enqueued_at) AS oldest_message_timestamp
           FROM ${__QUEUE_MESSAGES}
          WHERE purge_on_settle = 0 AND enqueued_at > ?
            AND (? = 0 OR (lease_id IS NULL AND dlq_target IS NULL
                           AND visible_at <= ?))`,
        expiresBefore,
        waitingOnly ? 1 : 0,
        now,
      ).toArray()[0];
      return {
        backlogCount: row.backlog_count,
        backlogBytes: row.backlog_bytes,
        oldestMessageTimestamp: row.oldest_message_timestamp ?? null,
      };
    }
    if (!waitingOnly) {
      if (typeof __test_queue_metrics_materialized === "function") {
        __test_queue_metrics_materialized();
      }
      return {
        backlogCount: stats.backlog_count,
        backlogBytes: stats.backlog_bytes,
        oldestMessageTimestamp: stats.oldest_message_timestamp ?? null,
      };
    }
    const blocked = sql.exec(
      `SELECT COALESCE(SUM(blocked_count), 0) AS blocked_count,
              COALESCE(SUM(blocked_bytes), 0) AS blocked_bytes
         FROM (
           SELECT COUNT(*) AS blocked_count,
                  COALESCE(SUM(size), 0) AS blocked_bytes
             FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_lease
            WHERE purge_on_settle = 0 AND lease_id IS NOT NULL
           UNION ALL
           SELECT COUNT(*) AS blocked_count,
                  COALESCE(SUM(size), 0) AS blocked_bytes
             FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_moving
            WHERE purge_on_settle = 0 AND dlq_target IS NOT NULL
           UNION ALL
           SELECT COUNT(*) AS blocked_count,
                  COALESCE(SUM(size), 0) AS blocked_bytes
             FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_ready
            WHERE purge_on_settle = 0 AND dlq_target IS NULL
              AND lease_id IS NULL AND visible_at > ?
         )`,
      now,
    ).toArray()[0];
    const unleasedOldest = sql.exec(
      `SELECT enqueued_at
         FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_retained
        WHERE purge_on_settle = 0 AND dlq_target IS NULL
          AND lease_id IS NULL AND visible_at <= ?
        ORDER BY enqueued_at, seq
        LIMIT 1`,
      now,
    ).toArray()[0]?.enqueued_at ?? null;
    return {
      backlogCount: stats.backlog_count - blocked.blocked_count,
      backlogBytes: stats.backlog_bytes - blocked.blocked_bytes,
      oldestMessageTimestamp: unleasedOldest,
    };
  }

  async _rearm(now = Date.now(), waitForWake = false) {
    const sql = this._open();
    const consumer = __cell.queueConsumers?.[this.queue];
    const paused = this._paused();
    const active = sql.exec(
      `SELECT COUNT(DISTINCT lease_id) AS count
         FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_lease
        WHERE lease_id IS NOT NULL AND leased_until > ?`,
      now,
    ).toArray()[0].count;
    const concurrency = consumer
      ? consumer.maxConcurrency ?? __queueLimits().maxConcurrency
      : 0;
    const hasCapacity = !paused && consumer !== undefined && __queuePolicy({
      op: "capacity",
      active,
      maximum: concurrency,
    });
    // We need only enough ready rows to distinguish a partial batch from a
    // full one. The two indexed arms cover a new row and an expired lease,
    // and the outer LIMIT keeps the work independent of backlog depth.
    let ready = [];
    if (hasCapacity) {
      const visibleReady = sql.exec(
        `SELECT visible_at AS ready_at, seq
           FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_ready
          WHERE purge_on_settle = 0 AND dlq_target IS NULL
            AND lease_id IS NULL AND visible_at <= ?
          ORDER BY visible_at, seq
          LIMIT ?`,
        now,
        consumer.maxBatchSize,
      ).toArray();
      const expiredReady = sql.exec(
        `SELECT leased_until AS ready_at, seq
           FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_lease
          WHERE purge_on_settle = 0 AND dlq_target IS NULL
            AND lease_id IS NOT NULL AND leased_until <= ?
          ORDER BY leased_until, seq
          LIMIT ?`,
        now,
        consumer.maxBatchSize,
      ).toArray();
      ready = [...visibleReady, ...expiredReady]
        .sort((left, right) => left.ready_at - right.ready_at || left.seq - right.seq)
        .slice(0, consumer.maxBatchSize);
    }
    if (typeof __test_queue_rearm_bounded === "function") {
      __test_queue_rearm_bounded(
        ready.length <= (consumer?.maxBatchSize ?? 0),
        ready.length > 0,
      );
    }
    const earliestVisible = hasCapacity
      ? sql.exec(
        `SELECT visible_at
           FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_ready
          WHERE purge_on_settle = 0 AND dlq_target IS NULL
            AND lease_id IS NULL AND visible_at > ?
          ORDER BY visible_at, seq
          LIMIT 1`,
        now,
      ).toArray()[0]?.visible_at ?? null
      : null;
    const earliestLeaseExpiry = paused
      ? null
      : sql.exec(
        `SELECT leased_until
           FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_lease
          WHERE lease_id IS NOT NULL
          ORDER BY leased_until
          LIMIT 1`,
      ).toArray()[0]?.leased_until ?? null;
    const nextSweep = sql.exec(
      `SELECT enqueued_at + ? AS next_sweep
         FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_retained
        WHERE dlq_target IS NULL AND purge_on_settle = 0
        ORDER BY enqueued_at, seq
        LIMIT 1`,
      __queueLimits().retentionMs,
    ).toArray()[0]?.next_sweep ?? null;
    const dlqPending = sql.exec(
      `SELECT 1 AS pending
         FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_moving
        WHERE dlq_target IS NOT NULL
        LIMIT 1`,
    ).toArray().length !== 0;
    const receiptSweep = sql.exec(
      `SELECT expires_at AS next_sweep
         FROM ${__QUEUE_TRANSFER_RECEIPTS}
        ORDER BY expires_at, transfer_id
        LIMIT 1`,
    ).toArray()[0]?.next_sweep ?? null;
    // A queue without an attached consumer only wakes to reclaim retention.
    // Consumer attachment will nudge the cell when the fleet catalog gains a
    // registration; spinning on an already-visible row cannot make progress.
    let batchDeadline = null;
    if (ready.length > 0) {
      batchDeadline = ready.length >= consumer.maxBatchSize
        ? now
        : ready[0].ready_at + consumer.maxBatchTimeoutMs;
    }
    if (dlqPending) {
      batchDeadline = batchDeadline === null ? now : Math.min(batchDeadline, now);
    }
    const sweepDeadlines = [nextSweep, receiptSweep]
      .filter((deadline) => deadline !== null && deadline !== undefined);
    const armAt = __queuePolicy({
      op: "rearm",
      now,
      batchDeadline,
      earliestVisible,
      // A live handler can settle after delivery pauses. Do not reclaim its
      // lease while paused; resume re-arms the expired deadline immediately
      // and only then can a new generation replace it.
      earliestLeaseExpiry,
      nextSweep: sweepDeadlines.length === 0 ? null : Math.min(...sweepDeadlines),
    });
    if (armAt !== null && !Number.isFinite(armAt)) {
      throw __queueError("the Queue sweep deadline is invalid");
    }
    if (armAt === null) await this.ctx.storage.deleteAlarm();
    else if (waitForWake) {
      await __queue_alarm_set_wait(this.ctx.storage._scope, armAt);
    } else await this.ctx.storage.setAlarm(armAt);
  }

  _newProducerGroup() {
    const group = {
      calls: [],
      entries: [],
      bytes: 0,
      flushing: false,
      timer: null,
    };
    this._producerGroup = group;
    group.timer = setTimeout(() => {
      void this._flushProducerGroup(group);
    }, __queueLimits().producerGroupMs);
    return group;
  }

  async _flushProducerGroup(group) {
    if (group.flushing) return;
    group.flushing = true;
    if (this._producerGroup === group) this._producerGroup = null;
    if (group.timer !== null) {
      clearTimeout(group.timer);
      group.timer = null;
    }

    const now = Date.now();
    try {
      const sql = this._open();
      this.ctx.storage.transactionSync(() => {
        for (const entry of group.entries) {
          sql.exec(
            `INSERT INTO ${__QUEUE_MESSAGES}
               (id, body, content_type, size, enqueued_at, visible_at)
             VALUES (?, ?, ?, ?, ?, ?)`,
            __queueMessageId(now),
            entry.body,
            entry.contentType,
            entry.size,
            now,
            now + entry.delaySeconds * 1000,
          );
        }
      });
      await this._rearm(now, true);
      const metrics = this._metrics(now);
      if (typeof __test_queue_producer_group === "function") {
        __test_queue_producer_group(group.calls.length, group.entries.length);
      }
      for (const call of group.calls) call.resolve(metrics);
    } catch (error) {
      for (const call of group.calls) call.reject(error);
    }
  }

  __queueSend({ entries }) {
    // Direct stub RPC bypasses the binding's encoder, so the cell repeats
    // its checks: the batch shape, every content type and delay, and the
    // per-message and per-batch byte bounds. The size must equal the
    // actual body length; the insert below mints the id.
    if (!Array.isArray(entries) || entries.length === 0) {
      throw __queueError("sendBatch() needs at least one message");
    }
    if (entries.length > __queueLimits().maxBatchMessages) {
      throw __queueError(
        `a batch has at most ${__queueLimits().maxBatchMessages} messages`,
      );
    }
    let bytes = 0;
    for (const entry of entries) {
      if (entry === null || typeof entry !== "object") {
        throw __queueError("each batch entry needs a body");
      }
      __queueCheckContentType(entry.contentType);
      const body = __queueEntryBody(entry.body);
      const size = __queueCheckBodySize(body);
      if (entry.size !== size) {
        throw __queueError(
          `a message size must equal its body byte length, got ${entry.size} for ${size} bytes`,
        );
      }
      entry.delaySeconds = __queueDelay(entry.delaySeconds, 0);
      entry.body = body;
      entry.size = size;
      bytes += size;
    }
    if (bytes > __queueLimits().maxBatchBytes) {
      throw __queueError(
        `a batch is at most ${__queueLimits().maxBatchBytes} bytes, got ${bytes}`,
      );
    }
    return new Promise((resolve, reject) => {
      let group = this._producerGroup;
      if (group !== null &&
          (group.calls.length + 1 > __QUEUE_PRODUCER_GROUP_CALLS ||
            group.entries.length + entries.length > __QUEUE_PRODUCER_GROUP_MESSAGES ||
            group.bytes + bytes > __QUEUE_PRODUCER_GROUP_BYTES)) {
        void this._flushProducerGroup(group);
        group = null;
      }
      if (group === null) group = this._newProducerGroup();
      group.calls.push({ resolve, reject });
      group.entries.push(...entries);
      group.bytes += bytes;
      if (group.calls.length === __QUEUE_PRODUCER_GROUP_CALLS ||
          group.entries.length === __QUEUE_PRODUCER_GROUP_MESSAGES ||
          group.bytes === __QUEUE_PRODUCER_GROUP_BYTES) {
        void this._flushProducerGroup(group);
      }
    });
  }

  __queueMetrics() {
    return this._metrics();
  }

  // Retention decides visibility through `_metrics` immediately, while this
  // bounded sweep only reclaims space. A live lease is marked instead of
  // deleted, so its handler can finish but settlement or expiry removes it
  // without a redelivery.
  _sweep(now) {
    const sql = this._open();
    const cutoff = now - __queueLimits().retentionMs;
    const messages = __cellSweepBatch(
      this.ctx.storage,
      (limit) => sql.exec(
        `SELECT seq, lease_id, leased_until FROM ${__QUEUE_MESSAGES}
          WHERE enqueued_at <= ?
            AND (purge_on_settle = 0
              OR lease_id IS NULL OR leased_until <= ?)
          ORDER BY enqueued_at, seq
          LIMIT ?`,
        cutoff,
        now,
        limit,
      ).toArray(),
      (row) => {
        if (row.lease_id !== null && row.leased_until > now) {
          sql.exec(
            `UPDATE ${__QUEUE_MESSAGES} SET purge_on_settle = 1 WHERE seq = ?`,
            row.seq,
          );
        } else {
          sql.exec(`DELETE FROM ${__QUEUE_MESSAGES} WHERE seq = ?`, row.seq);
        }
      },
      __queueLimits().sweepBatchRows,
    );
    const remaining = __queueLimits().sweepBatchRows - messages;
    if (remaining === 0) return messages;
    const receipts = __cellSweepBatch(
      this.ctx.storage,
      (limit) => sql.exec(
        `SELECT transfer_id FROM ${__QUEUE_TRANSFER_RECEIPTS}
          WHERE expires_at <= ?
          ORDER BY expires_at, transfer_id
          LIMIT ?`,
        now,
        limit,
      ).toArray(),
      (row) => {
        sql.exec(
          `DELETE FROM ${__QUEUE_TRANSFER_RECEIPTS} WHERE transfer_id = ?`,
          row.transfer_id,
        );
      },
      remaining,
    );
    return messages + receipts;
  }

  _candidateRows(now, limit) {
    return this._open().exec(
      `SELECT seq, id, body, content_type, size, enqueued_at, visible_at,
              attempts, lease_id, lease_generation, leased_until,
              purge_on_settle
         FROM ${__QUEUE_MESSAGES}
        WHERE dlq_target IS NULL AND ((purge_on_settle = 1
                 AND (lease_id IS NULL OR leased_until <= ?))
           OR (purge_on_settle = 0 AND visible_at <= ?
                 AND (lease_id IS NULL OR leased_until <= ?)))
        ORDER BY visible_at, seq
        LIMIT ?`,
      now,
      now,
      now,
      limit,
    ).toArray();
  }

  async _leaseBatch(now, consumer) {
    const sql = this._open();
    const active = sql.exec(
      `SELECT COUNT(DISTINCT lease_id) AS count
         FROM ${__QUEUE_MESSAGES}
        WHERE lease_id IS NOT NULL AND leased_until > ?`,
      now,
    ).toArray()[0].count;
    const concurrency = consumer.maxConcurrency ?? __queueLimits().maxConcurrency;
    if (!__queuePolicy({ op: "capacity", active, maximum: concurrency })) return null;

    // Read extra rows so expired purge markers cannot hide a deliverable
    // batch. Policy chooses at most `maxBatchSize` rows and returns every
    // purge deletion that precedes them in this bounded scan.
    const rows = this._candidateRows(now, consumer.maxBatchSize + 256);
    const deliverable = rows.filter((row) => row.purge_on_settle === 0);
    if (deliverable.length === 0) {
      if (rows.length > 0) {
        this.ctx.storage.transactionSync(() => {
          for (const row of rows) {
            sql.exec(`DELETE FROM ${__QUEUE_MESSAGES} WHERE seq = ?`, row.seq);
          }
        });
      }
      return null;
    }
    const readySince = Math.min(...deliverable.map((row) =>
      row.lease_id === null ? row.visible_at : row.leased_until
    ));
    if (deliverable.length < consumer.maxBatchSize &&
        readySince + consumer.maxBatchTimeoutMs > now) {
      return null;
    }

    const plan = __queuePolicy({
      op: "batch",
      now,
      maxBatchSize: consumer.maxBatchSize,
      rows: rows.map((row) => ({
        seq: row.seq,
        visibleAt: row.visible_at,
        leaseGeneration: String(row.lease_generation),
        leasedUntil: row.leased_until,
        purgeOnSettle: row.purge_on_settle !== 0,
      })),
    });
    if (plan.leases.length === 0 && plan.deletePurged.length === 0) return null;
    const rowBySeq = new Map(rows.map((row) => [row.seq, row]));
    const reclaimed = plan.leases.filter((lease) => lease.reclaimed);
    const expiryPolicy = __queuePolicy({
      op: "expiry",
      now,
      entries: reclaimed.map((lease) => {
        const row = rowBySeq.get(lease.seq);
        return {
          priorFailures: row.attempts,
          maxRetries: consumer.maxRetries,
          configuredSeconds: consumer.retryDelaySeconds ?? null,
          purgeOnSettle: row.purge_on_settle !== 0,
        };
      }),
    });
    const expiryBySeq = new Map(
      reclaimed.map((lease, index) => [lease.seq, expiryPolicy[index]]),
    );
    const installs = plan.leases.filter((lease) => {
      const expiry = expiryBySeq.get(lease.seq);
      return expiry === undefined ||
        (expiry.action.kind === "retry" && expiry.action.at <= now);
    });
    const exhaustedTransfers = new Map(
      reclaimed
        .filter((lease) =>
          expiryBySeq.get(lease.seq).action.kind === "exhausted" &&
          consumer.deadLetterQueue !== undefined
        )
        .map((lease) => [lease.seq, crypto.randomUUID()]),
    );
    const leaseId = crypto.randomUUID();
    const leasedUntil = Math.min(
      Number.MAX_SAFE_INTEGER,
      now + __cell.queueLeaseDurationMs,
    );
    this.ctx.storage.transactionSync(() => {
      for (const seq of plan.deletePurged) {
        sql.exec(
          `DELETE FROM ${__QUEUE_MESSAGES}
            WHERE seq = ? AND purge_on_settle = 1
              AND (lease_id IS NULL OR leased_until <= ?)`,
          seq,
          now,
        );
      }
      for (const lease of reclaimed) {
        const expiry = expiryBySeq.get(lease.seq);
        if (expiry.action.kind === "delete-purged") {
          sql.exec(
            `DELETE FROM ${__QUEUE_MESSAGES}
              WHERE seq = ? AND leased_until <= ?`,
            lease.seq,
            now,
          );
        } else if (expiry.action.kind === "exhausted") {
          if (consumer.deadLetterQueue === undefined) {
            sql.exec(
              `DELETE FROM ${__QUEUE_MESSAGES}
                WHERE seq = ? AND leased_until <= ?`,
              lease.seq,
              now,
            );
          } else {
            sql.exec(
              `UPDATE ${__QUEUE_MESSAGES}
                  SET attempts = ?, lease_id = NULL, leased_until = NULL,
                      dlq_target = ?, dlq_transfer_id = ?,
                      transfer_kind = 'dead-letter'
                WHERE seq = ? AND leased_until <= ?`,
              expiry.attempt,
              consumer.deadLetterQueue,
              exhaustedTransfers.get(lease.seq),
              lease.seq,
              now,
            );
          }
        } else if (expiry.action.at > now) {
          sql.exec(
            `UPDATE ${__QUEUE_MESSAGES}
                SET attempts = ?, visible_at = ?, lease_id = NULL,
                    leased_until = NULL
              WHERE seq = ? AND leased_until <= ?`,
            expiry.attempt,
            // Never move a row's visibility backwards: a lease the host
            // charged early carries its own retry floor.
            Math.max(expiry.action.at, rowBySeq.get(lease.seq).visible_at),
            lease.seq,
            now,
          );
        }
      }
      for (const lease of installs) {
        const generation = Number(lease.generation);
        if (!Number.isSafeInteger(generation)) {
          throw __queueError(`message ${lease.seq} exhausted its safe lease generation`);
        }
        sql.exec(
          `UPDATE ${__QUEUE_MESSAGES}
              SET lease_id = ?, lease_generation = ?, leased_until = ?,
                  attempts = attempts + ?
            WHERE seq = ? AND lease_generation = ?
              AND (lease_id IS NULL OR leased_until <= ?)`,
          leaseId,
          generation,
          leasedUntil,
          lease.reclaimed ? 1 : 0,
          lease.seq,
          generation - 1,
          now,
        );
      }
    });
    if (installs.length === 0) return null;
    const leasedQuery =
      `SELECT seq, id, body, content_type, enqueued_at, attempts,
              lease_generation
         FROM ${__QUEUE_MESSAGES}
        WHERE lease_id = ?
        ORDER BY visible_at, seq`;
    __queueReportLeaseLookupPlan(sql, leasedQuery, leaseId);
    const leased = sql.exec(leasedQuery, leaseId).toArray();
    if (leased.length !== installs.length) {
      throw __queueError("the Queue lease transaction changed fewer rows than its plan");
    }
    return {
      leaseId,
      leases: leased.map((row) => ({
        message_id: row.id,
        seq: row.seq,
        generation: row.lease_generation,
      })),
      messages: leased.map((row) => ({
        id: row.id,
        timestampMs: row.enqueued_at,
        // SQLite answers a BLOB as an ArrayBuffer. The raw base64 op accepts
        // typed views, so normalize here; otherwise it encodes the text
        // "[object ArrayBuffer]" and every consumer decoder sees corrupt data.
        bodyBase64: $$btoa(
          row.body instanceof Uint8Array ? row.body : new Uint8Array(row.body),
        ),
        contentType: row.content_type,
        attempts: row.attempts + 1,
      })),
      // Miniflare removes every dispatched batch from its backlog before it
      // invokes the consumer. Exclude every lease here for the same contract;
      // the operator aggregate still includes durable in-flight messages.
      metrics: this._metrics(now, true),
    };
  }

  // The target deduplicates the stable transfer identity. The source can
  // therefore repeat this call after a stop between the target commit and the
  // source delete without creating a second message.
  //
  // The forward arrives over the `__queue_dlq_forward` host op, never stub
  // RPC, so this method is off the tenant allowlist. The host still trusts
  // nothing on the wire: the id is minted here, the size recomputed from
  // the body bytes, and the content type and bounds rechecked.
  async __queueDlqAcceptBatch({ transfers }) {
    if (!Array.isArray(transfers) || transfers.length === 0 ||
        transfers.length > __queueLimits().maxBatchMessages) {
      throw __queueError("a Queue transfer batch has an invalid size");
    }
    const now = Date.now();
    const accepted = [];
    for (const transfer of transfers) {
      if (transfer === null || typeof transfer !== "object") {
        throw __queueError("a Queue transfer is malformed");
      }
      const { transferId, entry, deadLetterSource } = transfer;
      if (typeof transferId !== "string" || transferId.length === 0) {
        throw __queueError("a Queue transfer has an invalid identity");
      }
      if (deadLetterSource !== null &&
          (typeof deadLetterSource !== "string" || deadLetterSource.length === 0)) {
        throw __queueError("a Queue transfer has an invalid dead-letter source");
      }
      if (entry === null || typeof entry !== "object") {
        throw __queueError("a Queue transfer has an invalid entry");
      }
      __queueCheckContentType(entry.contentType);
      const body = __queueEntryBody(entry.body);
      const size = __queueCheckBodySize(body);
      accepted.push({
        transferId,
        deadLetterSource,
        id: __queueMessageId(now),
        body,
        contentType: entry.contentType,
        size,
      });
    }
    const sql = this._open();
    this.ctx.storage.transactionSync(() => {
      for (const transfer of accepted) {
        const receipt = sql.exec(
          `SELECT dead_letter_source
             FROM ${__QUEUE_TRANSFER_RECEIPTS}
            WHERE transfer_id = ?`,
          transfer.transferId,
        ).toArray()[0];
        if (receipt !== undefined) {
          if (receipt.dead_letter_source !== transfer.deadLetterSource) {
            throw __queueError("a DLQ transfer collided with another message identity");
          }
          continue;
        }
        sql.exec(
          `INSERT INTO ${__QUEUE_TRANSFER_RECEIPTS}
             (transfer_id, message_id, dead_letter_source, expires_at)
           VALUES (?, ?, ?, ?)`,
          transfer.transferId,
          transfer.id,
          transfer.deadLetterSource,
          now + __queueLimits().retentionMs,
        );
        sql.exec(
          `INSERT INTO ${__QUEUE_MESSAGES}
             (id, body, content_type, size, enqueued_at, visible_at,
              attempts, dlq_transfer_id, dead_letter_source)
           VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)`,
          transfer.id,
          transfer.body,
          transfer.contentType,
          transfer.size,
          now,
          now,
          transfer.transferId,
          transfer.deadLetterSource,
        );
      }
    });
    await this._rearm(now);
    return this._metrics(now);
  }

  async _driveDlq(limit = 100) {
    const now = Date.now();
    const pending = this._open().exec(
      `SELECT seq, id, body, content_type, size, dlq_target, dlq_transfer_id,
              transfer_kind
         FROM ${__QUEUE_MESSAGES}
        WHERE dlq_target IS NOT NULL
          AND enqueued_at > ?
        ORDER BY seq
        LIMIT ?`,
      now - __queueLimits().retentionMs,
      limit,
    ).toArray();
    const groups = new Map();
    for (const row of pending) {
      let group = groups.get(row.dlq_target);
      if (group === undefined) groups.set(row.dlq_target, group = []);
      group.push(row);
    }
    for (const [targetName, rows] of groups) {
      // A deployment-declared dead-letter edge: the target queue is not
      // necessarily one this cell's script binds as a producer, so the
      // forward grants its scope before resolving it. Dispatch refuses
      // undeclared names otherwise, and a Worker cannot name this op.
      __do_grant_name(__cell.namespaceKeys["__Queue"], targetName);
      // The forward rides the host op, never stub RPC: the target resolves
      // the accept without the tenant allowlist, so the method stays
      // uncallable for a stub holder. The scope and name come from the same
      // namespace lookup the binding uses, and the envelope matches the
      // stub proxy's (`__rpcOut`/`__rpcDes`). The wire carries only the
      // body and content type; the target mints the id and recomputes the
      // size, so a replayed batch cannot plant a forged identity.
      const { id, name } = __cell.makeNamespace("__Queue").getByName(targetName);
      await __rpcDes(await __queue_dlq_forward(
        __idScope(id),
        name ?? null,
        __rpcOut([{
          transfers: rows.map((row) => ({
            transferId: row.dlq_transfer_id,
            // A dead-letter transfer records where redrive returns the message.
            // A redrive returns it to ordinary queue state, so the source marker
            // must not follow it and make a later redrive bounce it backwards.
            deadLetterSource: row.transfer_kind === "dead-letter" ? this.queue : null,
            entry: {
              body: row.body,
              contentType: row.content_type,
            },
          })),
        }], "host"),
      ));
      if (typeof __test_queue_dlq_accepted === "function") {
        __test_queue_dlq_accepted();
      }
      // The target committed every receipt before this reply. Delete the
      // corresponding source marks together; a stop before this transaction
      // leaves the whole group replayable through those stable receipts.
      this.ctx.storage.transactionSync(() => {
        for (const row of rows) {
          this._open().exec(
            `DELETE FROM ${__QUEUE_MESSAGES}
              WHERE seq = ? AND dlq_target = ? AND dlq_transfer_id = ?
                AND transfer_kind = ?`,
            row.seq,
            row.dlq_target,
            row.dlq_transfer_id,
            row.transfer_kind,
          );
        }
      });
    }
    return pending.length;
  }

  async alarm() {
    const now = Date.now();
    try {
      await this._driveDlq();
    } catch (error) {
      // The durable source mark is the retry record. An alarm stays armed at
      // `now`, and the target's transfer identity makes a replay idempotent.
      console.error("Queue DLQ transfer failed:", error);
    }
    const swept = this._sweep(now);
    const consumer = this._paused()
      ? undefined
      : __cell.queueConsumers?.[this.queue];
    // Install every lease that the configured concurrency admits before the
    // first consumer can settle. One lease per alarm serialized full batches
    // because the next alarm depended on a later scheduler turn, so a queue
    // with maxConcurrency > 1 still used only one handler under backlog.
    // `_leaseBatch` counts the durable lease IDs on every iteration, which
    // makes the configured concurrency the structural bound for this loop.
    const batches = [];
    if (consumer !== undefined) {
      while (true) {
        const batch = await this._leaseBatch(now, consumer);
        if (batch === null) break;
        batches.push(batch);
      }
    }
    try {
      // `_leaseBatch` can exhaust an expired lease and create a new durable
      // DLQ mark. Drive that mark in the same wake; the first call above is
      // for a mark recovered from an earlier stopped process.
      await this._driveDlq();
    } catch (error) {
      console.error("Queue DLQ transfer failed:", error);
    }
    await this._rearm(now);
    if (swept >= __queueLimits().sweepBatchRows && batches.length === 0) {
      await this.ctx.storage.setAlarm(now + 1);
      return;
    }
    for (const batch of batches) {
      // This await covers only the output-gated handoff to the host. The
      // consumer runs after this alarm returns, and settlement comes back as
      // an independent cell event. All admitted leases are durable before the
      // first handoff, so a fast settlement cannot make this turn exceed the
      // configured concurrency.
      await __queue_dispatch(
        consumer.script,
        this.queue,
        JSON.stringify(batch),
      );
    }
  }

  _matchingLease(body) {
    if (typeof body.leaseId !== "string" || !Array.isArray(body.leases)) return null;
    const matchQuery =
      `SELECT seq, id, attempts, lease_generation, purge_on_settle
         FROM ${__QUEUE_MESSAGES}
        WHERE lease_id = ?
        ORDER BY seq`;
    __queueReportLeaseLookupPlan(this._open(), matchQuery, body.leaseId);
    const rows = this._open().exec(matchQuery, body.leaseId).toArray();
    for (const lease of body.leases) {
      if (typeof lease?.message_id !== "string" ||
          !Number.isSafeInteger(lease.seq) ||
          !Number.isSafeInteger(lease.generation)) return null;
    }
    const matches = __queuePolicy({
      op: "settlement",
      current: rows.map((row) => ({
        seq: String(row.seq),
        messageId: row.id,
        generation: String(row.lease_generation),
      })),
      submitted: body.leases.map((lease) => ({
        seq: String(lease.seq),
        messageId: lease.message_id,
        generation: String(lease.generation),
      })),
    });
    return matches ? rows : null;
  }

  _refusalPolicy(now, kind, unfittable) {
    const count = Number(this._meta("refusal_count") ?? "0");
    const startedAt = Number(this._meta("refusal_started_at") ?? "0");
    const at = Number(this._meta("refusal_at") ?? "0");
    const priorKind = this._meta("refusal_kind");
    const known = Number.isSafeInteger(count) && count > 0 &&
      Number.isSafeInteger(startedAt) && startedAt > 0 &&
      Number.isSafeInteger(at) && at > 0 &&
      (priorKind === "budget" || priorKind === "pressure");
    return __queuePolicy({
      op: "refusal",
      now,
      kind,
      unfittable,
      priorCount: known ? count : null,
      priorKind: known ? priorKind : null,
      priorStartedAt: known ? startedAt : null,
      priorAt: known ? at : null,
    });
  }

  // A batch its consumer isolate could not take never ran a line of the
  // handler, so the delivery attempt is not the consumer's to spend. The cell
  // decides between two outcomes, and the decision is persisted with the rows:
  //
  //   release -- clear the lease and make the rows visible after a backoff, so
  //     a full isolate is not dispatched to in a loop and the rows land behind
  //     whatever is already waiting;
  //   charge  -- spend the attempt and apply the ordinary retry delay (or the
  //     dead-letter move) by expiring the lease now, which is the transition
  //     `_leaseBatch` already runs for a lease that expired on its own. That
  //     is what terminates a batch that can never fit, or a squeeze that
  //     outlasts the free releases: the message reaches `max_retries` and its
  //     dead-letter queue instead of cycling until retention deletes it.
  //
  // The fence is the settlement's own, so a report that lost a race with a
  // new lease clears nothing. A success clears the streak in `_settle`, so
  // intermittent refusals never ratchet a healthy queue into charging.
  async _refuse(body) {
    const rows = this._matchingLease(body);
    if (rows === null) return null;
    const now = Date.now();
    const kind = body.kind === "pressure" ? "pressure" : "budget";
    const refusal = this._refusalPolicy(now, kind, body.unfittable === true);
    const sql = this._open();
    this.ctx.storage.transactionSync(() => {
      if (refusal.action === "charge") {
        // Expire the lease now and floor the row's next visibility at the
        // ladder delay: the ordinary expiry transition then spends the
        // attempt after that delay, so a high `max_retries` cannot burst up
        // to a hundred re-encoded dispatches.
        const retryAt = now + refusal.delayMs;
        for (const row of rows) {
          sql.exec(
            `UPDATE ${__QUEUE_MESSAGES}
                SET leased_until = ?, visible_at = ?
              WHERE seq = ? AND lease_id = ? AND lease_generation = ?`,
            now,
            Math.max(row.visible_at, retryAt),
            row.seq,
            body.leaseId,
            row.lease_generation,
          );
        }
      } else {
        const visibleAt = now + refusal.delayMs;
        for (const row of rows) {
          sql.exec(
            `UPDATE ${__QUEUE_MESSAGES}
                SET lease_id = NULL, leased_until = NULL, visible_at = ?
              WHERE seq = ? AND lease_id = ? AND lease_generation = ?`,
            visibleAt,
            row.seq,
            body.leaseId,
            row.lease_generation,
          );
        }
      }
      // One transaction with the rows: a stop before it commits leaves the
      // lease to expire as before, so no attempt is spent twice and no lease
      // is lost.
      this._setMeta("refusal_count", refusal.count);
      this._setMeta("refusal_kind", refusal.kind);
      this._setMeta("refusal_started_at", refusal.startedAt);
      this._setMeta("refusal_at", now);
    });
    await this._rearm(now);
    return refusal.action;
  }

  async _settle(body) {
    const rows = this._matchingLease(body);
    if (rows === null) return false;
    const consumer = __cell.queueConsumers?.[this.queue];
    if (consumer === undefined) return false;
    const explicitAcks = new Set(body.explicitAcks ?? []);
    const retryMessages = new Map(
      (body.retryMessages ?? []).map((retry) => [retry.msgId, retry.delaySeconds]),
    );
    const retryBatch = body.retryBatch?.retry === true;
    const batchDelay = body.retryBatch?.delaySeconds;
    const exception = body.outcome === "exception";
    // An explicit ack-all survives a later handler exception, so it must
    // override every batch-level and message-level retry signal.
    const ackAll = body.ackAll === true;
    const decisions = rows.map((row) => {
      const retry = !ackAll && !explicitAcks.has(row.id) && (
        retryMessages.has(row.id) || retryBatch || exception
      );
      return { row, retry };
    });
    const retrying = decisions.filter((decision) =>
      decision.retry && decision.row.purge_on_settle === 0
    );
    const retryPolicy = __queuePolicy({
      op: "retries",
      now: Date.now(),
      entries: retrying.map(({ row }) => ({
        attempt: row.attempts + 1,
        maxRetries: consumer.maxRetries,
        explicitSeconds: retryMessages.has(row.id)
          ? retryMessages.get(row.id)
          : retryBatch ? batchDelay : null,
        configuredSeconds: consumer.retryDelaySeconds ?? null,
      })),
    });
    const retryById = new Map(
      retrying.map(({ row }, index) => [row.id, retryPolicy[index]]),
    );
    const dlqTransferById = new Map(
      retrying
        .filter(({ row }) =>
          retryById.get(row.id).exhausted && consumer.deadLetterQueue !== undefined
        )
        .map(({ row }) => [row.id, crypto.randomUUID()]),
    );
    const sql = this._open();
    this.ctx.storage.transactionSync(() => {
      for (const { row, retry } of decisions) {
        const policy = retryById.get(row.id);
        if (row.purge_on_settle !== 0 || !retry ||
            (policy.exhausted && consumer.deadLetterQueue === undefined)) {
          sql.exec(
            `DELETE FROM ${__QUEUE_MESSAGES}
              WHERE seq = ? AND lease_id = ? AND lease_generation = ?`,
            row.seq,
            body.leaseId,
            row.lease_generation,
          );
          continue;
        }
        if (policy.exhausted) {
          sql.exec(
            `UPDATE ${__QUEUE_MESSAGES}
                SET attempts = attempts + 1, lease_id = NULL,
                    leased_until = NULL, dlq_target = ?, dlq_transfer_id = ?,
                    transfer_kind = 'dead-letter'
              WHERE seq = ? AND lease_id = ? AND lease_generation = ?`,
            consumer.deadLetterQueue,
            dlqTransferById.get(row.id),
            row.seq,
            body.leaseId,
            row.lease_generation,
          );
          continue;
        }
        sql.exec(
          `UPDATE ${__QUEUE_MESSAGES}
              SET attempts = attempts + 1, visible_at = ?,
                  lease_id = NULL, leased_until = NULL
            WHERE seq = ? AND lease_id = ? AND lease_generation = ?`,
          policy.at,
          row.seq,
          body.leaseId,
          row.lease_generation,
        );
      }
    });
    // A consumer ran here, so whatever was refusing the isolate is over: the
    // next refusal starts free instead of resuming at the sustained streak's
    // ladder. Only the three keys are read, so a healthy queue pays one
    // metadata lookup per settlement, not a write.
    if (this._meta("refusal_count") !== undefined) {
      this._setMeta("refusal_count", null);
      this._setMeta("refusal_kind", null);
      this._setMeta("refusal_started_at", null);
      this._setMeta("refusal_at", null);
    }
    try {
      await this._driveDlq();
    } catch (error) {
      // Settlement is durable once the source mark commits. A failed transfer
      // is alarm work now, not a reason to put the message back in delivery.
      console.error("Queue DLQ transfer deferred:", error);
    }
    await this._rearm();
    return true;
  }

  _requireOperator(request, operation, command) {
    // Defence in depth beyond the fetch gate: these methods are also
    // reachable over stub RPC, where only the tenant allowlist stands
    // between a stub holder and a purge. The marker lives in a
    // harness-private WeakSet keyed by the server-side Request, so an RPC
    // caller cannot supply a member: only the fetch branch below, holding
    // the host-built request, passes.
    if (!__operatorIngressRequests.has(request))
      throw __queueError(`${operation} is an operator operation; use \`${command}\``);
  }

  async _purge(request) {
    this._requireOperator(request, "purge", "cello queue purge");
    const now = Date.now();
    const sql = this._open();
    let deleted = 0;
    let leased = 0;
    this.ctx.storage.transactionSync(() => {
      let after = 0;
      while (true) {
        // Purge is necessarily proportional to the stored backlog, but its
        // policy envelope stays bounded instead of materializing the Queue in
        // V8 or Rust. One SQLite transaction keeps the visibility change
        // atomic across all chunks.
        const rows = sql.exec(
          `SELECT seq, lease_id, leased_until FROM ${__QUEUE_MESSAGES}
            WHERE seq > ?
            ORDER BY seq
            LIMIT 256`,
          after,
        ).toArray();
        if (rows.length === 0) break;
        after = rows[rows.length - 1].seq;
        const plan = __queuePolicy({
          op: "purge",
          now,
          rows: rows.map((row) => ({
            seq: String(row.seq),
            leaseIdPresent: row.lease_id !== null,
            leasedUntil: row.leased_until,
          })),
        });
        for (const seq of plan.delete) {
          sql.exec(
            `DELETE FROM ${__QUEUE_MESSAGES}
              WHERE seq = ?
                AND (lease_id IS NULL OR leased_until IS NULL OR leased_until <= ?)`,
            Number(seq),
            now,
          );
          deleted += 1;
        }
        for (const seq of plan.markForSettle) {
          sql.exec(
            `UPDATE ${__QUEUE_MESSAGES} SET purge_on_settle = 1
              WHERE seq = ? AND lease_id IS NOT NULL AND leased_until > ?`,
            Number(seq),
            now,
          );
          leased += 1;
        }
      }
    });
    await this._rearm(now);
    return { deleted, leased, metrics: this._metrics(now) };
  }

  async _setPaused(paused, request) {
    this._requireOperator(
      request,
      paused ? "pause" : "resume",
      paused ? "cello queue pause" : "cello queue resume",
    );
    this._setMeta("paused", paused ? "1" : null);
    await this._rearm();
    return { paused, ...this._metrics() };
  }

  _operatorLimit(value, fallback) {
    const limit = value ?? fallback;
    if (!Number.isInteger(limit) || limit < 1 || limit > 100) {
      throw __queueError("limit must be an integer from 1 through 100");
    }
    return limit;
  }

  _peek(limit, request) {
    this._requireOperator(request, "peek", "cello queue peek");
    const now = Date.now();
    const cutoff = now - __queueLimits().retentionMs;
    const rows = this._open().exec(
      `SELECT id, body, content_type, size, enqueued_at, visible_at, attempts,
              lease_id, leased_until, purge_on_settle, dlq_target,
              dead_letter_source
         FROM ${__QUEUE_MESSAGES}
        WHERE enqueued_at > ?
        ORDER BY visible_at, seq
        LIMIT ?`,
      cutoff,
      limit,
    ).toArray();
    return {
      messages: rows.map((row) => ({
        id: row.id,
        bodyBase64: $$btoa(
          row.body instanceof Uint8Array ? row.body : new Uint8Array(row.body),
        ),
        contentType: row.content_type,
        size: row.size,
        enqueuedAt: row.enqueued_at,
        visibleAt: row.visible_at,
        attempts: row.attempts,
        state: row.dlq_target !== null
          ? "moving"
          : row.purge_on_settle !== 0
          ? "purging"
          : row.lease_id !== null && row.leased_until > now
          ? "leased"
          : row.visible_at <= now
          ? "visible"
          : "delayed",
        leasedUntil: row.lease_id !== null && row.leased_until > now
          ? row.leased_until
          : null,
        transferTarget: row.dlq_target,
        deadLetterSource: row.dead_letter_source,
      })),
    };
  }

  async _redrive(limit, request) {
    this._requireOperator(request, "redrive", "cello queue redrive");
    // Finish an interrupted move before selecting more work. A transfer mark
    // is durable, so selecting it a second time would invent another identity
    // where replaying the existing identity is sufficient.
    await this._driveDlq(100);
    const now = Date.now();
    const cutoff = now - __queueLimits().retentionMs;
    const candidates = this._open().exec(
      `SELECT seq, dead_letter_source
         FROM ${__QUEUE_MESSAGES}
        WHERE dead_letter_source IS NOT NULL
          AND dlq_target IS NULL
          AND purge_on_settle = 0
          AND enqueued_at > ?
          AND (lease_id IS NULL OR leased_until <= ?)
        ORDER BY visible_at, seq
        LIMIT ?`,
      cutoff,
      now,
      limit,
    ).toArray();
    const transfers = candidates.map((row) => ({
      ...row,
      transferId: crypto.randomUUID(),
    }));
    this.ctx.storage.transactionSync(() => {
      for (const transfer of transfers) {
        this._open().exec(
          `UPDATE ${__QUEUE_MESSAGES}
              SET dlq_target = ?, dlq_transfer_id = ?, transfer_kind = 'redrive',
                  lease_id = NULL, leased_until = NULL
            WHERE seq = ? AND dead_letter_source = ? AND dlq_target IS NULL
              AND purge_on_settle = 0
              AND (lease_id IS NULL OR leased_until <= ?)`,
          transfer.dead_letter_source,
          transfer.transferId,
          transfer.seq,
          transfer.dead_letter_source,
          now,
        );
      }
    });
    // Arm before the cross-cell call. If the process stops after the durable
    // mark, the alarm replays the same transfer identity.
    await this._rearm(now);
    await this._driveDlq(100);
    await this._rearm();
    return { redriven: transfers.length, metrics: this._metrics() };
  }

  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (request.method !== "POST") {
      return new Response("Queue route not found", { status: 404 });
    }
    let body;
    try {
      body = await request.json();
    } catch {
      return new Response("invalid Queue request", { status: 400 });
    }
    // The same cell answers a Worker's stub.fetch() and the operator CLI's
    // `/runtime/` dispatch, so the operator-only branches demand the
    // host-set operator-ingress marker on the server-side Request. Settlement
    // stays open: the host's consumer driver settles through here without the
    // marker, and a settlement without a live lease id 409s.
    const operator = __operatorIngressRequests.has(request);
    const refuse = (operation, command) => Response.json(
      { error: `QUEUE_ERROR: ${operation} is an operator operation; use \`${command}\`` },
      { status: 403 },
    );
    try {
      if (path === "/__qSettle") {
        return await this._settle(body)
          ? new Response(null, { status: 204 })
          : new Response("stale Queue settlement", { status: 409 });
      }
      // The host reports back a batch its consumer isolate could not take.
      // This is not a settlement by the consumer: the cell either releases the
      // lease without an attempt or charges the attempt, and answers which.
      if (path === "/__qRefuse") {
        const action = await this._refuse(body);
        if (action === null) return new Response("stale Queue lease", { status: 409 });
        return Response.json({ action });
      }
      if (body.op === "info") {
        if (!operator) return refuse("info", "cello queue info");
        const metrics = this._metrics();
        const stored = this._open().exec(
          `SELECT stored_count FROM ${__QUEUE_STATS} WHERE id = 1`,
        ).toArray()[0].stored_count;
        return Response.json({ result: { ...metrics, stored, paused: this._paused() } });
      }
      if (body.op === "purge") {
        if (!operator) return refuse("purge", "cello queue purge");
        return Response.json({ result: await this._purge(request) });
      }
      if (body.op === "pause") {
        if (!operator) return refuse("pause", "cello queue pause");
        return Response.json({ result: await this._setPaused(true, request) });
      }
      if (body.op === "resume") {
        if (!operator) return refuse("resume", "cello queue resume");
        return Response.json({ result: await this._setPaused(false, request) });
      }
      if (body.op === "peek") {
        if (!operator) return refuse("peek", "cello queue peek");
        return Response.json({
          result: this._peek(this._operatorLimit(body.limit, 10), request),
        });
      }
      if (body.op === "redrive") {
        if (!operator) return refuse("redrive", "cello queue redrive");
        return Response.json({
          result: await this._redrive(this._operatorLimit(body.limit, 100), request),
        });
      }
      return Response.json(
        { error: `QUEUE_ERROR: unknown operation ${JSON.stringify(body.op)}` },
        { status: 400 },
      );
    } catch (error) {
      return Response.json(
        { error: String(error?.message ?? error) },
        { status: 400 },
      );
    }
  }
}

__runtimeClass("__Queue", __QueueCell);

class Queue {
  constructor(queue, cellName, deliveryDelay) {
    Object.defineProperties(this, {
      _queue: { value: queue },
      _cellName: { value: cellName },
      _deliveryDelay: { value: deliveryDelay },
    });
  }

  get _stub() {
    return __cell.makeNamespace("__Queue").getByName(this._cellName);
  }

  async _send(entries) {
    const metrics = await this._stub.__queueSend({ entries });
    return { metadata: { metrics: __queueMetricsView(metrics) } };
  }

  send(body, options = {}) {
    const encoded = __queueEncode(body, options.contentType);
    return this._send([{
      ...encoded,
      delaySeconds: __queueDelay(options.delaySeconds, this._deliveryDelay),
    }]);
  }

  sendBatch(messages, options = {}) {
    if (messages === null || messages === undefined ||
        typeof messages[Symbol.iterator] !== "function") {
      throw __queueError("sendBatch() needs an iterable of messages");
    }
    const entries = [];
    let bytes = 0;
    const batchDelay = __queueDelay(options.delaySeconds, this._deliveryDelay);
    for (const message of messages) {
      if (entries.length >= __queueLimits().maxBatchMessages) {
        throw __queueError(
          `a batch has at most ${__queueLimits().maxBatchMessages} messages`,
        );
      }
      if (message === null || typeof message !== "object" || !("body" in message)) {
        throw __queueError("each batch entry needs a body");
      }
      const encoded = __queueEncode(message.body, message.contentType);
      bytes += encoded.size;
      if (bytes > __queueLimits().maxBatchBytes) {
        throw __queueError(
          `a batch is at most ${__queueLimits().maxBatchBytes} bytes, got ${bytes}`,
        );
      }
      entries.push({
        ...encoded,
        delaySeconds: __queueDelay(message.delaySeconds, batchDelay),
      });
    }
    if (entries.length === 0) {
      throw __queueError("sendBatch() needs at least one message");
    }
    return this._send(entries);
  }

  async metrics() {
    return __queueMetricsView(await this._stub.__queueMetrics());
  }
}

__nativeMethodArities(Queue);
__cello.__makeQueue = (queue, cellName, deliveryDelay) =>
  new Queue(queue, cellName, deliveryDelay);

// ---- D1 -----------------------------------------------------------------
// A D1 database is a cell of a runtime-supplied Durable Object class, so it
// inherits ownership, fencing, LTX replication, RPO=0 acknowledgement and
// pressure shedding from the cell it already is. This half is the server;
// `__makeD1Database` below is the client the binding hands to a Worker.
//
// Everything SQL goes through the one `__d1_run` op. The engine owns the
// statement walk, the row and byte caps, and the meta snapshot, all inside
// one execution -- the previous shape assembled these from the general
// SqlStorage ops, and every seam between them was a contract only convention
// enforced.

// The error families applications match on. A message that already carries
// one must pass through unwrapped: wrapping again would bury the family the
// application is matching under a second D1_ERROR prefix.
const __d1IsFamilyError = (message) =>
  /^D1_([A-Z]+_)*(ERROR|NOTFOUND): /.test(String(message));

const __d1Error = (message, cause) => {
  const error = new Error("D1_ERROR: " + String(message));
  error.cause = cause !== undefined ? cause : new Error(String(message));
  return error;
};

// A typed refusal from the engine: `{family, message}` becomes the Error the
// application sees, with `.cause` carrying the bare message as upstream does.
const __d1Failure = (failure) => {
  const error = new Error(failure.family + ": " + failure.message);
  error.cause = new Error(failure.message);
  return error;
};

const __d1Run = (scope, request) => {
  const response = JSON.parse(__d1_run(scope, JSON.stringify(request)));
  if (response.error) throw __d1Failure(response.error);
  return response.ok;
};

class __D1DatabaseCell {
  constructor(ctx) {
    this.ctx = ctx;
  }
  // Takes an array so that `batch()` can ride this same method, and the
  // same turn, when it lands.
  __d1Query(statements) {
    const scope = __sqlStorageScope(this.ctx.storage.sql);
    return statements.map((statement) =>
      __d1Run(scope, {
        mode: "prepared",
        sql: String(statement.sql),
        params: statement.params || [],
        first: !!statement.first,
      })
    );
  }
  __d1Batch(statements) {
    return __d1Run(__sqlStorageScope(this.ctx.storage.sql), {
      mode: "batch",
      statements: statements.map((statement) => ({
        sql: String(statement.sql),
        params: statement.params || [],
      })),
    });
  }
  // The CLI's way in. `cello d1` signs each request with the fleet secret
  // and sends it to a live node's `/runtime/<scope>` route, which verifies the
  // signature and then forwards to the owner over the same dispatch `/do/`
  // uses -- so the CLI needs no ownership logic and the database is reached
  // the way a Worker reaches it. The unauthenticated `/do/` route refuses a
  // D1 scope outright: this cell answers arbitrary SQL, and its scope is an
  // HMAC over names that sit in the project's config, so the scope itself
  // is no secret.
  async fetch(request) {
    let body;
    try {
      body = await request.json();
    } catch {
      return Response.json({ error: "D1_ERROR: invalid request body" }, { status: 400 });
    }
    // The same cell answers a Worker's stub.fetch() and the operator CLI's
    // `/runtime/` dispatch, so the operator-only branches demand the
    // host-set operator-ingress marker on the server-side Request.
    const operator = __operatorIngressRequests.has(request);
    const refuse = (operation, command) => Response.json(
      { error: `D1_ERROR: ${operation} is an operator operation; use \`${command}\`` },
      { status: 403 },
    );
    try {
      let result;
      if (body.migrate !== undefined) {
        if (!operator) return refuse("migrate", "cello d1 migrations apply");
        result = this.__d1Migrate(
          String(body.migrate.name),
          String(body.migrate.sql),
          String(body.migrate.table || "d1_migrations"),
        );
      } else if (body.exec !== undefined) {
        // The CLI asks for rows; the Worker binding's exec() never does.
        if (!!body.exec.rows && !operator) return refuse("exec with rows", "cello d1 execute");
        result = __d1Run(__sqlStorageScope(this.ctx.storage.sql), {
          mode: "exec",
          sql: String(body.exec.sql),
          rows: !!body.exec.rows,
        });
      } else if (body.export !== undefined) {
        if (!operator) return refuse("export", "cello d1 export");
        result = this.__d1Export(body.export || {});
      } else if (body.batch !== undefined) {
        // The CLI's batch path rides the Worker's own __d1Batch, so the two
        // share one transactional implementation rather than two.
        result = this.__d1Batch((body.batch || {}).statements || []);
      } else {
        result = this.__d1Query(body.statements || []);
      }
      return Response.json({ result });
    } catch (error) {
      return Response.json({ error: String(error && error.message || error) }, { status: 400 });
    }
  }
  __d1Exec(source) {
    const result = __d1Run(__sqlStorageScope(this.ctx.storage.sql), {
      mode: "exec",
      sql: String(source),
      rows: false,
    });
    return { count: result.count, duration: result.duration };
  }
  // The CLI's dump path. The engine reads the whole dump under one
  // transaction, so what crosses here is a single snapshot even while
  // the database takes writes.
  __d1Export(options) {
    const tables = Array.isArray(options.tables) ? options.tables.map(String) : [];
    return __d1Run(__sqlStorageScope(this.ctx.storage.sql), {
      mode: "export",
      tables,
      no_schema: !!options.no_schema,
      no_data: !!options.no_data,
    });
  }
  // Apply one migration and record it. The engine runs the file, the
  // bookkeeping insert (the name is a bound parameter, so a file name never
  // reaches SQL text) and the BEGIN IMMEDIATE .. COMMIT bracket in one
  // request, so a migration that fails part-way rolls back whole and the
  // operator fixes the file and re-runs. Without the transaction the
  // statements that DID land are exactly the ones a re-run trips over.
  __d1Migrate(name, source, table) {
    return __d1Run(__sqlStorageScope(this.ctx.storage.sql), {
      mode: "migrate",
      name,
      sql: source,
      table,
    });
  }
}

__runtimeClass("__D1Database", __D1DatabaseCell);

// Validate and encode bind values once, at the public boundary, exactly as
// upstream does (workerd d1-api.ts). Byte-shaped values become byte arrays --
// the wire format for a BLOB bind -- and anything unsupported throws
// D1_TYPE_ERROR here, before any SQL runs. The first version deferred this
// to a JSON round-trip whose fallthrough was SQL NULL, so a Uint8Array bind
// stored NULL and nothing said so.
const __d1BindValue = (value) => {
  const kind = typeof value;
  if (kind === "number") {
    // A non-finite number has no JSON form, so it crosses as null and
    // stores SQL NULL. Upstream does the same (bind(NaN) selects typeof
    // "null"); making it explicit here turns the conversion from an
    // accident of JSON.stringify into a pinned decision.
    return Number.isFinite(value) ? value : null;
  }
  if (kind === "string") return value;
  if (kind === "boolean") return value ? 1 : 0;
  if (kind === "object") {
    if (value === null) return value;
    if (
      Array.isArray(value) &&
      value.every((byte) => typeof byte === "number" && byte >= 0 && byte < 256)
    ) {
      return value;
    }
    if (value instanceof ArrayBuffer) {
      return Array.from(new Uint8Array(value));
    }
    // A typed view binds its ELEMENT values, each truncated to a byte --
    // Int8Array([-1]) stores 0xff, Float32Array([1.5]) stores 0x01 -- not
    // its underlying byte window, so Uint16Array([65, 66]) stores 2 bytes
    // and not 4. Upstream behaves this way (verified against workerd's D1;
    // the differential suite pins it), and the bare Array.from that
    // preceded this produced elements the engine then refused as bytes.
    if (ArrayBuffer.isView(value)) return Array.from(Uint8Array.from(value));
  }
  const error = new Error(
    `D1_TYPE_ERROR: Type '${kind}' not supported for value '${value}'`,
  );
  error.cause = new Error(`Type '${kind}' not supported for value '${value}'`);
  throw error;
};

// The D1 interface objects publish their private-slot accessors here at
// definition time; the statements and the database are defined in different
// orders, so the object exists before any of them.
const __d1Hooks = {};
// The statement's text, parameters and owning database are private slots;
// workerd's D1PreparedStatement exposes bind/first/run/all/raw only.
let __d1StatementHooks;
const __d1Statement = (statement) => __d1StatementHooks.slots(statement);
class D1PreparedStatement {
  #database;
  #sql;
  #params;
  static #expose = (__d1StatementHooks = {
    slots: (statement) => ({
      sql: statement.#sql, params: statement.#params,
    }),
  });
  constructor(database, sql, params) {
    this.#database = database;
    this.#sql = sql;
    this.#params = params;
    // Workerd's D1PreparedStatement carries its text and bound values as own
    // enumerable data members; the slots above stay authoritative.
    this.statement = sql;
    this.params = params;
  }
  bind(...values) {
    return new D1PreparedStatement(
      this.#database,
      this.#sql,
      values.map(__d1BindValue),
    );
  }
  async #run(first) {
    const results = await __d1Query(this.#database, [
      { sql: this.#sql, params: this.#params, first },
    ]);
    return results[0];
  }
  async all() {
    const result = await this.#run();
    return {
      success: true,
      meta: result.meta,
      results: result.rows.map((row) => __d1Shape(result.columns, row)),
    };
  }
  async run() {
    return await this.all();
  }
  async first(column) {
    // `first: true` keeps one row on the cell side, so only that row crosses
    // the isolate boundary and the row cap cannot fire for a first().
    const result = await this.#run(true);
    if (result.rows.length === 0) return null;
    const row = __d1Shape(result.columns, result.rows[0]);
    if (column === undefined) return row;
    // The check runs on the shaped object, as upstream's does, so a column
    // named like an Object.prototype member behaves the same here as there.
    if (row[column] === undefined) {
      const error = new Error(
        `D1_COLUMN_NOTFOUND: Column not found (${column})`,
      );
      error.cause = new Error("Column not found");
      throw error;
    }
    return row[column];
  }
  async raw(options) {
    const result = await this.#run();
    return options && options.columnNames
      ? [result.columns, ...result.rows]
      : result.rows;
  }
}
// Object.fromEntries, not property assignment: assigning to a column named
// `__proto__` would walk the prototype chain and silently drop the value,
// while fromEntries defines an own property whatever the name.
const __d1Shape = (columns, row) =>
  Object.fromEntries(row.map((value, index) => [columns[index], value]));

const __d1PublicResult = (result) => ({
  success: true,
  meta: result.meta,
  results: result.rows.map((row) => __d1Shape(result.columns, row)),
});

// Cello has one primary and no replicas, so every completed query satisfies
// every earlier session constraint. One stable opaque token states that fact
// without pretending an isolate-local counter is a durable database position.
const __d1PrimaryBookmark = "cello:primary";

class D1DatabaseSession {
  #database;
  #bookmark;
  static #expose = (() => {
    __d1Hooks.sessionQuery = (session, statements) =>
      session.#query(statements);
    __d1Hooks.sessionBatch = (session, statements) =>
      session.#batch(statements);
  })();
  constructor(database, constraintOrBookmark) {
    this.#database = database;
    const value = constraintOrBookmark == null
      ? "first-unconstrained"
      : String(constraintOrBookmark).trim();
    if (!value) {
      throw new Error("D1_SESSION_ERROR: invalid bookmark or constraint");
    }
    this.#bookmark =
      value === "first-primary" || value === "first-unconstrained"
        ? null : value;
  }
  #advanceBookmark() {
    this.#bookmark = __d1PrimaryBookmark;
  }
  async #query(statements) {
    const results = await __d1Query(this.#database, statements);
    this.#advanceBookmark();
    return results;
  }
  prepare(sql) {
    return new D1PreparedStatement(this, String(sql), []);
  }
  async #batch(statements) {
    const results = await __d1Batch(this.#database, statements);
    this.#advanceBookmark();
    return results;
  }
  async batch(statements) {
    return await this.#batch(statements);
  }
  getBookmark() {
    return this.#bookmark;
  }
}

// Named so SDKs that sniff a binding by `constructor.name` recognise it.
// D1 binding state and the cell RPC are private members; workerd's interface
// exposes prepare/batch/exec/dump/withSession and the wrapped-binding tag only
// (#206). The wrapped-binding prototype level is what workerd's D1 database
// actually reflects: D1Database.prototype's parent is WrappedBinding.prototype,
// which carries the tag the instance reports.
const __d1Query = (target, statements) =>
  target instanceof D1Database
    ? __d1Hooks.query(target, statements)
    : __d1Hooks.sessionQuery(target, statements);
const __d1Batch = (target, statements) =>
  target instanceof D1Database
    ? __d1Hooks.batch(target, statements)
    : __d1Hooks.sessionBatch(target, statements);
class D1Database {
  #databaseName;
  static #expose = (() => {
    __d1Hooks.query = (db, statements) => db.#query(statements);
    __d1Hooks.batch = (db, statements) => db.#batch(statements);
    // Test-only seam for the operator-gate suites; a shipping node has no
    // such property, and the shape gate runs without CELLO_TEST_WAIT.
    if (__test_wait_enabled()) {
      Object.defineProperty(D1Database.prototype, "_stub", {
        configurable: true, get() { return this.#stub; },
      });
    }
  })();
  constructor(databaseName) {
    this.#databaseName = databaseName;
  }
  // Resolved per call rather than cached: a cell can move between calls, and
  // `getByName` is the same cost the Durable Object path already pays.
  get #stub() {
    return __cell.makeNamespace("__D1Database").getByName(this.#databaseName);
  }
  async #query(statements) {
    try {
      return await this.#stub.__d1Query(statements);
    } catch (error) {
      if (__d1IsFamilyError(error && error.message)) throw error;
      throw __d1Error(String(error && error.message || error), error);
    }
  }
  async #batch(statements) {
    // An empty statement set ran nothing, and upstream says so rather than
    // answering an empty result a caller reads as "every statement
    // succeeded".
    if (statements.length === 0) throw __d1Error("No SQL statements detected.");
    try {
      const encoded = statements.map((statement) => ({
        sql: __d1Statement(statement).sql,
        params: __d1Statement(statement).params,
      }));
      const results = await this.#stub.__d1Batch(encoded);
      return results.map(__d1PublicResult);
    } catch (error) {
      if (__d1IsFamilyError(error && error.message)) throw error;
      throw __d1Error(String(error && error.message || error), error);
    }
  }
  prepare(sql) {
    return new D1PreparedStatement(this, String(sql), []);
  }
  // Workerd defines query() but refuses it until the d1_binding_jsrpc compat
  // flag is on; a cell does not implement that flag, so it refuses exactly as
  // workerd does here.
  query(_sql) {
    throw __d1Error(
      "D1Database.query() requires the d1_binding_jsrpc compatibility " +
      "flag. Enable d1_binding_jsrpc before calling query().");
  }
  async exec(sql) {
    try {
      return await this.#stub.__d1Exec(String(sql));
    } catch (error) {
      if (__d1IsFamilyError(error && error.message)) throw error;
      throw __d1Error(String(error && error.message || error), error);
    }
  }
  async batch(statements) {
    return await this.#batch(statements);
  }
  withSession(constraintOrBookmark) {
    return new D1DatabaseSession(this, constraintOrBookmark);
  }
  async dump() {
    throw __d1Error(
      "dump() is not implemented in cello; the database is a SQLite file in " +
        "your own bucket",
    );
  }
}

// Workerd's shared parent for bindings that wrap a host object.
function WrappedBinding() {}
Object.defineProperty(WrappedBinding.prototype, Symbol.toStringTag, {
  value: "WrappedBinding", configurable: true, writable: true,
  enumerable: false,
});
Object.setPrototypeOf(D1Database.prototype, WrappedBinding.prototype);

const __makeD1Database = __cello.__makeD1Database =
  (databaseName) => new D1Database(databaseName);
// ---- Workflows ----------------------------------------------------------
// A workflow instance is one cell of the runtime-supplied `__Workflow` class,
// named `<workflow_name>/<instance_id>` through getByName, so it inherits
// ownership, fencing, LTX replication and the output gate from the cell it
// already is. Execution is replay: `run()` re-executes from the top on every
// resume and only steps are memoized, in `ctx.storage` KV under `__wf.*`.
// This half is the server; `__makeWorkflow` below is the client the binding
// hands to a Worker. The public contract is in docs/cloudflare-compat.md.

// Upstream's limit for a step return, an event payload, and the params. The
// error is distinct and names the step, because "too large" and "the isolate
// died" must not read the same (the D1 row-cap precedent).
const __WF_VALUE_CAP = 1 << 20;
// Workflow names, instance ids, and event types all share this charset.
// `/` is outside it, which is what makes the `<name>/<id>` cell-name join
// unambiguous.
const __WF_NAME_RE = /^[a-zA-Z0-9_][a-zA-Z0-9-_]*$/;
const __wfError = (message) => new Error("WORKFLOW_ERROR: " + message);
// The suspension primitive: a blocked step returns a promise that never
// settles in this invocation, so user code simply stops progressing. A
// sentinel thrown through user code was rejected because `Promise.all` in
// `run()` would observe it as a rejection and user try/catch would eat it.
const __wfNever = new Promise(() => {});
const __wfTerminal = (status) =>
  status === "complete" || status === "errored" || status === "terminated";
// Upstream's hard bounds: 10,000 retries per step, 365 days for any sleep or
// event wait, one second under a waitForEvent timeout. Enforced loudly at the
// call site -- a config outside them would otherwise misbehave only after
// the instance is minutes or days into its run.
const __WF_RETRY_LIMIT_CAP = 10000;
const __WF_STEP_NAME_LIMIT = 256;
const __WF_MAX_WAIT_MS = 365 * 86400000;
const __WF_MIN_EVENT_TIMEOUT_MS = 1000;
// Cello has no account plans, so it exposes the larger published retention
// ceiling as one stable application limit. Both an omitted policy and an
// omitted member of a partial policy use this value.
const __WF_RETENTION_MAX_MS = 30 * 86400000;
const __WF_LOCATION_HINTS = new Set([
  "wnam", "enam", "sam", "weur", "eeur", "apac", "apac-ne", "apac-se",
  "oc", "afr", "me",
]);
// How long run() can stay pending on non-step work while no step runs and
// none is blocked before the instance fails. Upstream permits un-stepped
// awaits between steps (they are merely non-durable), so erroring at the
// first quiescent macrotask would kill any run() whose external await
// outlives one turn of the loop. Sixty seconds is long enough for any
// sane un-stepped await and short enough that a truly never-settling
// promise fails the instance instead of wedging the alarm handler.
const __WF_STALL_GRACE_MS = 60000;
const __WF_UNITS = {
  second: 1000,
  minute: 60000,
  hour: 3600000,
  day: 86400000,
  week: 604800000,
  // Upstream does not document its month/year arithmetic; fixed 30/365-day
  // units are used so one expression cannot mean two durations depending on
  // when it runs.
  month: 30 * 86400000,
  year: 365 * 86400000,
};
const __wfDuration = (value, what) => {
  if (typeof value === "number" && Number.isFinite(value) && value >= 0) return value;
  if (typeof value !== "string") {
    throw __wfError(
      `invalid duration for ${what}: ${JSON.stringify(value)}; use milliseconds ` +
        'or "<n> <second|minute|hour|day|week|month|year>[s]"',
    );
  }
  const match = /^\s*(\d+(?:\.\d+)?)\s+(second|minute|hour|day|week|month|year)s?\s*$/
    .exec(value);
  if (!match) {
    throw __wfError(
      `invalid duration for ${what}: ${JSON.stringify(value)}; use milliseconds ` +
        'or "<n> <second|minute|hour|day|week|month|year>[s]"',
    );
  }
  return Number(match[1]) * __WF_UNITS[match[2]];
};
const __wfRetentionDuration = (value, what) => {
  const durationMs = __wfDuration(value, what);
  if (durationMs > __WF_RETENTION_MAX_MS) {
    throw __wfError(
      `${what} is ${durationMs} ms, above cello's 30-day retention limit`,
    );
  }
  return durationMs;
};
const __wfOwnObject = (value) =>
  value !== null && typeof value === "object" && !Array.isArray(value);
const __wfUnknownKey = (value, allowed) =>
  Object.keys(value).find((key) => !allowed.includes(key));
const __wfStepName = (name) => {
  // The pinned workers-sdk validator has no minimum length. An empty name is
  // therefore valid, although rejecting it looks safer at first glance. A
  // non-empty check made cello reject Worker code that the upstream runtime
  // accepts, so keep only the upstream length and control-character bounds.
  if (
    typeof name !== "string" || name.length > __WF_STEP_NAME_LIMIT ||
    /[\x00-\x1f]/.test(name)
  ) {
    throw __wfError(
      `invalid step name ${JSON.stringify(name)}: use at most ` +
        `${__WF_STEP_NAME_LIMIT} characters and no control characters`,
    );
  }
};
const __wfEventType = (type) => {
  if (typeof type !== "string" || type.length > 100 || !__WF_NAME_RE.test(type)) {
    throw __wfError(
      `invalid event type ${JSON.stringify(type)}: use at most 100 letters, ` +
        "digits, hyphens, or underscores",
    );
  }
};
const __wfSleepDuration = (duration, name) => {
  const durationMs = __wfDuration(
    duration,
    `the duration of sleep ${JSON.stringify(name)}`,
  );
  if (durationMs > __WF_MAX_WAIT_MS) {
    throw __wfError(
      `sleep ${JSON.stringify(name)} is ${durationMs} ms, above the upstream ` +
        "limit of 365 days",
    );
  }
  return durationMs;
};
const __wfSleepUntil = (timestamp, name) => {
  const at = timestamp instanceof Date ? timestamp.getTime() : timestamp;
  if (typeof at !== "number" || !Number.isFinite(at)) {
    throw __wfError(
      `invalid sleepUntil timestamp for ${JSON.stringify(name)}: use a Date ` +
        "or a UNIX timestamp in milliseconds",
    );
  }
  const delay = at - Date.now();
  if (delay > __WF_MAX_WAIT_MS) {
    throw __wfError(
      `sleepUntil ${JSON.stringify(name)} is ${delay} ms away, above the ` +
        "upstream limit of 365 days",
    );
  }
  return at;
};
const __wfWaitOptions = (options, name) => {
  if (!__wfOwnObject(options)) {
    throw __wfError(`invalid waitForEvent options for ${JSON.stringify(name)}`);
  }
  __wfEventType(options.type);
  const timeout = options.timeout ?? "24 hours";
  const timeoutMs = __wfDuration(
    timeout,
    `the timeout of waitForEvent ${JSON.stringify(name)}`,
  );
  if (timeoutMs < __WF_MIN_EVENT_TIMEOUT_MS || timeoutMs > __WF_MAX_WAIT_MS) {
    throw __wfError(
      `the timeout of waitForEvent ${JSON.stringify(name)} is ${timeoutMs} ms; ` +
        "upstream allows 1 second to 365 days",
    );
  }
  return { type: options.type, timeout, timeoutMs };
};
const __wfEventOptions = (options) => {
  if (!__wfOwnObject(options)) {
    throw __wfError("sendEvent() needs an event object");
  }
  __wfEventType(options.type);
  if (options.payload !== undefined) __wfCheckValue(options.payload, "the event payload");
  return { type: options.type, payload: options.payload };
};
const __wfRetentionOptions = (retention, what) => {
  if (retention === undefined) {
    return {
      successMs: __WF_RETENTION_MAX_MS,
      errorMs: __WF_RETENTION_MAX_MS,
    };
  }
  if (!__wfOwnObject(retention)) {
    throw __wfError(`${what} option retention must be an object`);
  }
  const unknown = __wfUnknownKey(retention, ["successRetention", "errorRetention"]);
  if (unknown !== undefined) {
    throw __wfError(
      `${what} option retention has an unknown field ${JSON.stringify(unknown)}`,
    );
  }
  return {
    successMs: retention.successRetention === undefined
      ? __WF_RETENTION_MAX_MS
      : __wfRetentionDuration(
        retention.successRetention,
        `${what} successRetention`,
      ),
    errorMs: retention.errorRetention === undefined
      ? __WF_RETENTION_MAX_MS
      : __wfRetentionDuration(retention.errorRetention, `${what} errorRetention`),
  };
};
const __wfLocationHint = (hint, what) => {
  if (hint !== undefined && !__WF_LOCATION_HINTS.has(hint)) {
    throw __wfError(
      `${what} has invalid locationHint ${JSON.stringify(hint)}`,
    );
  }
  return hint;
};
const __wfRestartOptions = (options) => {
  if (options === undefined) return {};
  if (!__wfOwnObject(options)) {
    throw __wfError("restart() options must be an object");
  }
  const unknown = __wfUnknownKey(options, ["from"]);
  if (unknown !== undefined) {
    throw __wfError(`restart() has an unknown option ${JSON.stringify(unknown)}`);
  }
  if (options.from === undefined) return {};
  const from = options.from;
  if (!__wfOwnObject(from)) {
    throw __wfError("restart() option from must be an object");
  }
  const fromUnknown = __wfUnknownKey(from, ["name", "count", "type"]);
  if (fromUnknown !== undefined) {
    throw __wfError(
      `restart() option from has an unknown field ${JSON.stringify(fromUnknown)}`,
    );
  }
  __wfStepName(from.name);
  const count = from.count ?? 1;
  if (!Number.isInteger(count) || count < 1) {
    throw __wfError("restart() option from.count must be a positive integer");
  }
  const type = from.type ?? "do";
  if (!["do", "sleep", "waitForEvent"].includes(type)) {
    throw __wfError(
      'restart() option from.type must be "do", "sleep", or "waitForEvent"',
    );
  }
  return { from: { name: from.name, count, type } };
};
const __wfStepConfig = (config, name) => {
  if (!__wfOwnObject(config)) {
    throw __wfError(`invalid config for step ${JSON.stringify(name)}: use an object`);
  }
  let unknown = __wfUnknownKey(config, ["retries", "timeout", "sensitive"]);
  if (unknown !== undefined) {
    throw __wfError(
      `invalid config for step ${JSON.stringify(name)}: unknown option ` +
        JSON.stringify(unknown),
    );
  }
  const retries = config.retries;
  if (retries !== undefined) {
    if (!__wfOwnObject(retries)) {
      throw __wfError(`invalid retries config for step ${JSON.stringify(name)}`);
    }
    unknown = __wfUnknownKey(retries, ["limit", "delay", "backoff"]);
    if (unknown !== undefined) {
      throw __wfError(
        `invalid retries config for step ${JSON.stringify(name)}: unknown option ` +
          JSON.stringify(unknown),
      );
    }
    if (
      typeof retries.limit !== "number" || !Number.isFinite(retries.limit) ||
      retries.limit < 0
    ) {
      throw __wfError(
        `invalid retries config for step ${JSON.stringify(name)}: ` +
          "limit must be a non-negative number",
      );
    }
    if (retries.limit > __WF_RETRY_LIMIT_CAP) {
      throw __wfError(
        `invalid retries config for step ${JSON.stringify(name)}: limit is above ` +
          String(__WF_RETRY_LIMIT_CAP),
      );
    }
    if (typeof retries.delay !== "function") {
      __wfDuration(retries.delay, `the retries config delay of step ${JSON.stringify(name)}`);
    }
    if (
      retries.backoff !== undefined &&
      !["constant", "linear", "exponential"].includes(retries.backoff)
    ) {
      throw __wfError(
        `invalid retries config for step ${JSON.stringify(name)}: ` +
          "backoff must be constant, linear, or exponential",
      );
    }
  }
  if (config.timeout !== undefined) {
    const timeout = __wfDuration(
      config.timeout,
      `the config timeout of step ${JSON.stringify(name)}`,
    );
    if (timeout === 0) {
      throw __wfError(
        `invalid config timeout for step ${JSON.stringify(name)}: use a value above zero`,
      );
    }
  }
  if (config.sensitive !== undefined && config.sensitive !== "output") {
    throw __wfError(
      `invalid config sensitive value for step ${JSON.stringify(name)}: use "output"`,
    );
  }
  return {
    ...config,
    ...(retries === undefined ? {} : { retries: { ...retries } }),
  };
};
// Serialization is checked with the same encoder storage and RPC use, so a
// value that passes here cannot fail later at the isolate boundary. The
// thrown errors are permanent: retrying cannot make a value cloneable or
// shrink it.
const __wfEncodeValue = (value, what) => {
  try {
    return __scEncodeStrict(value);
  } catch (error) {
    const failure = __wfError(
      `${what} is not serializable: ${String(error && error.message || error)}. ` +
        "It must survive structured clone; a ReadableStream return is not implemented in cello.",
    );
    failure.__wfPermanent = true;
    throw failure;
  }
};
const __wfCheckEncodedValue = (bytes, what) => {
  if (bytes.byteLength > __WF_VALUE_CAP) {
    const failure = __wfError(
      `${what} is ${bytes.byteLength} bytes, above the 1 MiB limit; ` +
        "store large output externally and return a reference",
    );
    failure.__wfPermanent = true;
    throw failure;
  }
};
const __wfCheckValue = (value, what) =>
  __wfCheckEncodedValue(__wfEncodeValue(value, what), what);
const __wfErrorRecord = (error) => ({
  name: String(error && error.name || "Error"),
  message: String(error && error.message || error),
});
// A permanently failed step rejects on every replay, so user try/catch
// around step.do keeps working after a resume, not only on the attempt that
// failed live.
const __wfReviveError = (record) => {
  const error = new Error(record.message);
  error.name = record.name;
  return error;
};
const __wfLedgerPrefix = (generation) => `__wf.${generation}.`;
const __wfEventPrefix = (generation) =>
  __wfLedgerPrefix(generation) + "event.";
const __wfEventKey = (generation, sequence) =>
  __wfEventPrefix(generation) + String(sequence).padStart(9, "0");
const __wfRetentionMs = (meta, status) =>
  status === "complete"
    ? (meta.retention?.successMs ?? __WF_RETENTION_MAX_MS)
    : (meta.retention?.errorMs ?? __WF_RETENTION_MAX_MS);
const __wfFinishRetention = (meta, transaction) => {
  meta.endedMs = Date.now();
  meta.expiresMs = meta.endedMs + __wfRetentionMs(meta, meta.status);
  transaction.setAlarm(meta.expiresMs);
};
// A ledger record replays only through the kind of call that wrote it. A
// `do` renamed into a `sleep`, or two calls reordered under one name, would
// otherwise read fields the other kind never wrote and continue on
// undefined -- a deadline of NaN, a value of nothing -- instead of failing.
const __wfKindCheck = (record, kind, name) => {
  if (record === undefined || record.kind === kind) return record;
  const failure = __wfError(
    `step ${JSON.stringify(name)} replays as ${kind} but its ledger record ` +
      `was written by ${record.kind}; a step keeps one kind across replays`,
  );
  failure.__wfPermanent = true;
  throw failure;
};
// Each step callback runs under an async-context frame naming its step (the
// same CPED-backed frame __ctxRun uses, so it survives the callback's
// awaits). A step started inside another step's callback would wedge
// quiescence: the outer callback cannot settle while it awaits the blocked
// inner step, so `running` never reaches zero and the instance can neither
// suspend nor finish. The frame is what lets the inner call refuse loudly
// at the nesting site instead.
const __wfStepKey = Symbol("cello.wfStep");
const __wfEnclosingStep = () => {
  const frame = __als_get();
  return frame === undefined ? undefined : frame.get(__wfStepKey);
};
const __wfEnterStep = (name, fn) => {
  const prior = __als_get();
  const frame = new Map(prior);
  frame.set(__wfStepKey, name);
  __als_set(frame);
  try {
    return fn();
  } finally {
    __als_set(prior);
  }
};

// Race an attempt against its deadline. The abandoned callback keeps
// running -- there is no cancellation to send it -- but its result is
// discarded and the timeout counts as a failed attempt, as upstream.
const __wfTimeout = (promise, ms, name, attempt) =>
  new Promise((resolve, reject) => {
    const timer = setTimeout(
      () =>
        reject(new Error(
          `step ${JSON.stringify(name)} timed out after ${ms} ms (attempt ${attempt})`,
        )),
      ms,
    );
    promise.then(
      (value) => { clearTimeout(timer); resolve(value); },
      (error) => { clearTimeout(timer); reject(error); },
    );
  });

// Storage failures belong to the engine even when workflow code catches the
// rejected step promise. Record them outside the Error object, so user code
// cannot suppress a retry or forge one by returning a marked exception.
const __wfTrackStorageFailures = (storage, driver) => {
  const tracked = Object.create(storage);
  for (const name of ["get", "put", "transactionSync"]) {
    Object.defineProperty(tracked, name, {
      value(...args) {
        try {
          const result = storage[name](...args);
          if (result !== null && typeof result === "object" &&
            typeof result.then === "function") {
            return result.catch((error) => {
              driver.storageFailure ??= error;
              throw error;
            });
          }
          return result;
        } catch (error) {
          driver.storageFailure ??= error;
          throw error;
        }
      },
    });
  }
  return tracked;
};

// The WorkflowStep the driver hands to run(). Each method resolves its step
// record by (name, occurrence count) -- the count is assigned synchronously
// at call time, so `Promise.all` of two same-named steps gets deterministic
// distinct keys in call order, and a repeated name in a loop cannot collide.
const __wfMakeStep = (driver) => {
  const storage = driver.storage;
  // The shared step frame. `body` resolves to {value} or {block: at, kind};
  // a blocked step leaves its promise unsettled forever in this invocation.
  // The running counter covers the whole body, ledger reads included, so
  // quiescence cannot fire between a step's read and its decision.
  const frame = (name, body) => {
    // A stale replay -- one whose invocation already suspended -- can wake
    // later if user code raced a step against an un-stepped await. It goes
    // quiet instead of executing an attempt beside the fresh replay.
    if (driver.finished) return __wfNever;
    const enclosing = __wfEnclosingStep();
    if (enclosing !== undefined) {
      const failure = __wfError(
        `step ${JSON.stringify(name)} cannot start inside the callback of ` +
          `step ${JSON.stringify(enclosing)}; a step callback must not use ` +
          "the step API",
      );
      failure.__wfPermanent = true;
      return Promise.reject(failure);
    }
    const count = (driver.counts.get(name) ?? 0) + 1;
    driver.counts.set(name, count);
    const ordinal = ++driver.ordinal;
    const key = driver.ledgerPrefix + `step.${count}.${name}`;
    // Step activity restarts the stall clock: a pending grace timer was
    // armed against a run() that looked stuck, and this call is progress.
    driver.clearStall();
    driver.running += 1;
    return new Promise((resolve, reject) => {
      (async () => {
        const meta = await storage.get("__wf.meta");
        if (
          meta === undefined || meta.generation !== driver.generation ||
          __wfTerminal(meta.status) || meta.status === "paused" ||
          meta.status === "waitingForPause"
        ) {
          return { pause: true };
        }
        return await body(key, count, ordinal);
      })().then(
        (result) => {
          driver.running -= 1;
          driver.clearStall();
          if (result.pause === true) {
            driver.pauseRequested = true;
            driver.check();
            return;
          }
          if (result.block !== undefined) {
            if (!driver.finished) {
              driver.blocked.push({ at: result.block, kind: result.kind });
            }
            driver.check();
            return;
          }
          driver.check();
          resolve(result.value);
        },
        (error) => {
          driver.running -= 1;
          driver.clearStall();
          driver.check();
          reject(error);
        },
      );
    });
  };
  const doStep = (name, configOrCallback, callbackOrRollback, maybeRollback) => {
    const hasConfig = typeof configOrCallback !== "function";
    let config = hasConfig ? (configOrCallback ?? {}) : {};
    const callback = hasConfig ? callbackOrRollback : configOrCallback;
    const rollback = hasConfig ? maybeRollback : callbackOrRollback;
    try {
      __wfStepName(name);
      config = __wfStepConfig(config, name);
    } catch (error) {
      return Promise.reject(error);
    }
    if (rollback !== undefined) {
      return Promise.reject(__wfError(
        "step rollbackOptions are not implemented in cello; remove the rollback handler",
      ));
    }
    if (config.sensitive !== undefined) {
      return Promise.reject(__wfError(
        "sensitive step output is not implemented in cello; remove `sensitive`",
      ));
    }
    if (typeof callback !== "function") {
      return Promise.reject(__wfError("step.do needs a callback function"));
    }
    return frame(name, async (key, count, ordinal) => {
      const record = __wfKindCheck(await storage.get(key), "do", name);
      if (record !== undefined && record.status === "completed") {
        return { value: record.value };
      }
      if (record !== undefined && record.status === "failed") {
        throw __wfReviveError(record.error);
      }
      // A pending retry whose backoff has not elapsed blocks without
      // executing; the deadline was persisted when the attempt failed, so a
      // replay cannot reset the backoff.
      if (record !== undefined && record.nextAt > Date.now()) {
        return { block: record.nextAt, kind: "retry" };
      }
      const attempt = (record === undefined ? 0 : record.attempt) + 1;
      const historyOrdinal = record?.ordinal ?? ordinal;
      const retries = config.retries ??
        { limit: 5, delay: 10000, backoff: "exponential" };
      const timeout = config.timeout ?? "10 minutes";
      const timeoutMs = __wfDuration(
        timeout,
        `the timeout of step ${JSON.stringify(name)}`,
      );
      // Upstream omits a dynamic delay from the step context. Copying the
      // config verbatim would expose the callback to itself as retry policy,
      // while callers expect only the serializable limit and backoff fields.
      const contextRetries = typeof retries.delay === "function"
        ? { limit: retries.limit, ...(retries.backoff === undefined
          ? {}
          : { backoff: retries.backoff }) }
        : retries;
      const context = {
        step: { name, count },
        attempt,
        config: { retries: contextRetries, timeout },
      };
      try {
        // Persist the history entry before the callback starts. A concurrent
        // selected restart can therefore name an in-flight occurrence, and
        // its new generation fences the callback's eventual old-ledger write.
        await storage.put(key, {
          kind: "do",
          status: "running",
          attempt: attempt - 1,
          ordinal: historyOrdinal,
        });
        const value = await __wfTimeout(
          __wfEnterStep(name, async () => callback(context)),
          timeoutMs,
          name,
          attempt,
        );
        if (value !== undefined) {
          __wfCheckValue(value, `the return value of step ${JSON.stringify(name)}`);
        }
        await storage.put(key, {
          kind: "do",
          status: "completed",
          value,
          ordinal: historyOrdinal,
        });
        return { value };
      } catch (error) {
        // NonRetryableError skips retries by contract; a value-cap or
        // serialization failure is marked permanent above because another
        // attempt returns the same value.
        const permanent = error instanceof NonRetryableError ||
          (error !== null && typeof error === "object" &&
            (error.name === "NonRetryableError" || error.__wfPermanent === true)) ||
          attempt > retries.limit;
        if (permanent) {
          await storage.put(key, {
            kind: "do",
            status: "failed",
            error: __wfErrorRecord(error),
            ordinal: historyOrdinal,
          });
          throw error;
        }
        // Upstream documents the backoff names, not the arithmetic; the
        // simplest reading is used and recorded in the design page. When
        // `retries` is given without `backoff`, cello uses constant -- the
        // documented exponential default is the whole-config-omitted case.
        const dynamic = typeof retries.delay === "function";
        const delay = dynamic
          ? await __wfEnterStep(name, () => retries.delay({ ctx: context, error }))
          : retries.delay;
        const base = __wfDuration(
          delay,
          `the retry delay of step ${JSON.stringify(name)}`,
        );
        // A delay callback replaces the fixed base duration, not the backoff
        // policy. Skipping this factor makes a dynamic exponential or linear
        // policy retry earlier than the same policy with a fixed base.
        const factor = retries.backoff === "exponential"
          ? 2 ** (attempt - 1)
          : retries.backoff === "linear"
          ? attempt
          : 1;
        const nextAt = Date.now() + base * factor;
        await storage.put(key, {
          kind: "do",
          status: "retrying",
          attempt,
          nextAt,
          ordinal: historyOrdinal,
        });
        return { block: nextAt, kind: "retry" };
      }
    });
  };
  const sleepUntilDeadline = (name, key, ordinal, deadline) => (async () => {
    const record = __wfKindCheck(await storage.get(key), "sleep", name);
    if (record !== undefined && record.status === "completed") {
      return { value: undefined };
    }
    const historyOrdinal = record?.ordinal ?? ordinal;
    let at = record === undefined ? undefined : record.deadline;
    if (at === undefined) {
      // The absolute deadline is computed once and persisted. Recomputing
      // from "now" on replay would extend the sleep by however long the
      // instance was down, so a crash could postpone a deadline forever.
      at = deadline();
      if (at - Date.now() > __WF_MAX_WAIT_MS) {
        throw __wfError(
          `sleep ${JSON.stringify(name)} ends ${at - Date.now()} ms from now, ` +
            "above the upstream limit of 365 days",
        );
      }
      await storage.put(key, {
        kind: "sleep",
        status: "sleeping",
        deadline: at,
        ordinal: historyOrdinal,
      });
    }
    if (at <= Date.now()) {
      await storage.put(key, {
        kind: "sleep",
        status: "completed",
        ordinal: historyOrdinal,
      });
      return { value: undefined };
    }
    return { block: at, kind: "sleep" };
  })();
  return {
    do: doStep,
    sleep: (name, duration) => {
      let durationMs;
      try {
        __wfStepName(name);
        durationMs = __wfSleepDuration(duration, name);
      } catch (error) {
        return Promise.reject(error);
      }
      return frame(name, (key, _count, ordinal) =>
        sleepUntilDeadline(
          name,
          key,
          ordinal,
          () => Date.now() + durationMs,
        ));
    },
    sleepUntil: (name, timestamp) => {
      let at;
      try {
        __wfStepName(name);
        at = __wfSleepUntil(timestamp, name);
      } catch (error) {
        return Promise.reject(error);
      }
      return frame(name, (key, _count, ordinal) =>
        sleepUntilDeadline(name, key, ordinal, () => {
          // A replay uses the persisted deadline after it has passed. Check
          // the moving future bound only while the deadline is first installed,
          // or every successful sleep would fail on the resume that completes it.
          if (at - Date.now() < 0) {
            throw __wfError(
              `sleepUntil ${JSON.stringify(name)} needs a future timestamp`,
            );
          }
          return at;
        }));
    },
    waitForEvent: (name, options) => {
      let checked;
      try {
        __wfStepName(name);
        checked = __wfWaitOptions(options, name);
      } catch (error) {
        return Promise.reject(error);
      }
      return frame(name, async (key, _count, ordinal) => {
        let record = __wfKindCheck(await storage.get(key), "event", name);
        if (record !== undefined && record.status === "completed") {
          return { value: record.value };
        }
        if (record !== undefined && record.status === "failed") {
          throw __wfReviveError(record.error);
        }
        if (record === undefined) {
          record = {
            kind: "event",
            status: "waiting",
            type: checked.type,
            deadline: Date.now() + checked.timeoutMs,
            ordinal,
          };
          await storage.put(key, record);
        }
        // The deadline is checked before the buffer: an event that arrived
        // after the persisted deadline must time the step out, not succeed
        // because the replay that noticed it ran late.
        if (record.deadline <= Date.now()) {
          const error = new Error(
            `waitForEvent ${JSON.stringify(name)} timed out waiting for an event ` +
              `of type ${JSON.stringify(record.type)}`,
          );
          await storage.put(key, {
            kind: "event",
            status: "failed",
            error: __wfErrorRecord(error),
            ordinal: record.ordinal,
          });
          throw error;
        }
        // Delivered events persist until a matching step consumes one, in
        // arrival order -- the zero-padded sequence key makes list order
        // arrival order -- so an event sent before the step is reached is
        // buffered, not lost.
        const consumed = storage.transactionSync(() => {
          const current = __wfKindCheck(storage.kv.get(key), "event", name);
          if (current !== undefined && current.status === "completed") {
            return { value: current.value };
          }
          if (current !== undefined && current.status === "failed") {
            throw __wfReviveError(current.error);
          }
          for (const [eventKey, event] of storage.kv.list({
            prefix: __wfEventPrefix(driver.generation),
          })) {
            if (event.type !== current.type) continue;
            const value = {
              payload: event.payload,
              timestamp: new Date(event.timestampMs),
              type: event.type,
            };
            // The delete and completed ledger record are one SQLite commit.
            // A crash can therefore expose both or neither, so an
            // acknowledged event cannot disappear between them.
            storage.kv.delete(eventKey);
            /*__CELLO_TEST_WORKFLOW_EVENT_CONSUMED__*/
            storage.kv.put(key, {
              kind: "event",
              status: "completed",
              value,
              ordinal: current.ordinal,
            });
            return { value };
          }
        });
        if (consumed !== undefined) return consumed;
        return { block: record.deadline, kind: "event" };
      });
    },
  };
};

const __WorkflowCell = (() => {
  class StorageTransaction {
    constructor(storage) {
      this.kv = storage.kv;
      this._scope = storage._scope;
    }
    setAlarm(t) {
      __alarm_set(this._scope, t instanceof Date ? t.getTime() : Number(t));
    }
    deleteAlarm() {
      __alarm_delete(this._scope);
    }
  }
  const transactionSync = (storage, callback) =>
    storage.transactionSync(() => callback(new StorageTransaction(storage)));
  const clearInstance = (transaction) => {
    for (const [key] of transaction.kv.list({ prefix: "__wf." })) {
      transaction.kv.delete(key);
    }
    transaction.deleteAlarm();
  };
  const expireTerminal = (transaction, meta, now) => {
    if (!__wfTerminal(meta.status)) return false;
    if (meta.expiresMs === undefined) {
      meta.endedMs = now;
      meta.expiresMs = now + __wfRetentionMs(meta, meta.status);
      transaction.kv.put("__wf.meta", meta);
    }
    if (meta.expiresMs > now) return false;
    clearInstance(transaction);
    return true;
  };

  // The adapter stays in this closure because Workflow needs synchronous
  // alarm writes inside transactionSync(). The public async alarm API runs
  // after that commit, while a global helper would let application code
  // split metadata from its wake or invoke the raw operation after commit.
  return class __WorkflowCell {
  constructor(state) {
    this._state = state;
  }
  async __wfCreate({
    workflowName,
    instanceId,
    params,
    retention,
    locationHint,
    skipExisting = false,
  }) {
    const storage = this._state.storage;
    if (params !== undefined) __wfCheckValue(params, "the workflow params");
    const result = transactionSync(storage, (transaction) => {
      let meta = transaction.kv.get("__wf.meta");
      if (meta !== undefined && expireTerminal(transaction, meta, Date.now())) {
        meta = undefined;
      }
      if (meta !== undefined && skipExisting) return { id: instanceId, created: false };
      if (meta !== undefined) {
        throw __wfError(
          `instance ${JSON.stringify(instanceId)} already exists with status ` +
            JSON.stringify(meta.status),
        );
      }
      const generation = crypto.randomUUID();
      transaction.kv.put("__wf.meta", {
        workflowName,
        instanceId,
        params,
        retention,
        ...(locationHint === undefined ? {} : { locationHint }),
        generation,
        createdMs: Date.now(),
        status: "queued",
      });
      // The metadata and alarm are one SQLite commit. A committed creation
      // therefore always has the wake that can start or recover its drive.
      transaction.setAlarm(Date.now());
      /*__CELLO_TEST_WORKFLOW_META_CREATED__*/
      return { id: instanceId, created: true };
    });
    return result;
  }
  async __wfStatus() {
    const meta = transactionSync(this._state.storage, (transaction) => {
      const current = transaction.kv.get("__wf.meta");
      if (current === undefined) return undefined;
      return expireTerminal(transaction, current, Date.now()) ? undefined : current;
    });
    // A cell exists on first address like any cell, so an absent ledger is
    // the only honest "no such instance" signal. Inventing an instance here
    // would turn every typo into an empty workflow.
    if (meta === undefined) throw __wfError("instance does not exist");
    const status = { status: meta.status, rollback: null };
    if (meta.error !== undefined) status.error = meta.error;
    if (meta.output !== undefined) status.output = meta.output;
    return status;
  }
  async __wfDelete() {
    const storage = this._state.storage;
    const deleted = transactionSync(storage, (transaction) => {
      const meta = transaction.kv.get("__wf.meta");
      if (meta === undefined) return false;
      if (expireTerminal(transaction, meta, Date.now())) return false;
      clearInstance(transaction);
      return true;
    });
    // Throw after the transaction commits. An expiry discovered here must
    // keep its cleanup even though delete() reports that the instance is gone.
    if (!deleted) throw __wfError("instance does not exist");
    // A callback can still be awaiting application I/O when deletion lands.
    // Reset the object after the deletion commit so that callback cannot
    // recreate an old-generation ledger entry after delete() reports success.
    this._state.abort(new Error("WORKFLOW_INSTANCE_DELETED"));
  }
  async __wfPause() {
    const storage = this._state.storage;
    transactionSync(storage, (transaction) => {
      const meta = transaction.kv.get("__wf.meta");
      if (meta === undefined) throw __wfError("instance does not exist");
      if (__wfTerminal(meta.status) || meta.status === "paused" ||
          meta.status === "waitingForPause") return;
      if (meta.status === "running") {
        meta.status = "waitingForPause";
        // A drive lost after this commit must re-enter and finish the pause.
        transaction.setAlarm(Date.now());
      } else {
        meta.status = "paused";
        meta.pausedMs = Date.now();
        transaction.deleteAlarm();
      }
      transaction.kv.put("__wf.meta", meta);
    });
  }
  async __wfResume() {
    const storage = this._state.storage;
    transactionSync(storage, (transaction) => {
      const meta = transaction.kv.get("__wf.meta");
      if (meta === undefined) throw __wfError("instance does not exist");
      if (meta.status === "waitingForPause") {
        meta.status = "running";
        transaction.kv.put("__wf.meta", meta);
        transaction.setAlarm(Date.now());
        return;
      }
      if (meta.status !== "paused") {
        throw __wfError(
          `cannot resume an instance with status ${JSON.stringify(meta.status)}`,
        );
      }
      const offset = Math.max(0, Date.now() - (meta.pausedMs ?? Date.now()));
      for (const [key, record] of transaction.kv.list({
        prefix: __wfLedgerPrefix(meta.generation) + "step.",
      })) {
        if (record.status === "retrying") record.nextAt += offset;
        if (record.status === "sleeping" || record.status === "waiting") {
          record.deadline += offset;
        }
        transaction.kv.put(key, record);
      }
      delete meta.pausedMs;
      meta.status = "queued";
      transaction.kv.put("__wf.meta", meta);
      transaction.setAlarm(Date.now());
    });
  }
  async __wfRestart(options) {
    const checked = __wfRestartOptions(options);
    const storage = this._state.storage;
    transactionSync(storage, (transaction) => {
      const meta = transaction.kv.get("__wf.meta");
      if (meta === undefined) throw __wfError("instance does not exist");
      const oldPrefix = __wfLedgerPrefix(meta.generation);
      let targetOrdinal;
      if (checked.from !== undefined) {
        const expectedKind = checked.from.type === "waitForEvent"
          ? "event"
          : checked.from.type;
        let occurrence = 0;
        const history = [...transaction.kv.list({ prefix: oldPrefix + "step." })]
          .filter(([, record]) => Number.isInteger(record.ordinal))
          .sort((left, right) => left[1].ordinal - right[1].ordinal);
        // The ledger key is `<prefix>step.<per-name-count>.<name>`. Derive
        // the name from it because a record stores replay state, not identity.
        for (const [key, record] of history) {
          const marker = oldPrefix + "step.";
          const suffix = key.slice(marker.length);
          const dot = suffix.indexOf(".");
          const name = dot < 0 ? "" : suffix.slice(dot + 1);
          if (record.kind !== expectedKind || name !== checked.from.name) continue;
          occurrence += 1;
          if (occurrence === checked.from.count) {
            targetOrdinal = record.ordinal;
            break;
          }
        }
        if (targetOrdinal === undefined) {
          throw __wfError(
            `restart() could not find ${checked.from.type} step ` +
              `${JSON.stringify(checked.from.name)} occurrence ${checked.from.count}`,
          );
        }
      }
      const generation = crypto.randomUUID();
      const newPrefix = __wfLedgerPrefix(generation);
      if (targetOrdinal !== undefined) {
        for (const [key, record] of transaction.kv.list({ prefix: oldPrefix + "step." })) {
          if (Number.isInteger(record.ordinal) && record.ordinal < targetOrdinal) {
            transaction.kv.put(newPrefix + key.slice(oldPrefix.length), record);
          }
        }
      }
      for (const [key] of transaction.kv.list({ prefix: oldPrefix })) {
        transaction.kv.delete(key);
      }
      meta.generation = generation;
      meta.status = "queued";
      delete meta.pausedMs;
      delete meta.output;
      delete meta.error;
      transaction.kv.put("__wf.meta", meta);
      transaction.setAlarm(Date.now());
    });
  }
  async __wfTerminate() {
    const storage = this._state.storage;
    transactionSync(storage, (transaction) => {
      const meta = transaction.kv.get("__wf.meta");
      if (meta === undefined) throw __wfError("instance does not exist");
      if (__wfTerminal(meta.status)) {
        throw __wfError(
          `cannot terminate an instance with status ${JSON.stringify(meta.status)}`,
        );
      }
      meta.status = "terminated";
      __wfFinishRetention(meta, transaction);
      transaction.kv.put("__wf.meta", meta);
      /*__CELLO_TEST_WORKFLOW_TERMINAL_ALARM_SET__*/
    });
  }
  async __wfSendEvent(options) {
    const storage = this._state.storage;
    // The binding validates first to avoid an unnecessary cell call. Repeat
    // the check here because this RPC owns the durable event invariant: no
    // current or future internal caller can bypass validation before storage.
    const { type, payload } = __wfEventOptions(options);
    transactionSync(storage, (transaction) => {
      const meta = transaction.kv.get("__wf.meta");
      if (meta === undefined) throw __wfError("instance does not exist");
      if (__wfTerminal(meta.status)) {
        throw __wfError(
          `cannot send an event to an instance with status ${JSON.stringify(meta.status)}`,
        );
      }
      const sequenceKey = __wfLedgerPrefix(meta.generation) + "eventSeq";
      const sequence = transaction.kv.get(sequenceKey) ?? 0;
      transaction.kv.put(sequenceKey, sequence + 1);
      transaction.kv.put(__wfEventKey(meta.generation, sequence), {
        type,
        payload,
        timestampMs: Date.now(),
      });
      // Allocation, the event body, and the nudge are one commit. Parallel
      // writers cannot reuse a sequence and a crash cannot expose a partial
      // acknowledged send.
      if (meta.status !== "paused" && meta.status !== "waitingForPause") {
        transaction.setAlarm(Date.now());
      }
    });
  }
  async alarm(info) {
    const storage = this._state.storage;
    // One drive at a time. A sendEvent nudge (setAlarm(now)) landing
    // mid-fire replaces the firing alarm in the core, and the replacement
    // can dispatch a second alarm() into this isolate while the first drive
    // still awaits user code. Two concurrent replays would both see the
    // same incomplete steps and double-execute their callbacks. The guard
    // is isolate-local and that is sufficient: every drive for a cell runs
    // in its owner's isolate, and a takeover starts a fresh isolate with no
    // drive in flight. The nudge is not lost -- the in-flight drive folds
    // `_wfNudge` into the minimum it re-arms, and if it suspended before
    // seeing the flag, the re-arm below starts a fresh drive immediately.
    // A failed drive is not re-raised here: the engine already retries the
    // alarm that fired it.
    if (this._wfDriving !== undefined) {
      // A dispatch after the previous turn died without settling (budget,
      // disconnect) is not a concurrent nudge: the in-flight drive is
      // orphaned. Its entry died without settling the drive, and the promise
      // it left behind only the dropped entry's ops could resolve. Awaiting
      // it would park this dispatch behind a promise that never settles, and
      // every redrive would fail the same way while the instance reads
      // "running" forever (denoland/celld#299). The host marks that death in
      // memory and hands it in as `isRedrive` on the next dispatch, whatever
      // triggered it; a racing nudge cannot clear it the way `setAlarm`
      // clears the stored retry counter to zero, so the counter alone cannot
      // tell this orphan from a live drive. Either signal drops the orphan:
      // the bit is authoritative, and a nonzero counter without the bit still
      // names a retry, which a live drive never owes. A nudge during a
      // healthy drive carries neither and still waits below, so two live
      // drives never double-execute steps. Forget the orphan and fall through
      // to a fresh drive; the ledger replay picks up where it left off.
      if (info !== undefined && (info.isRedrive === true || info.retryCount > 0)) {
        this._wfNudge = Date.now();
        this._wfDriving = undefined;
      } else {
        this._wfNudge = Date.now();
        await this._wfDriving.catch(() => {});
        if (this._wfNudge !== undefined) {
          this._wfNudge = undefined;
          const meta = await storage.get("__wf.meta");
          if (
            meta !== undefined && !__wfTerminal(meta.status) &&
            meta.status !== "paused" && meta.status !== "waitingForPause"
          ) {
            await storage.setAlarm(Date.now());
          }
        }
        return;
      }
    }
    const meta = await storage.get("__wf.meta");
    // A terminal instance's pending alarm can still fire. It owns retention:
    // remove expired state, or restore the expiry wake after a stale nudge.
    if (meta === undefined) {
      await storage.deleteAlarm();
      return;
    }
    if (__wfTerminal(meta.status)) {
      transactionSync(storage, (transaction) => {
        const current = transaction.kv.get("__wf.meta");
        if (current === undefined) {
          transaction.deleteAlarm();
          return;
        }
        if (!expireTerminal(transaction, current, Date.now())) {
          transaction.setAlarm(current.expiresMs);
        }
      });
      return;
    }
    if (meta.status === "paused") {
      await storage.deleteAlarm();
      return;
    }
    if (meta.status === "waitingForPause") {
      transactionSync(storage, (transaction) => {
        const current = transaction.kv.get("__wf.meta");
        if (current === undefined || current.generation !== meta.generation ||
            current.status !== "waitingForPause") return;
        current.status = "paused";
        current.pausedMs = Date.now();
        transaction.kv.put("__wf.meta", current);
        transaction.deleteAlarm();
      });
      return;
    }
    // The fired alarm stays armed while the drive runs. The engine consumes
    // it only when this handler returns cleanly, and retries it when the
    // handler fails -- that retry is the crash safety net. Deleting it here
    // would commit the deletion durably mid-drive (durability is per SQLite
    // commit, and replication ships frames per commit), so a SIGKILL before
    // the suspend-time re-arm would strand a "running" instance with no
    // alarm and no wake entry. The suspend path re-arms explicitly, and a
    // terminal drive deletes.
    const driving = this._drive(meta);
    this._wfDriving = driving;
    try {
      await driving;
    } finally {
      this._wfDriving = undefined;
    }
  }
  async _drive(meta) {
    const storage = this._state.storage;
    // The drive owns `status`, `output`, and `error`; every other piece of
    // instance state belongs to the RPCs (sendEvent's sequence lives in its
    // own key for the same reason). The gate is open at every await, so a
    // terminate can land mid-drive: re-reading before each write, and
    // writing nothing once the status is terminal, is what makes "terminate
    // wins the race" true rather than asserted. Writing back the meta this
    // drive entered with would resurrect a terminated instance as "waiting"
    // and re-arm the alarm its terminate deleted.
    const settle = (mutate) => transactionSync(storage, (transaction) => {
      const current = transaction.kv.get("__wf.meta");
      if (current === undefined || __wfTerminal(current.status) ||
          current.generation !== meta.generation) return false;
      mutate(current, transaction);
      transaction.kv.put("__wf.meta", current);
      return true;
    });
    const className = __cell.workflows[meta.workflowName];
    const cls = className === undefined ? undefined : __cf.exports[className];
    if (typeof cls !== "function" ||
        typeof (cls.prototype && cls.prototype.run) !== "function") {
      // Deploy cannot see inside the bundle, so a missing export fails
      // here, loudly, with the class and the config key named.
      settle((current, transaction) => {
        current.status = "errored";
        current.error = {
          name: "Error",
          message: className === undefined
            ? `workflow ${JSON.stringify(meta.workflowName)} is not declared ` +
              "in this deployment's `workflows`"
            : `workflow class ${JSON.stringify(className)} is not a module ` +
              "export with a run() method; export it and extend " +
              "WorkflowEntrypoint",
        };
        __wfFinishRetention(current, transaction);
        /*__CELLO_TEST_WORKFLOW_TERMINAL_ALARM_SET__*/
      });
      return;
    }
    const driver = {
      storage: undefined,
      storageFailure: undefined,
      generation: meta.generation,
      ledgerPrefix: __wfLedgerPrefix(meta.generation),
      counts: new Map(),
      ordinal: 0,
      running: 0,
      blocked: [],
      pauseRequested: false,
      finished: false,
      checkQueued: false,
      stallTimer: undefined,
      clearStall() {
        if (this.stallTimer !== undefined) {
          clearTimeout(this.stallTimer);
          this.stallTimer = undefined;
        }
      },
    };
    driver.storage = __wfTrackStorageFailures(storage, driver);
    const outcome = new Promise((resolve) => {
      driver.finish = (result) => {
        if (driver.finished) return;
        driver.finished = true;
        driver.clearStall();
        resolve(result);
      };
    });
    // Quiescence: when a step is blocked, no step callback is executing,
    // and a macrotask has passed, nothing can progress, so the invocation
    // suspends. The macrotask lets every microtask chain a completed step
    // unblocked drain first; checking on the spot would suspend an instance
    // whose next step is one `await` away from starting.
    driver.check = () => {
      if (driver.checkQueued || driver.finished) return;
      driver.checkQueued = true;
      setTimeout(() => {
        driver.checkQueued = false;
        if (driver.finished || driver.running !== 0) return;
        if (driver.pauseRequested) {
          driver.finish({ kind: "pause" });
          return;
        }
        if (driver.blocked.length > 0) {
          driver.finish({ kind: "suspend" });
          return;
        }
        // No step runs, none is blocked, and run() has not settled: it
        // awaits something that is not a step. Upstream permits that -- an
        // un-stepped `await fetch()` between steps is legal, merely
        // non-durable -- so this is not yet an error; but a promise that
        // never settles would wedge the alarm handler in silence, with the
        // instance "running" forever. A grace timer splits the two: if the
        // same nothing-can-progress state still holds when it fires, the
        // instance fails loudly. The timer is a plain in-isolate timeout,
        // not a durable alarm, on purpose: an eviction or a restart replays
        // run() from the top anyway, so losing the timer loses nothing --
        // the replay re-enters the same stall and re-arms its own grace.
        if (driver.stallTimer !== undefined) return;
        driver.stallTimer = setTimeout(() => {
          driver.stallTimer = undefined;
          if (driver.finished || driver.running !== 0 || driver.blocked.length > 0) {
            return;
          }
          driver.finish({
            kind: "error",
            error: __wfError(
              "run() has been awaiting something that is not a step for " +
                (__WF_STALL_GRACE_MS / 1000) + " seconds while no step is " +
                "running or blocked; a replay cannot resume this wait, so " +
                "wrap long async work in step.do",
            ),
          });
        }, __WF_STALL_GRACE_MS);
      }, 0);
    };
    if (
      meta.status !== "running" &&
      !(await settle((current) => {
        current.status = "running";
      }))
    ) {
      return;
    }
    const step = __wfMakeStep(driver);
    const event = {
      payload: meta.params,
      timestamp: new Date(meta.createdMs),
      instanceId: meta.instanceId,
      workflowName: meta.workflowName,
    };
    // ExecutionContext-shaped, as the WorkflowEntrypoint constructor
    // documents upstream.
    const ctx = {
      waitUntil: __registerWaitUntil,
      passThroughOnException() {},
    };
    (async () => new cls(ctx, __cell.env).run(event, step))().then(
      (value) => driver.finish({ kind: "complete", value }),
      (error) => driver.finish({ kind: "error", error }),
    );
    /*__CELLO_TEST_WORKFLOW_AFTER_RUN_STARTED__*/
    let result = await outcome;
    // The workflow can catch a rejected step promise, but it cannot turn an
    // engine storage failure into a successful run. Preserve the alarm so
    // the runtime retries over the last committed ledger prefix.
    if (driver.storageFailure !== undefined) {
      throw driver.storageFailure;
    }
    const currentMeta = await storage.get("__wf.meta");
    if (
      result.kind === "pause" ||
      (currentMeta !== undefined &&
        currentMeta.generation === meta.generation &&
        currentMeta.status === "waitingForPause")
    ) {
      /*__CELLO_TEST_WORKFLOW_BEFORE_PAUSE_SETTLE__*/
      settle((current, transaction) => {
        // A resume can cancel waitingForPause after this drive selected its
        // pause result. Preserve that newer running state and its alarm, or
        // this stale settlement strands an acknowledged resume as paused.
        if (current.status !== "waitingForPause") return;
        current.status = "paused";
        current.pausedMs = Date.now();
        transaction.deleteAlarm();
      });
      return;
    }
    if (result.kind === "suspend") {
      let at = Math.min(...driver.blocked.map((entry) => entry.at));
      // A nudge that landed mid-drive must not wait out the pending
      // deadline. It is visible in one of two places: getAlarm(), when the
      // nudge re-armed and no second alarm dispatched yet, or `_wfNudge`,
      // when a second alarm() already entered and parked on the drive
      // guard -- its dispatch consumed the armed alarm, so getAlarm()
      // alone would miss it.
      const nudge = await storage.getAlarm();
      if (nudge !== null) at = Math.min(at, nudge);
      if (this._wfNudge !== undefined) {
        at = Math.min(at, this._wfNudge);
        this._wfNudge = undefined;
      }
      settle((current, transaction) => {
        current.status = "waiting";
        transaction.setAlarm(at);
      });
      // The explicit re-arm replaces the alarm that fired this drive.
      // Durability is per SQLite commit, not per turn: a crash can persist
      // any prefix of this invocation's ledger writes, and the unconsumed
      // fired alarm is what re-drives the replay over whatever prefix
      // survived. A terminated instance re-arms nothing; its leftover
      // alarm fires once, sees the terminal status, and retires itself.
      return;
    }
    if (result.kind === "complete") {
      try {
        if (result.value !== undefined) {
          __wfCheckValue(result.value, "the run() return value");
        }
      } catch (error) {
        result = { kind: "error", error };
      }
      if (result.kind === "complete") {
        // Keep settlement outside the value-validation catch. A storage
        // failure must escape the alarm handler so the engine retries the
        // still-armed drive; converting it to a user error would consume the
        // alarm and make a transient commit failure terminal.
        settle((current, transaction) => {
          current.status = "complete";
          if (result.value !== undefined) current.output = result.value;
          // The terminal commit installs the expiry wake with the retained
          // state, so a crash can expose neither without the other.
          __wfFinishRetention(current, transaction);
          /*__CELLO_TEST_WORKFLOW_TERMINAL_ALARM_SET__*/
        });
        return;
      }
    }
    settle((current, transaction) => {
      current.status = "errored";
      current.error = __wfErrorRecord(result.error);
      __wfFinishRetention(current, transaction);
      /*__CELLO_TEST_WORKFLOW_TERMINAL_ALARM_SET__*/
    });
  }
  };
})();
__runtimeClass("__Workflow", __WorkflowCell);

class NonRetryableError extends Error {
  constructor(message, name = "NonRetryableError") {
    super(message);
    this.name = name;
  }
}
// Backing object for the `cloudflare:workflows` builtin module.
__cello.__cfWorkflows = { NonRetryableError };
// `cello:python`, imported only by the Python Worker bundle.
__cello.__pythonHost = {
  condemnIsolate: (reason) => __isolate_condemn(String(reason)),
};

const __wfValidInstanceId = (id) =>
  typeof id === "string" && id.length >= 1 && id.length <= 100 &&
  __WF_NAME_RE.test(id);
const __wfCreateOptions = (options, what, validateParams = true) => {
  if (!__wfOwnObject(options)) {
    throw __wfError(`${what} needs an object`);
  }
  const retention = __wfRetentionOptions(options.retention, what);
  const locationHint = __wfLocationHint(options.locationHint, what);
  const id = options.id === undefined ? crypto.randomUUID() : options.id;
  if (!__wfValidInstanceId(id)) {
    throw __wfError(
      `invalid instance id ${JSON.stringify(id)}: use letters, digits, ` +
        "\"-\" and \"_\" (not starting with \"-\"), at most 100 characters",
    );
  }
  if (validateParams && options.params !== undefined) {
    // The cell repeats this serialization check because it owns the durable
    // create invariant. A single create rejects before it addresses the cell.
    __wfCheckValue(options.params, "the workflow params");
  }
  return { id, params: options.params, retention, locationHint };
};

// Named so SDKs that sniff a binding by `constructor.name` recognise it.
class WorkflowInstance {
  constructor(workflowName, id) {
    Object.defineProperty(this, "_workflowName", { value: workflowName });
    this.id = id;
  }
  // Resolved per call rather than cached: a cell can move between calls.
  get _stub() {
    // The reserved class is script-scoped (`deploy::workflow_class`), because
    // a workflow instance is: two co-hosted scripts must not share one entry
    // in the deployment's flat class registry. `__cell.script` is injected
    // before any binding runs, so this resolves at call time.
    return __cell.makeNamespace("__Workflow." + __cell.script)
      .getByName(this._workflowName + "/" + this.id);
  }
  async status() {
    return await this._stub.__wfStatus();
  }
  async terminate(options) {
    if (options && options.rollback) {
      throw __wfError(
        "terminate({rollback: true}) is not implemented in cello; step " +
          "rollbackOptions are not implemented, so no handler could run",
      );
    }
    await this._stub.__wfTerminate();
  }
  async sendEvent(options) {
    await this._stub.__wfSendEvent(__wfEventOptions(options));
  }
  async pause() {
    await this._stub.__wfPause();
  }
  async resume() {
    await this._stub.__wfResume();
  }
  async restart(options) {
    await this._stub.__wfRestart(__wfRestartOptions(options));
  }
  async delete() {
    try {
      await this._stub.__wfDelete();
    } catch (error) {
      // The deletion commit resets an active Workflow cell so an in-flight
      // callback cannot put old ledger state back. That reset rejects this
      // RPC with its private completion marker; it is the successful path.
      if (!String(error && error.message || error).includes("WORKFLOW_INSTANCE_DELETED")) {
        throw error;
      }
    }
  }
}

// Workerd's workflow binding: a `WorkflowImpl` over the shared
// `WrappedBinding` level, both tagged `[object WrappedBinding]`, the name in a
// private slot so the instance carries no own property, and the methods on the
// prototype in workerd's order (#266). `ctx.exports` hands out the same shape
// over the same instance store as `env.<binding>`.
class WorkflowImpl extends WrappedBinding {
  #workflowName;
  constructor(workflowName) {
    super();
    this.#workflowName = workflowName;
  }
  async #create(options, skipExisting) {
    const { id, params, retention, locationHint } = options;
    const instance = new WorkflowInstance(this.#workflowName, id);
    const result = await instance._stub.__wfCreate({
      workflowName: this.#workflowName,
      instanceId: id,
      params,
      retention,
      locationHint,
      skipExisting,
    });
    return result.created ? instance : undefined;
  }
  async create(options = {}) {
    return await this.#create(__wfCreateOptions(options, "create()"), false);
  }
  async createBatch(batch) {
    if (!Array.isArray(batch)) {
      throw __wfError("createBatch() needs an array of create options");
    }
    if (batch.length > 100) {
      throw __wfError(
        `createBatch() accepts at most 100 instances, got ${batch.length}`,
      );
    }
    if (batch.length === 0) {
      throw __wfError("createBatch() needs at least one instance");
    }
    // Cloudflare rejects a structural error in any item before it creates the
    // first instance, but it filters a params clone failure per item. Keep
    // these two gates separate so a bad ID stays atomic while a clone failure
    // does not suppress the valid items around it.
    const optionsList = batch.map((options, index) =>
      __wfCreateOptions(options, `createBatch() item ${index}`, false));
    const creatable = [];
    for (const options of optionsList) {
      let encoded;
      try {
        if (options.params !== undefined) {
          encoded = __wfEncodeValue(options.params, "the workflow params");
        }
      } catch {
        continue;
      }
      if (encoded !== undefined) {
        __wfCheckEncodedValue(encoded, "the workflow params");
      }
      creatable.push(options);
    }
    const created = [];
    for (const options of creatable) {
      const instance = await this.#create(options, true);
      if (instance !== undefined) created.push(instance);
    }
    return created;
  }
  async get(id) {
    if (!__wfValidInstanceId(id)) {
      throw __wfError(`invalid instance id ${JSON.stringify(id)}`);
    }
    const instance = new WorkflowInstance(this.#workflowName, id);
    // The cell exists on first address like any cell, but get() must not
    // mint an instance for a typo: an id with no ledger rejects here.
    await instance._stub.__wfStatus();
    return instance;
  }
  async deleteBatch(instanceIds) {
    if (!Array.isArray(instanceIds)) {
      throw __wfError("deleteBatch() needs an array of instance ids");
    }
    if (instanceIds.length < 1 || instanceIds.length > 100) {
      throw __wfError(
        `deleteBatch() accepts 1 to 100 instance ids, got ${instanceIds.length}`,
      );
    }
    for (const id of instanceIds) {
      if (!__wfValidInstanceId(id)) {
        throw __wfError(`invalid instance id ${JSON.stringify(id)}`);
      }
    }
    const uniqueIds = [...new Set(instanceIds)];
    const settled = await Promise.allSettled(uniqueIds.map(async (id) => {
      const instance = new WorkflowInstance(this.#workflowName, id);
      await instance.delete();
    }));
    const results = new Map(uniqueIds.map((id, index) => [id, settled[index]]));
    const output = { deleted: [], errors: [] };
    for (const id of instanceIds) {
      const result = results.get(id);
      if (result.status === "fulfilled") {
        output.deleted.push({ id });
        continue;
      }
      const notFound = String(result.reason && result.reason.message || result.reason)
        .includes("instance does not exist");
      output.errors.push({
        id,
        code: notFound ? 10400 : 10001,
        message: notFound
          ? "workflows.api.error.instance.not_found"
          : "workflows.api.error.internal_server",
      });
    }
    return output;
  }
}

const __makeWorkflow = __cello.__makeWorkflow =
  (workflowName) => new WorkflowImpl(workflowName);
// `cloudflare:workers` module surface. The DO base class sets ctx/env the
// way `class X extends DurableObject` expects; env aliases the cell env.
const __cf = __cello.__cf = {
  DurableObject: class DurableObject {
    constructor(ctx, env) { this.ctx = ctx; this.env = env; }
  },
  // The enumerable function-valued brand makes V8's structured clone
  // fail on every RpcTarget instance, forcing it off the silent
  // plain-clone path and into the stub lift (Workerd rejects these
  // from plain serialization outright).
  RpcTarget: class RpcTarget {
    constructor() {
      Object.defineProperty(this, "__celloRpcTarget", {
        value: __rpcNoClone, enumerable: true,
      });
    }
  },
  // Named entrypoint for `[[services]]` with `entrypoint = "Name"`.
  WorkerEntrypoint: class WorkerEntrypoint {
    constructor(ctx, env) { this.ctx = ctx; this.env = env; }
  },
  // Base class for `workflows` classes. The reserved workflow cell
  // instantiates a subclass on every replay with an ExecutionContext-shaped
  // ctx -- never the cell's own DurableObjectState, which would hand user
  // code the ledger that replays it.
  WorkflowEntrypoint: class WorkflowEntrypoint {
    constructor(ctx, env) { this.ctx = ctx; this.env = env; }
  },
  // `new RpcStub(target)` wraps any local object or function in a
  // loopback stub; the constructor returns the proxy, so instanceof
  // works through __makeStub's getPrototypeOf trap.
  RpcStub: class RpcStub {
    constructor(target) {
      if (target === null ||
          (typeof target !== "object" && typeof target !== "function"))
        throw new TypeError(
          "RpcStub requires an object or function.");
      return __makeStub(
        __newEntry(target), typeof target === "function");
    }
  },
  RpcPromise: class RpcPromise extends Promise {},
  RpcProperty: class RpcProperty {},
  ServiceStub: class ServiceStub {},
  // `import { waitUntil } from "cloudflare:workers"`: register into
  // the current event or extend its active background work. Outside either
  // lifetime this is Workerd's global-scope error.
  waitUntil(promise) {
    if (!__wait_until_active())
      throw new Error(
        "Disallowed operation called within global scope.");
    __registerWaitUntil(promise);
  },
  // `import { getCurrentTraceparent } from "cloudflare:workers"`: the W3C
  // traceparent of the span the running handler executes in, so the handler
  // can return the trace id to the caller or record it with the data it
  // writes (denoland/celld#313). `undefined` when telemetry is off or the
  // request is unsampled, like every other unsampled path that builds
  // nothing.
  getCurrentTraceparent: () => __traceparent(),
  exports: {},
  get env() { return __cell.env; },
};
// workerd's service stub is a `Fetcher` whose members sit on its own level,
// and the interface's constructor is `ServiceStub`: the tag, `fetch` and
// `connect` go on the prototype rather than on each stub proxy, so a loopback
// stub's extra level can sit above exactly that shape (#259).
__interfaceTag(__cf.ServiceStub.prototype, "Fetcher");
Object.defineProperty(__cf.ServiceStub.prototype, "fetch", {
  value: __serviceStubFetch, enumerable: true, configurable: true, writable: true,
});
Object.defineProperty(__cf.ServiceStub.prototype, "connect", {
  value: __serviceStubConnect, enumerable: true, configurable: true,
  writable: true,
});
// Proxy standing in for unsupported node:*/cloudflare:* builtins. Property
// walks stay inert -- real bundles reference these at module scope, and
// evaluation must not crash on a builtin the fetch path never exercises --
// but a call or construct throws: the compat contract is "reject at first
// use", and the old silent pass-through turned a missing builtin into a
// wrong result far from the cause. Memoized by dotted path so repeated
// reads keep identity (`mod.foo === mod.foo`).
const __stubCache = new Map();
const __nodeStubFor = __cello.__nodeStubFor = (path) => {
  let stub = __stubCache.get(path);
  if (stub) return stub;
  stub = new Proxy(function () {}, {
    get: (_t, p) => {
      // Never masquerade as a thenable. `await` probes `.then`; returning
      // a callable here creates a promise that can never settle.
      if (p === "then") return undefined;
      // coerce cleanly in string/number contexts so evaluation never throws
      if (p === Symbol.toPrimitive || p === "toString" || p === "valueOf")
        return () => "";
      if (p === Symbol.toStringTag) return "NodeStub";
      if (p === Symbol.iterator) return function* () {};
      if (typeof p !== "string") return undefined;
      return __nodeStubFor(path + "." + p);
    },
    apply() { throw new Error(path + " is not implemented in cello"); },
    construct() { throw new Error(path + " is not implemented in cello"); },
  });
  __stubCache.set(path, stub);
  return stub;
};
__cello.__nodeStub = __nodeStubFor("node builtin");
// Installed by the Event class below; EventTarget drives dispatch through it.
// Both blocks share this module scope, and neither name is guest-visible.
let __eventHooks;
if (!globalThis.Event) {
  // Internal dispatch hooks are module-private: workerd's Event prototype is
  // the DOM surface only, and the reflection APIs must not name host helpers
  // (#206). EventTarget reaches them through `__eventHooks`.
  globalThis.Event = class Event {
    // Private fields: stored in the object itself, so an Event costs no
    // side allocation and no defineProperty call at construction. The
    // public members are accessors because the DOM standard makes them
    // read-only; EventTarget drives them through the hooks below.
    #type; #bubbles; #cancelable; #composed;
    #defaultPrevented = false;
    #eventPhase = 0;
    #target; #currentTarget; #path;
    #stop = false; #stopImmediate = false; #dispatching = false;
    // Workerd: events the runtime delivers are trusted; events a
    // script constructs and dispatches itself are not.
    #trusted = false;
    static #expose = (__eventHooks = {
      begin: (event, target) => event.#begin(target),
      end: (event) => event.#end(),
      trust: (event) => { event.#trusted = true; },
      dispatching: (event) => event.#dispatching,
      stopImmediate: (event) => event.#stopImmediate,
    });
    constructor(type, init = {}) {
      if (arguments.length === 0)
        throw new TypeError(
          "Failed to construct 'Event': 1 argument required, but only " +
          "0 present.");
      if (init !== undefined && init !== null &&
          typeof init !== "object")
        throw new TypeError(
          "Failed to construct 'Event': The provided value is not of " +
          "type 'EventInit'.");
      const options = init || {};
      // Template interpolation, not String(): it must throw for a
      // Symbol, which String() would happily format.
      this.#type = `${type}`;
      this.#bubbles = !!options.bubbles;
      this.#cancelable = !!options.cancelable;
      this.#composed = !!options.composed;
    }
    get type() { return this.#type; }
    get eventPhase() { return this.#eventPhase; }
    get composed() { return this.#composed; }
    get bubbles() { return this.#bubbles; }
    get cancelable() { return this.#cancelable; }
    get defaultPrevented() { return this.#defaultPrevented; }
    get returnValue() { return !this.#defaultPrevented; }
    get currentTarget() { return this.#currentTarget; }
    get target() { return this.#target; }
    // The DOM standard's legacy alias for target; workerd defines it.
    get srcElement() { return this.#target; }
    get timeStamp() { return 0; }
    get isTrusted() { return this.#trusted; }
    // The one writable member, per the DOM standard.
    get cancelBubble() { return this.#stop; }
    set cancelBubble(value) { if (value) this.#stop = true; }
    composedPath() { return this.#path ? this.#path.slice() : []; }
    preventDefault() {
      if (this.#cancelable) this.#defaultPrevented = true;
    }
    stopPropagation() { this.#stop = true; }
    stopImmediatePropagation() {
      this.#stop = true;
      this.#stopImmediate = true;
    }
    #begin(target) {
      this.#dispatching = true;
      this.#target = target;
      this.#currentTarget = target;
      this.#eventPhase = 2; // AT_TARGET
      this.#path = [target];
      this.#stop = false;
      this.#stopImmediate = false;
    }
    // Workerd leaves currentTarget set after dispatch (the DOM standard
    // nulls it); only the phase is reset.
    #end() {
      this.#eventPhase = 0;
      this.#dispatching = false;
    }
  };
  {
    const phases = {
      NONE: 0, CAPTURING_PHASE: 1, AT_TARGET: 2, BUBBLING_PHASE: 3,
    };
    for (const [name, value] of Object.entries(phases)) {
      const d = { value, writable: false, enumerable: true,
        configurable: false };
      Object.defineProperty(globalThis.Event, name, d);
      Object.defineProperty(globalThis.Event.prototype, name, d);
    }
  }
  __webidlTag(globalThis.Event, "Event");
  __webidlPrototype(globalThis.Event);
  __cello.$$trustEvent = (event) => __eventHooks.trust(event);
}
if (!globalThis.CustomEvent) {
  globalThis.CustomEvent = class CustomEvent extends Event {
    #detail;
    constructor(type, init = {}) {
      super(type, init);
      this.#detail = init.detail === undefined ? null : init.detail;
    }
    get detail() { return this.#detail; }
  };
  __webidlTag(globalThis.CustomEvent, "CustomEvent");
  __webidlPrototype(globalThis.CustomEvent);
}
if (!globalThis.ExtendableEvent) {
  // Exists as a type but is not constructable from user code.
  globalThis.ExtendableEvent = class ExtendableEvent extends Event {
    constructor() {
      throw new TypeError("Illegal constructor");
    }
  };
  __webidlTag(globalThis.ExtendableEvent, "ExtendableEvent");
}
// An event-handler IDL attribute occupies one listener-list position. Replacing
// its callback must retain that position, or listeners added after `onabort`
// run before it and observe an order that differs from the web platform.
const __eventHandlerEntry = Symbol("eventHandlerEntry");
// The listener map is a private slot on EventTarget (#206); these accessors are
// installed when EventTarget is defined below and are the only way in.
let __eventTargetListeners;
const __eventListeners = (target) => __eventTargetListeners(target);
const __getEventHandler = (target, type) =>
  (__eventListeners(target).get(type) || [])
    .find((entry) => entry[__eventHandlerEntry])?.callback ?? null;
const __setEventHandler = (target, type, callback) => {
  const listeners = __eventListeners(target);
  const list = listeners.get(type) || [];
  const entry = list.find((item) => item[__eventHandlerEntry]);
  if (typeof callback !== "function") {
    if (entry) list.splice(list.indexOf(entry), 1);
    return;
  }
  if (entry) {
    entry.callback = callback;
  } else {
    list.push({ callback, once: false, [__eventHandlerEntry]: true });
    listeners.set(type, list);
  }
};
if (!globalThis.EventTarget) {
  globalThis.EventTarget = class EventTarget {
    #listeners = new Map();
    static #expose = (__eventTargetListeners = (target) => target.#listeners);
    addEventListener(type, callback, options = {}) {
      if (callback === null || callback === undefined) return;
      if (typeof callback !== "function" &&
          typeof callback.handleEvent !== "function")
        throw new TypeError(
          "Failed to execute 'addEventListener' on 'EventTarget': " +
          "parameter 2 is not of type 'EventListener'.");
      const object = typeof options === "object" && options !== null;
      // Capture and passive are accepted for portability but must be
      // false: Cells dispatches only at the target, so honouring them
      // would silently change ordering.
      if (object ? options.capture || options.passive : !!options)
        throw new TypeError(
          "Cells does not support the 'capture' or 'passive' options " +
          "on addEventListener().");
      const signal = object ? options.signal : undefined;
      if (signal && signal.aborted) return;
      const key = String(type);
      const list = this.#listeners.get(key) || [];
      if (list.some((e) => e.callback === callback)) return;
      list.push({ callback, once: object && !!options.once });
      this.#listeners.set(key, list);
      // A type may react to its first listener (MessagePort starts on
      // addEventListener("message")); the hook is installed by message_channel.
      const added = __cello.$$listenerAdded;
      if (typeof added === "function") added(this, key);
      if (signal)
        signal.addEventListener("abort", () =>
          this.removeEventListener(key, callback), { once: true });
    }
    removeEventListener(type, callback, options = {}) {
      const object = typeof options === "object" && options !== null;
      if (object ? options.capture : !!options)
        throw new TypeError(
          "Cells does not support the 'capture' option on " +
          "removeEventListener().");
      const list = this.#listeners.get(String(type));
      if (!list) return;
      const index = list.findIndex((e) => e.callback === callback);
      if (index >= 0) list.splice(index, 1);
    }
    dispatchEvent(event) {
      if (!(event instanceof Event))
        throw new TypeError("argument is not an Event");
      if (__eventHooks.dispatching(event))
        throw new DOMException(
          "The event is already being dispatched.",
          "InvalidStateError");
      __eventHooks.begin(event, this);
      // Copy: a listener may add or remove listeners mid-dispatch.
      for (const item of (this.#listeners.get(event.type) || []).slice()) {
        if (__eventHooks.stopImmediate(event)) break;
        if (item.once)
          this.removeEventListener(event.type, item.callback);
        if (typeof item.callback === "function")
          item.callback.call(this, event);
        else item.callback.handleEvent(event);
      }
      const handler = this["on" + event.type];
      const hasEventHandler = (this.#listeners.get(event.type) || [])
        .some((item) => item[__eventHandlerEntry]);
      if (!hasEventHandler && !__eventHooks.stopImmediate(event) &&
          typeof handler === "function")
        handler.call(this, event);
      __eventHooks.end(event);
      return !event.defaultPrevented;
    }
  };
  __webidlTag(globalThis.EventTarget, "EventTarget");
  __webidlPrototype(globalThis.EventTarget);
}

// The global scope is itself an EventTarget.
if (typeof globalThis.addEventListener !== "function") {
  const globalTarget = new EventTarget();
  for (const name of
    ["addEventListener", "removeEventListener", "dispatchEvent"])
    globalThis[name] = globalTarget[name].bind(globalTarget);
}
if (!globalThis.MessageEvent) {
  // Workerd's MessageEvent (messageevent.c++). The five initializer members
  // are own, enumerable, non-writable data properties, installed in this
  // order; `ports` is a fresh array (the init array is copied, not adopted)
  // and every absent member takes its IDL default. `source` is only ever a
  // MessagePort; MessagePort itself installs after the harness, so the check
  // runs at construction, not here.
  globalThis.MessageEvent = class MessageEvent extends Event {
    // Rest parameter so the IDL constructor length is 0, as workerd's is.
    constructor(...args) {
      const type = args[0];
      const init = args[1];
      // A missing argument is an error, but Web IDL converts a present
      // non-string one (workerd constructs `new MessageEvent(5)` as "5").
      if (args.length === 0)
        throw new TypeError(
          "Failed to construct 'MessageEvent': constructor parameter 1 " +
          "is not of type 'string'.");
      if (init !== undefined && init !== null &&
          typeof init !== "object")
        throw new TypeError(
          "Failed to construct 'MessageEvent': constructor parameter 2 " +
          "is not of type 'Initializer'.");
      const options = init ?? {};
      let ports = [];
      if (options.ports !== undefined) {
        if (!Array.isArray(options.ports))
          throw new TypeError(
            "Incorrect type for the 'ports' field on 'Initializer': the " +
            "provided value is not of type 'Array'.");
        ports = Array.prototype.slice.call(options.ports);
        for (let index = 0; index < ports.length; index++) {
          if (typeof MessagePort !== "function" ||
              !(ports[index] instanceof MessagePort))
            throw new TypeError(
              `Incorrect type for array element ${index}: the provided ` +
              "value is not of type 'MessagePort'.");
        }
      }
      const source = options.source === undefined ? null : options.source;
      if (source !== null &&
          (typeof MessagePort !== "function" ||
           !(source instanceof MessagePort)))
        throw new TypeError(
          "Incorrect type for the 'source' field on 'Initializer': the " +
          "provided value is not of type 'MessagePort'.");
      super(`${type}`, options);
      const usv = (value) => {
        const text = String(value ?? "");
        return text.toWellFormed ? text.toWellFormed() : text;
      };
      Object.defineProperties(this, {
        ports: {
          value: ports, enumerable: true, writable: false,
          configurable: true,
        },
        source: {
          value: source, enumerable: true, writable: false,
          configurable: true,
        },
        lastEventId: {
          value: usv(options.lastEventId), enumerable: true, writable: false,
          configurable: true,
        },
        origin: {
          value: usv(options.origin), enumerable: true, writable: false,
          configurable: true,
        },
        data: {
          value: options.data === undefined ? null : options.data,
          enumerable: true, writable: false, configurable: true,
        },
      });
    }
  };
  // WebIDL reflection: enumerable own data members already, plus the
  // interface tag and the zero-length constructor (#206).
  __webidlTag(globalThis.MessageEvent, "MessageEvent");
  __webidlPrototype(globalThis.MessageEvent);
  __webidlLength(globalThis.MessageEvent);
}
if (!globalThis.CloseEvent) {
  globalThis.CloseEvent = class CloseEvent extends Event {
    // Own enumerable data members in workerd's order (#206).
    constructor(type, init = {}) {
      super(type, init);
      this.wasClean = !!init.wasClean;
      // `reason` is a USVString: a lone surrogate becomes U+FFFD rather than
      // travelling as an unpaired code unit.
      this.reason = String(init.reason || "").toWellFormed
        ? String(init.reason || "").toWellFormed()
        : String(init.reason || "").replace(
          /[\uD800-\uDFFF]/g,
          (unit, index, whole) => {
            const code = unit.charCodeAt(0);
            const next = whole.charCodeAt(index + 1);
            const prev = whole.charCodeAt(index - 1);
            const paired = code <= 0xDBFF
              ? next >= 0xDC00 && next <= 0xDFFF
              : prev >= 0xD800 && prev <= 0xDBFF;
            return paired ? unit : "\uFFFD";
          },
        );
      this.code = Number(init.code || 0);
    }
  };
  __webidlTag(globalThis.CloseEvent, "CloseEvent");
  __webidlPrototype(globalThis.CloseEvent);
}
if (!globalThis.ErrorEvent) {
  globalThis.ErrorEvent = class ErrorEvent extends Event {
    #message; #filename; #lineno; #colno; #error;
    constructor(type, init = {}) {
      super(type, init);
      this.#message = String(init.message ?? "");
      this.#filename = String(init.filename ?? "");
      this.#lineno = Number(init.lineno ?? 0);
      this.#colno = Number(init.colno ?? 0);
      this.#error = init.error;
    }
    get message() { return this.#message; }
    get filename() { return this.#filename; }
    get lineno() { return this.#lineno; }
    get colno() { return this.#colno; }
    get error() { return this.#error; }
  };
  __webidlTag(globalThis.ErrorEvent, "ErrorEvent");
  __webidlPrototype(globalThis.ErrorEvent);
}
if (!globalThis.DOMException) {
  const __domExceptionInstances = new __primWeakSet();
  __cello.__isDOMExceptionInstance = (value) =>
    __weakSetHas(__domExceptionInstances, value);
  // The legacy `code` member maps the error name to its constant, per
  // WebIDL; names outside the table (and every modern name) are 0.
  const legacy = {
    IndexSizeError: 1, HierarchyRequestError: 3, WrongDocumentError: 4,
    InvalidCharacterError: 5, NoModificationAllowedError: 7,
    NotFoundError: 8, NotSupportedError: 9, InUseAttributeError: 10,
    InvalidStateError: 11, SyntaxError: 12, InvalidModificationError: 13,
    NamespaceError: 14, InvalidAccessError: 15, TypeMismatchError: 17,
    SecurityError: 18, NetworkError: 19, AbortError: 20,
    URLMismatchError: 21, QuotaExceededError: 22, TimeoutError: 23,
    InvalidNodeTypeError: 24, DataCloneError: 25,
  };
  // A function, not a class extending Error: workerd's interface object
  // inherits no Error static (`DOMException.captureStackTrace` is undefined),
  // and a class's [[Prototype]] cannot be moved off Error without breaking
  // `super()` (#207). Constructing through Error with `new.target` keeps
  // `instanceof Error`, the stack and subclassing.
  const __domExceptionStacks = new __primWeakMap();
  globalThis.DOMException = function DOMException(message = "", name = "Error") {
    if (new.target === undefined)
      throw new TypeError(
        "Failed to construct 'DOMException': Please use the 'new' operator.");
    const instance = __primConstruct(Error, [message], new.target);
    // Workerd's own shape (#207): enumerable data members in the order
    // code, name, message, and an enumerable accessor for stack. Error
    // already created `message` and `stack` own properties, and redefining
    // one keeps its insertion position, so `message` is removed first and
    // both are re-added in order. The stack string is read after `name` is
    // set, because V8 formats its header from the error's name.
    delete instance.message;
    instance.code = legacy[name] ?? 0;
    instance.name = name;
    __primDefineProp(instance, "message", {
      value: String(message), writable: true, enumerable: true,
      configurable: true,
    });
    const stack = instance.stack;
    delete instance.stack;
    __weakSet(__domExceptionStacks, instance, stack);
    __primDefineProp(instance, "stack", {
      enumerable: true, configurable: true,
      get() { return __weakGet(__domExceptionStacks, this); },
      set(value) { __weakSet(__domExceptionStacks, this, value); },
    });
    __weakSetAdd(__domExceptionInstances, instance);
    return instance;
  };
  Object.setPrototypeOf(globalThis.DOMException.prototype, Error.prototype);
  // WebIDL legacy code constants, enumerable on both the interface
  // object and its prototype, in spec order.
  {
    const codes = [
      ["INDEX_SIZE_ERR", 1], ["DOMSTRING_SIZE_ERR", 2],
      ["HIERARCHY_REQUEST_ERR", 3], ["WRONG_DOCUMENT_ERR", 4],
      ["INVALID_CHARACTER_ERR", 5], ["NO_DATA_ALLOWED_ERR", 6],
      ["NO_MODIFICATION_ALLOWED_ERR", 7], ["NOT_FOUND_ERR", 8],
      ["NOT_SUPPORTED_ERR", 9], ["INUSE_ATTRIBUTE_ERR", 10],
      ["INVALID_STATE_ERR", 11], ["SYNTAX_ERR", 12],
      ["INVALID_MODIFICATION_ERR", 13], ["NAMESPACE_ERR", 14],
      ["INVALID_ACCESS_ERR", 15], ["VALIDATION_ERR", 16],
      ["TYPE_MISMATCH_ERR", 17], ["SECURITY_ERR", 18],
      ["NETWORK_ERR", 19], ["ABORT_ERR", 20],
      ["URL_MISMATCH_ERR", 21], ["QUOTA_EXCEEDED_ERR", 22],
      ["TIMEOUT_ERR", 23], ["INVALID_NODE_TYPE_ERR", 24],
      ["DATA_CLONE_ERR", 25],
    ];
    for (const [name, value] of codes) {
      const d = { value, writable: false, enumerable: true,
        configurable: false };
      Object.defineProperty(globalThis.DOMException, name, d);
      Object.defineProperty(globalThis.DOMException.prototype, name, d);
    }
  }
  __webidlTag(globalThis.DOMException, "DOMException");
}
// The realm's DOMException, captured before tenant code can replace the
// global: error classification must not follow a Worker's own global.
const __primDOMException = globalThis.DOMException;
// The host classifies a rejected Durable Object handler by this constructor,
// not by the global a Worker may have replaced.
__cello.__primDOMException = __primDOMException;
if (!globalThis.AbortSignal) {
  const __abortBrand = Symbol("abortSignal");
  // A constructed signal is the only thing the clone codec may refuse as one;
  // a forged prototype flattens to plain data (#221).
  const __abortSignalInstances = new __primWeakSet();
  __cello.$$isAbortSignalInstance = (value) =>
    __weakSetHas(__abortSignalInstances, value);
  globalThis.AbortSignal = class AbortSignal extends EventTarget {
    #aborted = false;
    #reason;
    static #expose = (__abortSignalImpl = (signal, reason) => {
      if (signal.#aborted) return;
      signal.#aborted = true;
      signal.#reason = reason;
      signal.dispatchEvent(new Event("abort"));
    });
    constructor() {
      // Per spec AbortSignal has no constructor: it is only reachable
      // through AbortController, AbortSignal.abort/timeout/any. The internal
      // paths pass the brand to get past this.
      if (arguments[0] !== __abortBrand) {
        throw new TypeError("Illegal constructor");
      }
      super();
      __weakSetAdd(__abortSignalInstances, this);
    }
    get aborted() { return this.#aborted; }
    get reason() { return this.#reason; }
    throwIfAborted() { if (this.#aborted) throw this.#reason; }
    static abort(reason = new DOMException("The operation was aborted", "AbortError")) {
      const c = new AbortController(); c.abort(reason); return c.signal;
    }
    static timeout(ms) {
      const c = new AbortController();
      // Unreferenced: nobody waits on this timer and nothing can cancel it,
      // so a referenced setTimeout would pin the event (and its admission
      // slot) until it fires. Node, Bun, and workerd all let the timeout die
      // with the request instead.
      __op_timer_unrefed(Number(ms) || 0).then(() => {
        try {
          c.abort(new DOMException(
            "The operation was aborted due to timeout", "TimeoutError",
          ));
        } catch {}
      }, () => {});
      return c.signal;
    }
    static any(signals) {
      // Workerd validates the parameter and every element before it uses
      // one, so a non-signal names the element instead of failing later with
      // "signal.addEventListener is not a function" (#184).
      if (!Array.isArray(signals))
        throw new TypeError(
          "Failed to execute 'any' on 'AbortSignal': parameter 1 is not of " +
          "type 'Array'.");
      for (let i = 0; i < signals.length; i++) {
        if (!(signals[i] instanceof globalThis.AbortSignal))
          throw new TypeError(
            `Incorrect type for array element ${i}: the provided value is ` +
            "not of type 'AbortSignal'.");
      }
      const c = new AbortController();
      for (const signal of signals) {
        if (signal.aborted) { c.abort(signal.reason); break; }
        signal.addEventListener("abort", () => c.abort(signal.reason), { once: true });
      }
      return c.signal;
    }
  };
  __primDefineProp(globalThis.AbortSignal.prototype, "onabort", {
    configurable: true,
    enumerable: true,
    get() {
      if (!(this instanceof globalThis.AbortSignal))
        throw new TypeError("Illegal invocation");
      return __getEventHandler(this, "abort");
    },
    set(value) {
      if (!(this instanceof globalThis.AbortSignal))
        throw new TypeError("Illegal invocation");
      __setEventHandler(this, "abort", value);
    },
  });
  __webidlTag(globalThis.AbortSignal, "AbortSignal");
  __webidlPrototype(globalThis.AbortSignal);
  __webidlStatics(globalThis.AbortSignal);
  __webidlLength(globalThis.AbortSignal);
  __cello.$$abortSignal = __abortSignal;
  globalThis.AbortController = class AbortController {
    #signal;
    constructor() { this.#signal = new AbortSignal(__abortBrand); }
    get signal() { return this.#signal; }
    abort(reason = new DOMException("The operation was aborted", "AbortError")) {
      __abortSignal(this.#signal, reason);
    }
  };
  __webidlTag(globalThis.AbortController, "AbortController");
  __webidlPrototype(globalThis.AbortController);
  __webidlLength(globalThis.AbortController);
}
if (!globalThis.Blob) {
  // A constructed Blob is the only thing the clone codec may treat as one;
  // a guest can reach `Blob.prototype` but not this set (#221). The bytes,
  // size and type are private slots, so a Worker sees the Blob surface only
  // (#170, #206).
  const __blobInstances = new __primWeakSet();
  __cello.__isBlobInstance = (value) => __weakSetHas(__blobInstances, value);
  globalThis.Blob = class Blob {
    #bytes; #size = 0; #type = "";
    static #expose = (() => {
      __cello.$$blobBytes = (blob) => blob.#bytes;
      __cello.$$setBlobBytes = (blob, bytes) => {
        blob.#bytes = bytes;
        blob.#size = bytes.byteLength;
      };
      // blob() re-serializes the content-type, so the type must be stored
      // verbatim rather than through the constructor's lower-casing.
      __cello.$$setBlobType = (blob, type) => {
        blob.#type = type;
      };
    })();
    constructor(parts = [], options = {}) {
      if (options && options.endings !== undefined)
        throw new Error(
          "The 'endings' field on 'Options' is not implemented.");
      // A type outside U+0020..U+007E is not a valid MIME type and is
      // dropped rather than stored.
      const rawType = String(options.type || "");
      this.#type = /^[\u0020-\u007e]*$/.test(rawType)
        ? rawType.toLowerCase() : "";
      __weakSetAdd(__blobInstances, this);
      // Two passes. First convert every part in order -- string
      // conversion runs user code (Symbol.toPrimitive), which may
      // resize a backing buffer. Buffers and views are carried through
      // as-is so a length-tracking view is measured *after* those side
      // effects, matching Workerd. Then allocate once and copy.
      //
      // The previous shape pushed every byte into a plain array with a
      // spread, costing roughly 8x the memory and exceeding the
      // argument limit on multi-megabyte parts.
      const sources = [];
      for (const part of parts) {
        if (part instanceof Blob) sources.push(__cello.$$blobBytes(part));
        else if (part instanceof ArrayBuffer ||
                 ArrayBuffer.isView(part)) sources.push(part);
        else sources.push(new TextEncoder().encode(String(part)));
      }
      // Every part goes through the slot reader, including a real
      // `Uint8Array`: returning the guest's object let the size and copy
      // below read its own `byteLength` (#130). A part that is not bytes
      // keeps its string form, as before.
      const chunks = sources.map((source) =>
        __hostChunk(source, "Blob parts must be bytes") ??
          new TextEncoder().encode(String(source)));
      let total = 0;
      for (const chunk of chunks) total += chunk.byteLength;
      const LIMIT = 134217728;
      if (total > LIMIT)
        throw new RangeError(
          `Blob size ${total} exceeds limit ${LIMIT}`);
      const bytes = new Uint8Array(total);
      let offset = 0;
      for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
      }
      this.#bytes = bytes;
      this.#size = total;
    }
    get size() { return this.#size; }
    get type() { return this.#type; }
    async arrayBuffer() { return this.#bytes.slice().buffer; }
    async bytes() { return this.#bytes.slice(); }
    async text() { return new TextDecoder().decode(this.#bytes); }
    stream() {
      const bytes = this.#bytes;
      return new ReadableStream({
        start(controller) {
          if (bytes.byteLength) controller.enqueue(bytes.slice());
          controller.close();
        },
      });
    }
    [Symbol.for("nodejs.util.inspect.custom")]() {
      return `Blob { size: ${this.size}, type: '${this.type}' }`;
    }
    slice(start = 0, end = this.size, type = "") {
      const norm = (n, fallback) => {
        n = n === undefined ? fallback : Number(n);
        return n < 0 ? Math.max(this.size + n, 0) : Math.min(n, this.size);
      };
      const a = norm(start, 0), b = norm(end, this.size);
      return new Blob(
        [this.#bytes.subarray(a, Math.max(a, b))], { type });
    }
  };
  __webidlTag(globalThis.Blob, "Blob");
  __webidlPrototype(globalThis.Blob);
}
if (!globalThis.File) {
  const __fileInstances = new __primWeakSet();
  __cello.__isFileInstance = (value) => __weakSetHas(__fileInstances, value);
  // WebIDL-ish per Workerd: name is required, name/lastModified are
  // brand-checked prototype getters (enumerable, matching the
  // workers_api_getters_setters_on_prototype flag), and lastModified
  // coerces via ToNumber (throws for BigInt), NaN becoming 0.
  globalThis.File = class File extends Blob {
    #name;
    #lastModified;
    constructor(parts, name, options = {}) {
      if (arguments.length < 2)
        throw new TypeError(
          "Failed to construct 'File': 2 arguments required, but " +
          `only ${arguments.length} present.`);
      super(parts, options);
      __weakSetAdd(__fileInstances, this);
      this.#name = String(name);
      const lm = options == null ? undefined : options.lastModified;
      if (lm === undefined) this.#lastModified = Date.now();
      else {
        const n = +lm; // ToNumber: throws for BigInt, like WebIDL
        // Workerd stores ToNumber's value, including a fraction and
        // +/-Infinity; only NaN becomes 0 (#179).
        this.#lastModified = Number.isNaN(n) ? 0 : n;
      }
    }
    get name() { return this.#name; }
    get lastModified() { return this.#lastModified; }
    [Symbol.for("nodejs.util.inspect.custom")]() {
      return `File { name: '${this.name}', ` +
        `lastModified: ${this.lastModified}, size: ${this.size}, ` +
        `type: '${this.type}' }`;
    }
  };
  __webidlTag(globalThis.File, "File");
  __webidlPrototype(globalThis.File);
}
if (!globalThis.FormData) {
// Byte offsets the multipart scanner compares against. A part body is
// arbitrary bytes, so every framing decision below is made on bytes.
const __CR = 13, __LF = 10, __DASH = 45;
const __bytesIndexOf = (haystack, needle, from) => {
  const last = haystack.length - needle.length;
  outer: for (let i = from; i <= last; i++) {
    for (let j = 0; j < needle.length; j++)
      if (haystack[i + j] !== needle[j]) continue outer;
    return i;
  }
  return -1;
};
// A boundary delimiter is a delimiter only at the start of a line (RFC 2046
// section 5.1.1). Searching for the bytes anywhere would let a binary part
// that happens to contain `--<boundary>` split itself, which is a corrupt
// upload rather than a parse error, so the caller never learns of it.
const __delimiterIndex = (bytes, delimiter, from) => {
  for (let at = from;;) {
    const found = __bytesIndexOf(bytes, delimiter, at);
    if (found < 0) return -1;
    if (found === 0 || bytes[found - 1] === __LF) return found;
    at = found + 1;
  }
};
// A `Content-Disposition` parameter, anchored to the start of a parameter:
// the parameter name must follow the header name or a `;`. An unanchored
// pattern found the `name=` inside `filename=`, so a part whose header wrote
// the filename first landed under the filename. The grammar does not
// constrain the parameter order, and both orders occur in the wild. Hoisted
// because the previous shape compiled a regex per parameter per part.
const __CD_NAME =
  /(?:^|;)\s*name\s*=\s*(?:"((?:[^"\\]|\\.)*)"|([^;]*))/i;
const __CD_FILENAME =
  /(?:^|;)\s*filename\s*=\s*(?:"((?:[^"\\]|\\.)*)"|([^;]*))/i;
// Body -> FormData for multipart/form-data and
// application/x-www-form-urlencoded. Kept out of the class so V8 only
// pre-parses it; the body is compiled on first actual formData() call.
//
// `bytes` is the undecoded body. A file part must reach `File` as the bytes
// that arrived: decoding the body first and slicing part bodies out of the
// string replaced every non-UTF-8 sequence with U+FFFD, and the re-encode in
// the `Blob` constructor then wrote those replacement bytes into the file.
__cello.__parseFormData = (bytes, contentType) => {
  const ct = String(contentType || "");
  if (/^\s*application\/x-www-form-urlencoded/i.test(ct)) {
    // This form is decoded as UTF-8, so a declared charset that is not UTF-8
    // cannot be honoured. Refuse rather than mis-decode.
    const charset = /;\s*charset\s*=\s*"?([^";]+)/i.exec(ct);
    if (charset && !/^utf-?8$/i.test(charset[1].trim()))
      throw new TypeError(
        `Unsupported charset "${charset[1].trim()}". FormData can only ` +
        "parse UTF-8 encoded bodies.");
    const form = new FormData();
    for (const [key, value] of new URLSearchParams(
      new TextDecoder().decode(bytes)))
      form.append(key, value);
    return form;
  }
  if (!/^\s*multipart\/form-data/i.test(ct))
    throw new TypeError(
      "Unrecognized Content-Type header value. FormData can only " +
      "parse the following MIME types: multipart/form-data, " +
      "application/x-www-form-urlencoded");
  const found = /boundary\s*=\s*(?:"([^"]*)"|([^;]+))/i.exec(ct);
  if (!found)
    throw new TypeError(
      "No boundary was found in the multipart/form-data " +
      "Content-Type header.");
  const boundary = (found[1] !== undefined ? found[1] : found[2]).trim();
  const form = new FormData();
  const delimiter = new TextEncoder().encode("--" + boundary);
  // Walk the delimiters rather than splitting on them, so that a body whose
  // framing is truncated or corrupt is refused instead of silently yielding
  // the parts that happened to parse. Only the framing is checked; a part's
  // own bytes are carried through unexamined. The preamble before the first
  // delimiter and the epilogue after the closing one are ignored.
  let cursor = __delimiterIndex(bytes, delimiter, 0);
  if (cursor < 0)
    throw new TypeError(
      "No boundary delimiter was found in the multipart/form-data body.");
  cursor += delimiter.length;
  for (;;) {
    // What follows a delimiter decides everything: "--" closes the body, a
    // line break opens another part, and anything else is malformed.
    if (bytes[cursor] === __DASH && bytes[cursor + 1] === __DASH) break;
    const lineBreak = bytes[cursor] === __CR && bytes[cursor + 1] === __LF
      ? 2
      : bytes[cursor] === __LF ? 1 : 0;
    if (lineBreak === 0)
      throw new TypeError(
        "A boundary delimiter must be followed by CRLF, LF, or \"--\".");
    cursor += lineBreak;
    const partStart = cursor;
    const next = __delimiterIndex(bytes, delimiter, partStart);
    if (next < 0)
      throw new TypeError(
        "The multipart/form-data body ended without a closing boundary " +
        "delimiter.");
    // The line break before a delimiter belongs to the delimiter, not to the
    // part it terminates.
    let chunkEnd = next;
    if (chunkEnd > partStart && bytes[chunkEnd - 1] === __LF) {
      chunkEnd -= 1;
      if (chunkEnd > partStart && bytes[chunkEnd - 1] === __CR) chunkEnd -= 1;
    }
    cursor = next + delimiter.length;
    // The headers end at the first blank line. Either line break can be CRLF
    // or a bare LF, which accepted inputs can contain.
    let headerEnd = -1, bodyStart = -1;
    for (let i = partStart; i < chunkEnd; i++) {
      const first = bytes[i] === __CR && bytes[i + 1] === __LF
        ? 2
        : bytes[i] === __LF ? 1 : 0;
      if (first === 0) continue;
      const after = i + first;
      const second = bytes[after] === __CR && bytes[after + 1] === __LF
        ? 2
        : bytes[after] === __LF ? 1 : 0;
      if (second === 0) continue;
      headerEnd = i;
      bodyStart = after + second;
      break;
    }
    if (headerEnd < 0)
      throw new TypeError(
        "A FormData part's headers must be terminated by a blank line.");
    // Part headers are text. Decoding this slice as UTF-8 keeps a non-ASCII
    // name or filename intact and cannot reach the part body.
    const rawHeaders = new TextDecoder().decode(
      bytes.subarray(partStart, headerEnd));
    const body = bytes.subarray(bodyStart, chunkEnd);
    let disposition = null;
    let type = "";
    for (const line of rawHeaders.split(/\r?\n/)) {
      const colon = line.indexOf(":");
      if (colon < 0) continue;
      const name = line.slice(0, colon).trim().toLowerCase();
      const value = line.slice(colon + 1).trim();
      if (name === "content-disposition") disposition = value;
      else if (name === "content-type") type = value;
    }
    if (disposition === null)
      throw new TypeError(
        "Content-Disposition header is required for each FormData " +
        "part.");
    const kind = disposition.split(";")[0].trim();
    if (kind.toLowerCase() !== "form-data")
      throw new TypeError(
        "Content-Disposition header for FormData part must have the " +
        "value \"form-data\", possibly followed by parameters. Got: " +
        "\"" + kind + "\"");
    const param = (pattern) => {
      const m = pattern.exec(disposition);
      if (!m) return undefined;
      const raw = m[1] !== undefined ? m[1] : (m[2] || "").trim();
      if (/\\$/.test(raw.replace(/\\\\/g, "")))
        throw new TypeError(
          "Name or filename can't end with backslash");
      return raw.replace(/\\(.)/g, "$1");
    };
    const name = param(__CD_NAME);
    if (name === undefined)
      throw new TypeError(
        "Content-Disposition header for FormData part must have a " +
        "name parameter.");
    const filename = param(__CD_FILENAME);
    // A part without a filename is an entry value, which the spec defines as a
    // string, so this one part is decoded as UTF-8. A file part keeps its
    // bytes.
    if (filename === undefined)
      form.append(name, new TextDecoder().decode(body));
    else
      form.append(
        name, new File([body], filename, { type: type || "" }));
  }
  return form;
};
  // Spec entry conversion: strings stay strings; a Blob becomes a File (a
  // File is renamed only when a filename is supplied).
  // A Blob appended without a filename is stored as a File named after the
  // entry key. That is workerd's choice; the WHATWG spec says "blob" (#188).
  const __formDataValue = (value, filename, name) => {
    if (!(value instanceof Blob)) return String(value);
    if (value instanceof File && filename === undefined) return value;
    return new File(
      [value],
      filename === undefined ? String(name) : String(filename),
      {
        type: value.type,
        lastModified:
          value instanceof File ? value.lastModified : undefined,
      },
    );
  };
  // The entry list is a private slot: `Object.keys(new FormData())` used to
  // expose the live backing array, so a worker could forge entries by
  // pushing into it (#170, #206).
  const __formDataInstances = new __primWeakSet();
  __cello.$$isFormDataInstance = (value) =>
    __weakSetHas(__formDataInstances, value);
  globalThis.FormData = class FormData {
    #entries = [];
    constructor() {
      __weakSetAdd(__formDataInstances, this);
    }
    append(name, value, filename) {
      const key = String(name);
      this.#entries.push(
        [key, __formDataValue(value, filename, key)]);
    }
    set(name, value, filename) {
      const key = String(name);
      value = __formDataValue(value, filename, key);
      // Spec: replace the first match in place, keeping its position,
      // and drop any later matches.
      const first = this.#entries.findIndex(([k]) => k === key);
      if (first < 0) { this.#entries.push([key, value]); return; }
      this.#entries[first] = [key, value];
      for (let i = this.#entries.length - 1; i > first; i--)
        if (this.#entries[i][0] === key) this.#entries.splice(i, 1);
    }
    get(name) {
      const row = this.#entries.find(([key]) => key === String(name));
      return row ? row[1] : null;
    }
    getAll(name) {
      return this.#entries.filter(([key]) => key === String(name)).map(([, value]) => value);
    }
    has(name) { return this.#entries.some(([key]) => key === String(name)); }
    delete(name) {
      const key = String(name);
      // Splice in place: the iterators below read the live list, so the
      // array identity must survive a delete during iteration.
      for (let i = this.#entries.length - 1; i >= 0; i--)
        if (this.#entries[i][0] === key) this.#entries.splice(i, 1);
    }
    // Iteration is live: the iterator holds an index and re-reads the
    // entry list, so entries appended during iteration are visited and
    // an exhausted iterator resumes if entries are added later. A
    // generator cannot do this -- once it returns it is done forever.
    #iterate(pick) {
      const self = this;
      let index = 0;
      const iterator = {
        next() {
          if (index >= self.#entries.length)
            return { value: undefined, done: true };
          return { value: pick(self.#entries[index++]), done: false };
        },
        [Symbol.iterator]() { return iterator; },
      };
      return iterator;
    }
    entries() { return this.#iterate((e) => [e[0], e[1]]); }
    keys() { return this.#iterate((e) => e[0]); }
    values() { return this.#iterate((e) => e[1]); }
    [Symbol.iterator]() { return this.entries(); }
    forEach(callback, thisArg) {
      if (typeof callback !== "function")
        throw new TypeError(
          "Failed to execute 'forEach' on 'FormData': parameter 1 is " +
          "not of type 'Function'.");
      for (const [key, value] of this.entries())
        callback.call(thisArg, value, key, this);
    }
  };
  // WebIDL enumerability, the interface tag and a zero-length constructor,
  // matching workerd's reflected shape (#171, #206).
  for (const key of Object.getOwnPropertyNames(FormData.prototype)) {
    if (key === "constructor") continue;
    const descriptor = Object.getOwnPropertyDescriptor(FormData.prototype, key);
    if (descriptor === undefined || descriptor.enumerable) continue;
    descriptor.enumerable = true;
    Object.defineProperty(FormData.prototype, key, descriptor);
  }
  Object.defineProperty(FormData.prototype, Symbol.toStringTag, {
    value: "FormData", configurable: true, writable: true, enumerable: false,
  });
  Object.defineProperty(FormData, "length", {
    value: 0, configurable: true, writable: false,
  });
}
// Node's Buffer lives in src/js/node_buffer.js, compiled
// lazily (LAZY_GLOBALS / LAZY_MODULES) on first use.
// Web API globals a real bundle references at module scope but that the
// prelude doesn't provide. Stub as empty classes (guarded so we never
// clobber a real prelude implementation). Enough to LOAD; every name in
// this first list has a real implementation elsewhere, so the empty class
// only exists for the load-order window.
for (const n of ["WebSocket", "EventTarget", "Event", "MessageEvent",
  "CloseEvent", "ErrorEvent", "SubtleCrypto", "TransformStream",
  "ReadableStream", "WritableStream", "ReadableStreamDefaultReader",
  "WritableStreamDefaultWriter",
  "ByteLengthQueuingStrategy", "CountQueuingStrategy",
  "TextEncoderStream", "TextDecoderStream"]) {
  if (!globalThis[n]) globalThis[n] = class {};
}
// A BroadcastChannel has no implementation anywhere in cello. An empty class
// would let `new BroadcastChannel(...)` hand back an inert object that fails
// far from the cause; the compat contract is "reject at first use", so the
// name loads but construction throws. (EventSource, MessageChannel, and
// MessagePort are real; their scripts run after the harness.) FileReader is
// not here: workerd has no such global, so neither does cello.
for (const n of ["BroadcastChannel"]) {
  if (!globalThis[n]) globalThis[n] = class {
    constructor() {
      throw new Error(n + " is not implemented in cello");
    }
  };
}
const __sockets = __cello.__sockets = new Map();
// Installed by the class below; the runtime's own paths (the DO hibernation
// hooks, the response and pair plumbing) drive a socket through these, so no
// host helper and no host field is a guest-visible member (#206).
let __wsHooks;
const __wsState = (ws) => __wsHooks.state(ws);
globalThis.WebSocket = class WebSocket extends EventTarget {
  #s;
  static #expose = (__wsHooks = {
    state: (ws) => ws.#s,
    dispatchMessage: (ws, data) => ws.#dispatchMessage(data),
    dispatchClose: (ws, code, reason, wasClean) =>
      ws.#dispatchClose(code, reason, wasClean),
    flushPending: (ws) => ws.#flushPending(),
    flushToPeer: (ws) => ws.#flushToPeer(),
    startPump: (ws) => ws.#startPump(),
  });
  // The spec names, which workerd exposes and real bundles use. cello only
  // had the READY_STATE_* aliases, which no other runtime defines. Static
  // fields are enumerable, as workerd's interface statics are.
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;
  static READY_STATE_CONNECTING = 0;
  static READY_STATE_OPEN = 1;
  static READY_STATE_CLOSING = 2;
  static READY_STATE_CLOSED = 3;
  constructor(id, protocols = [], boundTarget = null) {
    super();
    const dialed = typeof id === "string";
    // A socket bound to a Durable Object's socket behaves like a dialed one
    // in every way that matters here: the host carries its frames, so every
    // operation goes out through `__ws_*` on this id rather than to a peer
    // in this heap.
    const outbound = dialed || boundTarget !== null;
    this.#s = {
      outbound,
      boundTarget,
      bound: false,
      pendingClose: null,
      id: outbound ? __ws_alloc() : Number(id),
      attachment: undefined,
      target: null,
      accepted: false,
      // Accepting for hibernation is a second, distinct acceptance:
      // workerd refuses `acceptWebSocket()` once either form ran.
      hibernationAccepted: false,
      hibernatable: false,
      pending: [],
      // The other end of an in-isolate upgrade (a WebSocketPair served over
      // a same-script service binding); frames route to it directly instead
      // of through the host connection.
      loopback: null,
      peer: null,
      tags: [],
      polled: false,
      pumping: false,
      readyState: outbound ? 0 : 1,
      binaryType: __cell.compat?.websocketStandardBinaryType
        ? "blob"
        : "arraybuffer",
      // A server-side socket has no URL, and reports null rather than an
      // empty string -- workerd's inspect output pins this. A socket that
      // came back from an upgrade has none either: nothing in this isolate
      // dialed it.
      url: dialed ? id : null,
      protocol: "",
      extensions: "",
    };
    const state = this.#s;
    if (boundTarget !== null) {
      // The caller drives this socket itself, exactly as a Worker drives one
      // it opened. The host pipe is not built until `accept()`, so a
      // response passed straight back out is left alone.
      state.polled = true;
      state.target = boundTarget;
    }
    if (dialed) {
      // workerd validates the URL in the constructor and throws
      // synchronously; letting the connector reject later would surface a
      // scheme mistake as a network failure instead of a TypeError.
      let parsed;
      try {
        parsed = new URL(id);
      } catch {
        throw new DOMException(
          "WebSocket Constructor: The url is invalid.",
          "SyntaxError");
      }
      if (parsed.protocol !== "ws:" && parsed.protocol !== "wss:") {
        throw new DOMException(
          "WebSocket Constructor: The url scheme must be ws or wss.",
          "SyntaxError");
      }
      // A Durable Object socket is pushed events by the host so it can revive
      // a hibernated cell. A Worker socket has no cell: the isolate polls it,
      // and it lives and dies with the request, which is the lifetime
      // Cloudflare gives one too.
      const scope = __currentActorScope();
      state.polled = !scope;
      const requested = typeof protocols === "string"
        ? [protocols]
        : Array.from(protocols, String);
      for (const value of requested) {
        // RFC 6455 subprotocols are HTTP tokens; a space or separator would
        // otherwise be smuggled into the Sec-WebSocket-Protocol header.
        if (!/^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/.test(value)) {
          throw new DOMException(
            `WebSocket Constructor: The subprotocol '${value}' is invalid.`,
            "SyntaxError");
        }
      }
      if (new Set(requested).size !== requested.length) {
        throw new DOMException(
          "WebSocket Constructor: The subprotocols must be unique.",
          "SyntaxError");
      }
      __sockets.set(state.id, this);
      const connection = __ws_connect(state.id, scope, id, JSON.stringify(requested));
      __registerWaitUntil(connection);
      if (state.polled) this.#startPump();
      connection.then((protocol) => {
        if (state.readyState === WebSocket.READY_STATE_CLOSING) {
          if (state.pendingClose) {
            const [code, reason] = state.pendingClose;
            __ws_close(state.id, code, reason);
          }
          return;
        }
        if (state.readyState !== WebSocket.READY_STATE_CONNECTING) return;
        state.protocol = protocol;
        state.readyState = WebSocket.READY_STATE_OPEN;
        this.dispatchEvent(new Event("open"));
      }, (error) => {
        state.readyState = WebSocket.READY_STATE_CLOSED;
        __sockets.delete(state.id);
        this.dispatchEvent(new ErrorEvent("error", { message: String(error?.message || error), error }));
        this.#dispatchClose(1006, "", false);
      });
    }
  }
  get readyState() { return this.#s.readyState; }
  // No frame is buffered across the host op: send() either reaches the host
  // synchronously or queues on an unbound pair, so workerd's always-zero
  // answer is the honest one.
  get bufferedAmount() { return 0; }
  get url() { return this.#s.url; }
  get protocol() { return this.#s.protocol; }
  get extensions() { return this.#s.extensions; }
  get binaryType() { return this.#s.binaryType; }
  // The spec ignores an unrecognised value rather than throwing.
  set binaryType(value) {
    if (value === "blob" || value === "arraybuffer")
      this.#s.binaryType = value;
  }
  // The pump ends only when the socket closes, so it must NOT be registered
  // as waitUntil work: that would hold the request open for as long as the
  // socket lives, and the host reclaims an abandoned socket only once the
  // request has retired. Its `__ws_next` ops are driven while the request
  // runs and normally resolve with the host's 1001 "request ended" close.
  // They resolve as 1006 only if the sender disappears without a close frame.
  #startPump() {
    const state = this.#s;
    if (state.pumping) return;
    state.pumping = true;
    this.#pump().catch(() => {});
  }
  // Drain the host queue for an isolate-polled socket. Each `__ws_next` is an
  // ordinary async op the request owns; a request that retires with the
  // socket still open closes it, which ends this loop with a close frame.
  async #pump() {
    const state = this.#s;
    for (;;) {
      const frame = await __ws_next(state.id);
      const tag = frame[0];
      const body = frame.subarray(1);
      if (tag === 0) {
        this.#dispatchMessage(new TextDecoder().decode(body));
      } else if (tag === 1) {
        this.#dispatchMessage(
          body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength),
        );
      } else if (tag === 2) {
        if (state.readyState === WebSocket.READY_STATE_CONNECTING) {
          state.protocol = new TextDecoder().decode(body);
          state.readyState = WebSocket.READY_STATE_OPEN;
          this.dispatchEvent(new Event("open"));
        }
      } else {
        const info = JSON.parse(new TextDecoder().decode(body));
        this.#dispatchClose(info.code, info.reason, info.wasClean);
        return;
      }
    }
  }
  // Deliver whatever this end queued before it had somewhere to send.
  #flushToPeer() {
    const state = this.#s;
    const peer = state.loopback;
    if (!peer) return;
    for (const frame of state.pending.splice(0)) {
      if (frame[0] === "send") {
        queueMicrotask(() => __wsHooks.dispatchMessage(peer, frame[1]));
      } else if (frame[0] === "send-binary") {
        const data = frame[1];
        const bytes = data instanceof ArrayBuffer
          ? data
          : data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
        queueMicrotask(() => __wsHooks.dispatchMessage(peer, bytes));
      } else {
        queueMicrotask(() =>
          __wsHooks.dispatchClose(peer, frame[1], frame[2], true));
      }
    }
  }
  accept() {
    const state = this.#s;
    state.accepted = true;
    // A pair used directly, without being returned through a 101 response,
    // still has to carry frames once both ends are accepted. cello only
    // linked a pair on the upgrade path, so such a pair queued forever.
    if (state.peer && __wsState(state.peer).accepted && !state.loopback) {
      state.loopback = state.peer;
      __wsState(state.peer).loopback = this;
      this.#flushToPeer();
      __wsHooks.flushToPeer(state.peer);
    }
    // A socket obtained from a `fetch()` upgrade is already connected and
    // delivers nothing until it is accepted; this is where it starts.
    if (state.outbound && state.readyState === WebSocket.READY_STATE_CONNECTING) {
      // Bind before the pump: the op registers this socket's queue on this
      // thread, so the first `__ws_next` cannot run ahead of it.
      if (state.boundTarget !== null && !state.bound) {
        state.bound = true;
        __ws_bind_target(
          state.id,
          JSON.stringify(state.boundTarget),
          __currentActorScope(),
        );
      }
      state.readyState = WebSocket.READY_STATE_OPEN;
      if (state.polled) this.#startPump();
    }
  }
  send(data) {
    const state = this.#s;
    if (state.readyState !== WebSocket.READY_STATE_OPEN)
      throw new DOMException("WebSocket is not open", "InvalidStateError");
    // Workerd (web-socket.c++ send): a socket no accept() or
    // state.acceptWebSocket() has claimed has no transport on either end,
    // and sending is an error rather than a frame dropped without a trace.
    // A socket that a 101 response released, one the host joined, or one the
    // state handed out from `getWebSockets()` is already accepted.
    if (!state.outbound && !state.accepted && !state.target &&
        !state.hibernatable && !state.loopback)
      throw new TypeError(
        "You must call one of accept() or state.acceptWebSocket() on this " +
        "WebSocket before sending messages.");
    // Bytes come from the internal slots (#130); anything else is text.
    const bytes = __hostChunk(
      data, "WebSocket.send accepts a string, ArrayBuffer, or view");
    const binary = bytes !== null;
    const text = binary ? null : String(data);
    if (state.loopback) {
      const peer = state.loopback;
      // `#dispatchMessage` converts an ArrayBuffer per `binaryType`, so hand
      // it the copy's buffer, as `data.slice(0)` did before #130.
      const message = binary ? bytes.slice().buffer : text;
      queueMicrotask(() => __wsHooks.dispatchMessage(peer, message));
      return;
    }
    // The pending queue belongs to an inbound pair socket that has been
    // accepted but not yet bound to a host transport. An outbound socket is
    // already bound -- queueing here would silently swallow its frames.
    if (!state.outbound && state.accepted && !state.target) {
      // Copy at send: the frame waits in the queue, and workerd snapshots the
      // bytes the caller passed rather than aliasing its buffer (#130).
      state.pending.push([
        binary ? "send-binary" : "send", binary ? bytes.slice() : text,
      ]);
      return;
    }
    if (binary) __ws_send_binary(state.id, bytes);
    else __ws_send(state.id, text);
  }
  close(code = 1000, reason = "") {
    // workerd parity, not the WHATWG-only 1000/3000-4999 rule: an endpoint
    // may send 1000, the 1001-1014 application range, and 3000-4999. The
    // reserved no-status codes (1004, 1005, 1006, 1015) must never appear on
    // the wire, so they stay refused. Refusing the 1008 policy-violation
    // close dropped a Worker's refusal on the floor: the listener threw,
    // and the client hung (upstream #279).
    const wanted = Number(code);
    const reserved = wanted === 1004 || wanted === 1005 || wanted === 1006 ||
      wanted === 1015;
    if (!(wanted >= 1000 && wanted <= 4999) || reserved) {
      throw new DOMException(
        `WebSocket close code ${code} is not permitted`,
        "InvalidAccessError");
    }
    // RFC 6455 5.5: the close frame body is at most 125 bytes, two of which
    // are the code. Measured in UTF-8, so a multibyte reason can exceed the
    // cap at well under 123 characters.
    if (new TextEncoder().encode(String(reason)).length > 123) {
      throw new DOMException(
        "WebSocket close reason must not exceed 123 bytes",
        "SyntaxError");
    }
    const state = this.#s;
    // Outbound sockets follow the WebSocket spec: closing a closed socket is a
    // no-op. Inbound sockets must NOT take that shortcut -- a Durable Object
    // answers a peer-initiated close from inside `webSocketClose`, by which
    // point `#dispatchClose` has already marked this side closed, and its
    // reply carries the reason the client is entitled to see.
    if (state.outbound && state.readyState === WebSocket.READY_STATE_CLOSED) {
      return;
    }
    const connecting = state.readyState === WebSocket.READY_STATE_CONNECTING;
    state.readyState = WebSocket.READY_STATE_CLOSING;
    if (state.outbound && connecting) {
      state.pendingClose = [code, reason];
      return;
    }
    if (state.outbound) {
      __ws_close(state.id, code, reason);
      return;
    }
    state.readyState = 3;
    if (state.loopback) {
      const peer = state.loopback;
      queueMicrotask(() => __wsHooks.dispatchClose(peer, code, reason, true));
      return;
    }
    if (state.accepted && !state.target) {
      state.pending.push(["close", code, reason]);
      return;
    }
    __ws_close(state.id, code, reason);
  }
  #flushPending() {
    const state = this.#s;
    const pending = state.pending.splice(0);
    for (const frame of pending) {
      if (frame[0] === "send") __ws_send(state.id, frame[1]);
      else if (frame[0] === "send-binary") {
        // The queue holds an internal view: send() read the guest's bytes
        // from the internal slots when it queued the frame (#130).
        __ws_send_binary(state.id, frame[1]);
      }
      else __ws_close(state.id, frame[1], frame[2]);
    }
  }
  serializeAttachment(value) {
    // Structured clone, not JSON: Cloudflare accepts anything cloneable
    // here, so a Date must come back a Date and a Map a Map.
    const state = this.#s;
    state.attachment = value;
    __ws_attachment_set(state.id, __scEncodeStrict(value));
  }
  deserializeAttachment() { return this.#s.attachment; }
  #dispatchMessage(data) {
    const state = this.#s;
    if (state.binaryType === "blob" && data instanceof ArrayBuffer) {
      data = new Blob([data]);
    }
    const event = new MessageEvent("message", { data });
    __cello.$$trustEvent(event);
    this.dispatchEvent(event);
  }
  #dispatchClose(code, reason, wasClean) {
    if (this.#s.readyState === WebSocket.READY_STATE_CLOSED) return;
    const state = this.#s;
    state.readyState = WebSocket.READY_STATE_CLOSED;
    __sockets.delete(state.id);
    // An abnormal closure fires `error` before `close`. Code written against
    // the standard onerror never ran otherwise, which matters most for an
    // outbound socket: its peer is a remote server that can simply vanish.
    if (!wasClean) {
      this.dispatchEvent(new ErrorEvent("error", {
        message: reason
          ? `WebSocket closed abnormally: ${reason}`
          : "WebSocket closed abnormally",
      }));
    }
    const event = new CloseEvent("close", { code, reason, wasClean });
    __cello.$$trustEvent(event);
    this.dispatchEvent(event);
  }
};
// Workerd also carries the state constants on the prototype, enumerable.
for (const [name, value] of [
  ["READY_STATE_CONNECTING", 0], ["CONNECTING", 0],
  ["READY_STATE_OPEN", 1], ["OPEN", 1],
  ["READY_STATE_CLOSING", 2], ["CLOSING", 2],
  ["READY_STATE_CLOSED", 3], ["CLOSED", 3],
]) {
  Object.defineProperty(globalThis.WebSocket.prototype, name, {
    value, writable: false, enumerable: true, configurable: false,
  });
}
__webidlTag(globalThis.WebSocket, "WebSocket");
__webidlPrototype(globalThis.WebSocket);
__webidlLength(globalThis.WebSocket);
const __makeSocket = __cello.__makeSocket = (id) => new WebSocket(id);
const __socketFromRow = __cello.__socketFromRow = (row) => {
  let ws = __sockets.get(Number(row.id));
  if (!ws) {
    ws = __makeSocket(row.id);
    __sockets.set(Number(row.id), ws);
  }
  const wsState = __wsState(ws);
  wsState.tags = row.tags || [];
  wsState.hibernatable = true;
  if (row.attachment != null) {
    try {
      wsState.attachment = __sc_decode(new Uint8Array(row.attachment));
    } catch {
      wsState.attachment = undefined;
    }
  }
  return ws;
};
// Workerd's api::WebSocketRequestResponsePair (websocket.h): an immutable
// request/response pair for state.setWebSocketAutoResponse.
globalThis.WebSocketRequestResponsePair =
  class WebSocketRequestResponsePair {
    #request;
    #response;
    constructor(request, response) {
      this.#request = String(request);
      this.#response = String(response);
    }
    get request() { return this.#request; }
    get response() { return this.#response; }
  };
globalThis.WebSocketPair = function WebSocketPair() {
  const id = __ws_alloc();
  const client = __makeSocket(id);
  const server = __makeSocket(id);
  __wsState(client).peer = server;
  __wsState(server).peer = client;
  // Indexed AND iterable: `const [client, server] = new WebSocketPair()` is
  // the form most Cloudflare code uses. No own `length` -- workerd's pair is
  // `{0, 1}`, so Object.keys/spread see exactly the two sockets.
  return {
    0: client,
    1: server,
    [Symbol.iterator]() {
      return [client, server][Symbol.iterator]();
    },
  };
};
// Cloudflare exposes the time of the last I/O, not a clock that advances
// while JavaScript runs. Keep Date and performance on one turn timestamp so
// neither becomes a timing side channel and the two APIs cannot disagree.
const __NativeDate = Date;
let __ioTimestamp = 0;
const __CloudflareDate = function Date(...args) {
  if (new.target) {
    return Reflect.construct(
      __NativeDate,
      args.length === 0 ? [__ioTimestamp] : args,
      new.target,
    );
  }
  return new __NativeDate(__ioTimestamp).toString();
};
Object.defineProperties(__CloudflareDate, {
  length: { value: 7 },
  prototype: { value: __NativeDate.prototype },
  now: {
    value: () => __ioTimestamp,
    writable: true,
    configurable: true,
  },
  parse: {
    value: __NativeDate.parse,
    writable: true,
    configurable: true,
  },
  UTC: {
    value: __NativeDate.UTC,
    writable: true,
    configurable: true,
  },
});
Object.defineProperty(__NativeDate.prototype, "constructor", {
  value: __CloudflareDate,
  writable: true,
  configurable: true,
});
globalThis.Date = __CloudflareDate;
// ---- Performance ---------------------------------------------------------
// Workerd's clock is I/O-bound: `Date.now()` and `performance.now()` both
// read the turn's last-I/O timestamp, so neither advances while JavaScript
// runs. The entry/observer surface is bookkeeping on top of that clock.
const __perfData = new WeakMap();
const __perfEntries = [];
const __perfToken = Symbol("cello.Performance");
const __perfEntryToken = Symbol("cello.PerformanceEntry");
const __perfListToken = Symbol("cello.PerformanceObserverEntryList");
const __perfDetail = (detail) => {
  if (detail === undefined) return null;
  if (detail === null ||
      (typeof detail !== "object" && typeof detail !== "function"))
    throw new TypeError(
      "Incorrect type for the 'detail' field on 'Options': the provided " +
      "value is not of type 'Object'.");
  return detail;
};
const __perfRecord = (entry) => __perfData.get(entry);

class PerformanceEntry {
  constructor(...args) {
    if (args[0] !== __perfEntryToken)
      throw new TypeError("Illegal constructor");
  }
  get name() { return __perfRecord(this).name; }
  get entryType() { return __perfRecord(this).entryType; }
  get startTime() { return __perfRecord(this).startTime; }
  get duration() { return __perfRecord(this).duration; }
  toJSON() {
    const entry = __perfRecord(this);
    return {
      name: entry.name, entryType: entry.entryType,
      startTime: entry.startTime, duration: entry.duration,
    };
  }
  get [Symbol.toStringTag]() { return "PerformanceEntry"; }
}

// Workerd's PerformanceMark is the one constructible entry, and its
// `detail` is held by reference (measured: mutating the init object after
// mark() changes the entry).
class PerformanceMark extends PerformanceEntry {
  constructor(...args) {
    if (args.length === 0)
      throw new TypeError(
        "Failed to construct 'PerformanceMark': constructor parameter 1 " +
        "is not of type 'string'.");
    const name = String(args[0]);
    super(__perfEntryToken);
    const options = args[1] == null ? {} : args[1];
    __perfData.set(this, {
      name,
      entryType: "mark",
      startTime: options.startTime === undefined
        ? __ioTimestamp : Number(options.startTime),
      duration: 0,
      detail: __perfDetail(options.detail),
    });
  }
  get detail() { return __perfRecord(this).detail; }
  toJSON() {
    const entry = __perfRecord(this);
    const out = {
      name: entry.name, entryType: entry.entryType,
      startTime: entry.startTime, duration: entry.duration,
    };
    if (entry.detail !== null) out.detail = entry.detail;
    return out;
  }
  get [Symbol.toStringTag]() { return "PerformanceMark"; }
}

class PerformanceMeasure extends PerformanceEntry {
  constructor(...args) {
    if (args[0] !== __perfEntryToken)
      throw new TypeError("Illegal constructor");
    super(__perfEntryToken);
  }
  get detail() { return __perfRecord(this).detail; }
  toJSON() {
    const entry = __perfRecord(this);
    const out = {
      name: entry.name, entryType: entry.entryType,
      startTime: entry.startTime, duration: entry.duration,
    };
    if (entry.detail !== null) out.detail = entry.detail;
    return out;
  }
  get [Symbol.toStringTag]() { return "PerformanceMeasure"; }
}

class PerformanceResourceTiming extends PerformanceEntry {
  constructor(...args) {
    if (args[0] !== __perfEntryToken)
      throw new TypeError("Illegal constructor");
    super(__perfEntryToken);
  }
  get initiatorType() { return __perfRecord(this).initiatorType; }
  get nextHopProtocol() { return ""; }
  get workerStart() { return 0; }
  get redirectStart() { return 0; }
  get redirectEnd() { return 0; }
  get fetchStart() { return 0; }
  get domainLookupStart() { return 0; }
  get domainLookupEnd() { return 0; }
  get connectStart() { return 0; }
  get connectEnd() { return 0; }
  get secureConnectionStart() { return 0; }
  get requestStart() { return 0; }
  get responseStart() { return 0; }
  get responseStatus() { return 0; }
  get responseEnd() { return 0; }
  get transferSize() { return 0; }
  get encodedBodySize() { return 0; }
  get decodedBodySize() { return 0; }
  get [Symbol.toStringTag]() { return "PerformanceResourceTiming"; }
}

// Workerd's nodeTiming is a PerformanceNodeTiming whose fields carry zeros
// (there is no libuv loop here); the shape is what code reads.
class PerformanceNodeTiming extends PerformanceEntry {
  constructor(...args) {
    if (args[0] !== __perfEntryToken)
      throw new TypeError("Illegal constructor");
    super(__perfEntryToken);
    const fields = {
      nodeStart: 0, v8Start: 0, bootstrapComplete: 0, environment: 0,
      loopStart: 0, loopExit: 0, idleTime: 0,
      uvMetricsInfo: { loopCount: 0, events: 0, eventsWaiting: 0 },
    };
    __perfData.set(this, {
      name: "node", entryType: "node", startTime: 0, duration: 0, ...fields,
    });
    for (const key of Object.keys(fields)) {
      Object.defineProperty(this, key, {
        value: fields[key], enumerable: true, writable: false,
        configurable: true,
      });
    }
  }
  toJSON() {
    const entry = __perfRecord(this);
    return {
      name: entry.name, entryType: entry.entryType,
      startTime: entry.startTime, duration: entry.duration,
      nodeStart: entry.nodeStart, v8Start: entry.v8Start,
      bootstrapComplete: entry.bootstrapComplete,
      environment: entry.environment, loopStart: entry.loopStart,
      loopExit: entry.loopExit, idleTime: entry.idleTime,
      uvMetricsInfo: entry.uvMetricsInfo,
    };
  }
  get [Symbol.toStringTag]() { return "PerformanceNodeTiming"; }
}

// An EventCounts is a Map in shape only; the prototype members are its own,
// as workerd's are.
class EventCounts {
  #map = new Map();
  get size() { return this.#map.size; }
  get(key) { return this.#map.get(key); }
  has(key) { return this.#map.has(key); }
  entries() { return this.#map.entries(); }
  keys() { return this.#map.keys(); }
  values() { return this.#map.values(); }
  forEach(callback, thisArg) { this.#map.forEach(callback, thisArg); }
  get [Symbol.toStringTag]() { return "EventCounts"; }
}

class PerformanceObserverEntryList {
  #entries;
  constructor(...args) {
    if (args[0] !== __perfListToken)
      throw new TypeError("Illegal constructor");
  }
  getEntries() { return (this.#entries ?? []).slice(); }
  getEntriesByType(type) {
    return (this.#entries ?? []).filter(
      (entry) => entry.entryType === String(type));
  }
  getEntriesByName(name, type) {
    return (this.#entries ?? []).filter(
      (entry) => entry.name === String(name) &&
        (type === undefined || entry.entryType === String(type)));
  }
  get [Symbol.toStringTag]() { return "PerformanceObserverEntryList"; }
}

// Workerd's PerformanceObserver never delivers a callback inside a request
// (measured: a mark, a real timer turn and a takeRecords all leave the
// callback unrun), so this one records nothing and hands back an empty
// array. Feature detection and construction are what code exercises.
class PerformanceObserver {
  static supportedEntryTypes = ["measure", "mark"];
  #callback;
  constructor(...args) { this.#callback = args[0]; }
  observe() {}
  disconnect() {}
  takeRecords() { return []; }
  get [Symbol.toStringTag]() { return "PerformanceObserver"; }
}

const __perfTime = (value) => {
  if (typeof value === "number") return value;
  if (value === undefined) return undefined;
  const mark = __perfEntries.find(
    (entry) => entry.entryType === "mark" && entry.name === String(value));
  return mark === undefined ? undefined : mark.startTime;
};

class Performance extends EventTarget {
  #nodeTiming;
  constructor(...args) {
    if (args[0] !== __perfToken)
      throw new TypeError("Illegal constructor");
    super();
    this.#nodeTiming = new PerformanceNodeTiming(__perfEntryToken);
  }
  get timeOrigin() { return 0; }
  now() { return __ioTimestamp; }
  get eventCounts() { return __eventCounts; }
  clearMarks(name) {
    __perfClear("mark", name);
  }
  clearMeasures(name) {
    __perfClear("measure", name);
  }
  clearResourceTimings() {
    __perfClear("resource", undefined);
  }
  getEntries() { return __perfEntries.slice(); }
  getEntriesByName(name, type) {
    return __perfEntries.filter(
      (entry) => entry.name === String(name) &&
        (type === undefined || entry.entryType === String(type)));
  }
  getEntriesByType(type) {
    return __perfEntries.filter(
      (entry) => entry.entryType === String(type));
  }
  mark(...args) {
    if (args.length === 0)
      throw new TypeError(
        "Failed to execute 'mark' on 'Performance': parameter 1 is not " +
        "of type 'string'.");
    const name = String(args[0]);
    const entry = new PerformanceMark(name, args[1]);
    __perfEntries.push(entry);
    return entry;
  }
  measure(...args) {
    if (args.length === 0)
      throw new TypeError(
        "Failed to execute 'measure' on 'Performance': parameter 1 is " +
        "not of type 'string'.");
    const name = String(args[0]);
    const second = args[1];
    let start;
    let end;
    let detail = null;
    if (second !== null && typeof second === "object") {
      start = __perfTime(second.start) ?? 0;
      end = __perfTime(second.end) ?? __ioTimestamp;
      detail = __perfDetail(second.detail);
    } else {
      start = __perfTime(second) ?? 0;
      end = __perfTime(args[2]) ?? __ioTimestamp;
    }
    const entry = new PerformanceMeasure(__perfEntryToken);
    __perfData.set(entry, {
      name, entryType: "measure", startTime: start,
      duration: Math.max(0, end - start), detail,
    });
    __perfEntries.push(entry);
    return entry;
  }
  setResourceTimingBufferSize() {}
  toJSON() { return { timeOrigin: 0 }; }
  get nodeTiming() { return this.#nodeTiming; }
  eventLoopUtilization() {
    return { idle: 0, active: 0, utilization: 0 };
  }
  markResourceTiming() {}
  timerify(...args) {
    const fn = args[0];
    if (typeof fn !== "function")
      throw new TypeError(
        "Failed to execute 'timerify' on 'Performance': parameter 1 is " +
        "not of type 'function'.");
    const wrapped = function (...inner) { return fn.apply(this, inner); };
    Object.defineProperty(wrapped, "name", { value: "", configurable: true });
    Object.defineProperty(wrapped, "length", {
      value: fn.length, configurable: true,
    });
    return wrapped;
  }
  get [Symbol.toStringTag]() { return "Performance"; }
}
const __perfClear = (entryType, name) => {
  for (let index = __perfEntries.length - 1; index >= 0; index--) {
    const entry = __perfEntries[index];
    if (entry.entryType !== entryType) continue;
    if (name !== undefined && entry.name !== String(name)) continue;
    __perfEntries.splice(index, 1);
  }
};
const __eventCounts = new EventCounts();
globalThis.PerformanceEntry = PerformanceEntry;
globalThis.PerformanceMark = PerformanceMark;
globalThis.PerformanceMeasure = PerformanceMeasure;
globalThis.PerformanceResourceTiming = PerformanceResourceTiming;
globalThis.PerformanceObserverEntryList = PerformanceObserverEntryList;
globalThis.PerformanceObserver = PerformanceObserver;
globalThis.Performance = Performance;
globalThis.performance = new Performance(__perfToken);
__cello.__advanceIoTime = (timestamp) => {
  __ioTimestamp = timestamp;
};
// Workerd ServiceWorkerGlobalScope: `self` is the global scope, as a writable,
// enumerable data property. Libraries probe it to find the global, and
// @cloudflare/worker-bundler throws when it is absent.
globalThis.self = globalThis;
// Navigator: the five values are own, non-writable data properties of the
// one instance, and `sendBeacon` is the only prototype member (workerd
// installs it that way). `platform` is the empty string there.
const __navigatorToken = Symbol("cello.Navigator");
class Navigator {
  constructor(...args) {
    if (args[0] !== __navigatorToken)
      throw new TypeError("Illegal constructor");
    const own = (value) => ({
      value, enumerable: true, writable: false, configurable: true,
    });
    Object.defineProperties(this, {
      languages: own(["en"]),
      language: own("en"),
      platform: own(""),
      hardwareConcurrency: own(1),
      userAgent: own("Cloudflare-Workers"),
    });
  }
  // Workerd queues a POST and reports whether it was queued; the request's
  // own failure never reaches the caller. A data: URL stays inside the
  // isolate, so a probe of the return value sends nothing outward.
  sendBeacon(...args) {
    if (args.length === 0)
      throw new TypeError(
        "Failed to execute 'sendBeacon' on 'Navigator': parameter 1 is " +
        "not of type 'string'.");
    const url = String(args[0]);
    try {
      Promise.resolve(fetch(url, { method: "POST", body: args[1] ?? null }))
        .catch(() => {});
    } catch { /* queueing never throws */ }
    return true;
  }
  get [Symbol.toStringTag]() { return "Navigator"; }
}
globalThis.Navigator = Navigator;
globalThis.navigator = new Navigator(__navigatorToken);
// The remaining interface constructors workerd exposes. Each exists so
// feature detection and `instanceof` agree; only PerformanceMark and
// PerformanceObserver are constructible from script.
const __illegal = () => {
  throw new TypeError("Illegal constructor");
};
const __interfaceToken = Symbol("cello.interface");
class FetchEvent extends ExtendableEvent {
  constructor(...args) { if (args[0] !== __interfaceToken) __illegal(); }
  respondWith() {}
  passThroughOnException() {}
  get [Symbol.toStringTag]() { return "FetchEvent"; }
}
class ScheduledEvent extends ExtendableEvent {
  constructor(...args) { if (args[0] !== __interfaceToken) __illegal(); }
  noRetry() {}
  get [Symbol.toStringTag]() { return "ScheduledEvent"; }
}
class TailEvent extends ExtendableEvent {
  constructor(...args) { if (args[0] !== __interfaceToken) __illegal(); }
  get [Symbol.toStringTag]() { return "TailEvent"; }
}
class TraceEvent extends ExtendableEvent {
  constructor(...args) { if (args[0] !== __interfaceToken) __illegal(); }
  get [Symbol.toStringTag]() { return "TailEvent"; }
}
class PromiseRejectionEvent extends Event {
  constructor(...args) { if (args[0] !== __interfaceToken) __illegal(); }
  get [Symbol.toStringTag]() { return "PromiseRejectionEvent"; }
}
class WorkerGlobalScope extends EventTarget {
  constructor(...args) { if (args[0] !== __interfaceToken) __illegal(); }
  get [Symbol.toStringTag]() { return "WorkerGlobalScope"; }
}
class ServiceWorkerGlobalScope extends WorkerGlobalScope {
  constructor(...args) { if (args[0] !== __interfaceToken) __illegal(); }
  get [Symbol.toStringTag]() { return "ServiceWorkerGlobalScope"; }
}
class Crypto {
  constructor(...args) { if (args[0] !== __interfaceToken) __illegal(); }
  get [Symbol.toStringTag]() { return "Crypto"; }
}
globalThis.FetchEvent = FetchEvent;
globalThis.ScheduledEvent = ScheduledEvent;
globalThis.TailEvent = TailEvent;
globalThis.TraceEvent = TraceEvent;
// Workerd's TraceEvent carries TailEvent's name (a copy/paste in its
// interface table); the surface dump compares the function name, so match it.
Object.defineProperty(TraceEvent, "name", {
  value: "TailEvent", configurable: true,
});
globalThis.PromiseRejectionEvent = PromiseRejectionEvent;
globalThis.WorkerGlobalScope = WorkerGlobalScope;
globalThis.ServiceWorkerGlobalScope = ServiceWorkerGlobalScope;
// `Body` is the real mixin installed with Request/Response; assigning the
// empty stub here would orphan it, leaving `instanceof Body` false and
// `Body.prototype` with no members (#206).
globalThis.Crypto = Crypto;
// `origin` is a global string on workerd, not the per-request URL.
Object.defineProperty(globalThis, "origin", {
  value: "null", enumerable: true, writable: false, configurable: true,
});
// `reportError`'s message is workerd's `"Uncaught "` plus V8's no-side-effects
// describer (#228). The text comes from the host op `__detail_string`, which
// calls V8's `Object::NoSideEffectsToString`: it reads only data properties --
// never a guest `toString`, never a getter, never a Proxy trap -- and caps a
// long `Symbol.toStringTag` at 1000 characters. Describing the value here in
// JS would have to walk the prototype chain with `getOwnPropertyDescriptor`
// and `getPrototypeOf`, which a Proxy turns into guest code inside an API
// whose contract is that it runs none.
// reportError hands a value to the global "error" event. It is not a
// constructor and takes one required Value.
globalThis.reportError = function reportError(...args) {
  if (args.length === 0)
    throw new TypeError(
      "Failed to execute 'reportError' on 'ServiceWorkerGlobalScope': " +
      "parameter 1 is not of type 'Value'.");
  const value = args[0];
  const described = __detail_string(value);
  globalThis.dispatchEvent(new ErrorEvent("error", {
    // Workerd carries the value itself, whatever its type; only the message
    // is a description of it.
    error: value,
    message: "Uncaught " + (typeof described === "string" ? described : ""),
  }));
};
// Object URLs need a blob registry workerd does not implement either: both
// statics exist and throw its own message.
Object.defineProperties(URL, {
  createObjectURL: {
    value: function createObjectURL() {
      throw new Error("URL.createObjectURL() is not implemented");
    },
    enumerable: true, writable: true, configurable: true,
  },
  revokeObjectURL: {
    value: function revokeObjectURL() {
      throw new Error("URL.revokeObjectURL() is not implemented");
    },
    enumerable: true, writable: true, configurable: true,
  },
});
// The global object is a ServiceWorkerGlobalScope in workerd; a library that
// sniffs the tag (or Object.prototype.toString.call(globalThis)) expects it.
Object.defineProperty(globalThis, Symbol.toStringTag, {
  value: "ServiceWorkerGlobalScope", configurable: true,
});
if (!globalThis.queueMicrotask)
  globalThis.queueMicrotask = (f) => Promise.resolve().then(f);
if (!globalThis.scheduler)
  globalThis.scheduler = {
    wait: (ms) => new Promise((resolve) => setTimeout(resolve, Number(ms) || 0)),
  };
if (!globalThis.structuredClone) {
  // Module-private registry: prototype -> { serialize, brand }. There is no
  // symbol on a prototype, so a guest can neither discover the serializer nor
  // make an object look like a host object. `brand` is an internal-slot or
  // constructor-brand check, so `Object.create(Blob.prototype)` is plain data
  // and only a constructed instance is serialized as its host type.
  const hostRegistry = new __primWeakMap();
  const defineHostType = (prototype, serialize, brand) => {
    if (!__weakHas(hostRegistry, prototype)) {
      __weakSet(hostRegistry, prototype, { serialize, brand });
    }
  };
  const hostEntryOf = (value) => {
    if (value === null || typeof value !== "object") return undefined;
    let prototype = __primGetProto(value);
    while (prototype !== null) {
      const entry = __weakGet(hostRegistry, prototype);
      if (entry !== undefined) return entry;
      prototype = __primGetProto(prototype);
    }
    return undefined;
  };
  const isHostObject = (value) => {
    const entry = hostEntryOf(value);
    if (entry === undefined) return false;
    try {
      return entry.brand(value) === true;
    } catch {
      return false;
    }
  };
  const writeHostObject = (value) => {
    const entry = hostEntryOf(value);
    if (entry === undefined || entry.brand(value) !== true) {
      throw new DOMException(
        'Could not serialize object of type "Object". This type does not ' +
          "support serialization.", "DataCloneError");
    }
    if (entry.serialize === null) {
      throw new DOMException("Unsupported object type", "DataCloneError");
    }
    // The serializers read the instance through `this`.
    return entry.serialize.call(value);
  };
  const deserializers = {
    Blob: (data) => new Blob([data.bytes], { type: data.mimeType }),
    File: (data) => new File([data.bytes], data.name, {
      type: data.mimeType,
      lastModified: data.lastModified,
    }),
    DOMException: (data) => {
      const exception = new DOMException(data.message, data.name);
      if (data.stack !== undefined)
        __primDefineProp(exception, "stack", {
          value: data.stack, configurable: true, writable: true,
        });
      return exception;
    },
  };
  const reviveHostObject = (data) => {
    const type = data !== null && typeof data === "object"
      ? data.type : undefined;
    const revive = typeof type === "string" && __primHasOwn(deserializers, type)
      ? deserializers[type] : undefined;
    if (typeof revive !== "function") {
      throw new DOMException(
        `Could not serialize object of type "${String(type)}". This type ` +
          "does not support serialization.", "DataCloneError");
    }
    return revive(data);
  };
  // A brand helper for a type whose constructor records membership in a
  // module-private WeakSet. A runtime without that set falls back to the
  // captured `instanceof`, which is still enough to refuse, never to serialize
  // guest state.
  const brandOf = (name, constructor) =>
    typeof __cello[name] === "function"
      ? __cello[name]
      : (value) => __instanceOf(value, constructor);

  defineHostType(Blob.prototype, function() {
    return {
      type: "Blob", bytes: __cello.$$blobBytes(this),
      mimeType: this.type,
    };
  }, brandOf("__isBlobInstance", Blob));
  defineHostType(File.prototype, function() {
    return {
      type: "File", bytes: __cello.$$blobBytes(this),
      mimeType: this.type, name: this.name,
      lastModified: this.lastModified,
    };
  }, brandOf("__isFileInstance", File));
  defineHostType(DOMException.prototype, function() {
    return {
      type: "DOMException", message: this.message,
      name: this.name, stack: this.stack,
    };
  }, brandOf("__isDOMExceptionInstance", DOMException));

  globalThis.structuredClone = function(value, options) {
    if (arguments.length < 1)
      throw new TypeError(
        "Failed to execute 'structuredClone': 1 argument required");
    const transfer = options?.transfer === undefined
      ? [] : Array.from(options.transfer);
    const seen = new Set();
    for (let index = 0; index < transfer.length; index++) {
      const item = transfer[index];
      if (!(item instanceof ArrayBuffer))
        throw new DOMException(
          `Value at index ${index} is not transferable`, "DataCloneError");
      if (item.detached)
        throw new DOMException(
          `ArrayBuffer at index ${index} is already detached`,
          "DataCloneError");
      if (seen.has(item))
        throw new DOMException(
          `ArrayBuffer at index ${index} is a duplicate`, "DataCloneError");
      seen.add(item);
    }

    // Crypto and MessagePort load after this harness, so register the late
    // types on every call (registration is idempotent) before V8 traverses
    // the graph.
    if (typeof CryptoKey === "function") {
      // Workerd refuses a CryptoKey outright: a clone of a key would be a
      // second handle to the same material. The brand is the slot map, so a
      // guest `Object.create(CryptoKey.prototype)` is not affected.
      defineHostType(CryptoKey.prototype, function() {
        throw new DOMException(
          'Could not serialize object of type "CryptoKey". This type does ' +
            "not support serialization.", "DataCloneError");
      }, (key) => __cello.$$isCryptoKey(key));
    }
    // Types the runtime refuses to clone. They carry a constructor brand, so
    // a forged prototype clones as plain data as workerd does; a genuine
    // instance keeps the runtime's "Unsupported object type" refusal.
    // A tuple's optional third entry is the refusal text: a stream and a
    // bucket have their own in workerd, everything else answers the generic
    // "Unsupported object type" (#206).
    const brandFn = (name) => (v) =>
      typeof __cello[name] === "function" && __cello[name](v) === true;
    const unsupported = [
      [Request, (v) => __cello.$$isRequestInstance(v)],
      [Response, (v) => __cello.$$isResponseInstance(v)],
      [Headers, (v) => __cello.$$isHeadersInstance(v)],
      [FormData, (v) => __cello.$$isFormDataInstance(v)],
      [URL, brandOf("__isUrlInstance", URL)],
      [URLSearchParams, brandOf("__isUrlSearchParamsInstance", URLSearchParams)],
      [typeof MessagePort === "function" ? MessagePort : undefined,
        (v) => typeof __cello.$$isMessagePortInstance === "function" &&
          __cello.$$isMessagePortInstance(v)],
      [AbortSignal, (v) => __cello.$$isAbortSignalInstance(v)],
      // The interfaces whose state moved into private slots without a brand
      // entry: a genuine instance must refuse, not flatten to `{}` (#206).
      [globalThis.ReadableStream, brandFn("$$isReadableStream"),
        "ReadableStream can only be serialized for RPC"],
      [globalThis.WritableStream, brandFn("$$isWritableStream"),
        "WritableStream can only be serialized for RPC"],
      [globalThis.TransformStream, brandFn("$$isTransformStream"),
        (value) => 'Could not serialize object of type "' +
          __cello.$$transformTypeName(value) + '". This type does not ' +
          "support serialization."],
      [globalThis.R2Bucket, brandFn("$$isR2Bucket"),
        "An R2 bucket binding cannot be serialized."],
    ];
    for (const [constructor, brand, message] of unsupported) {
      if (typeof constructor !== "function") continue;
      // A message may be a function of the value: the subclass refuses under
      // its own interface name, as workerd reports it.
      defineHostType(constructor.prototype,
        message === undefined ? null : function() {
          throw new DOMException(
            typeof message === "function" ? message(this) : message,
            "DataCloneError");
        }, brand);
    }

    let result;
    try {
      result = __structured_clone(
        value, isHostObject, writeHostObject, reviveHostObject);
    } catch (error) {
      if (error instanceof TypeError)
        throw new DOMException(error.message, "DataCloneError");
      throw error;
    }
    // Validate the complete list and clone the value before detaching any
    // source. A later invalid entry therefore cannot leave an earlier buffer
    // detached after the operation fails.
    for (const item of transfer) item.transfer();
    return result;
  };
}
// Web Crypto is installed after the rest of the harness so it can use
// DOMException, structuredClone, and Buffer.
// `Buffer` is read at call time, which materializes the lazy global.
const __zlibSync = (mode, data) =>
  Buffer.from(__zlib(mode, Buffer.from(data)));
__cello.__zlibModule = {
  constants: {
    Z_NO_FLUSH: 0,
    Z_PARTIAL_FLUSH: 1,
    Z_SYNC_FLUSH: 2,
    Z_FULL_FLUSH: 3,
    Z_FINISH: 4,
    Z_BLOCK: 5,
  },
  gzipSync: (data, _options) => __zlibSync("gzip", data),
  gunzipSync: (data, _options) => __zlibSync("gunzip", data),
  deflateSync: (data, _options) => __zlibSync("deflate", data),
  inflateSync: (data, _options) => __zlibSync("inflate", data),
  deflateRawSync: (data, _options) => __zlibSync("deflateRaw", data),
  inflateRawSync: (data, _options) => __zlibSync("inflateRaw", data),
};
// A Node dependency reads a `process` field at module scope and passes it
// straight to something that rejects `undefined` -- `dirname(process.execPath)`
// is one such case. Workerd defines every field
// below with a neutral value, so the same bundle evaluates there. A missing
// field therefore fails the whole Worker at load, which is why each one is
// defined even though nothing in a cell has an executable path, a parent
// process, or a real exit code. The values match workerd's
// `src/node/internal/public_process.ts` at workerd revision 04a08513a419
// (2026-09-04), including `version`, the 28-key `versions` record and the
// `/bundle` cwd (#210): a dependency that reads them -- undici's feature
// checks do -- is comparing them to what workerd reports. cello does not
// invent its own. `cwd()` is `/bundle` there, and cello's own `/bundle`
// projection is the same read-only view of the deployed modules, so a
// relative path still resolves onto the worker's files. A field whose workerd
// implementation throws at that revision -- `kill`, `binding`, `dlopen`,
// `cpuUsage` -- stays absent, because it fails on both runtimes and a stub
// here would only change the message.
if (!globalThis.process) globalThis.process = {
  env: {}, platform: "linux", arch: "x64", version: "v22.23.4",
  // The same keys, in workerd's (Node's) order. Every value but `node` is
  // empty on workerd, which is what a library sees, so cello reports the same
  // rather than a version this runtime does not have.
  versions: {
    node: "22.23.4", v8: "", modules: "", zlib: "", brotli: "", zstd: "",
    ares: "", napi: "", llhttp: "", nghttp2: "", acorn: "", ada: "",
    simdutf: "", nbytes: "", ncrypto: "", uvwasi: "", uv: "", openssl: "",
    sqlite: "", icu: "", undici: "", simdjson: "", unicode: "", cldr: "",
    tz: "", cjs_module_lexer: "", amaro: "", merve: "",
  },
  cwd: () => "/bundle",
  title: "workerd", argv: ["workerd"], argv0: "workerd", execArgv: [],
  execPath: "", pid: 1, ppid: 0, exitCode: undefined,
  allowedNodeEnvironmentFlags: new Set(),
  release: { name: "node", lts: true, sourceUrl: "", headersUrl: "" },
  channel: null, connected: false, debugPort: 0, domain: null,
  noDeprecation: false, traceDeprecation: false, throwDeprecation: false,
  sourceMapsEnabled: false, moduleLoadList: [],
  stdin: { fd: 0, isTTY: false },
  stdout: { fd: 1, isTTY: false, write: (s) => { __log(String(s)); return true; } },
  stderr: { fd: 2, isTTY: false, write: (s) => { __log(String(s)); return true; } },
  nextTick: (f, ...a) => queueMicrotask(() => f(...a)),
  on() {}, once() {}, off() {}, emit() {}, hrtime: () => [0, 0],
  // Workerd routes a warning to the process 'warning' event, and this
  // process emits nothing, so a no-op keeps the two runtimes equivalent.
  emitWarning() {},
  ref() {}, unref() {}, uptime: () => 0,
  constrainedMemory: () => 0, availableMemory: () => 0,
  memoryUsage: () => ({
    rss: 0, heapTotal: 0, heapUsed: 0, external: 0, arrayBuffers: 0,
  }),
};
globalThis.process.exit = (code = 0) => {
  const actorScope = __currentActorScope();
  if (actorScope) {
    const instance = __cell.instances[actorScope];
    if (instance?.__celloState)
      __stateFailBlock(instance.__celloState);
  }
  __process_exit(Number(code) || 0, actorScope);
};
globalThis.process.getBuiltinModule =
  (id) => __builtin_module(String(id));
// `node:process` (and bare `process`) is the global process object itself:
// the same object, not a copy, so `nodeProcess === globalThis.process` and
// a named `env` import stays live. Set before any user module links.
__cello.__processGlobal = globalThis.process;
if (!globalThis.global) globalThis.global = globalThis;
// Events belong to the request, and the host owns the request. `__event_*`
// and `__wait_until` operate on whichever context the host has made current
// for this turn, so nothing here has to know which request is running -- and
// two requests sharing an isolate cannot pop each other's events.
const __registerWaitUntil = __cello.__registerWaitUntil = (promise) => {
  const tracked = Promise.resolve(promise).catch((error) => {
    console.error("waitUntil rejected", error);
  });
  __wait_until(tracked);
};
// A response body is response work, not `waitUntil` work: it streams for as
// long as the client stays connected and the body keeps producing, which
// outlasts the post-response background budget by design. Its own hold keeps
// the event (and the ops its body needs) alive, so the budget never seals the
// pump the response is made of.
const __holdResponseStream = (promise) => {
  __response_stream_hold(Promise.resolve(promise).catch((error) => {
    console.error("response body pump failed", error);
  }));
};
// `props` are the per-stub props a loopback service stub carries
// (ctx.props); `exports` is built once, on first access.
const __defaultProps = {};
const __entrypointContext = (props = __defaultProps) => ({
  waitUntil: __registerWaitUntil,
  passThroughOnException() {},
  abort: __ctxAbortCurrent,
  props,
  get exports() { return __ctxExports(); },
});
const __beginEvent = __cello.__beginEvent = (props = __defaultProps) => {
  __rpcSignalRefresh();
  __refreshRetainedWaits("");
  __event_begin();
  return __entrypointContext(props);
};
const __endEvent = __cello.__endEvent = () => __event_end();
// Workerd's writable filesystem is memory-backed and request-scoped. The
// native IoContext owns the directory tree; this facade supplies Node's path,
// error, Promise, and Stats shapes without exposing a host filesystem.
const __fsPath = (value) => {
  if (value instanceof URL) {
    if (value.protocol !== "file:") {
      const error = new TypeError('The URL must be of scheme file');
      error.code = "ERR_INVALID_URL_SCHEME";
      throw error;
    }
    value = decodeURIComponent(value.pathname);
  } else if (globalThis.Buffer?.isBuffer(value)) {
    value = value.toString();
  } else if (typeof value !== "string") {
    const error = new TypeError(
      'The "path" argument must be of type string or an instance of Buffer or URL',
    );
    error.code = "ERR_INVALID_ARG_TYPE";
    throw error;
  }
  if (value.includes("\0")) {
    const error = new TypeError('The argument "path" must not contain null bytes');
    error.code = "ERR_INVALID_ARG_VALUE";
    throw error;
  }
  const absolute = value.startsWith("/") ? value : process.cwd() + "/" + value;
  const parts = [];
  for (const part of absolute.split("/")) {
    if (!part || part === ".") continue;
    if (part === "..") parts.pop();
    else parts.push(part);
  }
  return "/" + parts.join("/");
};
const __fsError = (code, syscall, path) => {
  const e = new Error(`${code}: ${syscall} '${path}'`);
  e.errno = { EPERM: -1, ENOENT: -2, ENOSPC: -28, EEXIST: -17, ENOTDIR: -20 }[code];
  e.code = code;
  e.syscall = syscall;
  e.path = path;
  return e;
};
const __enoent = (path = "") => { throw __fsError("ENOENT", "open", path); };
const __fsStatsBadge = Symbol("fs.Stats");
class __FsStats {
  constructor(badge, kind, size, bigint) {
    if (badge !== __fsStatsBadge) throw new TypeError("Illegal constructor");
    const file = kind === 3 || kind === 4;
    this.dev = 0;
    this.ino = 0;
    // A bundle file is 0o100444, a request `/tmp` file 0o100666, and a
    // writable temporary directory 0o40666, matching the mode values
    // workerd's fs-stat-test.js asserts.
    this.mode = (file ? 0o100000 : 0o40000) | 0o444 | (kind === 2 || kind === 4 ? 0o222 : 0);
    this.nlink = 1;
    this.uid = 0;
    this.gid = 0;
    this.rdev = 0;
    this.size = size;
    this.blksize = 0;
    this.blocks = 0;
    this.atimeMs = this.mtimeMs = this.ctimeMs = this.birthtimeMs = 0;
    this.atime = new Date(0);
    this.mtime = new Date(0);
    this.ctime = new Date(0);
    this.birthtime = new Date(0);
    Object.defineProperty(this, "__kind", { value: file ? "file" : "directory" });
    if (bigint) {
      for (const name of [
        "dev", "ino", "mode", "nlink", "uid", "gid", "rdev", "size",
        "blksize", "blocks", "atimeMs", "mtimeMs", "ctimeMs", "birthtimeMs",
      ]) this[name] = BigInt(this[name]);
      this.atimeNs = 0n;
      this.mtimeNs = 0n;
      this.ctimeNs = 0n;
      this.birthtimeNs = 0n;
    }
  }
  isFile() { return this.__kind === "file"; }
  isDirectory() { return this.__kind === "directory"; }
  isBlockDevice() { return false; }
  isCharacterDevice() { return false; }
  isSymbolicLink() { return false; }
  isFIFO() { return false; }
  isSocket() { return false; }
}
const __fsOptions = (options, names) => {
  if (options === undefined) return {};
  if (options === null || typeof options !== "object") {
    const error = new TypeError('The "options" argument must be of type object');
    error.code = "ERR_INVALID_ARG_TYPE";
    throw error;
  }
  for (const name of names) {
    if (options[name] !== undefined && typeof options[name] !== "boolean") {
      const error = new TypeError(`The "options.${name}" property must be of type boolean`);
      error.code = "ERR_INVALID_ARG_TYPE";
      throw error;
    }
  }
  return options;
};
const __fsMkdirSync = (value, options = {}) => {
  const path = __fsPath(value);
  if (typeof options === "number") options = { mode: options };
  options = __fsOptions(options, ["recursive"]);
  const result = __vfs_mkdir(path, options.recursive === true);
  if (result && !result.startsWith("/")) throw __fsError(result, "mkdir", path);
  return result || undefined;
};
const __fsStatSync = (value, options, syscall = "stat") => {
  const path = __fsPath(value);
  options = __fsOptions(options, ["bigint", "throwIfNoEntry"]);
  const [kind, size] = __vfs_stat(path);
  if (!kind) {
    if (options.throwIfNoEntry === false) return undefined;
    throw __fsError("ENOENT", syscall, path);
  }
  return new __FsStats(__fsStatsBadge, kind, size, options.bigint === true);
};
const __fsLstatSync = (value, options) => __fsStatSync(value, options, "lstat");
const __fsRealpathSync = (value) => {
  const path = __fsPath(value);
  if (!__vfs_stat(path)[0]) throw __fsError("ENOENT", "realpath", path);
  return path;
};
const __fsExistsSync = (value) => {
  try { return __vfs_stat(__fsPath(value))[0] !== 0; }
  catch { return false; }
};
// Node returns a Buffer unless an encoding is given, as a string or as
// `options.encoding`. Only `utf8` is decoded here; any other encoding falls
// through to Buffer's own decoder.
const __fsReadFileSync = (value, options) => {
  const path = __fsPath(value);
  const encoding = typeof options === "string" ? options : options?.encoding;
  const bytes = __vfs_read_file(path);
  if (bytes === undefined) {
    // A directory read fails differently from a missing path, so the stat
    // decides which error the caller sees.
    if (__vfs_stat(path)[0]) throw __fsError("EISDIR", "read", path);
    throw __fsError("ENOENT", "open", path);
  }
  const buffer = Buffer.from(bytes);
  return encoding ? buffer.toString(encoding) : buffer;
};
// `writeFile` data is a string or a binary view, as Node accepts. Anything
// else is an argument error, thrown synchronously by every form.
const __fsWriteData = (data, encoding) => {
  if (typeof data === "string") return Buffer.from(data, encoding);
  const bytes = __hostChunk(
    data, "The \"data\" argument must be a string or bytes");
  if (bytes !== null) return bytes;
  const error = new TypeError(
    "The \"data\" argument must be of type string or an instance of " +
    "Buffer, TypedArray, or DataView",
  );
  error.code = "ERR_INVALID_ARG_TYPE";
  throw error;
};
// Only `/tmp` is writable; the host answers `""` on success and a Node
// error code otherwise, so the syscall name is the only thing added here.
// A write that would push the request past `CELLO_TMP_MAX_BYTES` fails with
// `ENOSPC`. `writeFile` truncates (`"w"`) unless an append flag (`"a"`)
// appends like `appendFile`; an exclusive flag (`"x"`) fails with `EEXIST`
// when the path already exists, as Node's `O_EXCL` does. The tree has no
// read handles, so a read-mode flag (`"r"`) would write through a
// descriptor Node opened read-only: it is refused loudly instead of
// silently truncating.
const __fsParseFlag = (options, fallback) => {
  const flag = options?.flag ?? fallback;
  if (typeof flag !== "string") {
    const error = new TypeError(
      `The "flag" option must be of type string. Received ${flag}`);
    error.code = "ERR_INVALID_ARG_TYPE";
    throw error;
  }
  if (flag[0] !== "w" && flag[0] !== "a") {
    const error = new TypeError(
      `The flag "${flag}" is not supported for /tmp files`);
    error.code = "ERR_INVALID_ARG_VALUE";
    throw error;
  }
  return { append: flag[0] === "a", exclusive: flag.includes("x") };
};
// The exclusive half of the flag: fail before any mutation when the path
// already exists. Pure validation lives in `__fsParseFlag`, so callback
// forms can throw argument errors synchronously while this filesystem
// error reaches the callback.
const __fsCheckExclusive = (path, exclusive, syscall) => {
  if (exclusive && __vfs_stat(path)[0]) throw __fsError("EEXIST", syscall, path);
};
const __fsWriteFileSync = (value, data, options = {}) => {
  const path = __fsPath(value);
  const encoding = typeof options === "string" ? options : options?.encoding ?? "utf8";
  const bytes = __fsWriteData(data, encoding);
  const { append, exclusive } = __fsParseFlag(options, "w");
  __fsCheckExclusive(path, exclusive, "open");
  const result = __vfs_write_file(path, bytes, append);
  if (result && !result.startsWith("/")) throw __fsError(result, "open", path);
};
const __fsAppendFileSync = (value, data, options = {}) => {
  const path = __fsPath(value);
  const encoding = typeof options === "string" ? options : options?.encoding ?? "utf8";
  const bytes = __fsWriteData(data, encoding);
  const { append, exclusive } = __fsParseFlag(options, "a");
  __fsCheckExclusive(path, exclusive, "open");
  const result = __vfs_write_file(path, bytes, append);
  if (result && !result.startsWith("/")) throw __fsError(result, "open", path);
};
const __fsWriteFile = (value, data, options, callback) => {
  [options, callback] = __fsCallbackArgs(options, callback);
  __fsCallback(
    () => {
      __fsPath(value);
      __fsWriteData(data, typeof options === "string" ? options : options?.encoding);
      __fsParseFlag(options, "w");
    },
    () => __fsWriteFileSync(value, data, options),
    callback,
  );
};
const __fsAppendFile = (value, data, options, callback) => {
  [options, callback] = __fsCallbackArgs(options, callback);
  __fsCallback(
    () => {
      __fsPath(value);
      __fsWriteData(data, typeof options === "string" ? options : options?.encoding);
      __fsParseFlag(options, "a");
    },
    () => __fsAppendFileSync(value, data, options),
    callback,
  );
};
// `node:stream` classes for the file streams below, loaded on first use:
// the fs surface is eager but an isolate that never opens a file stream
// must not pay for the stream bundle.
const __fsStreamClasses = () => globalThis.process.getBuiltinModule("node:stream");
// A writable `/tmp` file. Chunks reach the host as they arrive: the first
// truncates unless the flags ask to append, every later chunk appends, so
// the per-request cap applies as data arrives instead of at `final`. `mode`
// is accepted and ignored, as the tree has no modes. `start` throws: the
// tree has no positioned writes, so silently writing at offset 0 would be
// silently wrong bytes rather than a loud failure.
const __fsCreateWriteStream = (value, options = {}) => {
  const path = __fsPath(value);
  if (options !== undefined && (options === null || typeof options !== "object")) {
    const error = new TypeError("The \"options\" argument must be of type object");
    error.code = "ERR_INVALID_ARG_TYPE";
    throw error;
  }
  if (options.start !== undefined) {
    const error = new TypeError("The \"start\" option is not supported for /tmp streams");
    error.code = "ERR_INVALID_ARG_VALUE";
    throw error;
  }
  const { append: appendFlag, exclusive } = __fsParseFlag(
    { flag: options.flags ?? options.flag ?? "w" }, "w");
  // A string chunk decodes with this encoding; the Writable validates it
  // here, so `end("4142", ...)` under `"hex"` is two bytes, not four.
  const encoding = options.encoding ?? "utf8";
  if (!Buffer.isEncoding(encoding)) {
    const error = new TypeError(`Unknown encoding: ${encoding}`);
    error.code = "ERR_UNKNOWN_ENCODING";
    throw error;
  }
  const { Writable } = __fsStreamClasses();
  let opened = false;
  let openFailed = false;
  // Chunks that arrive before `open`, as a synchronous `end("data")` does:
  // Node holds them until the async open completes, so an `open` listener
  // still reads the just-created (or just-truncated) file. Each entry keeps
  // its callback, so a failed open fails the buffered writes too instead of
  // confirming bytes that never landed.
  let pendingChunks = [];
  const failPending = (error) => {
    const queued = pendingChunks;
    pendingChunks = [];
    for (const { cb } of queued) cb(error);
  };
  const onWrite = (chunk, chunkEncoding, cb) => {
    try {
      // An exclusive stream never mutates a file it did not create.
      // Buffered chunks have landed nothing yet, so any file present
      // before `open` is someone else's.
      if (exclusive && !opened && __vfs_stat(path)[0]) {
        throw __fsError("EEXIST", "open", path);
      }
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, chunkEncoding);
      if (!opened) {
        pendingChunks.push({ bytes, cb });
        return;
      }
      // Past `open` the file is the stream's own: every chunk appends.
      const result = __vfs_write_file(path, bytes, true);
      if (result && !result.startsWith("/")) throw __fsError(result, "write", path);
      cb();
    } catch (error) { cb(error); }
  };
  const onFinal = (cb) => {
    // `final` can run before the queued open did (a synchronous `end`),
    // so it waits its turn behind it: the open microtask was queued at
    // construction and therefore runs first. The open microtask already
    // created (or truncated) the file, so there is nothing left to do
    // here: an untouched stream keeps the open-time file, and a written
    // one keeps its appended chunks.
    if (!opened && !openFailed) {
      queueMicrotask(() => onFinal(cb));
      return;
    }
    cb();
  };
  const stream = new Writable({
    defaultEncoding: encoding,
    write: onWrite,
    final: onFinal,
  });
  stream.path = path;
  stream.pending = true;
  queueMicrotask(() => {
    stream.pending = false;
    // The file is created (or truncated) before `open`, as Node's async
    // open does: an `open` listener that reads the path must see the new
    // file, not `ENOENT` or the pre-truncate bytes. Initialization errors
    // destroy the stream instead of emitting `open`, and fail the chunks
    // buffered above. `open` fires before the buffered chunks flush, so
    // the listener never observes them.
    if (exclusive && __vfs_stat(path)[0]) {
      openFailed = true;
      const error = __fsError("EEXIST", "open", path);
      failPending(error);
      stream.destroy(error);
      return;
    }
    const result = __vfs_write_file(path, new Uint8Array(0), appendFlag);
    if (result && !result.startsWith("/")) {
      openFailed = true;
      const error = __fsError(result, "open", path);
      failPending(error);
      stream.destroy(error);
      return;
    }
    opened = true;
    stream.emit("open");
    const queued = pendingChunks;
    pendingChunks = [];
    for (const { bytes, cb } of queued) {
      try {
        const flushed = __vfs_write_file(path, bytes, true);
        if (flushed && !flushed.startsWith("/")) throw __fsError(flushed, "write", path);
        cb();
      } catch (error) { cb(error); }
    }
  });
  return stream;
};
// A readable `/tmp` (or `/bundle`) file. The whole file is read up front
// and pushed in 64 KiB chunks; `start`/`end` ranges and `encoding` throw,
// because serving the whole file under a range request would be silently
// wrong bytes rather than a loud failure. A missing path destroys the
// stream with `ENOENT` (or `EISDIR` for a directory), asynchronously like Node.
const __fsCreateReadStream = (value, options = {}) => {
  const path = __fsPath(value);
  if (options !== undefined && (options === null || typeof options !== "object")) {
    const error = new TypeError("The \"options\" argument must be of type object");
    error.code = "ERR_INVALID_ARG_TYPE";
    throw error;
  }
  if (options.start !== undefined || options.end !== undefined ||
      typeof options.encoding === "string") {
    const error = new TypeError("The \"start\", \"end\" and \"encoding\" options are not supported for /tmp streams");
    error.code = "ERR_INVALID_ARG_VALUE";
    throw error;
  }
  const { Readable } = __fsStreamClasses();
  const stream = new Readable({ read() {} });
  stream.path = path;
  stream.pending = true;
  queueMicrotask(() => {
    stream.pending = false;
    const data = __vfs_read_file(path);
    if (data === undefined) {
      const [kind] = __vfs_stat(path);
      stream.destroy(kind ? __fsError("EISDIR", "read", path) : __fsError("ENOENT", "open", path));
      return;
    }
    stream.emit("open");
    const bytes = new Uint8Array(data);
    for (let offset = 0; offset < bytes.byteLength; offset += 65536) {
      stream.push(bytes.subarray(offset, offset + 65536));
    }
    stream.push(null);
  });
  return stream;
};
// `fs.constants`, with the four access modes from workerd's
// `src/node/internal/internal_fs_constants.ts`. The table carries only the
// modes `access` reads. Every other constant belongs to an operation the
// virtual filesystem does not implement, and that operation already fails
// loudly at the unimplemented-builtin stub, so publishing its constants would
// only make a caller build a mode for a call it cannot make.
const __fsConstants = Object.freeze({ F_OK: 0, X_OK: 1, W_OK: 2, R_OK: 4 });
// Node validates a mode before it reaches the filesystem, so a bad mode is an
// argument error and never a filesystem error. The range is the union of the
// four access modes, which is what workerd's `validateMode` allows.
const __fsMode = (mode) => {
  if (mode === undefined) return __fsConstants.F_OK;
  if (typeof mode !== "number") {
    const error = new TypeError('The "mode" argument must be of type number');
    error.code = "ERR_INVALID_ARG_TYPE";
    throw error;
  }
  if (!Number.isInteger(mode) || mode < 0 || mode > 7) {
    const error = new RangeError(
      `The value of "mode" is out of range. It must be >= 0 && <= 7. Received ${mode}`,
    );
    error.code = "ERR_OUT_OF_RANGE";
    throw error;
  }
  return mode;
};
// `access` reports what the virtual filesystem can do and not a POSIX
// permission, so it follows workerd's `accessSyncImpl`: no file is executable,
// therefore `X_OK` always fails, and an unwritable path fails exactly like a
// missing one, so a caller cannot tell the two apart. Kinds 2 and 4 are the
// writable tree (directory and file); see `op_vfs_stat`.
const __fsAccessSync = (value, mode) => {
  const path = __fsPath(value);
  mode = __fsMode(mode);
  const [kind] = __vfs_stat(path);
  const denied = mode & __fsConstants.X_OK
    || !kind
    || (mode & __fsConstants.W_OK && kind !== 2 && kind !== 4);
  if (denied) throw __fsError("ENOENT", "access", path);
};
// The callback forms run the same synchronous core as `fs.promises`.
// Validation and the operation are separate arguments, not one function,
// because they fail differently: an argument error throws, and only a
// filesystem error reaches the callback. One combined body would route both to
// the callback and hide a caller's bug behind an error it did not expect.
//
// `validate()` runs before the callback check, which is workerd's order: each
// entry point in `internal_fs_callback.ts` validates its path and its options,
// and only then calls `callWithSingleArgCallback`. Node checks the callback
// first, so a call that has a bad path and no callback reports the path here
// and reports the callback in Node. Both are `ERR_INVALID_ARG_TYPE`, and the
// runtime follows workerd.
const __fsDeliver = (validate, run, callback, deliver) => {
  validate();
  if (typeof callback !== "function") {
    const error = new TypeError('The "cb" argument must be of type function');
    error.code = "ERR_INVALID_ARG_TYPE";
    throw error;
  }
  let result;
  try {
    result = run();
  } catch (error) {
    // A callback delivered in this turn would let a caller observe the result
    // before its own call returned, which Node never does.
    queueMicrotask(() => callback(error));
    return;
  }
  queueMicrotask(() => deliver(result));
};
// An operation with a result calls back with `(error, result)`, and one
// without calls back with `(error)` alone. Workerd splits the two the same way
// (`callWithSingleArgCallback` and `callWithErrorOnlyCallback`), because a
// caller can read `arguments.length` and see the difference.
const __fsCallback = (validate, run, callback) =>
  __fsDeliver(validate, run, callback, (result) => callback(null, result));
const __fsErrorOnlyCallback = (validate, run, callback) =>
  __fsDeliver(validate, run, callback, () => callback(null));
// Node accepts `(path, callback)` or `(path, options, callback)` everywhere.
const __fsCallbackArgs = (options, callback) =>
  typeof options === "function" ? [undefined, options] : [options, callback];
const __fsAccess = (value, mode, callback) => {
  [mode, callback] = __fsCallbackArgs(mode, callback);
  __fsErrorOnlyCallback(
    () => { __fsPath(value); __fsMode(mode); },
    () => __fsAccessSync(value, mode),
    callback,
  );
};
const __fsStat = (value, options, callback) => {
  [options, callback] = __fsCallbackArgs(options, callback);
  __fsCallback(
    () => { __fsPath(value); __fsOptions(options, ["bigint", "throwIfNoEntry"]); },
    () => __fsStatSync(value, options),
    callback,
  );
};
const __fsLstat = (value, options, callback) => {
  [options, callback] = __fsCallbackArgs(options, callback);
  __fsCallback(
    () => { __fsPath(value); __fsOptions(options, ["bigint", "throwIfNoEntry"]); },
    () => __fsLstatSync(value, options),
    callback,
  );
};
const __fsRealpath = (value, options, callback) => {
  [options, callback] = __fsCallbackArgs(options, callback);
  __fsCallback(
    () => __fsPath(value),
    () => __fsRealpathSync(value),
    callback,
  );
};
const __fsReadFile = (value, options, callback) => {
  [options, callback] = __fsCallbackArgs(options, callback);
  __fsCallback(
    () => __fsPath(value),
    () => __fsReadFileSync(value, options),
    callback,
  );
};
const __fsMkdir = (value, options, callback) => {
  [options, callback] = __fsCallbackArgs(options, callback);
  __fsCallback(
    () => {
      __fsPath(value);
      // A number is the mode, which `mkdirSync` validates but does not apply.
      if (typeof options !== "number") __fsOptions(options, ["recursive"]);
    },
    () => __fsMkdirSync(value, options),
    callback,
  );
};
const __fsPromisesSurface = {
  // The promise form validates inside the promise, so an argument error
  // rejects instead of throwing. Workerd makes the same split, because a
  // caller of a promise API has no synchronous frame to catch.
  async access(...args) { return __fsAccessSync(...args); },
  async mkdir(...args) { return __fsMkdirSync(...args); },
  async stat(...args) { return __fsStatSync(...args); },
  async lstat(...args) { return __fsLstatSync(...args); },
  async realpath(...args) { return __fsRealpathSync(...args); },
  async readFile(...args) { return __fsReadFileSync(...args); },
  async writeFile(...args) { return __fsWriteFileSync(...args); },
  async appendFile(...args) { return __fsAppendFileSync(...args); },
};
// A namespace import gets a fixed export set when V8 links the module, so a
// partial module must enumerate unsupported Node exports too. A get trap alone
// keeps default and named imports loud but silently omits these names from a
// namespace. Keep the unsupported names lazy: materializing every stub in
// every isolate would charge Workers that use only the implemented subset.
const __fsNamespaceProxy = (surface, path, names, enoentNames = []) => {
  const namespaceNames = new Set([...Reflect.ownKeys(surface), ...names]);
  const enoentNameSet = new Set(enoentNames);
  return new Proxy(surface, {
    get: (target, p) => {
      if (Reflect.has(target, p)) return Reflect.get(target, p);
      if (enoentNameSet.has(p)) return __enoent;
      if (typeof p !== "string") return undefined;
      return __nodeStubFor(path + "." + p);
    },
    ownKeys: () => [...namespaceNames],
    // A lazy name has no descriptor on the target, so the synthetic one below
    // completes to `writable: false`. Without this trap an assignment would
    // reach OrdinarySet, read that descriptor off the proxy as the receiver,
    // and throw "Cannot redefine property". CommonJS dependencies patch a
    // builtin at module scope, so the throw takes the whole Worker down at
    // load. Write through to the surface instead, which also promotes the name
    // to a real own property and keeps the descriptor honest from then on.
    // Materializing a complete descriptor in the trap below would fix the
    // assignment too, but it would charge every Worker for all the stubs the
    // moment anything enumerates the namespace.
    set: (target, p, value) => Reflect.set(target, p, value),
    getOwnPropertyDescriptor: (target, p) =>
      Reflect.getOwnPropertyDescriptor(target, p) ??
        (namespaceNames.has(p)
          ? { configurable: true, enumerable: true }
          : undefined),
  });
};
const __fsPromises = __cello.__fsPromises = __fsNamespaceProxy(
  __fsPromisesSurface,
  "node:fs/promises",
  [
    "chmod", "chown", "copyFile", "cp", "lchmod", "lchown",
    "link", "lutimes", "mkdtemp", "open", "opendir", "readdir", "readlink",
    "rename", "rm", "rmdir", "statfs", "symlink", "truncate", "unlink",
    "utimes", "watch",
  ],
);
const __fsSurface = {
  Stats: __FsStats,
  constants: __fsConstants,
  promises: __fsPromises,
  accessSync: __fsAccessSync,
  access: __fsAccess,
  existsSync: __fsExistsSync,
  mkdirSync: __fsMkdirSync,
  statSync: __fsStatSync,
  lstatSync: __fsLstatSync,
  realpathSync: __fsRealpathSync,
  readFileSync: __fsReadFileSync,
  writeFileSync: __fsWriteFileSync,
  appendFileSync: __fsAppendFileSync,
  stat: __fsStat,
  lstat: __fsLstat,
  realpath: __fsRealpath,
  readFile: __fsReadFile,
  mkdir: __fsMkdir,
  writeFile: __fsWriteFile,
  appendFile: __fsAppendFile,
  createWriteStream: __fsCreateWriteStream,
  createReadStream: __fsCreateReadStream,
};
const __fsNamespaceNames = [
  "Dir", "Dirent", "ReadStream", "WriteStream", "_toUnixTimestamp",
  "chmod", "chmodSync", "chown", "chownSync",
  "close", "closeSync", "copyFile", "copyFileSync", "cp", "cpSync",
  "exists", "fchmod", "fchmodSync",
  "fchown", "fchownSync", "fdatasync", "fdatasyncSync", "fstat", "fstatSync",
  "fsync", "fsyncSync", "ftruncate", "ftruncateSync", "futimes", "futimesSync",
  "lchmod", "lchmodSync", "lchown", "lchownSync", "link", "linkSync",
  "lutimes", "lutimesSync", "mkdtemp", "mkdtempSync", "open", "openSync",
  "opendir", "opendirSync", "read", "readSync", "readdir", "readdirSync",
  "readlink", "readlinkSync", "readv", "readvSync", "rename", "renameSync",
  "rm", "rmSync", "rmdir", "rmdirSync", "statfs", "statfsSync", "symlink",
  "symlinkSync", "truncate", "truncateSync", "unlink", "unlinkSync",
  "unwatchFile", "utimes", "utimesSync", "watch", "watchFile", "write",
  "writeSync", "writev", "writevSync",
];
// Writable file contents and every mutation remain explicit unsupported
// surfaces until a failing application seam requires them. These two reads
// use ENOENT so dependencies can take their no-filesystem fallback path.
__cello.__fs = __fsNamespaceProxy(
  __fsSurface,
  "node:fs",
  __fsNamespaceNames,
  ["readdirSync", "readlinkSync"],
);
const __bridgeResponseStream = (body, requestControllers, onSettle) => {
  let streamId;
  try {
    streamId = __response_stream_create();
  } catch (error) {
    onSettle?.();
    throw error;
  }
  const pump = (async () => {
    try {
      const reader = body.getReader();
      const consumerClosed = __response_stream_closed(streamId)
        .then((status) => status === "cancelled"
          ? { consumerClosed: true }
          : new Promise(() => {}));
      const cancelProducer = async () => {
        const reason = new Error("The client has disconnected");
        for (const controller of requestControllers || []) {
          if (!controller.signal.aborted) controller.abort(reason);
        }
        // The consumer is already gone, so the host stream closes first: a
        // body whose cancel() never settles must not keep this pump (and the
        // entry behind it) alive, and the close is the host work the driver
        // can still drive. The producer's cancel still runs; nothing awaits
        // it past this point.
        await __response_stream_close(streamId, "");
        void reader.cancel(reason).catch(() => {});
      };
      try {
        for (;;) {
          const result = await Promise.race([
            reader.read().then((read) => ({ read })),
            consumerClosed,
          ]);
          if (result.consumerClosed) {
            await cancelProducer();
            return;
          }
          if (result.read.done) {
            await __response_stream_close(streamId, "");
            return;
          }
          const bytes = __bodyPumpChunk(result.read.value);
          for (let offset = 0; offset < bytes.byteLength; offset += 64 * 1024) {
            await __response_stream_write(
              streamId,
              bytes.subarray(offset, offset + 64 * 1024),
            );
          }
        }
      } catch (error) {
        if (String(error).includes("response stream consumer canceled")) {
          await cancelProducer();
          return;
        }
        await __response_stream_close(streamId, String(error));
      }
    } finally {
      // A live body holds its fetch's stubs until it settles: the pump
      // pulls through their handles, so ending the context earlier
      // would disconnect the body mid-stream.
      onSettle?.();
    }
  })();
  // Response work, not background work: the pump outlives the response head
  // and the post-response budget, and ends when the body ends, the client
  // disconnects, or the runtime stops.
  __holdResponseStream(pump);
  return streamId;
};
__cello.__readResponse = (r) => {
  // The fetch context that produced this response, if any: end it once
  // the body no longer needs it. Bytes bodies end here; a live body ends
  // when its bridge pump settles (below); a host-stream body needs no JS
  // handles and ends here too.
  const fetchCtx = (r !== null &&
      (typeof r === "object" || typeof r === "function"))
    ? __fetchCtxs.get(r)
    : undefined;
  if (fetchCtx !== undefined) __fetchCtxs.delete(r);
  const endFetchCtx = () => {
    if (fetchCtx !== undefined) __ctxEnd(fetchCtx);
  };
  if (!(r instanceof Response)) {
    endFetchCtx();
    return {
      status: 200,
      bodyBytes: new TextEncoder().encode(String(r)),
      bodyStreamId: 0,
      headersJson: "[]",
      wsTargetJson: "null",
      workerSocketId: 0,
    };
  }
  // Keep the network-error marker out of the HTTP response fields. If status
  // zero reaches Rust as an ordinary response, the outer server can report
  // only a generic invalid status instead of the Worker contract failure.
  if (r.type === "error" || r.status === 0) {
    endFetchCtx();
    return { error: __ERROR_RESPONSE_MESSAGE };
  }
  let workerSocketId = 0;
  let wsTarget = __responseState(r).wsTarget ||
    (r.webSocket && __wsState(r.webSocket).target) || null;
  if (r.status === 101 && r.webSocket && wsTarget === null) {
    const server = __wsState(r.webSocket).peer;
    if (server && __wsState(server).accepted) {
      const serverState = __wsState(server);
      workerSocketId = serverState.id;
      // Install the host queue before the pump asks for its first frame. The
      // target marker makes sends use that queue instead of remaining in the
      // pair's pre-upgrade buffer.
      __ws_prepare_worker_handoff(workerSocketId);
      serverState.target = { id: workerSocketId, scope: "" };
      serverState.polled = true;
      __sockets.set(workerSocketId, server);
      __wsHooks.startPump(server);
      __wsHooks.flushPending(server);
    }
  }
  let bodyStreamId;
  let pumpEndsCtx = false;
  if (__responseState(r).bytes === null) {
    // The controllers live in the response's private state now (#206):
    // `__attachResponseRequestCancellation` records them there, and reading
    // the removed own field here left the pump with none, so a client
    // disconnect never aborted the producer behind a streamed response.
    const responseState = __responseState(r);
    const hostStreamId = __cello.$$httpStreamId(r.body);
    if (typeof hostStreamId === "number" &&
        responseState.requestControllers === undefined) {
      bodyStreamId = hostStreamId;
    } else {
      bodyStreamId = __bridgeResponseStream(
        r.body,
        responseState.requestControllers,
        endFetchCtx,
      );
      pumpEndsCtx = true;
    }
  } else {
    bodyStreamId = 0;
  }
  if (bodyStreamId) {
    if (!pumpEndsCtx) endFetchCtx();
    return {
      status: r.status,
      bodyBytes: new Uint8Array(),
      bodyStreamId,
      headersJson: JSON.stringify(Array.from(r.headers)),
      wsTargetJson: JSON.stringify(wsTarget),
      workerSocketId,
    };
  }
  const bytes = __responseState(r).bytes;
  endFetchCtx();
  return {
    status: r.status,
    bodyBytes: bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes),
    bodyStreamId: 0,
    headersJson: JSON.stringify(Array.from(r.headers)),
    wsTargetJson: JSON.stringify(wsTarget),
    workerSocketId,
  };
};
