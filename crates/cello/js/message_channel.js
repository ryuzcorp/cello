// MessageChannel/MessagePort for one isolate, following Workerd
// (src/workerd/api/messagechannel.c++): ports entangle in pairs, a port
// buffers messages until it has a message listener, close() closes both
// ports and fires "close" on each, and transfer lists are refused.
//
// Port state lives in a module-private WeakMap, so a Worker reflects the
// MessagePort surface only: `Object.keys(port)` is empty and the prototype
// names no host helper (#206).
(function () {
  const kCreate = Symbol("cello.MessagePort.create");
  const _states = new WeakMap();
  const _state = (port) => {
    const state = _states.get(port);
    if (state === undefined) throw new TypeError("Illegal invocation");
    return state;
  };

  const _startPort = (port) => {
    const state = _state(port);
    if (state.started || state.closed) return;
    state.started = true;
    const pending = state.buffer;
    state.buffer = [];
    for (const data of pending) _deliver(port, data);
  };

  const _deliver = (port, data) => {
    const state = _state(port);
    if (state.closed) return;
    if (!state.started) {
      state.buffer.push(data);
      return;
    }
    // Deferred like Workerd's delivery task: a handler attached in the
    // same turn as postMessage still sees the message.
    queueMicrotask(() => {
      if (state.closed) return;
      const event = new MessageEvent("message", { data });
      __cello.$$trustEvent(event);
      port.dispatchEvent(event);
    });
  };

  class MessagePort extends EventTarget {
    constructor(token = undefined) {
      super();
      if (token !== kCreate) {
        throw new TypeError("Illegal constructor");
      }
      _states.set(this, {
        peer: null, started: false, closed: false, buffer: [], handler: null,
      });
    }

    get onmessage() { return _state(this).handler; }
    set onmessage(handler) {
      _state(this).handler = typeof handler === "function" ? handler : null;
      // Assigning a message handler starts the port, per the spec.
      _startPort(this);
    }

    postMessage(value, options = undefined) {
      const transfer = Array.isArray(options) ? options : options?.transfer;
      if (transfer != null && transfer.length > 0) {
        throw new TypeError("Transfer list is not supported");
      }
      // RPC objects are host objects the structured clone algorithm
      // refuses. Workerd serializes before it looks at the peer, so a
      // closed port still surfaces the DataCloneError; matching that
      // keeps postMessage's behavior independent of peer liveness.
      const cf = __cello.__cf;
      if (cf && (value instanceof cf.RpcTarget ||
        value instanceof cf.ServiceStub)) {
        throw new DOMException(
          "Could not serialize an RPC object for postMessage().",
          "DataCloneError");
      }
      const cloned = structuredClone(value);
      const state = _state(this);
      const peer = state.peer;
      if (state.closed || !peer || _state(peer).closed) return;
      _deliver(peer, cloned);
    }

    start() { _startPort(this); }

    close() {
      const state = _state(this);
      if (state.closed) return;
      state.closed = true;
      state.buffer.length = 0;
      const peer = state.peer;
      if (peer && !_state(peer).closed) peer.close();
      const event = new Event("close");
      __cello.$$trustEvent(event);
      this.dispatchEvent(event);
    }
  }

  class MessageChannel {
    constructor() {
      const port1 = new MessagePort(kCreate);
      const port2 = new MessagePort(kCreate);
      _state(port1).peer = port2;
      _state(port2).peer = port1;
      // Workerd's own-property order is port2 then port1, which JSON and
      // Object.keys both observe.
      this.port2 = port2;
      this.port1 = port1;
    }
  }

  // WebIDL reflection: prototype members enumerable, tag a data property.
  const webidl = (ctor, tag) => {
    for (const key of Object.getOwnPropertyNames(ctor.prototype)) {
      if (key === "constructor") continue;
      const descriptor = Object.getOwnPropertyDescriptor(ctor.prototype, key);
      if (descriptor === undefined || descriptor.enumerable) continue;
      descriptor.enumerable = true;
      Object.defineProperty(ctor.prototype, key, descriptor);
    }
    if (tag !== undefined) {
      Object.defineProperty(ctor.prototype, Symbol.toStringTag, {
        value: tag, configurable: true, writable: true, enumerable: false,
      });
    }
  };
  webidl(MessagePort, "MessagePort");
  webidl(MessageChannel, "MessageChannel");
  Object.defineProperty(MessagePort, "length", {
    value: 0, configurable: true, writable: false,
  });

  // Captured: the host storage predicate calls this while a guest may have
  // patched `WeakMap.prototype.has`.
  const _statesHas = WeakMap.prototype.has;
  __cello.$$isMessagePortInstance = (value) =>
    Reflect.apply(_statesHas, _states, [value]);

  // EventTarget tells this hook when a listener arrives, so `start()` runs on
  // addEventListener("message") without MessagePort overriding the method
  // (workerd's prototype does not declare one).
  __cello.$$listenerAdded = (target, type) => {
    if (type === "message" && target instanceof MessagePort) _startPort(target);
  };

  globalThis.MessagePort = MessagePort;
  globalThis.MessageChannel = MessageChannel;
})();
