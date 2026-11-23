// Code generation is an isolate capability, not a JS builtin. workerd's
// isolate has no string code generation and no wasm compiler, so `eval`,
// `Function`, and compiling wasm from bytes all throw; a `.wasm` module the
// deployment compiled ahead of time still instantiates. Every route reports
// names and messages, which are V8's own text on both runtimes.
import answer from "./answer.wasm";

const report = (value) => new Response(JSON.stringify(value), {
  headers: { "content-type": "application/json" },
});
const errorInfo = (error) => ({ name: error?.name ?? String(error), message: error?.message ?? String(error) });
const attempt = (fn) => {
  try {
    return { ok: true, value: fn() };
  } catch (error) {
    return { ok: false, error: errorInfo(error) };
  }
};
const attemptAsync = async (fn) => {
  try {
    return { ok: true, value: await fn() };
  } catch (error) {
    return { ok: false, error: errorInfo(error) };
  }
};

// (module (func (export "run") (result i32) i32.const 42))
const BYTES = new Uint8Array([
  0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x05, 0x01, 0x60,
  0x00, 0x01, 0x7f, 0x03, 0x02, 0x01, 0x00, 0x07, 0x07, 0x01, 0x03, 0x72,
  0x75, 0x6e, 0x00, 0x00, 0x0a, 0x06, 0x01, 0x04, 0x00, 0x41, 0x2a, 0x0b,
]);

const routes = {
  "/eval"() {
    const out = {
      eval: attempt(() => eval("1 + 1")),
      newFunction: attempt(() => new Function("return 7")()),
      functionCtor: attempt(() => Function("return 8")()),
      // The same throw from a non-global scope, where a feature probe
      // usually lands.
      evalScope: attempt(() => {
        const local = 5;
        return eval("typeof local");
      }),
      // Feature detection must only cost the probe: a library that wraps
      // eval in try/catch keeps running, it just detects no support.
      featureDetect: (() => {
        let supported = true;
        try {
          eval("0");
        } catch {
          supported = false;
        }
        return supported;
      })(),
    };
    return report(out);
  },

  async "/wasm-runtime"() {
    const out = {
      module: attempt(() => new WebAssembly.Module(BYTES)),
      compile: await attemptAsync(() => WebAssembly.compile(BYTES)),
      instantiate: await attemptAsync(() => WebAssembly.instantiate(BYTES)),
      // Validation reads bytes without compiling them, so it still agrees.
      validate: attempt(() => WebAssembly.validate(BYTES)),
      // Memory allocation is not code generation.
      memory: attempt(() => new WebAssembly.Memory({ initial: 1 }).buffer.byteLength),
    };
    return report(out);
  },

  async "/wasm-import"() {
    const instance = new WebAssembly.Instance(answer, {});
    const out = {
      isModule: answer instanceof WebAssembly.Module,
      run: instance.exports.run(),
      // Instantiate an already-compiled Module: no compilation, so allowed.
      // With a Module the promise resolves to an Instance, not a pair.
      instantiateModule: await attemptAsync(async () => {
        const created = await WebAssembly.instantiate(answer, {});
        return created.exports.run();
      }),
    };
    return report(out);
  },
};

export default {
  fetch(request) {
    const route = new URL(request.url).pathname;
    const handler = routes[route];
    if (handler === undefined) return new Response("not found", { status: 404 });
    return handler();
  },
};
