// Workflows in `ctx.exports` (#266): a module's `WorkflowEntrypoint` classes
// are the Workflows it declares, and workerd exposes each one as the workflow
// *binding*, keyed by class name. Inside a Durable Object as in a plain Worker.
import { WorkflowEntrypoint, DurableObject } from "cloudflare:workers";

export class Slow extends WorkflowEntrypoint {
  async run(_event, step) {
    return await step.do("one", async () => "ok");
  }
}

export class Probe extends DurableObject {
  async fetch() {
    return report(await surface(this.ctx.exports, this.env));
  }
}

const report = (value) => new Response(JSON.stringify(canonical(value)), {
  headers: { "content-type": "text/plain; charset=utf-8" },
});
const canonical = (value) => {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.keys(value).sort().map((k) => [k, canonical(value[k])]));
  }
  return value === undefined ? "<undefined>" : value;
};
const errorInfo = (error) => ({
  name: error?.name ?? String(error),
  message: error?.message ?? String(error),
});
const attempt = async (fn) => {
  try {
    return { ok: await fn() };
  } catch (error) {
    return { error: errorInfo(error) };
  }
};
// Own properties keep their order -- a Worker can read it -- while a
// prototype's members are sorted, because a native prototype's definition
// order is not a contract.
const descriptors = (names, value) => names.map((name) => {
  const descriptor = Object.getOwnPropertyDescriptor(value, name);
  return [name, descriptor.get ? "getter" : typeof descriptor.value,
    descriptor.enumerable, descriptor.writable ?? null, descriptor.configurable];
});
const surfaceOwn = (value) => descriptors(Object.getOwnPropertyNames(value), value);
const surfaceKeys = (value) => descriptors(Object.keys(value), value);
const surfaceChain = (value) => {
  const out = [];
  for (let proto = Object.getPrototypeOf(value);
       proto && proto !== Object.prototype;
       proto = Object.getPrototypeOf(proto)) {
    const ctor = Object.getOwnPropertyDescriptor(proto, "constructor");
    const members = Object.getOwnPropertyNames(proto).map((name) => {
      const descriptor = Object.getOwnPropertyDescriptor(proto, name);
      return [name, descriptor.get ? "getter" : typeof descriptor.value,
        descriptor.enumerable];
    });
    members.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
    out.push([ctor?.value?.name ?? null, members]);
  }
  return out;
};

// A workflow binding is opaque: a `[object WrappedBinding]` whose chain is
// `WorkflowImpl` over `WrappedBinding`, with no own property to read the
// workflow name from, exactly as `env.SLOW` reflects.
const bindingSurface = (value) => ({
  type: typeof value,
  tag: Object.prototype.toString.call(value),
  ctor: value.constructor?.name ?? null,
  own: surfaceOwn(value),
  keys: surfaceKeys(value),
  chain: surfaceChain(value),
  methods: ["create", "createBatch", "deleteBatch", "get"]
    .map((name) => [name, typeof value?.[name]]),
});

// The entry must be the same instance store `env.SLOW` reaches, through its
// own object: a create through one path is visible to the other.
const sharedStore = async (exports, env, id) => {
  const out = {};
  out.createViaExports = await attempt(async () => {
    const instance = await exports.Slow.create({ id, params: { round: 1 } });
    return [typeof instance, instance?.id];
  });
  out.getViaEnv = await attempt(async () => {
    const instance = await env.SLOW.get(id);
    return [typeof instance, instance?.id];
  });
  out.getViaExports = await attempt(async () => {
    const instance = await exports.Slow.get(id);
    return [typeof instance, instance?.id];
  });
  out.deleteBatch = await attempt(
    () => exports.Slow.deleteBatch([id, `${id}-absent`]));
  out.goneViaExports = await attempt(() => exports.Slow.get(id)).then(
    (result) => (result.error ? { failed: true } : result));
  out.goneViaEnv = await attempt(() => env.SLOW.get(id)).then(
    (result) => (result.error ? { failed: true } : result));
  return out;
};

const surface = async (exports, env) => {
  const names = Object.keys(exports).sort();
  const out = {
    names,
    ownNames: Object.getOwnPropertyNames(exports).sort(),
    // A reserved cell's class name -- the script-scoped `__Workflow.<script>`
    // alias included -- is the runtime's business, never an export handle.
    reserved: Object.getOwnPropertyNames(exports)
      .filter((name) => name.startsWith("__") || name.startsWith(".")).sort(),
    entry: bindingSurface(exports.Slow),
    envEntry: bindingSurface(env.SLOW),
    // The same shape and the same store, but its own object, as workerd has it.
    sharedPrototype: Object.getPrototypeOf(exports.Slow) ===
      Object.getPrototypeOf(env.SLOW),
    sameObject: exports.Slow === env.SLOW,
    cached: exports.Slow === exports.Slow,
  };
  return { ...out, ...(await sharedStore(exports, env, "gate-1")) };
};

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === "/exports") return report(await surface(ctx.exports, env));
    if (url.pathname === "/do") return env.OBJ.getByName("x").fetch(request);
    return new Response("not found", { status: 404 });
  },
};
