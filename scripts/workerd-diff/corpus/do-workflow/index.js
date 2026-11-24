// #259: a deployment that declares a workflow. The runtime aliases its own
// workflow class under a script-scoped reserved name (`__Workflow.<script>`),
// which a Worker's `ctx.exports` must never expose: it is the runtime's cell,
// not the module's export.
import { WorkflowEntrypoint } from "cloudflare:workers";

export class Slow extends WorkflowEntrypoint {
  async run(event, step) {
    return await step.do("one", async () => 1);
  }
}

export class Probe {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }
  async fetch() {
    const exports = this.state.exports;
    const names = Object.keys(exports);
    return Response.json({
      // Only what both runtimes can name: the module's own workflow class is
      // a handle in workerd and is not compared here.
      leaks: names.filter((name) => name.startsWith("__") || name.includes(".")),
      alias: Object.hasOwn(exports, "__Workflow.exports-workflow"),
      cron: Object.hasOwn(exports, ".cron"),
      aliasType: typeof exports["__Workflow.exports-workflow"],
      cronType: typeof exports[".cron"],
    });
  }
}

export default {
  async fetch(request, env) {
    return env.OBJ.getByName("main").fetch(request);
  },
};
