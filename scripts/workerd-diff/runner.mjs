// Differential conformance runner: every case is one fixture Worker plus a
// scripted request sequence. The fixture runs on workerd (Cloudflare's
// open-source runtime) and on cello, and the normalized transcripts must be
// equal. Fixtures put anything order- or timing-sensitive into the body they
// return, so a transcript never depends on wall time.
//
// Usage: CELLO_BIN=/path/to/cello node runner.mjs [case-name ...]
// Exit 0 only when every case agrees; exit 2 for a bad invocation or install.

import { execFileSync, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { cp, mkdtemp, readFile, rm } from "node:fs/promises";
import { createConnection } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { cases } from "./cases.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const corpus = join(here, "corpus");
// The native binary from the platform package the lockfile pins, never the
// npm shim or install.js's unchecked download: `npm ci --ignore-scripts`
// installs only what the lockfile names.
const platform = { linux: "linux", darwin: "darwin", win32: "windows" }[process.platform];
const arch = process.arch === "x64" ? "64" : process.arch;
const nativeWorkerd = join(here, `node_modules/@cloudflare/workerd-${platform}-${arch}/bin/workerd`);
// The spawns run inside the fixture copy, so binary paths must be absolute.
const workerdBin = resolve(process.env.WORKERD_BIN ?? nativeWorkerd);
const celloBin = process.env.CELLO_BIN ? resolve(process.env.CELLO_BIN) : undefined;
const bootDeadlineMs = 120_000;
const requestDeadlineMs = 60_000;

// Headers that differ per run or per host and carry no Worker behaviour.
const volatileHeaders = new Set(["date", "server", "cf-ray", "nel", "report-to"]);

const children = new Set();
const dirs = new Set();
let cleaning;

/** Resolves once the child has actually exited. */
const exited = (child) => new Promise((done) => (child.exitCode === null ? child.once("exit", done) : done()));

/** Kills every child and removes every fixture copy, once. */
function cleanUp() {
  cleaning ??= (async () => {
    await Promise.all([...children].map(async (child) => {
      child.kill("SIGKILL");
      await exited(child);
    }));
    children.clear();
    await Promise.all([...dirs].map((dir) => rm(dir, { recursive: true, force: true })));
    dirs.clear();
  })();
  return cleaning;
}

// A signal must leave no runtime and no fixture copy behind, so unwind the
// same way a normal exit does.
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => cleanUp().then(() => process.exit(130)));
}

if (!celloBin) {
  console.error("CELLO_BIN must name the locally built cello binary");
  process.exit(2);
}

/** The installed workerd must be the version package.json pins. */
async function verifyWorkerd() {
  try {
    const pinned = JSON.parse(await readFile(join(here, "package.json"), "utf8")).dependencies.workerd;
    const installed = JSON.parse(await readFile(join(here, "node_modules/workerd/package.json"), "utf8")).version;
    // The binary names its own build date: 1.YYYYMMDD.N reports YYYY-MM-DD.
    const reported = execFileSync(workerdBin, ["--version"], { encoding: "utf8" }).trim();
    const date = /^1\.(\d{4})(\d{2})(\d{2})\./.exec(pinned);
    const expected = date && `${date[1]}-${date[2]}-${date[3]}`;
    if (pinned !== installed || !expected || !reported.endsWith(expected)) {
      throw new Error(`package.json pins ${pinned}, installed ${installed}, binary reports "${reported}"`);
    }
  } catch (error) {
    console.error(`workerd install is not the pinned one: ${error.message}`);
    console.error("run: npm --prefix scripts/workerd-diff ci --ignore-scripts");
    process.exit(2);
  }
}

/** The fixture's own files plus a `wrangler.json` both runtimes understand. */
async function fixtureDir(name) {
  const dir = await mkdtemp(join(tmpdir(), "cello-workerd-diff-"));
  dirs.add(dir);
  await cp(join(corpus, name), dir, { recursive: true });
  return dir;
}

/** Stops one runtime and waits for it, so its fixture copy can be removed. */
const release = async (child) => {
  children.delete(child);
  child.kill("SIGKILL");
  await exited(child);
};

/** Runs a boot inside its own cleanup boundary: a failed one leaves nothing. */
async function owned(child, boot) {
  children.add(child);
  try {
    return await boot();
  } catch (error) {
    await release(child);
    throw error;
  }
}

const hostPort = (addr) => {
  const at = addr.lastIndexOf(":");
  return { host: addr.slice(0, at).replace(/^\[|\]$/g, ""), port: Number(addr.slice(at + 1)) };
};

/** Resolves once `addr` accepts TCP, so the first request never races boot. */
function waitListening(addr) {
  const deadline = Date.now() + bootDeadlineMs;
  return new Promise((resolve, reject) => {
    const attempt = () => {
      const socket = createConnection(hostPort(addr));
      socket.once("connect", () => {
        socket.destroy();
        resolve();
      });
      socket.once("error", () => {
        socket.destroy();
        if (Date.now() > deadline) reject(new Error(`no listener on ${addr}`));
        else setTimeout(attempt, 25);
      });
    };
    attempt();
  });
}

/** Boots workerd for the fixture; resolves with its address and a stop hook. */
async function bootWorkerd(dir) {
  // A fixture that needs an instance-only service (the Cache API's backend,
  // which a deployment config cannot define) ships a `workerd.capnp`. Binding
  // fixtures -- `kv_namespaces`, `r2_buckets`, `d1_databases`,
  // `durable_objects`, `queues` -- need `--experimental` for `wrangler.json`
  // too, because workerd refuses those keys without it. Passing the flag for
  // every fixture is harmless for the ones that declare nothing.
  const instanceConfig = join(dir, "workerd.capnp");
  const fromInstanceConfig = existsSync(instanceConfig);
  const child = spawn(
    workerdBin,
    [
      "serve",
      fromInstanceConfig ? "workerd.capnp" : "wrangler.json",
      "-s", "http=127.0.0.1:0",
      "--control-fd", "3",
      "--experimental",
    ],
    { cwd: dir, stdio: ["ignore", "ignore", "pipe", "pipe"] },
  );
  let stderr = "";
  child.stderr.setEncoding("utf8").on("data", (chunk) => (stderr += chunk));
  return owned(child, async () => {
    // The control fd names the ephemeral port once the socket is bound.
    const port = await new Promise((resolve, reject) => {
      let buffered = "";
      const deadline = setTimeout(() => reject(new Error(`workerd boot timed out:\n${stderr}`)), bootDeadlineMs);
      child.stdio[3].setEncoding("utf8").on("data", (chunk) => {
        const lines = (buffered + chunk).split("\n");
        buffered = lines.pop();
        for (const line of lines) {
          const event = JSON.parse(line);
          if (event.event === "listen" && event.socket === "http") {
            clearTimeout(deadline);
            resolve(event.port);
          }
        }
      });
      child.once("error", reject);
      child.once("exit", (code) => reject(new Error(`workerd exited ${code}:\n${stderr}`)));
    });
    const addr = `127.0.0.1:${port}`;
    await waitListening(addr);
    return { addr, stop: () => release(child) };
  });
}

/** Boots `cello dev` for the fixture; the announce line names its address. */
async function bootCello(dir) {
  const env = { HOME: join(dir, ".test-home"), PATH: process.env.PATH ?? "" };
  if (process.env.TMPDIR) env.TMPDIR = process.env.TMPDIR;
  if (process.env.CELLO_ESBUILD) env.CELLO_ESBUILD = process.env.CELLO_ESBUILD;
  const child = spawn(
    celloBin,
    ["dev", dir, "--port", "0", "--no-watch", "--clean", "--logs"],
    { cwd: dir, env, stdio: ["ignore", "ignore", "pipe"] },
  );
  let stderr = "";
  return owned(child, async () => {
    const addr = await new Promise((resolve, reject) => {
      const deadline = setTimeout(() => reject(new Error(`cello boot timed out:\n${stderr}`)), bootDeadlineMs);
      child.stderr.setEncoding("utf8").on("data", (chunk) => {
        stderr += chunk;
        const at = stderr.indexOf("cello listening on ");
        if (at === -1) return;
        const found = stderr.slice(at + "cello listening on ".length).split(" ")[0];
        if (!found) return;
        clearTimeout(deadline);
        resolve(found.replace(/^0\.0\.0\.0/, "127.0.0.1"));
      });
      child.once("error", reject);
      child.once("exit", (code) => reject(new Error(`cello exited ${code}:\n${stderr}`)));
    });
    await waitListening(addr);
    return { addr, stop: () => release(child) };
  });
}

/**
 * One HTTP/1.1 exchange over a raw socket: the client half of a differential
 * harness has to control chunked uploads and read bodies byte-exactly, which
 * `fetch` does not offer. `body` is an array of chunks, staged one write per
 * chunk; an empty array sends no body at all.
 */
function exchange(addr, spec) {
  const { method = "GET", path = "/", headers = {}, body = [], noFraming = false } = spec;
  const chunks = typeof body === "string" ? [body] : body;
  return new Promise((resolve, reject) => {
    const socket = createConnection(hostPort(addr));
    const timer = setTimeout(() => {
      socket.destroy();
      reject(new Error(`${method} ${path}: no response within ${requestDeadlineMs} ms`));
    }, requestDeadlineMs);
    const received = [];
    socket.on("data", (chunk) => received.push(chunk));
    socket.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    socket.once("end", () => {
      clearTimeout(timer);
      try {
        resolve(parseResponse(Buffer.concat(received)));
      } catch (error) {
        reject(error);
      }
    });
    socket.once("connect", () => {
      const lines = [
        `${method} ${path} HTTP/1.1`,
        `Host: ${addr}`,
        "Connection: close",
        ...Object.entries(headers).map(([name, value]) => `${name}: ${value}`),
      ];
      if (chunks.length === 0) {
        // A request with no body at all carries no framing header. Sending
        // `Content-Length: 0` instead frames an empty body, which workerd
        // reports as a present body stream rather than `null` (#142).
        if (!noFraming) lines.push("Content-Length: 0");
        socket.write(lines.join("\r\n") + "\r\n\r\n");
        return;
      }
      lines.push("Transfer-Encoding: chunked");
      socket.write(lines.join("\r\n") + "\r\n\r\n");
      for (const chunk of chunks) {
        const bytes = Buffer.from(chunk);
        socket.write(Buffer.concat([Buffer.from(bytes.length.toString(16) + "\r\n"), bytes, Buffer.from("\r\n")]));
      }
      socket.write("0\r\n\r\n");
    });
  });
}

function parseResponse(raw) {
  const split = raw.indexOf("\r\n\r\n");
  if (split === -1) throw new Error(`response has no header block: ${raw.toString().slice(0, 200)}`);
  const head = raw.subarray(0, split).toString("latin1").split("\r\n");
  const status = Number(head[0].split(" ")[1]);
  const headers = {};
  for (const line of head.slice(1)) {
    const at = line.indexOf(":");
    headers[line.slice(0, at).trim().toLowerCase()] = line.slice(at + 1).trim();
  }
  let body = raw.subarray(split + 4);
  if (headers["transfer-encoding"] === "chunked") body = dechunk(body);
  else if (headers["content-length"] !== undefined) body = body.subarray(0, Number(headers["content-length"]));
  return { status, headers, body };
}

function dechunk(raw) {
  const parts = [];
  let at = 0;
  for (;;) {
    const lineEnd = raw.indexOf("\r\n", at);
    if (lineEnd === -1) throw new Error("truncated chunked body");
    const size = parseInt(raw.subarray(at, lineEnd).toString("latin1").split(";")[0], 16);
    if (Number.isNaN(size)) throw new Error("bad chunk size");
    if (size === 0) return Buffer.concat(parts);
    parts.push(raw.subarray(lineEnd + 2, lineEnd + 2 + size));
    at = lineEnd + 2 + size + 2;
  }
}

/** One masked client frame (RFC 6455). */
function websocketFrame(opcode, payload) {
  const bytes = Buffer.from(payload);
  const mask = Buffer.from([0x11, 0x22, 0x33, 0x44]);
  let header;
  if (bytes.length < 126) header = Buffer.from([0x80 | opcode, 0x80 | bytes.length]);
  else {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 0x80 | 126;
    header.writeUInt16BE(bytes.length, 2);
  }
  return Buffer.concat([header, mask, Buffer.from(bytes.map((b, i) => b ^ mask[i % 4]))]);
}

/** Frames in `buffer`, unmasked; a truncated tail is left unread. */
function websocketFrames(buffer) {
  const frames = [];
  let at = 0;
  while (at + 2 <= buffer.length) {
    const opcode = buffer[at] & 0x0f;
    let length = buffer[at + 1] & 0x7f;
    let offset = at + 2;
    if (length === 126) {
      if (offset + 2 > buffer.length) break;
      length = buffer.readUInt16BE(offset);
      offset += 2;
    } else if (length === 127) {
      if (offset + 8 > buffer.length) break;
      length = Number(buffer.readBigUInt64BE(offset));
      offset += 8;
    }
    if (offset + length > buffer.length) break;
    frames.push({ opcode, payload: buffer.subarray(offset, offset + length) });
    at = offset + length;
  }
  return frames;
}

/** A frame as a normalized transcript entry. */
function websocketFrameRecord(frame) {
  if (frame.opcode === 8) {
    return {
      opcode: 8,
      code: frame.payload.length >= 2 ? frame.payload.readUInt16BE(0) : null,
      reason: frame.payload.subarray(2).toString("utf8"),
    };
  }
  const text = frame.payload.toString("utf8");
  if (Buffer.from(text, "utf8").equals(frame.payload)) return { opcode: frame.opcode, text };
  return {
    opcode: frame.opcode,
    bytes: frame.payload.length,
    sha256: createHash("sha256").update(frame.payload).digest("hex"),
  };
}

/**
 * One raw WebSocket conversation: handshake, a text frame, two binary
 * frames, then a close. The transcript is the response head plus every
 * server frame up to and including its close, so a case never depends on
 * wall time -- it resolves on the frames, not on a delay.
 */
function websocketExchange(addr, path) {
  return new Promise((resolve, reject) => {
    const socket = createConnection(hostPort(addr));
    let received = Buffer.alloc(0);
    let handshake = null;
    let settled = false;
    const finish = (error, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.destroy();
      if (error) reject(error);
      else resolve(value);
    };
    const timer = setTimeout(
      () => finish(new Error(`websocket ${path}: no completion within ${requestDeadlineMs} ms`)),
      requestDeadlineMs,
    );
    socket.on("error", (error) => finish(error));
    socket.on("data", (chunk) => {
      received = Buffer.concat([received, chunk]);
      try {
        if (handshake === null) {
          const split = received.indexOf("\r\n\r\n");
          if (split === -1) return;
          const head = received.subarray(0, split).toString("latin1").split("\r\n");
          const headers = {};
          for (const line of head.slice(1)) {
            const at = line.indexOf(":");
            headers[line.slice(0, at).trim().toLowerCase()] = line.slice(at + 1).trim();
          }
          for (const name of volatileHeaders) delete headers[name];
          handshake = { status: Number(head[0].split(" ")[1]), headers };
          received = received.subarray(split + 4);
          socket.write(websocketFrame(1, "hi"));
          socket.write(websocketFrame(2, Buffer.from([1, 2, 250])));
          socket.write(websocketFrame(2, Buffer.from([9, 8])));
          socket.write(websocketFrame(8, Buffer.from([0x03, 0xe8])));
        }
        const frames = websocketFrames(received);
        if (frames.some((frame) => frame.opcode === 8)) {
          finish(null, { ...handshake, frames: frames.map(websocketFrameRecord) });
        }
      } catch (error) {
        finish(error);
      }
    });
    socket.once("connect", () => {
      socket.write([
        `GET ${path} HTTP/1.1`,
        `Host: ${addr}`,
        "Upgrade: websocket",
        "Connection: Upgrade",
        "Sec-WebSocket-Key: MDEyMzQ1Njc4OWFiY2RlZg==",
        "Sec-WebSocket-Version: 13",
        "",
        "",
      ].join("\r\n"));
    });
  });
}

/** The transcript of one request: status, stable headers, body where equal. */
function record(response) {
  const headers = {};
  for (const [name, value] of Object.entries(response.headers)) {
    if (!volatileHeaders.has(name)) headers[name] = value;
  }
  const text = response.body.toString("utf8");
  const isText = Buffer.from(text, "utf8").equals(response.body) && !text.includes("\u0000");
  return {
    status: response.status,
    headers,
    body: isText ? text : undefined,
    bytes: response.body.length,
    sha256: isText ? undefined : createHash("sha256").update(response.body).digest("hex"),
  };
}

/** Boots the fixture once per runtime and runs every one of its cases. */
async function runOn(boot, fixture, items) {
  const dir = await fixtureDir(fixture);
  let node;
  try {
    node = await boot(dir);
    const transcripts = {};
    for (const item of items) {
      const transcript = [];
      try {
        for (const spec of item.requests) transcript.push(record(await exchange(node.addr, spec)));
      } catch (error) {
        transcript.push({ error: String(error.message ?? error) });
      }
      transcripts[item.name] = transcript;
    }
    return transcripts;
  } finally {
    await node?.stop();
    await rm(dir, { recursive: true, force: true });
    dirs.delete(dir);
  }
}

function differences(a, b, path = "", out = []) {
  if (JSON.stringify(a) === JSON.stringify(b)) return out;
  const bothObjects = a && b && typeof a === "object" && typeof b === "object";
  if (!bothObjects) {
    out.push(`${path || "<root>"}: workerd=${JSON.stringify(a)} cello=${JSON.stringify(b)}`);
    return out;
  }
  for (const key of new Set([...Object.keys(a), ...Object.keys(b)])) {
    differences(a[key], b[key], path ? `${path}.${key}` : key, out);
  }
  return out;
}

// A case that names no request would compare two empty transcripts and pass,
// so refuse the invocation rather than run less than it says.
const selected = process.argv.slice(2);
const unknown = selected.filter((name) => !cases.some((item) => item.name === name));
if (unknown.length > 0) {
  console.error(`unknown case(s): ${unknown.join(", ")}`);
  process.exit(2);
}
const run = selected.length > 0 ? cases.filter((item) => selected.includes(item.name)) : cases;
if (run.length === 0) {
  console.error("the corpus has no cases to run");
  process.exit(2);
}
for (const item of run) {
  item.requests = item.requests ?? (item.path ? [{ path: item.path }] : []);
  if (item.requests.length === 0) {
    console.error(`${item.name}: no request to send`);
    process.exit(2);
  }
}

await verifyWorkerd();
const byFixture = new Map();
for (const item of run) byFixture.set(item.fixture, [...(byFixture.get(item.fixture) ?? []), item]);

const failures = [];
let executed = 0;
for (const [fixture, items] of byFixture) {
  let fromWorkerd;
  let fromCello;
  try {
    fromWorkerd = await runOn(bootWorkerd, fixture, items);
    fromCello = await runOn(bootCello, fixture, items);
  } catch (error) {
    console.log(`${fixture}: ERROR`);
    console.log(`  ${String(error.message ?? error).replaceAll("\n", "\n  ")}`);
    failures.push(...items.map((item) => item.name));
    continue;
  }
  for (const item of items) {
    process.stdout.write(`${item.name} ... `);
    executed += 1;
    // A route answering anything but 200 never exercised its behaviour.
    const unexpected = [...fromWorkerd[item.name], ...fromCello[item.name]].find((entry) => entry.status !== 200);
    if (unexpected) {
      console.log("ERROR");
      console.log(`  a request answered ${unexpected.status}, not 200`);
      failures.push(item.name);
      continue;
    }
    if (process.env.SHOW_TRANSCRIPTS) console.log(`\n  workerd: ${JSON.stringify(fromWorkerd[item.name])}\n  cello:   ${JSON.stringify(fromCello[item.name])}`);
    const diff = differences(fromWorkerd[item.name], fromCello[item.name]);
    if (diff.length === 0) {
      console.log("ok");
    } else {
      console.log("DIVERGES");
      for (const line of diff) console.log(`  ${line}`);
      failures.push(item.name);
    }
  }
}
await cleanUp();
if (executed !== run.length) {
  console.log(`\nran ${executed} of ${run.length} case(s)`);
  process.exit(1);
}
if (failures.length > 0) {
  console.log(`\n${failures.length} divergence(s): ${failures.join(", ")}`);
  process.exit(1);
}
console.log(`\nall ${executed} cases agree`);
