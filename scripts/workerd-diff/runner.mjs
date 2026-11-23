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
import { cp, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { gzipSync } from "node:zlib";
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
    if (helperServer) await new Promise((done) => helperServer.close(done));
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

/** A second origin the Worker can fetch: the fixture itself cannot serve as one. */
function startHelper() {
  const server = createServer((req, res) => {
    const url = new URL(req.url, "http://helper.test");
    if (url.pathname === "/target") {
      res.writeHead(200, { "content-type": "text/plain", "x-helper": "yes" });
      res.end("target-body");
    } else if (url.pathname === "/redirect") {
      res.writeHead(Number(url.searchParams.get("code") ?? "302"), { location: url.searchParams.get("to") ?? "/target" });
      res.end();
    } else if (url.pathname === "/not-found") {
      res.writeHead(404, { "content-type": "text/plain" });
      res.end("missing-body");
    } else if (url.pathname === "/gzip") {
      const body = gzipSync(Buffer.from("hello gzip"));
      res.writeHead(200, { "content-encoding": "gzip", "content-type": "text/plain", "content-length": String(body.length) });
      res.end(body);
    } else if (url.pathname === "/no-content") {
      // 204/205/304 carry no body and no Content-Length on the wire; a
      // fetched Response for one has `body === null` in workerd (#165).
      res.writeHead(204, { "x-helper": "yes" });
      res.end();
    } else if (url.pathname === "/reset-content") {
      res.writeHead(205);
      res.end();
    } else if (url.pathname === "/not-modified") {
      res.writeHead(304, { etag: '"v1"' });
      res.end();
    } else if (url.pathname === "/echo") {
      const headers = {};
      for (let i = 0; i < req.rawHeaders.length; i += 2) headers[req.rawHeaders[i].toLowerCase()] = req.rawHeaders[i + 1];
      // The body is measured, not just ignored: a replayed Content-Length
      // would frame it wrongly and a strict client would stall (#180).
      let bodyBytes = 0;
      req.on("data", (chunk) => { bodyBytes += chunk.length; });
      req.on("end", () => {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ method: req.method, url: req.url, headers, bodyBytes }));
      });
    } else {
      res.writeHead(418);
      res.end();
    }
  });
  return new Promise((resolve) => server.listen(0, "127.0.0.1", () => resolve(server)));
}

let helperServer;
let helperOrigin;

/** The fixture's own files plus a `wrangler.json` both runtimes understand. */
async function fixtureDir(name) {
  const dir = await mkdtemp(join(tmpdir(), "cello-workerd-diff-"));
  dirs.add(dir);
  await cp(join(corpus, name), dir, { recursive: true });
  // The helper origin's port is only known at boot, so the var is written here.
  const configPath = join(dir, "wrangler.json");
  if (existsSync(configPath)) {
    const config = JSON.parse(await readFile(configPath, "utf8"));
    config.vars = { ...(config.vars ?? {}), HELPER: helperOrigin };
    await writeFile(configPath, JSON.stringify(config, null, 2));
  }
  const capnpPath = join(dir, "workerd.capnp");
  if (existsSync(capnpPath)) {
    await writeFile(capnpPath, (await readFile(capnpPath, "utf8")).replaceAll("__HELPER__", helperOrigin));
  }
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
function websocketExchange(addr, spec) {
  const path = typeof spec === "string" ? spec : spec.path;
  // Workerd's hibernation path answers no close frame of its own: a
  // hibernatable socket's close is the application's to send, so the probe
  // ends after the message replies there.
  const sendClose = typeof spec === "string" ? true : spec.close !== false;
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
    // One frame at a time, each sent only after every earlier reply arrived:
    // close is last, so the transcript holds the reply to each probe whatever
    // order a runtime's output pump settles messages in.
    let stage = 0;
    const advance = (frames) => {
      if (stage === 0) {
        stage = 1;
        socket.write(websocketFrame(1, "hi"));
      }
      if (stage === 1 && frames.length >= 1) {
        stage = 2;
        socket.write(websocketFrame(2, Buffer.from([1, 2, 250])));
      }
      if (stage === 2 && frames.length >= 2) {
        stage = 3;
        socket.write(websocketFrame(2, Buffer.from([9, 8])));
      }
      if (stage === 3 && frames.length >= 3) {
        stage = 4;
        if (sendClose) socket.write(websocketFrame(8, Buffer.from([0x03, 0xe8])));
        else finish(null, { ...handshake, frames: frames.map(websocketFrameRecord) });
      }
    };
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
          // These two values are case-insensitive tokens: fastwebsockets (and
          // thus cello) writes them lowercase, workerd capitalizes them.
          for (const name of ["connection", "upgrade"]) {
            if (headers[name] !== undefined) headers[name] = headers[name].toLowerCase();
          }
          handshake = { status: Number(head[0].split(" ")[1]), headers };
          received = received.subarray(split + 4);
        }
        const frames = websocketFrames(received);
        advance(frames);
        if (stage === 4 && frames.some((frame) => frame.opcode === 8)) {
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
        if (item.websocket !== undefined) {
          transcript.push(await websocketExchange(node.addr, item.websocket));
        } else {
          for (const spec of item.requests) transcript.push(record(await exchange(node.addr, spec)));
        }
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
  if (item.websocket !== undefined) {
    if (item.requests !== undefined || item.path !== undefined) {
      console.error(`${item.name}: a websocket case names a path or requests as well`);
      process.exit(2);
    }
    continue;
  }
  item.requests = item.requests ?? (item.path ? [{ path: item.path }] : []);
  if (item.requests.length === 0) {
    console.error(`${item.name}: no request to send`);
    process.exit(2);
  }
}

await verifyWorkerd();
helperServer = await startHelper();
helperOrigin = `http://127.0.0.1:${helperServer.address().port}`;
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
    // A route answering anything but 200 (101 for a WebSocket upgrade) never
    // exercised its behaviour.
    if (process.env.SHOW_TRANSCRIPTS) console.log(`\n  workerd: ${JSON.stringify(fromWorkerd[item.name])}\n  cello:   ${JSON.stringify(fromCello[item.name])}`);
    const wantStatus = item.websocket !== undefined ? 101 : 200;
    const unexpected = [...fromWorkerd[item.name], ...fromCello[item.name]].find((entry) => entry.status !== wantStatus);
    if (unexpected) {
      console.log("ERROR");
      console.log(`  a request answered ${unexpected.status}, not ${wantStatus}`);
      failures.push(item.name);
      continue;
    }
    if (process.env.DUMP_DIR) {
      const { mkdir, writeFile: write } = await import("node:fs/promises");
      const dir = join(process.env.DUMP_DIR, item.name);
      await mkdir(dir, { recursive: true });
      await write(join(dir, "workerd.json"), JSON.stringify(fromWorkerd[item.name], null, 2));
      await write(join(dir, "cello.json"), JSON.stringify(fromCello[item.name], null, 2));
    }
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
