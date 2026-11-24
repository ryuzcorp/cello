// R2 binding fixture: record shapes, ranges, conditions, listings and the
// key/metadata limits, with every answer's volatile parts (dates, durations)
// reduced to a fixed shape before it is reported.
const canonical = (value) => {
  if (value instanceof Date) return "<date>";
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value).sort().map((key) => [key, canonical(value[key])]),
    );
  }
  return value;
};
const report = (value) =>
  new Response(JSON.stringify(canonical(value)), {
    headers: { "content-type": "text/plain; charset=utf-8" },
  });
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
const hex = (buffer) =>
  [...new Uint8Array(buffer)].map((b) => b.toString(16).padStart(2, "0")).join("");
// The record shape both runtimes must agree on, minus the raw bytes of a
// body and with `version` reduced to its type (the ids are unrelated).
const objectShape = (object) => object === null ? null : {
  key: object.key,
  size: object.size,
  etag: object.etag,
  httpEtag: object.httpEtag,
  versionType: typeof object.version,
  storageClass: object.storageClass,
  uploaded: object.uploaded instanceof Date,
  md5: object.checksums?.md5 ? hex(object.checksums.md5) : null,
  httpMetadata: object.httpMetadata ?? null,
  customMetadata: object.customMetadata ?? null,
};

let B;
const routes = {
  // The content etag: identical bytes answer one etag and two versions.
  async "/etag"() {
    const first = await B.put("etag/first", "abc");
    const second = await B.put("etag/second", "abc");
    const third = await B.put("etag/first", "abcd");
    const head = await B.head("etag/second");
    const missing = await B.head("etag/absent");
    const got = await B.get("etag/second");
    return report({
      first: objectShape(first),
      second: objectShape(second),
      sameEtag: first.etag === second.etag,
      httpEtagQuoted: first.httpEtag === `"${first.etag}"`,
      rewritten: objectShape(third),
      rewrittenChanged: third.etag !== first.etag,
      head: objectShape(head),
      missing,
      got: objectShape(got),
      gotText: got === null ? null : await got.text(),
      after: await attempt(async () => {
        await B.delete(["etag/second", "etag/absent"]);
        return "ok";
      }),
      deleted: await B.head("etag/second"),
    });
  },

  // Ranges: offset+length, offset only, suffix, a Range header, and the
  // past-EOF refusal with R2's own code.
  async "/ranges"() {
    await B.put("range/key", "0123456789");
    const slice = async (range) => {
      const got = await B.get("range/key", { range });
      return got === null
        ? null
        : { size: got.size, range: got.range, text: await got.text() };
    };
    return report({
      bounded: await attempt(() => slice({ offset: 2, length: 3 })),
      offset: await attempt(() => slice({ offset: 7 })),
      suffix: await attempt(() => slice({ suffix: 4 })),
      header: await attempt(() => slice(new Headers({ Range: "bytes=0-1" }))),
      pastEof: await attempt(() => slice({ offset: 50, length: 5 })),
      zeroLength: await attempt(() => slice({ offset: 5, length: 0 })),
    });
  },

  // onlyIf: an unquoted etag matches, a wrong one answers null, `*` works,
  // and the quoted `httpEtag` is refused.
  async "/conditional"() {
    const put = await B.put("cond/key", "v1");
    const out = {
      etag: put.etag,
      httpEtag: put.httpEtag,
    };
    out.unquotedMatch = await attempt(async () => {
      const stored = await B.put("cond/key", "v2", {
        onlyIf: { etagMatches: put.etag },
      });
      return stored === null ? null : objectShape(stored).etag;
    });
    out.wrong = await attempt(async () =>
      (await B.put("cond/key", "v3", { onlyIf: { etagMatches: "deadbeef" } }))
        === null ? null : "stored");
    out.starOnExisting = await attempt(async () =>
      (await B.put("cond/key", "v4", { onlyIf: { etagDoesNotMatch: "*" } }))
        === null ? null : "stored");
    out.quotedMatch = await attempt(() =>
      B.put("cond/key", "v5", { onlyIf: { etagMatches: put.httpEtag } }));
    out.quotedDoesNot = await attempt(() =>
      B.put("cond/key", "v5", { onlyIf: { etagDoesNotMatch: put.httpEtag } }));
    out.getQuoted = await attempt(() =>
      B.get("cond/key", { onlyIf: { etagMatches: put.httpEtag } }));
    out.getUnquotedMiss = await attempt(async () => {
      const got = await B.get("cond/key", { onlyIf: { etagMatches: put.etag } });
      return got === null ? null : "unmet";
    });
    return report(out);
  },

  // Listing: prefix, delimiter, cursor paging, include, the limit bounds,
  // and the answer an undecodable cursor gets (#164).
  async "/list"() {
    for (const key of ["l/a", "l/b", "l/dir/c", "l/dir/d", "m/e"]) {
      await B.put(key, "v");
    }
    await B.put("l/meta", "v", {
      httpMetadata: { contentType: "text/plain" },
      customMetadata: { owner: "ada" },
    });
    const all = await B.list({ prefix: "l/" });
    const delimited = await B.list({ prefix: "l/", delimiter: "/" });
    const page = await B.list({ prefix: "l/", limit: 2 });
    const next = await B.list({ prefix: "l/", limit: 2, cursor: page.cursor });
    const withMeta = await B.list({
      prefix: "l/meta",
      include: ["httpMetadata", "customMetadata"],
    });
    const bad = await B.list({ prefix: "l/", cursor: "garbage" });
    const pageShape = async (options) => {
      const listed = await B.list(options);
      return {
        names: listed.objects.map((object) => object.key),
        truncated: listed.truncated,
      };
    };
    return report({
      allNames: all.objects.map((object) => object.key),
      allTruncated: all.truncated,
      allCursorAbsent: all.cursor === undefined,
      allKeys: Object.keys(all).sort(),
      delimitedNames: delimited.objects.map((object) => object.key),
      delimitedPrefixes: delimited.delimitedPrefixes,
      pageNames: page.objects.map((object) => object.key),
      pageTruncated: page.truncated,
      pageCursorSet: page.cursor !== undefined,
      // A cursor is opaque in both runtimes: it is never the key the page
      // stopped at, so a client cannot mint one from a key.
      pageCursorIsLastKey: page.cursor === page.objects[page.objects.length - 1].key,
      nextNames: next.objects.map((object) => object.key),
      nextTruncated: next.truncated,
      withMeta: withMeta.objects.map((object) => ({
        key: object.key,
        httpMetadata: object.httpMetadata,
        customMetadata: object.customMetadata,
      })),
      limitZero: await attempt(() => B.list({ prefix: "l/", limit: 0 })),
      limitTooBig: await attempt(() => B.list({ prefix: "l/", limit: 1001 })),
      // A negative bound is R2's "use the default"; a fractional one
      // truncates; a finite value outside int32 is the conversion refusal.
      limitNegative: await attempt(() => pageShape({ prefix: "l/", limit: -1 })),
      limitFraction: await attempt(() => pageShape({ prefix: "l/", limit: 1.5 })),
      limitString: await attempt(() => pageShape({ prefix: "l/", limit: "2" })),
      limitNull: await attempt(() => pageShape({ prefix: "l/", limit: null })),
      limitNan: await attempt(() => pageShape({ prefix: "l/", limit: NaN })),
      limitOutOfRange: await attempt(() =>
        pageShape({ prefix: "l/", limit: Number.MAX_SAFE_INTEGER })),
      // An invalid cursor answers the empty page, and a cursor field that is
      // not a string is a conversion refusal.
      badCursorObjects: bad.objects.length,
      badCursorTruncated: bad.truncated,
      badCursorPrefixes: bad.delimitedPrefixes,
      badCursorAbsent: bad.cursor === undefined,
      badCursorKeys: Object.keys(bad).sort(),
      corruptToken: await attempt(() =>
        pageShape({ prefix: "l/", cursor: "cello-r2-cursor-v1.not-base64!!!" })),
      emptyCursor: await attempt(() => pageShape({ prefix: "l/", cursor: "" })),
      cursorNumber: await attempt(() => pageShape({ prefix: "l/", cursor: 5 })),
      cursorNull: await attempt(() => pageShape({ prefix: "l/", cursor: null })),
      prefixNumber: await attempt(() => pageShape({ prefix: 5 })),
      prefixNull: await attempt(() => pageShape({ prefix: null })),
      startAfterNumber: await attempt(() => pageShape({ startAfter: 5 })),
      startAfterNull: await attempt(() => pageShape({ startAfter: null })),
      delimiterNumber: await attempt(() => pageShape({ delimiter: 5 })),
      delimiterNull: await attempt(() => pageShape({ delimiter: null })),
      includeString: await attempt(() => pageShape({ include: "httpMetadata" })),
      includeNull: await attempt(() => pageShape({ include: null })),
    });
  },

  // Key rules: a `.`/`..` segment is not an object name, on every method.
  async "/keys"() {
    const out = {};
    for (const [name, key] of [
      ["dot", "."],
      ["dotdot", ".."],
      ["segment", "a/../b"],
      ["dotSegment", "a/./b"],
      ["leading", "./a"],
      ["trailing", "a/."],
      ["emptySegment", "a//b"],
      ["plain", "a/b"],
    ]) {
      out[`put:${name}`] = await attempt(async () => {
        const stored = await B.put(key, "v");
        return stored === null ? null : "ok";
      });
      out[`get:${name}`] = await attempt(async () => {
        const got = await B.get(key);
        return got === null ? null : await got.text();
      });
      out[`head:${name}`] = await attempt(async () =>
        (await B.head(key)) === null ? null : "ok");
      out[`delete:${name}`] = await attempt(async () => {
        await B.delete(key);
        return "ok";
      });
    }
    return report(out);
  },

  // User metadata size: name plus value bytes, 8 KiB inclusive.
  async "/metadata"() {
    const out = {};
    out.atLimit = await attempt(async () => {
      await B.put("meta/at", "v", { customMetadata: { k: "v".repeat(8191) } });
      return "ok";
    });
    out.overLimit = await attempt(() =>
      B.put("meta/over", "v", { customMetadata: { k: "v".repeat(8192) } }));
    out.twoKeysOver = await attempt(() =>
      B.put("meta/two", "v", {
        customMetadata: { a: "x", b: "y".repeat(8190) },
      }));
    out.httpMetadataNotCounted = await attempt(async () => {
      await B.put("meta/http", "v", {
        httpMetadata: { contentType: "x".repeat(9000) },
      });
      return "ok";
    });
    return report(out);
  },

  // Multipart: the object's etag is md5(md5s)-N and the part count is N.
  // The part etags a completion names come from `uploadPart`, so each
  // phase is reported even when one fails.
  async "/multipart"() {
    const out = {};
    out.single = await attempt(async () => {
      const upload = await B.createMultipartUpload("mp/one");
      const part = await upload.uploadPart(1, "hello world");
      const object = await upload.complete([{ partNumber: 1, etag: part.etag }]);
      return objectShape(object);
    });
    out.pair = await attempt(async () => {
      const upload = await B.createMultipartUpload("mp/two", {
        customMetadata: { m: "1" },
      });
      // R2 requires every part but the last to be at least 5 MiB.
      const first = await upload.uploadPart(
        1,
        new Uint8Array(5 * 1024 * 1024).fill(97),
      );
      const second = await upload.uploadPart(2, "second");
      const object = await upload.complete([
        { partNumber: 1, etag: first.etag },
        { partNumber: 2, etag: second.etag },
      ]);
      const got = await B.get("mp/two");
      if (got === null) return { object: objectShape(object), tail: null };
      const tail = (await got.text()).slice(-6);
      return {
        object: objectShape(object),
        tail,
        customMetadata: objectShape(got).customMetadata,
      };
    });
    out.abortUnknown = await attempt(async () => {
      const upload = await B.createMultipartUpload("mp/abort");
      await upload.abort();
      return typeof upload.uploadId;
    });
    return report(out);
  },

  // The options argument of each method, converted as workerd's JSG converts
  // a dictionary: an absent or null argument is the empty dictionary, an
  // object is the dictionary its properties spell, and anything else is the
  // WebIDL refusal (#187). `head` and `delete` take no options in workerd,
  // so a stray second argument is ignored there.
  async "/options"() {
    await B.put("o/key", "v");
    const out = {};
    const values = [
      ["string", "arrayBuffer"],
      ["number", 1],
      ["true", true],
      ["null", null],
      ["array", []],
      ["function", () => {}],
      ["date", new Date(0)],
      ["undefined", undefined],
      ["object", {}],
    ];
    for (const [name, value] of values) {
      out[`get:${name}`] = await attempt(async () => {
        const got = await B.get("o/key", value);
        return got === null ? null : "body";
      });
      out[`head:${name}`] = await attempt(async () =>
        (await B.head("o/key", value)) === null ? null : "object");
      out[`put:${name}`] = await attempt(async () =>
        (await B.put("o/put", "v", value)) === null ? null : "object");
      out[`list:${name}`] = await attempt(async () => {
        const page = await B.list(value);
        return { n: page.objects.length };
      });
      out[`delete:${name}`] = await attempt(async () => {
        await B.delete("o/absent", value);
        return "ok";
      });
    }
    return report(out);
  },
};

export default {
  async fetch(request, env) {
    B = env.B;
    const { pathname } = new URL(request.url);
    const route = routes[pathname];
    if (!route) return new Response("not found", { status: 404 });
    try {
      return await route(request);
    } catch (error) {
      return new Response(JSON.stringify(canonical({ uncaught: errorInfo(error) })), {
        status: 500,
      });
    }
  },
};
