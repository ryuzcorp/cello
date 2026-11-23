// Web Crypto conformance fixture: key-usage enforcement (#197), argument
// validation (#198), and AES-GCM IV/tag ranges (#199), plus the values the
// sweep measured as already agreeing so they cannot regress.
import { KeyObject } from "node:crypto";

const enc = new TextEncoder();
const utf8 = (text) => enc.encode(text);
const hex = (bytes) =>
  [...new Uint8Array(bytes)].map((b) => b.toString(16).padStart(2, "0")).join("");

const USAGES = ["sign", "verify", "encrypt", "decrypt", "wrapKey", "unwrapKey", "deriveKey", "deriveBits", "bogus"];

function errorInfo(error) {
  return { name: error?.name, code: error?.code ?? null, message: String(error?.message) };
}
async function attempt(fn) {
  try {
    return { ok: true, value: await fn() };
  } catch (error) {
    return { ok: false, error: errorInfo(error) };
  }
}

// Imports one key per usage and records only the shape (usages/type) or the
// error, never key material.
async function importUsages(format, material, algorithm, extractable) {
  const rows = {};
  for (const usage of USAGES) {
    rows[usage] = await attempt(async () => {
      const key = await crypto.subtle.importKey(format, material, algorithm, extractable, [usage]);
      return { usages: key.usages, type: key.type };
    });
  }
  rows.empty = await attempt(async () => {
    const key = await crypto.subtle.importKey(format, material, algorithm, extractable, []);
    return { usages: key.usages, type: key.type };
  });
  return rows;
}

async function pairUsages(pair, algorithm) {
  const spki = await crypto.subtle.exportKey("spki", pair.publicKey);
  const pkcs8 = await crypto.subtle.exportKey("pkcs8", pair.privateKey);
  return {
    public: await importUsages("spki", spki, algorithm, true),
    private: await importUsages("pkcs8", pkcs8, algorithm, true),
  };
}

async function generateUsages(algorithm) {
  const rows = {};
  for (const usage of [...USAGES, null]) {
    rows[usage ?? "empty"] = await attempt(async () => {
      const key = await crypto.subtle.generateKey(algorithm, true, usage === null ? [] : [usage]);
      if (key?.publicKey) {
        return {
          publicUsages: key.publicKey.usages,
          privateUsages: key.privateKey.usages,
          publicExtractable: key.publicKey.extractable,
        };
      }
      return { usages: key.usages, type: key.type };
    });
  }
  return rows;
}

export default {
  async fetch(request) {
    const path = new URL(request.url).pathname;
    const out = {};

    if (path === "/usages") {
      const material = (n, fill = 1) => new Uint8Array(n).fill(fill);
      out.import = {
        HMAC: await importUsages("raw", utf8("secret"), { name: "HMAC", hash: "SHA-256" }, true),
        "AES-GCM": await importUsages("raw", material(16), "AES-GCM", true),
        "AES-CBC": await importUsages("raw", material(16), "AES-CBC", true),
        "AES-CTR": await importUsages("raw", material(16), "AES-CTR", true),
        "AES-KW": await importUsages("raw", material(16), "AES-KW", true),
        HKDF: await importUsages("raw", material(16), "HKDF", false),
        PBKDF2: await importUsages("raw", material(16), "PBKDF2", false),
      };
      out.hkdfExtractable = await attempt(() =>
        crypto.subtle.importKey("raw", material(16), "HKDF", true, ["deriveBits"]));
      out.pbkdf2Extractable = await attempt(() =>
        crypto.subtle.importKey("raw", material(16), "PBKDF2", true, ["deriveBits"]));

      const ecdsa = await crypto.subtle.generateKey({ name: "ECDSA", namedCurve: "P-256" }, true, ["sign", "verify"]);
      const ecdh = await crypto.subtle.generateKey({ name: "ECDH", namedCurve: "P-256" }, true, ["deriveKey", "deriveBits"]);
      const ed = await crypto.subtle.generateKey("Ed25519", true, ["sign", "verify"]);
      const x = await crypto.subtle.generateKey("X25519", true, ["deriveKey", "deriveBits"]);
      const rsaOaep = await crypto.subtle.generateKey({ name: "RSA-OAEP", modulusLength: 1024, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" }, true, ["decrypt", "unwrapKey"]);
      const rsaPkcs1 = await crypto.subtle.generateKey({ name: "RSASSA-PKCS1-v1_5", modulusLength: 1024, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" }, true, ["sign", "verify"]);
      const rsaPss = await crypto.subtle.generateKey({ name: "RSA-PSS", modulusLength: 1024, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" }, true, ["sign", "verify"]);
      out.asymmetricImport = {
        ECDSA: await pairUsages(ecdsa, { name: "ECDSA", namedCurve: "P-256" }),
        ECDH: await pairUsages(ecdh, { name: "ECDH", namedCurve: "P-256" }),
        Ed25519: await pairUsages(ed, "Ed25519"),
        X25519: await pairUsages(x, "X25519"),
        "RSA-OAEP": await pairUsages(rsaOaep, { name: "RSA-OAEP", hash: "SHA-256" }),
        "RSASSA-PKCS1-v1_5": await pairUsages(rsaPkcs1, { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" }),
        "RSA-PSS": await pairUsages(rsaPss, { name: "RSA-PSS", hash: "SHA-256" }),
      };
      out.generate = {
        HMAC: await generateUsages({ name: "HMAC", hash: "SHA-256" }),
        "AES-GCM": await generateUsages({ name: "AES-GCM", length: 128 }),
        "AES-KW": await generateUsages({ name: "AES-KW", length: 128 }),
        ECDSA: await generateUsages({ name: "ECDSA", namedCurve: "P-256" }),
        ECDH: await generateUsages({ name: "ECDH", namedCurve: "P-256" }),
        Ed25519: await generateUsages("Ed25519"),
        X25519: await generateUsages("X25519"),
      };
      return Response.json(out);
    }

    if (path === "/ops") {
      const hmac = (usages) => crypto.subtle.importKey("raw", utf8("secret"), { name: "HMAC", hash: "SHA-256" }, true, usages);
      const signOnly = await hmac(["sign"]);
      const verifyOnly = await hmac(["verify"]);
      const signature = await crypto.subtle.sign("HMAC", signOnly, utf8("message"));
      out.hmac = {
        signVerifyOnly: await attempt(() => crypto.subtle.sign("HMAC", verifyOnly, utf8("message"))),
        verifySignOnly: await attempt(() => crypto.subtle.verify("HMAC", signOnly, signature, utf8("message"))),
        verifyGood: await attempt(() => crypto.subtle.verify("HMAC", verifyOnly, signature, utf8("message"))),
        emptyUsages: await attempt(() => hmac([])),
        duplicateUsages: await attempt(async () => (await hmac(["sign", "sign"])).usages),
      };

      const aesEncrypt = await crypto.subtle.importKey("raw", new Uint8Array(16), "AES-GCM", true, ["encrypt"]);
      const aesDecrypt = await crypto.subtle.importKey("raw", new Uint8Array(16), "AES-GCM", true, ["decrypt"]);
      const iv = new Uint8Array(12);
      const ciphertext = await crypto.subtle.encrypt({ name: "AES-GCM", iv }, aesEncrypt, utf8("x"));
      out.aes = {
        decryptEncryptOnly: await attempt(() => crypto.subtle.decrypt({ name: "AES-GCM", iv }, aesEncrypt, ciphertext)),
        encryptDecryptOnly: await attempt(() => crypto.subtle.encrypt({ name: "AES-GCM", iv }, aesDecrypt, utf8("x"))),
        wrapEncryptOnly: await attempt(() => crypto.subtle.wrapKey("raw", aesDecrypt, aesEncrypt, { name: "AES-GCM", iv })),
        unwrapEncryptOnly: await attempt(async () => {
          const wrapped = await crypto.subtle.wrapKey("raw", aesDecrypt, aesEncrypt, { name: "AES-GCM", iv });
          return crypto.subtle.unwrapKey("raw", wrapped, aesEncrypt, { name: "AES-GCM", iv }, "AES-GCM", true, ["encrypt"]);
        }),
      };

      const kwWrap = await crypto.subtle.importKey("raw", new Uint8Array(16), "AES-KW", true, ["wrapKey"]);
      out.aesKw = {
        wrap: await attempt(async () => new Uint8Array(await crypto.subtle.wrapKey("raw", aesEncrypt, kwWrap, "AES-KW")).length),
        unwrapWithoutUsage: await attempt(async () => {
          const wrapped = await crypto.subtle.wrapKey("raw", aesEncrypt, kwWrap, "AES-KW");
          return crypto.subtle.unwrapKey("raw", wrapped, kwWrap, "AES-KW", "AES-GCM", true, ["encrypt"]);
        }),
      };

      const ecdsa = await crypto.subtle.generateKey({ name: "ECDSA", namedCurve: "P-256" }, true, ["sign", "verify"]);
      const ecdsaSignature = await crypto.subtle.sign({ name: "ECDSA", hash: "SHA-256" }, ecdsa.privateKey, utf8("message"));
      out.ecdsa = {
        signPublic: await attempt(() => crypto.subtle.sign({ name: "ECDSA", hash: "SHA-256" }, ecdsa.publicKey, utf8("message"))),
        verifyPrivate: await attempt(() => crypto.subtle.verify({ name: "ECDSA", hash: "SHA-256" }, ecdsa.privateKey, ecdsaSignature, utf8("message"))),
        verifyGood: await attempt(() => crypto.subtle.verify({ name: "ECDSA", hash: "SHA-256" }, ecdsa.publicKey, ecdsaSignature, utf8("message"))),
      };

      const ed = await crypto.subtle.generateKey("Ed25519", true, ["sign", "verify"]);
      const edSignature = await crypto.subtle.sign("Ed25519", ed.privateKey, utf8("message"));
      out.ed25519 = {
        signPublic: await attempt(() => crypto.subtle.sign("Ed25519", ed.publicKey, utf8("message"))),
        encrypt: await attempt(() => crypto.subtle.encrypt("Ed25519", ed.publicKey, utf8("x"))),
        verifyGood: await attempt(() => crypto.subtle.verify("Ed25519", ed.publicKey, edSignature, utf8("message"))),
      };

      const ecdh = await crypto.subtle.generateKey({ name: "ECDH", namedCurve: "P-256" }, true, ["deriveKey", "deriveBits"]);
      out.ecdh = {
        deriveBitsPublic: await attempt(() => crypto.subtle.deriveBits({ name: "ECDH", public: ecdh.publicKey }, ecdh.publicKey, 256)),
        deriveBitsGood: await attempt(async () => (await crypto.subtle.deriveBits({ name: "ECDH", public: ecdh.publicKey }, ecdh.privateKey, 256)).byteLength),
        deriveKeyGood: await attempt(async () => {
          const key = await crypto.subtle.deriveKey({ name: "ECDH", public: ecdh.publicKey }, ecdh.privateKey, { name: "AES-GCM", length: 128 }, true, ["encrypt"]);
          return { usages: key.usages, type: key.type };
        }),
      };

      const hkdfBits = await crypto.subtle.importKey("raw", new Uint8Array(16), "HKDF", false, ["deriveBits"]);
      const hkdfKey = await crypto.subtle.importKey("raw", new Uint8Array(16), "HKDF", false, ["deriveKey"]);
      const hkdfParams = { name: "HKDF", hash: "SHA-256", salt: new Uint8Array(), info: new Uint8Array() };
      out.hkdf = {
        deriveBits: await attempt(async () => hex(await crypto.subtle.deriveBits(hkdfParams, hkdfBits, 128))),
        deriveKeyWithoutUsage: await attempt(() =>
          crypto.subtle.deriveKey(hkdfParams, hkdfBits, { name: "HMAC", hash: "SHA-256", length: 128 }, true, ["sign"])),
        deriveBitsWithDeriveKey: await attempt(() => crypto.subtle.deriveBits(hkdfParams, hkdfKey, 128)),
        deriveKey: await attempt(async () => (await crypto.subtle.deriveKey(hkdfParams, hkdfKey, { name: "HMAC", hash: "SHA-256", length: 128 }, true, ["sign"])).usages),
      };

      const pbkdf2 = await crypto.subtle.importKey("raw", utf8("password"), "PBKDF2", false, ["deriveBits", "deriveKey"]);
      const pbkdf2Params = { name: "PBKDF2", hash: "SHA-256", salt: utf8("salt"), iterations: 10 };
      out.pbkdf2 = {
        deriveBits: await attempt(async () => hex(await crypto.subtle.deriveBits(pbkdf2Params, pbkdf2, 128))),
        deriveKeyBadUsage: await attempt(() =>
          crypto.subtle.deriveKey(pbkdf2Params, pbkdf2, { name: "AES-GCM", length: 128 }, true, ["sign"])),
        deriveKeyEmptyUsages: await attempt(() =>
          crypto.subtle.deriveKey(pbkdf2Params, pbkdf2, { name: "AES-GCM", length: 128 }, true, [])),
      };
      return Response.json(out);
    }

    if (path === "/validation") {
      out.digestNoArgs = await attempt(() => crypto.subtle.digest());
      out.digestNull = await attempt(() => crypto.subtle.digest(null, new Uint8Array([1])));
      out.digestBadData = await attempt(() => crypto.subtle.digest("SHA-256", "abc"));
      out.digestUnknown = await attempt(() => crypto.subtle.digest("SHA-999", utf8("abc")));
      out.digestUnknownObject = await attempt(() => crypto.subtle.digest({ name: "SHA-999" }, utf8("abc")));
      out.exportBogus = await attempt(() => crypto.subtle.exportKey("bogus", {}));
      out.deriveNoKey = await attempt(() => crypto.subtle.deriveBits({ name: "PBKDF2" }, {}, 128));
      out.deriveNullKey = await attempt(() => crypto.subtle.deriveBits({ name: "PBKDF2" }, null, 128));
      out.signNoKey = await attempt(() => crypto.subtle.sign("HMAC", {}, new Uint8Array([1])));
      out.signNullKey = await attempt(() => crypto.subtle.sign("HMAC", null, new Uint8Array([1])));
      out.signBadData = await attempt(async () => {
        const key = await crypto.subtle.importKey("raw", utf8("s"), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
        return crypto.subtle.sign("HMAC", key, "str");
      });
      out.signNoAlgorithm = await attempt(async () => {
        const key = await crypto.subtle.importKey("raw", utf8("s"), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
        return crypto.subtle.sign(undefined, key, utf8("m"));
      });
      out.signWrongKeyAlgorithm = await attempt(async () => {
        const key = await crypto.subtle.generateKey({ name: "ECDSA", namedCurve: "P-256" }, true, ["sign", "verify"]);
        return crypto.subtle.sign("HMAC", key.privateKey, utf8("m"));
      });
      out.verifyBadSignature = await attempt(async () => {
        const key = await crypto.subtle.importKey("raw", utf8("s"), { name: "HMAC", hash: "SHA-256" }, false, ["verify"]);
        return crypto.subtle.verify("HMAC", key, "notbytes", utf8("x"));
      });
      out.encryptNoKey = await attempt(() => crypto.subtle.encrypt("AES-GCM", {}, new Uint8Array([1])));
      out.encryptNoIv = await attempt(async () => {
        const key = await crypto.subtle.importKey("raw", new Uint8Array(16), "AES-GCM", false, ["encrypt"]);
        return crypto.subtle.encrypt("AES-GCM", key, utf8("x"));
      });
      out.encryptNullIv = await attempt(async () => {
        const key = await crypto.subtle.importKey("raw", new Uint8Array(16), "AES-GCM", false, ["encrypt"]);
        return crypto.subtle.encrypt({ name: "AES-GCM", iv: null }, key, utf8("x"));
      });
      out.encryptBadData = await attempt(async () => {
        const key = await crypto.subtle.importKey("raw", new Uint8Array(16), "AES-GCM", false, ["encrypt"]);
        return crypto.subtle.encrypt({ name: "AES-GCM", iv: new Uint8Array(12) }, key, "str");
      });
      out.wrapNoKey = await attempt(() => crypto.subtle.wrapKey("raw", {}, {}, "AES-GCM"));
      out.unwrapNoKey = await attempt(() => crypto.subtle.unwrapKey("raw", new Uint8Array(8), {}, "AES-GCM", "AES-GCM", true, []));
      out.generateBadAlgorithm = await attempt(() => crypto.subtle.generateKey("bogus", true, []));
      out.importBadFormat = await attempt(() => crypto.subtle.importKey("bogus", utf8("x"), "AES-GCM", true, ["encrypt"]));
      out.importBadUsage = await attempt(() => crypto.subtle.importKey("raw", utf8("x"), "AES-GCM", true, ["bogus"]));
      out.importUsagesUndefined = await attempt(() => crypto.subtle.importKey("raw", utf8("s"), { name: "HMAC", hash: "SHA-256" }, true, undefined));
      out.importUsagesString = await attempt(() => crypto.subtle.importKey("raw", utf8("s"), { name: "HMAC", hash: "SHA-256" }, true, "sign"));
      out.importUsageNumber = await attempt(() => crypto.subtle.importKey("raw", utf8("s"), { name: "HMAC", hash: "SHA-256" }, true, [1]));
      out.deriveBadLength = await attempt(async () => {
        const key = await crypto.subtle.importKey("raw", utf8("p"), "PBKDF2", false, ["deriveBits"]);
        return crypto.subtle.deriveBits({ name: "PBKDF2", hash: "SHA-256", salt: utf8("s"), iterations: 1 }, key, 7);
      });
      out.deriveBadIterations = await attempt(async () => {
        const key = await crypto.subtle.importKey("raw", utf8("p"), "PBKDF2", false, ["deriveBits"]);
        return crypto.subtle.deriveBits({ name: "PBKDF2", hash: "SHA-256", salt: utf8("s"), iterations: 0 }, key, 128);
      });
      out.hkdfBadLength = await attempt(async () => {
        const key = await crypto.subtle.importKey("raw", utf8("p"), "HKDF", false, ["deriveBits"]);
        return crypto.subtle.deriveBits({ name: "HKDF", hash: "SHA-256", salt: utf8("s"), info: utf8("i") }, key, 7);
      });
      out.deriveBitsNoAlgorithm = await attempt(async () => {
        const key = await crypto.subtle.importKey("raw", utf8("p"), "PBKDF2", false, ["deriveBits", "deriveKey"]);
        return crypto.subtle.deriveBits(undefined, key, 128);
      });
      out.deriveKeyNoAlgorithm = await attempt(async () => {
        const key = await crypto.subtle.importKey("raw", utf8("p"), "PBKDF2", false, ["deriveBits", "deriveKey"]);
        return crypto.subtle.deriveKey(null, key, { name: "AES-GCM", length: 128 }, true, ["encrypt"]);
      });
      out.importNoAlgorithm = await attempt(() =>
        crypto.subtle.importKey("raw", utf8("s"), undefined, true, ["sign"]));
      out.generateNoAlgorithm = await attempt(() =>
        crypto.subtle.generateKey(undefined, true, ["sign"]));
      out.wrapNoAlgorithm = await attempt(async () => {
        const key = await crypto.subtle.importKey("raw", new Uint8Array(16), "AES-GCM", true, ["encrypt", "wrapKey"]);
        return crypto.subtle.wrapKey("raw", key, key, undefined);
      });
      out.getRandomValuesArray = attemptSync(() => crypto.getRandomValues([1, 2, 3]));
      out.getRandomValuesFloat = attemptSync(() => crypto.getRandomValues(new Float64Array(2)));
      out.getRandomValuesLimit = attemptSync(() => crypto.getRandomValues(new Uint8Array(65537)));
      out.exportNonExtractable = await attempt(async () => {
        const key = await crypto.subtle.importKey("raw", utf8("s"), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
        return crypto.subtle.exportKey("raw", key);
      });
      return Response.json(out);
    }

    if (path === "/isolation") {
      // Attack probes whose outcome agrees with workerd: the material is not
      // reachable, the clone registry has no callable symbol, a CryptoKey
      // refuses structuredClone, a prototype-only forgery clones as plain
      // data, and the guest constructor throws.
      const key = await crypto.subtle.importKey("raw", utf8("s"), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
      out.materialType = typeof key.__celloMaterial;
      out.ownNames = Object.getOwnPropertyNames(key).sort();
      out.symbolFunctions = Object.getOwnPropertySymbols(CryptoKey.prototype)
        .filter((symbol) => typeof CryptoKey.prototype[symbol] === "function")
        .map(String);
      out.constructor = attemptSync(() => new CryptoKey("secret", { name: "HMAC" }, false, ["sign"], {}));
      out.constructorCall = attemptSync(() => CryptoKey.call({}));
      out.constructorLength = CryptoKey.length;
      out.constructorTag = String(key);
      const forged = Object.create(CryptoKey.prototype);
      Object.assign(forged, {
        type: "secret", algorithm: key.algorithm, extractable: false,
        usages: ["sign"], __celloMaterial: { bytes: utf8("s") },
      });
      out.forgedSign = await attempt(() => crypto.subtle.sign("HMAC", forged, utf8("m")));
      out.forgedWrap = await attempt(() => crypto.subtle.wrapKey("raw", forged, key, { name: "AES-GCM", iv: new Uint8Array(12) }));
      out.exportNonExtractable = await attempt(() => crypto.subtle.exportKey("raw", key));
      const verifyOnly = await crypto.subtle.importKey("raw", utf8("s"), { name: "HMAC", hash: "SHA-256" }, false, ["verify"]);
      out.verifyOnlySign = await attempt(() => crypto.subtle.sign("HMAC", verifyOnly, utf8("m")));

      // structuredClone: a key is refused with workerd's message; a plain
      // object with key-shaped fields is plain data; a prototype-only forgery
      // of any host type clones as plain data rather than reviving.
      out.cloneKey = await attempt(() => structuredClone(key));
      out.cloneProto = await attempt(() => {
        const clone = structuredClone(Object.create(CryptoKey.prototype));
        return {
          isKey: clone instanceof CryptoKey,
          tag: Object.prototype.toString.call(clone),
          keys: Object.keys(clone),
        };
      });
      out.cloneForged = await attempt(() => {
        const clone = structuredClone({
          type: "CryptoKey", keyType: "secret", material: { bytes: [1] },
        });
        return { isKey: clone instanceof CryptoKey, keys: Object.keys(clone) };
      });
      out.cloneForgedBlob = await attempt(() => {
        const clone = structuredClone(Object.create(Blob.prototype));
        return {
          isBlob: clone instanceof Blob,
          tag: Object.prototype.toString.call(clone),
          keys: Object.keys(clone),
        };
      });
      out.cloneForgedRequest = await attempt(() => {
        const clone = structuredClone(Object.create(Request.prototype));
        return {
          isRequest: clone instanceof Request,
          tag: Object.prototype.toString.call(clone),
        };
      });
      out.keyObjectExport = await attempt(() => {
        const bytes = KeyObject.from(key).export();
        return new Uint8Array(bytes).byteLength;
      });
      return Response.json(out);
    }

    if (path === "/isolation-strict") {
      // The Cello-only side of the same contract (workerd's own properties are
      // mutable and its sign reads internal slots; Cello serves frozen copies,
      // so a mutation attempt throws). Driven by the Rust conformance test.
      const key = await crypto.subtle.importKey("raw", utf8("s"), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
      const before = hex(await crypto.subtle.sign("HMAC", key, utf8("m")));
      let mutate = "ok";
      try {
        key.algorithm.hash.name = "SHA-1";
      } catch (error) {
        mutate = `${error.name}: ${error.message}`;
      }
      const after = hex(await crypto.subtle.sign("HMAC", key, utf8("m")));
      let push = "ok";
      try {
        key.usages.push("verify");
      } catch (error) {
        push = `${error.name}: ${error.message}`;
      }
      out.mutation = {
        before, after, same: before === after, reported: key.algorithm.hash.name,
      };
      out.mutateThrew = mutate !== "ok";
      out.pushThrew = push !== "ok";
      out.usagesAfterPush = key.usages;
      out.algorithmFrozen = Object.isFrozen(key.algorithm) && Object.isFrozen(key.algorithm.hash);
      out.usagesFrozen = Object.isFrozen(key.usages);
      const descriptor = Object.getOwnPropertyDescriptor(key, "algorithm");
      out.descriptor = {
        accessor: descriptor.get !== undefined,
        enumerable: descriptor.enumerable,
        configurable: descriptor.configurable,
      };

      // The old registry's brand was a discoverable symbol. Assert no
      // symbol on the key or its prototype is callable as a serializer, and
      // that the global registry name is gone.
      out.legacySymbol = [
        CryptoKey.prototype, Blob.prototype, DOMException.prototype,
      ].map((prototype) =>
        typeof prototype[Symbol.for("Cells.structuredCloneHost")]);
      out.callableSymbols = [];
      for (const symbol of [
        ...Object.getOwnPropertySymbols(CryptoKey.prototype),
        ...Object.getOwnPropertySymbols(key),
      ]) {
        let value;
        try {
          value = symbol in CryptoKey.prototype
            ? CryptoKey.prototype[symbol] : key[symbol];
        } catch {
          continue;
        }
        if (typeof value === "function") {
          try {
            out.callableSymbols.push([String(symbol), value.call(key)]);
          } catch (error) {
            out.callableSymbols.push([String(symbol), `${error.name}`]);
          }
        }
      }

      // A forged Blob that copies a real Blob's constructor brand property is
      // still not in the internal brand set: V8 refuses the function-valued
      // property, and the bytes are never revived as a Blob.
      const realBlob = new Blob(["hi"]);
      const forgedBlob = Object.create(Blob.prototype);
      forgedBlob._bytes = utf8("hi");
      forgedBlob.__celloHost = realBlob.__celloHost;
      const copied = attemptSync(() => structuredClone(forgedBlob));
      out.copiedBlobBrand = {
        ok: copied.ok,
        isBlob: copied.ok ? copied.value instanceof Blob : false,
      };

      // A CryptoKey is never revived through the clone path.
      const cloneKey = attemptSync(() => structuredClone(key));
      out.cloneKey = cloneKey.ok
        ? { ok: true, isKey: cloneKey.value instanceof CryptoKey }
        : { ok: false, name: cloneKey.error.name, message: cloneKey.error.message };
      return Response.json(out);
    }

    if (path === "/gcm") {
      const key = await crypto.subtle.importKey("raw", new Uint8Array(16).fill(1), "AES-GCM", true, ["encrypt", "decrypt"]);
      for (const length of [0, 1, 2, 3, 4, 8, 11, 12, 13, 15, 16, 17, 24, 32, 64, 128, 129]) {
        out["iv" + length] = await attempt(async () => {
          const iv = new Uint8Array(length).fill(length);
          const ciphertext = await crypto.subtle.encrypt({ name: "AES-GCM", iv }, key, utf8("x"));
          const plaintext = await crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, ciphertext);
          return { ciphertext: new Uint8Array(ciphertext).length, plaintext: new TextDecoder().decode(plaintext) };
        });
      }
      for (const tagLength of [0, 32, 63, 64, 95, 96, 104, 112, 120, 128, 129]) {
        out["tag" + tagLength] = await attempt(async () => {
          const iv = new Uint8Array(12);
          const ciphertext = await crypto.subtle.encrypt({ name: "AES-GCM", iv, tagLength }, key, utf8("x"));
          const plaintext = await crypto.subtle.decrypt({ name: "AES-GCM", iv, tagLength }, key, ciphertext);
          return { ciphertext: new Uint8Array(ciphertext).length, plaintext: new TextDecoder().decode(plaintext) };
        });
      }
      // Deterministic BoringSSL vectors across the lengths the old
      // fixed-type implementation could not take.
      const vectors = [
        [new Uint8Array(12).fill(2), 32],
        [new Uint8Array(12).fill(2), 64],
        [new Uint8Array([1, 2, 3, 4]), 128],
        [new Uint8Array([9]), 32],
        [new Uint8Array([1, 2, 3]), 64],
        [new Uint8Array(12).fill(7), 96],
        [new Uint8Array(24).fill(5), 104],
        [new Uint8Array(33).fill(3), 128],
      ];
      for (const [iv, tagLength] of vectors) {
        out["vector" + iv.length + "-" + tagLength] = await attempt(async () => {
          const ciphertext = await crypto.subtle.encrypt(
            { name: "AES-GCM", iv, tagLength, additionalData: utf8("aad") }, key, utf8("payload"));
          return { hex: hex(ciphertext), plaintext: new TextDecoder().decode(await crypto.subtle.decrypt(
            { name: "AES-GCM", iv, tagLength, additionalData: utf8("aad") }, key, ciphertext)) };
        });
      }
      return Response.json(out);
    }

    if (path === "/values") {
      out.digest = {
        sha256: hex(await crypto.subtle.digest("SHA-256", utf8("abc"))),
        sha1: hex(await crypto.subtle.digest("SHA-1", utf8("abc"))),
        lowercase: hex(await crypto.subtle.digest("sha-256", utf8("abc"))),
        md5: hex(await crypto.subtle.digest("MD5", utf8("abc"))),
        arrayBuffer: hex(await crypto.subtle.digest("SHA-256", utf8("abc").buffer)),
      };
      const hmacKey = await crypto.subtle.importKey("raw", utf8("secret"), { name: "HMAC", hash: "SHA-256" }, false, ["sign", "verify"]);
      const hmacSignature = await crypto.subtle.sign("HMAC", hmacKey, utf8("message"));
      out.hmac = {
        signature: hex(hmacSignature),
        verifyGood: await crypto.subtle.verify("HMAC", hmacKey, hmacSignature, utf8("message")),
        verifyBad: await crypto.subtle.verify("HMAC", hmacKey, hmacSignature, utf8("messagf")),
      };
      const aesKey = await crypto.subtle.importKey("raw", new Uint8Array(16).fill(4), "AES-GCM", true, ["encrypt", "decrypt"]);
      const aesIv = new Uint8Array(12).fill(2);
      const aesCiphertext = await crypto.subtle.encrypt({ name: "AES-GCM", iv: aesIv }, aesKey, utf8("hello world"));
      out.aesGcm = {
        ciphertext: hex(aesCiphertext),
        plaintext: new TextDecoder().decode(await crypto.subtle.decrypt({ name: "AES-GCM", iv: aesIv }, aesKey, aesCiphertext)),
        exported: hex(await crypto.subtle.exportKey("raw", aesKey)),
      };
      const ecdsa = await crypto.subtle.generateKey({ name: "ECDSA", namedCurve: "P-256" }, true, ["sign", "verify"]);
      const ecdsaSignature = await crypto.subtle.sign({ name: "ECDSA", hash: "SHA-256" }, ecdsa.privateKey, utf8("message"));
      out.ecdsa = {
        verifyGood: await crypto.subtle.verify({ name: "ECDSA", hash: "SHA-256" }, ecdsa.publicKey, ecdsaSignature, utf8("message")),
        verifyBad: await crypto.subtle.verify({ name: "ECDSA", hash: "SHA-256" }, ecdsa.publicKey, ecdsaSignature, utf8("messagf")),
        rawLength: (await crypto.subtle.exportKey("raw", ecdsa.publicKey)).byteLength,
      };
      const ed = await crypto.subtle.generateKey("Ed25519", true, ["sign", "verify"]);
      const edSignature = await crypto.subtle.sign("Ed25519", ed.privateKey, utf8("message"));
      out.ed25519 = {
        // The key is generated per runtime, so only the signature length is
        // comparable; the verifies prove the bytes are a working signature.
        signatureLength: edSignature.byteLength,
        verifyGood: await crypto.subtle.verify("Ed25519", ed.publicKey, edSignature, utf8("message")),
        verifyBad: await crypto.subtle.verify("Ed25519", ed.publicKey, edSignature, utf8("messagf")),
        rawLength: (await crypto.subtle.exportKey("raw", ed.publicKey)).byteLength,
      };
      out.random = {
        uuid: (() => {
          const id = crypto.randomUUID();
          // The variant nibble is any of 8b9a-ab; only that range is fixed.
          return {
            length: id.length,
            hex: /^[0-9a-f-]+$/.test(id),
            version: id[14],
            variant: "89ab".includes(id[19]),
          };
        })(),
        limit: crypto.getRandomValues(new Uint8Array(65536)).length,
        zero: crypto.getRandomValues(new Uint8Array(0)).length,
        sameBuffer: crypto.getRandomValues(new Uint8Array(16)).byteLength,
      };
      out.timingSafeEqual = {
        same: crypto.subtle.timingSafeEqual(utf8("abc"), utf8("abc")),
        different: crypto.subtle.timingSafeEqual(utf8("abc"), utf8("abd")),
        lengthMismatch: (() => {
          try {
            crypto.subtle.timingSafeEqual(utf8("abc"), utf8("ab"));
            return "no error";
          } catch (error) {
            return `${error.name}: ${error.message}`;
          }
        })(),
      };
      const digestStream = new crypto.DigestStream("SHA-256");
      const writer = digestStream.getWriter();
      await writer.write("abc");
      await writer.close();
      out.digestStream = hex(await digestStream.digest);
      return Response.json(out);
    }

    return Response.json({ routes: ["/usages", "/ops", "/validation", "/gcm", "/values"] });
  },
};

function attemptSync(fn) {
  try {
    return { ok: true, value: fn() };
  } catch (error) {
    return { ok: false, error: errorInfo(error) };
  }
}
