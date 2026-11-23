// Modified by Ryuz Corp (Cello): AES-KW import/generate/wrap/unwrap + symmetric JWK import/export (upstream #295); raw EC key import/export (upstream #271).
// Web Crypto implementation for the embedded runtime.
//
// The common WebCrypto paths use typed-array host ops.
// Cells keeps a second, JSON-shaped op for Workers-compatible algorithms that
// require structured arguments (RSA-OAEP, ECDSA, and Ed25519).
(function () {
  const _randomValues = $$randomValues;
  const _digest = $$digest;
  const _hmacSign = $$hmacSign;
  const _hmacVerify = $$hmacVerify;
  const _aesEncrypt = $$aesEncrypt;
  const _aesDecrypt = $$aesDecrypt;

  // MD5 is not in the Web Crypto spec; Cloudflare accepts it for `digest`
  // and DigestStream, and the host op has always implemented it.
  const _CRC_ALGS = new Set(["CRC32", "CRC32C", "CRC64NVME"]);
  const _DIGEST_ALGS = new Set([
    "SHA-1", "SHA-256", "SHA-384", "SHA-512", "MD5",
  ]);
  const _SECRET_KEY_ALGS = new Set(
    ["HMAC", "AES-GCM", "AES-CBC", "AES-CTR", "AES-KW"],
  );
  // base64url without padding, the JWK encoding. `atob`/`btoa` are the
  // binary-string pair from the prelude; the alphabet swap keeps padding
  // out, because a padded `k` still decodes but no writer emits one. The
  // binary string builds in 32 KiB chunks: spreading a large key into one
  // `fromCharCode` call would blow the engine's argument-count limit.
  function _b64urlEncode(bytes) {
    let bin = "";
    for (let i = 0; i < bytes.length; i += 0x8000) {
      bin += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
    }
    return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  }
  function _b64urlDecode(text) {
    const padded = String(text).replace(/-/g, "+").replace(/_/g, "/");
    const bin = atob(padded);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  }
  // The JWK `alg` for a symmetric key: HMAC by hash, AES by length. Any
  // other length has no JOSE name, so it exports without one rather than
  // with a wrong one.
  function _secretJwkAlg(name, bytes, hash) {
    if (name === "HMAC") {
      // Every hash has a JOSE name, SHA-1 included (W3C WebCrypto).
      return { "SHA-1": "HS1", "SHA-256": "HS256", "SHA-384": "HS384", "SHA-512": "HS512" }[hash];
    }
    const bits = bytes.byteLength * 8;
    if (name === "AES-GCM")
      return { 128: "A128GCM", 192: "A192GCM", 256: "A256GCM" }[bits];
    if (name === "AES-CBC")
      return { 128: "A128CBC", 192: "A192CBC", 256: "A256CBC" }[bits];
    if (name === "AES-CTR")
      return { 128: "A128CTR", 192: "A192CTR", 256: "A256CTR" }[bits];
    if (name === "AES-KW")
      return { 128: "A128KW", 192: "A192KW", 256: "A256KW" }[bits];
    return undefined;
  }
  // Cloudflare accepts its own pre-standard curve spellings beside the
  // standard ones.
  const _curveName = (curve) =>
    curve === "NODE-ED25519" ? "Ed25519" : String(curve ?? "");
  // The curves cello carries, under every spelling Web Crypto and Node use.
  const _EC_CURVES = {
    "P-256": "P-256", "prime256v1": "P-256", "secp256r1": "P-256",
    "P-384": "P-384", "secp384r1": "P-384",
    "P-521": "P-521", "secp521r1": "P-521",
  };
  const _canonicalEcCurve = (curve) => _EC_CURVES[_curveName(curve)];
  // The asymmetric algorithms, as the uppercased name `_algorithmName`
  // produces mapped to the spelling `key.algorithm.name` reports. The
  // uppercased form is for lookup only: echoing it back names an algorithm
  // no library recognizes, because `RSASSA-PKCS1-V1_5` is not the Web
  // Crypto spelling of `RSASSA-PKCS1-v1_5`.
  const _ASYM_NAMES = {
    "RSASSA-PKCS1-V1_5": "RSASSA-PKCS1-v1_5",
    "RSA-OAEP": "RSA-OAEP",
    "RSA-PSS": "RSA-PSS",
    "ECDSA": "ECDSA",
    "ECDH": "ECDH",
    "ED25519": "Ed25519",
    // Cloudflare's pre-standard spelling reports itself back, because a
    // caller that asked with it matches on what it asked for -- which is
    // what the upstream `eddsa_test` group pins.
    "NODE-ED25519": "NODE-ED25519",
    "X25519": "X25519",
  };
  // The one signature algorithm over Curve25519, under both spellings. Its
  // two names share every host op, so sign, verify and generate look the
  // algorithm up here rather than testing two strings each.
  const _ED25519_ALGS = new Set(["ED25519", "NODE-ED25519"]);
  // The Secure Curves, whose `raw` Web Crypto form is the bare 32-byte
  // public point rather than a DER document.
  const _OKP_CURVES = { "ED25519": "Ed25519", "NODE-ED25519": "Ed25519",
    "X25519": "X25519" };
  // Algorithms whose keys are asymmetric, whatever cello can then *do* with
  // them: import validates the key, and an unsupported operation throws
  // later at sign/verify/encrypt rather than here. Derived from the table
  // above, not written twice: an algorithm that imports but has no reported
  // spelling would report the uppercased lookup key.
  const _ASYM_ALGS = new Set(Object.keys(_ASYM_NAMES));

  function _toBuf(data) {
    if (data instanceof ArrayBuffer) return new Uint8Array(data);
    if (ArrayBuffer.isView(data)) {
      return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    }
    throw new TypeError("expected an ArrayBuffer or ArrayBufferView");
  }

  function _algorithmName(algorithm) {
    return String(
      typeof algorithm === "string" ? algorithm : algorithm?.name || "",
    ).toUpperCase();
  }

  // The caller's spelling of the algorithm, for the message Web Crypto raises
  // when it does not match the key. Web IDL keeps the string the caller wrote;
  // only the comparison is case-insensitive.
  function _requestedAlgorithmName(algorithm) {
    if (algorithm !== null && typeof algorithm === "object") {
      const name = algorithm.name;
      return name === undefined ? "" : String(name);
    }
    return String(algorithm);
  }

  // Web IDL argument errors, in workerd's exact text: a Worker that catches a
  // `TypeError` and matches the message keeps working. The type names are the
  // IDL ones (`CryptoKey`, `BufferSource`, `Array`), not the JS constructors.
  function _argumentTypeError(method, index, type) {
    return new TypeError(
      `Failed to execute '${method}' on 'SubtleCrypto': parameter ${index} ` +
        `is not of type '${type}'.`,
    );
  }

  function _requireAlgorithm(algorithm, interfaceName) {
    if (algorithm === undefined || algorithm === null) {
      throw new TypeError(
        `Cannot initialize ${interfaceName} with required members from an ` +
          "undefined or null value.",
      );
    }
  }

  function _requireCryptoKey(method, index, key) {
    // Internal-slot membership, not `instanceof`: a forged object with
    // `CryptoKey.prototype` in its chain has no slots and is refused.
    if (!_KEY_SLOTS.has(key)) {
      throw _argumentTypeError(method, index, "CryptoKey");
    }
    return key;
  }

  function _requireBufferSource(method, index, data) {
    if (!(data instanceof ArrayBuffer) && !ArrayBuffer.isView(data)) {
      throw _argumentTypeError(method, index, "BufferSource");
    }
    return data;
  }

  function _requireUsages(method, index, usages) {
    if (!Array.isArray(usages)) {
      throw _argumentTypeError(method, index, "Array");
    }
    // Web IDL converts each entry to a KeyUsage string, and the key stores its
    // usages as a set: a duplicate is kept once, in first-seen order.
    return [...new Set(usages.map((usage) => String(usage)))];
  }

  // The usages Web Crypto lets each algorithm's key carry, split by the key's
  // type. A public key has no operation of its own for ECDH/X25519, so its
  // list is empty and only the private half derives. Measured against workerd
  // 1.20261121.1: importKey, generateKey and deriveKey reject anything else
  // with a SyntaxError.
  const _KEY_USAGES = {
    "HMAC": { secret: ["sign", "verify"] },
    "AES-GCM": { secret: ["encrypt", "decrypt", "wrapKey", "unwrapKey"] },
    "AES-CBC": { secret: ["encrypt", "decrypt", "wrapKey", "unwrapKey"] },
    "AES-CTR": { secret: ["encrypt", "decrypt", "wrapKey", "unwrapKey"] },
    "AES-KW": { secret: ["wrapKey", "unwrapKey"] },
    "HKDF": { secret: ["deriveKey", "deriveBits"] },
    "PBKDF2": { secret: ["deriveKey", "deriveBits"] },
    "RSASSA-PKCS1-V1_5": { public: ["verify"], private: ["sign"] },
    "RSA-PSS": { public: ["verify"], private: ["sign"] },
    "RSA-OAEP": { public: ["encrypt", "wrapKey"], private: ["decrypt", "unwrapKey"] },
    "ECDSA": { public: ["verify"], private: ["sign"] },
    "ECDH": { public: [], private: ["deriveKey", "deriveBits"] },
    "ED25519": { public: ["verify"], private: ["sign"] },
    "NODE-ED25519": { public: ["verify"], private: ["sign"] },
    "X25519": { public: [], private: ["deriveKey", "deriveBits"] },
  };

  // The name a key reports and the error messages use: `RSASSA-PKCS1-v1_5`,
  // not the uppercased lookup spelling.
  function _reportedAlgorithmName(name) {
    return _ASYM_NAMES[name] ?? name;
  }

  // Validates a key's requested usages for `type`, returning the deduplicated
  // list. `verb` is "import" or "generate"; workerd phrases both.
  function _checkedUsages(name, type, usages, verb, allowEmpty = false) {
    const reported = _reportedAlgorithmName(name);
    const allowed = (_KEY_USAGES[name] ?? {})[type] ?? [];
    for (const usage of usages) {
      if (!allowed.includes(usage)) {
        const subject = verb === "generate"
          ? reported
          : `${type} ${reported}`;
        throw new DOMException(
          `Attempt to ${verb} ${subject} key with invalid usage "${usage}".`,
          "SyntaxError",
        );
      }
    }
    // The stored list follows the algorithm's own usage order, not the
    // caller's: workerd reports `["encrypt", "decrypt", "wrapKey",
    // "unwrapKey"]` for any input ordering of those four.
    const kept = allowed.filter((usage) => usages.includes(usage));
    // A secret or private key with no usage could never be used, so Web Crypto
    // refuses to mint one; a public key may legitimately carry none.
    if (!allowEmpty && type !== "public" && kept.length === 0) {
      throw new DOMException(
        "Secret/private CryptoKeys must have at least one usage.",
        "SyntaxError",
      );
    }
    return kept;
  }

  // generateKey's asymmetric form: every usage must belong to one of the two
  // halves, and the private half must keep at least one (workerd's own error,
  // not the generic empty-usages one).
  function _generateAsymmetricUsages(name, usages) {
    const reported = _reportedAlgorithmName(name);
    const table = _KEY_USAGES[name] ?? {};
    const publicAllowed = table.public ?? [];
    const privateAllowed = table.private ?? [];
    for (const usage of usages) {
      if (!publicAllowed.includes(usage) && !privateAllowed.includes(usage)) {
        throw new DOMException(
          `Attempt to generate ${reported} key with invalid usage "${usage}".`,
          "SyntaxError",
        );
      }
    }
    if (!usages.some((usage) => privateAllowed.includes(usage))) {
      throw new DOMException(
        "Attempt to generate asymmetric keys with no valid private key usages.",
        "SyntaxError",
      );
    }
    return {
      publicUsages: publicAllowed.filter((usage) => usages.includes(usage)),
      privateUsages: privateAllowed.filter((usage) => usages.includes(usage)),
    };
  }

  // The two checks every keyed operation makes, in workerd's order: the
  // requested algorithm must name the key's own, then the key must list the
  // operation's usage. A key used beyond its declared usages is the security
  // bug this guards (issue #197).
  // `NODE-ED25519` is Cloudflare's pre-standard spelling of `Ed25519`; the
  // two name one algorithm for the key-match check (the reported name keeps
  // the caller's spelling).
  function _canonicalAlgorithmName(name) {
    const upper = String(name ?? "").toUpperCase();
    return upper === "NODE-ED25519" ? "ED25519" : upper;
  }

  function _checkKeyUsage(key, requestedName, usage) {
    const slots = _keySlots(key);
    const keyName = String(slots.algorithm?.name ?? "");
    if (
      _canonicalAlgorithmName(requestedName) !==
        _canonicalAlgorithmName(keyName)
    ) {
      throw new DOMException(
        `Requested algorithm "${requestedName}" does not match this ` +
          `CryptoKey's algorithm "${keyName}".`,
        "InvalidAccessError",
      );
    }
    if (!slots.usages.includes(usage)) {
      throw new DOMException(
        `Requested key usage "${usage}" does not match any usage listed in ` +
          "this CryptoKey.",
        "InvalidAccessError",
      );
    }
  }

  function _notExtractable(key) {
    return new DOMException(
      `Attempt to export non-extractable ` +
        `${String(_keySlots(key).algorithm?.name)} key.`,
      "InvalidAccessError",
    );
  }

  function _hashName(hash) {
    return _algorithmName(
      typeof hash === "string" ? hash : hash?.name || "SHA-256",
    );
  }

  function _notSupported(message) {
    return new DOMException(message, "NotSupportedError");
  }

  function _operationError(message) {
    return new DOMException(message, "OperationError");
  }

  function _rsaOaepLabel(algorithm) {
    const label = _toBuf(algorithm?.label ?? new Uint8Array());
    try {
      new TextDecoder("utf-8", { fatal: true }).decode(label);
    } catch {
      throw _notSupported("RSA-OAEP labels must contain UTF-8 bytes");
    }
    return Array.from(label);
  }

  // A key's state lives only in these internal slots. The JS properties are
  // fresh copies built from the slots on every read, so guest code can
  // neither see the material of a non-extractable key nor change what an
  // operation uses by mutating a returned dictionary (issue #197's review).
  const _KEY_SLOTS = new WeakMap();
  const _KEY_CONSTRUCTION = Symbol("CryptoKey construction");

  // Captured for the host storage predicate: a guest may patch the prototype.
  const _KEY_SLOTS_HAS = WeakMap.prototype.has;
  __cello.$$isCryptoKey = (value) =>
    value !== null && typeof value === "object" &&
    Reflect.apply(_KEY_SLOTS_HAS, _KEY_SLOTS, [value]);
  function _keySlots(key) {
    const slots = _KEY_SLOTS.get(key);
    if (slots === undefined) throw new TypeError("Illegal invocation");
    return slots;
  }

  // A detached copy of a key algorithm dictionary. Workerd's own attribute
  // is a mutable decoy: `key.algorithm.name = "bogus"` takes effect on what
  // the object reports, while the operations keep reading the internal slots
  // (measured on 1.20261121.1). The copy is deep so a nested mutation cannot
  // alias the slots, but it is deliberately not frozen.
  function _copyKeyAlgorithm(value) {
    if (ArrayBuffer.isView(value)) return value.slice();
    if (Array.isArray(value)) return value.map(_copyKeyAlgorithm);
    if (value !== null && typeof value === "object") {
      const copy = {};
      for (const [name, member] of Object.entries(value)) {
        copy[name] = _copyKeyAlgorithm(member);
      }
      return copy;
    }
    return value;
  }

  // A plain function, not a class: workerd answers `CryptoKey.call()` with
  // `TypeError: Illegal constructor`, and a class would answer "Class
  // constructor cannot be invoked without 'new'". Only the private symbol
  // performs construction.
  function CryptoKey(token, type, algorithm, extractable, usages, material) {
    if (token !== _KEY_CONSTRUCTION) {
      throw new TypeError("Illegal constructor");
    }
    const slots = {
      type,
      algorithm,
      extractable: Boolean(extractable),
      usages: Object.freeze(Array.from(usages || [])),
      material,
    };
    _KEY_SLOTS.set(this, slots);
    // Workerd's own-property shape: four enumerable, non-writable data
    // members in the order usages, algorithm, extractable, type (#206).
    // The values are mutable copies for reflection only -- every operation
    // reads the slots, so neither reassigning the member nor mutating the
    // copy can change what a key does (#197 review F2).
    Object.defineProperties(this, {
      usages: {
        value: Array.from(slots.usages),
        writable: false, enumerable: true, configurable: true,
      },
      algorithm: {
        value: _copyKeyAlgorithm(slots.algorithm),
        writable: false, enumerable: true, configurable: true,
      },
      extractable: {
        value: slots.extractable,
        writable: false, enumerable: true, configurable: true,
      },
      type: {
        value: slots.type,
        writable: false, enumerable: true, configurable: true,
      },
    });
  }
  Object.defineProperty(CryptoKey, "length", { value: 0 });
  Object.defineProperty(CryptoKey.prototype, Symbol.toStringTag, {
    value: "CryptoKey", configurable: true, writable: true,
    enumerable: false,
  });

  function _makeKey(type, algorithm, extractable, usages, material) {
    return new CryptoKey(
      _KEY_CONSTRUCTION, type, algorithm, extractable, usages, material);
  }

  // Web Crypto reports a key's algorithm as its *KeyAlgorithm dictionary,
  // built from the parsed key, not as the request the caller passed. An
  // RSA key carries RsaHashedKeyAlgorithm: the modulus length, the public
  // exponent as bytes, and the hash as `{ name }`. An EC key carries its
  // curve. A JOSE library reads `algorithm.hash.name` and `modulusLength`
  // to validate an RS256 key before it ever calls verify(), so a key that
  // echoes the request's hash string fails with a TypeError. The host
  // parses every asymmetric key, so its details are the source here.
  //
  // `algorithm` is the caller's request, or null when there is none -- a
  // node:crypto KeyObject crossing to Web Crypto has only the parsed key.
  // RSA is the one algorithm whose hash the key itself does not carry, so
  // the request is the only source for it and a missing one means SHA-256.
  function _keyAlgorithm(name, algorithm, keyType, details) {
    const reported = { name: _ASYM_NAMES[_algorithmName(name)] ?? name };
    if (keyType === "rsa") {
      // The exponent is reported as a Uint8Array, which is what
      // `crypto_preserve_public_exponent` fixed upstream -- an ArrayBuffer
      // here is the bug that flag names. The host sends a decimal string,
      // because JSON cannot carry the BigInt the exponent can reach.
      const exponent = [];
      for (let n = BigInt(details.publicExponent); n; n >>= 8n) {
        exponent.unshift(Number(n & 255n));
      }
      reported.modulusLength = details.modulusLength;
      reported.publicExponent = Uint8Array.from(exponent);
      reported.hash = { name: _hashName(algorithm?.hash) };
    } else if (keyType === "ec") {
      // The host names the curve as node:crypto does; Web Crypto spells it
      // as the caller did, so map it back.
      reported.namedCurve =
        _canonicalEcCurve(details.namedCurve) ?? details.namedCurve;
    } else if (algorithm?.namedCurve !== undefined) {
      // Ed25519 and X25519 have one curve each, so the parsed key carries
      // no curve to report. Cloudflare still puts `namedCurve` on such a
      // key, and a caller that asked with `NODE-ED25519` matches on the
      // spelling it used, so the request's curve passes through.
      reported.namedCurve = algorithm.namedCurve;
    }
    return reported;
  }

  function _extra(operation, input) {
    return JSON.parse(__crypto_operation(operation, JSON.stringify(input)));
  }

  // The AES modes the host ops accept. `encrypt` and `decrypt` both test
  // against this one set, so neither can grow a mode the other refuses.
  const _AES_MODES = new Set(["AES-GCM", "AES-CBC", "AES-CTR"]);
  // SP 800-38D and the Web Crypto specification name these seven tag
  // lengths; workerd accepts exactly them (measured), including 32 and 64
  // bit, which the previous list and the AEAD type omitted.
  const _AES_GCM_TAG_LENGTHS = new Set([32, 64, 96, 104, 112, 120, 128]);
  const _RSA_OAEP_HASHES = new Set([
    "SHA-1", "SHA-256", "SHA-384", "SHA-512",
  ]);

  // All three modes take the same typed-array host ops: the key, the IV or
  // counter block, and the data are all bytes, so nothing here crosses as
  // JSON. CBC is PKCS#7-padded and CTR is its own inverse, so `encrypting`
  // only matters for CBC.
  //
  // AES-CTR names its IV `counter`, and that block belongs to the caller.
  // The host copies every view it is given, so nothing on this path can
  // increment the caller's block in place.
  //
  // AES-GCM reports a failure by returning nothing, and this turns that into
  // the `OperationError` the Web Crypto specification names. The block modes
  // throw from the host with the cause, so they never reach that branch.
  // Web IDL converts the algorithm dictionary before any other parameter, so
  // a missing or mistyped member is reported before the key is touched. The
  // texts are workerd's.
  function _aesValidate(name, algorithm, encrypting) {
    const field = name === "AES-CTR" ? "counter" : "iv";
    if (
      algorithm === null || algorithm === undefined ||
      algorithm[field] === undefined
    ) {
      throw new TypeError(
        field === "iv"
          ? 'Missing field "iv" in "algorithm".'
          : 'Missing "counter" member in "algorithm".',
      );
    }
    const interfaceName = encrypting ? "EncryptAlgorithm" : "DecryptAlgorithm";
    const checkType = (member) => {
      const value = algorithm[member];
      if (
        value !== undefined && !(value instanceof ArrayBuffer) &&
        !ArrayBuffer.isView(value)
      ) {
        throw new TypeError(
          `Incorrect type for the '${member}' field on '${interfaceName}': ` +
            "the provided value is not of type 'BufferSource'.",
        );
      }
    };
    checkType(field);
    if (name === "AES-GCM") checkType("additionalData");
  }

  function _aes(name, algorithm, key, data, encrypting) {
    const ivBytes = _toBuf(name === "AES-CTR" ? algorithm.counter : algorithm.iv);
    if (name === "AES-GCM" && ivBytes.byteLength === 0) {
      throw new DOMException("AES-GCM IV must not be empty.", "OperationError");
    }
    if (name === "AES-CBC" && ivBytes.byteLength !== 16) {
      throw _operationError(
        `AES-CBC IV must be 16 bytes long (provided ` +
          `${ivBytes.byteLength} bytes).`,
      );
    }
    if (name === "AES-CTR" && ivBytes.byteLength !== 16) {
      throw _operationError(
        "Counter must have length of 16 bytes " +
          `(provided ${ivBytes.byteLength}).`,
      );
    }
    const tagLength = name === "AES-GCM"
      ? Number(algorithm.tagLength ?? 128)
      : 128;
    if (name === "AES-GCM" && !_AES_GCM_TAG_LENGTHS.has(tagLength)) {
      throw _operationError("Invalid AES-GCM tag length " + tagLength + ".");
    }
    const run = encrypting ? _aesEncrypt : _aesDecrypt;
    const out = run(
      name,
      _keySlots(key).material.bytes,
      ivBytes,
      _toBuf(data),
      name === "AES-GCM"
        ? _toBuf(algorithm.additionalData ?? new Uint8Array())
        : new Uint8Array(),
      tagLength / 8,
    );
    if (!out) {
      throw _operationError(
        name + (encrypting ? " encrypt failed" : " decrypt failed"),
      );
    }
    return out.buffer.slice(out.byteOffset, out.byteOffset + out.byteLength);
  }

  class SubtleCrypto {
    get [Symbol.toStringTag]() { return "SubtleCrypto"; }

    async digest(algorithm, data) {
      // Web IDL's union conversion: an omitted argument is not a
      // `string or HashAlgorithm`, while an explicit undefined or null cannot
      // initialize the dictionary, and workerd names the cases differently.
      if (arguments.length === 0) {
        throw new TypeError(
          "Failed to execute 'digest' on 'SubtleCrypto': parameter 1 is not " +
            "of type 'string or HashAlgorithm'.",
        );
      }
      if (algorithm === undefined || algorithm === null) {
        throw new TypeError(
          "Cannot initialize HashAlgorithm with required members from an " +
            "undefined or null value.",
        );
      }
      const name = _algorithmName(algorithm);
      if (!_DIGEST_ALGS.has(name)) {
        throw _notSupported(
          "Unrecognized or unimplemented digest algorithm requested.");
      }
      const out = _digest(name, _toBuf(_requireBufferSource("digest", 2, data)));
      return out.buffer.slice(out.byteOffset, out.byteOffset + out.byteLength);
    }

    async importKey(format, keyData, algorithm, extractable, usages) {
      // Web IDL converts `keyData` (parameter 2) before the algorithm
      // (parameter 3). A buffer-based format needs a BufferSource, `jwk`
      // needs a JsonWebKey; workerd words each case.
      const isBuffer =
        keyData instanceof ArrayBuffer || ArrayBuffer.isView(keyData);
      if (keyData === undefined || keyData === null) {
        throw new TypeError(
          "Cannot initialize JsonWebKey with required members from an " +
            "undefined or null value.",
        );
      }
      if (!isBuffer && typeof keyData !== "object") {
        throw _argumentTypeError(
          "importKey", 2, "BufferSource or JsonWebKey");
      }
      if (!isBuffer && format !== "jwk") {
        throw new TypeError(
          "Import data provided for buffer-based import formats must be a " +
            "buffer source.",
        );
      }
      _requireAlgorithm(algorithm, "ImportKeyAlgorithm");
      const name = _algorithmName(algorithm);
      const requested = _requireUsages("importKey", 5, usages);
      if (!["raw", "spki", "pkcs8", "jwk"].includes(String(format))) {
        throw _notSupported(`Unrecognized key import format "${format}".`);
      }
      // Every import answer goes through this: the usages are validated
      // against the algorithm and the key's type right where the key is
      // built, so no branch can return a key with a usage it cannot honour.
      const make = (type, algorithmDict, extractableValue, material) =>
        _makeKey(
          type, algorithmDict, extractableValue,
          _checkedUsages(name, type, requested, "import"), material,
        );
      if (format === "raw" && (name === "HKDF" || name === "PBKDF2")) {
        // An unusable usage is reported before the extractability rule; a
        // valid one then hits it, because the spec makes KDF secrets
        // non-extractable so the password material cannot come back out.
        const keyUsages = _checkedUsages(
          name, "secret", requested, "import", true);
        if (extractable) {
          throw new DOMException(
            name + " key cannot be extractable.", "SyntaxError");
        }
        if (keyUsages.length === 0) {
          throw new DOMException(
            "Secret/private CryptoKeys must have at least one usage.",
            "SyntaxError",
          );
        }
        return _makeKey(
          "secret", { name }, false, keyUsages,
          { bytes: _toBuf(keyData).slice() },
        );
      }
      if (format === "raw" && _SECRET_KEY_ALGS.has(name)) {
        const raw = _toBuf(keyData).slice();
        const normalized = name === "HMAC"
          ? { name, hash: { name: _hashName(algorithm?.hash) }, length: raw.byteLength * 8 }
          : { name, length: raw.byteLength * 8 };
        return make(
          "secret", normalized, extractable, { bytes: raw },
        );
      }
      // A symmetric key's JWK is `{"kty":"oct","k":...}`. `k` carries the
      // bytes, so the import is the raw arm above once decoded; a missing or
      // non-oct `kty` is not a symmetric key at all.
      if (format === "jwk" && _SECRET_KEY_ALGS.has(name)) {
        if (keyData?.kty !== undefined && keyData.kty !== "oct") {
          throw _notSupported("unsupported JWK kty: " + keyData.kty);
        }
        let raw;
        try {
          raw = _b64urlDecode(keyData?.k ?? "");
        } catch {
          throw new DOMException("Invalid JWK oct key", "DataError");
        }
        if (raw.byteLength === 0) {
          throw new DOMException("Invalid JWK oct key", "DataError");
        }
        // AES keys are 128, 192, or 256 bits: any other length has no
        // algorithm to run (W3C WebCrypto). HMAC keys stay unrestricted.
        if (name !== "HMAC" &&
            raw.byteLength !== 16 && raw.byteLength !== 24 && raw.byteLength !== 32) {
          throw new DOMException("Invalid JWK oct key length", "DataError");
        }
        // A present `use` must name this key's operation class: a `sig`
        // JWK is not an AES key that can encrypt (W3C WebCrypto).
        // HMAC keys carry no such restriction here.
        if (name !== "HMAC" && keyData?.use !== undefined && keyData.use !== "enc") {
          throw new DOMException("JWK use mismatch: " + keyData.use, "DataError");
        }
        // `ext: false` marks a non-extractable key: importing it as
        // extractable would mint an exportable key the JWK forbids. Absent
        // `ext` carries no such promise, so only an explicit `false`
        // conflicts.
        if (keyData?.ext === false && extractable) {
          throw new DOMException(
            "JWK ext false conflicts with extractable true", "DataError");
        }
        // A present `alg` must name this algorithm: an `A128KW` JWK is not
        // an AES-GCM key that can encrypt. `_secretJwkAlg` is the same
        // table export uses, so an absent `alg` stays valid and a matching
        // one imports; anything else (including an `alg` for a length with
        // no JOSE name) is a mismatch.
        if (keyData?.alg !== undefined) {
          const expected = _secretJwkAlg(
            name, raw, _hashName(algorithm?.hash));
          if (keyData.alg !== expected) {
            throw new DOMException(
              "JWK alg mismatch: " + keyData.alg, "DataError");
          }
        }
        const normalized = name === "HMAC"
          ? { name, hash: { name: _hashName(algorithm?.hash) }, length: raw.byteLength * 8 }
          : { name, length: raw.byteLength * 8 };
        return make(
          "secret", normalized, extractable, { bytes: raw },
        );
      }
      // An EC public key's `raw` form is its SEC1 point, uncompressed or
      // compressed. Like the Secure Curves arm below, it is checked before
      // the asymmetric block, which would read the point as a DER document
      // and can only report a parse failure for it.
      if (format === "raw" && (name === "ECDH" || name === "ECDSA")) {
        const curve = _canonicalEcCurve(algorithm?.namedCurve);
        if (curve === undefined) {
          throw _notSupported("unsupported curve: " + algorithm?.namedCurve);
        }
        const imported = _extra("asym-key-import", {
          key: Array.from(_toBuf(keyData)),
          format: "raw",
          type: "ec",
          namedCurve: curve,
          visibility: "public",
          passphrase: null,
        });
        return make(
          "public",
          _keyAlgorithm(name, algorithm, imported.keyType, imported.details),
          extractable,
          {
            bytes: Uint8Array.from(imported.der),
            keyType: imported.keyType,
            details: imported.details,
          },
        );
      }
      // A Secure Curves public key has a `raw` form: the 32-byte point on
      // its own. It is checked before the asymmetric block below, because
      // that block reads `raw` bytes as a DER document and can only report a
      // parse failure for them.
      if (format === "raw" && _OKP_CURVES[name] !== undefined) {
        const imported = _extra("asym-key-import", {
          key: Array.from(_toBuf(keyData)),
          format: "raw",
          type: _OKP_CURVES[name],
          visibility: "public",
          passphrase: null,
        });
        return make(
          "public",
          _keyAlgorithm(name, algorithm, imported.keyType, imported.details),
          extractable,
          {
            bytes: Uint8Array.from(imported.der),
            keyType: imported.keyType,
            details: imported.details,
          },
        );
      }
      // A JWK's `alg` names the algorithm it was made for. Anything that is
      // not a string is not an algorithm name, and importing it would leave
      // a key claiming to be something it cannot be. Web Crypto ignores a
      // mismatched `alg` on EC keys, so only the type is checked here.
      if (format === "jwk" && keyData?.alg !== undefined &&
          typeof keyData.alg !== "string") {
        throw new DOMException(
          `Unrecognized or unimplemented algorithm "${String(keyData.alg)}"`,
          "NotSupportedError",
        );
      }
      // Asymmetric keys go through the same host import node:crypto uses, so
      // the CryptoKey carries the key type and details beside its bytes.
      // KeyObject.from() then sees a real asymmetric key rather than opaque
      // material, and the key is validated at import instead of at first use.
      if (
        (format === "spki" || format === "pkcs8" ||
          format === "jwk") &&
        _ASYM_ALGS.has(name)
      ) {
        const jwk = format === "jwk";
        const visibility =
          format === "pkcs8" || (jwk && keyData?.d !== undefined)
            ? "private"
            : "public";
        const imported = _extra("asym-key-import", {
          key: jwk ? keyData : Array.from(_toBuf(keyData)),
          format: jwk ? "jwk" : "der",
          type: jwk ? null : format,
          visibility,
          passphrase: null,
        });
        return make(
          visibility,
          _keyAlgorithm(name, algorithm, imported.keyType, imported.details),
          extractable,
          {
            bytes: Uint8Array.from(imported.der),
            keyType: imported.keyType,
            details: imported.details,
          },
        );
      }
      throw _notSupported(
        `Unrecognized key import algorithm ` +
          `"${_requestedAlgorithmName(algorithm)}" requested.`);
    }

    async exportKey(format, key) {
      _requireCryptoKey("exportKey", 2, key);
      const slots = _keySlots(key);
      // The `raw` form of a Secure Curves public key is its 32-byte point,
      // and of an EC public key its uncompressed SEC1 point. The branch
      // below answers every `raw` request from the stored bytes, which for
      // an asymmetric key are its SPKI document -- so without this arm an
      // X25519 peer key exports as 44 bytes that no counterpart can read,
      // and an EC key as 91 bytes of SPKI, and nothing reports an error.
      if (format === "raw" && key?.type === "public" &&
          (slots.material?.keyType === "ed25519" ||
            slots.material?.keyType === "x25519" ||
            slots.material?.keyType === "ec")) {
        if (!slots.extractable) {
          throw _notExtractable(key);
        }
        return Uint8Array.from(_extra("asym-key-raw-public", {
          der: Array.from(slots.material.bytes),
        }).bytes).buffer;
      }
      if (format === "raw" && slots.material?.bytes) {
        if (!slots.extractable) {
          throw _notExtractable(key);
        }
        const raw = slots.material.bytes;
        return raw.buffer.slice(raw.byteOffset, raw.byteOffset + raw.byteLength);
      }
      // A symmetric key exports as `{"kty":"oct",...}` from its bytes, with
      // the JOSE `alg` for its algorithm and length. Lengths with no JOSE
      // name export without one rather than with a wrong one.
      if (format === "jwk" && key?.type === "secret" &&
          slots.material?.bytes !== undefined &&
          slots.material?.keyType === undefined) {
        if (!slots.extractable) {
          throw _notExtractable(key);
        }
        const bytes = slots.material.bytes;
        const name = slots.algorithm?.name;
        const jwk = {
          kty: "oct",
          k: _b64urlEncode(bytes),
          key_ops: Array.from(slots.usages),
          ext: true,
        };
        const alg = _secretJwkAlg(
          String(name ?? "").toUpperCase(), bytes,
          _hashName(slots.algorithm?.hash));
        if (alg !== undefined) jwk.alg = alg;
        return jwk;
      }
      if (format === "jwk" && slots.material?.jwk) {
        if (!slots.extractable) {
          throw _notExtractable(key);
        }
        return structuredClone(slots.material.jwk);
      }
      // Asymmetric keys export from their normalized DER: spki and pkcs8 as
      // they are stored, jwk through the host.
      const material = slots.material;
      if (material?.keyType !== undefined) {
        if (!slots.extractable) {
          throw _notExtractable(key);
        }
        const visibility = slots.type;
        if (format === "jwk") {
          return _extra("asym-key-export", {
            der: Array.from(material.bytes),
            visibility,
          }).jwk;
        }
        if ((format === "spki" && visibility === "public") ||
            (format === "pkcs8" && visibility === "private")) {
          const der = material.bytes;
          return der.buffer.slice(der.byteOffset, der.byteOffset + der.byteLength);
        }
      }
      if (!slots.extractable) throw _notExtractable(key);
      throw _notSupported("unsupported key export");
    }

    // Cloudflare's extension, not Web Crypto: a constant-time compare for
    // signatures and MACs, where `===` on a decoded string leaks by timing.
    // Equal lengths are required, so this cannot be used to probe length.
    timingSafeEqual(a, b) {
      const left = _toBuf(a), right = _toBuf(b);
      if (left.byteLength !== right.byteLength) {
        throw new TypeError(
          "Input buffers must have the same byte length.");
      }
      return $$timingSafeEqual(left, right);
    }

    // ECDH. `length` may be null or undefined, a recent spec change: the
    // shared secret is the curve's field size, so there is a right answer
    // without being told one. A shorter length truncates, as the spec says.
    // HKDF and PBKDF2 have no natural output size, so for them a missing
    // or unaligned length is the OperationError the spec names.
    async deriveBits(algorithm, baseKey, length) {
      _requireAlgorithm(algorithm, "DeriveKeyAlgorithm");
      const requested = _requestedAlgorithmName(algorithm);
      _requireCryptoKey("deriveBits", 2, baseKey);
      _checkKeyUsage(baseKey, requested, "deriveBits");
      return this.#deriveBits(algorithm, baseKey, length);
    }

    // The derivation itself, shared by `deriveBits` and `deriveKey` so the
    // latter checks only its own `deriveKey` usage and does not re-require
    // `deriveBits` on the same base key (workerd does not either).
    async #deriveBits(algorithm, baseKey, length) {
      const name = _algorithmName(algorithm);
      if (name === "HKDF" || name === "PBKDF2") {
        const bits = Number(length);
        if (!Number.isInteger(bits) || bits <= 0 || bits % 8 !== 0) {
          throw _operationError(
            `${name} requires a derived key length that is a multiple of ` +
              `eight (requested ${length}).`,
          );
        }
        const hash = _hashName(algorithm?.hash);
        if (!_DIGEST_ALGS.has(hash)) {
          throw _notSupported("unsupported " + name + " hash: " + hash);
        }
        const ikm = _keySlots(baseKey).material.bytes;
        const salt = _toBuf(algorithm?.salt ?? new Uint8Array());
        let out;
        if (name === "HKDF") {
          out = $$hkdf(
            hash, ikm, salt,
            _toBuf(algorithm?.info ?? new Uint8Array()), bits / 8);
          if (!out) {
            throw _operationError("HKDF length is too large for the hash");
          }
        } else {
          const iterations = Number(algorithm?.iterations);
          if (!Number.isInteger(iterations) || iterations <= 0) {
            throw _operationError(
              "PBKDF2 requires a positive iteration count " +
                `(requested ${algorithm?.iterations}).`,
            );
          }
          out = $$pbkdf2(hash, ikm, salt, iterations, bits / 8);
        }
        return out.buffer.slice(
          out.byteOffset, out.byteOffset + out.byteLength);
      }
      if (name !== "ECDH" && name !== "X25519") {
        throw _notSupported("unsupported derive algorithm: " + name);
      }
      const publicKey = algorithm?.public;
      if (!_KEY_SLOTS.has(publicKey) || _keySlots(publicKey).type !== "public") {
        throw new TypeError(name + " requires a public key in algorithm.public");
      }
      // X25519 carries no `namedCurve`, so it cannot ride the curve match
      // `ecdh-derive` runs; its op is separate for that reason alone. Both
      // answer the raw shared secret, so everything below is shared.
      const shared = Uint8Array.from(_extra(
        name === "X25519" ? "x25519-derive" : "ecdh-derive", {
          private: Array.from(_toBuf(_keySlots(baseKey).material.bytes)),
          public: Array.from(_toBuf(_keySlots(publicKey).material.bytes)),
        }).bytes);
      if (length === null || length === undefined) return shared.buffer;
      const bytes = Number(length) / 8;
      if (!Number.isInteger(bytes) || bytes < 0 || bytes > shared.byteLength) {
        throw _operationError("requested length exceeds the derived secret");
      }
      return shared.slice(0, bytes).buffer;
    }

    async deriveKey(algorithm, baseKey, derived, extractable, usages) {
      _requireAlgorithm(algorithm, "DeriveKeyAlgorithm");
      const requested = _requestedAlgorithmName(algorithm);
      _requireCryptoKey("deriveKey", 2, baseKey);
      _checkKeyUsage(baseKey, requested, "deriveKey");
      const name = _algorithmName(derived);
      // An HMAC key with no stated length defaults to its hash's block
      // size, which is what the spec's getKeyLength returns.
      const length = derived?.length ??
        (name === "AES-GCM" || name === "AES-CBC" || name === "AES-CTR" ||
          name === "AES-KW"
          ? 256
          : name === "HMAC"
            ? ({ "SHA-384": 1024, "SHA-512": 1024 }[
              _hashName(derived?.hash)] ?? 512)
            : null);
      const bits = await this.#deriveBits(algorithm, baseKey, length);
      return this.importKey(
        "raw", bits, derived, extractable, usages);
    }

    async generateKey(algorithm, extractable, usages) {
      _requireAlgorithm(algorithm, "GenerateKeyAlgorithm");
      const name = _algorithmName(algorithm);
      const requested = _requireUsages("generateKey", 3, usages);
      if (_SECRET_KEY_ALGS.has(name)) {
        const keyUsages = _checkedUsages(name, "secret", requested, "generate");
        let byteLength;
        if (name === "HMAC") {
          const defaults = {
            "SHA-1": 20,
            "SHA-256": 32,
            "SHA-384": 48,
            "SHA-512": 64,
          };
          const hash = _hashName(algorithm?.hash);
          byteLength = algorithm?.length
            ? Number(algorithm.length) / 8
            : defaults[hash];
          if (!byteLength) throw _notSupported("unsupported HMAC hash: " + hash);
          const raw = new Uint8Array(byteLength);
          crypto.getRandomValues(raw);
          return _makeKey(
            "secret",
            { name, hash: { name: hash }, length: raw.byteLength * 8 },
            extractable,
            keyUsages,
            { bytes: raw },
          );
        }
        byteLength = Number(algorithm?.length || 256) / 8;
        if (name === "AES-KW") {
          if (byteLength !== 16 && byteLength !== 24 && byteLength !== 32) {
            throw new DOMException(
              "AES-KW length must be 128, 192 or 256", "OperationError");
          }
        } else if (byteLength !== 16 && byteLength !== 32) {
          throw new DOMException("AES-GCM length must be 128 or 256", "OperationError");
        }
        const raw = new Uint8Array(byteLength);
        crypto.getRandomValues(raw);
        return _makeKey(
          "secret",
          { name, length: raw.byteLength * 8 },
          extractable,
          keyUsages,
          { bytes: raw },
        );
      }
      // Asymmetric signing keys. `NODE-ED25519` is Cloudflare's pre-standard
      // spelling of Ed25519 and stays as the reported algorithm name, because
      // the caller matched on what it asked for.
      const ASYM_GENERATE = {
        "RSASSA-PKCS1-V1_5": "rsa",
        "RSA-OAEP": "rsa",
        "RSA-PSS": "rsa",
        "ED25519": "ed25519",
        "NODE-ED25519": "ed25519",
        "X25519": "x25519",
        "ECDSA": "ec",
        "ECDH": "ec",
      };
      const kind = ASYM_GENERATE[name];
      if (kind !== undefined) {
        const { publicUsages, privateUsages } =
          _generateAsymmetricUsages(name, requested);
        const options = { type: kind };
        if (kind === "rsa") {
          // Only 3 and 65537 are legal exponents, and cello generates with
          // 65537. Rejecting the rest here is what stops a pathological
          // exponent reaching the prime search at all.
          const raw = algorithm?.publicExponent;
          const bytes = raw ? Array.from(_toBuf(raw)) : [1, 0, 1];
          let exponent = 0;
          for (const byte of bytes) exponent = exponent * 256 + byte;
          if (exponent !== 3 && exponent !== 65537) {
            throw new DOMException(
              `The "publicExponent" must be either 3 or 65537, but got ` +
                `${exponent}.`,
              "OperationError",
            );
          }
          if (exponent !== 65537) {
            throw _notSupported("publicExponent 3 is not implemented");
          }
          options.modulusLength = Number(algorithm?.modulusLength ?? 2048);
          if (
            !Number.isInteger(options.modulusLength) ||
            options.modulusLength < 1024 ||
            options.modulusLength > 16384 ||
            options.modulusLength % 8 !== 0
          ) {
            throw _operationError(
              "RSA modulusLength must be a multiple of 8 from 1024 to 16384",
            );
          }
          const hash = _hashName(algorithm?.hash);
          if (name === "RSA-OAEP" && !_RSA_OAEP_HASHES.has(hash)) {
            throw _notSupported("unsupported RSA-OAEP hash: " + hash);
          }
        }
        if (kind === "ec") {
          const curve = _canonicalEcCurve(algorithm?.namedCurve);
          if (curve === undefined)
            throw _notSupported("unsupported curve: " + algorithm?.namedCurve);
          options.namedCurve = curve;
        }
        const pair = _extra("asym-key-generate", options);
        // A dictionary per half, not one shared between them. The reported
        // hash is what sign() then hashes with, so a caller that mutates
        // `publicKey.algorithm.hash.name` on a shared object would change
        // what the private key signs.
        const half = (der, type, assigned) =>
          _makeKey(type,
            _keyAlgorithm(name, algorithm, pair.keyType, pair.details),
            type === "public" ? true : extractable,
            assigned, {
              bytes: Uint8Array.from(der),
              keyType: pair.keyType,
              details: pair.details,
            });
        return {
          publicKey: half(pair.publicDer, "public", publicUsages),
          privateKey: half(pair.privateDer, "private", privateUsages),
        };
      }
      throw _notSupported(
        `Unrecognized key generation algorithm ` +
          `"${_requestedAlgorithmName(algorithm)}" requested.`);
    }

    async sign(algorithm, key, data) {
      _requireAlgorithm(algorithm, "SignAlgorithm");
      const requested = _requestedAlgorithmName(algorithm);
      _requireCryptoKey("sign", 2, key);
      const bytes = _toBuf(_requireBufferSource("sign", 3, data));
      const name = _algorithmName(algorithm);
      _checkKeyUsage(key, requested, "sign");
      // Web Crypto makes the RSA-PSS salt an explicit parameter on both
      // halves; workerd refuses to sign or verify without it.
      if (name === "RSA-PSS" && algorithm?.saltLength === undefined) {
        throw new DOMException(
          "Failed to provide salt for RSA-PSS key operation which requires " +
            "a salt",
          "DataError",
        );
      }
      if (name === "HMAC") {
        const hash = _hashName(_keySlots(key).algorithm?.hash);
        const sig = _hmacSign(hash, _keySlots(key).material.bytes, bytes);
        if (!sig) throw _operationError("HMAC sign failed");
        return sig.buffer.slice(sig.byteOffset, sig.byteOffset + sig.byteLength);
      }
      const operation = _ED25519_ALGS.has(name)
        ? "ed25519-sign"
        : name === "ECDSA"
          ? "p256-sign"
          : name === "RSASSA-PKCS1-V1_5"
            ? "rsa-pkcs1-sign"
            : name === "RSA-PSS"
              ? "rsa-pss-sign"
              : null;
      if (!operation) throw _notSupported("unsupported sign algorithm: " + name);
      if (name === "ECDSA") {
        if (_canonicalEcCurve(_keySlots(key).algorithm?.namedCurve) !== "P-256") {
          throw _notSupported("ECDSA signatures support only P-256");
        }
        if (_hashName(algorithm?.hash) !== "SHA-256") {
          throw _notSupported("ECDSA P-256 supports only SHA-256");
        }
      }
      const slots = _keySlots(key);
      const result = _extra(operation, {
        key: Array.from(slots.material?.bytes || []),
        data: Array.from(bytes),
        // RSA carries its hash on the key; the others ignore the field.
        hash: _hashName(slots.algorithm?.hash),
        saltLength: Number(algorithm?.saltLength),
      });
      return Uint8Array.from(result.bytes).buffer;
    }

    async verify(algorithm, key, signature, data) {
      _requireAlgorithm(algorithm, "VerifyAlgorithm");
      const requested = _requestedAlgorithmName(algorithm);
      _requireCryptoKey("verify", 2, key);
      const signatureBytes = _toBuf(_requireBufferSource("verify", 3, signature));
      const dataBytes = _toBuf(_requireBufferSource("verify", 4, data));
      const name = _algorithmName(algorithm);
      _checkKeyUsage(key, requested, "verify");
      if (name === "RSA-PSS" && algorithm?.saltLength === undefined) {
        throw new TypeError(
          "Failed to provide salt for RSA-PSS key operation which requires " +
            "a salt",
        );
      }
      if (name === "HMAC") {
        return _hmacVerify(
          _hashName(_keySlots(key).algorithm?.hash),
          _keySlots(key).material.bytes,
          signatureBytes,
          dataBytes,
        );
      }
      // Ed25519 signs through `ed25519-sign` above, so a key pair that can
      // be generated and signed with had no way to check its own signature
      // until this arm existed (denoland/celld#221). The host op is the one
      // node:crypto's `verify()` already calls.
      const operation = _ED25519_ALGS.has(name)
        ? "ed25519-verify"
        : name === "ECDSA"
        ? "p256-verify"
        : name === "RSASSA-PKCS1-V1_5"
        ? "rsa-pkcs1-verify"
        : name === "RSA-PSS"
        ? "rsa-pss-verify"
        : null;
      if (!operation) {
        throw _notSupported("unsupported verify algorithm: " + name);
      }
      const slots = _keySlots(key);
      const material = slots.material?.bytes;
      if (!material) throw _notSupported("verify needs an spki public key");
      if (name === "ECDSA") {
        if (_canonicalEcCurve(_keySlots(key).algorithm?.namedCurve) !== "P-256") {
          throw _notSupported("ECDSA signatures support only P-256");
        }
        if (_hashName(algorithm?.hash) !== "SHA-256") {
          throw _notSupported("ECDSA P-256 supports only SHA-256");
        }
      }
      return _extra(operation, {
        key: Array.from(material),
        data: Array.from(dataBytes),
        signature: Array.from(signatureBytes),
        // ECDSA carries its hash on the call, RSASSA on the key.
        hash: _hashName(
          name === "ECDSA" ? algorithm?.hash : slots.algorithm?.hash,
        ),
        // RSA-PSS verification is bound to the salt length the caller names;
        // a signature whose salt differs must not verify.
        saltLength: name === "RSA-PSS" ? Number(algorithm?.saltLength) : 0,
      }).ok;
    }

    async encrypt(algorithm, key, data) {
      _requireAlgorithm(algorithm, "EncryptAlgorithm");
      _requireCryptoKey("encrypt", 2, key);
      const bytes = _toBuf(_requireBufferSource("encrypt", 3, data));
      const requested = _requestedAlgorithmName(algorithm);
      const name = _algorithmName(algorithm);
      _checkKeyUsage(key, requested, "encrypt");
      if (_AES_MODES.has(name)) _aesValidate(name, algorithm, true);
      return this.#encrypt(algorithm, key, bytes);
    }

    // The encrypted primitive, shared by `encrypt` and `wrapKey`: wrapping
    // checks the `wrapKey` usage, so it cannot re-enter the public method and
    // be refused for lacking `encrypt` on the same key.
    async #encrypt(algorithm, key, bytes) {
      const name = _algorithmName(algorithm);
      if (_AES_MODES.has(name)) {
        return _aes(name, algorithm, key, bytes, true);
      }
      if (name === "RSA-OAEP") {
        const hash = _hashName(_keySlots(key).algorithm?.hash);
        if (!_RSA_OAEP_HASHES.has(hash)) {
          throw _notSupported("unsupported RSA-OAEP hash: " + hash);
        }
        const result = _extra("rsa-oaep-encrypt", {
          key: Array.from(_keySlots(key).material?.bytes || []),
          data: Array.from(bytes),
          hash,
          label: _rsaOaepLabel(algorithm),
        });
        return Uint8Array.from(result.bytes).buffer;
      }
      throw _notSupported("unsupported encrypt algorithm: " + name);
    }

    // wrapKey and unwrapKey compose the primitives exactly the way the
    // spec defines them: export + encrypt, and decrypt + import. A
    // jwk-format key crosses as its JSON bytes.
    //
    // AES-KW wraps rather than encrypts: the primitive is RFC 3394 through
    // the host op, not one of the `_AES_MODES`. A host failure is the
    // `OperationError` the spec names - a short KEK or corrupt input - not
    // the host's own error shape.
    async wrapKey(format, key, wrappingKey, wrapAlgorithm) {
      _requireAlgorithm(wrapAlgorithm, "EncryptAlgorithm");
      _requireCryptoKey("wrapKey", 2, key);
      _requireCryptoKey("wrapKey", 3, wrappingKey);
      const requested = _requestedAlgorithmName(wrapAlgorithm);
      const name = _algorithmName(wrapAlgorithm);
      _checkKeyUsage(wrappingKey, requested, "wrapKey");
      if (_AES_MODES.has(name)) _aesValidate(name, wrapAlgorithm, true);
      const exported = await this.exportKey(format, key);
      const raw = _toBuf(
        format === "jwk"
          ? new TextEncoder().encode(JSON.stringify(exported))
          : exported,
      );
      if (name === "AES-KW") {
        if (_keySlots(wrappingKey).type !== "secret") {
          throw new DOMException(
            "AES-KW wrapping key must be a secret key", "InvalidAccessError");
        }
        try {
          const out = _extra("aes-kw-wrap", {
            key: Array.from(_keySlots(wrappingKey).material.bytes),
            data: Array.from(raw),
          });
          return Uint8Array.from(out.bytes).buffer;
        } catch {
          throw _operationError("AES-KW wrapKey failed");
        }
      }
      return this.#encrypt(wrapAlgorithm, wrappingKey, raw);
    }

    async unwrapKey(
      format, wrapped, unwrappingKey, unwrapAlgorithm,
      algorithm, extractable, usages,
    ) {
      _requireAlgorithm(unwrapAlgorithm, "DecryptAlgorithm");
      const wrappedBytes = _toBuf(_requireBufferSource("unwrapKey", 2, wrapped));
      _requireCryptoKey("unwrapKey", 3, unwrappingKey);
      const requested = _requestedAlgorithmName(unwrapAlgorithm);
      const name = _algorithmName(unwrapAlgorithm);
      _checkKeyUsage(unwrappingKey, requested, "unwrapKey");
      if (_AES_MODES.has(name)) _aesValidate(name, unwrapAlgorithm, false);
      if (name === "AES-KW") {
        if (_keySlots(unwrappingKey).type !== "secret") {
          throw new DOMException(
            "AES-KW unwrapping key must be a secret key",
            "InvalidAccessError");
        }
        let raw;
        try {
          const out = _extra("aes-kw-unwrap", {
            key: Array.from(_keySlots(unwrappingKey).material.bytes),
            data: Array.from(wrappedBytes),
          });
          raw = Uint8Array.from(out.bytes);
        } catch {
          throw _operationError("AES-KW unwrapKey failed");
        }
        const keyData = format === "jwk"
          ? JSON.parse(new TextDecoder().decode(raw))
          : raw.buffer.slice(raw.byteOffset, raw.byteOffset + raw.byteLength);
        return this.importKey(format, keyData, algorithm, extractable, usages);
      }
      const raw = await this.#decrypt(unwrapAlgorithm, unwrappingKey, wrappedBytes);
      const keyData = format === "jwk"
        ? JSON.parse(new TextDecoder().decode(raw))
        : raw;
      return this.importKey(format, keyData, algorithm, extractable, usages);
    }

    async decrypt(algorithm, key, data) {
      _requireAlgorithm(algorithm, "DecryptAlgorithm");
      _requireCryptoKey("decrypt", 2, key);
      const bytes = _toBuf(_requireBufferSource("decrypt", 3, data));
      const requested = _requestedAlgorithmName(algorithm);
      const name = _algorithmName(algorithm);
      _checkKeyUsage(key, requested, "decrypt");
      if (_AES_MODES.has(name)) _aesValidate(name, algorithm, false);
      return this.#decrypt(algorithm, key, bytes);
    }

    async #decrypt(algorithm, key, bytes) {
      const name = _algorithmName(algorithm);
      if (_AES_MODES.has(name)) {
        return _aes(name, algorithm, key, bytes, false);
      }
      if (name === "RSA-OAEP") {
        const hash = _hashName(_keySlots(key).algorithm?.hash);
        if (!_RSA_OAEP_HASHES.has(hash)) {
          throw _notSupported("unsupported RSA-OAEP hash: " + hash);
        }
        const result = _extra("rsa-oaep-decrypt", {
          key: Array.from(_keySlots(key).material?.bytes || []),
          data: Array.from(bytes),
          hash,
          label: _rsaOaepLabel(algorithm),
        });
        return Uint8Array.from(result.bytes).buffer;
      }
      throw _notSupported("unsupported decrypt algorithm: " + name);
    }
  }

  const subtle = new SubtleCrypto();
  const crypto = {
    getRandomValues(array) {
      // Web IDL brand check, observable via node:crypto's webcrypto alias.
      if (this !== crypto) throw new TypeError("Illegal invocation");
      if (!(array instanceof ArrayBuffer) && !ArrayBuffer.isView(array)) {
        throw new TypeError(
          "Failed to execute 'getRandomValues' on 'Crypto': parameter 1 is " +
            "not of type 'ArrayBufferView'.",
        );
      }
      if (
        !(array instanceof Int8Array) &&
        !(array instanceof Uint8Array) &&
        !(array instanceof Uint8ClampedArray) &&
        !(array instanceof Int16Array) &&
        !(array instanceof Uint16Array) &&
        !(array instanceof Int32Array) &&
        !(array instanceof Uint32Array) &&
        !(array instanceof BigInt64Array) &&
        !(array instanceof BigUint64Array)
      ) {
        throw new DOMException(
          "ArrayBufferView argument to getRandomValues() must be an " +
            "integer-typed view.",
          "TypeMismatchError",
        );
      }
      if (array.byteLength > 65536) {
        throw new DOMException(
          "getRandomValues() only accepts buffers of size <= 64K but " +
            `provided ${array.byteLength} bytes.`,
          "QuotaExceededError",
        );
      }
      _randomValues(array);
      return array;
    },

    randomUUID() {
      const bytes = new Uint8Array(16);
      _randomValues(bytes);
      bytes[6] = (bytes[6] & 0x0f) | 0x40;
      bytes[8] = (bytes[8] & 0x3f) | 0x80;
      const h = (index) => bytes[index].toString(16).padStart(2, "0");
      return h(0) + h(1) + h(2) + h(3) + "-" +
        h(4) + h(5) + "-" + h(6) + h(7) + "-" +
        h(8) + h(9) + "-" + h(10) + h(11) +
        h(12) + h(13) + h(14) + h(15);
    },

    subtle,
    get [Symbol.toStringTag]() { return "Crypto"; },
  };

  // Cloudflare's DigestStream: a WritableStream that hashes what is written
  // to it and resolves `digest` when the stream closes. Not Web Crypto, and
  // the reason a Worker can hash a body it never has to hold whole -- though
  // Cello buffers here, as its Hash already does.
  class DigestStream extends WritableStream {
    // The digest promise and the hashing state live in a private slot: a
    // DigestStream is a Web API object, so a Worker holding one sees the
    // WritableStream surface plus `digest`/`bytesWritten` and nothing else
    // (#170, #206). Workerd reflects both as prototype accessors.
    #s;
    constructor(algorithm) {
      const name = _algorithmName(algorithm);
      // DigestStream takes the CRC checksums too; `subtle.digest` does not.
      if (!_DIGEST_ALGS.has(name) && !_CRC_ALGS.has(name)) {
        throw _notSupported("unsupported digest algorithm: " + name);
      }
      const state = { chunks: [], written: 0, resolve: null, reject: null };
      const digest = new Promise((resolve, reject) => {
        state.resolve = resolve;
        state.reject = reject;
      });
      super({
        write(chunk) {
          // A string is written as its UTF-8 bytes, as workerd does; every
          // other chunk must already be binary.
          if (typeof chunk !== "string" && !ArrayBuffer.isView(chunk) &&
              !(chunk instanceof ArrayBuffer)) {
            throw new TypeError(
              "DigestStream is a byte stream but received an object of " +
              "non-ArrayBuffer/ArrayBufferView/string type on its " +
              "writable side.");
          }
          const bytes = typeof chunk === "string"
            ? new TextEncoder().encode(chunk)
            : _toBuf(chunk);
          state.chunks.push(bytes);
          state.written += bytes.byteLength;
        },
        close() {
          const joined = new Uint8Array(state.written);
          let offset = 0;
          for (const chunk of state.chunks) {
            joined.set(chunk, offset);
            offset += chunk.byteLength;
          }
          state.chunks.length = 0;
          const out = _digest(name, joined);
          state.resolve(
            out.buffer.slice(out.byteOffset, out.byteOffset + out.byteLength));
        },
        abort(reason) {
          state.chunks.length = 0;
          state.reject(reason);
        },
      });
      // A stream that is written to and never closed is legal, and so is one
      // that is disposed. Neither should raise an unhandled rejection just
      // because nobody awaited `digest`.
      digest.catch(() => {});
      this.#s = { digest, state };
    }
    get digest() { return this.#s.digest; }
    get bytesWritten() { return BigInt(this.#s.state.written); }
    [Symbol.dispose]() {
      const state = this.#s.state;
      if (state.disposed) return; // disposing twice is a no-op
      state.disposed = true;
      state.chunks.length = 0;
      const error = new Error("The DigestStream was disposed.");
      state.reject(error);
      // Error the stream itself, so a later write() rejects with the same
      // reason rather than succeeding into a digest nobody will resolve.
      this.abort(error).catch(() => {});
    }
  }
  __cello.$$webidl(DigestStream, "DigestStream");

  crypto.DigestStream = DigestStream;
  globalThis.DigestStream = DigestStream;
  globalThis.CryptoKey = CryptoKey;
  globalThis.SubtleCrypto = SubtleCrypto;
  for (const ctor of [CryptoKey, SubtleCrypto]) {
    for (const key of Object.getOwnPropertyNames(ctor.prototype)) {
      if (key === "constructor") continue;
      const descriptor = Object.getOwnPropertyDescriptor(ctor.prototype, key);
      if (descriptor === undefined || descriptor.enumerable) continue;
      descriptor.enumerable = true;
      Object.defineProperty(ctor.prototype, key, descriptor);
    }
    Object.defineProperty(ctor.prototype, Symbol.toStringTag, {
      value: ctor.name, configurable: true, writable: true, enumerable: false,
    });
  }
  globalThis.crypto = crypto;
  // `node_crypto.js` hands a parsed key to Web Crypto through
  // `KeyObject.toCryptoKey()`, so it reports the same dictionary. It built
  // a second one by hand, which drifted: it named every EC curve P-256 and
  // left the exponent off an RSA key. The loop below hides this name, and
  // the module is lazy, so it always resolves after this script runs.
  __cello.$$keyAlgorithm = _keyAlgorithm;
  __cello.$$canonicalEcCurve = _canonicalEcCurve;
  // The clone reviver and node:crypto construct and read keys through these;
  // guest modules cannot reach `__cello`, and the slots never leave it.
  __cello.$$makeCryptoKey = _makeKey;
  __cello.$$cryptoKeyMaterial = (key) => _KEY_SLOTS.get(key)?.material;
  __cello.$$isCryptoKey = (key) => _KEY_SLOTS.has(key);
})();
