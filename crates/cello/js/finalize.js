// Final bootstrap step: runs after every other prelude script, so the
// objects those scripts install (crypto, MessageChannel, MessagePort) exist.
//
// Two jobs, both shape: workerd's `crypto` is a `Crypto` instance whose
// members live on the interface prototype (crypto.js builds a plain object
// with own members), and a handful of globals workerd's V8 build does not
// define must not leak out of cello.
(() => {
  const cryptoObject = globalThis.crypto;
  if (cryptoObject && typeof globalThis.Crypto === "function") {
    const prototype = globalThis.Crypto.prototype;
    const descriptors = Object.getOwnPropertyDescriptors(cryptoObject);
    for (const key of Reflect.ownKeys(descriptors)) {
      if (key === "constructor") continue;
      if (Object.prototype.hasOwnProperty.call(prototype, key)) continue;
      Object.defineProperty(prototype, key, descriptors[key]);
    }
    for (const key of Reflect.ownKeys(cryptoObject)) {
      if (key === "constructor") continue;
      delete cryptoObject[key];
    }
    Object.setPrototypeOf(cryptoObject, prototype);
  }
  // Temporal is a V8 feature workerd's build leaves off; its `crypto`
  // exposes DigestStream but no global does; FileReader and the internal
  // routing error have no workerd counterpart at all.
  delete globalThis.Temporal;
  // V8's Temporal build also hangs an entry point off Date; leaving it makes
  // `if (Date.prototype.toTemporalInstant)` feature detection true.
  delete Date.prototype.toTemporalInstant;
  delete globalThis.DigestStream;
  delete globalThis.FileReader;
  delete globalThis.DurableObjectRoutingError;
})();
