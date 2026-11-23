# Workerd's KV namespace is a service that speaks the KV HTTP protocol; the
# in-memory `kv.js` stands in for it. Cello implements the binding natively.
using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "main", worker = (
      modules = [(name = "index.js", esModule = embed "index.js")],
      compatibilityDate = "2026-11-01",
      bindings = [(name = "KV", kvNamespace = "kv")],
    )),
    (name = "kv", worker = (
      modules = [(name = "kv.js", esModule = embed "kv.js")],
      compatibilityDate = "2026-11-01",
    )),
  ],
  sockets = [(name = "http", service = "main")],
);
