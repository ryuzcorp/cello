# Workerd's instance config for this fixture, used instead of `wrangler.json`
# because a Worker's own fetch to a loopback origin is refused unless the
# config names a network service that allows private addresses. The same
# Worker runs on cello from `wrangler.json`, whose `vars` carry the helper
# origin; the runner substitutes __HELPER__ here before boot.
using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "main", worker = (
      modules = [(name = "index.js", esModule = embed "index.js")],
      compatibilityDate = "2026-11-01",
      bindings = [(name = "HELPER", text = "__HELPER__"),
                  (name = "EMPTY_BODY", durableObjectNamespace = "NullBodyObject")],
      durableObjectNamespaces = [(className = "NullBodyObject", uniqueKey = "fetchapi-null-body")],
      durableObjectStorage = (inMemory = void),
      globalOutbound = "internet",
    )),
    (name = "internet", network = (
      allow = ["private", "public"],
      tlsOptions = (trustBrowserCas = true),
    )),
  ],
  sockets = [(name = "http", service = "main")],
);
