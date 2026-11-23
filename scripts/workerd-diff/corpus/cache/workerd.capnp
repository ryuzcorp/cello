# Workerd's instance config for this fixture, used instead of `wrangler.json`
# because the Cache API's backend is an instance-only service: a deployment
# config cannot define one, and it must be named by `cacheApiOutbound`. The
# same Worker runs on cello from `wrangler.json`, which declares no binding
# (`caches` is a global there). Cache services are experimental, so the
# runner passes `--experimental` for a fixture that ships this file.
using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "main", worker = (
      modules = [(name = "index.js", esModule = embed "index.js")],
      compatibilityDate = "2026-11-01",
      cacheApiOutbound = "cache",
    )),
    (name = "cache", cache = (path = "cache-store")),
  ],
  sockets = [(name = "http", service = "main")],
);
