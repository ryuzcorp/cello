# Contributing to Cello

Cello is run by agents: every change lands through a pull request, and
every PR gets an independent review before merge. There is no human
backstop, so the process is strict on purpose.

## Branching

- PRs target `main`. The repo allows rebase merges only; keep history
  linear and rebase instead of merging `main` into your branch.
- `upstream/main` mirrors upstream denoland/celld and carries no patches. Fixes
  from upstream are ported by hand, one commit per fix, never merged.
- Releases are cut from tags: pushing `v<semver>` runs the release
  pipeline (build, provenance, image publish). The tag must be
  annotated, name the version in `crates/cello/Cargo.toml` exactly, and
  point at a commit on `main` — anything else is refused before the
  build. Never reuse a version, never move a tag.
- Direct pushes, force-pushes, and deletions on protected branches are
  blocked by ruleset.

## When a publish run fails

`publish.yml` publishes the immutable `<version>` image first, then attests the
image and the release files, and only then moves `beta`/`latest` and creates the
GitHub Release (`scripts/publish-release.sh stitch`, the two attestation steps,
`promote`). The `<version>` image is pullable from the moment `stitch` succeeds,
but no channel tag moves and no Release appears until both attestations pass, so
a failure in `stitch` or an attestation costs nothing user-visible and the same
tag is retried:

```sh
gh run rerun <run-id> --failed
```

The retry re-stitches from the per-arch digests the first attempt uploaded and
accepts the already-published `<version>` only when it is exactly that index;
anything else is refused. Both attestations run again, so a digest can end up
with more than one provenance bundle — each verifies on its own. `promote` reads
the image attestation back before it moves the first channel tag, so the
ordering is enforced, not just intended. A failure inside `promote` leaves the
channel tags that already moved (each already the attested index) and no
Release; the retry re-runs those carbon copies and finishes. Channel tags only
move forward (a release outranks its own prereleases), so a retry of an older
version releases that version without rolling `beta`/`latest` back over a newer
one. Use "Re-run failed jobs" (`--failed`), not a full re-run: `push` still
refuses an already-published `<version>`. A published `<version>` and a
published Release are never replaced, so once the Release exists the only
recovery is a new tag — which is why v0.2.6 was cut for v0.2.5 rather than
re-running it.

The retry window is 7 days: that is how long the per-arch artifacts (digests and
tarballs) are kept, and `stitch` refuses to publish without them. The whole
workflow also runs in one concurrency group, so two tags never promote at once.

## Required checks

A PR merges only when its required checks are green — currently the
`test` job (full suite plus clippy in the Dockerfile test stage) on
both architectures — and an independent review is recorded, with every
High and Medium finding fixed and re-verified before merge. The review
must explicitly diff `.github/` and `Dockerfile` on every PR: a PR runs
its own CI, so a patch that neuters its own tests still shows green
checks.

## Testing locally

- Test spawns of the `cello` binary go through the hermetic helper in
  `crates/cello/tests/common/mod.rs`: the inherited environment is
  cleared and rebuilt from `PATH`, `TMPDIR`, a fixture-scoped `HOME`,
  and `CELLO_ESBUILD` when set — nothing else. A stray `CELLD_*` in the
  ambient environment makes `cello` refuse to start, which is how the
  harness proves the child sees nothing ambient; CI runs the suite with
  `CELLD_HERMETIC_CANARY=1` set for exactly this reason.
- If the `esbuild` on your `PATH` is a wrapper script (for example a
  mise shim), bundling tests fail with a permission error. Point the
  child at the real binary instead:
  `CELLO_ESBUILD=/path/to/real/esbuild cargo test --profile release`.
  The Docker test stage needs no knob: it installs the real esbuild at
  `/usr/local/bin/esbuild`.
- Long stream timelines are tested in **virtual time**, not wall time.
  `crates/cello/tests/virtual_clock_conformance.rs` runs the real driver
  in-process on a Tokio runtime built with `start_paused(true)`, so a
  five-minute SSE that must outlive the 30-second background budget, a
  60-second idle body, and a stalled `TransformStream` producer finish in
  milliseconds, reproducibly. Run a scenario with
  `cargo test -p cello --test virtual_clock_conformance <name>`.
  - Every request deadline reads one clock, `asyncrt::mono_now` (a
    `tokio::time::Instant`), or a Tokio timer: handler and background
    budgets, response-stream idle expiry, JS timers. Never add
    `std::time::Instant` to one of those: a paused clock does not move it.
    Some timers are deliberately wall-clock and stay on `std`: the SQL
    statement deadline (`storage.rs`), the egress DNS TTL
    (`egress.rs`), the cache-prune and handoff windows in
    `main.rs`, and the CPU watchdog's per-thread CPU sampler. A virtual
    clock must not stretch or compress those.
  - Each scenario asserts its own wall time (under one second), which is
    the guard against a deadline drifting back onto the real clock.
  - Perturbations are seeded. The tee property in the same file draws
    chunk sizes and drain orders from proptest and prints the seed and
    minimal input on failure. The recorded failing case also has its own
    fixed test, because seeds are random: replay it there.
  - A driven request must be awaited to retire (`retire` in that file), not
    dropped: a detached drive keeps polling its isolate, and its timers,
    into the next scenario.
  - A new stream scenario belongs there when it needs minutes of clock,
    a deterministic interleaving, or a fail-before against a timing bug.

## Differential conformance against workerd

`scripts/workerd-diff/` runs each corpus Worker on
[workerd](https://github.com/cloudflare/workerd), Cloudflare's open-source
runtime, and on the locally built `cello` binary, then diffs a normalized
transcript (status, stable headers, body bytes). Fixtures put anything
order- or timing-sensitive into the body they return: no case depends on
wall time.

```sh
npm --prefix scripts/workerd-diff ci --ignore-scripts
cargo build --profile ci --bin cello
CELLO_BIN=target/ci/cello node scripts/workerd-diff/runner.mjs [case ...]
```

`--ignore-scripts` matters: the `workerd` package's postinstall downloads a
binary of its own choosing, while the lockfile's
`@cloudflare/workerd-<platform>` package already carries the pinned one. The
runner uses that native binary and refuses to start when it is not the version
`package.json` pins.

The gate only lists cases that agree: a divergence is filed as a
`workerd diff:` issue with both transcripts and its case removed from
`cases.mjs` until it passes, so there are no skip lists or expected-diff
allowlists. `workerd` is pinned exactly; bump it in its own PR and expect new
differences. CI runs the suite on amd64 inside the required `test` check.

The cases boot `cello dev`, the development node, so a bug that only appears
behind the production serve path is out of scope here; the release image is
driven by `scripts/release-smoke.sh` instead (long-lived SSE, WebSocket
messages, tee and backpressure at production shapes).

## What to change where

- Rust, Docker, CI, tools, examples: this repo, same PR as its tests.
- Docs: `docs/` is the Blume content folder; see
  [Maintainers](https://cello.ryuz.eu/project/contributing). Build with
  `npm run dev`, gate with `npm run validate`.
- One concern per PR, one commit per patch, with tests. Delete
  throwaway scripts before requesting review.

## License

Apache-2.0. Modified files keep their `Modified by Ryuz Corp (Cello)`
notice, per Apache-2.0 §4(b); `NOTICE` keeps every existing
third-party notice.
