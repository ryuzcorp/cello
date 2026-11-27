---
title: Maintainers and contributing
description: How Cello is branched, reviewed, and released — and how to run the test suite locally.
---

Cello is agent-run: every change lands through a pull request into `main` (rebase merges only), required checks stay green on both architectures, and an independent review — explicitly diffing `.github/` and `Dockerfile` — records its verdict before merge. `upstream/main` mirrors upstream denoland/celld and carries no patches; upstream fixes are ported by hand. Releases are cut by pushing `v<semver>` tags; tags are never moved or reused.

The full process, including what goes where and the license notices, lives in [`CONTRIBUTING.md`](https://github.com/ryuzcorp/cello/blob/main/CONTRIBUTING.md) at the repo root. This page adds only the local test how-to.

## Retry a failed release run

`publish.yml` publishes the immutable `<version>` image, attests the image and the release files, and only then moves the `beta`/`latest` channel tags and creates the GitHub Release. The `<version>` image is pullable once it is published, but no channel tag moves and no Release appears until both attestations pass, so a failure before them costs nothing user-visible and the same tag is retried:

```sh
gh run rerun <run-id> --failed
```

The retry accepts the already-published `<version>` only when it is exactly the image index stitched from the per-arch digests the first attempt uploaded; a different index is refused, and a published `<version>` or Release is never replaced — for those, cut a new tag. A failure while promoting leaves the channel tags that already moved and no Release; the retry finishes them, and a channel tag that already holds a newer version (a release outranks its own prereleases) is never rolled back. Use "Re-run failed jobs", not a full re-run: the per-arch build refuses an already-published `<version>`. The retry window is 7 days, the retention of the per-arch artifacts a retry re-stitches from.

## Run the test suite locally

Run the suite the way CI runs it:

```sh
cargo test --profile ci --locked
```

If the `esbuild` on your `PATH` is a wrapper script (for example a mise shim), bundling tests fail with a permission error. Point the child at the real binary instead:

```sh
CELLO_ESBUILD=/path/to/real/esbuild cargo test --profile ci --locked
```

The official image needs no knob: it ships the real esbuild at `/usr/local/bin/esbuild`.

Every test spawn goes through the hermetic helper in [`crates/cello/tests/common/mod.rs`](https://github.com/ryuzcorp/cello/blob/main/crates/cello/tests/common/mod.rs). It clears the inherited environment and rebuilds it from `PATH`, `TMPDIR`, a fixture-scoped `HOME`, and `CELLO_ESBUILD` when set — nothing else. A stray `CELLD_*` in your shell makes `cello` refuse to start, which is how the harness proves the child sees nothing ambient. CI runs the suite in a hostile environment — `CELLD_HERMETIC_CANARY=1` set and `CELLO_DEV_STATE` pointed nowhere writable — so a test whose outcome depends on the harness environment, or a spawn that bypasses the cleared environment, fails in CI instead of on your machine.
