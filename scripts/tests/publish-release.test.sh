#!/usr/bin/env bash
# Tests for `scripts/publish-release.sh` with `docker` and `gh` stubbed on
# PATH. The stub registry keeps tag -> digest in $STUB_STATE/tags and raw
# manifests in $STUB_STATE/raw/, and logs every `imagetools create` to
# $STUB_STATE/create.log, so a case can assert exactly what was written (a
# refusal must write nothing, and a retry must not touch `<version>`).
#
# Each case runs the script in a cleared environment (`env -i`) against its own
# state directory, so no case can see another's registry or release.
# shellcheck disable=SC1090,SC1091 # the sourced files are generated at run time
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
script=$root/scripts/publish-release.sh
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

image=ghcr.io/ryuzcorp/cello
repo=ryuzcorp/cello
sha=0123456789abcdef0123456789abcdef01234567
version=0.2.7
minor=0.2
amd="sha256:$(printf '1%.0s' {1..64})"
arm="sha256:$(printf '2%.0s' {1..64})"

failures=0
fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}
ok() { echo "ok: $*"; }

# --- stub registry -------------------------------------------------------

mkdir -p "$tmp/bin"
cat >"$tmp/bin/registry-lib.sh" <<'LIB'
# The stitched index buildx would print: `version` as the version annotation
# (empty for none), then the digest references it is stitched from.
index_json() {
  local version=${1:-}
  shift
  local out='{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json"'
  [ -z "$version" ] || out+=",\"annotations\":{\"org.opencontainers.image.version\":\"$version\"}"
  out+=',"manifests":['
  local first=1 ref
  for ref in "$@"; do
    [ "$first" = 1 ] || out+=','
    first=0
    out+="{\"mediaType\":\"application/vnd.oci.image.manifest.v1+json\",\"digest\":\"${ref##*@}\",\"size\":1}"
  done
  printf '%s]}\n' "$out"
}
digest_of() {
  local sum
  sum=$(printf '%s' "$1" | sha256sum)
  printf 'sha256:%s' "${sum%% *}"
}
# Tag and digest references both map to one file name.
raw_name() { printf '%s' "$1" | tr '/@:' '___'; }
LIB

cat >"$tmp/bin/docker" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/registry-lib.sh"
state=${STUB_STATE:?}
printf 'docker %s\n' "$*" >>"$state/docker.log"
[ "${1:-}" = buildx ] && shift || { echo "docker stub: unsupported: $*" >&2; exit 1; }
[ "${1:-}" = imagetools ] && shift || { echo "docker stub: unsupported: $*" >&2; exit 1; }
cmd=${1:-}
shift
case $cmd in
  inspect)
    raw=0 fmt="" ref=""
    while [ $# -gt 0 ]; do
      case $1 in
        --raw) raw=1; shift ;;
        --format) fmt=$2; shift 2 ;;
        *) ref=$1; shift ;;
      esac
    done
    digest=$(awk -v t="$ref" '$1 == t { print $2; exit }' "$state/tags" 2>/dev/null || true)
    if [ -z "$digest" ]; then
      echo "ERROR: $ref: not found" >&2
      exit 1
    fi
    if [ "$raw" = 1 ]; then
      cat "$state/raw/$(raw_name "$ref")"
      exit 0
    fi
    case $fmt in
      '{{.Manifest.Digest}}') printf '%s\n' "$digest" ;;
      *) echo "docker stub: unsupported format: $fmt" >&2; exit 1 ;;
    esac
    ;;
  create)
    dry=0 tag="" annotation="" srcs=()
    while [ $# -gt 0 ]; do
      case $1 in
        --dry-run) dry=1; shift ;;
        --tag) tag=$2; shift 2 ;;
        --progress) shift 2 ;;
        --annotation)
          case $2 in
            org.opencontainers.image.version=*) annotation=${2#*=} ;;
            *) echo "docker stub: unsupported annotation: $2" >&2; exit 1 ;;
          esac
          shift 2
          ;;
        *) srcs+=("$1"); shift ;;
      esac
    done
    printf '%s\n' "$(index_json "$annotation" "${srcs[@]}")" >"$state/dry.json"
    if [ "$dry" = 1 ]; then
      cat "$state/dry.json"
      exit 0
    fi
    echo "create $tag <- ${srcs[*]}" >>"$state/create.log"
    if [ "${#srcs[@]}" -eq 1 ]; then
      # A carbon copy of the source index, byte for byte.
      cp "$state/raw/$(raw_name "${srcs[0]}")" "$state/manifest.json"
      digest=${srcs[0]##*@}
    else
      digest=$(digest_of "$(cat "$state/dry.json")")
      cp "$state/dry.json" "$state/manifest.json"
    fi
    # A tag has one target: replace any earlier entry, as the registry does.
    awk -v t="$tag" '$1 != t' "$state/tags" >"$state/tags.next"
    mv "$state/tags.next" "$state/tags"
    printf '%s %s\n' "$tag" "$digest" >>"$state/tags"
    cp "$state/manifest.json" "$state/raw/$(raw_name "$tag")"
    cp "$state/manifest.json" "$state/raw/$(raw_name "${tag%:*}@$digest")"
    ;;
  *) echo "docker stub: unsupported: $cmd $*" >&2; exit 1 ;;
esac
STUB
chmod +x "$tmp/bin/docker"

cat >"$tmp/bin/gh" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
state=${STUB_STATE:?}
printf 'gh %s\n' "$*" >>"$state/gh.log"
case "${1:-}" in
  api)
    case "${2:-}" in
      */releases/tags/*)
        [ ! -f "$state/release" ] || exit 0
        # An injected `gh` failure (auth, outage) must not read as "no release".
        if [ -f "$state/api-error" ]; then
          cat "$state/api-error" >&2
          exit 1
        fi
        echo "gh: Not Found (HTTP 404)" >&2
        exit 1
        ;;
      *) echo "gh stub: unsupported api path: ${2:-}" >&2; exit 1 ;;
    esac
    ;;
  attestation)
    [ "${2:-}" = verify ] || { echo "gh stub: unsupported: $*" >&2; exit 1; }
    printf '%s\n' "$*" >>"$state/attestation.log"
    # The read-back must be bound to this workflow and commit, not just the repo.
    case " $* " in
      *" --signer-workflow ryuzcorp/cello/.github/workflows/publish.yml "*) ;;
      *) echo "gh stub: attestation verify without --signer-workflow" >&2; exit 1 ;;
    esac
    case " $* " in
      *" --source-digest ${GITHUB_SHA:-} "*) ;;
      *) echo "gh stub: attestation verify without this run's --source-digest" >&2; exit 1 ;;
    esac
    [ ! -f "$state/attestation-missing" ] ||
      { echo "no attestations found for the subject" >&2; exit 1; }
    ;;
  release)
    [ "${2:-}" = create ] || { echo "gh stub: unsupported: $*" >&2; exit 1; }
    [ ! -f "$state/release" ] || { echo "release already exists" >&2; exit 1; }
    shift 2
    : >"$state/release"
    printf '%s\n' "$*" >>"$state/release.log"
    while [ $# -gt 0 ]; do
      case $1 in
        --notes-file) cp "$2" "$state/notes.md"; shift 2 ;;
        *) shift ;;
      esac
    done
    ;;
  *) echo "gh stub: unsupported: $*" >&2; exit 1 ;;
esac
STUB
chmod +x "$tmp/bin/gh"

# --- fixtures ------------------------------------------------------------

# One case directory: dist artifacts for `version`, an empty registry and no
# release. `$dir/tag` is the tag the case runs against.
new_case() {
  local dir=$tmp/$1 ver=${2:-$version}
  mkdir -p "$dir/dist" "$dir/raw"
  : >"$dir/tags"
  printf 'v%s' "$ver" >"$dir/tag"
  printf 'amd64 tarball\n' >"$dir/dist/cello-$ver-linux-amd64.tar.gz"
  printf 'arm64 tarball\n' >"$dir/dist/cello-$ver-linux-arm64.tar.gz"
  (
    cd "$dir/dist"
    sha256sum "cello-$ver-linux-amd64.tar.gz" >SHA256SUMS-amd64
    sha256sum "cello-$ver-linux-arm64.tar.gz" >SHA256SUMS-arm64
  )
  printf '%s' "$amd" >"$dir/dist/arch-digest-amd64.txt"
  printf '%s' "$arm" >"$dir/dist/arch-digest-arm64.txt"
  printf '%s' "$dir"
}

# The digest `stitch` publishes for the fixture arch digests.
stitched() {
  . "$tmp/bin/registry-lib.sh"
  digest_of "$(index_json "$version" "$image@$amd" "$image@$arm")"
}

# Publish `digest` as `ref` in the case's registry, with `manifest` as its raw
# bytes (default: the index the fixture stitches).
seed_tag() {
  local dir=$1 ref=$2 digest=$3 manifest=${4:-}
  [ -n "$manifest" ] || manifest=$(. "$tmp/bin/registry-lib.sh"; index_json "$version" "$image@$amd" "$image@$arm")
  printf '%s %s\n' "$ref" "$digest" >>"$dir/tags"
  . "$tmp/bin/registry-lib.sh"
  printf '%s\n' "$manifest" >"$dir/raw/$(raw_name "$ref")"
  printf '%s\n' "$manifest" >"$dir/raw/$(raw_name "${ref%:*}@$digest")"
}

# Publish `version`'s index as the channel `tag`, as an earlier release left it.
seed_channel() {
  local dir=$1 tag=$2 ver=$3 manifest digest
  . "$tmp/bin/registry-lib.sh"
  manifest=$(index_json "$ver" "$image@$amd" "$image@$arm")
  digest=$(digest_of "$manifest")
  seed_tag "$dir" "$image:$tag" "$digest" "$manifest"
}

run() { # dir subcommand [extra env as NAME=value ...]
  local dir=$1 sub=$2
  shift 2
  (
    cd "$dir" || exit 1
    env -i PATH="$tmp/bin:$PATH" HOME="$tmp" TMPDIR="$tmp" \
      STUB_STATE="$dir" IMAGE="$image" GITHUB_REF_NAME="$(cat "$dir/tag")" \
      GH_REPO="$repo" GITHUB_SHA="$sha" GITHUB_OUTPUT="$dir/out" DIST="$dir/dist" \
      "$@" "$script" "$sub"
  ) >"$dir/stdout" 2>"$dir/stderr"
}

out_digest() { sed -n 's/^digest=//p' "$1/out"; }
creates() { [ -f "$1/create.log" ] && cat "$1/create.log" || true; }

# --- cases ---------------------------------------------------------------

case_fresh_stitch_creates_only_the_version_tag() {
  local dir rc=0
  dir=$(new_case fresh-stitch)
  run "$dir" stitch || rc=$?
  [ "$rc" -eq 0 ] || { fail "fresh-stitch: exit $rc: $(cat "$dir/stderr")"; return; }
  [ "$(out_digest "$dir")" = "$(stitched)" ] ||
    fail "fresh-stitch: digest $(out_digest "$dir") is not $(stitched)"
  [ "$(creates "$dir")" = "create $image:$version <- $image@$amd $image@$arm" ] ||
    fail "fresh-stitch: creates were: $(creates "$dir")"
  grep -q 'stitched ' "$dir/stdout" || fail "fresh-stitch: no stitched line"
  ok "fresh stitch publishes only <version>"
}

case_retry_accepts_the_identical_index() {
  local dir rc=0 expected
  dir=$(new_case retry-same)
  expected=$(stitched)
  seed_tag "$dir" "$image:$version" "$expected"
  run "$dir" stitch || rc=$?
  [ "$rc" -eq 0 ] || { fail "retry-same: exit $rc: $(cat "$dir/stderr")"; return; }
  [ "$(out_digest "$dir")" = "$expected" ] || fail "retry-same: digest $(out_digest "$dir")"
  [ -z "$(creates "$dir")" ] || fail "retry-same: rewrote tags: $(creates "$dir")"
  grep -q 'continuing the retry' "$dir/stdout" || fail "retry-same: no retry line"
  ok "retry accepts the identical index without writing"
}

case_retry_accepts_a_reencoded_index() {
  local dir rc=0 expected reencoded digest
  dir=$(new_case retry-reencoded)
  expected=$(stitched)
  # The same index content, pretty-printed by a different buildx: equal after
  # normalization, different bytes and so a different digest.
  reencoded=$(printf '{\n  "annotations": {"org.opencontainers.image.version": "%s"},\n  "mediaType": "application/vnd.oci.image.index.v1+json",\n  "schemaVersion": 2,\n  "manifests": [\n    {"size": 1, "digest": "%s", "mediaType": "application/vnd.oci.image.manifest.v1+json"},\n    {"size": 1, "digest": "%s", "mediaType": "application/vnd.oci.image.manifest.v1+json"}\n  ]\n}' "$version" "$amd" "$arm")
  digest="sha256:$(printf '3%.0s' {1..64})"
  seed_tag "$dir" "$image:$version" "$digest" "$reencoded"
  run "$dir" stitch || rc=$?
  [ "$rc" -eq 0 ] || { fail "retry-reencoded: exit $rc: $(cat "$dir/stderr")"; return; }
  [ "$(out_digest "$dir")" = "$digest" ] || fail "retry-reencoded: digest $(out_digest "$dir")"
  [ -z "$(creates "$dir")" ] || fail "retry-reencoded: rewrote tags: $(creates "$dir")"
  grep -q 're-encoded' "$dir/stdout" || fail "retry-reencoded: no re-encoded line"
  [ "$expected" != "$digest" ] || fail "retry-reencoded: fixture digests must differ"
  ok "retry accepts the same index content re-encoded by buildx"
}

case_retry_refuses_a_different_index() {
  local dir rc=0 other
  dir=$(new_case retry-different)
  . "$tmp/bin/registry-lib.sh"
  other=$(index_json "$version" "$image@$amd" "sha256:$(printf '4%.0s' {1..64})")
  seed_tag "$dir" "$image:$version" "$(digest_of "$other")" "$other"
  run "$dir" stitch || rc=$?
  [ "$rc" -ne 0 ] || { fail "retry-different: expected a refusal"; return; }
  grep -q "refusing: $image:$version already exists" "$dir/stderr" ||
    fail "retry-different: stderr was: $(cat "$dir/stderr")"
  [ -z "$(creates "$dir")" ] || fail "retry-different: rewrote tags: $(creates "$dir")"
  ok "retry refuses a <version> stitched from other digests"
}

case_stitch_refuses_an_existing_release() {
  local dir rc=0
  dir=$(new_case release-exists)
  : >"$dir/release"
  run "$dir" stitch || rc=$?
  [ "$rc" -ne 0 ] || { fail "release-exists: expected a refusal"; return; }
  grep -q 'refusing: release v0.2.7 already exists; cut a new tag' "$dir/stderr" ||
    fail "release-exists: stderr was: $(cat "$dir/stderr")"
  [ -z "$(creates "$dir")" ] || fail "release-exists: wrote: $(creates "$dir")"
  ok "stitch refuses once the release exists"
}

case_stitch_refuses_an_unreadable_release_state() {
  local dir rc=0
  dir=$(new_case release-error)
  printf 'HTTP 401: Bad credentials (https://api.github.com/graphql)\n' >"$dir/api-error"
  run "$dir" stitch || rc=$?
  [ "$rc" -ne 0 ] || { fail "release-error: expected a refusal"; return; }
  grep -q 'refusing: cannot tell whether release v0.2.7 exists' "$dir/stderr" ||
    fail "release-error: stderr was: $(cat "$dir/stderr")"
  [ -z "$(creates "$dir")" ] || fail "release-error: wrote: $(creates "$dir")"
  ok "stitch refuses when the release state cannot be read"
}

case_promote_refuses_an_unreadable_release_state() {
  local dir rc=0 index
  dir=$(new_case promote-release-error)
  index=$(prepare_stitched "$dir") || return
  printf 'HTTP 401: Bad credentials (https://api.github.com/graphql)\n' >"$dir/api-error"
  run "$dir" promote INDEX_DIGEST="$index" || rc=$?
  [ "$rc" -ne 0 ] || { fail "promote-release-error: expected a refusal"; return; }
  grep -q 'refusing: cannot tell whether release v0.2.7 exists' "$dir/stderr" ||
    fail "promote-release-error: stderr was: $(cat "$dir/stderr")"
  [ -z "$(creates "$dir")" ] || fail "promote-release-error: moved: $(creates "$dir")"
  ok "promote refuses when the release state cannot be read"
}

case_stitch_refuses_a_malformed_digest() {
  local dir rc=0
  dir=$(new_case bad-digest)
  printf 'sha256:abc' >"$dir/dist/arch-digest-amd64.txt"
  run "$dir" stitch || rc=$?
  [ "$rc" -ne 0 ] || { fail "bad-digest: expected a refusal"; return; }
  grep -q 'refusing: bad arch digest: sha256:abc' "$dir/stderr" ||
    fail "bad-digest: stderr was: $(cat "$dir/stderr")"
  [ -z "$(creates "$dir")" ] || fail "bad-digest: wrote: $(creates "$dir")"
  ok "stitch refuses a malformed arch digest"
}

case_stitch_refuses_unreadable_registry_state() {
  local dir rc=0
  dir=$(new_case registry-error)
  # A failure that is not a confirmed "not found" must never read as "nothing
  # published yet": an auth error or an outage has to stop the run.
  mkdir -p "$dir/bin"
  cat >"$dir/bin/docker" <<BROKEN
#!/usr/bin/env bash
if [ "\${3:-}" = inspect ]; then
  echo "unauthorized: bad credentials" >&2
  exit 1
fi
exec "$tmp/bin/docker" "\$@"
BROKEN
  chmod +x "$dir/bin/docker"
  (
    cd "$dir" || exit 1
    env -i PATH="$dir/bin:$tmp/bin:$PATH" HOME="$tmp" TMPDIR="$tmp" \
      STUB_STATE="$dir" IMAGE="$image" GITHUB_REF_NAME="$(cat "$dir/tag")" \
      GH_REPO="$repo" GITHUB_SHA="$sha" GITHUB_OUTPUT="$dir/out" DIST="$dir/dist" \
      "$script" stitch
  ) >"$dir/stdout" 2>"$dir/stderr" || rc=$?
  [ "$rc" -ne 0 ] || { fail "registry-error: expected a refusal"; return; }
  grep -q 'refusing: cannot tell whether' "$dir/stderr" ||
    fail "registry-error: stderr was: $(cat "$dir/stderr")"
  [ -z "$(creates "$dir")" ] || fail "registry-error: wrote: $(creates "$dir")"
  ok "stitch refuses when the registry state cannot be read"
}

# The workflow runs stitch, then both attestations, then promote in one job:
# simulate that order and start the promote assertions from an empty create log.
prepare_stitched() {
  local dir=$1 rc=0
  run "$dir" stitch || rc=$?
  if [ "$rc" -ne 0 ]; then
    fail "$(basename "$dir"): stitch exit $rc: $(cat "$dir/stderr")"
    return 1
  fi
  : >"$dir/create.log"
  out_digest "$dir"
}

case_promote_moves_channels_then_releases() {
  local dir rc=0 index
  dir=$(new_case promote-stable)
  index=$(prepare_stitched "$dir") || return
  run "$dir" promote INDEX_DIGEST="$index" || rc=$?
  [ "$rc" -eq 0 ] || { fail "promote-stable: exit $rc: $(cat "$dir/stderr")"; return; }
  [ "$(creates "$dir")" = "create $image:beta <- $image@$index
create $image:$minor <- $image@$index
create $image:latest <- $image@$index" ] ||
    fail "promote-stable: creates were: $(creates "$dir")"
  [ -f "$dir/release" ] || fail "promote-stable: no release created"
  grep -q -- "--title Cello $version" "$dir/release.log" || fail "promote-stable: release title"
  if grep -q -- '--prerelease' "$dir/release.log"; then
    fail "promote-stable: marked a stable release prerelease"
  fi
  grep -q "$index" "$dir/notes.md" || fail "promote-stable: notes lack the index digest"
  [ "$(cat "$dir/attestation.log")" = "attestation verify oci://$image@$index --repo $repo --signer-workflow $repo/.github/workflows/publish.yml --source-digest $sha" ] ||
    fail "promote-stable: attestation read-back was: $(cat "$dir/attestation.log" 2>/dev/null)"
  if grep -q "refusing" "$dir/stderr"; then
    fail "promote-stable: refused: $(cat "$dir/stderr")"
  fi
  ok "promote moves the stable channel tags, then creates the release"
}

case_promote_prerelease_moves_only_beta() {
  local dir rc=0 index
  dir=$(new_case promote-prerelease 0.3.0-beta.1)
  index=$(prepare_stitched "$dir") || return
  run "$dir" promote INDEX_DIGEST="$index" || rc=$?
  [ "$rc" -eq 0 ] || { fail "promote-prerelease: exit $rc: $(cat "$dir/stderr")"; return; }
  [ "$(creates "$dir")" = "create $image:beta <- $image@$index" ] ||
    fail "promote-prerelease: creates were: $(creates "$dir")"
  grep -q -- '--prerelease' "$dir/release.log" || fail "promote-prerelease: not marked prerelease"
  grep -q -- "--title Cello 0.3.0-beta.1" "$dir/release.log" ||
    fail "promote-prerelease: release title"
  ok "promote moves only beta for a prerelease"
}

case_promote_never_rolls_a_channel_back() {
  local dir rc=0 index newer
  dir=$(new_case promote-back)
  index=$(prepare_stitched "$dir") || return
  seed_channel "$dir" beta 0.3.0
  seed_channel "$dir" latest 0.2.5
  newer=$(awk -v t="$image:beta" '$1 == t { print $2 }' "$dir/tags")
  run "$dir" promote INDEX_DIGEST="$index" || rc=$?
  [ "$rc" -eq 0 ] || { fail "promote-back: exit $rc: $(cat "$dir/stderr")"; return; }
  grep -q "not moving $image:beta: it already holds 0.3.0, newer than 0.2.7" "$dir/stdout" ||
    fail "promote-back: no skip line: $(cat "$dir/stdout")"
  [ "$(creates "$dir")" = "create $image:$minor <- $image@$index
create $image:latest <- $image@$index" ] ||
    fail "promote-back: creates were: $(creates "$dir")"
  [ "$(awk -v t="$image:beta" '$1 == t { print $2 }' "$dir/tags")" = "$newer" ] ||
    fail "promote-back: beta moved off the newer version"
  if grep -q -- '--latest=false' "$dir/release.log"; then
    fail "promote-back: latest moved forward, so the release is the latest"
  fi
  ok "promote moves a channel forward, keeps a newer one and still releases"
}

case_promote_releases_even_when_every_channel_is_newer() {
  local dir rc=0 index
  dir=$(new_case promote-all-newer)
  index=$(prepare_stitched "$dir") || return
  seed_channel "$dir" beta 0.3.0
  seed_channel "$dir" "$minor" 0.2.9
  seed_channel "$dir" latest 0.2.9
  run "$dir" promote INDEX_DIGEST="$index" || rc=$?
  [ "$rc" -eq 0 ] || { fail "promote-all-newer: exit $rc: $(cat "$dir/stderr")"; return; }
  [ -z "$(creates "$dir")" ] || fail "promote-all-newer: moved: $(creates "$dir")"
  [ -f "$dir/release" ] || fail "promote-all-newer: no release created"
  grep -q -- '--latest=false' "$dir/release.log" ||
    fail "promote-all-newer: an older version claimed the release page latest"
  ok "an older retry releases its own version without touching a newer channel"
}

case_promote_moves_beta_off_a_same_version_prerelease() {
  local dir rc=0 index
  dir=$(new_case promote-beta-prerelease)
  index=$(prepare_stitched "$dir") || return
  # A release outranks its own prereleases: 0.2.7 beats 0.2.7-beta.2, where a
  # plain `sort -V` would rank the longer string higher and freeze `beta`.
  seed_channel "$dir" beta 0.2.7-beta.2
  run "$dir" promote INDEX_DIGEST="$index" || rc=$?
  [ "$rc" -eq 0 ] || { fail "promote-beta-prerelease: exit $rc: $(cat "$dir/stderr")"; return; }
  if grep -q "not moving" "$dir/stdout"; then
    fail "promote-beta-prerelease: skipped a channel: $(cat "$dir/stdout")"
  fi
  [ "$(creates "$dir")" = "create $image:beta <- $image@$index
create $image:$minor <- $image@$index
create $image:latest <- $image@$index" ] ||
    fail "promote-beta-prerelease: creates were: $(creates "$dir")"
  ok "promote moves beta off a prerelease of the version being released"
}

case_promote_refuses_without_an_image_attestation() {
  local dir rc=0 index
  dir=$(new_case promote-no-attestation)
  index=$(prepare_stitched "$dir") || return
  : >"$dir/attestation-missing"
  run "$dir" promote INDEX_DIGEST="$index" || rc=$?
  [ "$rc" -ne 0 ] || { fail "promote-no-attestation: expected a refusal"; return; }
  grep -q "refusing: no attestation from this workflow for $image@$index at $sha; not moving a channel tag" "$dir/stderr" ||
    fail "promote-no-attestation: stderr was: $(cat "$dir/stderr")"
  [ -z "$(creates "$dir")" ] || fail "promote-no-attestation: moved: $(creates "$dir")"
  ok "promote refuses to move a channel without the image attestation"
}

case_promote_refuses_an_existing_release() {
  local dir rc=0 index
  dir=$(new_case promote-release-exists)
  index=$(prepare_stitched "$dir") || return
  : >"$dir/release"
  run "$dir" promote INDEX_DIGEST="$index" || rc=$?
  [ "$rc" -ne 0 ] || { fail "promote-release-exists: expected a refusal"; return; }
  [ -z "$(creates "$dir")" ] || fail "promote-release-exists: moved: $(creates "$dir")"
  ok "promote refuses once the release exists, before moving anything"
}

case_promote_refuses_a_moved_version_tag() {
  local dir rc=0 index other
  dir=$(new_case promote-moved)
  index=$(prepare_stitched "$dir") || return
  other="sha256:$(printf '5%.0s' {1..64})"
  run "$dir" promote INDEX_DIGEST="$other" || rc=$?
  [ "$rc" -ne 0 ] || { fail "promote-moved: expected a refusal"; return; }
  grep -q "refusing: $image:$version is $index, not the attested $other" "$dir/stderr" ||
    fail "promote-moved: stderr was: $(cat "$dir/stderr")"
  [ -z "$(creates "$dir")" ] || fail "promote-moved: moved: $(creates "$dir")"
  ok "promote refuses a <version> that is not the attested index"
}

case_promote_refuses_a_missing_release_file() {
  local dir rc=0 index
  dir=$(new_case promote-missing-file)
  index=$(prepare_stitched "$dir") || return
  rm -f "$dir/dist/cello-$version-linux-arm64.tar.gz"
  run "$dir" promote INDEX_DIGEST="$index" || rc=$?
  [ "$rc" -ne 0 ] || { fail "promote-missing-file: expected a refusal"; return; }
  grep -q 'refusing: missing release file' "$dir/stderr" ||
    fail "promote-missing-file: stderr was: $(cat "$dir/stderr")"
  [ -z "$(creates "$dir")" ] || fail "promote-missing-file: moved: $(creates "$dir")"
  ok "promote refuses a missing release file before moving a channel tag"
}

# The order is the fix: a release or a moved channel tag before the
# attestations is exactly the failure this pipeline had, and nothing else in
# the repo would notice it.
# The channel guard only moves a tag when the version is at least as new, so
# the comparator itself is checked against the semver cases the pipeline can
# meet -- including the one a `sort -V` gets backwards.
case_channel_order_compares_versions_as_semver() {
  local bad
  # Sourced in a subshell: the script installs its own EXIT trap, and a trap
  # set inside a function does not survive the function's return.
  bad=$(
    . "$script"
    while read -r lower higher; do
      [ -n "$lower" ] || continue
      at_least "$higher" "$lower" || echo "$higher is not at least $lower"
      at_least "$lower" "$higher" && echo "$lower is at least $higher"
    done <<'TABLE'
0.2.6 0.2.7
0.2.7-beta.2 0.2.7
0.2.7-beta.2 0.2.7-beta.10
0.2.7-alpha 0.2.7-beta
0.2.7-beta 0.2.7-beta.1
0.2.7-beta.9 0.2.7-rc.1
0.2.7-9 0.2.7-11
0.2.7-1 0.2.7-alpha
0.2.7-Beta 0.2.7-alpha
0.9.0 0.10.0
0.2.9 0.2.10
1.0.0-rc.1 1.0.0
TABLE
    at_least 0.2.7 0.2.7 || echo "0.2.7 is not at least itself"
  ) || true
  if [ -n "$bad" ]; then
    fail "wrong ordering: $bad"
  fi
  ok "channel ordering follows semver, including releases above their prereleases"
}

case_workflow_attests_before_it_promotes() {
  local workflow=$root/.github/workflows/publish.yml stitch attest_image attest_files promote
  stitch=$(grep -n 'run: scripts/publish-release.sh stitch' "$workflow" | cut -d: -f1)
  attest_image=$(grep -n 'name: Attest image provenance' "$workflow" | cut -d: -f1)
  attest_files=$(grep -n 'name: Attest release files' "$workflow" | cut -d: -f1)
  promote=$(grep -n 'run: scripts/publish-release.sh promote' "$workflow" | cut -d: -f1)
  local marker
  for marker in "$stitch" "$attest_image" "$attest_files" "$promote"; do
    [ -n "$marker" ] || { fail "workflow: a publish step marker is missing"; return; }
  done
  if ! { [ "$stitch" -lt "$attest_image" ] &&
    [ "$attest_image" -lt "$attest_files" ] &&
    [ "$attest_files" -lt "$promote" ]; }; then
    fail "workflow: publish must stitch, attest the image, attest the files, then promote (lines $stitch/$attest_image/$attest_files/$promote)"
  fi
  if grep -q 'gh release create' "$workflow"; then
    fail "workflow: the release must be created by the script, after the attestations"
  fi
  ok "workflow attests the image and the files before it promotes"
}

case_bad_subcommand() {
  local rc=0
  "$script" nonsense >"$tmp/out" 2>"$tmp/err" || rc=$?
  [ "$rc" -eq 2 ] || fail "bad-subcommand: exit $rc"
  grep -q 'usage:' "$tmp/err" || fail "bad-subcommand: no usage"
  ok "an unknown subcommand prints usage"
}

for test in $(declare -F | awk '$3 ~ /^case_/ { print $3 }'); do
  "$test"
done

# Removed here as well as in the EXIT trap: a shell that sources this file
# instead of executing it (which some harnesses do) never runs the trap, and a
# leaked case directory is a bug in its own right.
rm -rf "$tmp"

if [ "$failures" -ne 0 ]; then
  echo "$failures failure(s)" >&2
  exit 1
fi
echo "all publish-release tests passed"
