#!/usr/bin/env bash
# Publishing for `.github/workflows/publish.yml`, split in two so the workflow
# can put the attestations between the immutable image and everything a user
# sees:
#
#   stitch   publish the immutable `<version>` index stitched from this run's
#            validated per-arch digests, or accept the identical index a
#            previous attempt of the same tag already published, then print
#            its digest
#   promote  move the channel tags (`beta`; plus `<major>.<minor>` and `latest`
#            for a stable release) as carbon copies of the attested index, and
#            create the GitHub Release
#
# A failure before `promote` leaves no release and no moved channel tag, so the
# failed run is retried with "Re-run failed jobs" on the same tag instead of a
# new version. The guarantees that survive the retry:
#
#   * `<version>` is never replaced. `stitch` accepts an existing `<version>`
#     only when it is exactly the index this run would stitch from its own
#     arch digests (the same digest, or the same index content when a bumped
#     buildx re-encoded it between attempts); anything else is refused.
#   * a published release is never overwritten: both subcommands refuse once
#     the GitHub Release for the tag exists, and refuse again when its
#     existence cannot be checked.
#   * the channel tags are carbon copies of the attested index, so no channel
#     can point at a manifest the attestations did not cover, and they only
#     ever move forward: a retry of an older version leaves a channel that
#     already holds a newer version alone (it still gets its own image and
#     release).
#
# Environment:
#   IMAGE            image repository (default `ghcr.io/ryuzcorp/cello`)
#   DIST             artifact directory (default `dist`)
#   GITHUB_REF_NAME  the tag: it names the version (`v0.2.7` -> `0.2.7`), so a
#                    retry needs nothing from the first attempt's job outputs;
#                    `gh` reads GH_TOKEN and GH_REPO
#   GITHUB_OUTPUT    where `stitch` writes `digest=` (stdout when unset)
#   stitch only:     DIST/SHA256SUMS-<arch> and DIST/arch-digest-<arch>.txt
#   promote only:    INDEX_DIGEST (the attested index)
set -euo pipefail

image=${IMAGE:-ghcr.io/ryuzcorp/cello}
dist=${DIST:-dist}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

usage() {
  echo "usage: $0 stitch|promote" >&2
  exit 2
}

refuse() {
  echo "refusing: $*" >&2
  exit 1
}

need() {
  local name=$1
  [ -n "${!name:-}" ] || refuse "$name is not set"
}

# `sha256:` plus 64 lowercase hex digits.
digest_ok() {
  case "$1" in
    sha256:*) ;;
    *) return 1 ;;
  esac
  local hex=${1#sha256:}
  [ "${#hex}" -eq 64 ] || return 1
  case "$hex" in
    *[!0-9a-f]*) return 1 ;;
  esac
}

index_media_type_ok() {
  case "$1" in
    application/vnd.oci.image.index.v1+json | \
      application/vnd.docker.distribution.manifest.list.v2+json) return 0 ;;
    *) return 1 ;;
  esac
}

# Sets EXISTING to the digest of `ref`, or to the empty string when the
# registry confirms there is no such tag. Any other failure stops the run: an
# outage or an auth error must never read as "nothing published yet".
EXISTING=""
existing_digest() {
  local ref=$1 out
  EXISTING=""
  if ! out=$(docker buildx imagetools inspect "$ref" --format '{{.Manifest.Digest}}' 2>&1); then
    grep -qiE 'not found|manifest unknown' <<<"$out" ||
      refuse "cannot tell whether $ref exists: $out"
    return 0
  fi
  EXISTING=$(printf '%s' "$out" | tr -d '[:space:]')
  [ -n "$EXISTING" ] || refuse "cannot read the digest of $ref: $out"
  digest_ok "$EXISTING" || refuse "bad digest for $ref: $EXISTING"
}

# The tag names the version; `verify` already proved it matches
# crates/cello/Cargo.toml. Deriving it here rather than passing it from
# `verify` keeps a retry independent of the first attempt's job outputs.
VERSION=""
PRERELEASE=""
MINOR_TAG=""
# The index carries the version it was stitched for, so a retry can tell what a
# channel tag already holds without asking GitHub, and a consumer can read the
# version off the index.
version_annotation=""
version_from_tag() {
  need GITHUB_REF_NAME
  case $GITHUB_REF_NAME in
    v?*) ;;
    *) refuse "GITHUB_REF_NAME is not a v<semver> tag: $GITHUB_REF_NAME" ;;
  esac
  VERSION=${GITHUB_REF_NAME#v}
  version_annotation="org.opencontainers.image.version=$VERSION"
  case $VERSION in
    *-*) PRERELEASE=true ;;
    *) PRERELEASE=false ;;
  esac
  local rest=${VERSION#*.}
  MINOR_TAG=${VERSION%%.*}.${rest%%.*}
}

# The versions this repository publishes carry digits, dots and at most one
# prerelease suffix, which `sort -V` orders as semver does: numeric parts
# numerically, `-beta.2` before `-beta.10`, a prerelease before its release.
at_least() {
  [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -n 1)" = "$1" ]
}

# Sets CHANNEL_VERSION to the version annotation on `tag`, empty when the tag
# does not exist or predates the annotation.
CHANNEL_VERSION=""
channel_version() {
  local tag=$1
  CHANNEL_VERSION=""
  existing_digest "$image:$tag"
  [ -n "$EXISTING" ] || return 0
  CHANNEL_VERSION=$(docker buildx imagetools inspect --raw "$image:$tag" 2>/dev/null |
    jq -r '.annotations["org.opencontainers.image.version"] // empty')
}

# Sets STITCHED to the digest `<version>` gets when stitched from this run's
# arch digests. `imagetools create --dry-run` prints the exact manifest bytes
# it would push followed by one newline, so their sha256 is that digest: the
# retry decision needs no write to the registry. Only a retry calls this, so a
# first release depends on nothing here.
STITCHED=""
stitched_digest() {
  local amd=$1 arm=$2 dry=$work/index.json err=$work/index.err
  if ! docker buildx imagetools create --dry-run --annotation "$version_annotation" \
    --tag "$image:$VERSION" "$image@$amd" "$image@$arm" >"$dry" 2>"$err"; then
    cat "$err" >&2
    refuse "could not compute the index for $image:$VERSION"
  fi
  local media_type
  media_type=$(jq -r '.mediaType // empty' "$dry" 2>/dev/null) || media_type=""
  index_media_type_ok "$media_type" ||
    refuse "imagetools --dry-run printed no image index for $image:$VERSION (media type: ${media_type:-none})"
  printf '%s\n' "$(cat "$dry")" | cmp -s - "$dry" ||
    refuse "imagetools --dry-run output is not the manifest plus one trailing newline"
  local sum
  sum=$(head -c -1 "$dry" | sha256sum)
  STITCHED="sha256:${sum%% *}"
  digest_ok "$STITCHED" || refuse "bad digest: $STITCHED"
}

# Refuses when the GitHub Release for the tag exists, and when its existence
# cannot be established: a transient `gh` failure must never read as "no
# release yet" and let a released version's channel tags move again.
release_guard() {
  local out
  if out=$(gh release view "$GITHUB_REF_NAME" 2>&1); then
    refuse "release $GITHUB_REF_NAME already exists; cut a new tag"
  fi
  grep -qiE 'release not found|not found|http 404' <<<"$out" ||
    refuse "cannot tell whether release $GITHUB_REF_NAME exists: $out"
}

# Whether `ref` holds exactly the index `dry` describes, comparing the parsed
# JSON rather than its bytes: a buildx bump between two attempts of the same
# run can re-encode the same index, which changes its digest but not what
# consumers pull.
same_index_content() {
  local ref=$1 dry=$2 raw=$work/existing.json
  docker buildx imagetools inspect --raw "$ref" >"$raw" 2>/dev/null || return 1
  jq -S . "$dry" >"$work/dry.canon" 2>/dev/null || return 1
  jq -S . "$raw" >"$work/raw.canon" 2>/dev/null || return 1
  cmp -s "$work/dry.canon" "$work/raw.canon"
}

emit_digest() {
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    echo "digest=$1" >>"$GITHUB_OUTPUT"
  else
    echo "digest=$1"
  fi
}

stitch() {
  version_from_tag
  command -v jq >/dev/null 2>&1 || refuse "jq is required"

  # One SHA256SUMS for the release, verified before anything is published.
  local arch
  for arch in amd64 arm64; do
    [ -f "$dist/SHA256SUMS-$arch" ] || refuse "missing release file: $dist/SHA256SUMS-$arch"
    [ -f "$dist/arch-digest-$arch.txt" ] || refuse "missing release file: $dist/arch-digest-$arch.txt"
  done
  cat "$dist"/SHA256SUMS-* >"$dist/SHA256SUMS"
  rm -f "$dist/SHA256SUMS-amd64" "$dist/SHA256SUMS-arm64"
  (cd "$dist" && sha256sum -c SHA256SUMS)

  # The release is created last, and an existing one is never replaced: a
  # re-run after the release was created stops here.
  release_guard

  local amd arm
  amd=$(cat "$dist/arch-digest-amd64.txt")
  arm=$(cat "$dist/arch-digest-arm64.txt")
  digest_ok "$amd" || refuse "bad arch digest: $amd"
  digest_ok "$arm" || refuse "bad arch digest: $arm"

  existing_digest "$image:$VERSION"
  if [ -z "$EXISTING" ]; then
    # First attempt for this tag: stitch from this run's digests, never from a
    # tag, so nothing pushed under another name can slip into the release.
    docker buildx imagetools create --annotation "$version_annotation" \
      --tag "$image:$VERSION" "$image@$amd" "$image@$arm"
    existing_digest "$image:$VERSION"
    [ -n "$EXISTING" ] ||
      refuse "$image:$VERSION is missing right after the push; cut a new tag"
    echo "stitched $image:$VERSION ($EXISTING)"
  else
    # A retry: the index a previous attempt published is accepted only when it
    # is exactly the one this run stitches, so `--dry-run` runs here and not on
    # a first release.
    stitched_digest "$amd" "$arm"
    if [ "$EXISTING" = "$STITCHED" ]; then
      echo "$image:$VERSION already holds this run's index ($EXISTING); continuing the retry"
    elif same_index_content "$image:$VERSION" "$work/index.json"; then
      echo "$image:$VERSION already holds this run's index as $EXISTING, re-encoded by this attempt's buildx; continuing the retry"
    else
      refuse "$image:$VERSION already exists as $EXISTING, but this run stitches $STITCHED from its own arch digests; a published <version> is never replaced, so cut a new tag"
    fi
  fi
  emit_digest "$EXISTING"
}

promote() {
  version_from_tag
  need INDEX_DIGEST
  command -v jq >/dev/null 2>&1 || refuse "jq is required"

  local version=$VERSION index=$INDEX_DIGEST
  digest_ok "$index" || refuse "bad index digest: $index"

  # Everything user-visible happens after the attestations, and the release is
  # created last: a re-run of a partial promote is safe, one after the release
  # is not needed and refuses.
  release_guard

  # `<version>` must still be the index the attestations covered, or the
  # channel tags below would promote an unattested image.
  existing_digest "$image:$version"
  [ "$EXISTING" = "$index" ] ||
    refuse "$image:$version is ${EXISTING:-missing}, not the attested $index"

  # The release files are checked before anything moves, so a missing artifact
  # cannot leave a channel tag promoted without a release.
  local files=("$dist/SHA256SUMS") tarball arch
  [ -f "$dist/SHA256SUMS" ] || refuse "missing release file: $dist/SHA256SUMS"
  for arch in amd64 arm64; do
    tarball="$dist/cello-$version-linux-$arch.tar.gz"
    [ -f "$tarball" ] || refuse "missing release file: $tarball"
    files+=("$tarball")
  done
  (cd "$dist" && sha256sum -c SHA256SUMS)

  local tags=("beta") moved=() tag claim_latest=true
  if [ "$PRERELEASE" != "true" ]; then
    tags+=("$MINOR_TAG" "latest")
  fi
  for tag in "${tags[@]}"; do
    # A retry must never roll a channel back over a newer version: the release
    # that already owns the channel keeps it, and this version still gets its
    # own image and release.
    channel_version "$tag"
    if [ -n "$CHANNEL_VERSION" ] && ! at_least "$version" "$CHANNEL_VERSION"; then
      echo "not moving $image:$tag: it already holds $CHANNEL_VERSION, newer than $version"
      if [ "$tag" = "latest" ]; then
        claim_latest=false
      fi
      continue
    fi
    # A carbon copy of the attested index, not a fresh stitch: a channel tag
    # then cannot differ from `<version>` by a byte.
    docker buildx imagetools create --tag "$image:$tag" "$image@$index"
    existing_digest "$image:$tag"
    [ "$EXISTING" = "$index" ] ||
      refuse "$image:$tag is ${EXISTING:-missing}, not the attested $index"
    moved+=("$tag")
  done
  if [ "${#moved[@]}" -gt 0 ]; then
    echo "moved ${moved[*]} to $index"
  fi

  local notes=$work/notes.md args=()
  if [ "$PRERELEASE" = "true" ]; then
    args+=(--prerelease)
  fi
  if [ "$claim_latest" = false ]; then
    # The releases page's "latest" follows the newest stable version too: an
    # older retry must not claim it.
    args+=(--latest=false)
  fi
  {
    echo "## Cello $version"
    echo
    echo "Image: \`$image:$version\` (\`$index\`)"
    echo "Full changelog: https://cello.ryuz.eu/changelog"
    echo
    echo "Install on Linux (amd64/arm64):"
    echo '```sh'
    echo "curl -fsSL https://cello.ryuz.eu/install.sh | sh"
    echo '```'
    echo
    echo "Verify provenance (needs gh):"
    echo '```sh'
    echo "gh attestation verify oci://$image@$index --repo ryuzcorp/cello"
    echo "gh attestation verify cello-$version-linux-amd64.tar.gz --repo ryuzcorp/cello"
    echo '```'
  } >"$notes"
  gh release create "$GITHUB_REF_NAME" --title "Cello $version" \
    --notes-file "$notes" "${args[@]}" "${files[@]}"

  local summary
  summary=$(
    echo "## cello $version"
    echo "index digest: \`$index\`"
    echo "published \`$image:$version\`"
    for tag in "${moved[@]}"; do
      echo "published \`$image:$tag\`"
    done
    echo "Verify the SLSA provenance attestation:"
    echo '```'
    echo "gh attestation verify oci://$image@$index --repo ryuzcorp/cello"
    echo '```'
    echo "Verify a release tarball attestation:"
    echo '```'
    echo "gh attestation verify dist/cello-$version-linux-amd64.tar.gz --repo ryuzcorp/cello"
    echo '```'
  )
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    printf '%s\n' "$summary" >>"$GITHUB_STEP_SUMMARY"
  else
    printf '%s\n' "$summary"
  fi
}

case "${1:-}" in
  stitch)
    stitch
    ;;
  promote)
    promote
    ;;
  *)
    usage
    ;;
esac
