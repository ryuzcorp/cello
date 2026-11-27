# syntax=docker/dockerfile:1.7
# Modified by Ryuz Corp (Cello): the build stage bakes CELLO_VERSION into the binary for --version.
# Tests and clippy run natively in CI (ci.yml, `--profile ci`); the image only builds the
# release binary. publish.yml keeps the "what ships was tested" guarantee by refusing a
# tag whose commit lacks a passing `ci-ok` check.

ARG RUST_VERSION=1.98.1
ARG CELLO_COMMIT=unknown
ARG ESBUILD_VERSION=0.28.2

FROM rust:${RUST_VERSION}-bookworm AS build
ARG TARGETARCH
ARG ESBUILD_VERSION
# `release` for shipped artifacts; a fast-loop caller passes `lab` to skip
# the fat-LTO relink and keep incremental state in the target cache.
ARG CELLO_PROFILE=release
# The full build version the binary's --version reports
# (`<version>`, `<version>-cello.ci` in CI). Unset here,
# build.rs falls back to the crate version.
ARG CELLO_VERSION
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,id=cello-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cello-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=cello-target-${TARGETARCH},target=/src/target,sharing=locked \
    mkdir -p /out && \
    CELLO_VERSION="${CELLO_VERSION}" cargo build --profile "${CELLO_PROFILE}" --locked -p cello && \
    install -m 755 "target/${CELLO_PROFILE}/cello" /out/cello
# Self-contained esbuild for `cello dev`/`cello deploy` of Worker code: the
# same repo-pinned npm tarballs (by version and sha512) the CI test job
# installs, selected by TARGETARCH. Verified here, then copied into the
# runtime stage, so the final image needs no Node/npm/curl.
RUN set -eu; \
    case "${TARGETARCH}" in \
      amd64) pkg=linux-x64; \
             sha=e314d9af5154992a105b85c85a68addedcd0ad44d933e3773f45d5f3144e298179d177c8ef178ef74fb56d9bcdc3122e7d0f610d0551247e588603d517c03f1d ;; \
      arm64) pkg=linux-arm64; \
             sha=a56e000b43f78adf1cedda3d315338a79d45cc7cdd33f4d9adeaeeae045c1c9d964dad55435088ab5f319dc7e9049c38a23922659aca5b6c8805604ff768fa52 ;; \
      *) echo "unsupported arch ${TARGETARCH} for esbuild" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/esbuild.tgz \
      "https://registry.npmjs.org/@esbuild/${pkg}/-/${pkg}-${ESBUILD_VERSION}.tgz"; \
    echo "${sha}  /tmp/esbuild.tgz" | sha512sum -c -; \
    tar -xzf /tmp/esbuild.tgz -C /tmp package/bin/esbuild; \
    install -m 755 /tmp/package/bin/esbuild /out/esbuild; \
    rm -rf /tmp/esbuild.tgz /tmp/package; \
    [ "$(/out/esbuild --version)" = "${ESBUILD_VERSION}" ]

FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
ARG CELLO_COMMIT
ARG CELLO_VERSION=unknown
ARG ESBUILD_VERSION=0.28.2
LABEL org.opencontainers.image.title="cello" \
      org.opencontainers.image.revision="${CELLO_COMMIT}" \
      org.opencontainers.image.version="${CELLO_VERSION}"
COPY LICENSE NOTICE /usr/share/doc/cello/
COPY --from=build /out/cello /usr/local/bin/cello
COPY --from=build /out/esbuild /usr/local/bin/esbuild
# Mechanical assertion: the shipped binary runs on the image architecture
# (a wrong-arch binary fails here with an exec-format error) and matches
# the pinned version.
RUN test "$(/usr/local/bin/esbuild --version)" = "${ESBUILD_VERSION}"
ENTRYPOINT ["/usr/local/bin/cello"]
