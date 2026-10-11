#!/bin/bash
# Build the latency probe in a container. The binary goes to bin/.
set -u
D=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$D/bin"
docker run --rm --label purpose=benchmark-temp \
  -v "$D/probe":/src -v indra-cargo-registry:/usr/local/cargo/registry \
  -w /src rust:1.96-bookworm bash -c 'cargo build --release 2>&1 | tail -30 && cp target/release/latency-probe /src/latency-probe'
mv "$D/probe/latency-probe" "$D/bin/latency-probe" && ls -la "$D/bin/latency-probe"
