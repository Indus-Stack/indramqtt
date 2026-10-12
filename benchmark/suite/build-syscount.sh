#!/bin/bash
# Build the call counter library in a container.
D=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$D/bin"
docker run --rm --label purpose=benchmark-temp -v "$D":/w -w /w rust:1-bookworm \
  gcc -O2 -shared -fPIC -o bin/libsyscount.so tools/syscount.c -ldl && ls -la "$D/bin/libsyscount.so"
