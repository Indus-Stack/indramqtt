# Bench base image. Adds broker runtime libs.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libssl3 zlib1g libzstd1 ca-certificates && rm -rf /var/lib/apt/lists/*
