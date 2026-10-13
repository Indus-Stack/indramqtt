---
title: "Benchmarks"
description: "Measured numbers and how to reproduce them."
sidebar:
  order: 90
---

# Benchmarks

This page gives no new numbers. It points to measured results and to the commands that reproduce them.

![Bar chart of router and rule engine rates.](../assets/microbench.svg)

Source: `docs/_data/microbench.json`. The script `docs/_tools/render_charts.py` draws the chart. The values come from `BENCHMARKS.md` and `crates/broker-node/benches/broker_throughput.rs`.

| Test | Rate | Unit |
|---|---|---|
| Router hit-path | 3.05 | million lookups/s |
| Router fast-miss | 7.80 | million lookups/s |
| Rule engine ingress | 4.77 | million events/s |

The rates are single-threaded and in memory. They change with the hardware. They are not network values.

## Broker comparison

The README compares brokers at 1 CPU and 1 GB. It states the method, the limits and the raw data. Read it before you quote a number.

## Microbenchmarks

The router and rule engine microbenchmarks measure one function in one thread, in memory. They are not network values. They change with the hardware. Run them on your hardware:

```sh no-test
# Slow: runs release benchmarks. The example test does not run it.
cargo test --release --bench broker_throughput -- --nocapture
```

The source is `crates/broker-node/benches/broker_throughput.rs`. The method is in `BENCHMARKS.md`. End-to-end loopback results live in `benchmark_suite/`.

## Rules for numbers

Quote a number only with its hardware, its scenario and its repeats. A figure that still rises at cutoff is "still rising at cutoff", never final. Report the slow cases first.

## Check that it works

Start the broker with this file. The health endpoint returns `OK`.

```toml test
[node]
id = "docs-benchmarks"

[listeners.tcp]
bind = "0.0.0.0:1883"

[listeners.api]
bind = "0.0.0.0:18083"

[logging]
level = "warn"
```

```sh test
python3 - <<'PY'
import os, socket
port = int(os.environ.get("API_PORT", "18083"))
sock = socket.create_connection(("127.0.0.1", port), timeout=5)
sock.sendall(b"GET /healthz HTTP/1.0\r\nHost: docs\r\n\r\n")
print(sock.makefile().read().strip().splitlines()[-1])
PY
```

```output
OK
```
