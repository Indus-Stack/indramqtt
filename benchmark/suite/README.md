# Benchmark suite

This suite measures one broker on a fixed budget. It uses 4 pinned
cores and 8 GiB for the broker. It uses 128 B payloads unless a
scenario says otherwise.

## Method

- Each scenario starts a fresh broker in a container. The container
  has 4 pinned cores (`0-3`), 8 GiB of memory and the host network.
- Load generators run in other containers on other cores. Publishers
  run on `4-9,21-24`. Subscribers run on `10-17,19`.
- The latency probe runs on core `20` alone. It sends 1000 msg/s in
  total (20 pairs, 50 msg/s per pair). It records end-to-end latency
  in a histogram. It shows the latency under the main load.
- Subscribers connect first. They stay for all steps. Each step starts
  new publishers.
- A step lasts `--step-secs` (default 300 s). The first `--warm`
  seconds (default 60 s) stay out of the averages. Each rate is the
  change of a counter over the window, divided by the window length.
- Load numbers come from the counters of the broker itself
  (`GET /api/v5/metrics`). The publish counter of the generator counts
  each publish twice. The suite never uses it. The suite subtracts the
  probe traffic (1000 msg/s in and out).
- CPU comes from `cpu.stat` of the cgroup of the broker container. It
  shows as a share of the 4 cores, with user and system split. Memory
  is `memory.current`.
- System calls come from `/proc/<pid>`. The fields `syscr` and `syscw`
  count only `read` and `write` calls. The broker uses `send` and
  `recv` on sockets. The kernel does not count them there. The library
  `tools/syscount.c` (`--syscount`) counts the calls of the C library
  instead. Use it for per-message call counts.
- A step has flag `generator_limited` when a generator group uses 90
  percent of its cores on average, or 97 percent in one 5 s sample. Do
  not use such a step for a broker limit.
- `delivery_ratio` is delivered messages divided by received messages
  times the fan-out.
- The recommended load is the step with CPU nearest 75 percent. Steps
  with a limit flag or delivery below 90 percent do not count.

## Pass rule

A step passes when all these hold:

- Delivery is 90 percent or more.
- The broker stays alive.
- The generator shows no limit flag.
- Memory growth stays below 15 MiB or 4 percent.
- The p99 latency of the probe stays level (last third median below
  two times the first third median plus 50 ms).
- The drain after the publishers stop stays below 30 s.

The driver stops a scenario after two failing steps in a row. A fail
is delivery below 90 percent or a stopped broker.

## Files

- `suite.py` is the driver. Use `suite.py list` and
  `suite.py run <scenario> --binary B --out DIR`.
- `suite.py summarize DIR` writes `all-scenarios.csv`.
- `suite.py compare NEW OLD --out CMP.csv` compares recommended rates.
- `report.py` makes one HTML page from a run directory.
- `probe/` is the latency probe (Rust). `build-probe.sh` builds it.
- `build-broker.sh` builds a broker from a source tree in a container.
- `tools/syscount.c` and `build-syscount.sh` build the call counter.
- `tools/folded_top.py` sums a folded stack file.
- `run_all.sh` builds the binary, runs all scenarios, makes the page.
- Output holds `<scenario>.jsonl` (one line per step),
  `<scenario>.csv`, `<scenario>.summary.json`, `all-scenarios.csv`,
  `env.json` and `report.html`.

## Scenarios

| Name | Content |
| --- | --- |
| `s1-qos0`, `s1-qos1`, `s1-qos2` | 1000 publishers, 1000 subscribers, 1000 topics, rising rate |
| `s2-payload` | 20000 msg/s inbound, QoS 1, sizes 128 B, 1 KiB, 4 KiB, 8 KiB (`--rates` lists sizes) |
| `s3-fanin` | 2000 publishers, 1000 subscribers in shared groups of 5; QoS 1 |
| `s3-fanout` | 10 publishers, 2000 subscribers, 200 per topic; QoS 1; `--rates` is inbound rate |

## Bridging across two brokers (planned, not built)

Two brokers with 2 cores each need a link between them. This needs two
broker containers on separate core sets, a link setting on both,
publishers on broker A and subscribers on broker B (and back),
counters from both brokers, and the probe on broker B with publishers
on broker A.

TODO(parity): which link setting joins two brokers? The suite waits
for the product link. It does not test bridging today.

## Run the suite

Set the generator image first. The default is a placeholder:

```bash
export LOADGEN_IMAGE=mqtt-loadgen:latest
export LOADGEN_BIN=/loadgen/bin/loadgen
bash benchmark/suite/build-probe.sh
bash benchmark/suite/run_all.sh <commit> /tmp/bench-out
```

Each step lasts 300 s by default. A full run takes many hours. Use
`--step-secs 40 --warm 10 --rates ...` for short diagnosis runs only.
Do not report short runs as results.

Run the unit tests:

```bash
cd benchmark/suite && python3 -m unittest
```

## Limits of the method

- Each step runs once. Results vary between runs.
- The generator shares the machine with the broker.
- The probe adds 1000 msg/s. The suite subtracts it.
- Latency covers probe pairs only.
- The CPU layout fits one machine shape. Override it with
  `BENCH_BROKER_CPUS`, `BENCH_PUB_CPUS`, `BENCH_SUB_CPUS` and
  `BENCH_PROBE_CPUS`.
