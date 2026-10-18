# Client library matrix results

Two libraries ran the full checklist: Paho Python and Node `mqtt`.
Both use the real client package over a client socket. The Rust
`rumqttc` project is written but not built in this run; the gates
build it. The Rust `paho-mqtt` crate and the mosquitto tools are not
installable here.

## Environment

- The broker is the prebuilt kernel binary from the build cache.
- `run_all.sh` starts the broker from an `indra.toml` it writes, with
  `session.max_packet_size = 4194304` so the 1 MiB payload fits.
- TLS uses the test certificates of the kernel TLS tests.
- WebSocket uses path `/mqtt`.
- Each run uses a fresh data directory and fresh ports.

## Install report

Try each library in order. Report what each attempt gives.

| Library | Attempt | Outcome |
|---|---|---|
| Paho Python client | `pip install --target` outside the tree. | Installed. `paho-mqtt 2.1.0` ran the full checklist on TCP, TLS and WebSocket. |
| Rust `rumqttc` | Crate `rumqttc-0.25.1` is in the registry cache. | Project written under `rust_rumqttc/`. The loop cannot run cargo, so this run did not build it. `not verified` for every Rust row. The gates build and run it. |
| Rust `paho-mqtt` | Needs the Eclipse Paho C library and system header changes. | Not installable: the task forbids root changes to the system. Each check reports `not-supported` with that reason. |
| `mosquitto_pub`, `mosquitto_sub` | `command -v` for both. | Both are missing. Each check reports `not-supported: tools missing`. |
| Node `mqtt` package | `npm install` outside the tree (`mqtt@5.16.1`). | Installed. `node_mqtt/` ran the full checklist on TCP and WebSocket. |

No library is vendored into the tree. Packages live outside the tree
and resolve at run time.

## Result table

`pass` means the broker did the work through a client socket.
`fail` gives the observed error. `not-supported` names the reason.
`not verified` means this run did not execute it.

| Check | Python TCP | Python TLS | Python WS | Node TCP | Node WS | Rust rumqttc | Rust paho-mqtt | mosquitto |
|---|---|---|---|---|---|---|---|---|
| v311-connect | pass | pass | pass | pass | pass | not verified | not-supported: C library | not-supported: tools missing |
| v5-connect | pass | pass | pass | pass | pass | not verified | not-supported: C library | not-supported: tools missing |
| qos0 | pass | pass | pass | pass | pass | not verified | not-supported: C library | not-supported: tools missing |
| qos1 | pass | pass | pass | pass | pass | not verified | not-supported: C library | not-supported: tools missing |
| qos2 | pass | pass | pass | pass | pass | not verified | not-supported: C library | not-supported: tools missing |
| retained | pass | pass | pass | pass | pass | not verified | not-supported: C library | not-supported: tools missing |
| will | pass | pass | pass | pass | pass | not verified | not-supported: C library | not-supported: tools missing |
| session-resume | pass | pass | pass | pass | pass | not verified | not-supported: C library | not-supported: tools missing |
| shared | pass | pass | pass | pass | pass | not verified | not-supported: C library | not-supported: tools missing |
| payload-1mib | pass | pass | pass | pass | pass | not verified | not-supported: C library | not-supported: tools missing |
| auth | pass | pass | pass | pass | pass | not verified | not-supported: C library | not-supported: tools missing |
| keepalive | pass | pass | pass | pass | pass | not verified | not-supported: C library | not-supported: tools missing |
| restart | pass | pass | pass | pass | pass | not verified | not-supported: C library | not-supported: tools missing |

Evidence lines are in the run output of `run_all.sh`
(`results-lines.txt`). Example lines from the run:

```
RESULT auth pass good accepted, bad refused (CONNACK 134)
RESULT restart pass reconnect after restart works
RESULT payload-1mib pass 1 MiB payload delivered
```

Two different libraries ran the full checklist. The spec requirement
of at least two is met.

## Failures observed during earlier runs, with evidence

The arbiter log records two failures from the implementer runs. This
section reports them with their evidence.

### 1. `auth_refused ... wrong password accepted`

Evidence: `XT-02.log:1633`

```
{"check": "auth_refused", "status": "fail", "detail": "wrong password accepted (Success)"}
```

Cause: that run used the dead duplicate program `checklist.py`, which
never created a user and never set a credential. It tested the empty
username with a wrong password against an open broker (empty
`authn_chain` accepts any credential by design). This is a test
harness defect, not a broker defect.

Current state: `checklist.py` is removed. `check.py` creates a real
user through the management API, connects with the good password, then
connects with a wrong password. The wrong password is refused:

```
RESULT auth pass good accepted, bad refused (CONNACK 134)
```

No broker code change was needed. The broker refuses a wrong password
whenever a local user exists; verified against the live broker during
this task.

### 2. `will ... will victim did not connect`

Evidence: `XT-02.log:1733`

```
RESULT will fail RuntimeError: will victim did not connect (got '')
```

Cause: a race in the harness. The child process could exit before it
printed `READY`. A later run of the same check passed
(`XT-02.log` line 667 shows `will_message ... pass`).

Current state: the Node runner now holds the child alive after
`READY`, and the Python runner keeps the same spawn pattern that later
passed. Both libraries pass the will check in the run above:

```
RESULT will pass will message published on an abrupt close
```

No broker defect was found. The failure was in the test harness and is
fixed there.

## Restart check

`run_all.sh` starts the broker and exports `MATRIX_RESTART_CMD` before
it runs each library. The command stops the broker named in the pid
file, starts it again, and waits for `/healthz`. Every library that
runs in this setup reports `restart pass`. A run without
`MATRIX_RESTART_CMD` reports `not-supported: no restart command in
this run`; that state is honest and does not claim coverage.

## Notes

- The `auth` check creates one user per run. The check runs after the
  message checks so a new user does not disturb earlier connections.
- Each check uses a random topic. Runs do not clash.
- The Rust runner covers plain TCP only. TLS and WebSocket report
  not-supported by that client.
- `results-lines.txt` from a run is the raw evidence for this table.
