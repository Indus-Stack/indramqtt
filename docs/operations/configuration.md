---
title: "Configuration"
description: "File, environment and flag layers for each setting."
sidebar:
  order: 71
---

# IndraMQTT configuration layers

Every startup setting has exactly one typed home in the configuration
schema (`crates/broker-config/src/schema.rs`). Files, environment
variables and flags only supply values; the schema decides types,
defaults and valid ranges. A wrong type or an out-of-range value refuses
startup with an error naming the setting.

## Precedence

The effective configuration resolves in this order, each layer winning
over the ones before it:

```text
built-in schema defaults, then `indra.toml`, then `conf.d/*.toml` in file-name order, then `INDRA_*` environment variables, then command-line flags, then runtime changes made through the API or CLI.
```

```mermaid
flowchart LR
    Def["schema defaults"] -- "read" --> File["indra.toml"]
    File -- "read" --> Frag["conf.d fragments"]
    Frag -- "read" --> Env["INDRA_* variables"]
    Env -- "read" --> Flag["command flags"]
    Flag -- "apply" --> Kernel["kernel values"]
```

An explicitly passed command-line flag always wins over files and the
environment, so an operator can override anything at startup without
editing files. Runtime changes (history, `explain`, secrets) arrive with
the runtime task; files under the config directory are never rewritten
by the broker.

## Files: `indra.toml` plus `conf.d/*.toml`

The config directory (`--config-dir`, default `/etc/indramqtt`) holds
the operator's main file `<config-dir>/indra.toml` and optional
drop-in fragments `<config-dir>/conf.d/*.toml`, applied in file-name
order so `20-local.toml` wins over `10-base.toml`. The broker reads
only `*.toml` entries. A missing config directory or a missing `indra.toml`
starts on defaults. The broker refuses startup on bad input. Bad input
means an unreadable file, a TOML syntax error, an unknown setting, a
wrong-typed value or a validation failure. Each error names the file
and the setting.

```toml no-test
# Fragment: shows one file only. It is not a full boot file.
# /etc/indramqtt/indra.toml
[node]
id = "indra-node-1"

[session]
max_qos0_backlog = 1000

[auth]
allow_anonymous = false
```

Section names follow the schema. Use `[node]`, `[listeners.tcp]`,
`[listeners.tls]`, `[listeners.ws]`, `[listeners.wss]`, `[listeners.api]`,
`[quotas]`, `[session]`, `[logging]`, `[rules_engine]`, `[auth]`,
`[cluster]`, `[licence]`, `[gateway]`, `[persistence]`, `[ldap]`,
`[kerberos]` and `[mcp]`. `[listeners.websocket]` still means
`[listeners.ws]`. `[rules]` still means `[rules_engine]`. See
`docs/reference/settings.md` and `indramqtt.example.toml` for every setting.

## Environment: `INDRA_` plus the setting path

`INDRA_` plus the setting path with `__` between levels:

```sh no-test
# Example only: lists the variable names. Values differ per site.
INDRA_LISTENERS__TCP__BIND=0.0.0.0:1883
INDRA_SESSION__MAX_QOS0_BACKLOG=1000
INDRA_AUTH__ALLOW_ANONYMOUS=false
INDRA_CLUSTER__SEED_NODES=host1:19883,host2:19883
```

Values are typed by the schema: booleans take `true`/`false`, integers
and floats parse as numbers, everything else is a string, and string
lists split on commas. A wrong-typed value stops startup. The error
names the variable. An unknown `INDRA_*` variable stops startup. A typo
never boots an unwanted broker.

An explicit non-default `licence.license_file` is refused at load. The
broker keeps the stored licence and never reads this path. The error
names the file and the key. The default path keeps booting.

Two names from the shipped container files keep working as aliases:
`INDRA_LICENSE_KEY` sets `licence.license_key` and `INDRA_API_BIND`
sets `listeners.api.bind`. The canonical `INDRA_LICENCE__LICENSE_KEY`
and `INDRA_LISTENERS__API__BIND` forms win when both are set.

Two `INDRA_` variables hold credentials. They are not settings. They
have no schema home. `explain` and the exports omit them.

- `INDRA_API_KEYS`: the operator API keys that the broker accepts. Use
  a comma between keys. Each key must have 16 characters or more.
- `INDRA_API_KEY`: the key that `indra ctl` sends.

## How the broker uses the result

The kernel starts with the resolved values. The value that `explain`
shows for a setting is the value in operation. If no file, variable or
flag sets a setting, the kernel uses the default of the flag.

`logging.level` sets the log level. If you set `RUST_LOG`, it overrides
`logging.level`. `RUST_LOG` is a developer filter
(`RUST_LOG=broker_router=trace`) and has no schema home.

The kernel serves every enabled listener. It binds the MQTT, TLS and
WebSocket sockets itself. Set a listener in `indra.toml` only:

1. `indramqtt --check-config` loads the layered configuration. It prints
   `ok` and exits 0 for a valid directory. It prints the error and
   exits 2 for an invalid directory.
2. The container image starts only the kernel. It starts no edge process.

A listener bind must be an IP address and a port. The broker applies
`listeners.tcp.max_connections` and `listeners.tcp.backlog` at each
bind. Each kernel WebSocket listener uses the limit of the plaintext
listener.

`listeners.api.enabled = false` opens no API socket. MQTT still
serves from the same file. No flag sets `enabled`: a file or variable
with `enabled = false` wins over a bind value. The kernel then answers
no HTTP, including `/healthz`.

## MQTT listeners

The kernel accepts the clients of every enabled listener. The kernel
ignores four `native` settings. They are `listeners.tcp.native`,
`listeners.tls.native`, `listeners.ws.native` and `listeners.wss.native`.
Files and `INDRA_*` variables that set them still load. The kernel logs
a warning for each one that a layer sets. A value of `native = false`
requests the removed edge, so its warning is stronger. Do not set
`native` in new files:

```toml no-test
# Fragment: shows listeners only. It is not a full boot file.
[listeners.tcp]
bind = "0.0.0.0:1883"

[listeners.tls]
enabled = true
cert_file = "/etc/indramqtt/server.pem"
key_file = "/etc/indramqtt/server.key"

[listeners.wss]
enabled = true
cert_file = "/etc/indramqtt/server.pem"
key_file = "/etc/indramqtt/server.key"
```

If a kernel listener cannot start, the kernel stops with an error.

## Flags: one more layer, then the schema

Each long flag feeds exactly one schema home. A flag that is not passed
leaves its setting to the lower layers. `--config-dir` is the only flag
with no schema home: it locates the files, so it cannot come from them.

| Flag | Schema home |
|---|---|
| `-b`, `--bind` | `listeners.tcp.bind` |
| `--brokerlink-bind` | `node.brokerlink_bind` (deprecated) |
| `--api-bind` | `listeners.api.bind` |
| `--data-dir` | `node.data_dir` |
| `--node-id` | `node.id` |
| `--allow-anonymous` | `auth.allow_anonymous` |
| `--qos0-backlog` | `session.max_qos0_backlog` |
| `--license-key` | `licence.license_key` |
| `--license-keys` | `licence.trusted_keys_path` |
| `--licence-request-out` | `licence.request_out` |
| `--licence-install-file` | `licence.install_file` |
| `--licence-expiry-warn-days` | `licence.expiry_warn_days` |
| `--cluster-seeds` | `cluster.seed_nodes` |
| `--cluster-bind` | `cluster.bind` |
| `--coap-bind` | `gateway.coap_bind` |
| `--stream-dir` | `persistence.stream_dir` |
| `--ldap-url` | `ldap.server_url` |
| `--ldap-base-dn` | `ldap.base_dn` |
| `--ldap-bind-dn` | `ldap.bind_dn` |
| `--ldap-bind-password` | `ldap.bind_password` |
| `--ldap-user-filter` | `ldap.user_filter` |
| `--ldap-group-attribute` | `ldap.group_attribute` |
| `--ldap-required-group` | `ldap.required_group` |
| `--ldap-ca-cert` | `ldap.ca_cert_path` |
| `--kerberos-keytab` | `kerberos.keytab_path` |
| `--kerberos-service-principal` | `kerberos.service_principal` |
| `--kerberos-realm` | `kerberos.realm` |
| `--kerberos-allowed-realms` | `kerberos.allowed_realms` |
| `--kerberos-clock-skew-secs` | `kerberos.clock_skew_secs` |
| `--kerberos-role-map` | `kerberos.role_map` |
| `--kerberos-replay-max` | `kerberos.replay_max_entries` |
| `--config-dir` | bootstrap only (locates `indra.toml` + `conf.d`) |
