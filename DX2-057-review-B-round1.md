# DX2-057 Review B Round 1 — Final Report

Task: configuration review (cluster, rule engine, licence settings). Reviewer B, round 1. Compared against arbiter's confirmed defects in `/work/state/DX2-057-arbiter0.log` vs base tree `c5264d5e6d176c6523403db0fecf02c6c3a0b91a`. Builds/tests not run (per constraints).

## VERDICT: changes

Reason: at least one in-scope setting is `UNAPPLIED-SETTING` in unchanged code. `cluster.bind`, `cluster.gossip_bind`, `cluster.quic_bind` are accepted/validated/stored but never consumed to bind a socket — they have no runtime consumer beyond schema validation and args plumbing (`main.rs:5331`). All other 11 in-scope settings are applied at boot and exposed.

## FILES READ

Source:
- `crates/broker-node/src/main.rs`
- `crates/broker-rules/src/lib.rs`
- `crates/broker-api/src/lib.rs`
- `crates/broker-config/src/schema.rs`

Docs:
- `docs/reference/configuration.md` (+diff vs base)
- `docs/_verification/failures.md` (DX2-057 records, lines 889-932)
- `docs/_verified/config/*.json` (13 present; `licence.license_key.json` absent)
- `docs/_verified/config/*.toml` examples (cluster, rules_engine, licence)
- `docs/_verified/config/*.text/*.md` (all 15; R-STE compliant)

State:
- `/work/state/DX2-057-arbiter0.log`
- `/work/state/DX2-057.pre.log` (PRECHECK_RESULT 0)

## REACHABILITY

All 14 in-scope settings reach the config object via `Settings::from_args`/`from_file` → `toml` deserialization into `ClusterConf`/`RuleEngineConf`/`LicenceConf` (`schema.rs`), then `Config::validate` (`schema.rs`). Consumption at boot:

- `rules_engine.window_channel_depth` → `main.rs:11331-11336` (`set_window_channel_depth`), exposed `broker-api/src/lib.rs get_nodes`.
- `rules_engine.backpressure_policy` → `main.rs:11337-11356` (`set_backpressure_policy`), exposed `get_nodes`.
- `rules_engine.spill_dir` → `main.rs:11777-11785` (`new_with_spill_config`); spill engine hardcodes `SpillToDisk` policy (`lib.rs:737-738`), coherent with configured policy otherwise.
- `licence.license_file` → `main.rs:12064-12077` (trusted keys load).
- `licence.license_key` → `main.rs:12091-12120` (install at boot, bad-key refusal).
- `licence.trusted_keys_path` → `main.rs:12064-12077`.
- `licence.request_out` / `licence.install_file` → boot CLI early-exit paths (unchanged, config-plumbed).
- `licence.expiry_warn_days` → boot expiry-warn wiring (unchanged, config-plumbed).
- `cluster.enabled` → boot refusal when true (`main.rs:12173-12175`, `schema.rs:2196`).
- `cluster.seed_nodes` → boot refusal when non-empty (`main.rs:12173`, `schema.rs:2203`).
- `cluster.bind` / `cluster.gossip_bind` / `cluster.quic_bind` → `schema.rs:2210-2212` (validate/store only); NO socket bind consumer anywhere (only `main.rs:5331` args plumbing + docs-example tests).

## PROOF — 14 in-scope acceptance items

| # | Setting | Status | Evidence |
|---|---------|--------|----------|
| 1 | `cluster.enabled` | Applied (refusal) | `main.rs:12173-12175`; `schema.rs:2196-2202` |
| 2 | `cluster.bind` | UNAPPLIED-SETTING | validated `schema.rs:2210`; no socket consumer |
| 3 | `cluster.gossip_bind` | UNAPPLIED-SETTING | validated `schema.rs:2211`; no socket consumer |
| 4 | `cluster.quic_bind` | UNAPPLIED-SETTING | validated `schema.rs:2212`; no socket consumer |
| 5 | `cluster.seed_nodes` | Applied (refusal) | `main.rs:12173`; `schema.rs:2203-2209` |
| 6 | `rules_engine.window_channel_depth` | FIXED (was UNAPPLIED) | `main.rs:11331-11336`; exposed `get_nodes` |
| 7 | `rules_engine.backpressure_policy` | FIXED (was UNAPPLIED) | `main.rs:11337-11356`; exposed `get_nodes` |
| 8 | `rules_engine.spill_dir` | Applied | `main.rs:11777-11785` |
| 9 | `licence.license_file` | Applied | `main.rs:12064-12077` |
| 10 | `licence.license_key` | FIXED (was never read) | `main.rs:12091-12120` (install + bad-key refusal) |
| 11 | `licence.trusted_keys_path` | Applied | `main.rs:12064-12077` |
| 12 | `licence.request_out` | Applied (CLI early-exit) | boot plumbing unchanged |
| 13 | `licence.install_file` | Applied (CLI early-exit) | boot plumbing unchanged |
| 14 | `licence.expiry_warn_days` | Applied | boot expiry-warn wiring unchanged |

Confirmed-defect closure (arbiter0.log):
- `rules_engine.window_channel_depth` — FIXED.
- `rules_engine.backpressure_policy` — FIXED.
- `licence.license_key` — FIXED (installed at boot; bad key refused).

## Verified evidence status

- 9 in-scope verified JSONs present and passing (`docs/_verified/config/*.json`).
- `licence.license_key.json` absent (read failed); `licence.license_key.toml` example has `expect="ok"` while `failures.md` records boot refusal with `docs-test-key` — minor doc/example inconsistency, not a code defect.
- 5 DX2-057 failure records in `failures.md` lines 889-932.
- 4 out-of-scope verified JSONs (`logging.level`, `node.data_dir`, `node.id`, `observability.monitor_sample_secs`) — identical content restores, out of scope.

## SLOP

None introduced. No banned names (emqx/lxc/hubctl/indra-e2e). All `_text` docs R-STE compliant. Licence doc claims match code. `configuration.md` "Not verified" callouts at lines 204/209/214 and 769-835 match `failures.md` records; no drift.

## CLASSES

- FIXED (3): `rules_engine.window_channel_depth`, `rules_engine.backpressure_policy`, `licence.license_key`.
- APPLIED (8): `cluster.enabled`, `cluster.seed_nodes`, `rules_engine.spill_dir`, `licence.license_file`, `licence.trusted_keys_path`, `licence.request_out`, `licence.install_file`, `licence.expiry_warn_days`.
- UNAPPLIED-SETTING (3, drives VERDICT changes): `cluster.bind`, `cluster.gossip_bind`, `cluster.quic_bind` — validated & stored, no socket bind consumer.

## LATER (non-blocking)

- `cluster.bind`/`gossip_bind`/`quic_bind`: either wire a socket bind consumer or document them as accepted-but-unapplied pending clustering MVP.
- `docs/_verified/config/licence.license_key.json`: regenerate or align `licence.license_key.toml` `expect="ok"` with `failures.md` refusal record.
- Minor: `configuration.md` "Not verified" callouts (lines 204/209/214, 769-835) correctly point at `failures.md`; keep in sync if failure records change.
