VERDICT: accept
FILES READ: crates/broker-rules/src/lib.rs, crates/broker-rules/src/runtime.rs, crates/broker-node/src/main.rs, crates/broker-api/src/v5/rules.rs, crates/broker-config/src/lib.rs, crates/broker-config/src/runtime.rs, crates/broker-node/src/indra.rs, tools/boot_smoke.sh, crates/broker-rules/tests/rule_runtime_through_broker.rs, tests/mt_publish_bench.rs, /work/state/TM-04.gates.log, /work/team/migration/RULEBOOK.md, /work/team/tickets/TM-04-rules.md, /work/team/parity/REVIEW-BRIEF.md

REACHABILITY:
- Rule.tenant field: Written by RuleEngine::create_rule_in_tenant (crates/broker-rules/src/lib.rs:1824) and RuleEngine::create_rule_with_id_in_tenant (crates/broker-rules/src/lib.rs:1850) called from API handlers create_rule (crates/broker-api/src/v5/rules.rs:338) and update_rule (crates/broker-api/src/v5/rules.rs:643). Read by dispatch_ingress_in_tenant at line 3148 for tenant filtering.
- dispatch_ingress_in_tenant: Called from ingress_pipeline_with_publisher (crates/broker-node/src/main.rs:6660) on every broker publish — the production publish path. The tenant argument is publisher_tenant resolved from the publishing client's session at bind time.
- BrokerSink::publish_in_tenant: Implemented by InMemoryBrokerSink (crates/broker-node/src/main.rs:4248) — the production sink. The default trait impl ignores tenant (for test sinks), but production code uses InMemoryBrokerSink which fans out in the named tenant via build_downlink_frames with the tenant parameter.
- RuleEngine::run_actions_in_tenant: Called from dispatch_ingress_in_tenant (line 3201) and from rule-task flush path (line 1764). Passes rule.tenant to sink.publish_in_tenant (line 3332).
- Tenant validation at create/update: resolve_rule_tenant (crates/broker-api/src/v5/rules.rs:107) checks tenant against state.namespaces (managed namespaces from TM-03) and is_valid_rule_tenant (crates/broker-config/src/lib.rs:696).
- BoundedEventInput spill: The spill_enabled check at dispatch_ingress_in_tenant line 3244 is a lock-free atomic load; the spill directory is configured at boot via --rule-spill-dir (main.rs:189). No new lock on hot path.

PROOF:
- Rule in tenant A fires only for publish in tenant A: Test tm04_rule_in_tenant_a_fires_only_for_a_through_broker_path (crates/broker-rules/tests/rule_runtime_through_broker.rs:399). Production entry: MQTT client socket → edge → kernel ingress_pipeline_with_publisher → dispatch_ingress_in_tenant. Change that breaks: remove `rule.tenant == tenant` filter at lib.rs:3148.
- Rule output in tenant A reaches subscriber in tenant A only: Test tm04_rule_output_stays_in_own_tenant_through_broker_path (crates/broker-rules/tests/rule_runtime_through_broker.rs:452). Production entry: Same publish path; output via sink.publish_in_tenant. Change that breaks: pass wrong tenant to publish_in_tenant at lib.rs:3333.
- Legacy rule serves default tenant: Test tm04_legacy_rule_serves_default_tenant_through_broker_path (crates/broker-rules/tests/rule_runtime_through_broker.rs:508). Production entry: Same. Change that breaks: change canonical_rule_tenant default at config/lib.rs:704.
- API validates tenant at create/update: Boot smoke section 8 (tools/boot_smoke.sh:660) creates rule via POST /api/v1/rules with tenant field, verifies isolation via real client sockets. Change that breaks: remove resolve_rule_tenant validation at rules.rs:107.
- Boot smoke rule tenant checks pass: Gates log lines 77-82 show "ok a rule of the first tenant with a republish action is stored through the API", "ok the rule output arrives in the tenant of the rule", "ok the rule output does not leak to the other tenant", "ok a publish in the tenant without the rule fires no output there", "ok a publish in the tenant without the rule sends nothing to the rule tenant".

SLOP: none

CLASSES: none