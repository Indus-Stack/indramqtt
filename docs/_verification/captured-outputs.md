# Captured outputs (DOC-02)

```mermaid
flowchart LR
    H["docs harness"] -- "run block" --> B["broker"]
    B -- "compare output" --> O["output block"]
```

The docs harness writes this file on each run. It runs each `sh test` block against a started broker.
It stores the real output it compares with the `output` block. The gates record the run result.

## Captured blocks

The list holds one row per `sh test` block. The output column holds the real output.

| Page | Line | Output |
|---|---|---|
| `benchmarks/index.md` | 115 | `OK` |
| `concepts/index.md` | 85 | `hello-docs` |
| `concepts/listeners.md` | 78 | `ok` |
| `concepts/listeners.md` | 88 | `hello-listeners` |
| `connectors/cassandra.md` | 46 | `ok` |
| `connectors/cassandra.md` | 78 | `ok` |
| `connectors/cassandra.md` | 107 | `ok` |
| `connectors/cassandra.md` | 136 | `ok` |
| `connectors/cassandra.md` | 166 | `ok` |
| `connectors/cassandra.md` | 196 | `ok` |
| `connectors/cassandra.md` | 258 | `ok` |
| `connectors/clickhouse.md` | 84 | `ok` |
| `connectors/couchbase.md` | 100 | `ok` |
| `connectors/datalayers.md` | 44 | `ok` |
| `connectors/datalayers.md` | 73 | `ok` |
| `connectors/datalayers.md` | 102 | `ok` |
| `connectors/datalayers.md` | 131 | `ok` |
| `connectors/datalayers.md` | 161 | `ok` |
| `connectors/datalayers.md` | 191 | `ok` |
| `connectors/datalayers.md` | 253 | `ok` |
| `connectors/doris.md` | 45 | `ok` |
| `connectors/doris.md` | 77 | `ok` |
| `connectors/doris.md` | 106 | `ok` |
| `connectors/doris.md` | 135 | `ok` |
| `connectors/doris.md` | 165 | `ok` |
| `connectors/doris.md` | 195 | `ok` |
| `connectors/doris.md` | 257 | `ok` |
| `connectors/elasticsearch.md` | 110 | `ok` |
| `connectors/greptimedb.md` | 45 | `ok` |
| `connectors/greptimedb.md` | 74 | `ok` |
| `connectors/greptimedb.md` | 103 | `ok` |
| `connectors/greptimedb.md` | 132 | `ok` |
| `connectors/greptimedb.md` | 162 | `ok` |
| `connectors/greptimedb.md` | 192 | `ok` |
| `connectors/greptimedb.md` | 226 | `ok` |
| `connectors/greptimedb.md` | 292 | `ok` |
| `connectors/http.md` | 93 | `delivered` |
| `connectors/index.md` | 139 | `ok` |
| `connectors/influxdb.md` | 86 | `influxdb` |
| `connectors/iotdb.md` | 45 | `ok` |
| `connectors/iotdb.md` | 73 | `ok` |
| `connectors/iotdb.md` | 102 | `ok` |
| `connectors/iotdb.md` | 131 | `ok` |
| `connectors/iotdb.md` | 160 | `ok` |
| `connectors/iotdb.md` | 190 | `ok` |
| `connectors/iotdb.md` | 255 | `ok` |
| `connectors/kafka.md` | 83 | `ok` |
| `connectors/mongodb.md` | 102 | `ok` |
| `connectors/mqtt-bridge.md` | 91 | `mqtt_bridge` |
| `connectors/mysql.md` | 79 | `mysql` |
| `connectors/opentsdb.md` | 43 | `ok` |
| `connectors/opentsdb.md` | 72 | `ok` |
| `connectors/opentsdb.md` | 101 | `ok` |
| `connectors/opentsdb.md` | 130 | `ok` |
| `connectors/opentsdb.md` | 160 | `ok` |
| `connectors/opentsdb.md` | 222 | `ok` |
| `connectors/postgres.md` | 79 | `postgres` |
| `connectors/redis.md` | 81 | `redis` |
| `connectors/s3.md` | 88 | `ok` |
| `connectors/tdengine.md` | 48 | `ok` |
| `connectors/tdengine.md` | 76 | `ok` |
| `connectors/tdengine.md` | 104 | `ok` |
| `connectors/tdengine.md` | 133 | `ok` |
| `connectors/tdengine.md` | 162 | `ok` |
| `connectors/tdengine.md` | 192 | `ok` |
| `connectors/tdengine.md` | 222 | `ok` |
| `connectors/tdengine.md` | 253 | `ok` |
| `connectors/tdengine.md` | 284 | `ok` |
| `connectors/tdengine.md` | 355 | `ok` |
| `connectors/timestream.md` | 49 | `ok` |
| `connectors/timestream.md` | 80 | `ok` |
| `connectors/timestream.md` | 111 | `ok` |
| `connectors/timestream.md` | 142 | `ok` |
| `connectors/timestream.md` | 172 | `ok` |
| `connectors/timestream.md` | 202 | `ok` |
| `connectors/timestream.md` | 232 | `ok` |
| `connectors/timestream.md` | 263 | `ok` |
| `connectors/timestream.md` | 294 | `ok` |
| `connectors/timestream.md` | 325 | `ok` |
| `connectors/timestream.md` | 391 | `ok` |
| `getting-started/console.md` | 48 | `200` |
| `getting-started/first-publish.md` | 49 | `hello-docs` |
| `getting-started/first-start.md` | 43 | `ok` |
| `getting-started/first-start.md` | 62 | `OK` |
| `getting-started/first-user.md` | 39 | `201` |
| `getting-started/first-user.md` | 82 | `ok<br>denied` |
| `getting-started/install.md` | 66 | `indramqtt 0.1.0` |
| `getting-started/install.md` | 74 | `indra 0.1.0` |
| `index.md` | 54 | `OK` |
| `mcp.md` | 314 | `401` |
| `migration.md` | 94 | `OK` |
| `multi-tenancy/index.md` | 96 | `isolated` |
| `multi-tenancy/quotas.md` | 102 | `ok` |
| `operations/backup.md` | 90 | `ok` |
| `operations/deployment.md` | 118 | `ok` |
| `operations/index.md` | 96 | `ok` |
| `operations/logging.md` | 73 | `ok` |
| `operations/monitoring.md` | 77 | `OK` |
| `operations/sizing.md` | 71 | `ok` |
| `operations/troubleshooting.md` | 111 | `ok` |
| `operations/upgrade.md` | 68 | `ok` |
| `reference/api/authentication-acl.md` | 437 | `OK` |
| `reference/api/authentication-acl.md` | 500 | `OK` |
| `reference/api/authentication-acl.md` | 516 | `OK` |
| `reference/api/clients-sessions.md` | 162 | `OK` |
| `reference/api/clients-sessions.md` | 214 | `OK` |
| `reference/api/clients-sessions.md` | 230 | `OK` |
| `reference/api/cluster-nodes.md` | 144 | `OK` |
| `reference/api/cluster-nodes.md` | 196 | `OK` |
| `reference/api/cluster-nodes.md` | 212 | `OK` |
| `reference/api/configuration.md` | 479 | `OK` |
| `reference/api/configuration.md` | 531 | `OK` |
| `reference/api/configuration.md` | 547 | `OK` |
| `reference/api/data-connectors.md` | 149 | `OK` |
| `reference/api/data-connectors.md` | 207 | `OK` |
| `reference/api/data-connectors.md` | 223 | `OK` |
| `reference/api/index.md` | 74 | `OK` |
| `reference/api/index.md` | 90 | `OK` |
| `reference/api/licence.md` | 203 | `OK` |
| `reference/api/licence.md` | 257 | `OK` |
| `reference/api/licence.md` | 273 | `OK` |
| `reference/api/observability.md` | 79 | `OK` |
| `reference/api/observability.md` | 132 | `OK` |
| `reference/api/observability.md` | 148 | `OK` |
| `reference/api/rule-engine.md` | 437 | `OK` |
| `reference/api/rule-engine.md` | 500 | `OK` |
| `reference/api/rule-engine.md` | 516 | `OK` |
| `reference/cli.md` | 2330 | `OK` |
| `reference/cli.md` | 2346 | `OK` |
| `reference/configuration.md` | 2382 | `OK` |
| `reference/configuration.md` | 2398 | `OK` |
| `reference/index.md` | 41 | `OK` |
| `reference/metrics.md` | 118 | `OK` |
| `reference/metrics.md` | 134 | `OK` |
| `rules/actions.md` | 64 | `delivered` |
| `rules/actions.md` | 258 | `matched=2 success=2 reset=0` |
| `rules/functions.md` | 506 | `ok` |
| `rules/functions.md` | 531 | `ok` |
| `rules/index.md` | 70 | `OK` |
| `rules/sql.md` | 197 | `matched<br>unmatched` |
| `security/audit-log.md` | 53 | `ok` |
| `security/audit-log.md` | 65 | `200` |
| `security/authentication-builtin.md` | 57 | `ok` |
| `security/authentication-builtin.md` | 69 | `201` |
| `security/authentication-builtin.md` | 102 | `ok<br>denied` |
| `security/authentication-jwt.md` | 63 | `ok` |
| `security/authentication-jwt.md` | 75 | `denied` |
| `security/authentication-jwt.md` | 109 | `ok` |
| `security/authentication-jwt.md` | 156 | `ok` |
| `security/authentication.md` | 71 | `ok` |
| `security/authentication.md` | 83 | `201` |
| `security/authorization.md` | 66 | `ok` |
| `security/authorization.md` | 78 | `201` |
| `security/authorization.md` | 111 | `hello-press` |
| `security/index.md` | 81 | `201` |
| `security/management-access.md` | 65 | `ok` |
| `security/management-access.md` | 77 | `200` |
| `security/psk.md` | 75 | `ok` |
| `security/psk.md` | 85 | `OK` |
| `security/tls.md` | 93 | `ok` |
| `security/tls.md` | 103 | `OK` |

The run captured 161 blocks. Each output matches its page.
