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
| `connectors/alloydb.md` | 93 | `alloydb` |
| `connectors/aws_iot.md` | 100 | `ok` |
| `connectors/aws_timestream.md` | 103 | `timestream` |
| `connectors/azure_blob.md` | 91 | `ok` |
| `connectors/azure_blob_storage.md` | 81 | `ok` |
| `connectors/azure_event_grid.md` | 71 | `ok` |
| `connectors/azure_event_hub_producer.md` | 97 | `ok` |
| `connectors/azure_eventhubs.md` | 118 | `ok` |
| `connectors/azure_iot.md` | 110 | `ok` |
| `connectors/bigquery.md` | 103 | `ok` |
| `connectors/cassandra.md` | 103 | `ok` |
| `connectors/clickhouse.md` | 91 | `ok` |
| `connectors/cockroachdb.md` | 90 | `ok` |
| `connectors/confluent.md` | 105 | `ok` |
| `connectors/confluent_producer.md` | 94 | `ok` |
| `connectors/console.md` | 65 | `ok` |
| `connectors/couchbase.md` | 99 | `ok` |
| `connectors/databricks.md` | 109 | `databricks` |
| `connectors/datalayers.md` | 97 | `datalayers` |
| `connectors/disk_log.md` | 95 | `disk_log` |
| `connectors/doris.md` | 106 | `doris` |
| `connectors/dynamo.md` | 110 | `ok` |
| `connectors/dynamodb.md` | 110 | `ok` |
| `connectors/elasticsearch.md` | 95 | `ok` |
| `connectors/gcp_iot.md` | 98 | `ok` |
| `connectors/gcp_pubsub.md` | 99 | `ok` |
| `connectors/gcp_pubsub_consumer.md` | 89 | `ok` |
| `connectors/gcp_pubsub_producer.md` | 85 | `ok` |
| `connectors/greptimedb.md` | 90 | `greptimedb` |
| `connectors/http.md` | 93 | `delivered` |
| `connectors/http.md` | 192 | `http` |
| `connectors/index.md` | 127 | `ok` |
| `connectors/influxdb.md` | 89 | `influxdb` |
| `connectors/iotdb.md` | 98 | `iotdb` |
| `connectors/kafka.md` | 88 | `ok` |
| `connectors/kafka_consumer.md` | 89 | `ok` |
| `connectors/kafka_producer.md` | 80 | `ok` |
| `connectors/kinesis.md` | 101 | `ok` |
| `connectors/mongodb.md` | 94 | `ok` |
| `connectors/mqtt-bridge.md` | 94 | `mqtt_bridge` |
| `connectors/mqtt.md` | 97 | `ok` |
| `connectors/mssql.md` | 99 | `mssql` |
| `connectors/mysql.md` | 82 | `mysql` |
| `connectors/oci_streaming.md` | 107 | `delivered` |
| `connectors/opc_ua.md` | 97 | `ok` |
| `connectors/opents.md` | 91 | `delivered` |
| `connectors/opentsdb.md` | 93 | `opentsdb` |
| `connectors/oracle.md` | 91 | `oracle` |
| `connectors/pgsql.md` | 96 | `connected` |
| `connectors/postgres.md` | 82 | `postgres` |
| `connectors/pulsar.md` | 98 | `pulsar` |
| `connectors/rabbitmq.md` | 80 | `ok` |
| `connectors/redis.md` | 86 | `redis` |
| `connectors/s3.md` | 88 | `ok` |
| `connectors/sparkplug_b.md` | 84 | `sparkplug_b` |
| `connectors/tdengine.md` | 101 | `tdengine` |
| `connectors/timescaledb.md` | 85 | `timescaledb` |
| `getting-started/console.md` | 48 | `200` |
| `getting-started/first-publish.md` | 49 | `hello-docs` |
| `getting-started/first-start.md` | 43 | `ok` |
| `getting-started/first-start.md` | 62 | `OK` |
| `getting-started/first-user.md` | 39 | `201` |
| `getting-started/first-user.md` | 82 | `ok<br>denied` |
| `getting-started/install.md` | 66 | `indramqtt 0.1.0` |
| `getting-started/install.md` | 74 | `indra 0.1.0` |
| `index.md` | 56 | `OK` |
| `mcp.md` | 322 | `401` |
| `migration.md` | 90 | `OK` |
| `multi-tenancy/index.md` | 100 | `isolated` |
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
| `reference/configuration.md` | 2381 | `OK` |
| `reference/configuration.md` | 2397 | `OK` |
| `reference/index.md` | 41 | `OK` |
| `reference/metrics.md` | 118 | `OK` |
| `reference/metrics.md` | 134 | `OK` |
| `rules/actions.md` | 64 | `delivered` |
| `rules/functions.md` | 506 | `ok` |
| `rules/functions.md` | 531 | `ok` |
| `rules/index.md` | 78 | `OK` |
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
| `security/management-access.md` | 81 | `ok` |
| `security/management-access.md` | 93 | `200` |
| `security/psk.md` | 75 | `ok` |
| `security/psk.md` | 85 | `OK` |
| `security/tls.md` | 93 | `ok` |
| `security/tls.md` | 103 | `OK` |

The run captured 141 blocks. Each output matches its page.
