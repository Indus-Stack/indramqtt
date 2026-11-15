# Connector end-to-end inventory

This file is generated from the connector source.
Run `python3 tools/connector_inventory.py --write` to update it.
The drift test fails when this file differs from the source.

Connectors: 47.
Items: 775.

| Kind | Crate | Direction | Status | Config type | Items |
|---|---|---|---|---|---|
| `alloydb` | broker-connectors-enterprise | sink | mvp | `AlloydbConfig` | 19 |
| `aws_iot` | broker-connectors-enterprise | both | out-of-mvp | `AwsIotConfig` | 24 |
| `azure_blob` | broker-connectors-enterprise | sink | mvp | `AzureBlobSinkConfig` | 17 |
| `azure_eventhubs` | broker-connectors-enterprise | sink | mvp | `AzureEventHubsSinkConfig` | 16 |
| `azure_iot` | broker-connectors-enterprise | both | out-of-mvp | `AzureIotConfig` | 19 |
| `bigquery` | broker-connectors-enterprise | sink | mvp | `BigQuerySinkConfig` | 19 |
| `cassandra` | broker-connectors-enterprise | sink | mvp | `CassandraSinkConfig` | 21 |
| `clickhouse` | broker-connectors | sink | mvp | `ClickHouseSinkConfig` | 10 |
| `cockroachdb` | broker-connectors-enterprise | sink | mvp | `CockroachDbConfig` | 8 |
| `confluent` | broker-connectors-enterprise | sink | mvp | `ConfluentKafkaConfig` | 29 |
| `console` | broker-connectors | sink | mvp | `none` | 0 |
| `couchbase` | broker-connectors-enterprise | sink | mvp | `CouchbaseSinkConfig` | 22 |
| `databricks` | broker-connectors-enterprise | sink | mvp | `DatabricksSinkConfig` | 16 |
| `datalayers` | broker-connectors-enterprise | sink | mvp | `DatalayersConfig` | 13 |
| `disk_log` | broker-connectors | sink | mvp | `DiskLogSinkConfig` | 19 |
| `doris` | broker-connectors-enterprise | sink | mvp | `DorisSinkConfig` | 21 |
| `dynamodb` | broker-connectors-enterprise | sink | mvp | `DynamoDbSinkConfig` | 26 |
| `elasticsearch` | broker-connectors | sink | mvp | `ElasticsearchSinkConfig` | 22 |
| `gcp_iot` | broker-connectors-enterprise | both | mvp | `GcpIotConfig` | 16 |
| `gcp_pubsub` | broker-connectors-enterprise | sink | mvp | `GcpPubSubSinkConfig` | 17 |
| `greptimedb` | broker-connectors | sink | mvp | `GreptimeDbConfig` | 17 |
| `influxdb` | broker-connectors | sink | mvp | `InfluxDbSinkConfig` | 10 |
| `iotdb` | broker-connectors-enterprise | sink | mvp | `IotDbSinkConfig` | 22 |
| `kafka` | broker-connectors | sink | mvp | `KafkaSinkConfig` | 15 |
| `kinesis` | broker-connectors-enterprise | sink | mvp | `KinesisSinkConfig` | 16 |
| `mongodb` | broker-connectors-enterprise | sink | mvp | `MongoDbSinkConfig` | 15 |
| `mqtt_bridge` | broker-connectors | sink | mvp | `MqttBridgeSinkConfig` | 18 |
| `mssql` | broker-connectors-enterprise | sink | mvp | `MssqlSinkConfig` | 19 |
| `mysql` | broker-connectors | sink | mvp | `MySqlSinkConfig` | 5 |
| `oci_streaming` | broker-connectors-enterprise | sink | out-of-mvp | `OciStreamingSinkConfig` | 17 |
| `opc_ua` | broker-connectors-enterprise | both | mvp | `OpcUaSinkConfig` | 22 |
| `opentsdb` | broker-connectors | sink | mvp | `OpenTsdbConfig` | 15 |
| `oracle` | broker-connectors-enterprise | sink | mvp | `OracleSinkConfig` | 11 |
| `postgres` | broker-connectors | sink | mvp | `PostgreSqlSinkConfig` | 6 |
| `pulsar` | broker-connectors-enterprise | sink | mvp | `PulsarSinkConfig` | 18 |
| `rabbitmq` | broker-connectors | sink | mvp | `RabbitMqSinkConfig` | 6 |
| `redis` | broker-connectors | sink | mvp | `RedisSinkConfig` | 7 |
| `redshift` | broker-connectors-enterprise | sink | mvp | `RedshiftSinkConfig` | 19 |
| `rocketmq` | broker-connectors-enterprise | sink | mvp | `RocketMqSinkConfig` | 14 |
| `s3` | broker-connectors | sink | mvp | `S3SinkConfig` | 14 |
| `snowflake` | broker-connectors-enterprise | sink | mvp | `SnowflakeSinkConfig` | 18 |
| `sparkplug_b` | broker-connectors-enterprise | both | mvp | `SparkplugSinkConfig` | 9 |
| `tablestore` | broker-connectors-enterprise | sink | mvp | `TablestoreSinkConfig` | 27 |
| `tdengine` | broker-connectors-enterprise | sink | mvp | `TdengineSinkConfig` | 18 |
| `timescaledb` | broker-connectors | sink | mvp | `TimescaleDbSinkConfig` | 8 |
| `timestream` | broker-connectors-enterprise | sink | mvp | `TimestreamSinkConfig` | 22 |
| `webhook` | broker-connectors | sink | mvp | `HttpSinkConfig` | 33 |

## `alloydb`

Source: `crates/broker-connectors-enterprise/src/alloydb.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `alloydb.host` | `host` | `String` | endpoint |
| `alloydb.port` | `port` | `u16` | endpoint |
| `alloydb.database` | `database` | `String` | endpoint |
| `alloydb.username` | `username` | `String` | auth |
| `alloydb.auth` | `auth` | `AlloydbAuth` | auth |
| `alloydb.auth.Password` | `Password` | `AlloydbAuth` | auth |
| `alloydb.auth.IamToken` | `IamToken` | `AlloydbAuth` | auth |
| `alloydb.auth.IamAuto` | `IamAuto` | `AlloydbAuth` | auth |
| `alloydb.table` | `table` | `String` | endpoint |
| `alloydb.column_mappings` | `column_mappings` | `Vec<AlloydbColumnMapping>` | mapping |
| `alloydb.column_mappings.source_field` | `source_field` | `String` | mapping |
| `alloydb.column_mappings.db_column` | `db_column` | `String` | mapping |
| `alloydb.column_mappings.data_type` | `data_type` | `Option<String>` | other |
| `alloydb.batch_size` | `batch_size` | `Option<usize>` | batching |
| `alloydb.buffer_capacity` | `buffer_capacity` | `Option<usize>` | buffering |
| `alloydb.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `alloydb.tls` | `tls` | `Option<bool>` | tls |
| `alloydb.ca_bundle_pem` | `ca_bundle_pem` | `Option<String>` | tls |
| `alloydb.tls_ca_file` | `tls_ca_file` | `Option<String>` | tls |

## `aws_iot`

Source: `crates/broker-connectors-enterprise/src/aws_iot.rs`.
Direction: both. Status: out-of-mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `aws_iot.endpoint` | `endpoint` | `String` | endpoint |
| `aws_iot.region` | `region` | `String` | endpoint |
| `aws_iot.client_id` | `client_id` | `String` | mapping |
| `aws_iot.auth` | `auth` | `AwsIotAuth` | auth |
| `aws_iot.auth.Mtls` | `Mtls` | `AwsIotAuth` | auth |
| `aws_iot.auth.SigV4` | `SigV4` | `AwsIotAuth` | auth |
| `aws_iot.topic_mappings` | `topic_mappings` | `Vec<BridgeTopicMapping>` | mapping |
| `aws_iot.topic_mappings.local_topic` | `local_topic` | `String` | endpoint |
| `aws_iot.topic_mappings.remote_topic` | `remote_topic` | `String` | endpoint |
| `aws_iot.topic_mappings.direction` | `direction` | `BridgeDirection` | other |
| `aws_iot.topic_mappings.direction.LocalToRemote` | `LocalToRemote` | `BridgeDirection` | other |
| `aws_iot.topic_mappings.direction.RemoteToLocal` | `RemoteToLocal` | `BridgeDirection` | other |
| `aws_iot.topic_mappings.direction.BiDirectional` | `BiDirectional` | `BridgeDirection` | other |
| `aws_iot.shadow_sync` | `shadow_sync` | `Option<ShadowSyncConfig>` | other |
| `aws_iot.shadow_sync.thing_name_template` | `thing_name_template` | `String` | mapping |
| `aws_iot.batch_size` | `batch_size` | `Option<usize>` | batching |
| `aws_iot.buffer_capacity` | `buffer_capacity` | `Option<usize>` | buffering |
| `aws_iot.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `aws_iot.max_retries` | `max_retries` | `Option<usize>` | retry |
| `aws_iot.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `aws_iot.connect_timeout_ms` | `connect_timeout_ms` | `Option<u64>` | retry |
| `aws_iot.handshake_timeout_ms` | `handshake_timeout_ms` | `Option<u64>` | retry |
| `aws_iot.ca_bundle_pem` | `ca_bundle_pem` | `Option<String>` | tls |
| `aws_iot.alpn_protocols` | `alpn_protocols` | `Option<Vec<String>>` | mapping |

## `azure_blob`

Source: `crates/broker-connectors-enterprise/src/azure_blob.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `azure_blob.account_name` | `account_name` | `String` | other |
| `azure_blob.container_name` | `container_name` | `String` | endpoint |
| `azure_blob.endpoint` | `endpoint` | `Option<String>` | endpoint |
| `azure_blob.auth` | `auth` | `AzureBlobAuth` | auth |
| `azure_blob.auth.SharedKey` | `SharedKey` | `AzureBlobAuth` | auth |
| `azure_blob.auth.SasToken` | `SasToken` | `AzureBlobAuth` | auth |
| `azure_blob.auth.BearerToken` | `BearerToken` | `AzureBlobAuth` | auth |
| `azure_blob.blob_path_template` | `blob_path_template` | `String` | mapping |
| `azure_blob.compression` | `compression` | `AzureBlobCompression` | other |
| `azure_blob.compression.None` | `None` | `AzureBlobCompression` | other |
| `azure_blob.compression.Gzip` | `Gzip` | `AzureBlobCompression` | other |
| `azure_blob.max_records_per_blob` | `max_records_per_blob` | `Option<usize>` | other |
| `azure_blob.max_bytes_per_blob` | `max_bytes_per_blob` | `Option<usize>` | other |
| `azure_blob.flush_interval_secs` | `flush_interval_secs` | `u64` | batching |
| `azure_blob.buffer_capacity` | `buffer_capacity` | `Option<usize>` | buffering |
| `azure_blob.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `azure_blob.tls` | `tls` | `feature` | tls |

## `azure_eventhubs`

Source: `crates/broker-connectors-enterprise/src/azure_eventhubs.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `azure_eventhubs.namespace` | `namespace` | `String` | endpoint |
| `azure_eventhubs.event_hub` | `event_hub` | `String` | other |
| `azure_eventhubs.endpoint` | `endpoint` | `Option<String>` | endpoint |
| `azure_eventhubs.shared_access_key_name` | `shared_access_key_name` | `String` | auth |
| `azure_eventhubs.shared_access_key` | `shared_access_key` | `String` | auth |
| `azure_eventhubs.partition_key_template` | `partition_key_template` | `Option<String>` | mapping |
| `azure_eventhubs.user_properties` | `user_properties` | `HashMap<String, String>` | other |
| `azure_eventhubs.token_ttl_secs` | `token_ttl_secs` | `u64` | auth |
| `azure_eventhubs.batch_size` | `batch_size` | `Option<usize>` | batching |
| `azure_eventhubs.batch_bytes` | `batch_bytes` | `Option<usize>` | batching |
| `azure_eventhubs.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `azure_eventhubs.max_retries` | `max_retries` | `Option<usize>` | retry |
| `azure_eventhubs.initial_backoff_ms` | `initial_backoff_ms` | `Option<u64>` | retry |
| `azure_eventhubs.max_backoff_ms` | `max_backoff_ms` | `Option<u64>` | retry |
| `azure_eventhubs.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `azure_eventhubs.tls` | `tls` | `feature` | tls |

## `azure_iot`

Source: `crates/broker-connectors-enterprise/src/azure_iot.rs`.
Direction: both. Status: out-of-mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `azure_iot.iot_hub_name` | `iot_hub_name` | `String` | other |
| `azure_iot.device_id` | `device_id` | `String` | other |
| `azure_iot.module_id` | `module_id` | `Option<String>` | other |
| `azure_iot.auth` | `auth` | `AzureIotAuth` | auth |
| `azure_iot.auth.SharedAccessKey` | `SharedAccessKey` | `AzureIotAuth` | auth |
| `azure_iot.auth.SasToken` | `SasToken` | `AzureIotAuth` | auth |
| `azure_iot.auth.X509` | `X509` | `AzureIotAuth` | auth |
| `azure_iot.api_version` | `api_version` | `String` | other |
| `azure_iot.direct_methods_enabled` | `direct_methods_enabled` | `bool` | mapping |
| `azure_iot.twin_sync_enabled` | `twin_sync_enabled` | `bool` | other |
| `azure_iot.batch_size` | `batch_size` | `Option<usize>` | batching |
| `azure_iot.buffer_capacity` | `buffer_capacity` | `Option<usize>` | buffering |
| `azure_iot.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `azure_iot.max_retries` | `max_retries` | `Option<usize>` | retry |
| `azure_iot.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `azure_iot.connect_timeout_ms` | `connect_timeout_ms` | `Option<u64>` | retry |
| `azure_iot.handshake_timeout_ms` | `handshake_timeout_ms` | `Option<u64>` | retry |
| `azure_iot.ca_bundle_pem` | `ca_bundle_pem` | `Option<String>` | tls |
| `azure_iot.sas_ttl_secs` | `sas_ttl_secs` | `Option<u64>` | auth |

## `bigquery`

Source: `crates/broker-connectors-enterprise/src/bigquery.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `bigquery.project_id` | `project_id` | `String` | endpoint |
| `bigquery.dataset_id` | `dataset_id` | `String` | other |
| `bigquery.table_template` | `table_template` | `String` | mapping |
| `bigquery.endpoint` | `endpoint` | `Option<String>` | endpoint |
| `bigquery.auth` | `auth` | `GcpAuth` | auth |
| `bigquery.auth.None` | `None` | `GcpAuth` | auth |
| `bigquery.auth.AccessToken` | `AccessToken` | `GcpAuth` | auth |
| `bigquery.auth.ServiceAccountKey` | `ServiceAccountKey` | `GcpAuth` | auth |
| `bigquery.ignore_unknown_values` | `ignore_unknown_values` | `bool` | other |
| `bigquery.skip_invalid_rows` | `skip_invalid_rows` | `bool` | other |
| `bigquery.template_suffix` | `template_suffix` | `Option<String>` | mapping |
| `bigquery.batch_size` | `batch_size` | `Option<usize>` | batching |
| `bigquery.batch_bytes` | `batch_bytes` | `Option<usize>` | batching |
| `bigquery.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `bigquery.max_retries` | `max_retries` | `Option<usize>` | retry |
| `bigquery.initial_backoff_ms` | `initial_backoff_ms` | `Option<u64>` | retry |
| `bigquery.max_backoff_ms` | `max_backoff_ms` | `Option<u64>` | retry |
| `bigquery.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `bigquery.tls` | `tls` | `feature` | tls |

## `cassandra`

Source: `crates/broker-connectors-enterprise/src/cassandra.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `cassandra.contact_points` | `contact_points` | `Vec<String>` | other |
| `cassandra.keyspace` | `keyspace` | `String` | mapping |
| `cassandra.table_template` | `table_template` | `String` | mapping |
| `cassandra.auth` | `auth` | `CassandraAuth` | auth |
| `cassandra.auth.None` | `None` | `CassandraAuth` | auth |
| `cassandra.auth.Password` | `Password` | `CassandraAuth` | auth |
| `cassandra.consistency` | `consistency` | `CqlConsistency` | other |
| `cassandra.consistency.One` | `One` | `CqlConsistency` | other |
| `cassandra.consistency.Quorum` | `Quorum` | `CqlConsistency` | other |
| `cassandra.consistency.LocalQuorum` | `LocalQuorum` | `CqlConsistency` | other |
| `cassandra.consistency.All` | `All` | `CqlConsistency` | other |
| `cassandra.partition_key_template` | `partition_key_template` | `String` | mapping |
| `cassandra.cql_statement_template` | `cql_statement_template` | `String` | mapping |
| `cassandra.ttl_secs` | `ttl_secs` | `Option<u32>` | other |
| `cassandra.batch_size` | `batch_size` | `Option<usize>` | batching |
| `cassandra.batch_bytes` | `batch_bytes` | `Option<usize>` | batching |
| `cassandra.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `cassandra.max_retries` | `max_retries` | `Option<usize>` | retry |
| `cassandra.initial_backoff_ms` | `initial_backoff_ms` | `Option<u64>` | retry |
| `cassandra.max_backoff_ms` | `max_backoff_ms` | `Option<u64>` | retry |
| `cassandra.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |

## `clickhouse`

Source: `crates/broker-connectors/src/clickhouse.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `clickhouse.endpoint` | `endpoint` | `String` | endpoint |
| `clickhouse.database` | `database` | `String` | endpoint |
| `clickhouse.table` | `table` | `String` | endpoint |
| `clickhouse.format` | `format` | `String` | mapping |
| `clickhouse.batch_size` | `batch_size` | `usize` | batching |
| `clickhouse.batch_timeout_ms` | `batch_timeout_ms` | `u64` | batching |
| `clickhouse.username` | `username` | `String` | auth |
| `clickhouse.password` | `password` | `String` | auth |
| `clickhouse.request_timeout_ms` | `request_timeout_ms` | `Option<u64>` | retry |
| `clickhouse.tls` | `tls` | `feature` | tls |

## `cockroachdb`

Source: `crates/broker-connectors-enterprise/src/cockroachdb.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `cockroachdb.connection_string` | `connection_string` | `String` | other |
| `cockroachdb.table` | `table` | `String` | endpoint |
| `cockroachdb.upsert_conflict_columns` | `upsert_conflict_columns` | `Vec<String>` | mapping |
| `cockroachdb.batch_size` | `batch_size` | `Option<usize>` | batching |
| `cockroachdb.max_retry_attempts` | `max_retry_attempts` | `usize` | retry |
| `cockroachdb.buffer_capacity` | `buffer_capacity` | `Option<usize>` | buffering |
| `cockroachdb.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `cockroachdb.tls` | `tls` | `feature` | tls |

## `confluent`

Source: `crates/broker-connectors-enterprise/src/confluent.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `confluent.bootstrap_servers` | `bootstrap_servers` | `Vec<String>` | endpoint |
| `confluent.api_key` | `api_key` | `String` | auth |
| `confluent.api_secret` | `api_secret` | `String` | auth |
| `confluent.auth_mechanism` | `auth_mechanism` | `SaslMechanism` | auth |
| `confluent.auth_mechanism.Plain` | `Plain` | `SaslMechanism` | auth |
| `confluent.auth_mechanism.ScramSha256` | `ScramSha256` | `SaslMechanism` | auth |
| `confluent.auth_mechanism.ScramSha512` | `ScramSha512` | `SaslMechanism` | auth |
| `confluent.security_protocol` | `security_protocol` | `ConfluentSecurityProtocol` | mapping |
| `confluent.security_protocol.SaslSsl` | `SaslSsl` | `ConfluentSecurityProtocol` | mapping |
| `confluent.security_protocol.SaslPlaintext` | `SaslPlaintext` | `ConfluentSecurityProtocol` | mapping |
| `confluent.topic_template` | `topic_template` | `String` | mapping |
| `confluent.partition_key_template` | `partition_key_template` | `Option<String>` | mapping |
| `confluent.schema_registry` | `schema_registry` | `Option<ConfluentSchemaRegistryConfig>` | other |
| `confluent.schema_registry.endpoint` | `endpoint` | `String` | endpoint |
| `confluent.schema_registry.api_key` | `api_key` | `String` | auth |
| `confluent.schema_registry.api_secret` | `api_secret` | `String` | auth |
| `confluent.schema_registry.schema_id` | `schema_id` | `u32` | other |
| `confluent.partitions` | `partitions` | `u32` | other |
| `confluent.acks` | `acks` | `String` | other |
| `confluent.random_partition` | `random_partition` | `bool` | other |
| `confluent.headers_template` | `headers_template` | `Option<String>` | mapping |
| `confluent.extra_headers` | `extra_headers` | `Vec<(String, String)>` | mapping |
| `confluent.header_encode_mode` | `header_encode_mode` | `ConfluentHeaderEncodeMode` | mapping |
| `confluent.header_encode_mode.None` | `None` | `ConfluentHeaderEncodeMode` | mapping |
| `confluent.header_encode_mode.Json` | `Json` | `ConfluentHeaderEncodeMode` | mapping |
| `confluent.batch_size` | `batch_size` | `Option<usize>` | batching |
| `confluent.buffer_capacity` | `buffer_capacity` | `Option<usize>` | buffering |
| `confluent.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `confluent.tls` | `tls` | `feature` | tls |

## `console`

Source: `crates/broker-connectors/src/lib.rs`.
Direction: sink. Status: mvp.

No configuration fields.

## `couchbase`

Source: `crates/broker-connectors-enterprise/src/couchbase.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `couchbase.connection_string` | `connection_string` | `String` | other |
| `couchbase.bucket` | `bucket` | `String` | endpoint |
| `couchbase.scope` | `scope` | `Option<String>` | other |
| `couchbase.collection` | `collection` | `Option<String>` | endpoint |
| `couchbase.auth` | `auth` | `CouchbaseAuth` | auth |
| `couchbase.auth.username` | `username` | `String` | auth |
| `couchbase.auth.password` | `password` | `String` | auth |
| `couchbase.doc_id_template` | `doc_id_template` | `String` | mapping |
| `couchbase.sql_template` | `sql_template` | `Option<String>` | mapping |
| `couchbase.operation` | `operation` | `CouchbaseOperation` | other |
| `couchbase.operation.Upsert` | `Upsert` | `CouchbaseOperation` | other |
| `couchbase.operation.Insert` | `Insert` | `CouchbaseOperation` | other |
| `couchbase.operation.Replace` | `Replace` | `CouchbaseOperation` | other |
| `couchbase.expiry_secs` | `expiry_secs` | `Option<u32>` | other |
| `couchbase.batch_size` | `batch_size` | `Option<usize>` | batching |
| `couchbase.batch_bytes` | `batch_bytes` | `Option<usize>` | batching |
| `couchbase.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `couchbase.max_retries` | `max_retries` | `Option<usize>` | retry |
| `couchbase.initial_backoff_ms` | `initial_backoff_ms` | `Option<u64>` | retry |
| `couchbase.max_backoff_ms` | `max_backoff_ms` | `Option<u64>` | retry |
| `couchbase.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `couchbase.tls` | `tls` | `feature` | tls |

## `databricks`

Source: `crates/broker-connectors-enterprise/src/databricks.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `databricks.host` | `host` | `String` | endpoint |
| `databricks.token` | `token` | `String` | auth |
| `databricks.catalog` | `catalog` | `String` | other |
| `databricks.schema` | `schema` | `String` | other |
| `databricks.table_template` | `table_template` | `String` | mapping |
| `databricks.http_path` | `http_path` | `Option<String>` | endpoint |
| `databricks.partition_key_template` | `partition_key_template` | `Option<String>` | mapping |
| `databricks.column_mappings` | `column_mappings` | `HashMap<String, String>` | mapping |
| `databricks.batch_size` | `batch_size` | `Option<usize>` | batching |
| `databricks.batch_bytes` | `batch_bytes` | `Option<usize>` | batching |
| `databricks.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `databricks.max_retries` | `max_retries` | `Option<usize>` | retry |
| `databricks.initial_backoff_ms` | `initial_backoff_ms` | `Option<u64>` | retry |
| `databricks.max_backoff_ms` | `max_backoff_ms` | `Option<u64>` | retry |
| `databricks.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `databricks.tls` | `tls` | `feature` | tls |

## `datalayers`

Source: `crates/broker-connectors-enterprise/src/datalayers.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `datalayers.endpoint` | `endpoint` | `String` | endpoint |
| `datalayers.database` | `database` | `String` | endpoint |
| `datalayers.table` | `table` | `String` | endpoint |
| `datalayers.auth_token` | `auth_token` | `Option<String>` | auth |
| `datalayers.username` | `username` | `Option<String>` | auth |
| `datalayers.password` | `password` | `Option<String>` | auth |
| `datalayers.timestamp_field` | `timestamp_field` | `Option<String>` | mapping |
| `datalayers.tag_columns` | `tag_columns` | `Vec<String>` | mapping |
| `datalayers.field_columns` | `field_columns` | `Vec<String>` | mapping |
| `datalayers.precision` | `precision` | `Option<String>` | other |
| `datalayers.batch_size` | `batch_size` | `Option<usize>` | batching |
| `datalayers.buffer_capacity` | `buffer_capacity` | `Option<usize>` | buffering |
| `datalayers.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |

## `disk_log`

Source: `crates/broker-connectors/src/disk_log.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `disk_log.directory` | `directory` | `String` | other |
| `disk_log.filename_prefix` | `filename_prefix` | `String` | mapping |
| `disk_log.filename_extension` | `filename_extension` | `String` | other |
| `disk_log.format` | `format` | `DiskLogFormat` | mapping |
| `disk_log.format.Ndjson` | `Ndjson` | `DiskLogFormat` | mapping |
| `disk_log.format.Raw` | `Raw` | `DiskLogFormat` | mapping |
| `disk_log.format.Csv` | `Csv` | `DiskLogFormat` | mapping |
| `disk_log.max_file_size_bytes` | `max_file_size_bytes` | `Option<u64>` | other |
| `disk_log.max_file_age_secs` | `max_file_age_secs` | `Option<u64>` | other |
| `disk_log.compression` | `compression` | `DiskLogCompression` | other |
| `disk_log.compression.None` | `None` | `DiskLogCompression` | other |
| `disk_log.compression.Gzip` | `Gzip` | `DiskLogCompression` | other |
| `disk_log.max_backup_files` | `max_backup_files` | `Option<usize>` | other |
| `disk_log.max_retention_days` | `max_retention_days` | `Option<u32>` | other |
| `disk_log.sync_mode` | `sync_mode` | `DiskSyncMode` | other |
| `disk_log.sync_mode.EveryBatch` | `EveryBatch` | `DiskSyncMode` | other |
| `disk_log.sync_mode.Interval` | `Interval` | `DiskSyncMode` | other |
| `disk_log.sync_mode.OsDefault` | `OsDefault` | `DiskSyncMode` | other |
| `disk_log.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |

## `doris`

Source: `crates/broker-connectors-enterprise/src/doris.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `doris.fe_host` | `fe_host` | `String` | endpoint |
| `doris.http_port` | `http_port` | `u16` | endpoint |
| `doris.database` | `database` | `String` | endpoint |
| `doris.table_template` | `table_template` | `String` | mapping |
| `doris.auth` | `auth` | `DorisAuth` | auth |
| `doris.auth.username` | `username` | `String` | auth |
| `doris.auth.password` | `password` | `String` | auth |
| `doris.format` | `format` | `DorisFormat` | mapping |
| `doris.format.Json` | `Json` | `DorisFormat` | mapping |
| `doris.format.Csv` | `Csv` | `DorisFormat` | mapping |
| `doris.jsonpaths` | `jsonpaths` | `Option<String>` | endpoint |
| `doris.strip_outer_array` | `strip_outer_array` | `bool` | other |
| `doris.max_filter_ratio` | `max_filter_ratio` | `Option<f64>` | other |
| `doris.batch_size` | `batch_size` | `Option<usize>` | batching |
| `doris.batch_bytes` | `batch_bytes` | `Option<usize>` | batching |
| `doris.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `doris.max_retries` | `max_retries` | `Option<usize>` | retry |
| `doris.initial_backoff_ms` | `initial_backoff_ms` | `Option<u64>` | retry |
| `doris.max_backoff_ms` | `max_backoff_ms` | `Option<u64>` | retry |
| `doris.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `doris.tls` | `tls` | `feature` | tls |

## `dynamodb`

Source: `crates/broker-connectors-enterprise/src/dynamodb.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `dynamodb.table_name` | `table_name` | `String` | endpoint |
| `dynamodb.region` | `region` | `String` | endpoint |
| `dynamodb.endpoint` | `endpoint` | `Option<String>` | endpoint |
| `dynamodb.access_key_id` | `access_key_id` | `String` | auth |
| `dynamodb.secret_access_key` | `secret_access_key` | `String` | auth |
| `dynamodb.session_token` | `session_token` | `Option<String>` | auth |
| `dynamodb.partition_key` | `partition_key` | `DynamoKeyConfig` | mapping |
| `dynamodb.partition_key.name` | `name` | `String` | other |
| `dynamodb.partition_key.template` | `template` | `String` | mapping |
| `dynamodb.partition_key.key_type` | `key_type` | `String` | mapping |
| `dynamodb.sort_key` | `sort_key` | `Option<DynamoKeyConfig>` | mapping |
| `dynamodb.sort_key.name` | `name` | `String` | other |
| `dynamodb.sort_key.template` | `template` | `String` | mapping |
| `dynamodb.sort_key.key_type` | `key_type` | `String` | mapping |
| `dynamodb.ttl_attribute` | `ttl_attribute` | `Option<String>` | other |
| `dynamodb.ttl_secs` | `ttl_secs` | `Option<u64>` | other |
| `dynamodb.attributes_mapping` | `attributes_mapping` | `HashMap<String, String>` | mapping |
| `dynamodb.undefined_vars_as_null` | `undefined_vars_as_null` | `bool` | other |
| `dynamodb.batch_size` | `batch_size` | `Option<usize>` | batching |
| `dynamodb.batch_bytes` | `batch_bytes` | `Option<usize>` | batching |
| `dynamodb.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `dynamodb.max_retries` | `max_retries` | `Option<usize>` | retry |
| `dynamodb.initial_backoff_ms` | `initial_backoff_ms` | `Option<u64>` | retry |
| `dynamodb.max_backoff_ms` | `max_backoff_ms` | `Option<u64>` | retry |
| `dynamodb.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `dynamodb.tls` | `tls` | `feature` | tls |

## `elasticsearch`

Source: `crates/broker-connectors/src/elasticsearch.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `elasticsearch.endpoint` | `endpoint` | `String` | endpoint |
| `elasticsearch.index_template` | `index_template` | `String` | mapping |
| `elasticsearch.doc_id_template` | `doc_id_template` | `Option<String>` | mapping |
| `elasticsearch.operation` | `operation` | `ElasticsearchOperation` | other |
| `elasticsearch.operation.Index` | `Index` | `ElasticsearchOperation` | other |
| `elasticsearch.operation.Create` | `Create` | `ElasticsearchOperation` | other |
| `elasticsearch.operation.Update` | `Update` | `ElasticsearchOperation` | other |
| `elasticsearch.operation.Delete` | `Delete` | `ElasticsearchOperation` | other |
| `elasticsearch.doc_template` | `doc_template` | `Option<String>` | mapping |
| `elasticsearch.overwrite` | `overwrite` | `bool` | other |
| `elasticsearch.doc_as_upsert` | `doc_as_upsert` | `bool` | other |
| `elasticsearch.require_alias` | `require_alias` | `bool` | other |
| `elasticsearch.routing` | `routing` | `Option<String>` | other |
| `elasticsearch.auth` | `auth` | `ElasticsearchAuth` | auth |
| `elasticsearch.auth.None` | `None` | `ElasticsearchAuth` | auth |
| `elasticsearch.auth.Basic` | `Basic` | `ElasticsearchAuth` | auth |
| `elasticsearch.auth.ApiKey` | `ApiKey` | `ElasticsearchAuth` | auth |
| `elasticsearch.batch_size` | `batch_size` | `usize` | batching |
| `elasticsearch.batch_timeout_ms` | `batch_timeout_ms` | `u64` | batching |
| `elasticsearch.max_retries` | `max_retries` | `usize` | retry |
| `elasticsearch.request_timeout_ms` | `request_timeout_ms` | `Option<u64>` | retry |
| `elasticsearch.tls` | `tls` | `feature` | tls |

## `gcp_iot`

Source: `crates/broker-connectors-enterprise/src/gcp_iot.rs`.
Direction: both. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `gcp_iot.project_id` | `project_id` | `String` | endpoint |
| `gcp_iot.cloud_region` | `cloud_region` | `String` | endpoint |
| `gcp_iot.registry_id` | `registry_id` | `String` | other |
| `gcp_iot.device_id` | `device_id` | `String` | other |
| `gcp_iot.private_key_pem` | `private_key_pem` | `String` | mapping |
| `gcp_iot.algorithm` | `algorithm` | `GcpIotAlgorithm` | other |
| `gcp_iot.algorithm.Rs256` | `Rs256` | `GcpIotAlgorithm` | other |
| `gcp_iot.algorithm.Es256` | `Es256` | `GcpIotAlgorithm` | other |
| `gcp_iot.token_lifetime_secs` | `token_lifetime_secs` | `u64` | auth |
| `gcp_iot.endpoint` | `endpoint` | `String` | endpoint |
| `gcp_iot.batch_size` | `batch_size` | `Option<usize>` | batching |
| `gcp_iot.buffer_capacity` | `buffer_capacity` | `Option<usize>` | buffering |
| `gcp_iot.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `gcp_iot.max_retries` | `max_retries` | `Option<usize>` | retry |
| `gcp_iot.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `gcp_iot.ca_bundle_pem` | `ca_bundle_pem` | `Option<String>` | tls |

## `gcp_pubsub`

Source: `crates/broker-connectors-enterprise/src/gcp_pubsub.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `gcp_pubsub.project_id` | `project_id` | `String` | endpoint |
| `gcp_pubsub.topic_id` | `topic_id` | `String` | endpoint |
| `gcp_pubsub.endpoint` | `endpoint` | `Option<String>` | endpoint |
| `gcp_pubsub.auth` | `auth` | `GcpAuth` | auth |
| `gcp_pubsub.auth.None` | `None` | `GcpAuth` | auth |
| `gcp_pubsub.auth.AccessToken` | `AccessToken` | `GcpAuth` | auth |
| `gcp_pubsub.auth.ServiceAccountKey` | `ServiceAccountKey` | `GcpAuth` | auth |
| `gcp_pubsub.ordering_key_template` | `ordering_key_template` | `Option<String>` | mapping |
| `gcp_pubsub.attributes` | `attributes` | `HashMap<String, String>` | other |
| `gcp_pubsub.batch_size` | `batch_size` | `Option<usize>` | batching |
| `gcp_pubsub.batch_bytes` | `batch_bytes` | `Option<usize>` | batching |
| `gcp_pubsub.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `gcp_pubsub.max_retries` | `max_retries` | `Option<usize>` | retry |
| `gcp_pubsub.initial_backoff_ms` | `initial_backoff_ms` | `Option<u64>` | retry |
| `gcp_pubsub.max_backoff_ms` | `max_backoff_ms` | `Option<u64>` | retry |
| `gcp_pubsub.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `gcp_pubsub.tls` | `tls` | `feature` | tls |

## `greptimedb`

Source: `crates/broker-connectors/src/greptimedb.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `greptimedb.endpoint` | `endpoint` | `String` | endpoint |
| `greptimedb.database` | `database` | `String` | endpoint |
| `greptimedb.auth` | `auth` | `Option<GreptimeDbAuth>` | auth |
| `greptimedb.auth.username` | `username` | `String` | auth |
| `greptimedb.auth.password` | `password` | `String` | auth |
| `greptimedb.format` | `format` | `GreptimeFormat` | mapping |
| `greptimedb.format.SqlInsert` | `SqlInsert` | `GreptimeFormat` | mapping |
| `greptimedb.format.InfluxLineProtocol` | `InfluxLineProtocol` | `GreptimeFormat` | mapping |
| `greptimedb.table_template` | `table_template` | `String` | mapping |
| `greptimedb.timestamp_precision` | `timestamp_precision` | `GreptimePrecision` | other |
| `greptimedb.timestamp_precision.Nanosecond` | `Nanosecond` | `GreptimePrecision` | other |
| `greptimedb.timestamp_precision.Microsecond` | `Microsecond` | `GreptimePrecision` | other |
| `greptimedb.timestamp_precision.Millisecond` | `Millisecond` | `GreptimePrecision` | other |
| `greptimedb.timestamp_precision.Second` | `Second` | `GreptimePrecision` | other |
| `greptimedb.batch_size` | `batch_size` | `Option<usize>` | batching |
| `greptimedb.buffer_capacity` | `buffer_capacity` | `Option<usize>` | buffering |
| `greptimedb.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |

## `influxdb`

Source: `crates/broker-connectors/src/influxdb.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `influxdb.endpoint` | `endpoint` | `String` | endpoint |
| `influxdb.bucket` | `bucket` | `String` | endpoint |
| `influxdb.org` | `org` | `String` | other |
| `influxdb.token` | `token` | `String` | auth |
| `influxdb.measurement_template` | `measurement_template` | `String` | mapping |
| `influxdb.precision` | `precision` | `String` | other |
| `influxdb.batch_size` | `batch_size` | `usize` | batching |
| `influxdb.batch_timeout_ms` | `batch_timeout_ms` | `u64` | batching |
| `influxdb.request_timeout_ms` | `request_timeout_ms` | `Option<u64>` | retry |
| `influxdb.tls` | `tls` | `feature` | tls |

## `iotdb`

Source: `crates/broker-connectors-enterprise/src/iotdb.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `iotdb.endpoint` | `endpoint` | `String` | endpoint |
| `iotdb.device_path_template` | `device_path_template` | `String` | mapping |
| `iotdb.auth` | `auth` | `IotDbAuth` | auth |
| `iotdb.auth.username` | `username` | `String` | auth |
| `iotdb.auth.password` | `password` | `String` | auth |
| `iotdb.is_aligned` | `is_aligned` | `bool` | other |
| `iotdb.measurements` | `measurements` | `Vec<String>` | other |
| `iotdb.data_types` | `data_types` | `Vec<IotDbDataType>` | other |
| `iotdb.data_types.Boolean` | `Boolean` | `IotDbDataType` | other |
| `iotdb.data_types.Int32` | `Int32` | `IotDbDataType` | other |
| `iotdb.data_types.Int64` | `Int64` | `IotDbDataType` | other |
| `iotdb.data_types.Float` | `Float` | `IotDbDataType` | other |
| `iotdb.data_types.Double` | `Double` | `IotDbDataType` | other |
| `iotdb.data_types.Text` | `Text` | `IotDbDataType` | other |
| `iotdb.batch_size` | `batch_size` | `Option<usize>` | batching |
| `iotdb.batch_bytes` | `batch_bytes` | `Option<usize>` | batching |
| `iotdb.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `iotdb.max_retries` | `max_retries` | `Option<usize>` | retry |
| `iotdb.initial_backoff_ms` | `initial_backoff_ms` | `Option<u64>` | retry |
| `iotdb.max_backoff_ms` | `max_backoff_ms` | `Option<u64>` | retry |
| `iotdb.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `iotdb.tls` | `tls` | `feature` | tls |

## `kafka`

Source: `crates/broker-connectors/src/kafka.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `kafka.bootstrap_servers` | `bootstrap_servers` | `String` | endpoint |
| `kafka.topic_template` | `topic_template` | `String` | mapping |
| `kafka.partition_key_field` | `partition_key_field` | `Option<String>` | mapping |
| `kafka.key_template` | `key_template` | `Option<String>` | mapping |
| `kafka.partitions` | `partitions` | `u32` | other |
| `kafka.client_id` | `client_id` | `String` | mapping |
| `kafka.acks` | `acks` | `String` | other |
| `kafka.batch_max_records` | `batch_max_records` | `usize` | batching |
| `kafka.batch_max_bytes` | `batch_max_bytes` | `usize` | batching |
| `kafka.random_partition` | `random_partition` | `bool` | other |
| `kafka.headers_template` | `headers_template` | `Option<String>` | mapping |
| `kafka.extra_headers` | `extra_headers` | `Vec<(String, String)>` | mapping |
| `kafka.header_encode_mode` | `header_encode_mode` | `KafkaHeaderEncodeMode` | mapping |
| `kafka.header_encode_mode.None` | `None` | `KafkaHeaderEncodeMode` | mapping |
| `kafka.header_encode_mode.Json` | `Json` | `KafkaHeaderEncodeMode` | mapping |

## `kinesis`

Source: `crates/broker-connectors-enterprise/src/kinesis.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `kinesis.stream_name` | `stream_name` | `String` | other |
| `kinesis.region` | `region` | `String` | endpoint |
| `kinesis.endpoint` | `endpoint` | `Option<String>` | endpoint |
| `kinesis.access_key_id` | `access_key_id` | `String` | auth |
| `kinesis.secret_access_key` | `secret_access_key` | `String` | auth |
| `kinesis.session_token` | `session_token` | `Option<String>` | auth |
| `kinesis.partition_key_template` | `partition_key_template` | `Option<String>` | mapping |
| `kinesis.explicit_hash_key` | `explicit_hash_key` | `Option<String>` | mapping |
| `kinesis.batch_size` | `batch_size` | `Option<usize>` | batching |
| `kinesis.batch_bytes` | `batch_bytes` | `Option<usize>` | batching |
| `kinesis.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `kinesis.max_retries` | `max_retries` | `Option<usize>` | retry |
| `kinesis.initial_backoff_ms` | `initial_backoff_ms` | `Option<u64>` | retry |
| `kinesis.max_backoff_ms` | `max_backoff_ms` | `Option<u64>` | retry |
| `kinesis.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `kinesis.tls` | `tls` | `feature` | tls |

## `mongodb`

Source: `crates/broker-connectors-enterprise/src/mongodb.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `mongodb.connection_string` | `connection_string` | `String` | other |
| `mongodb.database` | `database` | `String` | endpoint |
| `mongodb.collection_template` | `collection_template` | `String` | mapping |
| `mongodb.payload_template` | `payload_template` | `Option<String>` | mapping |
| `mongodb.operation` | `operation` | `MongoOperation` | other |
| `mongodb.operation.InsertOne` | `InsertOne` | `MongoOperation` | other |
| `mongodb.operation.UpdateOne` | `UpdateOne` | `MongoOperation` | other |
| `mongodb.operation.ReplaceOne` | `ReplaceOne` | `MongoOperation` | other |
| `mongodb.batch_size` | `batch_size` | `Option<usize>` | batching |
| `mongodb.batch_bytes` | `batch_bytes` | `Option<usize>` | batching |
| `mongodb.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `mongodb.max_retries` | `max_retries` | `Option<usize>` | retry |
| `mongodb.initial_backoff_ms` | `initial_backoff_ms` | `Option<u64>` | retry |
| `mongodb.max_backoff_ms` | `max_backoff_ms` | `Option<u64>` | retry |
| `mongodb.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |

## `mqtt_bridge`

Source: `crates/broker-connectors/src/mqtt_bridge.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `mqtt_bridge.broker_address` | `broker_address` | `String` | endpoint |
| `mqtt_bridge.client_id` | `client_id` | `String` | mapping |
| `mqtt_bridge.clean_start` | `clean_start` | `bool` | other |
| `mqtt_bridge.username` | `username` | `Option<String>` | auth |
| `mqtt_bridge.password` | `password` | `Option<String>` | auth |
| `mqtt_bridge.keep_alive_secs` | `keep_alive_secs` | `u16` | retry |
| `mqtt_bridge.topic_prefix` | `topic_prefix` | `Option<String>` | mapping |
| `mqtt_bridge.topic_template` | `topic_template` | `Option<String>` | mapping |
| `mqtt_bridge.payload_template` | `payload_template` | `Option<String>` | mapping |
| `mqtt_bridge.qos_override` | `qos_override` | `Option<u8>` | mapping |
| `mqtt_bridge.retain_override` | `retain_override` | `Option<bool>` | mapping |
| `mqtt_bridge.max_inflight` | `max_inflight` | `Option<usize>` | buffering |
| `mqtt_bridge.max_batch_size` | `max_batch_size` | `Option<usize>` | batching |
| `mqtt_bridge.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `mqtt_bridge.protocol` | `protocol` | `MqttBridgeProtocol` | mapping |
| `mqtt_bridge.protocol.V311` | `V311` | `MqttBridgeProtocol` | mapping |
| `mqtt_bridge.protocol.V50` | `V50` | `MqttBridgeProtocol` | mapping |
| `mqtt_bridge.tls` | `tls` | `feature` | tls |

## `mssql`

Source: `crates/broker-connectors-enterprise/src/mssql.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `mssql.host` | `host` | `String` | endpoint |
| `mssql.port` | `port` | `Option<u16>` | endpoint |
| `mssql.database` | `database` | `String` | endpoint |
| `mssql.table_template` | `table_template` | `String` | mapping |
| `mssql.auth` | `auth` | `MssqlAuth` | auth |
| `mssql.auth.SqlPassword` | `SqlPassword` | `MssqlAuth` | auth |
| `mssql.auth.Integrated` | `Integrated` | `MssqlAuth` | auth |
| `mssql.auth.ActiveDirectoryPassword` | `ActiveDirectoryPassword` | `MssqlAuth` | auth |
| `mssql.query_mode` | `query_mode` | `MssqlQueryMode` | other |
| `mssql.query_mode.InsertJson` | `InsertJson` | `MssqlQueryMode` | other |
| `mssql.query_mode.CustomUpsert` | `CustomUpsert` | `MssqlQueryMode` | other |
| `mssql.trust_server_certificate` | `trust_server_certificate` | `bool` | tls |
| `mssql.batch_size` | `batch_size` | `Option<usize>` | batching |
| `mssql.batch_bytes` | `batch_bytes` | `Option<usize>` | batching |
| `mssql.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `mssql.max_retries` | `max_retries` | `Option<usize>` | retry |
| `mssql.initial_backoff_ms` | `initial_backoff_ms` | `Option<u64>` | retry |
| `mssql.max_backoff_ms` | `max_backoff_ms` | `Option<u64>` | retry |
| `mssql.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |

## `mysql`

Source: `crates/broker-connectors/src/mysql.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `mysql.connection_url` | `connection_url` | `String` | endpoint |
| `mysql.sql_template` | `sql_template` | `String` | mapping |
| `mysql.pool_size` | `pool_size` | `usize` | other |
| `mysql.batch_size` | `batch_size` | `usize` | batching |
| `mysql.batch_timeout_ms` | `batch_timeout_ms` | `u64` | batching |

## `oci_streaming`

Source: `crates/broker-connectors-enterprise/src/oci_streaming.rs`.
Direction: sink. Status: out-of-mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `oci_streaming.endpoint` | `endpoint` | `String` | endpoint |
| `oci_streaming.stream_pool_id` | `stream_pool_id` | `String` | other |
| `oci_streaming.stream_id` | `stream_id` | `String` | other |
| `oci_streaming.tenancy_ocid` | `tenancy_ocid` | `String` | other |
| `oci_streaming.user_ocid` | `user_ocid` | `String` | other |
| `oci_streaming.fingerprint` | `fingerprint` | `String` | other |
| `oci_streaming.private_key_pem` | `private_key_pem` | `String` | mapping |
| `oci_streaming.partition_key_template` | `partition_key_template` | `String` | mapping |
| `oci_streaming.batch_size` | `batch_size` | `Option<usize>` | batching |
| `oci_streaming.buffer_capacity` | `buffer_capacity` | `Option<usize>` | buffering |
| `oci_streaming.batch_bytes` | `batch_bytes` | `Option<usize>` | batching |
| `oci_streaming.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `oci_streaming.max_retries` | `max_retries` | `Option<usize>` | retry |
| `oci_streaming.initial_backoff_ms` | `initial_backoff_ms` | `Option<u64>` | retry |
| `oci_streaming.max_backoff_ms` | `max_backoff_ms` | `Option<u64>` | retry |
| `oci_streaming.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `oci_streaming.tls` | `tls` | `feature` | tls |

## `opc_ua`

Source: `crates/broker-connectors-enterprise/src/opc_ua.rs`.
Direction: both. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `opc_ua.endpoint_url` | `endpoint_url` | `String` | endpoint |
| `opc_ua.security_policy` | `security_policy` | `OpcUaSecurityPolicy` | other |
| `opc_ua.security_policy.None` | `None` | `OpcUaSecurityPolicy` | other |
| `opc_ua.security_policy.Basic256Sha256` | `Basic256Sha256` | `OpcUaSecurityPolicy` | other |
| `opc_ua.security_policy.Aes128Sha256RsaOaep` | `Aes128Sha256RsaOaep` | `OpcUaSecurityPolicy` | other |
| `opc_ua.security_mode` | `security_mode` | `OpcUaSecurityMode` | other |
| `opc_ua.security_mode.None` | `None` | `OpcUaSecurityMode` | other |
| `opc_ua.security_mode.Sign` | `Sign` | `OpcUaSecurityMode` | other |
| `opc_ua.security_mode.SignAndEncrypt` | `SignAndEncrypt` | `OpcUaSecurityMode` | other |
| `opc_ua.auth` | `auth` | `OpcUaAuth` | auth |
| `opc_ua.auth.Anonymous` | `Anonymous` | `OpcUaAuth` | auth |
| `opc_ua.auth.UsernamePassword` | `UsernamePassword` | `OpcUaAuth` | auth |
| `opc_ua.auth.Certificate` | `Certificate` | `OpcUaAuth` | auth |
| `opc_ua.node_subscriptions` | `node_subscriptions` | `Vec<NodeSubscriptionConfig>` | other |
| `opc_ua.node_subscriptions.node_id` | `node_id` | `String` | other |
| `opc_ua.node_subscriptions.sampling_interval_ms` | `sampling_interval_ms` | `u64` | other |
| `opc_ua.node_subscriptions.publish_topic_template` | `publish_topic_template` | `String` | mapping |
| `opc_ua.node_subscriptions.write_topic_pattern` | `write_topic_pattern` | `Option<String>` | endpoint |
| `opc_ua.buffer_capacity` | `buffer_capacity` | `Option<usize>` | buffering |
| `opc_ua.batch_size` | `batch_size` | `Option<usize>` | batching |
| `opc_ua.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `opc_ua.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |

## `opentsdb`

Source: `crates/broker-connectors/src/opentsdb.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `opentsdb.endpoint` | `endpoint` | `String` | endpoint |
| `opentsdb.protocol` | `protocol` | `OpenTsdbProtocol` | mapping |
| `opentsdb.protocol.Http` | `Http` | `OpenTsdbProtocol` | mapping |
| `opentsdb.protocol.Telnet` | `Telnet` | `OpenTsdbProtocol` | mapping |
| `opentsdb.metric_template` | `metric_template` | `String` | mapping |
| `opentsdb.tag_mappings` | `tag_mappings` | `HashMap<String, String>` | mapping |
| `opentsdb.value_field` | `value_field` | `String` | mapping |
| `opentsdb.summary` | `summary` | `bool` | other |
| `opentsdb.compression` | `compression` | `OpenTsdbCompression` | other |
| `opentsdb.compression.None` | `None` | `OpenTsdbCompression` | other |
| `opentsdb.compression.Gzip` | `Gzip` | `OpenTsdbCompression` | other |
| `opentsdb.batch_size` | `batch_size` | `Option<usize>` | batching |
| `opentsdb.buffer_capacity` | `buffer_capacity` | `Option<usize>` | buffering |
| `opentsdb.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `opentsdb.tls` | `tls` | `feature` | tls |

## `oracle`

Source: `crates/broker-connectors-enterprise/src/oracle.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `oracle.url` | `url` | `String` | endpoint |
| `oracle.schema` | `schema` | `String` | other |
| `oracle.table` | `table` | `String` | endpoint |
| `oracle.username` | `username` | `String` | auth |
| `oracle.password` | `password` | `String` | auth |
| `oracle.custom_upsert` | `custom_upsert` | `Option<String>` | other |
| `oracle.key_columns` | `key_columns` | `Vec<String>` | mapping |
| `oracle.batch_size` | `batch_size` | `Option<usize>` | batching |
| `oracle.buffer_capacity` | `buffer_capacity` | `Option<usize>` | buffering |
| `oracle.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `oracle.tls` | `tls` | `feature` | tls |

## `postgres`

Source: `crates/broker-connectors/src/postgres.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `postgres.connection_url` | `connection_url` | `String` | endpoint |
| `postgres.sql_template` | `sql_template` | `String` | mapping |
| `postgres.pool_size` | `pool_size` | `usize` | other |
| `postgres.batch_size` | `batch_size` | `usize` | batching |
| `postgres.batch_timeout_ms` | `batch_timeout_ms` | `u64` | batching |
| `postgres.tls` | `tls` | `feature` | tls |

## `pulsar`

Source: `crates/broker-connectors-enterprise/src/pulsar.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `pulsar.service_url` | `service_url` | `String` | endpoint |
| `pulsar.tenant` | `tenant` | `String` | other |
| `pulsar.namespace` | `namespace` | `String` | endpoint |
| `pulsar.topic` | `topic` | `String` | endpoint |
| `pulsar.auth` | `auth` | `PulsarAuth` | auth |
| `pulsar.auth.None` | `None` | `PulsarAuth` | auth |
| `pulsar.auth.Token` | `Token` | `PulsarAuth` | auth |
| `pulsar.partition_key_template` | `partition_key_template` | `Option<String>` | mapping |
| `pulsar.strategy` | `strategy` | `String` | other |
| `pulsar.properties` | `properties` | `HashMap<String, String>` | other |
| `pulsar.batch_size` | `batch_size` | `Option<usize>` | batching |
| `pulsar.batch_bytes` | `batch_bytes` | `Option<usize>` | batching |
| `pulsar.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `pulsar.max_retries` | `max_retries` | `Option<usize>` | retry |
| `pulsar.initial_backoff_ms` | `initial_backoff_ms` | `Option<u64>` | retry |
| `pulsar.max_backoff_ms` | `max_backoff_ms` | `Option<u64>` | retry |
| `pulsar.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `pulsar.tls` | `tls` | `feature` | tls |

## `rabbitmq`

Source: `crates/broker-connectors/src/rabbitmq.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `rabbitmq.endpoint` | `endpoint` | `String` | endpoint |
| `rabbitmq.exchange` | `exchange` | `String` | other |
| `rabbitmq.routing_key_template` | `routing_key_template` | `String` | mapping |
| `rabbitmq.delivery_mode` | `delivery_mode` | `u8` | other |
| `rabbitmq.extra_headers` | `extra_headers` | `Vec<(String, String)>` | mapping |
| `rabbitmq.payload_template` | `payload_template` | `Option<String>` | mapping |

## `redis`

Source: `crates/broker-connectors/src/redis.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `redis.endpoint` | `endpoint` | `String` | endpoint |
| `redis.command` | `command` | `RedisCommandKind` | other |
| `redis.command.Set` | `Set` | `RedisCommandKind` | other |
| `redis.command.HSet` | `HSet` | `RedisCommandKind` | other |
| `redis.command.Publish` | `Publish` | `RedisCommandKind` | other |
| `redis.command.XAdd` | `XAdd` | `RedisCommandKind` | other |
| `redis.command.Raw` | `Raw` | `RedisCommandKind` | other |

## `redshift`

Source: `crates/broker-connectors-enterprise/src/redshift.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `redshift.database` | `database` | `String` | endpoint |
| `redshift.table_template` | `table_template` | `String` | mapping |
| `redshift.cluster_identifier` | `cluster_identifier` | `Option<String>` | endpoint |
| `redshift.workgroup_name` | `workgroup_name` | `Option<String>` | other |
| `redshift.region` | `region` | `String` | endpoint |
| `redshift.endpoint` | `endpoint` | `Option<String>` | endpoint |
| `redshift.access_key_id` | `access_key_id` | `String` | auth |
| `redshift.secret_access_key` | `secret_access_key` | `String` | auth |
| `redshift.session_token` | `session_token` | `Option<String>` | auth |
| `redshift.db_user` | `db_user` | `Option<String>` | other |
| `redshift.sql_template` | `sql_template` | `Option<String>` | mapping |
| `redshift.batch_size` | `batch_size` | `Option<usize>` | batching |
| `redshift.batch_bytes` | `batch_bytes` | `Option<usize>` | batching |
| `redshift.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `redshift.max_retries` | `max_retries` | `Option<usize>` | retry |
| `redshift.initial_backoff_ms` | `initial_backoff_ms` | `Option<u64>` | retry |
| `redshift.max_backoff_ms` | `max_backoff_ms` | `Option<u64>` | retry |
| `redshift.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `redshift.tls` | `tls` | `feature` | tls |

## `rocketmq`

Source: `crates/broker-connectors-enterprise/src/rocketmq.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `rocketmq.endpoints` | `endpoints` | `Vec<String>` | endpoint |
| `rocketmq.topic` | `topic` | `String` | endpoint |
| `rocketmq.tag_template` | `tag_template` | `Option<String>` | mapping |
| `rocketmq.keys_template` | `keys_template` | `Option<String>` | mapping |
| `rocketmq.strategy` | `strategy` | `String` | other |
| `rocketmq.message_group_template` | `message_group_template` | `Option<String>` | mapping |
| `rocketmq.access_key` | `access_key` | `Option<String>` | auth |
| `rocketmq.secret_key` | `secret_key` | `Option<String>` | auth |
| `rocketmq.security_token` | `security_token` | `Option<String>` | auth |
| `rocketmq.batch_size` | `batch_size` | `Option<usize>` | batching |
| `rocketmq.buffer_capacity` | `buffer_capacity` | `Option<usize>` | buffering |
| `rocketmq.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `rocketmq.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `rocketmq.tls` | `tls` | `feature` | tls |

## `s3`

Source: `crates/broker-connectors/src/s3.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `s3.endpoint` | `endpoint` | `String` | endpoint |
| `s3.bucket` | `bucket` | `String` | endpoint |
| `s3.region` | `region` | `String` | endpoint |
| `s3.access_key_id` | `access_key_id` | `String` | auth |
| `s3.secret_access_key` | `secret_access_key` | `String` | auth |
| `s3.key_template` | `key_template` | `String` | mapping |
| `s3.compression` | `compression` | `S3Compression` | other |
| `s3.compression.None` | `None` | `S3Compression` | other |
| `s3.compression.Gzip` | `Gzip` | `S3Compression` | other |
| `s3.batch_size` | `batch_size` | `usize` | batching |
| `s3.batch_bytes` | `batch_bytes` | `usize` | batching |
| `s3.batch_timeout_ms` | `batch_timeout_ms` | `u64` | batching |
| `s3.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `s3.tls` | `tls` | `feature` | tls |

## `snowflake`

Source: `crates/broker-connectors-enterprise/src/snowflake.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `snowflake.account` | `account` | `String` | other |
| `snowflake.user` | `user` | `String` | other |
| `snowflake.database` | `database` | `String` | endpoint |
| `snowflake.schema` | `schema` | `String` | other |
| `snowflake.table_template` | `table_template` | `String` | mapping |
| `snowflake.private_key_pem` | `private_key_pem` | `String` | mapping |
| `snowflake.endpoint` | `endpoint` | `Option<String>` | endpoint |
| `snowflake.role` | `role` | `Option<String>` | other |
| `snowflake.channel` | `channel` | `String` | other |
| `snowflake.column_mappings` | `column_mappings` | `HashMap<String, String>` | mapping |
| `snowflake.batch_size` | `batch_size` | `Option<usize>` | batching |
| `snowflake.batch_bytes` | `batch_bytes` | `Option<usize>` | batching |
| `snowflake.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `snowflake.max_retries` | `max_retries` | `Option<usize>` | retry |
| `snowflake.initial_backoff_ms` | `initial_backoff_ms` | `Option<u64>` | retry |
| `snowflake.max_backoff_ms` | `max_backoff_ms` | `Option<u64>` | retry |
| `snowflake.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `snowflake.tls` | `tls` | `feature` | tls |

## `sparkplug_b`

Source: `crates/broker-connectors-enterprise/src/sparkplug_b.rs`.
Direction: both. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `sparkplug_b.topic_prefix` | `topic_prefix` | `Option<String>` | mapping |
| `sparkplug_b.tier` | `tier` | `String` | other |
| `sparkplug_b.batch_size` | `batch_size` | `Option<usize>` | batching |
| `sparkplug_b.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `sparkplug_b.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `sparkplug_b.mqtt_url` | `mqtt_url` | `Option<String>` | endpoint |
| `sparkplug_b.client_id` | `client_id` | `Option<String>` | mapping |
| `sparkplug_b.max_aliases` | `max_aliases` | `Option<usize>` | other |
| `sparkplug_b.tls` | `tls` | `feature` | tls |

## `tablestore`

Source: `crates/broker-connectors-enterprise/src/tablestore.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `tablestore.endpoint` | `endpoint` | `String` | endpoint |
| `tablestore.instance_name` | `instance_name` | `String` | other |
| `tablestore.table_name` | `table_name` | `String` | endpoint |
| `tablestore.access_key_id` | `access_key_id` | `String` | auth |
| `tablestore.access_key_secret` | `access_key_secret` | `String` | auth |
| `tablestore.primary_keys` | `primary_keys` | `Vec<PrimaryKeyMapping>` | mapping |
| `tablestore.primary_keys.name` | `name` | `String` | other |
| `tablestore.primary_keys.source` | `source` | `String` | other |
| `tablestore.primary_keys.data_type` | `data_type` | `PrimaryKeyType` | other |
| `tablestore.primary_keys.data_type.String` | `String` | `PrimaryKeyType` | other |
| `tablestore.primary_keys.data_type.Integer` | `Integer` | `PrimaryKeyType` | other |
| `tablestore.primary_keys.data_type.Binary` | `Binary` | `PrimaryKeyType` | other |
| `tablestore.attribute_columns` | `attribute_columns` | `Vec<AttributeColumnMapping>` | mapping |
| `tablestore.attribute_columns.name` | `name` | `String` | other |
| `tablestore.attribute_columns.source` | `source` | `String` | other |
| `tablestore.attribute_columns.data_type` | `data_type` | `AttributeColumnType` | other |
| `tablestore.attribute_columns.data_type.String` | `String` | `AttributeColumnType` | other |
| `tablestore.attribute_columns.data_type.Integer` | `Integer` | `AttributeColumnType` | other |
| `tablestore.attribute_columns.data_type.Double` | `Double` | `AttributeColumnType` | other |
| `tablestore.attribute_columns.data_type.Boolean` | `Boolean` | `AttributeColumnType` | other |
| `tablestore.attribute_columns.data_type.Binary` | `Binary` | `AttributeColumnType` | other |
| `tablestore.measurement` | `measurement` | `String` | other |
| `tablestore.batch_size` | `batch_size` | `Option<usize>` | batching |
| `tablestore.buffer_capacity` | `buffer_capacity` | `Option<usize>` | buffering |
| `tablestore.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `tablestore.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `tablestore.tls` | `tls` | `feature` | tls |

## `tdengine`

Source: `crates/broker-connectors-enterprise/src/tdengine.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `tdengine.endpoint` | `endpoint` | `String` | endpoint |
| `tdengine.database` | `database` | `String` | endpoint |
| `tdengine.stable_name` | `stable_name` | `String` | endpoint |
| `tdengine.subtable_template` | `subtable_template` | `String` | mapping |
| `tdengine.auth` | `auth` | `TdengineAuth` | auth |
| `tdengine.auth.Basic` | `Basic` | `TdengineAuth` | auth |
| `tdengine.auth.Token` | `Token` | `TdengineAuth` | auth |
| `tdengine.tags_template` | `tags_template` | `HashMap<String, String>` | mapping |
| `tdengine.metrics_template` | `metrics_template` | `HashMap<String, String>` | mapping |
| `tdengine.batch_size` | `batch_size` | `Option<usize>` | batching |
| `tdengine.batch_bytes` | `batch_bytes` | `Option<usize>` | batching |
| `tdengine.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `tdengine.buffer_capacity` | `buffer_capacity` | `Option<usize>` | buffering |
| `tdengine.max_retries` | `max_retries` | `Option<usize>` | retry |
| `tdengine.initial_backoff_ms` | `initial_backoff_ms` | `Option<u64>` | retry |
| `tdengine.max_backoff_ms` | `max_backoff_ms` | `Option<u64>` | retry |
| `tdengine.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `tdengine.tls` | `tls` | `feature` | tls |

## `timescaledb`

Source: `crates/broker-connectors/src/timescaledb.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `timescaledb.connection_url` | `connection_url` | `String` | endpoint |
| `timescaledb.hypertable` | `hypertable` | `String` | endpoint |
| `timescaledb.time_column` | `time_column` | `String` | mapping |
| `timescaledb.sql_template` | `sql_template` | `String` | mapping |
| `timescaledb.pool_size` | `pool_size` | `usize` | other |
| `timescaledb.batch_size` | `batch_size` | `usize` | batching |
| `timescaledb.batch_timeout_ms` | `batch_timeout_ms` | `u64` | batching |
| `timescaledb.tls` | `tls` | `feature` | tls |

## `timestream`

Source: `crates/broker-connectors-enterprise/src/timestream.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `timestream.database_name` | `database_name` | `String` | endpoint |
| `timestream.table_name` | `table_name` | `String` | endpoint |
| `timestream.region` | `region` | `String` | endpoint |
| `timestream.endpoint` | `endpoint` | `Option<String>` | endpoint |
| `timestream.access_key_id` | `access_key_id` | `String` | auth |
| `timestream.secret_access_key` | `secret_access_key` | `String` | auth |
| `timestream.session_token` | `session_token` | `Option<String>` | auth |
| `timestream.dimensions` | `dimensions` | `HashMap<String, String>` | other |
| `timestream.time_unit` | `time_unit` | `TimestreamTimeUnit` | other |
| `timestream.time_unit.Milliseconds` | `Milliseconds` | `TimestreamTimeUnit` | other |
| `timestream.time_unit.Microseconds` | `Microseconds` | `TimestreamTimeUnit` | other |
| `timestream.time_unit.Nanoseconds` | `Nanoseconds` | `TimestreamTimeUnit` | other |
| `timestream.measure_name_template` | `measure_name_template` | `Option<String>` | mapping |
| `timestream.multi_measure_mappings` | `multi_measure_mappings` | `HashMap<String, String>` | mapping |
| `timestream.batch_size` | `batch_size` | `Option<usize>` | batching |
| `timestream.batch_bytes` | `batch_bytes` | `Option<usize>` | batching |
| `timestream.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `timestream.max_retries` | `max_retries` | `Option<usize>` | retry |
| `timestream.initial_backoff_ms` | `initial_backoff_ms` | `Option<u64>` | retry |
| `timestream.max_backoff_ms` | `max_backoff_ms` | `Option<u64>` | retry |
| `timestream.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `timestream.tls` | `tls` | `feature` | tls |

## `webhook`

Source: `crates/broker-connectors/src/http.rs`.
Direction: sink. Status: mvp.

| Item | Field | Type | Category |
|---|---|---|---|
| `webhook.url` | `url` | `String` | endpoint |
| `webhook.method` | `method` | `HttpMethod` | mapping |
| `webhook.method.Post` | `Post` | `HttpMethod` | mapping |
| `webhook.method.Put` | `Put` | `HttpMethod` | mapping |
| `webhook.method.Patch` | `Patch` | `HttpMethod` | mapping |
| `webhook.headers` | `headers` | `HashMap<String, String>` | mapping |
| `webhook.auth` | `auth` | `HttpAuth` | auth |
| `webhook.auth.None` | `None` | `HttpAuth` | auth |
| `webhook.auth.Basic` | `Basic` | `HttpAuth` | auth |
| `webhook.auth.Bearer` | `Bearer` | `HttpAuth` | auth |
| `webhook.auth.ApiKey` | `ApiKey` | `HttpAuth` | auth |
| `webhook.body_format` | `body_format` | `HttpBodyFormat` | mapping |
| `webhook.body_format.RawJson` | `RawJson` | `HttpBodyFormat` | mapping |
| `webhook.body_format.JsonBatchArray` | `JsonBatchArray` | `HttpBodyFormat` | mapping |
| `webhook.body_format.FormUrlEncoded` | `FormUrlEncoded` | `HttpBodyFormat` | mapping |
| `webhook.signature` | `signature` | `Option<HttpHmacSignature>` | auth |
| `webhook.signature.header_name` | `header_name` | `String` | mapping |
| `webhook.signature.algorithm` | `algorithm` | `HmacAlgorithm` | other |
| `webhook.signature.algorithm.Sha256` | `Sha256` | `HmacAlgorithm` | other |
| `webhook.signature.algorithm.Sha1` | `Sha1` | `HmacAlgorithm` | other |
| `webhook.signature.secret` | `secret` | `String` | auth |
| `webhook.signature.encoding` | `encoding` | `HmacEncoding` | other |
| `webhook.signature.encoding.Hex` | `Hex` | `HmacEncoding` | other |
| `webhook.signature.encoding.Base64` | `Base64` | `HmacEncoding` | other |
| `webhook.batch_size` | `batch_size` | `Option<usize>` | batching |
| `webhook.batch_bytes` | `batch_bytes` | `Option<usize>` | batching |
| `webhook.linger_ms` | `linger_ms` | `Option<u64>` | batching |
| `webhook.timeout_ms` | `timeout_ms` | `Option<u64>` | retry |
| `webhook.max_retries` | `max_retries` | `Option<usize>` | retry |
| `webhook.initial_backoff_ms` | `initial_backoff_ms` | `Option<u64>` | retry |
| `webhook.max_backoff_ms` | `max_backoff_ms` | `Option<u64>` | retry |
| `webhook.buffer_capacity` | `buffer_capacity` | `Option<usize>` | buffering |
| `webhook.tls` | `tls` | `feature` | tls |
