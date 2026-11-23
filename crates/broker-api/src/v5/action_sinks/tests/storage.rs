use super::*;

#[test]
fn canonical_action_kind_maps_every_mv02d_alias() {
    assert_eq!(canonical_action_kind("minio"), "s3");
    assert_eq!(canonical_action_kind("azure_blob_storage"), "azure_blob");
    assert_eq!(canonical_action_kind("dynamo"), "dynamodb");
    assert_eq!(canonical_action_kind("ddb"), "dynamodb");
    assert_eq!(canonical_action_kind("bq"), "bigquery");
    assert_eq!(canonical_action_kind("gbq"), "bigquery");
    assert_eq!(canonical_action_kind("snowflake_streaming"), "snowflake");
    assert_eq!(canonical_action_kind("snowflake_aggregated"), "snowflake");
    assert_eq!(canonical_action_kind("ots"), "tablestore");
    assert_eq!(canonical_action_kind("delta_lake"), "databricks");
}

#[test]
fn action_owned_kind_covers_the_mv02d_kinds() {
    for kind in [
        "s3",
        "minio",
        "azure_blob",
        "azure_blob_storage",
        "dynamodb",
        "dynamo",
        "ddb",
        "bigquery",
        "bq",
        "gbq",
        "disk_log",
        "snowflake",
        "snowflake_streaming",
        "snowflake_aggregated",
        "tablestore",
        "ots",
    ] {
        assert!(action_owned_kind(kind), "{kind} owns a runtime");
    }
    assert!(!action_owned_kind("databricks"));
    assert!(!action_owned_kind("delta_lake"));
}

#[cfg(feature = "s3")]
#[tokio::test]
async fn s3_action_builder_requires_the_bucket_and_key() {
    let rec = mv02d_record("act", "s3", "s3:up", serde_json::json!({}));
    let doc = serde_json::json!({"endpoint": "http://127.0.0.1:9000"});
    let error = build_s3_object_sink(&rec, &doc, &ActionResourceOpts::default())
        .err()
        .expect("a missing bucket refuses");
    assert!(error.contains("bucket"), "{error}");
    let rec = mv02d_record(
        "act",
        "minio",
        "s3:up",
        serde_json::json!({"bucket": "test-bucket", "key": "k/${topic}.ndjson"}),
    );
    let sink = build_s3_object_sink(&rec, &doc, &ActionResourceOpts::default())
        .expect("a full s3 pair builds");
    assert_eq!(sink.kind(), "s3");
}

#[cfg(feature = "azure_blob")]
#[tokio::test]
async fn azure_blob_action_builder_requires_the_container_and_blob() {
    let rec = mv02d_record("act", "azure_blob", "azure:up", serde_json::json!({}));
    let doc = serde_json::json!({"account_name": "acct", "account_key": "a2V5"});
    let error = build_azure_blob_sink(&rec, &doc, &ActionResourceOpts::default())
        .err()
        .expect("a missing container refuses");
    assert!(error.contains("container"), "{error}");
    let rec = mv02d_record(
        "act",
        "azure_blob_storage",
        "azure:up",
        serde_json::json!({"container": "test-container", "blob": "b/${batch_id}.json"}),
    );
    let sink = build_azure_blob_sink(&rec, &doc, &ActionResourceOpts::default())
        .expect("a full azure pair builds");
    assert_eq!(sink.kind(), "azure_blob");
}

#[cfg(feature = "dynamodb")]
#[tokio::test]
async fn dynamodb_action_builder_requires_the_table_and_hash_key() {
    let rec = mv02d_record("act", "dynamo", "ddb:up", serde_json::json!({}));
    let doc = serde_json::json!({
        "url": "http://127.0.0.1:8000",
        "region": "us-east-1",
        "aws_access_key_id": "AKID",
        "aws_secret_access_key": "secret",
    });
    let error = build_dynamodb_action_sink(&rec, &doc, &ActionResourceOpts::default())
        .err()
        .expect("a missing table refuses");
    assert!(error.contains("table"), "{error}");
    let rec = mv02d_record(
        "act",
        "ddb",
        "ddb:up",
        serde_json::json!({
            "table": "events",
            "hash_key": "id",
            "template": "{\"id\":\"${payload.id}\",\"temp\":\"${payload.temp}\"}",
        }),
    );
    let sink = build_dynamodb_action_sink(&rec, &doc, &ActionResourceOpts::default())
        .expect("a full dynamo pair builds");
    assert_eq!(sink.kind(), "dynamodb");
}

#[cfg(feature = "bigquery")]
#[tokio::test]
async fn bigquery_action_builder_requires_the_dataset_and_table() {
    let rec = mv02d_record("act", "bigquery", "bq:up", serde_json::json!({}));
    let doc = serde_json::json!({
        "authentication": {"type": "attached_service_account", "gcp_project_id": "p",
                           "access_token": "test-attached-token"},
    });
    let error = build_bigquery_sink(&rec, &doc, &ActionResourceOpts::default())
        .err()
        .expect("a missing dataset refuses");
    assert!(error.contains("dataset"), "{error}");
    let rec = mv02d_record(
        "act",
        "bq",
        "bq:up",
        serde_json::json!({"dataset": "d", "table": "events"}),
    );
    let sink = build_bigquery_sink(&rec, &doc, &ActionResourceOpts::default())
        .expect("a full bigquery pair builds");
    assert_eq!(sink.kind(), "bigquery");
    // The attached form uses the runtime environment.
    // It builds without a configured token.
    let bare = serde_json::json!({
        "authentication": {"type": "attached_service_account", "gcp_project_id": "p"},
    });
    let sink = build_bigquery_sink(&rec, &bare, &ActionResourceOpts::default())
        .expect("attached without token builds");
    assert_eq!(sink.kind(), "bigquery");
}

#[tokio::test]
async fn disk_log_action_builder_refuses_a_path_outside_the_data_directory() {
    let doc = serde_json::json!({
        "filepath": "../escape.log",
        "max_file_size": "1MB",
        "max_file_number": 3,
    });
    let rec = mv02d_record("act", "disk_log", "disk:up", serde_json::json!({}));
    let error = build_disk_log_action_sink(&rec, &doc, &ActionResourceOpts::default())
        .err()
        .expect("a parent path refuses");
    assert!(error.contains("leave the data directory"), "{error}");
    let doc = serde_json::json!({
        "filepath": "/tmp/escape.log",
        "max_file_size": "1MB",
        "max_file_number": 3,
    });
    let error = build_disk_log_action_sink(&rec, &doc, &ActionResourceOpts::default())
        .err()
        .expect("an absolute path refuses");
    assert!(error.contains("relative to the data directory"), "{error}");
    // A symlink that points out is refused, even with a clean name.
    let dir = std::env::temp_dir().join(format!(
        "mv02d-disklink-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let outside = dir.join("outside.log");
    std::fs::write(&outside, b"x").expect("outside file");
    let link = dir.join("link.log");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside, &link).expect("symlink");
    #[cfg(unix)]
    {
        let error =
            disk_log_parts(link.to_str().expect("path"), "act").expect_err("a symlink refuses");
        assert!(error.contains("symlink"), "{error}");
    }
    std::fs::remove_dir_all(&dir).ok();
}

async fn disk_log_write_modes_check(scratch: &std::path::Path) {
    use broker_connectors::disk_log::{DiskLogSink, MemoryDiskLogWriter};
    // Each write mode delivers through the runtime. The flush count
    // changes with the mode: `sync` flushes each batch, `async`
    // leaves sync to the OS. The memory sink clones the entry
    // config, so the test sets no field by hand.
    for (mode, flushes) in [("sync", 1_u64), ("async", 0_u64)] {
        let rec = mv02d_record(
            "act",
            "disk_log",
            "disk:up",
            serde_json::json!({"write_mode": mode}),
        );
        let doc = serde_json::json!({
            "filepath": format!("logs/mv02d-{mode}.log"),
            "max_file_size": "1MB",
            "max_file_number": 3,
        });
        let file_sink = build_disk_log_action_sink_with_dir(
            &rec,
            &doc,
            scratch,
            &ActionResourceOpts::default(),
        )
        .expect("both write modes build");
        assert_eq!(file_sink.kind(), "disk_log");
        let config = file_sink.config().clone();
        let metrics = std::sync::Arc::new(ActionMetrics::new());
        let runtime = ActionRuntimeSink::new(
            file_sink as std::sync::Arc<dyn broker_connectors::Sink>,
            ActionResourceOpts::default(),
            std::sync::Arc::clone(&metrics),
            None,
        );
        let topic = Topic::new("t/mode").unwrap();
        runtime
            .send(&topic, &Bytes::from("{}"), QoS::AtMostOnce)
            .await
            .expect("runtime sends the mode");
        assert_eq!(
            metrics.snapshot().success,
            1,
            "mode {mode} delivers through the runtime"
        );
        let writer =
            std::sync::Arc::new(MemoryDiskLogWriter::new(&config).expect("entry config opens"));
        let mem_sink = std::sync::Arc::new(
            DiskLogSink::new(config, writer.clone()).expect("memory sink builds"),
        );
        let mem_metrics = std::sync::Arc::new(ActionMetrics::new());
        let mem_runtime = ActionRuntimeSink::new(
            std::sync::Arc::clone(&mem_sink) as std::sync::Arc<dyn broker_connectors::Sink>,
            ActionResourceOpts::default(),
            std::sync::Arc::clone(&mem_metrics),
            None,
        );
        mem_runtime
            .send(&topic, &Bytes::from("{}"), QoS::AtMostOnce)
            .await
            .expect("memory runtime sends the mode");
        assert_eq!(
            mem_metrics.snapshot().success,
            1,
            "mode {mode} delivers in memory"
        );
        assert_eq!(
            writer.flush_count(),
            flushes,
            "write mode {mode} flushes {flushes} time(s)"
        );
    }
}

async fn disk_log_retention_check(scratch: &std::path::Path) {
    use broker_connectors::disk_log::{DiskLogSink, MemoryDiskLogWriter};
    // `rotation.retention_period` builds through the broker entry.
    // Delivery through the runtime is proven in the mode check
    // and the period check. This check keeps the entry builds
    // and the pruning difference.
    let mut retention_configs = Vec::new();
    for (tag, retention) in [
        ("one", serde_json::json!(1)),
        ("unlimited", serde_json::json!("infinity")),
    ] {
        let doc = serde_json::json!({
            "filepath": format!("logs/mv02d-ret-{tag}.log"),
            "max_file_size": "1MB",
            "max_file_number": 10,
            "rotation": {"period": "hour", "retention_period": retention.clone()},
        });
        let rec = mv02d_record("act", "disk_log", "disk:up", serde_json::json!({}));
        let file_sink = build_disk_log_action_sink_with_dir(
            &rec,
            &doc,
            scratch,
            &ActionResourceOpts::default(),
        )
        .expect("both retention values build");
        assert_eq!(file_sink.kind(), "disk_log");
        retention_configs.push((tag, file_sink.config().clone()));
    }
    // `rotation.retention_period` changes pruning at the driver.
    // One day keeps one fresh backup, no limit keeps three.
    // Explicit timestamps keep the test exact. The configs come
    // from the entry builds above, so no field is set by hand.
    // Four writes land one hour apart, then one lands after one day.
    let mut kept = Vec::new();
    let topic = Topic::new("t").unwrap();
    for (tag, config) in retention_configs {
        let writer =
            std::sync::Arc::new(MemoryDiskLogWriter::new(&config).expect("entry config opens"));
        let sink = DiskLogSink::new(config, writer.clone()).expect("memory sink builds");
        for millis in [0_i64, 3_700_000, 7_400_000, 92_000_000] {
            sink.append_at(&topic, &Bytes::from("{}"), QoS::AtMostOnce, millis)
                .await
                .unwrap();
        }
        kept.push((tag, writer.backup_blobs().len(), sink.backup_names().len()));
    }
    assert_eq!(kept[0].1, 1, "one day keeps one backup");
    assert_eq!(kept[0].2, 1, "one day lists one backup");
    assert_eq!(kept[1].1, 3, "no limit keeps three backups");
    assert_eq!(kept[1].2, 3, "no limit lists three backups");
}

async fn disk_log_period_check(scratch: &std::path::Path) {
    use broker_connectors::disk_log::{DiskLogSink, MemoryDiskLogWriter};
    // `rotation.period` changes age rotation at the driver.
    // `hour` rotates a segment older than 3_600 s, `none` never
    // rotates by age. Explicit timestamps keep the test exact.
    for (period, rotates) in [("hour", 1_u64), ("none", 0_u64)] {
        let doc = serde_json::json!({
            "filepath": format!("logs/mv02d-age-{period}.log"),
            "max_file_size": "1MB",
            "max_file_number": 3,
            "rotation": {"period": period},
        });
        let rec = mv02d_record("act", "disk_log", "disk:up", serde_json::json!({}));
        let file_sink = build_disk_log_action_sink_with_dir(
            &rec,
            &doc,
            scratch,
            &ActionResourceOpts::default(),
        )
        .expect("both periods build");
        assert_eq!(file_sink.kind(), "disk_log");
        let config = file_sink.config().clone();
        let metrics = std::sync::Arc::new(ActionMetrics::new());
        let runtime = ActionRuntimeSink::new(
            file_sink as std::sync::Arc<dyn broker_connectors::Sink>,
            ActionResourceOpts::default(),
            std::sync::Arc::clone(&metrics),
            None,
        );
        let topic = Topic::new("t/age").unwrap();
        runtime
            .send(&topic, &Bytes::from("{}"), QoS::AtMostOnce)
            .await
            .expect("runtime sends with period");
        assert_eq!(
            metrics.snapshot().success,
            1,
            "period {period} delivers through the runtime"
        );
        let writer =
            std::sync::Arc::new(MemoryDiskLogWriter::new(&config).expect("entry config opens"));
        let sink = DiskLogSink::new(config, writer).expect("memory sink builds");
        sink.append_at(&topic, &Bytes::from("{}"), QoS::AtMostOnce, 0)
            .await
            .unwrap();
        sink.append_at(&topic, &Bytes::from("{}"), QoS::AtMostOnce, 3_601_000)
            .await
            .unwrap();
        assert_eq!(
            sink.rotation_count(),
            rotates,
            "period {period} rotates by age: {rotates}"
        );
    }
}

#[tokio::test]
async fn disk_log_rotation_and_write_mode_change_the_runtime_result() {
    // Both documented write modes build through the broker entry.
    // One scratch directory serves all three legs. Distinct file
    // names keep isolation. The final call cleans the directory.
    let scratch = std::env::temp_dir().join(format!(
        "mv02d-diskmodes-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    disk_log_write_modes_check(&scratch).await;
    disk_log_retention_check(&scratch).await;
    disk_log_period_check(&scratch).await;
    std::fs::remove_dir_all(&scratch).ok();
}

#[tokio::test]
async fn snowflake_and_tablestore_builders_refuse_the_missing_action_fields() {
    let doc = serde_json::json!({
        "account": "acct",
        "pipe_user": "user",
        "private_key": "not-a-key",
    });
    let rec = mv02d_record("act", "snowflake", "sf:up", serde_json::json!({}));
    let error = build_snowflake_sink(&rec, &doc, &ActionResourceOpts::default())
        .err()
        .expect("a missing database refuses");
    assert!(error.contains("database"), "{error}");

    let doc = serde_json::json!({
        "endpoint": "http://127.0.0.1:9000",
        "instance_name": "i",
        "access_key_id": "ak",
        "access_key_secret": "sk",
    });
    let rec = mv02d_record("act", "ots", "ots:up", serde_json::json!({}));
    let error = build_tablestore_sink(&rec, &doc, &ActionResourceOpts::default())
        .err()
        .expect("a missing table refuses");
    assert!(error.contains("table_name"), "{error}");
    let rec = mv02d_record(
        "act",
        "tablestore",
        "ots:up",
        serde_json::json!({
            "table_name": "t",
            "measurement": "m",
            "tags": {"device": "${client_id}"},
            "fields": [{"column": "temp", "value": "${payload.temp}"}],
        }),
    );
    let sink = build_tablestore_sink(&rec, &doc, &ActionResourceOpts::default())
        .expect("a full tablestore pair builds");
    assert_eq!(sink.kind(), "tablestore");
}

#[cfg(feature = "bigquery")]
#[test]
fn bigquery_auth_refuses_bad_service_account_json() {
    let rec = record("act", "bigquery", "bq:up");
    let doc = serde_json::json!({
        "authentication": {"type": "service_account_json", "service_account_json": "{not json"}
    });
    let error = bigquery_auth(&doc, &rec.connector, &rec.id).expect_err("bad JSON refuses");
    assert!(
        error.contains("authentication.service_account_json"),
        "names the field: {error}"
    );
}

#[cfg(feature = "bigquery")]
#[test]
fn bigquery_auth_refuses_missing_client_email() {
    let rec = record("act", "bigquery", "bq:up");
    let doc = serde_json::json!({
        "authentication": {"type": "service_account_json", "service_account_json": "{\"private_key\":\"pk\"}"}
    });
    let error =
        bigquery_auth(&doc, &rec.connector, &rec.id).expect_err("missing client_email refuses");
    assert!(
        error.contains("authentication.service_account_json"),
        "names the field: {error}"
    );
}

#[cfg(feature = "bigquery")]
#[test]
fn bigquery_auth_refuses_missing_private_key() {
    let rec = record("act", "bigquery", "bq:up");
    let doc = serde_json::json!({
        "authentication": {"type": "service_account_json", "service_account_json": "{\"client_email\":\"a@b\"}"}
    });
    let error =
        bigquery_auth(&doc, &rec.connector, &rec.id).expect_err("missing private_key refuses");
    assert!(
        error.contains("authentication.service_account_json"),
        "names the field: {error}"
    );
}

#[cfg(feature = "bigquery")]
#[test]
fn bigquery_auth_accepts_valid_service_account() {
    let rec = record("act", "bigquery", "bq:up");
    let doc = serde_json::json!({
        "authentication": {"type": "service_account_json", "service_account_json": "{\"client_email\":\"a@b\",\"private_key\":\"pk\",\"project_id\":\"proj\"}"}
    });
    let (project_id, _auth) =
        bigquery_auth(&doc, &rec.connector, &rec.id).expect("valid account builds");
    assert_eq!(project_id, "proj");
}
