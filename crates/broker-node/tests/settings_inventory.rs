//! Entry proof for the temporary accepted-settings inventory.

#[test]
fn az01_settings_through_the_real_broker() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = std::process::Command::new("python3")
        .arg(root.join("tests/settings_inventory/entry.py"))
        .arg(env!("CARGO_BIN_EXE_indramqtt"))
        .env(
            "AZ01_RABBITMQ_COMPILED",
            if cfg!(feature = "rabbitmq") { "1" } else { "0" },
        )
        .env(
            "AZ01_CLICKHOUSE_COMPILED",
            if cfg!(feature = "clickhouse") {
                "1"
            } else {
                "0"
            },
        )
        .env(
            "AZ01_ELASTICSEARCH_COMPILED",
            if cfg!(feature = "elasticsearch") {
                "1"
            } else {
                "0"
            },
        )
        .env(
            "AZ01_S3_COMPILED",
            if cfg!(feature = "s3") { "1" } else { "0" },
        )
        .env(
            "AZ01_AZURE_BLOB_COMPILED",
            if cfg!(feature = "azure_blob") {
                "1"
            } else {
                "0"
            },
        )
        .env(
            "AZ01_DYNAMODB_COMPILED",
            if cfg!(feature = "dynamodb") { "1" } else { "0" },
        )
        .env(
            "AZ01_KINESIS_COMPILED",
            if cfg!(feature = "kinesis") { "1" } else { "0" },
        )
        .env(
            "AZ01_BIGQUERY_COMPILED",
            if cfg!(feature = "bigquery") { "1" } else { "0" },
        )
        .current_dir(&root)
        .output()
        .expect("run the inventory entry checks");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stdout}\n{stderr}");
    assert!(stderr.contains("Ran "), "the entry suite must run cases");
    println!("{stdout}\n{stderr}");
}

#[test]
fn az01_typed_field_coverage_and_table_are_current() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for (script, args) in [("audit.py", &[][..]), ("render.py", &["--check"][..])] {
        let output = std::process::Command::new("python3")
            .arg(root.join("tests/settings_inventory").join(script))
            .args(args)
            .current_dir(&root)
            .output()
            .expect("run the inventory document checks");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{script}: {stdout}\n{stderr}");
        println!("{stdout}");
    }
}
