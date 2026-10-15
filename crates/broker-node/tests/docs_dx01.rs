//! Drift and coverage tests for the DX-01 reference framework.
//!
//! The generator owns the inventories. The entry builder owns the
//! CLI page. The verifier runs each CLI example through the real
//! binary. These tests pin the three together: committed JSON
//! matches a fresh generator run, the page matches a fresh entry
//! build, every CLI example verifies through `--help`, and every
//! CLI item owns text plus a verified file or a failure record.

use std::path::PathBuf;
use std::process::Command;

/// Locates the workspace root from this crate directory.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

/// Runs one docs tool. Returns its stdout on success.
fn run_tool(args: &[&str]) -> String {
    let output = Command::new("python3")
        .args(args)
        .current_dir(workspace_root())
        .output()
        .expect("python3 runs the docs tool");
    let text = String::from_utf8_lossy(&output.stdout).to_string();
    assert!(
        output.status.success(),
        "docs tool {args:?} failed:\n{text}"
    );
    text
}

/// Reads one committed JSON file under `docs/`.
fn read_json(relative: &str) -> serde_json::Value {
    let path = workspace_root().join(relative);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {relative}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {relative}: {e}"))
}

/// Lists every CLI item id. Mirrors `entries.py::cli_items`.
fn cli_item_ids(doc: &serde_json::Value) -> Vec<String> {
    let mut ids = Vec::new();
    for row in doc["binaries"][0]["flags"].as_array().expect("flags") {
        let flag = row["flag"].as_str().expect("flag name");
        let long = flag.split(", ").last().expect("long flag").trim();
        ids.push(format!("flag-{}", long.trim_start_matches('-')));
    }
    for row in doc["binaries"][1]["commands"].as_array().expect("commands") {
        let name = row["command"].as_str().expect("command name");
        let words: Vec<&str> = name.split_whitespace().collect();
        ids.push(format!("cmd-{}", words[1..].join("-")));
    }
    ids.push("bin-indramqtt".to_string());
    ids.push("bin-indra".to_string());
    ids
}

/// Counts verified and failed items of one area.
fn area_counts(area: &str, ids: &[String]) -> (usize, usize) {
    let root = workspace_root();
    let failures = root.join("docs").join("_verification").join("failures.md");
    let record = std::fs::read_to_string(&failures).unwrap_or_default();
    let mut verified = 0usize;
    let mut failed = 0usize;
    for id in ids {
        let path = root
            .join("docs")
            .join("_verified")
            .join(area)
            .join(format!("{id}.json"));
        if path.is_file() {
            verified += 1;
        } else if record.contains(&format!("DX-01 item `{id}`")) {
            failed += 1;
        }
    }
    (verified, failed)
}

#[test]
fn dx01_inventories_match_generator() {
    let text = run_tool(&["docs/_tools/inventory.py", "--check"]);
    assert!(text.contains("inventories match"), "{text}");
}

#[test]
fn dx01_cli_page_matches_entries() {
    let text = run_tool(&["docs/_tools/entries.py", "--check"]);
    assert!(text.contains("entries match"), "{text}");
}

#[test]
fn dx01_cli_examples_verify_through_entry() {
    // The verifier uses the binaries under test. Point it at them.
    let mut command = Command::new("python3");
    if let Some(built) = option_env!("CARGO_BIN_EXE_indramqtt") {
        command.env("INDRAMQTT_BIN", built);
    }
    if let Some(built) = option_env!("CARGO_BIN_EXE_indra") {
        command.env("INDRACLI_BIN", built);
    }
    let output = command
        .args(["docs/_tools/verify.py", "--area", "cli"])
        .current_dir(workspace_root())
        .output()
        .expect("python3 runs the verifier");
    let text = String::from_utf8_lossy(&output.stdout).to_string();
    assert!(output.status.success(), "verifier failed:\n{text}");
    assert!(text.contains("failed=0"), "{text}");
    // Each verified file holds a real run: exit 0, named match.
    let doc = read_json("docs/_inventory/cli.json");
    let ids = cli_item_ids(&doc);
    assert!(!ids.is_empty(), "inventory lists CLI items");
    for id in &ids {
        let record = read_json(&format!("docs/_verified/cli/{id}.json"));
        assert_eq!(record["returncode"], 0, "{id} exits 0");
        assert!(record["verified"] == true, "{id} is marked");
        let output = record["output"].as_str().unwrap_or("");
        assert!(!output.is_empty(), "{id} captures output");
    }
    println!("DX01_VERIFY items={} failed=0", ids.len());
}

#[test]
fn dx01_cli_coverage_is_complete() {
    let doc = read_json("docs/_inventory/cli.json");
    let ids = cli_item_ids(&doc);
    let root = workspace_root();
    let mut missing = Vec::new();
    for id in &ids {
        let text = root
            .join("docs")
            .join("_text")
            .join("cli")
            .join(format!("{id}.md"));
        assert!(text.is_file(), "{id} owns an explanation");
        let verified = root
            .join("docs")
            .join("_verified")
            .join("cli")
            .join(format!("{id}.json"));
        let record =
            std::fs::read_to_string(root.join("docs").join("_verification").join("failures.md"))
                .unwrap_or_default();
        if !verified.is_file() && !record.contains(&format!("DX-01 item `{id}`")) {
            missing.push(id.clone());
        }
    }
    let (verified, failed) = area_counts("cli", &ids);
    println!(
        "DX01_COVERAGE items={} verified={verified} failed={failed}",
        ids.len()
    );
    assert!(missing.is_empty(), "CLI items with no entry: {missing:?}");
}

#[test]
fn dx01_all_areas_report_counts() {
    // The CLI area is complete in this task. Wave DX2 owns the rest.
    // This test prints the size of that wave. It fails only for CLI.
    let areas: Vec<(&str, Vec<String>)> = vec![
        ("cli", cli_item_ids(&read_json("docs/_inventory/cli.json"))),
        (
            "config",
            read_json("docs/_inventory/config.json")["settings"]
                .as_array()
                .expect("settings")
                .iter()
                .map(|s| s["path"].as_str().expect("path").to_string())
                .collect(),
        ),
        (
            "api",
            read_json("docs/_inventory/api.json")["operations"]
                .as_array()
                .expect("operations")
                .iter()
                .map(|o| {
                    format!(
                        "{} {}",
                        o["method"].as_str().expect("method"),
                        o["path"].as_str().expect("path")
                    )
                })
                .collect(),
        ),
        (
            "mcp",
            read_json("docs/_inventory/mcp.json")["tools"]
                .as_array()
                .expect("tools")
                .iter()
                .map(|t| t["name"].as_str().unwrap_or("").to_string())
                .collect(),
        ),
        ("rules", {
            let doc = read_json("docs/_inventory/rules.json");
            let mut ids: Vec<String> = doc["functions"]
                .as_array()
                .expect("functions")
                .iter()
                .map(|f| format!("fn:{}", f["name"].as_str().expect("name")))
                .collect();
            for key in ["clauses", "operators", "actions", "events"] {
                let prefix = key.trim_end_matches('s');
                for entry in doc[key].as_array().expect(key) {
                    let name = entry
                        .as_str()
                        .unwrap_or_else(|| entry["name"].as_str().expect("name"));
                    ids.push(format!("{prefix}:{name}"));
                }
            }
            ids
        }),
        (
            "connectors",
            read_json("docs/_inventory/connectors.json")["connectors"]
                .as_array()
                .expect("connectors")
                .iter()
                .map(|c| c["kind"].as_str().expect("kind").to_string())
                .collect(),
        ),
        (
            "metrics",
            read_json("docs/_inventory/metrics.json")["metrics"]
                .as_array()
                .expect("metrics")
                .iter()
                .map(|m| m["name"].as_str().expect("name").to_string())
                .collect(),
        ),
    ];
    for (area, ids) in &areas {
        let (verified, failed) = area_counts(area, ids);
        println!(
            "DX01_AREA area={area} items={} verified={verified} failed={failed}",
            ids.len()
        );
    }
    let cli = areas.iter().find(|(name, _)| *name == "cli").expect("cli");
    let (verified, failed) = area_counts("cli", &cli.1);
    assert_eq!(verified + failed, cli.1.len(), "CLI area is fully covered");
}
