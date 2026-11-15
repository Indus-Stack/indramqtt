//! Inventory drift and coverage tests for connector end-to-end work.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::Deserialize;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

#[derive(Deserialize)]
struct Coverage {
    #[serde(default)]
    items: BTreeMap<String, Entry>,
}

#[derive(Deserialize)]
struct Entry {
    test: Option<String>,
    #[serde(default)]
    not_built: Option<NotBuilt>,
    #[serde(default)]
    out_of_mvp: Option<bool>,
    #[serde(default)]
    ignored: Option<bool>,
    #[serde(default)]
    note: Option<String>,
}

#[derive(Deserialize)]
struct NotBuilt {
    file: String,
    line: u64,
    reason: String,
}

fn read_inventory() -> serde_json::Value {
    let path = workspace_root()
        .join("tests")
        .join("e2e")
        .join("connectors")
        .join("inventory.json");
    let text = std::fs::read_to_string(&path).expect("committed inventory exists");
    serde_json::from_str(&text).expect("committed inventory is JSON")
}

#[test]
fn connector_inventory_matches_generator() {
    let output = std::process::Command::new("python3")
        .arg("tools/connector_inventory.py")
        .arg("--check")
        .current_dir(workspace_root())
        .output()
        .expect("python3 runs the inventory generator");
    assert!(
        output.status.success(),
        "inventory drift:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn connector_inventory_covers_both_crates() {
    let inventory = read_inventory();
    let connectors = inventory["connectors"]
        .as_array()
        .expect("inventory lists connectors");
    assert!(
        !connectors.is_empty(),
        "inventory lists at least one connector"
    );
    let mut crates = BTreeSet::new();
    let mut kinds = BTreeSet::new();
    for connector in connectors {
        crates.insert(connector["crate"].as_str().expect("crate name").to_string());
        kinds.insert(connector["kind"].as_str().expect("kind name").to_string());
    }
    assert!(crates.contains("broker-connectors"));
    assert!(crates.contains("broker-connectors-enterprise"));
    for kind in [
        "webhook",
        "mqtt_bridge",
        "postgres",
        "cassandra",
        "timestream",
    ] {
        assert!(kinds.contains(kind), "inventory lists {kind}");
    }
    for kind in ["aws_iot", "azure_iot", "oci_streaming"] {
        let record = connectors
            .iter()
            .find(|record| record["kind"] == kind)
            .expect("out-of-mvp kind is listed");
        assert_eq!(record["status"], "out-of-mvp");
    }
}

fn source_kinds() -> BTreeSet<String> {
    let root = workspace_root();
    let mut kinds = BTreeSet::new();
    for crate_name in ["broker-connectors", "broker-connectors-enterprise"] {
        let directory = root.join("crates").join(crate_name).join("src");
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("rs") {
                continue;
            }
            if path.file_name().and_then(|value| value.to_str()) == Some("lib.rs") {
                continue;
            }
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            let Some(start) = text.find("fn kind(&self) -> &'static str") else {
                continue;
            };
            let rest = &text[start..];
            let Some(open) = rest.find('{') else {
                continue;
            };
            let after = &rest[open + 1..];
            let mut depth = 0usize;
            let mut end = after.len();
            for (index, character) in after.char_indices() {
                match character {
                    '{' => depth += 1,
                    '}' if depth == 0 => {
                        end = index;
                        break;
                    }
                    '}' => depth -= 1,
                    _ => {}
                }
            }
            let body = &after[..end];
            let kind = if let Some(first) = body.find('"') {
                let Some(second) = body[first + 1..].find('"') else {
                    continue;
                };
                body[first + 1..first + 1 + second].to_string()
            } else {
                let Some(field) = text.find("kind: \"") else {
                    continue;
                };
                let value = &text[field + "kind: \"".len()..];
                let Some(close) = value.find('"') else {
                    continue;
                };
                value[..close].to_string()
            };
            if kind != "test" {
                kinds.insert(kind);
            }
        }
    }
    kinds
}

#[test]
fn inventory_lists_every_connector_kind_in_source() {
    let inventory = read_inventory();
    let listed: BTreeSet<String> = inventory["connectors"]
        .as_array()
        .expect("inventory lists connectors")
        .iter()
        .map(|connector| connector["kind"].as_str().expect("kind name").to_string())
        .collect();
    for kind in source_kinds() {
        assert!(
            listed.contains(&kind),
            "connector source kind {kind} is missing from the inventory"
        );
    }
}

fn collect_test_names(directory: &Path, names: &mut BTreeSet<String>) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_test_names(&path, names);
            continue;
        }
        if path.extension().and_then(|value| value.to_str()) != Some("rs") {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for line in text.lines() {
            let line = line.trim_start();
            let Some(rest) = line.strip_prefix("fn ").or_else(|| {
                line.strip_prefix("pub fn ")
                    .or_else(|| line.strip_prefix("async fn "))
                    .or_else(|| line.strip_prefix("pub async fn "))
            }) else {
                continue;
            };
            let name: String = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() {
                names.insert(name);
            }
        }
    }
}

fn test_names() -> BTreeSet<String> {
    let root = workspace_root();
    let mut names = BTreeSet::new();
    collect_test_names(&root.join("tests"), &mut names);
    collect_test_names(
        &root.join("crates").join("broker-node").join("tests"),
        &mut names,
    );
    if let Ok(entries) = std::fs::read_dir(root.join("crates")) {
        for entry in entries.flatten() {
            let path = entry.path().join("tests");
            if path.is_dir() {
                collect_test_names(&path, &mut names);
            }
        }
    }
    names
}

fn check_not_built(id: &str, entry: &Entry) {
    let Some(not_built) = &entry.not_built else {
        panic!("{id} needs a test, a not-built record, or out_of_mvp");
    };
    let path = workspace_root().join(&not_built.file);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!(
            "{id}: not-built file {} does not read: {error}",
            not_built.file
        )
    });
    let lines = text.lines().count() as u64;
    assert!(
        not_built.line >= 1 && not_built.line <= lines,
        "{id}: not-built line {} is outside {} ({} lines)",
        not_built.line,
        not_built.file,
        lines
    );
    assert!(
        !not_built.reason.trim().is_empty(),
        "{id}: not-built reason is empty"
    );
}

#[test]
fn coverage_maps_every_required_item_to_a_real_test() {
    let inventory = read_inventory();
    let coverage_text = std::fs::read_to_string(
        workspace_root()
            .join("tests")
            .join("e2e")
            .join("connectors")
            .join("coverage.toml"),
    )
    .expect("coverage file exists");
    let coverage: Coverage = toml::from_str(&coverage_text).expect("coverage file parses");
    let names = test_names();
    let mut inventory_ids = BTreeSet::new();
    let mut missing = Vec::new();
    let mut mapped = 0usize;
    let mut ignored = 0usize;
    let mut unmapped_other = 0usize;
    for connector in inventory["connectors"]
        .as_array()
        .expect("inventory lists connectors")
    {
        let kind = connector["kind"].as_str().expect("kind name");
        let status = connector["status"].as_str().expect("status");
        let required = kind == "webhook" || kind == "mqtt_bridge";
        for item in connector["items"].as_array().expect("item list") {
            let id = item["id"].as_str().expect("item id");
            inventory_ids.insert(id.to_string());
            let entry = coverage.items.get(id);
            if let Some(entry) = entry {
                if let Some(test) = &entry.test {
                    assert!(
                        names.contains(test),
                        "{id}: mapped test {test} does not exist"
                    );
                    mapped += 1;
                    if entry.ignored.unwrap_or(false) {
                        ignored += 1;
                    }
                } else if entry.not_built.is_some() {
                    check_not_built(id, entry);
                    mapped += 1;
                } else if entry.out_of_mvp.unwrap_or(false) {
                    mapped += 1;
                } else {
                    panic!("{id}: coverage entry has no test, not_built, or out_of_mvp");
                }
                if entry.note.as_deref() == Some("") {
                    panic!("{id}: coverage note is empty");
                }
            } else if required {
                missing.push(id.to_string());
            } else if status != "out-of-mvp" {
                unmapped_other += 1;
            }
        }
    }
    for id in coverage.items.keys() {
        assert!(
            inventory_ids.contains(id),
            "coverage names unknown item {id}"
        );
    }
    assert!(
        missing.is_empty(),
        "required items have no coverage: {}",
        missing.join(", ")
    );
    println!("COVERAGE mapped={mapped} ignored={ignored} unmapped_other={unmapped_other}");
}
