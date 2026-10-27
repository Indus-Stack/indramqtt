//! zstd advisory check (RUSTSEC-2026-0403).
//!
//! The rule engine compresses with `zstd` level 0.
//! This test uses the SQL functions.
//! It checks new data and old data.

use rekuiper_sql::{Evaluator, Parser};
use serde_json::{json, Value};
use std::collections::HashMap;

type Record = HashMap<String, Value>;

fn eval_one(sql: &str) -> Value {
    let record: Record = HashMap::new();
    let mut parser = Parser::new(sql);
    let stmt = parser.parse_select().expect("parse SELECT");
    match Evaluator::eval_select(&stmt, &record) {
        Some(mut row) => row.shift_remove("v").unwrap_or(Value::Null),
        None => Value::Null,
    }
}

// Frame written by the system library at default level.
// It holds `hello rekuiper sb-24 pre-change fixture`.
// The frame format stays stable across crate versions.
const PRE_CHANGE_B64: &str = "KLUv/SAnOQEAaGVsbG8gcmVrdWlwZXIgc2ItMjQgcHJlLWNoYW5nZSBmaXh0dXJl";

#[test]
fn zstd_roundtrip_after_change() {
    let compressed = eval_one("SELECT compress('hello rekuiper sb-24', 'zstd') AS v FROM demo");
    let text = compressed.as_str().expect("compress gives text");
    let sql = format!("SELECT decompress('{text}', 'zstd') AS v FROM demo");
    assert_eq!(eval_one(&sql), json!("hello rekuiper sb-24"));
}

#[test]
fn zstd_reads_pre_change_frame() {
    let sql = format!("SELECT decompress('{PRE_CHANGE_B64}', 'zstd') AS v FROM demo");
    assert_eq!(
        eval_one(&sql),
        json!("hello rekuiper sb-24 pre-change fixture")
    );
}
