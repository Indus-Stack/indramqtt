//! RQ-03 table-driven coverage for the standard-dialect built-ins.
//!
//! Every implemented `new` / `adapter` row from `FUNCTIONS.md` gets at least
//! three cases here (happy path plus documented edge cases: wrong types,
//! empty input, out-of-range indexes). Cases call the registered handler
//! through the engine's public user-function table. Colliding adapters
//! (`substr`, `split`, `concat`, `length`, multi-arg `base64_*`, `rtrim`)
//! are rewritten by `broker_rules::rewrite_colliding_functions` to internal
//! `std_*` aliases before parsing, so SQL evaluation uses the standard
//! semantics; the SQL-level blocks below prove that for every colliding
//! name. A SQL-level block proves registration is
//! visible to the evaluator for non-colliding names, and the final block
//! goes through the broker ingress path with a rule sink.

use async_trait::async_trait;
use broker_protocol::{QoS, Topic, TopicFilter};
use broker_rules::stdlib::ensure_stdlib_registered;
use broker_rules::stdlib::jq_timeout_count;
use broker_rules::{BackpressurePolicy, BrokerSink, RuleAction, RuleEngine, RuleEngineError};
use bytes::Bytes;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex as StdMutex};

// The `jq` stop counter is process-global. The tests that measure its
// delta must not overlap, or a stop from the other test lands in the
// delta. This lock holds no production state; it only orders the tests.
static JQ_COUNTER_TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn call(name: &str, args: Vec<Value>) -> Value {
    ensure_stdlib_registered();
    rekuiper_core::plugin::get_global_udf_registry()
        .call_udf(name, &args)
        .unwrap_or(Value::Null)
}

fn s(v: &str) -> Value {
    Value::String(v.to_string())
}

fn table(name: &str, cases: Vec<(Vec<Value>, Value)>) {
    for (i, (args, want)) in cases.into_iter().enumerate() {
        let got = call(name, args);
        assert_eq!(got, want, "{name} case {i}");
    }
}

fn approx(name: &str, args: Vec<Value>, want: f64) {
    let got = call(name, args);
    let v = got
        .as_f64()
        .unwrap_or_else(|| panic!("{name} not numeric: {got}"));
    assert!((v - want).abs() < 1e-9, "{name}: got {v} want {want}");
}

// ---------------------------------------------------------------------------
// Math / bitwise additions
// ---------------------------------------------------------------------------

#[test]
fn std_math_hyperbolic() {
    approx("acosh", vec![json!(1.0)], 0.0);
    approx("acosh", vec![json!(2.0)], 1.316957896924816);
    table(
        "acosh",
        vec![
            (vec![json!(0.5)], Value::Null),
            (vec![s("x")], Value::Null),
            (vec![], Value::Null),
        ],
    );
    approx("asinh", vec![json!(0.0)], 0.0);
    approx("asinh", vec![json!(1.0)], 0.881373587019543);
    table(
        "asinh",
        vec![
            (vec![Value::Null], Value::Null),
            (vec![json!(1.0), json!(2.0)], Value::Null),
            (vec![], Value::Null),
        ],
    );
    approx("atanh", vec![json!(0.0)], 0.0);
    approx("atanh", vec![json!(0.5)], 0.549306144334055);
    table(
        "atanh",
        vec![
            (vec![json!(1.0)], Value::Null),
            (vec![json!(-1.0)], Value::Null),
            (vec![s("x")], Value::Null),
        ],
    );
    table(
        "fmod",
        vec![
            (vec![json!(5.5), json!(2.0)], json!(1.5)),
            (vec![json!(5.0), json!(0.0)], Value::Null),
            (vec![s("x"), json!(2.0)], Value::Null),
            (vec![json!(5.0)], Value::Null),
        ],
    );
    table(
        "div",
        vec![
            (vec![json!(7), json!(2)], json!(3)),
            (vec![json!(7), json!(0)], Value::Null),
            (vec![s("x"), json!(2)], Value::Null),
            (vec![json!(7)], Value::Null),
        ],
    );
}

#[test]
fn std_bitwise_shifts() {
    table(
        "bitsl",
        vec![
            (vec![json!(1), json!(3)], json!(8)),
            (vec![json!(1), json!(-1)], Value::Null),
            (vec![json!(1), json!(64)], Value::Null),
            (vec![s("x"), json!(1)], Value::Null),
        ],
    );
    table(
        "bitsr",
        vec![
            (vec![json!(8), json!(3)], json!(1)),
            (vec![json!(-8), json!(1)], json!(-4)),
            (vec![json!(8), json!(-1)], Value::Null),
            (vec![json!(8)], Value::Null),
        ],
    );
}

// ---------------------------------------------------------------------------
// Bit and byte
// ---------------------------------------------------------------------------

#[test]
fn std_bit_byte() {
    table(
        "bitsize",
        vec![
            (vec![s("A")], json!(8)),
            (vec![s("")], json!(0)),
            (vec![json!(1)], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "bytesize",
        vec![
            (vec![s("abc")], json!(3)),
            (vec![s("")], json!(0)),
            (vec![json!(1)], Value::Null),
            (vec![json!([1, 2])], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "subbits",
        vec![
            (vec![s("A"), json!(8)], json!(65)),
            (vec![s("A"), json!(1), json!(8)], json!(65)),
            (vec![s("A"), json!(2), json!(7)], json!(65)),
            (vec![s("A"), json!(99), json!(8)], Value::Null),
            (vec![json!(1), json!(8)], Value::Null),
        ],
    );
}

// ---------------------------------------------------------------------------
// Conversion
// ---------------------------------------------------------------------------

#[test]
fn std_conversion() {
    table(
        "str",
        vec![
            (vec![json!(42)], s("42")),
            (vec![json!(true)], s("true")),
            (vec![Value::Null], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "str_utf8",
        vec![
            (vec![s("hi")], s("hi")),
            (vec![json!(7)], s("7")),
            (vec![Value::Null], Value::Null),
        ],
    );
    // "A" in UTF-16LE is [0x41, 0x00] -> base64 "QQA=".
    table(
        "str_utf16_le",
        vec![
            (vec![s("A")], s("QQA=")),
            (vec![Value::Null], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "bool",
        vec![
            (vec![json!(true)], json!(true)),
            (vec![json!(false)], json!(false)),
            (vec![json!(1)], json!(true)),
            (vec![json!(0)], json!(false)),
            (vec![json!(2)], Value::Null),
            (vec![json!(1.5)], Value::Null),
            (vec![s("true")], json!(true)),
            (vec![s("false")], json!(false)),
            (vec![s("nope")], Value::Null),
            (vec![json!(1), json!(2)], Value::Null),
        ],
    );
    table(
        "int",
        vec![
            (vec![json!(3.9)], json!(3)),
            (vec![s("42")], json!(42)),
            (vec![s("x")], Value::Null),
            (vec![json!(true)], json!(1)),
        ],
    );
    table(
        "float",
        vec![
            (vec![s("2.5")], json!(2.5)),
            (vec![json!(true)], json!(1.0)),
            (vec![s("x")], Value::Null),
            (vec![json!(2.5), json!(1)], json!(2.5)),
        ],
    );
    table(
        "float2str",
        vec![
            (vec![json!(2.5), json!(1)], s("2.5")),
            (vec![json!(2.567), json!(2)], s("2.57")),
            (vec![s("x"), json!(1)], Value::Null),
            (vec![json!(1.0), json!(99)], Value::Null),
        ],
    );
    table(
        "map",
        vec![
            (vec![s("{\"a\":1}")], json!({"a": 1})),
            (vec![s("nope")], Value::Null),
            (vec![json!(1)], Value::Null),
        ],
    );
    table(
        "bin2hexstr",
        vec![
            (vec![s("AB")], s("4142")),
            (vec![s("AB"), s("0x")], s("0x4142")),
            (vec![Value::Null], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "hexstr2bin",
        vec![
            (vec![s("4142")], s("AB")),
            (vec![s("0x4142"), s("0x")], s("AB")),
            (vec![s("0x4142"), s("!!")], Value::Null),
            (vec![s("zz")], Value::Null),
            (vec![s("abc")], Value::Null),
        ],
    );
    table(
        "sqlserver_bin2hexstr",
        vec![
            (vec![s("AB")], s("0x4142")),
            (vec![Value::Null], Value::Null),
            (vec![], Value::Null),
        ],
    );
}

// ---------------------------------------------------------------------------
// Checks
// ---------------------------------------------------------------------------

#[test]
fn std_checks() {
    table(
        "is_null",
        vec![
            (vec![Value::Null], json!(true)),
            (vec![json!(0)], json!(false)),
            (vec![], Value::Null),
        ],
    );
    table(
        "is_null_var",
        vec![
            (vec![Value::Null], json!(true)),
            (vec![s("x")], json!(false)),
            (vec![], Value::Null),
        ],
    );
    table(
        "is_not_null",
        vec![
            (vec![Value::Null], json!(false)),
            (vec![json!(1)], json!(true)),
            (vec![], Value::Null),
        ],
    );
    table(
        "is_not_null_var",
        vec![
            (vec![s("x")], json!(true)),
            (vec![Value::Null], json!(false)),
            (vec![], Value::Null),
        ],
    );
    table(
        "is_str",
        vec![
            (vec![s("x")], json!(true)),
            (vec![json!(1)], json!(false)),
            (vec![], Value::Null),
        ],
    );
    table(
        "is_bool",
        vec![
            (vec![json!(true)], json!(true)),
            (vec![json!(1)], json!(false)),
            (vec![], Value::Null),
        ],
    );
    table(
        "is_int",
        vec![
            (vec![json!(3)], json!(true)),
            (vec![json!(3.0)], json!(false)),
            (vec![], Value::Null),
        ],
    );
    table(
        "is_float",
        vec![
            (vec![json!(3.0)], json!(true)),
            (vec![json!(3)], json!(false)),
            (vec![], Value::Null),
        ],
    );
    table(
        "is_num",
        vec![
            (vec![json!(3)], json!(true)),
            (vec![s("x")], json!(false)),
            (vec![], Value::Null),
        ],
    );
    table(
        "is_map",
        vec![
            (vec![json!({"a": 1})], json!(true)),
            (vec![json!([1])], json!(false)),
            (vec![], Value::Null),
        ],
    );
    table(
        "is_array",
        vec![
            (vec![json!([1])], json!(true)),
            (vec![json!({"a": 1})], json!(false)),
            (vec![], Value::Null),
        ],
    );
    table(
        "is_empty",
        vec![
            (vec![s("")], json!(true)),
            (vec![json!([])], json!(true)),
            (vec![json!({})], json!(true)),
            (vec![s("x")], json!(false)),
            (vec![Value::Null], json!(true)),
        ],
    );
}

// ---------------------------------------------------------------------------
// Strings
// ---------------------------------------------------------------------------

#[test]
fn std_strings_basic() {
    table(
        "strlen",
        vec![
            (vec![s("abc")], json!(3)),
            (vec![s("")], json!(0)),
            (vec![json!(1)], Value::Null),
            (vec![], Value::Null),
        ],
    );
    // Standard 0-based slice (differs from the engine 1-based built-in).
    table(
        "substr",
        vec![
            (vec![s("abcdef"), json!(0), json!(3)], s("abc")),
            (vec![s("abcdef"), json!(2)], s("cdef")),
            (vec![s("abcdef"), json!(-2)], s("ef")),
            (vec![s("abcdef"), json!(99)], s("")),
            (vec![json!(1), json!(0)], Value::Null),
        ],
    );
    table(
        "split",
        vec![
            (vec![s("a,b,c"), s(",")], json!(["a", "b", "c"])),
            (vec![s("a,,b"), s(",")], json!(["a", "b"])),
            (
                vec![s("a,b,c"), s(","), s("notrim")],
                json!(["a", "b", "c"]),
            ),
            (vec![s("a,b"), s("")], Value::Null),
            (vec![json!(1), s(",")], Value::Null),
        ],
    );
    table(
        "concat",
        vec![
            (vec![s("a"), s("b")], s("ab")),
            (vec![json!([s("a"), s("b")])], s("ab")),
            (vec![Value::Null, s("b")], Value::Null),
            (vec![s("a")], Value::Null),
        ],
    );
    table(
        "tokens",
        vec![
            (vec![s("a,b;c"), s(",;")], json!(["a", "b", "c"])),
            (vec![s("a  b"), s(" ")], json!(["a", "b"])),
            (vec![json!(1), s(" ")], Value::Null),
            (vec![s("a"), s("")], Value::Null),
        ],
    );
    table(
        "sprintf_s",
        vec![
            (vec![s("hi ~s!"), json!([s("bob")])], s("hi bob!")),
            (vec![s("~n"), json!([])], s("\n")),
            (vec![s("~z"), json!([])], Value::Null),
            (vec![s("hi")], Value::Null),
        ],
    );
    table(
        "pad",
        vec![
            (vec![s("ab"), json!(5)], s("ab   ")),
            (vec![s("ab"), json!(5), s("leading")], s("   ab")),
            (vec![s("ab"), json!(5), s("leading"), s("0")], s("000ab")),
            (vec![s("abcdef"), json!(3)], s("abcdef")),
            (vec![s("ab"), json!(-1)], Value::Null),
        ],
    );
    table(
        "replace",
        vec![
            (vec![s("aaa"), s("a"), s("b")], s("bbb")),
            (vec![s("ab"), s("a"), s("x"), s("leading")], s("xb")),
            (vec![s("ab"), s("b"), s("x"), s("trailing")], s("ax")),
            (vec![s("ab"), s(""), s("x")], Value::Null),
            (vec![s("ab"), s("a"), s("x"), s("bogus")], Value::Null),
        ],
    );
    table(
        "regex_match",
        vec![
            (vec![s("abc"), s("^a")], json!(true)),
            (vec![s("abc"), s("^z")], json!(false)),
            (vec![s("abc"), s("[")], Value::Null),
            (vec![json!(1), s("a")], Value::Null),
        ],
    );
    table(
        "regex_replace",
        vec![
            (vec![s("abc"), s("b"), s("x")], s("axc")),
            (vec![s("abc"), s("[")], Value::Null),
            (vec![s("abc"), s("b")], Value::Null),
        ],
    );
    table(
        "regex_extract",
        vec![
            (vec![s("a1b22"), s("([0-9]+)")], json!(["1"])),
            (vec![s("abc"), s("([0-9]+)")], json!([])),
            (vec![s("abc"), s("[")], Value::Null),
        ],
    );
    table(
        "ascii",
        vec![
            (vec![s("A")], json!(65)),
            (vec![s("")], Value::Null),
            (vec![json!(1)], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "find",
        vec![
            (vec![s("hello"), s("ll")], s("llo")),
            (vec![s("hello"), s("z")], s("")),
            (vec![s("aXbXc"), s("X"), s("trailing")], s("Xc")),
            (vec![s("a"), s("")], Value::Null),
            (vec![s("a"), s("b"), s("bogus")], Value::Null),
        ],
    );
    table(
        "join_to_string",
        vec![
            (vec![json!([s("a"), s("b")])], s("a, b")),
            (vec![s("|"), json!([json!(1), json!(2)])], s("1|2")),
            (vec![json!(1)], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "rm_prefix",
        vec![
            (vec![s("foobar"), s("foo")], s("bar")),
            (vec![s("foobar"), s("z")], s("foobar")),
            (vec![Value::Null, s("f")], Value::Null),
            (vec![s("a")], Value::Null),
        ],
    );
    table(
        "rtrim",
        vec![
            (vec![s("abc  ")], s("abc")),
            (vec![s("abcxx"), s("x")], s("abc")),
            (vec![s("abc"), s("z")], s("abc")),
            (vec![json!(1)], Value::Null),
        ],
    );
    table(
        "unescape",
        vec![
            (vec![s("a\\nb")], s("a\nb")),
            (vec![s("a\\x41")], s("aA")),
            (vec![s("a\\qb")], Value::Null),
            (vec![json!(1)], Value::Null),
        ],
    );
    table(
        "jq",
        vec![
            (vec![s(".a"), s("{\"a\":1}")], json!([1])),
            (vec![s(".missing"), s("{\"a\":1}")], json!([])),
            (vec![s(".a | .b"), s("{\"a\":1}")], json!([])),
            (vec![s(".a")], Value::Null),
        ],
    );
    table(
        "join_to_sql_values_string",
        vec![
            (vec![json!([json!(1), s("a")])], s("1, 'a'")),
            (vec![json!([])], s("")),
            (vec![json!(1)], Value::Null),
        ],
    );
    let redis = call("map_to_redis_hset_args", vec![json!({"k": "v"})]);
    match redis {
        Value::Array(items) => {
            assert_eq!(items[0], s("map_to_redis_hset_args"));
            assert!(items.contains(&s("k")));
        }
        other => panic!("redis hset shape: {other}"),
    }
    table(
        "map_to_redis_hset_args",
        vec![
            (vec![json!(1)], Value::Null),
            (vec![s("nope")], Value::Null),
            (vec![json!([1])], Value::Null),
            (vec![], Value::Null),
        ],
    );
}

// ---------------------------------------------------------------------------
// Maps
// ---------------------------------------------------------------------------

#[test]
fn std_maps() {
    table(
        "map_new",
        vec![
            (vec![], json!({})),
            (vec![json!(1)], Value::Null),
            (vec![s("x"), json!(1)], Value::Null),
        ],
    );
    table(
        "map_get",
        vec![
            (vec![s("a"), json!({"a": 1})], json!(1)),
            (vec![s("a.b"), json!({"a": {"b": 2}})], json!(2)),
            (vec![s("missing"), json!({"a": 1}), json!(9)], json!(9)),
            (vec![s("missing"), json!({"a": 1})], Value::Null),
            (vec![json!(1), json!({"a": 1})], Value::Null),
        ],
    );
    table(
        "map_put",
        vec![
            (
                vec![s("b"), json!(2), json!({"a": 1})],
                json!({"a": 1, "b": 2}),
            ),
            (vec![s("a.b"), json!(2), json!({})], json!({"a": {"b": 2}})),
            (vec![json!(1), json!(2), json!({})], Value::Null),
            (vec![s("a"), json!(1)], Value::Null),
        ],
    );
    table(
        "map_keys",
        vec![
            (vec![json!({"a": 1})], json!(["a"])),
            (vec![json!(1)], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "map_values",
        vec![
            (vec![json!({"a": 1})], json!([1])),
            (vec![json!(1)], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "map_to_entries",
        vec![
            (vec![json!({"a": 1})], json!([{"key": "a", "value": 1}])),
            (vec![json!(1)], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "map_size",
        vec![
            (vec![json!({"a": 1, "b": 2})], json!(2)),
            (vec![json!(1)], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "mget",
        vec![
            (vec![s("a"), json!({"a": 1})], json!(1)),
            (vec![s("missing"), json!({"a": 1}), json!(7)], json!(7)),
            (vec![s("missing"), json!({"a": 1})], Value::Null),
        ],
    );
    table(
        "mput",
        vec![
            (vec![s("a"), json!(1), json!({})], json!({"a": 1})),
            (vec![s("a"), json!(1)], Value::Null),
            (vec![json!(1), json!(1), json!(1)], Value::Null),
        ],
    );
}

// ---------------------------------------------------------------------------
// Arrays
// ---------------------------------------------------------------------------

#[test]
fn std_arrays() {
    table(
        "nth",
        vec![
            (vec![json!(2), json!(["a", "b"])], s("b")),
            (vec![json!(9), json!(["a"])], s("")),
            (vec![s("x"), json!(["a"])], Value::Null),
            (vec![json!(1), json!(1)], Value::Null),
        ],
    );
    table(
        "length",
        vec![
            (vec![json!([1, 2, 3])], json!(3)),
            (vec![s("abc")], json!(3)),
            (vec![json!(1)], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "sublist",
        vec![
            (vec![json!(2), json!([1, 2, 3])], json!([1, 2])),
            (vec![json!(2), json!(2), json!([1, 2, 3])], json!([2, 3])),
            (vec![json!(-1), json!([1])], Value::Null),
            (vec![json!(1)], Value::Null),
        ],
    );
    table(
        "first",
        vec![
            (vec![json!([1, 2])], json!(1)),
            (vec![json!([])], Value::Null),
            (vec![json!(1)], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "last",
        vec![
            (vec![json!([1, 2])], json!(2)),
            (vec![json!([])], Value::Null),
            (vec![json!(1)], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "contains",
        vec![
            (vec![json!(2), json!([1, 2])], json!(true)),
            (vec![json!(9), json!([1, 2])], json!(false)),
            (vec![json!(1), json!(1)], Value::Null),
            (vec![json!(1)], Value::Null),
        ],
    );
}

// ---------------------------------------------------------------------------
// Hash / compression / encoding
// ---------------------------------------------------------------------------

#[test]
fn std_hash() {
    table(
        "sha",
        vec![
            (
                vec![s("abc")],
                s("a9993e364706816aba3e25717850c26c9cd0d89d"),
            ),
            (vec![Value::Null], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "hash",
        vec![
            (
                vec![s("sha256"), s("abc")],
                s("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
            ),
            (
                vec![s("md5"), s("abc")],
                s("900150983cd24fb0d6963f7d28e17f72"),
            ),
            (vec![s("nope"), s("abc")], Value::Null),
            (vec![s("sha256")], Value::Null),
        ],
    );
    table(
        "hash_to_range",
        vec![
            (vec![s(""), json!(1), json!(10)], Value::Null),
            (vec![s("k"), json!(10), json!(1)], Value::Null),
            (vec![json!(1), json!(1), json!(10)], Value::Null),
        ],
    );
    let in_range = call("hash_to_range", vec![s("k"), json!(1), json!(10)]);
    let v = in_range.as_i64().expect("range number");
    assert!((1..=10).contains(&v), "range bounds: {v}");

    table(
        "map_to_range",
        vec![
            (vec![json!(5), json!(1), json!(10)], json!(6)),
            (vec![json!(1), json!(10), json!(1)], Value::Null),
            (vec![Value::Null, json!(1), json!(10)], Value::Null),
        ],
    );
    table(
        "coalesce_ne",
        vec![
            (vec![Value::Null, s("b")], s("b")),
            (vec![s(""), s("b")], s("b")),
            (vec![s("a"), s("b")], s("a")),
            (vec![], Value::Null),
        ],
    );
    table(
        "null",
        vec![
            (vec![], Value::Null),
            (vec![json!(1)], Value::Null),
            (vec![s("x")], Value::Null),
        ],
    );
}

#[test]
fn std_compression_roundtrip() {
    for name in ["zip", "gzip", "zip_compress", "lz4_compress"] {
        let un = match name {
            "zip" => "unzip",
            "gzip" => "gunzip",
            "zip_compress" => "zip_uncompress",
            _ => "lz4_uncompress",
        };
        let enc = call(name, vec![s("hello world hello world")]);
        assert!(enc.is_string(), "{name} encodes to string");
        let dec = call(un, vec![enc]);
        assert_eq!(dec, s("hello world hello world"), "{name} roundtrip");
    }
    table(
        "unzip",
        vec![
            (vec![s("!!!not-base64!!!")], Value::Null),
            (vec![json!(1)], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "gunzip",
        vec![
            (vec![s("")], Value::Null),
            (vec![json!(1)], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "lz4_uncompress",
        vec![
            (vec![s("")], Value::Null),
            (vec![json!(1)], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "zip",
        vec![
            (vec![], Value::Null),
            (vec![Value::Null], Value::Null),
            (vec![json!(1), json!(2)], Value::Null),
        ],
    );
    table(
        "gzip",
        vec![
            (vec![], Value::Null),
            (vec![Value::Null], Value::Null),
            (vec![json!(1), json!(2)], Value::Null),
        ],
    );
    table(
        "zip_compress",
        vec![
            (vec![], Value::Null),
            (vec![Value::Null], Value::Null),
            (vec![json!(1), json!(2)], Value::Null),
        ],
    );
    table(
        "lz4_compress",
        vec![
            (vec![], Value::Null),
            (vec![Value::Null], Value::Null),
            (vec![json!(1), json!(2)], Value::Null),
        ],
    );
    table(
        "zip_uncompress",
        vec![
            (vec![s("!!!not-base64!!!")], Value::Null),
            (vec![json!(1)], Value::Null),
            (vec![], Value::Null),
        ],
    );
}

#[test]
fn std_encoding() {
    table(
        "base64_encode",
        vec![
            (vec![s("hello")], s("aGVsbG8=")),
            (vec![s("hello"), s("urlsafe")], s("aGVsbG8=")),
            (
                vec![s("hello"), s("urlsafe"), s("no_padding")],
                s("aGVsbG8"),
            ),
            (vec![s("hello"), s("bogus")], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "base64_decode",
        vec![
            (vec![s("aGVsbG8=")], s("hello")),
            (vec![s("aGVsbG8"), s("no_padding")], s("hello")),
            (vec![s("!!!")], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "json_encode",
        vec![
            (vec![json!({"a": 1})], s("{\"a\":1}")),
            (vec![json!([1, 2])], s("[1,2]")),
            (vec![], Value::Null),
            (vec![json!(1), json!(2)], Value::Null),
        ],
    );
    table(
        "json_decode",
        vec![
            (vec![s("{\"a\":1}")], json!({"a": 1})),
            (vec![s("nope")], Value::Null),
            (vec![json!(1)], Value::Null),
        ],
    );
    table(
        "term_encode",
        vec![
            (vec![json!({"a": 1})], s("{\"a\":1}")),
            (vec![json!([1])], s("[1]")),
            (vec![], Value::Null),
            (vec![json!(1), json!(2)], Value::Null),
        ],
    );
    table(
        "term_decode",
        vec![
            (vec![s("{\"a\":1}")], json!({"a": 1})),
            (vec![json!(1)], Value::Null),
            (vec![], Value::Null),
        ],
    );
}

// ---------------------------------------------------------------------------
// Dates / system / state / context
// ---------------------------------------------------------------------------

#[test]
fn std_dates() {
    let rfc = call("now_rfc3339", vec![]);
    assert!(
        rfc.as_str().is_some_and(|v| v.contains('T')),
        "rfc3339: {rfc}"
    );
    table(
        "now_rfc3339",
        vec![
            (vec![s("bogus")], Value::Null),
            (vec![json!(1)], Value::Null),
            (vec![s("second"), s("extra")], Value::Null),
        ],
    );
    table(
        "unix_ts_to_rfc3339",
        vec![
            (vec![json!(0)], s("1970-01-01T00:00:00Z")),
            (vec![json!(0), s("bogus")], Value::Null),
            (vec![s("x")], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "rfc3339_to_unix_ts",
        vec![
            (vec![s("1970-01-01T00:00:00Z")], json!(0)),
            (vec![s("nope")], Value::Null),
            (vec![s("1970-01-01T00:00:00Z"), s("bogus")], Value::Null),
            (vec![], Value::Null),
        ],
    );
    let ts = call("now_timestamp", vec![]);
    assert!(ts.as_i64().is_some_and(|v| v > 1_700_000_000), "ts: {ts}");
    table(
        "now_timestamp",
        vec![
            (vec![s("bogus")], Value::Null),
            (vec![json!(1)], Value::Null),
            (vec![s("second"), s("x")], Value::Null),
        ],
    );
    table(
        "format_date",
        vec![
            (
                vec![s("second"), json!(0), s("yyyy-MM-dd"), json!(0)],
                s("1970-01-01"),
            ),
            (vec![s("bogus"), json!(0), s("yyyy")], Value::Null),
            (vec![s("second"), json!(0)], Value::Null),
        ],
    );
    table(
        "date_to_unix_ts",
        vec![
            (
                vec![s("second"), s("yyyy-MM-dd"), s("1970-01-02")],
                json!(86400),
            ),
            (vec![s("second"), s("nope")], Value::Null),
            (vec![s("second"), s("yyyy-MM-dd")], Value::Null),
        ],
    );
    table(
        "timezone_to_offset_seconds",
        vec![
            (vec![s("Z")], json!(0)),
            (vec![s("+08:00")], json!(28800)),
            (vec![s("Mars/Olympus")], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "timezone_to_second",
        vec![
            (vec![s("Z")], json!(0)),
            (vec![s("bogus-zone")], Value::Null),
            (vec![], Value::Null),
        ],
    );
    let mongo = call("mongo_date", vec![]);
    assert!(
        mongo.as_str().is_some_and(|v| v.contains('T')),
        "mongo_date: {mongo}"
    );
    table(
        "mongo_date",
        vec![
            (vec![s("x")], Value::Null),
            (vec![json!(0), s("bogus")], Value::Null),
            (vec![json!(1), json!(2), json!(3)], Value::Null),
        ],
    );
}

#[test]
fn std_system_state_context() {
    table(
        "getenv",
        vec![
            (vec![s("STD_QQQ_MISSING_12345")], s("")),
            (vec![json!(1)], Value::Null),
            (vec![], Value::Null),
        ],
    );
    let r = call("random", vec![]);
    let v = r.as_f64().expect("random float");
    assert!((0.0..1.0).contains(&v), "random range: {v}");
    table(
        "random",
        vec![
            (vec![json!(1)], Value::Null),
            (vec![s("x")], Value::Null),
            (vec![json!(1), json!(2)], Value::Null),
        ],
    );
    let id = call("uuid_v4", vec![]);
    let text = id.as_str().expect("uuid string").to_string();
    assert_eq!(text.len(), 36, "uuid len: {text}");
    assert_eq!(text.matches('-').count(), 4, "uuid dashes: {text}");
    let bare = call("uuid_v4_no_hyphen", vec![]);
    let flat = bare.as_str().expect("uuid string").to_string();
    assert_eq!(flat.len(), 32, "uuid flat len: {flat}");
    assert!(!flat.contains('-'), "uuid flat: {flat}");
    table(
        "uuid_v4",
        vec![
            (vec![json!(1)], Value::Null),
            (vec![s("x")], Value::Null),
            (vec![json!(1), json!(2)], Value::Null),
        ],
    );
    table(
        "uuid_v4_no_hyphen",
        vec![
            (vec![json!(1)], Value::Null),
            (vec![s("x")], Value::Null),
            (vec![json!(1), json!(2)], Value::Null),
        ],
    );

    table(
        "contains_topic",
        vec![
            (vec![json!(["a/b"]), s("a/b")], json!(true)),
            (vec![json!(["a/b"]), s("a/c")], json!(false)),
            (
                vec![json!([{"topic": "a/b", "qos": 1}]), s("a/b"), json!(1)],
                json!(true),
            ),
            (
                vec![json!([{"topic": "a/b", "qos": 1}]), s("a/b"), json!(0)],
                json!(false),
            ),
            (vec![json!(1), s("a")], Value::Null),
        ],
    );
    table(
        "contains_topic_match",
        vec![
            (vec![json!(["a/+"]), s("a/b")], json!(true)),
            (vec![json!(["a/+"]), s("a")], json!(false)),
            (vec![json!(["#"]), s("a/b/c")], json!(true)),
            (vec![json!(1), s("a")], Value::Null),
        ],
    );
    // No context installed on a bare registry call: accessors fail closed.
    table(
        "topic",
        vec![
            (vec![], Value::Null),
            (vec![json!(0)], Value::Null),
            (vec![json!(1), json!(2)], Value::Null),
        ],
    );
    table(
        "payload",
        vec![
            (vec![], Value::Null),
            (vec![s("a"), s("b")], Value::Null),
            (vec![s("a"), s("b"), s("c")], Value::Null),
        ],
    );
    table(
        "msgid",
        vec![
            (vec![], Value::Null),
            (vec![json!(1)], Value::Null),
            (vec![json!(1), json!(2)], Value::Null),
        ],
    );
    table(
        "qos",
        vec![
            (vec![], Value::Null),
            (vec![json!(1)], Value::Null),
            (vec![json!(1), json!(2)], Value::Null),
        ],
    );
    table(
        "clientid",
        vec![
            (vec![], Value::Null),
            (vec![json!(1)], Value::Null),
            (vec![json!(1), json!(2)], Value::Null),
        ],
    );
    table(
        "clientip",
        vec![
            (vec![], Value::Null),
            (vec![json!(1)], Value::Null),
            (vec![json!(1), json!(2)], Value::Null),
        ],
    );
    table(
        "username",
        vec![
            (vec![], Value::Null),
            (vec![json!(1)], Value::Null),
            (vec![json!(1), json!(2)], Value::Null),
        ],
    );
    table(
        "peerhost",
        vec![
            (vec![], Value::Null),
            (vec![json!(1)], Value::Null),
            (vec![json!(1), json!(2)], Value::Null),
        ],
    );
    table(
        "flag",
        vec![
            (vec![s("x")], Value::Null),
            (vec![json!(1)], Value::Null),
            (vec![], Value::Null),
        ],
    );
    table(
        "flags",
        vec![
            (vec![], json!({})),
            (vec![json!(1)], Value::Null),
            (vec![s("x"), s("y")], Value::Null),
        ],
    );

    // Bounded state round-trips.
    assert_eq!(call("proc_dict_put", vec![s("rk03-k"), json!(7)]), json!(7));
    assert_eq!(call("proc_dict_get", vec![s("rk03-k")]), json!(7));
    assert_eq!(call("proc_dict_del", vec![s("rk03-k")]), json!(7));
    assert_eq!(call("proc_dict_get", vec![s("rk03-k")]), Value::Null);
    table(
        "proc_dict_put",
        vec![
            (vec![s("k")], Value::Null),
            (vec![], Value::Null),
            (vec![json!(1), json!(1), json!(1)], Value::Null),
        ],
    );
    assert_eq!(call("kv_store_put", vec![s("rk03-g"), s("v")]), s("v"));
    assert_eq!(call("kv_store_get", vec![s("rk03-g")]), s("v"));
    assert_eq!(
        call("kv_store_get", vec![s("rk03-missing"), s("d")]),
        s("d")
    );
    assert_eq!(call("kv_store_del", vec![s("rk03-g")]), s("v"));
    table(
        "kv_store_get",
        vec![
            (vec![], Value::Null),
            (vec![s("rk03-never-present-xyz")], Value::Null),
            (vec![s("a"), s("b"), s("c")], Value::Null),
        ],
    );
    table(
        "proc_dict_get",
        vec![
            (vec![s("rk03-never-present-xyz")], Value::Null),
            (vec![], Value::Null),
            (vec![s("a"), s("b")], Value::Null),
        ],
    );
    table(
        "proc_dict_del",
        vec![
            (vec![s("rk03-never-present-xyz")], Value::Null),
            (vec![], Value::Null),
            (vec![s("a"), s("b")], Value::Null),
        ],
    );
    table(
        "kv_store_put",
        vec![
            (vec![s("k")], Value::Null),
            (vec![], Value::Null),
            (vec![s("a"), s("b"), s("c")], Value::Null),
        ],
    );
    table(
        "kv_store_del",
        vec![
            (vec![s("rk03-never-present-xyz")], Value::Null),
            (vec![], Value::Null),
            (vec![s("a"), s("b")], Value::Null),
        ],
    );
}

// ---------------------------------------------------------------------------
// SQL-level visibility (non-colliding names) and broker-path coverage
// ---------------------------------------------------------------------------

#[test]
fn std_sql_level_registration() {
    // These names do not collide with built-ins, so evaluation reaches the
    // registry through the normal SQL path.
    let (matched, projected) = broker_rules::try_evaluate(
        Some("SELECT coalesce_ne(a, 'dflt') AS v FROM s"),
        None,
        "t",
        &json!({"a": null}),
    )
    .expect("evaluates");
    assert!(matched);
    assert_eq!(projected, Some(json!({"v": "dflt"})));

    let (matched, projected) = broker_rules::try_evaluate(
        Some("SELECT acosh(1.0) AS v FROM s"),
        None,
        "t",
        &json!({"a": 1}),
    )
    .expect("evaluates");
    assert!(matched);
    assert_eq!(projected, Some(json!({"v": 0.0})));

    let (matched, projected) = broker_rules::try_evaluate(
        Some("SELECT topic() AS t FROM s"),
        None,
        "sensors/kitchen",
        &json!({"a": 1}),
    )
    .expect("evaluates");
    assert!(matched);
    assert_eq!(projected, Some(json!({"t": "sensors/kitchen"})));

    // Colliding adapters go through the SQL rewriter to the standard
    // semantics (0-based substr, empty-dropping split, alias dispatch).
    let (matched, projected) = broker_rules::try_evaluate(
        Some("SELECT substr('abcdef', 0, 3) AS v FROM s"),
        None,
        "t",
        &json!({"a": 1}),
    )
    .expect("evaluates");
    assert!(matched);
    assert_eq!(projected, Some(json!({"v": "abc"})));

    let (matched, projected) = broker_rules::try_evaluate(
        Some("SELECT split('a,,b', ',') AS v FROM s"),
        None,
        "t",
        &json!({"a": 1}),
    )
    .expect("evaluates");
    assert!(matched);
    assert_eq!(projected, Some(json!({"v": ["a", "b"]})));

    let (matched, projected) = broker_rules::try_evaluate(
        Some("SELECT concat('a', 'b') AS v FROM s"),
        None,
        "t",
        &json!({"a": 1}),
    )
    .expect("evaluates");
    assert!(matched);
    assert_eq!(projected, Some(json!({"v": "ab"})));

    let (matched, projected) = broker_rules::try_evaluate(
        Some("SELECT length(a) AS v FROM s"),
        None,
        "t",
        &json!({"a": [1, 2, 3]}),
    )
    .expect("evaluates");
    assert!(matched);
    assert_eq!(projected, Some(json!({"v": 3})));

    let (matched, projected) = broker_rules::try_evaluate(
        Some("SELECT clientip() AS v FROM s"),
        None,
        "t",
        &json!({"peerhost": "10.0.0.9"}),
    )
    .expect("evaluates");
    assert!(matched);
    assert_eq!(projected, Some(json!({"v": "10.0.0.9"})));

    // Remaining colliding adapters at SQL level: multi-arg base64 and
    // 2-arg rtrim must also use the standard semantics, not the engine
    // 1-arg built-ins of the same name.
    let (matched, projected) = broker_rules::try_evaluate(
        Some("SELECT base64_encode('hello', 'urlsafe', 'no_padding') AS v FROM s"),
        None,
        "t",
        &json!({"a": 1}),
    )
    .expect("evaluates");
    assert!(matched);
    assert_eq!(projected, Some(json!({"v": "aGVsbG8"})));

    let (matched, projected) = broker_rules::try_evaluate(
        Some("SELECT base64_decode('aGVsbG8', 'no_padding') AS v FROM s"),
        None,
        "t",
        &json!({"a": 1}),
    )
    .expect("evaluates");
    assert!(matched);
    assert_eq!(projected, Some(json!({"v": "hello"})));

    let (matched, projected) = broker_rules::try_evaluate(
        Some("SELECT rtrim('xxhelloxx', 'x') AS v FROM s"),
        None,
        "t",
        &json!({"a": 1}),
    )
    .expect("evaluates");
    assert!(matched);
    assert_eq!(projected, Some(json!({"v": "xxhello"})));

    let (matched, projected) = broker_rules::try_evaluate(
        Some("SELECT rtrim('hello   ') AS v FROM s"),
        None,
        "t",
        &json!({"a": 1}),
    )
    .expect("evaluates");
    assert!(matched);
    assert_eq!(projected, Some(json!({"v": "hello"})));

    // Batch (window-style) path installs the partition context so
    // topic()/payload() resolve instead of reading stale state.
    let (matched, projected) = broker_rules::try_evaluate(
        Some("SELECT topic() AS t FROM s"),
        None,
        "sensors/batch",
        &json!([{"a": 1}, {"a": 2}]),
    )
    .expect("evaluates");
    if matched {
        let rows = projected.expect("rows");
        let arr = rows.as_array().expect("row array");
        assert!(!arr.is_empty(), "batch must project rows");
        for row in arr {
            assert_eq!(row.get("t"), Some(&s("sensors/batch")));
        }
    }
}

#[derive(Debug, Default)]
struct RecordingSink {
    published: StdMutex<Vec<(Topic, Bytes, QoS, bool)>>,
}

#[async_trait]
impl BrokerSink for RecordingSink {
    async fn publish(
        &self,
        topic: Topic,
        payload: Bytes,
        qos: QoS,
        retain: bool,
    ) -> Result<(), RuleEngineError> {
        self.published
            .lock()
            .unwrap()
            .push((topic, payload, qos, retain));
        Ok(())
    }
}

#[tokio::test]
async fn std_broker_path_rule_output() {
    // Through the broker ingress path (not the store): publish a message,
    // run standard functions in SELECT, and assert the sink output.
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    engine
        .create_rule(
            "rq03-probe".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            Some(
                "SELECT coalesce_ne(nick, 'anon') AS nick, strlen(s) AS n, bitsl(q, 1) AS q2 FROM s"
                    .to_string(),
            ),
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("out/rq03").unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("rule creates");
    let sink = Arc::new(RecordingSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    engine
        .dispatch_ingress(
            &Topic::new("sensors/kitchen").unwrap(),
            &Bytes::from_static(br#"{"nick": null, "s": "abcd", "q": 21}"#),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;
    let published = sink.published.lock().unwrap();
    assert_eq!(published.len(), 1, "rule must fire inline");
    assert_eq!(published[0].0.as_str(), "out/rq03");
    let row: Value = serde_json::from_slice(&published[0].1).expect("projected JSON");
    assert_eq!(row.get("nick"), Some(&s("anon")));
    assert_eq!(row.get("n"), Some(&json!(4)));
    assert_eq!(row.get("q2"), Some(&json!(42)));
}

#[tokio::test]
async fn std_broker_path_state() {
    // State functions through the broker ingress path (not the store
    // alone): kv_store is node-global so a broker put is visible to a
    // later broker get; proc_dict is rule-local so one rule's put/get
    // round-trips across dispatches with stable prev/cur semantics.
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    engine
        .create_rule(
            "rq03-kv-put".to_string(),
            TopicFilter::new("s/kvput").unwrap(),
            Some("SELECT kv_store_put('rk03-bkg', q) AS v FROM s".to_string()),
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("out/kvput").unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("rule creates");
    engine
        .create_rule(
            "rq03-kv-get".to_string(),
            TopicFilter::new("s/kvget").unwrap(),
            Some("SELECT kv_store_get('rk03-bkg') AS v FROM s".to_string()),
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("out/kvget").unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("rule creates");
    engine
        .create_rule(
            "rq03-pd-put".to_string(),
            TopicFilter::new("s/pda").unwrap(),
            Some("SELECT proc_dict_put('rk03-iso', q) AS cur FROM s".to_string()),
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("out/pda").unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("rule creates");
    engine
        .create_rule(
            "rq03-pd-get".to_string(),
            TopicFilter::new("s/pdb").unwrap(),
            Some("SELECT proc_dict_get('rk03-iso') AS v FROM s".to_string()),
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("out/pdb").unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("rule creates");
    let sink = Arc::new(RecordingSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    engine
        .dispatch_ingress(
            &Topic::new("s/kvput").unwrap(),
            &Bytes::from_static(br#"{"q": "hello"}"#),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;
    engine
        .dispatch_ingress(
            &Topic::new("s/kvget").unwrap(),
            &Bytes::from_static(br#"{"x": 1}"#),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;
    engine
        .dispatch_ingress(
            &Topic::new("s/pda").unwrap(),
            &Bytes::from_static(br#"{"q": 99}"#),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;
    engine
        .dispatch_ingress(
            &Topic::new("s/pdb").unwrap(),
            &Bytes::from_static(br#"{"x": 1}"#),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;
    let published = sink.published.lock().unwrap();
    assert_eq!(published.len(), 4, "all broker-path rules must fire");
    let kvput: Value = serde_json::from_slice(&published[0].1).expect("json");
    assert_eq!(kvput.get("v"), Some(&s("hello")));
    let kvget: Value = serde_json::from_slice(&published[1].1).expect("json");
    assert_eq!(kvget.get("v"), Some(&s("hello")));
    // proc_dict is rule-local: another rule's put is invisible here, so
    // the get rule observes Null (isolation holds on the broker path).
    let pda: Value = serde_json::from_slice(&published[2].1).expect("json");
    assert_eq!(pda.get("cur"), Some(&json!(99)));
    let pdb: Value = serde_json::from_slice(&published[3].1).expect("json");
    assert_eq!(pdb.get("v"), Some(&Value::Null));
}

#[tokio::test]
async fn std_broker_path_colliding() {
    // Colliding adapters through the broker: SQL uses the standard names
    // and the rewriter gives them the standard semantics.
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    engine
        .create_rule(
            "rq03-collide".to_string(),
            TopicFilter::new("s/col").unwrap(),
            Some(
                "SELECT substr(txt, 0, 3) AS v, split(txt, ',') AS parts, length(a) AS n, base64_encode(txt2, 'urlsafe', 'no_padding') AS b64, base64_decode(b64in, 'no_padding') AS plain, rtrim(padded, 'x') AS trimmed FROM s"
                    .to_string(),
            ),
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("out/col").unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("rule creates");
    let sink = Arc::new(RecordingSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    engine
        .dispatch_ingress(
            &Topic::new("s/col").unwrap(),
            &Bytes::from_static(
                br#"{"txt": "a,b,c", "a": [1, 2, 3], "txt2": "hello", "b64in": "aGVsbG8", "padded": "xxhelloxx"}"#,
            ),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;
    let published = sink.published.lock().unwrap();
    assert_eq!(published.len(), 1, "rule must fire inline");
    let row: Value = serde_json::from_slice(&published[0].1).expect("projected JSON");
    assert_eq!(row.get("v"), Some(&s("a,b")));
    assert_eq!(row.get("parts"), Some(&json!(["a", "b", "c"])));
    assert_eq!(row.get("n"), Some(&json!(3)));
    assert_eq!(row.get("b64"), Some(&s("aGVsbG8")));
    assert_eq!(row.get("plain"), Some(&s("hello")));
    assert_eq!(row.get("trimmed"), Some(&s("xxhello")));
}

// ---------------------------------------------------------------------------
// MV-12b: jq timeout, D4 execution fail-closed and static refusal
// ---------------------------------------------------------------------------

#[test]
fn std_jq_timeout_applied() {
    let _counter_guard = JQ_COUNTER_TEST_SERIAL.blocking_lock();
    // A valid per-call timeout (unit: milliseconds) keeps the 2-arg result.
    table(
        "jq",
        vec![
            (vec![s(".a"), s("{\"a\":1}"), json!(5000)], json!([1])),
            (vec![s(".missing"), s("{\"a\":1}"), json!(5000)], json!([])),
            (vec![s("."), json!({"a": 1}), s("5000")], json!([{"a": 1}])),
        ],
    );
    // Out-of-range or mistyped timeouts fail closed.
    table(
        "jq",
        vec![
            (vec![s(".a"), s("{\"a\":1}"), json!(0)], Value::Null),
            (vec![s(".a"), s("{\"a\":1}"), json!(-1)], Value::Null),
            (vec![s(".a"), s("{\"a\":1}"), json!(3_600_001)], Value::Null),
            (vec![s(".a"), s("{\"a\":1}"), s("soon")], Value::Null),
            (vec![s(".a"), s("{\"a\":1}"), json!(true)], Value::Null),
            (vec![s(".a"), s("{\"a\":1}"), Value::Null], Value::Null),
        ],
    );
    // A program past the step budget stops and counts exactly once.
    let deep = format!(".{}", vec!["a"; 65].join("."));
    let before = jq_timeout_count();
    assert_eq!(call("jq", vec![s(&deep), s("{\"a\":1}")]), Value::Null);
    assert_eq!(jq_timeout_count(), before + 1, "one stopped run");
    let before = jq_timeout_count();
    assert_eq!(
        call("jq", vec![s(&deep), s("{\"a\":1}"), json!(5000)]),
        Value::Null
    );
    assert_eq!(jq_timeout_count(), before + 1, "one stopped run");
    // A short program inside the budget never counts.
    let before = jq_timeout_count();
    assert_eq!(
        call("jq", vec![s(".a"), s("{\"a\":1}"), json!(5000)]),
        json!([1])
    );
    assert_eq!(jq_timeout_count(), before, "no stop in budget");
}

#[test]
fn std_d4_refused_at_execution() {
    // D4 hash algorithms fail closed at execution (creation refuses them).
    table(
        "hash",
        vec![
            (vec![s("sha3_256"), s("abc")], Value::Null),
            (vec![s("SHA3-512"), s("abc")], Value::Null),
            (vec![s("blake2b"), s("abc")], Value::Null),
            (vec![s("blake2s"), s("abc")], Value::Null),
        ],
    );
    // Named zones fail closed at execution (creation refuses literals).
    table(
        "date_to_unix_ts",
        vec![
            (
                vec![
                    s("second"),
                    s("+08:00"),
                    s("yyyy-MM-dd HH:mm:ss"),
                    s("1970-01-01 08:00:00"),
                ],
                json!(0),
            ),
            (
                vec![
                    s("second"),
                    s("Europe/Berlin"),
                    s("yyyy-MM-dd"),
                    s("1970-01-02"),
                ],
                Value::Null,
            ),
        ],
    );
}

#[test]
fn std_create_refuses_unsupported_static_calls() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    // Each refused call fails creation and names itself; nothing is stored.
    for (sql, want) in [
        ("SELECT schema_encode(a) AS v FROM s", "schema_encode"),
        ("SELECT SCHEMA_DECODE(a) AS v FROM s", "SCHEMA_DECODE"),
        ("SELECT hash('sha3_256', a) AS v FROM s", "sha3_256"),
        ("SELECT hash('blake2b', a) AS v FROM s", "blake2b"),
        (
            "SELECT timezone_to_offset_seconds('Europe/Berlin') AS v FROM s",
            "Europe/Berlin",
        ),
        (
            "SELECT timezone_to_second('Mars/Olympus Base') AS v FROM s",
            "Mars/Olympus Base",
        ),
        (
            "SELECT date_to_unix_ts('second', 'Europe/Berlin', 'yyyy-MM-dd', d) AS v FROM s",
            "Europe/Berlin",
        ),
        (
            "SELECT a AS v FROM s WHERE schema_encode(a) = 'x'",
            "schema_encode",
        ),
        (
            "SELECT count(*) AS n FROM s GROUP BY hash('sha3_256', a)",
            "sha3_256",
        ),
        (
            "FOREACH payload.items DO item AS v INCASE schema_encode(item) = 'x' FROM \"t/#\"",
            "schema_encode",
        ),
        (
            "SELECT a AS v FROM s ORDER BY schema_encode(a)",
            "schema_encode",
        ),
        (
            "SELECT * FROM s JOIN t ON schema_encode(a) = t.id",
            "schema_encode",
        ),
    ] {
        let err = engine
            .create_rule_with_id(
                "refused-probe".to_string(),
                "probe".to_string(),
                TopicFilter::new("s/r").unwrap(),
                Some(sql.to_string()),
                true,
                vec![],
            )
            .expect_err("refused call must fail creation");
        let message = match err {
            RuleEngineError::InvalidRule(message) => message,
            other => panic!("wrong error kind for {sql:?}: {other:?}"),
        };
        assert!(
            message.contains(want),
            "error names {want:?} for {sql:?}: {message:?}"
        );
        assert!(
            engine.get_rule("refused-probe").is_none(),
            "no refused rule is stored for {sql:?}"
        );
    }
    // UTC and fixed offsets keep working; a dynamic value also creates.
    engine
        .create_rule_with_id(
            "utc-ok".to_string(),
            "probe".to_string(),
            TopicFilter::new("s/r").unwrap(),
            Some("SELECT timezone_to_offset_seconds('+08:00') AS v FROM s".to_string()),
            true,
            vec![],
        )
        .expect("fixed offset creates");
    engine
        .create_rule_with_id(
            "dynamic-ok".to_string(),
            "probe".to_string(),
            TopicFilter::new("s/r").unwrap(),
            Some("SELECT hash(alg, 'abc') AS h FROM s".to_string()),
            true,
            vec![],
        )
        .expect("dynamic value creates; execution fails closed");
    // The tester refuses the same static calls.
    let err = broker_rules::try_evaluate(
        Some("SELECT schema_encode(a) AS v FROM s"),
        None,
        "t/a",
        &json!({"a": 1}),
    )
    .expect_err("tester refuses too");
    assert!(err.contains("schema_encode"), "tester names it: {err:?}");
    // A stored snapshot with a refused call fails loading loudly.
    let conf = broker_config::RulesConf {
        rules: vec![broker_config::RuleEntry {
            id: "bad-file".to_string(),
            name: "bad".to_string(),
            tenant: "default".to_string(),
            topic_filter: "s/r".to_string(),
            sql_query: Some("SELECT schema_encode(a) AS v FROM s".to_string()),
            enabled: true,
            actions: vec![],
        }],
    };
    let err = match RuleEngine::from_snapshot(&conf) {
        Ok(_) => panic!("file load must refuse the static call"),
        Err(err) => err,
    };
    let message = match err {
        RuleEngineError::InvalidRule(message) => message,
        other => panic!("wrong error kind for file load: {other:?}"),
    };
    assert!(
        message.contains("schema_encode"),
        "file load names it: {message:?}"
    );
}

#[tokio::test]
async fn std_refused_update_keeps_old_rule() {
    // A refused update fails and leaves the live rule working.
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    engine
        .create_rule_with_id(
            "live".to_string(),
            "live".to_string(),
            TopicFilter::new("s/live").unwrap(),
            Some("SELECT a AS v FROM s".to_string()),
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("out/live").unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("live rule creates");
    let err = engine
        .replace_rule_in_tenant(
            "live",
            "default".to_string(),
            "live".to_string(),
            TopicFilter::new("s/live").unwrap(),
            Some("SELECT schema_encode(a) AS v FROM s".to_string()),
            true,
            vec![],
        )
        .expect_err("refused update must fail");
    let message = match err {
        RuleEngineError::InvalidRule(message) => message,
        other => panic!("wrong error kind for update: {other:?}"),
    };
    assert!(
        message.contains("schema_encode"),
        "update names it: {message:?}"
    );
    let sink = Arc::new(RecordingSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    engine
        .dispatch_ingress(
            &Topic::new("s/live").unwrap(),
            &Bytes::from_static(br#"{"a": 7}"#),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;
    let published = sink.published.lock().unwrap();
    assert_eq!(published.len(), 1, "old rule still fires");
    let row: Value = serde_json::from_slice(&published[0].1).expect("row is JSON");
    assert_eq!(row.get("v"), Some(&json!(7)));
}

#[tokio::test]
async fn std_jq_timeout_counts_failed_rule_run() {
    let _counter_guard = JQ_COUNTER_TEST_SERIAL.lock().await;
    // A stopped jq program fails the run through the broker entry.
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let deep = format!(".{}", vec!["a"; 65].join("."));
    engine
        .create_rule_with_id(
            "jq-ok".to_string(),
            "ok".to_string(),
            TopicFilter::new("s/ok").unwrap(),
            Some("SELECT jq('.a', payload) AS v FROM \"s/ok\"".to_string()),
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("out/ok").unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("control rule creates");
    engine
        .create_rule_with_id(
            "jq-slow".to_string(),
            "slow".to_string(),
            TopicFilter::new("s/slow").unwrap(),
            Some(format!(
                "SELECT jq('.a', payload) AS v FROM \"s/slow\" WHERE jq('{deep}', payload) IS NOT NULL"
            )),
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("out/slow").unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("timeout rule creates; the stop happens at run time");
    let sink = Arc::new(RecordingSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    engine
        .dispatch_ingress(
            &Topic::new("s/ok").unwrap(),
            &Bytes::from_static(br#"{"a": 1}"#),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;
    let before = jq_timeout_count();
    engine
        .dispatch_ingress(
            &Topic::new("s/slow").unwrap(),
            &Bytes::from_static(br#"{"a": 1}"#),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;
    assert_eq!(jq_timeout_count(), before + 1, "one stopped run");
    let slow = engine.get_rule("jq-slow").expect("rule is stored");
    assert_eq!(
        slow.matched_cnt.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        slow.passed_cnt.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        slow.failed_cnt.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    let published = sink.published.lock().unwrap();
    assert_eq!(published.len(), 1, "only the control rule fires");
    assert_eq!(published[0].0.as_str(), "out/ok");
}
