//! Shared PostgreSQL-wire number binding for JSON-fed sinks.
//!
//! A JSON integer (for example `"temp": 0`) carries no column type.
//! The driver binds each Rust value to exactly one server type, so a
//! boxed `i64` fails against a `FLOAT8` parameter before the statement
//! reaches the server. This module converts each scalar value by the
//! declared parameter type that `prepare` reports, so one payload
//! writes to integer and float columns alike.

use super::{ConnectorError, Result};
use tokio_postgres::types::{IsNull, Type};

/// One scalar value bound to a PostgreSQL-wire parameter.
#[derive(Debug, Clone, PartialEq)]
pub enum PgScalarValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Text(String),
}

/// Largest integer with an exact `f64` spelling: 2^53. A float past it
/// cannot name one integer, so integer targets refuse it below.
const MAX_EXACT_INT_IN_F64: f64 = 9_007_199_254_740_992.0;

/// True for the text-like server types the driver's `String` binding
/// accepts, plus the `citext` extension type the sibling wrappers
/// accept by name.
fn is_text_family(ty: &Type) -> bool {
    matches!(
        *ty,
        Type::VARCHAR | Type::TEXT | Type::BPCHAR | Type::NAME | Type::UNKNOWN
    ) || ty.name() == "citext"
}

/// Shortest round-trip spelling of a float: `20.5` stays `20.5`.
fn float_spelling(value: f64) -> String {
    value.to_string()
}

/// NULL binds as SQL NULL for any declared type. `None::<String>`
/// accepts text-like types only, so NULL into a float column fails
/// the same way an integer does; this wrapper accepts every type.
#[derive(Debug, Clone, Copy)]
struct PgNullParam;

impl tokio_postgres::types::ToSql for PgNullParam {
    fn to_sql(
        &self,
        _ty: &Type,
        _out: &mut bytes::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        Ok(IsNull::Yes)
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !<Self as tokio_postgres::types::ToSql>::accepts(ty) {
            return Err(format!("null parameter has no binding for {ty}").into());
        }
        <Self as tokio_postgres::types::ToSql>::to_sql(self, ty, out)
    }
}

/// NUMERIC text binding. The driver has no native NUMERIC value, so
/// the decimal spelling travels in text format and the server parses it.
#[derive(Debug, Clone)]
struct PgNumericTextParam(String);

impl tokio_postgres::types::ToSql for PgNumericTextParam {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::NUMERIC
    }

    fn encode_format(&self, _ty: &Type) -> tokio_postgres::types::Format {
        tokio_postgres::types::Format::Text
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !<Self as tokio_postgres::types::ToSql>::accepts(ty) {
            return Err(format!("numeric parameter has no binding for {ty}").into());
        }
        <Self as tokio_postgres::types::ToSql>::to_sql(self, ty, out)
    }
}

/// JSON/JSONB binding for scalar values. JSONB needs the version byte
/// plus the document; plain JSON needs the document alone.
#[derive(Debug, Clone)]
struct PgJsonTextParam(String);

impl tokio_postgres::types::ToSql for PgJsonTextParam {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if *ty == Type::JSONB {
            out.extend_from_slice(&[1u8]);
        }
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::JSON | Type::JSONB)
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !<Self as tokio_postgres::types::ToSql>::accepts(ty) {
            return Err(format!("json parameter has no binding for {ty}").into());
        }
        <Self as tokio_postgres::types::ToSql>::to_sql(self, ty, out)
    }
}

/// Permanent error for an out-of-range numeric value. Carries SQLSTATE
/// `22003` (numeric value out of range, class `22`), so the sink counts
/// the row rejected through its existing per-row path.
fn range_error(column: &str, sink: &str, detail: String) -> ConnectorError {
    ConnectorError::Dispatch(format!(
        "22003: {sink} value for column '{column}' {detail}"
    ))
}

/// Permanent error for a value of the wrong kind. Carries SQLSTATE
/// `42804` (datatype mismatch), so the sink counts the row rejected
/// through its existing per-row path.
fn type_error(column: &str, sink: &str, detail: String) -> ConnectorError {
    ConnectorError::Dispatch(format!(
        "42804: {sink} value for column '{column}' {detail}"
    ))
}

/// Read a float as one exact integer. Only a finite float with no
/// fraction and a magnitude at or below 2^53 names an integer; the
/// bound keeps the cast exact, so nothing is silently cut.
// TODO(parity): should a fractional float with its fraction dropped
// ever bind to an integer column, or stay refused as here?
fn float_as_integer(value: f64, column: &str, sink: &str) -> Result<i64> {
    if !value.is_finite() || value.fract() != 0.0 || value.abs() > MAX_EXACT_INT_IN_F64 {
        return Err(range_error(
            column,
            sink,
            format!("cannot bind {value} to an integer type"),
        ));
    }
    Ok(value as i64)
}

/// Bind one integer by the declared parameter type. Integers into
/// `INT2`/`INT4`/`OID` check the range; out-of-range values are a
/// permanent error that names the column, never a silent cut.
pub fn coerce_integer_for_type(
    value: i64,
    ty: &Type,
    column: &str,
    sink: &str,
) -> Result<Box<dyn tokio_postgres::types::ToSql + Send + Sync>> {
    if *ty == Type::INT2 {
        let narrowed = i16::try_from(value).map_err(|_| {
            range_error(
                column,
                sink,
                format!("cannot bind {value} to INT2: out of range"),
            )
        })?;
        return Ok(Box::new(narrowed));
    }
    if *ty == Type::INT4 {
        let narrowed = i32::try_from(value).map_err(|_| {
            range_error(
                column,
                sink,
                format!("cannot bind {value} to INT4: out of range"),
            )
        })?;
        return Ok(Box::new(narrowed));
    }
    if *ty == Type::INT8 {
        return Ok(Box::new(value));
    }
    if *ty == Type::OID {
        // OID is unsigned 32-bit; negative values have no binding.
        if value < 0 || value > u32::MAX as i64 {
            return Err(range_error(
                column,
                sink,
                format!("cannot bind {value} to OID: out of range"),
            ));
        }
        return Ok(Box::new(value as u32));
    }
    // Float columns are approximate by design, so the cast is exact
    // for the magnitudes devices send; no range applies here.
    if *ty == Type::FLOAT4 {
        return Ok(Box::new(value as f32));
    }
    if *ty == Type::FLOAT8 {
        return Ok(Box::new(value as f64));
    }
    if *ty == Type::NUMERIC {
        return Ok(Box::new(PgNumericTextParam(value.to_string())));
    }
    if *ty == Type::JSON || *ty == Type::JSONB {
        return Ok(Box::new(PgJsonTextParam(value.to_string())));
    }
    if is_text_family(ty) {
        return Ok(Box::new(value.to_string()));
    }
    Err(type_error(
        column,
        sink,
        format!("cannot bind INTEGER {value} to {}", ty.name()),
    ))
}

/// Bind one float by the declared parameter type. Floats into integer
/// types must name an exact integer (see [`float_as_integer`); the
/// range check then follows the integer path, with the same permanent
/// error.
pub fn coerce_float_for_type(
    value: f64,
    ty: &Type,
    column: &str,
    sink: &str,
) -> Result<Box<dyn tokio_postgres::types::ToSql + Send + Sync>> {
    if *ty == Type::FLOAT8 {
        return Ok(Box::new(value));
    }
    if *ty == Type::FLOAT4 {
        return Ok(Box::new(value as f32));
    }
    if *ty == Type::INT2 || *ty == Type::INT4 || *ty == Type::INT8 || *ty == Type::OID {
        let integer = float_as_integer(value, column, sink)?;
        return coerce_integer_for_type(integer, ty, column, sink);
    }
    if *ty == Type::NUMERIC {
        // The server parses decimal text but rejects infinity, so a
        // non-finite float stays refused instead of reaching the server.
        if !value.is_finite() {
            return Err(range_error(
                column,
                sink,
                format!("cannot bind {value} to NUMERIC"),
            ));
        }
        return Ok(Box::new(PgNumericTextParam(float_spelling(value))));
    }
    if *ty == Type::JSON || *ty == Type::JSONB {
        // `inf` has no JSON spelling, so it stays refused here; finite
        // values travel as their JSON document.
        if !value.is_finite() {
            return Err(type_error(
                column,
                sink,
                format!("cannot bind {value} to {}", ty.name()),
            ));
        }
        return Ok(Box::new(PgJsonTextParam(float_spelling(value))));
    }
    if is_text_family(ty) {
        return Ok(Box::new(float_spelling(value)));
    }
    Err(type_error(
        column,
        sink,
        format!("cannot bind NUMBER {value} to {}", ty.name()),
    ))
}

/// Bind one boolean by the declared parameter type.
fn coerce_bool_for_type(
    value: bool,
    ty: &Type,
    column: &str,
    sink: &str,
) -> Result<Box<dyn tokio_postgres::types::ToSql + Send + Sync>> {
    if *ty == Type::BOOL {
        return Ok(Box::new(value));
    }
    if *ty == Type::JSON || *ty == Type::JSONB {
        return Ok(Box::new(PgJsonTextParam(value.to_string())));
    }
    if is_text_family(ty) {
        return Ok(Box::new(value.to_string()));
    }
    Err(type_error(
        column,
        sink,
        format!("cannot bind BOOLEAN {value} to {}", ty.name()),
    ))
}

/// Bind one string by the declared parameter type. Numeric text stays
/// refused: parsing it would guess a type the payload never stated.
// TODO(parity): should numeric text (for example `"20"`) parse into a
// numeric column, or stay refused as here?
fn coerce_text_for_type(
    value: String,
    ty: &Type,
    column: &str,
    sink: &str,
) -> Result<Box<dyn tokio_postgres::types::ToSql + Send + Sync>> {
    if is_text_family(ty) {
        return Ok(Box::new(value));
    }
    if *ty == Type::JSON || *ty == Type::JSONB {
        // A plain string is a JSON string scalar, so it travels quoted.
        let document = serde_json::Value::String(value).to_string();
        return Ok(Box::new(PgJsonTextParam(document)));
    }
    Err(type_error(
        column,
        sink,
        format!("cannot bind TEXT to {}", ty.name()),
    ))
}

/// Bind one scalar value by the declared parameter type. A value that
/// cannot become the declared type is a permanent error that names the
/// column.
pub fn coerce_scalar_value(
    value: PgScalarValue,
    ty: &Type,
    column: &str,
    sink: &str,
) -> Result<Box<dyn tokio_postgres::types::ToSql + Send + Sync>> {
    match value {
        PgScalarValue::Null => Ok(Box::new(PgNullParam)),
        PgScalarValue::Boolean(b) => coerce_bool_for_type(b, ty, column, sink),
        PgScalarValue::Integer(i) => coerce_integer_for_type(i, ty, column, sink),
        PgScalarValue::Float(f) => coerce_float_for_type(f, ty, column, sink),
        PgScalarValue::Text(s) => coerce_text_for_type(s, ty, column, sink),
    }
}

/// Bind one JSON value by the declared parameter type. Integers past
/// `i64::MAX` travel as floats to float-like targets and stay refused
/// for integer targets, so no bit is silently cut.
// TODO(parity): should integers past `i64::MAX` that must stay exact
// use a wider binding instead of the float spelling used here?
pub fn coerce_json_value(
    value: &serde_json::Value,
    ty: &Type,
    column: &str,
    sink: &str,
) -> Result<Box<dyn tokio_postgres::types::ToSql + Send + Sync>> {
    match value {
        serde_json::Value::Null => coerce_scalar_value(PgScalarValue::Null, ty, column, sink),
        serde_json::Value::Bool(b) => {
            coerce_scalar_value(PgScalarValue::Boolean(*b), ty, column, sink)
        }
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                coerce_scalar_value(PgScalarValue::Integer(i), ty, column, sink)
            } else if let Some(u) = n.as_u64() {
                if u <= i64::MAX as u64 {
                    coerce_scalar_value(PgScalarValue::Integer(u as i64), ty, column, sink)
                } else {
                    coerce_scalar_value(PgScalarValue::Float(u as f64), ty, column, sink)
                }
            } else if let Some(f) = n.as_f64() {
                coerce_scalar_value(PgScalarValue::Float(f), ty, column, sink)
            } else {
                Err(range_error(
                    column,
                    sink,
                    format!("cannot bind NUMBER {n} to {}", ty.name()),
                ))
            }
        }
        serde_json::Value::String(s) => {
            coerce_scalar_value(PgScalarValue::Text(s.clone()), ty, column, sink)
        }
        // Arrays and objects have no scalar binding; they travel as
        // their canonical JSON document to text-like and JSON targets.
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            let document = serde_json::to_string(value).map_err(|e| {
                type_error(column, sink, format!("cannot encode JSON document: {e}"))
            })?;
            coerce_scalar_value(PgScalarValue::Text(document), ty, column, sink)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::types::ToSql;

    fn accepts(boxed: &(dyn tokio_postgres::types::ToSql + Send + Sync), ty: &Type) -> bool {
        boxed
            .to_sql_checked(ty, &mut bytes::BytesMut::new())
            .is_ok()
    }

    #[test]
    fn test_null_binds_to_any_declared_type() {
        // NULL reaches every column kind, including float columns where
        // the old `None::<String>` binding fails client-side.
        for ty in [Type::FLOAT8, Type::INT4, Type::TEXT, Type::BOOL] {
            let boxed =
                coerce_scalar_value(PgScalarValue::Null, &ty, "col", "test").expect("null binds");
            assert!(accepts(boxed.as_ref(), &ty), "null must bind to {ty}");
        }
    }

    #[test]
    fn test_integer_binds_to_float_numeric_and_text() {
        // The defect: a boxed `i64` reaches only INT8. The coercion
        // reaches the declared type instead.
        let old: Box<dyn ToSql + Send + Sync> = Box::new(0i64);
        assert!(old
            .to_sql_checked(&Type::FLOAT8, &mut bytes::BytesMut::new())
            .is_err());
        for ty in [Type::FLOAT8, Type::FLOAT4, Type::NUMERIC, Type::TEXT] {
            let boxed = coerce_scalar_value(PgScalarValue::Integer(0), &ty, "temp", "test")
                .expect("integer binds");
            assert!(accepts(boxed.as_ref(), &ty), "integer must bind to {ty}");
        }
    }

    #[test]
    fn test_fractional_float_into_integer_stays_refused() {
        // A fractional float names no integer, so it stays a permanent
        // error that names the column.
        let err = coerce_scalar_value(PgScalarValue::Float(20.5), &Type::INT4, "seq", "test")
            .expect_err("fraction must fail");
        assert!(matches!(err, ConnectorError::Dispatch(_)));
        assert!(err.to_string().contains("seq"));
    }

    #[test]
    fn test_text_into_json_travels_quoted() {
        // A plain string is a JSON string scalar, so the document
        // carries quotes; the driver accepts the JSONB target.
        let boxed = coerce_scalar_value(
            PgScalarValue::Text("edge-node".into()),
            &Type::JSONB,
            "doc",
            "test",
        )
        .expect("text binds to jsonb");
        let mut out = bytes::BytesMut::new();
        boxed
            .to_sql_checked(&Type::JSONB, &mut out)
            .expect("driver accepts");
        assert_eq!(&out[1..], br#""edge-node""#);
    }

    #[test]
    fn test_json_value_binding_covers_wide_integers() {
        // Integers past `i64::MAX` travel as floats to float targets
        // and stay refused for integer targets; arrays travel as
        // documents to text targets and stay refused for integers.
        let wide = serde_json::json!(u64::MAX);
        let boxed = coerce_json_value(&wide, &Type::FLOAT8, "n", "test").expect("wide int binds");
        assert!(accepts(boxed.as_ref(), &Type::FLOAT8));
        let err = coerce_json_value(&wide, &Type::INT8, "n", "test")
            .expect_err("wide int into bigint must fail");
        assert!(matches!(err, ConnectorError::Dispatch(_)));
        let array = serde_json::json!([1, 2]);
        let boxed =
            coerce_json_value(&array, &Type::TEXT, "doc", "test").expect("array binds to text");
        assert!(accepts(boxed.as_ref(), &Type::TEXT));
        let err = coerce_json_value(&array, &Type::INT4, "n", "test")
            .expect_err("array into integer must fail");
        assert!(err.to_string().contains("n"));
    }
}
