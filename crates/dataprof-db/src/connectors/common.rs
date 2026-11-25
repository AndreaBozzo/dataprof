//! Common utilities and shared logic for database connectors
//!
//! This module provides reusable functions to reduce code duplication across
//! PostgreSQL, MySQL, and SQLite connectors.

use crate::DataProfilerError;
use crate::security::{validate_base_query, validate_sql_identifier};

/// Begin the transaction a profiled query runs in, which the database itself
/// keeps from writing (#1169). `begin` is the backend's statement for it.
#[cfg(any(feature = "postgres", feature = "mysql"))]
pub async fn read_only_transaction<DB: sqlx::Database>(
    pool: &sqlx::Pool<DB>,
    begin: &'static str,
) -> Result<sqlx::Transaction<'static, DB>, DataProfilerError> {
    pool.begin_with(begin).await.map_err(|e| {
        DataProfilerError::database_query(&format!("Could not start a read-only transaction: {e}"))
    })
}

/// End a read-only transaction. Rolling back also undoes a session setting
/// the query changed, such as PostgreSQL's `set_config`. A transaction dropped
/// on an early return is rolled back by sqlx too.
#[cfg(any(feature = "postgres", feature = "mysql"))]
pub async fn end_read_only_transaction<DB: sqlx::Database>(
    transaction: sqlx::Transaction<'static, DB>,
) -> Result<(), DataProfilerError> {
    transaction.rollback().await.map_err(|e| {
        DataProfilerError::database_query(&format!("Could not end the read-only transaction: {e}"))
    })
}

/// Generate "not connected to database" error
#[allow(dead_code)]
pub fn not_connected_error() -> DataProfilerError {
    DataProfilerError::database_connection("Not connected to database")
}

/// Generate feature-not-enabled error for a specific database
#[allow(dead_code)]
pub fn feature_not_enabled_error(db_name: &str, feature: &str) -> DataProfilerError {
    DataProfilerError::database_feature_disabled(db_name, feature)
}

/// Render one column of one row as the string the profiler ingests.
///
/// The profiler consumes columns as `Vec<String>` and re-infers types from the
/// textual form, so every SQL type has to be turned into a faithful string.
/// sqlx offers no backend-agnostic "decode as whatever this is" call, so we try
/// concrete types in order and take the first that decodes.
///
/// Order is load-bearing:
///
/// * `Option<String>` first. sqlx skips the type-compatibility check when the
///   value is NULL, so a NULL of *any* SQL type decodes here as `Ok(None)`.
///   That makes this arm the NULL detector as well as the text arm; everything
///   below it is known to be a non-null value.
/// * Integers before `bool`. SQLite stores booleans as INTEGER and will happily
///   decode `42` as `true`, so trying `bool` early would render integers as
///   "true"/"false".
/// * Widest integer first, so an INT8/BIGINT is not truncated by a narrower arm.
/// * The caller's backend-specific arms sit between the signed integers and
///   `bool`, for the same reason: MySQL's `BIGINT UNSIGNED` fits no signed arm
///   and used to reach `bool`, where 18446744073709551615 rendered as "true"
///   (#365).
/// * Temporal and UUID after the primitives. Neither is compatible with an
///   earlier arm, so their position is about keeping the common path short
///   rather than about correctness.
///
/// Two types cannot live in the shared chain because they have no impl for
/// every backend, so each connector passes its own: `u64` (MySQL only, no
/// `Type<Postgres>`) and `BigDecimal` (PostgreSQL and MySQL, no `Type<Sqlite>`
/// because SQLite has no decimal type).
///
/// PostgreSQL `TIMESTAMPTZ` renders as a UTC instant with a trailing `Z`,
/// matching Arrow's UTC timestamp spelling, for years 0 to 9999; a year
/// outside RFC 3339 keeps the naive form. Other temporal renderings keep
/// their existing forms, including naive ISO text for PostgreSQL `TIMESTAMP`.
///
/// The last arm is [`DecodeStructured`]: JSON, array and PostgreSQL enum types,
/// which each backend decodes its own way (#766, #952). It yields the only
/// values that can be containers, so the macro evaluates to the text and
/// whether the value was a container: `Ok((Option<String>, bool))`.
///
/// A non-null value no arm decodes is an error naming the column and its type
/// (#952): recording it as null reported a column the connector could not read
/// as entirely missing. Binary values are the exception, see
/// [`is_binary_type`]: they still read as null until they have a column type.
#[macro_export]
macro_rules! db_column_to_string {
    ($row:expr, $index:expr) => {
        $crate::db_column_to_string!($row, $index, [])
    };
    ($row:expr, $index:expr, [$($backend_ty:ty),* $(,)?]) => {{
        let row = $row;
        let index = $index;
        let mut container = false;
        let mut undecodable = false;

        let text = if let Ok(v) = row.try_get::<Option<String>, _>(index) {
            v
        } else if let Ok(v) = row.try_get::<Option<i64>, _>(index) {
            v.map(|x| x.to_string())
        } else if let Ok(v) = row.try_get::<Option<i32>, _>(index) {
            v.map(|x| x.to_string())
        } else if let Ok(v) = row.try_get::<Option<i16>, _>(index) {
            v.map(|x| x.to_string())
        }
        // Arms the shared chain cannot express because the type has no impl
        // for one of the backends, each rendered by its `BackendText`. The text
        // is exact: `u64` prints its digits, and `BigDecimal` keeps the
        // database's value in plain digits rather than rounding through f64.
        // A PostgreSQL `NUMERIC` reads through `PgNumericText`, at its display
        // scale: sqlx's `BigDecimal` pads it to whole groups of four decimals
        // and drops that scale (transact-rs/sqlx#468).
        $(else if let Ok(v) = row.try_get::<Option<$backend_ty>, _>(index) {
            v.map(|x| $crate::BackendText::backend_text(&x))
        })*
        else if let Ok(v) = row.try_get::<Option<f64>, _>(index) {
            // `{:?}` keeps the decimal point on integral floats ("100.0", not
            // "100"), so a REAL column of whole numbers is still inferred as a
            // float downstream. It also stays compact at the extremes ("1e300").
            v.map(|x| format!("{:?}", x))
        } else if let Ok(v) = row.try_get::<Option<f32>, _>(index) {
            v.map(|x| format!("{:?}", x))
        } else if let Ok(v) = row.try_get::<Option<bool>, _>(index) {
            v.map(|x| x.to_string())
        } else if let Ok(v) =
            row.try_get::<Option<::sqlx::types::chrono::DateTime<::sqlx::types::chrono::Utc>>, _>(
                index,
            )
        {
            let column = ::sqlx::Row::column(row, index);
            let type_name = ::sqlx::TypeInfo::name(::sqlx::Column::type_info(column));
            v.map(|x| {
                let mut text = $crate::render_naive_datetime(x.naive_utc());
                // RFC 3339 has four-digit years. A year outside 0 to 9999
                // (PostgreSQL allows them) keeps the naive form the date
                // grammar reads, rather than a `Z` it would not parse; `%Y`
                // signs exactly those years (`+10000`, `-0001`).
                if type_name == "TIMESTAMPTZ" && !text.starts_with(['+', '-']) {
                    text.push('Z');
                }
                text
            })
        } else if let Ok(v) =
            row.try_get::<Option<::sqlx::types::chrono::NaiveDateTime>, _>(index)
        {
            v.map($crate::render_naive_datetime)
        } else if let Ok(v) = row.try_get::<Option<::sqlx::types::chrono::NaiveDate>, _>(index) {
            v.map(|x| x.format("%Y-%m-%d").to_string())
        } else if let Ok(v) = row.try_get::<Option<::sqlx::types::chrono::NaiveTime>, _>(index) {
            // A time of day carries no date, so it profiles as text. Decoding
            // it is still better than reporting the column as entirely null.
            v.map(|x| x.format("%H:%M:%S%.f").to_string())
        } else if let Ok(v) = row.try_get::<Option<::sqlx::types::Uuid>, _>(index) {
            v.map(|x| x.to_string())
        } else if let Some(value) = $crate::DecodeStructured::decode_structured(row, index) {
            container = value.container;
            Some(value.text)
        } else {
            undecodable = true;
            None
        };
        if undecodable {
            let column = ::sqlx::Row::column(row, index);
            let type_name = ::sqlx::TypeInfo::name(::sqlx::Column::type_info(column));
            if $crate::is_binary_type(type_name) {
                // decode-audit: no-data — binary columns still read as null,
                // as the connector guide documents, until they have a column
                // type of their own (#645).
                Ok((None, false))
            } else {
                // A value the chain cannot decode is not a missing value:
                // recording it as null reported a column the connector could
                // not read as entirely missing (#952).
                Err($crate::undecodable_value_error(
                    ::sqlx::Column::name(column),
                    type_name,
                ))
            }
        } else {
            Ok((text, container))
        }
    }};
}

/// The error for a non-null value the decode chain cannot read (#952).
///
/// It names the column and its type and says how to get past it, because the
/// alternative the chain used to take, recording the value as null, reported a
/// column the connector could not read as entirely missing.
pub fn undecodable_value_error(column: &str, type_name: &str) -> DataProfilerError {
    DataProfilerError::database_query(&format!(
        "column \"{column}\" has type {type_name}, which dataprof cannot decode; \
         cast it to text in the query, or leave the column out"
    ))
}

/// Whether a column of this declared type holds raw bytes.
///
/// Binary values fall past the whole decode chain, and the connector guide
/// documents them as profiling as null until they have a column type (#645),
/// so they are the one exception to failing on an undecodable value (#952).
/// The names are sqlx's: PostgreSQL `BYTEA`, MySQL's blob and binary types, and
/// SQLite's `BLOB` storage class.
pub fn is_binary_type(type_name: &str) -> bool {
    matches!(
        type_name,
        "BYTEA" | "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BINARY" | "VARBINARY"
    )
}

/// A value of a JSON or array column, rendered for the profiler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuredValue {
    /// The text the profiler ingests. Empty for a JSON `null`, which then
    /// counts as missing, as it does in the JSON readers.
    pub text: String,
    /// Whether the value was an object or an array. A column whose non-null
    /// values all are is reported as nested.
    pub container: bool,
}

/// Decode the types one backend reads its own way: JSON, JSONB and arrays, and
/// PostgreSQL enums (#952).
///
/// Neither decodes as `String` through sqlx, so before this every such value
/// fell past the whole chain and was recorded as SQL NULL (#766). Called for
/// non-null values only: the `Option<String>` arm has already taken every NULL.
/// `None` means the column is of none of these types.
pub trait DecodeStructured {
    fn decode_structured(&self, index: usize) -> Option<StructuredValue>;
}

/// Render a decoded JSON value the way the JSON readers render it.
///
/// Objects get their keys sorted, so two equal documents render alike however
/// the database ordered them: PostgreSQL `json` and MySQL keep the source order,
/// and duplicate-row detection compares this text (#767). A scalar renders as
/// itself and is profiled as a scalar, so a `jsonb` column of numbers is typed
/// from its values, as a JSONL field of numbers is.
#[cfg(any(feature = "postgres", feature = "mysql"))]
pub fn json_structured_value(value: &::sqlx::types::JsonValue) -> StructuredValue {
    StructuredValue {
        text: dataprof_json::record::json_value_text(value),
        container: value.is_array() || value.is_object(),
    }
}

/// A JSON document serde_json could not decode into a value, or would round,
/// kept as the database's text.
///
/// The database accepted it, so it is a value, not a missing one: PostgreSQL
/// takes numbers beyond `f64` (`1e400`), integers beyond 64 bits that serde_json
/// rounds to `f64` (#1104), and nesting deeper than serde_json's recursion
/// limit. The text is still made canonical, with sorted keys and no
/// whitespace between tokens, so key order does not split duplicate rows here
/// either. Only nesting past the recursion limit keeps the text as stored.
#[cfg(any(feature = "postgres", feature = "mysql"))]
pub fn raw_json_structured_value(text: &str) -> StructuredValue {
    let text = text.trim();
    let mut canonical = String::with_capacity(text.len());
    let text = match write_canonical_raw_json(text, &mut canonical) {
        Ok(()) => canonical,
        Err(_) => text.to_string(),
    };
    let container = text.starts_with(['{', '[']);
    StructuredValue { text, container }
}

/// Write `text` with every object's keys sorted, as
/// [`json_value_text`](dataprof_json::record::json_value_text) does, without
/// converting any number. Containers are split into raw members, which serde_json
/// only checks for syntax, so a number beyond `f64` keeps its digits.
#[cfg(any(feature = "postgres", feature = "mysql"))]
fn write_canonical_raw_json(text: &str, out: &mut String) -> Result<(), serde_json::Error> {
    use serde_json::value::RawValue;
    use std::collections::BTreeMap;

    match text.as_bytes().first() {
        Some(b'{') => {
            // A later duplicate key wins, as when serde_json builds a value.
            let fields: BTreeMap<String, &RawValue> = serde_json::from_str(text)?;
            out.push('{');
            for (index, (key, field)) in fields.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(&key)?);
                out.push(':');
                write_canonical_raw_json(field.get().trim(), out)?;
            }
            out.push('}');
        }
        Some(b'[') => {
            let items: Vec<&RawValue> = serde_json::from_str(text)?;
            out.push('[');
            for (index, item) in items.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_canonical_raw_json(item.get().trim(), out)?;
            }
            out.push(']');
        }
        // One spelling per string: `"\u0041"` and `"A"` are the same value.
        Some(b'"') => out.push_str(&serde_json::to_string(&serde_json::from_str::<String>(
            text,
        )?)?),
        _ => out.push_str(text),
    }
    Ok(())
}

/// Render a naive datetime in the ISO form dataprof's date grammar accepts.
///
/// `%.f` is empty at whole-second precision and prints a fractional part
/// otherwise, both of which the grammar's `(?:\.\d+)?` allows.
///
/// Gated on a backend being enabled: with no feature on, sqlx is not linked at
/// all and this signature would not resolve.
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub fn render_naive_datetime(value: ::sqlx::types::chrono::NaiveDateTime) -> String {
    value.format("%Y-%m-%dT%H:%M:%S%.f").to_string()
}

/// The text of a value decoded through one of a connector's own arms of
/// [`db_column_to_string!`](crate::db_column_to_string), for the types the
/// shared chain cannot hold.
pub trait BackendText {
    /// The value as the profiler reads it.
    fn backend_text(&self) -> String;
}

impl BackendText for u64 {
    fn backend_text(&self) -> String {
        self.to_string()
    }
}

/// A `NUMERIC` or `DECIMAL` in plain digits. `Display` writes an exponent past
/// 15 trailing zeros or 5 leading ones (`-900e+20`, `1E-7`), and how many is
/// set when `bigdecimal` is built. The profiler holds a declared decimal
/// exactly from its digits as written, so a value written short read as a
/// float beside its exactly held neighbours, and their mean kept its rounding
/// (#1342).
#[cfg(any(feature = "postgres", feature = "mysql"))]
impl BackendText for ::sqlx::types::BigDecimal {
    fn backend_text(&self) -> String {
        self.to_plain_string()
    }
}

/// A PostgreSQL `NUMERIC` in plain digits at its own display scale, as
/// PostgreSQL prints it and as an Arrow or Parquet decimal is rendered.
///
/// sqlx's `BigDecimal` takes its scale from whole groups of four decimals and
/// ignores the display scale the value carries (transact-rs/sqlx#468):
/// `NUMERIC(9,2)` 0.01 reads `0.0100`, 4.00 reads `4`. The profiler keeps a
/// declared decimal exact from 11 written digits, so the padding turned
/// `1000000.01` exact beside its neighbours read as floats, and moved the
/// quartiles off the other routes (#1342). The digits past the display scale
/// are zero, so setting it drops only padding.
#[cfg(feature = "postgres")]
pub struct PgNumericText(String);

#[cfg(feature = "postgres")]
impl ::sqlx::Type<::sqlx::Postgres> for PgNumericText {
    fn type_info() -> ::sqlx::postgres::PgTypeInfo {
        <::sqlx::types::BigDecimal as ::sqlx::Type<::sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &::sqlx::postgres::PgTypeInfo) -> bool {
        <::sqlx::types::BigDecimal as ::sqlx::Type<::sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "postgres")]
impl<'r> ::sqlx::Decode<'r, ::sqlx::Postgres> for PgNumericText {
    fn decode(value: ::sqlx::postgres::PgValueRef<'r>) -> Result<Self, ::sqlx::error::BoxDynError> {
        use std::str::FromStr;
        match value.format() {
            ::sqlx::postgres::PgValueFormat::Binary => {
                // The header is four 16-bit fields: digit count, weight, sign
                // and display scale.
                let bytes = value.as_bytes()?;
                let scale = bytes
                    .get(6..8)
                    .map(|field| i16::from_be_bytes([field[0], field[1]]))
                    .ok_or("NUMERIC value shorter than its header")?;
                let decimal =
                    <::sqlx::types::BigDecimal as ::sqlx::Decode<::sqlx::Postgres>>::decode(value)?;
                Ok(Self(decimal.with_scale(i64::from(scale)).to_plain_string()))
            }
            ::sqlx::postgres::PgValueFormat::Text => Ok(Self(
                ::sqlx::types::BigDecimal::from_str(value.as_str()?)?.to_plain_string(),
            )),
        }
    }
}

#[cfg(feature = "postgres")]
impl BackendText for PgNumericText {
    fn backend_text(&self) -> String {
        self.0.clone()
    }
}

#[cfg(feature = "mysql")]
impl BackendText for ::sqlx::mysql::types::MySqlTime {
    fn backend_text(&self) -> String {
        self.to_string()
    }
}

/// Macro to fetch a profiled query's result in one execution, read as a stream.
///
/// The result comes from a single execution. Paging it as `LIMIT n OFFSET k`
/// executions with no order of their own let pages overlap and skip rows: on
/// PostgreSQL a sequential scan of a large table starts near where the previous
/// one stopped (#887). `$batch_size` only sets how often progress is logged.
#[macro_export]
macro_rules! streaming_profile_loop {
    ($pool:expr, $query:expr, $batch_size:expr, $db_name:literal) => {
        $crate::streaming_profile_loop!($pool, $query, $batch_size, $db_name, [])
    };
    ($pool:expr, $query:expr, $batch_size:expr, $db_name:literal,
     [$($backend_ty:ty),* $(,)?]) => {{
        use futures::TryStreamExt;
        use sqlx::{Column, Executor, Row};
        use $crate::connectors::common::build_select_query;

        let select_query = build_select_query($query)?;
        // Describe the query before fetching rows. Row metadata is unavailable
        // when the result is empty, but projection still needs the source
        // schema to validate duplicate and unknown requested names.
        let description = ($pool).describe(::sqlx::SqlSafeStr::into_sql_str(::sqlx::AssertSqlSafe(select_query.as_str()))).await.map_err(|e| {
            $crate::DataProfilerError::DatabaseQueryError {
                message: format!("Query description failed: {}", e),
            }
        })?;
        let column_names: Vec<String> = description
            .columns()
            .iter()
            .map(|column| column.name().to_string())
            .collect();
        dataprof_core::validate_unique_column_names(
            &column_names,
            concat!($db_name, " query result"),
        )?;

        // Built from the driver's column list, so a zero-row query still returns
        // named empty columns, and values are filed by position in query order.
        let mut columns = $crate::QueryColumns::with_names(column_names.clone(), 0);
        columns.set_type_names(
            description
                .columns()
                .iter()
                .map(|column| ::sqlx::TypeInfo::name(column.type_info()).to_string())
                .collect(),
        );
        let mut rows = sqlx::query(::sqlx::AssertSqlSafe(select_query.as_str())).fetch($pool);
        let mut rows_read = 0usize;
        let mut log_progress = |read: usize| {
            rows_read += read;
            log::info!("{} streaming progress: {} rows read", $db_name, rows_read);
        };
        let mut unlogged_rows = 0usize;

        while let Some(row) = rows.try_next().await.map_err(|e| {
            $crate::DataProfilerError::DatabaseQueryError {
                message: format!("Query execution failed: {}", e),
            }
        })? {
            for i in 0..column_names.len() {
                let (value, container) =
                    $crate::db_column_to_string!(&row, i, [$($backend_ty),*])?;
                // decode-audit: no-data — None is SQL NULL or a binary
                // value (#645), the only ones db_column_to_string returns
                // as None; "" is the profiler's textual null.
                columns.push_decoded(i, value.unwrap_or_default(), container);
            }

            unlogged_rows += 1;
            if unlogged_rows == $batch_size {
                log_progress(unlogged_rows);
                unlogged_rows = 0;
            }
        }
        // The rows after the last full interval are progress too, so the last
        // line accounts for every row read.
        if unlogged_rows > 0 {
            log_progress(unlogged_rows);
        }

        Ok(columns)
    }};
}

/// Macro to process rows into column-oriented results, in query column order.
#[macro_export]
macro_rules! process_rows_to_columns {
    ($rows:expr) => {
        $crate::process_rows_to_columns!($rows, [])
    };
    ($rows:expr, [$($backend_ty:ty),* $(,)?]) => {{
        use sqlx::{Column, Row};

        if $rows.is_empty() {
            Ok($crate::QueryColumns::new())
        } else {
            let columns = $rows[0].columns();
            let column_names: Vec<String> = columns
                .iter()
                .map(|column| column.name().to_string())
                .collect();
            match dataprof_core::validate_unique_column_names(
                &column_names,
                "database query result",
            ) {
                Err(error) => Err(error),
                Ok(()) => {
                    // Built from the driver's column list, so the result carries
                    // the query's column order and values are filed by position.
                    let mut result = $crate::QueryColumns::with_names(column_names, $rows.len());
                    result.set_type_names(
                        columns
                            .iter()
                            .map(|column| ::sqlx::TypeInfo::name(column.type_info()).to_string())
                            .collect(),
                    );

                    for row in &$rows {
                        for i in 0..columns.len() {
                            let (value, container) =
                                $crate::db_column_to_string!(row, i, [$($backend_ty),*])?;
                            // decode-audit: no-data — None is SQL NULL or a binary
                            // value (#645), the only ones db_column_to_string
                            // returns as None; "" is the profiler's textual null.
                            result.push_decoded(i, value.unwrap_or_default(), container);
                        }
                    }

                    Ok(result)
                }
            }
        }
    }};
}

/// Build the statement a query or table name is profiled with.
#[allow(dead_code)]
pub fn build_select_query(query: &str) -> Result<String, DataProfilerError> {
    if crate::security::is_query(query) {
        validate_base_query(query)
    } else {
        validate_sql_identifier(query)?;
        Ok(format!("SELECT * FROM {}", query))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A decimal in plain digits, where `Display` writes an exponent (#1342).
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    #[test]
    fn a_big_decimal_is_written_in_plain_digits() {
        use std::str::FromStr;
        for (value, plain) in [
            ("-9E+22", "-90000000000000000000000"),
            ("1E-7", "0.0000001"),
            ("90000000000000000000001", "90000000000000000000001"),
            ("1.5000", "1.5000"),
        ] {
            let decimal = ::sqlx::types::BigDecimal::from_str(value).unwrap();
            assert_eq!(BackendText::backend_text(&decimal), plain, "{value}");
        }
    }

    /// A PostgreSQL `NUMERIC` reads at its display scale, as `psql` prints
    /// it: sqlx alone pads to groups of four decimals or drops trailing
    /// zeros. `NaN` stays a decode error. Needs `POSTGRES_TEST_URL`.
    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn a_postgresql_numeric_reads_at_its_display_scale() {
        use sqlx::Row;

        let Ok(url) = std::env::var("POSTGRES_TEST_URL") else {
            eprintln!("POSTGRES_TEST_URL not set; skipping");
            return;
        };
        let mut connection = <::sqlx::PgConnection as ::sqlx::Connection>::connect(&url)
            .await
            .expect("postgres connection");
        for (literal, text) in [
            ("0.01::numeric(9,2)", "0.01"),
            ("4.00::numeric(9,2)", "4.00"),
            ("1000000.01::numeric(9,2)", "1000000.01"),
            ("0.00000001::numeric", "0.00000001"),
            ("'-0.0'::numeric(3,1)", "0.0"),
            (
                "'-90000000000000000000000'::numeric",
                "-90000000000000000000000",
            ),
            (
                "'1e40'::numeric",
                "10000000000000000000000000000000000000000",
            ),
            (
                "'-9000000000000000000000000000000000000.0'::numeric(38,1)",
                "-9000000000000000000000000000000000000.0",
            ),
            ("0::numeric", "0"),
            ("12.3400::numeric", "12.3400"),
        ] {
            let row = sqlx::query(sqlx::AssertSqlSafe(format!("SELECT {literal}")))
                .fetch_one(&mut connection)
                .await
                .unwrap();
            let got: PgNumericText = row.try_get(0).unwrap();
            assert_eq!(got.backend_text(), text, "{literal}");
        }
        let row = sqlx::query("SELECT 'NaN'::numeric")
            .fetch_one(&mut connection)
            .await
            .unwrap();
        assert!(row.try_get::<PgNumericText, _>(0).is_err());
    }

    #[test]
    fn test_build_select_query() {
        assert_eq!(build_select_query("users").unwrap(), "SELECT * FROM users");
        assert_eq!(
            build_select_query("SELECT id FROM users").unwrap(),
            "SELECT id FROM users"
        );
    }
}
