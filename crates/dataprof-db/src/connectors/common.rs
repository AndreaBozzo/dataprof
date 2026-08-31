//! Common utilities and shared logic for database connectors
//!
//! This module provides reusable functions to reduce code duplication across
//! PostgreSQL, MySQL, and SQLite connectors.

use crate::DataProfilerError;
use crate::security::{validate_base_query, validate_sql_identifier};

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
/// Temporal values render in the naive ISO form dataprof's date grammar
/// accepts (`YYYY-MM-DD` and `YYYY-MM-DDTHH:MM:SS`). That grammar rejects a
/// trailing offset or `Z`, so rendering a timestamp as true RFC 3339 would
/// profile the column as *text* and leave the Timeliness dimension exactly as
/// inert as the nulls did. `TIMESTAMPTZ` is converted to UTC and its offset
/// dropped; the instant survives, the "this is UTC" marker does not.
///
/// Still unsupported and still recorded as null: `BLOB`/`BYTEA`, which needs a
/// binary column type rather than a decode arm, and MySQL `TIME`, which sqlx
/// decodes as a duration rather than a time of day.
#[macro_export]
macro_rules! db_column_to_string {
    ($row:expr, $index:expr) => {
        $crate::db_column_to_string!($row, $index, [], [])
    };
    ($row:expr, $index:expr, [$($backend_ty:ty),* $(,)?]) => {
        $crate::db_column_to_string!($row, $index, [$($backend_ty),*], [])
    };
    ($row:expr, $index:expr, [$($backend_ty:ty),* $(,)?], [$( ($custom_ty:ty, $mapper:expr) ),* $(,)?]) => {{
        let row = $row;
        let index = $index;

        if let Ok(v) = row.try_get::<Option<String>, _>(index) {
            v
        } else if let Ok(v) = row.try_get::<Option<i64>, _>(index) {
            v.map(|x| x.to_string())
        } else if let Ok(v) = row.try_get::<Option<i32>, _>(index) {
            v.map(|x| x.to_string())
        } else if let Ok(v) = row.try_get::<Option<i16>, _>(index) {
            v.map(|x| x.to_string())
        }
        // Arms the shared chain cannot express because the type has no impl
        // for one of the backends. `to_string` is exact for both of today's
        // callers: `u64` prints its digits, and `BigDecimal` keeps the scale
        // the database stored rather than rounding through f64.
        $(else if let Ok(v) = row.try_get::<Option<$backend_ty>, _>(index) {
            v.map(|x| x.to_string())
        })*
        $(else if let Ok(v) = row.try_get::<Option<$custom_ty>, _>(index) {
            v.map($mapper)
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
            v.map(|x| $crate::render_naive_datetime(x.naive_utc()))
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
        } else {
            None
        }
    }};
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

/// Macro to generate the streaming batch loop for profiling queries.
#[macro_export]
macro_rules! streaming_profile_loop {
    ($pool:expr, $query:expr, $batch_size:expr, $total_rows:expr, $db_name:literal) => {
        $crate::streaming_profile_loop!($pool, $query, $batch_size, $total_rows, $db_name, [], [])
    };
    ($pool:expr, $query:expr, $batch_size:expr, $total_rows:expr, $db_name:literal,
     [$($backend_ty:ty),* $(,)?]) => {
        $crate::streaming_profile_loop!($pool, $query, $batch_size, $total_rows, $db_name, [$($backend_ty),*], [])
    };
    ($pool:expr, $query:expr, $batch_size:expr, $total_rows:expr, $db_name:literal,
     [$($backend_ty:ty),* $(,)?], [$( ($custom_ty:ty, $mapper:expr) ),* $(,)?]) => {{
        use sqlx::{Column, Row};
        use $crate::connectors::common::build_batch_query;
        use $crate::streaming::{StreamingProgress, merge_column_batches};

        let mut progress = StreamingProgress::new(Some($total_rows as u64));
        let mut all_batches: Vec<$crate::QueryColumns> = Vec::new();
        let mut offset = 0usize;

        loop {
            let batch_query = build_batch_query($query, $batch_size, offset)?;
            let rows = sqlx::query(&batch_query)
                .fetch_all($pool)
                .await
                .map_err(|e| $crate::DataProfilerError::DatabaseQueryError {
                    message: format!("Batch query execution failed: {}", e),
                })?;

            if rows.is_empty() {
                break;
            }

            let columns = rows[0].columns();
            let column_names: Vec<String> = columns
                .iter()
                .map(|column| column.name().to_string())
                .collect();
            dataprof_core::validate_unique_column_names(
                &column_names,
                concat!($db_name, " query result"),
            )?;
            // Built from the driver's column list, so the batch carries the
            // query's column order and values are filed by position.
            let mut batch_result =
                $crate::QueryColumns::with_names(column_names.clone(), rows.len());

            for row in &rows {
                for i in 0..columns.len() {
                    let value: Option<String> =
                        $crate::db_column_to_string!(row, i, [$($backend_ty),*], [$( ($custom_ty, $mapper) ),*]);
                    // decode-audit: no-data — None is SQL NULL (or a type
                    // db_column_to_string documents as unsupported); "" is
                    // the profiler's textual null.
                    batch_result.push_value(i, value.unwrap_or_default());
                }
            }

            let batch_size_actual = rows.len();
            all_batches.push(batch_result);
            progress.update(batch_size_actual as u64);

            if let Some(percentage) = progress.percentage() {
                log::info!(
                    "{} streaming progress: {:.1}% ({}/{} rows)",
                    $db_name,
                    percentage,
                    progress.processed_rows,
                    $total_rows
                );
            }

            offset += $batch_size;
            if batch_size_actual < $batch_size {
                break;
            }
        }

        Ok(merge_column_batches(all_batches))
    }};
}

/// Macro to process rows into column-oriented results, in query column order.
#[macro_export]
macro_rules! process_rows_to_columns {
    ($rows:expr) => {
        $crate::process_rows_to_columns!($rows, [], [])
    };
    ($rows:expr, [$($backend_ty:ty),* $(,)?]) => {
        $crate::process_rows_to_columns!($rows, [$($backend_ty),*], [])
    };
    ($rows:expr, [$($backend_ty:ty),* $(,)?], [$( ($custom_ty:ty, $mapper:expr) ),* $(,)?]) => {{
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

                    for row in &$rows {
                        for i in 0..columns.len() {
                            let value: Option<String> =
                                $crate::db_column_to_string!(row, i, [$($backend_ty),*], [$( ($custom_ty, $mapper) ),*]);
                            // decode-audit: no-data — None is SQL NULL (or a type
                            // db_column_to_string documents as unsupported); "" is
                            // the profiler's textual null.
                            result.push_value(i, value.unwrap_or_default());
                        }
                    }

                    Ok(result)
                }
            }
        }
    }};
}

/// Build a count query for a given table or query
#[allow(dead_code)]
pub fn build_count_query(query: &str) -> Result<String, DataProfilerError> {
    if query.trim().to_uppercase().starts_with("SELECT") {
        let validated_query = validate_base_query(query)?;
        Ok(format!(
            "SELECT COUNT(*) FROM ({}) as count_subquery",
            validated_query
        ))
    } else {
        validate_sql_identifier(query)?;
        Ok(format!("SELECT COUNT(*) FROM {}", query))
    }
}

/// Build a batch query with LIMIT and OFFSET
#[allow(dead_code)]
pub fn build_batch_query(
    query: &str,
    batch_size: usize,
    offset: usize,
) -> Result<String, DataProfilerError> {
    let validated_query = if query.trim().to_uppercase().starts_with("SELECT") {
        validate_base_query(query)?
    } else {
        validate_sql_identifier(query)?;
        format!("SELECT * FROM {}", query)
    };
    Ok(format!(
        "{} LIMIT {} OFFSET {}",
        validated_query, batch_size, offset
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_count_query_table() {
        let result = build_count_query("users").unwrap();
        assert_eq!(result, "SELECT COUNT(*) FROM users");
    }

    #[test]
    fn test_build_count_query_select() {
        let result = build_count_query("SELECT * FROM users WHERE active = true").unwrap();
        assert!(result.contains("SELECT COUNT(*) FROM"));
        assert!(result.contains("count_subquery"));
    }

    #[test]
    fn test_build_batch_query() {
        let result = build_batch_query("users", 100, 0).unwrap();
        assert_eq!(result, "SELECT * FROM users LIMIT 100 OFFSET 0");
    }
}
