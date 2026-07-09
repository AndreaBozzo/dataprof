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
///
/// A value whose type matches none of these arms (NUMERIC/DECIMAL, dates and
/// times without the corresponding sqlx feature, BLOB) yields `None` and is
/// recorded as a null, which is the pre-existing behaviour for those types.
#[macro_export]
macro_rules! db_column_to_string {
    ($row:expr, $index:expr) => {{
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
        } else if let Ok(v) = row.try_get::<Option<f64>, _>(index) {
            // `{:?}` keeps the decimal point on integral floats ("100.0", not
            // "100"), so a REAL column of whole numbers is still inferred as a
            // float downstream. It also stays compact at the extremes ("1e300").
            v.map(|x| format!("{:?}", x))
        } else if let Ok(v) = row.try_get::<Option<f32>, _>(index) {
            v.map(|x| format!("{:?}", x))
        } else if let Ok(v) = row.try_get::<Option<bool>, _>(index) {
            v.map(|x| x.to_string())
        } else {
            None
        }
    }};
}

/// Macro to generate the streaming batch loop for profiling queries.
#[macro_export]
macro_rules! streaming_profile_loop {
    ($pool:expr, $query:expr, $batch_size:expr, $total_rows:expr, $db_name:literal) => {{
        use sqlx::{Column, Row};
        use $crate::connectors::common::build_batch_query;
        use $crate::streaming::{StreamingProgress, merge_column_batches};

        let mut progress = StreamingProgress::new(Some($total_rows as u64));
        let mut all_batches: Vec<std::collections::HashMap<String, Vec<String>>> = Vec::new();
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
            let mut batch_result: std::collections::HashMap<String, Vec<String>> =
                std::collections::HashMap::with_capacity(columns.len());

            for col in columns {
                batch_result.insert(col.name().to_string(), Vec::with_capacity(rows.len()));
            }

            for row in &rows {
                for (i, col) in columns.iter().enumerate() {
                    let value: Option<String> = $crate::db_column_to_string!(row, i);
                    if let Some(column_data) = batch_result.get_mut(col.name()) {
                        column_data.push(value.unwrap_or_default());
                    }
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

        merge_column_batches(all_batches)
    }};
}

/// Macro to process rows into column-oriented HashMap.
#[macro_export]
macro_rules! process_rows_to_columns {
    ($rows:expr) => {{
        use sqlx::{Column, Row};

        if $rows.is_empty() {
            std::collections::HashMap::new()
        } else {
            let columns = $rows[0].columns();
            let mut result: std::collections::HashMap<String, Vec<String>> =
                std::collections::HashMap::with_capacity(columns.len());

            for col in columns {
                result.insert(col.name().to_string(), Vec::with_capacity($rows.len()));
            }

            for row in &$rows {
                for (i, col) in columns.iter().enumerate() {
                    let value: Option<String> = $crate::db_column_to_string!(row, i);
                    if let Some(column_data) = result.get_mut(col.name()) {
                        column_data.push(value.unwrap_or_default());
                    }
                }
            }

            result
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
