//! Common utilities and shared logic for database connectors
//!
//! This module provides reusable functions to reduce code duplication across
//! PostgreSQL, MySQL, and SQLite connectors.

use crate::database::{validate_base_query, validate_sql_identifier};
use anyhow::Result;

// ============================================================================
// Error Helpers
// ============================================================================

/// Generate "not connected to database" error
pub fn not_connected_error() -> anyhow::Error {
    anyhow::anyhow!("Not connected to database")
}

/// Generate feature-not-enabled error for a specific database
#[allow(dead_code)]
pub fn feature_not_enabled_error(db_name: &str, feature: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "{} support not compiled. Enable '{}' feature.",
        db_name,
        feature
    )
}

// ============================================================================
// Streaming Batch Loop Macro
// ============================================================================

/// Macro to generate the streaming batch loop for profiling queries.
/// Handles the common pattern while allowing database-specific pool types.
/// Includes inline row processing to avoid complex generic trait bounds.
#[macro_export]
macro_rules! streaming_profile_loop {
    ($pool:expr, $query:expr, $batch_size:expr, $total_rows:expr, $db_name:literal) => {{
        use sqlx::{Column, Row};
        use $crate::database::connectors::common::build_batch_query;
        use $crate::database::streaming::{StreamingProgress, merge_column_batches};

        let mut progress = StreamingProgress::new(Some($total_rows as u64));
        let mut all_batches: Vec<std::collections::HashMap<String, Vec<String>>> = Vec::new();
        let mut offset = 0usize;

        loop {
            let batch_query = build_batch_query($query, $batch_size, offset)?;
            let rows = sqlx::query(&batch_query)
                .fetch_all($pool)
                .await
                .map_err(|e| anyhow::anyhow!("Batch query execution failed: {}", e))?;

            if rows.is_empty() {
                break;
            }

            // Process batch - inline to use concrete row types
            let columns = rows[0].columns();
            let mut batch_result: std::collections::HashMap<String, Vec<String>> =
                std::collections::HashMap::with_capacity(columns.len());

            for col in columns {
                batch_result.insert(col.name().to_string(), Vec::with_capacity(rows.len()));
            }

            for row in &rows {
                for (i, col) in columns.iter().enumerate() {
                    let value: Option<String> = row.try_get(i).ok();
                    if let Some(column_data) = batch_result.get_mut(col.name()) {
                        column_data.push(value.unwrap_or_default());
                    }
                }
            }

            let batch_size_actual = rows.len();
            all_batches.push(batch_result);
            progress.update(batch_size_actual as u64);

            if let Some(percentage) = progress.percentage() {
                println!(
                    "{} streaming progress: {:.1}% ({}/{} rows)",
                    $db_name, percentage, progress.processed_rows, $total_rows
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
/// Used for single-query (non-streaming) profiling.
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
                    let value: Option<String> = row.try_get(i).ok();
                    if let Some(column_data) = result.get_mut(col.name()) {
                        column_data.push(value.unwrap_or_default());
                    }
                }
            }

            result
        }
    }};
}

// ============================================================================
// Query Building Helpers
// ============================================================================

/// Build a count query for a given table or query
pub fn build_count_query(query: &str) -> Result<String> {
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
pub fn build_batch_query(query: &str, batch_size: usize, offset: usize) -> Result<String> {
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
