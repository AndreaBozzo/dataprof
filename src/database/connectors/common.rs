//! Common utilities and shared logic for database connectors
//!
//! This module provides reusable functions to reduce code duplication across
//! PostgreSQL, MySQL, and SQLite connectors.

use crate::database::{validate_base_query, validate_sql_identifier};
use anyhow::Result;

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
