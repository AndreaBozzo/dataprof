//! SQL validation utilities to prevent injection attacks

use crate::DataProfilerError;
use std::collections::HashSet;

/// Validate SQL identifiers (table names, column names) to prevent injection
pub fn validate_sql_identifier(identifier: &str) -> Result<(), DataProfilerError> {
    if identifier.trim().is_empty() {
        return Err(DataProfilerError::sql_validation(
            "SQL identifier cannot be empty",
        ));
    }

    if identifier.len() > 128 {
        return Err(DataProfilerError::sql_validation(
            "SQL identifier too long (max 128 chars)",
        ));
    }

    let allowed_chars: HashSet<char> =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_."
            .chars()
            .collect();

    if (identifier.starts_with('"') && identifier.ends_with('"'))
        || (identifier.starts_with('`') && identifier.ends_with('`'))
        || (identifier.starts_with('[') && identifier.ends_with(']'))
    {
        let inner = &identifier[1..identifier.len() - 1];
        if inner.is_empty() {
            return Err(DataProfilerError::sql_validation(
                "Quoted identifier cannot be empty",
            ));
        }
        let quote_char = identifier
            .chars()
            .next()
            .ok_or_else(|| DataProfilerError::sql_validation("Invalid identifier format"))?;
        if inner.contains(quote_char)
            || inner.contains(';')
            || inner.contains("--")
            || inner.contains("/*")
            || inner.contains("*/")
        {
            return Err(DataProfilerError::sql_validation(
                "Invalid characters in quoted identifier",
            ));
        }
    } else {
        if !identifier.chars().all(|c| allowed_chars.contains(&c)) {
            return Err(DataProfilerError::sql_validation(&format!(
                "Invalid SQL identifier '{}': only alphanumeric, underscore, and dot allowed",
                identifier
            )));
        }

        if let Some(first_char) = identifier.chars().next()
            && !first_char.is_alphabetic()
            && first_char != '_'
        {
            return Err(DataProfilerError::sql_validation(
                "SQL identifier must start with letter or underscore",
            ));
        }
    }

    let identifier_upper = identifier.to_uppercase();
    let dangerous_keywords = [
        "DROP",
        "DELETE",
        "INSERT",
        "UPDATE",
        "TRUNCATE",
        "ALTER",
        "CREATE",
        "GRANT",
        "REVOKE",
        "EXEC",
        "EXECUTE",
        "UNION",
        "--",
        "/*",
        "*/",
        ";",
        "INFORMATION_SCHEMA",
        "SYS",
        "MASTER",
        "PG_",
        "MYSQL",
    ];

    for keyword in &dangerous_keywords {
        if identifier_upper.contains(keyword) {
            return Err(DataProfilerError::sql_validation(&format!(
                "SQL identifier contains dangerous keyword or pattern: {}",
                keyword
            )));
        }
    }

    Ok(())
}

/// Validate and sanitize a basic SQL query to ensure it's a SELECT statement
pub fn validate_base_query(query: &str) -> Result<String, DataProfilerError> {
    let trimmed = query.trim();

    if trimmed.is_empty() {
        return Err(DataProfilerError::sql_validation("Query cannot be empty"));
    }

    if trimmed.len() > 10000 {
        return Err(DataProfilerError::sql_validation(
            "Query too long (max 10000 chars)",
        ));
    }

    let query_upper = trimmed.to_uppercase();
    if !query_upper.starts_with("SELECT") {
        return Err(DataProfilerError::sql_validation(
            "Only SELECT queries are allowed for sampling",
        ));
    }

    let dangerous_patterns = [
        "DROP",
        "DELETE",
        "INSERT",
        "UPDATE",
        "TRUNCATE",
        "ALTER",
        "CREATE",
        "GRANT",
        "REVOKE",
        "EXEC",
        "EXECUTE",
        "UNION",
        "--",
        "/*",
        "INFORMATION_SCHEMA",
        "SYS",
        "MASTER",
        "PG_",
        "MYSQL",
        "WAITFOR",
        "SLEEP",
        "EXTRACTVALUE",
        "LOAD_FILE",
        "COPY",
        "ATTACH",
        "PROGRAM",
        "XP_CMDSHELL",
    ];

    for pattern in &dangerous_patterns {
        if query_upper.contains(pattern) {
            return Err(DataProfilerError::sql_validation(&format!(
                "Query contains dangerous SQL pattern: {}",
                pattern
            )));
        }
    }

    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_identifier_validation() {
        assert!(validate_sql_identifier("users").is_ok());
        assert!(validate_sql_identifier("user_table").is_ok());
        assert!(validate_sql_identifier("schema.table").is_ok());
        assert!(validate_sql_identifier("\"quoted table\"").is_ok());
        assert!(validate_sql_identifier("`quoted_table`").is_ok());

        assert!(validate_sql_identifier("").is_err());
        assert!(validate_sql_identifier("DROP TABLE").is_err());
        assert!(validate_sql_identifier("users; DROP TABLE users; --").is_err());
        assert!(validate_sql_identifier("table/* comment */").is_err());
        assert!(validate_sql_identifier("123invalid").is_err());
    }

    #[test]
    fn test_base_query_validation() {
        assert!(validate_base_query("SELECT * FROM users").is_ok());
        assert!(validate_base_query("  SELECT id, name FROM products  ").is_ok());

        assert!(validate_base_query("").is_err());
        assert!(validate_base_query("DROP TABLE users").is_err());
        assert!(validate_base_query("SELECT * FROM users; DROP TABLE users").is_err());
        assert!(validate_base_query("SELECT * FROM users UNION SELECT * FROM admin").is_err());
    }
}
