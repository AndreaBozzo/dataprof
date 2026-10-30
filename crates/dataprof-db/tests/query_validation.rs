//! Queries naming ordinary columns are profiled, not refused (#1167).
//!
//! The query check matched keywords as substrings, so `created_at` was refused
//! for CREATE, `updated_at` for UPDATE, `is_deleted` for DELETE and
//! `system_id` for SYS, and a read-only `UNION ALL` for UNION. They need the
//! CI database services (`POSTGRES_TEST_URL`, `MYSQL_TEST_URL`) and pass
//! without running when those are unset.

#![cfg(any(feature = "postgres", feature = "mysql"))]

use dataprof_db::DatabaseConfig;

/// Each query, and the rows it returns.
const QUERIES: [(&str, usize); 5] = [
    ("SELECT 1 AS id, 'a' AS created_at", 1),
    ("SELECT 1 AS id, 'a' AS updated_at", 1),
    ("SELECT 1 AS id, 0 AS is_deleted", 1),
    ("SELECT 1 AS system_id", 1),
    ("SELECT 1 AS id UNION ALL SELECT 2", 2),
];

/// Collects every wrong query before failing.
async fn every_query_is_profiled(url: &str) {
    let mut wrong = Vec::new();
    for (query, rows) in QUERIES {
        let config = DatabaseConfig {
            connection_string: url.to_string(),
            load_credentials_from_env: false,
            ..Default::default()
        };
        match dataprof_db::analyze_database(config, query, true, None).await {
            Ok(report) if report.execution.rows_processed == rows => {}
            Ok(report) => wrong.push(format!(
                "{query}: {} rows, expected {rows}",
                report.execution.rows_processed
            )),
            Err(error) => wrong.push(format!("{query}: {error}")),
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_profiles_queries_naming_ordinary_columns() {
    if let Ok(url) = std::env::var("POSTGRES_TEST_URL") {
        every_query_is_profiled(&url).await;
    }
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn mysql_profiles_queries_naming_ordinary_columns() {
    if let Ok(url) = std::env::var("MYSQL_TEST_URL") {
        every_query_is_profiled(&url).await;
    }
}
