//! A database decimal is read as a decimal, as an Arrow decimal is (#1268).
//!
//! PostgreSQL `NUMERIC` and MySQL `DECIMAL` values arrive as text. They were
//! typed `float`, got no exact statistics, and kept `f64` rounding past 11
//! significant digits, where the same values as an Arrow decimal are exact.
//! The column's declared type now selects the decimal reading, for its
//! statistics and for the accuracy pass, with or without statistics.
//!
//! `CENTS` is `9999999999996 + cents / 100`; its lowest value lies 0.00375
//! below the lower Tukey fence, past what the `f64` fences resolve, so only
//! the decimal reading counts it: an `outlier_ratio` of 12.5 against 0. The
//! base keeps every value at 13 integer digits: sqlx renders a PostgreSQL
//! `NUMERIC(16, 2)` padded to four decimals (`9999999999996.8000`), and 18
//! written digits are held exactly even as plain text, which would pass
//! without the declared reading.
//!
//! PostgreSQL and MySQL need `POSTGRES_TEST_URL` / `MYSQL_TEST_URL`; CI sets
//! both. `tests/declared_decimals.rs` covers the Arrow decimals, and
//! `python/tests/test_declared_decimals.py` is the Python twin.

#![cfg(any(feature = "postgres", feature = "mysql"))]

use dataprof_core::{AnalysisOptions, ColumnStats, MetricPack};
use dataprof_db::{DatabaseConfig, analyze_database_with_options};
use serde_json::Value;

const CENTS: [&str; 8] = [
    "9999999999996.80",
    "9999999999997.73",
    "9999999999997.95",
    "9999999999998.09",
    "9999999999998.22",
    "9999999999998.55",
    "9999999999998.84",
    "9999999999999.31",
];

fn rows() -> String {
    CENTS
        .iter()
        .map(|value| format!("({value})"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Each wrong (run, field) of `query`, profiled with every pack and with the
/// quality pack only.
async fn wrong_readings(url: &str, query: &str) -> Vec<String> {
    let config = DatabaseConfig {
        connection_string: url.to_string(),
        ..DatabaseConfig::default()
    };
    let mut wrong = Vec::new();
    for (label, options) in [
        ("every pack", AnalysisOptions::default()),
        (
            "quality only",
            AnalysisOptions::default().with_metric_packs(Some(vec![MetricPack::Quality])),
        ),
    ] {
        let report = analyze_database_with_options(config.clone(), query, &options)
            .await
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        let column = &report.column_profiles[0];
        let declared = column
            .native_type
            .as_ref()
            .is_some_and(|native| native.declares_decimal());
        if !declared {
            wrong.push(format!("[{label}] declared {:?}", column.native_type));
        }
        if label == "every pack" {
            match &column.stats {
                ColumnStats::Numeric(stats)
                    if stats.exact.is_some() && stats.outlier_count == Some(1) => {}
                other => wrong.push(format!("[{label}] stats {other:?}")),
            }
        }
        let document: Value = serde_json::from_str(&report.to_json().unwrap()).unwrap();
        let ratio = &document["quality"]["metrics"]["accuracy"]["outlier_ratio"];
        if ratio.as_f64() != Some(12.5) {
            wrong.push(format!("[{label}] outlier_ratio {ratio}"));
        }
    }
    wrong
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn a_postgresql_numeric_reads_as_a_decimal() {
    use sqlx::postgres::PgPoolOptions;

    let Ok(url) = std::env::var("POSTGRES_TEST_URL") else {
        eprintln!("POSTGRES_TEST_URL not set; skipping");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("postgres connection");
    for statement in [
        "DROP TABLE IF EXISTS declared_decimals_pg".to_string(),
        "CREATE TABLE declared_decimals_pg (cents NUMERIC(16,2))".to_string(),
        format!("INSERT INTO declared_decimals_pg VALUES {}", rows()),
    ] {
        sqlx::query(&statement).execute(&pool).await.unwrap();
    }
    pool.close().await;

    let wrong = wrong_readings(&url, "SELECT cents FROM declared_decimals_pg").await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn a_mysql_decimal_reads_as_a_decimal() {
    use sqlx::mysql::MySqlPoolOptions;

    let Ok(url) = std::env::var("MYSQL_TEST_URL") else {
        eprintln!("MYSQL_TEST_URL not set; skipping");
        return;
    };
    let pool = MySqlPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("mysql connection");
    for statement in [
        "DROP TABLE IF EXISTS declared_decimals_my".to_string(),
        "CREATE TABLE declared_decimals_my (cents DECIMAL(16,2))".to_string(),
        format!("INSERT INTO declared_decimals_my VALUES {}", rows()),
    ] {
        sqlx::query(&statement).execute(&pool).await.unwrap();
    }
    pool.close().await;

    let wrong = wrong_readings(&url, "SELECT cents FROM declared_decimals_my").await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
