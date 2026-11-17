//! A database decimal is read from its plain digits (#1342).
//!
//! sqlx decodes a PostgreSQL `NUMERIC` to a `BigDecimal`, whose `Display`
//! writes an exponent past 15 trailing zeros: `-90000000000000000000000`
//! printed as `-900e+20`. A declared decimal is held exactly from 11 digits as
//! written, so that value read as a float beside its exactly held neighbour,
//! and the mean of the pair kept its rounding: 2097152.5 where the same values
//! as an Arrow `decimal128(23, 0)` give 0.5. The connectors now write every
//! `BigDecimal` in plain digits.
//!
//! PostgreSQL and MySQL need `POSTGRES_TEST_URL` / `MYSQL_TEST_URL`; CI sets
//! both. MySQL decodes `DECIMAL` from its text and already printed the digits;
//! it is checked for parity.

#![cfg(any(feature = "postgres", feature = "mysql"))]

use dataprof_core::ColumnStats;
use dataprof_db::{DatabaseConfig, analyze_database};
use serde_json::Value;

/// Two values whose exact mean and median are 0.5; the first has 22 trailing
/// zeros.
const ROWS: &str = "(-90000000000000000000000), (90000000000000000000001)";

/// Each wrong field of `query`'s first column, against the Arrow route's
/// mean and median of 0.5.
async fn wrong_readings(url: &str, query: &str) -> Vec<String> {
    let config = DatabaseConfig {
        connection_string: url.to_string(),
        ..DatabaseConfig::default()
    };
    let report = analyze_database(config, query, true, None)
        .await
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    let mut wrong = Vec::new();
    let column = &report.column_profiles[0];
    if !matches!(&column.stats, ColumnStats::Numeric(stats) if stats.exact.is_some()) {
        wrong.push(format!("stats {:?}", column.stats));
    }
    let document: Value = serde_json::from_str(&report.to_json().unwrap()).unwrap();
    let stats = &document["column_profiles"][0]["stats"]["Numeric"];
    for field in ["mean", "median"] {
        if stats[field].as_f64() != Some(0.5) {
            wrong.push(format!("{field} {}", stats[field]));
        }
    }
    if stats["exact"]["min"] != "-90000000000000000000000" {
        wrong.push(format!("exact.min {}", stats["exact"]["min"]));
    }
    wrong
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn a_postgresql_numeric_with_trailing_zeros_is_read_from_its_digits() {
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
        "DROP TABLE IF EXISTS numeric_plain_digits_pg".to_string(),
        "CREATE TABLE numeric_plain_digits_pg (x NUMERIC(23,0))".to_string(),
        format!("INSERT INTO numeric_plain_digits_pg VALUES {ROWS}"),
    ] {
        sqlx::query(&statement).execute(&pool).await.unwrap();
    }
    pool.close().await;

    let wrong = wrong_readings(&url, "SELECT x FROM numeric_plain_digits_pg").await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn a_mysql_decimal_with_trailing_zeros_is_read_from_its_digits() {
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
        "DROP TABLE IF EXISTS numeric_plain_digits_my".to_string(),
        "CREATE TABLE numeric_plain_digits_my (x DECIMAL(23,0))".to_string(),
        format!("INSERT INTO numeric_plain_digits_my VALUES {ROWS}"),
    ] {
        sqlx::query(&statement).execute(&pool).await.unwrap();
    }
    pool.close().await;

    let wrong = wrong_readings(&url, "SELECT x FROM numeric_plain_digits_my").await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
