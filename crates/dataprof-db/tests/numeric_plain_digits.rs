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
        sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
            .execute(&pool)
            .await
            .unwrap();
    }
    pool.close().await;

    let wrong = wrong_readings(&url, "SELECT x FROM numeric_plain_digits_pg").await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// `NUMERIC(9,2)` values whose quartiles the Arrow `decimal128(9, 2)` route
/// serializes as below. sqlx pads a PostgreSQL `NUMERIC` to groups of four
/// decimals (`1000000.0100`), which turned the widest value exact beside
/// three read as floats, and q1 serialized 0.14.
const SHORT_ROWS: &str = "(0.01), (0.19), (0.30), (1000000.01)";

/// Each field of `query`'s first column that differs from the Arrow route's.
async fn wrong_short_readings(url: &str, query: &str) -> Vec<String> {
    let config = DatabaseConfig {
        connection_string: url.to_string(),
        ..DatabaseConfig::default()
    };
    let report = analyze_database(config, query, true, None)
        .await
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    let document: Value = serde_json::from_str(&report.to_json().unwrap()).unwrap();
    let stats = &document["column_profiles"][0]["stats"]["Numeric"];
    let expected = [
        ("/mean", 250000.1275),
        ("/median", 0.245),
        ("/quartiles/q1", 0.15),
        ("/quartiles/q2", 0.25),
        ("/quartiles/q3", 250000.23),
        ("/quartiles/iqr", 250000.08),
    ];
    expected
        .iter()
        .filter(|(field, value)| stats.pointer(field).and_then(Value::as_f64) != Some(*value))
        .map(|(field, value)| {
            format!(
                "{field} {} for {value}",
                stats.pointer(field).unwrap_or(&Value::Null)
            )
        })
        .collect()
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn a_postgresql_numeric_is_read_at_its_display_scale() {
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
        "DROP TABLE IF EXISTS numeric_display_scale_pg".to_string(),
        "CREATE TABLE numeric_display_scale_pg (x NUMERIC(9,2))".to_string(),
        format!("INSERT INTO numeric_display_scale_pg VALUES {SHORT_ROWS}"),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
            .execute(&pool)
            .await
            .unwrap();
    }
    pool.close().await;

    let wrong = wrong_short_readings(&url, "SELECT x FROM numeric_display_scale_pg").await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn a_mysql_decimal_is_read_at_its_declared_scale() {
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
        "DROP TABLE IF EXISTS numeric_display_scale_my".to_string(),
        "CREATE TABLE numeric_display_scale_my (x DECIMAL(9,2))".to_string(),
        format!("INSERT INTO numeric_display_scale_my VALUES {SHORT_ROWS}"),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
            .execute(&pool)
            .await
            .unwrap();
    }
    pool.close().await;

    let wrong = wrong_short_readings(&url, "SELECT x FROM numeric_display_scale_my").await;
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
        sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
            .execute(&pool)
            .await
            .unwrap();
    }
    pool.close().await;

    let wrong = wrong_readings(&url, "SELECT x FROM numeric_plain_digits_my").await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
