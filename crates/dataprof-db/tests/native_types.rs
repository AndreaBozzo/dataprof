//! Database columns record the type the server declared (#906).
//!
//! PostgreSQL and MySQL report each result column's type with the result, and
//! the profile records it as `{"system": "postgresql" | "mysql", "name": ...}`.
//! A JSON column of objects is reported as nested and keeps its declaration.
//! SQLite records none: sqlx reads its declared type into a handful of classes
//! and, for an expression or a declaration it does not know (`DECIMAL(12,2)`),
//! reports the first value's storage class instead.
//!
//! PostgreSQL and MySQL need `POSTGRES_TEST_URL` / `MYSQL_TEST_URL`; CI sets
//! both. `python/tests/test_column_native_type.py` is the Python twin.

#![cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]

use dataprof_core::{AnalysisOptions, DataType, NativeType};
use dataprof_db::{DatabaseConfig, analyze_database_with_options};

/// A profiled column: its name, the declaration recorded, the profiled type.
type Declared = (String, Option<NativeType>, DataType);

fn config(url: &str) -> DatabaseConfig {
    DatabaseConfig {
        connection_string: url.to_string(),
        ..DatabaseConfig::default()
    }
}

/// Profile `query` with every column, then with `selected` only.
///
/// The selection drops a leading column, so a type list that the selection
/// did not filter in step with the columns files every type one column off.
async fn declared(url: &str, query: &str, selected: &[&str]) -> [Vec<Declared>; 2] {
    let all = AnalysisOptions::default();
    let some = AnalysisOptions::default()
        .with_columns(Some(selected.iter().map(|name| name.to_string()).collect()));
    let mut runs = Vec::new();
    for options in [all, some] {
        let report = analyze_database_with_options(config(url), query, &options)
            .await
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        runs.push(
            report
                .column_profiles
                .iter()
                .map(|column| {
                    (
                        column.name.clone(),
                        column.native_type.clone(),
                        column.data_type.clone(),
                    )
                })
                .collect(),
        );
    }
    runs.try_into().unwrap()
}

/// Every (run, column) whose declaration is not `expected`'s, so one failing
/// run names each wrong column. A column `expected` does not list must have
/// none.
fn mismatches(runs: &[Vec<Declared>; 2], system: &str, expected: &[(&str, &str)]) -> Vec<String> {
    let mut wrong = Vec::new();
    for (run, columns) in ["all columns", "selected columns"].iter().zip(runs) {
        for (name, declared, _) in columns {
            let want = expected
                .iter()
                .find(|(column, _)| column == name)
                .map(|(_, type_name)| NativeType::new(system, *type_name));
            if declared != &want {
                wrong.push(format!("{run}, {name}: {declared:?} != {want:?}"));
            }
        }
    }
    wrong
}

/// The `doc` column holds JSON objects only, so it is reported as nested; its
/// declaration has to survive the rebuild of its profile, which `mismatches`
/// checks. This checks the rebuild happened, or that check proves nothing.
#[cfg(any(feature = "postgres", feature = "mysql"))]
fn doc_not_nested(runs: &[Vec<Declared>; 2]) -> Vec<String> {
    runs.iter()
        .flatten()
        .filter(|(name, _, data_type)| name == "doc" && *data_type != DataType::Nested)
        .map(|(_, _, data_type)| format!("doc profiled as {data_type:?}, not nested"))
        .collect()
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_records_no_declared_type() {
    use sqlx::sqlite::SqlitePoolOptions;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.db");
    std::fs::File::create(&path).unwrap();
    let url = format!("sqlite://{}", path.display());
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    for statement in [
        "CREATE TABLE t (id INTEGER, amount DECIMAL(12,2), note TEXT)",
        "INSERT INTO t VALUES (1, 12.34, 'a'), (2, 5.00, NULL)",
    ] {
        sqlx::query(statement).execute(&pool).await.unwrap();
    }
    pool.close().await;

    let runs = declared(&url, "SELECT * FROM t", &["amount", "note"]).await;
    let wrong = mismatches(&runs, "sqlite", &[]);
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgresql_records_the_server_type() {
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
        "DROP TABLE IF EXISTS native_types_pg",
        "CREATE TABLE native_types_pg (
            id INT4, amount NUMERIC(12,2), at TIMESTAMPTZ, doc JSONB, note TEXT
        )",
        r#"INSERT INTO native_types_pg VALUES
            (1, 12.34, '2026-01-01T00:00:00Z', '{"a": 1}', 'x'),
            (2, NULL, NULL, '{"a": 2}', NULL)"#,
    ] {
        sqlx::query(statement).execute(&pool).await.unwrap();
    }
    pool.close().await;

    let runs = declared(&url, "SELECT * FROM native_types_pg", &["amount", "doc"]).await;
    let mut wrong = mismatches(
        &runs,
        NativeType::POSTGRESQL,
        &[
            ("id", "INT4"),
            ("amount", "NUMERIC"),
            ("at", "TIMESTAMPTZ"),
            ("doc", "JSONB"),
            ("note", "TEXT"),
        ],
    );
    wrong.extend(doc_not_nested(&runs));
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn mysql_records_the_server_type() {
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
        "DROP TABLE IF EXISTS native_types_my",
        "CREATE TABLE native_types_my (
            id BIGINT UNSIGNED, amount DECIMAL(12,2), at DATETIME, doc JSON, note VARCHAR(20)
        )",
        r#"INSERT INTO native_types_my VALUES
            (1, 12.34, '2026-01-01 00:00:00', '{"a": 1}', 'x'),
            (2, NULL, NULL, '{"a": 2}', NULL)"#,
    ] {
        sqlx::query(statement).execute(&pool).await.unwrap();
    }
    pool.close().await;

    let runs = declared(&url, "SELECT * FROM native_types_my", &["amount", "doc"]).await;
    let mut wrong = mismatches(
        &runs,
        NativeType::MYSQL,
        &[
            ("id", "BIGINT UNSIGNED"),
            ("amount", "DECIMAL"),
            ("at", "DATETIME"),
            ("doc", "JSON"),
            ("note", "VARCHAR"),
        ],
    );
    wrong.extend(doc_not_nested(&runs));
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
