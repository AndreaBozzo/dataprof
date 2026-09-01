//! Query results keep the query's column order (#496).
//!
//! The connectors used to return `HashMap<String, Vec<String>>`, so
//! `SELECT a, b FROM t` could come back as `["b", "a"]` — and because hash
//! iteration is not stable between processes, two runs of the same query could
//! disagree. These tests pin the order at the connector boundary, where it is
//! established, rather than only at the report.

#![cfg(feature = "sqlite")]

use dataprof_core::AnalysisOptions;
use dataprof_db::{
    DatabaseConfig, DatabaseConnector, QueryColumns, analyze_database_with_options,
    create_connector,
};
use sqlx::sqlite::SqlitePoolOptions;

/// Four columns whose declaration order differs from alphabetical order in
/// every position, so a sorted or hashed result is visibly wrong rather than
/// accidentally right.
const DECLARED_ORDER: [&str; 4] = ["id", "amount", "active", "date"];

/// Create a SQLite file with a table in [`DECLARED_ORDER`] and `rows` rows.
async fn fixture(rows: usize) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("order.db");
    std::fs::File::create(&db_path).unwrap();
    let db_path = db_path.display().to_string();

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{db_path}"))
        .await
        .unwrap();
    sqlx::query("CREATE TABLE t (id INTEGER, amount REAL, active INTEGER, date TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    for i in 0..rows {
        sqlx::query("INSERT INTO t VALUES (?, ?, ?, ?)")
            .bind(i as i64)
            .bind(i as f64 + 0.5)
            .bind(i64::from(i % 2 == 0))
            .bind("2026-08-06")
            .execute(&pool)
            .await
            .unwrap();
    }
    pool.close().await;

    (dir, db_path)
}

async fn connect(db_path: &str) -> Box<dyn DatabaseConnector> {
    let mut connector = create_connector(DatabaseConfig {
        connection_string: db_path.to_string(),
        load_credentials_from_env: false,
        ..Default::default()
    })
    .unwrap();
    connector.connect().await.unwrap();
    connector
}

fn names(columns: &QueryColumns) -> Vec<&str> {
    columns.names().collect()
}

#[tokio::test]
async fn profile_query_reports_select_list_order() {
    let (_dir, db_path) = fixture(4).await;
    let mut connector = connect(&db_path).await;

    let columns = connector.profile_query("SELECT * FROM t").await.unwrap();
    assert_eq!(names(&columns), DECLARED_ORDER);

    connector.disconnect().await.unwrap();
}

#[tokio::test]
async fn an_explicit_select_list_wins_over_the_table_declaration() {
    // The query asked for this order; the table's declaration order is not the
    // answer here.
    let (_dir, db_path) = fixture(4).await;
    let mut connector = connect(&db_path).await;

    let columns = connector
        .profile_query("SELECT date, id, active, amount FROM t")
        .await
        .unwrap();
    assert_eq!(names(&columns), ["date", "id", "active", "amount"]);

    connector.disconnect().await.unwrap();
}

#[tokio::test]
async fn streaming_batches_preserve_the_schema_order() {
    // Three batches over four rows, so the merge path is exercised rather than
    // a single batch that would pass trivially.
    let (_dir, db_path) = fixture(4).await;
    let mut connector = connect(&db_path).await;

    let columns = connector
        .profile_query_streaming("SELECT date, id, active, amount FROM t", 2)
        .await
        .unwrap();

    assert_eq!(names(&columns), ["date", "id", "active", "amount"]);
    assert_eq!(columns.row_count(), 4, "every row survives the merge");
    assert_eq!(columns["id"], ["0", "1", "2", "3"]);

    connector.disconnect().await.unwrap();
}

#[tokio::test]
async fn streaming_empty_results_preserve_the_schema_order() {
    let (_dir, db_path) = fixture(0).await;
    let mut connector = connect(&db_path).await;

    let columns = connector
        .profile_query_streaming("SELECT date, id, active, amount FROM t", 2)
        .await
        .unwrap();

    assert_eq!(names(&columns), ["date", "id", "active", "amount"]);
    assert_eq!(columns.row_count(), 0);

    connector.disconnect().await.unwrap();
}

#[tokio::test]
async fn empty_results_still_validate_column_projection() {
    let (_dir, db_path) = fixture(0).await;
    let database_config = || DatabaseConfig {
        connection_string: db_path.clone(),
        load_credentials_from_env: false,
        ..Default::default()
    };

    let unknown = AnalysisOptions::default().with_columns(Some(vec!["missing".to_string()]));
    let error = analyze_database_with_options(database_config(), "SELECT * FROM t", &unknown)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("absent from the source: missing"),
        "unexpected unknown-column error: {error}"
    );

    let duplicate =
        AnalysisOptions::default().with_columns(Some(vec!["id".to_string(), "id".to_string()]));
    let error = analyze_database_with_options(database_config(), "SELECT * FROM t", &duplicate)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("duplicate name(s): id"),
        "unexpected duplicate-column error: {error}"
    );
}

#[tokio::test]
async fn column_values_stay_with_their_column() {
    // Order is only worth having if the data under each name is still right:
    // a positional fill that drifted would keep the names and corrupt the values.
    let (_dir, db_path) = fixture(3).await;
    let mut connector = connect(&db_path).await;

    let columns = connector
        .profile_query("SELECT amount, id FROM t ORDER BY id")
        .await
        .unwrap();

    assert_eq!(names(&columns), ["amount", "id"]);
    assert_eq!(columns["id"], ["0", "1", "2"]);
    assert_eq!(columns["amount"], ["0.5", "1.5", "2.5"]);

    connector.disconnect().await.unwrap();
}

#[tokio::test]
async fn the_order_is_the_same_on_every_run() {
    // Hash iteration order varies per process and could also vary per map
    // instance; profiling the same query repeatedly must not.
    let (_dir, db_path) = fixture(2).await;
    let mut connector = connect(&db_path).await;

    let mut seen = Vec::new();
    for _ in 0..8 {
        let columns = connector
            .profile_query("SELECT date, id, active, amount FROM t")
            .await
            .unwrap();
        seen.push(names(&columns).join(","));
    }
    seen.dedup();
    assert_eq!(
        seen,
        ["date,id,active,amount"],
        "repeated runs disagreed on column order"
    );

    connector.disconnect().await.unwrap();
}
