//! Every non-text column used to profile as an all-null string column.
//!
//! The connectors read each column as `Option<String>` and dropped the decode
//! error with `.ok()`, so an INTEGER or REAL value was indistinguishable from
//! SQL NULL. These tests pin the decoding contract for the types the profiler
//! can actually infer.

#![cfg(feature = "sqlite")]

use sqlx::sqlite::SqlitePoolOptions;

/// Read a query's rows into the column-oriented map the profiler consumes.
async fn columns_of(query: &str, ddl: &[&str]) -> std::collections::HashMap<String, Vec<String>> {
    let pool = SqlitePoolOptions::new()
        .connect("sqlite::memory:")
        .await
        .expect("in-memory sqlite");
    for stmt in ddl {
        sqlx::query(stmt).execute(&pool).await.expect("ddl");
    }
    let rows = sqlx::query(query).fetch_all(&pool).await.expect("query");
    dataprof_db::process_rows_to_columns!(rows)
}

#[tokio::test]
async fn numeric_and_boolean_columns_survive_decoding() {
    let cols = columns_of(
        "SELECT * FROM t ORDER BY i",
        &[
            "CREATE TABLE t (i INTEGER, r REAL, b BOOLEAN, s TEXT)",
            "INSERT INTO t VALUES (42, 10.5, 1, 'hi')",
            "INSERT INTO t VALUES (-7, -0.25, 0, 'there')",
        ],
    )
    .await;

    assert_eq!(cols["i"], vec!["-7", "42"]);
    assert_eq!(cols["r"], vec!["-0.25", "10.5"]);
    // SQLite stores BOOLEAN as INTEGER, so it renders through the integer arm.
    assert_eq!(cols["b"], vec!["0", "1"]);
    assert_eq!(cols["s"], vec!["there", "hi"]);
}

#[tokio::test]
async fn sql_null_is_still_empty_for_every_type() {
    let cols = columns_of(
        "SELECT * FROM t",
        &[
            "CREATE TABLE t (i INTEGER, r REAL, s TEXT)",
            "INSERT INTO t VALUES (NULL, NULL, NULL)",
        ],
    )
    .await;

    assert_eq!(cols["i"], vec![""]);
    assert_eq!(cols["r"], vec![""]);
    assert_eq!(cols["s"], vec![""]);
}

#[tokio::test]
async fn nulls_do_not_swallow_neighbouring_values() {
    let cols = columns_of(
        "SELECT * FROM t ORDER BY rowid",
        &[
            "CREATE TABLE t (amt REAL)",
            "INSERT INTO t VALUES (100.0), (NULL), (1.0), (NULL), (2.0)",
        ],
    )
    .await;

    assert_eq!(cols["amt"], vec!["100.0", "", "1.0", "", "2.0"]);
}

#[tokio::test]
async fn integral_floats_keep_their_decimal_point() {
    // A REAL column holding whole numbers must not stringify as "100", or the
    // profiler infers it as an integer column.
    let cols = columns_of(
        "SELECT * FROM t ORDER BY rowid",
        &[
            "CREATE TABLE t (r REAL)",
            "INSERT INTO t VALUES (100.0), (1.0), (2.5)",
        ],
    )
    .await;

    assert_eq!(cols["r"], vec!["100.0", "1.0", "2.5"]);
}

#[tokio::test]
async fn large_integers_are_not_truncated_through_a_float_arm() {
    // 2^53 + 1 is the smallest integer f64 cannot represent exactly. Decoding it
    // through the f64 arm would round it to 9007199254740992.
    let cols = columns_of(
        "SELECT * FROM t",
        &[
            "CREATE TABLE t (big INTEGER)",
            "INSERT INTO t VALUES (9007199254740993)",
        ],
    )
    .await;

    assert_eq!(cols["big"], vec!["9007199254740993"]);
}
