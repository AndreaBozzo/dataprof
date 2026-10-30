//! A profiled query cannot write, whatever the word check lets through (#1169).
//!
//! The query runs in a read-only transaction on PostgreSQL and MySQL, so the
//! database refuses a write the word list does not name: `nextval`, and a
//! stored function that inserts. Both query paths are checked: the batch
//! `profile_query` and the streaming one behind `analyze_database`. On SQLite
//! the connection is `query_only`, and a query cannot switch that off: a
//! second statement is refused before it runs. A read-only `WITH` query is
//! profiled on every backend. The PostgreSQL and MySQL tests need the CI
//! database services (`POSTGRES_TEST_URL`, `MYSQL_TEST_URL`) and pass without
//! running when those are unset.

#![cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]

use dataprof_db::DatabaseConfig;

fn config(url: &str) -> DatabaseConfig {
    DatabaseConfig {
        connection_string: url.to_string(),
        load_credentials_from_env: false,
        ..Default::default()
    }
}

/// Run `query` through the connector's batch path and describe what happened.
async fn batch(url: &str, query: &str) -> Result<(), String> {
    let mut connector = dataprof_db::create_connector(config(url)).expect("connector");
    connector.connect().await.expect("connect");
    let result = connector.profile_query(query).await;
    connector.disconnect().await.expect("disconnect");
    result.map(|_| ()).map_err(|error| error.to_string())
}

/// The rows `query` profiles to through the streaming path, or the error.
async fn profiled_rows(url: &str, query: &str) -> Result<usize, String> {
    dataprof_db::analyze_database(config(url), query, false, None)
        .await
        .map(|report| report.execution.rows_processed)
        .map_err(|error| error.to_string())
}

/// Run `query` on both paths, recording each that succeeded.
async fn both_paths_refuse(url: &str, query: &str, wrong: &mut Vec<String>) {
    if batch(url, query).await.is_ok() {
        wrong.push(format!("batch ran {query}"));
    }
    if let Ok(rows) = profiled_rows(url, query).await {
        wrong.push(format!("streaming profiled {rows} rows from {query}"));
    }
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_profiles_cannot_write() {
    use sqlx::postgres::PgPoolOptions;

    let Ok(url) = std::env::var("POSTGRES_TEST_URL") else {
        return;
    };
    let pool = PgPoolOptions::new().connect(&url).await.expect("connect");
    for statement in [
        "DROP TABLE IF EXISTS read_only_pg",
        "DROP SEQUENCE IF EXISTS read_only_pg_seq",
        "CREATE TABLE read_only_pg (id INTEGER)",
        "INSERT INTO read_only_pg VALUES (1), (2), (3)",
        "CREATE SEQUENCE read_only_pg_seq",
    ] {
        sqlx::query(statement)
            .execute(&pool)
            .await
            .expect(statement);
    }

    let mut wrong = Vec::new();
    both_paths_refuse(&url, "SELECT nextval('read_only_pg_seq') AS n", &mut wrong).await;
    let advanced: bool = sqlx::query_scalar("SELECT is_called FROM read_only_pg_seq")
        .fetch_one(&pool)
        .await
        .expect("sequence");
    if advanced {
        wrong.push("the sequence advanced".to_string());
    }
    match profiled_rows(
        &url,
        "WITH r AS (SELECT id FROM read_only_pg) SELECT * FROM r",
    )
    .await
    {
        Ok(3) => {}
        other => wrong.push(format!("WITH query: {other:?}")),
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn mysql_profiles_cannot_write() {
    use sqlx::mysql::MySqlPoolOptions;

    let Ok(url) = std::env::var("MYSQL_TEST_URL") else {
        return;
    };
    let pool = MySqlPoolOptions::new()
        .connect(&url)
        .await
        .expect("connect");
    for statement in [
        "DROP TABLE IF EXISTS read_only_my",
        "DROP FUNCTION IF EXISTS read_only_my_bump",
        "CREATE TABLE read_only_my (id INTEGER)",
        "INSERT INTO read_only_my VALUES (1), (2), (3)",
    ] {
        sqlx::raw_sql(statement)
            .execute(&pool)
            .await
            .expect(statement);
    }
    // MySQL has no built-in function that writes, so the transaction is
    // checked through a stored one. With binary logging on, creating it needs
    // SUPER or `log_bin_trust_function_creators`, which a CI service user may
    // not have.
    let function = sqlx::raw_sql(
        "CREATE FUNCTION read_only_my_bump() RETURNS INTEGER MODIFIES SQL DATA
         BEGIN INSERT INTO read_only_my VALUES (99); RETURN 1; END",
    )
    .execute(&pool)
    .await;

    let mut wrong = Vec::new();
    match function {
        Ok(_) => both_paths_refuse(&url, "SELECT read_only_my_bump() AS b", &mut wrong).await,
        Err(error) => eprintln!("writing function not checked: {error}"),
    }
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM read_only_my")
        .fetch_one(&pool)
        .await
        .expect("count");
    if rows != 3 {
        wrong.push(format!("table holds {rows} rows"));
    }
    match profiled_rows(
        &url,
        "WITH r AS (SELECT id FROM read_only_my) SELECT * FROM r",
    )
    .await
    {
        Ok(3) => {}
        other => wrong.push(format!("WITH query: {other:?}")),
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_profiles_cannot_write() {
    use sqlx::sqlite::SqlitePoolOptions;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("read_only.db");
    std::fs::File::create(&db_path).unwrap();
    let db_path = db_path.display().to_string();
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{db_path}"))
        .await
        .unwrap();
    for statement in [
        "CREATE TABLE read_only_lite (id INTEGER)",
        "INSERT INTO read_only_lite VALUES (1), (2), (3)",
    ] {
        sqlx::query(statement)
            .execute(&pool)
            .await
            .expect(statement);
    }

    let mut wrong = Vec::new();
    // SQLite runs every statement of a query: a second one could switch the
    // connection's `query_only` off and then write.
    both_paths_refuse(
        &db_path,
        "SELECT 1; PRAGMA query_only = OFF; REPLACE INTO read_only_lite SELECT 4",
        &mut wrong,
    )
    .await;
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM read_only_lite")
        .fetch_one(&pool)
        .await
        .expect("count");
    if rows != 3 {
        wrong.push(format!("table holds {rows} rows"));
    }
    match profiled_rows(
        &db_path,
        "WITH r AS (SELECT id FROM read_only_lite) SELECT * FROM r",
    )
    .await
    {
        Ok(3) => {}
        other => wrong.push(format!("WITH query: {other:?}")),
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}
