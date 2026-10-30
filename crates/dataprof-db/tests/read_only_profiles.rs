//! A profiled query cannot write, whatever the word check lets through (#1169).
//!
//! The query runs in a read-only transaction on PostgreSQL and MySQL, and on
//! a `query_only` connection on SQLite, so the database refuses a write the
//! word list does not name: `nextval` and a stored function that inserts, and
//! a DELETE handed straight to `profile_query`, which runs no word check. A
//! read-only `WITH` query is profiled. The PostgreSQL and MySQL tests need the
//! CI database services (`POSTGRES_TEST_URL`, `MYSQL_TEST_URL`) and pass
//! without running when those are unset.

#![cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]

use dataprof_db::DatabaseConfig;

fn config(url: &str) -> DatabaseConfig {
    DatabaseConfig {
        connection_string: url.to_string(),
        load_credentials_from_env: false,
        ..Default::default()
    }
}

/// Run `statement` through the connector's batch path, which runs no word
/// check, and describe what happened.
async fn batch(url: &str, statement: &str) -> Result<(), String> {
    let mut connector = dataprof_db::create_connector(config(url)).expect("connector");
    connector.connect().await.expect("connect");
    let result = connector.profile_query(statement).await;
    connector.disconnect().await.expect("disconnect");
    result.map(|_| ()).map_err(|error| error.to_string())
}

/// The rows `query` profiles to, or the error.
async fn profiled_rows(url: &str, query: &str) -> Result<usize, String> {
    dataprof_db::analyze_database(config(url), query, false, None)
        .await
        .map(|report| report.execution.rows_processed)
        .map_err(|error| error.to_string())
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
    if let Ok(()) = batch(&url, "DELETE FROM read_only_pg").await {
        wrong.push("batch DELETE ran".to_string());
    }
    if let Ok(rows) = profiled_rows(&url, "SELECT nextval('read_only_pg_seq') AS n").await {
        wrong.push(format!("nextval profiled {rows} rows"));
    }
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM read_only_pg")
        .fetch_one(&pool)
        .await
        .expect("count");
    let advanced: bool = sqlx::query_scalar("SELECT is_called FROM read_only_pg_seq")
        .fetch_one(&pool)
        .await
        .expect("sequence");
    if rows != 3 || advanced {
        wrong.push(format!(
            "table holds {rows} rows, sequence advanced: {advanced}"
        ));
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
    // MySQL has no built-in function that writes, so the profiled path is
    // checked through a stored one. With binary logging on, creating it needs
    // SUPER or `log_bin_trust_function_creators`, which a CI service user may
    // not have; the batch DELETE below covers the transaction there.
    let function = sqlx::raw_sql(
        "CREATE FUNCTION read_only_my_bump() RETURNS INTEGER MODIFIES SQL DATA
         BEGIN INSERT INTO read_only_my VALUES (99); RETURN 1; END",
    )
    .execute(&pool)
    .await;

    let mut wrong = Vec::new();
    if let Ok(()) = batch(&url, "DELETE FROM read_only_my").await {
        wrong.push("batch DELETE ran".to_string());
    }
    match function {
        Ok(_) => {
            if let Ok(rows) = profiled_rows(&url, "SELECT read_only_my_bump() AS b").await {
                wrong.push(format!("a writing function profiled {rows} rows"));
            }
        }
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
    if let Ok(()) = batch(&db_path, "DELETE FROM read_only_lite").await {
        wrong.push("batch DELETE ran".to_string());
    }
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
