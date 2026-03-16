//! Database integration tests.
//!
//! SQLite tests run unconditionally when the `sqlite` feature is enabled (in-memory, no Docker).
//! PostgreSQL/MySQL tests require env vars: `POSTGRES_TEST_URL` / `MYSQL_TEST_URL`.

#![cfg(feature = "sqlite")]

use dataprof::database::{DatabaseConfig, analyze_database, create_connector};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a DatabaseConfig for SQLite with credentials loading disabled.
fn sqlite_config(connection_string: &str) -> DatabaseConfig {
    DatabaseConfig {
        connection_string: connection_string.to_string(),
        load_credentials_from_env: false,
        ..Default::default()
    }
}

/// Create a temporary SQLite file.
/// Returns the TempDir (must be kept alive) and the file path as a string
/// (e.g. `/tmp/.../test.db`). Using a raw `.db` path lets `create_connector`
/// match it as SQLite via the `.db` extension.
fn create_test_db() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    // Create the empty file so SQLite can open it
    std::fs::File::create(&db_path).unwrap();

    (dir, db_path.display().to_string())
}

/// Populate a SQLite database with test data via sqlx.
async fn populate_test_db(db_path: &str) {
    let pool = get_sqlite_pool(db_path).await;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS test_users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            age INTEGER,
            salary REAL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    type TestRow<'a> = (&'a str, Option<&'a str>, Option<i32>, Option<f64>);
    let rows: Vec<TestRow<'_>> = vec![
        ("alice", Some("alice@example.com"), Some(25), Some(50000.0)),
        ("bob", Some("bob@example.com"), Some(30), Some(60000.0)),
        ("charlie", None, Some(35), None),
        ("diana", Some("diana@example.com"), None, Some(55000.0)),
        ("eve", Some("eve@example.com"), Some(28), Some(70000.0)),
    ];

    for (name, email, age, salary) in rows {
        sqlx::query("INSERT INTO test_users (name, email, age, salary) VALUES (?, ?, ?, ?)")
            .bind(name)
            .bind(email)
            .bind(age)
            .bind(salary)
            .execute(&pool)
            .await
            .unwrap();
    }

    pool.close().await;
}

/// Open a raw sqlx SqlitePool for DDL operations.
async fn get_sqlite_pool(db_path: &str) -> sqlx::SqlitePool {
    use sqlx::sqlite::SqlitePoolOptions;

    let db_url = format!("sqlite://{}", db_path);

    SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// SQLite tests
// ---------------------------------------------------------------------------

mod sqlite_tests {
    use super::*;

    #[tokio::test]
    async fn test_connect_disconnect_lifecycle() {
        let (_dir, conn_str) = create_test_db();
        let config = sqlite_config(&conn_str);
        let mut connector = create_connector(config).unwrap();

        connector.connect().await.unwrap();
        connector.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_connection_test() {
        let (_dir, conn_str) = create_test_db();
        let config = sqlite_config(&conn_str);
        let mut connector = create_connector(config).unwrap();

        connector.connect().await.unwrap();
        let result = connector.test_connection().await.unwrap();
        assert!(result);
        connector.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_profile_query() {
        let (_dir, conn_str) = create_test_db();
        populate_test_db(&conn_str).await;

        let config = sqlite_config(&conn_str);
        let mut connector = create_connector(config).unwrap();
        connector.connect().await.unwrap();

        let columns = connector
            .profile_query("SELECT * FROM test_users")
            .await
            .unwrap();

        assert_eq!(columns.len(), 5); // id, name, email, age, salary
        // Each column should have 5 values
        for values in columns.values() {
            assert_eq!(values.len(), 5);
        }

        connector.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_profile_query_streaming() {
        let (_dir, conn_str) = create_test_db();
        populate_test_db(&conn_str).await;

        let config = sqlite_config(&conn_str);
        let mut connector = create_connector(config).unwrap();
        connector.connect().await.unwrap();

        let columns = connector
            .profile_query_streaming("SELECT * FROM test_users", 2)
            .await
            .unwrap();

        assert_eq!(columns.len(), 5);
        for values in columns.values() {
            assert_eq!(values.len(), 5);
        }

        connector.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_table_schema() {
        let (_dir, conn_str) = create_test_db();
        populate_test_db(&conn_str).await;

        let config = sqlite_config(&conn_str);
        let mut connector = create_connector(config).unwrap();
        connector.connect().await.unwrap();

        let schema = connector.get_table_schema("test_users").await.unwrap();

        assert!(schema.contains(&"id".to_string()));
        assert!(schema.contains(&"name".to_string()));
        assert!(schema.contains(&"email".to_string()));
        assert!(schema.contains(&"age".to_string()));
        assert!(schema.contains(&"salary".to_string()));

        connector.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_count_table_rows() {
        let (_dir, conn_str) = create_test_db();
        populate_test_db(&conn_str).await;

        let config = sqlite_config(&conn_str);
        let mut connector = create_connector(config).unwrap();
        connector.connect().await.unwrap();

        let count = connector.count_table_rows("test_users").await.unwrap();
        assert_eq!(count, 5);

        connector.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_analyze_database_full() {
        let (_dir, conn_str) = create_test_db();
        populate_test_db(&conn_str).await;

        let config = sqlite_config(&conn_str);
        let report = analyze_database(config, "SELECT * FROM test_users")
            .await
            .unwrap();

        assert_eq!(report.execution.rows_processed, 5);
        assert_eq!(report.column_profiles.len(), 5);
    }

    #[tokio::test]
    async fn test_analyze_database_table_name() {
        let (_dir, conn_str) = create_test_db();
        populate_test_db(&conn_str).await;

        let config = sqlite_config(&conn_str);
        // Pass table name instead of SELECT query
        let report = analyze_database(config, "test_users").await.unwrap();

        assert_eq!(report.execution.rows_processed, 5);
        assert_eq!(report.column_profiles.len(), 5);
    }

    #[tokio::test]
    async fn test_invalid_query_error() {
        let (_dir, conn_str) = create_test_db();
        populate_test_db(&conn_str).await;

        let config = sqlite_config(&conn_str);
        let result = analyze_database(config, "SELECT * FROM nonexistent_table").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sql_injection_rejected() {
        let (_dir, conn_str) = create_test_db();
        populate_test_db(&conn_str).await;

        let config = sqlite_config(&conn_str);
        // Semicolon-based injection — the DROP keyword triggers validation rejection
        let result =
            analyze_database(config, "SELECT * FROM test_users; DROP TABLE test_users").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_empty_result_set() {
        let (_dir, conn_str) = create_test_db();
        populate_test_db(&conn_str).await;

        let config = sqlite_config(&conn_str);
        let report = analyze_database(config, "SELECT * FROM test_users WHERE 1=0")
            .await
            .unwrap();

        assert_eq!(report.execution.rows_processed, 0);
    }

    #[tokio::test]
    async fn test_null_handling() {
        let (_dir, conn_str) = create_test_db();
        populate_test_db(&conn_str).await;

        let config = sqlite_config(&conn_str);
        let report = analyze_database(config, "SELECT * FROM test_users")
            .await
            .unwrap();

        // charlie has email=NULL and salary=NULL, diana has age=NULL
        // Verify we get profiles for all 5 columns with 5 rows
        assert_eq!(report.execution.rows_processed, 5);
        assert_eq!(report.column_profiles.len(), 5);
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL tests (require POSTGRES_TEST_URL env var)
// ---------------------------------------------------------------------------

#[cfg(feature = "postgres")]
mod postgres_tests {
    use super::*;

    fn postgres_url() -> Option<String> {
        std::env::var("POSTGRES_TEST_URL").ok()
    }

    fn postgres_config(url: &str) -> DatabaseConfig {
        DatabaseConfig {
            connection_string: url.to_string(),
            load_credentials_from_env: false,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_postgres_connect() {
        let Some(url) = postgres_url() else {
            return;
        };
        let config = postgres_config(&url);
        let mut connector = create_connector(config).unwrap();
        connector.connect().await.unwrap();

        let result = connector.test_connection().await.unwrap();
        assert!(result);

        connector.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_postgres_profile_query() {
        let Some(url) = postgres_url() else {
            return;
        };
        let config = postgres_config(&url);
        let mut connector = create_connector(config).unwrap();
        connector.connect().await.unwrap();

        let columns = connector
            .profile_query("SELECT * FROM test_users")
            .await
            .unwrap();

        assert!(!columns.is_empty());
        connector.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_postgres_get_table_schema() {
        let Some(url) = postgres_url() else {
            return;
        };
        let config = postgres_config(&url);
        let mut connector = create_connector(config).unwrap();
        connector.connect().await.unwrap();

        let schema = connector.get_table_schema("test_users").await.unwrap();
        assert!(!schema.is_empty());

        connector.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_postgres_count_table_rows() {
        let Some(url) = postgres_url() else {
            return;
        };
        let config = postgres_config(&url);
        let mut connector = create_connector(config).unwrap();
        connector.connect().await.unwrap();

        let count = connector.count_table_rows("test_users").await.unwrap();
        assert!(count > 0);

        connector.disconnect().await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// MySQL tests (require MYSQL_TEST_URL env var)
// ---------------------------------------------------------------------------

#[cfg(feature = "mysql")]
mod mysql_tests {
    use super::*;

    fn mysql_url() -> Option<String> {
        std::env::var("MYSQL_TEST_URL").ok()
    }

    fn mysql_config(url: &str) -> DatabaseConfig {
        DatabaseConfig {
            connection_string: url.to_string(),
            load_credentials_from_env: false,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_mysql_connect() {
        let Some(url) = mysql_url() else {
            return;
        };
        let config = mysql_config(&url);
        let mut connector = create_connector(config).unwrap();
        connector.connect().await.unwrap();

        let result = connector.test_connection().await.unwrap();
        assert!(result);

        connector.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_mysql_profile_query() {
        let Some(url) = mysql_url() else {
            return;
        };
        let config = mysql_config(&url);
        let mut connector = create_connector(config).unwrap();
        connector.connect().await.unwrap();

        let columns = connector
            .profile_query("SELECT * FROM test_products")
            .await
            .unwrap();

        assert!(!columns.is_empty());
        connector.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_mysql_get_table_schema() {
        let Some(url) = mysql_url() else {
            return;
        };
        let config = mysql_config(&url);
        let mut connector = create_connector(config).unwrap();
        connector.connect().await.unwrap();

        let schema = connector.get_table_schema("test_products").await.unwrap();
        assert!(!schema.is_empty());

        connector.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_mysql_count_table_rows() {
        let Some(url) = mysql_url() else {
            return;
        };
        let config = mysql_config(&url);
        let mut connector = create_connector(config).unwrap();
        connector.connect().await.unwrap();

        let count = connector.count_table_rows("test_products").await.unwrap();
        assert!(count > 0);

        connector.disconnect().await.unwrap();
    }
}
