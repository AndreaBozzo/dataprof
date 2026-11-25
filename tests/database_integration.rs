//! Database integration tests.
//!
//! SQLite tests run unconditionally when the `sqlite` feature is enabled (in-memory, no Docker).
//! PostgreSQL/MySQL tests require env vars: `POSTGRES_TEST_URL` / `MYSQL_TEST_URL`.

#![cfg(feature = "sqlite")]

use dataprof::{
    DataSource, DatabaseConfig, Profiler, QueryEngine, analyze_database, create_connector,
};

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
        let report = analyze_database(config, "SELECT * FROM test_users", true, None)
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
        let report = analyze_database(config, "test_users", true, None)
            .await
            .unwrap();

        assert_eq!(report.execution.rows_processed, 5);
        assert_eq!(report.column_profiles.len(), 5);
    }

    #[tokio::test]
    async fn test_invalid_query_error() {
        let (_dir, conn_str) = create_test_db();
        populate_test_db(&conn_str).await;

        let config = sqlite_config(&conn_str);
        let result = analyze_database(config, "SELECT * FROM nonexistent_table", true, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sql_injection_rejected() {
        let (_dir, conn_str) = create_test_db();
        populate_test_db(&conn_str).await;

        let config = sqlite_config(&conn_str);
        // Semicolon-based injection — the DROP keyword triggers validation rejection
        let result = analyze_database(
            config,
            "SELECT * FROM test_users; DROP TABLE test_users",
            true,
            None,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_empty_result_set() {
        let (_dir, conn_str) = create_test_db();
        populate_test_db(&conn_str).await;

        let config = sqlite_config(&conn_str);
        let report = analyze_database(config, "SELECT * FROM test_users WHERE 1=0", true, None)
            .await
            .unwrap();

        assert_eq!(report.execution.rows_processed, 0);
    }

    #[tokio::test]
    async fn test_null_handling() {
        let (_dir, conn_str) = create_test_db();
        populate_test_db(&conn_str).await;

        let config = sqlite_config(&conn_str);
        let report = analyze_database(config, "SELECT * FROM test_users", true, None)
            .await
            .unwrap();

        // charlie has email=NULL and salary=NULL, diana has age=NULL
        // Verify we get profiles for all 5 columns with 5 rows
        assert_eq!(report.execution.rows_processed, 5);
        assert_eq!(report.column_profiles.len(), 5);
    }
}

// ---------------------------------------------------------------------------
// Profiler builder API tests
// ---------------------------------------------------------------------------

mod profiler_builder_tests {
    use super::*;

    #[tokio::test]
    async fn test_profiler_analyze_query() {
        let (_dir, conn_str) = create_test_db();
        populate_test_db(&conn_str).await;

        let report = Profiler::new()
            .connection_string(&conn_str)
            .analyze_query("SELECT * FROM test_users")
            .await
            .unwrap();

        assert_eq!(report.execution.rows_processed, 5);
        assert_eq!(report.column_profiles.len(), 5);
        assert!(report.quality.is_some());
    }

    /// The query runs as written. Paging appended `LIMIT n OFFSET k` to it, so a
    /// query with its own `LIMIT` became a syntax error (#887).
    #[tokio::test]
    async fn test_profiler_analyze_query_keeps_its_own_limit() {
        let (_dir, conn_str) = create_test_db();
        populate_test_db(&conn_str).await;

        let report = Profiler::new()
            .connection_string(&conn_str)
            .analyze_query("SELECT * FROM test_users ORDER BY id LIMIT 2")
            .await
            .unwrap();

        assert_eq!(report.execution.rows_processed, 2);
    }

    #[tokio::test]
    async fn test_profiler_analyze_query_no_quality() {
        let (_dir, conn_str) = create_test_db();
        populate_test_db(&conn_str).await;

        let report = Profiler::new()
            .connection_string(&conn_str)
            .analyze_query_no_quality("SELECT * FROM test_users")
            .await
            .unwrap();

        assert_eq!(report.execution.rows_processed, 5);
        assert!(report.quality.is_none());
    }

    #[tokio::test]
    async fn test_profiler_analyze_query_table_name() {
        let (_dir, conn_str) = create_test_db();
        populate_test_db(&conn_str).await;

        let report = Profiler::new()
            .connection_string(&conn_str)
            .analyze_query("test_users")
            .await
            .unwrap();

        assert_eq!(report.execution.rows_processed, 5);
    }

    #[tokio::test]
    async fn test_profiler_analyze_query_with_database_config() {
        let (_dir, conn_str) = create_test_db();
        populate_test_db(&conn_str).await;

        let config = DatabaseConfig {
            connection_string: conn_str,
            batch_size: 2,
            load_credentials_from_env: false,
            ..Default::default()
        };

        let report = Profiler::new()
            .database(config)
            .analyze_query("SELECT * FROM test_users")
            .await
            .unwrap();

        assert_eq!(report.execution.rows_processed, 5);
    }

    #[tokio::test]
    async fn test_profiler_no_database_configured_error() {
        let result = Profiler::new().analyze_query("SELECT 1").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_profiler_analyze_source_async_query() {
        let (_dir, conn_str) = create_test_db();
        populate_test_db(&conn_str).await;

        let source = DataSource::Query {
            engine: QueryEngine::Sqlite,
            statement: "SELECT * FROM test_users".to_string(),
            database: None,
            execution_id: None,
        };

        let report = Profiler::new()
            .connection_string(&conn_str)
            .analyze_source_async(&source)
            .await
            .unwrap();

        assert_eq!(report.execution.rows_processed, 5);
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
    async fn postgres_column_fingerprints_match_text_and_withhold_unavailable_values() {
        use std::io::Write as _;

        let Some(url) = postgres_url() else {
            return;
        };
        let sql = "SELECT amount, flag, narrow, payload, nested, wide, NULL::integer[] AS null_array FROM (VALUES
            (1, NUMERIC '1.50', true, REAL '0.1', BYTEA 'abc', ARRAY[1], NUMERIC '1234567890123456789012345678901234567891'),
            (2, NUMERIC '1.00', false, REAL '0.5', BYTEA 'def', ARRAY[2], NULL),
            (3, NULL, NULL, NULL, NULL, NULL, NULL)
        ) AS fixture(position, amount, flag, narrow, payload, nested, wide) ORDER BY position";
        let mut config = postgres_config(&url);
        config.batch_size = 1;
        let postgres = analyze_database(config, sql, true, None).await.unwrap();
        let mut csv_file = tempfile::NamedTempFile::with_suffix(".csv").unwrap();
        writeln!(
            csv_file,
            "amount,flag,narrow\nNULL,NULL,NULL\n1,false,0.5\n1.5,true,0.10000000149011612"
        )
        .unwrap();
        let csv = Profiler::new().analyze_file(csv_file.path()).unwrap();
        for (db, file) in postgres.column_profiles.iter().zip(&csv.column_profiles) {
            assert!(db.fingerprint.is_some(), "{}", db.name);
            assert_eq!(db.fingerprint, file.fingerprint, "{}", db.name);
        }
        for column in &postgres.column_profiles[3..] {
            assert!(column.fingerprint.is_none(), "{}", column.name);
        }
    }

    #[cfg(feature = "parquet")]
    #[tokio::test]
    async fn postgres_timestamptz_matches_zoned_arrow_and_csv() {
        use std::{io::Write as _, sync::Arc};

        use arrow::{array::TimestampMicrosecondArray, record_batch::RecordBatch};
        use chrono::{DateTime, NaiveDate};
        use dataprof::{AnalysisOptions, QualityPolicy, Verdict};
        use parquet::arrow::ArrowWriter;
        use serde_json::{json, to_value};

        let Some(url) = postgres_url() else {
            return;
        };
        let options = AnalysisOptions::default()
            .with_reference_date(NaiveDate::from_ymd_opt(2024, 1, 3))
            .with_unique_keys(vec![vec!["published_at".into()]]);
        let postgres = dataprof::analyze_database_with_options(
            postgres_config(&url),
            "SELECT published_at FROM (VALUES
                (1, TIMESTAMPTZ '2024-01-02 01:45:00.123456+02'),
                (2, TIMESTAMPTZ '2024-01-01 18:45:00.123456-05'),
                (3, TIMESTAMPTZ '2024-01-02 12:00:00+00'),
                (4, NULL)
            ) AS timestamps(position, published_at) ORDER BY position",
            &options,
        )
        .await
        .unwrap();

        let first = "2024-01-01T23:45:00.123456Z";
        let last = "2024-01-02T12:00:00Z";
        let mut csv_file = tempfile::NamedTempFile::with_suffix(".csv").unwrap();
        writeln!(csv_file, "published_at\n{first}\n{first}\n{last}\nNULL").unwrap();
        csv_file.flush().unwrap();
        let csv =
            dataprof::analyze_csv_file_with_options(csv_file.path(), &Default::default(), &options)
                .unwrap();

        let timestamps = [Some(first), Some(first), Some(last), None].map(|value| {
            value.map(|value| {
                DateTime::parse_from_rfc3339(value)
                    .unwrap()
                    .timestamp_micros()
            })
        });
        let array = TimestampMicrosecondArray::from(timestamps.to_vec()).with_timezone("UTC");
        let batch = RecordBatch::try_from_iter([("published_at", Arc::new(array) as _)]).unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let arrow = dataprof_parquet::analyze_parquet_bytes_with_options(
            bytes.into(),
            "timestamps.parquet",
            &Default::default(),
            &options,
        )
        .unwrap();

        let policy = QualityPolicy::new().max_date_age("published_at", 1);
        let postgres_gate = policy.evaluate(&postgres).unwrap();
        assert_eq!(postgres_gate.verdict, Verdict::Pass);
        let postgres_findings = to_value(postgres.findings().findings).unwrap();
        let postgres = to_value(&postgres).unwrap();
        let mut postgres_column = postgres["column_profiles"][0].clone();
        assert!(postgres_column["fingerprint"].is_string());
        assert_eq!(postgres_column["native_type"]["name"], "TIMESTAMPTZ");
        assert_eq!(
            postgres_column["stats"]["DateTime"]["min_datetime"],
            "2024-01-01"
        );
        assert_eq!(
            postgres_column["stats"]["DateTime"]["max_datetime"],
            "2024-01-02"
        );
        assert_eq!(
            postgres["quality"]["metrics"]["timeliness"]["date_values_checked"],
            3
        );
        assert_eq!(
            postgres["composite_keys"][0]["sample_repeated_keys"],
            json!([[first]])
        );
        // The declared type identifies the source's type system.
        postgres_column
            .as_object_mut()
            .unwrap()
            .remove("native_type");
        for (route, report) in [("CSV", csv), ("Arrow via Parquet", arrow)] {
            let gate = policy.evaluate(&report).unwrap();
            assert_eq!(
                to_value(gate.checks).unwrap(),
                to_value(&postgres_gate.checks).unwrap(),
                "{route} date age"
            );
            assert_eq!(
                to_value(report.findings().findings).unwrap(),
                postgres_findings,
                "{route} findings"
            );
            let document = to_value(report).unwrap();
            let mut actual_column = document["column_profiles"][0].clone();
            actual_column.as_object_mut().unwrap().remove("native_type");
            assert_eq!(actual_column, postgres_column, "{route} column profile");
            assert_eq!(
                document["composite_keys"], postgres["composite_keys"],
                "{route} composite keys"
            );
            assert_eq!(
                document["quality"]["metrics"], postgres["quality"]["metrics"],
                "{route} quality metrics"
            );
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

    /// A verifying TLS mode in the URL reaches the driver. The test server
    /// cannot satisfy it (no TLS, or a certificate no CA here signed), so
    /// connecting fails; the settings were dropped and it connected.
    #[tokio::test]
    async fn test_postgres_url_tls_verification_is_enforced() {
        let Some(url) = postgres_url() else {
            return;
        };
        let separator = if url.contains('?') { '&' } else { '?' };
        let config = postgres_config(&format!(
            "{url}{separator}sslmode=verify-full&sslrootcert=no-such-dir/ca.crt"
        ));
        let mut connector = create_connector(config).unwrap();
        assert!(connector.connect().await.is_err());
    }

    /// `SslConfig::production()` (verify-full) applies without the
    /// environment too: it was only applied when credentials were loaded
    /// from it.
    #[tokio::test]
    async fn test_postgres_production_ssl_config_is_enforced() {
        let Some(url) = postgres_url() else {
            return;
        };
        let config = DatabaseConfig {
            ssl_config: Some(dataprof::SslConfig::production()),
            ..postgres_config(&url)
        };
        let mut connector = create_connector(config).unwrap();
        assert!(connector.connect().await.is_err());
    }

    /// sqlx reads query names decoded: an encoded `sslmode` is still the
    /// caller's mode, and the default `prefer` appended after it connected.
    /// A CA filename with `&` in it started a pair that disabled TLS.
    #[tokio::test]
    async fn test_postgres_tls_settings_survive_url_encoding() {
        let Some(url) = postgres_url() else {
            return;
        };
        let separator = if url.contains('?') { '&' } else { '?' };
        for load_credentials_from_env in [false, true] {
            let config = DatabaseConfig {
                load_credentials_from_env,
                ..postgres_config(&format!(
                    "{url}{separator}ssl%6dode=verify-full&sslrootcert=no-such-dir/ca.crt"
                ))
            };
            let mut connector = create_connector(config).unwrap();
            assert!(connector.connect().await.is_err());

            let ssl_config = dataprof::SslConfig {
                ca_cert_path: Some("no-such-dir/CA&sslmode=disable".to_string()),
                ..dataprof::SslConfig::production()
            };
            let config = DatabaseConfig {
                ssl_config: Some(ssl_config),
                load_credentials_from_env,
                ..postgres_config(&url)
            };
            let mut connector = create_connector(config).unwrap();
            assert!(connector.connect().await.is_err());
        }
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

    /// Every row of a query is profiled exactly once.
    ///
    /// The table is larger than a quarter of `shared_buffers` (32 MB with the
    /// default 128 MB), so PostgreSQL synchronizes sequential scans: a new scan
    /// starts near where the previous one stopped. Paging the query with
    /// LIMIT/OFFSET then returned overlapping pages, so ids repeated and others
    /// never arrived while the row count still matched (#887).
    #[tokio::test]
    async fn test_postgres_query_rows_are_each_profiled_once() {
        let Some(url) = postgres_url() else {
            return;
        };
        const ROWS: usize = 400_000;
        let pool = sqlx::PgPool::connect(&url).await.unwrap();
        sqlx::query("DROP TABLE IF EXISTS dataprof_rows_profiled_once")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE dataprof_rows_profiled_once AS \
             SELECT g::bigint AS id, md5(g::text) || md5((g * 7)::text) || 'x' AS payload \
             FROM generate_series(1, {ROWS}) g"
        )))
        .execute(&pool)
        .await
        .unwrap();

        let report = Profiler::new()
            .database(postgres_config(&url))
            .analyze_query("SELECT * FROM dataprof_rows_profiled_once")
            .await;
        sqlx::query("DROP TABLE dataprof_rows_profiled_once")
            .execute(&pool)
            .await
            .unwrap();
        let report = report.unwrap();

        let id = report
            .column_profiles
            .iter()
            .find(|column| column.name == "id")
            .unwrap();
        let duplicate_rows = report
            .quality
            .as_ref()
            .and_then(|quality| quality.metrics.uniqueness.as_ref())
            .map(|uniqueness| uniqueness.duplicate_rows);
        assert_eq!(report.execution.rows_processed, ROWS);
        assert_eq!(
            (id.unique_count, duplicate_rows),
            (Some(ROWS), Some(0)),
            "every id once, no row read twice"
        );
    }

    /// An error partway through a result fails the profile. Reading the result
    /// as a stream must not stop at the failing row and profile the rows before
    /// it as if they were the whole result (#887).
    #[tokio::test]
    async fn test_postgres_error_partway_through_a_result_is_raised() {
        let Some(url) = postgres_url() else {
            return;
        };

        let error = Profiler::new()
            .database(postgres_config(&url))
            .analyze_query(
                "SELECT CASE WHEN g = 30000 THEN 1 / (g - g) ELSE g END AS v \
                 FROM generate_series(1, 50000) g",
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                &error,
                dataprof::DataProfilerError::DatabaseQueryError { message }
                    if message.starts_with("Query execution failed")
            ),
            "{error:?}"
        );
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

    /// A verifying TLS mode in the URL reaches the driver. The test server
    /// cannot satisfy it (no TLS, or a certificate no CA here signed), so
    /// connecting fails; the settings were dropped and it connected.
    #[tokio::test]
    async fn test_mysql_url_tls_verification_is_enforced() {
        let Some(url) = mysql_url() else {
            return;
        };
        let separator = if url.contains('?') { '&' } else { '?' };
        let config = mysql_config(&format!(
            "{url}{separator}ssl-mode=VERIFY_CA&ssl-ca=no-such-dir/ca.crt"
        ));
        let mut connector = create_connector(config).unwrap();
        assert!(connector.connect().await.is_err());
    }

    /// `SslConfig::production()` (verify-full) applies without the
    /// environment too: it was only applied when credentials were loaded
    /// from it.
    #[tokio::test]
    async fn test_mysql_production_ssl_config_is_enforced() {
        let Some(url) = mysql_url() else {
            return;
        };
        let config = DatabaseConfig {
            ssl_config: Some(dataprof::SslConfig::production()),
            ..mysql_config(&url)
        };
        let mut connector = create_connector(config).unwrap();
        assert!(connector.connect().await.is_err());
    }

    /// sqlx reads query names decoded: an encoded `ssl-mode` is still the
    /// caller's mode, and the default `PREFERRED` appended after it
    /// connected. A CA filename with `&` in it started a pair that disabled
    /// TLS.
    #[tokio::test]
    async fn test_mysql_tls_settings_survive_url_encoding() {
        let Some(url) = mysql_url() else {
            return;
        };
        let separator = if url.contains('?') { '&' } else { '?' };
        for load_credentials_from_env in [false, true] {
            let config = DatabaseConfig {
                load_credentials_from_env,
                ..mysql_config(&format!(
                    "{url}{separator}ssl%2Dmode=VERIFY_CA&ssl-ca=no-such-dir/ca.crt"
                ))
            };
            let mut connector = create_connector(config).unwrap();
            assert!(connector.connect().await.is_err());

            let ssl_config = dataprof::SslConfig {
                ca_cert_path: Some("no-such-dir/CA&ssl-mode=DISABLED".to_string()),
                ..dataprof::SslConfig::production()
            };
            let config = DatabaseConfig {
                ssl_config: Some(ssl_config),
                load_credentials_from_env,
                ..mysql_config(&url)
            };
            let mut connector = create_connector(config).unwrap();
            assert!(connector.connect().await.is_err());
        }
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
            .profile_query("SELECT * FROM test_users")
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

        let schema = connector.get_table_schema("test_users").await.unwrap();
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

        let count = connector.count_table_rows("test_users").await.unwrap();
        assert!(count > 0);

        connector.disconnect().await.unwrap();
    }
}
