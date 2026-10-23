//! A non-null value the connectors cannot decode is an error, not a null (#952).
//!
//! `db_column_to_string!` tried a fixed chain of decodes and fell back to
//! `None`, which the profiler cannot tell from SQL NULL. A column of any type
//! the chain did not know was then reported as entirely missing: a PostgreSQL
//! enum, the usual way to declare a status or category column, read as 100%
//! null. These tests pin the three parts of the fix separately, so a failing
//! run says which one regressed:
//!
//! - enum labels decode, with their real nulls, on both connector paths;
//! - a non-null value of a type without a decoder fails the query and names
//!   the column and its type, on both paths, while a NULL of that type still
//!   reads as null;
//! - binary values still read as null, as the connector guide documents, until
//!   they have a column type (#645).
//!
//! PostgreSQL and MySQL need the CI database services (`POSTGRES_TEST_URL`,
//! `MYSQL_TEST_URL`) and skip without them; SQLite runs everywhere.

#![cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]

use dataprof_core::DataProfilerError;
use dataprof_db::{DatabaseConfig, QueryColumns};

fn config(url: &str) -> DatabaseConfig {
    DatabaseConfig {
        connection_string: url.to_string(),
        load_credentials_from_env: false,
        ..Default::default()
    }
}

/// The query read through the connector's batch and streaming paths.
async fn both_paths(
    url: &str,
    query: &str,
) -> [(&'static str, Result<QueryColumns, DataProfilerError>); 2] {
    let mut connector = dataprof_db::create_connector(config(url)).expect("connector");
    connector.connect().await.expect("connect");
    let batch = connector.profile_query(query).await;
    let streaming = connector.profile_query_streaming(query, 1).await;
    connector.disconnect().await.expect("disconnect");
    [("batch", batch), ("streaming", streaming)]
}

/// What is wrong with each path's values of `name`, against `expected`.
fn value_wrongs(
    paths: &[(&'static str, Result<QueryColumns, DataProfilerError>)],
    name: &str,
    expected: &[&str],
) -> Vec<String> {
    let mut wrong = Vec::new();
    for (path, result) in paths {
        match result {
            Ok(columns) => {
                let values = columns.get(name).expect("column in the result");
                if values != expected {
                    wrong.push(format!("{path} {name}: {values:?}, expected {expected:?}"));
                }
            }
            Err(error) => wrong.push(format!("{path} {name}: {error}")),
        }
    }
    wrong
}

/// What is wrong with each path's error for an undecodable `column` of a type
/// whose name contains `type_name`.
fn error_wrongs(
    paths: &[(&'static str, Result<QueryColumns, DataProfilerError>)],
    column: &str,
    type_name: &str,
) -> Vec<String> {
    let mut wrong = Vec::new();
    for (path, result) in paths {
        match result {
            Ok(columns) => wrong.push(format!(
                "{path} {type_name}: decoded as {:?}, expected an error",
                columns.get(column)
            )),
            Err(DataProfilerError::DatabaseQueryError { message }) => {
                let named = message.contains(&format!("column \"{column}\" has type "))
                    && message.to_lowercase().contains(&type_name.to_lowercase())
                    && message.contains("cast it to text in the query");
                if !named {
                    wrong.push(format!("{path} {type_name}: {message}"));
                }
            }
            Err(other) => wrong.push(format!("{path} {type_name}: {other:?}")),
        }
    }
    wrong
}

#[cfg(feature = "postgres")]
mod postgres {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    /// Created once per process; the tests only read it.
    static FIXTURE: tokio::sync::OnceCell<Option<String>> = tokio::sync::OnceCell::const_new();

    async fn url() -> Option<String> {
        FIXTURE.get_or_init(build).await.clone()
    }

    async fn build() -> Option<String> {
        let url = std::env::var("POSTGRES_TEST_URL").ok()?;
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("postgres connection");
        for statement in [
            "DROP TABLE IF EXISTS unsupported_types_pg",
            "DROP TYPE IF EXISTS mood_952",
            "DROP TYPE IF EXISTS pair_952",
            "CREATE TYPE mood_952 AS ENUM ('ok', 'sad')",
            "CREATE TYPE pair_952 AS (a INTEGER, b TEXT)",
            "CREATE TABLE unsupported_types_pg (id INTEGER, mood mood_952, raw BYTEA)",
            r"INSERT INTO unsupported_types_pg VALUES
                (1, 'ok', '\x01'), (2, 'sad', '\x0203'), (3, NULL, NULL)",
        ] {
            sqlx::query(statement)
                .execute(&pool)
                .await
                .expect("fixture");
        }
        Some(url)
    }

    #[tokio::test]
    async fn enum_labels_decode_with_their_nulls_on_both_paths() {
        let Some(url) = url().await else { return };
        let paths = both_paths(&url, "SELECT mood FROM unsupported_types_pg ORDER BY id").await;
        let wrong = value_wrongs(&paths, "mood", &["ok", "sad", ""]);
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    #[tokio::test]
    async fn an_enum_column_profiles_its_labels_and_real_null_count() {
        let Some(url) = url().await else { return };
        let report = dataprof_db::analyze_database(
            config(&url),
            "SELECT mood FROM unsupported_types_pg",
            true,
            None,
        )
        .await
        .expect("profile");
        let column = report
            .column_profiles
            .iter()
            .find(|column| column.name == "mood")
            .expect("mood column");
        assert_eq!((column.total_count, column.null_count), (3, 1));
        assert_eq!(column.unique_count, Some(2));
    }

    #[tokio::test]
    async fn values_of_types_without_a_decoder_fail_on_both_paths() {
        let Some(url) = url().await else { return };
        let mut wrong = Vec::new();
        for (expression, type_name) in [
            ("interval '1 day'", "interval"),
            ("inet '10.0.0.1'", "inet"),
            ("12.5::money", "money"),
            ("point(1, 2)", "point"),
            ("'a b'::tsvector", "tsvector"),
            ("'<a/>'::xml", "xml"),
            ("int4range(1, 5)", "int4range"),
            ("ROW(1, 'x')::pair_952", "pair_952"),
            ("B'101'", "bit"),
        ] {
            let query = format!("SELECT {expression} AS v");
            wrong.extend(error_wrongs(
                &both_paths(&url, &query).await,
                "v",
                type_name,
            ));
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    #[tokio::test]
    async fn a_null_of_a_type_without_a_decoder_still_reads_as_null() {
        let Some(url) = url().await else { return };
        let paths = both_paths(&url, "SELECT NULL::interval AS v").await;
        let wrong = value_wrongs(&paths, "v", &[""]);
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    #[tokio::test]
    async fn bytea_still_reads_as_null() {
        let Some(url) = url().await else { return };
        let paths = both_paths(&url, "SELECT raw FROM unsupported_types_pg ORDER BY id").await;
        let wrong = value_wrongs(&paths, "raw", &["", "", ""]);
        assert!(wrong.is_empty(), "{wrong:#?}");
    }
}

#[cfg(feature = "mysql")]
mod mysql {
    use super::*;
    use sqlx::mysql::MySqlPoolOptions;

    static FIXTURE: tokio::sync::OnceCell<Option<String>> = tokio::sync::OnceCell::const_new();

    async fn url() -> Option<String> {
        FIXTURE.get_or_init(build).await.clone()
    }

    async fn build() -> Option<String> {
        let url = std::env::var("MYSQL_TEST_URL").ok()?;
        let pool = MySqlPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("mysql connection");
        for statement in [
            "DROP TABLE IF EXISTS unsupported_types_my",
            "CREATE TABLE unsupported_types_my (
                id INTEGER, blob_v BLOB, varbinary_v VARBINARY(4), binary_v BINARY(2),
                geometry_v GEOMETRY
            )",
            "INSERT INTO unsupported_types_my VALUES
                (1, x'01', x'0203', x'0405', ST_GeomFromText('POINT(1 2)')),
                (2, NULL, NULL, NULL, NULL)",
        ] {
            sqlx::query(statement)
                .execute(&pool)
                .await
                .expect("fixture");
        }
        Some(url)
    }

    #[tokio::test]
    async fn binary_columns_still_read_as_null() {
        let Some(url) = url().await else { return };
        let paths = both_paths(
            &url,
            "SELECT blob_v, varbinary_v, binary_v FROM unsupported_types_my ORDER BY id",
        )
        .await;
        let mut wrong = Vec::new();
        for name in ["blob_v", "varbinary_v", "binary_v"] {
            wrong.extend(value_wrongs(&paths, name, &["", ""]));
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    #[tokio::test]
    async fn a_geometry_value_fails_on_both_paths() {
        let Some(url) = url().await else { return };
        let paths = both_paths(
            &url,
            "SELECT geometry_v FROM unsupported_types_my ORDER BY id",
        )
        .await;
        let wrong = error_wrongs(&paths, "geometry_v", "geometry");
        assert!(wrong.is_empty(), "{wrong:#?}");
    }
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use super::*;

    #[tokio::test]
    async fn a_blob_still_reads_as_null() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("blob.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        let pool = sqlx::SqlitePool::connect(&url).await.expect("sqlite");
        for statement in [
            "CREATE TABLE blobs (id INTEGER, raw BLOB)",
            "INSERT INTO blobs VALUES (1, x'0102'), (2, NULL)",
        ] {
            sqlx::query(statement)
                .execute(&pool)
                .await
                .expect("fixture");
        }
        pool.close().await;
        let paths = both_paths(&url, "SELECT raw FROM blobs ORDER BY id").await;
        let wrong = value_wrongs(&paths, "raw", &["", ""]);
        assert!(wrong.is_empty(), "{wrong:#?}");
    }
}
