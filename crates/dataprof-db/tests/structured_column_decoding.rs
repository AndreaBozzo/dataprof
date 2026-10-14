//! JSON, JSONB and array columns decode instead of reading as null (#766).
//!
//! None of them decodes as `String` through sqlx, so every value fell past
//! `db_column_to_string!` and was recorded as SQL NULL: a column of real JSON
//! documents was reported as entirely missing. These tests pin three parts of
//! the fix separately, so a failing run says which one regressed:
//!
//! - the values decode, on the batch and streaming paths of both connectors;
//! - a column of containers is reported as nested with its real null count,
//!   as the JSON readers report the same records;
//! - JSON objects render with sorted keys, so key order does not split
//!   duplicate rows (#767).
//!
//! They need the CI database services (`POSTGRES_TEST_URL`, `MYSQL_TEST_URL`)
//! and skip without them, like the other database tests.

#![cfg(any(feature = "postgres", feature = "mysql"))]

use dataprof_core::DataType;
use dataprof_db::{DatabaseConfig, QueryColumns};
use dataprof_runtime::ProfileReport;

fn config(url: &str) -> DatabaseConfig {
    DatabaseConfig {
        connection_string: url.to_string(),
        load_credentials_from_env: false,
        ..Default::default()
    }
}

/// The query read through the connector's batch and streaming paths.
async fn both_paths(url: &str, query: &str) -> [(&'static str, QueryColumns); 2] {
    let mut connector = dataprof_db::create_connector(config(url)).expect("connector");
    connector.connect().await.expect("connect");
    let batch = connector.profile_query(query).await.expect("batch query");
    let streaming = connector
        .profile_query_streaming(query, 1)
        .await
        .expect("streaming query");
    connector.disconnect().await.expect("disconnect");
    [("batch", batch), ("streaming", streaming)]
}

async fn profile(url: &str, query: &str) -> ProfileReport {
    dataprof_db::analyze_database(config(url), query, true, None)
        .await
        .expect("profile")
}

/// Every structured column holds two values and a NULL: the values must come
/// back non-empty, the NULL empty, and the containers counted.
fn decoding_wrongs(
    paths: &[(&'static str, QueryColumns)],
    containers: &[(&str, usize)],
) -> Vec<String> {
    let mut wrong = Vec::new();
    for (path, columns) in paths {
        let counts = columns.container_counts();
        for &(name, expected) in containers {
            let values = columns.get(name).expect("column in the result");
            let decoded: Vec<bool> = values.iter().map(|value| !value.is_empty()).collect();
            if decoded != [true, true, false] {
                wrong.push(format!("{path} {name}: values {values:?}"));
            }
            let counted = counts.get(name).copied().unwrap_or(0);
            if counted != expected {
                wrong.push(format!(
                    "{path} {name}: {counted} containers, expected {expected}"
                ));
            }
        }
    }
    wrong
}

/// Container columns must be nested with one null in three rows; scalar JSON
/// columns keep the type of their values.
fn profile_wrongs(report: &ProfileReport, expected: &[(&str, DataType)]) -> Vec<String> {
    let mut wrong = Vec::new();
    for &(name, ref data_type) in expected {
        let column = report
            .column_profiles
            .iter()
            .find(|column| column.name == name)
            .expect("profiled column");
        if column.data_type != *data_type || column.null_count != 1 || column.total_count != 3 {
            wrong.push(format!(
                "{name}: {:?} with {} nulls in {} rows, expected {data_type:?} with 1 in 3",
                column.data_type, column.null_count, column.total_count
            ));
        }
    }
    wrong
}

#[cfg(feature = "postgres")]
mod postgres {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    const TABLE: &str = "SELECT * FROM structured_decoding_pg ORDER BY id";

    fn duplicate_rows(report: &ProfileReport) -> Option<usize> {
        report
            .quality
            .as_ref()
            .and_then(|quality| quality.metrics.uniqueness.as_ref())
            .map(|uniqueness| uniqueness.duplicate_rows)
    }

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
            "DROP TABLE IF EXISTS structured_decoding_pg",
            "CREATE TABLE structured_decoding_pg (
                id INTEGER, doc JSONB, doc_text JSON, tags INTEGER[], labels TEXT[],
                scalars JSONB
            )",
            r#"INSERT INTO structured_decoding_pg VALUES
                (1, '{"a":1,"b":2}', '{"b":2,"a":1}', '{1,2}', '{x,y}', '5'),
                (2, '{"a":2}', '[1,{"z":0,"y":1}]', '{3}', '{z,NULL}', '7'),
                (3, NULL, NULL, NULL, NULL, NULL)"#,
        ] {
            sqlx::query(statement)
                .execute(&pool)
                .await
                .expect("fixture");
        }
        Some(url)
    }

    #[tokio::test]
    async fn json_jsonb_and_array_values_decode_on_both_paths() {
        let Some(url) = url().await else {
            return;
        };
        let paths = both_paths(&url, TABLE).await;

        let mut wrong = decoding_wrongs(
            &paths,
            &[
                ("doc", 2),
                ("doc_text", 2),
                ("tags", 2),
                ("labels", 2),
                ("scalars", 0),
            ],
        );
        for (path, columns) in &paths {
            // A JSONB scalar renders as itself, as a JSONL scalar does.
            if columns["scalars"] != ["5", "7", ""] {
                wrong.push(format!("{path} scalars: {:?}", columns["scalars"]));
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    #[tokio::test]
    async fn container_columns_profile_as_nested_with_their_real_nulls() {
        let Some(url) = url().await else {
            return;
        };
        let report = profile(&url, TABLE).await;

        let wrong = profile_wrongs(
            &report,
            &[
                ("doc", DataType::Nested),
                ("doc_text", DataType::Nested),
                ("tags", DataType::Nested),
                ("labels", DataType::Nested),
                ("scalars", DataType::Integer),
            ],
        );
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    /// `json` keeps the source key order, so the first two documents arrive
    /// with their keys in different orders and must still compare equal. The
    /// third differs from them only in its document, so it is not a duplicate.
    #[tokio::test]
    async fn json_key_order_does_not_split_duplicate_rows() {
        let Some(url) = url().await else {
            return;
        };
        // `n` fixes the row order, which `VALUES` alone does not promise.
        let query = r#"SELECT id, j FROM (VALUES
            (1, 1, '{"x":1,"y":[{"q":1,"p":2}]}'::json),
            (2, 1, '{"y":[{"p":2,"q":1}],"x":1}'::json),
            (3, 1, '{"x":2}'::json)) AS t(n, id, j) ORDER BY n"#;

        let mut wrong = Vec::new();
        for (path, columns) in both_paths(&url, query).await {
            if columns["j"][0] != columns["j"][1] {
                wrong.push(format!("{path}: {:?}", columns["j"]));
            }
        }
        let duplicates = duplicate_rows(&profile(&url, query).await);
        if duplicates != Some(1) {
            wrong.push(format!(
                "profile: {duplicates:?} duplicate rows, expected 1"
            ));
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    /// PostgreSQL accepts numbers serde_json cannot hold. Such a document keeps
    /// the database's digits instead of falling back to null, and its keys are
    /// still sorted: the two rows differ only in key order, whitespace and
    /// string escapes, so they are duplicates.
    #[tokio::test]
    async fn documents_serde_json_cannot_hold_keep_their_text() {
        let Some(url) = url().await else {
            return;
        };
        let query = r#"SELECT j, b, s FROM (VALUES
            (1, '{"a": 1e400, "b": [2, {"d": "A", "c": 0}]}'::json, '[1e400]'::jsonb,
             '1e400'::jsonb),
            (2, '{"b":[2,{"c":0,"d":"\u0041"}],"a":1e400}'::json, '[1e400]'::jsonb,
             '1e400'::jsonb)) AS t(n, j, b, s) ORDER BY n"#;

        // `json` keeps the input text, here made canonical; `jsonb` stores an
        // exact numeric and prints every digit.
        let digits = format!("1{}", "0".repeat(400));
        let expected = [
            r#"{"a":1e400,"b":[2,{"c":0,"d":"A"}]}"#.to_string(),
            format!("[{digits}]"),
            digits.clone(),
        ];

        let mut wrong = Vec::new();
        for (path, columns) in both_paths(&url, query).await {
            for row in 0..2 {
                let texts = [&columns["j"][row], &columns["b"][row], &columns["s"][row]];
                if texts != expected.each_ref() {
                    wrong.push(format!("{path} row {row}: {texts:?}"));
                }
            }
            let counts = columns.container_counts();
            if counts.get("j") != Some(&2)
                || counts.get("b") != Some(&2)
                || counts.contains_key("s")
            {
                wrong.push(format!("{path}: containers {counts:?}"));
            }
        }
        let duplicates = duplicate_rows(&profile(&url, query).await);
        if duplicates != Some(1) {
            wrong.push(format!(
                "profile: {duplicates:?} duplicate rows, expected 1"
            ));
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    /// The same records as a JSONL file report the same container columns.
    #[tokio::test]
    async fn the_query_and_a_jsonl_file_of_its_rows_agree() {
        let Some(url) = url().await else {
            return;
        };
        let jsonl = concat!(
            r#"{"id":1,"doc":{"a":1,"b":2},"doc_text":{"b":2,"a":1},"tags":[1,2],"labels":["x","y"],"scalars":5}"#,
            "\n",
            r#"{"id":2,"doc":{"a":2},"doc_text":[1,{"z":0,"y":1}],"tags":[3],"labels":["z",null],"scalars":7}"#,
            "\n",
            r#"{"id":3,"doc":null,"doc_text":null,"tags":null,"labels":null,"scalars":null}"#,
            "\n",
        );
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("rows.jsonl");
        std::fs::write(&path, jsonl).expect("write jsonl");
        let file_report =
            dataprof_json::analyze_json_file(&path, &dataprof_json::JsonParserConfig::jsonl())
                .expect("jsonl profile");
        let query_report = profile(&url, TABLE).await;

        let mut wrong = Vec::new();
        for name in ["doc", "doc_text", "tags", "labels", "scalars"] {
            let [query, file] = [&query_report, &file_report].map(|report| {
                let column = report
                    .column_profiles
                    .iter()
                    .find(|column| column.name == name)
                    .expect("profiled column");
                (
                    column.data_type.clone(),
                    column.null_count,
                    column.total_count,
                )
            });
            if query != file {
                wrong.push(format!("{name}: query {query:?}, jsonl {file:?}"));
            }
        }
        if duplicate_rows(&query_report) != duplicate_rows(&file_report) {
            wrong.push(format!(
                "duplicate rows: query {:?}, jsonl {:?}",
                duplicate_rows(&query_report),
                duplicate_rows(&file_report)
            ));
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }
}

#[cfg(feature = "mysql")]
mod mysql {
    use super::*;
    use sqlx::mysql::MySqlPoolOptions;

    const TABLE: &str = "SELECT * FROM structured_decoding_my ORDER BY id";

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
            "DROP TABLE IF EXISTS structured_decoding_my",
            "CREATE TABLE structured_decoding_my (id INT, doc JSON, list JSON, scalars JSON)",
            r#"INSERT INTO structured_decoding_my VALUES
                (1, '{"b":2,"a":1}', '[1,2]', '5'),
                (2, '{"a":2}', '[]', '7'),
                (3, NULL, NULL, NULL)"#,
        ] {
            sqlx::query(statement)
                .execute(&pool)
                .await
                .expect("fixture");
        }
        Some(url)
    }

    #[tokio::test]
    async fn json_values_decode_on_both_paths() {
        let Some(url) = url().await else {
            return;
        };
        let paths = both_paths(&url, TABLE).await;

        let mut wrong = decoding_wrongs(&paths, &[("doc", 2), ("list", 2), ("scalars", 0)]);
        for (path, columns) in &paths {
            if columns["doc"][0] != r#"{"a":1,"b":2}"# {
                wrong.push(format!("{path} doc: {:?}", columns["doc"]));
            }
            if columns["scalars"] != ["5", "7", ""] {
                wrong.push(format!("{path} scalars: {:?}", columns["scalars"]));
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    #[tokio::test]
    async fn container_columns_profile_as_nested_with_their_real_nulls() {
        let Some(url) = url().await else {
            return;
        };
        let report = profile(&url, TABLE).await;

        let wrong = profile_wrongs(
            &report,
            &[
                ("doc", DataType::Nested),
                ("list", DataType::Nested),
                ("scalars", DataType::Integer),
            ],
        );
        assert!(wrong.is_empty(), "{wrong:#?}");
    }
}
