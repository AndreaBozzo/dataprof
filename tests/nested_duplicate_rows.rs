//! Duplicate rows holding a nested value (#767).
//!
//! A row signature renders each field as text. The JSON readers rendered an
//! object in its source key order, so two equal records whose keys came in a
//! different order were not duplicates. Arrow and Parquet rendered a nested
//! value as its display string, which does not quote strings or show a null
//! list element, so `["p, q"]` matched `["p", "q"]` and `[null]` matched `[]`.
//!
//! `python/tests/test_nested_duplicate_rows.py` is the Python twin.

use std::io::Write;

use dataprof::{ProfileReport, Profiler};
use tempfile::NamedTempFile;

/// JSON Lines records, and the duplicate rows they hold. `arrow` is false
/// where Arrow cannot hold the records in one column type.
struct Case {
    name: &'static str,
    records: &'static str,
    duplicates: usize,
    arrow: bool,
}

const CASES: [Case; 6] = [
    Case {
        name: "key order",
        records: r#"{"id": 1, "a": {"x": 1, "y": 2}}
{"id": 1, "a": {"y": 2, "x": 1}}"#,
        duplicates: 1,
        arrow: true,
    },
    Case {
        name: "nested key order",
        records: r#"{"id": 1, "a": {"b": {"x": 1, "y": [1, 2]}, "c": "k"}}
{"id": 1, "a": {"c": "k", "b": {"y": [1, 2], "x": 1}}}"#,
        duplicates: 1,
        arrow: true,
    },
    Case {
        name: "comma inside a list element",
        records: r#"{"id": 1, "tags": ["p, q"]}
{"id": 1, "tags": ["p", "q"]}"#,
        duplicates: 0,
        arrow: true,
    },
    Case {
        name: "null element against an empty list",
        records: r#"{"id": 1, "tags": ["p"]}
{"id": 2, "tags": [null]}
{"id": 2, "tags": []}"#,
        duplicates: 0,
        arrow: true,
    },
    Case {
        name: "null child repeated",
        records: r#"{"id": 1, "a": {"x": null, "y": "1"}}
{"id": 1, "a": {"x": null, "y": "1"}}
{"id": 1, "a": {"x": "1", "y": null}}"#,
        duplicates: 1,
        arrow: true,
    },
    Case {
        name: "empty object against an empty list",
        records: r#"{"id": 1, "a": {}}
{"id": 1, "a": []}"#,
        duplicates: 0,
        arrow: false,
    },
];

fn file(suffix: &str, contents: &[u8]) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    file.write_all(contents).unwrap();
    file.flush().unwrap();
    file
}

fn json_array(records: &str) -> String {
    format!("[{}]", records.lines().collect::<Vec<_>>().join(","))
}

/// The records as Parquet, typed by Arrow's own JSON schema inference.
#[cfg(feature = "parquet")]
fn parquet(records: &str) -> NamedTempFile {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::json::ReaderBuilder;
    use arrow::json::reader::infer_json_schema;
    use parquet::arrow::ArrowWriter;

    let (schema, _) = infer_json_schema(Cursor::new(records), None).unwrap();
    let schema = Arc::new(schema);
    let reader = ReaderBuilder::new(schema.clone())
        .build(Cursor::new(records))
        .unwrap();
    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    for batch in reader {
        writer.write(&batch.unwrap()).unwrap();
    }
    writer.close().unwrap();
    file
}

fn duplicates(report: &ProfileReport) -> Option<usize> {
    report
        .quality
        .as_ref()
        .and_then(|quality| quality.metrics.uniqueness.as_ref())
        .map(|uniqueness| uniqueness.duplicate_rows)
}

/// Every wrong (case, route) at once, so one run shows which routes each
/// part of the fix guards.
#[test]
fn equal_nested_rows_are_duplicates_on_every_file_route() {
    let mut wrong = Vec::new();
    for case in &CASES {
        let mut routes = vec![
            ("jsonl", file(".jsonl", case.records.as_bytes())),
            ("json", file(".json", json_array(case.records).as_bytes())),
        ];
        #[cfg(feature = "parquet")]
        if case.arrow {
            routes.push(("parquet", parquet(case.records)));
        }
        for (route, source) in routes {
            let report = Profiler::new()
                .analyze_file(source.path())
                .unwrap_or_else(|e| panic!("[{} / {route}] {e}", case.name));
            let found = duplicates(&report);
            if found != Some(case.duplicates) {
                wrong.push(format!(
                    "{} / {route}: {found:?}, expected {}",
                    case.name, case.duplicates
                ));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "wrong duplicate counts:\n{}",
        wrong.join("\n")
    );
}

#[cfg(feature = "async-streaming")]
#[tokio::test]
async fn equal_nested_rows_are_duplicates_on_the_async_route() {
    use dataprof::{AsyncSourceInfo, BytesSource, FileFormat};

    let mut wrong = Vec::new();
    for case in &CASES {
        let source = BytesSource::new(
            bytes::Bytes::from(case.records.to_string()),
            AsyncSourceInfo::new(case.name, FileFormat::Jsonl)
                .size_hint(Some(case.records.len() as u64)),
        );
        let report = Profiler::new()
            .profile_stream(source)
            .await
            .unwrap_or_else(|e| panic!("[{} / async] {e}", case.name));
        let found = duplicates(&report);
        if found != Some(case.duplicates) {
            wrong.push(format!(
                "{} / async: {found:?}, expected {}",
                case.name, case.duplicates
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "wrong duplicate counts:\n{}",
        wrong.join("\n")
    );
}
