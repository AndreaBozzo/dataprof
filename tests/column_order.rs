//! Column-ordering contract (#465).
//!
//! Columns are reported in source order: for CSV the header order, for Parquet
//! the schema order, and for JSON/JSONL the first record's field order with
//! fields that only appear later appended where they were first seen. Every
//! format and transport must agree, so a format conversion never reshuffles a
//! report.
//!
//! Every fixture uses deliberately non-alphabetical field names: sorting any of
//! them yields `active, amount, date, id`, which is what JSON profiling used to
//! return.

use std::io::Write;

use dataprof::{
    CsvParserConfig, JsonParserConfig, ProfileReport, analyze_csv_file, analyze_json_file,
    analyze_structure, infer_schema,
};
use tempfile::NamedTempFile;

/// Source field order for every fixture below. Alphabetical order differs in
/// all four positions.
const SOURCE_ORDER: [&str; 4] = ["id", "amount", "active", "date"];

const RECORD_ONE: &str = r#"{"id":1,"amount":12.5,"active":true,"date":"2026-07-23"}"#;
const RECORD_TWO: &str = r#"{"id":2,"amount":7.25,"active":false,"date":"2026-07-24"}"#;

fn write_fixture(suffix: &str, contents: &str) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

fn csv_fixture() -> NamedTempFile {
    write_fixture(
        ".csv",
        "id,amount,active,date\n1,12.5,true,2026-07-23\n2,7.25,false,2026-07-24\n",
    )
}

fn json_fixture() -> NamedTempFile {
    write_fixture(".json", &format!("[{RECORD_ONE},{RECORD_TWO}]"))
}

fn jsonl_fixture() -> NamedTempFile {
    write_fixture(".jsonl", &format!("{RECORD_ONE}\n{RECORD_TWO}\n"))
}

fn column_names(report: &ProfileReport) -> Vec<&str> {
    report
        .column_profiles
        .iter()
        .map(|profile| profile.name.as_str())
        .collect()
}

#[test]
fn csv_json_and_jsonl_files_agree_on_column_order() {
    let csv = csv_fixture();
    let json = json_fixture();
    let jsonl = jsonl_fixture();

    let csv_report = analyze_csv_file(csv.path(), &CsvParserConfig::default()).unwrap();
    let json_report = analyze_json_file(json.path(), &JsonParserConfig::default()).unwrap();
    let jsonl_report = analyze_json_file(jsonl.path(), &JsonParserConfig::default()).unwrap();

    assert_eq!(column_names(&csv_report), SOURCE_ORDER, "csv");
    assert_eq!(column_names(&json_report), SOURCE_ORDER, "json");
    assert_eq!(column_names(&jsonl_report), SOURCE_ORDER, "jsonl");
}

#[test]
fn infer_schema_reports_json_columns_in_source_order() {
    for (label, file) in [
        ("csv", csv_fixture()),
        ("json", json_fixture()),
        ("jsonl", jsonl_fixture()),
    ] {
        let schema = infer_schema(file.path()).unwrap();
        let names: Vec<&str> = schema
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(names, SOURCE_ORDER, "{label}");
    }
}

#[test]
fn analyze_structure_reports_json_columns_in_source_order() {
    for (label, file) in [
        ("csv", csv_fixture()),
        ("json", json_fixture()),
        ("jsonl", jsonl_fixture()),
    ] {
        let structure = analyze_structure(file.path(), None).unwrap();
        let names: Vec<&str> = structure
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(names, SOURCE_ORDER, "{label}");
    }
}

/// Fields that only appear in a later record are appended where they were first
/// seen, never folded into alphabetical position.
#[test]
fn late_fields_are_appended_in_first_seen_order() {
    let jsonl = write_fixture(
        ".jsonl",
        "{\"zulu\":1,\"mike\":2}\n{\"zulu\":3,\"mike\":4,\"delta\":5,\"alpha\":6}\n",
    );
    let report = analyze_json_file(jsonl.path(), &JsonParserConfig::default()).unwrap();

    assert_eq!(column_names(&report), ["zulu", "mike", "delta", "alpha"]);
}

#[cfg(feature = "parquet")]
#[test]
fn parquet_agrees_with_csv_and_json_column_order() {
    use std::sync::Arc;

    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("active", DataType::Boolean, false),
        Field::new("date", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Float64Array::from(vec![12.5, 7.25])),
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(StringArray::from(vec!["2026-07-23", "2026-07-24"])),
        ],
    )
    .unwrap();

    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let report = dataprof::analyze_parquet_with_quality(file.path()).unwrap();
    assert_eq!(column_names(&report), SOURCE_ORDER);
}

#[cfg(feature = "async-streaming")]
mod async_transport {
    use super::*;

    use dataprof::{AsyncSourceInfo, BytesSource, FileFormat, Profiler, infer_schema_stream};

    fn source(body: &str, format: FileFormat) -> BytesSource {
        let bytes = bytes::Bytes::from(body.to_string());
        let size = bytes.len() as u64;
        BytesSource::new(
            bytes,
            AsyncSourceInfo::new("column-order", format).size_hint(Some(size)),
        )
    }

    /// The async pipeline registers columns from the streamed records rather
    /// than a header row, so it needs its own ordering guard.
    #[tokio::test]
    async fn async_streaming_preserves_json_source_order() {
        for (label, body, format) in [
            (
                "json",
                format!("[{RECORD_ONE},{RECORD_TWO}]"),
                FileFormat::Json,
            ),
            (
                "jsonl",
                format!("{RECORD_ONE}\n{RECORD_TWO}\n"),
                FileFormat::Jsonl,
            ),
        ] {
            let report = Profiler::new()
                .profile_stream(source(&body, format))
                .await
                .unwrap();
            assert_eq!(column_names(&report), SOURCE_ORDER, "{label}");
        }
    }

    #[tokio::test]
    async fn async_schema_inference_preserves_json_source_order() {
        let body = format!("{RECORD_ONE}\n{RECORD_TWO}\n");
        let schema = infer_schema_stream(source(&body, FileFormat::Jsonl))
            .await
            .unwrap();
        let names: Vec<&str> = schema
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();

        assert_eq!(names, SOURCE_ORDER);
    }
}
