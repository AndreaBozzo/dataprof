//! Common missing-value markers count as nulls (#813).
//!
//! Only empty, `null` and `nan` used to be nulls, so `NA` (R), `#N/A` (Excel),
//! `\N` (MySQL and PostgreSQL text dumps), `None` (Python's `str(None)`) and
//! `N/A`/`n/a` were counted as values: completeness read 100% on columns that
//! were half missing, and a numeric column with markers in it was typed text.
//! The vocabulary is now `NullTokenSet::CommonMarkers`, recorded in each
//! report's metric semantics, and is the same on every engine and input path.
//!
//! `python/tests/test_null_markers.py` is the Python twin.

use std::io::Write;

use dataprof::{
    ColumnProfile, ColumnStats, CsvParserConfig, DataType, EngineType, NullTokenSet, ProfileReport,
    Profiler, analyze_column, analyze_csv_file,
};
use serde_json::json;
use tempfile::NamedTempFile;

/// Ten rows. `na` and `mysql` are missing on the even rows, each marker
/// spelled the way its writer spells it. `txt` holds look-alikes that are
/// values: sodium, an answer, and placeholders that are data in some files.
const ROWS: [(&str, &str, &str); 10] = [
    ("1", "a", "Na"),
    ("NA", "\\N", "none"),
    ("3", "b", "-"),
    ("N/A", "\\N", "?"),
    ("5", "c", "x"),
    ("#N/A", "\\N", "y"),
    ("7", "d", "n.d."),
    ("n/a", "\\N", "z"),
    ("9", "e", "NONE"),
    ("None", "\\N", "w"),
];

fn csv_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "na,mysql,txt").unwrap();
    for (na, mysql, txt) in ROWS {
        writeln!(file, "{na},{mysql},{txt}").unwrap();
    }
    file.flush().unwrap();
    file
}

fn json_records() -> Vec<serde_json::Value> {
    ROWS.iter()
        .map(|(na, mysql, txt)| json!({"na": na, "mysql": mysql, "txt": txt}))
        .collect()
}

fn json_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".json").unwrap();
    serde_json::to_writer(&mut file, &json_records()).unwrap();
    file.flush().unwrap();
    file
}

fn jsonl_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".jsonl").unwrap();
    for record in json_records() {
        writeln!(file, "{record}").unwrap();
    }
    file.flush().unwrap();
    file
}

#[cfg(feature = "parquet")]
fn parquet_fixture() -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("na", DataType::Utf8, false),
        Field::new("mysql", DataType::Utf8, false),
        Field::new("txt", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(ROWS.map(|row| row.0).to_vec())),
            Arc::new(StringArray::from(ROWS.map(|row| row.1).to_vec())),
            Arc::new(StringArray::from(ROWS.map(|row| row.2).to_vec())),
        ],
    )
    .unwrap();

    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

fn reports() -> Vec<(String, ProfileReport)> {
    let csv = csv_fixture();
    let mut reports = vec![(
        "standard".to_string(),
        analyze_csv_file(csv.path(), &CsvParserConfig::default()).expect("standard CSV"),
    )];
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
        reports.push((format!("{engine:?}"), report));
    }
    let mut files = vec![("json", json_fixture()), ("jsonl", jsonl_fixture())];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_fixture()));
    for (label, file) in files {
        let report = Profiler::new()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    reports
}

fn column<'a>(report: &'a ProfileReport, name: &str, label: &str) -> &'a ColumnProfile {
    report
        .column_profiles
        .iter()
        .find(|column| column.name == name)
        .unwrap_or_else(|| panic!("[{label}] {name}"))
}

fn assert_numeric_with_markers(profile: &ColumnProfile, label: &str) {
    assert_eq!(profile.null_count, 5, "[{label}]");
    assert_eq!(profile.data_type, DataType::Integer, "[{label}]");
    let ColumnStats::Numeric(stats) = &profile.stats else {
        panic!("[{label}] na has no numeric stats: {:?}", profile.stats);
    };
    assert_eq!(
        (stats.min, stats.max, stats.mean),
        (1.0, 9.0, 5.0),
        "[{label}]"
    );
}

#[test]
fn every_path_counts_the_markers_as_nulls() {
    for (label, report) in reports() {
        assert_numeric_with_markers(column(&report, "na", &label), &label);
        assert_eq!(column(&report, "mysql", &label).null_count, 5, "[{label}]");
        assert_eq!(column(&report, "txt", &label).null_count, 0, "[{label}]");

        let completeness = report
            .quality
            .as_ref()
            .and_then(|quality| quality.metrics.completeness.as_ref())
            .unwrap_or_else(|| panic!("[{label}] completeness assessed"));
        // The markers share rows, so half the records are complete and a
        // third of the cells are missing.
        assert_eq!(completeness.complete_records_ratio, 50.0, "[{label}]");
        assert!(
            (completeness.missing_values_ratio - 100.0 / 3.0).abs() < 1e-9,
            "[{label}] {}",
            completeness.missing_values_ratio
        );
    }
}

/// The database connectors and in-memory inputs profile through
/// `analyze_column`.
#[test]
fn column_analysis_counts_them_too() {
    let values: Vec<String> = ROWS.iter().map(|row| row.0.to_string()).collect();
    assert_numeric_with_markers(&analyze_column("na", &values), "analyze_column");
}

#[test]
fn every_report_records_the_vocabulary() {
    for (label, report) in reports() {
        let semantics = report
            .metric_semantics
            .as_ref()
            .unwrap_or_else(|| panic!("[{label}] semantics recorded"));
        assert_eq!(
            semantics.null_tokens,
            Some(NullTokenSet::CommonMarkers),
            "[{label}]"
        );

        let document = serde_json::to_value(&report).expect("serializes");
        assert_eq!(
            document["metric_semantics"]["null_tokens"],
            json!("common_markers"),
            "[{label}]"
        );
        let restored: ProfileReport = serde_json::from_value(document).expect("reads back");
        assert_eq!(
            restored.metric_semantics.and_then(|s| s.null_tokens),
            Some(NullTokenSet::CommonMarkers),
            "[{label}]"
        );
    }
}

/// A 0.12 report recorded its text-length unit but not its null vocabulary,
/// which was smaller. It reads back with the vocabulary unknown.
#[test]
fn a_report_without_the_vocabulary_reads_back_unknown() {
    let csv = csv_fixture();
    let report = analyze_csv_file(csv.path(), &CsvParserConfig::default()).unwrap();
    let mut document = serde_json::to_value(&report).unwrap();
    document["metric_semantics"]
        .as_object_mut()
        .unwrap()
        .remove("null_tokens");
    let restored: ProfileReport = serde_json::from_value(document).unwrap();
    let semantics = restored.metric_semantics.expect("still recorded");
    assert_eq!(semantics.null_tokens, None);
    assert!(semantics.text_length_unit.is_some());

    let mut document = serde_json::to_value(&report).unwrap();
    document["metric_semantics"]["null_tokens"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<ProfileReport>(document).is_err());
}
