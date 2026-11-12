//! Which columns the quality score's value checks skipped (#1134).
//!
//! A struct, list or map column is counted and nothing more (#637): its nulls
//! count toward completeness and its values toward duplicate rows, but no
//! other check reads them. A score of 100 over a source whose payload is all
//! in such columns said nothing about that. The report now names them under
//! `quality.columns_without_value_checks`, the same on every input route.
//!
//! `python/tests/test_columns_without_value_checks.py` is the Python twin.

use std::io::Write;

use dataprof::{ColumnWithoutValueChecks, ProfileReport, Profiler, ValueChecksSkipReason};
use tempfile::NamedTempFile;

/// The issue's fixture: `props.mag` is null in half the rows and
/// `props.place` is empty in all of them, yet the score was 100.
fn records() -> Vec<String> {
    (0..50)
        .map(|i| {
            let mag = if i % 2 == 0 { "1.5" } else { "null" };
            format!(
                r#"{{"id": {i}, "props": {{"mag": {mag}, "place": ""}}, "geom": {{"coords": [0, 0]}}}}"#
            )
        })
        .collect()
}

fn file(suffix: &str, contents: &str) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

fn jsonl() -> String {
    records().join("\n") + "\n"
}

fn json_array() -> String {
    format!("[{}]", records().join(","))
}

/// The records as Parquet, typed by Arrow's own JSON schema inference.
#[cfg(feature = "parquet")]
fn parquet() -> NamedTempFile {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::json::ReaderBuilder;
    use arrow::json::reader::infer_json_schema;
    use parquet::arrow::ArrowWriter;

    let text = jsonl();
    let (schema, _) = infer_json_schema(Cursor::new(&text), None).unwrap();
    let schema = Arc::new(schema);
    let reader = ReaderBuilder::new(schema.clone())
        .build(Cursor::new(&text))
        .unwrap();
    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    for batch in reader {
        writer.write(&batch.unwrap()).unwrap();
    }
    writer.close().unwrap();
    file
}

fn expected() -> Vec<ColumnWithoutValueChecks> {
    ["props", "geom"]
        .into_iter()
        .map(|column| ColumnWithoutValueChecks {
            column: column.to_string(),
            reason: ValueChecksSkipReason::Nested,
        })
        .collect()
}

/// What each report dialect serializes for the field.
fn serialized(report: &ProfileReport) -> (serde_json::Value, serde_json::Value) {
    let canonical = serde_json::to_value(report).unwrap();
    let summary: serde_json::Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    (
        canonical["quality"]["columns_without_value_checks"].clone(),
        summary["quality"]["columns_without_value_checks"].clone(),
    )
}

fn check(route: &str, report: &ProfileReport, wrong: &mut Vec<String>) {
    let quality = report.quality.as_ref().expect("quality is computed");
    let found = quality.columns_without_value_checks();
    if found != Some(expected().as_slice()) {
        wrong.push(format!("{route}: {found:?}"));
    }
    let expected_json = serde_json::json!([
        {"column": "props", "reason": "nested"},
        {"column": "geom", "reason": "nested"},
    ]);
    let (canonical, summary) = serialized(report);
    if canonical != expected_json || summary != expected_json {
        wrong.push(format!("{route}: serialized {canonical} / {summary}"));
    }
}

/// Every wrong route at once, so one run shows which routes each part of the
/// change guards.
#[test]
fn nested_columns_are_named_on_every_file_route() {
    let mut routes = vec![
        ("jsonl", file(".jsonl", &jsonl())),
        ("json", file(".json", &json_array())),
    ];
    #[cfg(feature = "parquet")]
    routes.push(("parquet", parquet()));

    let mut wrong = Vec::new();
    for (route, source) in routes {
        let report = Profiler::new()
            .analyze_file(source.path())
            .unwrap_or_else(|e| panic!("[{route}] {e}"));
        check(route, &report, &mut wrong);
    }
    assert!(wrong.is_empty(), "wrong lists:\n{}", wrong.join("\n"));
}

/// The async route, unsampled and sampled. A reservoir sample takes the
/// assembler's other branch, which files each dimension as exact or sampled.
#[cfg(feature = "async-streaming")]
#[tokio::test]
async fn nested_columns_are_named_on_the_async_route() {
    use dataprof::{AsyncSourceInfo, BytesSource, FileFormat, MetricConfidence, SamplingStrategy};

    let text = jsonl();
    let mut wrong = Vec::new();
    for (route, sampling) in [
        ("async jsonl", SamplingStrategy::None),
        (
            "async jsonl sampled",
            SamplingStrategy::Reservoir { size: 10 },
        ),
    ] {
        let sampled = !matches!(sampling, SamplingStrategy::None);
        let source = BytesSource::new(
            bytes::Bytes::from(text.clone()),
            AsyncSourceInfo::new("records", FileFormat::Jsonl).size_hint(Some(text.len() as u64)),
        );
        let report = Profiler::new()
            .sampling(sampling)
            .profile_stream(source)
            .await
            .unwrap_or_else(|e| panic!("[{route}] {e}"));
        let mixed = matches!(
            report.quality.as_ref().map(|quality| &quality.confidence),
            Some(MetricConfidence::Mixed { .. })
        );
        assert_eq!(mixed, sampled, "[{route}] took the wrong assembler branch");
        check(route, &report, &mut wrong);
    }
    assert!(wrong.is_empty(), "wrong lists:\n{}", wrong.join("\n"));
}

/// Every column checked is an empty list, which a reader tells apart from a
/// report that does not say.
#[test]
fn a_source_without_nested_columns_records_an_empty_list() {
    let csv = file(".csv", "id,name\n1,a\n2,b\n");
    let report = Profiler::new().analyze_file(csv.path()).unwrap();
    let quality = report.quality.as_ref().unwrap();
    assert_eq!(quality.columns_without_value_checks(), Some(&[][..]));
    let (canonical, summary) = serialized(&report);
    assert_eq!(canonical, serde_json::json!([]));
    assert_eq!(summary, serde_json::json!([]));
}

/// A document written before 0.14 does not record the list, so it reads as
/// unknown. An explicit null or an unknown reason is malformed.
#[test]
fn the_field_round_trips_and_its_absence_is_kept() {
    let source = file(".jsonl", &jsonl());
    let report = Profiler::new().analyze_file(source.path()).unwrap();
    let document = serde_json::to_value(&report).unwrap();

    let restored: ProfileReport = serde_json::from_value(document.clone()).unwrap();
    assert_eq!(
        restored
            .quality
            .as_ref()
            .unwrap()
            .columns_without_value_checks(),
        Some(expected().as_slice())
    );

    let mut older = document.clone();
    older["quality"]
        .as_object_mut()
        .unwrap()
        .remove("columns_without_value_checks");
    let older: ProfileReport = serde_json::from_value(older).unwrap();
    let quality = older.quality.as_ref().unwrap();
    assert_eq!(quality.columns_without_value_checks(), None);
    assert!(
        serde_json::to_value(&older).unwrap()["quality"]
            .get("columns_without_value_checks")
            .is_none(),
        "an unrecorded list must stay absent, not become empty"
    );

    for malformed in [
        serde_json::Value::Null,
        serde_json::json!([{"column": "props", "reason": "unknown"}]),
    ] {
        let mut document = document.clone();
        document["quality"]["columns_without_value_checks"] = malformed.clone();
        assert!(
            serde_json::from_value::<ProfileReport>(document).is_err(),
            "{malformed} must not load"
        );
    }
}
