//! An Arrow C stream records `format: arrow`, and the baseline gate reads it as
//! declaring its columns, as it reads a CSV or Parquet source (#1230). A
//! stored stream report without a format keeps the conservative reading.
//!
//! The Arrow C stream route lives in the Python binding, so
//! `python/tests/test_arrow_stream_format.py` covers the reports it writes.
//! This file covers the gate's reading of a stored report, and every file,
//! reader and URL route refusing an `arrow` format override.

use std::io::Write;

use dataprof::{
    CheckCode, DataProfilerError, DataSource, FileFormat, ProfileReport, Profiler, QualityPolicy,
    StopCondition,
};
use dataprof_core::StreamSourceSystem;

const NO_NOTE_CSV: &str = "id,amount\n1,10\n2,20\n3,30\n";

fn file(suffix: &str, contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

/// The outcome of `require_columns(["note"])`: `passed`, `failed` or the
/// reason it was not evaluated.
fn note_outcome(report: &ProfileReport) -> String {
    let result = QualityPolicy::new()
        .require_columns(["note"])
        .evaluate(report)
        .unwrap();
    let check = result
        .checks
        .iter()
        .find(|check| check.code == CheckCode::RequireColumn)
        .unwrap();
    let document = serde_json::to_value(check).unwrap();
    let text = |key: &str| document[key].as_str().unwrap_or_default().to_string();
    match text("status").as_str() {
        "not_evaluated" => format!("{}:{}", text("reason"), text("gap")),
        status => status.to_string(),
    }
}

/// A report capped at one row, re-labelled as the stream source the Arrow C
/// stream route writes, with `format`.
fn capped_stream_report(format: Option<FileFormat>) -> ProfileReport {
    let csv = file(".csv", NO_NOTE_CSV);
    let mut report = Profiler::new()
        .stop_when(StopCondition::MaxRows(1))
        .analyze_file(csv.path())
        .unwrap();
    report.data_source = DataSource::Stream {
        topic: "arrow_stream".to_string(),
        batch_id: "0".to_string(),
        partition: None,
        consumer_group: None,
        source_system: Some(StreamSourceSystem::Custom("arrow_c_stream".to_string())),
        format,
        session_id: None,
        first_record_at: None,
        last_record_at: None,
        csv_metadata: None,
    };
    report
}

#[test]
fn a_capped_arrow_stream_declares_its_columns() {
    let mut wrong = Vec::new();
    for (format, expected) in [
        (Some(FileFormat::Arrow), "failed"),
        (None, "evidence_incomplete:truncated"),
        (Some(FileFormat::Jsonl), "evidence_incomplete:truncated"),
    ] {
        let report = capped_stream_report(format.clone());
        let outcome = note_outcome(&report);
        if outcome != expected {
            wrong.push((format!("{format:?} in memory"), outcome));
        }
        let text = serde_json::to_string(&report).unwrap();
        let stored: ProfileReport = serde_json::from_str(&text).unwrap();
        let outcome = note_outcome(&stored);
        if outcome != expected {
            wrong.push((format!("{format:?} stored"), outcome));
        }
    }
    assert!(wrong.is_empty(), "wrong outcomes: {wrong:#?}");
}

#[test]
fn the_arrow_format_is_stored_as_arrow() {
    assert_eq!(
        serde_json::to_value(FileFormat::Arrow).unwrap(),
        serde_json::json!("arrow")
    );
    let read: FileFormat = serde_json::from_str("\"arrow\"").unwrap();
    assert_eq!(read, FileFormat::Arrow);
    assert_eq!(FileFormat::Arrow.to_string(), "arrow");
    // No file is detected as an Arrow stream, whatever its extension.
    assert_ne!(
        Profiler::detect_format(std::path::Path::new("data.arrow")),
        FileFormat::Arrow
    );
}

/// `None` when `result` is an error whose message contains `expected`,
/// otherwise what the route returned instead.
fn refusal<T>(result: Result<T, DataProfilerError>, expected: &str) -> Option<String> {
    match result {
        Ok(_) => Some("accepted".to_string()),
        Err(error) if error.to_string().contains(expected) => None,
        Err(error) => Some(error.to_string()),
    }
}

const PARTIAL_REFUSAL: &str = "Unsupported file format: arrow";

#[test]
fn no_file_route_reads_an_arrow_override() {
    let csv = file(".csv", NO_NOTE_CSV);
    let profiler = Profiler::new().format(FileFormat::Arrow);
    let mut wrong = Vec::new();
    for (route, outcome) in [
        (
            "infer_schema",
            refusal(profiler.infer_schema(csv.path()), PARTIAL_REFUSAL),
        ),
        (
            "quick_row_count",
            refusal(profiler.quick_row_count(csv.path()), PARTIAL_REFUSAL),
        ),
        (
            "analyze_structure",
            refusal(
                profiler.analyze_structure(csv.path(), None),
                PARTIAL_REFUSAL,
            ),
        ),
    ] {
        if let Some(outcome) = outcome {
            wrong.push(format!("{route}: {outcome}"));
        }
    }
    // The synchronous file route reads an unknown override as CSV (#891); it
    // must never record the file as an Arrow stream.
    if let Ok(report) = profiler.analyze_file(csv.path())
        && matches!(
            report.data_source,
            DataSource::File {
                format: FileFormat::Arrow,
                ..
            }
        )
    {
        wrong.push("analyze_file recorded arrow".to_string());
    }
    assert!(
        wrong.is_empty(),
        "routes that did not refuse arrow: {wrong:#?}"
    );
}

#[cfg(feature = "async-streaming")]
#[test]
fn no_async_route_reads_an_arrow_override() {
    use dataprof::{AsyncSourceInfo, BytesSource};

    let csv = file(".csv", NO_NOTE_CSV);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let profiler = Profiler::new().format(FileFormat::Arrow);
    let bytes = || {
        BytesSource::new(
            bytes::Bytes::from_static(NO_NOTE_CSV.as_bytes()),
            AsyncSourceInfo::new("pipe", FileFormat::Arrow),
        )
    };
    let mut wrong = Vec::new();
    for (route, outcome) in [
        (
            "profile_file",
            refusal(
                rt.block_on(profiler.profile_file(csv.path())),
                "The arrow format has no file reader",
            ),
        ),
        // Refused before any request is sent: a connection error to this
        // closed port would not name the format.
        (
            "profile_url",
            refusal(
                rt.block_on(profiler.profile_url("http://127.0.0.1:9/data.csv")),
                "The arrow format has no URL reader",
            ),
        ),
        (
            "profile_stream",
            refusal(
                rt.block_on(Profiler::new().profile_stream(bytes())),
                "does not support Arrow format",
            ),
        ),
        (
            "infer_schema_stream",
            refusal(
                rt.block_on(Profiler::new().infer_schema_stream(bytes())),
                PARTIAL_REFUSAL,
            ),
        ),
        (
            "quick_row_count_stream",
            refusal(
                rt.block_on(Profiler::new().quick_row_count_stream(bytes())),
                PARTIAL_REFUSAL,
            ),
        ),
    ] {
        if let Some(outcome) = outcome {
            wrong.push(format!("{route}: {outcome}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "routes that did not refuse arrow: {wrong:#?}"
    );
}
