//! An async stream records the format it parsed, so the baseline gate reads a
//! capped async CSV as declaring every column in its header, as it reads a
//! capped synchronous one (#1042). A JSONL stream still covers only the
//! records read.
//!
//! `python/tests/test_async_stream_format.py` is the Python twin; it also
//! covers the URL route.

#![cfg(feature = "async-streaming")]

use std::io::Write;

use dataprof::{
    AsyncSourceInfo, BytesSource, CheckCode, FileFormat, ProfileReport, Profiler, QualityPolicy,
    StopCondition,
};
use dataprof_core::StreamSourceSystem;

const NO_NOTE_CSV: &str = "id,amount\n1,10\n2,20\n3,30\n";
const LATE_NOTE_JSONL: &str = "{\"id\": 1}\n{\"id\": 2}\n{\"id\": 3, \"note\": \"a\"}\n";

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn file(suffix: &str, contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

fn capped() -> Profiler {
    Profiler::new().stop_when(StopCondition::MaxRows(1))
}

/// Every async route that records a stream source, capped at one row.
fn stream_routes(
    path: &std::path::Path,
    contents: &'static str,
    format: FileFormat,
) -> Vec<(&'static str, ProfileReport)> {
    let rt = runtime();
    vec![
        (
            "profile_file",
            rt.block_on(capped().profile_file(path)).unwrap(),
        ),
        (
            "reader from an undeclared system",
            rt.block_on(async {
                let handle = tokio::fs::File::open(path).await.unwrap();
                capped()
                    .profile_stream((handle, AsyncSourceInfo::new("pipe", format.clone())))
                    .await
            })
            .unwrap(),
        ),
        (
            "buffer from a declared system",
            rt.block_on(capped().profile_stream(BytesSource::new(
                bytes::Bytes::from_static(contents.as_bytes()),
                AsyncSourceInfo::new("orders", format).source_system(StreamSourceSystem::Kafka),
            )))
            .unwrap(),
        ),
    ]
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

fn format_of(report: &ProfileReport) -> serde_json::Value {
    serde_json::to_value(&report.data_source).unwrap()["format"].clone()
}

#[test]
fn a_capped_async_csv_stream_declares_its_columns() {
    let csv = file(".csv", NO_NOTE_CSV);
    let jsonl = file(".jsonl", LATE_NOTE_JSONL);
    let mut wrong = Vec::new();
    for (format, path, contents, expected_format, expected_outcome) in [
        (FileFormat::Csv, csv.path(), NO_NOTE_CSV, "csv", "failed"),
        (
            FileFormat::Jsonl,
            jsonl.path(),
            LATE_NOTE_JSONL,
            "jsonl",
            "evidence_incomplete:truncated",
        ),
    ] {
        for (route, report) in stream_routes(path, contents, format) {
            let label = format!("{expected_format} {route}");
            if !report.data_source.is_stream() {
                wrong.push(format!("{label}: not a stream source"));
            }
            if report.execution.source_exhausted {
                wrong.push(format!("{label}: the cap did not stop the scan"));
            }
            if format_of(&report) != expected_format {
                wrong.push(format!("{label}: format {}", format_of(&report)));
            }
            let outcome = note_outcome(&report);
            if outcome != expected_outcome {
                wrong.push(format!(
                    "{label}: require note {outcome}, expected {expected_outcome}"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn a_stream_saved_without_a_format_stays_conservative() {
    let csv = file(".csv", NO_NOTE_CSV);
    let report = runtime()
        .block_on(capped().profile_file(csv.path()))
        .unwrap();

    let text = serde_json::to_string(&report).unwrap();
    let reloaded: ProfileReport = serde_json::from_str(&text).unwrap();
    assert_eq!(format_of(&reloaded), "csv");
    assert_eq!(note_outcome(&reloaded), "failed");

    // A stream report written before 0.14 records no format.
    let mut document = serde_json::to_value(&report).unwrap();
    document["data_source"]
        .as_object_mut()
        .unwrap()
        .remove("format")
        .unwrap();
    let legacy: ProfileReport = serde_json::from_value(document).unwrap();
    assert_eq!(note_outcome(&legacy), "evidence_incomplete:truncated");
}

/// A CSV stream that records its format but not how it was split leaves the
/// delimiter rules unevaluated, as a CSV file or bytes source does, instead
/// of passing them.
#[test]
fn a_csv_stream_without_its_split_leaves_the_delimiter_rules_unevaluated() {
    let csv = file(".csv", NO_NOTE_CSV);
    let report = runtime()
        .block_on(capped().profile_file(csv.path()))
        .unwrap();
    let unevaluated = |report: &ProfileReport| {
        report
            .findings()
            .not_evaluated
            .iter()
            .map(|rule| serde_json::to_value(rule).unwrap())
            .filter(|rule| rule["reason"] == "unrecorded")
            .map(|rule| rule["code"].as_str().unwrap().to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(unevaluated(&report), Vec::<String>::new());

    let mut document = serde_json::to_value(&report).unwrap();
    document["data_source"]
        .as_object_mut()
        .unwrap()
        .remove("csv_metadata")
        .unwrap();
    let stripped: ProfileReport = serde_json::from_value(document).unwrap();
    assert_eq!(
        unevaluated(&stripped),
        ["delimiter_ambiguous", "header_not_split"]
    );
}
