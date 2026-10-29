//! A report can record which data it describes (#866).
//!
//! `python/tests/test_source_identity.py` is the Python twin; it covers the
//! `source_version=` keyword on every input route and `python -m dataprof.check`.

use std::io::Write;

use dataprof::{ProfileReport, Profiler, SourceIdentity};
use serde_json::{Value, json};

fn report() -> ProfileReport {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    file.write_all(b"id\n1\n2\n").unwrap();
    Profiler::new().analyze_file(file.path()).unwrap()
}

fn document(report: &ProfileReport) -> Value {
    serde_json::to_value(report).unwrap()
}

#[test]
fn a_recorded_version_round_trips_through_both_documents() {
    let mut report = report();
    report.source_identity = Some(SourceIdentity::with_version("snapshot 42 "));
    let mut wrong = Vec::new();

    let canonical = document(&report);
    if canonical["source_identity"] != json!({"version": "snapshot 42 "}) {
        wrong.push(format!("to_json: {}", canonical["source_identity"]));
    }
    let restored: ProfileReport = serde_json::from_value(canonical).unwrap();
    if restored.source_identity != report.source_identity {
        wrong.push(format!("reloaded: {:?}", restored.source_identity));
    }
    let summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    if summary["source_identity"] != json!({"version": "snapshot 42 "}) {
        wrong.push(format!("summary: {}", summary["source_identity"]));
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// Nothing records an identity on its own, and a report without one says so
/// by leaving the field out.
#[test]
fn a_report_without_an_identity_serializes_without_the_field() {
    let report = report();
    assert_eq!(report.source_identity, None);
    assert!(document(&report).get("source_identity").is_none());
    let summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    assert!(summary.get("source_identity").is_none());
}

#[test]
fn a_malformed_identity_fails_to_decode() {
    let report = report();
    let mut decoded = Vec::new();
    for value in [
        Value::Null,
        json!("snapshot-42"),
        json!({"version": null}),
        json!({"version": 42}),
    ] {
        let mut document = document(&report);
        document["source_identity"] = value.clone();
        if serde_json::from_value::<ProfileReport>(document).is_ok() {
            decoded.push(value);
        }
    }
    assert!(decoded.is_empty(), "decoded: {decoded:?}");
}

/// A later release may add other kinds of identity; this reader keeps the
/// ones it knows and reads one holding none of them as recording nothing it
/// can name, without failing.
#[test]
fn unknown_identity_members_are_ignored() {
    let report = report();
    let mut document = document(&report);
    document["source_identity"] = json!({"version": "v7", "content_sha256": "ab"});
    let restored: ProfileReport = serde_json::from_value(document.clone()).unwrap();
    assert_eq!(
        restored.source_identity,
        Some(SourceIdentity::with_version("v7"))
    );

    document["source_identity"] = json!({"content_sha256": "ab"});
    let restored: ProfileReport = serde_json::from_value(document).unwrap();
    assert_eq!(restored.source_identity, Some(SourceIdentity::default()));
}
