//! Reports name the dataprof build that produced them (#907).
//!
//! `python/tests/test_report_generator.py` is the Python twin; it also checks
//! that the extension records the commit `build.rs` read from git.

use std::io::Write;

use dataprof::{EngineType, ProfileReport, Profiler, ReportGenerator, analyze_csv_file};
use serde_json::{Value, json};

const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

/// The identity the Python extension records at load. Only the first call in
/// a process takes effect, so every test records the same one.
fn record() {
    dataprof_runtime::record_build_identity(Some(COMMIT), Some(true));
}

fn file(suffix: &str, contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file
}

fn generator(report: &ProfileReport) -> Value {
    serde_json::to_value(report).unwrap()["generator"].clone()
}

fn recorded() -> Value {
    json!({"version": env!("CARGO_PKG_VERSION"), "commit": COMMIT, "dirty": true})
}

#[test]
fn every_engine_and_file_format_records_the_build() {
    record();
    let csv = file(".csv", "id,name\n1,a\n2,b\n");
    let json = file(".json", r#"[{"id": 1}, {"id": 2}]"#);
    let jsonl = file(".jsonl", "{\"id\": 1}\n{\"id\": 2}\n");
    let routes: Vec<(&str, &std::path::Path, Option<EngineType>)> = vec![
        ("csv auto", csv.path(), Some(EngineType::Auto)),
        ("csv incremental", csv.path(), Some(EngineType::Incremental)),
        ("csv columnar", csv.path(), Some(EngineType::Columnar)),
        ("json", json.path(), None),
        ("jsonl", jsonl.path(), None),
    ];
    let mut wrong = Vec::new();
    for (name, path, engine) in routes {
        let mut profiler = Profiler::new();
        if let Some(engine) = engine {
            profiler = profiler.engine(engine);
        }
        let report = profiler.analyze_file(path).unwrap();
        if generator(&report) != recorded() {
            wrong.push(format!("{name}: {}", generator(&report)));
        }
    }
    let legacy = analyze_csv_file(csv.path(), &Default::default()).unwrap();
    if generator(&legacy) != recorded() {
        wrong.push(format!("analyze_csv_file: {}", generator(&legacy)));
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn the_flat_summary_records_the_build() {
    record();
    let csv = file(".csv", "id\n1\n");
    let report = Profiler::new().analyze_file(csv.path()).unwrap();
    let summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    assert_eq!(summary["generator"], recorded());
}

/// A stored report names the build that wrote it, not the one reading it.
#[test]
fn a_stored_report_keeps_its_generator_and_absence_stays_unknown() {
    record();
    let csv = file(".csv", "id\n1\n");
    let mut document =
        serde_json::to_value(Profiler::new().analyze_file(csv.path()).unwrap()).unwrap();

    document["generator"] = json!({"version": "0.11.0"});
    let older: ProfileReport = serde_json::from_value(document.clone()).unwrap();
    let kept = older.generator.clone().unwrap();
    assert_eq!(
        (kept.version.as_str(), kept.commit, kept.dirty),
        ("0.11.0", None, None)
    );
    assert_eq!(generator(&older), json!({"version": "0.11.0"}));

    document.as_object_mut().unwrap().remove("generator");
    let legacy: ProfileReport = serde_json::from_value(document).unwrap();
    assert_eq!(legacy.generator, None);
    assert!(
        serde_json::to_value(&legacy)
            .unwrap()
            .get("generator")
            .is_none()
    );
}

#[test]
fn a_malformed_generator_fails_to_decode() {
    record();
    let csv = file(".csv", "id\n1\n");
    let report = Profiler::new().analyze_file(csv.path()).unwrap();
    let mut decoded = Vec::new();
    for value in [
        Value::Null,
        json!("0.13.0"),
        json!({}),
        json!({"version": 13}),
        json!({"version": "0.13.0", "commit": null}),
        json!({"version": "0.13.0", "commit": 7}),
        json!({"version": "0.13.0", "dirty": null}),
        json!({"version": "0.13.0", "dirty": "yes"}),
    ] {
        let mut document = serde_json::to_value(&report).unwrap();
        document["generator"] = value.clone();
        if serde_json::from_value::<ProfileReport>(document).is_ok() {
            decoded.push(value);
        }
    }
    assert!(decoded.is_empty(), "decoded: {decoded:?}");
}

/// A later release may add fields; this reader keeps the ones it knows.
#[test]
fn unknown_generator_fields_are_ignored() {
    record();
    let csv = file(".csv", "id\n1\n");
    let mut document =
        serde_json::to_value(Profiler::new().analyze_file(csv.path()).unwrap()).unwrap();
    document["generator"]["profile"] = json!("release");
    let restored: ProfileReport = serde_json::from_value(document).unwrap();
    assert_eq!(restored.generator, Some(ReportGenerator::current()));
}
