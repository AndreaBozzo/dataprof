//! Every route reports progress to a sink, at least as a start and a finish.
//!
//! Only the incremental CSV engine reports while it reads. A sink on the
//! default `Auto` engine used to receive nothing at all, which is what the
//! Python guide's `dp.profile("data.csv", on_progress=...)` example runs.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use dataprof::{EngineType, ProfileReport, Profiler, ProgressEvent};

fn profile_with_events(path: &Path, engine: EngineType) -> (ProfileReport, Vec<ProgressEvent>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&events);
    let report = Profiler::new()
        .engine(engine)
        .on_progress(move |event| sink.lock().unwrap().push(event))
        .analyze_file(path)
        .expect("the fixture profiles");
    let events = events.lock().unwrap().clone();
    (report, events)
}

fn write(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::File::create(&path)
        .and_then(|mut file| file.write_all(contents.as_bytes()))
        .expect("write fixture");
    path
}

fn assert_bracketed(label: &str, report: &ProfileReport, events: &[ProgressEvent]) {
    let starts = events
        .iter()
        .filter(|e| matches!(e, ProgressEvent::Started { .. }))
        .count();
    assert_eq!(starts, 1, "{label}: exactly one Started, got {events:?}");
    assert!(
        matches!(events.first(), Some(ProgressEvent::Started { .. })),
        "{label}: first event must be Started, got {events:?}"
    );
    match events.last() {
        Some(ProgressEvent::Finished {
            total_rows,
            truncated,
            ..
        }) => {
            assert_eq!(*total_rows, report.execution.rows_processed, "{label}");
            assert_eq!(*truncated, !report.execution.source_exhausted, "{label}");
        }
        other => panic!("{label}: last event must be Finished, got {other:?}"),
    }
}

#[test]
fn every_route_reports_a_start_and_a_finish() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut csv = String::from("id,amount\n");
    for i in 0..2_000 {
        csv.push_str(&format!("{i},{}\n", i % 97));
    }
    let csv = write(dir.path(), "orders.csv", &csv);
    let jsonl = write(dir.path(), "orders.jsonl", "{\"id\":1}\n{\"id\":2}\n");
    let parquet = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("test_data")
        .join("simple.parquet");

    for (label, path, engine, streams) in [
        ("csv/auto", &csv, EngineType::Auto, true),
        ("csv/incremental", &csv, EngineType::Incremental, true),
        ("csv/columnar", &csv, EngineType::Columnar, false),
        ("jsonl/auto", &jsonl, EngineType::Auto, false),
        ("parquet/auto", &parquet, EngineType::Auto, false),
    ] {
        let (report, events) = profile_with_events(path, engine);
        assert_bracketed(label, &report, &events);
        let schema_reported = events
            .iter()
            .any(|e| matches!(e, ProgressEvent::SchemaDetected { .. }));
        assert_eq!(
            schema_reported, streams,
            "{label}: only the streaming engine reports its schema"
        );
        if !streams {
            assert_eq!(events.len(), 2, "{label}: bracketed by exactly two events");
        }
    }
}
