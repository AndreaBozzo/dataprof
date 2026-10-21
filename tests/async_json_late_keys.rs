//! A JSON key first seen after the async reader's first chunk is a column, as
//! it is on the sync path (#1020).
//!
//! `python/tests/test_async_json_late_keys.py` is the Python twin, through
//! `dataprof.asyncio`.
#![cfg(feature = "async-streaming")]

use std::io::Write;

use dataprof::{
    AsyncSourceInfo, AsyncStreamingProfiler, BytesSource, ChunkSize, FileFormat, ProfileReport,
    Profiler,
};
use serde_json::Value;

fn jsonl(records: &[&str]) -> String {
    records.iter().map(|record| format!("{record}\n")).collect()
}

/// Everything a report says about the data, without identity and execution.
fn measured(report: &ProfileReport) -> Value {
    let document = serde_json::to_value(report).unwrap();
    serde_json::json!({
        "column_profiles": document["column_profiles"],
        "quality": document["quality"],
    })
}

fn sync(contents: &str, suffix: &str) -> ProfileReport {
    let mut file = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    Profiler::new().analyze_file(file.path()).unwrap()
}

async fn by_chunk(contents: &str, format: FileFormat, chunk: ChunkSize) -> ProfileReport {
    let info = AsyncSourceInfo::new("late-keys", format).size_hint(Some(contents.len() as u64));
    AsyncStreamingProfiler::new()
        .chunk_size(chunk)
        .analyze_stream(BytesSource::new(contents.as_bytes().to_vec().into(), info))
        .await
        .unwrap()
}

#[tokio::test]
async fn a_late_key_profiles_as_on_the_sync_path_at_every_chunk_size() {
    let many: Vec<String> = (0..3000)
        .map(|i| {
            if i == 2999 {
                format!(r#"{{"id": {i}, "late": 1}}"#)
            } else {
                format!(r#"{{"id": {i}}}"#)
            }
        })
        .collect();
    let many: Vec<&str> = many.iter().map(String::as_str).collect();
    let cases: Vec<(&str, String)> = vec![
        (
            "second record",
            jsonl(&[r#"{"id": 1}"#, r#"{"id": 2, "note": "a"}"#]),
        ),
        (
            "mid-stream, absent again after",
            jsonl(&[
                r#"{"id": 1}"#,
                r#"{"id": 2}"#,
                r#"{"id": 3, "note": "a"}"#,
                r#"{"id": 4}"#,
            ]),
        ),
        (
            "two keys in different chunks",
            jsonl(&[r#"{"a": 1}"#, r#"{"a": 2, "b": 3}"#, r#"{"a": 4, "c": 5}"#]),
        ),
        (
            "a late container",
            jsonl(&[r#"{"id": 1}"#, r#"{"id": 2, "tags": [1, 2]}"#]),
        ),
        (
            "duplicates either side of the key",
            jsonl(&[r#"{"id": 1}"#, r#"{"id": 1, "x": 2}"#, r#"{"id": 1}"#]),
        ),
        ("last of 3000", jsonl(&many)),
    ];
    let chunks = [
        ("1 byte", ChunkSize::Fixed(1)),
        ("64 bytes", ChunkSize::Fixed(64)),
        ("adaptive", ChunkSize::Adaptive),
    ];
    // A fieldless record before the first key is left out of the row counts
    // on the file path only (#1021), so it is not compared here.
    let mut wrong = Vec::new();
    for (name, contents) in &cases {
        let expected = measured(&sync(contents, ".jsonl"));
        for (size, chunk) in &chunks {
            let report = by_chunk(contents, FileFormat::Jsonl, chunk.clone()).await;
            assert!(report.execution.source_exhausted, "{name} at {size}");
            if measured(&report) != expected {
                let names: Vec<&str> = report
                    .column_profiles
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect();
                wrong.push(format!("{name} at {size}: columns {names:?}"));
            }
        }
    }
    // A JSON array goes through the same reader task.
    let array = r#"[{"id": 1}, {"id": 2, "note": "a"}]"#;
    let expected = measured(&sync(array, ".json"));
    for (size, chunk) in &chunks {
        if measured(&by_chunk(array, FileFormat::Json, chunk.clone()).await) != expected {
            wrong.push(format!("json array at {size}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[tokio::test]
async fn a_late_key_counts_the_earlier_records_as_missing() {
    let contents = jsonl(&[r#"{"id": 1}"#, r#"{"id": 2}"#, r#"{"id": 3, "note": "a"}"#]);
    let report = by_chunk(&contents, FileFormat::Jsonl, ChunkSize::Fixed(1)).await;
    let note = report
        .column_profiles
        .iter()
        .find(|column| column.name == "note")
        .expect("#1020: a key first seen in a later chunk is a column");
    assert_eq!((note.total_count, note.null_count), (3, 2));
}
