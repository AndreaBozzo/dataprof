//! A JSON key first seen after the async reader's first chunk is a column, as
//! it is on the sync path (#1020).
//!
//! `python/tests/test_async_json_late_keys.py` is the Python twin, through
//! `dataprof.asyncio`.
#![cfg(feature = "async-streaming")]

use std::io::Write;

use dataprof::{
    AsyncSourceInfo, AsyncStreamingProfiler, BytesSource, ChunkSize, FileFormat, ProfileReport,
    Profiler, SamplingStrategy, StopCondition,
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

fn counts(report: &ProfileReport) -> Vec<(String, usize, usize)> {
    report
        .column_profiles
        .iter()
        .map(|column| (column.name.clone(), column.total_count, column.null_count))
        .collect()
}

#[tokio::test]
async fn a_late_key_only_unsampled_rows_carry_is_a_column_at_every_chunk_size() {
    // The last row is the only one with `late`, and no folded row is read
    // after it. Systematic sampling folds rows as it reads and skips it; a
    // one-row reservoir folds its sample at the end. Either way the source
    // read has the column, and every folded row is missing it.
    let contents = jsonl(&[r#"{"id": 1}"#, r#"{"id": 2, "late": 7}"#]);
    let mut wrong = Vec::new();
    for strategy in [
        SamplingStrategy::Systematic { interval: 2 },
        SamplingStrategy::Reservoir { size: 1 },
    ] {
        let mut seen = Vec::new();
        for chunk in [
            ChunkSize::Fixed(1),
            ChunkSize::Fixed(16),
            ChunkSize::Adaptive,
        ] {
            let info = AsyncSourceInfo::new("sampled", FileFormat::Jsonl)
                .size_hint(Some(contents.len() as u64));
            let report = AsyncStreamingProfiler::new()
                .chunk_size(chunk.clone())
                .sampling(strategy.clone())
                .analyze_stream(BytesSource::new(contents.as_bytes().to_vec().into(), info))
                .await
                .unwrap();
            seen.push((chunk, counts(&report)));
        }
        let names: Vec<Vec<String>> = seen
            .iter()
            .map(|(_, counts)| counts.iter().map(|(name, ..)| name.clone()).collect())
            .collect();
        if names.iter().any(|columns| columns != &["id", "late"])
            || seen.windows(2).any(|pair| pair[0].1 != pair[1].1)
        {
            wrong.push(format!("{strategy:?}: {seen:?}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[tokio::test]
async fn a_key_only_rows_past_a_row_cap_carry_is_not_a_column() {
    // The long first record fills a chunk on its own; the next two share one,
    // which announces `late`. The cap stops after the second record, so the
    // third, the only one with `late`, was never read.
    let pad = "x".repeat(64);
    let first = format!(r#"{{"id": 1, "pad": "{pad}"}}"#);
    let contents = jsonl(&[&first, r#"{"id": 2}"#, r#"{"id": 3, "late": 7}"#]);
    let info = AsyncSourceInfo::new("capped", FileFormat::Jsonl);
    let report = AsyncStreamingProfiler::new()
        .chunk_size(ChunkSize::Fixed(64))
        .stop_condition(StopCondition::MaxRows(2))
        .analyze_stream(BytesSource::new(contents.as_bytes().to_vec().into(), info))
        .await
        .unwrap();
    assert_eq!(report.execution.rows_processed, 2);
    assert_eq!(
        counts(&report),
        [("id".to_string(), 2, 0), ("pad".to_string(), 2, 1)]
    );
}

/// The column lists of the `SchemaDetected` events, one record per chunk.
async fn schema_events(contents: &str, max_rows: Option<u64>) -> Vec<Vec<String>> {
    use std::sync::{Arc, Mutex};

    let lists = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&lists);
    let sink = dataprof::ProgressSink::Callback(Arc::new(move |event| {
        if let dataprof::ProgressEvent::SchemaDetected { column_names } = event {
            captured.lock().unwrap().push(column_names);
        }
    }));
    let mut profiler = Profiler::new()
        .chunk_size(ChunkSize::Fixed(1))
        .progress_interval(std::time::Duration::ZERO)
        .progress_sink(sink);
    if let Some(rows) = max_rows {
        profiler = profiler.stop_when(StopCondition::MaxRows(rows));
    }
    let info =
        AsyncSourceInfo::new("late-keys", FileFormat::Jsonl).size_hint(Some(contents.len() as u64));
    profiler
        .profile_stream(BytesSource::new(contents.as_bytes().to_vec().into(), info))
        .await
        .unwrap();
    let lists = lists.lock().unwrap().clone();
    lists
}

#[tokio::test]
async fn a_late_key_is_announced_by_a_second_schema_event() {
    let names = |list: &[&str]| list.iter().map(|name| name.to_string()).collect::<Vec<_>>();
    let contents = jsonl(&[
        r#"{"id": 1}"#,
        r#"{"id": 2, "late": 7}"#,
        r#"{"id": 3}"#,
        r#"{"id": 4, "later": 8}"#,
    ]);

    // Each event extends the last; a chunk with no new key sends none (#1034).
    assert_eq!(
        schema_events(&contents, None).await,
        [
            names(&["id"]),
            names(&["id", "late"]),
            names(&["id", "late", "later"])
        ]
    );
    // A cap that stops before the late key never makes it a column, so it is
    // never announced either.
    assert_eq!(schema_events(&contents, Some(1)).await, [names(&["id"])]);
}
