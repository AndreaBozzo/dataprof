//! An async report records the source it read, not a transport it guessed
//! (#1219). A buffer already in memory is the `bytes` source the synchronous
//! route records; a stream whose caller declared no system records none, where
//! report assembly used to fill in `http`.
//!
//! `python/tests/test_async_source_provenance.py` is the Python twin; it also
//! covers the sync bytes and URL routes.

#![cfg(feature = "async-streaming")]

use std::io::Write;

use dataprof::{AsyncSourceInfo, BytesSource, FileFormat, Profiler};
use dataprof_core::StreamSourceSystem;
use serde_json::{Value, json};

const CSV: &[u8] = b"a,b\n1,2\n3,4\n";
const JSONL: &[u8] = b"{\"a\":1}\n{\"a\":2}\n";

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn source_of(report: &dataprof::ProfileReport) -> Value {
    let mut source = serde_json::to_value(report).unwrap()["data_source"].clone();
    // A fresh UUID per run; its presence is pinned, its value cannot be.
    if let Some(batch_id) = source.get_mut("batch_id") {
        assert!(batch_id.is_string(), "batch_id: {batch_id}");
        *batch_id = json!("<uuid>");
    }
    // The CSV split is #1100's to pin; only whether it is recorded matters here.
    if let Some(csv_metadata) = source.get_mut("csv_metadata") {
        *csv_metadata = json!("<recorded>");
    }
    source
}

#[test]
fn every_async_source_records_what_it_read() {
    let rt = runtime();
    let profiler = Profiler::new();
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    file.write_all(CSV).unwrap();
    file.flush().unwrap();
    let path = file.path().display().to_string();

    let bytes = |data: &'static [u8], format| {
        BytesSource::new(
            bytes::Bytes::from_static(data),
            AsyncSourceInfo::new("upload", format),
        )
    };
    let routes: Vec<(&str, Value, Value)> = vec![
        (
            "csv buffer",
            source_of(
                &rt.block_on(profiler.profile_stream(bytes(CSV, FileFormat::Csv)))
                    .unwrap(),
            ),
            json!({"type": "bytes", "name": "upload", "format": "csv", "size_bytes": CSV.len(),
                   "csv_metadata": "<recorded>"}),
        ),
        (
            "jsonl buffer",
            source_of(
                &rt.block_on(profiler.profile_stream(bytes(JSONL, FileFormat::Jsonl)))
                    .unwrap(),
            ),
            json!({"type": "bytes", "name": "upload", "format": "jsonl",
                   "size_bytes": JSONL.len()}),
        ),
        (
            "buffer from a declared system",
            source_of(
                &rt.block_on(
                    profiler.profile_stream(BytesSource::new(
                        bytes::Bytes::from_static(CSV),
                        AsyncSourceInfo::new("orders", FileFormat::Csv)
                            .source_system(StreamSourceSystem::Kafka),
                    )),
                )
                .unwrap(),
            ),
            json!({"type": "stream", "topic": "orders", "batch_id": "<uuid>",
                   "source_system": "kafka", "format": "csv", "csv_metadata": "<recorded>"}),
        ),
        (
            "stream from an undeclared system",
            source_of(
                &rt.block_on(async {
                    let handle = tokio::fs::File::open(file.path()).await.unwrap();
                    profiler
                        .profile_stream((handle, AsyncSourceInfo::new("pipe", FileFormat::Csv)))
                        .await
                })
                .unwrap(),
            ),
            json!({"type": "stream", "topic": "pipe", "batch_id": "<uuid>", "format": "csv",
                   "csv_metadata": "<recorded>"}),
        ),
        (
            "profile_file",
            source_of(&rt.block_on(profiler.profile_file(file.path())).unwrap()),
            json!({"type": "stream", "topic": path, "batch_id": "<uuid>",
                   "source_system": "file", "format": "csv", "csv_metadata": "<recorded>"}),
        ),
    ];

    let wrong: Vec<String> = routes
        .into_iter()
        .filter(|(_, got, expected)| got != expected)
        .map(|(route, got, expected)| format!("{route}: got {got}, expected {expected}"))
        .collect();
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn a_stream_without_a_system_reloads_without_one() {
    let rt = runtime();
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    file.write_all(CSV).unwrap();
    file.flush().unwrap();
    let report = rt
        .block_on(async {
            let handle = tokio::fs::File::open(file.path()).await.unwrap();
            Profiler::new()
                .profile_stream((handle, AsyncSourceInfo::new("pipe", FileFormat::Csv)))
                .await
        })
        .unwrap();

    let text = serde_json::to_string(&report).unwrap();
    let reloaded: dataprof::ProfileReport = serde_json::from_str(&text).unwrap();
    assert_eq!(
        serde_json::to_value(&reloaded.data_source).unwrap(),
        serde_json::to_value(&report.data_source).unwrap()
    );
    assert!(
        reloaded
            .data_source
            .identifier()
            .starts_with("stream[pipe]-batch:"),
        "{}",
        reloaded.data_source.identifier()
    );
}
