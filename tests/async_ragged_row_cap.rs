//! `ragged_row_count` counts the rows a profile consumed, on the async route as
//! on the file engines (#1203). The async CSV reader parses ahead of the row
//! cap, so counting there included ragged rows past it, as many as the reader
//! reached before the stream was dropped.

#![cfg(feature = "async-streaming")]

use std::io::Write;

use dataprof::{
    AsyncSourceInfo, BytesSource, ChunkSize, EngineType, FileFormat, Profiler, StopCondition,
};

/// `(case, cap, source, ragged rows within the cap)`.
const CASES: [(&str, u64, &str, usize); 4] = [
    (
        "ragged rows only past the cap",
        1,
        "name,age,city\nAlice,25,Rome\nBob,30\nCarl,40,Milan,x\n",
        0,
    ),
    (
        "ragged rows on both sides of the cap",
        2,
        "name,age,city\nAlice,25\nBob,30,Rome\nCarl,40\nDan,50,Turin,x\n",
        1,
    ),
    (
        "cap of zero",
        0,
        "name,age,city\nAlice,25\nBob,30,Rome\n",
        0,
    ),
    (
        "cap the file never reaches",
        10,
        "name,age,city\nAlice,25\nBob,30,Rome\nCarl,40,Milan,x\n",
        2,
    ),
];

fn source(data: &'static str) -> BytesSource {
    BytesSource::new(
        bytes::Bytes::from_static(data.as_bytes()),
        AsyncSourceInfo::new("ragged-cap", FileFormat::Csv).size_hint(Some(data.len() as u64)),
    )
}

#[tokio::test]
async fn async_csv_counts_no_ragged_row_past_the_cap() {
    let mut wrong = Vec::new();
    for (case, cap, data, expected) in CASES {
        let mut csv = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
        write!(csv, "{data}").unwrap();
        csv.flush().unwrap();

        let mut counts = Vec::new();
        for engine in [EngineType::Incremental, EngineType::Columnar] {
            let report = Profiler::new()
                .engine(engine)
                .stop_when(StopCondition::MaxRows(cap))
                .analyze_file(csv.path())
                .unwrap();
            counts.push((format!("{engine:?}"), report.execution.ragged_row_count));
        }
        for (route, chunk_size) in [
            ("async", ChunkSize::Adaptive),
            ("async, one-record chunks", ChunkSize::Fixed(1)),
        ] {
            let report = Profiler::new()
                .chunk_size(chunk_size)
                .stop_when(StopCondition::MaxRows(cap))
                .profile_stream(source(data))
                .await
                .unwrap();
            counts.push((route.to_string(), report.execution.ragged_row_count));
        }

        for (route, count) in counts {
            if count != expected {
                wrong.push(format!("{route}, {case}: {count}, expected {expected}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
