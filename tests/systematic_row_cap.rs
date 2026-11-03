//! A row cap under sampling analyzes the same rows whatever the chunk size
//! (#1208). A systematic interval counts the rows it keeps against
//! `max_rows`; the per-row check did, but the check after each chunk counted
//! every row read, so with small chunks a systematic(2) sample under
//! `max_rows(5)` stopped after five rows read and three kept, and still named
//! the cap of five as its truncation reason.

use std::io::Write;

use dataprof::{
    ChunkSize, EngineType, ProfileReport, Profiler, SamplingStrategy, StopCondition,
    TruncationReason,
};

const ROWS: usize = 20;

fn csv_text() -> String {
    let mut data = String::from("id,value\n");
    for i in 0..ROWS {
        data.push_str(&format!("r{i},{}\n", 100 + i));
    }
    data
}

fn samplers() -> [(&'static str, SamplingStrategy); 3] {
    [
        ("no sampling", SamplingStrategy::None),
        (
            "systematic(2)",
            SamplingStrategy::Systematic { interval: 2 },
        ),
        ("reservoir(8)", SamplingStrategy::Reservoir { size: 8 }),
    ]
}

/// `(rows analyzed, truncated)` for 20 rows under a cap of `cap` rows. A
/// systematic(2) sample keeps rows 0, 2, 4, ..., so a cap of 10 is reached at
/// row 18 with row 19 unread, and a cap of 11 is never reached. A reservoir
/// caps the rows read and samples eight of them.
fn expected(sampler: &str, cap: u64) -> (usize, bool) {
    match (sampler, cap) {
        ("no sampling", cap) => (cap as usize, true),
        ("systematic(2)", 11) => (10, false),
        ("systematic(2)", cap) => (cap as usize, true),
        (_, cap) => ((cap as usize).min(8), true),
    }
}

/// `(label, condition, the cap it enforces)`. The composite names its larger
/// cap first: when the fifth kept row stops a systematic(2) scan, nine rows
/// have been read, so a cap counted on rows read also meets eight and the
/// report names `max_rows(8)`, a cap that did not stop the scan.
fn conditions() -> [(&'static str, StopCondition, u64); 4] {
    [
        ("max_rows(5)", StopCondition::MaxRows(5), 5),
        ("max_rows(10)", StopCondition::MaxRows(10), 10),
        ("max_rows(11)", StopCondition::MaxRows(11), 11),
        (
            "any(max_rows(8), max_rows(5))",
            StopCondition::Any(vec![StopCondition::MaxRows(8), StopCondition::MaxRows(5)]),
            5,
        ),
    ]
}

fn check(route: &str, sampler: &str, cap: u64, report: &ProfileReport, wrong: &mut Vec<String>) {
    let (rows, truncated) = expected(sampler, cap);
    let reason = truncated.then_some(TruncationReason::MaxRows(cap));
    let execution = &report.execution;
    if (execution.rows_processed, &execution.truncation_reason) != (rows, &reason) {
        wrong.push(format!(
            "{route}: rows {}, truncation {:?}; expected rows {rows}, truncation {reason:?}",
            execution.rows_processed, execution.truncation_reason
        ));
    }
}

#[test]
fn incremental_row_cap_does_not_depend_on_the_chunk_size() {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    file.write_all(csv_text().as_bytes()).unwrap();
    file.flush().unwrap();

    // Chunk sizes are bytes here: one byte reads one record per chunk, 16
    // bytes two or three, and adaptive the whole file.
    let chunk_sizes = [
        ("1-byte chunks", ChunkSize::Fixed(1)),
        ("16-byte chunks", ChunkSize::Fixed(16)),
        ("adaptive chunks", ChunkSize::Adaptive),
    ];
    let mut wrong = Vec::new();
    for (sampler, strategy) in samplers() {
        for (condition, stop, cap) in conditions() {
            for (chunks, chunk_size) in &chunk_sizes {
                let route = format!("incremental, {sampler}, {condition}, {chunks}");
                match Profiler::new()
                    .engine(EngineType::Incremental)
                    .sampling(strategy.clone())
                    .chunk_size(chunk_size.clone())
                    .stop_when(stop.clone())
                    .analyze_file(file.path())
                {
                    Ok(report) => check(&route, sampler, cap, &report, &mut wrong),
                    Err(error) => wrong.push(format!("{route}: {error}")),
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[cfg(feature = "async-streaming")]
mod async_route {
    use super::{ROWS, check, conditions, csv_text, samplers};

    use dataprof::{AsyncSourceInfo, BytesSource, ChunkSize, FileFormat, Profiler};

    fn jsonl_text() -> String {
        (0..ROWS)
            .map(|i| format!("{{\"id\":\"r{i}\",\"value\":{}}}\n", 100 + i))
            .collect()
    }

    #[tokio::test]
    async fn async_row_cap_does_not_depend_on_the_chunk_size() {
        // Chunk sizes are records here.
        let chunk_sizes = [
            ("one-record chunks", ChunkSize::Fixed(1)),
            ("four-record chunks", ChunkSize::Fixed(4)),
            ("adaptive chunks", ChunkSize::Adaptive),
        ];
        let mut wrong = Vec::new();
        for (name, format, text) in [
            ("CSV", FileFormat::Csv, csv_text()),
            ("JSONL", FileFormat::Jsonl, jsonl_text()),
        ] {
            for (sampler, strategy) in samplers() {
                for (condition, stop, cap) in conditions() {
                    for (chunks, chunk_size) in &chunk_sizes {
                        let route = format!("async {name}, {sampler}, {condition}, {chunks}");
                        let source = BytesSource::new(
                            bytes::Bytes::from(text.clone()),
                            AsyncSourceInfo::new("systematic-cap", format.clone())
                                .size_hint(Some(text.len() as u64)),
                        );
                        match Profiler::new()
                            .sampling(strategy.clone())
                            .chunk_size(chunk_size.clone())
                            .stop_when(stop.clone())
                            .profile_stream(source)
                            .await
                        {
                            Ok(report) => check(&route, sampler, cap, &report, &mut wrong),
                            Err(error) => wrong.push(format!("{route}: {error}")),
                        }
                    }
                }
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }
}
