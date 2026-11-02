//! A JSON record the async route cannot turn into a row is judged only when
//! the row cap reaches it, under every sampling strategy (#1206). The reader
//! stops at the cap only when the cap counts every row read; under a
//! systematic sample it parses ahead of the cap, and counted or refused the
//! records past it there.

#![cfg(feature = "async-streaming")]

use dataprof::{
    AsyncSourceInfo, BytesSource, ChunkSize, FileFormat, JsonErrorPolicy, Profiler,
    SamplingStrategy, StopCondition,
};

/// `(case, format, cap, source, faults the cap reaches)`. Every sampler below
/// reaches the same records: the faults sit before the first row or after the
/// fifth, and a cap of three kept rows stops by the fifth under each. Content
/// after the closing bracket follows the last row; a cap of five reaches it
/// under each sampler, at the cap or with rows to spare.
const CASES: [(&str, FileFormat, u64, &str, usize); 5] = [
    (
        "JSONL, malformed lines past the cap",
        FileFormat::Jsonl,
        3,
        "{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n{\"a\":4}\n{\"a\":5}\n{\"a\":6}\n{\"a\":7}\n{bad\n[1]\n",
        0,
    ),
    (
        "JSONL, malformed lines on both sides of the cap",
        FileFormat::Jsonl,
        3,
        "{bad\n{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n{\"a\":4}\n{\"a\":5}\n{\"a\":6}\n{\"a\":7}\n{bad\n",
        1,
    ),
    (
        "JSON array, non-object elements past the cap",
        FileFormat::Json,
        3,
        "[{\"a\":1},{\"a\":2},{\"a\":3},{\"a\":4},{\"a\":5},{\"a\":6},{\"a\":7},1,\"x\"]",
        0,
    ),
    (
        "JSON array, non-object elements on both sides of the cap",
        FileFormat::Json,
        3,
        "[1,{\"a\":1},{\"a\":2},{\"a\":3},{\"a\":4},{\"a\":5},{\"a\":6},{\"a\":7},\"x\"]",
        1,
    ),
    (
        "JSON array, content after the closing bracket at the cap",
        FileFormat::Json,
        5,
        "[{\"a\":1},{\"a\":2},{\"a\":3},{\"a\":4},{\"a\":5}] trailing",
        1,
    ),
];

fn source(data: &'static str, format: FileFormat) -> BytesSource {
    BytesSource::new(
        bytes::Bytes::from_static(data.as_bytes()),
        AsyncSourceInfo::new("json-cap", format).size_hint(Some(data.len() as u64)),
    )
}

fn samplers() -> [(&'static str, SamplingStrategy); 3] {
    [
        ("no sampling", SamplingStrategy::None),
        (
            "systematic(2)",
            SamplingStrategy::Systematic { interval: 2 },
        ),
        ("reservoir(20)", SamplingStrategy::Reservoir { size: 20 }),
    ]
}

const CHUNK_SIZES: [(&str, ChunkSize); 2] = [
    ("adaptive chunks", ChunkSize::Adaptive),
    ("one-record chunks", ChunkSize::Fixed(1)),
];

#[tokio::test]
async fn tolerant_json_counts_only_the_faults_the_cap_reaches() {
    let mut wrong = Vec::new();
    for (case, format, cap, data, reached) in CASES {
        for (sampler, strategy) in samplers() {
            for (chunks, chunk_size) in CHUNK_SIZES {
                let route = format!("{case}, {sampler}, {chunks}");
                match Profiler::new()
                    .sampling(strategy.clone())
                    .chunk_size(chunk_size)
                    .stop_when(StopCondition::MaxRows(cap))
                    .profile_stream(source(data, format.clone()))
                    .await
                {
                    Ok(report) if report.execution.error_count == reached => {}
                    Ok(report) => wrong.push(format!(
                        "{route}: error_count {}, expected {reached}",
                        report.execution.error_count
                    )),
                    Err(error) => wrong.push(format!("{route}: {error}")),
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[tokio::test]
async fn strict_json_refuses_only_a_fault_the_cap_reaches() {
    let mut wrong = Vec::new();
    for (case, format, cap, data, reached) in CASES {
        for (sampler, strategy) in samplers() {
            for (chunks, chunk_size) in CHUNK_SIZES {
                let route = format!("{case}, {sampler}, {chunks}");
                let outcome = Profiler::new()
                    .sampling(strategy.clone())
                    .chunk_size(chunk_size)
                    .json_error_policy(JsonErrorPolicy::Strict)
                    .stop_when(StopCondition::MaxRows(cap))
                    .profile_stream(source(data, format.clone()))
                    .await;
                match (outcome, reached > 0) {
                    (Ok(_), false) | (Err(_), true) => {}
                    (Ok(_), true) => wrong.push(format!("{route}: profiled, expected a refusal")),
                    (Err(error), false) => wrong.push(format!("{route}: {error}")),
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
