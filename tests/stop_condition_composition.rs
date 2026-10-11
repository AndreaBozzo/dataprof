//! Schema stability composes with every other leaf and needs a prior baseline.
//! Python twin: `python/tests/test_stop_condition_composition.py`.

use std::io::Write;

use dataprof::{ChunkSize, EngineType, Profiler, StopCondition};

fn fixture() -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "value").unwrap();
    for _ in 0..20_000 {
        writeln!(file, "12345").unwrap();
    }
    file.flush().unwrap();
    file
}

fn conditions() -> Vec<(&'static str, StopCondition, usize, usize)> {
    let stable = |n| StopCondition::SchemaStable {
        consecutive_stable_rows: n,
    };
    vec![
        (
            "all_row_and_schema",
            StopCondition::All(vec![StopCondition::MaxRows(15_000), stable(100)]),
            15_000,
            15_200,
        ),
        (
            "all_two_schema_thresholds",
            StopCondition::All(vec![stable(100), stable(3_000)]),
            3_000,
            3_400,
        ),
        (
            "nested",
            StopCondition::All(vec![
                StopCondition::Any(vec![stable(100), StopCondition::MaxRows(1_000)]),
                StopCondition::MaxRows(15_000),
            ]),
            15_000,
            15_200,
        ),
        ("needs_baseline", stable(1), 190, 400),
    ]
}

#[test]
fn schema_stability_is_evaluated_inside_each_composite() {
    let file = fixture();
    let mut wrong = Vec::new();
    for (case, condition, lower, upper) in conditions() {
        let report = Profiler::new()
            .engine(EngineType::Incremental)
            .chunk_size(ChunkSize::Fixed(600))
            .stop_when(condition)
            .analyze_file(file.path())
            .unwrap();
        let rows = report.execution.rows_processed;
        if rows < lower || rows > upper {
            wrong.push(format!(
                "[incremental] {case}: {rows}, expected {lower}..={upper}"
            ));
        }
        if report.execution.source_exhausted || report.execution.truncation_reason.is_none() {
            wrong.push(format!(
                "[incremental] {case}: missing truncation provenance"
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[cfg(feature = "async-streaming")]
#[tokio::test]
async fn async_schema_stability_is_evaluated_inside_each_composite() {
    use dataprof::{AsyncSourceInfo, AsyncStreamingProfiler, BytesSource, FileFormat};
    let file = fixture();
    let bytes = std::fs::read(file.path()).unwrap();
    let mut wrong = Vec::new();
    for (case, condition, lower, upper) in conditions() {
        let source = BytesSource::new(
            bytes.clone().into(),
            AsyncSourceInfo::new(case, FileFormat::Csv).size_hint(Some(bytes.len() as u64)),
        );
        let report = AsyncStreamingProfiler::new()
            .chunk_size(ChunkSize::Fixed(600))
            .stop_condition(condition)
            .analyze_stream(source)
            .await
            .unwrap();
        let rows = report.execution.rows_processed;
        if rows < lower || rows > upper {
            wrong.push(format!(
                "[async] {case}: {rows}, expected {lower}..={upper}"
            ));
        }
        if report.execution.source_exhausted || report.execution.truncation_reason.is_none() {
            wrong.push(format!("[async] {case}: missing truncation provenance"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
