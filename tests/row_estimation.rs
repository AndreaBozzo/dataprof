//! Logical record counts and stopping fractions. Python twin: test_row_estimation.py.

use dataprof::{ChunkSize, EngineType, Profiler, StopCondition, quick_row_count};
use std::io::Write;

fn fixtures() -> Vec<(&'static str, Vec<u8>, u64)> {
    let mut growing = String::from("a,b\n");
    for i in 0..200_000 {
        growing.push_str(&format!("{i},{}\n", i * 2));
    }
    let mut quoted = String::from("id,note\n");
    for i in 0..30_000 {
        quoted.push_str(&format!("{i},\"{}\nline two {i}\"\n", "x".repeat(400)));
    }
    let short = format!("value\n{}", "123456789\n".repeat(20_000));
    let wide = format!("value\n{}", format!("{}\n", "7".repeat(100)).repeat(20_000));
    vec![
        ("growing", growing.into_bytes(), 200_000),
        ("quoted", quoted.into_bytes(), 30_000),
        ("short", short.into_bytes(), 20_000),
        ("wide", wide.into_bytes(), 20_000),
    ]
}

fn file(bytes: &[u8]) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::with_suffix(".csv").unwrap();
    file.write_all(bytes).unwrap();
    file.flush().unwrap();
    file
}

#[test]
fn quick_count_uses_records_and_reports_exactness() {
    let mut wrong = Vec::new();
    for (case, bytes, expected) in fixtures() {
        let file = file(&bytes);
        let count = quick_row_count(file.path()).unwrap();
        if count.count != expected || !count.exact || count.relative_error.is_some() {
            wrong.push(format!("{case}: {count:?}, expected exact {expected}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn incremental_confidence_means_fraction_of_logical_records() {
    let mut wrong = Vec::new();
    for (case, bytes, total) in fixtures() {
        let file = file(&bytes);
        let report = Profiler::new()
            .engine(EngineType::Incremental)
            .chunk_size(ChunkSize::Fixed(4096))
            .stop_when(StopCondition::ConfidenceThreshold(0.5))
            .analyze_file(file.path())
            .unwrap();
        let rows = report.execution.rows_processed as u64;
        // At most one parsed chunk crosses the fraction. The shortest row is 4 bytes.
        if rows < total / 2 || rows > total / 2 + 1024 {
            wrong.push(format!(
                "{case}: {rows}, expected {}..={}",
                total / 2,
                total / 2 + 1024
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[cfg(feature = "async-streaming")]
#[tokio::test]
async fn async_confidence_uses_exact_bytes_or_observed_stream_density() {
    use dataprof::{AsyncSourceInfo, AsyncStreamingProfiler, BytesSource, FileFormat};
    let mut wrong = Vec::new();
    for (case, bytes, total) in fixtures() {
        let info = AsyncSourceInfo::new(case, FileFormat::Csv).size_hint(Some(bytes.len() as u64));
        let report = AsyncStreamingProfiler::new()
            .chunk_size(ChunkSize::Fixed(4096))
            .stop_condition(StopCondition::ConfidenceThreshold(0.5))
            .analyze_stream(BytesSource::new(bytes.clone().into(), info.clone()))
            .await
            .unwrap();
        let rows = report.execution.rows_processed as u64;
        if rows < total / 2 || rows > total / 2 + 1024 {
            wrong.push(format!("[bytes] {case}: {rows}, expected half of {total}"));
        }
        // Uniform widths give a known density even when the source cannot be pre-scanned.
        if matches!(case, "short" | "wide") {
            let file = file(&bytes);
            let input = tokio::fs::File::open(file.path()).await.unwrap();
            let report = AsyncStreamingProfiler::new()
                .chunk_size(ChunkSize::Fixed(4096))
                .stop_condition(StopCondition::ConfidenceThreshold(0.5))
                .analyze_stream((input, info))
                .await
                .unwrap();
            let rows = report.execution.rows_processed as u64;
            if rows < total / 2 || rows > total / 2 + 1024 {
                wrong.push(format!(
                    "[file stream] {case}: {rows}, expected half of {total}"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[cfg(feature = "async-streaming")]
#[tokio::test]
async fn exact_async_count_marks_eof_without_a_size_hint() {
    use dataprof::{AsyncSourceInfo, AsyncStreamingProfiler, BytesSource, FileFormat};
    let mut wrong = Vec::new();
    for (case, bytes) in [
        ("small", "value\n1\n2\n3\n".to_owned()),
        ("chunks", format!("value\n{}", "123456789\n".repeat(2000))),
    ] {
        for size in [None, Some(bytes.len() as u64)] {
            let info = AsyncSourceInfo::new(case, FileFormat::Csv).size_hint(size);
            let direct = AsyncStreamingProfiler::new()
                .chunk_size(ChunkSize::Fixed(4096))
                .stop_condition(StopCondition::ConfidenceThreshold(1.0))
                .analyze_stream(BytesSource::new(
                    bytes.clone().into_bytes().into(),
                    info.clone(),
                ))
                .await
                .unwrap();
            let facade = Profiler::new()
                .chunk_size(ChunkSize::Fixed(4096))
                .stop_when(StopCondition::ConfidenceThreshold(1.0))
                .profile_stream(BytesSource::new(bytes.clone().into_bytes().into(), info))
                .await
                .unwrap();
            for (route, report) in [("direct", direct), ("facade", facade)] {
                if !report.execution.source_exhausted
                    || report.execution.truncation_reason.is_some()
                {
                    wrong.push(format!(
                        "[{route}] {case} size={size:?}: {:?}",
                        report.execution
                    ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
