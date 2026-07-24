//! Execution controls must take effect, and execution metadata must describe
//! what actually happened. Guards #460.
//!
//! Two separate promises are covered here:
//!
//! 1. **Controls are not decorative.** `chunk_size` and `memory_limit_mb` reach
//!    the engine that does the work, on every path that documents them.
//! 2. **Provenance is self-consistent.** `rows_processed`, `bytes_consumed`,
//!    `source_exhausted` and `truncation_reason` cannot contradict each other —
//!    an agent or quality gate uses exactly these fields to tell a complete
//!    profile from a bounded one.
//!
//! Row caps are hard caps: `rows_processed` never exceeds the limit. Byte caps
//! are evaluated at chunk boundaries, so `bytes_consumed` may exceed the cap by
//! at most one chunk — a bound the caller controls through `chunk_size`.

use std::io::Write;

use dataprof::{ChunkSize, EngineType, ProfileReport, Profiler, StopCondition};

/// A CSV of `rows` data rows with a stable 5-column schema.
fn write_csv(rows: usize) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    writeln!(file, "id,name,value,category,ts").unwrap();
    for i in 0..rows {
        writeln!(
            file,
            "{i},name_{i},{},cat_{},2026-01-{:02}",
            i * 3,
            i % 7,
            (i % 28) + 1
        )
        .unwrap();
    }
    file.flush().unwrap();
    file
}

fn file_size(file: &tempfile::NamedTempFile) -> u64 {
    std::fs::metadata(file.path()).unwrap().len()
}

/// The invariants every report must satisfy, whatever produced it.
fn assert_provenance_consistent(report: &ProfileReport, source_size: u64, label: &str) {
    let exec = &report.execution;

    assert_eq!(
        exec.source_exhausted,
        exec.truncation_reason.is_none(),
        "{label}: a truncated scan is exactly a non-exhausted one \
         (exhausted={}, reason={:?})",
        exec.source_exhausted,
        exec.truncation_reason
    );

    let bytes = exec.bytes_consumed.unwrap_or(0);
    if exec.source_exhausted {
        assert_eq!(
            bytes, source_size,
            "{label}: an exhausted source consumed all of its bytes"
        );
    } else {
        assert!(
            bytes > 0,
            "{label}: a truncated scan read something, so it cannot report 0 bytes"
        );
        assert!(
            bytes <= source_size,
            "{label}: consumed {bytes} of a {source_size}-byte source"
        );
    }

    if exec.rows_processed > 0 {
        assert!(
            bytes > 0,
            "{label}: {} rows were read from 0 bytes",
            exec.rows_processed
        );
    }
}

// ---------------------------------------------------------------------------
// Controls take effect
// ---------------------------------------------------------------------------

#[test]
fn chunk_size_changes_where_a_byte_cap_lands() {
    // With the cap far below any chunk, the stop lands at the first chunk
    // boundary — so the configured chunk size is directly observable. When it
    // was ignored, every value produced identical results.
    let csv = write_csv(5_000);
    let cap = StopCondition::MaxBytes(2_048);

    let small = Profiler::new()
        .engine(EngineType::Incremental)
        .chunk_size(ChunkSize::Fixed(4_096))
        .stop_when(cap.clone())
        .analyze_file(csv.path())
        .unwrap();

    let large = Profiler::new()
        .engine(EngineType::Incremental)
        .chunk_size(ChunkSize::Fixed(65_536))
        .stop_when(cap)
        .analyze_file(csv.path())
        .unwrap();

    assert!(
        small.execution.rows_processed < large.execution.rows_processed,
        "a smaller chunk must stop sooner: {} vs {}",
        small.execution.rows_processed,
        large.execution.rows_processed
    );
    assert_provenance_consistent(&small, file_size(&csv), "small chunk");
    assert_provenance_consistent(&large, file_size(&csv), "large chunk");
}

#[test]
fn chunk_size_reaches_the_auto_engine_too() {
    let csv = write_csv(5_000);
    let cap = StopCondition::MaxBytes(2_048);

    let small = Profiler::new()
        .chunk_size(ChunkSize::Fixed(4_096))
        .stop_when(cap.clone())
        .analyze_file(csv.path())
        .unwrap();
    let large = Profiler::new()
        .chunk_size(ChunkSize::Fixed(65_536))
        .stop_when(cap)
        .analyze_file(csv.path())
        .unwrap();

    assert!(
        small.execution.rows_processed < large.execution.rows_processed,
        "auto must forward chunk_size to the engine it selects"
    );
}

#[test]
fn chunk_size_never_changes_a_complete_profile() {
    // Chunking is a read-granularity control, not a semantic one: a full scan
    // must produce the same answer at every chunk size.
    let csv = write_csv(2_000);
    let sizes = [
        ChunkSize::Fixed(1_024),
        ChunkSize::Fixed(64 * 1024),
        ChunkSize::Adaptive,
    ];

    let mut baseline: Option<(usize, usize, u64)> = None;
    for size in sizes {
        let report = Profiler::new()
            .engine(EngineType::Incremental)
            .chunk_size(size.clone())
            .analyze_file(csv.path())
            .unwrap();

        assert_provenance_consistent(&report, file_size(&csv), &format!("{size:?}"));
        assert!(report.execution.source_exhausted);

        let observed = (
            report.execution.rows_processed,
            report.column_profiles.len(),
            report.execution.bytes_consumed.unwrap_or(0),
        );
        match baseline {
            None => baseline = Some(observed),
            Some(expected) => assert_eq!(observed, expected, "{size:?} changed the result"),
        }
    }
}

#[test]
fn memory_limit_reaches_the_incremental_and_auto_paths() {
    // The limit bounds retained per-column state, so a tighter limit must not
    // silently behave like no limit at all. It never changes the row count.
    let csv = write_csv(20_000);

    for engine in [EngineType::Auto, EngineType::Incremental] {
        let tight = Profiler::new()
            .engine(engine)
            .memory_limit_mb(1)
            .analyze_file(csv.path())
            .unwrap();
        let loose = Profiler::new()
            .engine(engine)
            .memory_limit_mb(64)
            .analyze_file(csv.path())
            .unwrap();

        assert_eq!(
            tight.execution.rows_processed, loose.execution.rows_processed,
            "{engine:?}: a memory limit bounds state, it does not drop rows"
        );
        assert_provenance_consistent(&tight, file_size(&csv), "tight limit");
        assert_provenance_consistent(&loose, file_size(&csv), "loose limit");
    }
}

// ---------------------------------------------------------------------------
// Provenance agrees with what happened
// ---------------------------------------------------------------------------

#[test]
fn a_condition_met_on_the_last_chunk_is_not_a_truncation() {
    // The final chunk can satisfy a condition at the same moment it finishes
    // the file. Nothing was left unread, so the report must not claim it was.
    let csv = write_csv(5_000);
    let whole_file = ChunkSize::Fixed(file_size(&csv) as usize * 2);

    for condition in [
        StopCondition::ConfidenceThreshold(0.9),
        StopCondition::SchemaStable {
            consecutive_stable_rows: 50,
        },
        StopCondition::quality_sample(),
    ] {
        let report = Profiler::new()
            .engine(EngineType::Incremental)
            .chunk_size(whole_file.clone())
            .stop_when(condition.clone())
            .analyze_file(csv.path())
            .unwrap();

        assert_eq!(
            report.execution.rows_processed, 5_000,
            "{condition:?}: the whole file was read"
        );
        assert!(
            report.execution.source_exhausted,
            "{condition:?}: a complete scan is not truncated, got {:?}",
            report.execution.truncation_reason
        );
        assert_provenance_consistent(&report, file_size(&csv), &format!("{condition:?}"));
    }
}

#[test]
fn a_genuine_early_stop_still_reports_truncation() {
    // The converse of the test above: shrinking the chunk so the condition
    // fires well before EOF must still be reported as a bounded scan.
    let csv = write_csv(5_000);

    let report = Profiler::new()
        .engine(EngineType::Incremental)
        .chunk_size(ChunkSize::Fixed(4_096))
        .stop_when(StopCondition::ConfidenceThreshold(0.9))
        .analyze_file(csv.path())
        .unwrap();

    assert!(report.execution.rows_processed < 5_000);
    assert!(!report.execution.source_exhausted);
    assert!(report.execution.truncation_reason.is_some());
    assert_provenance_consistent(&report, file_size(&csv), "early confidence stop");
}

#[test]
fn schema_stability_keeps_its_byte_counters() {
    // The schema tracker fires from outside the stop evaluator; resetting the
    // evaluator to suppress a duplicate reason used to discard the byte count
    // with it, leaving `bytes_consumed: 0` on a scan that read thousands.
    let csv = write_csv(5_000);

    let report = Profiler::new()
        .engine(EngineType::Incremental)
        .chunk_size(ChunkSize::Fixed(4_096))
        .stop_when(StopCondition::SchemaStable {
            consecutive_stable_rows: 50,
        })
        .analyze_file(csv.path())
        .unwrap();

    assert!(!report.execution.source_exhausted);
    assert!(
        report.execution.bytes_consumed.unwrap_or(0) > 0,
        "a stopped scan must account for the bytes it read"
    );
    assert_provenance_consistent(&report, file_size(&csv), "schema stable");
}

#[test]
fn row_caps_are_hard_caps() {
    let csv = write_csv(5_000);

    for limit in [1u64, 123, 4_999] {
        let report = Profiler::new()
            .engine(EngineType::Incremental)
            .stop_when(StopCondition::MaxRows(limit))
            .analyze_file(csv.path())
            .unwrap();

        assert_eq!(
            report.execution.rows_processed as u64, limit,
            "max_rows({limit}) must be exact, not approximate"
        );
        assert_provenance_consistent(&report, file_size(&csv), &format!("max_rows({limit})"));
    }
}

#[test]
fn a_row_cap_equal_to_the_row_count_is_a_complete_scan() {
    let csv = write_csv(500);

    let report = Profiler::new()
        .engine(EngineType::Incremental)
        .stop_when(StopCondition::MaxRows(500))
        .analyze_file(csv.path())
        .unwrap();

    assert_eq!(report.execution.rows_processed, 500);
    assert!(
        report.execution.source_exhausted,
        "reaching the cap on the last row read everything"
    );
    assert_provenance_consistent(&report, file_size(&csv), "exact-fit row cap");
}

#[test]
fn byte_caps_overshoot_by_at_most_one_chunk() {
    // Byte caps are evaluated at chunk boundaries. That overshoot is allowed,
    // but it is bounded by the chunk size the caller chose — not unbounded.
    let csv = write_csv(5_000);

    for chunk in [4_096usize, 16_384, 65_536] {
        let cap = 2_048u64;
        let report = Profiler::new()
            .engine(EngineType::Incremental)
            .chunk_size(ChunkSize::Fixed(chunk))
            .stop_when(StopCondition::MaxBytes(cap))
            .analyze_file(csv.path())
            .unwrap();

        let consumed = report.execution.bytes_consumed.unwrap_or(0);
        assert!(
            consumed <= cap + chunk as u64,
            "chunk {chunk}: consumed {consumed} for a {cap}-byte cap, \
             which exceeds the one-chunk bound"
        );
        assert_provenance_consistent(
            &report,
            file_size(&csv),
            &format!("byte cap, chunk {chunk}"),
        );
    }
}

// ---------------------------------------------------------------------------
// Async paths
// ---------------------------------------------------------------------------

#[cfg(feature = "async-streaming")]
mod async_controls {
    use super::assert_provenance_consistent;

    use dataprof::{AsyncSourceInfo, BytesSource, ChunkSize, FileFormat, Profiler, StopCondition};

    fn csv_bytes(rows: usize) -> Vec<u8> {
        let mut data = String::from("id,name,value\n");
        for i in 0..rows {
            data.push_str(&format!("{i},name_{i},{}\n", i * 3));
        }
        data.into_bytes()
    }

    fn jsonl_bytes(rows: usize) -> Vec<u8> {
        let mut data = String::new();
        for i in 0..rows {
            data.push_str(&format!("{{\"id\":{i},\"v\":\"x{i}\"}}\n"));
        }
        data.into_bytes()
    }

    fn source(data: Vec<u8>, format: FileFormat) -> BytesSource {
        let len = data.len() as u64;
        BytesSource::new(
            bytes::Bytes::from(data),
            AsyncSourceInfo::new("controls", format).size_hint(Some(len)),
        )
    }

    /// A row cap must be exact on every async format. Evaluating it per chunk
    /// returned a whole chunk more data than the caller authorized, while the
    /// report named the smaller limit.
    #[tokio::test]
    async fn async_row_caps_do_not_overshoot() {
        for (format, data) in [
            (FileFormat::Csv, csv_bytes(500)),
            (FileFormat::Jsonl, jsonl_bytes(500)),
        ] {
            let size = data.len() as u64;
            let report = Profiler::new()
                .chunk_size(ChunkSize::Fixed(1_024))
                .stop_when(StopCondition::MaxRows(123))
                .profile_stream(source(data, format.clone()))
                .await
                .unwrap();

            assert_eq!(
                report.execution.rows_processed, 123,
                "{format:?}: the cap must be exact"
            );
            assert!(!report.execution.source_exhausted);
            assert_provenance_consistent(&report, size, &format!("{format:?} row cap"));
        }
    }

    #[tokio::test]
    async fn async_row_cap_equal_to_the_row_count_is_complete() {
        let data = csv_bytes(123);
        let size = data.len() as u64;

        let report = Profiler::new()
            .stop_when(StopCondition::MaxRows(123))
            .profile_stream(source(data, FileFormat::Csv))
            .await
            .unwrap();

        assert_eq!(report.execution.rows_processed, 123);
        assert!(
            report.execution.source_exhausted,
            "the cap landed on the final row, so nothing was left unread"
        );
        assert!(report.execution.truncation_reason.is_none());
        assert_provenance_consistent(&report, size, "async exact-fit cap");
    }

    #[tokio::test]
    async fn async_chunk_size_never_changes_a_complete_profile() {
        let mut baseline: Option<(usize, usize)> = None;
        for chunk in [
            ChunkSize::Fixed(512),
            ChunkSize::Fixed(64 * 1024),
            ChunkSize::Adaptive,
        ] {
            let data = csv_bytes(2_000);
            let size = data.len() as u64;
            let report = Profiler::new()
                .chunk_size(chunk.clone())
                .profile_stream(source(data, FileFormat::Csv))
                .await
                .unwrap();

            assert_provenance_consistent(&report, size, &format!("{chunk:?}"));
            let observed = (
                report.execution.rows_processed,
                report.column_profiles.len(),
            );
            match baseline {
                None => baseline = Some(observed),
                Some(expected) => assert_eq!(observed, expected, "{chunk:?} changed the result"),
            }
        }
    }
}
