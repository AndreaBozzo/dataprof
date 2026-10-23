//! Leading fieldless JSON records belong to the row-level counts (#1021).
//!
//! Columns already include these rows as missing values. Completeness and
//! duplicates must describe the same records, including when an async chunk
//! knows columns that a row cap prevents the profile from reaching.

use std::io::Write;

use dataprof::{EngineType, FileFormat, ProfileReport, Profiler, StopCondition, TruncationReason};
use serde_json::Value;

struct Case {
    name: &'static str,
    records: &'static [&'static str],
    // In source order, with each column's expected null count.
    columns: &'static [(&'static str, usize)],
    complete_percent: f64,
    duplicates: usize,
}

const CASES: &[Case] = &[
    Case {
        name: "issue 1021: two complete records out of three",
        records: &["{}", r#"{"a":1}"#, r#"{"a":2}"#],
        columns: &[("a", 1)],
        complete_percent: 66.67,
        duplicates: 0,
    },
    Case {
        name: "empty records before and after the first key",
        records: &["{}", "{}", r#"{"a":1}"#, "{}"],
        columns: &[("a", 3)],
        complete_percent: 25.0,
        duplicates: 2,
    },
    Case {
        name: "missing, null, and empty share a signature; a null token does not",
        records: &[
            "{}",
            "{}",
            r#"{"a":null}"#,
            r#"{"a":""}"#,
            r#"{"a":"null"}"#,
            r#"{"a":1}"#,
        ],
        columns: &[("a", 5)],
        complete_percent: 16.67,
        duplicates: 3,
    },
    Case {
        name: "another key invalidates previously complete records",
        records: &["{}", r#"{"z":1}"#, r#"{"z":1,"a":2}"#, "{}"],
        columns: &[("z", 2), ("a", 3)],
        complete_percent: 25.0,
        duplicates: 1,
    },
    Case {
        name: "a fieldless record after the first key remains counted",
        records: &[r#"{"a":1}"#, "{}", r#"{"a":2}"#],
        columns: &[("a", 1)],
        complete_percent: 66.67,
        duplicates: 0,
    },
];

const CAPPED_RECORDS: &[&str] = &["{}", r#"{"a":1}"#, r#"{"a":2,"later":7}"#];
// The two-record source distinguishes a cap reached exactly at EOF from a
// truncation. The three-record source keeps `later` beyond the same cap.
const CAPS: &[(usize, u64)] = &[(2, 0), (2, 1), (2, 2), (3, 2)];

fn contents(records: &[&str], format: &FileFormat) -> String {
    match format {
        FileFormat::Json => format!("[{}]", records.join(",")),
        FileFormat::Jsonl => records.iter().map(|record| format!("{record}\n")).collect(),
        _ => unreachable!("JSON fixtures only"),
    }
}

fn from_file(
    records: &[&str],
    format: &FileFormat,
    engine: EngineType,
    cap: Option<u64>,
) -> ProfileReport {
    let suffix = if *format == FileFormat::Json {
        ".json"
    } else {
        ".jsonl"
    };
    let mut file = tempfile::NamedTempFile::with_suffix(suffix).unwrap();
    file.write_all(contents(records, format).as_bytes())
        .unwrap();
    let mut profiler = Profiler::new().engine(engine).format(format.clone());
    if let Some(cap) = cap {
        profiler = profiler.stop_when(StopCondition::MaxRows(cap));
    }
    profiler.analyze_file(file.path()).unwrap()
}

fn measured(report: &ProfileReport) -> Value {
    let document = serde_json::to_value(report).unwrap();
    serde_json::json!({
        "column_profiles": document["column_profiles"],
        "quality": document["quality"],
        "quality_status": document["quality_status"],
    })
}

fn assert_case(report: &ProfileReport, case: &Case) {
    assert_eq!(
        report.execution.rows_processed,
        case.records.len(),
        "{}",
        case.name
    );
    assert_eq!(
        report.execution.columns_detected,
        case.columns.len(),
        "{}",
        case.name
    );
    assert_eq!(report.execution.error_count, 0, "{}", case.name);
    assert!(report.execution.source_exhausted, "{}", case.name);
    let columns: Vec<_> = report
        .column_profiles
        .iter()
        .map(|column| (column.name.as_str(), column.total_count, column.null_count))
        .collect();
    let expected: Vec<_> = case
        .columns
        .iter()
        .map(|&(name, nulls)| (name, case.records.len(), nulls))
        .collect();
    assert_eq!(columns, expected, "{}", case.name);

    // Serialized percentages are the public numeric contract, including 66.67.
    let document = measured(report);
    let metrics = &document["quality"]["metrics"];
    assert_eq!(
        metrics["completeness"]["complete_records_ratio"], case.complete_percent,
        "{}",
        case.name
    );
    assert_eq!(
        metrics["uniqueness"]["rows_checked"],
        case.records.len(),
        "{}",
        case.name
    );
    assert_eq!(
        metrics["uniqueness"]["duplicate_rows"], case.duplicates,
        "{}",
        case.name
    );
    // Canonical reports omit this flag when false; the native value still
    // distinguishes exact row counts from an estimate.
    let uniqueness = report
        .quality
        .as_ref()
        .expect("quality assessed")
        .metrics
        .uniqueness
        .as_ref()
        .expect("uniqueness assessed");
    assert!(!uniqueness.duplicate_rows_approximate, "{}", case.name);
}

fn assert_fieldless(report: &ProfileReport, rows: usize) {
    assert_eq!(report.execution.rows_processed, rows);
    assert_eq!(report.execution.columns_detected, 0);
    assert_eq!(report.execution.error_count, 0);
    assert!(report.column_profiles.is_empty());
    assert_eq!(report.quality_score(), None);
    // Canonical reports omit unassessed dimensions. Native counters preserve
    // the empty-assessment contract without interpreting absence as zero.
    let metrics = &report
        .quality
        .as_ref()
        .expect("fieldless input was analyzed")
        .metrics;
    assert!(metrics.assessed_dimensions().is_empty());
    assert_eq!(
        metrics
            .completeness
            .as_ref()
            .expect("completeness requested")
            .total_cells,
        0
    );
    let uniqueness = metrics.uniqueness.as_ref().expect("uniqueness requested");
    assert_eq!(uniqueness.rows_checked, 0);
    assert_eq!(uniqueness.duplicate_rows, 0);
}

fn assert_cap(report: &ProfileReport, source_rows: usize, cap: u64) {
    let rows = source_rows.min(cap as usize);
    let exhausted = rows == source_rows;
    assert_eq!(report.execution.rows_processed, rows);
    assert_eq!(report.execution.source_exhausted, exhausted);
    assert_eq!(
        report.execution.truncation_reason,
        (!exhausted).then_some(TruncationReason::MaxRows(cap))
    );
    if rows < 2 {
        assert_fieldless(report, rows);
    } else {
        let columns: Vec<_> = report
            .column_profiles
            .iter()
            .map(|column| (column.name.as_str(), column.total_count, column.null_count))
            .collect();
        assert_eq!(columns, [("a", 2, 1)]);
        let document = measured(report);
        assert_eq!(
            document["quality"]["metrics"]["completeness"]["complete_records_ratio"],
            50.0
        );
        assert_eq!(
            document["quality"]["metrics"]["uniqueness"]["rows_checked"],
            2
        );
    }
}

#[test]
fn file_engines_count_every_fieldless_json_record() {
    for format in [FileFormat::Json, FileFormat::Jsonl] {
        for case in CASES {
            for engine in [
                EngineType::Auto,
                EngineType::Incremental,
                EngineType::Columnar,
            ] {
                assert_case(&from_file(case.records, &format, engine, None), case);
            }
        }
    }
}

#[test]
fn fieldless_sources_keep_their_shape_and_empty_assessment() {
    for format in [FileFormat::Json, FileFormat::Jsonl] {
        let empty = from_file(&[], &format, EngineType::Auto, None);
        assert_fieldless(&empty, 0);
        for engine in [
            EngineType::Auto,
            EngineType::Incremental,
            EngineType::Columnar,
        ] {
            let report = from_file(&["{}", "{}"], &format, engine, None);
            assert_fieldless(&report, 2);
            assert!(report.execution.source_exhausted);
            assert_eq!(measured(&report), measured(&empty));
        }
    }
}

#[test]
fn file_caps_count_only_consumed_records_and_columns() {
    for format in [FileFormat::Json, FileFormat::Jsonl] {
        for &(source_rows, cap) in CAPS {
            let report = from_file(
                &CAPPED_RECORDS[..source_rows],
                &format,
                EngineType::Auto,
                Some(cap),
            );
            assert_cap(&report, source_rows, cap);
        }
    }
}

#[cfg(feature = "async-streaming")]
mod asynchronous {
    use super::*;
    use std::sync::{Arc, Mutex};

    use dataprof::{AsyncSourceInfo, BytesSource, ChunkSize, ProgressEvent};

    fn chunks() -> [ChunkSize; 3] {
        [
            ChunkSize::Fixed(1),
            ChunkSize::Fixed(64),
            ChunkSize::Adaptive,
        ]
    }

    async fn from_stream(
        records: &[&str],
        format: &FileFormat,
        chunk: ChunkSize,
        cap: Option<u64>,
    ) -> (ProfileReport, Vec<ProgressEvent>) {
        let contents = contents(records, format);
        let info = AsyncSourceInfo::new("fieldless-rows", format.clone())
            .size_hint(Some(contents.len() as u64));
        let events = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&events);
        let mut profiler = Profiler::new()
            .chunk_size(chunk)
            .progress_interval(std::time::Duration::ZERO)
            .on_progress(move |event| captured.lock().unwrap().push(event));
        if let Some(cap) = cap {
            profiler = profiler.stop_when(StopCondition::MaxRows(cap));
        }
        let report = profiler
            .profile_stream(BytesSource::new(contents.into_bytes().into(), info))
            .await
            .unwrap();
        let events = events.lock().unwrap().clone();
        (report, events)
    }

    fn assert_progress(report: &ProfileReport, events: &[ProgressEvent]) {
        assert!(matches!(
            events.first(),
            Some(ProgressEvent::Started { .. })
        ));
        let schemas: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                ProgressEvent::SchemaDetected { column_names } => Some(column_names),
                _ => None,
            })
            .collect();
        let expected: Vec<_> = report
            .column_profiles
            .iter()
            .map(|column| column.name.clone())
            .collect();
        assert_eq!(schemas.last().copied(), Some(&expected));
        for pair in schemas.windows(2) {
            assert!(pair[1].starts_with(pair[0]));
            assert!(
                pair[1].len() > pair[0].len(),
                "schema events only announce growth"
            );
        }
        for schema in schemas {
            assert!(
                expected.starts_with(schema),
                "no unconsumed key is announced"
            );
        }
        match events.last() {
            Some(ProgressEvent::Finished {
                total_rows,
                truncated,
                ..
            }) => {
                assert_eq!(*total_rows, report.execution.rows_processed);
                assert_eq!(*truncated, !report.execution.source_exhausted);
            }
            other => panic!("expected a terminal Finished event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn streamed_fieldless_rows_match_files_at_every_chunk_size() {
        for format in [FileFormat::Json, FileFormat::Jsonl] {
            for case in CASES {
                let expected = from_file(case.records, &format, EngineType::Auto, None);
                for chunk in chunks() {
                    let (report, events) = from_stream(case.records, &format, chunk, None).await;
                    assert_case(&report, case);
                    assert_eq!(measured(&report), measured(&expected), "{}", case.name);
                    assert_progress(&report, &events);
                }
            }
        }
    }

    #[tokio::test]
    async fn streamed_fieldless_sources_keep_the_empty_assessment() {
        for format in [FileFormat::Json, FileFormat::Jsonl] {
            for records in [&[][..], &["{}", "{}"][..]] {
                let expected = from_file(records, &format, EngineType::Auto, None);
                for chunk in chunks() {
                    let (report, events) = from_stream(records, &format, chunk, None).await;
                    assert_fieldless(&report, records.len());
                    assert!(report.execution.source_exhausted);
                    assert_eq!(measured(&report), measured(&expected));
                    assert_progress(&report, &events);
                }
            }
        }
    }

    #[tokio::test]
    async fn streamed_caps_do_not_register_unconsumed_first_chunk_keys() {
        for format in [FileFormat::Json, FileFormat::Jsonl] {
            for &(source_rows, cap) in CAPS {
                let records = &CAPPED_RECORDS[..source_rows];
                let expected = from_file(records, &format, EngineType::Auto, Some(cap));
                for chunk in chunks() {
                    let (report, events) = from_stream(records, &format, chunk, Some(cap)).await;
                    assert_cap(&report, source_rows, cap);
                    assert_eq!(measured(&report), measured(&expected));
                    assert_progress(&report, &events);
                }
            }
        }
    }
}
