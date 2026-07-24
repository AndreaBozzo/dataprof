//! Every exposed sampling strategy must actually sample, on every path that
//! accepts one, and the report must say what it did. Guards #459.
//!
//! Two contracts:
//!
//! 1. **No silent ignore.** A path either applies the strategy or refuses it
//!    before reading. A full profile returned under a sampling request is the
//!    worst outcome: the caller's memory and time assumptions are discarded and
//!    the report looks complete.
//! 2. **Provenance matches.** `sampling_applied` and `sampling_ratio` describe
//!    the rows that reached the profile, and sampling never claims the source
//!    was left unread — that is what `truncation_reason` is for.

use std::io::Write;

use dataprof::{EngineType, ProfileReport, Profiler, SamplingStrategy, StopCondition};

const ROWS: usize = 100;
const GROUPS: usize = 5;

/// 100 rows: `group` cycles over 5 values, `weight` over 0..9, `value` ascends.
fn write_csv() -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    writeln!(file, "group,weight,value").unwrap();
    for i in 0..ROWS {
        writeln!(file, "g{},{},{}", i % GROUPS, i % 10, 100 + i).unwrap();
    }
    file.flush().unwrap();
    file
}

fn strategies() -> Vec<(&'static str, SamplingStrategy, usize)> {
    vec![
        (
            "random",
            SamplingStrategy::Random { size: 10 },
            10, // a uniform sample of exactly 10 rows
        ),
        ("reservoir", SamplingStrategy::Reservoir { size: 10 }, 10),
        (
            "stratified",
            SamplingStrategy::Stratified {
                key_columns: vec!["group".into()],
                samples_per_stratum: 2,
            },
            GROUPS * 2, // 2 rows from each of the 5 groups
        ),
        (
            "progressive",
            SamplingStrategy::Progressive {
                initial_size: 5,
                confidence_level: 0.95,
                max_size: 10,
            },
            10, // grows to the cap on this spread of values
        ),
        (
            "systematic",
            SamplingStrategy::Systematic { interval: 10 },
            10,
        ),
        (
            "importance",
            SamplingStrategy::Importance {
                weight_column: "weight".into(),
                weight_threshold: 8.0,
            },
            20, // weights cycle 0..9, so 8 and 9 qualify
        ),
        (
            "multi_stage",
            SamplingStrategy::MultiStage {
                stages: vec![
                    SamplingStrategy::Systematic { interval: 2 },
                    SamplingStrategy::Reservoir { size: 10 },
                ],
            },
            10, // the filter halves the rows, the reservoir bounds the sample
        ),
    ]
}

fn assert_sampled(report: &ProfileReport, expected_rows: usize, label: &str) {
    assert_eq!(
        report.execution.rows_processed, expected_rows,
        "{label}: sampled row count"
    );
    assert!(
        report.execution.sampling_applied,
        "{label}: a report built from {expected_rows} of {ROWS} rows must say so"
    );

    let ratio = report
        .execution
        .sampling_ratio
        .unwrap_or_else(|| panic!("{label}: sampling_applied without a ratio"));
    let expected_ratio = expected_rows as f64 / ROWS as f64;
    assert!(
        (ratio - expected_ratio).abs() < 1e-9,
        "{label}: ratio {ratio} does not match {expected_rows}/{ROWS}"
    );

    // Sampling is not truncation: the whole source was still read.
    assert!(
        report.execution.source_exhausted,
        "{label}: sampling must not mark a fully read source as unexhausted"
    );
    assert!(report.execution.truncation_reason.is_none(), "{label}");
}

#[test]
fn every_strategy_samples_on_the_incremental_engine() {
    let csv = write_csv();
    for (name, strategy, expected) in strategies() {
        let report = Profiler::new()
            .engine(EngineType::Incremental)
            .sampling(strategy)
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_sampled(&report, expected, name);
    }
}

/// The documented default. `engine="auto"` used to drop the strategy entirely,
/// so the most natural call silently profiled everything.
#[test]
fn every_strategy_samples_on_the_default_engine() {
    let csv = write_csv();
    for (name, strategy, expected) in strategies() {
        let report = Profiler::new()
            .sampling(strategy)
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_sampled(&report, expected, name);
    }
}

#[test]
fn auto_and_incremental_agree_on_every_strategy() {
    let csv = write_csv();
    for (name, strategy, _) in strategies() {
        let auto = Profiler::new()
            .sampling(strategy.clone())
            .analyze_file(csv.path())
            .unwrap();
        let incremental = Profiler::new()
            .engine(EngineType::Incremental)
            .sampling(strategy)
            .analyze_file(csv.path())
            .unwrap();

        assert_eq!(
            auto.execution.rows_processed, incremental.execution.rows_processed,
            "{name}: engines disagree on the sample size"
        );
        assert_eq!(
            auto.execution.sampling_ratio, incremental.execution.sampling_ratio,
            "{name}: engines disagree on the sampling ratio"
        );
    }
}

#[test]
fn no_sampling_reports_no_sampling() {
    let csv = write_csv();
    let report = Profiler::new()
        .sampling(SamplingStrategy::None)
        .analyze_file(csv.path())
        .unwrap();

    assert_eq!(report.execution.rows_processed, ROWS);
    assert!(!report.execution.sampling_applied);
    assert!(report.execution.sampling_ratio.is_none());
}

#[test]
fn a_strategy_that_keeps_every_row_is_not_sampling() {
    // An interval of 1 takes every row. Nothing was dropped, so the report must
    // not claim a sample was taken.
    let csv = write_csv();
    let report = Profiler::new()
        .sampling(SamplingStrategy::Systematic { interval: 1 })
        .analyze_file(csv.path())
        .unwrap();

    assert_eq!(report.execution.rows_processed, ROWS);
    assert!(!report.execution.sampling_applied);
}

#[test]
fn a_fixed_size_sample_larger_than_the_source_keeps_everything() {
    let csv = write_csv();
    let report = Profiler::new()
        .sampling(SamplingStrategy::Reservoir { size: ROWS * 10 })
        .analyze_file(csv.path())
        .unwrap();

    assert_eq!(report.execution.rows_processed, ROWS);
    assert!(!report.execution.sampling_applied);
}

#[test]
fn the_sample_drives_the_statistics() {
    // A profile built from a sample must describe the sample. With a systematic
    // interval of 10 the `value` column holds 100, 110, ... 190 — its minimum
    // is the first row's value and its maximum is far below the source's.
    let csv = write_csv();
    let report = Profiler::new()
        .sampling(SamplingStrategy::Systematic { interval: 10 })
        .analyze_file(csv.path())
        .unwrap();

    let value = report
        .column_profiles
        .iter()
        .find(|c| c.name == "value")
        .expect("value column");
    assert_eq!(value.total_count, 10, "statistics cover only the sample");
}

#[test]
fn a_fixed_size_sample_produces_statistics_over_exactly_that_sample() {
    // The reservoir is only final at end of stream. If rows were folded in as
    // they were provisionally selected, evicted rows would leave their values
    // behind and the column counts would exceed the sample size.
    let csv = write_csv();
    let report = Profiler::new()
        .sampling(SamplingStrategy::Reservoir { size: 10 })
        .analyze_file(csv.path())
        .unwrap();

    for column in &report.column_profiles {
        assert_eq!(
            column.total_count, 10,
            "column '{}' carries values from outside the final sample",
            column.name
        );
    }
}

// ---------------------------------------------------------------------------
// Sampling composed with a row cap
// ---------------------------------------------------------------------------

/// A row cap is a hard ceiling on what reaches the profile, whatever the
/// strategy. A fixed-size sample folds nothing in until the scan ends, so a cap
/// checked against folded rows never fired and `random(100)` under
/// `max_rows(20)` returned 100 rows — over the caller's ceiling, and reported
/// as a complete scan.
#[test]
fn a_row_cap_bounds_every_strategy() {
    let csv = write_csv();

    for (name, strategy, _) in strategies() {
        for limit in [1u64, 5, 20] {
            let report = Profiler::new()
                .engine(EngineType::Incremental)
                .sampling(strategy.clone())
                .stop_when(StopCondition::MaxRows(limit))
                .analyze_file(csv.path())
                .unwrap_or_else(|e| panic!("{name}: {e}"));

            assert!(
                report.execution.rows_processed as u64 <= limit,
                "{name} under max_rows({limit}) produced {} rows",
                report.execution.rows_processed
            );
        }
    }
}

#[test]
fn a_row_cap_below_a_fixed_size_sample_wins() {
    // The cap bounds the rows read; the sample is drawn from those, so it can
    // only be smaller than the cap, never larger.
    let csv = write_csv();
    let report = Profiler::new()
        .sampling(SamplingStrategy::Reservoir { size: 50 })
        .stop_when(StopCondition::MaxRows(10))
        .analyze_file(csv.path())
        .unwrap();

    assert_eq!(report.execution.rows_processed, 10);
    assert!(!report.execution.source_exhausted);
    assert!(report.execution.truncation_reason.is_some());
}

// ---------------------------------------------------------------------------
// Stratum identity
// ---------------------------------------------------------------------------

#[test]
fn strata_are_not_merged_by_values_containing_the_separator() {
    // ("a|b", "c") and ("a", "b|c") join to the same string under a naive
    // separator, which would merge two strata and apply one cap to both.
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    writeln!(file, "left,right,value").unwrap();
    for i in 0..10 {
        writeln!(file, "a|b,c,{i}").unwrap();
        writeln!(file, "a,b|c,{i}").unwrap();
    }
    file.flush().unwrap();

    let report = Profiler::new()
        .sampling(SamplingStrategy::Stratified {
            key_columns: vec!["left".into(), "right".into()],
            samples_per_stratum: 3,
        })
        .analyze_file(file.path())
        .unwrap();

    assert_eq!(
        report.execution.rows_processed, 6,
        "two distinct strata, three rows each"
    );
}

// ---------------------------------------------------------------------------
// Paths that cannot sample must say so
// ---------------------------------------------------------------------------

#[test]
fn the_columnar_engine_rejects_sampling() {
    let csv = write_csv();
    let error = Profiler::new()
        .engine(EngineType::Columnar)
        .sampling(SamplingStrategy::Reservoir { size: 10 })
        .analyze_file(csv.path())
        .expect_err("the columnar engine cannot sample row by row");

    let message = error.to_string();
    assert!(message.contains("sampling"), "unexpected error: {message}");
    assert!(
        message.contains("incremental"),
        "the error must name a path that can sample: {message}"
    );
}

#[test]
fn the_json_parser_rejects_sampling() {
    let mut file = tempfile::Builder::new()
        .suffix(".jsonl")
        .tempfile()
        .unwrap();
    for i in 0..50 {
        writeln!(file, "{{\"id\":{i}}}").unwrap();
    }
    file.flush().unwrap();

    let error = Profiler::new()
        .sampling(SamplingStrategy::Reservoir { size: 10 })
        .analyze_file(file.path())
        .expect_err("the JSON parser cannot sample row by row");
    assert!(error.to_string().contains("sampling"));
}

// ---------------------------------------------------------------------------
// Strategies that cannot be composed must be refused before reading
// ---------------------------------------------------------------------------

#[test]
fn a_multi_stage_with_two_fixed_size_stages_is_refused() {
    let csv = write_csv();
    let error = Profiler::new()
        .sampling(SamplingStrategy::MultiStage {
            stages: vec![
                SamplingStrategy::Reservoir { size: 10 },
                SamplingStrategy::Random { size: 5 },
            ],
        })
        .analyze_file(csv.path())
        .expect_err("two fixed-size stages have no combined meaning");
    assert!(
        error.to_string().contains("at most one fixed-size stage"),
        "unexpected error: {error}"
    );
}

#[test]
fn a_multi_stage_with_a_filter_after_the_sample_is_refused() {
    let csv = write_csv();
    let error = Profiler::new()
        .sampling(SamplingStrategy::MultiStage {
            stages: vec![
                SamplingStrategy::Reservoir { size: 10 },
                SamplingStrategy::Systematic { interval: 2 },
            ],
        })
        .analyze_file(csv.path())
        .expect_err("a filter after the reservoir has nothing to filter");
    assert!(
        error.to_string().contains("must be the last stage"),
        "unexpected error: {error}"
    );
}

// ---------------------------------------------------------------------------
// Async byte streams
// ---------------------------------------------------------------------------

#[cfg(feature = "async-streaming")]
mod async_sampling {
    use super::{GROUPS, ROWS, assert_sampled, strategies};

    use dataprof::{AsyncSourceInfo, BytesSource, FileFormat, Profiler, SamplingStrategy};

    fn csv_bytes() -> Vec<u8> {
        let mut data = String::from("group,weight,value\n");
        for i in 0..ROWS {
            data.push_str(&format!("g{},{},{}\n", i % GROUPS, i % 10, 100 + i));
        }
        data.into_bytes()
    }

    fn source() -> BytesSource {
        let data = csv_bytes();
        let len = data.len() as u64;
        BytesSource::new(
            bytes::Bytes::from(data),
            AsyncSourceInfo::new("sampling", FileFormat::Csv).size_hint(Some(len)),
        )
    }

    #[tokio::test]
    async fn every_strategy_samples_over_a_byte_stream() {
        for (name, strategy, expected) in strategies() {
            let report = Profiler::new()
                .sampling(strategy)
                .profile_stream(source())
                .await
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_sampled(&report, expected, name);
        }
    }

    #[tokio::test]
    async fn async_agrees_with_the_file_path() {
        use std::io::Write;

        let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
        file.write_all(&csv_bytes()).unwrap();
        file.flush().unwrap();

        for (name, strategy, _) in strategies() {
            let from_file = Profiler::new()
                .sampling(strategy.clone())
                .analyze_file(file.path())
                .unwrap();
            let from_stream = Profiler::new()
                .sampling(strategy)
                .profile_stream(source())
                .await
                .unwrap();

            assert_eq!(
                from_stream.execution.rows_processed, from_file.execution.rows_processed,
                "{name}: file and stream disagree on the sample size"
            );
        }
    }

    #[tokio::test]
    async fn an_unusable_strategy_fails_before_the_stream_is_read() {
        let error = Profiler::new()
            .sampling(SamplingStrategy::MultiStage {
                stages: vec![
                    SamplingStrategy::Reservoir { size: 10 },
                    SamplingStrategy::Random { size: 5 },
                ],
            })
            .profile_stream(source())
            .await
            .expect_err("two fixed-size stages have no combined meaning");
        assert!(error.to_string().contains("at most one fixed-size stage"));
    }
}
