//! `complete_records_ratio` must count rows that have no nulls.
//!
//! The metric is published as a percentage of complete records, so it has to
//! be measured on rows. Deriving it from per-column null totals assumes nulls
//! never share a row, which understates it — without bound — on exactly the
//! datasets it is meant to describe: a few optional columns that are empty
//! together.

use std::io::Write;

use dataprof::{
    CsvParserConfig, EngineType, JsonParserConfig, ProfileReport, Profiler, analyze_csv_file,
    analyze_json_file,
};
use tempfile::NamedTempFile;

fn completeness_ratio(report: &ProfileReport) -> f64 {
    report
        .quality
        .as_ref()
        .expect("quality assessed")
        .metrics
        .completeness
        .as_ref()
        .expect("completeness assessed")
        .complete_records_ratio
}

/// 10 rows; `notes` empty in 7, `city` empty in 3, and every `city` null
/// falls on a row that is already missing `notes`. Rows 8-10 are complete,
/// so the honest answer is 30%.
fn co_occurring_nulls_csv() -> NamedTempFile {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "id,notes,city").unwrap();
    for id in 1..=3 {
        writeln!(f, "{id},,").unwrap();
    }
    for id in 4..=7 {
        writeln!(f, "{id},,Rome").unwrap();
    }
    for id in 8..=10 {
        writeln!(f, "{id},ok,Rome").unwrap();
    }
    f.flush().unwrap();
    f
}

#[test]
fn complete_records_ratio_counts_rows_not_null_cells() {
    let csv = co_occurring_nulls_csv();
    let report = analyze_csv_file(csv.path(), &CsvParserConfig::default())
        .expect("CSV analysis should succeed");

    let ratio = completeness_ratio(&report);
    assert!(
        (ratio - 30.0).abs() < 0.01,
        "3 of 10 rows have no nulls, so complete_records_ratio should be 30.0, got {ratio}"
    );
}

/// The output contract: the same data must profile to the same numbers on
/// every engine. The count is row-level, and each engine reaches rows its
/// own way — record by record, or batch by batch through Arrow.
#[test]
fn complete_records_ratio_agrees_across_engines() {
    let csv = co_occurring_nulls_csv();
    let engines = [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ];

    for engine in engines {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap_or_else(|error| panic!("{engine:?} analysis should succeed: {error}"));
        let ratio = completeness_ratio(&report);
        assert!(
            (ratio - 30.0).abs() < 0.01,
            "{engine:?} reported complete_records_ratio {ratio}, expected 30.0"
        );
    }
}

/// A record missing a field that only later records introduce is incomplete,
/// even though it was counted before the field was known to exist.
#[test]
fn complete_records_ratio_accounts_for_late_json_fields() {
    let mut json = NamedTempFile::with_suffix(".jsonl").unwrap();
    writeln!(json, r#"{{"id": 1}}"#).unwrap();
    writeln!(json, r#"{{"id": 2, "city": "Rome"}}"#).unwrap();
    json.flush().unwrap();

    let report = analyze_json_file(json.path(), &JsonParserConfig::default())
        .expect("JSON analysis should succeed");
    let ratio = completeness_ratio(&report);
    assert!(
        (ratio - 50.0).abs() < 0.01,
        "the first record has no `city`, so 1 of 2 records is complete, got {ratio}"
    );
}

/// Sampling shrinks the retained sample, never the counters: the ratio is
/// accumulated over every row the engine read.
#[test]
fn complete_records_ratio_survives_sampling() {
    let mut csv = NamedTempFile::new().unwrap();
    writeln!(csv, "id,notes,city").unwrap();
    // 40k rows, well past the per-column sample reservoirs. Every fourth row
    // is missing both optional fields, so exactly 75% of records are
    // complete while the null cells number half the rows.
    for id in 0..40_000 {
        if id % 4 == 0 {
            writeln!(csv, "{id},,").unwrap();
        } else {
            writeln!(csv, "{id},ok,Rome").unwrap();
        }
    }
    csv.flush().unwrap();

    let report = analyze_csv_file(csv.path(), &CsvParserConfig::default())
        .expect("CSV analysis should succeed");
    let ratio = completeness_ratio(&report);
    assert!(
        (ratio - 75.0).abs() < 0.01,
        "3 of every 4 rows are complete, so the ratio should be 75.0, got {ratio}"
    );
}

#[test]
fn complete_records_ratio_is_exact_when_every_row_is_complete() {
    let mut csv = NamedTempFile::new().unwrap();
    writeln!(csv, "id,city").unwrap();
    for id in 1..=5 {
        writeln!(csv, "{id},Rome").unwrap();
    }
    csv.flush().unwrap();

    let report = analyze_csv_file(csv.path(), &CsvParserConfig::default())
        .expect("CSV analysis should succeed");
    assert!((completeness_ratio(&report) - 100.0).abs() < 0.01);
}
