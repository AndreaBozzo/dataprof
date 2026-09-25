//! The columnar engine keeps its exact distinct sets within `memory_limit_mb`.
//!
//! Distinct counts stay exact up to a million values per column, about 19 MB
//! of fingerprints for a fully distinct column. The incremental engine spills
//! those sets under memory pressure; the columnar engine accepted a memory
//! limit and never read it, so a wide table of such columns grew without bound.

use std::io::Write;

use dataprof::{EngineType, Profiler};

fn write_csv(rows: usize) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    writeln!(file, "id,code,flag").unwrap();
    for row in 0..rows {
        let flag = if row % 2 == 0 { "yes" } else { "no" };
        writeln!(file, "{row},c{row},{flag}").unwrap();
    }
    file.flush().unwrap();
    file
}

fn approximate(report: &dataprof::ProfileReport, column: &str) -> bool {
    report
        .column_profiles
        .iter()
        .find(|profile| profile.name == column)
        .and_then(|profile| profile.unique_count_is_approximate)
        .expect("a counted column")
}

fn rows_approximate(report: &dataprof::ProfileReport) -> bool {
    report
        .quality
        .as_ref()
        .and_then(|quality| quality.metrics.uniqueness.as_ref())
        .expect("uniqueness assessed")
        .duplicate_rows_approximate
}

#[test]
fn the_columnar_engine_spills_the_largest_sets_past_its_memory_limit() {
    // Each unique column holds ~2.4 MB of fingerprints at 200,000 values, and
    // the row tracker as much again. At 4 MB (3.2 MB for the sets) both
    // column sets spill, `id` first on the tie, and the row tracker then fits.
    let csv = write_csv(200_000);
    let report = Profiler::new()
        .engine(EngineType::Columnar)
        .memory_limit_mb(4)
        .analyze_file(csv.path())
        .unwrap();
    assert!(approximate(&report, "id"));
    assert!(approximate(&report, "code"));
    assert!(!approximate(&report, "flag"), "the small set is kept");
    assert!(!rows_approximate(&report), "the row tracker spills last");

    // The default limit leaves every count exact.
    let report = Profiler::new()
        .engine(EngineType::Columnar)
        .analyze_file(csv.path())
        .unwrap();
    for column in ["id", "code", "flag"] {
        assert!(!approximate(&report, column), "{column}");
    }
    assert!(!rows_approximate(&report));
}
