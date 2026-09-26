//! Start/end dates are compared only when they come from the same row (#787).
//!
//! The quality calculators read each column's retained values, which hold
//! non-null values only. Once either column of a pair has a null, value `k` of
//! one column and value `k` of the other come from different rows, and zipping
//! them counted violations nobody made. Such a pair is now not compared, which
//! the findings report as not assessed. A pair of null-free columns still is,
//! because both columns' samplers keep the same rows.

use std::collections::HashMap;
use std::io::Write;

use dataprof::{
    ColumnProfile, ColumnStats, DataType, EngineType, FindingCode, Profiler, QualityMetrics,
    TextStats,
};

const ENGINES: [EngineType; 3] = [
    EngineType::Auto,
    EngineType::Incremental,
    EngineType::Columnar,
];

fn write_csv(contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    write!(file, "{contents}").unwrap();
    file.flush().unwrap();
    file
}

fn pairs(report: &dataprof::ProfileReport) -> (usize, usize) {
    let timeliness = report
        .quality
        .as_ref()
        .and_then(|quality| quality.metrics.timeliness.as_ref())
        .expect("timeliness assessed");
    (
        timeliness.temporal_pairs_checked,
        timeliness.temporal_violations,
    )
}

#[test]
fn a_pair_with_nulls_is_not_compared_on_any_engine() {
    // The issue's example: no row has its start after its end, but zipping
    // the null-free values paired 2024-01-05 with 2024-01-02, and 2024-01-10
    // with 2024-01-06, and reported two violations.
    let csv = write_csv(
        "start_date,end_date\n,2024-01-02\n2024-01-05,2024-01-06\n2024-01-10,2024-01-11\n",
    );
    for engine in ENGINES {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap();
        assert_eq!(pairs(&report), (0, 0), "{engine:?}");
        let findings = report.findings();
        assert!(
            !findings
                .findings
                .iter()
                .any(|finding| finding.code == FindingCode::TemporalOrderViolations),
            "{engine:?} reported violations nobody made"
        );
        assert!(
            findings
                .not_evaluated
                .iter()
                .any(|rule| rule.code == FindingCode::TemporalOrderViolations),
            "{engine:?} must say the ordering was not assessed"
        );
    }
}

#[test]
fn whole_columns_with_nulls_in_place_are_still_compared() {
    // A caller of `calculate_from_data` passes every row, nulls in place, so
    // value `k` of each column is row `k` even with nulls. Rows with a null on
    // either side are skipped; the others are compared.
    let column = |name: &str, values: &[&str]| {
        (
            name.to_string(),
            values
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>(),
        )
    };
    let data = HashMap::from([
        column(
            "start_date",
            &["2023-06-01", "", "2023-01-01", "2023-02-01"],
        ),
        column("end_date", &["2023-01-01", "", "2023-06-01", ""]),
    ]);
    let profile = |name: &str, nulls: usize| ColumnProfile {
        name: name.to_string(),
        data_type: DataType::Date,
        null_count: nulls,
        total_count: 4,
        unique_count: None,
        unique_count_is_approximate: None,
        invalid_count: None,
        type_homogeneity: None,
        stats: ColumnStats::Text(TextStats::from_lengths(10, 10, 10.0)),
        patterns: None,
    };
    let metrics = QualityMetrics::calculate_from_data(
        &data,
        &[profile("start_date", 1), profile("end_date", 2)],
    )
    .unwrap();
    let timeliness = metrics.timeliness.expect("timeliness assessed");
    assert_eq!(timeliness.temporal_pairs_checked, 2);
    assert_eq!(timeliness.temporal_violations, 1);
}

#[test]
fn a_null_free_pair_is_still_compared_row_by_row() {
    let csv = write_csv(
        "start_date,end_date,note\n2024-01-05,2024-01-02,\n2024-01-05,2024-01-06,x\n2024-01-10,2024-01-11,\n",
    );
    for engine in ENGINES {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap();
        // A null in another column does not matter; the one inversion is real.
        assert_eq!(pairs(&report), (3, 1), "{engine:?}");
    }
}

#[test]
fn null_free_columns_keep_the_same_rows_past_the_sample() {
    // Every end date is the day after its start, so any start compared with
    // another row's end would show up as violations. 20,000 rows is past the
    // 10,000-value reservoir, where each column samples on its own.
    let mut contents = String::from("id,start_date,end_date,note\n");
    for id in 0..20_000u64 {
        // Scatter the dates so no two neighbouring rows are ordered alike.
        let day = id * 7_919 % 2_000;
        let (year, month, day) = (2020 + day / 324, 1 + day % 324 / 27, 1 + day % 27);
        let note = if id % 3 == 0 { "x" } else { "" };
        contents.push_str(&format!(
            "{id},{year}-{month:02}-{day:02},{year}-{month:02}-{:02},{note}\n",
            day + 1
        ));
    }
    let csv = write_csv(&contents);
    for engine in ENGINES {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap();
        let (checked, violations) = pairs(&report);
        assert_eq!(checked, 10_000, "{engine:?}");
        assert_eq!(
            violations, 0,
            "{engine:?} paired values from different rows"
        );
    }
}
