//! The floating median and quartiles of a column held exactly (#1334).
//!
//! An integer column, or a column a source declares decimal, is held exactly,
//! and `stats.exact` interpolates its median and quartiles on the exact
//! values. The plain fields rounded the two middle values to `f64` first, so
//! their difference cancelled: the median of -2^53 and 2^53 + 1 was 0.0 where
//! the exact one is 0.5. They are now the exact value rounded once, on every
//! route. A float column, including decimal text with more digits than `f64`
//! keeps, is left on `f64`.
//!
//! `python/tests/test_exact_median_interpolation.py` is the Python twin; it
//! covers Arrow tables and declared decimals, the Arrow C stream and Parquet
//! bytes.

use std::io::Write;

use dataprof::{ColumnStats, EngineType, NumericStats, ProfileReport, Profiler};
use tempfile::NamedTempFile;

/// (case, values, median, q1, q2, q3); quartiles `None` below four values.
type Case = (&'static str, &'static [&'static str], f64, Option<[f64; 3]>);

const CASES: [Case; 3] = [
    (
        "the issue's pair",
        &["-9007199254740992", "9007199254740993"],
        0.5,
        None,
    ),
    (
        "four values: every quartile interpolates past 2^53",
        &[
            "-9007199254740994",
            "-9007199254740992",
            "9007199254740993",
            "9007199254740995",
        ],
        0.5,
        // -9007199254740992.5 and 9007199254740993.5 are halfway between
        // `f64` neighbours 2 apart, and round to the even one.
        Some([-9007199254740992.0, 0.5, 9007199254740994.0]),
    ),
    (
        "small integers: unchanged",
        &["1", "2", "3", "4"],
        2.5,
        Some([1.75, 2.5, 3.25]),
    ),
];

fn fixture(suffix: &str, text: &str) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    file.write_all(text.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

/// The values through every text route. JSON carries them as numbers.
fn reports(values: &[&str]) -> Vec<(String, ProfileReport)> {
    let csv = fixture(".csv", &format!("amount\n{}\n", values.join("\n")));
    let mut reports = Vec::new();
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[csv {engine:?}] {e}"));
        reports.push((format!("csv {engine:?}"), report));
    }
    let records: Vec<String> = values
        .iter()
        .map(|value| format!("{{\"amount\": {value}}}"))
        .collect();
    for (label, file) in [
        (
            "json",
            fixture(".json", &format!("[{}]", records.join(","))),
        ),
        ("jsonl", fixture(".jsonl", &(records.join("\n") + "\n"))),
    ] {
        let report = Profiler::new()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    reports
}

fn numeric(report: &ProfileReport) -> &NumericStats {
    match &report.column_profiles[0].stats {
        ColumnStats::Numeric(stats) => stats,
        other => panic!("not numeric: {other:?}"),
    }
}

/// The plain median and quartiles as `(median, [q1, q2, q3])`.
fn plain(stats: &NumericStats) -> (Option<f64>, Option<[f64; 3]>) {
    (
        stats.median,
        stats
            .quartiles
            .as_ref()
            .map(|quartiles| [quartiles.q1, quartiles.q2, quartiles.q3]),
    )
}

/// Every route reports the exact median and quartiles rounded once, and where
/// an `exact` block is reported, the plain fields are its values rounded.
#[test]
fn the_plain_median_and_quartiles_round_the_exact_ones() {
    let mut wrong = Vec::new();
    for (case, values, median, quartiles) in CASES {
        for (route, report) in reports(values) {
            let stats = numeric(&report);
            let got = plain(stats);
            if got != (Some(median), quartiles) {
                wrong.push(format!("{case} [{route}]: {got:?}"));
            }
            if let Some(exact) = &stats.exact {
                let rounded = |text: &Option<String>| {
                    text.as_deref().map(|text| text.parse::<f64>().unwrap())
                };
                let from_exact = (
                    rounded(&exact.median),
                    exact
                        .q1
                        .as_ref()
                        .map(|_| [&exact.q1, &exact.q2, &exact.q3].map(|q| rounded(q).unwrap())),
                );
                if got != from_exact {
                    wrong.push(format!("{case} [{route}]: {got:?} vs exact {from_exact:?}"));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// Past the 10,000 values order statistics are sampled from, the plain fields
/// still round the exact ones the same sample gives.
#[test]
fn a_sampled_column_rounds_the_exact_order_statistics_of_its_sample() {
    // 30,001 integers past 2^53, odd and even, so most midpoints have a half.
    let values: Vec<String> = (-15_000i64..=15_000)
        .map(|i| (9_007_199_254_740_993i64 + 3 * i).to_string())
        .collect();
    let refs: Vec<&str> = values.iter().map(String::as_str).collect();
    let mut wrong = Vec::new();
    for (route, report) in reports(&refs) {
        let stats = numeric(&report);
        assert_eq!(stats.is_approximate, Some(true), "[{route}] sampled");
        let exact = stats.exact.as_ref().expect("an integer column is exact");
        let rounded = |text: &Option<String>| text.as_deref().unwrap().parse::<f64>().unwrap();
        let from_exact = (
            Some(rounded(&exact.median)),
            Some([rounded(&exact.q1), rounded(&exact.q2), rounded(&exact.q3)]),
        );
        if plain(stats) != from_exact {
            wrong.push(format!("[{route}]: {:?} vs {from_exact:?}", plain(stats)));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
