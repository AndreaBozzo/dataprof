//! A column with no non-null value carries no statistics (#983).
//!
//! Statistics are aggregates over values, and over none they are undefined,
//! which the report states as absent. An all-null text column used to report
//! lengths of 0, the same as a column of real zero-length values, while an
//! all-null numeric column already reported no statistics.
//!
//! `python/tests/test_all_null_columns.py` is the Python twin, over the
//! in-memory routes.

use std::io::Write;

use dataprof::{ColumnStats, EngineType, ProfileReport, Profiler};

const ENGINES: [EngineType; 3] = [
    EngineType::Auto,
    EngineType::Incremental,
    EngineType::Columnar,
];

fn write(suffix: &str, contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
    write!(file, "{contents}").unwrap();
    file.flush().unwrap();
    file
}

/// Every route this file can reach, by label.
fn reports(
    csv: &str,
    jsonl: &str,
    profiler: impl Fn() -> Profiler,
) -> Vec<(String, ProfileReport)> {
    let csv_file = write(".csv", csv);
    let jsonl_file = write(".jsonl", jsonl);
    let mut out: Vec<(String, ProfileReport)> = ENGINES
        .iter()
        .map(|engine| {
            (
                format!("csv {engine:?}"),
                profiler()
                    .engine(*engine)
                    .analyze_file(csv_file.path())
                    .unwrap(),
            )
        })
        .collect();
    out.push((
        "jsonl".to_string(),
        profiler().analyze_file(jsonl_file.path()).unwrap(),
    ));
    out
}

fn column<'r>(report: &'r ProfileReport, name: &str) -> &'r dataprof::ColumnProfile {
    report
        .column_profiles
        .iter()
        .find(|column| column.name == name)
        .unwrap()
}

#[test]
fn a_column_of_nulls_reports_no_statistics_and_keeps_its_counts() {
    // `blank` holds only null tokens; `note` holds one real value.
    let csv = "id,empty,blank,note\n1,,NA,\n2,,null,x\n3,,,\n";
    let jsonl = concat!(
        "{\"id\":1,\"empty\":null,\"blank\":\"NA\",\"note\":null}\n",
        "{\"id\":2,\"empty\":null,\"blank\":\"null\",\"note\":\"x\"}\n",
        "{\"id\":3,\"empty\":null,\"blank\":\"\",\"note\":null}\n",
    );
    let mut wrong = Vec::new();
    for (route, report) in reports(csv, jsonl, Profiler::new) {
        for name in ["empty", "blank"] {
            let profile = column(&report, name);
            if !matches!(profile.stats, ColumnStats::None) {
                wrong.push(format!("{route}: {name} stats {:?}", profile.stats));
            }
            if (profile.total_count, profile.null_count) != (3, 3) {
                wrong.push(format!(
                    "{route}: {name} counts {}/{}",
                    profile.null_count, profile.total_count
                ));
            }
        }
        // One value is enough for lengths.
        match &column(&report, "note").stats {
            ColumnStats::Text(text) if (text.min_length, text.max_length) == (1, 1) => {}
            other => wrong.push(format!("{route}: note stats {other:?}")),
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn an_identifier_column_of_nulls_reports_no_statistics() {
    let csv = "id,code\n1,\n2,\n";
    let jsonl = "{\"id\":1,\"code\":null}\n{\"id\":2,\"code\":null}\n";
    let mut wrong = Vec::new();
    for (route, report) in reports(csv, jsonl, || {
        Profiler::new().identifier_columns(vec!["code".to_string()])
    }) {
        let profile = column(&report, "code");
        if !matches!(profile.stats, ColumnStats::None) {
            wrong.push(format!(
                "{route}: {:?} {:?}",
                profile.data_type, profile.stats
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
