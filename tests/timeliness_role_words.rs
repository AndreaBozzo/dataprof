//! Timeliness pairs a start column with an end column by the words of their
//! names (#882).
//!
//! The roles used to match as substrings, so `end` inside `send_date` or
//! `weekend_date` and `to_date` inside `photo_date` paired two unrelated
//! dates. Every row where the second date fell before the first counted as a
//! temporal violation, lowered the timeliness score and could fail a quality
//! gate on clean data. The pairing lives in the shared metrics crate, so every
//! engine and input format has to agree.
//!
//! `python/tests/test_timeliness_role_words.py` is the Python twin.

use std::io::Write;

use dataprof::{CsvParserConfig, EngineType, ProfileReport, Profiler, analyze_csv_file};
use tempfile::NamedTempFile;

/// The first column of every fixture.
const FIRST: [&str; 3] = ["2024-03-15", "2024-04-20", "2024-05-10"];
/// The second column, one to two months before the first on every row, so a
/// pair that forms counts three violations.
const SECOND: [&str; 3] = ["2024-02-01", "2024-03-01", "2024-03-20"];

/// Column names that hold a role only inside a longer word.
const NOT_PAIRS: [(&str, &str); 4] = [
    ("start_date", "send_date"),
    ("start_date", "weekend_date"),
    ("restart_date", "end_date"),
    ("from_date", "photo_date"),
];

/// Column names whose words carry the roles: the pairs the issue lists, the
/// inflected and all-lowercase forms the substring match also paired, and the
/// camelCase forms of the two-word roles, which it did not.
const PAIRS: [(&str, &str); 9] = [
    ("start_date", "end_date"),
    ("created_at", "updated_at"),
    ("begin_date", "end_date"),
    ("from_date", "to_date"),
    ("startDate", "endDate"),
    ("starts_at", "ends_at"),
    ("startdate", "enddate"),
    ("beginDate", "endDate"),
    ("fromDate", "toDate"),
];

fn text_fixture(suffix: &str, body: String) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    write!(file, "{body}").unwrap();
    file.flush().unwrap();
    file
}

#[cfg(feature = "parquet")]
fn parquet_fixture(first: &str, second: &str) -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringArray};
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let days = |values: [&str; 3]| -> ArrayRef {
        cast(&StringArray::from(values.to_vec()), &DataType::Date32).unwrap()
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new(first, DataType::Date32, false),
        Field::new(second, DataType::Date32, false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![days(FIRST), days(SECOND)]).unwrap();

    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// Every engine over the CSV, then every other format.
fn reports(first: &str, second: &str) -> Vec<(String, ProfileReport)> {
    let rows: Vec<(&str, &str)> = FIRST.into_iter().zip(SECOND).collect();
    let csv_body = std::iter::once(format!("{first},{second}"))
        .chain(rows.iter().map(|(a, b)| format!("{a},{b}")))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let csv = text_fixture(".csv", csv_body);
    let mut reports = vec![(
        "standard".to_string(),
        analyze_csv_file(csv.path(), &CsvParserConfig::default()).expect("standard CSV"),
    )];
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
        reports.push((format!("{engine:?}"), report));
    }

    let records: Vec<String> = rows
        .iter()
        .map(|(a, b)| format!(r#"{{"{first}":"{a}","{second}":"{b}"}}"#))
        .collect();
    let mut files = vec![
        (
            "json",
            text_fixture(".json", format!("[{}]", records.join(","))),
        ),
        ("jsonl", text_fixture(".jsonl", records.join("\n") + "\n")),
    ];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_fixture(first, second)));
    for (label, file) in files {
        let report = Profiler::new()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    reports
}

/// `(temporal_pairs_checked, temporal_violations)`.
fn ordering(report: &ProfileReport, label: &str) -> (usize, usize) {
    let timeliness = report
        .quality
        .as_ref()
        .and_then(|quality| quality.metrics.timeliness.as_ref())
        .unwrap_or_else(|| panic!("[{label}] timeliness assessed"));
    (
        timeliness.temporal_pairs_checked,
        timeliness.temporal_violations,
    )
}

/// Collects every wrong (path, pair) before failing, so a regression names
/// all the paths it reaches rather than the first.
#[test]
fn names_that_merely_contain_a_role_word_are_not_paired() {
    let mut wrong = Vec::new();
    for (first, second) in NOT_PAIRS {
        for (label, report) in reports(first, second) {
            let (checked, violations) = ordering(&report, &label);
            if (checked, violations) != (0, 0) {
                wrong.push(format!(
                    "[{label}] {first}/{second}: pairs_checked {checked}, violations {violations}"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// The roles still pair columns whose words carry them.
#[test]
fn names_with_role_words_are_still_paired() {
    let mut wrong = Vec::new();
    for (first, second) in PAIRS {
        for (label, report) in reports(first, second) {
            let (checked, violations) = ordering(&report, &label);
            if (checked, violations) != (3, 3) {
                wrong.push(format!(
                    "[{label}] {first}/{second}: pairs_checked {checked}, violations {violations}"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
