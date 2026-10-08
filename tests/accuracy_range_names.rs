//! Accuracy range rules read a column name by its words (#871).
//!
//! The rules used to match English substrings: `age` inside `average_price`
//! and `mileage`, `rate` inside `migrated_rows`, `count` inside `discount`.
//! Clean numbers under those names were counted as range violations, scored
//! 0% accuracy and could fail a quality gate. The rules live in the shared
//! metrics crate, so every engine and input format has to agree.
//!
//! `python/tests/test_accuracy_range_names.py` is the Python twin.

use std::io::Write;

use dataprof::{CsvParserConfig, EngineType, ProfileReport, Profiler, analyze_csv_file};
use tempfile::NamedTempFile;

/// Names holding a rule word only inside a longer word, each with clean values
/// that the rule it used to match would reject.
const NOT_RULE_NAMES: [(&str, [i64; 4]); 7] = [
    ("average_price", [200, 250, 300, 180]),
    ("mileage", [120000, 85000, 43000, 99000]),
    ("page_views", [310, 1200, 450, 980]),
    ("migrated_rows", [5000, 7000, 6500, 8000]),
    ("generated_tokens", [512, 1024, 2048, 700]),
    ("discount", [-5, -10, -2, -1]),
    ("account_number", [-5, 10, 20, 30]),
];

/// Names whose words carry a rule, each with values that break it.
const RULE_NAMES: [(&str, [i64; 4]); 6] = [
    ("age", [30, 200, 45, 300]),
    ("customer_age", [30, 200, 45, 300]),
    ("conversion_rate", [12, 250, 40, 180]),
    ("item_count", [3, -1, 5, -2]),
    ("birth_year", [1985, 1850, 1990, 2500]),
    ("customerAge", [30, 200, 45, 300]),
];

fn csv_fixture(name: &str, values: &[i64]) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "{name}").unwrap();
    for value in values {
        writeln!(file, "{value}").unwrap();
    }
    file.flush().unwrap();
    file
}

fn text_fixture(suffix: &str, body: String) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    write!(file, "{body}").unwrap();
    file.flush().unwrap();
    file
}

#[cfg(feature = "parquet")]
fn parquet_fixture(name: &str, values: &[i64]) -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
    let array: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// Every engine over the CSV, then every other format.
fn reports(name: &str, values: &[i64]) -> Vec<(String, ProfileReport)> {
    let csv = csv_fixture(name, values);
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

    let records: Vec<String> = values
        .iter()
        .map(|value| format!(r#"{{"{name}":{value}}}"#))
        .collect();
    let mut files = vec![
        (
            "json",
            text_fixture(".json", format!("[{}]", records.join(","))),
        ),
        ("jsonl", text_fixture(".jsonl", records.join("\n") + "\n")),
    ];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_fixture(name, values)));
    for (label, file) in files {
        let report = Profiler::new()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    reports
}

fn range_violations(report: &ProfileReport, label: &str) -> usize {
    report
        .quality
        .as_ref()
        .and_then(|quality| quality.metrics.accuracy.as_ref())
        .unwrap_or_else(|| panic!("[{label}] accuracy assessed"))
        .range_violations
}

/// Collects every wrong (path, column) before failing, so a regression names
/// all the paths it reaches rather than the first.
#[test]
fn names_that_merely_contain_a_rule_word_are_not_held_to_its_range() {
    let mut wrong = Vec::new();
    for (name, values) in NOT_RULE_NAMES {
        for (label, report) in reports(name, &values) {
            let violations = range_violations(&report, &label);
            let score = report
                .quality
                .as_ref()
                .and_then(|quality| quality.metrics.accuracy_score());
            if violations != 0 || score != Some(100.0) {
                wrong.push(format!(
                    "[{label}] {name}: range_violations {violations}, accuracy {score:?}"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// The rules still apply where a word of the name is a rule word.
#[test]
fn a_name_with_a_rule_word_still_holds_its_column_to_the_range() {
    let mut wrong = Vec::new();
    for (name, values) in RULE_NAMES {
        for (label, report) in reports(name, &values) {
            let violations = range_violations(&report, &label);
            if violations != 2 {
                wrong.push(format!("[{label}] {name}: range_violations {violations}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
