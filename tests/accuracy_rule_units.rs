//! The range and sign rules of the accuracy score count values of the columns
//! they apply to, each value once (#1005).
//!
//! The failures used to be divided by the numeric values of every column, so
//! clean columns beside a broken one diluted its penalty, and a value breaking
//! two rules (a negative percentage hinted positive) counted twice. The rules
//! live in the shared metrics crate, so every engine and input format has to
//! report the same counts and score.
//!
//! `python/tests/test_accuracy_rule_units.py` is the Python twin.

use std::io::Write;

use dataprof::{EngineType, ProfileReport, Profiler};
use serde_json::Value;
use tempfile::NamedTempFile;

/// A percentage column hinted positive: -10, 0, 10, ..., 180. Evenly spaced,
/// so no value is a Tukey outlier and the outlier component stays at zero.
/// -10 breaks the percentage and the sign rule, 110 to 180 the percentage
/// rule: 9 violating values of 20.
const PERCENT: &str = "discount_percent";

/// Clean numeric columns under no rule: 1 to 20, no outliers either.
const CLEAN: [&str; 9] = [
    "reading_1",
    "reading_2",
    "reading_3",
    "reading_4",
    "reading_5",
    "reading_6",
    "reading_7",
    "reading_8",
    "reading_9",
];

const ROWS: i64 = 20;

fn table(with_clean: bool) -> (Vec<&'static str>, Vec<Vec<i64>>) {
    let mut names = vec![PERCENT];
    if with_clean {
        names.extend(CLEAN);
    }
    let rows = (0..ROWS)
        .map(|row| {
            let mut values = vec![row * 10 - 10];
            if with_clean {
                values.extend(CLEAN.iter().map(|_| row + 1));
            }
            values
        })
        .collect();
    (names, rows)
}

fn fixture(suffix: &str, body: String) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    write!(file, "{body}").unwrap();
    file.flush().unwrap();
    file
}

fn records(names: &[&str], rows: &[Vec<i64>]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            let fields: Vec<String> = names
                .iter()
                .zip(row)
                .map(|(name, value)| format!(r#""{name}":{value}"#))
                .collect();
            format!("{{{}}}", fields.join(","))
        })
        .collect()
}

#[cfg(feature = "parquet")]
fn parquet_fixture(names: &[&str], rows: &[Vec<i64>]) -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(*name, DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let columns: Vec<ArrayRef> = (0..names.len())
        .map(|index| {
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row[index]).collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// Every engine over the CSV, then every other format, with `PERCENT`
/// hinted positive.
fn reports(with_clean: bool) -> Vec<(String, ProfileReport)> {
    let (names, rows) = table(with_clean);
    let profiler = || Profiler::new().positive_columns(vec![PERCENT.to_string()]);

    let csv_lines: Vec<String> = std::iter::once(names.join(","))
        .chain(
            rows.iter()
                .map(|row| row.iter().map(i64::to_string).collect::<Vec<_>>().join(",")),
        )
        .collect();
    let csv = fixture(".csv", csv_lines.join("\n") + "\n");
    let mut reports = Vec::new();
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = profiler()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
        reports.push((format!("{engine:?}"), report));
    }

    let records = records(&names, &rows);
    let mut files = vec![
        ("json", fixture(".json", format!("[{}]", records.join(",")))),
        ("jsonl", fixture(".jsonl", records.join("\n") + "\n")),
    ];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_fixture(&names, &rows)));
    for (label, file) in files {
        let report = profiler()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    reports
}

/// The serialized accuracy evidence and score, as the output contract
/// compares them.
fn accuracy(report: &ProfileReport) -> (Value, Value) {
    let document = serde_json::to_value(report).unwrap();
    let quality = &document["quality"];
    (
        quality["metrics"]["accuracy"].clone(),
        quality["scores"]["dimension_scores"]["accuracy"].clone(),
    )
}

#[test]
fn clean_columns_and_overlapping_rules_leave_the_rule_component_alone() {
    let mut wrong = Vec::new();
    for with_clean in [false, true] {
        let numeric_values = if with_clean { 200 } else { 20 };
        for (label, report) in reports(with_clean) {
            let (metrics, score) = accuracy(&report);
            let expected = [
                ("range_violations", 9),
                ("negative_values_in_positive", 1),
                ("numeric_values_checked", numeric_values),
                ("rule_values_checked", 20),
                ("rule_violating_values", 9),
            ];
            for (field, value) in expected {
                if metrics[field] != value {
                    wrong.push(format!(
                        "[{label}, clean columns: {with_clean}] {field} {}, expected {value}",
                        metrics[field]
                    ));
                }
            }
            // 9 violating values of 20, no outliers: 100 - 45.
            if score != 55.0 {
                wrong.push(format!(
                    "[{label}, clean columns: {with_clean}] accuracy {score}, expected 55.0"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn reports_record_the_range_rule_definition() {
    for (label, report) in reports(false) {
        let document = serde_json::to_value(&report).unwrap();
        assert_eq!(
            document["metric_semantics"]["range_rule_count"], "violating_values",
            "[{label}]"
        );
    }
}
