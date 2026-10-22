//! The outlier component is measured over the values of the columns that
//! reached `outlier_min_samples`, and is absent when none did (#1005).
//!
//! An untested source used to report `outlier_ratio` 0 and score accuracy
//! 100: three numbers holding one value a million times the others read as
//! perfectly accurate, though no outlier test ran. Accuracy is now assessed
//! only when a component had units. The rules live in the shared metrics
//! crate, so every engine and input format has to agree.
//!
//! `python/tests/test_outlier_component_units.py` is the Python twin.

use std::io::Write;

use dataprof::{EngineType, ProfileReport, Profiler};
use serde_json::{Value, json};
use tempfile::NamedTempFile;

/// One numeric column per case. The default `outlier_min_samples` is 4.
struct Case {
    name: &'static str,
    column: &'static str,
    values: &'static [i64],
    /// Serialized `outlier_values_checked` and accuracy score; `None` for an
    /// unassessed dimension, which serializes no accuracy block.
    expected: Option<(u64, f64)>,
}

const CASES: [Case; 3] = [
    Case {
        // Too few values for the test and no rule: nothing measured.
        name: "untested, no rule",
        column: "amount",
        values: &[1, 2, 1_000_000],
        expected: None,
    },
    Case {
        // Too few values for the test, but the percentage rule applies: the
        // rule component alone scores, 1 violating value of 3.
        name: "untested, ruled",
        column: "discount_percent",
        values: &[10, 20, 150],
        expected: Some((0, 66.67)),
    },
    Case {
        // Four values: tested, and 1,000,000 is outside the fences.
        name: "tested",
        column: "amount",
        values: &[1, 2, 3, 1_000_000],
        expected: Some((4, 75.0)),
    },
];

fn fixture(suffix: &str, body: String) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    write!(file, "{body}").unwrap();
    file.flush().unwrap();
    file
}

#[cfg(feature = "parquet")]
fn parquet_fixture(case: &Case) -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![Field::new(
        case.column,
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(case.values.to_vec()))],
    )
    .unwrap();

    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// Every engine over the CSV, then every other format.
fn reports(case: &Case) -> Vec<(String, ProfileReport)> {
    let csv_lines: Vec<String> = std::iter::once(case.column.to_string())
        .chain(case.values.iter().map(i64::to_string))
        .collect();
    let csv = fixture(".csv", csv_lines.join("\n") + "\n");
    let mut reports = Vec::new();
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

    let records: Vec<String> = case
        .values
        .iter()
        .map(|value| json!({ case.column: value }).to_string())
        .collect();
    let mut files = vec![
        ("json", fixture(".json", format!("[{}]", records.join(",")))),
        ("jsonl", fixture(".jsonl", records.join("\n") + "\n")),
    ];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_fixture(case)));
    for (label, file) in files {
        let report = Profiler::new()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    reports
}

#[test]
fn an_untested_outlier_component_is_absent_on_every_path() {
    let mut wrong = Vec::new();
    for case in &CASES {
        for (label, report) in reports(case) {
            let document = serde_json::to_value(&report).unwrap();
            let quality = &document["quality"];
            let accuracy = &quality["metrics"]["accuracy"];
            let observed = (
                accuracy.get("outlier_values_checked").cloned(),
                quality["scores"]["dimension_scores"]["accuracy"].clone(),
            );
            let expected = match case.expected {
                Some((tested, score)) => (Some(Value::from(tested)), Value::from(score)),
                None => (None, Value::Null),
            };
            if observed != expected {
                wrong.push(format!("[{label}] {}: {observed:?}", case.name));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn reports_record_the_outlier_definition() {
    for (label, report) in reports(&CASES[2]) {
        let document = serde_json::to_value(&report).unwrap();
        assert_eq!(
            document["metric_semantics"]["outlier_units"], "tested_values",
            "[{label}]"
        );
    }
}
