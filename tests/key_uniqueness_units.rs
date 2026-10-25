//! Key uniqueness divides a key's distinct values by its non-null values
//! (#1005).
//!
//! It used to divide by every row, nulls included, so a nullable key lost
//! uniqueness for each null the completeness score already counted: half a
//! column of distinct ids and half nulls scored 50. A key holding no value at
//! all measured nothing, and still scored 0. Uniqueness is computed in the
//! shared metrics crate, so every engine and input format has to agree.
//!
//! `python/tests/test_key_uniqueness_units.py` is the Python twin.

use std::io::Write;

use dataprof::{EngineType, ProfileReport, Profiler};
use serde_json::{Value, json};
use tempfile::NamedTempFile;

const ROWS: usize = 200;

/// A key column, `customer_id`, beside an `amount`. A distinct `amount`
/// keeps every row unique, so the duplicate component stays at 100.
struct Case {
    name: &'static str,
    key: fn(usize) -> Option<String>,
    amount: fn(usize) -> usize,
    /// Serialized `key_uniqueness`, `key_values_checked`, uniqueness score.
    expected: (f64, u64, f64),
}

const CASES: [Case; 3] = [
    Case {
        // 100 distinct ids, then 100 nulls: every value is unique.
        name: "half null, distinct",
        key: |row| (row < 100).then(|| format!("C{row}")),
        amount: |row| row,
        expected: (100.0, 100, 100.0),
    },
    Case {
        // 100 values over 50 ids, then 100 nulls.
        name: "half null, repeated",
        key: |row| (row < 100).then(|| format!("C{}", row % 50)),
        amount: |row| row,
        expected: (50.0, 50 * 2, 75.0),
    },
    Case {
        // No value: identified, measured nothing, so only the duplicate
        // component scores. Half the rows repeat, so that component is 50:
        // counting the key's placeholder 100 beside it would read 75.
        name: "all null",
        key: |_| None,
        amount: |row| row % 100,
        expected: (100.0, 0, 50.0),
    },
];

/// Only a declared key is measured (#1065).
fn key() -> Vec<String> {
    vec!["customer_id".to_string()]
}

fn fixture(suffix: &str, body: String) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    write!(file, "{body}").unwrap();
    file.flush().unwrap();
    file
}

fn records(case: &Case) -> Vec<String> {
    (0..ROWS)
        .map(|row| {
            json!({"customer_id": (case.key)(row), "amount": (case.amount)(row)}).to_string()
        })
        .collect()
}

#[cfg(feature = "parquet")]
fn parquet_fixture(case: &Case) -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                (0..ROWS).map(case.key).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (0..ROWS)
                    .map(|row| (case.amount)(row) as i64)
                    .collect::<Vec<_>>(),
            )),
        ],
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
    let csv_lines: Vec<String> = std::iter::once("customer_id,amount".to_string())
        .chain((0..ROWS).map(|row| {
            format!(
                "{},{}",
                (case.key)(row).unwrap_or_default(),
                (case.amount)(row)
            )
        }))
        .collect();
    let csv = fixture(".csv", csv_lines.join("\n") + "\n");
    let mut reports = Vec::new();
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = Profiler::new()
            .identifier_columns(key())
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
        reports.push((format!("{engine:?}"), report));
    }

    let records = records(case);
    let mut files = vec![
        ("json", fixture(".json", format!("[{}]", records.join(",")))),
        ("jsonl", fixture(".jsonl", records.join("\n") + "\n")),
    ];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_fixture(case)));
    for (label, file) in files {
        let report = Profiler::new()
            .identifier_columns(key())
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    reports
}

#[test]
fn key_uniqueness_counts_the_key_non_null_values_on_every_path() {
    let mut wrong = Vec::new();
    for case in &CASES {
        let (share, values, score) = case.expected;
        for (label, report) in reports(case) {
            let document = serde_json::to_value(&report).unwrap();
            let quality = &document["quality"];
            let uniqueness = &quality["metrics"]["uniqueness"];
            let observed = (
                &uniqueness["key_column"],
                &uniqueness["key_uniqueness"],
                &uniqueness["key_values_checked"],
                &quality["scores"]["dimension_scores"]["uniqueness"],
            );
            let expected = (
                &Value::from("customer_id"),
                &Value::from(share),
                &Value::from(values),
                &Value::from(score),
            );
            if observed != expected {
                wrong.push(format!("[{label}] {}: {observed:?}", case.name));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn reports_record_the_key_uniqueness_definition() {
    for (label, report) in reports(&CASES[0]) {
        let document = serde_json::to_value(&report).unwrap();
        assert_eq!(
            document["metric_semantics"]["key_uniqueness_units"], "non_null_values",
            "[{label}]"
        );
    }
}
