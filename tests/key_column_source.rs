//! Key uniqueness measures only a declared key (#1065).
//!
//! With no key declared, the first column whose name had an `id`, `key` or
//! `pk` word was the key. A name cannot tell a table's key from a foreign key,
//! so a `customer_id` listed before `order_id` was scored as the table key, and
//! the same clean table scored differently by column order. Uniqueness is
//! computed in the shared metrics crate, so every engine and input format has
//! to agree.
//!
//! `python/tests/test_key_column_source.py` is the Python twin.

use std::io::Write;

use dataprof::{EngineType, ProfileReport, Profiler};
use serde_json::Value;
use tempfile::NamedTempFile;

const ROWS: usize = 200;

/// The same table in two column orders: the foreign key last, then first.
const ORDERS: [[&str; 3]; 2] = [
    ["order_id", "customer_id", "amount"],
    ["customer_id", "order_id", "amount"],
];

/// `order_id` is unique, `customer_id` repeats over 80 customers, and a
/// distinct `amount` keeps every row unique, so the duplicate component is 100.
fn value(column: &str, row: usize) -> String {
    match column {
        "order_id" => format!("O{row}"),
        "customer_id" => format!("C{}", row % 80),
        _ => row.to_string(),
    }
}

/// A declaration and the serialized key column, `key_uniqueness` and
/// uniqueness score it must give in either column order.
struct Case {
    declared: &'static [&'static str],
    expected: (Option<&'static str>, f64, f64),
}

const CASES: [Case; 3] = [
    Case {
        // No key component: uniqueness rests on the duplicate rows.
        declared: &[],
        expected: (None, 100.0, 100.0),
    },
    Case {
        declared: &["order_id"],
        expected: (Some("order_id"), 100.0, 100.0),
    },
    Case {
        // Declared, a foreign key is measured as asked: 80 of 200.
        declared: &["customer_id"],
        expected: (Some("customer_id"), 40.0, 70.0),
    },
];

fn fixture(suffix: &str, body: String) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    write!(file, "{body}").unwrap();
    file.flush().unwrap();
    file
}

/// One JSON object per row, keys in `columns` order.
fn records(columns: &[&str]) -> Vec<String> {
    (0..ROWS)
        .map(|row| {
            let fields: Vec<String> = columns
                .iter()
                .map(|column| match *column {
                    "amount" => format!("\"amount\":{row}"),
                    _ => format!("\"{column}\":\"{}\"", value(column, row)),
                })
                .collect();
            format!("{{{}}}", fields.join(","))
        })
        .collect()
}

#[cfg(feature = "parquet")]
fn parquet_fixture(columns: &[&str]) -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = columns
        .iter()
        .map(|column| match *column {
            "amount" => (
                Field::new("amount", DataType::Int64, false),
                Arc::new(Int64Array::from(
                    (0..ROWS).map(|row| row as i64).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            _ => (
                Field::new(*column, DataType::Utf8, false),
                Arc::new(StringArray::from(
                    (0..ROWS).map(|row| value(column, row)).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
        })
        .unzip();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();

    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

fn profiler(declared: &[&str]) -> Profiler {
    Profiler::new().identifier_columns(declared.iter().map(|name| name.to_string()).collect())
}

/// Every engine over the CSV, then every other format.
fn reports(columns: &[&str], declared: &[&str]) -> Vec<(String, ProfileReport)> {
    let csv_lines: Vec<String> = std::iter::once(columns.join(","))
        .chain((0..ROWS).map(|row| {
            columns
                .iter()
                .map(|column| value(column, row))
                .collect::<Vec<_>>()
                .join(",")
        }))
        .collect();
    let csv = fixture(".csv", csv_lines.join("\n") + "\n");
    let mut reports = Vec::new();
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = profiler(declared)
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
        reports.push((format!("{engine:?}"), report));
    }

    let records = records(columns);
    let mut files = vec![
        ("json", fixture(".json", format!("[{}]", records.join(",")))),
        ("jsonl", fixture(".jsonl", records.join("\n") + "\n")),
    ];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_fixture(columns)));
    for (label, file) in files {
        let report = profiler(declared)
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    reports
}

#[test]
fn only_a_declared_key_is_measured_whatever_the_column_order() {
    let mut wrong = Vec::new();
    for case in &CASES {
        let (key, share, score) = case.expected;
        let mut scores: Vec<(String, Value)> = Vec::new();
        for columns in &ORDERS {
            for (label, report) in reports(columns, case.declared) {
                let document = serde_json::to_value(&report).unwrap();
                let quality = &document["quality"];
                let uniqueness = &quality["metrics"]["uniqueness"];
                let observed = (
                    uniqueness
                        .get("key_column")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    &uniqueness["key_uniqueness"],
                    &quality["scores"]["dimension_scores"]["uniqueness"],
                );
                if observed
                    != (
                        key.map(str::to_string),
                        &Value::from(share),
                        &Value::from(score),
                    )
                {
                    wrong.push(format!(
                        "[{label}] {:?} {columns:?}: {observed:?}",
                        case.declared
                    ));
                }
                // The other column order on the same path must score the same.
                match scores.iter().find(|(seen, _)| *seen == label) {
                    Some((_, first)) if *first != quality["scores"] => wrong.push(format!(
                        "[{label}] {:?}: scores move with column order: {first} vs {}",
                        case.declared, quality["scores"]
                    )),
                    Some(_) => {}
                    None => scores.push((label, quality["scores"].clone())),
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn reports_record_the_key_column_source() {
    for (label, report) in reports(&ORDERS[0], &[]) {
        let document = serde_json::to_value(&report).unwrap();
        assert_eq!(
            document["metric_semantics"]["key_column_source"], "declared",
            "[{label}]"
        );
    }
}
