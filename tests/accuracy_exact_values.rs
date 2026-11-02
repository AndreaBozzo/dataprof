//! The accuracy dimension reads values exactly where the column statistics do
//! (#1181).
//!
//! `accuracy.outlier_ratio` and a column's `outlier_count` apply the same
//! Tukey rule to the same values. On a column of 64-bit integers past 2^53
//! the column count takes its fences from exact differences, while the
//! accuracy pass reparsed every cell as `f64`, where values sit 256 apart: it
//! missed an outlier about 6 below the lower fence. `WIDE` is that column.
//! `CENTS` is a decimal column on 10^13, which the typed routes read as a
//! decimal: its lowest value lies 0.00375 below the lower fence, past what
//! the `f64` fences resolve.
//!
//! The range and sign rules compared the same `f64`: `100.000000000000000001`
//! read as 100, `-1e-400` as -0, and `1e400` as infinity, which dropped the
//! value from the rule component. Only text carries those literals, so the
//! rule cases run over CSV.
//!
//! `python/tests/test_accuracy_exact_values.py` is the Python twin.

use std::io::Write;

use dataprof::{ColumnStats, EngineType, MetricPack, ProfileReport, Profiler};
use serde_json::{Value, json};
use tempfile::NamedTempFile;

/// One value about 6 below the lower fence, the rest a few hundred apart.
const WIDE: [i64; 8] = [
    1_759_700_000_123_456_512,
    1_759_700_000_123_456_916,
    1_759_700_000_123_456_940,
    1_759_700_000_123_456_945,
    1_759_700_000_123_457_061,
    1_759_700_000_123_457_175,
    1_759_700_000_123_457_320,
    1_759_700_000_123_457_409,
];

/// `9999999999999 + cents / 100`: the lower fence is at 80.375 cents.
#[cfg(feature = "parquet")]
const CENTS: [i128; 8] = [80, 173, 195, 209, 222, 255, 284, 331];
#[cfg(feature = "parquet")]
const CENTS_BASE: i128 = 999_999_999_999_900;

fn fixture(suffix: &str, body: String) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    write!(file, "{body}").unwrap();
    file.flush().unwrap();
    file
}

fn csv_reports(body: String) -> Vec<(String, ProfileReport)> {
    let csv = fixture(".csv", body);
    [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ]
    .into_iter()
    .map(|engine| {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
        (format!("{engine:?}"), report)
    })
    .collect()
}

#[cfg(feature = "parquet")]
fn parquet_fixture(column: arrow::array::ArrayRef) -> NamedTempFile {
    use std::sync::Arc;

    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        column.data_type().clone(),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![column]).unwrap();
    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// `WIDE` over every engine and format.
fn wide_reports() -> Vec<(String, ProfileReport)> {
    let lines: Vec<String> = std::iter::once("v".to_string())
        .chain(WIDE.iter().map(i64::to_string))
        .collect();
    let mut reports = csv_reports(lines.join("\n") + "\n");

    let records: Vec<String> = WIDE.iter().map(|v| json!({ "v": v }).to_string()).collect();
    let mut files = vec![
        ("json", fixture(".json", format!("[{}]", records.join(",")))),
        ("jsonl", fixture(".jsonl", records.join("\n") + "\n")),
    ];
    #[cfg(feature = "parquet")]
    files.push((
        "parquet",
        parquet_fixture(std::sync::Arc::new(arrow::array::Int64Array::from(
            WIDE.to_vec(),
        ))),
    ));
    for (label, file) in files {
        let report = Profiler::new()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    reports
}

/// The column's `outlier_count` and the serialized `outlier_ratio`.
fn outliers(report: &ProfileReport) -> (Option<usize>, Value) {
    let count = match &report.column_profiles[0].stats {
        ColumnStats::Numeric(stats) => stats.outlier_count,
        _ => None,
    };
    let document = serde_json::to_value(report).unwrap();
    (
        count,
        document["quality"]["metrics"]["accuracy"]["outlier_ratio"].clone(),
    )
}

#[test]
fn outlier_ratio_agrees_with_the_column_count_on_wide_integers() {
    let wrong: Vec<String> = wide_reports()
        .iter()
        .map(|(label, report)| (label, outliers(report)))
        .filter(|(_, observed)| *observed != (Some(1), Value::from(12.5)))
        .map(|(label, observed)| format!("[{label}] {observed:?}"))
        .collect();
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[cfg(feature = "parquet")]
#[test]
fn outlier_ratio_reads_a_decimal_column_as_a_decimal() {
    use arrow::array::Decimal128Array;

    let cents = Decimal128Array::from(
        CENTS
            .iter()
            .map(|cents| CENTS_BASE + cents)
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(16, 2)
    .unwrap();
    let file = parquet_fixture(std::sync::Arc::new(cents));
    let report = Profiler::new().analyze_file(file.path()).unwrap();
    assert_eq!(outliers(&report), (Some(1), Value::from(12.5)));

    // Pins a known gap, #906: the accuracy pass tells a decimal column from
    // its statistics, so a run without them reads its values as text and the
    // `f64` fences miss the outlier, as every run did before #1181. Once the
    // column records its source type, this run reports 12.5 too.
    let quality_only = Profiler::new()
        .metric_packs(vec![MetricPack::Quality])
        .analyze_file(file.path())
        .unwrap();
    assert_eq!(outliers(&quality_only), (None, Value::from(0.0)));
}

/// Rule cases: a column name, its values, and the expected serialized
/// `range_violations`, `rule_values_checked` and `rule_violating_values`.
/// Each column also holds a value exactly on each of its bounds.
const RULE_CASES: [(&str, [&str; 4], [u64; 3]); 4] = [
    (
        "discount_percent",
        ["100.000000000000000001", "100", "0", "50"],
        [1, 4, 1],
    ),
    ("customer_age", ["1e400", "150", "0", "30"], [1, 4, 1]),
    ("item_count", ["-1e-400", "0", "5", "1"], [1, 4, 1]),
    // Just inside each bound.
    (
        "birth_year",
        [
            "1900.000000000000000001",
            "2099.999999999999999999",
            "1900",
            "2100",
        ],
        [0, 4, 0],
    ),
];

#[test]
fn range_rules_compare_each_value_with_its_bounds_exactly() {
    let mut wrong = Vec::new();
    for (column, values, expected) in RULE_CASES {
        let body = std::iter::once(column)
            .chain(values)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        for (label, report) in csv_reports(body) {
            let document = serde_json::to_value(&report).unwrap();
            let accuracy = &document["quality"]["metrics"]["accuracy"];
            let observed = [
                "range_violations",
                "rule_values_checked",
                "rule_violating_values",
            ]
            .map(|field| accuracy[field].as_u64());
            if observed != expected.map(Some) {
                wrong.push(format!("[{label}] {column}: {observed:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}
