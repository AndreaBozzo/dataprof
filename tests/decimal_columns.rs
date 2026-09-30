//! Arrow and Parquet decimal columns are profiled at their value (#821).
//!
//! A decimal's stored integer is its value times `10^scale`. The batch analyzer
//! fed that integer to the statistics, so a `decimal128(10, 2)` column of
//! prices reported a maximum of 123456 for 1234.56, and a `decimal256` column
//! never read its values: every value counted as distinct and no statistic was
//! reported. The same values written to a CSV are the reference: every decimal
//! type has to serialize what the CSV does.
//!
//! `python/tests/test_decimal_columns.py` is the Python twin.

use std::io::Write;
use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Array, Decimal256Array};
use arrow::datatypes::{Field, Schema, i256};
use arrow::record_batch::RecordBatch;
use dataprof::{ProfileReport, Profiler};
use parquet::arrow::ArrowWriter;
use tempfile::NamedTempFile;

/// Prices at two decimal places, one repeated, one negative.
const VALUES: [&str; 5] = ["1.10", "2.25", "-3.00", "1234.56", "2.25"];

fn csv_reference() -> ProfileReport {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "amount").unwrap();
    for value in VALUES {
        writeln!(file, "{value}").unwrap();
    }
    file.flush().unwrap();
    Profiler::new().analyze_file(file.path()).expect("csv")
}

/// `value` as the unscaled integer a decimal of `scale` stores.
fn unscaled(value: &str, scale: i8) -> i128 {
    let negative = value.starts_with('-');
    let (whole, fraction) = value.trim_start_matches('-').split_once('.').unwrap();
    let digits: i128 = format!("{whole}{fraction}").parse().unwrap();
    let shift = i32::from(scale) - fraction.len() as i32;
    let magnitude = if shift >= 0 {
        digits * 10i128.pow(shift as u32)
    } else {
        digits / 10i128.pow((-shift) as u32)
    };
    if negative { -magnitude } else { magnitude }
}

fn parquet_of(array: ArrayRef) -> NamedTempFile {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "amount",
        array.data_type().clone(),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

fn decimal128(precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(VALUES.map(|value| unscaled(value, scale)).to_vec())
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

fn decimal256(precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal256Array::from(
            VALUES
                .map(|value| i256::from_i128(unscaled(value, scale)))
                .to_vec(),
        )
        .with_precision_and_scale(precision, scale)
        .unwrap(),
    )
}

fn column(report: &ProfileReport) -> serde_json::Value {
    let document = serde_json::to_value(report).expect("serializes");
    document["column_profiles"][0].clone()
}

#[test]
fn every_decimal_type_profiles_like_the_same_values_in_a_csv() {
    let reference = column(&csv_reference());
    assert_eq!(reference["data_type"], "Float");
    assert_eq!(reference["stats"]["Numeric"]["max"], 1234.56);

    for (label, array) in [
        ("decimal128(10, 2)", decimal128(10, 2)),
        ("decimal128(20, 4)", decimal128(20, 4)),
        ("decimal256(40, 2)", decimal256(40, 2)),
    ] {
        let file = parquet_of(array);
        let report = Profiler::new().analyze_file(file.path()).expect(label);
        let profiled = column(&report);
        for field in [
            "data_type",
            "unique_count",
            "invalid_count",
            "type_homogeneity",
        ] {
            assert_eq!(profiled[field], reference[field], "[{label}] {field}");
        }
        for statistic in ["min", "max", "mean", "std_dev", "variance", "median"] {
            assert_eq!(
                profiled["stats"]["Numeric"][statistic], reference["stats"]["Numeric"][statistic],
                "[{label}] {statistic}"
            );
        }
    }
}

#[test]
fn a_scale_zero_decimal_reads_its_values() {
    // Scale 0 stores the value itself. Negative scales cannot be written to
    // Parquet; the Python twin covers them through an in-memory Arrow table.
    let array: ArrayRef = Arc::new(
        Decimal128Array::from(vec![1, 22, -3, 1234])
            .with_precision_and_scale(5, 0)
            .unwrap(),
    );
    let file = parquet_of(array);
    let report = Profiler::new().analyze_file(file.path()).expect("scale 0");
    let profiled = column(&report);
    assert_eq!(profiled["stats"]["Numeric"]["min"], -3.0);
    assert_eq!(profiled["stats"]["Numeric"]["max"], 1234.0);
    assert_eq!(profiled["unique_count"], 4);
}
