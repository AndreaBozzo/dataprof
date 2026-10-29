//! Every Arrow decimal width profiles the same values the same way (#1157).
//!
//! The batch analyzer read `decimal128` and `decimal256` as decimals and had
//! no arm for `decimal32` or `decimal64`, which went through the generic text
//! formatter. A scale-0 column then typed as an integer where the same values
//! as a `decimal128` are a float, and 17 digits of cents read as the float
//! they might be, so a `decimal64(18, 2)` column reported a variance three
//! times the exact one. Each column is written once per width that holds it,
//! and every width has to serialize what `decimal128` does, whose exact spread
//! `tests/decimal_spread.rs` checks. A column longer than the sample the
//! analyzer keeps checks that each value reaches the exact accumulators, which
//! the text formatter never fed.
//!
//! `python/tests/test_decimal_widths.py` is the Python twin, and adds the
//! in-memory Arrow routes and a negative scale, which Parquet cannot store.

use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array};
use arrow::datatypes::{Field, Schema, i256};
use arrow::record_batch::RecordBatch;
use dataprof::{DataType, Profiler};
use parquet::arrow::ArrowWriter;
use tempfile::NamedTempFile;

const WIDTHS: [u16; 4] = [32, 64, 128, 256];

/// Past the 10,000 numeric values the analyzer samples.
const LONG_ROWS: usize = 20_000;

/// (name, precision, scale, values): a repeated value in each, one negative.
type Column = (&'static str, u8, i8, Vec<String>);

const SHORT: [(&str, u8, i8, [&str; 5]); 3] = [
    ("whole", 9, 0, ["1", "22", "-3", "123456789", "22"]),
    (
        "cents",
        9,
        2,
        [
            "1234567.01",
            "1234567.05",
            "-1234567.02",
            "1234567.09",
            "1234567.05",
        ],
    ),
    // 18 digits: past a `decimal32`, and past what an `f64` keeps apart.
    (
        "wide_cents",
        18,
        2,
        [
            "999999999999999.01",
            "999999999999999.05",
            "999999999999999.02",
            "999999999999999.09",
            "999999999999999.05",
        ],
    ),
];

/// The largest precision a decimal of `width` bits stores.
fn max_precision(width: u16) -> u8 {
    match width {
        32 => 9,
        64 => 18,
        128 => 38,
        _ => 76,
    }
}

fn decimal(width: u16, precision: u8, scale: i8, values: &[&str]) -> ArrayRef {
    // Every value is written at its column's scale, so the unscaled integer is
    // its digits.
    let unscaled: Vec<i128> = values
        .iter()
        .map(|value| value.replace('.', "").parse().unwrap())
        .collect();
    match width {
        32 => Arc::new(
            Decimal32Array::from(unscaled.iter().map(|&v| v as i32).collect::<Vec<_>>())
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        ),
        64 => Arc::new(
            Decimal64Array::from(unscaled.iter().map(|&v| v as i64).collect::<Vec<_>>())
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        ),
        128 => Arc::new(
            Decimal128Array::from(unscaled)
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        ),
        _ => Arc::new(
            Decimal256Array::from(
                unscaled
                    .into_iter()
                    .map(i256::from_i128)
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(precision, scale)
            .unwrap(),
        ),
    }
}

fn short_columns() -> Vec<Column> {
    SHORT
        .iter()
        .map(|(name, precision, scale, values)| {
            let values = values.iter().map(|value| value.to_string()).collect();
            (*name, *precision, *scale, values)
        })
        .collect()
}

/// Cents from -99.99 to 99.99, and the maximum, 250.00, at the first row.
fn long_columns() -> Vec<Column> {
    let values = (0..LONG_ROWS)
        .map(|row| {
            let cents = if row == 0 {
                25_000
            } else {
                row as i64 - 10_000
            };
            let sign = if cents < 0 { "-" } else { "" };
            format!("{sign}{}.{:02}", cents.abs() / 100, cents.abs() % 100)
        })
        .collect();
    vec![("long_cents", 9, 2, values)]
}

/// The columns a decimal of `width` holds, as a Parquet file.
fn parquet_of(width: u16, columns: &[Column]) -> NamedTempFile {
    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = columns
        .iter()
        .filter(|(_, precision, _, _)| *precision <= max_precision(width))
        .map(|(name, precision, scale, values)| {
            let values: Vec<&str> = values.iter().map(String::as_str).collect();
            let array = decimal(width, *precision, *scale, &values);
            (Field::new(*name, array.data_type().clone(), false), array)
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

/// Each column's serialized profile, by name.
fn columns(file: &NamedTempFile) -> Vec<(String, serde_json::Value)> {
    let report = Profiler::new().analyze_file(file.path()).expect("profile");
    let document = serde_json::to_value(&report).expect("serializes");
    document["column_profiles"]
        .as_array()
        .unwrap()
        .iter()
        .map(|column| (column["name"].as_str().unwrap().to_string(), column.clone()))
        .collect()
}

/// Every width's serialized columns against `decimal128`'s, and the schema
/// path's types.
fn wrong_against_decimal128(columns_of: &[Column]) -> Vec<String> {
    let reference = columns(&parquet_of(128, columns_of));
    assert_eq!(reference.len(), columns_of.len());

    let mut wrong = Vec::new();
    for width in WIDTHS {
        let file = parquet_of(width, columns_of);
        for (name, profiled) in columns(&file) {
            let (_, expected) = reference.iter().find(|(n, _)| *n == name).unwrap();
            if profiled != *expected {
                wrong.push(format!(
                    "[decimal{width}] {name}: {} / {}, decimal128 {} / {}",
                    profiled["data_type"],
                    profiled["stats"]["Numeric"]["variance"],
                    expected["data_type"],
                    expected["stats"]["Numeric"]["variance"],
                ));
            }
        }
        // The schema path types a column from its Arrow type alone.
        for column in Profiler::new().infer_schema(file.path()).unwrap().columns {
            if column.data_type != DataType::Float {
                wrong.push(format!(
                    "[decimal{width} infer_schema] {}: {:?}",
                    column.name, column.data_type
                ));
            }
        }
    }
    wrong
}

#[test]
fn every_decimal_width_profiles_like_decimal128() {
    let wrong = wrong_against_decimal128(&short_columns());
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn a_column_past_the_sample_is_read_whole_in_every_width() {
    let columns = long_columns();
    let reference = self::columns(&parquet_of(128, &columns));
    let stats = &reference[0].1["stats"]["Numeric"];
    // Every value but the first cancels out: only the whole column has this mean.
    assert_eq!(
        (
            stats["min"].as_f64(),
            stats["max"].as_f64(),
            stats["mean"].as_f64()
        ),
        (Some(-99.99), Some(250.0), Some(0.0125))
    );
    let wrong = wrong_against_decimal128(&columns);
    assert!(wrong.is_empty(), "{wrong:#?}");
}
