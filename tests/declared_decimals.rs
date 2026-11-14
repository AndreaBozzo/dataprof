//! A column declared a decimal is read as one whether or not statistics run
//! (#1268).
//!
//! The accuracy pass told a decimal column apart only by the exact block of
//! its statistics, so a run that skipped them (`metric_packs([Quality])`) read
//! an Arrow decimal as text, and the `f64` fences missed an outlier the exact
//! ones find: the same data and quality selection reported an `outlier_ratio`
//! of 0 without statistics and 12.5 with them. The column's declared type,
//! recorded since #906, now decides both runs.
//!
//! `CENTS` is `9999999999999 + cents / 100`; its lowest value lies 0.00375
//! below the lower fence, past what the `f64` fences resolve. Each width that
//! holds it is written to Parquet, and both report dialects are read. The
//! database decimals are covered in
//! `crates/dataprof-db/tests/declared_decimals.rs`.
//!
//! `python/tests/test_declared_decimals.py` is the Python twin, and adds the
//! in-memory Arrow, pandas and polars routes.

#![cfg(feature = "parquet")]

use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal64Array, Decimal128Array, Decimal256Array};
use arrow::datatypes::{Field, Schema, i256};
use arrow::record_batch::RecordBatch;
use dataprof::{MetricPack, Profiler};
use parquet::arrow::ArrowWriter;
use serde_json::Value;
use tempfile::NamedTempFile;

const CENTS: [i64; 8] = [80, 173, 195, 209, 222, 255, 284, 331];
const CENTS_BASE: i64 = 999_999_999_999_900;
const PRECISION: u8 = 16;
const SCALE: i8 = 2;

/// The widths whose precision holds 16 digits.
fn decimal(width: u16) -> ArrayRef {
    let unscaled = CENTS.map(|cents| CENTS_BASE + cents);
    match width {
        64 => Arc::new(
            Decimal64Array::from(unscaled.to_vec())
                .with_precision_and_scale(PRECISION, SCALE)
                .unwrap(),
        ),
        128 => Arc::new(
            Decimal128Array::from(unscaled.map(i128::from).to_vec())
                .with_precision_and_scale(PRECISION, SCALE)
                .unwrap(),
        ),
        _ => Arc::new(
            Decimal256Array::from(unscaled.map(|v| i256::from_i128(v.into())).to_vec())
                .with_precision_and_scale(PRECISION, SCALE)
                .unwrap(),
        ),
    }
}

fn parquet_of(width: u16) -> NamedTempFile {
    let array = decimal(width);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "cents",
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

#[test]
fn a_decimal_column_reads_as_a_decimal_with_or_without_statistics() {
    let mut wrong = Vec::new();
    for width in [64, 128, 256] {
        let file = parquet_of(width);
        for (packs, label) in [
            (None, "every pack"),
            (Some(vec![MetricPack::Quality]), "quality only"),
        ] {
            let mut profiler = Profiler::new();
            if let Some(packs) = packs {
                profiler = profiler.metric_packs(packs);
            }
            let report = profiler.analyze_file(file.path()).unwrap();
            let document: Value = serde_json::from_str(&report.to_json().unwrap()).unwrap();
            let summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
            for (dialect, ratio) in [
                (
                    "document",
                    &document["quality"]["metrics"]["accuracy"]["outlier_ratio"],
                ),
                ("summary", &summary["quality"]["accuracy"]["outlier_ratio"]),
            ] {
                if ratio.as_f64() != Some(12.5) {
                    wrong.push(format!("[decimal{width}, {label}, {dialect}] {ratio}"));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
