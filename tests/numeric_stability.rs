//! Numeric aggregates stay stable on every engine (#670, #671).
//!
//! Two failures found in a dogfooding matrix. Both survive report rounding and
//! both read as ordinary numbers, which is what makes them dangerous:
//!
//! * `sum_squares - n * mean²` cancels away the spread of a column sitting on
//!   a large offset. Four consecutive integers near 1e9 were reported with a
//!   variance of exactly 0.0 — a varying column described as constant — and
//!   near 1e8 with a variance 60% too large.
//! * A naive running sum drops a small contribution between large values that
//!   cancel (the mean of `[1e16, 1.0, -1e16]` came back as 0.0 instead of 1/3)
//!   and overflows where the mean itself is representable (`[1e308, 1e308]`
//!   gave `inf`, which then vanished from the serialized report).
//!
//! The engines disagreed with each other on these inputs, so every assertion
//! runs over all of them plus the serialized report.

use std::io::Write;

use dataprof::{
    ColumnStats, CsvParserConfig, EngineType, NumericStats, Profiler, analyze_csv_file,
};
use serde_json::Value;
use tempfile::NamedTempFile;

/// Sample variance of four consecutive integers, at any offset.
const CONSECUTIVE_FOUR_VARIANCE: f64 = 5.0 / 3.0;

fn csv_with(values: &[f64]) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "x").unwrap();
    for value in values {
        writeln!(file, "{value}").unwrap();
    }
    file.flush().unwrap();
    file
}

/// The numeric stats every CSV engine reports for `values`, each paired with
/// the same column as the report serializes it.
fn stats_per_engine(values: &[f64]) -> Vec<(&'static str, NumericStats, Value)> {
    let file = csv_with(values);
    let path = file.path();

    let reports = vec![
        (
            "standard",
            analyze_csv_file(path, &CsvParserConfig::default()).expect("standard analysis"),
        ),
        (
            "auto",
            Profiler::new()
                .engine(EngineType::Auto)
                .analyze_file(path)
                .expect("auto analysis"),
        ),
        (
            "incremental",
            Profiler::new()
                .engine(EngineType::Incremental)
                .analyze_file(path)
                .expect("incremental analysis"),
        ),
        (
            "columnar",
            Profiler::new()
                .engine(EngineType::Columnar)
                .analyze_file(path)
                .expect("columnar analysis"),
        ),
    ];

    reports
        .into_iter()
        .map(|(engine, report)| {
            let serialized = serde_json::to_value(&report).expect("report serializes");
            let column = serialized["column_profiles"][0]["stats"]["Numeric"].clone();
            let profile = report
                .column_profiles
                .into_iter()
                .next()
                .expect("one column");
            match profile.stats {
                ColumnStats::Numeric(stats) => (engine, stats, column),
                other => panic!("{engine} reported {other:?} for a numeric column"),
            }
        })
        .collect()
}

#[track_caller]
fn assert_close(engine: &str, field: &str, actual: f64, expected: f64, tolerance: f64) {
    assert!(
        (actual - expected).abs() <= tolerance,
        "{engine} reported {field} {actual}, expected {expected} (tolerance {tolerance})"
    );
}

/// Equality, not a tolerance: a stable accumulation of four values reaches the
/// correctly rounded answer, and every engine here does. A tolerance would let
/// a path drift without saying so.
#[track_caller]
fn assert_exact(engine: &str, field: &str, actual: f64, expected: f64) {
    assert_eq!(actual, expected, "{engine} reported {field} {actual}");
}

/// The serialized value, which is rounded and therefore what a quality gate
/// reading a report actually sees.
#[track_caller]
fn serialized(column: &Value, field: &str, engine: &str) -> f64 {
    column[field]
        .as_f64()
        .unwrap_or_else(|| panic!("{engine} serialized {field} as {}", column[field]))
}

#[test]
fn variance_survives_a_large_offset_on_every_engine() {
    for base in [1e6, 1e8, 1e9, 1e12] {
        let values: Vec<f64> = (0..4).map(|i| base + i as f64).collect();
        for (engine, stats, column) in stats_per_engine(&values) {
            let engine = &format!("{engine} at offset {base}");
            assert_exact(engine, "mean", stats.mean, base + 1.5);
            assert_exact(
                engine,
                "variance",
                stats.variance,
                CONSECUTIVE_FOUR_VARIANCE,
            );
            assert_exact(
                engine,
                "std_dev",
                stats.std_dev,
                CONSECUTIVE_FOUR_VARIANCE.sqrt(),
            );
            // Rounding cannot rescue this: 0.0 and 2.6667 round to themselves.
            assert_exact(
                engine,
                "serialized variance",
                serialized(&column, "variance", engine),
                1.6667,
            );
        }
    }
}

/// A genuinely constant column must still report zero variance — the half a
/// stability fix could get wrong by inventing spread out of rounding noise.
#[test]
fn a_constant_column_still_reports_zero_variance() {
    for value in [0.0, 1.0, 1e9] {
        for (engine, stats, column) in stats_per_engine(&[value; 4]) {
            assert_eq!(stats.variance, 0.0, "{engine} at value {value}");
            assert_eq!(stats.std_dev, 0.0, "{engine} at value {value}");
            assert_eq!(stats.mean, value, "{engine} at value {value}");
            assert_eq!(
                serialized(&column, "variance", engine),
                0.0,
                "{engine} at value {value}"
            );
        }
    }
}

#[test]
fn mean_survives_cancelling_values_in_any_order() {
    // The unit contribution is what a naive sum drops; the order decides
    // whether it is dropped, so both permutations run.
    for values in [[1e16, 1.0, -1e16], [1e16, -1e16, 1.0]] {
        for (engine, stats, column) in stats_per_engine(&values) {
            let engine = &format!("{engine} on {values:?}");
            assert_exact(engine, "mean", stats.mean, 1.0 / 3.0);
            assert_exact(
                engine,
                "serialized mean",
                serialized(&column, "mean", engine),
                0.3333,
            );
        }
    }
}

#[test]
fn mean_stays_finite_when_the_naive_sum_overflows() {
    for (engine, stats, column) in stats_per_engine(&[1e308, 1e308]) {
        assert_eq!(stats.mean, 1e308, "{engine}");
        assert_eq!(serialized(&column, "mean", engine), 1e308, "{engine}");
    }
}

/// A column long enough to reach the SIMD accumulation, whose spread only
/// survives if the lanes are as stable as the scalar path.
#[test]
fn stability_holds_past_the_simd_threshold() {
    let values: Vec<f64> = (0..1_000).map(|i| 1e9 + (i % 4) as f64).collect();
    let expected_variance = {
        // Sample variance of 250 copies each of 0, 1, 2, 3.
        let n = values.len() as f64;
        let squared_deviation: f64 = (0..4).map(|i| 250.0 * (i as f64 - 1.5).powi(2)).sum();
        squared_deviation / (n - 1.0)
    };

    for (engine, stats, column) in stats_per_engine(&values) {
        assert_close(engine, "mean", stats.mean, 1e9 + 1.5, 1e-6);
        assert_close(engine, "variance", stats.variance, expected_variance, 1e-6);
        assert_close(
            engine,
            "serialized variance",
            serialized(&column, "variance", engine),
            expected_variance,
            1e-4,
        );
    }
}

#[cfg(feature = "parquet")]
mod parquet_path {
    use std::sync::Arc;

    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use dataprof::{ColumnStats, NumericStats, Profiler};
    use parquet::arrow::ArrowWriter;
    use tempfile::NamedTempFile;

    /// One batch per chunk: the accumulators are merged across batches, so a
    /// merge that loses stability shows up here and not in a single-batch file.
    fn parquet_stats(values: &[f64], batch_size: usize) -> NumericStats {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, false)]));
        let file = NamedTempFile::with_suffix(".parquet").unwrap();
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), schema.clone(), None).unwrap();
        for batch in values.chunks(batch_size) {
            let array = Float64Array::from(batch.to_vec());
            writer
                .write(&RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap())
                .unwrap();
        }
        writer.close().unwrap();

        let report = Profiler::new()
            .analyze_file(file.path())
            .expect("parquet analysis");
        match report.column_profiles.into_iter().next().unwrap().stats {
            ColumnStats::Numeric(stats) => stats,
            other => panic!("parquet reported {other:?} for a numeric column"),
        }
    }

    #[test]
    fn parquet_aggregates_are_stable_across_row_groups() {
        let values: Vec<f64> = (0..400).map(|i| 1e9 + (i % 4) as f64).collect();
        let expected_variance = {
            let squared_deviation: f64 = (0..4).map(|i| 100.0 * (i as f64 - 1.5).powi(2)).sum();
            squared_deviation / (values.len() as f64 - 1.0)
        };

        for batch_size in [400, 64, 7] {
            let stats = parquet_stats(&values, batch_size);
            assert!(
                (stats.mean - (1e9 + 1.5)).abs() < 1e-6,
                "batch size {batch_size} reported mean {}",
                stats.mean
            );
            assert!(
                (stats.variance - expected_variance).abs() < 1e-6,
                "batch size {batch_size} reported variance {}",
                stats.variance
            );
        }
    }

    #[test]
    fn parquet_mean_survives_cancellation_and_overflow() {
        let cancelling = parquet_stats(&[1e16, 1.0, -1e16], 3);
        assert!(
            (cancelling.mean - 1.0 / 3.0).abs() < 1e-9,
            "reported mean {}",
            cancelling.mean
        );

        let overflowing = parquet_stats(&[1e308, 1e308], 1);
        assert_eq!(overflowing.mean, 1e308);
    }
}
