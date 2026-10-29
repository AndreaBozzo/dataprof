//! A decimal's spread survives digits that `f64` does not keep (#1149).
//!
//! `1000000000000000.01` and `1000000000000000.05` are one `f64`, so a column
//! of cents on 10^15 reported a variance, standard deviation, skewness and
//! kurtosis of 0.0 although every value differs, and 15 digits on 10^13
//! reported a standard deviation off in its third digit. Such a literal is now
//! held as a scaled integer, and its deviations taken exactly, so each
//! statistic is the one of the cents alone. Where a value is too wide to hold,
//! as a `decimal256` of 76 digits is, a spread too small to stand above its
//! rounding is absent rather than 0.0.
//!
//! A column typed as a decimal is held exactly from 11 significant digits on
//! every Arrow and Parquet route. Text is held exactly from 18, the most a
//! float's shortest form has fewer than, so below that a CSV still reads
//! `9999999999999.01` as the float it may be (#783): the 15-digit column is
//! checked on the typed route only. Every engine over the CSV, the JSON readers
//! (with the values in strings) and `analyze_column` read the rest as Parquet
//! does. `python/tests/test_decimal_spread.py` is the Python twin, and adds the
//! in-memory Arrow, pandas, polars and `Decimal` record routes.

use std::io::Write;

use dataprof::{
    ColumnProfile, ColumnStats, CsvParserConfig, EngineType, NumericStats, ProfileReport, Profiler,
    analyze_column, analyze_csv_file,
};
use tempfile::NamedTempFile;

/// Hundredths added to each column's base, one per row.
const CENTS: [u32; 8] = [1, 5, 2, 0, 9, 1, 5, 3];

/// `decimal128(18, 2)`: 10^15 and its cents.
const CENTS_BASE: &str = "1000000000000000";
/// `decimal128(15, 2)`: an `f64` keeps every value apart, and rounds each
/// by up to a tenth of a cent.
const FIFTEEN_BASE: &str = "9999999999999";
/// `decimal128(38, 2)`: the widest a 128-bit decimal holds.
const WIDE_BASE: &str = "123456789012345678901234567890123456";

fn cell(name: &str, row: usize) -> String {
    let cents = CENTS[row];
    match name {
        "cents" => format!("{CENTS_BASE}.{cents:02}"),
        "fifteen" => format!("{FIFTEEN_BASE}.{cents:02}"),
        "wide_cents" => format!("{WIDE_BASE}.{cents:02}"),
        // `decimal256(76, 0)`: 10^75 plus a multiple of 100, beyond `i128`.
        "unheld" => format!("1{}{:03}", "0".repeat(72), cents * 100),
        _ => unreachable!("{name}"),
    }
}

const NAMES: [&str; 4] = ["cents", "fifteen", "wide_cents", "unheld"];
/// Held exactly only where the source types it as a decimal.
const TYPED_ONLY: &str = "fifteen";
/// The routes that read a typed decimal column.
const TYPED_ROUTES: [&str; 1] = ["parquet"];

fn csv_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "{}", NAMES.join(",")).unwrap();
    for row in 0..CENTS.len() {
        let cells: Vec<String> = NAMES.iter().map(|name| cell(name, row)).collect();
        writeln!(file, "{}", cells.join(",")).unwrap();
    }
    file.flush().unwrap();
    file
}

/// The values as JSON strings: a JSON number is an `f64` before it reaches
/// the profiler.
fn json_records() -> Vec<String> {
    (0..CENTS.len())
        .map(|row| {
            let fields: Vec<String> = NAMES
                .iter()
                .map(|name| format!("\"{name}\":\"{}\"", cell(name, row)))
                .collect();
            format!("{{{}}}", fields.join(","))
        })
        .collect()
}

fn json_fixture(suffix: &str, contents: String) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    write!(file, "{contents}").unwrap();
    file.flush().unwrap();
    file
}

#[cfg(feature = "parquet")]
fn parquet_fixture() -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Decimal128Array, Decimal256Array};
    use arrow::datatypes::{Field, Schema, i256};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let unscaled = |name: &str, row: usize| cell(name, row).replace('.', "");
    let decimal128 = |name: &str, precision: u8| -> ArrayRef {
        let values: Vec<i128> = (0..CENTS.len())
            .map(|row| unscaled(name, row).parse().unwrap())
            .collect();
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(precision, 2)
                .unwrap(),
        )
    };
    let unheld: Vec<i256> = (0..CENTS.len())
        .map(|row| i256::from_string(&cell("unheld", row)).unwrap())
        .collect();
    let columns = vec![
        decimal128("cents", 18),
        decimal128("fifteen", 15),
        decimal128("wide_cents", 38),
        Arc::new(
            Decimal256Array::from(unheld)
                .with_precision_and_scale(76, 0)
                .unwrap(),
        ) as ArrayRef,
    ];
    let schema = Arc::new(Schema::new(
        NAMES
            .iter()
            .zip(&columns)
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), false))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// Every engine over the CSV, then every other format, then `analyze_column`.
fn profiles() -> Vec<(String, Vec<ColumnProfile>)> {
    let csv = csv_fixture();
    let mut reports: Vec<(String, ProfileReport)> = vec![(
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

    let records = json_records();
    let mut files = vec![
        (
            "json",
            json_fixture(".json", format!("[{}]", records.join(","))),
        ),
        ("jsonl", json_fixture(".jsonl", records.join("\n") + "\n")),
    ];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_fixture()));
    for (label, file) in files {
        let report = Profiler::new()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }

    let mut profiles: Vec<(String, Vec<ColumnProfile>)> = reports
        .into_iter()
        .map(|(label, report)| (label, report.column_profiles))
        .collect();
    let in_memory = NAMES
        .iter()
        .map(|name| {
            let values: Vec<String> = (0..CENTS.len()).map(|row| cell(name, row)).collect();
            analyze_column(name, &values)
        })
        .collect();
    profiles.push(("analyze_column".to_string(), in_memory));
    profiles
}

fn numeric<'a>(columns: &'a [ColumnProfile], name: &str) -> Result<&'a NumericStats, String> {
    let column = columns
        .iter()
        .find(|column| column.name == name)
        .ok_or_else(|| format!("no column {name}"))?;
    match &column.stats {
        ColumnStats::Numeric(stats) => Ok(stats),
        other => Err(format!("{name} has no numeric stats: {other:?}")),
    }
}

/// Variance, standard deviation, skewness and excess kurtosis of the cents as
/// hundredths, which `f64` holds to its last bit: a spread does not depend on
/// the base it sits on.
fn spread_of_the_cents() -> [f64; 4] {
    let values: Vec<f64> = CENTS.iter().map(|&c| f64::from(c) / 100.0).collect();
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let moment = |k: i32| values.iter().map(|v| (v - mean).powi(k)).sum::<f64>();
    let variance = moment(2) / (n - 1.0);
    let std_dev = variance.sqrt();
    let skewness = n / ((n - 1.0) * (n - 2.0)) * moment(3) / std_dev.powi(3);
    let kurtosis = n * (n + 1.0) / ((n - 1.0) * (n - 2.0) * (n - 3.0)) * moment(4)
        / std_dev.powi(4)
        - 3.0 * (n - 1.0).powi(2) / ((n - 2.0) * (n - 3.0));
    [variance, std_dev, skewness, kurtosis]
}

/// The documented relative tolerance for raw floats.
fn close(got: Option<f64>, expected: f64) -> bool {
    got.is_some_and(|got| (got - expected).abs() <= 1e-9 * expected.abs())
}

#[test]
fn every_path_keeps_the_spread_of_digits_beyond_f64() {
    let [variance, std_dev, skewness, kurtosis] = spread_of_the_cents();
    let mut wrong = Vec::new();
    for (label, columns) in profiles() {
        for name in NAMES {
            if name == TYPED_ONLY && !TYPED_ROUTES.contains(&label.as_str()) {
                continue;
            }
            let stats = match numeric(&columns, name) {
                Ok(stats) => stats,
                Err(error) => {
                    wrong.push(format!("[{label}] {error}"));
                    continue;
                }
            };
            let checks = [
                ("variance", stats.variance, variance),
                ("std_dev", stats.std_dev, std_dev),
                ("skewness", stats.skewness, skewness),
                ("kurtosis", stats.kurtosis, kurtosis),
                (
                    "cv",
                    stats.coefficient_of_variation,
                    std_dev / stats.mean * 100.0,
                ),
            ];
            for (key, got, expected) in checks {
                let right = if name == "unheld" {
                    got.is_none()
                } else {
                    close(got, expected)
                };
                if !right {
                    wrong.push(format!("[{label}] {name} {key}: {got:?}, exact {expected}"));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}
