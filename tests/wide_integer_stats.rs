//! The spread of 64-bit integers beyond 2^53 (#877).
//!
//! Statistics used to be taken on `value as f64`, where integers near 1.76e18
//! (epoch nanoseconds) sit 256 apart, so values a few units apart reported a
//! variance of 0.0. Skewness and kurtosis do not depend on a shift, and the
//! variance and standard deviation do not either, so each wide column here has
//! a twin holding the same values minus the first one, small enough to be exact
//! as `f64`: every path has to report the same spread and shape for both.
//!
//! `python/tests/test_wide_integer_stats.py` is the Python twin.

use std::io::Write;

use dataprof::{
    ColumnProfile, ColumnStats, CsvParserConfig, EngineType, ProfileReport, Profiler,
    analyze_column, analyze_csv_file,
};
use tempfile::NamedTempFile;

const COLUMNS: [&str; 4] = ["reading", "reading_offset", "counter", "counter_offset"];

/// `reading` is signed (Arrow `Int64`), `counter` lies beyond `i64::MAX`
/// (Arrow `UInt64`). Each `_offset` column is its twin minus the first value.
const ROWS: [[&str; 4]; 4] = [
    ["1759700000123456789", "0", "18446744073709551000", "0"],
    ["1759700000123456791", "2", "18446744073709551005", "5"],
    ["1759700000123456795", "6", "18446744073709551010", "10"],
    ["1759700000123456800", "11", "18446744073709551015", "15"],
];

/// (wide column, its offset twin, the twin's exact sample variance).
const PAIRS: [(&str, &str, f64); 2] = [
    ("reading", "reading_offset", 283.0 / 12.0),
    ("counter", "counter_offset", 125.0 / 3.0),
];

fn csv_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "{}", COLUMNS.join(",")).unwrap();
    for row in ROWS {
        writeln!(file, "{}", row.join(",")).unwrap();
    }
    file.flush().unwrap();
    file
}

/// JSON numbers, not strings: the readers have to keep them exact too.
fn json_records() -> Vec<String> {
    ROWS.iter()
        .map(|row| {
            let fields: Vec<String> = COLUMNS
                .iter()
                .zip(row)
                .map(|(name, value)| format!(r#""{name}":{value}"#))
                .collect();
            format!("{{{}}}", fields.join(","))
        })
        .collect()
}

fn json_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".json").unwrap();
    write!(file, "[{}]", json_records().join(",")).unwrap();
    file.flush().unwrap();
    file
}

fn jsonl_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".jsonl").unwrap();
    writeln!(file, "{}", json_records().join("\n")).unwrap();
    file.flush().unwrap();
    file
}

#[cfg(feature = "parquet")]
fn parquet_fixture() -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let column = |index: usize| ROWS.map(|row| row[index]);
    let signed = |index: usize| -> ArrayRef {
        Arc::new(Int64Array::from(
            column(index)
                .map(|value| value.parse::<i64>().unwrap())
                .to_vec(),
        ))
    };
    let unsigned = |index: usize| -> ArrayRef {
        Arc::new(UInt64Array::from(
            column(index)
                .map(|value| value.parse::<u64>().unwrap())
                .to_vec(),
        ))
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new(COLUMNS[0], DataType::Int64, false),
        Field::new(COLUMNS[1], DataType::Int64, false),
        Field::new(COLUMNS[2], DataType::UInt64, false),
        Field::new(COLUMNS[3], DataType::UInt64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![signed(0), signed(1), unsigned(2), unsigned(3)],
    )
    .unwrap();

    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// Every engine over the CSV, then every other format, then the database
/// connectors' `analyze_column`.
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

    let mut files = vec![("json", json_fixture()), ("jsonl", jsonl_fixture())];
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
    let in_memory = COLUMNS
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let values: Vec<String> = ROWS.iter().map(|row| row[index].to_string()).collect();
            analyze_column(name, &values)
        })
        .collect();
    profiles.push(("analyze_column".to_string(), in_memory));
    profiles
}

/// Variance, standard deviation, skewness and kurtosis, in that order.
fn spread(columns: &[ColumnProfile], name: &str) -> Result<[Option<f64>; 4], String> {
    let column = columns
        .iter()
        .find(|column| column.name == name)
        .ok_or_else(|| format!("no column {name}"))?;
    match &column.stats {
        ColumnStats::Numeric(stats) => Ok([
            stats.variance,
            stats.std_dev,
            stats.skewness,
            stats.kurtosis,
        ]),
        other => Err(format!("{name} has no numeric stats: {other:?}")),
    }
}

/// The documented raw-float tolerances: relative 1e-9, absolute 1e-12.
fn close(left: Option<f64>, right: Option<f64>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => (left - right).abs() <= 1e-12_f64.max(1e-9 * right.abs()),
        (None, None) => true,
        _ => false,
    }
}

#[test]
fn every_path_keeps_the_spread_of_large_integers() {
    let mut wrong = Vec::new();
    for (label, columns) in profiles() {
        for (wide, twin, variance) in PAIRS {
            let (wide_spread, twin_spread) = match (spread(&columns, wide), spread(&columns, twin))
            {
                (Ok(wide), Ok(twin)) => (wide, twin),
                (left, right) => {
                    wrong.push(format!("[{label}] {left:?} / {right:?}"));
                    continue;
                }
            };
            // The twin is exact as `f64`; it anchors the comparison.
            if !close(twin_spread[0], Some(variance)) {
                wrong.push(format!(
                    "[{label}] {twin}: variance {:?}, exact {variance}",
                    twin_spread[0]
                ));
            }
            for (statistic, (got, expected)) in ["variance", "std_dev", "skewness", "kurtosis"]
                .iter()
                .zip(wide_spread.into_iter().zip(twin_spread))
            {
                if !close(got, expected) {
                    wrong.push(format!(
                        "[{label}] {wide}.{statistic}: {got:?}, {twin} reports {expected:?}"
                    ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// A scale-0 decimal column, as Spark and database exports store 64-bit ids
/// and `uint64` values, holds the same integers.
#[cfg(feature = "parquet")]
#[test]
fn a_scale_zero_decimal_column_keeps_the_spread_of_large_integers() {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Decimal128Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let decimal = |index: usize| -> ArrayRef {
        let values = ROWS.map(|row| row[index].parse::<i128>().unwrap());
        Arc::new(
            Decimal128Array::from(values.to_vec())
                .with_precision_and_scale(20, 0)
                .unwrap(),
        )
    };
    let schema = Arc::new(Schema::new(
        COLUMNS
            .iter()
            .map(|name| Field::new(*name, DataType::Decimal128(20, 0), false))
            .collect::<Vec<_>>(),
    ));
    let batch =
        RecordBatch::try_new(schema.clone(), (0..COLUMNS.len()).map(decimal).collect()).unwrap();
    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let columns = Profiler::new()
        .analyze_file(file.path())
        .expect("decimal parquet")
        .column_profiles;
    let mut wrong = Vec::new();
    for (wide, twin, _) in PAIRS {
        let (wide_spread, twin_spread) = (
            spread(&columns, wide).unwrap(),
            spread(&columns, twin).unwrap(),
        );
        if !wide_spread
            .into_iter()
            .zip(twin_spread)
            .all(|(got, expected)| close(got, expected))
        {
            wrong.push(format!(
                "{wide}: {wide_spread:?}, {twin} reports {twin_spread:?}"
            ));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}
