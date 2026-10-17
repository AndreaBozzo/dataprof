//! A variance that fits in `f64` where its sum of squares does not (#805).
//!
//! `fits` holds `1e154` and `-1e154` in alternating blocks of four, 52 each:
//! its sample variance, 1.04e310 / 103, is about 1.01e308, but Welford's M2 (the
//! variance times 103) and every sign-changing increment (2e308) overflow, and
//! `variance` and `std_dev` used to be reported `null`. The blocks flip every
//! four-lane SIMD lane too. `exceeds` is ten times the spread, a variance of
//! about 1e310, which no `f64` holds: it still has to report `null`.
//!
//! `python/tests/test_large_spread_variance.py` is the Python twin.

use std::io::Write;

use dataprof::{
    ColumnProfile, ColumnStats, CsvParserConfig, EngineType, ProfileReport, Profiler,
    analyze_column, analyze_csv_file,
};
use tempfile::NamedTempFile;

const ROWS: usize = 104;

/// 1.04e310 / 103 by exact arithmetic.
const FITS_VARIANCE: f64 = 1.009_708_737_864_077_7e308;

fn value(column: &str, row: usize) -> &'static str {
    match (column, (row / 4) % 2) {
        ("fits", 0) => "1e154",
        ("fits", _) => "-1e154",
        (_, 0) => "1e155",
        (_, _) => "-1e155",
    }
}

fn csv_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "fits,exceeds").unwrap();
    for row in 0..ROWS {
        writeln!(file, "{},{}", value("fits", row), value("exceeds", row)).unwrap();
    }
    file.flush().unwrap();
    file
}

fn json_records() -> Vec<String> {
    (0..ROWS)
        .map(|row| {
            format!(
                r#"{{"fits":{},"exceeds":{}}}"#,
                value("fits", row),
                value("exceeds", row)
            )
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

    use arrow::array::{ArrayRef, Float64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let column = |name: &str| -> ArrayRef {
        Arc::new(Float64Array::from(
            (0..ROWS)
                .map(|row| value(name, row).parse::<f64>().unwrap())
                .collect::<Vec<_>>(),
        ))
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("fits", DataType::Float64, false),
        Field::new("exceeds", DataType::Float64, false),
    ]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![column("fits"), column("exceeds")]).unwrap();

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
    let in_memory = ["fits", "exceeds"]
        .iter()
        .map(|name| {
            let values: Vec<String> = (0..ROWS).map(|row| value(name, row).to_string()).collect();
            analyze_column(name, &values)
        })
        .collect();
    profiles.push(("analyze_column".to_string(), in_memory));
    profiles
}

/// Variance and standard deviation.
fn spread(columns: &[ColumnProfile], name: &str) -> Result<(Option<f64>, Option<f64>), String> {
    let column = columns
        .iter()
        .find(|column| column.name == name)
        .ok_or_else(|| format!("no column {name}"))?;
    match &column.stats {
        ColumnStats::Numeric(stats) => Ok((stats.variance, stats.std_dev)),
        other => Err(format!("{name} has no numeric stats: {other:?}")),
    }
}

/// The documented relative tolerance for raw floats.
fn close(got: Option<f64>, expected: f64) -> bool {
    got.is_some_and(|got| (got - expected).abs() <= 1e-9 * expected.abs())
}

#[test]
fn every_path_reports_a_variance_that_fits() {
    let mut wrong = Vec::new();
    for (label, columns) in profiles() {
        match spread(&columns, "fits") {
            Ok((variance, std_dev)) => {
                if !close(variance, FITS_VARIANCE) || !close(std_dev, FITS_VARIANCE.sqrt()) {
                    wrong.push(format!(
                        "[{label}] fits: variance {variance:?}, std_dev {std_dev:?}"
                    ));
                }
            }
            Err(error) => wrong.push(format!("[{label}] {error}")),
        }
        // Guards against over-correction: this spread does not fit.
        match spread(&columns, "exceeds") {
            Ok((None, None)) => {}
            other => wrong.push(format!("[{label}] exceeds: {other:?}")),
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}
