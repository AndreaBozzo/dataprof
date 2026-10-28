//! A spread too small to square in `f64` keeps its scale-free statistics (#1088).
//!
//! Values near 1e-200 have squared deviations that underflow to zero. The
//! variance read 0.0, and the coefficient of variation and the kurtosis then
//! described a constant column. They do not depend on scale, so every column
//! from 1e-300 to 1e100 must report those of the same values at unit scale, and
//! a standard deviation scaled with the values. A variance below `f64`'s range
//! is reported as `null`, as one above it is (#805). From 1e160 the spread
//! itself overflows, and below about 1e-304 subnormal rounding takes its
//! digits; every statistic derived from it is then absent rather than wrong.
//!
//! `cycled` repeats its pattern every row, so each four-lane SIMD lane is
//! constant and the spread is all in the merge of the lanes; `blocked` repeats
//! it in blocks of four, so each lane varies.
//!
//! `python/tests/test_small_spread_variance.py` is the Python twin.

use std::io::Write;

use dataprof::{
    ColumnProfile, ColumnStats, CsvParserConfig, EngineType, NumericStats, ProfileReport, Profiler,
    analyze_column, analyze_csv_file,
};
use tempfile::NamedTempFile;

const ROWS: usize = 104;
const PATTERN: [f64; 4] = [1.0, 2.0, 4.0, 9.0];
const SHAPES: [&str; 2] = ["cycled", "blocked"];
/// What the variance, about the scale squared, does in `f64`.
#[derive(Clone, Copy, PartialEq)]
enum Fate {
    Fits,
    /// Reported `null`; the standard deviation and what follows from it kept.
    Underflows,
    /// The standard deviation and everything derived from it absent too.
    Overflows,
    /// Lost to subnormal rounding: absent as for `Overflows`.
    Lost,
}

const SCALES: [(&str, Fate); 9] = [
    ("1", Fate::Fits),
    ("1e-160", Fate::Fits),
    ("1e-200", Fate::Underflows),
    ("1e-300", Fate::Underflows),
    ("1e-313", Fate::Lost),
    ("5e-324", Fate::Lost),
    ("1e100", Fate::Fits),
    ("1e160", Fate::Overflows),
    ("1e300", Fate::Overflows),
];

fn names() -> Vec<String> {
    SHAPES
        .iter()
        .flat_map(|shape| {
            SCALES
                .iter()
                .map(move |(scale, _)| format!("{shape}_{scale}"))
        })
        .collect()
}

fn value(name: &str, row: usize) -> f64 {
    let (shape, scale) = name.split_once('_').unwrap();
    let index = if shape == "cycled" {
        row % 4
    } else {
        (row / 4) % 4
    };
    PATTERN[index] * scale.parse::<f64>().unwrap()
}

fn text(name: &str, row: usize) -> String {
    format!("{:e}", value(name, row))
}

fn csv_fixture() -> NamedTempFile {
    let names = names();
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "{}", names.join(",")).unwrap();
    for row in 0..ROWS {
        let cells: Vec<String> = names.iter().map(|name| text(name, row)).collect();
        writeln!(file, "{}", cells.join(",")).unwrap();
    }
    file.flush().unwrap();
    file
}

fn json_records() -> Vec<String> {
    let names = names();
    (0..ROWS)
        .map(|row| {
            let fields: Vec<String> = names
                .iter()
                .map(|name| format!(r#""{name}":{}"#, text(name, row)))
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

    use arrow::array::{ArrayRef, Float64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let names = names();
    let schema = Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(name, DataType::Float64, false))
            .collect::<Vec<_>>(),
    ));
    let columns: Vec<ArrayRef> = names
        .iter()
        .map(|name| {
            Arc::new(Float64Array::from(
                (0..ROWS).map(|row| value(name, row)).collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

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
    let in_memory = names()
        .iter()
        .map(|name| {
            let values: Vec<String> = (0..ROWS).map(|row| text(name, row)).collect();
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

/// The documented relative tolerance for raw floats.
fn close(got: Option<f64>, expected: Option<f64>) -> bool {
    match (got, expected) {
        (Some(got), Some(expected)) => (got - expected).abs() <= 1e-9 * expected.abs(),
        _ => false,
    }
}

#[test]
fn every_path_keeps_the_scale_free_statistics_of_a_tiny_spread() {
    let mut wrong = Vec::new();
    for (label, columns) in profiles() {
        for shape in SHAPES {
            let unit = match numeric(&columns, &format!("{shape}_1")) {
                Ok(stats) => stats,
                Err(error) => {
                    wrong.push(format!("[{label}] {error}"));
                    continue;
                }
            };
            for (scale, fate) in SCALES {
                let name = format!("{shape}_{scale}");
                let stats = match numeric(&columns, &name) {
                    Ok(stats) => stats,
                    Err(error) => {
                        wrong.push(format!("[{label}] {error}"));
                        continue;
                    }
                };
                let factor: f64 = scale.parse().unwrap();
                let checks = [
                    (
                        "cv",
                        stats.coefficient_of_variation,
                        unit.coefficient_of_variation,
                    ),
                    ("skewness", stats.skewness, unit.skewness),
                    ("kurtosis", stats.kurtosis, unit.kurtosis),
                    ("std_dev", stats.std_dev, unit.std_dev.map(|s| s * factor)),
                ];
                for (key, got, expected) in checks {
                    let right = if matches!(fate, Fate::Overflows | Fate::Lost) {
                        got.is_none()
                    } else {
                        close(got, expected)
                    };
                    if !right {
                        wrong.push(format!(
                            "[{label}] {name} {key}: {got:?}, unit {expected:?}"
                        ));
                    }
                }
                if stats.variance.is_none() != (fate != Fate::Fits) {
                    wrong.push(format!("[{label}] {name} variance: {:?}", stats.variance));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}
