//! A sampled dimension's score estimates the score over every scanned value
//! (#1293).
//!
//! Each column keeps a reservoir of at most 10,000 non-null values, and the
//! sampled dimensions were scored over those reservoirs pooled together. A
//! column then weighed as many values as its reservoir kept, so a sparse
//! column read in full counted as much as a dense one that filled its
//! reservoir from many more values. `quality.score_bounds` bounds the score
//! over every value, where each column weighs its non-null values, so the
//! reported score could fall outside its own interval. Each column's sampled
//! counts are now scaled to its size in the source.
//!
//! `python/tests/test_sampled_score_estimate.py` is the Python twin.

use std::io::Write;

use dataprof::{CsvParserConfig, EngineType, ProfileReport, Profiler, analyze_csv_file};
use tempfile::NamedTempFile;

const ROWS: usize = 25_000;

const COLUMNS: [&str; 6] = ["dense", "sparse", "qty", "age", "rate", "amount"];

/// Dense columns hold a value in every row and fill their reservoirs from
/// 25,000 values: `dense` clean ISO dates, `qty` small integers, `rate`
/// two-decimal amounts. Sparse columns hold a value in one row in ten, 2,500
/// values all kept, and fail often: `sparse` has a slash date in one of three
/// (a format violation in an ISO column), `age` an impossible 400 in one of
/// five (an outlier and a range violation), `amount` a one-decimal value in
/// one of four. Every sampled dimension pools a dense sample beside a sparse
/// column read in full.
fn rows() -> Vec<[String; 6]> {
    (0..ROWS)
        .map(|row| {
            let dense = format!("2024-03-{:02}", 1 + row % 28);
            let qty = (20 + row % 40).to_string();
            let rate = format!("{}.{}{}", row % 50, row % 10, row % 7);
            let (sparse, age, amount) = if row % 10 == 0 {
                let k = row / 10;
                let sparse = if k % 3 == 0 {
                    format!("{:02}/03/2024", 1 + k % 28)
                } else {
                    format!("2024-04-{:02}", 1 + k % 28)
                };
                let age = if k % 5 == 0 {
                    "400".to_string()
                } else {
                    (20 + k % 40).to_string()
                };
                let amount = if k % 4 == 0 {
                    format!("{}.5", k % 50)
                } else {
                    format!("{}.{}{}", k % 50, k % 7, k % 3)
                };
                (sparse, age, amount)
            } else {
                (String::new(), String::new(), String::new())
            };
            [dense, sparse, qty, age, rate, amount]
        })
        .collect()
}

/// The consistency score over every value: the slash dates over all the
/// non-null values of every column, none of which fails otherwise.
fn full_source_consistency() -> f64 {
    let rows = rows();
    let checked = rows.iter().flatten().filter(|v| !v.is_empty()).count();
    let slash = rows.iter().filter(|row| row[1].contains('/')).count();
    100.0 - 100.0 * slash as f64 / checked as f64
}

fn csv_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "{}", COLUMNS.join(",")).unwrap();
    for row in rows() {
        writeln!(file, "{}", row.join(",")).unwrap();
    }
    file.flush().unwrap();
    file
}

/// One JSON object per row, every value a string and an empty one null.
fn json_records() -> Vec<String> {
    rows()
        .into_iter()
        .map(|row| {
            let fields: Vec<String> = COLUMNS
                .iter()
                .zip(row.iter())
                .map(|(name, value)| {
                    if value.is_empty() {
                        format!(r#""{name}":null"#)
                    } else {
                        format!(r#""{name}":"{value}""#)
                    }
                })
                .collect();
            format!("{{{}}}", fields.join(","))
        })
        .collect()
}

fn jsonl_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".jsonl").unwrap();
    for record in json_records() {
        writeln!(file, "{record}").unwrap();
    }
    file.flush().unwrap();
    file
}

fn json_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".json").unwrap();
    write!(file, "[{}]", json_records().join(",")).unwrap();
    file.flush().unwrap();
    file
}

#[cfg(feature = "parquet")]
fn parquet_fixture() -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let rows = rows();
    let schema = Arc::new(Schema::new(
        COLUMNS
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let arrays: Vec<ArrayRef> = (0..COLUMNS.len())
        .map(|index| {
            let values: Vec<Option<&str>> = rows
                .iter()
                .map(|row| Some(row[index].as_str()).filter(|value| !value.is_empty()))
                .collect();
            Arc::new(StringArray::from(values)) as ArrayRef
        })
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();

    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

fn profiler() -> Profiler {
    Profiler::new().reference_date(chrono::NaiveDate::from_ymd_opt(2024, 6, 1).unwrap())
}

/// Every engine over the CSV, every other format, and the async route.
fn reports() -> Vec<(String, ProfileReport)> {
    let csv = csv_fixture();
    let mut reports = vec![(
        "standard".to_string(),
        analyze_csv_file(csv.path(), &CsvParserConfig::default()).expect("standard CSV"),
    )];
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = profiler()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
        reports.push((format!("{engine:?}"), report));
    }

    let mut files = vec![("json", json_fixture()), ("jsonl", jsonl_fixture())];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_fixture()));
    for (label, file) in &files {
        let report = profiler()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }

    #[cfg(feature = "async-streaming")]
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = runtime
            .block_on(profiler().profile_file(csv.path()))
            .expect("async CSV");
        reports.push(("async csv".to_string(), report));
    }
    reports
}

#[test]
fn every_sampled_score_lies_within_its_own_bounds() {
    let mut wrong = Vec::new();
    let mut bounded = 0;
    for (label, report) in reports() {
        let quality = report.quality.as_ref().expect("quality computed");
        let Some(bounds) = quality.score_bounds() else {
            // A route that scored every value has nothing to estimate.
            continue;
        };
        bounded += 1;
        let scores = quality.scores();
        let mut pairs: Vec<(String, Option<f64>, Option<_>)> = scores
            .dimension_scores
            .iter()
            .map(|(dimension, score)| {
                let bound = bounds.dimension_scores.get(dimension).copied().flatten();
                (dimension.clone(), *score, bound)
            })
            .collect();
        pairs.push((
            "overall".to_string(),
            scores.overall_score,
            bounds.overall_score,
        ));
        for (dimension, score, bound) in pairs {
            let (Some(score), Some(bound)) = (score, bound) else {
                continue;
            };
            let score = (score * 100.0).round() / 100.0;
            if score < bound.lower || score > bound.upper {
                wrong.push(format!(
                    "[{label}] {dimension} {score} outside [{}, {}]",
                    bound.lower, bound.upper
                ));
            }
        }
    }
    assert!(bounded > 0, "no route sampled the fixture");
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn the_consistency_score_estimates_the_full_source_one() {
    let exact = full_source_consistency();
    let mut wrong = Vec::new();
    for (label, report) in reports() {
        let quality = report.quality.as_ref().expect("quality computed");
        let score = quality.scores().dimension_scores["consistency"].expect("consistency");
        // The sparse column is read in full and the dense one is clean, so
        // the estimate is the full-source score up to float rounding.
        if (score - exact).abs() > 0.01 {
            wrong.push(format!("[{label}] {score} against {exact}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}
