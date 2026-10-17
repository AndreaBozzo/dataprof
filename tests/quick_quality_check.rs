//! The quality one-liners keep absence absent (#889).
//!
//! `quick_quality_check` and `quick_quality_check_source` used to return
//! `quality_score().unwrap_or(0.0)`, so a file with nothing to assess read as
//! the worst possible data. They now return the report's score unchanged.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowType, Field, Schema};
use arrow::record_batch::RecordBatch;
use dataprof::{DataSource, FileFormat, Profiler, quick_quality_check, quick_quality_check_source};
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

fn write(dir: &TempDir, name: &str, contents: &str) -> PathBuf {
    let path = dir.path().join(name);
    std::fs::write(&path, contents).expect("write fixture");
    path
}

fn write_parquet(dir: &TempDir, name: &str, ids: Vec<i64>, names: Vec<&str>) -> PathBuf {
    let path = dir.path().join(name);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowType::Int64, true),
        Field::new("name", ArrowType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("batch");
    let file = std::fs::File::create(&path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
    path
}

fn file_source(path: &Path, format: FileFormat) -> DataSource {
    DataSource::File {
        path: path.display().to_string(),
        format,
        size_bytes: std::fs::metadata(path).expect("metadata").len(),
        modified_at: None,
        parquet_metadata: None,
    }
}

/// Both one-liners on one file, next to what the full profiler reports.
fn scores(path: &Path, format: FileFormat) -> (Option<f64>, Option<f64>, Option<f64>) {
    let profiled = Profiler::new()
        .analyze_file(path)
        .expect("analyze_file")
        .quality_score();
    let quick = quick_quality_check(path).expect("quick_quality_check");
    let quick_source =
        quick_quality_check_source(&file_source(path, format)).expect("quick_quality_check_source");
    (profiled, quick, quick_source)
}

#[test]
fn a_file_with_nothing_to_assess_has_no_quick_score() {
    let dir = TempDir::new().unwrap();
    let cases = [
        (
            "header-only csv",
            write(&dir, "header_only.csv", "id,name\n"),
            FileFormat::Csv,
        ),
        (
            "zero-row parquet",
            write_parquet(&dir, "empty.parquet", vec![], vec![]),
            FileFormat::Parquet,
        ),
    ];

    let mut wrong = Vec::new();
    for (label, path, format) in cases {
        let (profiled, quick, quick_source) = scores(&path, format);
        // Guard the premise: the full profiler has nothing to score here.
        if profiled.is_some() {
            wrong.push(format!("{label}: analyze_file scored {profiled:?}"));
        }
        if quick.is_some() {
            wrong.push(format!("{label}: quick_quality_check = {quick:?}"));
        }
        if quick_source.is_some() {
            wrong.push(format!(
                "{label}: quick_quality_check_source = {quick_source:?}"
            ));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn a_file_with_data_gets_the_profiler_score() {
    let dir = TempDir::new().unwrap();
    let cases = [
        (
            "csv",
            write(&dir, "data.csv", "id,name\n1,alpha\n2,\n3,gamma\n"),
            FileFormat::Csv,
        ),
        (
            "parquet",
            write_parquet(
                &dir,
                "data.parquet",
                vec![1, 2, 3],
                vec!["alpha", "", "gamma"],
            ),
            FileFormat::Parquet,
        ),
    ];

    let mut wrong = Vec::new();
    for (label, path, format) in cases {
        let (profiled, quick, quick_source) = scores(&path, format);
        if profiled.is_none() {
            wrong.push(format!("{label}: analyze_file has no score"));
        }
        if quick != profiled {
            wrong.push(format!(
                "{label}: quick_quality_check = {quick:?}, analyze_file = {profiled:?}"
            ));
        }
        if quick_source != profiled {
            wrong.push(format!(
                "{label}: quick_quality_check_source = {quick_source:?}, analyze_file = {profiled:?}"
            ));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}
