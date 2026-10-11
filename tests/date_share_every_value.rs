//! Every engine types a column `date` from the same share of its values
//! (#915).
//!
//! The streaming inferrer counted date-like values over only the first 100
//! values of a column's sample, while `infer_type` counts all of them. Below
//! the reservoir size the sample keeps file order, so a column that opened
//! with dates was a date on the streaming engines and text everywhere else,
//! and one that opened with words was the reverse.
//!
//! `python/tests/test_date_share_every_value.py` is the Python twin.

use std::io::Write;

use dataprof::{CsvParserConfig, DataType, EngineType, ProfileReport, Profiler, analyze_csv_file};
use tempfile::NamedTempFile;

const NAME: &str = "event";

fn date(index: usize) -> String {
    format!("2024-03-{:02}", 1 + index % 28)
}

fn word(index: usize) -> String {
    format!("word{index}")
}

/// 1,000 values each, with the type every path should agree on.
fn fixtures() -> [(&'static str, Vec<String>, DataType); 2] {
    [
        (
            "150 dates, then 850 words",
            (0..1_000)
                .map(|index| {
                    if index < 150 {
                        date(index)
                    } else {
                        word(index)
                    }
                })
                .collect(),
            DataType::String,
        ),
        (
            "100 words, then 900 dates",
            (0..1_000)
                .map(|index| {
                    if index < 100 {
                        word(index)
                    } else {
                        date(index)
                    }
                })
                .collect(),
            DataType::Date,
        ),
    ]
}

fn text_fixture(suffix: &str, body: String) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    write!(file, "{body}").unwrap();
    file.flush().unwrap();
    file
}

#[cfg(feature = "parquet")]
fn parquet_fixture(values: &[String]) -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![Field::new(NAME, DataType::Utf8, false)]));
    let array: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// Every engine over the CSV, then every other format.
fn reports(values: &[String]) -> Vec<(String, ProfileReport)> {
    let csv = text_fixture(".csv", format!("{NAME}\n{}\n", values.join("\n")));
    let mut reports = vec![(
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

    let records: Vec<String> = values
        .iter()
        .map(|value| format!(r#"{{"{NAME}":"{value}"}}"#))
        .collect();
    let mut files = vec![
        (
            "json",
            text_fixture(".json", format!("[{}]", records.join(","))),
        ),
        ("jsonl", text_fixture(".jsonl", records.join("\n") + "\n")),
    ];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_fixture(values)));
    for (label, file) in files {
        let report = Profiler::new()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    reports
}

/// Collects every wrong (path, fixture) before failing, so a regression names
/// all the paths it reaches rather than the first.
#[test]
fn every_path_types_a_column_from_the_date_share_of_all_its_values() {
    let mut wrong = Vec::new();
    for (fixture, values, expected) in fixtures() {
        for (label, report) in reports(&values) {
            let data_type = &report.column_profiles[0].data_type;
            if *data_type != expected {
                wrong.push(format!(
                    "[{label}] {fixture}: {data_type:?}, expected {expected:?}"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
