//! Mixed date formats are counted over every value of a column (#873).
//!
//! `format_violations` used to read only the first 50 non-empty values of each
//! column while `values_checked` counted all of them, so a minority format
//! after the first 50 values went unreported. The count lives in the shared
//! metrics crate, so every engine and input format has to agree.
//!
//! `python/tests/test_mixed_date_formats.py` is the Python twin.

use std::io::Write;

use dataprof::{CsvParserConfig, EngineType, ProfileReport, Profiler, analyze_csv_file};
use tempfile::NamedTempFile;

const NAME: &str = "order_date";

fn iso(index: usize) -> String {
    format!("2024-03-{:02}", 1 + index % 28)
}

fn slash(index: usize) -> String {
    format!("{:02}/03/2024", 1 + index % 28)
}

/// 300 values, each holding 50 in the minority form.
fn fixtures() -> [(&'static str, Vec<String>); 2] {
    [
        (
            "50 ISO, then 250 slash",
            (0..300)
                .map(|index| if index < 50 { iso(index) } else { slash(index) })
                .collect(),
        ),
        (
            "one slash in six",
            (0..300)
                .map(|index| {
                    if index % 6 == 0 {
                        slash(index)
                    } else {
                        iso(index)
                    }
                })
                .collect(),
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
fn a_minority_date_format_is_counted_over_every_value() {
    let mut wrong = Vec::new();
    for (fixture, values) in fixtures() {
        for (label, report) in reports(&values) {
            let consistency = report
                .quality
                .as_ref()
                .and_then(|quality| quality.metrics.consistency.as_ref())
                .unwrap_or_else(|| panic!("[{label}] consistency assessed"));
            if (consistency.format_violations, consistency.values_checked) != (50, 300) {
                wrong.push(format!(
                    "[{label}] {fixture}: format_violations {} of {} values",
                    consistency.format_violations, consistency.values_checked
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
