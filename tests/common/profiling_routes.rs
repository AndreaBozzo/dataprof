use dataprof::StopCondition;
use dataprof::{CsvParserConfig, EngineType, ProfileReport, Profiler, analyze_csv_file};
use std::io::Write;
use tempfile::NamedTempFile;
const NAME: &str = "event";
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
#[allow(dead_code)]
pub fn reports(values: &[String]) -> Vec<(String, ProfileReport)> {
    reports_with_stop(values, StopCondition::Never)
}
pub fn reports_with_stop(values: &[String], stop: StopCondition) -> Vec<(String, ProfileReport)> {
    let csv = text_fixture(".csv", format!("{NAME}\n{}\n", values.join("\n")));
    let mut reports = vec![(
        "standard".to_string(),
        analyze_csv_file(
            csv.path(),
            &CsvParserConfig::default().max_rows(stop.max_rows().map(|rows| rows as usize)),
        )
        .expect("standard CSV"),
    )];
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = Profiler::new()
            .engine(engine)
            .stop_when(stop.clone())
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
            .stop_when(stop.clone())
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    reports
}
