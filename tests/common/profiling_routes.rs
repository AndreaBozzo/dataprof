use dataprof::StopCondition;
use dataprof::{CsvParserConfig, EngineType, Locale, ProfileReport, Profiler, analyze_csv_file};
use std::io::Write;
use tempfile::NamedTempFile;
const NAME: &str = "event";
fn text_fixture(suffix: &str, bytes: &[u8]) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    file.write_all(bytes).unwrap();
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
    reports_with(values, StopCondition::Never, None)
}
#[allow(dead_code)]
pub fn reports_with_stop(values: &[String], stop: StopCondition) -> Vec<(String, ProfileReport)> {
    reports_with(values, stop, None)
}
#[allow(dead_code)]
pub fn reports_with_locale(
    values: &[String],
    locale: Option<Locale>,
) -> Vec<(String, ProfileReport)> {
    reports_with(values, StopCondition::Never, locale)
}

/// Values are written with CSV quoting and JSON escaping, so a value holding a
/// comma or a quote stays one cell on every route.
fn reports_with(
    values: &[String],
    stop: StopCondition,
    locale: Option<Locale>,
) -> Vec<(String, ProfileReport)> {
    let mut writer = csv::WriterBuilder::new()
        .terminator(csv::Terminator::Any(b'\n'))
        .from_writer(Vec::new());
    writer.write_record([NAME]).unwrap();
    for value in values {
        writer.write_record([value]).unwrap();
    }
    let csv = text_fixture(".csv", &writer.into_inner().unwrap());
    let profiler = || {
        let profiler = Profiler::new().stop_when(stop.clone());
        match locale {
            Some(locale) => profiler.locale(locale),
            None => profiler,
        }
    };
    let mut reports = Vec::new();
    // The standalone CSV API has no locale option; it runs in every case
    // without one.
    if locale.is_none() {
        reports.push((
            "standard".to_string(),
            analyze_csv_file(
                csv.path(),
                &CsvParserConfig::default().max_rows(stop.max_rows().map(|rows| rows as usize)),
            )
            .expect("standard CSV"),
        ));
    }
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

    let records: Vec<String> = values
        .iter()
        .map(|value| serde_json::json!({ NAME: value }).to_string())
        .collect();
    let mut files = vec![
        (
            "json",
            text_fixture(".json", format!("[{}]", records.join(",")).as_bytes()),
        ),
        (
            "jsonl",
            text_fixture(".jsonl", (records.join("\n") + "\n").as_bytes()),
        ),
    ];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_fixture(values)));
    for (label, file) in files {
        let report = profiler()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    reports
}
