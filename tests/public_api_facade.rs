use std::time::Duration;

use dataprof::{
    ChunkSize, CsvParserConfig, DataSource, DataType, EngineType, FileFormat, JsonFormat,
    JsonParserConfig, MetricPack, OutputFormat, Profiler, ProfilerConfig, ProgressSink,
    QualityDimension, SamplingStrategy, StopCondition, analyze_column_fast, analyze_structure,
    calculate_numeric_stats, calculate_text_stats, detect_patterns, infer_type,
};

#[test]
fn stable_facade_builder_surface_compiles() {
    let _profiler = Profiler::with_config(ProfilerConfig::default())
        .engine(EngineType::Auto)
        .chunk_size(ChunkSize::Fixed(128))
        .sampling(SamplingStrategy::None)
        .memory_limit_mb(32)
        .stop_when(StopCondition::MaxRows(1_000))
        .format(FileFormat::Csv)
        .csv_delimiter(b';')
        .csv_flexible(true)
        .quality_dimensions(vec![QualityDimension::Completeness])
        .metric_packs(vec![MetricPack::Schema, MetricPack::Quality])
        .locale("IT")
        .progress_interval(Duration::from_millis(25))
        .progress_sink(ProgressSink::None);

    let source = DataSource::File {
        path: "sample.csv".to_string(),
        format: FileFormat::Csv,
        size_bytes: 42,
        modified_at: None,
        parquet_metadata: None,
    };

    assert!(source.is_file());
    assert_eq!(FileFormat::Csv.to_string(), "csv");
    assert!(matches!(OutputFormat::Json, OutputFormat::Json));
}

#[test]
fn parser_and_metrics_reexports_compile() {
    let numeric_values = vec!["1".to_string(), "2".to_string(), "3".to_string()];
    let text_values = vec![
        "alice@example.com".to_string(),
        "bob@example.com".to_string(),
    ];

    assert_eq!(infer_type(&numeric_values), DataType::Integer);
    assert_eq!(analyze_column_fast("n", &numeric_values).name, "n");
    let _numeric_stats = calculate_numeric_stats(&numeric_values);
    let _text_stats = calculate_text_stats(&text_values);
    let _patterns = detect_patterns(&text_values, Some("US"));

    let csv_config = CsvParserConfig::strict()
        .with_delimiter(b';')
        .has_header(true)
        .max_rows(Some(10));
    assert_eq!(csv_config.delimiter, Some(b';'));

    let jsonl_config = JsonParserConfig::jsonl().with_max_rows(10);
    assert_eq!(jsonl_config.format, Some(JsonFormat::Jsonl));
    assert_eq!(
        JsonParserConfig::json_array().format,
        Some(JsonFormat::JsonArray)
    );
}

#[cfg(feature = "parquet")]
#[test]
fn parquet_facade_reexports_compile() {
    use dataprof::ParquetConfig;

    let config = ParquetConfig::batch_size(1_024);
    assert_eq!(config.batch_size, 1_024);
    assert_eq!(ParquetConfig::adaptive_batch_size(0), 1_024);
}

#[cfg(feature = "database")]
#[test]
fn database_facade_reexports_compile() {
    use dataprof::{DataProfilerError, DatabaseConfig, DatabaseConnector, create_connector};

    let _config = DatabaseConfig {
        connection_string: "sqlite::memory:".to_string(),
        load_credentials_from_env: false,
        ..Default::default()
    };

    let _factory: fn(DatabaseConfig) -> Result<Box<dyn DatabaseConnector>, DataProfilerError> =
        create_connector;
}

/// Regression: `Profiler::format()` must override extension-based detection,
/// even on the auto engine path that previously short-circuited to Parquet
/// based on the `.parquet` file extension.
#[test]
fn format_override_beats_extension() {
    use std::io::Write;

    let mut tmp = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .expect("tmpfile");
    writeln!(tmp, "city,population").unwrap();
    writeln!(tmp, "Rome,2873").unwrap();
    writeln!(tmp, "Milan,1352").unwrap();
    tmp.flush().unwrap();

    let report = Profiler::new()
        .format(FileFormat::Csv)
        .analyze_file(tmp.path())
        .expect("forced CSV parse of .parquet-named file should succeed");

    assert_eq!(report.execution.columns_detected, 2);
}

#[test]
fn analyze_structure_facade_compiles() {
    use std::io::Write;

    let mut tmp = tempfile::Builder::new()
        .suffix(".csv")
        .tempfile()
        .expect("tmpfile");
    writeln!(tmp, "name,age").unwrap();
    writeln!(tmp, "Alice,30").unwrap();
    writeln!(tmp, "Bob,").unwrap();
    tmp.flush().unwrap();

    let report = analyze_structure(tmp.path(), Some(10)).expect("free function");
    assert_eq!(report.columns.len(), 2);
    assert_eq!(report.rows_sampled, 2);

    let via_profiler = Profiler::new()
        .format(FileFormat::Csv)
        .analyze_structure(tmp.path(), Some(1))
        .expect("profiler method");
    assert!(via_profiler.truncated);
    assert_eq!(
        via_profiler.truncation_reason.as_deref(),
        Some("max_rows(1)")
    );
}

#[cfg(feature = "async-streaming")]
#[test]
fn async_streaming_facade_reexports_compile() {
    use dataprof::{AsyncSourceInfo, AsyncStreamingProfiler, BytesSource};
    use dataprof_engines::streaming::{IncrementalProfiler, MemoryMappedCsvReader};

    let info = AsyncSourceInfo::new("inline", FileFormat::Csv).size_hint(Some(4));
    let _source = BytesSource::new(bytes::Bytes::from_static(b"a\n1\n"), info);
    let _profiler = AsyncStreamingProfiler::new();
    let _incremental = IncrementalProfiler::new()
        .chunk_size(ChunkSize::Fixed(256))
        .sampling(SamplingStrategy::None)
        .stop_condition(StopCondition::Never);
    let _reader_type_size = std::mem::size_of::<MemoryMappedCsvReader>();
}
