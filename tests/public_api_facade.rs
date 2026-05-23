use std::time::Duration;

use dataprof::{
    ChunkSize, CsvParserConfig, DataSource, DataType, EngineType, FileFormat, JsonFormat,
    JsonParserConfig, MetricPack, OutputFormat, Profiler, ProfilerConfig, ProgressSink,
    QualityDimension, SamplingStrategy, StopCondition, analyze_column_fast,
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

#[test]
fn engine_compatibility_paths_compile() {
    use dataprof::engines::streaming::incremental::IncrementalProfiler;
    use dataprof::engines::streaming::memmap::MemoryMappedCsvReader;

    let _profiler = IncrementalProfiler::new()
        .chunk_size(ChunkSize::Fixed(256))
        .sampling(SamplingStrategy::None)
        .stop_condition(StopCondition::Never);
    let _reader_type_size = std::mem::size_of::<MemoryMappedCsvReader>();
}

#[cfg(feature = "arrow")]
#[test]
fn columnar_engine_compatibility_paths_compile() {
    use dataprof::engines::columnar::arrow_profiler::ArrowProfiler;
    use dataprof::engines::columnar::record_batch_analyzer::RecordBatchAnalyzer;

    let _profiler = ArrowProfiler::new();
    let _analyzer_type_size = std::mem::size_of::<RecordBatchAnalyzer>();
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
    #[allow(unused_imports)]
    use dataprof::{process_rows_to_columns, streaming_profile_loop};

    let _config = DatabaseConfig {
        connection_string: "sqlite::memory:".to_string(),
        load_credentials_from_env: false,
        ..Default::default()
    };

    let _factory: fn(DatabaseConfig) -> Result<Box<dyn DatabaseConnector>, DataProfilerError> =
        create_connector;
}

#[cfg(feature = "async-streaming")]
#[test]
fn async_streaming_facade_reexports_compile() {
    use dataprof::{AsyncSourceInfo, AsyncStreamingProfiler, BytesSource};

    let info = AsyncSourceInfo::new("inline", FileFormat::Csv).size_hint(Some(4));
    let _source = BytesSource::new(bytes::Bytes::from_static(b"a\n1\n"), info);
    let _profiler = AsyncStreamingProfiler::new();
}
