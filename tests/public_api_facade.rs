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

/// Regression: the default `EngineType::Auto` silently ignored `stop_when`,
/// scanning the whole file and reporting `source_exhausted` with no truncation
/// reason. Auto must route to an engine that honours the stop condition.
#[test]
fn auto_engine_honours_stop_condition() {
    use std::io::Write;

    let mut tmp = tempfile::Builder::new()
        .suffix(".csv")
        .tempfile()
        .expect("tmpfile");
    writeln!(tmp, "n").unwrap();
    for i in 0..500 {
        writeln!(tmp, "{i}").unwrap();
    }
    tmp.flush().unwrap();

    let report = Profiler::new()
        .engine(EngineType::Auto)
        .stop_when(StopCondition::MaxRows(50))
        .analyze_file(tmp.path())
        .expect("auto engine should profile the csv");

    assert!(
        report.execution.rows_processed < 500,
        "auto engine ignored max_rows: processed {} rows",
        report.execution.rows_processed
    );
    assert!(
        report.execution.truncation_reason.is_some(),
        "early stop must record a truncation reason"
    );
    assert!(
        !report.execution.source_exhausted,
        "a truncated scan must not claim the source was exhausted"
    );
}

/// The columnar engine enforces a row cap, slicing the batch that straddles the
/// limit so the row count is exact rather than rounded to a batch boundary.
///
/// The columnar CSV path is the `ArrowProfiler`, so this needs the `arrow` feature.
#[cfg(feature = "arrow")]
#[test]
fn columnar_engine_honours_max_rows() {
    use std::io::Write;

    let mut csv = tempfile::Builder::new()
        .suffix(".csv")
        .tempfile()
        .expect("tmpfile");
    writeln!(csv, "n").unwrap();
    for i in 0..500 {
        writeln!(csv, "{i}").unwrap();
    }
    csv.flush().unwrap();

    let report = Profiler::new()
        .engine(EngineType::Columnar)
        .stop_when(StopCondition::MaxRows(50))
        .analyze_file(csv.path())
        .expect("columnar should profile the csv");

    assert_eq!(report.execution.rows_processed, 50);
    assert!(report.execution.truncation_reason.is_some());
    assert!(!report.execution.source_exhausted);
}

/// The JSON parser enforces a row cap on every engine that routes to it.
#[test]
fn json_honours_max_rows() {
    use std::io::Write;

    let mut jsonl = tempfile::Builder::new()
        .suffix(".jsonl")
        .tempfile()
        .expect("tmpfile");
    for i in 0..500 {
        writeln!(jsonl, "{{\"a\": {i}}}").unwrap();
    }
    jsonl.flush().unwrap();

    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = Profiler::new()
            .engine(engine)
            .stop_when(StopCondition::MaxRows(50))
            .analyze_file(jsonl.path())
            .unwrap_or_else(|e| panic!("json profile failed for {engine:?}: {e}"));

        assert_eq!(
            report.execution.rows_processed, 50,
            "json ignored max_rows on {engine:?}"
        );
        assert!(
            report.execution.truncation_reason.is_some(),
            "no truncation reason on {engine:?}"
        );
        assert!(
            !report.execution.source_exhausted,
            "exhausted on {engine:?}"
        );
    }
}

/// Parquet enforces a row cap via the reader's `with_limit`.
#[cfg(feature = "parquet")]
#[test]
fn parquet_honours_max_rows() {
    let path = std::path::Path::new("examples/test_data/sensors.parquet");
    assert!(path.exists(), "fixture missing: {}", path.display());

    let full = Profiler::new()
        .analyze_file(path)
        .expect("parquet profile should succeed");
    assert_eq!(full.execution.rows_processed, 20);
    assert!(full.execution.truncation_reason.is_none());

    for engine in [EngineType::Auto, EngineType::Columnar] {
        let report = Profiler::new()
            .engine(engine)
            .stop_when(StopCondition::MaxRows(5))
            .analyze_file(path)
            .unwrap_or_else(|e| panic!("parquet profile failed for {engine:?}: {e}"));

        assert_eq!(
            report.execution.rows_processed, 5,
            "parquet ignored max_rows on {engine:?}"
        );
        assert!(
            report.execution.truncation_reason.is_some(),
            "no truncation reason on {engine:?}"
        );
        assert!(
            !report.execution.source_exhausted,
            "exhausted on {engine:?}"
        );
    }
}

/// A row cap must not fire when the source is smaller than the cap.
#[test]
fn max_rows_above_row_count_is_not_truncation() {
    use std::io::Write;

    let mut jsonl = tempfile::Builder::new()
        .suffix(".jsonl")
        .tempfile()
        .expect("tmpfile");
    writeln!(jsonl, "{{\"a\": 1}}").unwrap();
    writeln!(jsonl, "{{\"a\": 2}}").unwrap();
    jsonl.flush().unwrap();

    let report = Profiler::new()
        .stop_when(StopCondition::MaxRows(1_000))
        .analyze_file(jsonl.path())
        .expect("json profile should succeed");

    assert_eq!(report.execution.rows_processed, 2);
    assert!(report.execution.truncation_reason.is_none());
    assert!(report.execution.source_exhausted);
}

/// Row-capped parsers cannot evaluate richer conditions. Rejecting is correct;
/// silently returning a full scan marked `source_exhausted` is not.
#[test]
fn non_row_limit_stop_condition_is_rejected_not_ignored() {
    use std::io::Write;

    let mut jsonl = tempfile::Builder::new()
        .suffix(".jsonl")
        .tempfile()
        .expect("tmpfile");
    writeln!(jsonl, "{{\"a\": 1}}").unwrap();
    writeln!(jsonl, "{{\"a\": 2}}").unwrap();
    jsonl.flush().unwrap();

    let err = Profiler::new()
        .stop_when(StopCondition::MaxBytes(16))
        .analyze_file(jsonl.path())
        .expect_err("json must reject a byte-cap it cannot honour");
    assert!(
        err.to_string().contains("row-limit"),
        "unexpected error: {err}"
    );
}

/// The rejection must only fire when a stop condition is actually set.
#[test]
fn unsupported_combinations_still_profile_without_stop_condition() {
    use std::io::Write;

    let mut jsonl = tempfile::Builder::new()
        .suffix(".jsonl")
        .tempfile()
        .expect("tmpfile");
    writeln!(jsonl, "{{\"a\": 1}}").unwrap();
    writeln!(jsonl, "{{\"a\": 2}}").unwrap();
    jsonl.flush().unwrap();

    let report = Profiler::new()
        .analyze_file(jsonl.path())
        .expect("json without a stop condition must still profile");
    assert_eq!(report.execution.rows_processed, 2);
}

/// Auto must keep using the adaptive engine when no stop condition is set.
#[test]
fn auto_engine_without_stop_condition_reads_everything() {
    use std::io::Write;

    let mut tmp = tempfile::Builder::new()
        .suffix(".csv")
        .tempfile()
        .expect("tmpfile");
    writeln!(tmp, "n").unwrap();
    for i in 0..100 {
        writeln!(tmp, "{i}").unwrap();
    }
    tmp.flush().unwrap();

    let report = Profiler::new()
        .engine(EngineType::Auto)
        .analyze_file(tmp.path())
        .expect("auto engine should profile the csv");

    assert_eq!(report.execution.rows_processed, 100);
    assert!(report.execution.truncation_reason.is_none());
    assert!(report.execution.source_exhausted);
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
