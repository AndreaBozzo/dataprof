use std::time::Duration;

use dataprof::{
    ChunkSize, CsvParserConfig, DataSource, DataType, EngineType, FileFormat, JsonFormat,
    JsonParserConfig, LexicalClass, Locale, MetricPack, OutputFormat, Profiler, ProfilerConfig,
    ProgressSink, QualityDimension, SamplingStrategy, StopCondition, TypeHomogeneity,
    analyze_column, analyze_column_fast, analyze_structure, calculate_numeric_stats,
    calculate_text_stats, classify_lexical_forms, detect_patterns, infer_type, lexical_class,
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
        .locale(Locale::It)
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
fn recovery_field_types_are_available_through_the_facade() {
    use dataprof::{ExecutionMetadata, RecoveryEvent, RecoveryKind};

    let mut execution = ExecutionMetadata::new(1, 1, 0);
    execution.recovery_events = Some(vec![RecoveryEvent {
        kind: RecoveryKind::EngineFallback,
        attempted: "columnar".into(),
        retry: "incremental".into(),
        error: "primary failed".into(),
    }]);
    let event = &execution.recovery_events.as_ref().unwrap()[0];
    let kind = match event.kind {
        RecoveryKind::EngineFallback => "engine_fallback",
        RecoveryKind::CsvAutoRecovery => "csv_auto_recovery",
    };
    assert_eq!(kind, "engine_fallback");
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

    // A `ColumnProfile` field a consumer cannot name is a field they cannot
    // match on or construct, so the classification types travel with it.
    assert_eq!(lexical_class("42"), LexicalClass::Numeric);
    let homogeneity: TypeHomogeneity = classify_lexical_forms(&numeric_values);
    assert_eq!(homogeneity.dominant(), Some((LexicalClass::Numeric, 3)));
    assert_eq!(
        analyze_column("n", &numeric_values).type_homogeneity,
        Some(homogeneity)
    );
    let _numeric_stats = calculate_numeric_stats(&numeric_values);
    let _text_stats = calculate_text_stats(&text_values);
    let _patterns = detect_patterns(&text_values, Some(Locale::Us));

    let csv_config = CsvParserConfig::strict()
        .with_delimiter(b';')
        .has_header(true)
        .max_rows(Some(10));
    assert_eq!(csv_config.delimiter, Some(b';'));

    let jsonl_config = JsonParserConfig::jsonl().with_max_rows(10);
    assert_eq!(jsonl_config.format, Some(JsonFormat::Jsonl));
    assert_eq!(
        JsonParserConfig::json_document().format,
        Some(JsonFormat::Json)
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

/// Parquet enforces a row cap by selecting ranges spread across the file.
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
    assert!(!full.execution.sampling_applied);
    assert!(full.execution.sampled_row_ranges.is_none());

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
            report.execution.sampling_applied,
            "not sampled on {engine:?}"
        );
        assert_eq!(report.execution.sampling_ratio, Some(0.25));
        assert_eq!(
            report.execution.sampled_row_ranges,
            Some(vec![[0, 1], [4, 5], [9, 10], [14, 15], [19, 20]]),
            "wrong selection on {engine:?}",
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
    assert_eq!(report.execution.engine.as_deref(), Some("incremental"));
    assert!(report.execution.truncation_reason.is_none());
    assert!(report.execution.source_exhausted);
}

#[test]
fn explicit_incremental_engine_is_recorded() {
    use std::io::Write;

    let mut tmp = tempfile::Builder::new()
        .suffix(".csv")
        .tempfile()
        .expect("tmpfile");
    writeln!(tmp, "n").unwrap();
    writeln!(tmp, "1").unwrap();
    tmp.flush().unwrap();

    let report = Profiler::new()
        .engine(EngineType::Incremental)
        .analyze_file(tmp.path())
        .expect("incremental engine should profile the csv");

    assert_eq!(report.execution.engine.as_deref(), Some("incremental"));
}

#[cfg(feature = "arrow")]
#[test]
fn explicit_columnar_engine_is_recorded() {
    use std::io::Write;

    let mut tmp = tempfile::Builder::new()
        .suffix(".csv")
        .tempfile()
        .expect("tmpfile");
    writeln!(tmp, "n").unwrap();
    writeln!(tmp, "1").unwrap();
    tmp.flush().unwrap();

    let report = Profiler::new()
        .engine(EngineType::Columnar)
        .analyze_file(tmp.path())
        .expect("columnar engine should profile the csv");

    assert_eq!(report.execution.engine.as_deref(), Some("columnar"));
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

/// The quality-gate types are `#[non_exhaustive]` (#894): callers outside the
/// crate match them with a wildcard arm and destructure them with `..`. This
/// names every variant, and every field of `Check`, `CheckBounds` and
/// `GateResult`, through the facade in code that must compile, so the
/// `compile_fail` guards on `NonExhaustiveGuards` in `quality_gate.rs`, whose
/// error codes stable rustdoc does not check, cannot pass because a name
/// changed.
#[test]
fn quality_gate_types_are_matched_with_a_wildcard() {
    use dataprof::{
        Check, CheckBounds, CheckCode, CheckStatus, Evidence, EvidenceGap, Expectation, GateResult,
        MetricValue, NotEvaluated, PolicyError, PolicyScope, QualityDimension, QualityPolicy,
        RequiredMetric, Verdict,
    };

    fn bounds(bounds: &CheckBounds) -> f64 {
        let CheckBounds {
            lower,
            upper,
            confidence_level,
            ..
        } = bounds;
        lower + upper + confidence_level
    }

    fn check(check: &Check) -> usize {
        let Check {
            code,
            column,
            dimension,
            expected,
            observed,
            scope,
            evidence,
            bounds: interval,
            status,
            message,
            ..
        } = check;
        let _ = (code, column, dimension, expected, observed, scope, evidence);
        let _ = (interval.as_ref().map(bounds), status);
        message.len()
    }

    fn gate(result: &GateResult) -> usize {
        let GateResult {
            verdict,
            scope,
            evidence,
            checks,
            ..
        } = result;
        let _ = (verdict, scope, evidence);
        checks.iter().map(check).sum()
    }

    let policy_error = |e: &PolicyError| match e {
        PolicyError::ThresholdOutOfRange { .. } => 0,
        PolicyError::NoRequirements => 1,
        _ => 2,
    };
    let scope = |s: PolicyScope| match s {
        PolicyScope::FullSource => 0,
        PolicyScope::Observed => 1,
        _ => 2,
    };
    let gap = |g: EvidenceGap| match g {
        EvidenceGap::Truncated => 0,
        EvidenceGap::Sampled => 1,
        EvidenceGap::RecordsSkipped => 2,
        EvidenceGap::QualitySampled => 3,
        EvidenceGap::CoverageUnrecorded => 4,
        _ => 5,
    };
    let evidence = |e: Evidence| match e {
        Evidence::Complete => 0,
        Evidence::Incomplete { reason } => 1 + gap(reason),
        _ => 7,
    };
    let code = |c: CheckCode| match c {
        CheckCode::MinQualityScore => 0,
        CheckCode::MinDimensionScore => 1,
        CheckCode::MaxNullPercentage => 2,
        CheckCode::MaxDuplicateRows => 3,
        CheckCode::RequireMetric => 4,
        _ => 5,
    };
    let value = |v: MetricValue| match v {
        MetricValue::Count(_) => 0,
        MetricValue::Percentage(_) => 1,
        _ => 2,
    };
    let expectation = |e: Expectation| match e {
        Expectation::AtLeast { value: v } => value(v),
        Expectation::AtMost { value: v } => value(v),
        Expectation::Analyzed => 2,
        _ => 3,
    };
    let not_evaluated = |n: &NotEvaluated| match n {
        NotEvaluated::QualityUnavailable { .. } => 0,
        NotEvaluated::NotAssessed => 1,
        NotEvaluated::ColumnNotProfiled => 2,
        NotEvaluated::EvidenceIncomplete { gap: g } => 3 + gap(*g),
        _ => 9,
    };
    let status = |s: &CheckStatus| match s {
        CheckStatus::Passed => 0,
        CheckStatus::Failed => 1,
        CheckStatus::NotEvaluated(reason) => 2 + not_evaluated(reason),
        _ => 12,
    };
    let verdict = |v: Verdict| match v {
        Verdict::Pass => 0,
        Verdict::Fail => 1,
        Verdict::Inconclusive => 2,
        _ => 3,
    };
    let required = |m: RequiredMetric| match m {
        RequiredMetric::Quality => 0,
        RequiredMetric::Dimension(_) => 1,
        _ => 2,
    };

    let mut tmp = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    std::io::Write::write_all(&mut tmp, b"id,name\n1,alpha\n2,\n").unwrap();
    let report = Profiler::new().analyze_file(tmp.path()).unwrap();
    let result = QualityPolicy::new()
        .max_null_percentage("name", 10.0)
        .require_quality()
        .evaluate(&report)
        .unwrap();

    assert_eq!(verdict(result.verdict), 1, "{result:?}");
    assert_eq!(scope(result.scope), 0);
    assert_eq!(evidence(result.evidence), 0);
    assert!(gate(&result) > 0);
    let failed = result.violations().next().expect("a violation");
    assert_eq!(code(failed.code), 2);
    assert_eq!(expectation(failed.expected), 1);
    assert_eq!(status(&failed.status), 1);
    assert_eq!(
        required(RequiredMetric::Dimension(QualityDimension::Completeness)),
        1
    );

    let empty = QualityPolicy::new().evaluate(&report).unwrap_err();
    assert_eq!(policy_error(&empty), 1);
}
