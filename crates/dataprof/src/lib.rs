#![doc = include_str!("../README.md")]
//!
//! ## Arrow batches and Python
//!
//! Rust callers with Arrow `RecordBatch` values use
//! `dataprof_parquet::RecordBatchAnalyzer`. The Python package profiles
//! DataFrames and Arrow objects.

mod profiler;

pub use profiler::{
    EngineType, Profiler, ProfilerConfig, quick_quality_check, quick_quality_check_source,
};

/// The date type [`Profiler::reference_date`] takes, re-exported so a caller
/// needs no `chrono` dependency of its own.
pub use chrono::NaiveDate;
pub use dataprof_core::{
    AccuracyComparison, AnalysisOptions, BooleanStats, ChunkSize, ColumnProfile, ColumnSchema,
    ColumnStats, CountMethod, CsvSourceMetadata, DATE_GAP_YEAR_LIMIT, DataFrameLibrary,
    DataProfilerError, DataSource, DataType, DateForms, DateGap, DateStatistics, DateTimeStats,
    DelimiterDetection, ExactNumericStats, ExecutionMetadata, FileFormat, FrequencyItem,
    IsoQualityConfig, JsonNumberReading, KeyColumnSource, KeyUniquenessUnits, LexicalClass, Locale,
    MetricPack, NULL_MARKERS, NativeType, NullTokenSet, NumericStats, OutlierSpread, OutlierUnits,
    ParquetMetadata, Pattern, PatternCategory, PatternMatchCount, ProgressEvent, ProgressSink,
    QualityDimension, QualityScoreWeights, Quartiles, QueryEngine, RangeRuleCount, RecoveryEvent,
    RecoveryKind, RowCountEstimate, SampledScoreEstimator, SamplingStrategy, SchemaResult,
    SemanticHintBinding, SemanticHintKind, SemanticHints, ShapeMomentEstimator, SlashDateOrder,
    StatisticRounding, StopCondition, StopEvaluator, StructureColumnSummary, StructureReport,
    TextLengthUnit, TextStats, TruncationReason, TypeHomogeneity, ValueFacts,
    validate_unique_column_names,
};
pub use dataprof_csv::{
    CsvParserConfig, analyze_csv_file, analyze_csv_file_with_options, analyze_csv_from_reader,
    analyze_csv_from_reader_with_options,
};
pub use dataprof_json::{
    JsonErrorPolicy, JsonFormat, JsonParserConfig, analyze_json_file,
    analyze_json_file_with_options, analyze_json_from_reader,
    analyze_json_from_reader_with_options,
};
pub use dataprof_metrics::{
    AccuracyMetrics, ColumnWithoutValueChecks, CompletenessMetrics, ConsistencyMetrics,
    MetricConfidence, MetricsCalculator, PatternMetadata, PrecisionMetrics, QualityAssessment,
    QualityMetrics, QualityScores, ScoreBounds, ScoreInterval, TimelinessClock,
    TimelinessClockMode, TimelinessClockTimezone, TimelinessMetrics, UniquenessMetrics,
    ValidityMetrics, ValueChecksSkipReason, analyze_column, analyze_column_fast,
    analyze_column_with_analysis_options, calculate_datetime_stats, calculate_numeric_stats,
    calculate_text_stats, classify_lexical_forms, detect_patterns, infer_type, is_null_like_token,
    lexical_class, list_patterns,
};
#[cfg(feature = "parquet-async")]
pub use dataprof_parquet::{HttpParquetReader, analyze_parquet_async_http};
#[cfg(feature = "parquet")]
pub use dataprof_parquet::{
    ParquetConfig, analyze_parquet_bytes, analyze_parquet_bytes_with_options,
    analyze_parquet_with_config, analyze_parquet_with_options, analyze_parquet_with_quality,
    analyze_parquet_with_quality_dims, is_parquet_file,
};
pub use dataprof_partial::{analyze_structure, infer_schema, quick_row_count};
pub use dataprof_runtime::{
    AnalysisSettings, BaselineComparison, BaselineReading, COLUMN_SAMPLE_CAPACITY, Check,
    CheckBounds, CheckCode, CheckStatus, ColumnSelection, DEFAULT_MIXED_TYPES_PERCENTAGE,
    DEFAULT_NULL_HEAVY_PERCENTAGE, Diagnostic, DiagnosticCode, Evidence, EvidenceGap,
    EvidenceValue, Expectation, Finding, FindingCode, FindingPolicy, FindingPolicyError,
    FindingsInput, FindingsResult, GateResult, MetricSemantics, MetricValue, NotEvaluated,
    NotEvaluatedReason, PatternLocale, PolicyError, PolicyScope, ProfileReport,
    QualityAnalysisStatus, QualityPolicy, REPORT_SCHEMA_VERSION, ReportGenerator, RequiredMetric,
    SettingDifference, Severity, SourceIdentity, TemporalPair, TimelinessClockRule,
    UnevaluatedRule, Verdict,
};
pub use dataprof_runtime::{
    ColumnChange, ConfigurationComparison, DimensionChange, RecordedHints, ReportDelta, ReportPair,
    ReportReference, ReportSide, SchemaChange, ScoreChange, SemanticsComparison, SettingComparison,
    Sides,
};
pub use dataprof_runtime::{CompositeKeySummary, UniqueKeySettings};

#[cfg(feature = "async-streaming")]
pub use dataprof_engines::streaming::ReqwestSource;
#[cfg(feature = "async-streaming")]
pub use dataprof_engines::streaming::{
    AsyncDataSource, AsyncSourceInfo, AsyncStreamingProfiler, BytesSource,
};

#[cfg(feature = "async-streaming")]
pub use dataprof_partial::{
    infer_schema_async, infer_schema_stream, quick_row_count_async, quick_row_count_stream,
};

#[cfg(feature = "database")]
pub use dataprof_db::{
    DatabaseConfig, DatabaseConnector, DatabaseCredentials, MySqlConnector, PostgresConnector,
    SqliteConnector, SslConfig, analyze_database, analyze_database_with_options, create_connector,
};
