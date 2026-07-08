//! High-performance data profiling with ISO 8000/25012 quality metrics.
//!
//! The `dataprof` crate is the user-facing facade for profiling CSV, JSON,
//! JSONL, Parquet, database, DataFrame, and Arrow sources. Implementation
//! details live in the workspace crates under `crates/`; this package keeps the
//! public API compact and oriented around [`Profiler`].
//!
//! # Quick Start
//!
//! ```no_run
//! use dataprof::Profiler;
//!
//! let report = Profiler::new().analyze_file("data.csv")?;
//! println!("Rows: {}", report.execution.rows_processed);
//! println!("Quality: {:?}", report.quality_score());
//! # Ok::<(), dataprof::DataProfilerError>(())
//! ```

mod profiler;

pub use profiler::{
    EngineType, Profiler, ProfilerConfig, quick_quality_check, quick_quality_check_source,
};

pub use dataprof_core::{
    BooleanStats, ChunkSize, ColumnProfile, ColumnSchema, ColumnStats, CountMethod,
    DataFrameLibrary, DataProfilerError, DataSource, DataType, DataprofConfig,
    DataprofConfigBuilder, DateTimeStats, ExecutionMetadata, FileFormat, FrequencyItem,
    InputValidator, MetricPack, NumericStats, OutputFormat, ParquetMetadata, Pattern,
    PatternCategory, ProgressEvent, ProgressSink, QualityDimension, Quartiles, QueryEngine,
    RowCountEstimate, SamplingStrategy, SchemaResult, SemanticHints, StopCondition, StopEvaluator,
    StructureColumnSummary, StructureReport, TextStats, TruncationReason, ValidationError,
};
pub use dataprof_csv::{
    CsvDiagnostics, CsvParserConfig, analyze_csv_file, analyze_csv_from_reader,
};
pub use dataprof_json::{
    JsonFormat, JsonParserConfig, analyze_json_file, analyze_json_from_reader,
};
pub use dataprof_metrics::{
    AccuracyMetrics, CompletenessMetrics, ConsistencyMetrics, MetricConfidence, MetricsCalculator,
    PatternMetadata, QualityAssessment, QualityMetrics, TimelinessMetrics, UniquenessMetrics,
    analyze_column, analyze_column_fast, calculate_datetime_stats, calculate_numeric_stats,
    calculate_text_stats, detect_patterns, infer_type, is_null_like_token, list_patterns,
};
#[cfg(feature = "parquet-async")]
pub use dataprof_parquet::{HttpParquetReader, analyze_parquet_async_http};
#[cfg(feature = "parquet")]
pub use dataprof_parquet::{
    ParquetConfig, analyze_parquet_with_config, analyze_parquet_with_quality,
    analyze_parquet_with_quality_dims, is_parquet_file,
};
pub use dataprof_partial::{analyze_structure, infer_schema, quick_row_count};
pub use dataprof_runtime::ProfileReport;

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
    RetryConfig, SamplingConfig, SamplingStrategy as DbSamplingStrategy, SqliteConnector,
    SslConfig, analyze_database, create_connector,
};
