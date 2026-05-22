//! High-performance data profiling with ISO 8000/25012 quality metrics.
//!
//! dataprof analyzes tabular data (CSV, JSON, JSONL, Parquet, databases,
//! DataFrames, Arrow) and produces column-level statistics, pattern detection,
//! and a quality assessment scored against the ISO 8000/25012 standard.
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
//!
//! # Engines
//!
//! - [`EngineType::Auto`] — intelligent selection based on file size and format (default)
//! - [`EngineType::Incremental`] — true streaming with bounded memory
//! - [`EngineType::Columnar`] — Arrow-based batch processing
//!
//! # Feature Flags
//!
//! | Feature | Description |
//! |---|---|
//! | `cli` (default) | CLI binary |
//! | `parquet` (default) | Parquet and Arrow-backed columnar engine |
//! | `async-streaming` | Async profiling engine |
//! | `database` | Database connectivity |
//! | `postgres`, `mysql`, `sqlite` | Database connectors |
//! | `parquet-async` | Profile Parquet over HTTP |
//! | `python` | Python bindings via PyO3 |

pub mod acceleration;
pub mod analysis;
pub mod api;
pub mod core;
pub mod engines;
pub mod parsers;

// Terminal output (CLI-only: colored, indicatif, is-terminal)
#[cfg(feature = "cli")]
pub mod output;
pub mod serde_helpers;
pub mod stats;
pub mod types;

// Database connectors (default: postgres, mysql, sqlite)
#[cfg(feature = "database")]
pub mod database;

// Python bindings (optional)
#[cfg(feature = "python")]
pub mod python;

// Unified public API
pub use api::{
    EngineType, Profiler, ProfilerConfig, quick_quality_check, quick_quality_check_source,
};

// Partial analysis APIs (#226)
pub use api::partial::{infer_schema, quick_row_count};
pub use types::{ColumnSchema, CountMethod, RowCountEstimate, SchemaResult};

#[cfg(feature = "async-streaming")]
pub use api::partial::{
    infer_schema_async, infer_schema_stream, quick_row_count_async, quick_row_count_stream,
};

pub use core::errors::DataProfilerError;
pub use core::sampling::{ChunkSize, SamplingStrategy};
pub use parsers::CsvDiagnostics;

pub use core::config::{DataprofConfig, DataprofConfigBuilder};
pub use core::stop_condition::{StopCondition, StopEvaluator};
pub use core::validation::{InputValidator, ValidationError};

// Progress tracking (structured event-based, #223)
pub use core::progress::{ProgressEvent, ProgressSink};

// Async streaming engine (feature-gated)
#[cfg(feature = "async-streaming")]
pub use engines::streaming::{
    AsyncDataSource, AsyncSourceInfo, AsyncStreamingProfiler, BytesSource,
};

#[cfg(feature = "parquet-async")]
pub use engines::streaming::ReqwestSource;

#[cfg(feature = "datafusion")]
pub use engines::DataFusionLoader;

// Public API exports - Core types and functionality
pub use types::{
    AccuracyMetrics, ColumnProfile, ColumnStats, CompletenessMetrics, ConsistencyMetrics,
    DataFrameLibrary, DataSource, DataType, ExecutionMetadata, FileFormat, MetricConfidence,
    MetricPack, OutputFormat, Pattern, PatternCategory, ProfileReport, QualityAssessment,
    QualityDimension, QualityMetrics, QueryEngine, TimelinessMetrics, TruncationReason,
    UniquenessMetrics,
};

// Parser API - New config-based CSV API (#181 + #218)
pub use parsers::csv::{CsvParserConfig, analyze_csv_file, analyze_csv_from_reader};

// Parser API - New config-based JSON API (#218)
pub use parsers::json::{
    JsonFormat, JsonParserConfig, analyze_json_file, analyze_json_from_reader,
};

#[cfg(feature = "parquet")]
pub use parsers::parquet::{
    ParquetConfig, analyze_parquet_with_config, analyze_parquet_with_quality, is_parquet_file,
};

// Analysis utilities - Column-level and statistical functions
pub use analysis::{MetricsCalculator, analyze_column_fast, detect_patterns, infer_type};
pub use stats::{calculate_numeric_stats, calculate_text_stats};

// Database connectors re-exports (default: postgres, mysql, sqlite)
#[cfg(feature = "database")]
pub use database::{
    DatabaseConfig, DatabaseConnector, DatabaseCredentials, MySqlConnector, PostgresConnector,
    RetryConfig, SamplingConfig, SamplingStrategy as DbSamplingStrategy, SqliteConnector,
    SslConfig, analyze_database, create_connector,
};

/// Global memory leak detection utility
pub fn check_memory_leaks() -> String {
    use crate::core::MemoryTracker;

    let global_tracker = MemoryTracker::default();
    global_tracker.report_leaks()
}

/// Get global memory usage statistics
pub fn get_memory_usage_stats() -> (usize, usize, usize) {
    use crate::core::MemoryTracker;

    let global_tracker = MemoryTracker::default();
    global_tracker.get_memory_stats()
}
