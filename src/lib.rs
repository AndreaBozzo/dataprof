// New v0.3.0 modular architecture
pub mod acceleration;
pub mod analysis;
pub mod api;
pub mod core;
pub mod engines;
pub mod parsers;

// Organized modules
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

// v0.6.0 unified public API
pub use api::{
    EngineType, Profiler, ProfilerConfig, quick_quality_check, quick_quality_check_source,
};

#[deprecated(note = "Use the unified Profiler builder instead.")]
pub type DataProfiler = Profiler;

pub use core::batch::{BatchConfig, BatchProcessor, BatchResult, BatchSummary};
pub use core::errors::{DataProfilerError, ErrorSeverity};
pub use core::sampling::{ChunkSize, SamplingStrategy};
pub use parsers::CsvDiagnostics;

// CLI-specific exports
pub use core::config::{DataprofConfig, DataprofConfigBuilder};
pub use core::exit_codes;
pub use core::validation::{InputValidator, ValidationError};

// Progress tracking (needed for progress callbacks)
pub use engines::streaming::ProgressInfo;

// Async streaming engine (feature-gated)
#[cfg(feature = "async-streaming")]
pub use engines::streaming::{
    AsyncDataSource, AsyncSourceInfo, AsyncStreamingProfiler, BytesSource,
};

#[cfg(feature = "datafusion")]
pub use engines::DataFusionLoader;

// Public API exports - Core types and functionality
pub use output::html::generate_html_report;
pub use types::{
    ColumnProfile, ColumnStats, DataFrameLibrary, DataQualityMetrics, DataSource, DataType,
    ExecutionMetadata, FileFormat, OutputFormat, Pattern, QualityReport, QueryEngine,
    TruncationReason,
};

#[deprecated(since = "0.6.0", note = "Use ExecutionMetadata instead.")]
pub type ScanInfo = ExecutionMetadata;
// Note: Legacy Sampler removed - use core::sampling::SamplingStrategy instead

// Parser API - New config-based CSV API (#181 + #218)
pub use parsers::csv::{CsvParserConfig, analyze_csv_file, analyze_csv_from_reader};

#[deprecated(note = "Use analyze_csv_file() with CsvParserConfig::default() instead.")]
pub fn analyze_csv_robust(
    path: impl AsRef<std::path::Path>,
) -> Result<QualityReport, DataProfilerError> {
    analyze_csv_file(path.as_ref(), &CsvParserConfig::default())
}

// Parser API - New config-based JSON API (#218)
pub use parsers::json::{
    JsonFormat, JsonParserConfig, analyze_json_file, analyze_json_from_reader,
};

#[deprecated(note = "Use analyze_json_file() instead.")]
pub fn analyze_json_with_quality(
    path: impl AsRef<std::path::Path>,
) -> Result<QualityReport, DataProfilerError> {
    analyze_json_file(path.as_ref(), &JsonParserConfig::default())
}

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
