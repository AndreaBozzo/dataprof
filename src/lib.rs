// New v0.3.0 modular architecture
pub mod acceleration;
pub mod analysis;
pub mod api;
pub mod core;
pub mod engines;
pub mod parsers;

// Organized modules
pub mod output;
pub mod stats;
pub mod types;
pub mod utils;

// Testing utilities (for benchmarks and tests)
pub mod testing;

// Database connectors (default: postgres, mysql, sqlite)
#[cfg(feature = "database")]
pub mod database;

// Python bindings (optional)
#[cfg(feature = "python")]
pub mod python;

// Apache Arrow integration (optional)
#[cfg(feature = "arrow")]
pub use engines::columnar::ArrowProfiler;

// v0.3.0 public API - main exports
pub use api::{quick_quality_check, stream_profile, DataProfiler};
pub use core::batch::{BatchConfig, BatchProcessor, BatchResult, BatchSummary};
pub use core::errors::{DataProfilerError, ErrorSeverity};
pub use core::robust_csv::CsvDiagnostics;
pub use core::sampling::{ChunkSize, SamplingStrategy};

// CLI-specific exports
pub use core::config::DataprofConfig;
pub use core::exit_codes;
pub use core::validation::{InputValidator, ValidationError};

// ML Analysis exports
pub use analysis::MlReadinessEngine;
pub use engines::streaming::ProgressInfo;
pub use engines::{AdaptiveProfiler, EnginePerformance, ProcessingType};

// Public API exports - Core types and functionality
pub use output::html::generate_html_report;
pub use types::{
    ColumnProfile, ColumnStats, DataQualityMetrics, DataType, FileInfo, OutputFormat, Pattern,
    QualityIssue, QualityReport, ScanInfo, Severity,
};
pub use utils::quality::QualityChecker;
// Note: Legacy Sampler removed - use core::sampling::SamplingStrategy instead

// Parser API - CSV and JSON analysis functions
pub use parsers::csv::{
    analyze_csv, analyze_csv_fast, analyze_csv_robust, analyze_csv_with_sampling,
};
pub use parsers::json::{analyze_json, analyze_json_with_quality};

// Analysis utilities - Column-level and statistical functions
pub use analysis::{
    analyze_column_fast, detect_patterns, infer_type, MetricsCalculator, MlReadinessScore,
};
pub use stats::{calculate_numeric_stats, calculate_text_stats};

// Database connectors re-exports (default: postgres, mysql, sqlite)
#[cfg(feature = "database")]
pub use database::{
    create_connector, profile_database, profile_database_with_ml, DatabaseConfig,
    DatabaseConnector, DatabaseCredentials, DuckDbConnector, MySqlConnector, PostgresConnector,
    RetryConfig, SamplingConfig, SamplingStrategy as DbSamplingStrategy, SqliteConnector,
    SslConfig,
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
