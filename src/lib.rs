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

// Database connectors (optional)
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
pub use engines::streaming::ProgressInfo;

// Re-exports for backward compatibility
pub use output::html::generate_html_report;
pub use types::{
    ColumnProfile, ColumnStats, DataType, FileInfo, Pattern, QualityIssue, QualityReport, ScanInfo,
};
pub use utils::quality::QualityChecker;
pub use utils::sampler::{SampleInfo, Sampler};

// Re-export moved parsing functions for API compatibility
pub use parsers::csv::{analyze_csv, analyze_csv_robust, analyze_csv_with_sampling};
pub use parsers::json::{analyze_json, analyze_json_with_quality};

// Re-export moved analysis functions for API compatibility
pub use analysis::{detect_patterns, infer_type};
pub use stats::{calculate_numeric_stats, calculate_text_stats};

// Database connectors re-exports (optional)
#[cfg(feature = "database")]
pub use database::{
    create_connector, profile_database, DatabaseConfig, DatabaseConnector, DuckDbConnector,
    MySqlConnector, PostgresConnector, SqliteConnector,
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
