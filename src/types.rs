use anyhow::Result;
use std::collections::HashMap;

// ============================================================================
// Source-Agnostic Data Source Types
// ============================================================================

/// Supported file formats for data profiling
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileFormat {
    Csv,
    Json,
    Jsonl,
    Parquet,
    #[serde(untagged)]
    Unknown(String),
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Csv => write!(f, "csv"),
            Self::Json => write!(f, "json"),
            Self::Jsonl => write!(f, "jsonl"),
            Self::Parquet => write!(f, "parquet"),
            Self::Unknown(s) => write!(f, "{}", s),
        }
    }
}

/// Supported query engines for SQL-based profiling
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryEngine {
    DuckDb,
    Postgres,
    MySql,
    Sqlite,
    Snowflake,
    BigQuery,
    #[serde(untagged)]
    Custom(String),
}

impl std::fmt::Display for QueryEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuckDb => write!(f, "duckdb"),
            Self::Postgres => write!(f, "postgres"),
            Self::MySql => write!(f, "mysql"),
            Self::Sqlite => write!(f, "sqlite"),
            Self::Snowflake => write!(f, "snowflake"),
            Self::BigQuery => write!(f, "bigquery"),
            Self::Custom(s) => write!(f, "{}", s),
        }
    }
}

/// Source-agnostic data source metadata
///
/// Supports multiple data source types with proper semantics:
/// - Files: CSV, JSON, Parquet with path, size, and format metadata
/// - Queries: SQL queries with engine, statement, and execution metadata
///
/// # JSON Serialization
/// Uses tagged enum format for clean API output:
/// ```json
/// { "type": "file", "path": "/data/users.csv", "format": "csv", ... }
/// { "type": "query", "engine": "duckdb", "statement": "SELECT ...", ... }
/// ```
///
/// # Future Extensions
/// TODO: Implement when needed:
/// - Stream { topic, batch_id, partition } - for Kafka, Kinesis, etc.
/// - DataFrame { name, source_library } - for pandas, polars integration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DataSource {
    /// File-based data source (CSV, JSON, Parquet, etc.)
    File {
        /// Absolute or relative path to the file
        path: String,
        /// Detected or specified file format
        format: FileFormat,
        /// File size in bytes
        size_bytes: u64,
        /// Last modification timestamp (ISO 8601 / RFC 3339)
        #[serde(skip_serializing_if = "Option::is_none")]
        modified_at: Option<String>,
        /// Parquet-specific metadata (only present for Parquet files)
        #[serde(skip_serializing_if = "Option::is_none")]
        parquet_metadata: Option<ParquetMetadata>,
    },
    /// SQL query-based data source
    Query {
        /// Database engine used for the query
        engine: QueryEngine,
        /// SQL statement executed
        statement: String,
        /// Target database name (if applicable)
        #[serde(skip_serializing_if = "Option::is_none")]
        database: Option<String>,
        /// Unique execution identifier for tracing
        #[serde(skip_serializing_if = "Option::is_none")]
        execution_id: Option<String>,
    },
}

impl DataSource {
    /// Get a human-readable identifier for this data source
    ///
    /// Returns:
    /// - For files: the file path
    /// - For queries: "engine: truncated_statement"
    pub fn identifier(&self) -> String {
        match self {
            Self::File { path, .. } => path.clone(),
            Self::Query {
                engine, statement, ..
            } => {
                let truncated = if statement.len() > 50 {
                    format!("{}...", &statement[..47])
                } else {
                    statement.clone()
                };
                format!("{}: {}", engine, truncated)
            }
        }
    }

    /// Get file size in megabytes if this is a file-based source
    pub fn size_mb(&self) -> Option<f64> {
        match self {
            Self::File { size_bytes, .. } => Some(*size_bytes as f64 / 1_048_576.0),
            Self::Query { .. } => None,
        }
    }

    /// Check if this is a file-based source
    pub fn is_file(&self) -> bool {
        matches!(self, Self::File { .. })
    }

    /// Check if this is a query-based source
    pub fn is_query(&self) -> bool {
        matches!(self, Self::Query { .. })
    }

    /// Get the file path if this is a file-based source
    pub fn file_path(&self) -> Option<&str> {
        match self {
            Self::File { path, .. } => Some(path),
            Self::Query { .. } => None,
        }
    }
}

/// Comprehensive data quality metrics following industry standards
/// Provides structured assessment across five key dimensions (ISO 8000/25012)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DataQualityMetrics {
    // Completeness (ISO 8000-8)
    /// Percentage of missing values across all cells
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub missing_values_ratio: f64,
    /// Percentage of rows with no null values
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub complete_records_ratio: f64,
    /// Columns with more than 50% null values
    pub null_columns: Vec<String>,

    // Consistency (ISO 8000-61)
    /// Percentage of values conforming to expected data type
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub data_type_consistency: f64,
    /// Number of format violations (e.g., malformed dates)
    pub format_violations: usize,
    /// Number of UTF-8 encoding issues detected
    pub encoding_issues: usize,

    // Uniqueness (ISO 8000-110)
    /// Number of exact duplicate rows
    pub duplicate_rows: usize,
    /// Percentage of unique values in key columns (if applicable)
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub key_uniqueness: f64,
    /// Warning flag for columns with excessive unique values
    pub high_cardinality_warning: bool,

    // Accuracy (ISO 25012)
    /// Percentage of statistically anomalous values (outliers)
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub outlier_ratio: f64,
    /// Number of values outside expected ranges
    pub range_violations: usize,
    /// Number of negative values in positive-only fields (e.g., age)
    pub negative_values_in_positive: usize,

    // Timeliness (ISO 8000-8) - NEW
    /// Number of future dates detected (dates beyond current date)
    pub future_dates_count: usize,
    /// Percentage of dates older than staleness threshold (e.g., >5 years)
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub stale_data_ratio: f64,
    /// Temporal ordering violations (e.g., end_date < start_date)
    pub temporal_violations: usize,
}

impl DataQualityMetrics {
    /// Create metrics for an empty dataset (perfect quality, no data)
    pub fn empty() -> Self {
        Self {
            // Completeness: No data = no missing values
            missing_values_ratio: 0.0,
            complete_records_ratio: 100.0,
            null_columns: vec![],

            // Consistency: No data = perfect consistency
            data_type_consistency: 100.0,
            format_violations: 0,
            encoding_issues: 0,

            // Uniqueness: No data = perfect uniqueness
            duplicate_rows: 0,
            key_uniqueness: 100.0,
            high_cardinality_warning: false,

            // Accuracy: No data = no outliers
            outlier_ratio: 0.0,
            range_violations: 0,
            negative_values_in_positive: 0,

            // Timeliness: No data = no staleness
            future_dates_count: 0,
            stale_data_ratio: 0.0,
            temporal_violations: 0,
        }
    }

    /// Calculate comprehensive data quality metrics from column data
    ///
    /// Delegates to the specialized MetricsCalculator for proper separation of concerns.
    /// Uses default ISO 8000/25012 thresholds.
    ///
    /// # Arguments
    /// * `data` - HashMap containing column names and their values
    /// * `column_profiles` - Vector of analyzed column profiles
    ///
    /// # Returns
    /// * `Result<DataQualityMetrics>` - Comprehensive quality metrics or error
    ///
    /// # Errors
    /// Returns error if data is malformed or calculation fails
    pub fn calculate_from_data(
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<Self> {
        // Delegate to the specialized metrics calculator module with default ISO thresholds
        // This follows the Single Responsibility Principle
        let calculator = crate::analysis::MetricsCalculator::new();
        calculator.calculate_comprehensive_metrics(data, column_profiles)
    }

    /// Calculate overall quality score (0-100) based on ISO 8000/25012 dimensions
    ///
    /// Weighted formula:
    /// - Completeness: 30% (complete_records_ratio - already percentage 0-100)
    /// - Consistency: 25% (data_type_consistency - already percentage 0-100)
    /// - Uniqueness: 20% (key_uniqueness - already percentage 0-100)
    /// - Accuracy: 15% (100 - outlier_ratio) - outlier_ratio is already percentage 0-100
    /// - Timeliness: 10% (100 - stale_data_ratio) - stale_data_ratio is already percentage 0-100
    ///
    /// NOTE: ALL metrics are percentages (0-100), not ratios (0-1)
    pub fn overall_score(&self) -> f64 {
        let completeness = self.complete_records_ratio * 0.3;
        let consistency = self.data_type_consistency * 0.25;
        let uniqueness = self.key_uniqueness * 0.2;

        // Both outlier_ratio and stale_data_ratio are ALREADY percentages (0-100)
        let accuracy = (100.0 - self.outlier_ratio) * 0.15;
        let timeliness = (100.0 - self.stale_data_ratio) * 0.1;

        completeness + consistency + uniqueness + accuracy + timeliness
    }
}

// Main report structure
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QualityReport {
    /// Unique identifier for this report (UUID v4)
    pub id: String,
    /// Timestamp when the report was generated (ISO 8601 / RFC 3339)
    pub timestamp: String,
    /// Data source metadata (file, query, etc.)
    pub data_source: DataSource,
    /// Column-level profiling results
    pub column_profiles: Vec<ColumnProfile>,
    /// Scan operation metadata
    pub scan_info: ScanInfo,
    /// Data quality metrics following ISO 8000/25012 standards
    /// This is the single source of truth for data quality assessment
    pub data_quality_metrics: DataQualityMetrics,
}

impl QualityReport {
    /// Create a new QualityReport with auto-generated id and timestamp
    pub fn new(
        data_source: DataSource,
        column_profiles: Vec<ColumnProfile>,
        scan_info: ScanInfo,
        data_quality_metrics: DataQualityMetrics,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            data_source,
            column_profiles,
            scan_info,
            data_quality_metrics,
        }
    }

    /// Calculate overall quality score using ISO 8000/25012 metrics
    pub fn quality_score(&self) -> f64 {
        self.data_quality_metrics.overall_score()
    }

    /// Get the data source identifier (for backwards compatibility)
    pub fn source_identifier(&self) -> String {
        self.data_source.identifier()
    }
}

/// Metadata specific to Parquet files
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ParquetMetadata {
    /// Number of row groups in the Parquet file
    pub num_row_groups: usize,
    /// Compression codec used (e.g., "SNAPPY", "GZIP", "ZSTD", "UNCOMPRESSED")
    pub compression: String,
    /// Parquet file version (e.g., "1.0", "2.0")
    pub version: i32,
    /// Arrow schema as string representation
    pub schema_summary: String,
    /// Total compressed size in bytes
    pub compressed_size_bytes: u64,
    /// Estimated uncompressed size if available
    pub uncompressed_size_bytes: Option<u64>,
}

/// DEPRECATED: Use `DataSource::File` instead.
///
/// This struct is kept for backwards compatibility during migration.
/// Will be removed in version 0.6.0.
///
/// # Migration
/// Replace:
/// ```ignore
/// FileInfo { path, total_rows, total_columns, file_size_mb, parquet_metadata }
/// ```
/// With:
/// ```ignore
/// DataSource::File { path, format, size_bytes, modified_at, parquet_metadata }
/// // Move total_rows and total_columns to ScanInfo
/// ```
#[deprecated(
    since = "0.5.0",
    note = "Use DataSource::File instead. FileInfo will be removed in 0.6.0"
)]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileInfo {
    pub path: String,
    pub total_rows: Option<usize>,
    pub total_columns: usize,
    pub file_size_mb: f64,
    /// Parquet-specific metadata (only present for Parquet files)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parquet_metadata: Option<ParquetMetadata>,
}

#[allow(deprecated)]
impl FileInfo {
    /// Convert this FileInfo to a DataSource::File
    ///
    /// Note: `total_rows` and `total_columns` should be moved to `ScanInfo` separately.
    pub fn to_data_source(&self, format: FileFormat) -> DataSource {
        DataSource::File {
            path: self.path.clone(),
            format,
            size_bytes: (self.file_size_mb * 1_048_576.0) as u64,
            modified_at: None,
            parquet_metadata: self.parquet_metadata.clone(),
        }
    }
}

/// Metadata about the scanning/profiling operation
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScanInfo {
    /// Total number of rows in the data source
    pub total_rows: usize,
    /// Total number of columns in the data source
    pub total_columns: usize,
    /// Number of rows actually scanned (may differ from total if sampled)
    pub rows_scanned: usize,
    /// Ratio of rows scanned to total rows (0.0 to 1.0)
    pub sampling_ratio: f64,
    /// Total scan time in milliseconds
    pub scan_time_ms: u128,
    /// Throughput in rows per second (calculated: rows_scanned / (scan_time_ms / 1000))
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throughput_rows_sec: Option<f64>,
    /// Peak memory usage in megabytes (if tracked)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_peak_mb: Option<f64>,
    /// Number of errors encountered during scanning
    pub error_count: usize,
}

impl ScanInfo {
    /// Create a new ScanInfo with throughput calculated automatically
    pub fn new(
        total_rows: usize,
        total_columns: usize,
        rows_scanned: usize,
        sampling_ratio: f64,
        scan_time_ms: u128,
    ) -> Self {
        let throughput_rows_sec = if scan_time_ms > 0 {
            Some(rows_scanned as f64 / (scan_time_ms as f64 / 1000.0))
        } else {
            None
        };

        Self {
            total_rows,
            total_columns,
            rows_scanned,
            sampling_ratio,
            scan_time_ms,
            throughput_rows_sec,
            memory_peak_mb: None,
            error_count: 0,
        }
    }
}

// MVP: CSV profiling with pattern detection
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnProfile {
    pub name: String,
    pub data_type: DataType,
    pub null_count: usize,
    pub total_count: usize,
    pub unique_count: Option<usize>,
    pub stats: ColumnStats,
    pub patterns: Vec<Pattern>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DataType {
    String,
    Integer,
    Float,
    Date,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Quartiles {
    pub q1: f64,  // 25th percentile
    pub q2: f64,  // 50th percentile (median)
    pub q3: f64,  // 75th percentile
    pub iqr: f64, // Interquartile range (Q3 - Q1)
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FrequencyItem {
    pub value: String,
    pub count: usize,
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub percentage: f64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ColumnStats {
    Numeric {
        // Existing (always present)
        #[serde(serialize_with = "crate::serde_helpers::round_2")]
        min: f64,
        #[serde(serialize_with = "crate::serde_helpers::round_2")]
        max: f64,
        #[serde(serialize_with = "crate::serde_helpers::round_4")]
        mean: f64,

        // NEW - Streaming-compatible (always present)
        #[serde(serialize_with = "crate::serde_helpers::round_4")]
        std_dev: f64,
        #[serde(serialize_with = "crate::serde_helpers::round_4")]
        variance: f64,

        // NEW - Require sorted data (Option for large datasets)
        #[serde(
            skip_serializing_if = "Option::is_none",
            serialize_with = "crate::serde_helpers::round_2_opt"
        )]
        median: Option<f64>,
        #[serde(
            skip_serializing_if = "Option::is_none",
            serialize_with = "crate::serde_helpers::quartiles::serialize"
        )]
        quartiles: Option<Quartiles>,
        #[serde(
            skip_serializing_if = "Option::is_none",
            serialize_with = "crate::serde_helpers::round_2_opt"
        )]
        mode: Option<f64>,

        // NEW - Advanced metrics (Option for large datasets)
        #[serde(
            skip_serializing_if = "Option::is_none",
            serialize_with = "crate::serde_helpers::round_2_opt"
        )]
        coefficient_of_variation: Option<f64>,
        #[serde(
            skip_serializing_if = "Option::is_none",
            serialize_with = "crate::serde_helpers::round_4_opt"
        )]
        skewness: Option<f64>,
        #[serde(
            skip_serializing_if = "Option::is_none",
            serialize_with = "crate::serde_helpers::round_4_opt"
        )]
        kurtosis: Option<f64>,

        // NEW - Approximation flag
        #[serde(skip_serializing_if = "Option::is_none")]
        is_approximate: Option<bool>,
    },
    Text {
        // Existing
        min_length: usize,
        max_length: usize,
        #[serde(serialize_with = "crate::serde_helpers::round_2")]
        avg_length: f64,

        // NEW - Frequency analysis
        #[serde(skip_serializing_if = "Option::is_none")]
        most_frequent: Option<Vec<FrequencyItem>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        least_frequent: Option<Vec<FrequencyItem>>,
    },
    DateTime {
        // Basic range
        min_datetime: String, // ISO 8601 format
        max_datetime: String,
        #[serde(serialize_with = "crate::serde_helpers::round_2")]
        duration_days: f64,

        // Temporal distributions
        year_distribution: HashMap<i32, usize>,
        month_distribution: HashMap<u32, usize>,
        day_of_week_distribution: HashMap<String, usize>,

        // Optional: only if times are present
        #[serde(skip_serializing_if = "Option::is_none")]
        hour_distribution: Option<HashMap<u32, usize>>,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Pattern {
    pub name: String,
    pub regex: String,
    pub match_count: usize,
    pub match_percentage: f64,
}

// Output format types for CLI and output formatting
#[derive(Clone, Debug)]
pub enum OutputFormat {
    /// Human-readable text output
    Text,
    /// Machine-readable JSON output
    Json,
    /// CSV format for data processing
    Csv,
    /// Plain text without formatting for scripting
    Plain,
}
