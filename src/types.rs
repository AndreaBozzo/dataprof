use anyhow::Result;
use std::collections::HashMap;

/// Comprehensive data quality metrics following industry standards
/// Provides structured assessment across five key dimensions (ISO 8000/25012)
#[derive(Debug, Clone, serde::Serialize)]
pub struct DataQualityMetrics {
    // Completeness (ISO 8000-8)
    /// Percentage of missing values across all cells
    pub missing_values_ratio: f64,
    /// Percentage of rows with no null values
    pub complete_records_ratio: f64,
    /// Columns with more than 50% null values
    pub null_columns: Vec<String>,

    // Consistency (ISO 8000-61)
    /// Percentage of values conforming to expected data type
    pub data_type_consistency: f64,
    /// Number of format violations (e.g., malformed dates)
    pub format_violations: usize,
    /// Number of UTF-8 encoding issues detected
    pub encoding_issues: usize,

    // Uniqueness (ISO 8000-110)
    /// Number of exact duplicate rows
    pub duplicate_rows: usize,
    /// Percentage of unique values in key columns (if applicable)
    pub key_uniqueness: f64,
    /// Warning flag for columns with excessive unique values
    pub high_cardinality_warning: bool,

    // Accuracy (ISO 25012)
    /// Percentage of statistically anomalous values (outliers)
    pub outlier_ratio: f64,
    /// Number of values outside expected ranges
    pub range_violations: usize,
    /// Number of negative values in positive-only fields (e.g., age)
    pub negative_values_in_positive: usize,

    // Timeliness (ISO 8000-8) - NEW
    /// Number of future dates detected (dates beyond current date)
    pub future_dates_count: usize,
    /// Percentage of dates older than staleness threshold (e.g., >5 years)
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
#[derive(Debug, Clone, serde::Serialize)]
pub struct QualityReport {
    pub file_info: FileInfo,
    pub column_profiles: Vec<ColumnProfile>,
    pub scan_info: ScanInfo,
    /// Data quality metrics following ISO 8000/25012 standards
    /// This is the single source of truth for data quality assessment
    pub data_quality_metrics: DataQualityMetrics,
}

impl QualityReport {
    /// Calculate overall quality score using ISO 8000/25012 metrics
    pub fn quality_score(&self) -> f64 {
        self.data_quality_metrics.overall_score()
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct FileInfo {
    pub path: String,
    pub total_rows: Option<usize>,
    pub total_columns: usize,
    pub file_size_mb: f64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ScanInfo {
    pub rows_scanned: usize,
    pub sampling_ratio: f64,
    pub scan_time_ms: u128,
}

// MVP: CSV profiling with pattern detection
#[derive(Debug, Clone, serde::Serialize)]
pub struct ColumnProfile {
    pub name: String,
    pub data_type: DataType,
    pub null_count: usize,
    pub total_count: usize,
    pub unique_count: Option<usize>,
    pub stats: ColumnStats,
    pub patterns: Vec<Pattern>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub enum DataType {
    String,
    Integer,
    Float,
    Date,
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum ColumnStats {
    Numeric {
        min: f64,
        max: f64,
        mean: f64,
    },
    Text {
        min_length: usize,
        max_length: usize,
        avg_length: f64,
    },
}

#[derive(Debug, Clone, serde::Serialize)]
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
