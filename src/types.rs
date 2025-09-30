use anyhow::Result;
use std::collections::HashMap;

/// Comprehensive data quality metrics following industry standards
/// Provides structured assessment across five key dimensions (ISO 8000/25012)
#[derive(Debug, Clone)]
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
}

// Main report structure
#[derive(Debug, Clone)]
pub struct QualityReport {
    pub file_info: FileInfo,
    pub column_profiles: Vec<ColumnProfile>,
    pub issues: Vec<QualityIssue>,
    pub scan_info: ScanInfo,
    /// Enhanced data quality metrics following industry standards
    pub data_quality_metrics: Option<DataQualityMetrics>,
}

impl QualityReport {
    /// Calculate overall quality score based on issues severity
    pub fn quality_score(&self) -> Result<f64> {
        if self.issues.is_empty() {
            return Ok(100.0);
        }

        let total_columns = self.column_profiles.len() as f64;
        if total_columns == 0.0 {
            return Ok(100.0);
        }

        // Calculate penalty based on issue severity
        let mut total_penalty = 0.0;
        for issue in &self.issues {
            let penalty = match issue {
                QualityIssue::MixedDateFormats { .. } => 20.0, // Critical
                QualityIssue::NullValues { percentage, .. } => {
                    if *percentage > 50.0 {
                        20.0
                    } else if *percentage > 20.0 {
                        15.0
                    } else {
                        10.0
                    }
                }
                QualityIssue::MixedTypes { .. } => 20.0, // Critical
                QualityIssue::Outliers { .. } => 10.0,   // Medium
                QualityIssue::Duplicates { .. } => 5.0,  // Warning
            };
            total_penalty += penalty;
        }

        // Normalize penalty relative to number of columns
        let normalized_penalty = total_penalty / total_columns;

        // Quality score: 100 - penalty, but never below 0
        let score = (100.0 - normalized_penalty).max(0.0);
        Ok(score)
    }
}

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: String,
    pub total_rows: Option<usize>,
    pub total_columns: usize,
    pub file_size_mb: f64,
}

#[derive(Debug, Clone)]
pub struct ScanInfo {
    pub rows_scanned: usize,
    pub sampling_ratio: f64,
    pub scan_time_ms: u128,
}

// MVP: CSV profiling with pattern detection
#[derive(Debug, Clone)]
pub struct ColumnProfile {
    pub name: String,
    pub data_type: DataType,
    pub null_count: usize,
    pub total_count: usize,
    pub unique_count: Option<usize>,
    pub stats: ColumnStats,
    pub patterns: Vec<Pattern>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    String,
    Integer,
    Float,
    Date,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct Pattern {
    pub name: String,
    pub regex: String,
    pub match_count: usize,
    pub match_percentage: f64,
}

// Quality Issues
#[derive(Debug, Clone)]
pub enum QualityIssue {
    MixedDateFormats {
        column: String,
        formats: HashMap<String, usize>,
    },
    NullValues {
        column: String,
        count: usize,
        percentage: f64,
    },
    Duplicates {
        column: String,
        count: usize,
    },
    Outliers {
        column: String,
        values: Vec<String>,
        threshold: f64,
    },
    MixedTypes {
        column: String,
        types: HashMap<String, usize>,
    },
}

impl QualityIssue {
    pub fn severity(&self) -> Severity {
        match self {
            QualityIssue::MixedDateFormats { .. } => Severity::High,
            QualityIssue::NullValues { percentage, .. } => {
                if *percentage > 10.0 {
                    Severity::High
                } else if *percentage > 1.0 {
                    Severity::Medium
                } else {
                    Severity::Low
                }
            }
            QualityIssue::Duplicates { .. } => Severity::Medium,
            QualityIssue::Outliers { .. } => Severity::Low,
            QualityIssue::MixedTypes { .. } => Severity::High,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Severity {
    Low,
    Medium,
    High,
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
