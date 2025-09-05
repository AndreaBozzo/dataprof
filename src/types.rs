use anyhow::Result;
use std::collections::HashMap;

// Main report structure
#[derive(Debug, Clone)]
pub struct QualityReport {
    pub file_info: FileInfo,
    pub column_profiles: Vec<ColumnProfile>,
    pub issues: Vec<QualityIssue>,
    pub scan_info: ScanInfo,
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
