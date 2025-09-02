use chrono::NaiveDateTime;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct QualityReport {
    pub file_info: FileInfo,
    pub column_profiles: Vec<ColumnProfile>,
    pub issues: Vec<QualityIssue>,
    pub scan_info: ScanInfo,
}

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: String,
    pub total_rows: Option<usize>, // None se non conosciuto
    pub total_columns: usize,
    pub file_size_mb: f64,
}

#[derive(Debug, Clone)]
pub struct ScanInfo {
    pub rows_scanned: usize,
    pub sampling_ratio: f64,
    pub scan_time_ms: u128,
}

#[derive(Debug, Clone)]
pub struct ColumnProfile {
    pub name: String,
    pub data_type: DataType,
    pub null_count: usize,
    pub unique_count: Option<usize>,
    pub patterns: Vec<Pattern>,
    pub stats: ColumnStats,
}

#[derive(Debug, Clone)]
pub enum DataType {
    String,
    Integer,
    Float,
    Date,
    DateTime,
    Boolean,
    #[allow(dead_code)]
    Mixed(Vec<DataType>),
}

#[derive(Debug, Clone)]
pub enum ColumnStats {
    Numeric {
        min: f64,
        max: f64,
        mean: f64,
        #[allow(dead_code)]
        std: f64,
    },
    Text {
        min_length: usize,
        max_length: usize,
        avg_length: f64,
    },
    #[allow(dead_code)]
    Temporal {
        min: NaiveDateTime,
        max: NaiveDateTime,
    },
}

#[derive(Debug, Clone)]
pub struct Pattern {
    pub name: String,
    #[allow(dead_code)]
    pub regex: String,
    #[allow(dead_code)]
    pub match_count: usize,
    pub match_percentage: f64,
}

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
    #[allow(dead_code)]
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
