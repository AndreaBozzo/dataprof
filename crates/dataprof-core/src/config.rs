use serde::{Deserialize, Serialize};

/// ISO 8000/25012 compliant quality thresholds.
/// These thresholds are configurable for industry-specific requirements.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IsoQualityConfig {
    /// Maximum acceptable null percentage for a column (default: 50%).
    pub max_null_percentage: f64,
    /// Threshold for reporting null value issues (default: 10%).
    pub null_report_threshold: f64,
    /// Minimum acceptable type consistency percentage (default: 95%).
    pub min_type_consistency: f64,
    /// Threshold for reporting duplicate rows (default: 5%).
    pub duplicate_report_threshold: f64,
    /// High cardinality warning threshold (uniqueness ratio, default: 95%).
    pub high_cardinality_threshold: f64,
    /// IQR multiplier for outlier detection (default: 1.5, ISO standard).
    pub outlier_iqr_multiplier: f64,
    /// Minimum samples required for outlier detection (default: 4).
    pub outlier_min_samples: usize,
    /// Maximum age in years for data to be considered fresh (default: 5 years).
    pub max_data_age_years: f64,
    /// Percentage threshold for reporting stale data (default: 20%).
    pub stale_data_threshold: f64,
}

impl Default for IsoQualityConfig {
    fn default() -> Self {
        Self {
            max_null_percentage: 50.0,
            null_report_threshold: 10.0,
            min_type_consistency: 95.0,
            duplicate_report_threshold: 5.0,
            high_cardinality_threshold: 95.0,
            outlier_iqr_multiplier: 1.5,
            outlier_min_samples: 4,
            max_data_age_years: 5.0,
            stale_data_threshold: 20.0,
        }
    }
}

impl IsoQualityConfig {
    /// Create strict thresholds for high-compliance industries (finance, healthcare).
    pub fn strict() -> Self {
        Self {
            max_null_percentage: 30.0,
            null_report_threshold: 5.0,
            min_type_consistency: 98.0,
            duplicate_report_threshold: 1.0,
            high_cardinality_threshold: 98.0,
            outlier_iqr_multiplier: 1.5,
            outlier_min_samples: 10,
            max_data_age_years: 2.0,
            stale_data_threshold: 10.0,
        }
    }

    /// Create lenient thresholds for exploratory/marketing data.
    pub fn lenient() -> Self {
        Self {
            max_null_percentage: 70.0,
            null_report_threshold: 20.0,
            min_type_consistency: 90.0,
            duplicate_report_threshold: 10.0,
            high_cardinality_threshold: 90.0,
            outlier_iqr_multiplier: 2.0,
            outlier_min_samples: 4,
            max_data_age_years: 10.0,
            stale_data_threshold: 30.0,
        }
    }
}
