use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::classification::DataType;
use crate::pattern::Pattern;

/// Profiling statistics for a single column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnProfile {
    pub name: String,
    pub data_type: DataType,
    pub null_count: usize,
    pub total_count: usize,
    pub unique_count: Option<usize>,
    /// Whether `unique_count` is an approximate (HyperLogLog) estimate rather
    /// than an exact distinct count.
    ///
    /// `None` when `unique_count` is `None` (never computed); `Some(false)` for
    /// an exact count; `Some(true)` once the cardinality estimator has spilled
    /// to its HLL sketch (~1% relative error). Consumers running key or
    /// high-cardinality/uniqueness gates must treat `Some(true)` as "do not rely
    /// on this as an exact integer" -- an exact-looking count with no provenance
    /// is unsafe for those checks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unique_count_is_approximate: Option<bool>,
    /// Non-null values that did not parse for the column's inferred type and
    /// were therefore excluded from `stats`: non-finite or malformed numbers
    /// on numeric columns, and invalid calendar values on date columns.
    ///
    /// For numeric and date columns this makes the statistics denominator auditable:
    /// `mean`/`std_dev` cover `total_count - null_count - invalid_count`
    /// values. `None` means the check did not run (non-numeric column, or
    /// statistics skipped) — never "no invalid values", which is `Some(0)`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invalid_count: Option<usize>,
    pub stats: ColumnStats,
    /// Detected patterns, or `None` when pattern detection did not run.
    ///
    /// `None` and `Some(vec![])` are not interchangeable: the former means the
    /// column was never scanned, the latter that it was scanned and nothing
    /// matched. Consumers that gate on sensitivity -- redaction, agent-facing
    /// output -- must treat `None` as "unknown", never as "no sensitive data".
    pub patterns: Option<Vec<Pattern>>,
}

/// Quartile statistics for numeric distributions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Quartiles {
    pub q1: f64,
    pub q2: f64,
    pub q3: f64,
    pub iqr: f64,
}

/// A value and its frequency count within a column.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FrequencyItem {
    pub value: String,
    pub count: usize,
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub percentage: f64,
}

/// Statistics for numeric (integer or float) columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumericStats {
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub min: f64,
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub max: f64,
    #[serde(serialize_with = "crate::serde_helpers::round_4")]
    pub mean: f64,
    #[serde(serialize_with = "crate::serde_helpers::round_4")]
    pub std_dev: f64,
    #[serde(serialize_with = "crate::serde_helpers::round_4")]
    pub variance: f64,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::serde_helpers::round_2_opt"
    )]
    pub median: Option<f64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::serde_helpers::quartiles::serialize"
    )]
    pub quartiles: Option<Quartiles>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::serde_helpers::round_2_opt"
    )]
    pub mode: Option<f64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::serde_helpers::round_2_opt"
    )]
    pub coefficient_of_variation: Option<f64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::serde_helpers::round_4_opt"
    )]
    pub skewness: Option<f64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::serde_helpers::round_4_opt"
    )]
    pub kurtosis: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_approximate: Option<bool>,
    /// Number of values flagged as IQR-based outliers in this column.
    ///
    /// Uses the same Tukey-style detection (Q1 − k·IQR, Q3 + k·IQR with
    /// k = 1.5 by default) that feeds the global `accuracy.outlier_ratio`.
    /// `None` when outlier detection didn't run (sample below the configured
    /// minimum or non-numeric column).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outlier_count: Option<usize>,
}

impl NumericStats {
    pub fn empty() -> Self {
        Self {
            min: 0.0,
            max: 0.0,
            mean: 0.0,
            std_dev: 0.0,
            variance: 0.0,
            median: None,
            quartiles: None,
            mode: None,
            coefficient_of_variation: None,
            skewness: None,
            kurtosis: None,
            is_approximate: None,
            outlier_count: None,
        }
    }
}

/// Statistics for text/string columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextStats {
    pub min_length: usize,
    pub max_length: usize,
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub avg_length: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub most_frequent: Option<Vec<FrequencyItem>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub least_frequent: Option<Vec<FrequencyItem>>,
}

impl TextStats {
    pub fn empty() -> Self {
        Self {
            min_length: 0,
            max_length: 0,
            avg_length: 0.0,
            most_frequent: None,
            least_frequent: None,
        }
    }

    pub fn from_lengths(min_length: usize, max_length: usize, avg_length: f64) -> Self {
        Self {
            min_length: if min_length == usize::MAX {
                0
            } else {
                min_length
            },
            max_length,
            avg_length,
            most_frequent: None,
            least_frequent: None,
        }
    }
}

/// Statistics for date/datetime columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DateTimeStats {
    pub min_datetime: String,
    pub max_datetime: String,
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub duration_days: f64,
    pub year_distribution: HashMap<i32, usize>,
    pub month_distribution: HashMap<u32, usize>,
    pub day_of_week_distribution: HashMap<String, usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hour_distribution: Option<HashMap<u32, usize>>,
}

impl DateTimeStats {
    pub fn empty() -> Self {
        Self {
            min_datetime: String::new(),
            max_datetime: String::new(),
            duration_days: 0.0,
            year_distribution: HashMap::new(),
            month_distribution: HashMap::new(),
            day_of_week_distribution: HashMap::new(),
            hour_distribution: None,
        }
    }
}

/// Statistics for boolean columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BooleanStats {
    pub true_count: usize,
    pub false_count: usize,
    #[serde(serialize_with = "crate::serde_helpers::round_4")]
    pub true_ratio: f64,
}

/// Type-specific statistics for a column, determined by the inferred data type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnStats {
    Numeric(NumericStats),
    Text(TextStats),
    DateTime(DateTimeStats),
    Boolean(BooleanStats),
    None,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_profile_json_roundtrip() {
        let profile = ColumnProfile {
            name: "test_col".to_string(),
            data_type: DataType::Integer,
            null_count: 2,
            total_count: 10,
            unique_count: Some(8),
            unique_count_is_approximate: Some(false),
            invalid_count: Some(0),
            stats: ColumnStats::Numeric(NumericStats {
                min: 1.0,
                max: 100.0,
                mean: 50.5,
                std_dev: 28.87,
                variance: 833.25,
                median: Some(50.0),
                quartiles: Some(Quartiles {
                    q1: 25.0,
                    q2: 50.0,
                    q3: 75.0,
                    iqr: 50.0,
                }),
                mode: Some(42.0),
                coefficient_of_variation: Some(57.17),
                skewness: Some(0.0),
                kurtosis: Some(-1.2),
                is_approximate: Some(false),
                outlier_count: Some(0),
            }),
            patterns: Some(vec![]),
        };

        let json = serde_json::to_string(&profile).unwrap();
        let deserialized: ColumnProfile = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, "test_col");
        assert_eq!(deserialized.data_type, DataType::Integer);
        assert_eq!(deserialized.total_count, 10);
        assert_eq!(deserialized.null_count, 2);
        assert_eq!(deserialized.unique_count_is_approximate, Some(false));

        if let ColumnStats::Numeric(n) = &deserialized.stats {
            assert!((n.min - 1.0).abs() < 0.01);
            assert!((n.max - 100.0).abs() < 0.01);
            assert!((n.mean - 50.5).abs() < 0.01);
            assert!(n.median.is_some());
            assert!(n.quartiles.is_some());
        } else {
            panic!("Expected Numeric stats after roundtrip");
        }
    }

    #[test]
    fn test_text_stats_json_roundtrip() {
        let profile = ColumnProfile {
            name: "name".to_string(),
            data_type: DataType::String,
            null_count: 0,
            total_count: 3,
            unique_count: Some(3),
            unique_count_is_approximate: Some(false),
            invalid_count: None,
            stats: ColumnStats::Text(TextStats {
                min_length: 3,
                max_length: 7,
                avg_length: 5.0,
                most_frequent: None,
                least_frequent: None,
            }),
            patterns: Some(vec![]),
        };

        let json = serde_json::to_string(&profile).unwrap();
        let deserialized: ColumnProfile = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.data_type, DataType::String);
        if let ColumnStats::Text(t) = &deserialized.stats {
            assert_eq!(t.min_length, 3);
            assert_eq!(t.max_length, 7);
        } else {
            panic!("Expected Text stats after roundtrip");
        }
    }
}
