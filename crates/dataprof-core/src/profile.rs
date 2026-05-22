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
    pub stats: ColumnStats,
    pub patterns: Vec<Pattern>,
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
