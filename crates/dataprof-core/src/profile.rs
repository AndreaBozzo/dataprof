use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::classification::{DataType, TypeHomogeneity};
use crate::pattern::Pattern;

/// Profiling statistics for a single column.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
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
    /// Non-null values that failed the column type's raw validity predicate:
    /// non-finite or malformed numbers on numeric columns, and values that do
    /// not parse directly as calendar dates on date columns. The date predicate
    /// intentionally does not trim surrounding whitespace; descriptive date
    /// statistics may normalize whitespace independently.
    ///
    /// For numeric columns, `mean`/`std_dev` cover
    /// `total_count - null_count - invalid_count` values. For date columns,
    /// this count audits the strict raw-value quality predicate. `None` means
    /// the check did not run (another column type, or statistics skipped) —
    /// never "no invalid values", which is `Some(0)`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invalid_count: Option<usize>,
    /// How the column's non-null values distribute across lexical classes.
    ///
    /// `data_type` cannot answer "did this column have a dominant form?": a
    /// column of names and a column that is 60% numbers are both `String`, and
    /// `invalid_count` is absent on string columns by contract. This carries the
    /// evidence, so a consumer can tell a textual column from one that defeated
    /// type inference.
    ///
    /// Counted over the values the profiler retained — the engine's bounded
    /// reservoir sample on a large source, the whole column on a small or
    /// in-memory one. `classified_count()` against `total_count - null_count`
    /// is what says which happened; treat the shares as sampled whenever it is
    /// short.
    ///
    /// `None` means the classification did not run, never "one uniform class":
    /// a column that was classified and had nothing to classify (all-null, or
    /// zero rows) is `Some` with every count zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_homogeneity: Option<TypeHomogeneity>,
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Quartiles {
    pub q1: f64,
    pub q2: f64,
    pub q3: f64,
    /// `q3 - q1`, or `None` when that difference exceeds the finite `f64`
    /// range; the quartiles themselves are still reported. Equal quartiles
    /// give `Some(0.0)`, so `None` never means "no spread".
    #[serde(deserialize_with = "crate::serde_helpers::required_nullable")]
    pub iqr: Option<f64>,
}

/// A value and its frequency count within a column.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct FrequencyItem {
    pub value: String,
    pub count: usize,
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub percentage: f64,
}

/// Statistics for numeric (integer or float) columns.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct NumericStats {
    // min/max/median/mode are data values, not summary percentages: they are
    // rounded at the statistics precision so a column whose values carry more
    // than two decimals is not reported with a min it never contained.
    #[serde(serialize_with = "crate::serde_helpers::round_4")]
    pub min: f64,
    #[serde(serialize_with = "crate::serde_helpers::round_4")]
    pub max: f64,
    #[serde(serialize_with = "crate::serde_helpers::round_4")]
    pub mean: f64,
    /// `None` when the spread of the values exceeds the finite `f64` range.
    /// A constant column is `Some(0.0)`, so `None` never means "no spread".
    #[serde(
        serialize_with = "crate::serde_helpers::round_4_opt",
        deserialize_with = "crate::serde_helpers::required_nullable"
    )]
    pub std_dev: Option<f64>,
    /// `None` when the spread of the values exceeds the finite `f64` range.
    /// A constant column is `Some(0.0)`, so `None` never means "no spread".
    #[serde(
        serialize_with = "crate::serde_helpers::round_4_opt",
        deserialize_with = "crate::serde_helpers::required_nullable"
    )]
    pub variance: Option<f64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::serde_helpers::round_4_opt"
    )]
    pub median: Option<f64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::serde_helpers::quartiles::serialize"
    )]
    pub quartiles: Option<Quartiles>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::serde_helpers::round_4_opt"
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
            std_dev: Some(0.0),
            variance: Some(0.0),
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
///
/// Every length here is a count of Unicode scalar values, not UTF-8 bytes and
/// not grapheme clusters. ASCII text is unaffected by that distinction; a
/// combining sequence counts each scalar, so the decomposed and precomposed
/// spellings of the same word report different lengths. The `text_units` module
/// carries the reasoning.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TextStats {
    /// Shortest value, in Unicode scalar values.
    pub min_length: usize,
    /// Longest value, in Unicode scalar values.
    pub max_length: usize,
    /// Mean length in Unicode scalar values. A mean, so it rounds at the
    /// statistics precision rather than the percentage one.
    #[serde(serialize_with = "crate::serde_helpers::round_4")]
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
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
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
    /// How the column's `NN/NN/YYYY` values were read (#811).
    ///
    /// Decided once per column from its own values, so `01/02/2024` is read
    /// the same way as the `12/31/2024` beside it. `None` when the column
    /// holds no slash dates, or the report predates the field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slash_date_order: Option<SlashDateOrder>,
}

/// The day/month order a column's slash dates were read in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SlashDateOrder {
    /// Some values only parse day-first (`31/12/2024`) and none only
    /// month-first.
    DayFirst,
    /// Some values only parse month-first (`12/31/2024`) and none only
    /// day-first.
    MonthFirst,
    /// Every value parses both ways, so nothing in the column decides it; the
    /// values were read day-first.
    AssumedDayFirst,
    /// Values that only parse day-first and values that only parse
    /// month-first are both present. The column contradicts itself: each value
    /// was read in the order it parses, day-first where both do, and the dates
    /// cannot all be right.
    Mixed,
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
            slash_date_order: None,
        }
    }
}

/// Statistics for boolean columns.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct BooleanStats {
    pub true_count: usize,
    pub false_count: usize,
    #[serde(serialize_with = "crate::serde_helpers::round_4")]
    pub true_ratio: f64,
}

/// Type-specific statistics for a column, determined by the inferred data type.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
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

    /// `std_dev`, `variance` and `quartiles.iqr` are nullable but required: a
    /// `null` is a spread that overflowed `f64`, a missing key a malformed
    /// document. Serde reads a missing `Option` as `None`, which conflates them.
    #[test]
    fn nullable_spread_fields_are_still_required() {
        let stats = NumericStats {
            std_dev: None,
            variance: None,
            quartiles: Some(Quartiles {
                q1: 1.0,
                q2: 2.0,
                q3: 3.0,
                iqr: None,
            }),
            ..NumericStats::empty()
        };
        let document = serde_json::to_value(&stats).unwrap();
        let restored: NumericStats = serde_json::from_value(document.clone()).unwrap();
        assert_eq!(serde_json::to_value(restored).unwrap(), document);

        for (parent, key) in [
            (None, "std_dev"),
            (None, "variance"),
            (Some("quartiles"), "iqr"),
        ] {
            let mut malformed = document.clone();
            let object = match parent {
                Some(parent) => &mut malformed[parent],
                None => &mut malformed,
            };
            object.as_object_mut().unwrap().remove(key).unwrap();
            let error = serde_json::from_value::<NumericStats>(malformed).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains(&format!("missing field `{key}`")),
                "{key}: {error}"
            );
        }
    }

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
            type_homogeneity: None,
            stats: ColumnStats::Numeric(NumericStats {
                min: 1.0,
                max: 100.0,
                mean: 50.5,
                std_dev: Some(28.87),
                variance: Some(833.25),
                median: Some(50.0),
                quartiles: Some(Quartiles {
                    q1: 25.0,
                    q2: 50.0,
                    q3: 75.0,
                    iqr: Some(50.0),
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
            type_homogeneity: None,
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
