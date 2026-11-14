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
    /// A distinct count the column is certain to reach when `unique_count` is
    /// an estimate: the distinct values the exact set held when it was
    /// dropped. `None` when no floor was recorded, which includes every exact
    /// count (the count is its own floor) and every report read back from a
    /// document.
    ///
    /// Not serialized: it exists so quality bounds computed during the run can
    /// bound a key column's uniqueness with certainty (#819). A saved report
    /// keeps the bounds themselves.
    #[serde(skip)]
    #[schemars(skip)]
    pub unique_count_lower_bound: Option<usize>,
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
    /// Of the values `type_homogeneity` counted as `text`, how many are
    /// numbers written with a decimal comma or with digit-group separators:
    /// `10,50`, `1.234,56`, `1,234.56`, `1'234.56`, `1 234,56` (#433).
    ///
    /// dataprof does not parse these as numbers, so they are in no numeric
    /// statistic: a column of them is typed `string`, and a numeric column
    /// counts them in `invalid_count`. The count does not say which
    /// convention they follow; `1,234` is a thousand in one and a fraction in
    /// another.
    ///
    /// Counted over the same values as `type_homogeneity`, and present
    /// whenever it is. `None` means the count did not run, including a report
    /// written before dataprof recorded it; `Some(0)` means none were found.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locale_number_count: Option<usize>,
    /// The type the source declared for the column, recorded as provenance
    /// (#906). `None` when the source declares none, which is every text
    /// input (CSV, JSON, JSONL), Python objects and SQLite, and on a report
    /// written before dataprof recorded it. See [`NativeType`] for the
    /// sources that declare one.
    ///
    /// `data_type` is what dataprof profiled the column as, and several
    /// declared types share one: an Arrow `decimal128(12, 2)` profiles as
    /// `Float` and a `timestamp[us, tz=UTC]` as `Date`. This keeps the
    /// declaration itself, so a reader can tell that a column kept its type.
    ///
    /// Provenance, not a metric: two input paths over the same logical data
    /// declare different types, or none, so it stays outside the
    /// serialized-value parity comparison.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_type: Option<NativeType>,
    pub stats: ColumnStats,
    /// Detected patterns, or `None` when pattern detection did not run.
    ///
    /// `None` and `Some(vec![])` are not interchangeable: the former means the
    /// column was never scanned, the latter that it was scanned and nothing
    /// matched. Consumers that gate on sensitivity -- redaction, agent-facing
    /// output -- must treat `None` as "unknown", never as "no sensitive data".
    pub patterns: Option<Vec<Pattern>>,
}

/// A column type as the source declared it (#906).
///
/// `system` names the type system the declaration belongs to, and `name`
/// spells the type the way that system does:
///
/// - `arrow`: Arrow tables, record batches and streams, polars frames, ADBC
///   results and Parquet files. The name is the one pyarrow prints for the
///   type, such as `decimal128(12, 2)`, `timestamp[us, tz=UTC]` or
///   `dictionary<values=string, indices=int32, ordered=0>`. An extension type
///   is named as the extension (`extension<arrow.uuid>`), not its storage
///   type, and without the parameters pyarrow adds to a parameterized one.
///   A Parquet column
///   carries the Arrow type its reader produced from the file's schema, which
///   keeps the Arrow schema the writer stored with the file.
/// - `pandas`: pandas DataFrames. The name is the column's dtype as pandas
///   prints it (`int64`, `object`, `datetime64[ns, UTC]`, `Int64`). An
///   `object` column declares no type for its values; pyarrow infers one from
///   them, which is why the pandas dtype is recorded rather than the Arrow
///   type the profiler read.
/// - `postgresql` and `mysql`: the database connectors. The name is the
///   server's type name as the driver reports it (`NUMERIC`, `INT4`,
///   `TIMESTAMPTZ`, `DECIMAL`, `BIGINT UNSIGNED`).
///
/// Both are strings so that a report from a later release, which may record
/// a system this build does not know, still loads. Build one with
/// [`NativeType::new`]: the struct is `non_exhaustive`, as the rest of the
/// report model is to become before 1.0 (#1162).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[non_exhaustive]
pub struct NativeType {
    pub system: String,
    pub name: String,
}

impl NativeType {
    /// The type system of Arrow and the readers built on it.
    pub const ARROW: &'static str = "arrow";
    /// pandas dtypes.
    pub const PANDAS: &'static str = "pandas";
    /// PostgreSQL type names.
    pub const POSTGRESQL: &'static str = "postgresql";
    /// MySQL type names.
    pub const MYSQL: &'static str = "mysql";

    pub fn new(system: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            system: system.into(),
            name: name.into(),
        }
    }

    /// Whether this declares a decimal, a type whose values no float wrote,
    /// so they are read as decimals and kept exact (#1268): an Arrow
    /// `decimal32` to `decimal256`, a pandas column of one
    /// (`decimal128(12, 2)[pyarrow]`), PostgreSQL `NUMERIC` and MySQL
    /// `DECIMAL`.
    ///
    /// A pandas `object` column declares no type, though pyarrow may read its
    /// `Decimal` values into a decimal; nor does an Arrow extension type over
    /// decimal storage, which is named as the extension.
    pub fn declares_decimal(&self) -> bool {
        let arrow_decimal = |name: &str| {
            ["decimal32(", "decimal64(", "decimal128(", "decimal256("]
                .iter()
                .any(|prefix| name.starts_with(prefix))
        };
        match self.system.as_str() {
            Self::ARROW => arrow_decimal(&self.name),
            Self::PANDAS => self
                .name
                .strip_suffix("[pyarrow]")
                .is_some_and(arrow_decimal),
            Self::POSTGRESQL => self.name == "NUMERIC",
            Self::MYSQL => self.name == "DECIMAL",
            _ => false,
        }
    }
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

/// The order statistics of an integer or decimal column, exactly, as
/// canonical decimal strings: no exponent, no trailing zeros in a fraction,
/// `-` for a negative value (#1038). An integer past 2^53 or a decimal with
/// more digits than `f64` keeps is rounded in the `f64` fields beside these.
///
/// Present on every integer column and every decimal-typed column, absent on
/// a float column. Each field covers what the `f64` field of the same name
/// covers: `min` and `max` every value, the others the same sample, and each
/// is present exactly when that field is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExactNumericStats {
    pub min: String,
    pub max: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub median: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub q1: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub q2: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub q3: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
}

/// A value and its frequency count within a column.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct FrequencyItem {
    pub value: String,
    pub count: usize,
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub percentage: f64,
}

/// How a report's moment statistics (`mean`, `std_dev`, `variance`,
/// `skewness`, `kurtosis`) are rounded when serialized, as recorded in its
/// metric semantics.
///
/// Only the current rule has a value. Through 0.13 they kept 4 decimal places
/// at any magnitude, and those releases recorded no definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StatisticRounding {
    /// 4 decimal places and at most 12 significant digits, so a value past
    /// 1e8 drops the digits where accumulation order drifts (#1178).
    FourDecimalsTwelveSignificant,
}

/// The estimator behind `skewness` and `kurtosis`, as recorded in a report's
/// metric semantics.
///
/// Only the current estimator has a value. Through 0.12 both divided
/// population moments by the sample standard deviation, a form matching
/// neither the population nor the adjusted estimators, and those releases
/// recorded no definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ShapeMomentEstimator {
    /// Adjusted Fisher-Pearson sample skewness from three values and adjusted
    /// Fisher sample excess kurtosis from four, as pandas computes them.
    AdjustedSample,
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
    #[serde(serialize_with = "crate::serde_helpers::round_statistic")]
    pub mean: f64,
    /// `None` when the spread of the values exceeds the finite `f64` range.
    /// A constant column is `Some(0.0)`, so `None` never means "no spread".
    #[serde(
        serialize_with = "crate::serde_helpers::round_statistic_opt",
        deserialize_with = "crate::serde_helpers::required_nullable"
    )]
    pub std_dev: Option<f64>,
    /// `None` when the spread of the values exceeds the finite `f64` range.
    /// A constant column is `Some(0.0)`, so `None` never means "no spread".
    #[serde(
        serialize_with = "crate::serde_helpers::round_statistic_opt",
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
        serialize_with = "crate::serde_helpers::round_statistic_opt"
    )]
    pub skewness: Option<f64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::serde_helpers::round_statistic_opt"
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
    /// The order statistics exactly, on an integer or decimal column; `None`
    /// on a float column, whose values are not held exactly. See
    /// [`ExactNumericStats`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exact: Option<ExactNumericStats>,
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
            exact: None,
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
    /// The longest stretch between two consecutive distinct days of the
    /// column (#1300), over every value read.
    ///
    /// A daily file that lost a period from the middle of its range keeps its
    /// newest date and most of its rows, and shows here: the gap grows from 1
    /// to the length of the hole. `None` when no value parsed as a date, the
    /// column's dates span more than 10,000 distinct years (the profiler
    /// stops tracking days past that), or the report predates the field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub largest_gap: Option<DateGap>,
}

/// How many distinct years a date column may span before the profiler stops
/// tracking its distinct days, and reports no [`DateTimeStats::largest_gap`].
/// The days are kept as one bitmap per year, so this bounds that memory at
/// about 0.5 MB per column.
pub const DATE_GAP_YEAR_LIMIT: usize = 10_000;

/// Two consecutive distinct days of a date column, and the days between
/// them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct DateGap {
    /// Days from `from` to `to`: `1` for consecutive days, `0` for a column
    /// holding a single distinct day.
    pub days: i64,
    /// The last day before the gap, `YYYY-MM-DD`.
    pub from: String,
    /// The first day after the gap, `YYYY-MM-DD`.
    pub to: String,
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
            largest_gap: None,
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

    /// Every declaration that names a decimal, and the neighbours that do
    /// not: a float, an array of decimals, an extension over one, a pandas
    /// `object` column, and a system this build does not know (#1268).
    #[test]
    fn declares_decimal_names_each_systems_decimal() {
        let cases = [
            (NativeType::ARROW, "decimal32(9, 2)", true),
            (NativeType::ARROW, "decimal64(18, 2)", true),
            (NativeType::ARROW, "decimal128(12, 2)", true),
            (NativeType::ARROW, "decimal256(40, -2)", true),
            (NativeType::PANDAS, "decimal128(12, 2)[pyarrow]", true),
            (NativeType::POSTGRESQL, "NUMERIC", true),
            (NativeType::MYSQL, "DECIMAL", true),
            (NativeType::ARROW, "double", false),
            (NativeType::ARROW, "list<item: decimal128(12, 2)>", false),
            (NativeType::ARROW, "extension<example.money>", false),
            (NativeType::PANDAS, "object", false),
            (NativeType::PANDAS, "decimal128(12, 2)", false),
            (NativeType::PANDAS, "Float64", false),
            (NativeType::POSTGRESQL, "NUMERIC[]", false),
            (NativeType::POSTGRESQL, "FLOAT8", false),
            (NativeType::MYSQL, "DOUBLE", false),
            ("duckdb", "DECIMAL", false),
        ];
        let wrong: Vec<String> = cases
            .iter()
            .filter(|(system, name, want)| {
                NativeType::new(*system, *name).declares_decimal() != *want
            })
            .map(|(system, name, want)| format!("{system} {name}: expected {want}"))
            .collect();
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }

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
            unique_count_lower_bound: None,
            invalid_count: Some(0),
            type_homogeneity: None,
            locale_number_count: None,
            native_type: None,
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
                exact: None,
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
            unique_count_lower_bound: None,
            invalid_count: None,
            type_homogeneity: None,
            locale_number_count: None,
            native_type: None,
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
