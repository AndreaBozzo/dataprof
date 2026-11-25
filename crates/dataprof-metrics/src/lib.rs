pub mod acceleration;
pub mod analysis;
pub mod core;
pub mod quality;
pub mod serde_helpers;
pub mod stats;
pub mod types;

pub use analysis::{
    LexicalTally, MetricsCalculator, PairOrderCounts, PatternMetadata, TemporalPairTracker,
    TimelinessTallies, analyze_column, analyze_column_fast, analyze_column_with_analysis_options,
    analyze_declared_column, classify_lexical_forms, compute_value_hint_bindings,
    compute_value_hint_bindings_from_tallies, detect_patterns, infer_type,
    is_accuracy_ruled_column, is_null_like_token, lexical_class, list_patterns, value_matches_hint,
};
pub use quality::{
    AccuracyMetrics, ColumnWithoutValueChecks, CompletenessMetrics, ConsistencyMetrics,
    MetricConfidence, PrecisionMetrics, QualityAssessment, QualityMetrics, QualityScores,
    RowCompletenessSummary, RowDuplicateSummary, ScoreBounds, ScoreInterval, TimelinessClock,
    TimelinessClockMode, TimelinessClockTimezone, TimelinessMetrics, UniquenessMetrics,
    ValidityMetrics, ValueChecksSkipReason, columns_without_value_checks,
};
pub use stats::{
    CardinalityEstimator, DateTimeAccumulator, DualDate, EXACT_CARDINALITY_THRESHOLD, ExactDecimal,
    HyperLogLog, NumericAccumulator, ParsedNumber, TimelinessWindow, calculate_datetime_stats,
    calculate_numeric_stats, calculate_text_stats, parse_date_value, parse_dual_date,
    spill_largest_exact_sets,
};
