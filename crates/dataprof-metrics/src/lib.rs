pub mod acceleration;
pub mod analysis;
pub mod core;
pub mod quality;
pub mod serde_helpers;
pub mod stats;
pub mod types;

pub use analysis::{
    MetricsCalculator, PatternMetadata, analyze_column, analyze_column_fast,
    analyze_column_with_analysis_options, classify_lexical_forms, compute_value_hint_bindings,
    detect_patterns, infer_type, is_null_like_token, lexical_class, list_patterns,
    value_matches_hint,
};
pub use quality::{
    AccuracyMetrics, CompletenessMetrics, ConsistencyMetrics, MetricConfidence, PrecisionMetrics,
    QualityAssessment, QualityMetrics, RowCompletenessSummary, RowDuplicateSummary,
    TimelinessMetrics, UniquenessMetrics, ValidityMetrics,
};
pub use stats::{
    CardinalityEstimator, EXACT_CARDINALITY_THRESHOLD, HyperLogLog, calculate_datetime_stats,
    calculate_numeric_stats, calculate_text_stats,
};
