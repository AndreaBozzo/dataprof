pub mod acceleration;
pub mod analysis;
pub mod core;
pub mod quality;
pub mod serde_helpers;
pub mod stats;
pub mod types;

pub use analysis::{
    MetricsCalculator, analyze_column, analyze_column_fast, detect_patterns, infer_type,
};
pub use quality::{
    AccuracyMetrics, CompletenessMetrics, ConsistencyMetrics, MetricConfidence, QualityAssessment,
    QualityMetrics, TimelinessMetrics, UniquenessMetrics,
};
pub use stats::{calculate_datetime_stats, calculate_numeric_stats, calculate_text_stats};
