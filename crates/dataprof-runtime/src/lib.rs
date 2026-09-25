#[cfg(feature = "async-streaming")]
mod async_source;

pub mod findings;
pub mod hint_binding;
pub mod memory_config;
pub mod profile_builder;
pub mod profile_report;
pub mod quality_gate;
pub mod report_assembler;
pub mod streaming_stats;

#[cfg(feature = "async-streaming")]
pub use async_source::ReqwestSource;
#[cfg(feature = "async-streaming")]
pub use async_source::{AsyncDataSource, AsyncSourceInfo, BytesSource};
pub use findings::{
    DEFAULT_MIXED_TYPES_PERCENTAGE, DEFAULT_NULL_HEAVY_PERCENTAGE, EvidenceValue, Finding,
    FindingCode, FindingPolicy, FindingPolicyError, FindingsResult, NotEvaluatedReason, Severity,
    UnevaluatedRule,
};
pub use hint_binding::ValueHintBindingAccumulator;
pub use memory_config::MemoryConfig;
pub use profile_builder::{
    ColumnProfileInput, ExactNumericAggregates, TextLengths, build_column_profile,
    infer_data_type_streaming, mark_container_columns, nested_column_profile, profile_from_stats,
    profile_from_stats_with_hints, profiles_from_streaming, profiles_from_streaming_with_hints,
    quality_check_samples,
};
#[doc(hidden)]
pub use profile_report::profile_report_schema_document;
pub use profile_report::{
    MetricSemantics, ProfileReport, QualityAnalysisStatus, REPORT_SCHEMA_VERSION,
};
pub use quality_gate::{
    Check, CheckCode, CheckStatus, Evidence, EvidenceGap, Expectation, GateResult, MetricValue,
    NotEvaluated, PolicyError, PolicyScope, QualityPolicy, RequiredMetric, Verdict,
};
pub use report_assembler::ReportAssembler;
pub use streaming_stats::{
    RowCompletenessTracker, RowSignature, RowUniquenessTracker, StreamReservoirSampler,
    StreamingColumnCollection, StreamingStatistics, TextLengthStats,
};
