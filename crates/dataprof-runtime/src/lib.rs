#[cfg(feature = "async-streaming")]
mod async_source;

pub mod hint_binding;
pub mod memory_config;
pub mod profile_builder;
pub mod profile_report;
pub mod report_assembler;
pub mod streaming_stats;

#[cfg(feature = "async-streaming")]
pub use async_source::ReqwestSource;
#[cfg(feature = "async-streaming")]
pub use async_source::{AsyncDataSource, AsyncSourceInfo, BytesSource};
pub use hint_binding::ValueHintBindingAccumulator;
pub use memory_config::MemoryConfig;
pub use profile_builder::{
    ColumnProfileInput, ExactNumericAggregates, TextLengths, build_column_profile,
    infer_data_type_streaming, profile_from_stats, profile_from_stats_with_hints,
    profiles_from_streaming, profiles_from_streaming_with_hints, quality_check_samples,
};
pub use profile_report::{ProfileReport, REPORT_SCHEMA_VERSION};
pub use report_assembler::ReportAssembler;
pub use streaming_stats::{
    RowUniquenessTracker, StreamReservoirSampler, StreamingColumnCollection, StreamingStatistics,
    TextLengthStats, WelfordAccumulator,
};
