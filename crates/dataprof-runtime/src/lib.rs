#[cfg(feature = "async-streaming")]
mod async_source;

pub mod memory_config;
pub mod profile_builder;
pub mod profile_report;
pub mod report_assembler;
pub mod streaming_stats;

#[cfg(feature = "async-streaming")]
pub use async_source::{AsyncDataSource, AsyncSourceInfo, BytesSource};
#[cfg(feature = "parquet-async")]
pub use async_source::ReqwestSource;
pub use memory_config::MemoryConfig;
pub use profile_builder::{
    ColumnProfileInput, TextLengths, build_column_profile, infer_data_type_streaming,
    profile_from_stats, profiles_from_streaming, quality_check_samples,
};
pub use profile_report::ProfileReport;
pub use report_assembler::ReportAssembler;
pub use streaming_stats::{
    StreamingColumnCollection, StreamingStatistics, TextLengthStats, WelfordAccumulator,
};
