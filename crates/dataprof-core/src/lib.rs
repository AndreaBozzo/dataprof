pub mod classification;
pub mod config;
pub mod errors;
pub mod execution;
pub mod memory_sampler;
pub mod memory_tracker;
pub mod output;
pub mod partial;
pub mod pattern;
pub mod profile;
pub mod progress;
pub mod quality;
pub mod sampling;
pub mod semantic;
pub mod serde_helpers;
pub mod source;
pub mod stop_condition;
pub mod validation;

pub use classification::{DataType, PatternCategory};
#[cfg(feature = "database")]
pub use config::{DatabaseSamplingConfig, DatabaseSettings};
pub use config::{
    DataprofConfig, DataprofConfigBuilder, EngineConfig, IsoQualityConfig, MemoryConfig,
    OutputConfig, QualityConfig, QualityScoreWeights,
};
pub use errors::{DataProfilerError, RecoveryAttempt, RecoveryStrategy, RetryConfig};
pub use execution::{ExecutionMetadata, TruncationReason};
pub use memory_sampler::PeakMemorySampler;
pub use memory_tracker::{MemoryLeak, MemoryTracker};
pub use output::OutputFormat;
pub use partial::{
    ColumnSchema, CountMethod, RowCountEstimate, SchemaResult, StructureColumnSummary,
    StructureReport,
};
pub use pattern::Pattern;
pub use profile::{
    BooleanStats, ColumnProfile, ColumnStats, DateTimeStats, FrequencyItem, NumericStats,
    Quartiles, TextStats,
};
pub use progress::{ProgressEvent, ProgressSink};
pub use quality::{MetricPack, QualityDimension};
pub use sampling::{
    ChunkSize, MultiReservoirSampler, ReservoirSampler, ReservoirStats, SamplingState,
    SamplingStrategy, WeightedReservoirSampler,
};
pub use semantic::{SemanticHintBinding, SemanticHintKind, SemanticHints};
pub use source::{
    DataFrameLibrary, DataSource, FileFormat, ParquetMetadata, QueryEngine, StreamSourceSystem,
};
pub use stop_condition::{
    SchemaStabilityTracker, StopCondition, StopEvaluator, schema_stable_threshold,
};
pub use validation::{InputValidator, ValidationError, exit_codes};
