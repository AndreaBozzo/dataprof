pub mod config;
pub mod errors;
pub mod memory_tracker;
pub mod progress;
pub mod sampling;
pub mod stop_condition;
pub mod validation;

pub mod profile_builder {
    pub use dataprof_runtime::profile_builder::*;
}

pub mod report_assembler {
    pub use dataprof_runtime::report_assembler::*;
}

pub mod streaming_stats {
    pub use dataprof_runtime::streaming_stats::*;
}

// Re-export core types
pub use config::*;
pub use errors::*;
pub use memory_tracker::*;
pub use progress::{ProgressEvent, ProgressSink};
pub use report_assembler::*;
pub use sampling::*;
pub use stop_condition::*;
pub use streaming_stats::*;
pub use validation::*;

// Re-export pattern detection from analysis module for convenience
pub use crate::analysis::detect_patterns;
pub use crate::types::{Pattern, PatternCategory};
