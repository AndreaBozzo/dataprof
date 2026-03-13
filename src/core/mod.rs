pub mod config;
pub mod errors;
pub mod memory_tracker;
pub mod profile_builder;
pub mod progress;
pub mod report_assembler;
pub mod sampling;
pub mod stop_condition;
pub mod streaming_stats;
pub mod validation;

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
pub use crate::types::Pattern;
