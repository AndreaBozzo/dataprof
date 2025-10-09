pub mod batch;
pub mod benchmark_stats;
pub mod config;
pub mod errors;
pub mod memory_tracker;
pub mod performance;
pub mod sampling;
pub mod streaming_stats;
pub mod validation;

// Re-export core types
pub use batch::*;
pub use benchmark_stats::*;
pub use config::*;
pub use errors::*;
pub use memory_tracker::*;
pub use performance::*;
pub use sampling::*;
pub use streaming_stats::*;
pub use validation::*;

// Re-export pattern detection from analysis module for convenience
pub use crate::analysis::detect_patterns;
pub use crate::types::Pattern;
