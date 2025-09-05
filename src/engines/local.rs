// Local execution engine - backward compatibility with existing code
// Re-exports the current analyze functions

pub use crate::{analyze_csv, analyze_csv_with_sampling, analyze_json, analyze_json_with_quality};
