pub mod batch_results;
pub mod display;
pub mod formatters;
pub mod html;
pub mod progress;

pub use batch_results::display_batch_results;
// Legacy display functions - still used by database command, will be refactored
#[allow(deprecated)]
pub use display::{display_data_quality_metrics, display_quality_issues, output_json_profiles};
pub use formatters::{
    create_adaptive_formatter_with_format, output_with_adaptive_formatter, output_with_formatter,
    suggest_output_format, supports_enhanced_output, AdaptiveFormatter, InteractiveFormatter,
    OutputContext,
};
pub use html::*;
pub use progress::*;
