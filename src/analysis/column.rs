use crate::types::{ColumnProfile, DataType};

use crate::analysis::inference::infer_type;
use crate::analysis::patterns::detect_patterns;
use crate::stats::{calculate_numeric_stats, calculate_text_stats};

pub fn analyze_column(name: &str, data: &[String]) -> ColumnProfile {
    let total_count = data.len();
    let null_count = data.iter().filter(|s| s.is_empty()).count();

    // Infer type
    let data_type = infer_type(data);

    // Calculate stats
    let stats = match data_type {
        DataType::Integer | DataType::Float => calculate_numeric_stats(data),
        DataType::String | DataType::Date => calculate_text_stats(data),
    };

    // Detect patterns
    let patterns = detect_patterns(data);

    ColumnProfile {
        name: name.to_string(),
        data_type,
        null_count,
        total_count,
        unique_count: Some(data.iter().collect::<std::collections::HashSet<_>>().len()),
        stats,
        patterns,
    }
}
