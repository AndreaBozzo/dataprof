use crate::types::{ColumnProfile, DataType};

use crate::analysis::inference::infer_type;
use crate::analysis::patterns::detect_patterns;
use crate::stats::{calculate_numeric_stats, calculate_text_stats};

pub fn analyze_column(name: &str, data: &[String]) -> ColumnProfile {
    analyze_column_with_options(name, data, false)
}

pub fn analyze_column_fast(name: &str, data: &[String]) -> ColumnProfile {
    analyze_column_with_options(name, data, true)
}

fn analyze_column_with_options(name: &str, data: &[String], fast_mode: bool) -> ColumnProfile {
    let total_count = data.len();
    let null_count = data.iter().filter(|s| s.is_empty()).count();

    // Infer type
    let data_type = infer_type(data);

    // Calculate stats
    let stats = match data_type {
        DataType::Integer | DataType::Float => calculate_numeric_stats(data),
        DataType::String | DataType::Date => calculate_text_stats(data),
    };

    // Skip expensive operations in fast mode
    let patterns = if fast_mode {
        Vec::new()
    } else {
        detect_patterns(data)
    };

    let unique_count = if fast_mode {
        None // Skip expensive unique count in fast mode
    } else {
        Some(data.iter().collect::<std::collections::HashSet<_>>().len())
    };

    ColumnProfile {
        name: name.to_string(),
        data_type,
        null_count,
        total_count,
        unique_count,
        stats,
        patterns,
    }
}
