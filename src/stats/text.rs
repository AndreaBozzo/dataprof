use crate::types::ColumnStats;

pub fn calculate_text_stats(data: &[String]) -> ColumnStats {
    let non_empty: Vec<&String> = data.iter().filter(|s| !s.is_empty()).collect();

    if non_empty.is_empty() {
        return ColumnStats::Text {
            min_length: 0,
            max_length: 0,
            avg_length: 0.0,
        };
    }

    let lengths: Vec<usize> = non_empty.iter().map(|s| s.len()).collect();
    let min_length = lengths.iter().min().copied().unwrap_or(0);
    let max_length = lengths.iter().max().copied().unwrap_or(0);
    let avg_length = if lengths.is_empty() {
        0.0
    } else {
        lengths.iter().sum::<usize>() as f64 / lengths.len() as f64
    };

    ColumnStats::Text {
        min_length,
        max_length,
        avg_length,
    }
}
