use crate::types::ColumnStats;

pub fn calculate_numeric_stats(data: &[String]) -> ColumnStats {
    let numbers: Vec<f64> = data.iter().filter_map(|s| s.parse::<f64>().ok()).collect();

    if numbers.is_empty() {
        return ColumnStats::Numeric {
            min: 0.0,
            max: 0.0,
            mean: 0.0,
        };
    }

    let min = numbers.iter().copied().fold(f64::INFINITY, f64::min);
    let max = numbers.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let mean = numbers.iter().sum::<f64>() / numbers.len() as f64;

    ColumnStats::Numeric { min, max, mean }
}
