// MVP: CSV profiling with pattern detection
#[derive(Debug, Clone)]
pub struct ColumnProfile {
    pub name: String,
    pub data_type: DataType,
    pub null_count: usize,
    pub total_count: usize,
    pub stats: ColumnStats,
    pub patterns: Vec<Pattern>,
}

#[derive(Debug, Clone)]
pub enum DataType {
    String,
    Integer,
    Float,
    Date,
}

#[derive(Debug, Clone)]
pub enum ColumnStats {
    Numeric { min: f64, max: f64, mean: f64 },
    Text { min_length: usize, max_length: usize, avg_length: f64 },
}

#[derive(Debug, Clone)]
pub struct Pattern {
    pub name: String,
    pub match_count: usize,
    pub match_percentage: f64,
}
