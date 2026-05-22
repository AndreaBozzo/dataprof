use crate::classification::PatternCategory;

/// A detected value pattern within a column (e.g. email, phone, UUID).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Pattern {
    /// Pattern name (e.g. "Email", "Phone (US)", "UUID")
    pub name: String,
    /// Regex used for detection
    pub regex: String,
    /// Number of values matching this pattern
    pub match_count: usize,
    /// Percentage of non-null values matching (0.0--100.0)
    pub match_percentage: f64,
    /// Semantic category (e.g. contact, network, financial)
    pub category: PatternCategory,
    /// Detection confidence score (0.0--1.0), derived from pattern specificity and match rate
    pub confidence: f64,
}
