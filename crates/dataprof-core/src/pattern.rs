use crate::classification::PatternCategory;

/// A detected value pattern within a column (e.g. email, phone, UUID).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct Pattern {
    /// Pattern name (e.g. "Email", "Phone (US)", "UUID")
    pub name: String,
    /// Regex used for detection
    pub regex: String,
    /// Number of analyzed non-null values passing the regex and semantic validator.
    pub match_count: usize,
    /// Percentage of analyzed non-null values passing validation (0.0--100.0).
    pub match_percentage: f64,
    /// Semantic category (e.g. contact, network, financial)
    pub category: PatternCategory,
    /// Detection confidence score (0.0--1.0), derived from pattern specificity and match rate
    pub confidence: f64,
}

/// What a pattern's `match_count` counts, as recorded in a report's metric
/// semantics.
///
/// Only the current definition has a value. Through 0.12 the count was every
/// value the regular expression matched, whether or not the pattern's validator
/// accepted it, and those releases recorded no definition.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum PatternMatchCount {
    /// Values that pass the regular expression and, where the pattern has
    /// one, its semantic validator.
    Validated,
}
