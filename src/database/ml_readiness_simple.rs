//! Simplified ML readiness assessment compatible with existing types

use crate::ColumnProfile;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Simple ML readiness score for compatibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MLReadinessScore {
    pub overall_score: f32,
    pub column_scores: HashMap<String, f32>,
    pub issues: Vec<String>,
    pub recommendations: Vec<String>,
}

impl Default for MLReadinessScore {
    fn default() -> Self {
        Self {
            overall_score: 0.0,
            column_scores: HashMap::new(),
            issues: Vec::new(),
            recommendations: Vec::new(),
        }
    }
}

impl MLReadinessScore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_issue(&mut self, issue: String) {
        self.issues.push(issue);
    }

    pub fn add_recommendation(&mut self, recommendation: String) {
        self.recommendations.push(recommendation);
    }
}

/// Assess ML readiness using existing ColumnProfile structure
pub fn assess_ml_readiness(
    column_profiles: &[ColumnProfile],
    row_count: u64,
) -> Result<MLReadinessScore> {
    let mut ml_score = MLReadinessScore::new();

    // Assess each column
    let mut total_score = 0.0;
    for profile in column_profiles {
        let score = assess_column_ml_score(profile);
        ml_score.column_scores.insert(profile.name.clone(), score);
        total_score += score;
    }

    // Calculate overall score
    if !column_profiles.is_empty() {
        ml_score.overall_score = total_score / column_profiles.len() as f32;
    }

    // Add table-level assessments
    add_simple_table_assessments(&mut ml_score, column_profiles, row_count);

    Ok(ml_score)
}

/// Assess ML readiness for a single column
fn assess_column_ml_score(profile: &ColumnProfile) -> f32 {
    let mut score = 1.0;

    // Penalize missing values
    let missing_ratio = profile.null_count as f32 / profile.total_count as f32;
    score *= 1.0 - (missing_ratio * 0.5);

    // Score by data type
    let type_score = match profile.data_type {
        crate::types::DataType::Integer | crate::types::DataType::Float => 1.0,
        crate::types::DataType::Date => 0.8,
        crate::types::DataType::String => {
            // Score based on uniqueness for string data
            if let Some(unique_count) = profile.unique_count {
                let uniqueness_ratio = unique_count as f32 / profile.total_count as f32;
                if uniqueness_ratio > 0.95 {
                    0.3 // Likely ID column
                } else if unique_count <= 10 {
                    0.9 // Good categorical
                } else if unique_count <= 100 {
                    0.7 // Medium cardinality
                } else {
                    0.4 // High cardinality
                }
            } else {
                0.5 // Unknown uniqueness
            }
        }
    };

    score *= type_score;
    score.clamp(0.0, 1.0)
}

/// Add simple table-level assessments
fn add_simple_table_assessments(
    ml_score: &mut MLReadinessScore,
    column_profiles: &[ColumnProfile],
    row_count: u64,
) {
    // Check row count
    if row_count < 100 {
        ml_score.add_issue("Very small dataset - insufficient for most ML algorithms".to_string());
    } else if row_count < 1000 {
        ml_score.add_issue("Small dataset - limited ML algorithm choices".to_string());
    }

    // Check for high-cardinality columns
    let mut high_cardinality_count = 0;
    let mut id_columns = 0;

    for profile in column_profiles {
        if let Some(unique_count) = profile.unique_count {
            let uniqueness_ratio = unique_count as f32 / profile.total_count as f32;
            if uniqueness_ratio > 0.95 {
                id_columns += 1;
            } else if unique_count > 1000 {
                high_cardinality_count += 1;
            }
        }
    }

    if high_cardinality_count > 5 {
        ml_score.add_issue("Many high-cardinality features detected".to_string());
        ml_score
            .add_recommendation("Consider feature selection or encoding strategies".to_string());
    }

    if id_columns > 2 {
        ml_score.add_recommendation(
            "Multiple ID-like columns detected - consider removing from features".to_string(),
        );
    }

    // General recommendations
    if column_profiles.len() > row_count as usize {
        ml_score.add_issue("More features than samples - curse of dimensionality".to_string());
        ml_score
            .add_recommendation("Use dimensionality reduction or feature selection".to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnStats, DataType};

    #[test]
    fn test_assess_numeric_column() {
        let profile = ColumnProfile {
            name: "price".to_string(),
            data_type: DataType::Float,
            null_count: 10,
            total_count: 1000,
            unique_count: Some(800),
            stats: ColumnStats::Numeric {
                min: 10.0,
                max: 1000.0,
                mean: 250.0,
            },
            patterns: vec![],
        };

        let score = assess_column_ml_score(&profile);
        assert!(score > 0.8); // Should be high quality
    }

    #[test]
    fn test_assess_string_column() {
        let profile = ColumnProfile {
            name: "category".to_string(),
            data_type: DataType::String,
            null_count: 0,
            total_count: 1000,
            unique_count: Some(5),
            stats: ColumnStats::Text {
                min_length: 3,
                max_length: 10,
                avg_length: 6.5,
            },
            patterns: vec![],
        };

        let score = assess_column_ml_score(&profile);
        assert!(score > 0.8); // Low cardinality categorical should score well
    }
}
