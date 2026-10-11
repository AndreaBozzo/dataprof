use serde::{Deserialize, Serialize};

/// Relative weights used to aggregate assessed quality dimensions.
///
/// Weights do not need to sum to one: the score renormalizes them over the
/// dimensions that were actually assessed. Values should be non-negative and
/// at least one weight must be positive.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct QualityScoreWeights {
    pub completeness: f64,
    pub consistency: f64,
    pub uniqueness: f64,
    pub accuracy: f64,
    pub timeliness: f64,
    pub validity: f64,
    pub precision: f64,
}

impl Default for QualityScoreWeights {
    fn default() -> Self {
        Self {
            completeness: 0.25,
            consistency: 0.20,
            uniqueness: 0.15,
            accuracy: 0.15,
            timeliness: 0.10,
            validity: 0.10,
            precision: 0.05,
        }
    }
}

impl QualityScoreWeights {
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }

    pub fn is_valid(&self) -> bool {
        let weights = [
            self.completeness,
            self.consistency,
            self.uniqueness,
            self.accuracy,
            self.timeliness,
            self.validity,
            self.precision,
        ];
        weights
            .iter()
            .all(|weight| weight.is_finite() && *weight >= 0.0)
            && weights.iter().any(|weight| *weight > 0.0)
    }
}

/// Quality thresholds and score settings informed by ISO 8000/25012 concepts.
/// These values are dataprof configuration, not ISO-mandated parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IsoQualityConfig {
    /// Maximum acceptable null percentage for a column (default: 50%).
    pub max_null_percentage: f64,
    /// High cardinality warning threshold (uniqueness ratio, default: 95%).
    pub high_cardinality_threshold: f64,
    /// IQR multiplier for outlier detection (default: 1.5, ISO standard).
    pub outlier_iqr_multiplier: f64,
    /// Minimum samples required for outlier detection (default: 4).
    pub outlier_min_samples: usize,
    /// Maximum age in years for data to be considered fresh (default: 5 years).
    pub max_data_age_years: f64,
    /// Dataprof-defined weights for the overall quality score.
    #[serde(default)]
    pub score_weights: QualityScoreWeights,
}

impl Default for IsoQualityConfig {
    fn default() -> Self {
        Self {
            max_null_percentage: 50.0,
            high_cardinality_threshold: 95.0,
            outlier_iqr_multiplier: 1.5,
            outlier_min_samples: 4,
            max_data_age_years: 5.0,
            score_weights: QualityScoreWeights::default(),
        }
    }
}

impl IsoQualityConfig {
    /// Create strict thresholds for high-compliance industries (finance, healthcare).
    pub fn strict() -> Self {
        Self {
            max_null_percentage: 30.0,
            high_cardinality_threshold: 98.0,
            outlier_iqr_multiplier: 1.5,
            outlier_min_samples: 10,
            max_data_age_years: 2.0,
            score_weights: QualityScoreWeights::default(),
        }
    }

    /// Create lenient thresholds for exploratory/marketing data.
    pub fn lenient() -> Self {
        Self {
            max_null_percentage: 70.0,
            high_cardinality_threshold: 90.0,
            outlier_iqr_multiplier: 2.0,
            outlier_min_samples: 4,
            max_data_age_years: 10.0,
            score_weights: QualityScoreWeights::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_quality_config_defaults_score_weights() {
        let mut value = serde_json::to_value(IsoQualityConfig::default()).unwrap();
        value
            .as_object_mut()
            .expect("quality config object")
            .remove("score_weights");

        let restored: IsoQualityConfig = serde_json::from_value(value).unwrap();
        assert_eq!(restored.score_weights, QualityScoreWeights::default());
    }

    #[test]
    fn five_dimension_weights_gain_new_dimension_defaults() {
        let weights: QualityScoreWeights = serde_json::from_str(
            r#"{
                "completeness": 0.30,
                "consistency": 0.25,
                "uniqueness": 0.20,
                "accuracy": 0.15,
                "timeliness": 0.10
            }"#,
        )
        .unwrap();

        assert_eq!(weights.validity, 0.10);
        assert_eq!(weights.precision, 0.05);
    }

    #[test]
    fn all_zero_score_weights_are_invalid() {
        let weights = QualityScoreWeights {
            completeness: 0.0,
            consistency: 0.0,
            uniqueness: 0.0,
            accuracy: 0.0,
            timeliness: 0.0,
            validity: 0.0,
            precision: 0.0,
        };

        assert!(!weights.is_valid());
    }
}
