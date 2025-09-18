use crate::types::{ColumnProfile, ColumnStats, DataType, QualityIssue, QualityReport};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// ML Readiness Score engine - calculates how suitable data is for ML workflows
#[derive(Debug, Clone)]
pub struct MlReadinessEngine {
    /// Configuration for ML scoring
    pub config: MlScoringConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MlScoringConfig {
    /// Weight for completeness score (0.0-1.0)
    pub completeness_weight: f64,

    /// Weight for consistency score (0.0-1.0)
    pub consistency_weight: f64,

    /// Weight for data type suitability (0.0-1.0)
    pub type_suitability_weight: f64,

    /// Weight for feature quality (0.0-1.0)
    pub feature_quality_weight: f64,

    /// Minimum threshold for "good" ML readiness
    pub good_threshold: f64,

    /// Minimum threshold for "acceptable" ML readiness
    pub acceptable_threshold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MlReadinessScore {
    /// Overall ML readiness score (0-100)
    pub overall_score: f64,

    /// Category-specific scores
    pub completeness_score: f64,
    pub consistency_score: f64,
    pub type_suitability_score: f64,
    pub feature_quality_score: f64,

    /// ML readiness level
    pub readiness_level: MlReadinessLevel,

    /// Specific recommendations for improving ML readiness
    pub recommendations: Vec<MlRecommendation>,

    /// Blocking issues that must be resolved before ML use
    pub blocking_issues: Vec<MlBlockingIssue>,

    /// Feature analysis
    pub feature_analysis: Vec<FeatureAnalysis>,

    /// Preprocessing suggestions
    pub preprocessing_suggestions: Vec<PreprocessingSuggestion>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MlReadinessLevel {
    /// Ready for ML (score >= 80)
    Ready,
    /// Good for ML with minor preprocessing (score 60-79)
    Good,
    /// Needs significant preprocessing (score 40-59)
    NeedsWork,
    /// Major issues, not suitable for ML (score < 40)
    NotReady,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MlRecommendation {
    pub category: String,
    pub priority: RecommendationPriority,
    pub description: String,
    pub expected_impact: String,
    pub implementation_effort: ImplementationEffort,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecommendationPriority {
    Critical,
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ImplementationEffort {
    Trivial,
    Easy,
    Moderate,
    Significant,
    Complex,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MlBlockingIssue {
    pub issue_type: String,
    pub column: Option<String>,
    pub description: String,
    pub resolution_required: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureAnalysis {
    pub column_name: String,
    pub ml_suitability: f64,
    pub feature_type: MlFeatureType,
    pub encoding_suggestions: Vec<String>,
    pub potential_issues: Vec<String>,
    pub feature_importance_potential: FeatureImportancePotential,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MlFeatureType {
    /// Numeric feature ready for use
    NumericReady,
    /// Numeric feature needs scaling/normalization
    NumericNeedsScaling,
    /// Categorical feature, needs encoding
    CategoricalNeedsEncoding,
    /// Text feature, needs NLP preprocessing
    TextNeedsProcessing,
    /// Date/time feature, needs feature engineering
    TemporalNeedsEngineering,
    /// High cardinality feature, may need dimensionality reduction
    HighCardinalityRisky,
    /// Feature with too many missing values
    TooManyMissing,
    /// Constant or near-constant feature
    LowVariance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FeatureImportancePotential {
    High,
    Medium,
    Low,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreprocessingSuggestion {
    pub step: String,
    pub description: String,
    pub columns_affected: Vec<String>,
    pub priority: RecommendationPriority,
    pub tools_frameworks: Vec<String>,
}

impl Default for MlScoringConfig {
    fn default() -> Self {
        Self {
            completeness_weight: 0.3,
            consistency_weight: 0.25,
            type_suitability_weight: 0.25,
            feature_quality_weight: 0.2,
            good_threshold: 70.0,
            acceptable_threshold: 50.0,
        }
    }
}

impl MlReadinessEngine {
    pub fn new() -> Self {
        Self {
            config: MlScoringConfig::default(),
        }
    }

    pub fn with_config(config: MlScoringConfig) -> Self {
        Self { config }
    }

    /// Calculate comprehensive ML readiness score
    pub fn calculate_ml_score(&self, report: &QualityReport) -> Result<MlReadinessScore> {
        // Calculate component scores
        let completeness_score = self.calculate_completeness_score(&report.column_profiles);
        let consistency_score = self.calculate_consistency_score(&report.issues);
        let type_suitability_score = self.calculate_type_suitability_score(&report.column_profiles);
        let feature_quality_score =
            self.calculate_feature_quality_score(&report.column_profiles, &report.issues);

        // Calculate weighted overall score
        let overall_score = (completeness_score * self.config.completeness_weight
            + consistency_score * self.config.consistency_weight
            + type_suitability_score * self.config.type_suitability_weight
            + feature_quality_score * self.config.feature_quality_weight)
            * 100.0;

        // Determine readiness level
        let readiness_level = self.determine_readiness_level(overall_score);

        // Generate recommendations and analysis
        let recommendations =
            self.generate_recommendations(&report.column_profiles, &report.issues, overall_score);
        let blocking_issues = self.identify_blocking_issues(&report.issues);
        let feature_analysis = self.analyze_features(&report.column_profiles);
        let preprocessing_suggestions =
            self.generate_preprocessing_suggestions(&report.column_profiles, &report.issues);

        Ok(MlReadinessScore {
            overall_score,
            completeness_score: completeness_score * 100.0,
            consistency_score: consistency_score * 100.0,
            type_suitability_score: type_suitability_score * 100.0,
            feature_quality_score: feature_quality_score * 100.0,
            readiness_level,
            recommendations,
            blocking_issues,
            feature_analysis,
            preprocessing_suggestions,
        })
    }

    fn calculate_completeness_score(&self, profiles: &[ColumnProfile]) -> f64 {
        if profiles.is_empty() {
            return 1.0;
        }

        let total_cells: usize = profiles.iter().map(|p| p.total_count).sum();
        let missing_cells: usize = profiles.iter().map(|p| p.null_count).sum();

        if total_cells == 0 {
            1.0
        } else {
            1.0 - (missing_cells as f64 / total_cells as f64)
        }
    }

    fn calculate_consistency_score(&self, issues: &[QualityIssue]) -> f64 {
        let critical_consistency_issues = issues
            .iter()
            .filter(|issue| {
                matches!(
                    issue,
                    QualityIssue::MixedDateFormats { .. } | QualityIssue::MixedTypes { .. }
                )
            })
            .count();

        // Penalize mixed types and date formats heavily
        let penalty = critical_consistency_issues as f64 * 0.2;
        (1.0 - penalty).max(0.0)
    }

    fn calculate_type_suitability_score(&self, profiles: &[ColumnProfile]) -> f64 {
        if profiles.is_empty() {
            return 1.0;
        }

        let mut total_score = 0.0;

        for profile in profiles {
            let type_score = match profile.data_type {
                DataType::Integer | DataType::Float => 1.0, // Perfect for ML
                DataType::String => {
                    // Check if it's categorical (repeated values) or free text
                    let unique_ratio = self.estimate_unique_ratio(profile);
                    if unique_ratio < 0.1 {
                        0.8 // Good categorical feature
                    } else if unique_ratio > 0.9 {
                        0.3 // Likely free text, needs NLP
                    } else {
                        0.6 // Medium cardinality categorical
                    }
                }
                DataType::Date => 0.7, // Needs feature engineering but valuable
            };
            total_score += type_score;
        }

        total_score / profiles.len() as f64
    }

    fn calculate_feature_quality_score(
        &self,
        profiles: &[ColumnProfile],
        issues: &[QualityIssue],
    ) -> f64 {
        if profiles.is_empty() {
            return 1.0;
        }

        let mut total_score = 0.0;

        // Create issue map for quick lookup
        let issue_columns: HashMap<String, Vec<&QualityIssue>> = {
            let mut map: HashMap<String, Vec<&QualityIssue>> = HashMap::new();
            for issue in issues {
                let column = match issue {
                    QualityIssue::NullValues { column, .. }
                    | QualityIssue::MixedDateFormats { column, .. }
                    | QualityIssue::Duplicates { column, .. }
                    | QualityIssue::Outliers { column, .. }
                    | QualityIssue::MixedTypes { column, .. } => column.clone(),
                };
                map.entry(column).or_default().push(issue);
            }
            map
        };

        for profile in profiles {
            let mut feature_score = 1.0;

            // Penalize high null percentages
            let null_percentage = profile.null_count as f64 / profile.total_count as f64;
            if null_percentage > 0.5 {
                feature_score *= 0.3; // Very poor
            } else if null_percentage > 0.2 {
                feature_score *= 0.6; // Poor
            } else if null_percentage > 0.1 {
                feature_score *= 0.8; // Acceptable
            }

            // Check for specific issues
            if let Some(column_issues) = issue_columns.get(&profile.name) {
                for issue in column_issues {
                    match issue {
                        QualityIssue::MixedTypes { .. } => feature_score *= 0.2, // Major problem
                        QualityIssue::MixedDateFormats { .. } => feature_score *= 0.4, // Serious issue
                        QualityIssue::Outliers { .. } => feature_score *= 0.9, // Minor impact
                        _ => {}
                    }
                }
            }

            // Check for variance (constant features are bad for ML)
            if let ColumnStats::Numeric { min, max, .. } = &profile.stats {
                if (max - min).abs() < f64::EPSILON {
                    feature_score *= 0.1; // Constant feature
                }
            }

            total_score += feature_score;
        }

        total_score / profiles.len() as f64
    }

    fn estimate_unique_ratio(&self, profile: &ColumnProfile) -> f64 {
        // Estimate based on patterns if available
        if !profile.patterns.is_empty() {
            let pattern_coverage: f64 = profile
                .patterns
                .iter()
                .map(|p| p.match_percentage / 100.0)
                .sum();

            if pattern_coverage > 0.8 {
                0.05 // High pattern coverage = low uniqueness
            } else {
                0.5 // Medium estimate
            }
        } else {
            0.5 // Default estimate
        }
    }

    fn determine_readiness_level(&self, score: f64) -> MlReadinessLevel {
        if score >= 80.0 {
            MlReadinessLevel::Ready
        } else if score >= self.config.good_threshold {
            MlReadinessLevel::Good
        } else if score >= self.config.acceptable_threshold {
            MlReadinessLevel::NeedsWork
        } else {
            MlReadinessLevel::NotReady
        }
    }

    fn generate_recommendations(
        &self,
        profiles: &[ColumnProfile],
        issues: &[QualityIssue],
        _score: f64,
    ) -> Vec<MlRecommendation> {
        let mut recommendations = Vec::new();

        // High null percentage recommendation
        for profile in profiles {
            let null_percentage = (profile.null_count as f64 / profile.total_count as f64) * 100.0;
            if null_percentage > 20.0 {
                recommendations.push(MlRecommendation {
                    category: "Missing Data".to_string(),
                    priority: if null_percentage > 50.0 {
                        RecommendationPriority::Critical
                    } else {
                        RecommendationPriority::High
                    },
                    description: format!(
                        "Column '{}' has {:.1}% missing values. Consider imputation or removal.",
                        profile.name, null_percentage
                    ),
                    expected_impact: "Improves model training stability and accuracy".to_string(),
                    implementation_effort: ImplementationEffort::Easy,
                });
            }
        }

        // Mixed types recommendation
        for issue in issues {
            if let QualityIssue::MixedTypes { column, .. } = issue {
                recommendations.push(MlRecommendation {
                    category: "Data Types".to_string(),
                    priority: RecommendationPriority::Critical,
                    description: format!(
                        "Column '{}' has mixed data types. Clean and standardize before ML use.",
                        column
                    ),
                    expected_impact: "Prevents model training errors and improves feature quality"
                        .to_string(),
                    implementation_effort: ImplementationEffort::Moderate,
                });
            }
        }

        // String feature encoding recommendations
        for profile in profiles {
            if matches!(profile.data_type, DataType::String) {
                let unique_ratio = self.estimate_unique_ratio(profile);
                if unique_ratio < 0.1 {
                    recommendations.push(MlRecommendation {
                        category: "Feature Encoding".to_string(),
                        priority: RecommendationPriority::High,
                        description: format!(
                            "Column '{}' appears categorical. Consider one-hot or label encoding.",
                            profile.name
                        ),
                        expected_impact:
                            "Enables ML algorithms to process categorical data effectively"
                                .to_string(),
                        implementation_effort: ImplementationEffort::Easy,
                    });
                } else if unique_ratio > 0.8 {
                    recommendations.push(MlRecommendation {
                        category: "Text Processing".to_string(),
                        priority: RecommendationPriority::Medium,
                        description: format!("Column '{}' appears to be free text. Consider NLP preprocessing (tokenization, vectorization).", profile.name),
                        expected_impact: "Converts text to numerical features for ML algorithms".to_string(),
                        implementation_effort: ImplementationEffort::Significant,
                    });
                }
            }
        }

        // Date feature engineering recommendation
        for profile in profiles {
            if matches!(profile.data_type, DataType::Date) {
                recommendations.push(MlRecommendation {
                    category: "Feature Engineering".to_string(),
                    priority: RecommendationPriority::Medium,
                    description: format!("Column '{}' is a date. Extract features like year, month, day, day_of_week for better ML performance.", profile.name),
                    expected_impact: "Creates multiple useful features from temporal data".to_string(),
                    implementation_effort: ImplementationEffort::Easy,
                });
            }
        }

        // Numeric scaling recommendation
        let numeric_profiles: Vec<_> = profiles
            .iter()
            .filter(|p| matches!(p.data_type, DataType::Integer | DataType::Float))
            .collect();

        if numeric_profiles.len() > 1 {
            recommendations.push(MlRecommendation {
                category: "Feature Scaling".to_string(),
                priority: RecommendationPriority::Medium,
                description: "Multiple numeric columns detected. Consider standardization or normalization for algorithms sensitive to scale.".to_string(),
                expected_impact: "Improves convergence and performance of gradient-based algorithms".to_string(),
                implementation_effort: ImplementationEffort::Easy,
            });
        }

        recommendations
    }

    fn identify_blocking_issues(&self, issues: &[QualityIssue]) -> Vec<MlBlockingIssue> {
        let mut blocking = Vec::new();

        for issue in issues {
            match issue {
                QualityIssue::MixedTypes { column, .. } => {
                    blocking.push(MlBlockingIssue {
                        issue_type: "Mixed Data Types".to_string(),
                        column: Some(column.clone()),
                        description: format!(
                            "Column '{}' contains multiple data types in the same field",
                            column
                        ),
                        resolution_required:
                            "Clean data to have consistent types per column before ML training"
                                .to_string(),
                    });
                }
                QualityIssue::MixedDateFormats { column, .. } => {
                    blocking.push(MlBlockingIssue {
                        issue_type: "Inconsistent Date Formats".to_string(),
                        column: Some(column.clone()),
                        description: format!("Column '{}' has multiple date formats", column),
                        resolution_required: "Standardize all dates to a single format".to_string(),
                    });
                }
                _ => {} // Other issues are warnings, not blockers
            }
        }

        blocking
    }

    fn analyze_features(&self, profiles: &[ColumnProfile]) -> Vec<FeatureAnalysis> {
        profiles
            .iter()
            .map(|profile| {
                let (feature_type, ml_suitability, encoding_suggestions, potential_issues) =
                    self.analyze_single_feature(profile);

                let feature_importance_potential = self.estimate_feature_importance(profile);

                FeatureAnalysis {
                    column_name: profile.name.clone(),
                    ml_suitability,
                    feature_type,
                    encoding_suggestions,
                    potential_issues,
                    feature_importance_potential,
                }
            })
            .collect()
    }

    fn analyze_single_feature(
        &self,
        profile: &ColumnProfile,
    ) -> (MlFeatureType, f64, Vec<String>, Vec<String>) {
        let null_percentage = (profile.null_count as f64 / profile.total_count as f64) * 100.0;
        let mut potential_issues = Vec::new();
        let mut encoding_suggestions = Vec::new();

        if null_percentage > 50.0 {
            potential_issues.push("High missing value rate (>50%)".to_string());
            return (
                MlFeatureType::TooManyMissing,
                0.2,
                encoding_suggestions,
                potential_issues,
            );
        }

        let (feature_type, base_suitability) = match profile.data_type {
            DataType::Integer | DataType::Float => {
                encoding_suggestions.push("Consider standardization or normalization".to_string());

                // Check for variance
                if let ColumnStats::Numeric { min, max, .. } = &profile.stats {
                    if (max - min).abs() < f64::EPSILON {
                        potential_issues.push("Constant or near-constant values".to_string());
                        (MlFeatureType::LowVariance, 0.1)
                    } else {
                        (MlFeatureType::NumericReady, 0.9)
                    }
                } else {
                    (MlFeatureType::NumericNeedsScaling, 0.8)
                }
            }
            DataType::String => {
                let unique_ratio = self.estimate_unique_ratio(profile);

                if unique_ratio < 0.05 {
                    encoding_suggestions.push("One-hot encoding (low cardinality)".to_string());
                    (MlFeatureType::CategoricalNeedsEncoding, 0.8)
                } else if unique_ratio < 0.3 {
                    encoding_suggestions.push("Label encoding or target encoding".to_string());
                    (MlFeatureType::CategoricalNeedsEncoding, 0.7)
                } else if unique_ratio > 0.8 {
                    encoding_suggestions.push("TF-IDF or word embeddings".to_string());
                    encoding_suggestions
                        .push("Consider text preprocessing (tokenization, stemming)".to_string());
                    (MlFeatureType::TextNeedsProcessing, 0.4)
                } else {
                    potential_issues.push("High cardinality categorical feature".to_string());
                    encoding_suggestions
                        .push("Consider dimensionality reduction or feature hashing".to_string());
                    (MlFeatureType::HighCardinalityRisky, 0.5)
                }
            }
            DataType::Date => {
                encoding_suggestions.push("Extract year, month, day, day_of_week".to_string());
                encoding_suggestions
                    .push("Calculate time since epoch or reference date".to_string());
                (MlFeatureType::TemporalNeedsEngineering, 0.7)
            }
        };

        // Adjust suitability based on missing values
        let adjusted_suitability = if null_percentage > 20.0 {
            potential_issues.push(format!("High missing value rate ({:.1}%)", null_percentage));
            base_suitability * (1.0 - null_percentage / 200.0).max(0.3)
        } else {
            base_suitability
        };

        (
            feature_type,
            adjusted_suitability,
            encoding_suggestions,
            potential_issues,
        )
    }

    fn estimate_feature_importance(&self, profile: &ColumnProfile) -> FeatureImportancePotential {
        let null_percentage = (profile.null_count as f64 / profile.total_count as f64) * 100.0;

        // High null values reduce importance potential
        if null_percentage > 50.0 {
            return FeatureImportancePotential::Low;
        }

        match profile.data_type {
            DataType::Integer | DataType::Float => {
                if let ColumnStats::Numeric { min, max, .. } = &profile.stats {
                    if (max - min).abs() < f64::EPSILON {
                        FeatureImportancePotential::Low // Constant
                    } else {
                        FeatureImportancePotential::High // Numeric with variance
                    }
                } else {
                    FeatureImportancePotential::Medium
                }
            }
            DataType::String => {
                let unique_ratio = self.estimate_unique_ratio(profile);
                if unique_ratio < 0.1 {
                    FeatureImportancePotential::High // Good categorical
                } else {
                    FeatureImportancePotential::Medium // Text data or medium cardinality
                }
            }
            DataType::Date => FeatureImportancePotential::High, // Temporal features often important
        }
    }

    fn generate_preprocessing_suggestions(
        &self,
        profiles: &[ColumnProfile],
        issues: &[QualityIssue],
    ) -> Vec<PreprocessingSuggestion> {
        let mut suggestions = Vec::new();

        // Missing value handling
        let high_null_columns: Vec<String> = profiles
            .iter()
            .filter(|p| (p.null_count as f64 / p.total_count as f64) > 0.1)
            .map(|p| p.name.clone())
            .collect();

        if !high_null_columns.is_empty() {
            suggestions.push(PreprocessingSuggestion {
                step: "Missing Value Imputation".to_string(),
                description: "Handle missing values using appropriate strategies (mean/median for numeric, mode for categorical)".to_string(),
                columns_affected: high_null_columns,
                priority: RecommendationPriority::High,
                tools_frameworks: vec!["pandas".to_string(), "scikit-learn".to_string(), "feature-engine".to_string()],
            });
        }

        // Feature encoding
        let categorical_columns: Vec<String> = profiles
            .iter()
            .filter(|p| matches!(p.data_type, DataType::String))
            .map(|p| p.name.clone())
            .collect();

        if !categorical_columns.is_empty() {
            suggestions.push(PreprocessingSuggestion {
                step: "Categorical Encoding".to_string(),
                description:
                    "Encode categorical variables using one-hot, label, or target encoding"
                        .to_string(),
                columns_affected: categorical_columns,
                priority: RecommendationPriority::High,
                tools_frameworks: vec![
                    "pandas".to_string(),
                    "scikit-learn".to_string(),
                    "category_encoders".to_string(),
                ],
            });
        }

        // Feature scaling
        let numeric_columns: Vec<String> = profiles
            .iter()
            .filter(|p| matches!(p.data_type, DataType::Integer | DataType::Float))
            .map(|p| p.name.clone())
            .collect();

        if numeric_columns.len() > 1 {
            suggestions.push(PreprocessingSuggestion {
                step: "Feature Scaling".to_string(),
                description:
                    "Standardize or normalize numeric features for algorithms sensitive to scale"
                        .to_string(),
                columns_affected: numeric_columns,
                priority: RecommendationPriority::Medium,
                tools_frameworks: vec!["scikit-learn".to_string(), "pandas".to_string()],
            });
        }

        // Date feature engineering
        let date_columns: Vec<String> = profiles
            .iter()
            .filter(|p| matches!(p.data_type, DataType::Date))
            .map(|p| p.name.clone())
            .collect();

        if !date_columns.is_empty() {
            suggestions.push(PreprocessingSuggestion {
                step: "Temporal Feature Engineering".to_string(),
                description: "Extract date components (year, month, day, weekday) and calculate time-based features".to_string(),
                columns_affected: date_columns,
                priority: RecommendationPriority::Medium,
                tools_frameworks: vec!["pandas".to_string(), "feature-engine".to_string()],
            });
        }

        // Outlier handling
        let outlier_columns: Vec<String> = issues
            .iter()
            .filter_map(|issue| match issue {
                QualityIssue::Outliers { column, .. } => Some(column.clone()),
                _ => None,
            })
            .collect();

        if !outlier_columns.is_empty() {
            suggestions.push(PreprocessingSuggestion {
                step: "Outlier Treatment".to_string(),
                description: "Handle outliers using capping, transformation, or removal based on domain knowledge".to_string(),
                columns_affected: outlier_columns,
                priority: RecommendationPriority::Low,
                tools_frameworks: vec!["scipy".to_string(), "scikit-learn".to_string()],
            });
        }

        suggestions
    }
}

impl Default for MlReadinessEngine {
    fn default() -> Self {
        Self::new()
    }
}
