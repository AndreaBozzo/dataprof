use crate::types::{self, ColumnProfile, QualityIssue};
use polars::prelude::*;
use std::collections::HashMap;

pub struct QualityChecker;

impl QualityChecker {
    pub fn check_dataframe(df: &DataFrame, profiles: &[ColumnProfile]) -> Vec<QualityIssue> {
        let mut issues = Vec::new();
        
        for (column, profile) in df.get_columns().iter().zip(profiles.iter()) {
            // Check nulls
            if let Some(issue) = Self::check_nulls(column, profile) {
                issues.push(issue);
            }
            
            // Check mixed date formats
            if matches!(profile.data_type, types::DataType::String) {
                if let Some(issue) = Self::check_date_formats(column) {
                    issues.push(issue);
                }
            }
            
            // Check duplicates (solo per colonne che dovrebbero essere unique)
            if let Some(unique_count) = profile.unique_count {
                if unique_count < column.len() * 95 / 100 {
                    if let Some(issue) = Self::check_duplicates(column) {
                        issues.push(issue);
                    }
                }
            }
            
            // Check outliers per colonne numeriche
            if matches!(profile.data_type, types::DataType::Integer | types::DataType::Float) {
                if let Some(issue) = Self::check_outliers(column) {
                    issues.push(issue);
                }
            }
        }
        
        issues
    }
    
    fn check_nulls(column: &Series, profile: &ColumnProfile) -> Option<QualityIssue> {
        let null_count = column.null_count();
        if null_count > 0 {
            let percentage = null_count as f64 / column.len() as f64 * 100.0;
            Some(QualityIssue::NullValues {
                column: profile.name.clone(),
                count: null_count,
                percentage,
            })
        } else {
            None
        }
    }
    
    fn check_date_formats(column: &Series) -> Option<QualityIssue> {
        if let Ok(str_series) = column.str() {
            let mut format_counts = HashMap::new();
            let sample = str_series.head(Some(1000));
            
            for value in sample.into_iter().filter_map(|v| v) {
                for format in &[
                    r"\d{4}-\d{2}-\d{2}",  // YYYY-MM-DD
                    r"\d{2}/\d{2}/\d{4}",  // DD/MM/YYYY
                    r"\d{2}-\d{2}-\d{4}",  // DD-MM-YYYY
                ] {
                    if regex::Regex::new(format).unwrap().is_match(value) {
                        *format_counts.entry(format.to_string()).or_insert(0) += 1;
                        break;
                    }
                }
            }
            
            if format_counts.len() > 1 {
                return Some(QualityIssue::MixedDateFormats {
                    column: column.name().to_string(),
                    formats: format_counts,
                });
            }
        }
        None
    }
    
    fn check_duplicates(column: &Series) -> Option<QualityIssue> {
        let total = column.len();
        let unique = column.n_unique().unwrap_or(total);
        let duplicate_count = total - unique;
        
        if duplicate_count > 0 {
            Some(QualityIssue::Duplicates {
                column: column.name().to_string(),
                count: duplicate_count,
            })
        } else {
            None
        }
    }
    
    fn check_outliers(column: &Series) -> Option<QualityIssue> {
        if let Ok(numeric) = column.cast(&polars::datatypes::DataType::Float64) {
            let mean = numeric.mean().unwrap_or(0.0);
            let std = numeric.std(1).unwrap_or(1.0);
            let threshold = 3.0; // 3 sigma rule
            
            let mut outliers: Vec<String> = Vec::new();
            if let Ok(float_values) = numeric.f64() {
                for (idx, value) in float_values.into_iter().enumerate() {
                    if let Some(v) = value {
                        if (v - mean).abs() > threshold * std {
                            outliers.push(format!("Row {}: {:.2}", idx, v));
                            if outliers.len() >= 10 { // Limita a 10 esempi
                                break;
                            }
                        }
                    }
                }
            }
            
            if !outliers.is_empty() {
                return Some(QualityIssue::Outliers {
                    column: column.name().to_string(),
                    values: outliers,
                    threshold,
                });
            }
        }
        None
    }
}