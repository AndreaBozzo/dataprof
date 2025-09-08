use crate::types::*;
use regex::Regex;
use std::collections::HashMap;

pub struct QualityChecker;

impl QualityChecker {
    pub fn check_columns(
        column_profiles: &[ColumnProfile],
        data: &HashMap<String, Vec<String>>,
    ) -> Vec<QualityIssue> {
        let mut issues = Vec::new();

        for profile in column_profiles {
            if let Some(column_data) = data.get(&profile.name) {
                // Check nulls
                if let Some(issue) = Self::check_nulls(profile) {
                    issues.push(issue);
                }

                // Check mixed date formats per stringhe
                if matches!(profile.data_type, DataType::String) {
                    if let Some(issue) = Self::check_date_formats(&profile.name, column_data) {
                        issues.push(issue);
                    }
                }

                // Check duplicates se abbiamo unique_count
                if let Some(unique_count) = profile.unique_count {
                    if unique_count < profile.total_count {
                        let duplicate_count = profile.total_count - unique_count;
                        // Solo se i duplicati sono significativi (>5% dei dati)
                        if duplicate_count as f64 / profile.total_count as f64 > 0.05 {
                            issues.push(QualityIssue::Duplicates {
                                column: profile.name.to_string(),
                                count: duplicate_count,
                            });
                        }
                    }
                }

                // Check outliers per colonne numeriche
                if matches!(profile.data_type, DataType::Integer | DataType::Float) {
                    if let Some(issue) = Self::check_outliers(&profile.name, column_data) {
                        issues.push(issue);
                    }
                }
            }
        }

        issues
    }

    fn check_nulls(profile: &ColumnProfile) -> Option<QualityIssue> {
        if profile.null_count > 0 {
            let percentage = profile.null_count as f64 / profile.total_count as f64 * 100.0;
            Some(QualityIssue::NullValues {
                column: profile.name.to_string(),
                count: profile.null_count,
                percentage,
            })
        } else {
            None
        }
    }

    fn check_date_formats(column_name: &str, data: &[String]) -> Option<QualityIssue> {
        let mut format_counts = HashMap::new();

        // Pattern per diversi formati di data
        let date_patterns = vec![
            ("YYYY-MM-DD", r"\d{4}-\d{2}-\d{2}"),
            ("DD/MM/YYYY", r"\d{2}/\d{2}/\d{4}"),
            ("DD-MM-YYYY", r"\d{2}-\d{2}-\d{4}"),
            ("MM/DD/YYYY", r"\d{2}/\d{2}/\d{4}"), // Ambiguo con DD/MM/YYYY
            ("YYYY/MM/DD", r"\d{4}/\d{2}/\d{2}"),
        ];

        let non_empty: Vec<&String> = data.iter().filter(|s| !s.is_empty()).collect();
        if non_empty.is_empty() {
            return None;
        }

        // Campiona primi 100 valori per performance
        let sample_size = 100.min(non_empty.len());

        for value in non_empty.iter().take(sample_size) {
            for (format_name, pattern) in &date_patterns {
                if let Ok(regex) = Regex::new(pattern) {
                    if regex.is_match(value) {
                        *format_counts.entry(format_name.to_string()).or_insert(0) += 1;
                        break; // Un formato per valore
                    }
                }
            }
        }

        // Se troviamo più di un formato con count significativo
        if format_counts.len() > 1 {
            let total_matches: usize = format_counts.values().sum();
            // Solo se almeno il 10% dei valori sembrano date
            if total_matches as f64 / sample_size as f64 > 0.1 {
                return Some(QualityIssue::MixedDateFormats {
                    column: column_name.to_string(),
                    formats: format_counts,
                });
            }
        }

        None
    }

    fn check_outliers(column_name: &str, data: &[String]) -> Option<QualityIssue> {
        // Cerca di convertire i valori in numeri
        let numeric_values: Vec<f64> = data
            .iter()
            .filter_map(|s| {
                if s.is_empty() {
                    None
                } else {
                    s.parse::<f64>().ok()
                }
            })
            .collect();

        if numeric_values.len() < 3 {
            return None; // Troppo pochi valori per outlier detection
        }

        // Calcola mean e standard deviation
        let mean = numeric_values.iter().sum::<f64>() / numeric_values.len() as f64;
        let variance = numeric_values
            .iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>()
            / numeric_values.len() as f64;
        let std_dev = variance.sqrt();

        if std_dev == 0.0 {
            return None; // Tutti i valori sono uguali
        }

        // 3-sigma rule per outliers
        let threshold = 3.0;
        let outliers: Vec<String> = data
            .iter()
            .enumerate()
            .filter_map(|(idx, value)| {
                if let Ok(num_val) = value.parse::<f64>() {
                    if (num_val - mean).abs() > threshold * std_dev {
                        Some(format!("Row {}: {}", idx + 1, value))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .take(10) // Limita a primi 10 outliers
            .collect();

        if !outliers.is_empty() {
            Some(QualityIssue::Outliers {
                column: column_name.to_string(),
                values: outliers,
                threshold,
            })
        } else {
            None
        }
    }
}
