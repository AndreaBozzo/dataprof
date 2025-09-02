use once_cell::sync::Lazy;
use polars::prelude::*;
use regex::Regex;

use crate::types::{ColumnProfile, ColumnStats, DataType, Pattern};

// Pattern comuni pre-compilati
static EMAIL_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").unwrap());

static PHONE_IT_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^(\+39|0039|39)?[ ]?[0-9]{2,4}[ ]?[0-9]{5,10}$").unwrap());

static FISCAL_CODE_IT_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[A-Z]{6}[0-9]{2}[A-Z][0-9]{2}[A-Z][0-9]{3}[A-Z]$").unwrap());

static DATE_FORMATS: &[&str] = &["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%d.%m.%Y"];

pub struct Analyzer;

impl Analyzer {
    pub fn analyze_column(series: &Series) -> ColumnProfile {
        let name = series.name().to_string();
        let null_count = series.null_count();

        // Detect data type
        let data_type = Self::infer_data_type(series);

        // Get statistics based on type
        let stats = Self::compute_stats(series, &data_type);

        // Detect patterns for string columns
        let patterns = if matches!(data_type, DataType::String) {
            Self::detect_patterns(series)
        } else {
            vec![]
        };

        // Count unique values (solo per colonne piccole)
        let unique_count = if series.len() < 1_000_000 {
            Some(series.n_unique().unwrap_or(0))
        } else {
            None
        };

        ColumnProfile {
            name,
            data_type,
            null_count,
            unique_count,
            patterns,
            stats,
        }
    }

    fn infer_data_type(series: &Series) -> DataType {
        // Prova conversioni in ordine di specificità
        if series.dtype() == &polars::datatypes::DataType::String {
            // Per stringhe, controlla se sono date/numeri
            let sample = series.head(Some(1000));

            if let Ok(str_series) = sample.str() {
                let total_values = sample.len() - sample.null_count();
                if total_values == 0 {
                    return DataType::String;
                }

                // Check date - deve avere almeno 80% di successo
                for format in DATE_FORMATS {
                    let mut successful_parses = 0;
                    for value in str_series.into_iter().flatten() {
                        if chrono::NaiveDate::parse_from_str(value, format).is_ok() {
                            successful_parses += 1;
                        }
                    }
                    let success_rate = successful_parses as f64 / total_values as f64;
                    if success_rate > 0.8 {
                        return DataType::Date;
                    }
                }

                // Check numeric - controllo più rigoroso
                let mut successful_numeric_parses = 0;
                for value in str_series.into_iter().flatten() {
                    // Deve essere SOLO numerico (no lettere, no trattini, no spazi eccetto decimali)
                    if value
                        .chars()
                        .all(|c| c.is_ascii_digit() || c == '.' || c == '-' || c == '+')
                        && value.parse::<f64>().is_ok()
                    {
                        successful_numeric_parses += 1;
                    }
                }
                let numeric_success_rate = successful_numeric_parses as f64 / total_values as f64;
                if numeric_success_rate > 0.9 {
                    // Controlla se sono tutti interi
                    let mut all_integers = true;
                    for value in str_series.into_iter().flatten() {
                        if let Ok(num) = value.parse::<f64>()
                            && num.fract() != 0.0
                        {
                            all_integers = false;
                            break;
                        }
                    }
                    return if all_integers {
                        DataType::Integer
                    } else {
                        DataType::Float
                    };
                }
            }
        }

        // Map Polars dtype to our DataType
        match series.dtype() {
            polars::datatypes::DataType::Int32 | polars::datatypes::DataType::Int64 => {
                DataType::Integer
            }
            polars::datatypes::DataType::Float32 | polars::datatypes::DataType::Float64 => {
                DataType::Float
            }
            polars::datatypes::DataType::Boolean => DataType::Boolean,
            polars::datatypes::DataType::Date => DataType::Date,
            polars::datatypes::DataType::Datetime(_, _) => DataType::DateTime,
            _ => DataType::String,
        }
    }

    fn detect_patterns(series: &Series) -> Vec<Pattern> {
        let mut patterns = Vec::new();

        if let Ok(str_series) = series.str() {
            // Prendi un sample per performance
            let sample_size = 1000.min(series.len());
            let sample = str_series.head(Some(sample_size));

            // Test patterns
            let test_patterns = vec![
                ("Email", &*EMAIL_REGEX),
                ("Italian Phone", &*PHONE_IT_REGEX),
                ("Italian Fiscal Code", &*FISCAL_CODE_IT_REGEX),
            ];

            for (name, regex) in test_patterns {
                let matches = sample
                    .into_iter()
                    .flatten()
                    .filter(|s| regex.is_match(s))
                    .count();

                if matches > 0 {
                    let percentage = matches as f64 / sample_size as f64 * 100.0;
                    patterns.push(Pattern {
                        name: name.to_string(),
                        regex: regex.as_str().to_string(),
                        match_count: matches,
                        match_percentage: percentage,
                    });
                }
            }
        }

        patterns
    }

    fn compute_stats(series: &Series, data_type: &DataType) -> ColumnStats {
        match data_type {
            DataType::Integer | DataType::Float => {
                let stats = series
                    .cast(&polars::datatypes::DataType::Float64)
                    .unwrap_or(series.clone());

                ColumnStats::Numeric {
                    min: stats.min::<f64>().unwrap_or(Some(0.0)).unwrap_or(0.0),
                    max: stats.max::<f64>().unwrap_or(Some(0.0)).unwrap_or(0.0),
                    mean: stats.mean().unwrap_or(0.0),
                    std: stats.std(1).unwrap_or(0.0),
                }
            }
            DataType::String => {
                if let Ok(str_series) = series.str() {
                    let lengths: Vec<usize> = str_series
                        .into_iter()
                        .filter_map(|opt| opt.map(|s| s.len()))
                        .collect();

                    if !lengths.is_empty() {
                        let min = *lengths.iter().min().unwrap_or(&0);
                        let max = *lengths.iter().max().unwrap_or(&0);
                        let avg = lengths.iter().sum::<usize>() as f64 / lengths.len() as f64;

                        ColumnStats::Text {
                            min_length: min,
                            max_length: max,
                            avg_length: avg,
                        }
                    } else {
                        ColumnStats::Text {
                            min_length: 0,
                            max_length: 0,
                            avg_length: 0.0,
                        }
                    }
                } else {
                    ColumnStats::Text {
                        min_length: 0,
                        max_length: 0,
                        avg_length: 0.0,
                    }
                }
            }
            _ => ColumnStats::Text {
                min_length: 0,
                max_length: 0,
                avg_length: 0.0,
            },
        }
    }
}

// Funzione principale di analisi
pub fn analyze_dataframe(df: &DataFrame) -> Vec<ColumnProfile> {
    df.get_columns()
        .iter()
        .map(Analyzer::analyze_column)
        .collect()
}
