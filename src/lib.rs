pub mod quality;
pub mod sampler;
pub mod types;

use anyhow::Result;
use csv::ReaderBuilder;
use regex::Regex;
use std::collections::HashMap;
use std::path::Path;

// Re-export types
pub use quality::QualityChecker;
pub use sampler::{SampleInfo, Sampler};
pub use types::{
    ColumnProfile, ColumnStats, DataType, FileInfo, Pattern, QualityIssue, QualityReport, ScanInfo,
};

// Nuova funzione che usa il sampler per file grandi
pub fn analyze_csv_with_sampling(file_path: &Path) -> Result<QualityReport> {
    let metadata = std::fs::metadata(file_path)?;
    let file_size_mb = metadata.len() as f64 / 1_048_576.0;

    let sampler = Sampler::new(file_size_mb);
    let start = std::time::Instant::now();

    let (records, sample_info) = sampler.sample_csv(file_path)?;

    // Converti i records in formato compatibile
    let mut columns: HashMap<String, Vec<String>> = HashMap::new();

    if !records.is_empty() {
        // Usa gli header dal primo record (assumendo che ci siano)
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_path(file_path)?;
        let headers = reader.headers()?.clone();

        // Inizializza colonne
        for header in headers.iter() {
            columns.insert(header.to_string(), Vec::new());
        }

        // Aggiungi i dati campionati
        for record in records {
            for (i, field) in record.iter().enumerate() {
                if let Some(header) = headers.get(i) {
                    if let Some(column_data) = columns.get_mut(header) {
                        column_data.push(field.to_string());
                    }
                }
            }
        }
    }

    // Analizza le colonne
    let mut column_profiles = Vec::new();
    for (name, data) in &columns {
        let profile = analyze_column(name, data);
        column_profiles.push(profile);
    }

    // Check quality issues
    let issues = QualityChecker::check_columns(&column_profiles, &columns);

    let scan_time_ms = start.elapsed().as_millis();

    Ok(QualityReport {
        file_info: FileInfo {
            path: file_path.display().to_string(),
            total_rows: sample_info.total_rows,
            total_columns: column_profiles.len(),
            file_size_mb,
        },
        column_profiles,
        issues,
        scan_info: ScanInfo {
            rows_scanned: sample_info.sampled_rows,
            sampling_ratio: sample_info.sampling_ratio,
            scan_time_ms,
        },
    })
}

// Funzione originale per compatibilità
pub fn analyze_csv(file_path: &Path) -> Result<Vec<ColumnProfile>> {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_path(file_path)?;

    // Get headers
    let headers = reader.headers()?.clone();
    let mut columns: HashMap<String, Vec<String>> = HashMap::new();

    // Initialize columns
    for header in headers.iter() {
        columns.insert(header.to_string(), Vec::new());
    }

    // Read records
    for result in reader.records() {
        let record = result?;
        for (i, field) in record.iter().enumerate() {
            if let Some(header) = headers.get(i) {
                if let Some(column_data) = columns.get_mut(header) {
                    column_data.push(field.to_string());
                }
            }
        }
    }

    // Analyze each column
    let mut profiles = Vec::new();
    for (name, data) in columns {
        let profile = analyze_column(&name, &data);
        profiles.push(profile);
    }

    Ok(profiles)
}

fn analyze_column(name: &str, data: &[String]) -> ColumnProfile {
    let total_count = data.len();
    let null_count = data.iter().filter(|s| s.is_empty()).count();

    // Infer type
    let data_type = infer_type(data);

    // Calculate stats
    let stats = match data_type {
        DataType::Integer | DataType::Float => calculate_numeric_stats(data),
        DataType::String | DataType::Date => calculate_text_stats(data),
    };

    // Detect patterns
    let patterns = detect_patterns(data);

    ColumnProfile {
        name: name.to_string(),
        data_type,
        null_count,
        total_count,
        unique_count: Some(data.iter().collect::<std::collections::HashSet<_>>().len()),
        stats,
        patterns,
    }
}

fn infer_type(data: &[String]) -> DataType {
    let non_empty: Vec<&String> = data.iter().filter(|s| !s.is_empty()).collect();
    if non_empty.is_empty() {
        return DataType::String;
    }

    // Check dates first (before numeric to catch date-like numbers)
    let date_formats = [
        r"^\d{4}-\d{2}-\d{2}$", // YYYY-MM-DD
        r"^\d{2}/\d{2}/\d{4}$", // DD/MM/YYYY or MM/DD/YYYY
        r"^\d{2}-\d{2}-\d{4}$", // DD-MM-YYYY
    ];

    for pattern in &date_formats {
        let regex = Regex::new(pattern).unwrap();
        let date_matches = non_empty.iter().filter(|s| regex.is_match(s)).count();
        if date_matches as f64 / non_empty.len() as f64 > 0.8 {
            return DataType::Date;
        }
    }

    // Check if all are integers
    let integer_count = non_empty
        .iter()
        .filter(|s| s.parse::<i64>().is_ok())
        .count();

    if integer_count == non_empty.len() {
        return DataType::Integer;
    }

    // Check if all are floats
    let float_count = non_empty
        .iter()
        .filter(|s| s.parse::<f64>().is_ok())
        .count();

    if float_count == non_empty.len() {
        return DataType::Float;
    }

    DataType::String
}

fn calculate_numeric_stats(data: &[String]) -> ColumnStats {
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

fn calculate_text_stats(data: &[String]) -> ColumnStats {
    let non_empty: Vec<&String> = data.iter().filter(|s| !s.is_empty()).collect();

    if non_empty.is_empty() {
        return ColumnStats::Text {
            min_length: 0,
            max_length: 0,
            avg_length: 0.0,
        };
    }

    let lengths: Vec<usize> = non_empty.iter().map(|s| s.len()).collect();
    let min_length = *lengths.iter().min().unwrap();
    let max_length = *lengths.iter().max().unwrap();
    let avg_length = lengths.iter().sum::<usize>() as f64 / lengths.len() as f64;

    ColumnStats::Text {
        min_length,
        max_length,
        avg_length,
    }
}

fn detect_patterns(data: &[String]) -> Vec<Pattern> {
    let mut patterns = Vec::new();
    let non_empty: Vec<&String> = data.iter().filter(|s| !s.is_empty()).collect();

    if non_empty.is_empty() {
        return patterns;
    }

    // Common patterns to check
    let pattern_checks = [
        ("Email", r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),
        (
            "Phone (US)",
            r"^\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}$",
        ),
        (
            "Phone (IT)",
            r"^\+39|0039|39?[-.\s]?[0-9]{2,4}[-.\s]?[0-9]{5,10}$",
        ),
        ("URL", r"^https?://[^\s/$.?#].[^\s]*$"),
        (
            "UUID",
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        ),
    ];

    for (name, pattern_str) in &pattern_checks {
        if let Ok(regex) = Regex::new(pattern_str) {
            let matches = non_empty.iter().filter(|s| regex.is_match(s)).count();
            let percentage = (matches as f64 / non_empty.len() as f64) * 100.0;

            if percentage > 5.0 {
                // Only show patterns with >5% matches
                patterns.push(Pattern {
                    name: name.to_string(),
                    regex: pattern_str.to_string(),
                    match_count: matches,
                    match_percentage: percentage,
                });
            }
        }
    }

    patterns
}
