/// Simple columnar profiler without external dependencies
/// This provides column-oriented processing without Arrow until dependency conflicts are resolved
use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;

use crate::acceleration::simd::{compute_stats_auto, should_use_simd};
// use crate::core::streaming_stats::StreamingColumnCollection; // Future use
use crate::types::{ColumnProfile, ColumnStats, DataType, FileInfo, QualityReport, ScanInfo};
use crate::QualityChecker;

/// Simple columnar profiler that organizes data by columns for better cache performance
pub struct SimpleColumnarProfiler {
    batch_size: usize,
    use_simd: bool,
}

impl SimpleColumnarProfiler {
    pub fn new() -> Self {
        Self {
            batch_size: 8192,
            use_simd: true,
        }
    }

    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn use_simd(mut self, enabled: bool) -> Self {
        self.use_simd = enabled;
        self
    }

    pub fn analyze_csv_file(&self, file_path: &Path) -> Result<QualityReport> {
        let start = std::time::Instant::now();
        let file_size_bytes = std::fs::metadata(file_path)?.len();
        let file_size_mb = file_size_bytes as f64 / 1_048_576.0;

        // Read CSV data in row format first (simple approach)
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path(file_path)?;

        let headers = reader.headers()?;
        let header_names: Vec<String> = headers.iter().map(|s| s.to_string()).collect();

        // Organize data by columns for better processing
        let mut columnar_data: HashMap<String, Vec<String>> = HashMap::new();
        for header in &header_names {
            columnar_data.insert(header.to_string(), Vec::new());
        }

        let mut total_rows = 0;

        // Read data in batches to avoid memory explosion
        for result in reader.records() {
            let record = result?;
            total_rows += 1;

            // Add values to respective columns
            for (i, field) in record.iter().enumerate() {
                if let Some(header) = header_names.get(i) {
                    if let Some(column_data) = columnar_data.get_mut(header) {
                        column_data.push(field.to_string());
                    }
                }
            }

            // Process in batches if file is large
            if total_rows % self.batch_size == 0 && file_size_mb > 100.0 {
                // For very large files, we could process and clear batches here
                // For now, keep it simple
            }
        }

        // Process each column using optimized algorithms
        let mut column_profiles = Vec::new();
        for (column_name, column_data) in columnar_data.iter() {
            let profile = self.analyze_column_optimized(column_name, column_data)?;
            column_profiles.push(profile);
        }

        // Quality checking with sample data
        let issues = QualityChecker::check_columns(&column_profiles, &columnar_data);

        let scan_time_ms = start.elapsed().as_millis();

        Ok(QualityReport {
            file_info: FileInfo {
                path: file_path.display().to_string(),
                total_rows: Some(total_rows),
                total_columns: column_profiles.len(),
                file_size_mb,
            },
            column_profiles,
            issues,
            scan_info: ScanInfo {
                rows_scanned: total_rows,
                sampling_ratio: 1.0,
                scan_time_ms,
            },
        })
    }

    /// Analyze a single column with optimized algorithms
    fn analyze_column_optimized(&self, name: &str, data: &[String]) -> Result<ColumnProfile> {
        let total_count = data.len();
        let null_count = data.iter().filter(|s| s.is_empty()).count();

        // Try to parse as numeric for SIMD acceleration
        let numeric_values: Vec<f64> = data
            .iter()
            .filter_map(|s| if !s.is_empty() { s.parse().ok() } else { None })
            .collect();

        let data_type = self.infer_column_type(data, &numeric_values);

        let stats = match data_type {
            DataType::Integer | DataType::Float => {
                if !numeric_values.is_empty()
                    && self.use_simd
                    && should_use_simd(numeric_values.len())
                {
                    // Use SIMD-accelerated statistics
                    let simd_stats = compute_stats_auto(&numeric_values);
                    ColumnStats::Numeric {
                        min: simd_stats.min,
                        max: simd_stats.max,
                        mean: simd_stats.mean(),
                    }
                } else {
                    // Fallback to regular computation
                    crate::calculate_numeric_stats(data)
                }
            }
            DataType::String | DataType::Date => crate::calculate_text_stats(data),
        };

        // Calculate unique count efficiently
        let unique_values: std::collections::HashSet<_> = data.iter().collect();
        let unique_count = unique_values.len();

        // Pattern detection
        let patterns = crate::detect_patterns(data);

        Ok(ColumnProfile {
            name: name.to_string(),
            data_type,
            null_count,
            total_count,
            unique_count: Some(unique_count),
            stats,
            patterns,
        })
    }

    fn infer_column_type(&self, data: &[String], numeric_values: &[f64]) -> DataType {
        let non_empty: Vec<&String> = data.iter().filter(|s| !s.is_empty()).collect();

        if non_empty.is_empty() {
            return DataType::String;
        }

        // If we have numeric values and they represent most of the data, it's numeric
        let numeric_ratio = numeric_values.len() as f64 / non_empty.len() as f64;
        if numeric_ratio > 0.8 {
            // Check if all numeric values are integers
            let all_integers = numeric_values.iter().all(|&v| v.fract() == 0.0);
            return if all_integers {
                DataType::Integer
            } else {
                DataType::Float
            };
        }

        // Check for dates
        let date_pattern_count = non_empty
            .iter()
            .take(100) // Sample first 100 values
            .filter(|s| self.looks_like_date(s))
            .count();

        if date_pattern_count as f64 / non_empty.len().min(100) as f64 > 0.7 {
            DataType::Date
        } else {
            DataType::String
        }
    }

    fn looks_like_date(&self, value: &str) -> bool {
        use regex::Regex;

        let date_patterns = [
            r"^\d{4}-\d{2}-\d{2}$",
            r"^\d{2}/\d{2}/\d{4}$",
            r"^\d{2}-\d{2}-\d{4}$",
        ];

        date_patterns.iter().any(|pattern| {
            Regex::new(pattern)
                .map(|re| re.is_match(value))
                .unwrap_or(false)
        })
    }
}

impl Default for SimpleColumnarProfiler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_simple_columnar_profiler() -> Result<()> {
        // Create a test CSV file
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,salary")?;
        writeln!(temp_file, "Alice,25,50000.0")?;
        writeln!(temp_file, "Bob,30,60000.5")?;
        writeln!(temp_file, "Charlie,35,70000.0")?;
        temp_file.flush()?;

        // Test simple columnar profiler
        let profiler = SimpleColumnarProfiler::new().use_simd(true);
        let report = profiler.analyze_csv_file(temp_file.path())?;

        assert_eq!(report.column_profiles.len(), 3);

        // Find age column and verify it's detected correctly
        let age_column = report
            .column_profiles
            .iter()
            .find(|p| p.name == "age")
            .expect("Age column should exist");

        assert_eq!(age_column.data_type, DataType::Integer);
        assert_eq!(age_column.total_count, 3);

        Ok(())
    }

    #[test]
    fn test_simd_acceleration() -> Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "numbers")?;
        for i in 1..=1000 {
            writeln!(temp_file, "{}", i)?;
        }
        temp_file.flush()?;

        let profiler = SimpleColumnarProfiler::new().use_simd(true);
        let report = profiler.analyze_csv_file(temp_file.path())?;

        let numbers_column = report
            .column_profiles
            .iter()
            .find(|p| p.name == "numbers")
            .expect("Numbers column should exist");

        assert_eq!(numbers_column.data_type, DataType::Integer);

        if let ColumnStats::Numeric { mean, .. } = &numbers_column.stats {
            assert!((mean - 500.5).abs() < 1e-10);
        }

        Ok(())
    }
}
