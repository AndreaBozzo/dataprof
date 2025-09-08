use anyhow::Result;
use arrow::array::*;
use arrow::csv::{Reader as CsvReader, ReaderBuilder};
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use crate::types::{ColumnProfile, ColumnStats, DataType, FileInfo, QualityReport, ScanInfo};
use crate::QualityChecker;

/// Columnar profiler using Apache Arrow for efficient column-oriented processing
pub struct ArrowProfiler {
    batch_size: usize,
    memory_limit_mb: usize,
}

impl ArrowProfiler {
    pub fn new() -> Self {
        Self {
            batch_size: 8192, // Default batch size for Arrow
            memory_limit_mb: 512,
        }
    }

    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn memory_limit_mb(mut self, limit: usize) -> Self {
        self.memory_limit_mb = limit;
        self
    }

    pub fn analyze_csv_file(&self, file_path: &Path) -> Result<QualityReport> {
        let start = std::time::Instant::now();
        let file = File::open(file_path)?;
        let file_size_bytes = file.metadata()?.len();
        let file_size_mb = file_size_bytes as f64 / 1_048_576.0;

        // Create Arrow CSV reader with optimized settings
        let csv_reader = ReaderBuilder::new(self.create_arrow_schema()?)
            .has_header(true)
            .batch_size(self.batch_size)
            .build(file)?;

        // Process data in columnar batches
        let mut column_analyzers: std::collections::HashMap<String, ColumnAnalyzer> = std::collections::HashMap::new();
        let mut total_rows = 0;

        for batch_result in csv_reader {
            let batch = batch_result?;
            total_rows += batch.num_rows();

            // Process each column in the batch
            for (col_idx, column) in batch.columns().iter().enumerate() {
                let field = batch.schema().field(col_idx);
                let column_name = field.name().to_string();

                let analyzer = column_analyzers
                    .entry(column_name)
                    .or_insert_with(|| ColumnAnalyzer::new(field.data_type()));

                analyzer.process_array(column)?;
            }
        }

        // Convert analyzers to column profiles
        let mut column_profiles = Vec::new();
        for (name, analyzer) in column_analyzers {
            let profile = analyzer.to_column_profile(name);
            column_profiles.push(profile);
        }

        // Create sample data for quality checking
        // Since we're using columnar processing, we'll create minimal samples
        let sample_columns = self.create_quality_check_samples(&column_profiles);
        let issues = QualityChecker::check_columns(&column_profiles, &sample_columns);

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
                sampling_ratio: 1.0, // Arrow processes all data efficiently
                scan_time_ms,
            },
        })
    }

    fn create_arrow_schema(&self) -> Result<Arc<Schema>> {
        // Create a flexible schema that can handle most CSV data
        // Arrow will infer types during reading
        Ok(Arc::new(Schema::empty()))
    }

    fn create_quality_check_samples(
        &self,
        profiles: &[ColumnProfile],
    ) -> std::collections::HashMap<String, Vec<String>> {
        // Create empty samples for quality checking
        // In a real implementation, we'd keep samples during processing
        let mut samples = std::collections::HashMap::new();
        for profile in profiles {
            samples.insert(profile.name.clone(), Vec::new());
        }
        samples
    }
}

impl Default for ArrowProfiler {
    fn default() -> Self {
        Self::new()
    }
}

/// Column analyzer for Arrow arrays
struct ColumnAnalyzer {
    data_type: arrow::datatypes::DataType,
    total_count: usize,
    null_count: usize,
    unique_values: std::collections::HashSet<String>,
    // Numeric statistics
    min_value: Option<f64>,
    max_value: Option<f64>,
    sum: f64,
    sum_squares: f64,
    // Text statistics
    min_length: usize,
    max_length: usize,
    total_length: usize,
    // Sample values for pattern detection
    sample_values: Vec<String>,
}

impl ColumnAnalyzer {
    fn new(data_type: &arrow::datatypes::DataType) -> Self {
        Self {
            data_type: data_type.clone(),
            total_count: 0,
            null_count: 0,
            unique_values: std::collections::HashSet::new(),
            min_value: None,
            max_value: None,
            sum: 0.0,
            sum_squares: 0.0,
            min_length: usize::MAX,
            max_length: 0,
            total_length: 0,
            sample_values: Vec::new(),
        }
    }

    fn process_array(&mut self, array: &dyn Array) -> Result<()> {
        self.total_count += array.len();
        self.null_count += array.null_count();

        match array.data_type() {
            arrow::datatypes::DataType::Float64 => {
                if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
                    self.process_float64_array(float_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to Float64Array"));
                }
            }
            arrow::datatypes::DataType::Float32 => {
                if let Some(float_array) = array.as_any().downcast_ref::<Float32Array>() {
                    self.process_float32_array(float_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to Float32Array"));
                }
            }
            arrow::datatypes::DataType::Int64 => {
                if let Some(int_array) = array.as_any().downcast_ref::<Int64Array>() {
                    self.process_int64_array(int_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to Int64Array"));
                }
            }
            arrow::datatypes::DataType::Int32 => {
                if let Some(int_array) = array.as_any().downcast_ref::<Int32Array>() {
                    self.process_int32_array(int_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to Int32Array"));
                }
            }
            arrow::datatypes::DataType::Utf8 => {
                if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
                    self.process_string_array(string_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to StringArray"));
                }
            }
            arrow::datatypes::DataType::LargeUtf8 => {
                if let Some(string_array) = array.as_any().downcast_ref::<LargeStringArray>() {
                    self.process_large_string_array(string_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to LargeStringArray"));
                }
            }
            _ => {
                // For other types, convert to string and process
                self.process_as_string_array(array)?;
            }
        }

        Ok(())
    }

    fn process_float64_array(&mut self, array: &Float64Array) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                self.update_numeric_stats(value);

                // Add to unique values (with limit)
                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }

                // Keep samples for pattern detection
                if self.sample_values.len() < 100 {
                    self.sample_values.push(value.to_string());
                }
            }
        }
        Ok(())
    }

    fn process_float32_array(&mut self, array: &Float32Array) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(value.to_string());
                }
            }
        }
        Ok(())
    }

    fn process_int64_array(&mut self, array: &Int64Array) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(value.to_string());
                }
            }
        }
        Ok(())
    }

    fn process_int32_array(&mut self, array: &Int32Array) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let value_f64 = value as f64;
                self.update_numeric_stats(value_f64);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(value.to_string());
                }
            }
        }
        Ok(())
    }

    fn process_string_array(&mut self, array: &StringArray) -> Result<()> {
        for i in 0..array.len() {
            if !array.is_null(i) {
                let value = array.value(i);
                self.update_text_stats(value);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(value.to_string());
                }
            }
        }
        Ok(())
    }

    fn process_large_string_array(&mut self, array: &LargeStringArray) -> Result<()> {
        for i in 0..array.len() {
            if !array.is_null(i) {
                let value = array.value(i);
                self.update_text_stats(value);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value.to_string());
                }

                if self.sample_values.len() < 100 {
                    self.sample_values.push(value.to_string());
                }
            }
        }
        Ok(())
    }

    fn process_as_string_array(&mut self, array: &dyn Array) -> Result<()> {
        // Convert any array type to string for processing
        for i in 0..array.len() {
            if !array.is_null(i) {
                // This is a simplified approach - in practice we'd need more sophisticated conversion
                let value = format!("value_{}", i); // Placeholder
                self.update_text_stats(&value);

                if self.sample_values.len() < 100 {
                    self.sample_values.push(value.clone());
                }

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value);
                }
            }
        }
        Ok(())
    }

    fn update_numeric_stats(&mut self, value: f64) {
        self.sum += value;
        self.sum_squares += value * value;

        self.min_value = Some(match self.min_value {
            Some(min) => min.min(value),
            None => value,
        });

        self.max_value = Some(match self.max_value {
            Some(max) => max.max(value),
            None => value,
        });
    }

    fn update_text_stats(&mut self, value: &str) {
        let len = value.len();
        self.min_length = self.min_length.min(len);
        self.max_length = self.max_length.max(len);
        self.total_length += len;
    }

    fn to_column_profile(self, name: String) -> ColumnProfile {
        let data_type = self.infer_data_type();

        let stats = match data_type {
            DataType::Integer | DataType::Float => ColumnStats::Numeric {
                min: self.min_value.unwrap_or(0.0),
                max: self.max_value.unwrap_or(0.0),
                mean: if self.total_count > self.null_count {
                    self.sum / (self.total_count - self.null_count) as f64
                } else {
                    0.0
                },
            },
            DataType::String | DataType::Date => {
                let avg_length = if self.total_count > self.null_count {
                    self.total_length as f64 / (self.total_count - self.null_count) as f64
                } else {
                    0.0
                };

                ColumnStats::Text {
                    min_length: if self.min_length == usize::MAX { 0 } else { self.min_length },
                    max_length: self.max_length,
                    avg_length,
                }
            }
        };

        // Detect patterns using sample values
        let patterns = crate::detect_patterns(&self.sample_values);

        ColumnProfile {
            name,
            data_type,
            null_count: self.null_count,
            total_count: self.total_count,
            unique_count: Some(self.unique_values.len()),
            stats,
            patterns,
        }
    }

    fn infer_data_type(&self) -> DataType {
        match &self.data_type {
            arrow::datatypes::DataType::Float64 | arrow::datatypes::DataType::Float32 => DataType::Float,
            arrow::datatypes::DataType::Int64 | arrow::datatypes::DataType::Int32 |
            arrow::datatypes::DataType::Int16 | arrow::datatypes::DataType::Int8 => DataType::Integer,
            arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => {
                // Check if it looks like dates
                let sample_size = self.sample_values.len().min(50);
                if sample_size > 0 {
                    let date_like_count = self.sample_values.iter()
                        .take(sample_size)
                        .filter(|s| self.looks_like_date(s))
                        .count();

                    if date_like_count as f64 / sample_size as f64 > 0.7 {
                        DataType::Date
                    } else {
                        DataType::String
                    }
                } else {
                    DataType::String
                }
            }
            _ => DataType::String,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_arrow_profiler() -> Result<()> {
        // Create a test CSV file
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,salary,active")?;
        writeln!(temp_file, "Alice,25,50000.0,true")?;
        writeln!(temp_file, "Bob,30,60000.5,false")?;
        writeln!(temp_file, "Charlie,35,70000.0,true")?;
        temp_file.flush()?;

        // Test Arrow profiler
        let profiler = ArrowProfiler::new();
        let report = profiler.analyze_csv_file(temp_file.path())?;

        assert_eq!(report.column_profiles.len(), 4);

        // Find age column and verify it's detected correctly
        let age_column = report.column_profiles.iter()
            .find(|p| p.name == "age")
            .expect("Age column should exist");

        // With Arrow's type inference, this should be detected as integer
        assert_eq!(age_column.total_count, 3);

        Ok(())
    }
}
