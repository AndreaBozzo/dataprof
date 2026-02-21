use anyhow::Result;
use arrow::array::*;
use arrow::csv::ReaderBuilder;
use arrow::datatypes::*;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use crate::analysis::patterns::looks_like_date;
use crate::stats::numeric::calculate_numeric_stats;
use crate::types::DataQualityMetrics;
use crate::types::{
    ColumnProfile, ColumnStats, DataSource, DataType, FileFormat, QualityReport, ScanInfo,
};

/// Sample cap for numeric columns (matches SAMPLE_THRESHOLD in stats::numeric)
const NUMERIC_SAMPLE_CAP: usize = 10_000;

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
        let _file_size_mb = file_size_bytes as f64 / 1_048_576.0;

        // Read first to infer schema from headers
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path(file_path)?;

        // Get headers to create Arrow schema
        let headers = reader.headers()?.clone();
        let mut fields = Vec::new();
        for header in headers.iter() {
            // Start with string type, Arrow will convert during processing
            fields.push(Field::new(header, arrow::datatypes::DataType::Utf8, true));
        }
        let schema = Arc::new(Schema::new(fields));

        // Now create Arrow reader with proper schema
        let file = File::open(file_path)?;
        let csv_reader = ReaderBuilder::new(schema)
            .with_header(true)
            .with_batch_size(self.batch_size)
            .build(file)?;

        // Process data in columnar batches
        let mut column_analyzers: std::collections::HashMap<String, ColumnAnalyzer> =
            std::collections::HashMap::new();
        let mut total_rows = 0;

        for batch_result in csv_reader {
            let batch = batch_result?;
            total_rows += batch.num_rows();

            // Process each column in the batch
            for (col_idx, column) in batch.columns().iter().enumerate() {
                let schema = batch.schema();
                let field = schema.field(col_idx);
                let column_name = field.name().to_string();

                let analyzer = column_analyzers
                    .entry(column_name)
                    .or_insert_with(|| ColumnAnalyzer::new(field.data_type()));

                analyzer.process_array(column)?;
            }
        }

        // Convert analyzers to column profiles and extract samples
        let mut column_profiles = Vec::new();
        let mut sample_columns = std::collections::HashMap::new();

        for (name, analyzer) in &column_analyzers {
            let profile = analyzer.to_column_profile(name.clone());
            column_profiles.push(profile);

            // Extract samples from analyzer for quality metrics
            sample_columns.insert(name.clone(), analyzer.get_sample_values());
        }

        // Calculate data quality metrics using ISO 8000/25012 standards
        let data_quality_metrics =
            DataQualityMetrics::calculate_from_data(&sample_columns, &column_profiles).map_err(
                |e| anyhow::anyhow!("Quality metrics calculation failed for Arrow data: {}", e),
            )?;

        let scan_time_ms = start.elapsed().as_millis();
        let num_columns = column_profiles.len();

        Ok(QualityReport::new(
            DataSource::File {
                path: file_path.display().to_string(),
                format: FileFormat::Csv,
                size_bytes: file_size_bytes,
                modified_at: None,
                parquet_metadata: None,
            },
            column_profiles,
            ScanInfo::new(total_rows, num_columns, total_rows, 1.0, scan_time_ms),
            data_quality_metrics,
        ))
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
            arrow::datatypes::DataType::Boolean => {
                if let Some(bool_array) = array.as_any().downcast_ref::<BooleanArray>() {
                    self.process_boolean_array(bool_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to BooleanArray"));
                }
            }
            arrow::datatypes::DataType::Date32 => {
                if let Some(date_array) = array.as_any().downcast_ref::<Date32Array>() {
                    self.process_date32_array(date_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to Date32Array"));
                }
            }
            arrow::datatypes::DataType::Date64 => {
                if let Some(date_array) = array.as_any().downcast_ref::<Date64Array>() {
                    self.process_date64_array(date_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast to Date64Array"));
                }
            }
            arrow::datatypes::DataType::Timestamp(_, _) => {
                if let Some(ts_array) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
                    self.process_timestamp_array(ts_array)?;
                } else if let Some(ts_array) =
                    array.as_any().downcast_ref::<TimestampMicrosecondArray>()
                {
                    self.process_timestamp_micro_array(ts_array)?;
                } else if let Some(ts_array) =
                    array.as_any().downcast_ref::<TimestampMillisecondArray>()
                {
                    self.process_timestamp_milli_array(ts_array)?;
                } else if let Some(ts_array) = array.as_any().downcast_ref::<TimestampSecondArray>()
                {
                    self.process_timestamp_second_array(ts_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast Timestamp array"));
                }
            }
            arrow::datatypes::DataType::Binary | arrow::datatypes::DataType::LargeBinary => {
                if let Some(bin_array) = array.as_any().downcast_ref::<BinaryArray>() {
                    self.process_binary_array(bin_array)?;
                } else if let Some(bin_array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
                    self.process_large_binary_array(bin_array)?;
                } else {
                    return Err(anyhow::anyhow!("Failed to downcast Binary array"));
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
                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
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

                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
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

                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
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

                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
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

                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
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

                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(value.to_string());
                }
            }
        }
        Ok(())
    }

    fn process_boolean_array(&mut self, array: &BooleanArray) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let value_str = value.to_string();

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(value_str.clone());
                }

                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(value_str);
                }
            }
        }
        Ok(())
    }

    fn process_date32_array(&mut self, array: &Date32Array) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                // Date32 represents days since epoch
                let date_str = format!("date32:{}", value);
                self.update_text_stats(&date_str);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(date_str.clone());
                }

                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(date_str);
                }
            }
        }
        Ok(())
    }

    fn process_date64_array(&mut self, array: &Date64Array) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                // Date64 represents milliseconds since epoch
                let date_str = format!("date64:{}", value);
                self.update_text_stats(&date_str);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(date_str.clone());
                }

                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(date_str);
                }
            }
        }
        Ok(())
    }

    fn process_timestamp_array(&mut self, array: &TimestampNanosecondArray) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let ts_str = format!("timestamp_ns:{}", value);
                self.update_text_stats(&ts_str);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(ts_str.clone());
                }

                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(ts_str);
                }
            }
        }
        Ok(())
    }

    fn process_timestamp_micro_array(&mut self, array: &TimestampMicrosecondArray) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let ts_str = format!("timestamp_us:{}", value);
                self.update_text_stats(&ts_str);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(ts_str.clone());
                }

                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(ts_str);
                }
            }
        }
        Ok(())
    }

    fn process_timestamp_milli_array(&mut self, array: &TimestampMillisecondArray) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let ts_str = format!("timestamp_ms:{}", value);
                self.update_text_stats(&ts_str);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(ts_str.clone());
                }

                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(ts_str);
                }
            }
        }
        Ok(())
    }

    fn process_timestamp_second_array(&mut self, array: &TimestampSecondArray) -> Result<()> {
        for i in 0..array.len() {
            if let Some(value) = array.value(i).into() {
                let ts_str = format!("timestamp_s:{}", value);
                self.update_text_stats(&ts_str);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(ts_str.clone());
                }

                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(ts_str);
                }
            }
        }
        Ok(())
    }

    fn process_binary_array(&mut self, array: &BinaryArray) -> Result<()> {
        for i in 0..array.len() {
            if !array.is_null(i) {
                let value = array.value(i);
                // Convert to hex string manually (first 8 bytes)
                let sample_bytes = &value[..value.len().min(8)];
                let hex_str = format!(
                    "0x{}",
                    sample_bytes
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<String>()
                );
                self.update_text_stats(&hex_str);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(hex_str.clone());
                }

                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(hex_str);
                }
            }
        }
        Ok(())
    }

    fn process_large_binary_array(&mut self, array: &LargeBinaryArray) -> Result<()> {
        for i in 0..array.len() {
            if !array.is_null(i) {
                let value = array.value(i);
                // Convert to hex string manually (first 8 bytes)
                let sample_bytes = &value[..value.len().min(8)];
                let hex_str = format!(
                    "0x{}",
                    sample_bytes
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<String>()
                );
                self.update_text_stats(&hex_str);

                if self.unique_values.len() < 1000 {
                    self.unique_values.insert(hex_str.clone());
                }

                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
                    self.sample_values.push(hex_str);
                }
            }
        }
        Ok(())
    }

    fn process_as_string_array(&mut self, array: &dyn Array) -> Result<()> {
        // Convert any array type to string for processing using Arrow's display functionality
        use arrow::util::display::array_value_to_string;

        for i in 0..array.len() {
            if !array.is_null(i) {
                // Use Arrow's built-in conversion to string
                let value = array_value_to_string(array, i)
                    .unwrap_or_else(|_| format!("<type:{}>", array.data_type()));
                self.update_text_stats(&value);

                if self.sample_values.len() < NUMERIC_SAMPLE_CAP {
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

    fn to_column_profile(&self, name: String) -> ColumnProfile {
        let data_type = self.infer_data_type();

        let stats = match data_type {
            DataType::Integer | DataType::Float => calculate_numeric_stats(&self.sample_values),
            DataType::String | DataType::Date => {
                let avg_length = if self.total_count > self.null_count {
                    self.total_length as f64 / (self.total_count - self.null_count) as f64
                } else {
                    0.0
                };

                ColumnStats::Text {
                    min_length: if self.min_length == usize::MAX {
                        0
                    } else {
                        self.min_length
                    },
                    max_length: self.max_length,
                    avg_length,
                    most_frequent: None,
                    least_frequent: None,
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
            arrow::datatypes::DataType::Float64 | arrow::datatypes::DataType::Float32 => {
                DataType::Float
            }
            arrow::datatypes::DataType::Int64
            | arrow::datatypes::DataType::Int32
            | arrow::datatypes::DataType::Int16
            | arrow::datatypes::DataType::Int8 => DataType::Integer,
            arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => {
                let non_empty: Vec<&String> = self
                    .sample_values
                    .iter()
                    .filter(|s| !s.trim().is_empty())
                    .collect();

                if non_empty.is_empty() {
                    return DataType::String;
                }

                let sample_size = non_empty.len().min(50);

                // Check numeric first (most common for CSV data)
                let mut integer_count = 0usize;
                let mut float_count = 0usize;
                for s in non_empty.iter().take(sample_size) {
                    let trimmed = s.trim();
                    if trimmed.parse::<i64>().is_ok() {
                        integer_count += 1;
                        float_count += 1; // integers are also valid floats
                    } else if trimmed.parse::<f64>().is_ok() {
                        float_count += 1;
                    }
                }

                let numeric_threshold = 0.9;
                if integer_count as f64 / sample_size as f64 >= numeric_threshold {
                    DataType::Integer
                } else if float_count as f64 / sample_size as f64 >= numeric_threshold {
                    DataType::Float
                } else {
                    // Check dates
                    let date_like_count = non_empty
                        .iter()
                        .take(sample_size)
                        .filter(|s| looks_like_date(s))
                        .count();

                    if date_like_count as f64 / sample_size as f64 > 0.7 {
                        DataType::Date
                    } else {
                        DataType::String
                    }
                }
            }
            _ => DataType::String,
        }
    }

    /// Get collected sample values for quality metrics calculation
    fn get_sample_values(&self) -> Vec<String> {
        self.sample_values.clone()
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

        // Find age column and verify it's detected as numeric
        let age_column = report
            .column_profiles
            .iter()
            .find(|p| p.name == "age")
            .expect("Age column should exist");

        assert_eq!(age_column.total_count, 3);
        assert_eq!(
            age_column.data_type,
            DataType::Integer,
            "age column should be detected as Integer"
        );

        Ok(())
    }

    #[test]
    fn test_arrow_profiler_csv_with_mixed_columns() -> Result<()> {
        use crate::types::ColumnStats;

        // The Arrow CSV profiler reads all columns as Utf8 and then infers types.
        // Numeric-looking Utf8 columns that also have float-typed Arrow data
        // get properly typed. Test with a mixed CSV.
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,score")?;
        for i in 1..=20 {
            writeln!(temp_file, "Person{},{}", i, i * 10)?;
        }
        temp_file.flush()?;

        let profiler = ArrowProfiler::new();
        let report = profiler.analyze_csv_file(temp_file.path())?;

        assert_eq!(report.column_profiles.len(), 2);

        let score_col = report
            .column_profiles
            .iter()
            .find(|p| p.name == "score")
            .expect("score column should exist");

        assert_eq!(score_col.total_count, 20);

        // Numeric Utf8 columns should now be detected as Integer
        assert_eq!(
            score_col.data_type,
            DataType::Integer,
            "score column should be detected as Integer"
        );

        // Verify numeric stats are computed
        match &score_col.stats {
            ColumnStats::Numeric {
                min,
                max,
                mean,
                skewness,
                kurtosis,
                ..
            } => {
                assert!((min - 10.0).abs() < 0.01, "min should be 10");
                assert!((max - 200.0).abs() < 0.01, "max should be 200");
                assert!((mean - 105.0).abs() < 0.01, "mean should be 105");
                assert!(skewness.is_some(), "skewness should be computed");
                assert!(kurtosis.is_some(), "kurtosis should be computed");
            }
            other => panic!("score column should have Numeric stats, got {:?}", other),
        }

        // Verify name column is still String
        let name_col = report
            .column_profiles
            .iter()
            .find(|p| p.name == "name")
            .expect("name column should exist");
        assert_eq!(name_col.data_type, DataType::String);

        Ok(())
    }

    #[test]
    fn test_arrow_profiler_numeric_inference_float() -> Result<()> {
        use crate::types::ColumnStats;

        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "label,value")?;
        for i in 1..=20 {
            writeln!(temp_file, "item{},{:.2}", i, i as f64 * 1.5)?;
        }
        temp_file.flush()?;

        let profiler = ArrowProfiler::new();
        let report = profiler.analyze_csv_file(temp_file.path())?;

        let value_col = report
            .column_profiles
            .iter()
            .find(|p| p.name == "value")
            .expect("value column should exist");

        assert_eq!(
            value_col.data_type,
            DataType::Float,
            "value column with decimals should be detected as Float"
        );
        assert!(
            matches!(&value_col.stats, ColumnStats::Numeric { .. }),
            "Float column should have Numeric stats"
        );

        Ok(())
    }
}
