//! Apache Parquet file format parser
//!
//! This module provides analysis capabilities for Parquet files using Apache Arrow.
//! Parquet is a columnar storage format optimized for analytics workloads.
//!
//! # Example
//! ```no_run
//! use dataprof::analyze_parquet_with_quality;
//! use std::path::Path;
//!
//! let report = analyze_parquet_with_quality(Path::new("data.parquet")).unwrap();
//! println!("Quality score: {:.1}%", report.quality_score());
//! ```

use anyhow::Result;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::engines::columnar::record_batch_analyzer::RecordBatchAnalyzer;
use crate::types::{DataQualityMetrics, FileInfo, ParquetMetadata, QualityReport, ScanInfo};

/// Check if a file is a valid Parquet file by examining its magic number
///
/// Parquet files have the magic number "PAR1" at the beginning and end of the file.
/// This function checks both locations to validate the format.
///
/// # Arguments
/// * `file_path` - Path to the file to check
///
/// # Returns
/// * `true` if the file appears to be a valid Parquet file
/// * `false` otherwise (including if the file cannot be read)
///
/// # Example
/// ```no_run
/// use dataprof::is_parquet_file;
/// use std::path::Path;
///
/// if is_parquet_file(Path::new("data.parquet")) {
///     println!("This is a Parquet file!");
/// }
/// ```
pub fn is_parquet_file(file_path: &Path) -> bool {
    let mut file = match File::open(file_path) {
        Ok(f) => f,
        Err(_) => return false,
    };

    // Check file size - Parquet files need at least 8 bytes (4 for header + 4 for footer)
    let file_size = match file.metadata() {
        Ok(meta) => meta.len(),
        Err(_) => return false,
    };

    if file_size < 8 {
        return false;
    }

    // Read first 4 bytes - should be "PAR1"
    let mut header = [0u8; 4];
    if file.read_exact(&mut header).is_err() {
        return false;
    }

    if &header != b"PAR1" {
        return false;
    }

    // Read last 4 bytes - should also be "PAR1"
    if file.seek(SeekFrom::End(-4)).is_err() {
        return false;
    }

    let mut footer = [0u8; 4];
    if file.read_exact(&mut footer).is_err() {
        return false;
    }

    &footer == b"PAR1"
}

/// Configuration options for Parquet analysis
#[derive(Debug, Clone)]
pub struct ParquetConfig {
    /// Batch size for reading Parquet files (number of rows per batch)
    /// Default: 8192 (optimal for most workloads)
    /// - Smaller values (1024-4096): Better for memory-constrained environments
    /// - Larger values (16384-65536): Better for large files with many columns
    pub batch_size: usize,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192, // 8K rows - good balance between memory and performance
        }
    }
}

impl ParquetConfig {
    /// Create new config with custom batch size
    pub fn with_batch_size(batch_size: usize) -> Self {
        Self { batch_size }
    }

    /// Adaptive batch sizing based on file size
    /// Returns optimal batch size for the given file size
    pub fn adaptive_batch_size(file_size_bytes: u64) -> usize {
        match file_size_bytes {
            0..=1_048_576 => 1024,                // < 1MB: small batches
            1_048_577..=10_485_760 => 4096,       // 1-10MB: medium batches
            10_485_761..=104_857_600 => 8192,     // 10-100MB: default batches
            104_857_601..=1_073_741_824 => 16384, // 100MB-1GB: large batches
            _ => 32768,                           // > 1GB: very large batches
        }
    }
}

/// Analyze a Parquet file and return a comprehensive quality report
///
/// This function:
/// - Reads Parquet file using Apache Arrow's ParquetRecordBatchReader
/// - Processes data in columnar batches for efficiency
/// - Calculates comprehensive statistics for each column
/// - Computes ISO 8000/25012 compliant quality metrics
///
/// # Arguments
/// * `file_path` - Path to the Parquet file
///
/// # Returns
/// * `QualityReport` with column profiles and quality metrics
///
/// # Errors
/// Returns error if:
/// - File cannot be opened
/// - File is not valid Parquet format
/// - Data processing fails
///
/// # Example
/// ```no_run
/// use dataprof::analyze_parquet_with_quality;
/// use std::path::Path;
///
/// let report = analyze_parquet_with_quality(Path::new("data.parquet"))?;
/// println!("Analyzed {} rows in {} columns",
///          report.file_info.total_rows.unwrap_or(0),
///          report.file_info.total_columns);
/// # Ok::<(), anyhow::Error>(())
/// ```
pub fn analyze_parquet_with_quality(file_path: &Path) -> Result<QualityReport> {
    analyze_parquet_with_config(file_path, &ParquetConfig::default())
}

/// Analyze a Parquet file with custom configuration
///
/// Provides control over batch size and other processing parameters.
///
/// # Arguments
/// * `file_path` - Path to the Parquet file
/// * `config` - Configuration options for analysis
///
/// # Returns
/// * `QualityReport` with column profiles and quality metrics
///
/// # Example
/// ```no_run
/// use dataprof::{analyze_parquet_with_config, ParquetConfig};
/// use std::path::Path;
///
/// let config = ParquetConfig::with_batch_size(16384);
/// let report = analyze_parquet_with_config(Path::new("data.parquet"), &config)?;
/// # Ok::<(), anyhow::Error>(())
/// ```
pub fn analyze_parquet_with_config(
    file_path: &Path,
    config: &ParquetConfig,
) -> Result<QualityReport> {
    let start = std::time::Instant::now();

    // Open the Parquet file
    let file = File::open(file_path)?;
    let metadata = file.metadata()?;
    let file_size_bytes = metadata.len();
    let file_size_mb = file_size_bytes as f64 / 1_048_576.0;

    // Create Parquet reader using Arrow
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| anyhow::anyhow!("Failed to create Parquet reader: {}", e))?;

    // Extract Parquet metadata before consuming the builder
    let parquet_meta = builder.metadata();
    let file_metadata = parquet_meta.file_metadata();

    // Extract metadata information
    let num_row_groups = parquet_meta.num_row_groups();
    let version = file_metadata.version();

    // Get compression codec from first row group's first column (representative)
    let compression = if num_row_groups > 0 && parquet_meta.row_group(0).num_columns() > 0 {
        format!("{:?}", parquet_meta.row_group(0).column(0).compression())
    } else {
        "UNKNOWN".to_string()
    };

    // Calculate compressed size (sum of all row group compressed sizes)
    let compressed_size_bytes: u64 = (0..num_row_groups)
        .map(|i| parquet_meta.row_group(i).compressed_size() as u64)
        .sum();

    // Get schema information
    let schema = builder.schema();
    let schema_summary = format!("{}", schema);

    // Build the reader with configured batch size
    let reader = builder
        .with_batch_size(config.batch_size)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build Parquet reader: {}", e))?;

    // Use RecordBatchAnalyzer to process batches
    let mut analyzer = RecordBatchAnalyzer::new();

    // Process all record batches
    for batch_result in reader {
        let batch =
            batch_result.map_err(|e| anyhow::anyhow!("Failed to read Parquet batch: {}", e))?;
        analyzer.process_batch(&batch)?;
    }

    // Convert to column profiles
    let column_profiles = analyzer.to_profiles();
    let total_rows = analyzer.total_rows();

    // Calculate comprehensive quality metrics using ISO 8000/25012 standards
    let sample_columns = analyzer.create_sample_columns();
    let data_quality_metrics =
        DataQualityMetrics::calculate_from_data(&sample_columns, &column_profiles).map_err(
            |e| anyhow::anyhow!("Quality metrics calculation failed for Parquet data: {}", e),
        )?;

    let scan_time_ms = start.elapsed().as_millis();

    // Create Parquet metadata
    let parquet_metadata = Some(ParquetMetadata {
        num_row_groups,
        compression,
        version,
        schema_summary,
        compressed_size_bytes,
        uncompressed_size_bytes: None, // Not readily available without decompressing
    });

    Ok(QualityReport {
        file_info: FileInfo {
            path: file_path.display().to_string(),
            total_rows: Some(total_rows),
            total_columns: column_profiles.len(),
            file_size_mb,
            parquet_metadata,
        },
        column_profiles,
        scan_info: ScanInfo {
            rows_scanned: total_rows,
            sampling_ratio: 1.0, // Parquet processes all data efficiently
            scan_time_ms,
        },
        data_quality_metrics,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    #[test]
    fn test_analyze_parquet_basic() -> Result<()> {
        // Create a test Parquet file
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        // Define schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
            Field::new("salary", DataType::Float64, false),
        ]));

        // Create data
        let names = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let ages = Int32Array::from(vec![25, 30, 35]);
        let salaries = Float64Array::from(vec![50000.0, 60000.0, 70000.0]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(names), Arc::new(ages), Arc::new(salaries)],
        )?;

        // Write to Parquet
        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        // Test analysis
        let report = analyze_parquet_with_quality(path)?;

        assert_eq!(report.column_profiles.len(), 3);
        assert_eq!(report.file_info.total_rows, Some(3));
        assert_eq!(report.file_info.total_columns, 3);
        assert_eq!(report.scan_info.rows_scanned, 3);
        assert_eq!(report.scan_info.sampling_ratio, 1.0);

        // Verify column names
        let column_names: Vec<_> = report
            .column_profiles
            .iter()
            .map(|p| p.name.as_str())
            .collect();
        assert!(column_names.contains(&"name"));
        assert!(column_names.contains(&"age"));
        assert!(column_names.contains(&"salary"));

        // Verify age column statistics
        let age_profile = report
            .column_profiles
            .iter()
            .find(|p| p.name == "age")
            .expect("Age column should exist");
        assert_eq!(age_profile.total_count, 3);
        assert_eq!(age_profile.null_count, 0);

        Ok(())
    }

    #[test]
    fn test_analyze_parquet_with_nulls() -> Result<()> {
        // Create test file with nullable fields
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("optional_field", DataType::Utf8, true),
        ]));

        let ids = Int32Array::from(vec![1, 2, 3]);
        let optional = StringArray::from(vec![Some("value1"), None, Some("value3")]);

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(optional)])?;

        // Write Parquet
        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        // Analyze
        let report = analyze_parquet_with_quality(path)?;

        let optional_profile = report
            .column_profiles
            .iter()
            .find(|p| p.name == "optional_field")
            .expect("Optional field should exist");

        assert_eq!(optional_profile.total_count, 3);
        assert_eq!(optional_profile.null_count, 1);

        Ok(())
    }

    #[test]
    fn test_analyze_parquet_quality_metrics() -> Result<()> {
        // Create test file
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        let schema = Arc::new(Schema::new(vec![
            Field::new("complete", DataType::Int32, false),
            Field::new("incomplete", DataType::Int32, true),
        ]));

        let complete = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let incomplete = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(complete), Arc::new(incomplete)],
        )?;

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        // Analyze and check quality metrics
        let report = analyze_parquet_with_quality(path)?;

        // Should have quality metrics calculated
        assert!(report.data_quality_metrics.complete_records_ratio >= 0.0);
        assert!(report.data_quality_metrics.complete_records_ratio <= 100.0);

        // Quality score should be reasonable
        let quality_score = report.quality_score();
        assert!((0.0..=100.0).contains(&quality_score));

        Ok(())
    }

    #[test]
    fn test_analyze_parquet_empty_file() {
        // Test with non-existent file
        let result = analyze_parquet_with_quality(Path::new("nonexistent.parquet"));
        assert!(result.is_err());
    }

    #[test]
    fn test_is_parquet_file_detection() -> Result<()> {
        // Create a valid Parquet file
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "test",
            DataType::Int32,
            false,
        )]));
        let data = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(data)])?;

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        // Should detect it as Parquet
        assert!(is_parquet_file(path));

        Ok(())
    }

    #[test]
    fn test_is_parquet_file_false_positives() -> Result<()> {
        use std::io::Write;

        // Test with a non-Parquet file
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age")?;
        writeln!(temp_file, "Alice,25")?;
        temp_file.flush()?;

        assert!(!is_parquet_file(temp_file.path()));

        // Test with empty file
        let empty_file = NamedTempFile::new()?;
        assert!(!is_parquet_file(empty_file.path()));

        // Test with file that has PAR1 only at start
        let mut fake_file = NamedTempFile::new()?;
        fake_file.write_all(b"PAR1")?;
        fake_file.write_all(b"some other data")?;
        fake_file.flush()?;
        assert!(!is_parquet_file(fake_file.path()));

        Ok(())
    }

    #[test]
    fn test_is_parquet_file_nonexistent() {
        assert!(!is_parquet_file(Path::new("nonexistent_file.parquet")));
    }
}
