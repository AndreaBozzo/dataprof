use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::record_batch_analyzer::RecordBatchAnalyzer;
use dataprof_core::{
    DataProfilerError, DataSource, ExecutionMetadata, FileFormat, ParquetMetadata,
    QualityDimension, SemanticHints,
};
use dataprof_runtime::{ProfileReport, ReportAssembler};

/// Check if a file is a valid Parquet file by examining its magic number.
pub fn is_parquet_file(file_path: &Path) -> bool {
    let mut file = match File::open(file_path) {
        Ok(file) => file,
        Err(_) => return false,
    };

    let file_size = match file.metadata() {
        Ok(metadata) => metadata.len(),
        Err(_) => return false,
    };

    if file_size < 8 {
        return false;
    }

    let mut header = [0u8; 4];
    if file.read_exact(&mut header).is_err() {
        return false;
    }

    if &header != b"PAR1" {
        return false;
    }

    if file.seek(SeekFrom::End(-4)).is_err() {
        return false;
    }

    let mut footer = [0u8; 4];
    if file.read_exact(&mut footer).is_err() {
        return false;
    }

    &footer == b"PAR1"
}

/// Configuration options for Parquet analysis.
#[derive(Debug, Clone)]
pub struct ParquetConfig {
    pub batch_size: usize,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self { batch_size: 8192 }
    }
}

impl ParquetConfig {
    pub fn batch_size(batch_size: usize) -> Self {
        Self { batch_size }
    }

    pub fn adaptive_batch_size(file_size_bytes: u64) -> usize {
        match file_size_bytes {
            0..=1_048_576 => 1024,
            1_048_577..=10_485_760 => 4096,
            10_485_761..=104_857_600 => 8192,
            104_857_601..=1_073_741_824 => 16384,
            _ => 32768,
        }
    }
}

pub fn analyze_parquet_with_quality(file_path: &Path) -> Result<ProfileReport, DataProfilerError> {
    analyze_parquet_with_quality_dims(file_path, None)
}

pub fn analyze_parquet_with_quality_dims(
    file_path: &Path,
    quality_dimensions: Option<&[QualityDimension]>,
) -> Result<ProfileReport, DataProfilerError> {
    analyze_parquet_with_config_dims(file_path, &ParquetConfig::default(), quality_dimensions)
}

pub fn analyze_parquet_with_quality_dims_and_hints(
    file_path: &Path,
    quality_dimensions: Option<&[QualityDimension]>,
    semantic_hints: &SemanticHints,
) -> Result<ProfileReport, DataProfilerError> {
    analyze_parquet_with_config_dims_and_hints(
        file_path,
        &ParquetConfig::default(),
        quality_dimensions,
        semantic_hints,
    )
}

pub fn analyze_parquet_with_config(
    file_path: &Path,
    config: &ParquetConfig,
) -> Result<ProfileReport, DataProfilerError> {
    analyze_parquet_with_config_dims(file_path, config, None)
}

pub fn analyze_parquet_with_config_dims(
    file_path: &Path,
    config: &ParquetConfig,
    quality_dimensions: Option<&[QualityDimension]>,
) -> Result<ProfileReport, DataProfilerError> {
    analyze_parquet_with_config_dims_and_hints(
        file_path,
        config,
        quality_dimensions,
        &SemanticHints::default(),
    )
}

pub fn analyze_parquet_with_config_dims_and_hints(
    file_path: &Path,
    config: &ParquetConfig,
    quality_dimensions: Option<&[QualityDimension]>,
    semantic_hints: &SemanticHints,
) -> Result<ProfileReport, DataProfilerError> {
    let start = std::time::Instant::now();

    let file = File::open(file_path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            DataProfilerError::FileNotFound {
                path: file_path.display().to_string(),
            }
        } else {
            DataProfilerError::from(error)
        }
    })?;
    let metadata = file.metadata()?;
    let file_size_bytes = metadata.len();

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
        DataProfilerError::ParquetError {
            message: format!("Failed to create Parquet reader: {}", error),
        }
    })?;

    let parquet_meta = builder.metadata();
    let file_metadata = parquet_meta.file_metadata();

    let num_row_groups = parquet_meta.num_row_groups();
    let version = file_metadata.version();

    let compression = if num_row_groups > 0 && parquet_meta.row_group(0).num_columns() > 0 {
        format!("{:?}", parquet_meta.row_group(0).column(0).compression())
    } else {
        "UNKNOWN".to_string()
    };

    let compressed_size_bytes: u64 = (0..num_row_groups)
        .map(|index| parquet_meta.row_group(index).compressed_size() as u64)
        .sum();

    let schema_summary = format!("{}", builder.schema());

    let reader = builder
        .with_batch_size(config.batch_size)
        .build()
        .map_err(|error| DataProfilerError::ParquetError {
            message: format!("Failed to build Parquet reader: {}", error),
        })?;

    let mut analyzer = RecordBatchAnalyzer::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|error| DataProfilerError::ParquetError {
            message: format!("Failed to read Parquet batch: {}", error),
        })?;
        analyzer.process_batch(&batch)?;
    }

    let column_profiles = analyzer.to_profiles_with_hints(false, false, None, semantic_hints);
    let total_rows = analyzer.total_rows();
    let sample_columns = analyzer.create_sample_columns();
    let scan_time_ms = start.elapsed().as_millis();

    let parquet_metadata = Some(ParquetMetadata {
        num_row_groups,
        compression,
        version,
        schema_summary,
        compressed_size_bytes,
        uncompressed_size_bytes: None,
    });

    let num_columns = column_profiles.len();

    let mut assembler = ReportAssembler::new(
        DataSource::File {
            path: file_path.display().to_string(),
            format: FileFormat::Parquet,
            size_bytes: file_size_bytes,
            modified_at: None,
            parquet_metadata,
        },
        ExecutionMetadata::new(total_rows, num_columns, scan_time_ms),
    )
    .columns(column_profiles)
    .with_quality_data(sample_columns)
    .with_semantic_hints(semantic_hints.clone());
    if let Some(dimensions) = quality_dimensions {
        assembler = assembler.with_requested_dimensions(dimensions.to_vec());
    }
    Ok(assembler.build())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    #[test]
    fn test_analyze_parquet_basic() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
            Field::new("salary", DataType::Float64, false),
        ]));

        let names = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let ages = Int32Array::from(vec![25, 30, 35]);
        let salaries = Float64Array::from(vec![50000.0, 60000.0, 70000.0]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(names), Arc::new(ages), Arc::new(salaries)],
        )?;

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        let report = analyze_parquet_with_quality(path)?;

        assert_eq!(report.column_profiles.len(), 3);
        assert_eq!(report.execution.rows_processed, 3);
        assert_eq!(report.execution.columns_detected, 3);
        assert!(!report.execution.sampling_applied);

        let column_names: Vec<_> = report
            .column_profiles
            .iter()
            .map(|profile| profile.name.as_str())
            .collect();
        assert!(column_names.contains(&"name"));
        assert!(column_names.contains(&"age"));
        assert!(column_names.contains(&"salary"));

        let age_profile = report
            .column_profiles
            .iter()
            .find(|profile| profile.name == "age")
            .expect("Age column should exist");
        assert_eq!(age_profile.total_count, 3);
        assert_eq!(age_profile.null_count, 0);

        Ok(())
    }

    #[test]
    fn test_analyze_parquet_with_nulls() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("optional_field", DataType::Utf8, true),
        ]));

        let ids = Int32Array::from(vec![1, 2, 3]);
        let optional = StringArray::from(vec![Some("value1"), None, Some("value3")]);

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(optional)])?;

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        let report = analyze_parquet_with_quality(path)?;

        let optional_profile = report
            .column_profiles
            .iter()
            .find(|profile| profile.name == "optional_field")
            .expect("Optional field should exist");

        assert_eq!(optional_profile.total_count, 3);
        assert_eq!(optional_profile.null_count, 1);

        Ok(())
    }

    #[test]
    fn test_analyze_parquet_quality_metrics() -> Result<()> {
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

        let report = analyze_parquet_with_quality(path)?;

        let quality = report.quality.as_ref().expect("Quality should be present");
        assert!(quality.metrics.complete_records_ratio() >= 0.0);
        assert!(quality.metrics.complete_records_ratio() <= 100.0);

        let quality_score = report.quality_score().unwrap();
        assert!((0.0..=100.0).contains(&quality_score));

        Ok(())
    }

    #[test]
    fn test_analyze_parquet_empty_file() {
        let result = analyze_parquet_with_quality(Path::new("nonexistent.parquet"));
        assert!(result.is_err());
    }

    #[test]
    fn test_is_parquet_file_detection() -> Result<()> {
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

        assert!(is_parquet_file(path));

        Ok(())
    }

    #[test]
    fn test_is_parquet_file_false_positives() -> Result<()> {
        use std::io::Write;

        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age")?;
        writeln!(temp_file, "Alice,25")?;
        temp_file.flush()?;

        assert!(!is_parquet_file(temp_file.path()));

        let empty_file = NamedTempFile::new()?;
        assert!(!is_parquet_file(empty_file.path()));

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
