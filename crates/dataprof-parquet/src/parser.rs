use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::ChunkReader;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::record_batch_analyzer::RecordBatchAnalyzer;
use dataprof_core::{
    AnalysisOptions, DataProfilerError, DataSource, ExecutionMetadata, FileFormat, ParquetMetadata,
    QualityDimension, SemanticHints, TruncationReason,
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
    /// Stop after this many rows. `None` reads the whole file.
    pub max_rows: Option<usize>,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            max_rows: None,
        }
    }
}

impl ParquetConfig {
    pub fn batch_size(batch_size: usize) -> Self {
        Self {
            batch_size,
            ..Default::default()
        }
    }

    /// Cap the number of rows read from the file.
    pub fn with_max_rows(mut self, max_rows: usize) -> Self {
        self.max_rows = Some(max_rows);
        self
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

/// Profile Parquet bytes held in memory, without touching the filesystem.
///
/// Reads through the same Arrow reader as [`analyze_parquet_with_config_dims_and_hints`],
/// so a buffer and the file holding those same bytes profile identically. `name`
/// labels the source in the report, since an in-memory buffer has no path.
pub fn analyze_parquet_bytes(
    data: Bytes,
    name: &str,
    config: &ParquetConfig,
    quality_dimensions: Option<&[QualityDimension]>,
    semantic_hints: &SemanticHints,
) -> Result<ProfileReport, DataProfilerError> {
    let options = options_from_dims_and_hints(quality_dimensions, semantic_hints);
    analyze_parquet_bytes_with_options(data, name, config, &options)
}

/// Like [`analyze_parquet_bytes`], honouring the caller's full analysis selection.
pub fn analyze_parquet_bytes_with_options(
    data: Bytes,
    name: &str,
    config: &ParquetConfig,
    options: &AnalysisOptions,
) -> Result<ProfileReport, DataProfilerError> {
    let byte_len = data.len() as u64;
    analyze_parquet_chunks(
        data,
        ParquetOrigin::Memory {
            name: name.to_string(),
            byte_len,
        },
        config,
        options,
    )
}

/// Widen the legacy dimensions-and-hints parameter pair into the full selection.
fn options_from_dims_and_hints(
    quality_dimensions: Option<&[QualityDimension]>,
    semantic_hints: &SemanticHints,
) -> AnalysisOptions {
    AnalysisOptions::default()
        .with_quality_dimensions(quality_dimensions.map(<[_]>::to_vec))
        .with_semantic_hints(semantic_hints.clone())
}

/// Where the Parquet bytes came from. The reader and every statistic are shared;
/// only the report's [`DataSource`] differs.
enum ParquetOrigin {
    File { path: String, size_bytes: u64 },
    Memory { name: String, byte_len: u64 },
}

pub fn analyze_parquet_with_config_dims_and_hints(
    file_path: &Path,
    config: &ParquetConfig,
    quality_dimensions: Option<&[QualityDimension]>,
    semantic_hints: &SemanticHints,
) -> Result<ProfileReport, DataProfilerError> {
    let options = options_from_dims_and_hints(quality_dimensions, semantic_hints);
    analyze_parquet_with_options(file_path, config, &options)
}

/// Analyze a Parquet file, honouring the caller's full analysis selection.
///
/// This is the entry point that carries metric packs and locale as well as
/// dimensions and hints, so a Parquet profile reports exactly the analysis the
/// caller asked for — the same selection the CSV engines apply.
pub fn analyze_parquet_with_options(
    file_path: &Path,
    config: &ParquetConfig,
    options: &AnalysisOptions,
) -> Result<ProfileReport, DataProfilerError> {
    let file = File::open(file_path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            DataProfilerError::FileNotFound {
                path: file_path.display().to_string(),
            }
        } else {
            DataProfilerError::from(error)
        }
    })?;
    let file_size_bytes = file.metadata()?.len();

    analyze_parquet_chunks(
        file,
        ParquetOrigin::File {
            path: file_path.display().to_string(),
            size_bytes: file_size_bytes,
        },
        config,
        options,
    )
}

fn analyze_parquet_chunks<R: ChunkReader + 'static>(
    chunks: R,
    origin: ParquetOrigin,
    config: &ParquetConfig,
    options: &AnalysisOptions,
) -> Result<ProfileReport, DataProfilerError> {
    let semantic_hints = options.semantic_hints();
    let start = std::time::Instant::now();

    let builder = ParquetRecordBatchReaderBuilder::try_new(chunks).map_err(|error| {
        DataProfilerError::parquet_with_source(
            format!("Failed to create Parquet reader: {}", error),
            error,
        )
    })?;

    let parquet_meta = builder.metadata();
    let file_metadata = parquet_meta.file_metadata();

    let num_row_groups = parquet_meta.num_row_groups();
    let version = file_metadata.version();
    // Parquet knows its exact row count up front, so truncation can be decided on
    // what the file holds rather than inferred from how many rows we read.
    let file_rows = file_metadata.num_rows().max(0) as u64;

    let compression = if num_row_groups > 0 && parquet_meta.row_group(0).num_columns() > 0 {
        format!("{:?}", parquet_meta.row_group(0).column(0).compression())
    } else {
        "UNKNOWN".to_string()
    };

    let compressed_size_bytes: u64 = (0..num_row_groups)
        .map(|index| parquet_meta.row_group(index).compressed_size() as u64)
        .sum();

    let arrow_schema = builder.schema().clone();
    let schema_summary = format!("{arrow_schema}");

    let mut reader_builder = builder.with_batch_size(config.batch_size);
    if let Some(max) = config.max_rows {
        reader_builder = reader_builder.with_limit(max);
    }
    let reader = reader_builder.build().map_err(|error| {
        DataProfilerError::parquet_with_source(
            format!("Failed to build Parquet reader: {}", error),
            error,
        )
    })?;

    let mut analyzer = RecordBatchAnalyzer::new().with_semantic_hints(semantic_hints);
    analyzer.initialize_schema(arrow_schema.as_ref())?;
    for batch_result in reader {
        let batch = batch_result.map_err(|error| {
            DataProfilerError::parquet_with_source(
                format!("Failed to read Parquet batch: {}", error),
                error,
            )
        })?;
        analyzer.process_batch(&batch)?;
    }

    let column_profiles = analyzer.to_profiles_with_hints(
        !options.include_statistics(),
        !options.include_patterns(),
        options.locale(),
        semantic_hints,
    );
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

    let mut execution =
        ExecutionMetadata::new(total_rows, num_columns, scan_time_ms).with_engine("parquet");
    // A cap only truncates when the file actually holds more rows than the cap.
    // A file with exactly `max_rows` rows was read in full, not cut short.
    if let Some(max) = config.max_rows
        && file_rows > max as u64
    {
        execution = execution.with_truncation(TruncationReason::MaxRows(max as u64));
    }

    let data_source = match origin {
        ParquetOrigin::File { path, size_bytes } => DataSource::File {
            path,
            format: FileFormat::Parquet,
            size_bytes,
            modified_at: None,
            parquet_metadata,
        },
        // A buffer has no path, and `DataSource::File` is where Parquet metadata
        // lives, so the in-memory case reports the byte-buffer shape and drops
        // the file-level metadata.
        ParquetOrigin::Memory { name, byte_len } => DataSource::Bytes {
            name,
            format: FileFormat::Parquet,
            size_bytes: byte_len,
        },
    };

    Ok(ReportAssembler::new(data_source, execution)
        .columns(column_profiles)
        .with_row_duplicates(analyzer.row_duplicate_summary())
        .with_row_completeness(analyzer.row_completeness_summary())
        .with_quality_data(sample_columns)
        .with_exact_value_hint_bindings(analyzer.semantic_hint_bindings())
        .with_analysis_options(options)
        .build())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use arrow::array::{BooleanArray, Date32Array, Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    /// Write `batch` to Parquet and return the encoded bytes.
    fn to_parquet_bytes(batch: &RecordBatch) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;
        writer.write(batch)?;
        writer.close()?;
        Ok(buffer)
    }

    /// A batch exercising the shapes whose typing differs between readers:
    /// nullable integers, booleans, dates, non-finite floats, and strings.
    fn mixed_batch() -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("flag", DataType::Boolean, true),
            Field::new("day", DataType::Date32, true),
            Field::new("ratio", DataType::Float64, true),
            Field::new("label", DataType::Utf8, true),
        ]));

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
                Arc::new(Date32Array::from(vec![Some(19000), None, Some(19002)])),
                Arc::new(Float64Array::from(vec![
                    Some(1.5),
                    Some(f64::NAN),
                    Some(f64::INFINITY),
                ])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )?)
    }

    #[test]
    fn test_parquet_bytes_and_file_profile_identically() -> Result<()> {
        let batch = mixed_batch()?;
        let encoded = to_parquet_bytes(&batch)?;

        let temp_file = NamedTempFile::new()?;
        std::fs::write(temp_file.path(), &encoded)?;

        let from_file = analyze_parquet_with_quality(temp_file.path())?;
        let from_bytes = analyze_parquet_bytes(
            Bytes::from(encoded),
            "buffer",
            &ParquetConfig::default(),
            None,
            &SemanticHints::default(),
        )?;

        assert_eq!(
            from_bytes.execution.rows_processed,
            from_file.execution.rows_processed
        );
        // Column order and every per-column statistic must match: the two paths
        // share one reader, so any divergence here is a wiring mistake.
        assert_eq!(
            from_bytes.column_profiles.len(),
            from_file.column_profiles.len()
        );
        for (bytes_profile, file_profile) in from_bytes
            .column_profiles
            .iter()
            .zip(from_file.column_profiles.iter())
        {
            assert_eq!(bytes_profile.name, file_profile.name);
            assert_eq!(
                serde_json::to_value(bytes_profile)?,
                serde_json::to_value(file_profile)?,
                "{}",
                file_profile.name
            );
        }
        assert_eq!(from_bytes.quality_score(), from_file.quality_score());
        Ok(())
    }

    #[test]
    fn test_parquet_bytes_honor_max_rows() -> Result<()> {
        let encoded = to_parquet_bytes(&mixed_batch()?)?;
        let report = analyze_parquet_bytes(
            Bytes::from(encoded),
            "buffer",
            &ParquetConfig::default().with_max_rows(2),
            None,
            &SemanticHints::default(),
        )?;

        assert_eq!(report.execution.rows_processed, 2);
        assert!(report.execution.truncation_reason.is_some());
        Ok(())
    }

    #[test]
    fn test_parquet_bytes_report_the_buffer_length_they_were_given() -> Result<()> {
        let encoded = to_parquet_bytes(&mixed_batch()?)?;
        let byte_len = encoded.len() as u64;
        let report = analyze_parquet_bytes(
            Bytes::from(encoded),
            "buffer",
            &ParquetConfig::default(),
            None,
            &SemanticHints::default(),
        )?;

        // `size_bytes` describes the buffer handed over. Decoded values are a
        // different size entirely -- for Parquet, off by the compression ratio.
        match &report.data_source {
            DataSource::Bytes {
                format, size_bytes, ..
            } => {
                assert_eq!(*size_bytes, byte_len);
                assert_eq!(*format, FileFormat::Parquet);
            }
            other => panic!("expected a byte-buffer source, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_parquet_bytes_reject_a_non_parquet_buffer() {
        let error = analyze_parquet_bytes(
            Bytes::from_static(b"not parquet at all"),
            "buffer",
            &ParquetConfig::default(),
            None,
            &SemanticHints::default(),
        )
        .expect_err("a buffer that is not Parquet must not profile");

        assert!(
            matches!(error, DataProfilerError::ParquetError { .. }),
            "{error}"
        );
    }

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
    fn test_zero_row_parquet_preserves_schema() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        let file = File::create(temp_file.path())?;
        let writer = ArrowWriter::try_new(file, schema, None)?;
        writer.close()?;

        let report = analyze_parquet_with_quality(temp_file.path())?;
        assert_eq!(report.execution.rows_processed, 0);
        assert_eq!(report.execution.columns_detected, 2);
        assert_eq!(
            report
                .column_profiles
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "label"]
        );
        assert!(
            report
                .column_profiles
                .iter()
                .all(|column| column.total_count == 0 && column.null_count == 0)
        );
        assert_eq!(report.quality_score(), None);
        Ok(())
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
