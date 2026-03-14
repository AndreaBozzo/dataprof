//! Partial analysis APIs (#226)
//!
//! Fast, lightweight alternatives to full profiling:
//! - [`infer_schema`] — quick schema detection (column names + types)
//! - [`quick_row_count`] — fast row counting / estimation

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Instant;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::core::errors::DataProfilerError;
use crate::core::profile_builder;
use crate::core::streaming_stats::StreamingColumnCollection;
use crate::parsers::csv::CsvParserConfig;
use crate::parsers::json::JsonParserConfig;
use crate::types::{
    ColumnSchema, CountMethod, DataType, FileFormat, RowCountEstimate, SchemaResult,
};

use super::Profiler;

/// Schema inference sample size (rows to read for CSV/JSON).
const SCHEMA_SAMPLE_ROWS: usize = 1000;

/// File size threshold for full-scan vs sampling row count (10 MB).
const FULL_SCAN_THRESHOLD: u64 = 10 * 1024 * 1024;

/// Number of lines to sample for row estimation.
const ROW_SAMPLE_LINES: usize = 10_000;

// ---------------------------------------------------------------------------
// Public free functions
// ---------------------------------------------------------------------------

/// Infer the schema (column names + data types) of a file.
///
/// This is much faster than a full `Profiler::analyze_file` because it reads
/// only a small sample of rows (or just the metadata for Parquet files).
///
/// # Example
/// ```no_run
/// let schema = dataprof::infer_schema("data.csv").unwrap();
/// for col in &schema.columns {
///     println!("{}: {:?}", col.name, col.data_type);
/// }
/// ```
pub fn infer_schema<P: AsRef<Path>>(path: P) -> Result<SchemaResult, DataProfilerError> {
    let path = path.as_ref();
    let format = Profiler::detect_format(path);
    infer_schema_with_format(path, format)
}

/// Quick row count (exact or estimated) for a file.
///
/// Returns an exact count for small files and Parquet; an estimate for large
/// CSV/JSON files via sampling.
///
/// # Example
/// ```no_run
/// let est = dataprof::quick_row_count("data.csv").unwrap();
/// println!("{} rows (exact={})", est.count, est.exact);
/// ```
pub fn quick_row_count<P: AsRef<Path>>(path: P) -> Result<RowCountEstimate, DataProfilerError> {
    let path = path.as_ref();
    let format = Profiler::detect_format(path);
    quick_row_count_with_format(path, format)
}

// ---------------------------------------------------------------------------
// Format-aware internals (also used by Profiler methods)
// ---------------------------------------------------------------------------

pub(crate) fn infer_schema_with_format(
    path: &Path,
    format: FileFormat,
) -> Result<SchemaResult, DataProfilerError> {
    let start = Instant::now();

    match format {
        FileFormat::Parquet => infer_schema_parquet(path, start),
        FileFormat::Csv => infer_schema_csv(path, start),
        FileFormat::Json | FileFormat::Jsonl => infer_schema_json(path, start),
        FileFormat::Unknown(ref ext) => Err(DataProfilerError::UnsupportedFormat {
            format: ext.clone(),
        }),
    }
}

pub(crate) fn quick_row_count_with_format(
    path: &Path,
    format: FileFormat,
) -> Result<RowCountEstimate, DataProfilerError> {
    let start = Instant::now();

    match format {
        FileFormat::Parquet => count_parquet(path, start),
        FileFormat::Csv => count_csv(path, start),
        FileFormat::Json | FileFormat::Jsonl => count_json(path, format, start),
        FileFormat::Unknown(ref ext) => Err(DataProfilerError::UnsupportedFormat {
            format: ext.clone(),
        }),
    }
}

// ---------------------------------------------------------------------------
// Schema inference — per format
// ---------------------------------------------------------------------------

fn infer_schema_parquet(path: &Path, start: Instant) -> Result<SchemaResult, DataProfilerError> {
    let file = fs::File::open(path).map_err(|_| DataProfilerError::FileNotFound {
        path: path.display().to_string(),
    })?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        DataProfilerError::ParquetError {
            message: format!("Failed to read Parquet metadata: {}", e),
        }
    })?;

    let arrow_schema = builder.schema();
    let columns = arrow_schema
        .fields()
        .iter()
        .map(|field| ColumnSchema {
            name: field.name().clone(),
            data_type: arrow_type_to_dataprof(field.data_type()),
        })
        .collect();

    Ok(SchemaResult {
        columns,
        rows_sampled: 0,
        inference_time_ms: start.elapsed().as_millis(),
        schema_stable: true,
    })
}

fn infer_schema_csv(path: &Path, start: Instant) -> Result<SchemaResult, DataProfilerError> {
    let file = fs::File::open(path).map_err(|_| DataProfilerError::FileNotFound {
        path: path.display().to_string(),
    })?;

    let config = CsvParserConfig::default().max_rows(Some(SCHEMA_SAMPLE_ROWS));
    let (_profiles, column_stats, rows_read, headers) =
        crate::parsers::csv::analyze_csv_from_reader(file, &config)?;

    Ok(schema_from_streaming_stats(
        &column_stats,
        &headers,
        rows_read,
        start.elapsed().as_millis(),
    ))
}

fn infer_schema_json(path: &Path, start: Instant) -> Result<SchemaResult, DataProfilerError> {
    let file = fs::File::open(path).map_err(|_| DataProfilerError::FileNotFound {
        path: path.display().to_string(),
    })?;
    let reader = BufReader::new(file);

    let config = JsonParserConfig::default().with_max_rows(SCHEMA_SAMPLE_ROWS);
    let (_profiles, column_stats, rows_read, _format) =
        crate::parsers::json::analyze_json_from_reader(reader, &config)?;

    // JSON parser doesn't return headers in insertion order via a separate vec,
    // but column_stats.column_names() gives us the keys. For JSON the "header"
    // order is discovery order which column_names() preserves (HashMap iteration
    // order is arbitrary, but we use what we have).
    let names = column_stats.column_names();

    Ok(schema_from_streaming_stats(
        &column_stats,
        &names,
        rows_read,
        start.elapsed().as_millis(),
    ))
}

// ---------------------------------------------------------------------------
// Row count — per format
// ---------------------------------------------------------------------------

fn count_parquet(path: &Path, start: Instant) -> Result<RowCountEstimate, DataProfilerError> {
    let file = fs::File::open(path).map_err(|_| DataProfilerError::FileNotFound {
        path: path.display().to_string(),
    })?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        DataProfilerError::ParquetError {
            message: format!("Failed to read Parquet metadata: {}", e),
        }
    })?;

    let count = builder.metadata().file_metadata().num_rows() as u64;

    Ok(RowCountEstimate {
        count,
        exact: true,
        method: CountMethod::ParquetMetadata,
        count_time_ms: start.elapsed().as_millis(),
    })
}

fn count_csv(path: &Path, start: Instant) -> Result<RowCountEstimate, DataProfilerError> {
    let metadata = fs::metadata(path).map_err(|_| DataProfilerError::FileNotFound {
        path: path.display().to_string(),
    })?;
    let file_size = metadata.len();

    let file = fs::File::open(path).map_err(|_| DataProfilerError::FileNotFound {
        path: path.display().to_string(),
    })?;
    let reader = BufReader::new(file);

    if file_size < FULL_SCAN_THRESHOLD {
        // Full scan — exact count (subtract 1 for header)
        let line_count = reader.lines().count();
        let count = if line_count > 0 { line_count - 1 } else { 0 };

        Ok(RowCountEstimate {
            count: count as u64,
            exact: true,
            method: CountMethod::FullScan,
            count_time_ms: start.elapsed().as_millis(),
        })
    } else {
        // Sampling — read first N lines, estimate from avg line length
        let mut bytes_read: u64 = 0;
        let mut lines_read: u64 = 0;

        for line in reader.lines().take(ROW_SAMPLE_LINES) {
            let line = line.map_err(|e| DataProfilerError::io_error(&e))?;
            bytes_read += line.len() as u64 + 1; // +1 for newline
            lines_read += 1;
        }

        if lines_read == 0 {
            return Ok(RowCountEstimate {
                count: 0,
                exact: true,
                method: CountMethod::FullScan,
                count_time_ms: start.elapsed().as_millis(),
            });
        }

        let avg_bytes_per_line = bytes_read as f64 / lines_read as f64;
        let estimated_total_lines = (file_size as f64 / avg_bytes_per_line) as u64;
        // Subtract 1 for header
        let count = if estimated_total_lines > 0 {
            estimated_total_lines - 1
        } else {
            0
        };

        Ok(RowCountEstimate {
            count,
            exact: false,
            method: CountMethod::Sampling,
            count_time_ms: start.elapsed().as_millis(),
        })
    }
}

fn count_json(
    path: &Path,
    format: FileFormat,
    start: Instant,
) -> Result<RowCountEstimate, DataProfilerError> {
    let metadata = fs::metadata(path).map_err(|_| DataProfilerError::FileNotFound {
        path: path.display().to_string(),
    })?;
    let file_size = metadata.len();

    let file = fs::File::open(path).map_err(|_| DataProfilerError::FileNotFound {
        path: path.display().to_string(),
    })?;

    match format {
        FileFormat::Jsonl => {
            let reader = BufReader::new(file);

            if file_size < FULL_SCAN_THRESHOLD {
                // Full scan — count non-empty lines
                let count = reader
                    .lines()
                    .map_while(Result::ok)
                    .filter(|l| !l.trim().is_empty())
                    .count() as u64;

                Ok(RowCountEstimate {
                    count,
                    exact: true,
                    method: CountMethod::FullScan,
                    count_time_ms: start.elapsed().as_millis(),
                })
            } else {
                // Sampling — estimate from first N lines
                let reader = BufReader::new(fs::File::open(path).map_err(|_| {
                    DataProfilerError::FileNotFound {
                        path: path.display().to_string(),
                    }
                })?);
                let mut bytes_read: u64 = 0;
                let mut lines_read: u64 = 0;

                for line in reader.lines().take(ROW_SAMPLE_LINES) {
                    let line = line.map_err(|e| DataProfilerError::io_error(&e))?;
                    if !line.trim().is_empty() {
                        bytes_read += line.len() as u64 + 1;
                        lines_read += 1;
                    }
                }

                if lines_read == 0 {
                    return Ok(RowCountEstimate {
                        count: 0,
                        exact: true,
                        method: CountMethod::FullScan,
                        count_time_ms: start.elapsed().as_millis(),
                    });
                }

                let avg_bytes_per_line = bytes_read as f64 / lines_read as f64;
                let count = (file_size as f64 / avg_bytes_per_line) as u64;

                Ok(RowCountEstimate {
                    count,
                    exact: false,
                    method: CountMethod::Sampling,
                    count_time_ms: start.elapsed().as_millis(),
                })
            }
        }
        _ => {
            // JSON array — full parse via serde streaming deserializer
            let reader = BufReader::new(file);
            let deserializer = serde_json::Deserializer::from_reader(reader);
            let mut count: u64 = 0;

            // Use StreamDeserializer to count top-level values in the array
            // For a JSON array, we parse the outer structure
            use serde_json::Value;
            let value: Value =
                serde_json::from_reader(BufReader::new(fs::File::open(path).map_err(|_| {
                    DataProfilerError::FileNotFound {
                        path: path.display().to_string(),
                    }
                })?))
                .map_err(|e| DataProfilerError::JsonParsingError {
                    message: format!("Failed to parse JSON: {}", e),
                })?;

            drop(deserializer); // unused, we use from_reader instead

            if let Value::Array(arr) = value {
                count = arr.len() as u64;
            }

            Ok(RowCountEstimate {
                count,
                exact: true,
                method: CountMethod::FullScan,
                count_time_ms: start.elapsed().as_millis(),
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn schema_from_streaming_stats(
    column_stats: &StreamingColumnCollection,
    headers: &[String],
    rows_sampled: usize,
    elapsed_ms: u128,
) -> SchemaResult {
    let columns = headers
        .iter()
        .filter_map(|name| {
            column_stats
                .get_column_stats(name)
                .map(|stats| ColumnSchema {
                    name: name.clone(),
                    data_type: profile_builder::infer_data_type_streaming(stats),
                })
        })
        .collect();

    SchemaResult {
        columns,
        rows_sampled,
        inference_time_ms: elapsed_ms,
        schema_stable: true,
    }
}

/// Map Arrow data types to dataprof's simplified 4-variant DataType.
fn arrow_type_to_dataprof(arrow_type: &arrow::datatypes::DataType) -> DataType {
    use arrow::datatypes::DataType as AT;
    match arrow_type {
        AT::Int8
        | AT::Int16
        | AT::Int32
        | AT::Int64
        | AT::UInt8
        | AT::UInt16
        | AT::UInt32
        | AT::UInt64 => DataType::Integer,

        AT::Float16 | AT::Float32 | AT::Float64 | AT::Decimal128(_, _) | AT::Decimal256(_, _) => {
            DataType::Float
        }

        AT::Date32 | AT::Date64 | AT::Timestamp(_, _) | AT::Time32(_) | AT::Time64(_) => {
            DataType::Date
        }

        // Everything else (Utf8, LargeUtf8, Boolean, Binary, List, Struct, etc.)
        _ => DataType::String,
    }
}

// ---------------------------------------------------------------------------
// Async wrappers (feature: async-streaming)
// ---------------------------------------------------------------------------

#[cfg(feature = "async-streaming")]
pub async fn infer_schema_async<P: AsRef<Path> + Send + 'static>(
    path: P,
) -> Result<SchemaResult, DataProfilerError> {
    tokio::task::spawn_blocking(move || infer_schema(path))
        .await
        .map_err(|e| DataProfilerError::StreamingError {
            message: format!("Schema inference task failed: {}", e),
        })?
}

#[cfg(feature = "async-streaming")]
pub async fn quick_row_count_async<P: AsRef<Path> + Send + 'static>(
    path: P,
) -> Result<RowCountEstimate, DataProfilerError> {
    tokio::task::spawn_blocking(move || quick_row_count(path))
        .await
        .map_err(|e| DataProfilerError::StreamingError {
            message: format!("Row count task failed: {}", e),
        })?
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_temp_csv(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::with_suffix(".csv").unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    fn write_temp_jsonl(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::with_suffix(".jsonl").unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    fn write_temp_json(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::with_suffix(".json").unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn test_infer_schema_csv() {
        let f = write_temp_csv("name,age,salary\nAlice,30,50000.5\nBob,25,60000.0\n");
        let result = infer_schema(f.path()).unwrap();

        assert_eq!(result.columns.len(), 3);
        assert_eq!(result.columns[0].name, "name");
        assert_eq!(result.columns[0].data_type, DataType::String);
        assert_eq!(result.columns[1].name, "age");
        assert_eq!(result.columns[1].data_type, DataType::Integer);
        assert_eq!(result.columns[2].name, "salary");
        assert_eq!(result.columns[2].data_type, DataType::Float);
        assert!(result.rows_sampled > 0);
    }

    #[test]
    fn test_infer_schema_csv_with_dates() {
        let f =
            write_temp_csv("id,hired\n1,2023-01-15\n2,2023-02-20\n3,2023-03-25\n4,2023-04-10\n");
        let result = infer_schema(f.path()).unwrap();

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].data_type, DataType::Integer);
        assert_eq!(result.columns[1].data_type, DataType::Date);
    }

    #[test]
    fn test_infer_schema_jsonl() {
        let f = write_temp_jsonl(
            r#"{"name":"Alice","age":30}
{"name":"Bob","age":25}
"#,
        );
        let result = infer_schema(f.path()).unwrap();

        assert_eq!(result.columns.len(), 2);
        // JSON column order may vary — check by name
        let names: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"name"));
        assert!(names.contains(&"age"));
    }

    #[test]
    fn test_infer_schema_json_array() {
        let f = write_temp_json(r#"[{"x":1,"y":"hello"},{"x":2,"y":"world"}]"#);
        let result = infer_schema(f.path()).unwrap();

        assert_eq!(result.columns.len(), 2);
        let names: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"x"));
        assert!(names.contains(&"y"));
    }

    #[test]
    fn test_infer_schema_parquet() {
        use arrow::array::{Float64Array, Int32Array, StringArray};
        use arrow::datatypes::{DataType as ArrowDT, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;

        let schema = Schema::new(vec![
            Field::new("id", ArrowDT::Int32, false),
            Field::new("score", ArrowDT::Float64, false),
            Field::new("label", ArrowDT::Utf8, false),
        ]);

        let batch = RecordBatch::try_new(
            std::sync::Arc::new(schema),
            vec![
                std::sync::Arc::new(Int32Array::from(vec![1, 2])),
                std::sync::Arc::new(Float64Array::from(vec![1.5, 2.5])),
                std::sync::Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        let mut f = NamedTempFile::with_suffix(".parquet").unwrap();
        {
            let mut writer = ArrowWriter::try_new(&mut f, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let result = infer_schema(f.path()).unwrap();
        assert_eq!(result.rows_sampled, 0); // Parquet reads metadata only
        assert_eq!(result.columns.len(), 3);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].data_type, DataType::Integer);
        assert_eq!(result.columns[1].name, "score");
        assert_eq!(result.columns[1].data_type, DataType::Float);
        assert_eq!(result.columns[2].name, "label");
        assert_eq!(result.columns[2].data_type, DataType::String);
    }

    #[test]
    fn test_quick_row_count_csv_small() {
        let f = write_temp_csv("a,b\n1,2\n3,4\n5,6\n");
        let result = quick_row_count(f.path()).unwrap();

        assert_eq!(result.count, 3);
        assert!(result.exact);
        assert_eq!(result.method, CountMethod::FullScan);
    }

    #[test]
    fn test_quick_row_count_jsonl() {
        let f = write_temp_jsonl("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        let result = quick_row_count(f.path()).unwrap();

        assert_eq!(result.count, 3);
        assert!(result.exact);
        assert_eq!(result.method, CountMethod::FullScan);
    }

    #[test]
    fn test_quick_row_count_json_array() {
        let f = write_temp_json(r#"[{"a":1},{"a":2},{"a":3}]"#);
        let result = quick_row_count(f.path()).unwrap();

        assert_eq!(result.count, 3);
        assert!(result.exact);
    }

    #[test]
    fn test_quick_row_count_parquet() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType as ArrowDT, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;

        let schema = Schema::new(vec![Field::new("id", ArrowDT::Int32, false)]);
        let batch = RecordBatch::try_new(
            std::sync::Arc::new(schema),
            vec![std::sync::Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();

        let mut f = NamedTempFile::with_suffix(".parquet").unwrap();
        {
            let mut writer = ArrowWriter::try_new(&mut f, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let result = quick_row_count(f.path()).unwrap();
        assert_eq!(result.count, 5);
        assert!(result.exact);
        assert_eq!(result.method, CountMethod::ParquetMetadata);
    }

    #[test]
    fn test_quick_row_count_empty_csv() {
        let f = write_temp_csv("a,b\n");
        let result = quick_row_count(f.path()).unwrap();
        assert_eq!(result.count, 0);
    }

    #[test]
    fn test_arrow_type_mapping() {
        use arrow::datatypes::DataType as AT;

        assert_eq!(arrow_type_to_dataprof(&AT::Int32), DataType::Integer);
        assert_eq!(arrow_type_to_dataprof(&AT::UInt64), DataType::Integer);
        assert_eq!(arrow_type_to_dataprof(&AT::Float64), DataType::Float);
        assert_eq!(
            arrow_type_to_dataprof(&AT::Decimal128(10, 2)),
            DataType::Float
        );
        assert_eq!(arrow_type_to_dataprof(&AT::Date32), DataType::Date);
        assert_eq!(
            arrow_type_to_dataprof(&AT::Timestamp(
                arrow::datatypes::TimeUnit::Millisecond,
                None
            )),
            DataType::Date
        );
        assert_eq!(arrow_type_to_dataprof(&AT::Utf8), DataType::String);
        assert_eq!(arrow_type_to_dataprof(&AT::Boolean), DataType::String);
    }
}
