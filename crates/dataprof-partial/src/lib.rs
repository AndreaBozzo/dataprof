//! Partial analysis APIs (#226)
//!
//! Fast, lightweight alternatives to full profiling:
//! - [`infer_schema`] — quick schema detection (column names + types)
//! - [`quick_row_count`] — fast row counting / estimation

use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::time::Instant;

#[cfg(feature = "parquet")]
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Deserialize;

#[cfg(feature = "parquet")]
use dataprof_core::DataType;
use dataprof_core::{
    ColumnSchema, CountMethod, DataProfilerError, FileFormat, RowCountEstimate, SchemaResult,
};
use dataprof_csv::CsvParserConfig;
use dataprof_json::JsonParserConfig;
use dataprof_runtime::{StreamingColumnCollection, profile_builder};

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
/// let schema = dataprof_partial::infer_schema("data.csv").unwrap();
/// for col in &schema.columns {
///     println!("{}: {:?}", col.name, col.data_type);
/// }
/// ```
pub fn infer_schema<P: AsRef<Path>>(path: P) -> Result<SchemaResult, DataProfilerError> {
    let path = path.as_ref();
    let format = detect_format(path);
    infer_schema_with_format(path, format)
}

/// Quick row count (exact or estimated) for a file.
///
/// Returns an exact count for small files and Parquet; an estimate for large
/// CSV/JSON files via sampling.
///
/// # Example
/// ```no_run
/// let est = dataprof_partial::quick_row_count("data.csv").unwrap();
/// println!("{} rows (exact={})", est.count, est.exact);
/// ```
pub fn quick_row_count<P: AsRef<Path>>(path: P) -> Result<RowCountEstimate, DataProfilerError> {
    let path = path.as_ref();
    let format = detect_format(path);
    quick_row_count_with_format(path, format)
}

/// Detect file format from extension.
pub fn detect_format(file_path: &Path) -> FileFormat {
    file_path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| match ext.to_ascii_lowercase().as_str() {
            "csv" | "tsv" | "txt" => FileFormat::Csv,
            "json" => FileFormat::Json,
            "jsonl" | "ndjson" => FileFormat::Jsonl,
            "parquet" => FileFormat::Parquet,
            other => FileFormat::Unknown(other.to_string()),
        })
        .unwrap_or(FileFormat::Csv)
}

// ---------------------------------------------------------------------------
// Format-aware internals (also used by Profiler methods)
// ---------------------------------------------------------------------------

pub fn infer_schema_with_format(
    path: &Path,
    format: FileFormat,
) -> Result<SchemaResult, DataProfilerError> {
    let start = Instant::now();

    match format {
        FileFormat::Parquet => {
            #[cfg(feature = "parquet")]
            {
                infer_schema_parquet(path, start)
            }
            #[cfg(not(feature = "parquet"))]
            {
                Err(DataProfilerError::UnsupportedFormat {
                    format: "parquet (enable the `parquet` feature)".to_string(),
                })
            }
        }
        FileFormat::Csv => infer_schema_csv(path, start),
        FileFormat::Json | FileFormat::Jsonl => infer_schema_json(path, &format, start),
        FileFormat::Unknown(ref ext) => Err(DataProfilerError::UnsupportedFormat {
            format: ext.clone(),
        }),
    }
}

pub fn quick_row_count_with_format(
    path: &Path,
    format: FileFormat,
) -> Result<RowCountEstimate, DataProfilerError> {
    let start = Instant::now();

    match format {
        FileFormat::Parquet => {
            #[cfg(feature = "parquet")]
            {
                count_parquet(path, start)
            }
            #[cfg(not(feature = "parquet"))]
            {
                Err(DataProfilerError::UnsupportedFormat {
                    format: "parquet (enable the `parquet` feature)".to_string(),
                })
            }
        }
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

#[cfg(feature = "parquet")]
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
    let mut result = infer_schema_from_csv_reader(file, true)?;
    result.inference_time_ms = start.elapsed().as_millis();
    Ok(result)
}

fn infer_schema_json(
    path: &Path,
    format: &FileFormat,
    start: Instant,
) -> Result<SchemaResult, DataProfilerError> {
    let file = fs::File::open(path).map_err(|_| DataProfilerError::FileNotFound {
        path: path.display().to_string(),
    })?;
    let mut result = infer_schema_from_json_reader(BufReader::new(file), format)?;
    result.inference_time_ms = start.elapsed().as_millis();
    Ok(result)
}

// ---------------------------------------------------------------------------
// Reader-based helpers (shared by sync file and async stream paths)
// ---------------------------------------------------------------------------

/// Infer schema from any CSV reader. Reads up to `SCHEMA_SAMPLE_ROWS` rows.
fn infer_schema_from_csv_reader<R: Read>(
    reader: R,
    has_header: bool,
) -> Result<SchemaResult, DataProfilerError> {
    let config = CsvParserConfig::default()
        .has_header(has_header)
        .max_rows(Some(SCHEMA_SAMPLE_ROWS));
    let (_profiles, column_stats, rows_read, headers) =
        dataprof_csv::analyze_csv_from_reader(reader, &config)?;

    Ok(schema_from_streaming_stats(
        &column_stats,
        &headers,
        rows_read,
        0, // caller patches timing
    ))
}

/// Infer schema from any JSON/JSONL reader. Reads up to `SCHEMA_SAMPLE_ROWS` rows.
fn infer_schema_from_json_reader<R: BufRead>(
    reader: R,
    format: &FileFormat,
) -> Result<SchemaResult, DataProfilerError> {
    let config = match format {
        FileFormat::Jsonl => JsonParserConfig::jsonl().with_max_rows(SCHEMA_SAMPLE_ROWS),
        FileFormat::Json => JsonParserConfig::json_array().with_max_rows(SCHEMA_SAMPLE_ROWS),
        _ => JsonParserConfig::default().with_max_rows(SCHEMA_SAMPLE_ROWS),
    };
    let (_profiles, column_stats, rows_read, _malformed_lines, _detected_format) =
        dataprof_json::analyze_json_from_reader(reader, &config)?;

    // column_names() returns HashMap keys in arbitrary order. Sort
    // alphabetically so the output is deterministic across runs.
    let mut names = column_stats.column_names();
    names.sort();

    Ok(schema_from_streaming_stats(
        &column_stats,
        &names,
        rows_read,
        0, // caller patches timing
    ))
}

/// Infer schema from a generic reader, dispatching by format.
/// Parquet is not supported (requires seeking).
#[cfg(feature = "async-streaming")]
fn infer_schema_from_reader<R: Read>(
    reader: R,
    format: &FileFormat,
    has_header: bool,
) -> Result<SchemaResult, DataProfilerError> {
    match format {
        FileFormat::Csv => infer_schema_from_csv_reader(reader, has_header),
        FileFormat::Json | FileFormat::Jsonl => {
            infer_schema_from_json_reader(BufReader::new(reader), format)
        }
        FileFormat::Parquet => Err(DataProfilerError::StreamingError {
            message: "Parquet schema inference requires random access; use infer_schema() with a file path instead".into(),
        }),
        FileFormat::Unknown(ext) => Err(DataProfilerError::UnsupportedFormat {
            format: ext.clone(),
        }),
    }
}

/// Count rows from a generic reader, dispatching by format.
/// Always performs a full scan (no sampling — stream size is unknown).
#[cfg(feature = "async-streaming")]
fn count_from_reader<R: Read>(
    reader: R,
    format: &FileFormat,
) -> Result<RowCountEstimate, DataProfilerError> {
    let buf_reader = BufReader::new(reader);

    match format {
        FileFormat::Csv => {
            // Use the csv crate to count records properly — handles RFC 4180
            // quoted fields with embedded newlines, unlike raw line counting.
            let mut csv_reader = csv::ReaderBuilder::new()
                .has_headers(true)
                .flexible(true)
                .from_reader(buf_reader);
            let mut count: u64 = 0;
            for result in csv_reader.records() {
                let _record = result?;
                count += 1;
            }
            Ok(RowCountEstimate {
                count,
                exact: true,
                method: CountMethod::StreamFullScan,
                count_time_ms: 0, // caller patches timing
            })
        }
        FileFormat::Jsonl => {
            let mut count: u64 = 0;
            for line in buf_reader.lines() {
                let line = line.map_err(|e| DataProfilerError::io_error(&e))?;
                if !line.trim().is_empty() {
                    count += 1;
                }
            }
            Ok(RowCountEstimate {
                count,
                exact: true,
                method: CountMethod::StreamFullScan,
                count_time_ms: 0,
            })
        }
        FileFormat::Json => {
            // Walk into the top-level array and count elements one-by-one
            // with IgnoredAny, keeping memory O(1) instead of buffering
            // the entire array into a Vec.
            use serde::de::{Deserializer as _, SeqAccess, Visitor};
            struct ArrayCountVisitor;
            impl<'de> Visitor<'de> for ArrayCountVisitor {
                type Value = u64;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("a JSON array")
                }
                fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<u64, A::Error> {
                    let mut count = 0u64;
                    while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                        count += 1;
                    }
                    Ok(count)
                }
            }
            let mut de = serde_json::Deserializer::from_reader(buf_reader);
            let count = de.deserialize_seq(ArrayCountVisitor).map_err(|e| {
                DataProfilerError::JsonParsingError {
                    message: format!("Failed to parse JSON array: {}", e),
                }
            })?;
            Ok(RowCountEstimate {
                count,
                exact: true,
                method: CountMethod::StreamFullScan,
                count_time_ms: 0,
            })
        }
        FileFormat::Parquet => Err(DataProfilerError::StreamingError {
            message: "Parquet row counting requires random access; use quick_row_count() with a file path instead".into(),
        }),
        FileFormat::Unknown(ext) => Err(DataProfilerError::UnsupportedFormat {
            format: ext.clone(),
        }),
    }
}

// ---------------------------------------------------------------------------
// Row count — per format
// ---------------------------------------------------------------------------

#[cfg(feature = "parquet")]
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
        // Full scan — exact count using the CSV parser so embedded newlines in
        // quoted fields do not inflate the row count.
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(reader);
        let mut count: u64 = 0;
        for result in csv_reader.records() {
            let _record = result?;
            count += 1;
        }

        Ok(RowCountEstimate {
            count,
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
            if is_single_root_json_object(path)? {
                return Ok(RowCountEstimate {
                    count: 1,
                    exact: true,
                    method: CountMethod::FullScan,
                    count_time_ms: start.elapsed().as_millis(),
                });
            }

            let reader = BufReader::new(file);

            if file_size < FULL_SCAN_THRESHOLD {
                // Full scan — count non-empty lines
                let mut count: u64 = 0;
                for line in reader.lines() {
                    let line = line.map_err(|e| DataProfilerError::io_error(&e))?;
                    if !line.trim().is_empty() {
                        count += 1;
                    }
                }

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
            if is_single_root_json_object(path)? {
                return Ok(RowCountEstimate {
                    count: 1,
                    exact: true,
                    method: CountMethod::FullScan,
                    count_time_ms: start.elapsed().as_millis(),
                });
            }

            // JSON array — streaming count. We deserialize each element as
            // IgnoredAny which skips the value without allocating, keeping
            // memory usage constant regardless of array size.
            let reader = BufReader::new(file);
            let arr: Vec<serde::de::IgnoredAny> = serde_json::from_reader(reader).map_err(|e| {
                DataProfilerError::JsonParsingError {
                    message: format!("Failed to parse JSON array: {}", e),
                }
            })?;

            Ok(RowCountEstimate {
                count: arr.len() as u64,
                exact: true,
                method: CountMethod::FullScan,
                count_time_ms: start.elapsed().as_millis(),
            })
        }
    }
}

fn is_single_root_json_object(path: &Path) -> Result<bool, DataProfilerError> {
    let file = fs::File::open(path).map_err(|_| DataProfilerError::FileNotFound {
        path: path.display().to_string(),
    })?;
    let mut reader = BufReader::new(file);

    let first_non_whitespace = consume_leading_whitespace(&mut reader)?;

    if first_non_whitespace != Some(b'{') {
        return Ok(false);
    }

    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    match serde::de::IgnoredAny::deserialize(&mut deserializer) {
        Ok(_) => Ok(deserializer.end().is_ok()),
        Err(_) => Ok(false),
    }
}

fn consume_leading_whitespace<R: BufRead>(reader: &mut R) -> Result<Option<u8>, DataProfilerError> {
    loop {
        let mut bytes_to_consume = 0;
        let first_non_whitespace = {
            let buf = reader
                .fill_buf()
                .map_err(|e| DataProfilerError::io_error(&e))?;

            if buf.is_empty() {
                return Ok(None);
            }

            let first_non_whitespace = buf.iter().find(|byte| !byte.is_ascii_whitespace()).copied();
            if first_non_whitespace.is_none() {
                bytes_to_consume = buf.len();
            }
            first_non_whitespace
        };

        if first_non_whitespace.is_some() {
            return Ok(first_non_whitespace);
        }

        reader.consume(bytes_to_consume);
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

    // schema_stable is true when we read fewer rows than the sample cap
    // (meaning we hit EOF — the entire file was consumed), or for Parquet
    // (metadata-only). When we hit the SCHEMA_SAMPLE_ROWS cap, the schema
    // may not have stabilized.
    let schema_stable = rows_sampled < SCHEMA_SAMPLE_ROWS;

    SchemaResult {
        columns,
        rows_sampled,
        inference_time_ms: elapsed_ms,
        schema_stable,
    }
}

/// Map Arrow data types to dataprof's simplified 4-variant DataType.
#[cfg(feature = "parquet")]
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

        // Everything else (Utf8, LargeUtf8, Binary, List, Struct, etc.)
        // Boolean gets its own type
        AT::Boolean => DataType::Boolean,
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
// True async streaming (feature: async-streaming)
// ---------------------------------------------------------------------------

/// Infer schema from any async byte stream.
///
/// True async — no file path needed. Reads up to 1000 rows for
/// CSV/JSON/JSONL. Parquet is not supported (requires seeking).
///
/// # Example
/// ```no_run
/// use dataprof_core::FileFormat;
/// use dataprof_runtime::{AsyncSourceInfo, BytesSource};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let csv = b"name,age\nAlice,30\nBob,25\n";
/// let source = BytesSource::new(
///     bytes::Bytes::from_static(csv),
///     AsyncSourceInfo::new("api-body", FileFormat::Csv),
/// );
/// let schema = dataprof_partial::infer_schema_stream(source).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "async-streaming")]
pub async fn infer_schema_stream(
    source: impl dataprof_runtime::AsyncDataSource,
) -> Result<SchemaResult, DataProfilerError> {
    let info = source.source_info();
    let format = info.format.clone();
    let has_header = info.has_header.unwrap_or(true);

    let start = Instant::now();
    let async_reader = source.into_async_read().await?;
    let sync_reader = tokio_util::io::SyncIoBridge::new(async_reader);

    let mut result = tokio::task::spawn_blocking(move || {
        infer_schema_from_reader(sync_reader, &format, has_header)
    })
    .await
    .map_err(|e| DataProfilerError::StreamingError {
        message: format!("Schema inference task failed: {}", e),
    })??;

    result.inference_time_ms = start.elapsed().as_millis();
    Ok(result)
}

/// Quick row count from any async byte stream.
///
/// True async — always a full scan (no sampling, since stream size is unknown).
/// Parquet is not supported (requires seeking).
///
/// # Example
/// ```no_run
/// use dataprof_core::FileFormat;
/// use dataprof_runtime::{AsyncSourceInfo, BytesSource};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let csv = b"name,age\nAlice,30\nBob,25\n";
/// let source = BytesSource::new(
///     bytes::Bytes::from_static(csv),
///     AsyncSourceInfo::new("api-body", FileFormat::Csv),
/// );
/// let est = dataprof_partial::quick_row_count_stream(source).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "async-streaming")]
pub async fn quick_row_count_stream(
    source: impl dataprof_runtime::AsyncDataSource,
) -> Result<RowCountEstimate, DataProfilerError> {
    let info = source.source_info();
    let format = info.format.clone();

    let start = Instant::now();
    let async_reader = source.into_async_read().await?;
    let sync_reader = tokio_util::io::SyncIoBridge::new(async_reader);

    let mut result = tokio::task::spawn_blocking(move || count_from_reader(sync_reader, &format))
        .await
        .map_err(|e| DataProfilerError::StreamingError {
            message: format!("Row count task failed: {}", e),
        })??;

    result.count_time_ms = start.elapsed().as_millis();
    Ok(result)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use dataprof_core::DataType;
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
        // JSON columns are sorted alphabetically
        assert_eq!(result.columns[0].name, "age");
        assert_eq!(result.columns[1].name, "name");
    }

    #[test]
    fn test_infer_schema_json_array() {
        let f = write_temp_json(r#"[{"x":1,"y":"hello"},{"x":2,"y":"world"}]"#);
        let result = infer_schema(f.path()).unwrap();

        assert_eq!(result.columns.len(), 2);
        // JSON columns are sorted alphabetically
        assert_eq!(result.columns[0].name, "x");
        assert_eq!(result.columns[1].name, "y");
    }

    #[cfg(feature = "parquet")]
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
    fn test_quick_row_count_csv_with_quoted_newlines() {
        let f = write_temp_csv("id,text\n1,\"hello\"\n2,\"line1\nline2\"\n3,\"bye\"\n");
        let result = quick_row_count(f.path()).unwrap();

        assert_eq!(result.count, 3);
        assert!(result.exact);
        assert_eq!(result.method, CountMethod::FullScan);
    }

    #[test]
    fn test_quick_row_count_csv_with_inconsistent_fields() {
        let f = write_temp_csv("a,b\n1,2\n3,4,5\n6,7\n");
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

    // --- Issue #290: count_json with single root objects ---

    #[test]
    fn test_count_json_single_object_via_jsonl_format() {
        // A '{'-starting file passed as FileFormat::Jsonl must return 1, not line count.
        let f = write_temp_jsonl(r#"{"type":"FeatureCollection","features":[1,2,3]}"#);
        let result = quick_row_count(f.path()).unwrap();
        assert_eq!(result.count, 1);
        assert!(result.exact);
    }

    #[test]
    fn test_count_json_single_object_pretty_printed_via_jsonl_format() {
        // Pretty-printed single object (many lines) must not return line count.
        let content = "{\n  \"type\": \"FeatureCollection\",\n  \"features\": [\n    1,\n    2,\n    3\n  ]\n}\n";
        let f = write_temp_jsonl(content);
        let result = quick_row_count(f.path()).unwrap();
        assert_eq!(result.count, 1);
        assert!(result.exact);
    }

    #[test]
    fn test_count_json_single_object_via_json_format() {
        // A root-object .json file (not an array) must return 1, not an error.
        let f = write_temp_json(r#"{"type":"FeatureCollection","features":[1,2,3]}"#);
        let result = quick_row_count(f.path()).unwrap();
        assert_eq!(result.count, 1);
        assert!(result.exact);
    }

    #[test]
    fn test_count_json_array_unchanged() {
        // JSON array count must be unaffected by the fix.
        let f = write_temp_json(r#"[{"x":1},{"x":2},{"x":3}]"#);
        let result = quick_row_count(f.path()).unwrap();
        assert_eq!(result.count, 3);
        assert!(result.exact);
    }

    #[test]
    fn test_count_jsonl_multi_object_unchanged() {
        // Multi-object JSONL must still be counted correctly.
        let f = write_temp_jsonl("{\"x\":1}\n{\"x\":2}\n{\"x\":3}\n");
        let result = quick_row_count(f.path()).unwrap();
        assert_eq!(result.count, 3);
        assert!(result.exact);
    }

    #[test]
    fn test_count_jsonl_leading_blank_lines_unchanged() {
        let f = write_temp_jsonl("\n\n{\"x\":1}\n{\"x\":2}\n");
        let result = quick_row_count(f.path()).unwrap();
        assert_eq!(result.count, 2);
        assert!(result.exact);
    }

    #[test]
    fn test_count_json_single_object_with_large_leading_whitespace() {
        let content = format!("{}{{\"x\":1}}", " ".repeat(10_000));
        let f = write_temp_jsonl(&content);
        let result = quick_row_count(f.path()).unwrap();
        assert_eq!(result.count, 1);
        assert!(result.exact);
    }

    #[cfg(feature = "parquet")]
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

    #[cfg(feature = "parquet")]
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
        assert_eq!(arrow_type_to_dataprof(&AT::Boolean), DataType::Boolean);
    }
}

#[cfg(all(test, feature = "async-streaming"))]
mod async_tests {
    use super::*;
    use dataprof_core::{DataType, FileFormat};
    use dataprof_runtime::{AsyncSourceInfo, BytesSource};

    fn csv_source(data: &'static [u8]) -> BytesSource {
        BytesSource::new(
            bytes::Bytes::from_static(data),
            AsyncSourceInfo::new("test-csv", FileFormat::Csv).size_hint(Some(data.len() as u64)),
        )
    }

    fn jsonl_source(data: &'static [u8]) -> BytesSource {
        BytesSource::new(
            bytes::Bytes::from_static(data),
            AsyncSourceInfo::new("test-jsonl", FileFormat::Jsonl)
                .size_hint(Some(data.len() as u64)),
        )
    }

    fn json_source(data: &'static [u8]) -> BytesSource {
        BytesSource::new(
            bytes::Bytes::from_static(data),
            AsyncSourceInfo::new("test-json", FileFormat::Json).size_hint(Some(data.len() as u64)),
        )
    }

    fn parquet_source() -> BytesSource {
        BytesSource::new(
            bytes::Bytes::from_static(b""),
            AsyncSourceInfo::new("test-parquet", FileFormat::Parquet),
        )
    }

    #[tokio::test]
    async fn test_infer_schema_stream_csv() {
        let source = csv_source(b"name,age,salary\nAlice,30,50000.5\nBob,25,60000.0\n");
        let result = infer_schema_stream(source).await.unwrap();

        assert_eq!(result.columns.len(), 3);
        assert_eq!(result.columns[0].name, "name");
        assert_eq!(result.columns[0].data_type, DataType::String);
        assert_eq!(result.columns[1].name, "age");
        assert_eq!(result.columns[1].data_type, DataType::Integer);
        assert_eq!(result.columns[2].name, "salary");
        assert_eq!(result.columns[2].data_type, DataType::Float);
        assert!(result.rows_sampled > 0);
        assert!(result.schema_stable); // small data — all rows consumed
    }

    #[tokio::test]
    async fn test_infer_schema_stream_jsonl() {
        let source =
            jsonl_source(b"{\"name\":\"Alice\",\"age\":30}\n{\"name\":\"Bob\",\"age\":25}\n");
        let result = infer_schema_stream(source).await.unwrap();

        assert_eq!(result.columns.len(), 2);
        // JSON columns are sorted alphabetically
        assert_eq!(result.columns[0].name, "age");
        assert_eq!(result.columns[1].name, "name");
    }

    #[tokio::test]
    async fn test_infer_schema_stream_json_array() {
        let source = json_source(b"[{\"x\":1,\"y\":\"hello\"},{\"x\":2,\"y\":\"world\"}]");
        let result = infer_schema_stream(source).await.unwrap();

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "x");
        assert_eq!(result.columns[1].name, "y");
    }

    #[tokio::test]
    async fn test_infer_schema_stream_parquet_rejected() {
        let source = parquet_source();
        let result = infer_schema_stream(source).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("random access"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn test_quick_row_count_stream_csv() {
        let source = csv_source(b"a,b\n1,2\n3,4\n5,6\n");
        let result = quick_row_count_stream(source).await.unwrap();

        assert_eq!(result.count, 3);
        assert!(result.exact);
        assert_eq!(result.method, CountMethod::StreamFullScan);
    }

    #[tokio::test]
    async fn test_quick_row_count_stream_jsonl() {
        let source = jsonl_source(b"{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        let result = quick_row_count_stream(source).await.unwrap();

        assert_eq!(result.count, 3);
        assert!(result.exact);
        assert_eq!(result.method, CountMethod::StreamFullScan);
    }

    #[tokio::test]
    async fn test_quick_row_count_stream_json_array() {
        let source = json_source(b"[{\"a\":1},{\"a\":2},{\"a\":3}]");
        let result = quick_row_count_stream(source).await.unwrap();

        assert_eq!(result.count, 3);
        assert!(result.exact);
        assert_eq!(result.method, CountMethod::StreamFullScan);
    }

    #[tokio::test]
    async fn test_quick_row_count_stream_parquet_rejected() {
        let source = parquet_source();
        let result = quick_row_count_stream(source).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("random access"), "unexpected error: {err}");
    }
}
