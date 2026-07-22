//! Partial analysis APIs (#226)
//!
//! Fast, lightweight alternatives to full profiling:
//! - [`infer_schema`] — quick schema detection (column names + types)
//! - [`quick_row_count`] — fast row counting / estimation

use std::fs;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::time::Instant;

#[cfg(feature = "parquet")]
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Deserialize;

#[cfg(feature = "parquet")]
use dataprof_core::DataType;
use dataprof_core::{
    ColumnProfile, ColumnSchema, CountMethod, DataProfilerError, FileFormat, RowCountEstimate,
    SchemaResult, StructureColumnSummary, StructureReport,
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

/// Number of evenly-spaced windows sampled across a large file when estimating
/// a CSV row count. Sampling at multiple offsets removes the prefix bias of the
/// old first-N-lines approach.
const ROW_SAMPLE_WINDOWS: usize = 16;

/// Lines measured per sampling window. Keeps the total sampled-line budget in
/// line with [`ROW_SAMPLE_LINES`].
const ROW_SAMPLE_LINES_PER_WINDOW: usize = ROW_SAMPLE_LINES / ROW_SAMPLE_WINDOWS;

/// Default row cap for `analyze_structure()`.
const STRUCTURE_SAMPLE_ROWS: usize = 1000;

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

/// Analyze a file's structure with a bounded, lightweight pass.
///
/// This composes the existing partial-analysis primitives and parser-level
/// streaming counters. It does not compute quality scores, pattern detection,
/// recommendations, or raw sample values.
pub fn analyze_structure<P: AsRef<Path>>(
    path: P,
    max_rows: Option<usize>,
) -> Result<StructureReport, DataProfilerError> {
    let path = path.as_ref();
    let format = detect_format(path);
    analyze_structure_with_format(path, format, max_rows)
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

pub fn analyze_structure_with_format(
    path: &Path,
    format: FileFormat,
    max_rows: Option<usize>,
) -> Result<StructureReport, DataProfilerError> {
    // Establish file existence up front so all formats return the same
    // path-specific error shape as the existing partial APIs.
    fs::metadata(path).map_err(|_| DataProfilerError::FileNotFound {
        path: path.display().to_string(),
    })?;

    let limit = max_rows.unwrap_or(STRUCTURE_SAMPLE_ROWS);

    match format {
        FileFormat::Csv => analyze_structure_csv(path, limit),
        FileFormat::Json | FileFormat::Jsonl => analyze_structure_json(path, format, limit),
        FileFormat::Parquet => analyze_structure_parquet(path),
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
                relative_error: None,
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
                relative_error: None,
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
                relative_error: None,
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
        relative_error: None,
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
        full_scan_csv(reader, start)
    } else {
        // Sampling — estimate the local row density (rows per byte) at several
        // evenly-spaced offsets across the file, then integrate over the file
        // size. A prefix-only sample is systematically biased on the common
        // case of an autoincrement id or growing timestamp, where early rows
        // are shorter than later ones, so extrapolating from the file's head
        // overestimates. Sampling across offsets removes that bias for the same
        // I/O budget. Every physical line counts as a row for CSV.
        let sample = sample_row_density_multi_offset(path, file_size, |_| true)?;

        if sample.rows_sampled == 0 {
            // The sampler could not observe a single complete line — e.g. rows
            // longer than a sampling window, so every seek lands inside one row
            // and the resync consumes the rest to EOF. Fall back to an exact
            // scan rather than reporting a fabricated zero labelled `exact`.
            // `reader` is still positioned at the start of the file (the
            // sampler uses its own independent handle).
            return full_scan_csv(reader, start);
        }

        // Subtract 1 for the header line.
        let estimated_total_rows = sample.estimated_rows.round() as u64;
        let count = estimated_total_rows.saturating_sub(1);

        Ok(RowCountEstimate {
            count,
            exact: false,
            method: CountMethod::Sampling,
            count_time_ms: start.elapsed().as_millis(),
            relative_error: sample.relative_error,
        })
    }
}

/// Exact CSV row count via a full scan with the CSV parser, so embedded
/// newlines in quoted fields do not inflate the count. Shared by the
/// small-file path and the large-file fallback when sampling observes no
/// complete lines.
fn full_scan_csv<R: Read>(
    reader: R,
    start: Instant,
) -> Result<RowCountEstimate, DataProfilerError> {
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
        relative_error: None,
    })
}

/// Exact JSONL row count via a full scan counting non-empty lines. Shared by
/// the small-file path and the large-file fallback when the prefix sample
/// yields no non-empty lines.
fn full_scan_jsonl<R: BufRead>(
    reader: R,
    start: Instant,
) -> Result<RowCountEstimate, DataProfilerError> {
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
        relative_error: None,
    })
}

/// Statistics from sampling row density at several offsets across a file.
struct MultiOffsetRowSample {
    /// Estimated total number of qualifying rows in the whole file (rows for
    /// which the caller's predicate returned true).
    estimated_rows: f64,
    /// Total number of qualifying rows actually measured across all windows.
    rows_sampled: u64,
    /// Approximate 1-sigma relative standard error of `estimated_rows`, or
    /// `None` when fewer than two windows produced data.
    relative_error: Option<f64>,
}

/// Estimate a file's total row count by sampling local row density (qualifying
/// rows per byte) at [`ROW_SAMPLE_WINDOWS`] evenly-spaced offsets, reading up to
/// [`ROW_SAMPLE_LINES_PER_WINDOW`] physical lines per window. The total number
/// of lines read matches the previous prefix-only sampler's budget
/// ([`ROW_SAMPLE_LINES`]).
///
/// `counts_as_row` decides which physical lines count toward the row total
/// (e.g. every line for CSV; only non-empty lines for JSONL). Every line's
/// bytes are counted regardless, so skipped lines (like JSONL blanks) still
/// lower the local density and are accounted for.
///
/// Each window represents an equal share of the file's *bytes*, so averaging
/// the per-window density is a Riemann sum of density over the byte axis — i.e.
/// `total_rows ≈ file_size · mean(density)`. This stays unbiased whatever the
/// row-length distribution. (Averaging line *lengths* instead would be
/// length-biased: uniform byte offsets over-weight long rows, undercounting.)
///
/// Windows are probed at the *center* of each byte slice (midpoint rule), not
/// its leading edge: with a monotone row-length gradient a left-edge sample is
/// a left Riemann sum with O(1/W) bias, whereas centered samples give the
/// O(1/W²) midpoint rule. Every window seeks mid-file, so each discards the
/// partial line it lands in before measuring whole lines.
fn sample_row_density_multi_offset(
    path: &Path,
    file_size: u64,
    counts_as_row: impl Fn(&str) -> bool,
) -> Result<MultiOffsetRowSample, DataProfilerError> {
    let mut file = fs::File::open(path).map_err(|_| DataProfilerError::FileNotFound {
        path: path.display().to_string(),
    })?;

    let window_span = file_size / ROW_SAMPLE_WINDOWS as u64;
    // Per-window row density in qualifying rows per byte.
    let mut densities: Vec<f64> = Vec::with_capacity(ROW_SAMPLE_WINDOWS);
    let mut total_rows: u64 = 0;

    for w in 0..ROW_SAMPLE_WINDOWS {
        let offset = window_span * w as u64 + window_span / 2;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| DataProfilerError::io_error(&e))?;
        let mut reader = BufReader::new(&mut file);

        // Each window seeks into the middle of the file and most likely lands
        // inside a line; discard that partial remainder so we only measure
        // whole lines starting at the next boundary.
        {
            let mut partial = String::new();
            reader
                .read_line(&mut partial)
                .map_err(|e| DataProfilerError::io_error(&e))?;
        }

        let mut win_bytes: u64 = 0;
        let mut win_rows: u64 = 0;
        for _ in 0..ROW_SAMPLE_LINES_PER_WINDOW {
            let mut line = String::new();
            // `read_line` keeps the line terminator, so byte accounting matches
            // the on-disk length (including `\r\n`) without a manual +1.
            let n = reader
                .read_line(&mut line)
                .map_err(|e| DataProfilerError::io_error(&e))?;
            if n == 0 {
                break; // EOF
            }
            win_bytes += n as u64;
            if counts_as_row(&line) {
                win_rows += 1;
            }
        }

        if win_bytes > 0 {
            densities.push(win_rows as f64 / win_bytes as f64);
            total_rows += win_rows;
        }
    }

    let mean_density = if densities.is_empty() {
        0.0
    } else {
        densities.iter().sum::<f64>() / densities.len() as f64
    };
    let estimated_rows = file_size as f64 * mean_density;

    // Estimate the uncertainty from the spread of per-window densities. The
    // standard error of the mean density propagates directly to the relative
    // error of `file_size · mean_density`.
    let relative_error = if densities.len() >= 2 && mean_density > 0.0 {
        let k = densities.len() as f64;
        let variance = densities
            .iter()
            .map(|d| (d - mean_density).powi(2))
            .sum::<f64>()
            / (k - 1.0);
        let std_err = (variance / k).sqrt();
        Some((std_err / mean_density).min(1.0))
    } else {
        None
    };

    Ok(MultiOffsetRowSample {
        estimated_rows,
        rows_sampled: total_rows,
        relative_error,
    })
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
                    relative_error: None,
                });
            }

            let reader = BufReader::new(file);

            if file_size < FULL_SCAN_THRESHOLD {
                return full_scan_jsonl(reader, start);
            }

            // Sampling — multi-offset row-density estimate, same technique as
            // the CSV path (see #428), counting only non-empty lines as records
            // so a run of blank lines does not skew the estimate.
            let sample =
                sample_row_density_multi_offset(path, file_size, |line| !line.trim().is_empty())?;

            if sample.rows_sampled == 0 {
                // No non-empty line was observed in any window; the records may
                // start past every probe, or the whole file may be blank. Fall
                // back to an exact scan instead of a fabricated `exact` zero.
                return full_scan_jsonl(reader, start);
            }

            // No header line in JSONL, so the estimate is the record count.
            let count = sample.estimated_rows.round() as u64;

            Ok(RowCountEstimate {
                count,
                exact: false,
                method: CountMethod::Sampling,
                count_time_ms: start.elapsed().as_millis(),
                relative_error: sample.relative_error,
            })
        }
        _ => {
            if is_single_root_json_object(path)? {
                return Ok(RowCountEstimate {
                    count: 1,
                    exact: true,
                    method: CountMethod::FullScan,
                    count_time_ms: start.elapsed().as_millis(),
                    relative_error: None,
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
                relative_error: None,
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

fn analyze_structure_csv(
    path: &Path,
    max_rows: usize,
) -> Result<StructureReport, DataProfilerError> {
    let start = Instant::now();
    let delimiter = dataprof_csv::detect_delimiter_from_path(path).ok();
    let file = fs::File::open(path).map_err(|_| DataProfilerError::FileNotFound {
        path: path.display().to_string(),
    })?;

    let mut config = CsvParserConfig::default().max_rows(Some(max_rows));
    if let Some(delimiter) = delimiter {
        config = config.with_delimiter(delimiter);
    }

    let (profiles, column_stats, rows_sampled, headers) =
        dataprof_csv::analyze_csv_from_reader(file, &config)?;
    let row_count = if rows_sampled < max_rows {
        exact_row_count_from_sample(rows_sampled, start.elapsed().as_millis())
    } else {
        quick_row_count_with_format(path, FileFormat::Csv)?
    };
    let mut columns = structure_columns_from_profiles(&profiles, &column_stats, "sample");
    if columns.is_empty() && !headers.is_empty() {
        columns = empty_structure_columns_from_headers(&headers, "sample");
    }

    let (source_exhausted, truncated, truncation_reason) =
        sample_status(&row_count, rows_sampled, max_rows);
    let warnings = structure_warnings(&row_count, truncated, None);

    Ok(StructureReport {
        source: path.display().to_string(),
        format: FileFormat::Csv,
        row_count,
        rows_sampled,
        source_exhausted,
        truncated,
        truncation_reason,
        delimiter: delimiter.map(delimiter_to_string),
        columns,
        warnings,
    })
}

fn analyze_structure_json(
    path: &Path,
    format: FileFormat,
    max_rows: usize,
) -> Result<StructureReport, DataProfilerError> {
    let start = Instant::now();
    let file = fs::File::open(path).map_err(|_| DataProfilerError::FileNotFound {
        path: path.display().to_string(),
    })?;
    let reader = BufReader::new(file);
    let config = match format {
        FileFormat::Jsonl => JsonParserConfig::jsonl().with_max_rows(max_rows),
        FileFormat::Json => JsonParserConfig::json_array().with_max_rows(max_rows),
        _ => JsonParserConfig::default().with_max_rows(max_rows),
    };

    let (mut profiles, column_stats, rows_sampled, malformed_lines, detected_format) =
        dataprof_json::analyze_json_from_reader(reader, &config)?;
    profiles.sort_by(|a, b| a.name.cmp(&b.name));

    let row_count = if rows_sampled < max_rows && malformed_lines == 0 {
        exact_row_count_from_sample(rows_sampled, start.elapsed().as_millis())
    } else {
        quick_row_count_with_format(path, format)?
    };
    let (source_exhausted, truncated, truncation_reason) =
        sample_status(&row_count, rows_sampled, max_rows);
    let warnings = structure_warnings(&row_count, truncated, Some(malformed_lines));

    Ok(StructureReport {
        source: path.display().to_string(),
        format: detected_format,
        row_count,
        rows_sampled,
        source_exhausted,
        truncated,
        truncation_reason,
        delimiter: None,
        columns: structure_columns_from_profiles(&profiles, &column_stats, "sample"),
        warnings,
    })
}

fn analyze_structure_parquet(path: &Path) -> Result<StructureReport, DataProfilerError> {
    #[cfg(feature = "parquet")]
    {
        let schema = infer_schema_with_format(path, FileFormat::Parquet)?;
        let row_count = quick_row_count_with_format(path, FileFormat::Parquet)?;
        let columns = schema
            .columns
            .into_iter()
            .map(|column| StructureColumnSummary {
                name: column.name,
                data_type: column.data_type,
                total_count: None,
                null_count: None,
                null_ratio: None,
                unique_count: None,
                uniqueness_ratio: None,
                distinct_count_approximate: None,
                provenance: "metadata".to_string(),
            })
            .collect();

        Ok(StructureReport {
            source: path.display().to_string(),
            format: FileFormat::Parquet,
            row_count,
            rows_sampled: 0,
            source_exhausted: true,
            truncated: false,
            truncation_reason: None,
            delimiter: None,
            columns,
            warnings: Vec::new(),
        })
    }

    #[cfg(not(feature = "parquet"))]
    {
        let _ = path;
        Err(DataProfilerError::UnsupportedFormat {
            format: "parquet (enable the `parquet` feature)".to_string(),
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn structure_columns_from_profiles(
    profiles: &[ColumnProfile],
    column_stats: &StreamingColumnCollection,
    provenance: &str,
) -> Vec<StructureColumnSummary> {
    profiles
        .iter()
        .map(|profile| {
            let total_count = profile.total_count;
            let null_ratio = ratio(profile.null_count, total_count);
            let uniqueness_ratio = profile
                .unique_count
                .and_then(|unique| ratio(unique, total_count));
            let distinct_count_approximate = column_stats
                .get_column_stats(&profile.name)
                .map(|stats| stats.unique_count_is_approximate());

            StructureColumnSummary {
                name: profile.name.clone(),
                data_type: profile.data_type.clone(),
                total_count: Some(total_count),
                null_count: Some(profile.null_count),
                null_ratio,
                unique_count: profile.unique_count,
                uniqueness_ratio,
                distinct_count_approximate,
                provenance: provenance.to_string(),
            }
        })
        .collect()
}

fn empty_structure_columns_from_headers(
    headers: &[String],
    provenance: &str,
) -> Vec<StructureColumnSummary> {
    headers
        .iter()
        .map(|name| StructureColumnSummary {
            name: name.clone(),
            data_type: dataprof_core::DataType::String,
            total_count: Some(0),
            null_count: Some(0),
            null_ratio: None,
            unique_count: Some(0),
            uniqueness_ratio: None,
            distinct_count_approximate: Some(false),
            provenance: provenance.to_string(),
        })
        .collect()
}

fn ratio(numerator: usize, denominator: usize) -> Option<f64> {
    if denominator == 0 {
        None
    } else {
        Some(numerator as f64 / denominator as f64)
    }
}

fn exact_row_count_from_sample(rows_sampled: usize, count_time_ms: u128) -> RowCountEstimate {
    RowCountEstimate {
        count: rows_sampled as u64,
        exact: true,
        method: CountMethod::FullScan,
        count_time_ms,
        relative_error: None,
    }
}

fn sample_status(
    row_count: &RowCountEstimate,
    rows_sampled: usize,
    max_rows: usize,
) -> (bool, bool, Option<String>) {
    let truncated = if row_count.exact {
        (rows_sampled as u64) < row_count.count
    } else {
        rows_sampled >= max_rows
    };
    let truncation_reason = truncated.then(|| format!("max_rows({max_rows})"));
    (!truncated, truncated, truncation_reason)
}

fn structure_warnings(
    row_count: &RowCountEstimate,
    truncated: bool,
    malformed_lines: Option<usize>,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if !row_count.exact {
        warnings.push("row_count_estimated".to_string());
    }
    if truncated {
        warnings.push("structure_sample_truncated".to_string());
    }
    if let Some(count) = malformed_lines.filter(|count| *count > 0) {
        warnings.push(format!("malformed_lines_skipped:{count}"));
    }
    warnings
}

fn delimiter_to_string(delimiter: u8) -> String {
    match delimiter {
        b'\t' => "\t".to_string(),
        b'\n' => "\\n".to_string(),
        b'\r' => "\\r".to_string(),
        byte => char::from(byte).to_string(),
    }
}

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

    /// Write a CSV larger than `FULL_SCAN_THRESHOLD` so `count_csv` takes the
    /// sampling branch. `grow` makes each row's filler length increase with the
    /// row index, reproducing the short-early / long-late bias of autoincrement
    /// ids and growing timestamps (issue #428). Returns the exact data-row count.
    fn write_large_csv(grow: bool) -> (NamedTempFile, u64) {
        use std::io::BufWriter;
        let mut f = NamedTempFile::with_suffix(".csv").unwrap();
        let mut rows: u64 = 0;
        {
            let mut w = BufWriter::new(f.as_file_mut());
            w.write_all(b"id,payload\n").unwrap();
            let mut written: u64 = "id,payload\n".len() as u64;
            let target = FULL_SCAN_THRESHOLD + 1024 * 1024; // ~1 MB over threshold
            while written < target {
                let filler = if grow {
                    // 8..~108 chars as the file progresses.
                    8 + (rows / 2_000) as usize
                } else {
                    32
                };
                let line = format!("{},{}\n", rows, "x".repeat(filler));
                w.write_all(line.as_bytes()).unwrap();
                written += line.len() as u64;
                rows += 1;
            }
        }
        f.flush().unwrap();
        (f, rows)
    }

    #[test]
    fn test_quick_row_count_csv_sampling_uniform_is_accurate() {
        let (f, actual) = write_large_csv(false);
        let result = quick_row_count(f.path()).unwrap();

        assert_eq!(result.method, CountMethod::Sampling);
        assert!(!result.exact);

        let err = (result.count as f64 - actual as f64).abs() / actual as f64;
        assert!(
            err < 0.02,
            "uniform sampling estimate off by {:.2}% (count={}, actual={})",
            err * 100.0,
            result.count,
            actual
        );
        // A hint should be present and tight for a uniform file.
        let rel = result
            .relative_error
            .expect("relative_error should be Some");
        assert!(
            (0.0..0.05).contains(&rel),
            "unexpected relative_error {rel}"
        );
    }

    #[test]
    fn test_quick_row_count_csv_sampling_growing_rows_not_biased() {
        // With prefix-only sampling the short early rows made
        // `file_size / avg_row_len` overestimate by ~9%. Multi-offset sampling
        // should keep the estimate within a few percent of the true count.
        let (f, actual) = write_large_csv(true);
        let result = quick_row_count(f.path()).unwrap();

        assert_eq!(result.method, CountMethod::Sampling);
        let err = (result.count as f64 - actual as f64).abs() / actual as f64;
        assert!(
            err < 0.03,
            "multi-offset estimate off by {:.2}% (count={}, actual={}) — prefix bias not removed",
            err * 100.0,
            result.count,
            actual
        );
        assert!(result.relative_error.is_some());
    }

    #[test]
    fn test_exact_counts_have_no_relative_error() {
        let f = write_temp_csv("a,b\n1,2\n3,4\n");
        let result = quick_row_count(f.path()).unwrap();
        assert!(result.exact);
        assert_eq!(result.relative_error, None);
    }

    #[test]
    fn test_quick_row_count_csv_sampling_zero_lines_falls_back_to_exact() {
        // Header + a single data field larger than the whole sampling-window
        // grid: every window seeks inside that one line and the resync consumes
        // the remainder to EOF, so the sampler observes no complete line. The
        // result must be an exact full scan (count=1), not a fabricated zero.
        let mut f = NamedTempFile::with_suffix(".csv").unwrap();
        writeln!(f, "data").unwrap();
        let big = "x".repeat(FULL_SCAN_THRESHOLD as usize + 1024 * 1024);
        writeln!(f, "{big}").unwrap();
        f.flush().unwrap();

        let result = quick_row_count(f.path()).unwrap();
        assert_eq!(result.method, CountMethod::FullScan);
        assert!(result.exact);
        assert_eq!(result.count, 1);
        assert_eq!(result.relative_error, None);
    }

    /// Write a JSONL file larger than `FULL_SCAN_THRESHOLD` so `count_json`
    /// takes the sampling branch. `grow` makes each record's payload length
    /// increase with its index, reproducing the short-early / long-late bias.
    /// Returns the exact record count.
    fn write_large_jsonl(grow: bool) -> (NamedTempFile, u64) {
        use std::io::BufWriter;
        let mut f = NamedTempFile::with_suffix(".jsonl").unwrap();
        let mut rows: u64 = 0;
        {
            let mut w = BufWriter::new(f.as_file_mut());
            let mut written: u64 = 0;
            let target = FULL_SCAN_THRESHOLD + 1024 * 1024;
            while written < target {
                let filler = if grow {
                    8 + (rows / 2_000) as usize
                } else {
                    24
                };
                let line = format!("{{\"id\":{rows},\"v\":\"{}\"}}\n", "x".repeat(filler));
                w.write_all(line.as_bytes()).unwrap();
                written += line.len() as u64;
                rows += 1;
            }
        }
        f.flush().unwrap();
        (f, rows)
    }

    #[test]
    fn test_quick_row_count_jsonl_sampling_growing_rows_not_biased() {
        // Multi-offset density sampling should keep the estimate within a few
        // percent of the true record count despite the growing-row-length bias
        // that skewed the old prefix-only JSONL sampler.
        let (f, actual) = write_large_jsonl(true);
        let result = quick_row_count(f.path()).unwrap();

        assert_eq!(result.method, CountMethod::Sampling);
        let err = (result.count as f64 - actual as f64).abs() / actual as f64;
        assert!(
            err < 0.03,
            "JSONL multi-offset estimate off by {:.2}% (count={}, actual={})",
            err * 100.0,
            result.count,
            actual
        );
        assert!(result.relative_error.is_some());
    }

    #[test]
    fn test_quick_row_count_jsonl_blank_prefix_not_undercounted() {
        // A run of blank lines longer than a single sampling window ahead of the
        // records must not drag the estimate down: the density sampler counts
        // only non-empty lines while still charging the blank bytes, and probes
        // the whole file rather than just the prefix.
        use std::io::BufWriter;
        let (data_f, data_rows) = write_large_jsonl(false);
        let mut f = NamedTempFile::with_suffix(".jsonl").unwrap();
        {
            let mut w = BufWriter::new(f.as_file_mut());
            for _ in 0..(ROW_SAMPLE_LINES + 100) {
                w.write_all(b"\n").unwrap();
            }
            let data = std::fs::read(data_f.path()).unwrap();
            w.write_all(&data).unwrap();
        }
        f.flush().unwrap();

        let result = quick_row_count(f.path()).unwrap();
        assert_eq!(result.method, CountMethod::Sampling);
        let err = (result.count as f64 - data_rows as f64).abs() / data_rows as f64;
        assert!(
            err < 0.05,
            "blank-prefix estimate off by {:.2}% (count={}, actual={})",
            err * 100.0,
            result.count,
            data_rows
        );
    }

    #[test]
    fn test_quick_row_count_jsonl_all_blank_falls_back_to_exact() {
        // A large file whose every sampling window sees only blank lines yields
        // no sampled record, so the count must come from an exact full scan (0
        // records here) rather than a divide-by-zero or fabricated estimate.
        use std::io::BufWriter;
        let mut f = NamedTempFile::with_suffix(".jsonl").unwrap();
        {
            let mut w = BufWriter::new(f.as_file_mut());
            let block = vec![b'\n'; 1024 * 1024];
            for _ in 0..((FULL_SCAN_THRESHOLD / (1024 * 1024)) + 2) {
                w.write_all(&block).unwrap();
            }
        }
        f.flush().unwrap();

        let result = quick_row_count(f.path()).unwrap();
        assert_eq!(result.method, CountMethod::FullScan);
        assert!(result.exact);
        assert_eq!(result.count, 0);
        assert_eq!(result.relative_error, None);
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

    #[test]
    fn test_analyze_structure_csv() {
        let f = write_temp_csv("name,age\nAlice,30\nBob,\nCharlie,40\n");
        let report = analyze_structure(f.path(), Some(10)).unwrap();

        assert_eq!(report.format, FileFormat::Csv);
        assert_eq!(report.row_count.count, 3);
        assert!(report.row_count.exact);
        assert_eq!(report.rows_sampled, 3);
        assert!(report.source_exhausted);
        assert!(!report.truncated);
        assert_eq!(report.delimiter.as_deref(), Some(","));

        let age = report.columns.iter().find(|col| col.name == "age").unwrap();
        assert_eq!(age.data_type, DataType::Integer);
        assert_eq!(age.total_count, Some(3));
        assert_eq!(age.null_count, Some(1));
        assert!((age.null_ratio.unwrap() - (1.0 / 3.0)).abs() < 0.001);
        assert_eq!(age.provenance, "sample");
    }

    #[test]
    fn test_analyze_structure_jsonl() {
        let f = write_temp_jsonl(
            r#"{"name":"Alice","age":30}
{"name":"Bob","age":25}
"#,
        );
        let report = analyze_structure(f.path(), Some(10)).unwrap();

        assert_eq!(report.format, FileFormat::Jsonl);
        assert_eq!(report.row_count.count, 2);
        assert_eq!(report.rows_sampled, 2);
        assert_eq!(report.columns.len(), 2);
        assert!(report.columns.iter().any(|col| col.name == "age"));
    }

    #[test]
    fn test_analyze_structure_json_array() {
        let f = write_temp_json(r#"[{"x":1,"y":"a"},{"x":2,"y":"b"}]"#);
        let report = analyze_structure(f.path(), Some(10)).unwrap();

        assert_eq!(report.format, FileFormat::Json);
        assert_eq!(report.row_count.count, 2);
        assert_eq!(report.rows_sampled, 2);
        assert_eq!(
            report
                .columns
                .iter()
                .map(|col| col.name.as_str())
                .collect::<Vec<_>>(),
            vec!["x", "y"]
        );
    }

    #[test]
    fn test_analyze_structure_truncates_at_max_rows() {
        let f = write_temp_csv("a\n1\n2\n3\n");
        let report = analyze_structure(f.path(), Some(2)).unwrap();

        assert_eq!(report.row_count.count, 3);
        assert_eq!(report.rows_sampled, 2);
        assert!(!report.source_exhausted);
        assert!(report.truncated);
        assert_eq!(report.truncation_reason.as_deref(), Some("max_rows(2)"));
        assert!(
            report
                .warnings
                .iter()
                .any(|w| w == "structure_sample_truncated")
        );
    }

    #[test]
    fn test_analyze_structure_missing_file() {
        let err = analyze_structure("does_not_exist.csv", Some(10)).unwrap_err();
        assert!(matches!(err, DataProfilerError::FileNotFound { .. }));
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

    #[cfg(feature = "parquet")]
    #[test]
    fn test_analyze_structure_parquet_metadata() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType as ArrowDT, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;

        let schema = Schema::new(vec![
            Field::new("id", ArrowDT::Int32, false),
            Field::new("label", ArrowDT::Utf8, false),
        ]);

        let batch = RecordBatch::try_new(
            std::sync::Arc::new(schema),
            vec![
                std::sync::Arc::new(Int32Array::from(vec![1, 2])),
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

        let report = analyze_structure(f.path(), Some(1)).unwrap();
        assert_eq!(report.format, FileFormat::Parquet);
        assert_eq!(report.row_count.count, 2);
        assert_eq!(report.rows_sampled, 0);
        assert!(report.source_exhausted);
        assert!(!report.truncated);
        assert_eq!(report.columns.len(), 2);
        assert!(
            report
                .columns
                .iter()
                .all(|col| col.provenance == "metadata")
        );
        assert!(report.columns.iter().all(|col| col.total_count.is_none()));
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
