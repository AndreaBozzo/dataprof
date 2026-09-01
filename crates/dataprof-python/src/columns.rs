//! Native profiling of columnar Python data, with no third-party dependency.
//!
//! `dict`, list-of-dicts, and decoded byte buffers all reduce to "named columns
//! of optional strings". Routing them through this module keeps the base wheel's
//! promise -- ad-hoc inputs profile with zero Python dependencies -- and makes
//! the result independent of whether pandas happens to be installed.

use std::collections::HashSet;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use dataprof::{
    DataFrameLibrary, DataSource, ExecutionMetadata, FileFormat, MetricPack, TruncationReason,
    infer_type, is_null_like_token,
};
use dataprof_runtime::{
    ColumnProfileInput, ReportAssembler, RowCompletenessTracker, RowUniquenessTracker,
    build_column_profile,
};

use super::config::PyProfilerConfig;
use super::types::PyProfileReport;

/// One column as handed over from Python: a name and its cells, `None` for null.
pub type PyColumn = (String, Vec<Option<String>>);

/// Profile named columns of optional strings.
///
/// A cell is null when Python handed us `None` *or* when it is a null-like token
/// (`""`, `"null"`, `"nan"`), which is the same rule the Arrow string path and the
/// CSV engine apply. Nulls take no part in statistics, uniqueness, or inference.
///
/// Column order is preserved as given, so reports over the same input are
/// byte-identical across processes.
///
/// `row_count` states how many rows the source held. It is only needed when
/// `columns` is empty and the source still had rows -- JSON records with no
/// fields, which the file scanner counts as rows against no columns. When
/// columns are present their cell count already carries the row count, and a
/// `row_count` that disagrees with it is rejected rather than silently ignored.
///
/// `source_bytes` is the length of the original buffer, required when
/// `source_type` is `"bytes"`: the decoded cells are a different size from the
/// bytes they came out of, so the caller is the only one who knows it.
///
/// Raises `ValueError` when the columns do not all have the same length.
// One flat parameter per Python keyword argument: the pyo3 signature is the
// call surface, so grouping them into a struct would only move the list into
// `#[derive(FromPyObject)]`. Same reasoning as `PyProfilerConfig::new`.
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (columns, name = "dataframe".to_string(), max_rows = None, config = None, error_count = 0, row_count = None, source_type = "dataframe".to_string(), source_format = None, source_bytes = None))]
pub fn profile_columns(
    py: Python<'_>,
    mut columns: Vec<PyColumn>,
    name: String,
    max_rows: Option<usize>,
    config: Option<&PyProfilerConfig>,
    error_count: usize,
    row_count: Option<usize>,
    source_type: String,
    source_format: Option<String>,
    source_bytes: Option<u64>,
) -> PyResult<PyProfileReport> {
    let start = std::time::Instant::now();

    let options = config
        .map(PyProfilerConfig::analysis_options)
        .unwrap_or_default();
    let resolved_packs = options.effective_metric_packs();
    let packs = resolved_packs.as_deref();
    let skip_statistics = !MetricPack::include_statistics(packs);
    let skip_patterns = !MetricPack::include_patterns(packs);
    let include_quality = MetricPack::include_quality(packs);
    let locale = options.locale();
    let semantic_hints = options.semantic_hints().clone();

    let effective_max_rows =
        max_rows.or_else(|| config.and_then(|c| c.max_rows.map(|v| v as usize)));

    // This function is reachable from Python without going through `dp.profile`,
    // so ragged input must raise rather than panic across the FFI boundary --
    // and a short first column must not silently truncate the rest.
    let source_rows = match columns.first() {
        Some((_, cells)) => cells.len(),
        // No columns: the cells cannot carry a row count, so the caller states it.
        None => row_count.unwrap_or(0),
    };
    if let Some((name, cells)) = columns.iter().find(|(_, c)| c.len() != source_rows) {
        return Err(PyValueError::new_err(format!(
            "profile_columns: every column must have the same number of cells; \
             column {name:?} has {}, expected {source_rows}",
            cells.len()
        )));
    }
    if let Some(stated) = row_count
        && !columns.is_empty()
        && stated != source_rows
    {
        return Err(PyValueError::new_err(format!(
            "profile_columns: row_count is {stated} but the columns hold {source_rows} cells each"
        )));
    }

    // Backstop: the Python transport wrappers reject collisions with a precise
    // source label, but this entry is reachable directly, so keep the invariant
    // (one profile per name, never merged or shadowed) enforced here too.
    let column_names: Vec<String> = columns.iter().map(|(n, _)| n.clone()).collect();
    dataprof::validate_unique_column_names(&column_names, "columns")
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

    // Source metadata describes the whole materialised input, not just the
    // columns selected for analysis. Capture it before projection filters the
    // owned transport vector.
    let source_memory_bytes: u64 = columns
        .iter()
        .flat_map(|(_, cells)| cells.iter())
        .flatten()
        .map(|value| value.len() as u64)
        .sum();

    if let Some(indices) = options
        .column_indices(&column_names)
        .map_err(|error| PyValueError::new_err(error.to_string()))?
    {
        let selected = indices.into_iter().collect::<HashSet<_>>();
        columns = columns
            .into_iter()
            .enumerate()
            .filter_map(|(index, column)| selected.contains(&index).then_some(column))
            .collect();
    }

    let num_rows = effective_max_rows
        .map(|cap| cap.min(source_rows))
        .unwrap_or(source_rows);
    let truncated = num_rows < source_rows;
    let num_cols = columns.len();

    // Analysis is pure Rust over owned data, so the GIL buys us nothing here.
    let (column_profiles, sample_columns, row_duplicates, row_completeness) = py.detach(|| {
        let mut profiles = Vec::with_capacity(num_cols);
        let mut samples = std::collections::HashMap::new();

        // Full-stream duplicate-row tracking over the row-aligned input,
        // using the same length-prefixed signature encoding as the file
        // engines. Null cells contribute an empty value, so files and
        // ad-hoc inputs holding the same data agree on duplicate counts.
        let mut row_tracker = RowUniquenessTracker::default();
        let mut completeness_tracker = RowCompletenessTracker::default();
        if num_cols > 0 {
            use std::fmt::Write as _;
            for row_index in 0..num_rows {
                let mut row_signature = String::new();
                let mut row_has_null = false;
                for (_, cells) in &columns {
                    let value = cells[row_index].as_deref().unwrap_or("");
                    let _ = write!(row_signature, "{}:", value.len());
                    row_signature.push_str(value);
                    // A missing cell renders empty, which `is_null_like_token`
                    // already treats as null — the same rule that produces
                    // `null_count` below.
                    row_has_null |= is_null_like_token(value);
                }
                row_tracker.observe(row_signature);
                completeness_tracker.observe(row_has_null);
            }
        }

        for (col_name, cells) in &columns {
            let present: Vec<String> = cells[..num_rows]
                .iter()
                .flatten()
                .filter(|v| !is_null_like_token(v))
                .cloned()
                .collect();
            let null_count = num_rows - present.len();
            let unique_count = present.iter().collect::<HashSet<_>>().len();

            let data_type = if semantic_hints.is_identifier_column(col_name) {
                dataprof::DataType::Identifier
            } else {
                infer_type(&present)
            };

            profiles.push(build_column_profile(ColumnProfileInput {
                name: col_name.clone(),
                data_type,
                total_count: num_rows,
                null_count,
                unique_count: Some(unique_count),
                // Distinct values are counted with an exact HashSet above.
                unique_count_is_approximate: Some(false),
                sample_values: &present,
                text_lengths: None,
                boolean_counts: None,
                skip_statistics,
                skip_patterns,
                locale,
                // `present` is the full column, so the sample-derived stats
                // are already exact.
                exact_numeric: None,
                exact_date_matches: None,
            }));

            if include_quality {
                samples.insert(col_name.clone(), present);
            }
        }

        (
            profiles,
            samples,
            row_tracker.summary(),
            completeness_tracker.summary(),
        )
    });

    let mut exec = ExecutionMetadata::new(num_rows, num_cols, start.elapsed().as_millis())
        .with_engine("columnar")
        // Malformed records skipped upstream (e.g. tolerant JSONL byte parsing)
        // are reported here so a partial profile is distinguishable from a clean one.
        .with_error_count(error_count);
    if truncated {
        exec = exec.with_truncation(TruncationReason::MaxRows(
            effective_max_rows.unwrap_or(0) as u64
        ));
    }

    let data_source = if source_type == "bytes" {
        let format = match source_format.as_deref() {
            Some("csv") => FileFormat::Csv,
            Some("json") => FileFormat::Json,
            Some("jsonl") => FileFormat::Jsonl,
            Some("parquet") => FileFormat::Parquet,
            other => FileFormat::Unknown(other.unwrap_or_default().to_string()),
        };
        // `memory_bytes` is the decoded cells added up, which is not the size of
        // the buffer they were parsed from -- delimiters, quoting and encoding
        // all move it, and for Parquet it is off by the compression ratio.
        // Reporting it as the buffer size would be a plausible wrong number.
        let size_bytes = source_bytes.ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "source_type='bytes' requires source_bytes, the length of the original buffer",
            )
        })?;
        DataSource::Bytes {
            name,
            format,
            size_bytes,
        }
    } else {
        DataSource::DataFrame {
            name,
            source_library: DataFrameLibrary::Custom("python".to_string()),
            row_count: num_rows,
            column_count: num_cols,
            memory_bytes: Some(source_memory_bytes),
        }
    };

    let mut assembler = ReportAssembler::new(data_source, exec)
        .columns(column_profiles)
        .with_row_duplicates(row_duplicates)
        .with_row_completeness(row_completeness)
        .with_analysis_options(&options);

    if include_quality {
        assembler = assembler.with_quality_data(sample_columns);
    }

    let report = assembler.build();
    super::errors::validate_report_hints(&report, &semantic_hints, include_quality)?;

    Ok(PyProfileReport::new(report))
}
