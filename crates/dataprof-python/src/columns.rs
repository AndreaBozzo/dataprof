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
    DataFrameLibrary, DataSource, ExecutionMetadata, MetricPack, TruncationReason, infer_type,
    is_null_like_token,
};
use dataprof_runtime::{ColumnProfileInput, ReportAssembler, build_column_profile};

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
/// Raises `ValueError` when the columns do not all have the same length.
#[pyfunction]
#[pyo3(signature = (columns, name = "dataframe".to_string(), max_rows = None, config = None))]
pub fn profile_columns(
    py: Python<'_>,
    columns: Vec<PyColumn>,
    name: String,
    max_rows: Option<usize>,
    config: Option<&PyProfilerConfig>,
) -> PyResult<PyProfileReport> {
    let start = std::time::Instant::now();

    let resolved_packs = config.and_then(PyProfilerConfig::effective_metric_packs);
    let packs = resolved_packs.as_deref();
    let skip_statistics = !MetricPack::include_statistics(packs);
    let skip_patterns = !MetricPack::include_patterns(packs);
    let include_quality = MetricPack::include_quality(packs);
    let locale = config.and_then(|c| c.locale.as_deref());
    let semantic_hints = config.map(|c| c.semantic_hints()).unwrap_or_default();

    let effective_max_rows =
        max_rows.or_else(|| config.and_then(|c| c.max_rows.map(|v| v as usize)));

    // This function is reachable from Python without going through `dp.profile`,
    // so ragged input must raise rather than panic across the FFI boundary --
    // and a short first column must not silently truncate the rest.
    let source_rows = columns.first().map(|(_, cells)| cells.len()).unwrap_or(0);
    if let Some((name, cells)) = columns.iter().find(|(_, c)| c.len() != source_rows) {
        return Err(PyValueError::new_err(format!(
            "profile_columns: every column must have the same number of cells; \
             column {name:?} has {}, expected {source_rows}",
            cells.len()
        )));
    }

    let num_rows = effective_max_rows
        .map(|cap| cap.min(source_rows))
        .unwrap_or(source_rows);
    let truncated = num_rows < source_rows;
    let num_cols = columns.len();

    // Analysis is pure Rust over owned data, so the GIL buys us nothing here.
    let (column_profiles, sample_columns) = py.detach(|| {
        let mut profiles = Vec::with_capacity(num_cols);
        let mut samples = std::collections::HashMap::new();

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
            }));

            if include_quality {
                samples.insert(col_name.clone(), present);
            }
        }

        (profiles, samples)
    });

    let mut exec = ExecutionMetadata::new(num_rows, num_cols, start.elapsed().as_millis());
    if truncated {
        exec = exec.with_truncation(TruncationReason::MaxRows(
            effective_max_rows.unwrap_or(0) as u64
        ));
    }

    // Rough, but honest: the strings we were handed are the whole materialised input.
    let memory_bytes: u64 = columns
        .iter()
        .flat_map(|(_, cells)| cells.iter())
        .flatten()
        .map(|v| v.len() as u64)
        .sum();

    let mut assembler = ReportAssembler::new(
        DataSource::DataFrame {
            name,
            source_library: DataFrameLibrary::Custom("python".to_string()),
            row_count: num_rows,
            column_count: num_cols,
            memory_bytes: Some(memory_bytes),
        },
        exec,
    )
    .columns(column_profiles);

    if include_quality {
        assembler = assembler
            .with_quality_data(sample_columns)
            .with_semantic_hints(semantic_hints);
        if let Some(dims) = config.and_then(|c| c.quality_dimensions.clone()) {
            assembler = assembler.with_requested_dimensions(dims);
        }
    } else {
        assembler = assembler.skip_quality();
    }

    Ok(PyProfileReport::new(assembler.build()))
}
