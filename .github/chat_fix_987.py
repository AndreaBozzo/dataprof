from pathlib import Path
import textwrap

BASE = "4232d9211afa4ab7ce2b0781258065cabaac92b9"


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected one match, found {count}")
    return text.replace(old, new, 1)


# _inputs.py: preserve the existing Arrow path, but return fallback container positions.
path = Path("python/dataprof/_inputs.py")
text = path.read_text()
start = text.index("def _pandas_to_pyarrow_table(")
end = text.index("\n\ndef _columns_from_dict", start)
replacement = textwrap.dedent('''\
def _pandas_to_pyarrow_table(df: _Any, pyarrow: _Any) -> _Any:
    """Convert a pandas DataFrame to Arrow, including the mixed-object fallback."""
    table, _ = _pandas_to_pyarrow_table_with_containers(df, pyarrow)
    return table


def _pandas_to_pyarrow_table_with_containers(
    df: _Any, pyarrow: _Any
) -> tuple[_Any, list[list[int]] | None]:
    """Convert pandas to Arrow and retain container provenance for fallback cells.

    The optional row-index lists align with Arrow column order and are emitted only
    when the mixed object/category fallback stringified a dict or list. They let row
    duplicate tracking distinguish that container from a user string with the same
    JSON spelling while every column still follows the normal Arrow analysis path.
    """
    try:
        return pyarrow.Table.from_pandas(df), None
    except (pyarrow.ArrowInvalid, pyarrow.ArrowTypeError):
        if hasattr(df.columns, "is_unique") and not df.columns.is_unique:
            raise

        failing_cols: list[tuple[int, _Any]] = []
        for i in range(len(df.columns)):
            col = df.iloc[:, i]
            if col.dtype != object and getattr(col.dtype, "name", None) != "category":
                continue
            name = df.columns[i]
            try:
                pyarrow.Array.from_pandas(col)
            except (pyarrow.ArrowInvalid, pyarrow.ArrowTypeError):
                failing_cols.append((i, name))

        if not failing_cols:
            raise

        container_rows: list[list[int]] = [[] for _ in range(len(df.columns))]
        df_fixed = df.copy(deep=False)
        for column_index, name in failing_cols:
            col = df.iloc[:, column_index]
            try:
                converted: list[str | None] = []
                values = zip(col.isna(), col) if hasattr(col, "isna") else (
                    (False, value) for value in col
                )
                for row_index, (is_missing, value) in enumerate(values):
                    if is_missing:
                        converted.append(None)
                        continue
                    if _is_container(value):
                        container_rows[column_index].append(row_index)
                    converted.append(_cell_to_str(value))
                df_fixed[name] = converted
            except Exception as exc:
                raise ValueError(
                    f"pandas DataFrame: column {name!r} could not be converted for profiling: {exc}"
                ) from exc

        try:
            table = pyarrow.Table.from_pandas(df_fixed)
        except Exception as exc:
            for _, name in failing_cols:
                try:
                    pyarrow.Array.from_pandas(df_fixed[name])
                except (pyarrow.ArrowInvalid, pyarrow.ArrowTypeError) as col_exc:
                    raise ValueError(
                        f"pandas DataFrame: column {name!r} could not be converted for profiling: "
                        f"{col_exc}"
                    ) from col_exc
            raise ValueError(f"pandas DataFrame conversion failed: {exc}") from exc

        if table.num_columns > len(container_rows):
            container_rows.extend([[] for _ in range(table.num_columns - len(container_rows))])
        if table.num_columns != len(container_rows):
            raise ValueError("pandas DataFrame conversion changed the data-column count unexpectedly")

        return table, container_rows if any(container_rows) else None
''')
path.write_text(text[:start] + replacement + text[end:])


# Small row-only adapter: reuse BatchRowTracker on a shadow batch tagged with provenance.
Path("crates/dataprof-parquet/src/row_provenance.rs").write_text(textwrap.dedent('''\
use std::sync::Arc;

use anyhow::{Result, bail};
use arrow::array::{Array, ArrayRef, BooleanArray, StructArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use dataprof_metrics::RowDuplicateSummary;

use crate::record_batch_analyzer::{BatchRowTracker, DEFAULT_EXACT_SETS_BUDGET};

/// Duplicate-row tracker that adds pandas container provenance without changing columns.
///
/// For row equality only, affected fallback string columns are represented as
/// `(value, was_container)` structs and fed to the existing canonical batch tracker.
/// The original RecordBatch remains untouched for every column-level metric.
#[derive(Default)]
pub struct ContainerProvenanceRowTracker {
    tracker: BatchRowTracker,
}

impl ContainerProvenanceRowTracker {
    /// Observe one analyzed batch using global source row positions for containers.
    pub fn observe_batch(
        &mut self,
        batch: &RecordBatch,
        container_rows: &[Vec<usize>],
        row_offset: usize,
    ) -> Result<()> {
        if container_rows.len() != batch.num_columns() {
            bail!(
                "container provenance has {} columns for an Arrow batch with {} columns",
                container_rows.len(),
                batch.num_columns()
            );
        }

        let schema = batch.schema();
        let mut fields = Vec::with_capacity(batch.num_columns());
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

        for (column_index, column) in batch.columns().iter().enumerate() {
            let field = schema.field(column_index);
            let rows = &container_rows[column_index];
            if rows.is_empty() {
                fields.push(field.clone());
                columns.push(column.clone());
                continue;
            }

            let flags = BooleanArray::from_iter_values(
                (0..batch.num_rows())
                    .map(|row_index| rows.binary_search(&(row_offset + row_index)).is_ok()),
            );
            let struct_fields = Fields::from(vec![
                Arc::new(Field::new("value", column.data_type().clone(), true)),
                Arc::new(Field::new("was_container", ArrowDataType::Boolean, false)),
            ]);
            let tagged = StructArray::new(
                struct_fields.clone(),
                vec![column.clone(), Arc::new(flags)],
                column.logical_nulls(),
            );
            fields.push(Arc::new(Field::new(
                field.name(),
                ArrowDataType::Struct(struct_fields),
                field.is_nullable(),
            )));
            columns.push(Arc::new(tagged));
        }

        let shadow = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
        self.tracker.observe_batch(&shadow);
        if self.tracker.exact_bytes() > DEFAULT_EXACT_SETS_BUDGET {
            self.tracker
                .bound_exact_sets(&mut [], DEFAULT_EXACT_SETS_BUDGET);
        }
        Ok(())
    }

    /// Return duplicate-row counts from the provenance-aware signatures.
    pub fn summary(&self) -> Option<RowDuplicateSummary> {
        self.tracker.summary()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    fn batch(values: &[&str]) -> RecordBatch {
        RecordBatch::try_from_iter(vec![(
            "c",
            Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
        )])
        .unwrap()
    }

    #[test]
    fn container_flag_changes_row_equality_across_batches() {
        let provenance = vec![vec![1usize]];
        let mut tracker = ContainerProvenanceRowTracker::default();
        tracker.observe_batch(&batch(&[r#"{"x":1}"#]), &provenance, 0).unwrap();
        tracker.observe_batch(&batch(&[r#"{"x":1}"#]), &provenance, 1).unwrap();

        let summary = tracker.summary().unwrap();
        assert_eq!(summary.rows_checked, 2);
        assert_eq!(summary.duplicate_rows, 0);
    }

    #[test]
    fn equal_containers_and_equal_strings_still_duplicate() {
        let provenance = vec![vec![0usize, 1usize]];
        let mut tracker = ContainerProvenanceRowTracker::default();
        tracker
            .observe_batch(
                &batch(&[r#"{"x":1}"#, r#"{"x":1}"#, r#"{"x":1}"#, r#"{"x":1}"#]),
                &provenance,
                0,
            )
            .unwrap();
        assert_eq!(tracker.summary().unwrap().duplicate_rows, 2);
    }
}
'''))


# Register the helper module.
path = Path("crates/dataprof-parquet/src/lib.rs")
text = path.read_text()
text = replace_once(
    text,
    '#[cfg(feature = "arrow")]\npub mod record_batch_analyzer;\n',
    '#[cfg(feature = "arrow")]\npub mod record_batch_analyzer;\n\n#[cfg(feature = "arrow")]\nmod row_provenance;\n',
    "lib module",
)
export = textwrap.dedent('''\
#[cfg(feature = "arrow")]
pub use record_batch_analyzer::{
    RecordBatchAnalyzer, data_type_from_arrow_type, logical_arrow_type, sampling_can_decide_type,
};
''')
text = replace_once(
    text,
    export,
    export + '\n#[cfg(feature = "arrow")]\npub use row_provenance::ContainerProvenanceRowTracker;\n',
    "lib export",
)
path.write_text(text)


# Python binding: only the duplicate-row summary uses the provenance-aware tracker.
path = Path("crates/dataprof-python/src/arrow_export.rs")
text = path.read_text()
text = replace_once(
    text,
    'use dataprof_parquet::record_batch_analyzer::RecordBatchAnalyzer;',
    'use dataprof_parquet::{\n    ContainerProvenanceRowTracker, record_batch_analyzer::RecordBatchAnalyzer,\n};',
    "parquet import",
)
text = replace_once(
    text,
    '    let batches = convert_dataframe_to_batches(py, &df, &source_library)?;',
    '    let (batches, mut container_rows) = if matches!(&source_library, DataFrameLibrary::Pandas) {\n'
    '        convert_pandas_to_batches_with_containers(py, df.bind(py))?\n'
    '    } else {\n'
    '        (convert_dataframe_to_batches(py, &df, &source_library)?, None)\n'
    '    };',
    "dataframe conversion",
)

proj_start = text.index('    let batches = match options\n        .column_indices(&available_columns)')
proj_end = text.index('\n\n    let num_rows:', proj_start)
projection = '''    let selected_indices = options
        .column_indices(&available_columns)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    let batches = match selected_indices.as_deref() {
        Some(indices) => batches
            .into_iter()
            .map(|batch| {
                batch.project(indices).map_err(|error| {
                    PyRuntimeError::new_err(format!("Arrow projection failed: {error}"))
                })
            })
            .collect::<PyResult<Vec<_>>>()?,
        None => batches,
    };
    if let (Some(rows), Some(indices)) = (container_rows.take(), selected_indices.as_deref()) {
        container_rows = Some(indices.iter().map(|&index| rows[index].clone()).collect());
    }'''
text = text[:proj_start] + projection + text[proj_end:]

old_loop = (
    '    let mut analyzer = RecordBatchAnalyzer::new().with_semantic_hints(&semantic_hints);\n'
    '    for batch in &batches {\n'
    '        analyze_imported_batch(&mut analyzer, batch)?;\n'
    '    }\n'
)
new_loop = (
    '    let mut analyzer = RecordBatchAnalyzer::new().with_semantic_hints(&semantic_hints);\n'
    '    let mut provenance_tracker = container_rows\n'
    '        .as_ref()\n'
    '        .map(|_| ContainerProvenanceRowTracker::default());\n'
    '    let mut row_offset = 0usize;\n'
    '    for batch in &batches {\n'
    '        analyze_imported_batch(&mut analyzer, batch)?;\n'
    '        if let (Some(tracker), Some(rows)) =\n'
    '            (provenance_tracker.as_mut(), container_rows.as_deref())\n'
    '        {\n'
    '            tracker\n'
    '                .observe_batch(batch, rows, row_offset)\n'
    '                .map_err(crate::errors::analyzer_error_to_py)?;\n'
    '        }\n'
    '        row_offset += batch.num_rows();\n'
    '    }\n'
)
text = replace_once(text, old_loop, new_loop, "analyzer loop")

report_marker = '    // Create report with DataFrame data source\n'
row_summary = (
    '    let row_duplicates = match provenance_tracker.as_ref() {\n'
    '        Some(tracker) => tracker.summary(),\n'
    '        None => analyzer.row_duplicate_summary(),\n'
    '    };\n\n'
)
text = replace_once(text, report_marker, row_summary + report_marker, "report prelude")
text = replace_once(
    text,
    '    .with_row_duplicates(analyzer.row_duplicate_summary())',
    '    .with_row_duplicates(row_duplicates)',
    "row duplicate assembler",
)

pandas_start = text.index('fn convert_pandas_to_batches(')
pandas_end = text.index('\n\n/// Convert a polars DataFrame to its Arrow batches.', pandas_start)
pandas_conversion = textwrap.dedent('''\
fn convert_pandas_to_batches(
    py: Python<'_>,
    df: &Bound<'_, PyAny>,
) -> PyResult<Vec<RecordBatch>> {
    let (batches, _) = convert_pandas_to_batches_with_containers(py, df)?;
    Ok(batches)
}

/// Convert pandas to Arrow batches and retain mixed-fallback container provenance.
fn convert_pandas_to_batches_with_containers(
    py: Python<'_>,
    df: &Bound<'_, PyAny>,
) -> PyResult<(Vec<RecordBatch>, Option<Vec<Vec<usize>>>)> {
    let pyarrow = py.import("pyarrow").map_err(|_| {
        PyRuntimeError::new_err(
            "pyarrow required for pandas DataFrames. Install with: pip install pyarrow",
        )
    })?;

    let inputs_mod = py.import("dataprof._inputs")?;
    let converted = inputs_mod
        .getattr("_pandas_to_pyarrow_table_with_containers")?
        .call1((df, &pyarrow))?;
    let (pa_table, container_rows): (Py<PyAny>, Option<Vec<Vec<usize>>>) =
        converted.extract()?;
    let batches = import_batches_from_pyarrow(py, pa_table.bind(py))?;

    if let Some(rows) = &container_rows {
        let column_count = batches.first().map_or(0, RecordBatch::num_columns);
        if rows.len() != column_count {
            return Err(PyRuntimeError::new_err(format!(
                "pandas container provenance has {} columns for an Arrow table with {} columns",
                rows.len(), column_count
            )));
        }
    }

    Ok((batches, container_rows))
}
''').rstrip()
text = text[:pandas_start] + pandas_conversion + text[pandas_end:]
path.write_text(text)


# Regression tests for both public entrypoints, typed-neighbor parity, and row limits.
Path("python/tests/test_pandas_container_identity.py").write_text(textwrap.dedent('''\
"""Pandas fallback preserves container identity without changing Arrow column analysis."""

from __future__ import annotations

import dataprof as dp
import pytest

pd = pytest.importorskip("pandas", reason="pandas is required for pandas DataFrame tests")
pa = pytest.importorskip("pyarrow", reason="pyarrow is required for Arrow import tests")


def _report(entrypoint: str, values: list[object], *, max_rows: int | None = None):
    """Profile one mixed object column through either public DataFrame entrypoint."""
    df = pd.DataFrame({"c": pd.Series(values, dtype=object)})
    if entrypoint == "profile":
        return dp.profile(df, max_rows=max_rows)
    from dataprof.interop import profile_dataframe
    return dp.ProfileReport(profile_dataframe(df, max_rows=max_rows))


@pytest.mark.parametrize("entrypoint", ["profile", "interop"])
def test_container_and_json_spelling_are_distinct_rows(entrypoint: str):
    """A real container must not equal a user string with its JSON spelling."""
    report = _report(entrypoint, [{"x": 1}, '{"x":1}', [1, 2], "[1,2]"])
    assert report.quality.uniqueness["duplicate_rows"] == 0


@pytest.mark.parametrize("entrypoint", ["profile", "interop"])
def test_equal_containers_and_equal_strings_still_duplicate(entrypoint: str):
    """Provenance tagging must preserve equality within each value kind."""
    report = _report(entrypoint, [{"x": 1}, {"x": 1}, '{"x":1}', '{"x":1}'])
    assert report.quality.uniqueness["duplicate_rows"] == 2


def test_arrow_compatible_container_column_needs_no_fallback_provenance():
    """Native Arrow containers stay on the unchanged fast path."""
    from dataprof._inputs import _pandas_to_pyarrow_table_with_containers
    df = pd.DataFrame({"c": pd.Series([[1, 2], [3, 4]], dtype=object)})
    table, container_rows = _pandas_to_pyarrow_table_with_containers(df, pa)
    assert container_rows is None
    assert pa.types.is_list(table["c"].type)


def test_container_fallback_preserves_native_neighbor_profile():
    """A fallback column must not force neighboring typed columns through text inference."""
    clean = dp.profile(pd.DataFrame({"id": [1, 2, 3, 4]}))["id"]
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "c": pd.Series([{"x": 1}, '{"x":1}', [1, 2], "[1,2]"], dtype=object),
        }
    )
    mixed = dp.profile(df)["id"]
    assert mixed.data_type == clean.data_type
    assert mixed.null_count == clean.null_count
    assert mixed.unique_count == clean.unique_count
    assert mixed.mean == clean.mean
    assert mixed.min == clean.min
    assert mixed.max == clean.max


@pytest.mark.parametrize("entrypoint", ["profile", "interop"])
def test_container_provenance_respects_max_rows(entrypoint: str):
    """Rows beyond max_rows must not influence provenance-aware duplicate counts."""
    report = _report(entrypoint, [{"x": 1}, '{"x":1}', {"x": 1}, '{"x":1}'], max_rows=2)
    assert report.rows == 2
    assert report.quality.uniqueness["duplicate_rows"] == 0
'''))
