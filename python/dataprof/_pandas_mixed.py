"""Preserve pandas container identity when mixed-object Arrow fallback is needed."""

from __future__ import annotations as _annotations

from typing import Any as _Any

from . import _dataprof as _native
from ._dataprof import profile_dataframe as _native_profile_dataframe
from ._inputs import _is_container, _pandas_to_pyarrow_table


def _effective_max_rows(max_rows: int | None, config: _Any) -> int | None:
    """Resolve the row cap the native DataFrame binding will apply."""
    if max_rows is not None:
        return max_rows
    if config is None:
        return None
    return getattr(config, "max_rows", None)


def _pandas_container_fallback_table(
    df: _Any,
    row_limit: int | None = None,
) -> _Any:
    """Mark fallback containers without changing native Arrow row identity.

    Only object/category columns needing #976's fallback get a boolean marker
    distinguishing a real dict/list from a string spelling its JSON. Every
    original Arrow column remains unchanged, so the duplicate pass preserves
    native numeric, temporal, and nested equality in neighboring columns.
    ``row_limit`` bounds marker materialization and the duplicate scan. Arrow
    conversion still considers the full source, as the native profiler does.
    """
    source_module = type(df).__module__ or ""
    if not source_module.startswith("pandas"):
        return None

    try:
        import pyarrow as pa
    except ImportError:
        # Preserve the native binding's canonical "pyarrow required" error.
        return None

    markers: list[list[bool]] = []
    for index in range(len(df.columns)):
        col = df.iloc[:, index]
        if col.dtype != object and getattr(col.dtype, "name", None) != "category":
            continue

        analyzed = col if row_limit is None else col.iloc[:row_limit]
        if not any(_is_container(value) for value in analyzed):
            continue

        try:
            pa.Array.from_pandas(col)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            markers.append([_is_container(value) for value in analyzed])

    if not markers:
        return None

    table = _pandas_to_pyarrow_table(df, pa)
    if row_limit is not None:
        table = table.slice(0, min(row_limit, table.num_rows))
    names = set(table.column_names)
    for index, marker in enumerate(markers):
        name = f"__dataprof_container_{index}"
        while name in names:
            name = "_" + name
        names.add(name)
        table = table.append_column(name, pa.array(marker, type=pa.bool_()))
    return table


def profile_dataframe(
    df: _Any,
    name: str = "dataframe",
    max_rows: int | None = None,
    config: _Any = None,
) -> _Any:
    """Profile a DataFrame while preserving mixed-container row identity."""
    # The native Arrow path owns the report. In particular, typed neighbouring
    # columns keep their Arrow types and statistics instead of being converted
    # to strings merely because one object column needed #976's fallback.
    report = _native_profile_dataframe(df, name, max_rows, config)

    quality = report.quality
    if quality is None or quality.uniqueness is None:
        # Quality was deselected, uniqueness was not requested, or column
        # projection withheld whole-row metrics. There is nothing to repair.
        return report

    row_limit = _effective_max_rows(max_rows, config)
    table = _pandas_container_fallback_table(df, row_limit)
    if table is None:
        return report

    # Only row signatures are required. Avoid running a second complete
    # profile (per-column inference, statistics, and quality assessment).
    # Full-row uniqueness was withheld by the native path for projections.
    counts = _native._count_container_aware_duplicates(table)
    if counts is None:
        return report

    _native._replace_row_duplicates(report, *counts)
    return report


def _install_high_level_profile_dataframe() -> None:
    """Route ``dataprof.profile``'s DataFrame call through this adapter."""
    from . import _api

    setattr(_api, "_profile_dataframe", profile_dataframe)
