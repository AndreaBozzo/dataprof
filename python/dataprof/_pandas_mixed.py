"""Preserve pandas container identity when mixed-object Arrow fallback is needed."""

from __future__ import annotations as _annotations

from typing import Any as _Any

from . import _dataprof as _native
from ._dataprof import (
    profile_columns as _profile_columns,
    profile_dataframe as _native_profile_dataframe,
)
from ._inputs import _cell_to_str, _Column, _is_container


def _effective_max_rows(max_rows: int | None, config: _Any) -> int | None:
    """Resolve the row cap the native DataFrame binding will apply."""
    if max_rows is not None:
        return max_rows
    if config is None:
        return None
    return getattr(config, "max_rows", None)


def _pandas_container_fallback_columns(
    df: _Any,
    row_limit: int | None = None,
) -> list[_Column] | None:
    """Return container-aware rows only when pandas needs the mixed fallback.

    Arrow remains the source of every column profile. This helper is only a
    side-channel for full-row duplicate signatures when an object/category
    column contains a real dict/list *and* Arrow cannot represent that column
    without #976's string fallback. ``row_limit`` bounds the extra materialized
    pass to the rows the native profiler will actually analyze.
    """
    source_module = type(df).__module__ or ""
    if not source_module.startswith("pandas"):
        return None

    try:
        import pyarrow as pa
    except ImportError:
        # Preserve the native binding's canonical "pyarrow required" error.
        return None

    needs_container_fallback = False
    for index in range(len(df.columns)):
        col = df.iloc[:, index]
        if col.dtype != object and getattr(col.dtype, "name", None) != "category":
            continue

        missing = col.isna() if hasattr(col, "isna") else [False] * len(col)
        has_container = any(
            not bool(is_missing) and _is_container(value)
            for is_missing, value in zip(missing, col, strict=True)
        )
        if not has_container:
            continue

        try:
            pa.Array.from_pandas(col)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            needs_container_fallback = True
            break

    if not needs_container_fallback:
        return None

    columns: list[_Column] = []
    for index in range(len(df.columns)):
        raw_name = df.columns[index]
        name = str(raw_name)
        col = df.iloc[:, index]
        if row_limit is not None:
            col = col.iloc[:row_limit]
        try:
            missing = col.isna() if hasattr(col, "isna") else [False] * len(col)
            cells: list[str | None] = []
            containers: list[int] = []
            for row, (is_missing, value) in enumerate(zip(missing, col, strict=True)):
                if bool(is_missing):
                    cells.append(None)
                    continue
                if _is_container(value):
                    containers.append(row)
                cells.append(_cell_to_str(value))
            columns.append((name, cells, containers))
        except Exception as exc:
            raise ValueError(
                f"pandas DataFrame: column {raw_name!r} could not be converted for profiling: {exc}"
            ) from exc

    return columns


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
    columns = _pandas_container_fallback_columns(df, row_limit)
    if columns is None:
        return report

    # Only the duplicate summary is consumed from this pass. It intentionally
    # uses no caller config: columns were already row-limited above, and a
    # projection must never enter here because native uniqueness is withheld
    # for projected reports.
    identity_report = _profile_columns(columns, name, None, None)
    identity_quality = identity_report.quality
    if identity_quality is None:
        return report
    uniqueness = identity_quality.uniqueness
    if uniqueness is None:
        return report

    replace = getattr(_native, "_replace_row_duplicates")
    replace(
        report,
        uniqueness["duplicate_rows"],
        uniqueness["rows_checked"],
        uniqueness["duplicate_rows_approximate"],
    )
    return report


def _install_high_level_profile_dataframe() -> None:
    """Route ``dataprof.profile``'s DataFrame call through this adapter."""
    from . import _api

    setattr(_api, "_profile_dataframe", profile_dataframe)
