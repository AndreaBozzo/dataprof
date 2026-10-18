"""Preserve pandas container identity when mixed-object Arrow fallback is needed."""

from __future__ import annotations as _annotations

from typing import Any as _Any

from ._dataprof import profile_columns as _profile_columns
from ._dataprof import profile_dataframe as _native_profile_dataframe
from ._inputs import _Column, _cell_to_str, _is_container


def _pandas_container_fallback_columns(df: _Any) -> list[_Column] | None:
    """Return container-aware columns only when pandas needs the mixed fallback.

    The normal pandas path stays Arrow-native. We switch to ``profile_columns``
    only when an object/category column contains a real dict/list *and* Arrow
    cannot represent that column without the #976 string fallback. In that one
    case, the column transport preserves the original container row indices so
    row signatures cannot confuse a container with a string spelling its JSON.
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
    """Profile a DataFrame, preserving container identity on pandas fallback."""
    columns = _pandas_container_fallback_columns(df)
    if columns is None:
        return _native_profile_dataframe(df, name, max_rows, config)
    return _profile_columns(columns, name, max_rows, config)


def _install_high_level_profile_dataframe() -> None:
    """Route ``dataprof.profile``'s DataFrame call through this adapter."""
    from . import _api

    _api._profile_dataframe = profile_dataframe
