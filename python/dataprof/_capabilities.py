"""Feature discovery and optional Python dependency checks."""

from __future__ import annotations as _annotations

from dataclasses import dataclass as _dataclass
from importlib import util as _importlib_util

from ._dataprof import __version__, _compiled_capabilities


@_dataclass(frozen=True, slots=True)
class Capabilities:
    """Immutable snapshot of features available in this installation."""

    version: str
    local_csv: bool
    local_json: bool
    local_jsonl: bool
    #: Local Parquet, covering both file paths and byte buffers — they share one
    #: compiled reader, so no caller has to distinguish the two.
    local_parquet: bool
    pandas_interop: bool
    pandas_installed: bool
    polars_interop: bool
    polars_installed: bool
    arrow_interop: bool
    pyarrow_installed: bool
    async_streaming: bool
    url_profiling: bool
    remote_parquet: bool
    database: bool
    database_connectors: tuple[str, ...]


def _dependency_installed(module_name: str) -> bool:
    """Check availability without importing the optional dependency."""
    try:
        return _importlib_util.find_spec(module_name) is not None
    except (AttributeError, ImportError, ValueError):
        return False


def capabilities() -> Capabilities:
    """Return a side-effect-free snapshot of installed dataprof capabilities.

    Optional Python packages are discovered without importing them. Compiled
    async and database support reflects the feature flags of the native module.
    """
    database = bool(_compiled_capabilities.get("database", False))
    connectors = (
        tuple(
            name
            for name in ("postgres", "mysql", "sqlite")
            if _compiled_capabilities.get(name, False)
        )
        if database
        else ()
    )
    async_streaming = bool(_compiled_capabilities.get("async_streaming", False))

    return Capabilities(
        version=__version__,
        local_csv=True,
        local_json=True,
        local_jsonl=True,
        local_parquet=True,
        pandas_interop=True,
        pandas_installed=_dependency_installed("pandas"),
        polars_interop=True,
        polars_installed=_dependency_installed("polars"),
        arrow_interop=True,
        pyarrow_installed=_dependency_installed("pyarrow"),
        async_streaming=async_streaming,
        url_profiling=async_streaming,
        remote_parquet=bool(_compiled_capabilities.get("parquet_async", False)),
        database=database,
        database_connectors=connectors,
    )


def _require_pandas(feature: str):
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError(
            f"pandas is required to profile {feature}. "
            "Install it with: pip install dataprof[pandas]"
        ) from exc
    return pd


# Preserve the public import path for introspection and pickled API objects.
Capabilities.__module__ = "dataprof"
capabilities.__module__ = "dataprof"
