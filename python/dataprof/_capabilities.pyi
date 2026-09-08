"""Type stubs for _capabilities."""

from __future__ import annotations

from dataclasses import dataclass

@dataclass(frozen=True, slots=True)
class Capabilities:
    """Immutable snapshot of features available in this installation."""

    version: str
    local_csv: bool
    local_json: bool
    local_jsonl: bool
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

def capabilities() -> Capabilities:
    """Return a side-effect-free snapshot of installed dataprof capabilities."""
    ...
