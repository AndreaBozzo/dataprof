"""Builder-style configuration over the profiling entry point."""

from __future__ import annotations as _annotations

from collections.abc import Callable as _Callable
from typing import Any as _Any

from ._api import profile
from ._dataprof import SamplingStrategy, StopCondition
from ._report import ProfileReport

# Stop-when shorthand strings for the Profiler builder
_STOP_SHORTHANDS: dict[str, _Callable[[], StopCondition]] = {
    "schema_stable": lambda: StopCondition.schema_stable(1000),
    "schema_inference": lambda: StopCondition.schema_inference(),
    "quality_sample": lambda: StopCondition.quality_sample(),
}

# Valid metric pack names
_VALID_METRIC_PACKS = {"schema", "statistics", "patterns", "quality"}


class Profiler:
    """Builder-style profiler configuration.

    Chainable methods accumulate settings; call ``.profile(source)`` to run.

    Example::

        report = dp.Profiler().engine("incremental").max_rows(5000).profile("data.csv")
        report = dp.Profiler().stop_when("schema_stable").profile(df)
        report = dp.Profiler().metrics(["quality"]).profile("data.csv")
    """

    def __init__(self) -> None:
        self._kwargs: dict[str, _Any] = {}

    def engine(self, engine: str) -> Profiler:
        """Set file profiling engine ("auto", "incremental", "columnar").

        "streaming" aliases "incremental"; "arrow" aliases "columnar".
        Names are case-insensitive. CSV execution metadata reports the selected
        canonical engine ("incremental" or "columnar").
        """
        self._kwargs["engine"] = engine
        return self

    def chunk_size(self, n: int) -> Profiler:
        """Set fixed chunk size for streaming (None = adaptive)."""
        self._kwargs["chunk_size"] = n
        return self

    def memory_limit_mb(self, mb: int) -> Profiler:
        """Set memory limit in MB."""
        self._kwargs["memory_limit_mb"] = mb
        return self

    def format(self, fmt: str) -> Profiler:
        """Override format detection ("csv", "json", "jsonl", "parquet")."""
        self._kwargs["format"] = fmt
        return self

    def max_rows(self, n: int) -> Profiler:
        """Set maximum rows to process."""
        self._kwargs["max_rows"] = n
        return self

    def name(self, name: str) -> Profiler:
        """Set name for DataFrame sources in the report."""
        self._kwargs["name"] = name
        return self

    def csv_delimiter(self, d: str) -> Profiler:
        """Set single-character CSV delimiter."""
        self._kwargs["csv_delimiter"] = d
        return self

    def csv_flexible(self, flexible: bool) -> Profiler:
        """Allow variable-length CSV records."""
        self._kwargs["csv_flexible"] = flexible
        return self

    def sampling(self, strategy: SamplingStrategy) -> Profiler:
        """Set sampling strategy."""
        self._kwargs["sampling"] = strategy
        return self

    def stop_condition(self, cond: StopCondition) -> Profiler:
        """Set early stop condition."""
        self._kwargs["stop_condition"] = cond
        return self

    def on_progress(self, cb: object) -> Profiler:
        """Set progress callback (engine="incremental" only).

        Files that finish faster than ``progress_interval_ms`` may emit only
        start and finish events.
        """
        self._kwargs["on_progress"] = cb
        return self

    def progress_interval_ms(self, ms: int) -> Profiler:
        """Set minimum interval between progress events in ms."""
        self._kwargs["progress_interval_ms"] = ms
        return self

    def quality_dimensions(self, dims: list[str]) -> Profiler:
        """Select quality dimensions to evaluate."""
        self._kwargs["quality_dimensions"] = dims
        return self

    def columns(self, columns: list[str]) -> Profiler:
        """Select top-level columns to profile.

        Any explicit selection withholds row-level completeness and uniqueness.
        """
        self._kwargs["columns"] = columns
        return self

    def stop_when(self, condition: StopCondition | str) -> Profiler:
        """Set stop condition from a StopCondition or a shorthand string.

        Shorthand strings: "schema_stable", "schema_inference", "quality_sample".
        """
        if isinstance(condition, str):
            factory = _STOP_SHORTHANDS.get(condition)
            if factory is None:
                raise ValueError(
                    f"Unknown stop_when shorthand: {condition!r}. "
                    f"Valid shorthands: {sorted(_STOP_SHORTHANDS)}"
                )
            condition = factory()
        self._kwargs["stop_condition"] = condition
        return self

    def locale(self, locale: str) -> Profiler:
        """Set locale for pattern detection (e.g. "IT", "US", "GB").

        Raises ValueError at profiling time if the tag names no supported
        locale; see :func:`profile` for the accepted spellings.
        """
        self._kwargs["locale"] = locale
        return self

    def positive_columns(self, columns: list[str]) -> Profiler:
        """Mark columns whose numeric values are expected to be non-negative."""
        self._kwargs["positive_columns"] = columns
        return self

    def identifier_columns(self, columns: list[str]) -> Profiler:
        """Mark columns to profile as semantic identifiers."""
        self._kwargs["identifier_columns"] = columns
        return self

    def temporal_columns(self, columns: list[str]) -> Profiler:
        """Mark columns whose values should contribute to timeliness metrics."""
        self._kwargs["temporal_columns"] = columns
        return self

    def metrics(self, packs: list[str]) -> Profiler:
        """Select metric packs to compute.

        Valid packs: "schema" (always included), "statistics", "patterns", "quality".
        Omitting a pack skips that category of computation entirely.
        """
        normalized_packs = [pack.lower() for pack in packs]
        unknown = set(normalized_packs) - _VALID_METRIC_PACKS
        if unknown:
            raise ValueError(
                f"Unknown metric packs: {sorted(unknown)}. "
                f"Valid packs: {sorted(_VALID_METRIC_PACKS)}"
            )
        self._kwargs["metrics"] = normalized_packs
        return self

    def profile(self, source: _Any) -> ProfileReport:
        """Profile the given source with accumulated settings.

        Accepts file paths (str/Path), pandas DataFrames, polars DataFrames,
        or any object implementing the Arrow PyCapsule protocol.
        """
        return profile(source, **self._kwargs)

    def __repr__(self) -> str:
        settings = ", ".join(f"{k}={v!r}" for k, v in self._kwargs.items())
        return f"Profiler({settings})"


# Preserve the public import path for introspection and pickled API objects.
Profiler.__module__ = "dataprof"
