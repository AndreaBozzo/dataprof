"""Type stubs for dataprof."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from os import PathLike
from types import ModuleType
from typing import Any

# Raw data types are provided by the compiled extension and re-exported here.
from ._dataprof import (
    ColumnProfile as ColumnProfile,
    DataQualityMetrics as DataQualityMetrics,
    Pattern as Pattern,
    ProfilerConfig as ProfilerConfig,
    ProgressEvent as ProgressEvent,
    RecordBatch as RecordBatch,
    RowCountEstimate as RowCountEstimate,
    SamplingStrategy as SamplingStrategy,
    SchemaResult as SchemaResult,
    StopCondition as StopCondition,
    StructureColumnSummary as StructureColumnSummary,
    StructureReport as StructureReport,
    __version__ as __version__,
)

# --- Primary API ---

def profile(
    source: Any,
    *,
    engine: str = "auto",
    chunk_size: int | None = None,
    memory_limit_mb: int | None = None,
    format: str | None = None,
    max_rows: int | None = None,
    name: str | None = None,
    csv_delimiter: str | None = None,
    csv_flexible: bool | None = None,
    sampling: SamplingStrategy | None = None,
    stop_condition: StopCondition | None = None,
    on_progress: Callable[[ProgressEvent], None] | None = None,
    progress_interval_ms: int | None = None,
    quality_dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
    locale: str | None = None,
    positive_columns: list[str] | None = None,
    identifier_columns: list[str] | None = None,
) -> ProfileReport:
    """Profile a data source (file path, DataFrame, or Arrow object)."""
    ...

def profile_file(
    path: str | PathLike[str],
    *,
    engine: str = "auto",
    chunk_size: int | None = None,
    memory_limit_mb: int | None = None,
    format: str | None = None,
    max_rows: int | None = None,
    csv_delimiter: str | None = None,
    csv_flexible: bool | None = None,
    sampling: SamplingStrategy | None = None,
    stop_condition: StopCondition | None = None,
    on_progress: Callable[[ProgressEvent], None] | None = None,
    progress_interval_ms: int | None = None,
    quality_dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
    locale: str | None = None,
    positive_columns: list[str] | None = None,
    identifier_columns: list[str] | None = None,
) -> ProfileReport:
    """Profile a file path with file-oriented options."""
    ...

def list_patterns(locale: str | None = None) -> list[dict[str, Any]]:
    """List supported pattern detectors."""
    ...

def infer_schema(path: str | PathLike[str]) -> SchemaResult:
    """Infer the schema of a file from a string or path-like object."""
    ...

def quick_row_count(path: str | PathLike[str]) -> RowCountEstimate:
    """Estimate or count rows from a string or path-like object."""
    ...

def analyze_structure(path: str | PathLike[str], max_rows: int | None = None) -> StructureReport:
    """Analyze file structure with a bounded, lightweight pass."""
    ...

def column_to_dict(col: ColumnProfile) -> dict[str, Any]:
    """Convert a ColumnProfile to the nested dict layout used in ``report.to_dict()['columns']``."""
    ...

class Profiler:
    """Builder-style profiler configuration.

    Chainable methods accumulate settings; call ``.profile(source)`` to run.

    Example::

        report = dp.Profiler().engine("incremental").max_rows(5000).profile("data.csv")
    """

    def __init__(self) -> None: ...
    def engine(self, engine: str) -> Profiler: ...
    def chunk_size(self, n: int) -> Profiler: ...
    def memory_limit_mb(self, mb: int) -> Profiler: ...
    def format(self, fmt: str) -> Profiler: ...
    def max_rows(self, n: int) -> Profiler: ...
    def name(self, name: str) -> Profiler: ...
    def csv_delimiter(self, d: str) -> Profiler: ...
    def csv_flexible(self, flexible: bool) -> Profiler: ...
    def sampling(self, strategy: SamplingStrategy) -> Profiler: ...
    def stop_condition(self, cond: StopCondition) -> Profiler: ...
    def on_progress(self, cb: Callable[[ProgressEvent], None]) -> Profiler: ...
    def progress_interval_ms(self, ms: int) -> Profiler: ...
    def quality_dimensions(self, dims: list[str]) -> Profiler: ...
    def stop_when(self, condition: StopCondition | str) -> Profiler: ...
    def metrics(self, packs: list[str]) -> Profiler: ...
    def locale(self, locale: str) -> Profiler: ...
    def positive_columns(self, columns: list[str]) -> Profiler: ...
    def identifier_columns(self, columns: list[str]) -> Profiler: ...
    def profile(self, source: Any) -> ProfileReport: ...

# --- Result Types ---

class ProfileReport:
    """High-level profiling report with export methods.

    Wraps the raw ``dataprof._dataprof.ProfileReport`` (or a dict-backed proxy
    for reloaded reports). Supports dict-like column access::

        report["column_name"]          # -> ColumnProfile
        "column_name" in report        # -> bool
        for name in report: ...        # iterate column names
        len(report)                    # number of columns
    """

    def __init__(self, report: Any) -> None: ...
    @property
    def source(self) -> str: ...
    @property
    def source_type(self) -> str: ...
    @property
    def rows(self) -> int: ...
    @property
    def columns(self) -> int: ...
    @property
    def column_profiles(self) -> dict[str, ColumnProfile]: ...
    @property
    def quality_score(self) -> float | None: ...
    @property
    def quality(self) -> DataQualityMetrics | None: ...
    @property
    def low_sample_warning(self) -> bool: ...
    @property
    def execution_time_ms(self) -> int: ...
    @property
    def throughput(self) -> float | None: ...
    @property
    def memory_peak_mb(self) -> float | None: ...
    @property
    def truncation_reason(self) -> str | None: ...
    @property
    def source_exhausted(self) -> bool: ...
    @property
    def sampling_applied(self) -> bool: ...
    @property
    def sampling_ratio(self) -> float | None: ...

    # Mapping protocol
    def __getitem__(self, key: str) -> ColumnProfile: ...
    def __contains__(self, key: object) -> bool: ...
    def __iter__(self) -> Iterator[str]: ...
    def __len__(self) -> int: ...

    # Export methods
    def to_dict(self) -> dict[str, Any]: ...
    def to_json(self, indent: int = 2) -> str: ...
    def to_dataframe(self) -> Any:
        """Column profiles as pandas DataFrame (all stats, rounded). Requires pandas."""
        ...
    def to_polars(self) -> Any:
        """Column profiles as polars DataFrame (all stats, rounded). Requires polars."""
        ...
    def to_arrow(self) -> Any:
        """Column profiles as PyArrow Table (all stats, rounded). Requires pyarrow."""
        ...
    def describe(self) -> Any:
        """Transposed summary (like pandas describe). Returns pandas DataFrame or dict."""
        ...
    def quality_summary(self) -> dict[str, Any]:
        """Single-row quality summary dict for easy aggregation."""
        ...
    def save(self, path: str) -> ProfileReport:
        """Save report to file (.json, .csv, or .parquet), returning self for chaining."""
        ...
    def to_html(self) -> str:
        """Standalone HTML representation (same as Jupyter's rich display)."""
        ...
    def to_markdown(self) -> str:
        """GitHub-flavored markdown table of the column profiles."""
        ...
    def to_llm_context(self, max_tokens: int = 1000, include_samples: bool = False) -> str:
        """Token-bounded, agent-oriented summary: shape, caveats, flags, schema, patterns.

        Tokens are estimated as ``ceil(len(text) / 4)``, not counted with a real
        tokenizer. Raw cell values are excluded by default; ``include_samples=True``
        only includes non-sensitive numeric extrema.
        """
        ...
    def compare(self, other: ProfileReport) -> dict[str, Any]:
        """Dict of quality/schema/null deltas versus another report."""
        ...
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ProfileReport:
        """Rebuild a read-only ProfileReport from a to_dict() mapping."""
        ...
    @classmethod
    def from_json(cls, text: str) -> ProfileReport:
        """Rebuild a read-only ProfileReport from a to_json() string."""
        ...
    @classmethod
    def load(cls, path: str | PathLike[str]) -> ProfileReport:
        """Reload a report from a .json file written by save()."""
        ...

# --- Exports ---

__all__ = [
    "profile",
    "profile_file",
    "Profiler",
    "ProfileReport",
    "ProfilerConfig",
    "ColumnProfile",
    "DataQualityMetrics",
    "SamplingStrategy",
    "StopCondition",
    "ProgressEvent",
    "list_patterns",
    "infer_schema",
    "quick_row_count",
    "analyze_structure",
    "SchemaResult",
    "RowCountEstimate",
    "StructureColumnSummary",
    "StructureReport",
    "RecordBatch",
    "asyncio",
    "__version__",
]

asyncio: ModuleType
