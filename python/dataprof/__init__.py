"""DataProf — High-performance data profiling library."""

from __future__ import annotations

import html as _html
import json
import pathlib
import warnings
from typing import Any

from ._dataprof import (
    ColumnProfile,
    DataQualityMetrics,
    # Configuration
    ProfilerConfig,
    ProgressEvent,
    # Arrow interop
    RecordBatch,
    RowCountEstimate,
    # Sampling, stop conditions, progress
    SamplingStrategy,
    # Partial analysis
    SchemaResult,
    StopCondition,
    __version__,
    infer_schema,
    quick_row_count,
)
from ._dataprof import (
    # Result types (internal names)
    ProfileReport as _RustProfileReport,
)
from ._dataprof import (
    # Internal dispatch targets
    analyze_file as _analyze_file,
)
from ._dataprof import (
    profile_arrow as _profile_arrow,
)
from ._dataprof import (
    profile_dataframe as _profile_dataframe,
)

__all__ = [
    "profile",
    "ProfileReport",
    "ProfilerConfig",
    "ColumnProfile",
    "DataQualityMetrics",
    "SamplingStrategy",
    "StopCondition",
    "ProgressEvent",
    "infer_schema",
    "quick_row_count",
    "SchemaResult",
    "RowCountEstimate",
    "RecordBatch",
    "__version__",
]


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
    on_progress: object | None = None,
    progress_interval_ms: int | None = None,
    quality_dimensions: list[str] | None = None,
) -> ProfileReport:
    """Profile a data source and return a report.

    Accepts file paths (str/Path), pandas DataFrames, polars DataFrames,
    or any object implementing the Arrow PyCapsule protocol.

    Args:
        source: Data source to profile.
        engine: Engine to use ("auto", "incremental", "columnar").
        chunk_size: Fixed chunk size for streaming (None = adaptive).
        memory_limit_mb: Memory limit in MB.
        format: Override format detection ("csv", "json", "jsonl", "parquet").
        max_rows: Maximum rows to process before stopping.
        name: Name for DataFrame sources in the report.
        csv_delimiter: Single-character CSV delimiter (default: comma).
        csv_flexible: Allow variable-length CSV records.
        sampling: Sampling strategy (e.g. SamplingStrategy.random(1000)).
        stop_condition: Early stop condition (e.g. StopCondition.max_rows(5000)).
            Cannot be used together with max_rows.
        on_progress: Callable receiving ProgressEvent objects during profiling.
            Only effective with engine="incremental".
        progress_interval_ms: Minimum interval between progress events in ms
            (default: 500).
        quality_dimensions: List of ISO 25012 quality dimensions to evaluate.
            Valid values: "completeness", "consistency", "uniqueness",
            "accuracy", "timeliness". None = all dimensions (default).

    Returns:
        ProfileReport with analysis results and quality metrics.
    """
    # File path — build config and delegate to Rust
    if isinstance(source, (str, pathlib.PurePath)):
        config = ProfilerConfig(
            engine=engine,
            chunk_size=chunk_size,
            memory_limit_mb=memory_limit_mb,
            format=format,
            max_rows=max_rows,
            csv_delimiter=csv_delimiter,
            csv_flexible=csv_flexible,
            sampling=sampling,
            stop_condition=stop_condition,
            on_progress=on_progress,
            progress_interval_ms=progress_interval_ms,
            quality_dimensions=quality_dimensions,
        )
        rust_report = _analyze_file(str(source), config)
        return ProfileReport(rust_report)

    # DataFrame/Arrow paths — most config kwargs are file-only; max_rows is supported
    _file_only_kwargs = {
        "engine": engine != "auto",
        "chunk_size": chunk_size is not None,
        "memory_limit_mb": memory_limit_mb is not None,
        "format": format is not None,
        "csv_delimiter": csv_delimiter is not None,
        "csv_flexible": csv_flexible is not None,
        "sampling": sampling is not None,
        "stop_condition": stop_condition is not None,
        "on_progress": on_progress is not None,
        "progress_interval_ms": progress_interval_ms is not None,
    }

    def _warn_if_config_ignored():
        ignored = [k for k, v in _file_only_kwargs.items() if v]
        if ignored:
            warnings.warn(
                f"Config kwargs {ignored} are ignored for DataFrame/Arrow sources. "
                "These options only apply to file paths.",
                stacklevel=3,
            )

    # DataFrame detection via module name
    source_module = type(source).__module__ or ""

    if source_module.startswith("pandas"):
        _warn_if_config_ignored()
        rust_report = _profile_dataframe(source, name or "dataframe", max_rows)
        return ProfileReport(rust_report)

    if source_module.startswith("polars"):
        _warn_if_config_ignored()
        rust_report = _profile_dataframe(source, name or "dataframe", max_rows)
        return ProfileReport(rust_report)

    # PyArrow or any Arrow PyCapsule-compatible object
    if hasattr(source, "__arrow_c_array__"):
        _warn_if_config_ignored()
        rust_report = _profile_arrow(source, name or "arrow_data", max_rows)
        return ProfileReport(rust_report)

    raise TypeError(
        f"Unsupported source type: {type(source).__module__}.{type(source).__name__}. "
        "Expected a file path (str/Path), pandas DataFrame, polars DataFrame, "
        "or an object implementing the Arrow PyCapsule protocol."
    )


class ProfileReport:
    """High-level wrapper around the Rust ProfileReport with export methods."""

    def __init__(self, report: _RustProfileReport):
        self._report = report

    # -- Property accessors --

    @property
    def source(self) -> str:
        return self._report.source

    @property
    def source_type(self) -> str:
        return self._report.source_type

    @property
    def rows(self) -> int:
        return self._report.rows_processed

    @property
    def columns(self) -> int:
        return self._report.columns_detected

    @property
    def column_profiles(self) -> list:
        return self._report.column_profiles

    @property
    def quality_score(self) -> float | None:
        return self._report.quality_score

    @property
    def quality(self) -> DataQualityMetrics | None:
        return self._report.quality

    @property
    def execution_time_ms(self) -> int:
        return self._report.scan_time_ms

    @property
    def throughput(self) -> float | None:
        return self._report.throughput_rows_sec

    @property
    def memory_peak_mb(self) -> float | None:
        return self._report.memory_peak_mb

    @property
    def truncation_reason(self) -> str | None:
        return self._report.truncation_reason

    @property
    def source_exhausted(self) -> bool:
        return self._report.source_exhausted

    @property
    def sampling_applied(self) -> bool:
        return self._report.sampling_applied

    @property
    def sampling_ratio(self) -> float | None:
        return self._report.sampling_ratio

    # -- Export methods --

    def to_dict(self) -> dict:
        """Convert the report to a nested Python dict."""
        cols = []
        for col in self._report.column_profiles:
            col_data = {
                "name": col.name,
                "data_type": col.data_type,
                "total_count": col.total_count,
                "null_count": col.null_count,
                "null_percentage": col.null_percentage,
                "unique_count": col.unique_count,
                "uniqueness_ratio": col.uniqueness_ratio,
            }
            if col.min is not None:
                col_data["stats"] = {
                    k: v
                    for k, v in {
                        "min": col.min,
                        "max": col.max,
                        "mean": col.mean,
                        "std_dev": col.std_dev,
                        "variance": col.variance,
                        "median": col.median,
                        "mode": col.mode,
                        "skewness": col.skewness,
                        "kurtosis": col.kurtosis,
                        "coefficient_of_variation": col.coefficient_of_variation,
                        "quartiles": col.quartiles,
                        "is_approximate": col.is_approximate,
                    }.items()
                    if v is not None
                }
            cols.append(col_data)

        quality_dict = None
        q = self._report.quality
        if q is not None:
            quality_dict = {
                "overall_score": q.overall_quality_score(),
            }
            comp = q.completeness
            if comp is not None:
                quality_dict["completeness"] = comp
            cons = q.consistency
            if cons is not None:
                quality_dict["consistency"] = cons
            uniq = q.uniqueness
            if uniq is not None:
                quality_dict["uniqueness"] = uniq
            acc = q.accuracy
            if acc is not None:
                quality_dict["accuracy"] = acc
            tim = q.timeliness
            if tim is not None:
                quality_dict["timeliness"] = tim

        return {
            "source": self._report.source,
            "source_type": self._report.source_type,
            "execution": {
                "rows_processed": self._report.rows_processed,
                "columns_detected": self._report.columns_detected,
                "scan_time_ms": self._report.scan_time_ms,
                "source_exhausted": self._report.source_exhausted,
                "truncation_reason": self._report.truncation_reason,
                "bytes_consumed": self._report.bytes_consumed,
                "throughput_rows_sec": self._report.throughput_rows_sec,
                "memory_peak_mb": self._report.memory_peak_mb,
                "error_count": self._report.error_count,
                "sampling_applied": self._report.sampling_applied,
                "sampling_ratio": self._report.sampling_ratio,
            },
            "columns": cols,
            "quality": quality_dict,
        }

    def to_json(self, indent: int = 2) -> str:
        """Export the report as a JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    def to_dataframe(self):
        """Convert column profiles to a pandas DataFrame.

        Requires pandas to be installed.
        """
        import pandas as pd

        records = []
        for col in self._report.column_profiles:
            record = {
                "name": col.name,
                "data_type": col.data_type,
                "total_count": col.total_count,
                "null_count": col.null_count,
                "null_percentage": col.null_percentage,
                "unique_count": col.unique_count,
                "uniqueness_ratio": col.uniqueness_ratio,
                "min": col.min,
                "max": col.max,
                "mean": col.mean,
                "std_dev": col.std_dev,
            }
            records.append(record)
        return pd.DataFrame(records)

    def save(self, path: str) -> ProfileReport:
        """Save the report to a JSON file.

        Args:
            path: File path (must end with .json).
        """
        if path.endswith(".json"):
            with open(path, "w", encoding="utf-8") as f:
                f.write(self.to_json())
        else:
            raise ValueError("Unsupported format. Use .json")
        return self

    def __repr__(self) -> str:
        qs = self.quality_score
        qs_str = f"{qs:.1f}%" if qs is not None else "N/A"
        return (
            f"ProfileReport(source='{self.source}', "
            f"rows={self.rows:,}, columns={self.columns}, "
            f"time={self.execution_time_ms}ms, quality={qs_str})"
        )

    def _repr_html_(self) -> str:
        """Rich HTML representation for Jupyter notebooks."""
        qs = self.quality_score
        qs_str = f"{qs:.1f}%" if qs is not None else "N/A"
        col_rows = ""
        for col in self._report.column_profiles:
            stats = ""
            if col.mean is not None:
                stats = f"mean={col.mean:.2f}"
            col_rows += (
                f"<tr><td><code>{_html.escape(col.name)}</code></td>"
                f"<td>{_html.escape(col.data_type)}</td>"
                f"<td>{col.total_count:,}</td>"
                f"<td>{col.null_percentage:.1f}%</td>"
                f"<td>{_html.escape(stats)}</td></tr>"
            )
        return (
            f"<div style='font-family: sans-serif'>"
            f"<h3>ProfileReport</h3>"
            f"<p><b>Source:</b> {_html.escape(self.source)} | "
            f"<b>Rows:</b> {self.rows:,} | "
            f"<b>Columns:</b> {self.columns} | "
            f"<b>Quality:</b> {qs_str} | "
            f"<b>Time:</b> {self.execution_time_ms}ms</p>"
            f"<table><tr><th>Column</th><th>Type</th><th>Count</th>"
            f"<th>Null %</th><th>Stats</th></tr>"
            f"{col_rows}</table></div>"
        )
