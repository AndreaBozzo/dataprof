"""DataProf — High-performance data profiling library."""

from __future__ import annotations

import json
import os
import pathlib
from datetime import datetime
from typing import Any, Optional, Union

from ._dataprof import (
    __version__,
    # Configuration
    ProfilerConfig,
    # Result types (internal names)
    ProfileReport as _RustProfileReport,
    ColumnProfile,
    DataQualityMetrics,
    # Partial analysis
    SchemaResult,
    RowCountEstimate,
    infer_schema,
    quick_row_count,
    # Arrow interop
    RecordBatch,
    # Internal dispatch targets
    analyze_file as _analyze_file,
    profile_dataframe as _profile_dataframe,
    profile_arrow as _profile_arrow,
)

__all__ = [
    "profile",
    "ProfileReport",
    "ProfilerConfig",
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
    chunk_size: Optional[int] = None,
    memory_limit_mb: Optional[int] = None,
    format: Optional[str] = None,
    max_rows: Optional[int] = None,
    name: Optional[str] = None,
    csv_delimiter: Optional[str] = None,
    csv_flexible: Optional[bool] = None,
) -> "ProfileReport":
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
        csv_delimiter: Single-character CSV delimiter.
        csv_flexible: Allow variable-length CSV records.

    Returns:
        ProfileReport with analysis results and quality metrics.
    """
    config = ProfilerConfig(
        engine=engine,
        chunk_size=chunk_size,
        memory_limit_mb=memory_limit_mb,
        format=format,
        max_rows=max_rows,
        csv_delimiter=csv_delimiter,
        csv_flexible=csv_flexible,
    )

    # File path
    if isinstance(source, (str, pathlib.PurePath)):
        rust_report = _analyze_file(str(source), config)
        return ProfileReport(rust_report)

    # DataFrame detection via module name
    source_module = type(source).__module__ or ""

    if source_module.startswith("pandas"):
        rust_report = _profile_dataframe(source, name or "dataframe")
        return ProfileReport(rust_report)

    if source_module.startswith("polars"):
        rust_report = _profile_dataframe(source, name or "dataframe")
        return ProfileReport(rust_report)

    # PyArrow or any Arrow PyCapsule-compatible object
    if hasattr(source, "__arrow_c_array__"):
        rust_report = _profile_arrow(source, name or "arrow_data")
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
    def quality_score(self) -> Optional[float]:
        return self._report.quality_score

    @property
    def quality(self) -> Optional[DataQualityMetrics]:
        return self._report.quality

    @property
    def execution_time_ms(self) -> int:
        return self._report.scan_time_ms

    @property
    def throughput(self) -> Optional[float]:
        return self._report.throughput_rows_sec

    @property
    def memory_peak_mb(self) -> Optional[float]:
        return self._report.memory_peak_mb

    @property
    def truncation_reason(self) -> Optional[str]:
        return self._report.truncation_reason

    @property
    def source_exhausted(self) -> bool:
        return self._report.source_exhausted

    @property
    def sampling_applied(self) -> bool:
        return self._report.sampling_applied

    @property
    def sampling_ratio(self) -> Optional[float]:
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
                "completeness": {
                    "missing_values_ratio": q.missing_values_ratio,
                    "complete_records_ratio": q.complete_records_ratio,
                    "null_columns": q.null_columns,
                },
                "consistency": {
                    "data_type_consistency": q.data_type_consistency,
                    "format_violations": q.format_violations,
                    "encoding_issues": q.encoding_issues,
                },
                "uniqueness": {
                    "duplicate_rows": q.duplicate_rows,
                    "key_uniqueness": q.key_uniqueness,
                    "high_cardinality_warning": q.high_cardinality_warning,
                },
                "accuracy": {
                    "outlier_ratio": q.outlier_ratio,
                    "range_violations": q.range_violations,
                    "negative_values_in_positive": q.negative_values_in_positive,
                },
                "timeliness": {
                    "future_dates_count": q.future_dates_count,
                    "stale_data_ratio": q.stale_data_ratio,
                    "temporal_violations": q.temporal_violations,
                },
            }

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

    def save(self, path: str) -> "ProfileReport":
        """Save the report to a file. Format from extension: .html or .json"""
        if path.endswith(".html"):
            with open(path, "w", encoding="utf-8") as f:
                f.write(self._to_html())
        elif path.endswith(".json"):
            with open(path, "w", encoding="utf-8") as f:
                f.write(self.to_json())
        else:
            raise ValueError("Unsupported format. Use .html or .json")
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
                f"<tr><td><code>{col.name}</code></td>"
                f"<td>{col.data_type}</td>"
                f"<td>{col.total_count:,}</td>"
                f"<td>{col.null_percentage:.1f}%</td>"
                f"<td>{stats}</td></tr>"
            )
        return (
            f"<div style='font-family: sans-serif'>"
            f"<h3>ProfileReport</h3>"
            f"<p><b>Source:</b> {self.source} | "
            f"<b>Rows:</b> {self.rows:,} | "
            f"<b>Columns:</b> {self.columns} | "
            f"<b>Quality:</b> {qs_str} | "
            f"<b>Time:</b> {self.execution_time_ms}ms</p>"
            f"<table><tr><th>Column</th><th>Type</th><th>Count</th>"
            f"<th>Null %</th><th>Stats</th></tr>"
            f"{col_rows}</table></div>"
        )

    def _to_html(self) -> str:
        """Generate full HTML report with Tailwind CSS styling."""
        variables_html = ""
        for col in self._report.column_profiles:
            unique_display = f"{col.unique_count:,}" if col.unique_count is not None else "N/A"
            null_color = (
                "bg-green-500"
                if col.null_percentage == 0
                else ("bg-yellow-500" if col.null_percentage < 5 else "bg-red-500")
            )
            type_badge_color = "bg-gray-100 text-gray-800"
            if col.data_type.lower() in ("integer", "float", "number"):
                type_badge_color = "bg-blue-100 text-blue-800"
            elif col.data_type.lower() in ("string", "text"):
                type_badge_color = "bg-green-100 text-green-800"
            elif col.data_type.lower() in ("date", "datetime"):
                type_badge_color = "bg-purple-100 text-purple-800"

            stats_html = ""
            if col.data_type.lower() in ("integer", "float") and col.min is not None:
                stats_html = f"""
                    <div class="grid grid-cols-2 sm:grid-cols-4 gap-4 mt-4 pt-4 border-t border-gray-100">
                        <div class="flex flex-col">
                            <span class="text-xs text-gray-500 uppercase tracking-wider font-semibold">Min</span>
                            <span class="text-sm font-medium text-gray-900">{col.min:.2f}</span>
                        </div>
                        <div class="flex flex-col">
                            <span class="text-xs text-gray-500 uppercase tracking-wider font-semibold">Max</span>
                            <span class="text-sm font-medium text-gray-900">{col.max:.2f}</span>
                        </div>
                        <div class="flex flex-col">
                            <span class="text-xs text-gray-500 uppercase tracking-wider font-semibold">Mean</span>
                            <span class="text-sm font-medium text-gray-900">{col.mean:.4f}</span>
                        </div>
                        <div class="flex flex-col">
                            <span class="text-xs text-gray-500 uppercase tracking-wider font-semibold">Std Dev</span>
                            <span class="text-sm font-medium text-gray-900">{col.std_dev:.4f}</span>
                        </div>
                    </div>"""

            variables_html += f"""
            <div class="variable-card bg-white rounded-lg border border-gray-200 p-6 hover:shadow-md transition-shadow duration-200" data-name="{col.name.lower()}">
                <div class="flex flex-col md:flex-row md:items-start md:justify-between gap-4">
                    <div class="flex-1">
                        <div class="flex items-center gap-3 mb-2">
                            <h3 class="text-lg font-bold text-gray-900 font-mono">{col.name}</h3>
                            <span class="px-2.5 py-0.5 rounded-full text-xs font-medium {type_badge_color} border border-opacity-20">{col.data_type}</span>
                        </div>
                        <div class="grid grid-cols-2 sm:grid-cols-4 gap-4 mt-4">
                            <div class="flex flex-col">
                                <span class="text-xs text-gray-500 uppercase tracking-wider font-semibold">Count</span>
                                <span class="text-sm font-medium text-gray-900">{col.total_count:,}</span>
                            </div>
                            <div class="flex flex-col">
                                <span class="text-xs text-gray-500 uppercase tracking-wider font-semibold">Missing</span>
                                <div class="flex items-baseline gap-1">
                                    <span class="text-sm font-medium {'text-red-600' if col.null_percentage > 0 else 'text-gray-900'}">
                                        {col.null_count:,}
                                    </span>
                                    <span class="text-xs text-gray-400">({col.null_percentage:.1f}%)</span>
                                </div>
                            </div>
                            <div class="flex flex-col">
                                <span class="text-xs text-gray-500 uppercase tracking-wider font-semibold">Distinct</span>
                                <span class="text-sm font-medium text-gray-900">{unique_display}</span>
                            </div>
                            <div class="flex flex-col">
                                <span class="text-xs text-gray-500 uppercase tracking-wider font-semibold">Uniqueness</span>
                                <span class="text-sm font-medium text-gray-900">{col.uniqueness_ratio * 100:.1f}%</span>
                            </div>
                        </div>
                        {stats_html}
                    </div>
                    <div class="w-full md:w-48 flex flex-col gap-3 pt-2">
                        <div class="w-full">
                            <div class="flex justify-between text-xs mb-1">
                                <span class="text-gray-500">Completeness</span>
                                <span class="text-gray-700 font-medium">{100 - col.null_percentage:.1f}%</span>
                            </div>
                            <div class="w-full bg-gray-100 rounded-full h-2 overflow-hidden">
                                <div class="{null_color} h-2 rounded-full" style="width: {100 - col.null_percentage}%"></div>
                            </div>
                        </div>
                        <div class="w-full">
                            <div class="flex justify-between text-xs mb-1">
                                <span class="text-gray-500">Uniqueness</span>
                                <span class="text-gray-700 font-medium">{col.uniqueness_ratio * 100:.1f}%</span>
                            </div>
                            <div class="w-full bg-gray-100 rounded-full h-2 overflow-hidden">
                                <div class="bg-blue-500 h-2 rounded-full" style="width: {col.uniqueness_ratio * 100}%"></div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            """

        generated_date = datetime.now().strftime("%Y-%m-%d %H:%M")
        qs = self.quality_score
        qs_display = f"{qs:.1f}%" if qs is not None else "N/A"

        return f"""<!DOCTYPE html>
<html lang="en" class="scroll-smooth">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Analysis Report - {self.source}</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>body {{ font-family: 'Inter', sans-serif; }}</style>
</head>
<body class="bg-slate-50 text-slate-900 antialiased">
    <div class="max-w-5xl mx-auto p-4 lg:p-10">
        <header class="mb-10">
            <h1 class="text-3xl font-bold text-gray-900 mb-2">Analysis Report</h1>
            <p class="text-gray-500 font-mono text-sm break-all">{self.source}</p>
            <p class="text-sm text-gray-400 mt-1">Generated on {generated_date}</p>
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mt-6">
                <div class="bg-white p-5 rounded-xl border border-gray-200 shadow-sm">
                    <div class="text-sm font-medium text-gray-500 mb-1">Quality Score</div>
                    <span class="text-3xl font-bold text-blue-600">{qs_display}</span>
                </div>
                <div class="bg-white p-5 rounded-xl border border-gray-200 shadow-sm">
                    <div class="text-sm font-medium text-gray-500 mb-1">Variables</div>
                    <span class="text-3xl font-bold text-gray-900">{self.columns}</span>
                </div>
                <div class="bg-white p-5 rounded-xl border border-gray-200 shadow-sm">
                    <div class="text-sm font-medium text-gray-500 mb-1">Rows</div>
                    <span class="text-3xl font-bold text-gray-900">{self.rows:,}</span>
                </div>
                <div class="bg-white p-5 rounded-xl border border-gray-200 shadow-sm">
                    <div class="text-sm font-medium text-gray-500 mb-1">Analysis Time</div>
                    <span class="text-3xl font-bold text-purple-600">{self.execution_time_ms:,}ms</span>
                </div>
            </div>
        </header>
        <hr class="border-gray-200 my-10" />
        <section>
            <div class="flex justify-between items-center mb-6">
                <h2 class="text-2xl font-bold text-gray-900">Variables</h2>
                <input type="text" id="searchInput" placeholder="Search variables..."
                    class="w-64 px-4 py-2 rounded-lg border border-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm">
            </div>
            <div class="space-y-4" id="variablesContainer">
                {variables_html}
            </div>
        </section>
        <footer class="mt-20 pt-10 border-t border-gray-200 text-center text-gray-400 text-sm">
            <p>Generated by <strong>DataProf</strong> v{__version__}</p>
        </footer>
    </div>
    <script>
        document.getElementById('searchInput').addEventListener('input', function(e) {{
            const term = e.target.value.toLowerCase();
            document.querySelectorAll('.variable-card').forEach(card => {{
                card.style.display = card.getAttribute('data-name').includes(term) ? 'block' : 'none';
            }});
        }});
    </script>
</body>
</html>"""
