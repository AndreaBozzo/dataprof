"""dataprof - High-performance data profiling library."""

from __future__ import annotations as _annotations

from . import asyncio as asyncio
from ._api import (
    analyze_structure as analyze_structure,
    infer_schema as infer_schema,
    list_patterns as list_patterns,
    profile as profile,
    profile_file as profile_file,
    quick_row_count as quick_row_count,
)
from ._capabilities import Capabilities as Capabilities, capabilities as capabilities
from ._columns import column_to_dict as column_to_dict
from ._database import (
    analyze_database_async as analyze_database_async,
    count_table_rows_async as count_table_rows_async,
    get_table_schema_async as get_table_schema_async,
    test_connection_async as test_connection_async,
)
from ._dataprof import (
    ColumnProfile as ColumnProfile,
    DataQualityMetrics as DataQualityMetrics,
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
from ._profiler import Profiler as Profiler
from ._report import ProfileReport as ProfileReport
from ._report_schema import REPORT_SCHEMA_VERSION as REPORT_SCHEMA_VERSION

__all__ = [
    "Capabilities",
    "capabilities",
    "REPORT_SCHEMA_VERSION",
    "analyze_database_async",
    "count_table_rows_async",
    "get_table_schema_async",
    "test_connection_async",
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
    "column_to_dict",
    "asyncio",
    "__version__",
]
