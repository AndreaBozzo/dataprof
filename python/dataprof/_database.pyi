"""Type stubs for _database."""

from __future__ import annotations

from ._dataprof import ProfilerConfig
from ._report import ProfileReport

async def analyze_database_async(
    connection_string: str,
    query: str,
    batch_size: int = 10000,
    calculate_quality: bool | None = None,
    config: ProfilerConfig | None = None,
) -> ProfileReport: ...
async def count_table_rows_async(connection_string: str, table_name: str) -> int: ...
async def get_table_schema_async(connection_string: str, table_name: str) -> list[str]: ...
async def test_connection_async(connection_string: str) -> bool: ...

_HAS_DATABASE: bool
