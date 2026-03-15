"""Type stubs for dataprof.asyncio."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from . import ProfileReport, RowCountEstimate, SchemaResult

async def profile_bytes(
    data: bytes,
    *,
    format: str,
    **kwargs: Any,
) -> ProfileReport: ...
async def profile_file(
    path: str | Path,
    **kwargs: Any,
) -> ProfileReport: ...
async def profile_url(
    url: str,
    *,
    format: str | None = None,
    **kwargs: Any,
) -> ProfileReport: ...
async def infer_schema_stream(
    data: bytes,
    *,
    format: str,
) -> SchemaResult: ...
async def quick_row_count_stream(
    data: bytes,
    *,
    format: str,
) -> RowCountEstimate: ...
