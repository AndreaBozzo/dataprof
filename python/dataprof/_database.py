"""Optional native database helpers and unavailable-feature errors."""

from __future__ import annotations as _annotations

from typing import Any as _Any

from ._report import ProfileReport

# Database helpers are compiled in only when the extension is built with the
# `database` feature and a connector. The published wheels omit them, so bind
# stubs that explain how to get them rather than leaving an AttributeError.
try:
    from ._dataprof import (  # type: ignore[import-not-found]
        analyze_database_async as _analyze_database_async,
        count_table_rows_async as _count_table_rows_async,
        get_table_schema_async as _get_table_schema_async,
        test_connection_async as _test_connection_async,
    )

    _HAS_DATABASE = True

    # The native helpers return an awaitable that needs a running event loop
    # when they are *called*, so `asyncio.run(helper(...))` raised "no running
    # event loop" while the stubs declare coroutine functions. Wrapping them in
    # `async def` defers the native call until the coroutine runs, as
    # `analyze_database_async` already does.

    async def test_connection_async(connection_string: str) -> bool:
        """Return whether ``connection_string`` connects."""
        return await _test_connection_async(connection_string)

    async def get_table_schema_async(connection_string: str, table_name: str) -> list[str]:
        """Return the column names of ``table_name``."""
        return await _get_table_schema_async(connection_string, table_name)

    async def count_table_rows_async(connection_string: str, table_name: str) -> int:
        """Return the number of rows in ``table_name``."""
        return await _count_table_rows_async(connection_string, table_name)

    async def analyze_database_async(
        connection_string: str,
        query: str,
        batch_size: int = 10000,
        calculate_quality: bool | None = None,
        config: _Any | None = None,
    ) -> ProfileReport:
        """Profile the rows returned by ``query``.

        ``config`` takes a :class:`ProfilerConfig` carrying ``metrics``,
        ``quality_dimensions``, and ``locale``; they apply here exactly as they
        do on every file path. Quality is assessed by default, as it is on
        every file path; ``calculate_quality=False`` remains the coarse way to
        drop the quality pack.

        The native call yields a raw core report; wrap it so the result carries
        the same surface as every other ``ProfileReport`` in this package.
        """
        rust_report = await _analyze_database_async(
            connection_string, query, batch_size, calculate_quality, config
        )
        return ProfileReport(rust_report)

except ImportError:
    _HAS_DATABASE = False

    def _database_unavailable(name: str):
        def _stub(*_args, **_kwargs):
            raise ImportError(
                f"{name}() requires database support, which is not compiled into "
                "the published wheels. Rebuild from source with the shipped feature "
                "set plus the connectors you need, e.g. maturin develop --features "
                "'python,python-async,async-streaming,parquet-async,database,sqlite'. "
                "--features replaces the default list rather than extending it, so a "
                "shorter list silently drops async and remote Parquet support."
            )

        _stub.__name__ = name
        return _stub

    analyze_database_async = _database_unavailable("analyze_database_async")
    count_table_rows_async = _database_unavailable("count_table_rows_async")
    get_table_schema_async = _database_unavailable("get_table_schema_async")
    test_connection_async = _database_unavailable("test_connection_async")

# Every helper is Python-owned, as a wrapper or as a stub.
analyze_database_async.__module__ = "dataprof"
count_table_rows_async.__module__ = "dataprof"
get_table_schema_async.__module__ = "dataprof"
test_connection_async.__module__ = "dataprof"
