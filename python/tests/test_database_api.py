"""Database API integration tests.

These tests require building with database feature flags:
    uv run maturin develop --features "python,python-async,database,sqlite"

Or use the Makefile shortcut:
    make dev-db
"""

import asyncio
import sqlite3

import pytest

# Skip entire module if database features are not compiled
try:
    from dataprof._dataprof import (
        analyze_database_async,
        count_table_rows_async,
        get_table_schema_async,
        test_connection_async,
    )
except ImportError:
    pytest.skip(
        "Database features not compiled (need --features python-async,database,sqlite)",
        allow_module_level=True,
    )


def _run(async_fn, *args, **kwargs):
    """Call a pyo3-async-runtimes function and block until completion.

    pyo3-async-runtimes requires a running event loop when the function is
    invoked, so we must call it from inside an async context.
    """

    async def _inner():
        return await async_fn(*args, **kwargs)

    return asyncio.get_event_loop().run_until_complete(_inner())


@pytest.fixture()
def sqlite_db(tmp_path):
    """Create a temporary SQLite database with test data."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE test_users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            age INTEGER,
            salary REAL
        )
    """)
    conn.executemany(
        "INSERT INTO test_users (name, email, age, salary) VALUES (?, ?, ?, ?)",
        [
            ("alice", "alice@example.com", 25, 50000.0),
            ("bob", "bob@example.com", 30, 60000.0),
            ("charlie", None, 35, None),
            ("diana", "diana@example.com", None, 55000.0),
            ("eve", "eve@example.com", 28, 70000.0),
        ],
    )
    conn.commit()
    conn.close()
    return str(db_path)


@pytest.mark.database
class TestDatabaseConnection:
    """Test database connectivity via async Python API."""

    def test_connection_sqlite(self, sqlite_db):
        result = _run(test_connection_async, sqlite_db)
        assert result is True

    def test_connection_bad_string(self):
        with pytest.raises(RuntimeError):
            _run(test_connection_async, "invalid://nope")


@pytest.mark.database
class TestDatabaseAnalysis:
    """Test analyze_database_async with SQLite."""

    def test_analyze_basic(self, sqlite_db):
        report = _run(analyze_database_async, sqlite_db, "SELECT * FROM test_users")
        assert report.rows_processed == 5
        assert len(report.column_profiles) == 5  # id, name, email, age, salary
        assert report.source_type == "query"

    def test_analyze_with_quality(self, sqlite_db):
        report = _run(
            analyze_database_async,
            sqlite_db,
            "SELECT * FROM test_users",
            calculate_quality=True,
        )
        assert report.rows_processed == 5
        assert report.quality is not None

    def test_analyze_without_quality(self, sqlite_db):
        report = _run(
            analyze_database_async,
            sqlite_db,
            "SELECT * FROM test_users",
            calculate_quality=False,
        )
        assert report.rows_processed == 5
        assert report.quality is None

    def test_analyze_custom_batch_size(self, sqlite_db):
        report = _run(
            analyze_database_async,
            sqlite_db,
            "SELECT * FROM test_users",
            batch_size=2,
        )
        assert report.rows_processed == 5

    def test_analyze_filtered_query(self, sqlite_db):
        report = _run(
            analyze_database_async,
            sqlite_db,
            "SELECT name, age FROM test_users WHERE age > 28",
        )
        assert report.rows_processed == 2  # bob(30), charlie(35)
        assert len(report.column_profiles) == 2


@pytest.mark.database
class TestDatabaseSchema:
    """Test schema introspection and row counting."""

    def test_get_table_schema(self, sqlite_db):
        schema = _run(get_table_schema_async, sqlite_db, "test_users")
        assert isinstance(schema, list)
        assert "id" in schema
        assert "name" in schema
        assert "email" in schema
        assert "age" in schema
        assert "salary" in schema

    def test_count_table_rows(self, sqlite_db):
        count = _run(count_table_rows_async, sqlite_db, "test_users")
        assert count == 5


@pytest.mark.database
class TestDatabaseErrors:
    """Test error handling for invalid inputs."""

    def test_bad_connection_string(self):
        with pytest.raises(RuntimeError):
            _run(analyze_database_async, "invalid://bad", "SELECT 1")

    def test_invalid_query(self, sqlite_db):
        with pytest.raises(RuntimeError):
            _run(analyze_database_async, sqlite_db, "SELECT * FROM nonexistent_table")

    def test_sql_injection_rejected(self, sqlite_db):
        with pytest.raises(RuntimeError):
            _run(
                analyze_database_async,
                sqlite_db,
                "SELECT * FROM test_users; DROP TABLE test_users",
            )
