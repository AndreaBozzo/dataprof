"""``metrics``, ``quality_dimensions`` and ``locale`` apply to queries too (#536).

#494 made these options take effect on every file format and transport. The
database path was left out of that pass: it hardcoded quality on and called
pattern detection with no locale, so a query and a CSV holding the same rows
disagreed about what had been analyzed. The Python database entry point could
not express the selection at all.

These tests profile the same five records out of SQLite and off disk, and
require the two to agree.

Requires building with database feature flags::

    uv run maturin develop --features "python,python-async,database,sqlite"
"""

from __future__ import annotations

import asyncio
import sqlite3

import dataprof as dp
import pytest

try:
    from dataprof._dataprof import ProfilerConfig, analyze_database_async
except ImportError:
    pytest.skip(
        "Database features not compiled (need --features python-async,database,sqlite)",
        allow_module_level=True,
    )

# ``cap`` is what makes locale observable: a five-digit string matches both
# ``CAP (IT)`` and ``ZIP Code (US)``, and ``locale="IT"`` must suppress the US
# pattern. ``amount`` gives the fixture a numeric column to carry statistics.
RECORDS = [
    (1, "20121", 10.5),
    (2, "00184", 21.0),
    (3, "10121", 33.5),
    (4, "80132", 42.0),
    (5, "50122", 55.5),
]

QUERY = "SELECT * FROM parity"


def _run(async_fn, *args, **kwargs):
    """Call a pyo3-async-runtimes function and block until completion.

    pyo3-async-runtimes requires a running event loop when the function is
    invoked, so it must be called from inside an async context.
    """

    async def _inner():
        return await async_fn(*args, **kwargs)

    return asyncio.run(_inner())


def _profile_query(db_path, **config_kwargs):
    """Profile the fixture query, with quality on unless a test says otherwise."""
    calculate_quality = config_kwargs.pop("calculate_quality", True)
    config = ProfilerConfig(**config_kwargs) if config_kwargs else None
    report = _run(
        analyze_database_async,
        str(db_path),
        QUERY,
        10000,
        calculate_quality,
        config,
    )
    return dp.ProfileReport(report)


@pytest.fixture()
def sqlite_db(tmp_path):
    db_path = tmp_path / "parity.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE parity (id INTEGER, cap TEXT, amount REAL)")
    conn.executemany("INSERT INTO parity (id, cap, amount) VALUES (?, ?, ?)", RECORDS)
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture()
def csv_file(tmp_path):
    """The same records on disk, for cross-source comparison."""
    path = tmp_path / "parity.csv"
    lines = ["id,cap,amount"]
    lines += [f"{id_},{cap},{amount}" for id_, cap, amount in RECORDS]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _pattern_names(report, name: str) -> list[str] | None:
    for column in report.profiles:
        if column.name == name:
            patterns = column.patterns
            return None if patterns is None else sorted(p.name for p in patterns)
    raise AssertionError(f"column {name} missing from report")


def test_schema_pack_omits_patterns_and_quality(sqlite_db) -> None:
    report = _profile_query(sqlite_db, metrics=["schema"])

    assert report.quality is None, "quality must be absent under metrics=['schema']"
    assert report.quality_score is None, "absent quality has no score"
    for column in report.profiles:
        assert column.patterns is None, f"{column.name} kept patterns"
    # Schema itself survives: a narrowed profile, not an empty one. Compared as
    # a set because the database path does not preserve query column order —
    # that is #496, and asserting on it here would couple the two.
    assert sorted(c.name for c in report.profiles) == ["amount", "cap", "id"]


def test_empty_quality_dimensions_means_not_analyzed(sqlite_db) -> None:
    report = _profile_query(sqlite_db, quality_dimensions=[])

    assert report.quality is None, (
        "quality_dimensions=[] must mean 'not analyzed', not an assessment with nothing in it"
    )
    assert report.quality_score is None, "absent quality has no score"


def test_narrowed_quality_dimensions_still_analyze(sqlite_db) -> None:
    report = _profile_query(sqlite_db, quality_dimensions=["completeness"])
    assert report.quality is not None, "a narrowed selection is still an analysis"


def test_locale_reaches_pattern_detection_and_matches_csv(sqlite_db, csv_file) -> None:
    plain = _pattern_names(_profile_query(sqlite_db), "cap")
    localized = _pattern_names(_profile_query(sqlite_db, locale="IT"), "cap")

    assert plain is not None and localized is not None
    assert "ZIP Code (US)" in plain, "without a locale the US pattern should match"
    assert "ZIP Code (US)" not in localized, "locale='IT' must suppress the US pattern"
    assert "CAP (IT)" in localized, "locale='IT' must keep the Italian pattern"

    # The product contract: the same rows profile the same way whether they came
    # out of a query or off disk.
    assert localized == _pattern_names(dp.profile(csv_file, locale="IT"), "cap"), (
        "a query and a CSV holding the same rows disagree on locale-ranked patterns"
    )


def test_calculate_quality_false_still_honours_the_rest(sqlite_db) -> None:
    # The older, coarser toggle drops quality without narrowing anything else,
    # and must not swallow the locale.
    report = _profile_query(sqlite_db, calculate_quality=False, locale="IT")

    assert report.quality is None, "calculate_quality=False skips quality"
    patterns = _pattern_names(report, "cap")
    assert patterns is not None and "ZIP Code (US)" not in patterns, (
        f"locale must still apply without the quality pack, got {patterns}"
    )


def test_config_is_optional(sqlite_db) -> None:
    # The parameter is additive: existing positional callers keep working.
    report = dp.ProfileReport(_run(analyze_database_async, str(sqlite_db), QUERY, 10000, True))
    assert report.quality is not None
    assert report.rows == 5
