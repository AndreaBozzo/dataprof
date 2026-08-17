"""``metrics``, ``quality_dimensions`` and ``locale`` apply to queries too (#536).

#494 made these options take effect on every file format and transport. The
database path was left out of that pass: it hardcoded quality on and called
pattern detection with no locale, so a query and a CSV holding the same rows
disagreed about what had been analyzed. The Python database entry point could
not express the selection at all.

These tests profile the same records out of SQLite and off disk, and require the
two to agree — on what was analyzed, and on the quality numbers themselves.

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

# A second fixture, for quality parity rather than option plumbing. ``RECORDS``
# is five complete, unique, one-decimal rows with no date column, so it scores a
# flat 100 on four dimensions and leaves timeliness and validity unassessed —
# comparing two of those proves nothing. These records are deliberately
# imperfect in a different way per dimension: nulls for completeness, a repeated
# row for uniqueness, one malformed address for validity, mixed decimal places
# for precision, and dates so timeliness has something to read.
RICH_COLUMNS = ["id", "email", "signup_date", "amount", "notes"]
RICH_RECORDS = [
    (1, "anna@example.com", "2024-01-15", 10.5, "alpha"),
    (2, "bruno@example.com", "2024-02-20", 21.25, None),
    (3, "not-an-email", "2024-03-05", 33.0, "gamma"),
    (4, "dario@example.com", "2024-04-11", 42.125, None),
    (5, "elena@example.com", "2024-05-30", 55.5, "epsilon"),
    (6, "fabio@example.com", "2024-06-18", 60.0, "zeta"),
    (7, None, "2024-07-22", 71.75, "eta"),
    (8, "hugo@example.com", "2024-08-09", 80.5, "theta"),
    (9, "irene@example.com", "2024-09-14", 91.25, "iota"),
    (10, "jacopo@example.com", "2024-10-01", 100.0, "kappa"),
    (10, "jacopo@example.com", "2024-10-01", 100.0, "kappa"),
]

RICH_QUERY = "SELECT * FROM rich"

QUALITY_DIMENSIONS = {
    "completeness",
    "consistency",
    "uniqueness",
    "accuracy",
    "timeliness",
    "validity",
    "precision",
}


def _run(async_fn, *args, **kwargs):
    """Call a pyo3-async-runtimes function and block until completion.

    pyo3-async-runtimes requires a running event loop when the function is
    invoked, so it must be called from inside an async context.
    """

    async def _inner():
        return await async_fn(*args, **kwargs)

    return asyncio.run(_inner())


def _profile_query(db_path, **config_kwargs):
    """Profile the fixture query, leaving the quality toggle unset by default."""
    calculate_quality = config_kwargs.pop("calculate_quality", None)
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


def _profile_rich_query(db_path):
    """Profile the quality-parity fixture, with every option left at its default."""
    return dp.ProfileReport(
        _run(analyze_database_async, str(db_path), RICH_QUERY, 10000, None, None)
    )


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


@pytest.fixture()
def rich_sqlite_db(tmp_path):
    db_path = tmp_path / "rich.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE rich (id INTEGER, email TEXT, signup_date TEXT, amount REAL, notes TEXT)"
    )
    conn.executemany("INSERT INTO rich VALUES (?, ?, ?, ?, ?)", RICH_RECORDS)
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture()
def rich_csv_file(tmp_path):
    """``RICH_RECORDS`` on disk. A SQL NULL is an empty CSV field."""
    path = tmp_path / "rich.csv"
    lines = [",".join(RICH_COLUMNS)]
    lines += [
        ",".join("" if value is None else str(value) for value in record) for record in RICH_RECORDS
    ]
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
    # Schema itself survives: a narrowed profile, not an empty one. Order is
    # query order since #496; `test_column_order.py` is what pins that.
    assert [c.name for c in report.profiles] == ["id", "cap", "amount"]


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


def _quality_row(report) -> dict:
    """The quality half of ``quality_summary``, without the source-specific keys."""
    row = report.quality_summary()
    for key in ("source", "execution_time_ms"):
        row.pop(key, None)
    return row


def test_quality_is_assessed_by_default(sqlite_db) -> None:
    # #554: the entry point defaulted `calculate_quality` to False, so a query
    # profile came back with quality = None while a CSV of the same rows scored.
    # The toggle now defaults to unset, which means "whatever the selection
    # says", and an unnarrowed selection includes quality.
    report = _profile_query(sqlite_db)

    assert report.quality is not None, "a query must assess quality by default"
    assert report.quality_score is not None
    assert report.quality.assessed_dimensions(), "an assessment names what it assessed"


def test_default_quality_matches_the_same_rows_on_disk(rich_sqlite_db, rich_csv_file) -> None:
    # The output contract: the numbers do not depend on which path produced
    # them. Compares every dimension score plus the overall, not just presence.
    db_report = _profile_rich_query(rich_sqlite_db)
    csv_report = dp.profile(rich_csv_file)

    assert db_report.quality is not None, "a query must assess quality by default"
    assert csv_report.quality is not None
    assert set(db_report.quality.assessed_dimensions()) == QUALITY_DIMENSIONS
    assert set(csv_report.quality.assessed_dimensions()) == QUALITY_DIMENSIONS

    db_row = _quality_row(db_report)
    # Guard the fixture, not the code: an equality over flat 100s holds however
    # far the two paths drift, so fail loudly if the records stop exercising a
    # dimension. Consistency and accuracy are clean by construction here.
    for dimension in ("completeness", "uniqueness", "validity", "precision"):
        assert db_row[dimension] < 100.0, (
            f"{dimension} is a vacuous 100; the fixture stopped biting"
        )

    assert db_row == _quality_row(csv_report)


def test_explicit_quality_selection_survives_the_unset_flag(sqlite_db) -> None:
    # The legacy flag defaulted to False and filtered the quality pack out of
    # whatever the config had selected, so asking for quality and not mentioning
    # the flag silently got no quality (#554).
    for config_kwargs in ({"metrics": ["quality"]}, {"quality_dimensions": ["completeness"]}):
        report = _profile_query(sqlite_db, **config_kwargs)
        assert report.quality is not None, (
            f"an explicit {config_kwargs} selection must not be overridden by the unset flag"
        )


def test_calculate_quality_true_does_not_widen_a_narrowed_selection(sqlite_db) -> None:
    # The flag drops the pack; it never adds it back. `metrics=["schema"]` is a
    # schema-only profile no matter what the coarse toggle says.
    report = _profile_query(sqlite_db, metrics=["schema"], calculate_quality=True)

    assert report.quality is None, "calculate_quality=True must not override metrics=['schema']"


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
