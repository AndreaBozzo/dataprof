"""``metrics``, ``quality_dimensions`` and ``locale`` apply on every path (#494).

These options say *what to compute*. They were honoured on the synchronous CSV
path and quietly dropped by the native JSON/JSONL and Parquet parsers and by the
async streaming pipeline, so the same five records profiled differently
depending on which format they were stored in and which entry point read them.

Each test is table-driven over the same records in four formats, across the
synchronous file API, the async file API, and — for Parquet — the byte-buffer
API, which bypasses the profiler dispatch entirely.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import dataprof as dp
import pytest
from dataprof.asyncio import profile_file as profile_file_async

# ``cap`` is what makes locale observable: a five-digit string matches both
# ``CAP (IT)`` and ``ZIP Code (US)``, and ``locale="IT"`` must suppress the US
# pattern. ``amount`` gives every format a numeric column to carry statistics.
RECORDS = [
    {"id": 1, "cap": "20121", "amount": 10.5},
    {"id": 2, "cap": "00184", "amount": 21.0},
    {"id": 3, "cap": "10121", "amount": 33.5},
    {"id": 4, "cap": "80132", "amount": 42.0},
    {"id": 5, "cap": "50122", "amount": 55.5},
]

pa = pytest.importorskip("pyarrow", reason="pyarrow writes the Parquet fixture")
pq = pytest.importorskip("pyarrow.parquet", reason="pyarrow writes the Parquet fixture")

requires_async = pytest.mark.skipif(
    not dp.capabilities().async_streaming,
    reason="Async streaming not compiled. Build with --features "
    "'python,python-async,async-streaming'.",
)


@pytest.fixture(scope="module")
def fixtures(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Path]:
    """The same five records written out in every supported format."""
    import json

    directory = tmp_path_factory.mktemp("analysis_option_parity")

    csv_path = directory / "data.csv"
    lines = ["id,cap,amount"]
    lines += [f"{r['id']},{r['cap']},{r['amount']}" for r in RECORDS]
    csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    json_path = directory / "data.json"
    json_path.write_text(json.dumps(RECORDS), encoding="utf-8")

    jsonl_path = directory / "data.jsonl"
    jsonl_path.write_text(
        "\n".join(json.dumps(record) for record in RECORDS) + "\n", encoding="utf-8"
    )

    parquet_path = directory / "data.parquet"
    pq.write_table(
        pa.table(
            {
                "id": pa.array([r["id"] for r in RECORDS], type=pa.int64()),
                "cap": pa.array([r["cap"] for r in RECORDS], type=pa.string()),
                "amount": pa.array([r["amount"] for r in RECORDS], type=pa.float64()),
            }
        ),
        parquet_path,
    )

    return {
        "csv": csv_path,
        "json": json_path,
        "jsonl": jsonl_path,
        "parquet": parquet_path,
    }


def _column(report: dp.ProfileReport, name: str):
    for column in report.profiles:
        if column.name == name:
            return column
    raise AssertionError(f"column {name} missing from report")


def _pattern_names(report: dp.ProfileReport, name: str) -> list[str] | None:
    patterns = _column(report, name).patterns
    if patterns is None:
        return None
    return sorted(pattern.name for pattern in patterns)


@pytest.mark.parametrize("fmt", ["csv", "json", "jsonl", "parquet"])
def test_column_projection_applies_to_every_file_format(fixtures, fmt: str) -> None:
    report = dp.profile(fixtures[fmt], columns=["amount", "id"])

    assert [column.name for column in report.profiles] == ["id", "amount"]
    assert report.quality is not None
    assert report.quality.completeness is None
    assert report.quality.uniqueness is None


@pytest.mark.parametrize(
    "source",
    [
        {name: [record[name] for record in RECORDS] for name in ("id", "cap", "amount")},
        pa.table({name: [record[name] for record in RECORDS] for name in ("id", "cap", "amount")}),
    ],
)
def test_column_projection_applies_to_in_memory_sources(source) -> None:
    report = dp.profile(source, columns=["amount", "id"])

    assert [column.name for column in report.profiles] == ["id", "amount"]
    assert report.quality is not None
    assert report.quality.completeness is None
    assert report.quality.uniqueness is None


def test_unknown_and_empty_column_projection_are_explicit(fixtures) -> None:
    with pytest.raises(ValueError, match="missing"):
        dp.profile(fixtures["csv"], columns=["missing"])

    assert dp.profile(fixtures["json"], columns=[]).profiles == []


@pytest.mark.parametrize("fmt", ["csv", "json", "jsonl", "parquet"])
def test_schema_pack_omits_statistics_patterns_and_quality(fixtures, fmt: str) -> None:
    report = dp.profile(fixtures[fmt], metrics=["schema"])

    assert report.quality is None, f"{fmt}: quality must be absent under metrics=['schema']"
    assert report.quality_score is None, f"{fmt}: absent quality has no score"
    for column in report.profiles:
        assert column.patterns is None, f"{fmt}: {column.name} kept patterns"
    # Schema itself survives: this is a narrowed profile, not an empty one.
    assert [c.name for c in report.profiles] == ["id", "cap", "amount"]


@pytest.mark.parametrize("fmt", ["csv", "json", "jsonl", "parquet"])
def test_empty_quality_dimensions_means_not_analyzed(fixtures, fmt: str) -> None:
    report = dp.profile(fixtures[fmt], quality_dimensions=[])

    assert report.quality is None, (
        f"{fmt}: quality_dimensions=[] must mean 'not analyzed', "
        "not an assessment with nothing in it"
    )
    assert report.quality_score is None, f"{fmt}: absent quality has no score"


@pytest.mark.parametrize("fmt", ["csv", "json", "jsonl", "parquet"])
def test_narrowed_quality_dimensions_still_analyze(fixtures, fmt: str) -> None:
    # The counterpart: asking for *some* dimension is still a request to
    # analyze, so quality is present.
    report = dp.profile(fixtures[fmt], quality_dimensions=["completeness"])
    assert report.quality is not None, f"{fmt}: a narrowed selection is still an analysis"


@pytest.mark.parametrize("fmt", ["csv", "json", "jsonl", "parquet"])
def test_locale_reaches_pattern_detection(fixtures, fmt: str) -> None:
    plain = _pattern_names(dp.profile(fixtures[fmt]), "cap")
    localized = _pattern_names(dp.profile(fixtures[fmt], locale="IT"), "cap")

    assert plain is not None and localized is not None
    assert "ZIP Code (US)" in plain, f"{fmt}: without a locale the US pattern should match"
    assert "ZIP Code (US)" not in localized, f"{fmt}: locale='IT' must suppress the US pattern"
    assert "CAP (IT)" in localized, f"{fmt}: locale='IT' must keep the Italian pattern"


def test_every_format_agrees_on_locale_ranked_patterns(fixtures) -> None:
    localized: dict[str, list[str]] = {}
    for fmt, path in fixtures.items():
        names = _pattern_names(dp.profile(path, locale="IT"), "cap")
        assert names is not None, f"{fmt}: pattern detection must run by default"
        localized[fmt] = names

    distinct = {tuple(names) for names in localized.values()}
    assert len(distinct) == 1, f"formats disagree on locale-ranked patterns: {localized}"


@requires_async
@pytest.mark.parametrize("fmt", ["csv", "json", "jsonl", "parquet"])
def test_async_paths_apply_the_same_selection(fixtures, fmt: str) -> None:
    path = fixtures[fmt]

    schema_only = asyncio.run(profile_file_async(path, metrics=["schema"]))
    assert schema_only.quality is None, f"{fmt}, async: metrics=['schema'] must omit quality"
    for column in schema_only.profiles:
        assert column.patterns is None, f"{fmt}, async: {column.name} kept patterns"

    no_dimensions = asyncio.run(profile_file_async(path, quality_dimensions=[]))
    assert no_dimensions.quality is None, (
        f"{fmt}, async: quality_dimensions=[] must yield no quality"
    )

    assert _pattern_names(asyncio.run(profile_file_async(path, locale="IT")), "cap") == (
        _pattern_names(dp.profile(path, locale="IT"), "cap")
    ), f"{fmt}: sync and async disagree on locale-ranked patterns"

    projected = asyncio.run(profile_file_async(path, columns=["amount", "id"]))
    assert [column.name for column in projected.profiles] == ["id", "amount"]


def test_parquet_byte_buffers_apply_the_same_selection(fixtures) -> None:
    # ``profile(bytes, format="parquet")`` reaches the native reader without
    # going through the profiler dispatch, so it needs its own coverage.
    data = fixtures["parquet"].read_bytes()

    schema_only = dp.profile(data, format="parquet", metrics=["schema"])
    assert schema_only.quality is None, "parquet bytes: metrics=['schema'] must omit quality"
    for column in schema_only.profiles:
        assert column.patterns is None, f"parquet bytes: {column.name} kept patterns"

    assert dp.profile(data, format="parquet", quality_dimensions=[]).quality is None

    projected = dp.profile(data, format="parquet", columns=["amount", "id"])
    assert [column.name for column in projected.profiles] == ["id", "amount"]

    assert _pattern_names(dp.profile(data, format="parquet", locale="IT"), "cap") == (
        _pattern_names(dp.profile(fixtures["parquet"], locale="IT"), "cap")
    ), "parquet bytes and file disagree on locale-ranked patterns"
