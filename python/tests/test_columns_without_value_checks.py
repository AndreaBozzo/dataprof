"""Which columns the quality score's value checks skipped (#1134).

A struct, list or map column is counted and nothing more (#637): its nulls
count toward completeness and its values toward duplicate rows, but no other
check reads them. The report names those columns under
``quality.columns_without_value_checks``, the same on every input route, and
says so next to the score in the text and HTML renderings.

``tests/columns_without_value_checks.rs`` is the Rust twin.
"""

from __future__ import annotations

import asyncio
import html
import json
from pathlib import Path

import dataprof as dp
import dataprof.asyncio as dp_async
import pytest

pa = pytest.importorskip("pyarrow", reason="pyarrow builds the typed fixtures")
pq = pytest.importorskip("pyarrow.parquet", reason="pyarrow writes the Parquet fixture")

#: The issue's fixture: ``props.mag`` is null in half the rows and
#: ``props.place`` is empty in all of them, yet the score was 100.
RECORDS = [
    {
        "id": i,
        "props": {"mag": 1.5 if i % 2 == 0 else None, "place": ""},
        "geom": {"coords": [0, 0]},
    }
    for i in range(50)
]

EXPECTED = [
    {"column": "props", "reason": "nested"},
    {"column": "geom", "reason": "nested"},
]


def _routes(tmp_path: Path) -> dict[str, dp.ProfileReport]:
    jsonl_bytes = "".join(json.dumps(record) + "\n" for record in RECORDS).encode()
    json_bytes = json.dumps(RECORDS).encode()
    jsonl = tmp_path / "records.jsonl"
    jsonl.write_bytes(jsonl_bytes)
    table = pa.Table.from_pylist(RECORDS)
    parquet = tmp_path / "records.parquet"
    pq.write_table(table, parquet)
    routes = {
        "list of dicts": dp.profile(RECORDS),
        "json bytes": dp.profile(json_bytes, format="json"),
        "jsonl file": dp.profile(str(jsonl)),
        "async jsonl bytes": asyncio.run(dp_async.profile_bytes(jsonl_bytes, format="jsonl")),
        "async jsonl sampled": asyncio.run(
            dp_async.profile_bytes(
                jsonl_bytes, format="jsonl", sampling=dp.SamplingStrategy.reservoir(10)
            )
        ),
        "pyarrow table": dp.profile(table),
        "parquet file": dp.profile(str(parquet)),
        "parquet bytes": dp.profile(parquet.read_bytes(), format="parquet"),
    }
    try:
        import polars as pl

        routes["polars"] = dp.profile(pl.from_arrow(table))
    except ImportError:
        pass
    return routes


def test_nested_columns_are_named_on_every_path(tmp_path):
    # Every wrong route at once, so one run shows which routes each part of
    # the change guards.
    wrong = []
    for route, report in _routes(tmp_path).items():
        found = {
            "accessor": report.quality_columns_without_value_checks,
            "to_dict": report.to_dict()["quality"].get("columns_without_value_checks"),
            "to_json": json.loads(report.to_json())["quality"].get("columns_without_value_checks"),
        }
        for form, value in found.items():
            if value != EXPECTED:
                wrong.append(f"{route} / {form}: {value}")
    assert not wrong, "\n".join(wrong)


def test_the_sampled_route_takes_the_mixed_branch(tmp_path):
    report = _routes(tmp_path)["async jsonl sampled"]
    assert report.quality_sampled_dimensions, "the reservoir route must sample quality"


def test_a_source_without_nested_columns_records_an_empty_list(tmp_path):
    report = dp.profile({"id": [1, 2], "name": ["a", "b"]})
    assert report.quality_columns_without_value_checks == []
    assert report.to_dict()["quality"]["columns_without_value_checks"] == []
    assert "value checks skipped" not in repr(report)
    assert "value checks skipped" not in report.to_html()


def test_both_readers_keep_the_list_and_its_absence(tmp_path):
    report = dp.profile(RECORDS)
    summary = report.to_dict()
    assert dp.ProfileReport.from_dict(summary).quality_columns_without_value_checks == EXPECTED
    assert dp.ProfileReport.from_json(report.to_json()).quality_columns_without_value_checks == (
        EXPECTED
    )
    loaded = dp.ProfileReport.from_dict(summary)
    assert loaded.to_dict()["quality"]["columns_without_value_checks"] == EXPECTED

    del summary["quality"]["columns_without_value_checks"]
    older = dp.ProfileReport.from_dict(summary)
    assert older.quality_columns_without_value_checks is None
    assert "columns_without_value_checks" not in older.to_dict()["quality"]

    canonical = json.loads(report.to_json())
    del canonical["quality"]["columns_without_value_checks"]
    older = dp.ProfileReport.from_json(json.dumps(canonical))
    assert older.quality_columns_without_value_checks is None


@pytest.mark.parametrize(
    "malformed",
    [None, [{"column": "props", "reason": "unknown"}], [{"column": 1, "reason": "nested"}]],
)
def test_a_malformed_list_does_not_load(malformed):
    summary = dp.profile(RECORDS).to_dict()
    summary["quality"]["columns_without_value_checks"] = malformed
    with pytest.raises(ValueError, match="columns_without_value_checks"):
        dp.ProfileReport.from_dict(summary)


def test_the_renderings_say_so_next_to_the_score():
    report = dp.profile(RECORDS)
    note = "the quality score's value checks skipped 2 of 3 columns: props (nested), geom (nested)"
    assert repr(report).splitlines()[1] == f"Note: {note}"
    assert report.to_markdown().splitlines()[1] == f"**Note:** {note}"
    assert f"caveat: {note}" in report.to_llm_context().splitlines()
    assert f"<p><i>Note: {html.escape(note)}</i></p>" in report.to_html()


def test_the_note_names_a_bounded_number_of_columns():
    records = [{f"c{i}": {"v": i} for i in range(8)} for _ in range(3)]
    assert (
        "caveat: the quality score's value checks skipped 8 of 8 columns: "
        "c0 (nested), c1 (nested), c2 (nested), c3 (nested), c4 (nested), +3 more"
    ) in dp.profile(records).to_llm_context().splitlines()
