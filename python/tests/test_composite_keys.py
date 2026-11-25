"""Composite keys use the same cell values on each whole-row input route."""

from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any

import dataprof as dp
import dataprof.asyncio as dpa
import pytest

GROUPS = [["country", "amount"], ["id"]]
EXPECTED = [
    {
        "columns": ["country", "amount"],
        "rows_counted": 15,
        "rows_skipped_all_null": 2,
        "distinct_keys": 7,
        "repeated_rows": 8,
        "distinct_repeated_keys": 7,
        "is_exact": True,
        "sample_repeated_keys": [["A", None], *[[f"C{i}", "1.5"] for i in range(4)]],
    },
    {
        "columns": ["id"],
        "rows_counted": 17,
        "rows_skipped_all_null": 0,
        "distinct_keys": 17,
        "repeated_rows": 0,
        "distinct_repeated_keys": 0,
        "is_exact": True,
        "sample_repeated_keys": [],
    },
]


@pytest.fixture
def sources(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    cells: list[tuple[str | None, str | None]] = [
        (f"C{i}", value) for i in range(6) for value in ("1.50", "1.5")
    ]
    cells.extend([("C0", "1.50"), ("A", None), ("A", None), (None, None), (None, None)])
    records = [
        {"id": f"row{i}", "country": country, "amount": float(amount) if amount else None}
        for i, (country, amount) in enumerate(cells)
    ]
    csv_data = "id,country,amount\n" + "".join(
        f"row{i},{country or ''},{amount or ''}\n" for i, (country, amount) in enumerate(cells)
    )
    csv = tmp_path / "keys.csv"
    csv.write_text(csv_data, encoding="utf-8")
    json_file = tmp_path / "keys.json"
    json_file.write_text(json.dumps(records), encoding="utf-8")
    jsonl = tmp_path / "keys.jsonl"
    jsonl.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")
    table = pa.table(
        {
            "id": [record["id"] for record in records],
            "country": [country for country, _ in cells],
            "amount": pa.array(
                [Decimal(amount) if amount else None for _, amount in cells], pa.decimal128(5, 2)
            ),
        }
    )
    parquet = tmp_path / "keys.parquet"
    pq.write_table(table, parquet, row_group_size=4)
    return records, csv, json_file, jsonl, table, parquet


def compare_block(route: str, block: Any, wrong: list[str]) -> None:
    if not isinstance(block, list) or len(block) != len(EXPECTED):
        wrong.append(f"{route}: expected two key groups, got {block!r}")
        return
    for actual, expected in zip(block, EXPECTED, strict=True):
        for field in sorted(expected.keys() | actual.keys()):
            if field not in actual or field not in expected or actual[field] != expected[field]:
                wrong.append(
                    f"{route} {expected['columns']} {field}: "
                    f"expected {expected.get(field)!r}, got {actual.get(field)!r}"
                )


def verify_report(route: str, report: dp.ProfileReport, wrong: list[str]) -> None:
    flat = report.to_dict()
    canonical_json = report.to_json()
    canonical = json.loads(canonical_json)
    for label, block in [
        ("accessor", report.composite_keys),
        ("summary", flat.get("composite_keys")),
        ("document", canonical.get("composite_keys")),
        ("from_dict(summary)", dp.ProfileReport.from_dict(flat).composite_keys),
        ("from_dict(document)", dp.ProfileReport.from_dict(canonical).composite_keys),
        ("from_json(summary)", dp.ProfileReport.from_json(json.dumps(flat)).composite_keys),
        ("from_json(document)", dp.ProfileReport.from_json(canonical_json).composite_keys),
    ]:
        compare_block(f"{route} {label}", block, wrong)


def test_composite_keys_match_sync_routes(sources):
    records, csv, json_file, jsonl, table, parquet = sources
    pd = pytest.importorskip("pandas")
    pl = pytest.importorskip("polars")
    routes = {
        "CSV auto": lambda: dp.profile_file(csv, unique_keys=GROUPS),
        "CSV incremental": lambda: dp.profile_file(csv, engine="incremental", unique_keys=GROUPS),
        "CSV columnar": lambda: dp.profile_file(csv, engine="columnar", unique_keys=GROUPS),
        "CSV bytes": lambda: dp.profile(csv.read_bytes(), format="csv", unique_keys=GROUPS),
        "JSON": lambda: dp.profile(json_file, unique_keys=GROUPS),
        "JSONL": lambda: dp.profile(jsonl, unique_keys=GROUPS),
        "JSON bytes": lambda: dp.profile(json_file.read_bytes(), format="json", unique_keys=GROUPS),
        "JSONL bytes": lambda: dp.profile(jsonl.read_bytes(), format="jsonl", unique_keys=GROUPS),
        "Parquet file": lambda: dp.profile(parquet, unique_keys=GROUPS),
        "Parquet buffer": lambda: dp.profile(
            parquet.read_bytes(), format="parquet", unique_keys=GROUPS
        ),
        "Arrow table": lambda: dp.profile(table, unique_keys=GROUPS),
        "Arrow stream": lambda: dp.profile(table.to_reader(max_chunksize=3), unique_keys=GROUPS),
        "pandas": lambda: dp.profile(
            table.to_pandas(types_mapper=pd.ArrowDtype), unique_keys=GROUPS
        ),
        "polars": lambda: dp.profile(pl.from_arrow(table), unique_keys=GROUPS),
        "records": lambda: dp.profile(records, unique_keys=GROUPS),
        "dict of lists": lambda: dp.profile(
            {name: [row[name] for row in records] for name in records[0]}, unique_keys=GROUPS
        ),
    }
    wrong: list[str] = []
    gate_checks = None
    for route, run in routes.items():
        try:
            report = run()
            verify_report(route, report, wrong)
            result = report.check(unique_columns=GROUPS)
            assert result.verdict == "fail"
            failed, passed = result.checks
            assert (failed.code, failed.status, failed.observed) == ("unique_key", "failed", 8)
            assert failed.key == {
                "columns": GROUPS[0],
                "repeats": {
                    "distinct_repeated_keys": 7,
                    "sample_repeated_keys": EXPECTED[0]["sample_repeated_keys"],
                    "is_exact": True,
                },
            }
            assert (passed.code, passed.status, passed.observed) == ("unique_key", "passed", 0)
            checks = result.to_dict()["checks"]
            if gate_checks is None:
                gate_checks = checks
            assert checks == gate_checks
        except Exception as exc:
            wrong.append(f"{route}: {exc!r}")
    assert not wrong, "\n".join(wrong)


def test_composite_keys_match_async_routes(sources, monkeypatch):
    if not dp.capabilities().remote_parquet:
        pytest.skip("remote Parquet support is required")
    import test_remote_parquet_versions as remote

    _, csv, json_file, jsonl, _, parquet = sources
    monkeypatch.setattr(remote, "A", parquet.read_bytes())
    wrong: list[str] = []

    async def verify():
        with remote._swap_server(1_000_000, "etag_only") as (url, _):
            routes = {
                "HTTP Parquet": lambda: dpa.profile_url(url, unique_keys=GROUPS),
                "async CSV file": lambda: dpa.profile_file(csv, unique_keys=GROUPS),
                "async CSV reader": lambda: dpa.profile_bytes(
                    csv.read_bytes(), format="csv", unique_keys=GROUPS
                ),
                "async JSON reader": lambda: dpa.profile_bytes(
                    json_file.read_bytes(), format="json", unique_keys=GROUPS
                ),
                "async JSONL reader": lambda: dpa.profile_bytes(
                    jsonl.read_bytes(), format="jsonl", unique_keys=GROUPS
                ),
                "async Parquet file": lambda: dpa.profile_file(parquet, unique_keys=GROUPS),
                "async Parquet buffer": lambda: dpa.profile_bytes(
                    parquet.read_bytes(), format="parquet", unique_keys=GROUPS
                ),
            }
            for route, run in routes.items():
                try:
                    verify_report(route, await run(), wrong)
                except Exception as exc:
                    wrong.append(f"{route}: {exc!r}")

    asyncio.run(verify())
    assert not wrong, "\n".join(wrong)


def test_composite_key_configuration_and_validation():
    rows = {"a": ["left", "left", "right", "right"], "b": ["one", "one", "one", "one"]}
    plain = dp.profile(rows)
    exact = dp.profile(rows, unique_keys=[["a", "b"]], unique_keys_exact_limit=2)
    capped = dp.profile(rows, unique_keys=[["a", "b"]], unique_keys_exact_limit=1)
    assert plain.composite_keys is None and "composite_keys" not in plain.to_dict()
    assert "composite_keys" not in json.loads(plain.to_json())
    assert capped.to_dict()["composite_keys"][0] == {
        "columns": ["a", "b"],
        "rows_counted": 4,
        "rows_skipped_all_null": 0,
        "distinct_keys": 1,
        "repeated_rows": 1,
        "distinct_repeated_keys": 1,
        "is_exact": False,
        "sample_repeated_keys": [["left", "one"]],
    }
    for other in (exact, dp.ProfileReport.from_dict(exact.to_dict())):
        change = plain.compare(other)["configuration"]["unique_keys"]
        assert change["same"] is False
        assert change["b"] == {"groups": [["a", "b"]], "exact_limit": 2}
    with pytest.raises(ValueError, match="absent from the source"):
        dp.profile(rows, unique_keys=[["missing"]])
    with pytest.raises(ValueError, match="excluded from profiling"):
        dp.profile(rows, columns=["a"], unique_keys=[["a", "b"]])
    with pytest.raises(ValueError, match="distinct columns"):
        dp.profile(rows, unique_keys=[["a", "a"]])
