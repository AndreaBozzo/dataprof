"""Exact distinct limits across profilers, report formats and quality gates."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

try:
    import dataprof as dp
    from dataprof import asyncio as dp_async
    from dataprof.check.__main__ import main as check_main
except ImportError:
    pytest.skip("dataprof native extension not built", allow_module_level=True)

DISTINCT = 96
LOW_LIMIT = 32
REPEATED = 5
SETTING = "unique_count_exact_limits"


def _profiler(path: Path, **options: Any) -> dp.ProfileReport:
    return dp.Profiler().unique_count_exact_limits(options[SETTING]).profile(path)


def _routes(tmp_path: Path, repeated: int) -> dict[str, Callable[..., dp.ProfileReport]]:
    # Use the same text keys on inferred and declared-schema routes.
    ids = [f"O{i}" for i in [*range(DISTINCT), *range(repeated)]]
    labels = [f"L{i}" for i in range(len(ids))]
    rows = [{"label": label, "id": key} for label, key in zip(labels, ids, strict=True)]
    columns = {"label": labels, "id": ids}
    csv_data = ("label,id\n" + "".join(f"{r['label']},{r['id']}\n" for r in rows)).encode()
    json_data = json.dumps(rows).encode()
    jsonl_data = ("\n".join(map(json.dumps, rows)) + "\n").encode()
    csv = tmp_path / "keys.csv"
    csv.write_bytes(csv_data)
    as_json = tmp_path / "keys.json"
    as_json.write_bytes(json_data)
    jsonl = tmp_path / "keys.jsonl"
    jsonl.write_bytes(jsonl_data)

    routes: dict[str, Callable[..., dp.ProfileReport]] = {
        "profile_file": lambda **k: dp.profile_file(csv, **k),
        "csv auto": lambda **k: dp.profile(csv, **k),
        "csv incremental": lambda **k: dp.profile(csv, engine="incremental", **k),
        "csv columnar": lambda **k: dp.profile(csv, engine="columnar", **k),
        "json": lambda **k: dp.profile(as_json, **k),
        "jsonl": lambda **k: dp.profile(jsonl, **k),
        "dict": lambda **k: dp.profile(columns, **k),
        "records": lambda **k: dp.profile(rows, **k),
        "csv bytes": lambda **k: dp.profile(csv_data, format="csv", **k),
        "json bytes": lambda **k: dp.profile(json_data, format="json", **k),
        "jsonl bytes": lambda **k: dp.profile(jsonl_data, format="jsonl", **k),
        "Profiler": lambda **k: _profiler(csv, **k),
    }
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        pass
    else:
        table = pa.table(columns)
        parquet = tmp_path / "keys.parquet"
        pq.write_table(table, parquet, row_group_size=17)
        routes.update(
            {
                "parquet": lambda **k: dp.profile_file(parquet, **k),
                "parquet bytes": lambda **k: dp.profile(
                    parquet.read_bytes(), format="parquet", **k
                ),
                "arrow table": lambda **k: dp.profile(table, **k),
                "arrow stream": lambda **k: dp.profile(
                    pa.RecordBatchReader.from_batches(
                        table.schema, table.to_batches(max_chunksize=11)
                    ),
                    **k,
                ),
            }
        )
    try:
        import pandas as pd
    except ImportError:
        pass
    else:
        routes["pandas"] = lambda **k: dp.profile(pd.DataFrame(columns), **k)
    try:
        import polars as pl
    except ImportError:
        pass
    else:
        routes["polars"] = lambda **k: dp.profile(pl.DataFrame(columns), **k)
    if dp_async._HAS_ASYNC:
        routes.update(
            {
                "async file": lambda **k: asyncio.run(dp_async.profile_file(csv, **k)),
                "async CSV bytes": lambda **k: asyncio.run(
                    dp_async.profile_bytes(csv_data, format="csv", **k)
                ),
                "async JSON bytes": lambda **k: asyncio.run(
                    dp_async.profile_bytes(json_data, format="json", **k)
                ),
                "async JSONL bytes": lambda **k: asyncio.run(
                    dp_async.profile_bytes(jsonl_data, format="jsonl", **k)
                ),
            }
        )
    return routes


def test_every_route_honours_the_named_limit(tmp_path: Path) -> None:
    wrong: list[tuple[object, ...]] = []
    for limit, repeated in ((LOW_LIMIT, REPEATED), (DISTINCT, REPEATED), (DISTINCT, 0)):
        approximate = limit < DISTINCT
        expected_verdict = "inconclusive" if approximate else ("fail" if repeated else "pass")
        options = {SETTING: {"id": limit}}
        counts: dict[str, int] = {}
        for route, run in _routes(tmp_path, repeated).items():
            try:
                report = run(**options)
                document = json.loads(report.to_json())
                summary = report.to_dict()
                id_profile = next(c for c in document["column_profiles"] if c["name"] == "id")
                label_profile = next(c for c in document["column_profiles"] if c["name"] == "label")
                gate = report.check(unique_columns=["id"])
                check = gate.checks[0]
                got = {
                    "note": check.reason,
                    "verdict": gate.verdict,
                    "id_approximate": id_profile["unique_count_is_approximate"],
                    "label_approximate": label_profile["unique_count_is_approximate"],
                    "label_count": label_profile["unique_count"],
                    "configured": report.analysis_settings,
                    "canonical": document.get("analysis_settings"),
                    "summary": summary.get("analysis_settings"),
                    "json_roundtrip": dp.ProfileReport.from_json(
                        report.to_json()
                    ).analysis_settings,
                    "dict_roundtrip": dp.ProfileReport.from_dict(summary).analysis_settings,
                }
                expected = {
                    "note": {"reason": "approximate_count"} if approximate else None,
                    "verdict": expected_verdict,
                    "id_approximate": approximate,
                    "label_approximate": False,
                    "label_count": DISTINCT + repeated,
                }
                if not approximate:
                    got["id_count"] = id_profile["unique_count"]
                    expected["id_count"] = DISTINCT
                    got["repeated"] = check.observed
                    expected["repeated"] = repeated
                counts[route] = id_profile["unique_count"]
                for field, value in got.items():
                    if field in expected:
                        if value != expected[field]:
                            wrong.append((limit, repeated, route, field, value, expected[field]))
                    elif not isinstance(value, dict) or value.get(SETTING) != options[SETTING]:
                        wrong.append((limit, repeated, route, field, value, options[SETTING]))
            except Exception as exc:
                wrong.append((limit, repeated, route, type(exc).__name__, str(exc)))
        if len(set(counts.values())) > 1:
            wrong.extend(
                (limit, repeated, route, "unique_count parity", count)
                for route, count in counts.items()
            )
    assert not wrong, wrong


def test_limit_changes_are_recorded_and_exact_baselines_remain_comparable() -> None:
    data = {"id": ["a", "b", "c", "d"]}
    default = dp.profile(data)
    assert default.analysis_settings is not None
    assert SETTING not in default.analysis_settings
    assert SETTING not in default.compare(default)["configuration"]
    a = dp.profile(data, unique_count_exact_limits={"id": 4})
    b = dp.profile({"id": ["a", "a", "b", "b"]}, unique_count_exact_limits={"id": 5})
    estimated = dp.profile(data, unique_count_exact_limits={"id": 2})
    restored = [a, dp.ProfileReport.from_json(a.to_json()), dp.ProfileReport.from_dict(a.to_dict())]
    wrong = []
    for index, baseline in enumerate(restored):
        config = baseline.compare(b)["configuration"].get(SETTING)
        if config != {"a": {"id": 4}, "b": {"id": 5}, "same": False}:
            wrong.append((index, "configuration", config))
        status = b.check(baseline=baseline, max_distinct_drop={"id": 0}).verdict
        if status != "fail":
            wrong.append((index, "different limits with exact counts", status))
        uncertain = b.check(baseline=estimated, max_distinct_drop={"id": 0})
        if uncertain.verdict != "inconclusive" or uncertain.checks[0].reason != {
            "reason": "approximate_count"
        }:
            wrong.append(
                (index, "estimated baseline", uncertain.verdict, uncertain.checks[0].reason)
            )
    assert not wrong, wrong


def test_invalid_requests_use_column_names_in_errors(tmp_path: Path) -> None:
    data = {"id": ["x", "y"], "label": ["l1", "l2"]}
    for limit in (0, -1):
        with pytest.raises(ValueError, match=r"unique_count_exact_limits\['id'\]"):
            dp.profile(data, unique_count_exact_limits={"id": limit})
    with pytest.raises(ValueError, match="missing.*absent from the source"):
        dp.profile(data, unique_count_exact_limits={"missing": 10})
    with pytest.raises(ValueError, match="id.*excluded from profiling"):
        dp.profile(data, columns=["label"], unique_count_exact_limits={"id": 10})
    with pytest.raises(ValueError, match="at least 1"):
        dp.ProfilerConfig(unique_count_exact_limits={"id": 0})
    config = dp.ProfilerConfig(unique_count_exact_limits={"id": 1})
    assert config.unique_count_exact_limits == {"id": 1}
    # JSON keys can arrive after the first record, including after a fieldless object.
    jsonl = tmp_path / "late.jsonl"
    jsonl.write_text('{}\n{"id":"x"}\n{"id":"y"}\n', encoding="utf-8")
    late = dp.profile_file(jsonl, unique_count_exact_limits={"id": 1})
    assert late.check(unique_columns=["id"]).verdict == "inconclusive"


def test_cli_profiles_with_the_named_limit(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    csv = tmp_path / "keys.csv"
    csv.write_text("order=id,value\nx,1\ny,2\nx,3\n", encoding="utf-8")
    args = [str(csv), "--unique-column", "order=id", "--json"]
    assert check_main([*args, "--unique-count-exact-limit", "order=id=1"]) == 2
    spill = json.loads(capsys.readouterr().out)
    assert spill["verdict"] == "inconclusive"
    assert check_main([*args, "--unique-count-exact-limit", "order=id=2"]) == 1
    exact = json.loads(capsys.readouterr().out)
    assert exact["verdict"] == "fail"

    other = tmp_path / "different.csv"
    other.write_text("order=id,value\nx,1\ny,2\nz,3\n", encoding="utf-8")
    multi_args = [str(csv), str(other), "--unique-column", "order=id", "--json"]
    assert check_main([*multi_args, "--unique-count-exact-limit", "order=id=3"]) == 1
    combined = json.loads(capsys.readouterr().out)
    verdicts = {Path(item["path"]).name: item["result"]["verdict"] for item in combined["sources"]}
    assert verdicts == {"keys.csv": "fail", "different.csv": "pass"}
    assert check_main([*multi_args, "--unique-count-exact-limit", "order=id=1"]) == 2
    assert json.loads(capsys.readouterr().out)["verdict"] == "inconclusive"
