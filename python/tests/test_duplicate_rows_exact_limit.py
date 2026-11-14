"""A configurable exact limit for the duplicate-row count (#1292).

Past one million distinct rows the duplicate-row count became an estimate,
which witnesses no duplicate, so ``max_duplicate_rows`` could not fail on a
large source. ``duplicate_rows_exact_limit=`` sets the limit on every input
route, the report records it next to ``duplicate_rows_approximate``, and a
check left undecided by an estimate names the setting.

``tests/duplicate_rows_exact_limit.rs`` is the Rust twin.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

try:
    import dataprof as dp
    from dataprof.asyncio import (
        _HAS_ASYNC,
        profile_bytes as profile_bytes_async,
        profile_file as profile_file_async,
    )
    from dataprof.check.__main__ import main as check_main
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

# Distinct rows, and how many of them appear twice.
DISTINCT = 3_000
DUPLICATES = 40
# Under the distinct rows, so the count must be estimated; over them, exact.
LOW_LIMIT = 1_000
HIGH_LIMIT = 5_000
DEFAULT_LIMIT = 1_000_000

IDS = list(range(DISTINCT)) + list(range(DUPLICATES))
LABELS = [f"row-{i}" for i in IDS]
CSV = "id,label\n" + "".join(f"{i},row-{i}\n" for i in IDS)
RECORDS = [{"id": i, "label": label} for i, label in zip(IDS, LABELS, strict=True)]


def _routes(tmp_path: Path) -> dict[str, Callable[..., Any]]:
    csv = tmp_path / "t.csv"
    csv.write_text(CSV)
    as_json = tmp_path / "t.json"
    as_json.write_text(json.dumps(RECORDS))
    jsonl = tmp_path / "t.jsonl"
    jsonl_text = "".join(json.dumps(r) + "\n" for r in RECORDS)
    jsonl.write_text(jsonl_text)
    columns = {"id": IDS, "label": LABELS}

    routes: dict[str, Callable[..., Any]] = {
        "csv auto": lambda **k: dp.profile(csv, **k),
        "csv incremental": lambda **k: dp.profile(csv, engine="incremental", **k),
        "csv columnar": lambda **k: dp.profile(csv, engine="columnar", **k),
        "json": lambda **k: dp.profile(as_json, **k),
        "jsonl": lambda **k: dp.profile(jsonl, **k),
        "profile_file": lambda **k: dp.profile_file(csv, **k),
        "dict": lambda **k: dp.profile(columns, **k),
        "records": lambda **k: dp.profile(RECORDS, **k),
        "csv bytes": lambda **k: dp.profile(CSV.encode(), format="csv", **k),
        "jsonl bytes": lambda **k: dp.profile(jsonl_text.encode(), format="jsonl", **k),
        "Profiler": lambda **k: _profiler(csv, **k),
    }
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        pass
    else:
        table = pa.table(columns)
        parquet = tmp_path / "t.parquet"
        pq.write_table(table, parquet)
        routes["parquet"] = lambda **k: dp.profile(parquet, **k)
        routes["parquet bytes"] = lambda **k: dp.profile(
            parquet.read_bytes(), format="parquet", **k
        )
        routes["arrow table"] = lambda **k: dp.profile(table, **k)
        routes["arrow stream"] = lambda **k: dp.profile(
            pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=500)),
            **k,
        )
    try:
        import pandas as pd
    except ImportError:
        pass
    else:
        routes["pandas"] = lambda **k: dp.profile(pd.DataFrame(columns), **k)
    if _HAS_ASYNC:
        routes["async bytes"] = lambda **k: asyncio.run(
            profile_bytes_async(CSV.encode(), format="csv", **k)
        )
        routes["async file"] = lambda **k: asyncio.run(profile_file_async(csv, **k))
    return routes


def _profiler(path: Path, duplicate_rows_exact_limit: int | None = None) -> Any:
    profiler = dp.Profiler()
    if duplicate_rows_exact_limit is not None:
        profiler = profiler.duplicate_rows_exact_limit(duplicate_rows_exact_limit)
    return profiler.profile(path)


def _wrong_routes(tmp_path: Path, limit: int | None, approximate: bool) -> list[Any]:
    recorded = DEFAULT_LIMIT if limit is None else limit
    kwargs = {} if limit is None else {"duplicate_rows_exact_limit": limit}
    wrong: list[Any] = []
    for name, run in _routes(tmp_path).items():
        report = run(**kwargs)
        uniqueness = report.quality.uniqueness
        if uniqueness is None:
            wrong.append((name, "uniqueness not assessed"))
            continue
        if uniqueness["duplicate_rows_approximate"] is not approximate:
            wrong.append((name, "approximate", uniqueness))
        if not approximate and uniqueness["duplicate_rows"] != DUPLICATES:
            wrong.append((name, "count", uniqueness["duplicate_rows"]))
        # An estimate decides nothing; an exact count fails on what it saw.
        status = report.check(max_duplicate_rows=0).checks[0].status
        if (status == "failed") is approximate:
            wrong.append((name, "gate", status))
        summary = report.to_dict()["quality"]["uniqueness"].get("duplicate_rows_exact_limit")
        document = json.loads(report.to_json())["quality"]["metrics"]["uniqueness"].get(
            "duplicate_rows_exact_limit"
        )
        restored = dp.ProfileReport.from_dict(report.to_dict()).to_dict()
        restored_limit = restored["quality"]["uniqueness"].get("duplicate_rows_exact_limit")
        if not (
            uniqueness.get("duplicate_rows_exact_limit")
            == summary
            == document
            == restored_limit
            == recorded
        ):
            wrong.append((name, uniqueness, summary, document, restored_limit))
    return wrong


def test_every_route_estimates_past_a_lowered_limit_and_records_it(tmp_path: Path) -> None:
    wrong = _wrong_routes(tmp_path, LOW_LIMIT, approximate=True)
    assert not wrong, wrong


def test_every_route_counts_exactly_under_a_limit_and_records_it(tmp_path: Path) -> None:
    wrong = _wrong_routes(tmp_path, HIGH_LIMIT, approximate=False)
    assert not wrong, wrong


def test_without_a_limit_every_route_records_the_default(tmp_path: Path) -> None:
    wrong = _wrong_routes(tmp_path, None, approximate=False)
    assert not wrong, wrong


def test_a_set_limit_survives_the_memory_budget(tmp_path: Path) -> None:
    # 120,000 distinct rows hold about 2 MB of fingerprints, over a 1 MB budget.
    csv = tmp_path / "big.csv"
    csv.write_text("id,label\n" + "".join(f"{i},row-{i}\n" for i in [*range(120_000), *range(9)]))

    def uniqueness(**options: Any) -> dict[str, Any]:
        quality = dp.profile(csv, memory_limit_mb=1, **options).quality
        assert quality is not None and quality.uniqueness is not None
        return quality.uniqueness

    wrong = []
    for engine in ("incremental", "columnar"):
        spilled = uniqueness(engine=engine)
        if not spilled["duplicate_rows_approximate"]:
            wrong.append((engine, "the budget no longer spills; the test reaches nothing"))
        kept = uniqueness(engine=engine, duplicate_rows_exact_limit=200_000)
        if kept["duplicate_rows_approximate"] or kept["duplicate_rows"] != 9:
            wrong.append((engine, kept))
    assert not wrong, wrong


def test_the_config_reads_back_and_refuses_a_limit_that_counts_nothing() -> None:
    assert dp.ProfilerConfig(duplicate_rows_exact_limit=7).duplicate_rows_exact_limit == 7
    assert dp.ProfilerConfig().duplicate_rows_exact_limit is None
    with pytest.raises(ValueError, match="duplicate_rows_exact_limit must be at least 1"):
        dp.profile({"a": [1]}, duplicate_rows_exact_limit=0)
    with pytest.raises(ValueError, match="duplicate_rows_exact_limit must be an integer"):
        dp.profile({"a": [1]}, duplicate_rows_exact_limit=-1)


def test_an_undecided_check_names_the_setting_and_a_raised_limit_decides_it(
    tmp_path: Path,
) -> None:
    csv = tmp_path / "t.csv"
    csv.write_text(CSV)
    policy: dict[str, Any] = {"max_duplicate_rows": 0, "forbid_findings": ["duplicate_rows"]}
    rows = DISTINCT + DUPLICATES
    hint = (
        f"It was counted under a duplicate_rows_exact_limit of {LOW_LIMIT} distinct rows; "
        f"a limit of {rows}, the rows checked, counts every row exactly"
    )

    estimated = dp.profile(csv, duplicate_rows_exact_limit=LOW_LIMIT).check(**policy)
    assert estimated.verdict == "inconclusive"
    for check in estimated.checks:
        assert check.status == "not_evaluated", check
        assert check.message.endswith(hint), check.message

    decided = dp.profile(csv, duplicate_rows_exact_limit=rows).check(**policy)
    assert decided.verdict == "fail"
    assert [check.status for check in decided.checks] == ["failed", "failed"]


def test_the_check_command_takes_the_limit(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    csv = tmp_path / "t.csv"
    csv.write_text(CSV)
    argv = [str(csv), "--max-duplicate-rows", "0", "--json"]

    assert check_main([*argv, "--duplicate-rows-exact-limit", str(LOW_LIMIT)]) == 2
    undecided = json.loads(capsys.readouterr().out)
    assert "duplicate_rows_exact_limit" in undecided["checks"][0]["message"]

    assert check_main([*argv, "--duplicate-rows-exact-limit", str(HIGH_LIMIT)]) == 1
    failed = json.loads(capsys.readouterr().out)
    assert failed["checks"][0]["observed"] == DUPLICATES

    with pytest.raises(SystemExit) as refused:
        check_main([*argv, "--duplicate-rows-exact-limit", "0"])
    assert refused.value.code == 3
