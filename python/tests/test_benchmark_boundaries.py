"""Boundary attribution must preserve values, bounded streams and honest failures."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def bench(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / ".github/scripts"))
    spec = importlib.util.spec_from_file_location(
        "benchmark_boundaries", ROOT / ".github/scripts/benchmark_boundaries.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("kind", ["arrow_array", "arrow_table", "pandas", "polars", "arrow_stream"])
@pytest.mark.parametrize("offset", [0, 3])
def test_producers_preserve_values_and_serialized_metrics(bench, kind, offset):
    pytest.importorskip("pyarrow")
    if kind in ("pandas", "polars"):
        pytest.importorskip(kind)
    case = {
        "producer": kind,
        "rows": 103,
        "chunk_size": 103 if kind == "arrow_array" else 17,
        "offset": offset,
    }
    reference = bench.worker({"case": case, "reference": True})
    result = bench.preflight({"case": case, "expected": reference["columns"]})
    assert result["status"] == "complete"
    sample = result["samples"][0]
    assert sample["producer"]["rows"] == 103
    assert sample["producer"]["batches"] == (1 if kind == "arrow_array" else 7)
    seconds = sample["seconds"]
    assert seconds["end_to_end"] == pytest.approx(sum(seconds[s] for s in bench.STAGES[:-1]))
    bad = copy.deepcopy(reference["columns"])
    bad[0]["unique_count"] -= 1
    with pytest.raises(ValueError, match="metrics/absence/order mismatch"):
        bench.worker({"case": case, "expected": bad, "warmups": 0, "iterations": 1})


def test_stream_is_lazy_and_batch_memory_does_not_scale_with_rows(bench):
    pa = pytest.importorskip("pyarrow")
    observed = []
    for rows in (100, 1000):
        case = {"producer": "arrow_stream", "rows": rows, "chunk_size": 10, "offset": 3}
        evidence = dict.fromkeys(
            (
                "rows",
                "batches",
                "max_batch_rows",
                "max_batch_bytes",
                "observed_arrow_pool_bytes",
                "batch_preparation_seconds",
            ),
            0,
        )
        reader = bench.producer(pa, case, evidence)
        assert evidence["rows"] == 0
        for i, batch in enumerate(reader):
            assert evidence["rows"] == (i + 1) * 10
            assert batch.column(0).offset == 3
            assert batch.to_pydict() == bench.values(i * 10, 10)
        observed.append(evidence["max_batch_bytes"])
    assert max(observed) <= 10 * 40  # Three primitive/string columns, independent of total rows.


def test_isolated_worker_keeps_first_use_and_warmups(bench):
    pytest.importorskip("pyarrow")
    case = {"producer": "arrow_stream", "rows": 101, "chunk_size": 20, "offset": 3}
    reference = bench.worker({"case": case, "reference": True})
    result = bench.common.run_worker(
        {
            "tool": "stream",
            "threads": 1,
            "case": case,
            "expected": reference["columns"],
            "warmups": 1,
            "iterations": 2,
        },
        30,
        script=Path(bench.__file__),
    )
    assert result["pid"] != __import__("os").getpid()
    assert len(result["warmups"]) == 1
    assert len(result["samples"]) == 2
    assert result["process_seconds"] > sum(s["seconds"]["end_to_end"] for s in result["samples"])
    assert result["peak_rss"]["status"] in ("available", "unavailable")


def test_skips_have_reasons_and_stream_scaling_keeps_chunk_size(bench):
    matrix = bench.cases(100, [10, 100], 4)
    skipped = [c for c in matrix if "skip_reason" in c]
    assert all(c["producer"] == "arrow_array" and c["chunk_size"] == 10 for c in skipped)
    assert matrix[-1]["rows"] == 400
    assert matrix[-1]["chunk_size"] == 10
    assert bench.fixture_identity(100) == bench.fixture_identity(100)
    assert bench.fixture_identity(100)["sha256"] != bench.fixture_identity(101)["sha256"]


def test_failure_retains_completed_evidence_without_results(bench, tmp_path, monkeypatch):
    monkeypatch.setattr(bench.common, "environment_metadata", lambda: {})
    calls = 0

    def run(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise bench.common.WorkerError("unsupported type", stderr="decode error")
        return {"columns": [], "status": "complete"}

    monkeypatch.setattr(bench.common, "run_worker", run)
    output = tmp_path / "run"
    assert bench.main(["--rows", "100", "--chunks", "10", "--output", str(output)]) == 1
    retained = json.loads((output / "progress.json").read_text())
    assert retained["status"] == "incomplete"
    assert len(retained["preflights"]) == 1
    assert retained["failure"]["stderr"] == "decode error"
    assert not (output / "results.json").exists()


def test_runtime_budget_and_invalid_sizes_cannot_be_fast_successes(bench, tmp_path, monkeypatch):
    monkeypatch.setattr(bench.common, "environment_metadata", lambda: {})
    with pytest.raises(SystemExit):
        bench.main(["--rows", "100", "--chunks", "101", "--output", str(tmp_path / "bad")])
    ticks = iter([0, 1000])
    monkeypatch.setattr(bench.time, "monotonic", lambda: next(ticks))
    output = tmp_path / "budget"
    assert bench.main(["--rows", "100", "--chunks", "10", "--output", str(output)]) == 1
    assert "budget exhausted" in (output / "progress.json").read_text()
    assert not (output / "results.json").exists()
