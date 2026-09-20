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
    """Load the runner with its sibling harness available to imports."""
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
    """Every producer must preserve the fixture and fail when its metric evidence differs."""
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
    """Larger streams must request batches lazily without increasing individual buffer sizes."""
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
    """A real subprocess retains warmup evidence separately from measured samples."""
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
    """The matrix identifies unsupported chunked arrays and reproducible stream scaling."""
    matrix = bench.cases(100, [10, 100], 4)
    skipped = [c for c in matrix if "skip_reason" in c]
    assert all(c["producer"] == "arrow_array" and c["chunk_size"] == 10 for c in skipped)
    assert matrix[-1]["rows"] == 400
    assert matrix[-1]["chunk_size"] == 10
    assert bench.fixture_identity(100) == bench.fixture_identity(100)
    assert bench.fixture_identity(100)["sha256"] != bench.fixture_identity(101)["sha256"]


def test_failure_retains_completed_evidence_without_results(bench, tmp_path, monkeypatch):
    """Worker failures preserve earlier observations but cannot publish a successful result."""
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
    """Invalid fixture sizes and exhausted time budgets fail before measurement succeeds."""
    monkeypatch.setattr(bench.common, "environment_metadata", lambda: {})
    with pytest.raises(SystemExit):
        bench.main(["--rows", "100", "--chunks", "101", "--output", str(tmp_path / "bad")])
    ticks = iter([0, 1000])
    monkeypatch.setattr(bench.time, "monotonic", lambda: next(ticks))
    output = tmp_path / "budget"
    assert bench.main(["--rows", "100", "--chunks", "10", "--output", str(output)]) == 1
    assert "budget exhausted" in (output / "progress.json").read_text()
    assert not (output / "results.json").exists()


@pytest.fixture
def completed_run(bench, tmp_path, monkeypatch):
    """Exercise real orchestration and publication without timing native workers."""
    monkeypatch.setattr(bench.common, "environment_metadata", lambda: {})
    case = {"id": "test", "producer": "arrow_array", "rows": 100, "chunk_size": 100, "offset": 0}
    monkeypatch.setattr(bench, "cases", lambda *args: [case])

    def run(request, *args, **kwargs):
        if request.get("reference"):
            return {"columns": [], "status": "complete"}
        return {
            "status": "complete",
            "samples": [
                {"seconds": dict.fromkeys(bench.STAGES, 1.0)}
                for _ in range(request.get("iterations", 1))
            ],
        }

    monkeypatch.setattr(bench.common, "run_worker", run)
    output = tmp_path / "run"
    return output, [
        "--rows",
        "100",
        "--chunks",
        "100",
        "--iterations",
        "2",
        "--output",
        str(output),
    ]


@pytest.mark.parametrize(
    "failure", ["serialization", "render", "results.json", "boundaries.md", "rename", "checkpoint"]
)
def test_failed_publication_is_incomplete_and_removes_outputs(
    bench, completed_run, monkeypatch, failure
):
    """Inject failures throughout publication, including after a final output exists."""
    output, argv = completed_run
    write_text = Path.write_text
    replace = Path.replace
    checkpoint = bench.common.checkpoint

    def fail_write(path, *args, **kwargs):
        if path.name.removesuffix(".tmp") == failure:
            write_text(path, "partial output", encoding="utf-8")
            raise OSError("injected write failure")
        return write_text(path, *args, **kwargs)

    def fail_replace(path, target):
        if Path(target).name == "boundaries.md":
            raise OSError("injected rename failure")
        return replace(path, target)

    def fail_checkpoint(path, document):
        if document["status"] == "complete":
            raise OSError("injected completion failure")
        return checkpoint(path, document)

    if failure == "serialization":
        monkeypatch.setattr(bench, "summarize_runs", lambda runs: {"bad": float("nan")})
    elif failure == "render":

        def fail_render(document):
            raise ValueError("injected render failure")

        monkeypatch.setattr(bench, "render", fail_render)
    elif failure == "rename":
        monkeypatch.setattr(Path, "replace", fail_replace)
    elif failure == "checkpoint":
        monkeypatch.setattr(bench.common, "checkpoint", fail_checkpoint)
    else:
        monkeypatch.setattr(Path, "write_text", fail_write)
    assert bench.main(argv) == 1
    retained = json.loads((output / "progress.json").read_text())
    assert retained["status"] == "incomplete"
    assert retained["failure"]
    assert len(retained["runs"]) == 3
    assert not (output / "results.json").exists()
    assert not (output / "boundaries.md").exists()
    assert not list(output.glob("*.tmp"))


def test_completion_checkpoint_is_written_after_both_outputs(bench, completed_run, monkeypatch):
    """The complete checkpoint acts as the publication commit marker."""
    output, argv = completed_run
    write_text = Path.write_text
    observed = []

    def observe_write(path, contents, *args, **kwargs):
        if path.name.removesuffix(".tmp") in ("results.json", "boundaries.md"):
            observed.append(json.loads((output / "progress.json").read_text())["status"])
        if path.name == "progress.json.tmp" and json.loads(contents)["status"] == "complete":
            assert json.loads((output / "results.json").read_text())["status"] == "complete"
            assert "Python/Arrow boundary experiment" in (output / "boundaries.md").read_text()
        return write_text(path, contents, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", observe_write)
    assert bench.main(argv) == 0
    assert observed == ["incomplete", "incomplete"]
    assert json.loads((output / "progress.json").read_text())["status"] == "complete"
