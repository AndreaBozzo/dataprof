"""Protect benchmark isolation, honest failures, and statistical reporting."""

from __future__ import annotations

import copy
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "benchmark_comparison", ROOT / ".github/scripts/benchmark_comparison.py"
)
assert SPEC is not None and SPEC.loader is not None
bench = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bench)


def test_fixture_is_reproducible_and_records_nulls(tmp_path):
    first = bench.make_fixture(tmp_path / "first.csv", 100)
    second = bench.make_fixture(tmp_path / "second.csv", 100)
    assert first["sha256"] == second["sha256"]
    assert first["expected"] == {
        "rows": 100,
        "columns": ["id", "amount", "category"],
        "null_counts": [0, 10, 0],
    }
    assert "東京" in (tmp_path / "first.csv").read_text(encoding="utf-8")
    assert bench.make_fixture(tmp_path / "third.csv", 101)["sha256"] != first["sha256"]


def test_statistics_keep_raw_outliers_and_inclusive_iqr():
    summary = bench.summarize([1, 2, 3, 4, 100])
    assert summary["median_seconds"] == 3
    assert summary["q1_seconds"] == 2
    assert summary["q3_seconds"] == 4
    assert summary["iqr_seconds"] == 2
    assert summary["max_seconds"] == 100
    assert summary["samples_seconds"] == [1, 2, 3, 4, 100]
    assert bench.relative_time(1, 2) == "2.00x slowdown"
    assert bench.relative_time(2, 1) == "2.00x speedup"


def test_worker_warms_up_but_only_records_measured_calls(monkeypatch):
    calls = []

    def run():
        calls.append(1)
        return {"rows": 100}

    monkeypatch.setattr(bench, "operation", lambda *args: run)
    result = bench.worker(
        {
            "tool": "pandas",
            "path": "unused",
            "threads": 1,
            "warmups": 2,
            "iterations": 3,
            "expected": {"rows": 100},
        }
    )
    assert len(calls) == 5
    assert len(result["operation_seconds"]) == 3


@pytest.mark.parametrize("warmups", [0, 1])
def test_wrong_results_never_become_fast_successes(monkeypatch, warmups):
    monkeypatch.setattr(bench, "operation", lambda *args: lambda: {"rows": 99})
    with pytest.raises(ValueError, match="fixture mismatch"):
        bench.worker(
            {
                "tool": "pandas",
                "path": "unused",
                "threads": 1,
                "warmups": warmups,
                "iterations": 2,
                "expected": {"rows": 100},
            }
        )


def test_subprocess_failure_is_not_a_sample(monkeypatch):
    def failed(*args, **kwargs):
        return subprocess.CompletedProcess(args, 1, "", "decode failed")

    monkeypatch.setattr(bench.subprocess, "run", failed)
    with pytest.raises(RuntimeError, match="decode failed"):
        bench.run_worker({"tool": "pandas", "threads": 1}, 10)


def test_matrix_separates_process_and_operation_timers(tmp_path, monkeypatch):
    requests = []
    monkeypatch.setattr(bench, "environment_metadata", lambda: {})

    def run(request, timeout):
        requests.append(request)
        return {
            "pid": len(requests),
            "process_seconds": 10.0,
            "operation_seconds": [1.0] * request["iterations"],
        }

    monkeypatch.setattr(bench, "run_worker", run)
    output = tmp_path / "run"
    assert (
        bench.main(
            [
                "--output",
                str(output),
                "--tools",
                "pandas",
                "polars",
                "--iterations",
                "3",
                "--rows",
                "100",
            ]
        )
        == 0
    )
    result = json.loads((output / "results.json").read_text())
    assert len(requests) == 8  # Three fresh processes per tool, then one warm process per tool.
    assert all(r["warmups"] == 0 and r["iterations"] == 1 for r in requests[:6])
    assert all(r["warmups"] == 2 and r["iterations"] == 3 for r in requests[6:])
    for tool in ("pandas", "polars"):
        assert result["results"][tool]["cold"]["median_seconds"] == 10
        assert result["results"][tool]["warm"]["median_seconds"] == 1
    with pytest.raises(SystemExit):
        bench.main(["--output", str(output)])


def test_real_pandas_workers_are_fresh_processes(tmp_path):
    fixture = bench.make_fixture(tmp_path / "fixture.csv", 100)
    request = {
        "tool": "pandas",
        "path": fixture["path"],
        "threads": 1,
        "warmups": 0,
        "iterations": 1,
        "expected": fixture["expected"],
    }
    first = bench.run_worker(request, 60)
    second = bench.run_worker(request, 60)
    assert first["pid"] != second["pid"]
    assert first["observed"] == second["observed"] == fixture["expected"]
    assert first["process_seconds"] > first["operation_seconds"][0] > 0


def test_cache_eviction_is_never_silently_emulated(tmp_path, monkeypatch):
    path = tmp_path / "fixture.csv"
    path.write_text("id\n1\n")
    monkeypatch.delattr(bench.os, "posix_fadvise", raising=False)
    with pytest.raises(RuntimeError, match="POSIX_FADV_DONTNEED"):
        bench.prepare_cache(path, "evict")


def test_worker_protocol_has_json_only_on_stdout(tmp_path):
    fixture = bench.make_fixture(tmp_path / "fixture.csv", 100)
    completed = subprocess.run(
        [sys.executable, str(ROOT / ".github/scripts/benchmark_comparison.py"), "--worker"],
        input=json.dumps(
            {
                "tool": "pandas",
                "path": fixture["path"],
                "threads": 1,
                "warmups": 1,
                "iterations": 2,
                "expected": fixture["expected"],
            }
        ),
        capture_output=True,
        text=True,
        check=True,
        timeout=60,
    )
    assert len(json.loads(completed.stdout)["operation_seconds"]) == 2


def test_repeatability_refuses_changed_fixture():
    with pytest.raises(ValueError, match="same fixture"):
        bench.compare_runs({"fixture": {"sha256": "a"}}, {"fixture": {"sha256": "b"}})


@pytest.mark.parametrize("changed_field", ["cpu", "git_commit", "git_status"])
def test_repeatability_reports_disjoint_iqr_and_rejects_changed_environment(changed_field):
    previous: dict[str, Any] = {
        "fixture": {"sha256": "same"},
        "config": {"threads": 1},
        "environment": dict.fromkeys(
            [
                "python",
                "os",
                "architecture",
                "cpu",
                "hostname",
                "logical_cpus",
                "ram_bytes",
                "versions",
                "git_commit",
                "git_status",
                "native_extensions",
                "benchmark_script_sha256",
                "benchmark_lock_sha256",
                "cargo_lock_sha256",
                "build_environment",
            ],
            "same",
        ),
        "results": {"dataprof": {"warm": {"q1_seconds": 1, "q3_seconds": 2}}},
    }
    current = copy.deepcopy(previous)
    assert bench.compare_runs(previous, current) == {"dataprof": {"warm": True}}
    current["results"]["dataprof"]["warm"] = {"q1_seconds": 3, "q3_seconds": 4}
    assert bench.compare_runs(previous, current) == {"dataprof": {"warm": False}}
    current["environment"][changed_field] = "different"
    with pytest.raises(ValueError, match=rf"environment\.{changed_field}"):
        bench.compare_runs(previous, current)


@pytest.mark.parametrize(
    "args",
    [
        ["--iterations", "1"],
        ["--timeout", "nan"],
        ["--tools", "pandas", "pandas"],
        ["--reference", "polars", "--tools", "pandas"],
    ],
)
def test_invalid_configuration_fails_before_work(args):
    with pytest.raises(SystemExit) as exc:
        bench.main(args)
    assert exc.value.code == 2


def test_invalid_run_never_publishes_results(tmp_path, monkeypatch):
    monkeypatch.setattr(bench, "environment_metadata", lambda: {})

    def fail(*args):
        raise RuntimeError("bad metric")

    monkeypatch.setattr(bench, "run_worker", fail)
    assert bench.main(["--output", str(tmp_path), "--rows", "100"]) == 1
    assert not (tmp_path / "results.json").exists()
    assert not (tmp_path / "comparison.md").exists()
