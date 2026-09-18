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
        if request.get("preflight"):
            return {"pid": len(requests), "status": "complete"}
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
    assert len(requests) == 10  # Two import preflights, six cold workers, two warm workers.
    assert all(r["preflight"] for r in requests[:2])
    assert all(r["warmups"] == 0 and r["iterations"] == 1 for r in requests[2:8])
    assert all(r["warmups"] == 2 and r["iterations"] == 3 for r in requests[8:])
    assert result["status"] == "complete"
    assert "subsequent to preflight" in result["config"]["preflight"]
    assert json.loads((output / "progress.json").read_text())["status"] == "complete"
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
    progress = json.loads((tmp_path / "progress.json").read_text())
    assert progress["status"] == "incomplete"
    assert progress["failure"]["stage"] == "preflight"
    assert progress["failure"]["tool"] == "dataprof"
    assert progress["runs"] == []


def test_preflight_imports_without_running_an_operation(monkeypatch):
    imported = []

    def operation(tool, path, threads):
        imported.append(tool)

        def unexpected():
            pytest.fail("preflight must not read the fixture or initialize an operation")

        return unexpected

    monkeypatch.setattr(bench, "operation", operation)
    assert (
        bench.worker({"tool": "pandas", "path": "nonexistent", "threads": 1, "preflight": True})[
            "status"
        ]
        == "complete"
    )
    assert imported == ["pandas"]


def test_preflight_only_has_no_measurements(tmp_path, monkeypatch):
    monkeypatch.setattr(bench, "environment_metadata", lambda: {})
    assert bench.main(["--output", str(tmp_path), "--tools", "pandas", "--preflight-only"]) == 0
    progress = json.loads((tmp_path / "progress.json").read_text())
    assert progress["status"] == "complete"
    assert progress["preflight"][0]["tool"] == "pandas"
    assert progress["runs"] == []
    assert not (tmp_path / "fixture.csv").exists()
    assert not (tmp_path / "results.json").exists()


def test_broken_adapter_import_retains_diagnostics(tmp_path, monkeypatch):
    adapters = tmp_path / "adapters"
    adapters.mkdir()
    (adapters / "pandas.py").write_text("import deliberately_unavailable_benchmark_dependency\n")
    monkeypatch.setenv("PYTHONPATH", str(adapters))
    monkeypatch.setattr(bench, "environment_metadata", lambda: {})
    output = tmp_path / "output"
    assert bench.main(["--output", str(output), "--tools", "pandas"]) == 1
    progress = json.loads((output / "progress.json").read_text())
    assert progress["status"] == "incomplete"
    assert progress["failure"]["stage"] == "preflight"
    assert progress["failure"]["tool"] == "pandas"
    assert progress["failure"]["returncode"] != 0
    assert "deliberately_unavailable_benchmark_dependency" in progress["failure"]["stderr"]
    assert progress["runs"] == []
    assert not (output / "results.json").exists()


@pytest.mark.parametrize("mode", ["cold", "warm"])
def test_failure_retains_completed_workers_and_existing_criterion(tmp_path, monkeypatch, mode):
    criterion = tmp_path / "target/criterion/example/new/estimates.json"
    criterion.parent.mkdir(parents=True)
    criterion.write_text('{"mean": {"point_estimate": 123}}')
    monkeypatch.setattr(bench, "environment_metadata", lambda: {})
    real_worker = bench.run_worker
    completed = []

    def fail_later(request, timeout):
        if not request.get("preflight"):
            if (mode == "cold" and len(completed) == 1) or (mode == "warm" and request["warmups"]):
                # Exercise a real worker protocol failure, after real measurements.
                return real_worker({**request, "tool": "deliberately-broken-adapter"}, timeout)
            result = real_worker(request, timeout)
            completed.append(result)
            return result
        return real_worker(request, timeout)

    monkeypatch.setattr(bench, "run_worker", fail_later)
    output = tmp_path / "benchmark-results/comparison"
    assert (
        bench.main(
            ["--output", str(output), "--tools", "pandas", "--rows", "100", "--iterations", "2"]
        )
        == 1
    )
    progress = json.loads((output / "progress.json").read_text())
    assert progress["status"] == "incomplete"
    assert progress["failure"]["stage"] == mode
    assert progress["failure"]["tool"] == "pandas"
    assert "deliberately-broken-adapter" in progress["failure"]["stderr"]
    assert len(progress["runs"]) == len(completed) > 0
    assert all(run["operation_seconds"][0] > 0 for run in progress["runs"])
    assert progress["results"] == {}
    assert not (output / "results.json").exists()
    assert not (output / "comparison.md").exists()
    assert json.loads(criterion.read_text())["mean"]["point_estimate"] == 123


@pytest.mark.parametrize("failure", ["timeout", "invalid_json"])
def test_worker_protocol_failure_preserves_output(monkeypatch, failure):
    def run(*args, **kwargs):
        if failure == "timeout":
            raise subprocess.TimeoutExpired(
                "worker", 1, output=b"partial stdout", stderr=b"details"
            )
        return subprocess.CompletedProcess(args, 0, "partial stdout", "details")

    monkeypatch.setattr(bench.subprocess, "run", run)
    with pytest.raises(bench.WorkerError) as exc:
        bench.run_worker({"tool": "pandas", "threads": 1}, 1)
    assert exc.value.diagnostics["stdout"] == "partial stdout"
    assert exc.value.diagnostics["stderr"] == "details"


def test_environment_failure_writes_incomplete_status(tmp_path, monkeypatch):
    def fail():
        raise ModuleNotFoundError("missing psutil")

    monkeypatch.setattr(bench, "environment_metadata", fail)
    assert bench.main(["--output", str(tmp_path)]) == 1
    progress = json.loads((tmp_path / "progress.json").read_text())
    assert progress["failure"]["stage"] == "environment"
    assert progress["failure"]["type"] == "ModuleNotFoundError"
    assert "missing psutil" in progress["failure"]["traceback"]


def test_incomplete_repeatability_input_is_rejected():
    with pytest.raises(ValueError, match="complete previous run"):
        bench.compare_runs({"status": "incomplete"}, {})


def test_unreadable_baseline_fails_before_measurements(tmp_path, monkeypatch):
    def unexpected():
        pytest.fail("invalid baseline must fail before preparing the benchmark environment")

    monkeypatch.setattr(bench, "environment_metadata", unexpected)
    output = tmp_path / "run"
    assert (
        bench.main(["--output", str(output), "--compare", str(tmp_path / "missing-results.json")])
        == 1
    )
    progress = json.loads((output / "progress.json").read_text())
    assert progress["failure"]["stage"] == "compare_input"
    assert progress["runs"] == []


def test_export_failure_removes_success_files_but_retains_evidence(tmp_path, monkeypatch):
    monkeypatch.setattr(bench, "environment_metadata", lambda: {})

    def run(request, timeout):
        if request.get("preflight"):
            return {"pid": 1, "status": "complete"}
        return {
            "pid": 2,
            "process_seconds": 2.0,
            "operation_seconds": [1.0] * request["iterations"],
        }

    monkeypatch.setattr(bench, "run_worker", run)
    write_text = Path.write_text

    def fail_table(path, *args, **kwargs):
        if path.name == "comparison.md":
            raise OSError("injected table export failure")
        return write_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", fail_table)
    assert (
        bench.main(
            ["--output", str(tmp_path), "--tools", "pandas", "--rows", "100", "--iterations", "2"]
        )
        == 1
    )
    progress = json.loads((tmp_path / "progress.json").read_text())
    assert progress["status"] == "incomplete"
    assert progress["failure"]["stage"] == "export"
    assert len(progress["runs"]) == 3
    assert progress["results"]["pandas"]["cold"]["median_seconds"] == 2.0
    assert not (tmp_path / "results.json").exists()
    assert not (tmp_path / "comparison.md").exists()
