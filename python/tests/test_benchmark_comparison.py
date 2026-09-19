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
    assert len(result["warmup_seconds"]) == 2
    assert result["first_operation_seconds"] == result["warmup_seconds"][0]
    assert result["import_setup_seconds"] >= 0


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
        if request.get("preflight") or request.get("control"):
            return {"pid": len(requests), "status": "complete", "process_seconds": 0.1}
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
    assert len(requests) == 13  # Preflights, three controls, six cold and two warm workers.
    assert all(r["preflight"] for r in requests[:2])
    measured = [r for r in requests[2:] if not r.get("control")]
    assert all(r["warmups"] == 0 and r["iterations"] == 1 for r in measured[:6])
    assert all(r["warmups"] == 2 and r["iterations"] == 3 for r in measured[6:])
    assert len(result["controls"]) == 3
    assert result["control_summary"]["median_seconds"] == 0.1
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
    assert first["first_operation_seconds"] == first["operation_seconds"][0]
    assert first["process_seconds"] > first["import_setup_seconds"] > 0


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


@pytest.fixture
def repeatable_run() -> dict[str, Any]:
    """A complete timing comparison with stable environment and overlapping IQRs."""
    return {
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


@pytest.mark.parametrize("changed_field", ["cpu", "git_commit", "git_status"])
def test_repeatability_reports_disjoint_iqr_and_rejects_changed_environment(
    changed_field, repeatable_run
):
    previous = repeatable_run
    current = copy.deepcopy(previous)
    assert bench.compare_runs(previous, current) == {"dataprof": {"warm": True}}
    current["results"]["dataprof"]["warm"] = {"q1_seconds": 3, "q3_seconds": 4}
    assert bench.compare_runs(previous, current) == {"dataprof": {"warm": False}}
    current["environment"][changed_field] = "different"
    with pytest.raises(ValueError, match=rf"environment\.{changed_field}"):
        bench.compare_runs(previous, current)


@pytest.mark.parametrize("field", ["resource_script_sha256", "resource_host"])
@pytest.mark.parametrize("previous_value", [None, "older"])
@pytest.mark.parametrize("enabled", [False, True])
def test_repeatability_checks_resource_metadata_only_when_enabled(
    repeatable_run, field, previous_value, enabled
):
    """Missing or changed collector metadata only invalidates resource measurements."""
    previous = repeatable_run
    if enabled:
        previous["config"]["resources"] = {"protocol_version": 1}
    if previous_value is not None:
        previous["environment"][field] = previous_value
    current = copy.deepcopy(previous)
    current["environment"][field] = "current"
    if enabled:
        with pytest.raises(ValueError, match=rf"environment\.{field}"):
            bench.compare_runs(previous, current)
    else:
        assert bench.compare_runs(previous, current) == {"dataprof": {"warm": True}}


@pytest.mark.parametrize(
    "args",
    [
        ["--iterations", "1"],
        ["--timeout", "nan"],
        ["--tools", "pandas", "pandas"],
        ["--reference", "polars", "--tools", "pandas"],
        ["--blocks", "0"],
        ["--publication"],
        ["--publication", "--host-description", "  "],
        ["--publication", "--host-description", "controlled host", "--iterations", "3"],
        ["--publication", "--host-description", "controlled host", "--blocks", "1"],
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
        if not request.get("preflight") and not request.get("control"):
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


@pytest.mark.parametrize("filename", ["comparison.md", "results.json"])
@pytest.mark.parametrize("interrupted", [False, True])
def test_export_failure_keeps_run_incomplete(tmp_path, monkeypatch, filename, interrupted):
    monkeypatch.setattr(bench, "environment_metadata", lambda: {})

    def run(request, timeout):
        if request.get("preflight") or request.get("control"):
            return {"pid": 1, "status": "complete", "process_seconds": 0.1}
        return {
            "pid": 2,
            "process_seconds": 2.0,
            "operation_seconds": [1.0] * request["iterations"],
        }

    monkeypatch.setattr(bench, "run_worker", run)
    write_text = Path.write_text

    def fail_export(path, *args, **kwargs):
        if path.name == filename:
            if interrupted:
                # Bypass the Exception cleanup, as an abrupt interruption would.
                raise KeyboardInterrupt("injected export interruption")
            raise OSError("injected export failure")
        return write_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", fail_export)
    args = ["--output", str(tmp_path), "--tools", "pandas", "--rows", "100", "--iterations", "2"]
    if interrupted:
        with pytest.raises(KeyboardInterrupt):
            bench.main(args)
    else:
        assert bench.main(args) == 1
    progress = json.loads((tmp_path / "progress.json").read_text())
    assert progress["status"] == "incomplete"
    assert progress["active_stage"]["stage"] == "export"
    if not interrupted:
        assert progress["failure"]["stage"] == "export"
    assert len(progress["runs"]) == 3
    assert progress["results"]["pandas"]["cold"]["median_seconds"] == 2.0
    assert not (tmp_path / "results.json").exists()
    assert (tmp_path / "comparison.md").exists() == (interrupted and filename == "results.json")


def test_publication_retains_independent_blocks_and_every_first_sample(tmp_path, monkeypatch):
    monkeypatch.setattr(bench, "environment_metadata", lambda: {})
    requests = []

    def run(request, timeout):
        requests.append(request)
        if request.get("preflight") or request.get("control"):
            return {"pid": len(requests), "status": "complete", "process_seconds": 0.1}
        return {
            "pid": len(requests),
            "process_seconds": float(len(requests)),
            "operation_seconds": [float(len(requests))] * request["iterations"],
        }

    monkeypatch.setattr(bench, "run_worker", run)
    assert (
        bench.main(
            [
                "--output",
                str(tmp_path),
                "--tools",
                "pandas",
                "--rows",
                "100",
                "--publication",
                "--host-description",
                "lab host; AC; idle",
            ]
        )
        == 0
    )
    result = json.loads((tmp_path / "results.json").read_text())
    assert result["config"]["iterations"] == 21
    assert result["config"]["blocks"] == 3
    assert result["evidence_status"] == "diagnostic"
    assert len(result["controls"]) == 63
    assert len(result["block_results"]) == 3
    assert [run["invocation"] for run in result["runs"]].count(
        "first_fixture_operation_after_preflight"
    ) == 1
    cold = [r["process_seconds"] for r in result["runs"] if r["mode"] == "cold"]
    assert result["results"]["pandas"]["cold"]["samples_seconds"] == cold
    assert len(cold) == 63
    warm = [r for r in result["runs"] if r["mode"] == "warm"]
    assert len({r["pid"] for r in warm}) == 3
    assert result["results"]["pandas"]["warm"]["sample_count"] == 63
    for block in result["block_results"]:
        assert block["results"]["pandas"]["cold"]["sample_count"] == 21


def test_import_and_first_operation_boundaries_are_measured_directly(monkeypatch):
    ticks = iter([0, 5_000_000_000, 10_000_000_000, 13_000_000_000, 20_000_000_000, 21_000_000_000])
    monkeypatch.setattr(bench.time, "perf_counter_ns", lambda: next(ticks))
    monkeypatch.setattr(bench, "operation", lambda *args: lambda: {"rows": 100})
    result = bench.worker(
        {
            "tool": "pandas",
            "path": "unused",
            "threads": 1,
            "warmups": 1,
            "iterations": 1,
            "expected": {"rows": 100},
        }
    )
    assert result["import_setup_seconds"] == 5
    assert result["first_operation_seconds"] == 3
    assert result["warmup_seconds"] == [3]
    assert result["operation_seconds"] == [1]


def test_minimal_control_does_not_import_harness_or_tool(tmp_path, monkeypatch):
    (tmp_path / "pandas.py").write_text("raise RuntimeError('no tool import')")
    (tmp_path / "argparse.py").write_text("raise RuntimeError('no harness import')")
    monkeypatch.setenv("PYTHONPATH", str(tmp_path))
    result = bench.run_worker({"tool": "minimal-worker", "threads": 1, "control": True}, 60)
    assert result["status"] == "complete"
    assert result["process_seconds"] > 0


@pytest.mark.parametrize("cpu", ["x86_64", "armv8l", "armv7l", "ppc64le", "riscv64"])
def test_cpu_model_falls_back_when_processor_is_only_architecture(tmp_path, monkeypatch, cpu):
    cpuinfo = tmp_path / "cpuinfo"
    cpuinfo.write_text("processor : 0\nmodel name : Example CPU 1234\n")
    monkeypatch.setattr(bench.platform, "machine", lambda: "aarch64")
    monkeypatch.setattr(bench.platform, "processor", lambda: cpu)
    monkeypatch.setattr(bench, "Path", lambda path: cpuinfo)
    assert bench.cpu_model() == "Example CPU 1234"


@pytest.mark.parametrize(
    "cpu",
    [
        "unknown",
        "",
        "x86_64",
        "AMD64",
        "aarch64",
        "armv8l",
        " ARMv7L ",
        "armv8-a",
        "arm64e",
        "aarch64_be",
        "i486",
        "x86_64_v3",
        "ppc64le",
        "powerpc64",
        "mips64el",
        "s390x",
        "riscv64",
        "sparcv9",
        "loongarch64",
    ],
)
def test_unknown_hardware_cannot_establish_matched_repeat_run(cpu, monkeypatch):
    monkeypatch.setattr(bench.platform, "machine", lambda: "aarch64")
    document = {"fixture": {"sha256": "same"}, "config": {}, "environment": {"cpu": cpu}}
    with pytest.raises(ValueError, match="known CPU model"):
        bench.compare_runs(document, document)


@pytest.mark.parametrize(
    "cpu",
    [
        "Intel(R) Core(TM) Ultra 7 258V",
        "AMD Ryzen 9 7950X",
        "Apple M4",
        "ARM Cortex-A72",
        "POWER9",
        "Loongson-3A5000",
    ],
)
def test_concrete_cpu_models_remain_accepted(cpu):
    assert bench.known_cpu(cpu)
