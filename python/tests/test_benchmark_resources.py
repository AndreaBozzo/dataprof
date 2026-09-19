"""Protect resource units, counter wraparound, scope, and unavailable evidence."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "benchmark_comparison_resources", ROOT / ".github/scripts/benchmark_comparison.py"
)
assert SPEC is not None and SPEC.loader is not None
bench = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bench)
resources = bench.resource_collector()


def config(tmp_path):
    return {
        "powercap_root": str(tmp_path / "missing"),
        "poll_seconds": 0.01,
        "idle_seconds": 0.01,
        "max_zone_watts": 10000,
    }


def zone_files(path, name="package-0", value="900", maximum="1000"):
    path.mkdir(parents=True)
    (path / "energy_uj").write_text(value)
    (path / "max_energy_range_uj").write_text(maximum)
    (path / "name").write_text(name)


@pytest.mark.parametrize("before,after,expected", [(100, 300, 200), (900, 100, 200), (100, 100, 0)])
def test_counter_units_and_single_wrap(before, after, expected):
    assert resources.energy_delta(before, after, 1000, 0.0005, 1) == expected


@pytest.mark.parametrize("before,after", [(900, 100), (100, 300), (100, 100)])
def test_ambiguous_intervals_are_unavailable_even_without_observed_wrap(before, after):
    with pytest.raises(ValueError, match="hide a full wrap"):
        resources.energy_delta(before, after, 1000, 0.001, 1)


def test_counter_reset_or_violated_power_assumption_is_not_energy():
    with pytest.raises(ValueError, match="exceeds declared power bound"):
        resources.energy_delta(600, 100, 1000, 0.0001, 1)
    with pytest.raises(ValueError, match="outside"):
        resources.energy_delta(0, 1000, 1000, 0.0001, 1)


def test_discovery_keeps_parent_and_child_separate_and_read_errors_explicit(tmp_path):
    zone_files(tmp_path / "package")
    zone_files(tmp_path / "package/core", name="core")
    zone_files(tmp_path / "broken", value="not a number")
    result = resources.discover_zones(tmp_path)
    assert result["status"] == "available"
    assert len(result["zones"]) == 3
    assert {z["name"] for z in result["zones"] if z["status"] == "available"} == {
        "package-0",
        "core",
    }
    broken = next(z for z in result["zones"] if z["status"] == "unavailable")
    assert "invalid literal" in broken["reason"]
    assert "energy_uj" not in broken
    assert resources.discover_zones(tmp_path / "missing")["status"] == "unavailable"


def test_sysfs_symlink_aliases_are_deduplicated(tmp_path):
    real = tmp_path / "devices/package"
    zone_files(real)
    root = tmp_path / "class"
    root.mkdir()
    try:
        (root / "alias-one").symlink_to(real, target_is_directory=True)
        (root / "alias-two").symlink_to(real, target_is_directory=True)
    except OSError:
        pytest.skip("creating directory symlinks requires host privileges")
    assert len(resources.discover_zones(root)["zones"]) == 1


def test_meter_retains_raw_reads_and_corrects_multiple_observed_wraps(tmp_path, monkeypatch):
    zone_files(tmp_path / "package")
    discovery = resources.discover_zones(tmp_path)
    meter = resources.EnergyMeter(discovery, 0.01, 1)
    path = discovery["zones"][0]["path"]
    meter.readings[path] = [
        {"read_start_ns": i * 500000, "read_end_ns": i * 500000 + 100, "energy_uj": value}
        for i, value in enumerate([900, 100, 500, 900, 100])
    ]
    monkeypatch.setattr(meter, "read", lambda: None)
    zone = meter.finish()["zones"][0]
    assert zone["energy_uj"] == 1200
    assert zone["readings"] == meter.readings[path]
    meter.readings[path][2] = {
        "read_start_ns": 1000000,
        "read_end_ns": 1000100,
        "error": "permission denied",
    }
    failed = meter.finish()["zones"][0]
    assert failed["status"] == "unavailable"
    assert "energy_uj" not in failed
    assert "permission denied" in failed["readings"][2]["error"]


def test_idle_subtraction_keeps_negative_estimates_and_gross_measurement():
    measured = {
        "zones": [{"path": "package", "status": "available", "seconds": 2, "energy_uj": 1_000_000}]
    }
    idle = {
        "zones": [{"path": "package", "status": "available", "seconds": 1, "energy_uj": 1_000_000}]
    }
    zone = resources.subtract_baseline(measured, idle)["zones"][0]
    assert zone["energy_uj"] == 1_000_000
    assert zone["idle_adjusted"] == {
        "status": "available",
        "baseline_watts": 1,
        "energy_uj": -1_000_000,
    }
    idle["zones"][0] = {"path": "package", "status": "unavailable"}
    assert resources.subtract_baseline(measured, idle)["zones"][0]["idle_adjusted"] == {
        "status": "unavailable",
        "reason": "measurement or paired idle baseline unavailable",
    }


@pytest.mark.parametrize("system,expected", [("Linux", 4096), ("Darwin", 4), ("Windows", 8192)])
def test_peak_rss_is_os_high_water_mark_in_bytes(monkeypatch, system, expected):
    monkeypatch.setattr(resources.platform, "system", lambda: system)
    monkeypatch.setitem(
        sys.modules,
        "resource",
        SimpleNamespace(RUSAGE_SELF=0, getrusage=lambda who: SimpleNamespace(ru_maxrss=4)),
    )
    monkeypatch.setitem(
        sys.modules,
        "psutil",
        SimpleNamespace(
            Process=lambda: SimpleNamespace(
                memory_info=lambda: SimpleNamespace(peak_wset=8192, rss=1)
            )
        ),
    )
    result = resources.peak_rss()
    assert result["bytes"] == expected
    assert "no children" in result["scope"]


def test_missing_counters_do_not_sleep_or_invent_zero_energy(tmp_path, monkeypatch):
    def unexpected_sleep(seconds):
        pytest.fail("no idle wait needed without counters")

    monkeypatch.setattr(resources.time, "sleep", unexpected_sleep)
    measurement = resources.ResourceMeasurement(config(tmp_path))
    measurement.start()
    result = measurement.finish()
    assert result["energy"]["status"] == "unavailable"
    assert result["idle"]["status"] == "unavailable"
    assert result["energy"]["zones"] == []


def test_aggregates_require_complete_repeated_measurements():
    summary = resources.distribution([1, 2, 3, 100], 4, "bytes")
    assert summary["median"] == 2.5
    assert summary["iqr"] == 25.5
    assert summary["samples"] == [1, 2, 3, 100]
    assert resources.distribution([1], 2, "bytes")["status"] == "unavailable"
    assert "median" not in resources.distribution([1], 2, "bytes")
    assert resources.distribution([1], 1, "bytes")["status"] == "insufficient_samples"


def test_real_worker_collects_resources_with_unavailable_energy(tmp_path):
    fixture = bench.make_fixture(tmp_path / "fixture.csv", 100)
    result = bench.run_worker(
        {
            "tool": "pandas",
            "path": fixture["path"],
            "threads": 1,
            "warmups": 1,
            "iterations": 2,
            "expected": fixture["expected"],
            "resources": config(tmp_path),
        },
        60,
    )
    assert result["observed"] == fixture["expected"]
    assert len(result["operation_seconds"]) == 2
    assert result["resources"]["energy"]["status"] == "unavailable"
    assert result["resources"]["peak_rss"]["scope"] == resources.RSS_SCOPE
    json.dumps(result, allow_nan=False)


def test_timeout_stops_collector_and_retains_failure_evidence(tmp_path, monkeypatch):
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired("worker", 1)

    monkeypatch.setattr(bench.subprocess, "run", timeout)
    with pytest.raises(bench.WorkerError) as error:
        bench.run_worker({"tool": "pandas", "threads": 1, "resources": config(tmp_path)}, 1)
    assert error.value.diagnostics["resources"]["energy"]["status"] == "unavailable"


def test_readable_counter_runs_through_idle_polling_and_worker_collection(tmp_path):
    zone_files(tmp_path / "powercap/package", value="123", maximum="1000000000000")
    settings = {**config(tmp_path), "powercap_root": str(tmp_path / "powercap")}
    fixture = bench.make_fixture(tmp_path / "fixture.csv", 100)
    result = bench.run_worker(
        {
            "tool": "pandas",
            "path": fixture["path"],
            "threads": 1,
            "warmups": 0,
            "iterations": 1,
            "expected": fixture["expected"],
            "resources": settings,
        },
        60,
    )
    measured = result["resources"]
    assert measured["idle"]["zones"][0]["energy_uj"] == 0
    zone = measured["energy"]["zones"][0]
    assert zone["status"] == "available"
    assert zone["energy_uj"] == 0  # Synthetic constant counter: distinct from unavailable.
    assert len(zone["readings"]) > 2
    assert zone["readings"][0]["energy_uj"] == zone["readings"][-1]["energy_uj"] == 123
    runs = [
        {"tool": "pandas", "mode": mode, "resources": measured}
        for mode in ("cold", "warm")
        for _ in range(2)
    ]
    summary = resources.summarize_resources(runs, ["pandas"])
    energy = summary["pandas"]["cold"]["energy"][zone["path"]]
    assert energy["gross"]["median"] == 0
    assert energy["idle_adjusted"]["iqr"] == 0
    assert "0.000000 [0.000000]" in "\n".join(resources.render_summary(summary))


def test_resource_matrix_retains_scope_and_raw_evidence(tmp_path, monkeypatch):
    monkeypatch.setattr(bench, "environment_metadata", lambda: {})
    output = tmp_path / "run"
    assert (
        bench.main(
            [
                "--output",
                str(output),
                "--tools",
                "pandas",
                "--rows",
                "100",
                "--iterations",
                "2",
                "--warmups",
                "1",
                "--resources",
                "--powercap-root",
                str(tmp_path / "missing"),
            ]
        )
        == 0
    )
    document = json.loads((output / "results.json").read_text())
    assert len(document["runs"]) == 3
    assert all("resources" in r for r in document["runs"])
    for mode in ("cold", "warm"):
        assert document["resource_results"]["pandas"][mode]["energy"]["status"] == "unavailable"
    table = (output / "comparison.md").read_text()
    assert "block totals, not per-operation" in table
    assert "unavailable" in table
    assert document["environment"]["resource_host"]["kernel"]


@pytest.mark.parametrize(
    "flag,value",
    [("--idle-seconds", "nan"), ("--energy-poll-seconds", "0"), ("--max-zone-watts", "inf")],
)
def test_invalid_resource_settings_fail_before_measurement(tmp_path, flag, value):
    with pytest.raises(SystemExit):
        bench.main(["--output", str(tmp_path / "out"), flag, value])
    assert not (tmp_path / "out").exists()
