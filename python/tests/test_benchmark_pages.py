"""The common artifact publishes both suites without breaking Criterion links."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "build_benchmark_pages", ROOT / ".github/scripts/build_benchmark_pages.py"
)
assert SPEC is not None and SPEC.loader is not None
pages = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pages)


@pytest.fixture
def artifact(tmp_path):
    directory = tmp_path / "csv_parsing" / "parse" / "small"
    (directory / "new").mkdir(parents=True)
    (directory / "new/benchmark.json").write_text(
        json.dumps(
            {
                "group_id": "csv_parsing",
                "title": "csv_parsing/parse/small",
                "function_id": "parse",
                "value_str": "small",
                "directory_name": "csv_parsing/parse/small",
                "throughput": {"Bytes": 1024},
            }
        )
    )
    (directory / "new/estimates.json").write_text(
        json.dumps({"mean": {"point_estimate": 1_000_000}})
    )
    (directory / "report").mkdir()
    (directory / "report/index.html").write_text("original Criterion report")
    return tmp_path


def comparison_document():
    cell = {
        "median_seconds": 1.0,
        "iqr_seconds": 0.2,
        "samples_seconds": [0.9, 1.1],
        "q1_seconds": 0.9,
        "q3_seconds": 1.1,
    }
    return {
        "schema_version": 1,
        "created_at": "2026-09-17T12:00:00+00:00",
        "config": {
            "iterations": 2,
            "warmups": 1,
            "threads": 1,
            "cold_cache": "warm",
            "workloads": {"dataprof": 'profile(metrics=["schema", "statistics"])'},
        },
        "environment": {
            "os": "Windows",
            "cpu": "example CPU",
            "git_commit": "abc123",
            "git_status": "",
            "versions": {"dataprof": "0.11.0"},
        },
        "fixture": {"sha256": "a" * 64, "expected": {"rows": 100}},
        "results": {"dataprof": {"cold": cell, "warm": cell}},
    }


def write_comparison(artifact, document):
    (artifact / "comparison").mkdir(exist_ok=True)
    (artifact / "comparison/results.json").write_text(json.dumps(document), encoding="utf-8")
    (artifact / "comparison/comparison.md").write_text("comparison table")


def test_old_artifacts_keep_their_links(artifact, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["build_benchmark_pages.py", str(artifact)])
    assert pages.main() == 0
    index = (artifact / "index.html").read_text(encoding="utf-8")
    assert 'href="csv_parsing/report/index.html"' in index
    assert 'href="csv_parsing/parse/small/report/index.html"' in index
    assert 'id="comparison"' not in index
    assert "01 / Rust profiling" in index
    assert "02 / Rust profiling" not in index
    assert (
        artifact / "csv_parsing/parse/small/report/index.html"
    ).read_text() == "original Criterion report"
    summary = json.loads((artifact / "benchmark-summary.json").read_text())
    assert summary["benchmark_count"] == 1
    assert "comparison" not in summary


def test_combined_artifact_publishes_both_without_mixing_statistics(artifact, monkeypatch):
    document = comparison_document()
    write_comparison(artifact, document)
    monkeypatch.setattr(sys, "argv", ["build_benchmark_pages.py", str(artifact)])
    assert pages.main() == 0
    index = (artifact / "index.html").read_text(encoding="utf-8")
    assert 'href="csv_parsing/parse/small/report/index.html"' in index
    assert 'id="comparison"' in index
    assert "01 / Python tool comparison" in index
    assert "02 / Rust profiling" in index
    assert 'href="comparison/results.json"' in index
    assert "Cold median [IQR]" in index
    assert "metric equivalence is not established" in index
    assert "1.000000 [0.200000]" in index
    summary = json.loads((artifact / "benchmark-summary.json").read_text())
    assert summary["benchmark_count"] == 1
    assert summary["comparison"] == document


def test_comparison_html_escapes_metadata(artifact):
    document = comparison_document()
    document["environment"]["cpu"] = '<script>alert("cpu")</script>'
    document["config"]["workloads"]["dataprof"] = "<img src=x onerror=alert(1)>"
    write_comparison(artifact, document)
    output = pages.render_comparison(pages.load_comparison(artifact))
    assert "<script>" not in output
    assert "<img src=x" not in output
    assert "&lt;script&gt;" in output


@pytest.mark.parametrize("invalid", ["schema", "sample_count", "nan"])
def test_invalid_present_comparison_is_rejected(artifact, invalid):
    document = comparison_document()
    if invalid == "schema":
        document["schema_version"] = 999
    elif invalid == "sample_count":
        document["config"]["iterations"] = 3
    else:
        document["results"]["dataprof"]["cold"]["median_seconds"] = float("nan")
    write_comparison(artifact, document)
    with pytest.raises(ValueError):
        pages.load_comparison(artifact)


def test_placeholder_still_works_without_an_artifact(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["build_benchmark_pages.py", str(tmp_path), "--placeholder"])
    assert pages.main() == 0
    assert "No benchmark data" in (tmp_path / "index.html").read_text(encoding="utf-8")


def test_criterion_preserves_intervals_and_row_throughput(artifact):
    metadata_path = artifact / "csv_parsing/parse/small/new/benchmark.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["throughput"] = {"Elements": 1000}
    metadata_path.write_text(json.dumps(metadata))
    (metadata_path.with_name("estimates.json")).write_text(
        json.dumps(
            {
                "mean": {
                    "point_estimate": 1_000_000,
                    "confidence_interval": {
                        "confidence_level": 0.95,
                        "lower_bound": 900_000,
                        "upper_bound": 1_100_000,
                    },
                }
            }
        )
    )
    benchmarks = pages.collect_benchmarks(artifact)
    assert benchmarks[0]["throughput_rows_s"] == 1_000_000
    assert benchmarks[0]["throughput_mib_s"] is None
    assert benchmarks[0]["mean_confidence_interval"] == {
        "level": 0.95,
        "lower_ms": 0.9,
        "upper_ms": 1.1,
    }
    output = pages.render_index(artifact, benchmarks, pages.build_groups(artifact, benchmarks), [])
    assert "1,000,000 rows/s" in output
    assert "95%: 0.90 ms–1.10 ms" in output
