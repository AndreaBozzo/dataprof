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


def boundary_document():
    cell = {"median_seconds": 0.2, "iqr_seconds": 0.1, "samples_seconds": [0.1, 0.3]}
    stages = ("prepare", "import_profile", "export_dict", "export_json", "end_to_end")
    return {
        "schema_version": 1,
        "status": "complete",
        "config": {"iterations": 2},
        "cases": [
            {
                "id": "arrow_stream/100/10/offset-3",
                "status": "complete",
                "summary": {mode: dict.fromkeys(stages, cell) for mode in ("fresh", "warm")},
            },
            {
                "id": "arrow_array/100/10/offset-0",
                "status": "skipped",
                "skip_reason": "C Array requires one batch",
            },
        ],
    }


def test_boundary_artifact_is_published_with_explicit_scope(artifact, monkeypatch):
    (artifact / "boundaries").mkdir()
    (artifact / "boundaries/results.json").write_text(json.dumps(boundary_document()))
    monkeypatch.setattr(sys, "argv", ["build_benchmark_pages.py", str(artifact)])
    assert pages.main() == 0
    output = (artifact / "index.html").read_text(encoding="utf-8")
    assert 'id="boundaries"' in output
    assert "profiling share one timer" in output
    assert "C Array requires one batch" in output
    assert 'href="boundaries/results.json"' in output
    assert "boundaries" in json.loads((artifact / "benchmark-summary.json").read_text())


@pytest.mark.parametrize("invalid", ["status", "version", "samples", "nan", "skip"])
def test_invalid_present_boundary_results_are_rejected(artifact, invalid):
    document = boundary_document()
    if invalid == "status":
        document["status"] = "incomplete"
    elif invalid == "version":
        document["schema_version"] = 2
    elif invalid == "samples":
        document["cases"][0]["summary"]["warm"]["prepare"]["samples_seconds"] = [0.1]
    elif invalid == "nan":
        document["cases"][0]["summary"]["fresh"]["prepare"]["median_seconds"] = float("nan")
    else:
        del document["cases"][1]["skip_reason"]
    (artifact / "boundaries").mkdir()
    (artifact / "boundaries/results.json").write_text(json.dumps(document))
    with pytest.raises(ValueError):
        pages.load_boundaries(artifact)


def test_incomplete_boundary_checkpoint_is_not_an_absent_suite(artifact):
    assert pages.load_boundaries(artifact) is None
    (artifact / "boundaries").mkdir()
    (artifact / "boundaries/progress.json").write_text('{"status": "incomplete"}')
    with pytest.raises(ValueError, match="incomplete boundary"):
        pages.load_boundaries(artifact)


def test_resource_evidence_links_disclose_whole_worker_scope(artifact, monkeypatch):
    document = comparison_document()
    document["config"]["resources"] = {"protocol_version": 1}
    write_comparison(artifact, document)
    monkeypatch.setattr(sys, "argv", ["build_benchmark_pages.py", str(artifact)])
    assert pages.main() == 0
    index = (artifact / "index.html").read_text(encoding="utf-8")
    assert "Optional resource collection enabled" in index
    assert "cover the entire block, not a single operation" in index
    assert "resource median/IQR tables" in index
    assert "Unavailable counters remain unavailable" in index


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


@pytest.mark.parametrize("with_status", [False, True])
def test_combined_artifact_publishes_both_without_mixing_statistics(
    artifact, monkeypatch, with_status
):
    document = comparison_document()
    if with_status:
        document["status"] = "complete"
    write_comparison(artifact, document)
    if with_status:
        (artifact / "run-status.json").write_text(json.dumps({"status": "complete"}))
        (artifact / "comparison/progress.json").write_text(json.dumps({"status": "complete"}))
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


def test_process_blocks_and_ordered_boundaries_are_visible(artifact):
    document = comparison_document()
    document["config"].update({"blocks": 2, "mode": "publication", "host_description": "<lab>"})
    for mode in ("cold", "warm"):
        document["results"]["dataprof"][mode] = {
            **document["results"]["dataprof"][mode],
            "samples_seconds": [0.9, 1.1, 1.0, 1.0],
        }
    document["runs"] = [
        {
            "tool": "dataprof",
            "mode": "cold",
            "block": 1,
            "invocation": "first_fixture_operation_after_preflight",
            "process_seconds": 1.23,
            "import_setup_seconds": 0.12,
            "first_operation_seconds": 0.23,
            "operation_seconds": [0.23],
        }
    ]
    document["controls"] = [{"block": 1, "iteration": 1, "process_seconds": 0.045}]
    document["preflight"] = [{"tool": "dataprof", "import_setup_seconds": 0.34}]
    write_comparison(artifact, document)
    output = pages.render_comparison(pages.load_comparison(artifact))
    assert "4 samples per condition · 2 process blocks" in output
    assert "first_fixture_operation_after_preflight" in output
    for seconds in ("1.230000", "0.120000", "0.230000", "0.045000", "0.340000"):
        assert seconds in output
    assert "Diagnostic evidence — no established baseline" in output
    assert "not a significance test" in output
    assert "&lt;lab&gt;" in output
    assert "<lab>" not in output


def test_old_comparison_exposes_ordered_samples_without_inventing_boundaries():
    output = pages.render_comparison(comparison_document())
    assert "0.900000, 1.100000" in output
    assert "Not recorded" in output
    assert "Diagnostic evidence — no established baseline" in output


@pytest.mark.parametrize("invalid", ["schema", "sample_count", "nan", "incomplete", "missing_tool"])
def test_invalid_present_comparison_is_rejected(artifact, invalid):
    document = comparison_document()
    if invalid == "schema":
        document["schema_version"] = 999
    elif invalid == "sample_count":
        document["config"]["iterations"] = 3
    elif invalid == "incomplete":
        document["status"] = "incomplete"
    elif invalid == "missing_tool":
        document["config"]["workloads"]["pandas"] = "read_csv"
    else:
        document["results"]["dataprof"]["cold"]["median_seconds"] = float("nan")
    write_comparison(artifact, document)
    with pytest.raises(ValueError):
        pages.load_comparison(artifact)


@pytest.mark.parametrize("status_file", ["run-status.json", "comparison/progress.json"])
@pytest.mark.parametrize("has_results", [False, True])
def test_incomplete_artifact_cannot_publish_even_with_criterion(
    artifact, monkeypatch, status_file, has_results
):
    if has_results:
        write_comparison(artifact, comparison_document())
    path = artifact / status_file
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps({"status": "incomplete"}))
    monkeypatch.setattr(sys, "argv", ["build_benchmark_pages.py", str(artifact)])
    with pytest.raises(ValueError, match="incomplete benchmark artifact"):
        pages.main()
    assert not (artifact / "index.html").exists()
    assert not (artifact / "benchmark-summary.json").exists()


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
