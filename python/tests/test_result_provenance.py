"""Build metadata on compare and gate documents; Rust twin: result_provenance.rs."""

import json
from pathlib import Path

import dataprof as dp
import pytest
from dataprof._dataprof import report_delta_json

CASES = json.loads(
    (Path(__file__).parents[2] / "tests/fixtures/result_provenance.json").read_text()
)


def _with_generator(report, generator):
    document = json.loads(report.to_json())
    if generator is None:
        document.pop("generator", None)
    else:
        document["generator"] = generator
    return dp.ProfileReport.from_json(json.dumps(document))


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_compare_builds_on_native_canonical_and_flat_routes(case):
    native = dp.profile({"id": [1, 2], "value": [10, 20]})
    a = _with_generator(native, case["a"])
    b = _with_generator(native, case["b"])
    original = native.compare(native)
    for a in (a, dp.ProfileReport.from_dict(a.to_dict())):
        for b_side in (b, dp.ProfileReport.from_dict(b.to_dict())):
            delta = a.compare(b_side)
            assert delta["same_build"] == case["same_build"]
            for side in ("a", "b"):
                assert delta["reports"][side]["generator"] == case[side]
            for key in (
                "quality_score",
                "dimensions",
                "columns",
                "schema",
                "metric_semantics",
                "configuration",
            ):
                assert delta[key] == original[key]


def test_native_results_carry_the_report_generator():
    report = dp.profile({"id": [1, 2]})
    delta = report.compare(report)
    assert delta["reports"]["a"]["generator"] == report.generator
    assert delta["reports"]["b"]["generator"] == report.generator
    assert report.check(min_rows=1).generator == report.generator


@pytest.mark.parametrize(
    "generator", [{"version": "historical", "commit": "old", "dirty": False}, None]
)
def test_gate_names_evaluated_report_build_with_native_and_flat_baselines(generator):
    native = dp.profile({"id": [1, 2]})
    report = _with_generator(native, generator)
    baseline = _with_generator(native, {"version": "baseline"})
    original = native.check(min_rows=1).to_dict()
    for baseline in (None, baseline, dp.ProfileReport.from_dict(baseline.to_dict())):
        result = report.check(min_rows=1, baseline=baseline)
        assert result.generator == generator
        document = result.to_dict()
        assert document["generator"] == generator
        assert json.loads(result.to_json())["generator"] == generator
        for key in ("verdict", "scope", "evidence", "checks"):
            assert document[key] == original[key]
        if baseline is not None:
            assert document["baseline"]["reports"]["a"]["generator"] == {"version": "baseline"}
            assert document["baseline"]["reports"]["b"]["generator"] == generator


@pytest.mark.parametrize(
    "generator",
    [None, "v", {}, {"version": 1}, {"version": "v", "commit": None}, {"version": "v", "dirty": 1}],
)
def test_malformed_generator_rejected_by_python_and_rust_summary_readers(generator):
    report = dp.profile({"id": [1, 2]})
    summary = report.to_dict()
    summary["generator"] = generator
    with pytest.raises(ValueError):
        dp.ProfileReport.from_dict(summary)
    with pytest.raises(ValueError):
        report_delta_json(json.dumps(summary), json.dumps(summary))
    canonical = json.loads(report.to_json())
    canonical["generator"] = generator
    with pytest.raises(ValueError):
        dp.ProfileReport.from_json(json.dumps(canonical))
