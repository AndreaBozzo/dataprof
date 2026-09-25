"""The quality-gate contract, asserted against the shared fixture (#376).

``tests/fixtures/quality_gate_parity.json`` states one CSV, a set of profiling
options, a policy, and the exact result document for each case. This file
asserts the Python evaluator against it and ``tests/quality_gate_parity.rs``
asserts the Rust one against the same file, so changing either implementation
alone fails that layer's test.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import dataprof as dp
import pytest

FIXTURE = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "quality_gate_parity.json"


def _fixture() -> dict[str, Any]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _generated_csv(rows: int) -> str:
    """The fixture's second input, stated as a rule rather than as bytes.

    It has to exceed the quality reservoir (10,000 values per column) for the
    sampled-metric cases to mean anything, which is too much CSV to commit.
    Both layers build it from this same rule, so they profile the same file.
    """
    body = "".join(f"K-{n},{n % 7}\n" for n in range(rows))
    return f"key,bucket\n{body}"


def _profile(paths: dict[str, Path], options: dict[str, Any]):
    """Map the fixture's option names onto this layer's spelling of them."""
    kwargs: dict[str, Any] = {}
    if "metrics" in options:
        kwargs["metrics"] = options["metrics"]
    if "columns" in options:
        kwargs["columns"] = options["columns"]
    if "max_rows" in options:
        kwargs["stop_condition"] = dp.StopCondition.max_rows(options["max_rows"])
    return dp.profile_file(paths[options.get("input", "orders")], **kwargs)


@pytest.fixture(scope="module")
def cases() -> list[dict[str, Any]]:
    loaded = _fixture()["cases"]
    assert loaded, "fixture states no case"
    return loaded


@pytest.fixture(scope="module")
def csv_paths(tmp_path_factory) -> dict[str, Path]:
    document = _fixture()
    directory = tmp_path_factory.mktemp("gate")
    paths = {"orders": directory / "orders.csv", "generated": directory / "generated.csv"}
    paths["orders"].write_text(document["csv"], encoding="utf-8")
    paths["generated"].write_text(_generated_csv(document["generated_rows"]), encoding="utf-8")
    return paths


def test_python_gate_matches_the_shared_fixture(
    csv_paths: dict[str, Path], cases: list[dict[str, Any]]
):
    for case in cases:
        report = _profile(csv_paths, case["options"])
        result = report.check(**case["policy"])
        assert result.to_dict() == case["expected"], f"{case['name']}: {case['why']}"


def test_every_case_the_ticket_asks_for_is_covered(cases: list[dict[str, Any]]):
    """The fixture is the coverage claim, so state what it must contain.

    A parity fixture that quietly lost its capped-input or projected-columns
    case would still pass on both sides while guarding nothing.
    """
    verdicts = {case["expected"]["verdict"] for case in cases}
    assert verdicts == {"pass", "fail", "inconclusive"}

    coverage = {case["expected"]["evidence"]["coverage"] for case in cases}
    assert coverage == {"complete", "incomplete"}

    reasons = {
        check.get("reason")
        for case in cases
        for check in case["expected"]["checks"]
        if check["status"] == "not_evaluated"
    }
    assert reasons == {
        "quality_unavailable",
        "not_assessed",
        "column_not_profiled",
        "evidence_incomplete",
    }

    # A violation witnessed under incomplete coverage: the one case where a
    # partial scan still decides a full-source requirement.
    assert any(
        case["expected"]["verdict"] == "fail"
        and case["expected"]["evidence"]["coverage"] == "incomplete"
        and case["expected"]["scope"] == "full_source"
        for case in cases
    )

    # The other half of that rule: the same incomplete evidence is decidable
    # once the policy asks about what was measured rather than the source.
    assert any(
        case["expected"]["verdict"] == "pass"
        and case["expected"]["evidence"]["coverage"] == "incomplete"
        and case["expected"]["scope"] == "observed"
        for case in cases
    )

    # A metric sampled on a source that was read in full. Without this the two
    # implementations can disagree on sampled-dimension handling while every
    # other case still matches, because truncation reaches the same verdict by
    # a different route.
    assert any(
        check["evidence"].get("reason") == "quality_sampled"
        and case["expected"]["evidence"]["coverage"] == "complete"
        for case in cases
        for check in case["expected"]["checks"]
    )

    # A sampled score decided on its whole-source interval, and one whose
    # minimum falls inside it. Both layers read the same recorded bounds, so
    # without these they could disagree on which side of the interval decides.
    bounded = [
        check
        for case in cases
        for check in case["expected"]["checks"]
        if "bounds" in check and case["expected"]["scope"] == "full_source"
    ]
    assert {check["status"] for check in bounded} >= {"passed", "not_evaluated"}
