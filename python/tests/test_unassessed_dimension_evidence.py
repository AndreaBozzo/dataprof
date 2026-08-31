"""An unassessed dimension reports no evidence, on every accessor.

`assessed_dimensions()` is the authority on what was computed, and the
interpretation guide tells readers not to report a value for a dimension that is
missing from it. The per-dimension evidence accessors used to ignore that and
hand back a populated dict of ratios derived from zero inputs: a header-only file
reported 100% on five dimensions, and any ordinary file with no pattern-bearing
column reported `valid_values_ratio: 100.0` from `values_checked: 0` (#622).

Reporting 100% from zero checks is the same error as reporting "0% valid" for an
unpatterned column, with the opposite sign. Both invent a number where the honest
answer is "not analyzed", which is what `None` means here.
"""

from __future__ import annotations

import json

import dataprof
import pytest

HEADER_ONLY = "id,amount\n"
# Numeric only, so validity (no confident pattern) and timeliness (no dates)
# have nothing to assess while the rest of the dimensions do.
NUMERIC_ROWS = "id,amount\n1,3.5\n2,4.5\n3,5.5\n"

DIMENSIONS = (
    "completeness",
    "consistency",
    "uniqueness",
    "accuracy",
    "timeliness",
    "validity",
    "precision",
)

# The counter each dimension divides by. A dimension that reports evidence must
# have looked at something.
DENOMINATORS = {
    "completeness": ("total_cells",),
    "consistency": ("values_checked",),
    "uniqueness": ("rows_checked",),
    "accuracy": ("numeric_values_checked",),
    "timeliness": ("date_values_checked",),
    "validity": ("values_checked",),
    "precision": ("numeric_values_checked",),
}


def write(tmp_path, name: str, contents: str):
    path = tmp_path / name
    path.write_text(contents, encoding="utf-8", newline="")
    return path


@pytest.fixture
def unassessable(tmp_path):
    return dataprof.profile(write(tmp_path, "empty.csv", HEADER_ONLY))


@pytest.fixture
def partly_assessed(tmp_path):
    return dataprof.profile(write(tmp_path, "numeric.csv", NUMERIC_ROWS))


def test_an_unassessable_report_has_no_evidence_at_all(unassessable) -> None:
    quality = unassessable.quality
    assert quality is not None
    assert quality.assessed_dimensions() == []

    for dimension in DIMENSIONS:
        assert getattr(quality, dimension) is None, (
            f"{dimension} reported evidence on a file where nothing was assessed"
        )


def test_only_the_unassessed_dimensions_are_withheld(partly_assessed) -> None:
    quality = partly_assessed.quality
    assert quality is not None

    assessed = set(quality.assessed_dimensions())
    assert assessed, "fixture must assess something"
    assert {"validity", "timeliness"}.isdisjoint(assessed), (
        "fixture must leave validity and timeliness unassessed"
    )

    for dimension in DIMENSIONS:
        evidence = getattr(quality, dimension)
        if dimension in assessed:
            assert isinstance(evidence, dict), f"{dimension} was assessed but reports nothing"
        else:
            assert evidence is None, f"{dimension} reports evidence it did not gather"


@pytest.mark.parametrize("contents", [HEADER_ONLY, NUMERIC_ROWS])
def test_evidence_and_dimension_scores_agree(tmp_path, contents: str) -> None:
    # Two accessors over the same underlying counters. They used to disagree:
    # a None score beside a populated evidence dict.
    report = dataprof.profile(write(tmp_path, "agree.csv", contents))
    quality = report.quality
    assert quality is not None

    scores = quality.dimension_scores()
    for dimension in DIMENSIONS:
        has_evidence = getattr(quality, dimension) is not None
        assert has_evidence == (scores[dimension] is not None), (
            f"{dimension}: evidence and score disagree about whether it was assessed"
        )


@pytest.mark.parametrize("contents", [HEADER_ONLY, NUMERIC_ROWS])
def test_no_reported_ratio_comes_from_zero_inputs(tmp_path, contents: str) -> None:
    # The acceptance criterion stated directly: every dimension that reports
    # anything looked at something first.
    report = dataprof.profile(write(tmp_path, "denominators.csv", contents))
    quality = report.quality
    assert quality is not None

    for dimension in DIMENSIONS:
        evidence = getattr(quality, dimension)
        if evidence is None:
            continue
        counters = [evidence[name] for name in DENOMINATORS[dimension]]
        if dimension == "uniqueness":
            # Uniqueness averages whichever of its two components has data, so
            # a named key column is a denominator of its own.
            counters.append(1 if evidence.get("key_column") is not None else 0)
        assert any(counter > 0 for counter in counters), (
            f"{dimension} reported evidence with every denominator at zero: {evidence}"
        )


def test_the_serialized_report_omits_unassessed_dimensions(partly_assessed) -> None:
    quality = partly_assessed.to_dict()["quality"]
    assessed = set(quality["assessed_dimensions"])

    for dimension in DIMENSIONS:
        assert (dimension in quality) == (dimension in assessed), (
            f"{dimension} disagrees between the evidence keys and assessed_dimensions"
        )

    from_json = json.loads(partly_assessed.to_json())["quality"]
    assert set(from_json) & set(DIMENSIONS) == assessed


def test_an_unassessable_report_serializes_no_dimension_keys(unassessable) -> None:
    quality = unassessable.to_dict()["quality"]

    # The aggregate keys stay, carrying null rather than a fabricated zero.
    assert quality["overall_score"] is None
    assert quality["assessed_dimensions"] == []
    assert set(quality) & set(DIMENSIONS) == set()


def test_a_reloaded_report_reads_back_the_same_absence(partly_assessed) -> None:
    reloaded = dataprof.ProfileReport.from_dict(partly_assessed.to_dict())
    quality = reloaded.quality
    assert quality is not None

    assert quality.assessed_dimensions() == partly_assessed.quality.assessed_dimensions()
    for dimension in DIMENSIONS:
        original = getattr(partly_assessed.quality, dimension)
        assert (getattr(quality, dimension) is None) == (original is None), (
            f"{dimension} changed its assessed state across a round trip"
        )


def test_the_agent_facing_exports_are_unchanged(unassessable, partly_assessed) -> None:
    # These were already honest, and the fix must not disturb them.
    assert unassessable.quality_summary()["quality_score"] is None
    assert "quality: n/a" in unassessable.to_llm_context()
    assert partly_assessed.quality_summary()["quality_score"] == pytest.approx(100.0)
