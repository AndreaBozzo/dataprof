"""A flat summary's ``quality.sampled_dimensions`` is read only as a list of strings (#1235).

The loader used to take any value. A string was then read as a list of its
characters, no character matched a component label, and every sampled
component read as exact: ``compare()`` and ``check(baseline=)`` reported
``complete`` coverage for a dimension the document says was sampled. The
field is now read as ``exact_dimensions`` is (#1234): absent or null is
unrecorded, a list of strings is read, anything else raises ``ValueError``
naming the field. ``tests/summary_sampled_dimensions.rs`` is the Rust twin.
"""

from __future__ import annotations

import dataprof as dp
import pytest

DATED = {"d": ["2026-01-01"] * 4, "v": [1, 2, 3, 4]}
SAMPLED = {"coverage": "incomplete", "reason": "quality_sampled"}


def _summary() -> dict:
    return dp.profile(DATED).to_dict()


@pytest.mark.parametrize(
    "value",
    ["timeliness", "", ["timeliness", 1], {"timeliness": True}, 3],
    ids=["string", "empty-string", "non-string-item", "mapping", "number"],
)
def test_a_malformed_sampled_list_is_refused(value):
    summary = _summary()
    summary["quality"]["sampled_dimensions"] = value
    with pytest.raises(ValueError, match="quality.sampled_dimensions"):
        dp.ProfileReport.from_dict(summary)


def test_a_recorded_list_reads_as_sampled_coverage():
    summary = _summary()
    summary["quality"]["sampled_dimensions"] = ["timeliness"]
    report = dp.ProfileReport.from_dict(summary)
    assert report.quality_sampled_dimensions == ["timeliness"]
    assert report.compare(report)["dimensions"]["timeliness"]["evidence"]["a"] == SAMPLED


@pytest.mark.parametrize("absent", ["missing", "null"])
def test_an_unrecorded_list_stays_unrecorded(absent):
    summary = _summary()
    if absent == "missing":
        del summary["quality"]["sampled_dimensions"]
    else:
        summary["quality"]["sampled_dimensions"] = None
    assert dp.ProfileReport.from_dict(summary).quality_sampled_dimensions is None
