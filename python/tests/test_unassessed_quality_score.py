"""An unassessable report has no overall score, on every accessor.

`None` means "not analyzed"; empty means "analyzed, nothing found". A report
where every dimension had a zero denominator — a header-only file, a dataset
with no numeric, temporal or pattern-bearing column — assessed nothing, and its
aggregate has to say so. Averaging the empty set to `0.0` reads as "this data is
terrible", which is the opposite of the truth and the worst possible answer to
give a quality gate.

Before this was fixed the two aggregates on the same report disagreed:
`report.quality_score` returned `None` while `overall_quality_score()` and the
serialized `overall_score` both returned `0.0` (#571).
"""

from __future__ import annotations

import json

import dataprof
import pytest

HEADER_ONLY = "name,age\n"
ONE_ROW = "name,age\na,1\n"


def write(tmp_path, name: str, contents: str):
    path = tmp_path / name
    path.write_text(contents, encoding="utf-8", newline="")
    return path


@pytest.fixture
def unassessable(tmp_path):
    return dataprof.profile(write(tmp_path, "empty.csv", HEADER_ONLY))


@pytest.mark.parametrize("engine", ["auto", "incremental", "columnar"])
def test_every_engine_reports_no_score_for_an_unassessable_file(tmp_path, engine: str) -> None:
    report = dataprof.profile(write(tmp_path, f"empty_{engine}.csv", HEADER_ONLY), engine=engine)

    assert report.rows == 0
    assert report.quality is not None
    assert report.quality.assessed_dimensions() == []
    assert report.quality.overall_quality_score() is None
    assert report.quality_score is None


def test_the_two_report_aggregates_agree(unassessable) -> None:
    # These are computed independently, and used to contradict each other.
    assert unassessable.quality_score == unassessable.quality.overall_quality_score()


def test_dimension_scores_are_all_none(unassessable) -> None:
    assert set(unassessable.quality.dimension_scores().values()) == {None}


def test_serialized_report_carries_no_score(unassessable) -> None:
    quality = unassessable.to_dict()["quality"]

    # The key stays present so a reader never has to distinguish "absent" from
    # "no quality assessment at all"; its value is null, not zero.
    assert "overall_score" in quality
    assert quality["overall_score"] is None
    assert quality["assessed_dimensions"] == []
    assert json.loads(unassessable.to_json())["quality"]["overall_score"] is None


def test_a_reloaded_report_reads_back_the_same_absence(unassessable) -> None:
    reloaded = dataprof.ProfileReport.from_dict(unassessable.to_dict())
    quality = reloaded.quality
    assert quality is not None

    assert reloaded.quality_score is None
    assert quality.overall_quality_score() is None
    assert quality.assessed_dimensions() == []


def test_agent_facing_exports_say_not_assessed(unassessable) -> None:
    assert unassessable.quality_summary()["quality_score"] is None
    assert "quality: n/a" in unassessable.to_llm_context()
    # The repr must not list dimensions beside a score that does not exist: the
    # metric structs are all present (with zero denominators) on this input, so
    # deriving the list from them claimed seven assessed dimensions and no
    # score in the same breath.
    assert repr(unassessable.quality) == (
        "DataQualityMetrics(score=n/a, assessed=[], low_sample_warning=true)"
    )
    assert str(unassessable.quality) == "DataQualityMetrics(not assessed)"


def test_repr_lists_only_the_dimensions_behind_the_score(tmp_path) -> None:
    # Not confined to the unassessable case: an ordinary two-column file assesses
    # four of seven dimensions, and the repr used to name all seven regardless
    # because a metric struct exists whenever its dimension was requested.
    report = dataprof.profile(write(tmp_path, "rows.csv", ONE_ROW))
    quality = report.quality
    assert quality is not None

    assessed = quality.assessed_dimensions()
    assert 0 < len(assessed) < 7, "fixture must assess some but not all dimensions"
    assert f"assessed=[{', '.join(assessed)}]" in repr(quality)


def test_one_assessable_row_still_scores(tmp_path) -> None:
    # The guard is about the empty set, not about small inputs: as soon as one
    # dimension has a denominator the aggregate is a number again.
    report = dataprof.profile(write(tmp_path, "one.csv", ONE_ROW))
    quality = report.quality
    assert quality is not None

    assert quality.assessed_dimensions() != []
    assert quality.overall_quality_score() == pytest.approx(100.0)
    assert report.quality_score == pytest.approx(100.0)
    assert report.to_dict()["quality"]["overall_score"] == pytest.approx(100.0)
