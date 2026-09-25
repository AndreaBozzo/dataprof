"""One public report surface, with native precision and saved absence intact."""

from __future__ import annotations

from typing import Any

import dataprof
import pytest
from dataprof import _dataprof as native, interop


def _public_values(value: Any) -> Any:
    """Snapshot every public member, including nested native pattern objects."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {name: _public_values(item) for name, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_public_values(item) for item in value]
    values = {}
    for name in dir(value):
        if not name.startswith("_"):
            member = getattr(value, name)
            values[name] = _public_values(member() if callable(member) else member)
    return values


@pytest.fixture
def raw_report(tmp_path):
    path = tmp_path / "all-types.csv"
    path.write_text(
        "number,label,active,email,date\n"
        "0,hello,true,first@example.com,2026-01-01\n"
        "0,world,false,second@example.com,2026-01-02\n"
        "1,,true,third@example.com,2026-01-03\n",
        encoding="utf-8",
    )
    return native.analyze_file(str(path), None)


def test_public_views_preserve_every_native_field_exactly(raw_report):
    """New native fields cannot silently disappear behind the shared views."""
    report = dataprof.ProfileReport(raw_report)
    assert _public_values(report.profiles) == _public_values(raw_report.column_profiles)
    assert _public_values(report.quality) == _public_values(raw_report.quality)
    assert report["number"].mean == raw_report.column_profiles[0].mean == 1 / 3
    assert report["number"].mean != report.to_dict()["columns"][0]["stats"]["mean"]
    assert repr(report.quality) == repr(raw_report.quality)
    assert str(report.quality) == str(raw_report.quality)
    for view, raw in zip(report.profiles, raw_report.column_profiles, strict=True):
        assert repr(view) == repr(raw)


@pytest.mark.parametrize("via", ["dict", "json", "file"])
def test_both_backings_expose_the_same_public_classes(raw_report, tmp_path, via):
    report = dataprof.ProfileReport(raw_report)
    if via == "dict":
        restored = dataprof.ProfileReport.from_dict(report.to_dict())
    elif via == "json":
        restored = dataprof.ProfileReport.from_json(report.to_json())
    else:
        path = tmp_path / "report.json"
        report.save(path)
        restored = dataprof.ProfileReport.load(path)
    assert type(report.quality) is type(restored.quality) is dataprof.DataQualityMetrics
    for live, saved in zip(report.profiles, restored.profiles, strict=True):
        assert type(live) is type(saved) is dataprof.ColumnProfile
        for live_pattern, saved_pattern in zip(
            live.patterns or [], saved.patterns or [], strict=True
        ):
            assert type(live_pattern) is type(saved_pattern)
    if via != "dict":
        assert restored.to_json() == report.to_json()
    else:
        assert restored.to_dict() == report.to_dict()


@pytest.mark.parametrize("restore", [False, True])
def test_views_are_read_only_for_both_backings(raw_report, restore):
    report = dataprof.ProfileReport(raw_report)
    if restore:
        report = dataprof.ProfileReport.from_dict(report.to_dict())
    before = report.to_dict()
    patterns = report["email"].patterns
    assert patterns
    targets = [
        (report["number"], "mean", 0.0),
        (report.quality, "completeness", {}),
        (patterns[0], "match_count", 0),
    ]
    for target, name, value in targets:
        with pytest.raises(AttributeError):
            setattr(target, name, value)
    assert report.to_dict() == before


def test_removed_quality_names_use_the_native_definition(raw_report):
    report = dataprof.ProfileReport(raw_report)
    restored = dataprof.ProfileReport.from_dict(report.to_dict())
    # Native, freshly profiled public, and restored public surfaces all reject
    # the removed names with the same centrally owned migration guidance.
    quality = raw_report.quality
    for name in ("duplicate_rows", "outlier_ratio", "future_dates_count", "unknown_metric"):
        messages = []
        for view in (quality, report.quality, restored.quality):
            assert name not in dir(view)
            with pytest.raises(AttributeError) as exc:
                getattr(view, name)
            messages.append(str(exc.value))
        assert len(set(messages)) == 1


def test_missing_stats_stay_absent_and_untrusted_keys_cannot_replace_accessors(raw_report):
    document = dataprof.ProfileReport(raw_report).to_dict()
    column = document["columns"][0]
    column["stats"] = {"mean": 0.0, "name": "wrong", "__class__": "wrong", "_accessor": "wrong"}
    column["extra_metric"] = 123
    restored = dataprof.ProfileReport.from_dict(document)["number"]
    assert restored.mean == 0.0
    assert restored.min is None
    assert restored.variance is None
    assert restored.name == "number"
    assert type(restored) is dataprof.ColumnProfile
    assert not hasattr(restored, "extra_metric")


def test_legacy_quality_defaults_do_not_invent_an_assessment(raw_report):
    document = dataprof.ProfileReport(raw_report).to_dict()
    quality = document["quality"]
    for name in ("overall_score", "dimension_scores", "assessed_dimensions", "sampled_dimensions"):
        quality.pop(name, None)
    report = dataprof.ProfileReport.from_dict(document)
    assert report.quality is not None
    assert report.quality_score is None
    assert report.quality.overall_quality_score() is None
    assert report.quality.assessed_dimensions() == []
    assert set(report.quality.dimension_scores().values()) == {None}
    assert report.quality_sampled_dimensions is None


@pytest.mark.parametrize("restore", [False, True])
def test_mutating_returned_collections_does_not_change_report(raw_report, restore):
    report = dataprof.ProfileReport(raw_report)
    if restore:
        report = dataprof.ProfileReport.from_dict(report.to_dict())
    before = report.to_dict()
    quality = report.quality
    assert quality is not None
    quality.dimension_scores().clear()
    quality.score_weights.clear()
    quality.assessed_dimensions().clear()
    for value in (
        quality.completeness,
        report["number"].quartiles,
        report["number"].type_homogeneity,
    ):
        if value is not None:
            value.clear()
    assert report.to_dict() == before


def test_restored_report_owns_nested_input_collections(raw_report):
    document = dataprof.ProfileReport(raw_report).to_dict()
    document["columns"][0]["stats"]["quartiles"] = {"q1": 0.0, "q2": 0.0, "q3": 0.5}
    report = dataprof.ProfileReport.from_dict(document)
    before = report.to_dict()
    quartiles = report["number"].quartiles
    assert quartiles is not None
    quartiles.clear()
    assert report.to_dict() == before

    def corrupt_containers(value):
        if isinstance(value, dict):
            for child in list(value.values()):
                corrupt_containers(child)
            value.clear()
        elif isinstance(value, list):
            for child in value:
                corrupt_containers(child)
            value.clear()

    corrupt_containers(document)
    assert report.to_dict() == before


def test_interop_column_to_dict_accepts_native_column(tmp_path):
    path = tmp_path / "native.csv"
    path.write_text("number\n1\n2\n", encoding="utf-8")
    raw = interop.analyze_file(path)
    column = raw.column_profiles[0]
    assert isinstance(column, interop.ColumnProfile)
    assert interop.column_to_dict(column) == dataprof.ProfileReport(raw).to_dict()["columns"][0]
