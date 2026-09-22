"""Reports record how they measured, so comparisons across releases can tell (#675)."""

import json
from pathlib import Path

import dataprof
import pytest
from jsonschema import Draft202012Validator

FIXTURES = Path(__file__).parents[2] / "tests/fixtures"
CURRENT = {"text_length_unit": "unicode_scalar"}
# The same values the 0.11.0 fixture was profiled from, with the published wheel.
CITIES = {"city": ["東京", "Zürich", "Oslo", None]}


def _validator():
    schema = json.loads(
        (Path(__file__).parents[2] / "docs/schema/profile-report.v1.schema.json").read_text(
            encoding="utf-8"
        )
    )
    return Draft202012Validator(schema)


def _legacy_0_11():
    text = (FIXTURES / "legacy_0_11_non_ascii.summary.json").read_text(encoding="utf-8")
    return json.loads(text)


def test_a_new_report_records_its_semantics_in_both_documents():
    report = dataprof.profile(CITIES)
    assert report.metric_semantics == CURRENT
    summary = report.to_dict()
    canonical = json.loads(report.to_json())
    assert summary["metric_semantics"] == CURRENT
    assert canonical["metric_semantics"] == CURRENT
    for document in (summary, canonical):
        _validator().validate(document)
        assert dataprof.ProfileReport.from_dict(document).metric_semantics == CURRENT
    # A loaded flat summary exports through Python's own summary producer.
    assert dataprof.ProfileReport.from_dict(summary).to_dict() == summary


def test_a_0_11_report_reads_back_as_unknown_and_stays_unknown():
    document = _legacy_0_11()
    assert "metric_semantics" not in document
    restored = dataprof.ProfileReport.from_dict(document)
    assert restored.metric_semantics is None
    assert "metric_semantics" not in restored.to_dict()
    assert "metric_semantics" not in json.loads(restored.to_json())


def test_a_unit_change_is_not_reported_as_comparable():
    """The 0.11.0 wheel counted UTF-8 bytes; this build counts scalar values.

    Same data, different `max_length`. Only the comparison between two reports
    that both record their semantics may read that difference as data.
    """
    legacy = dataprof.ProfileReport.from_dict(_legacy_0_11())
    current = dataprof.profile(CITIES)
    assert legacy["city"].max_length == 7  # "Zürich" in UTF-8 bytes
    assert current["city"].max_length == 6  # ... in Unicode scalar values

    across = legacy.compare(current)["metric_semantics"]
    assert across == {"a": None, "b": CURRENT, "comparable": None}
    assert current.compare(legacy)["metric_semantics"]["comparable"] is None

    within = current.compare(dataprof.profile(CITIES))["metric_semantics"]
    assert within == {"a": CURRENT, "b": CURRENT, "comparable": True}

    # Recording the field without the text-length definition is still unknown.
    silent = current.to_dict()
    silent["metric_semantics"] = {}
    partial = dataprof.ProfileReport.from_dict(silent).compare(current)["metric_semantics"]
    assert partial == {"a": {}, "b": CURRENT, "comparable": None}


def test_the_canonical_fixture_without_semantics_resaves_byte_identical(tmp_path):
    fixture = FIXTURES / "canonical_report.json"
    report = dataprof.ProfileReport.load(fixture)
    assert report.metric_semantics is None
    report.save(tmp_path / "report.json")
    assert (tmp_path / "report.json").read_text(encoding="utf-8") == fixture.read_text(
        encoding="utf-8"
    ).rstrip()


def _through_both_readers(value):
    report = dataprof.profile(CITIES)
    summary = report.to_dict()
    summary["metric_semantics"] = value
    canonical = json.loads(report.to_json())
    canonical["metric_semantics"] = value
    return summary, canonical


@pytest.mark.parametrize(
    "value",
    [None, "unicode_scalar", {"text_length_unit": "utf8_byte"}, {"text_length_unit": 1}],
    ids=["null", "string", "unknown-unit", "non-string-unit"],
)
def test_malformed_semantics_are_rejected_by_both_readers(value):
    summary, canonical = _through_both_readers(value)
    with pytest.raises(ValueError, match="metric_semantics"):
        dataprof.ProfileReport.from_dict(summary)
    # Serde's messages name the type or variant, not the field path.
    with pytest.raises(ValueError, match="Invalid report document"):
        dataprof.ProfileReport.from_json(json.dumps(canonical))


@pytest.mark.parametrize(
    "value",
    [{}, {**CURRENT, "grapheme_policy": "extended"}],
    ids=["empty", "unknown-definition"],
)
def test_both_readers_agree_on_accepted_semantics(value):
    summary, canonical = _through_both_readers(value)
    flat = dataprof.ProfileReport.from_dict(summary).metric_semantics
    native = dataprof.ProfileReport.from_json(json.dumps(canonical)).metric_semantics
    assert flat == native
