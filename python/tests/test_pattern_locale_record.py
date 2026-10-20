"""Reports record the pattern locale they were profiled with (#1010).

``tests/pattern_locale_record.rs`` is the Rust twin. Here the extra routes are
the Python ones: in-memory inputs, DataFrames, Arrow, and a flat ``to_dict()``
summary.
"""

import json
from pathlib import Path
from typing import Any

import dataprof as dp
import pytest

EMAILS = {"email": ["a@example.com", "b@example.com", "c@example.com"]}


def _routes(tmp_path: Path) -> dict[str, Any]:
    csv = tmp_path / "emails.csv"
    csv.write_text("email\n" + "\n".join(EMAILS["email"]) + "\n")
    rows = [{"email": value} for value in EMAILS["email"]]
    jsonl = tmp_path / "emails.jsonl"
    jsonl.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    routes: dict[str, Any] = {
        "dict": EMAILS,
        "rows": rows,
        "csv auto": (str(csv), "auto"),
        "csv incremental": (str(csv), "incremental"),
        "csv columnar": (str(csv), "columnar"),
        "jsonl": str(jsonl),
    }
    pa = pytest.importorskip("pyarrow")
    routes["arrow"] = pa.table(EMAILS)
    import pyarrow.parquet as pq

    parquet = tmp_path / "emails.parquet"
    pq.write_table(pa.table(EMAILS), parquet)
    routes["parquet"] = str(parquet)
    pd = pytest.importorskip("pandas")
    routes["pandas"] = pd.DataFrame(EMAILS)
    return routes


def _profile(source: Any, **options: Any) -> dp.ProfileReport:
    if isinstance(source, tuple):
        path, engine = source
        return dp.profile(path, engine=engine, **options)
    return dp.profile(source, **options)


def test_every_input_path_records_the_locale(tmp_path: Path):
    wrong = []
    for name, source in _routes(tmp_path).items():
        for options, expected in (({}, "none"), ({"locale": "it"}, "IT")):
            settings = _profile(source, **options).analysis_settings
            if settings != {"pattern_locale": expected}:
                wrong.append((name, options, settings))
    assert wrong == []


def test_a_document_keeps_the_settings_and_their_absence(tmp_path: Path):
    report = dp.profile(EMAILS, locale="it")
    assert report.analysis_settings == {"pattern_locale": "IT"}

    saved = tmp_path / "report.json"
    report.save(saved)
    assert dp.ProfileReport.load(saved).analysis_settings == {"pattern_locale": "IT"}

    summary = report.to_dict()
    assert summary["analysis_settings"] == {"pattern_locale": "IT"}
    assert dp.ProfileReport.from_dict(summary).analysis_settings == {"pattern_locale": "IT"}

    # Written before the field: unknown, not the defaults.
    del summary["analysis_settings"]
    legacy = dp.ProfileReport.from_dict(summary)
    assert legacy.analysis_settings is None
    assert "analysis_settings" not in legacy.to_dict()

    document = json.loads(report.to_json())
    del document["analysis_settings"]
    assert dp.ProfileReport.from_json(json.dumps(document)).analysis_settings is None

    for malformed in (None, {}, {"pattern_locale": None}, {"pattern_locale": "it"}):
        broken = report.to_dict()
        broken["analysis_settings"] = malformed
        with pytest.raises(ValueError):
            dp.ProfileReport.from_dict(broken)
        canonical = json.loads(report.to_json())
        canonical["analysis_settings"] = malformed
        with pytest.raises(ValueError):
            dp.ProfileReport.from_json(json.dumps(canonical))

    # A setting a later release adds is ignored.
    later = report.to_dict()
    later["analysis_settings"]["metric_packs"] = ["schema"]
    assert dp.ProfileReport.from_dict(later).analysis_settings == {"pattern_locale": "IT"}


def test_compare_and_check_read_the_locale():
    unset = dp.profile(EMAILS)
    italian = dp.profile(EMAILS, locale="it")
    summary = unset.to_dict()
    del summary["analysis_settings"]
    legacy = dp.ProfileReport.from_dict(summary)

    def same(a: dp.ProfileReport, b: dp.ProfileReport) -> bool | None:
        return a.compare(b)["configuration"]["pattern_locale"]["same"]

    assert same(unset, dp.profile(EMAILS)) is True
    assert same(unset, italian) is False
    assert same(legacy, unset) is None
    assert unset.compare(italian)["configuration"]["pattern_locale"] == {
        "a": "none",
        "b": "IT",
        "same": False,
    }
    flat = dp.ProfileReport.from_dict(italian.to_dict())
    assert same(flat, italian) is True

    def validity(report: dp.ProfileReport, baseline: dp.ProfileReport) -> dp.QualityCheck:
        result = report.check(baseline=baseline, max_dimension_score_drops={"validity": 100})
        return result.checks[0]

    assert validity(dp.profile(EMAILS), unset).status == "passed"
    assert validity(italian, unset).reason == {
        "reason": "setting_changed",
        "setting": "pattern_locale",
    }
    assert validity(unset, legacy).reason == {
        "reason": "setting_unrecorded",
        "setting": "pattern_locale",
    }
