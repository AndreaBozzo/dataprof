"""Recovery is execution provenance and survives report persistence (#716)."""

import json

import dataprof
import pytest
from conftest import REPO_ROOT
from jsonschema import Draft202012Validator


@pytest.mark.parametrize("engine", ["auto", "incremental", "columnar"])
def test_successful_first_attempt_records_empty_history(tmp_path, engine):
    source = tmp_path / "clean.csv"
    source.write_text("name,value\na,1\nb,2\n", encoding="utf-8")
    report = dataprof.profile_file(source, engine=engine)
    assert report.recovery_events == []
    assert report.to_dict()["execution"]["recovery_events"] == []
    # The extension's Rust document and the Python export agree on provenance.
    native = getattr(report, "_report")._accessor._value
    assert json.loads(native.to_json())["execution"]["recovery_events"] == []
    assert dataprof.ProfileReport.from_json(report.to_json()).recovery_events == []


@pytest.mark.parametrize(
    "events",
    [
        [],
        [
            {
                "kind": "engine_fallback",
                "attempted": "columnar",
                "retry": "incremental",
                "error": "Primary parser failed",
            },
            {
                "kind": "csv_auto_recovery",
                "attempted": "strict",
                "retry": "flexible",
                "error": "Unequal field counts",
            },
        ],
    ],
)
def test_recovery_history_survives_save_load_and_is_defensively_copied(tmp_path, events):
    document = dataprof.profile({"value": [1, 2]}).to_dict()
    document["execution"]["recovery_events"] = events
    schema = json.loads(
        (REPO_ROOT / "docs/schema/profile-report.v1.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator(schema).validate(document)
    restored = dataprof.ProfileReport.from_dict(document)
    saved = tmp_path / "report.json"
    restored.save(saved)
    loaded = dataprof.ProfileReport.load(saved)
    assert loaded.recovery_events == events
    assert loaded.to_dict() == document
    copied = loaded.recovery_events
    assert copied is not None
    if copied:
        copied[0]["error"] = "mutated"
    copied.clear()
    assert loaded.recovery_events == events


def test_legacy_report_keeps_unknown_recovery_history():
    document = dataprof.profile({"value": [1, 2]}).to_dict()
    del document["execution"]["recovery_events"]
    restored = dataprof.ProfileReport.from_dict(document)
    assert restored.recovery_events is None
    assert "recovery_events" not in restored.to_dict()["execution"]
    assert dataprof.ProfileReport.from_json(restored.to_json()).recovery_events is None
