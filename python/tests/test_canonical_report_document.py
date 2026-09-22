"""One native document for Rust and Python JSON persistence (#714)."""

import json
from pathlib import Path

import dataprof
import pytest
from dataprof import _dataprof
from jsonschema import Draft202012Validator


def test_python_saves_the_same_shared_document_as_rust(tmp_path):
    fixture = Path(__file__).parents[2] / "tests/fixtures/canonical_report.json"
    expected = fixture.read_text(encoding="utf-8").rstrip()
    report = dataprof.ProfileReport.load(fixture)
    report.save(tmp_path / "report.json")
    assert (tmp_path / "report.json").read_bytes() == expected.encode("utf-8")
    assert report.to_json() == expected
    assert [column.name for column in report.profiles] == ["z", "a"]
    assert report["z"].patterns == []
    assert report["a"].patterns is None


@pytest.mark.parametrize("packs", [None, ["schema"], ["schema", "statistics"]])
def test_python_json_is_the_rust_document(tmp_path, packs):
    source = tmp_path / "input.csv"
    source.write_text("text,number,flag\n東京,1.23456,true\nété,2.34567,false\n", encoding="utf-8")
    report = dataprof.profile(source, **({"metrics": packs} if packs else {}))
    native = getattr(report, "_native_report")
    assert native is not None
    expected = native.to_json()
    assert report.to_json() == expected
    document = json.loads(expected)
    assert "id" in document and "timestamp" in document
    assert "data_source" in document and "column_profiles" in document
    assert "source" not in document and "columns" not in document
    assert json.loads(report.to_json(indent=4)) == document
    saved = tmp_path / "report.json"
    report.save(saved)
    assert saved.read_bytes() == expected.encode("utf-8")
    for restored in (
        dataprof.ProfileReport.load(saved),
        dataprof.ProfileReport.from_json(expected),
        dataprof.ProfileReport.from_dict(document),
    ):
        assert restored.to_json() == expected
        assert restored.to_dict() == report.to_dict()
        assert restored.to_markdown() == report.to_markdown()


def test_summary_is_produced_and_described_by_rust():
    report = dataprof.profile({"z": [1, None, 3], "a": ["東京", "é", "hello"]})
    assert report.to_dict() == json.loads(getattr(report, "_native_report").summary_json())
    schema = json.loads(
        (Path(__file__).parents[2] / "docs/schema/profile-report.v1.schema.json").read_text(
            encoding="utf-8"
        )
    )
    validator = Draft202012Validator(schema)
    validator.validate(report.to_dict())
    validator.validate(json.loads(report.to_json()))


def test_legacy_summary_is_not_given_invented_provenance():
    summary = dataprof.profile({"text": ["東京", None]}).to_dict()
    summary.pop("schema_version")
    summary["execution"].pop("recovery_events")
    summary["quality"].pop("sampled_dimensions", None)
    restored = dataprof.ProfileReport.from_dict(summary)
    # The resave is written by this build, so it states the version it wrote.
    resaved = {**summary, "schema_version": dataprof.REPORT_SCHEMA_VERSION}
    assert restored.to_dict() == resaved
    assert json.loads(restored.to_json()) == resaved
    assert restored.recovery_events is None
    assert restored.quality_sampled_dimensions is None
    summary["columns"].clear()
    assert len(restored.profiles) == 1


@pytest.mark.parametrize("version", [-1, True, None, 1.5, "1", 999])
def test_canonical_version_gate(version):
    with pytest.raises(ValueError):
        dataprof.ProfileReport.from_dict({"schema_version": version, "data_source": {}})


def test_canonical_failed_quality_and_unknown_history_survive():
    document = json.loads(dataprof.profile({"x": [1, 2]}).to_json())
    document.pop("quality")
    document["quality_status"] = {"state": "failed", "error": "calculation failed"}
    document["execution"].pop("recovery_events")
    restored = dataprof.ProfileReport.from_dict(document)
    assert restored.quality is None
    assert restored.quality_error == "calculation failed"
    assert restored.recovery_events is None
    assert json.loads(restored.to_json()) == document
    document["quality_status"] = {"state": "computed"}
    with pytest.raises(ValueError, match="quality"):
        dataprof.ProfileReport.from_dict(document)


def test_native_reader_rejects_future_schema_before_decoding():
    with pytest.raises(ValueError, match="newer"):
        _dataprof.ProfileReport.from_json('{"schema_version": 999}')


def test_saved_quality_scores_are_not_recomputed_from_rounded_inputs():
    report = dataprof.profile({"x": [None] * 23 + list(range(67))})
    document = json.loads(report.to_json())
    assert document["quality"]["scores"]["overall_score"] == report.quality_score
    restored = dataprof.ProfileReport.from_dict(document)
    assert restored.to_dict()["quality"] == report.to_dict()["quality"]
    assert (
        restored.check(min_quality_score=report.quality_score).to_dict()
        == report.check(min_quality_score=report.quality_score).to_dict()
    )


def _schema_validator():
    schema_path = Path(__file__).parents[2] / "docs/schema/profile-report.v1.schema.json"
    return Draft202012Validator(json.loads(schema_path.read_text(encoding="utf-8")))


def test_canonical_document_without_saved_scores_validates():
    document = json.loads(dataprof.profile({"x": [1, 2, None]}).to_json())
    document["quality"].pop("scores")
    _schema_validator().validate(document)
    quality = dataprof.ProfileReport.from_dict(document).quality
    assert quality is not None
    assert quality.overall_quality_score() is not None


def test_scores_for_a_later_dimension_survive_but_stay_out_of_the_summary():
    document = json.loads(dataprof.profile({"x": [1, 2, None]}).to_json())
    document["quality"]["scores"]["dimension_scores"]["lineage"] = None
    restored = dataprof.ProfileReport.from_dict(document)
    assert restored.quality is not None
    summary = restored.to_dict()
    assert "lineage" not in summary["quality"]["dimension_scores"]
    assert set(summary["quality"]["dimension_scores"]) == set(
        restored.quality.dimension_scores()
    ) - {"lineage"}
    assert (
        json.loads(restored.to_json())["quality"]["scores"]["dimension_scores"]["lineage"] is None
    )


def test_restored_quality_prints_the_score_it_returns():
    document = json.loads(dataprof.profile({"x": [1, 2, 3]}).to_json())
    scores = document["quality"]["scores"]
    assessed = [name for name, value in scores["dimension_scores"].items() if value is not None]
    assert "completeness" in assessed
    # Retained scores need not match a recomputation from rounded metrics.
    scores["overall_score"] = 12.5
    scores["dimension_scores"]["completeness"] = 37.5
    text = json.dumps(document)
    # The native object formats itself; the public wrapper reads accessors.
    for quality in (
        _dataprof.ProfileReport.from_json(text).quality,
        dataprof.ProfileReport.from_json(text).quality,
    ):
        assert quality is not None
        assert quality.overall_quality_score() == 12.5
        assert "score=12.5%" in str(quality)
        assert "completeness=37.5%" in str(quality)
        assert "score=12.5%" in repr(quality)


def test_legacy_summary_export_agrees_with_what_was_loaded():
    summary = dataprof.profile({"x": ["a", "1", None]}).to_dict()
    summary["columns"][0]["type_homogeneity"] = {"numeric": 1, "text": 1}
    restored = dataprof.ProfileReport.from_dict(summary)
    assert restored["x"].type_homogeneity is None
    for exported in (restored.to_dict(), json.loads(restored.to_json())):
        assert exported["columns"][0].get("type_homogeneity") is None


def test_custom_score_weights_survive_the_flat_summary():
    document = json.loads(dataprof.profile({"x": [1, None, 3], "y": [1, 1, 2]}).to_json())
    weights = {
        "completeness": 1.0,
        "consistency": 0.0,
        "uniqueness": 0.0,
        "accuracy": 0.0,
        "timeliness": 0.0,
        "validity": 0.0,
        "precision": 3.0,
    }
    document["quality"]["metrics"]["score_weights"] = weights
    canonical = dataprof.ProfileReport.from_dict(document)
    summary = canonical.to_dict()
    assert summary["quality"]["score_weights"] == weights
    _schema_validator().validate(summary)
    flat = dataprof.ProfileReport.from_dict(summary)
    assert flat.quality is not None
    assert flat.quality.score_weights == weights
    # The flat loader's own summary is a second producer and must agree.
    assert flat.to_dict() == summary


def test_default_score_weights_stay_out_of_both_summaries():
    summary = dataprof.profile({"x": [1, None, 3]}).to_dict()
    assert "score_weights" not in summary["quality"]
    assert "score_weights" not in dataprof.ProfileReport.from_dict(summary).to_dict()["quality"]


def _weights_through_both_readers(weights):
    """The same value read from a flat summary and from a canonical document."""
    report = dataprof.profile({"x": [1, None, 3]})
    summary = report.to_dict()
    summary["quality"]["score_weights"] = weights
    canonical = json.loads(report.to_json())
    canonical["quality"]["metrics"]["score_weights"] = weights
    flat = dataprof.ProfileReport.from_dict(summary).quality
    native = dataprof.ProfileReport.from_json(json.dumps(canonical)).quality
    assert flat is not None and native is not None
    return flat.score_weights, native.score_weights


@pytest.mark.parametrize(
    "weights",
    [
        {"completeness": 1.0},
        {"completeness": 2, "precision": 0},
        {"completeness": 1.0, "lineage": 5.0},
    ],
    ids=["partial", "integers", "unknown-dimension"],
)
def test_flat_score_weights_read_as_the_rust_reader_reads_them(weights):
    flat, native = _weights_through_both_readers(weights)
    assert flat == native


@pytest.mark.parametrize(
    "weights",
    ["x", None, {"completeness": "high"}, {"completeness": True}, [1, 2]],
    ids=["string", "null", "string-weight", "bool-weight", "sequence"],
)
def test_malformed_flat_score_weights_are_rejected_not_defaulted(weights):
    summary = dataprof.profile({"x": [1, None, 3]}).to_dict()
    summary["quality"]["score_weights"] = weights
    with pytest.raises(ValueError, match=r"quality\.score_weights"):
        dataprof.ProfileReport.from_dict(summary)


def test_absent_flat_score_weights_are_the_defaults():
    summary = dataprof.profile({"x": [1, None, 3]}).to_dict()
    assert "score_weights" not in summary["quality"]
    flat = dataprof.ProfileReport.from_dict(summary).quality
    live = dataprof.profile({"x": [1]}).quality
    assert flat is not None and live is not None
    assert flat.score_weights == live.score_weights


@pytest.mark.parametrize("key", ["data_source", "column_profiles"])
def test_additive_canonical_key_does_not_reroute_a_flat_summary(key):
    summary = dataprof.profile({"x": [1, 2, None]}).to_dict()
    summary[key] = {"added_by": "a later release"}
    restored = dataprof.ProfileReport.from_dict(summary)
    assert [column.name for column in restored.profiles] == ["x"]
    assert restored.to_dict() == {k: v for k, v in summary.items() if k != key}
