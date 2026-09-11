"""Why a report has no quality assessment (#715).

``quality is None`` is the answer to two different questions. A run that never
asked for quality metrics and a run whose quality computation failed both left
it absent, and the failure's only record was a log line that no consumer reads
back -- so absence itself was the plausible value that hid the failure.

Every report now carries ``quality_status``. These tests pin the states a
Python caller can reach, the message that comes with a failure, and that the
answer survives ``to_dict`` / ``from_dict``. The failure state itself is forced
in the Rust assembler tests, where the computation lives.
"""

from __future__ import annotations

import json
from typing import Any, cast

import pytest

try:
    import dataprof
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop --features python",
        allow_module_level=True,
    )


@pytest.fixture()
def csv_path(tmp_path):
    path = tmp_path / "rows.csv"
    path.write_text("id,cap,amount\n1,20121,10.5\n2,00184,21.0\n3,10121,33.5\n")
    return str(path)


def _native_status(report: dataprof.ProfileReport) -> dict:
    """The status as the Rust dialect serializes it."""
    return json.loads(cast(Any, report)._report.to_json())["quality_status"]


class TestStates:
    def test_computed(self, csv_path):
        report = dataprof.profile(csv_path)

        assert report.quality is not None
        assert report.quality_status == "computed"
        assert report.quality_error is None

    def test_deselected_pack_reads_as_not_requested(self, csv_path):
        report = dataprof.profile(csv_path, metrics=["schema"])

        assert report.quality is None
        assert report.quality_status == "not_requested"
        assert report.quality_error is None

    def test_empty_source_reads_as_no_data(self, tmp_path):
        path = tmp_path / "empty.csv"
        path.write_text("")

        report = dataprof.profile(str(path))

        assert report.quality is None
        assert report.quality_status == "no_data"

    def test_projection_withholding_every_dimension_is_not_a_skip(self, csv_path):
        # Completeness and uniqueness both measure whole rows, so projecting a
        # column withholds them. The caller did ask; there is nothing to report
        # under full-row names.
        report = dataprof.profile(
            csv_path,
            columns=["amount"],
            quality_dimensions=["completeness", "uniqueness"],
        )

        assert report.quality is None
        assert report.quality_status == "withheld_by_projection"

    def test_header_only_input_is_analyzed_not_skipped(self, tmp_path):
        """Columns but no rows is "analyzed, nothing found", not "not analyzed"."""
        path = tmp_path / "header_only.csv"
        path.write_text("a,b,c\n")

        report = dataprof.profile(str(path))

        assert report.quality_status == "computed"
        assert report.quality_score is None


def _every_reachable_state(csv_path, tmp_path):
    """One report per state a Python caller can actually produce.

    `failed` is absent because no input path can make the metrics calculator
    return `Err`; it is forced in the Rust assembler tests. `unrecorded` only
    comes from loading an older document, which `TestSerialization` covers.
    """
    empty = tmp_path / "empty.csv"
    empty.write_bytes(b"")
    return {
        "computed": dataprof.profile(csv_path),
        "not_requested": dataprof.profile(csv_path, metrics=["schema"]),
        "no_data": dataprof.profile(str(empty)),
        "withheld_by_projection": dataprof.profile(
            csv_path,
            columns=["amount"],
            quality_dimensions=["completeness", "uniqueness"],
        ),
    }


class TestSerialization:
    def test_both_dialects_agree(self, csv_path, tmp_path):
        """The binding spells these strings by hand next to serde's rename_all.

        Nothing but this comparison stops the two from drifting apart, so it
        covers every state a Python caller can reach rather than a sample.
        """
        for expected, report in _every_reachable_state(csv_path, tmp_path).items():
            native = _native_status(report)
            assert native == {"state": expected}, expected
            assert report.to_dict()["quality_status"] == native, expected
            assert report.quality_status == expected
            assert report.quality_error is None

    def test_document_shape(self, csv_path):
        report = dataprof.profile(csv_path, metrics=["schema"])

        # One object, one fact: no `error` key unless there was an error.
        assert report.to_dict()["quality_status"] == {"state": "not_requested"}
        assert json.loads(report.to_json())["quality_status"] == {"state": "not_requested"}

    def test_round_trip(self, csv_path, tmp_path):
        for expected, report in _every_reachable_state(csv_path, tmp_path).items():
            reloaded = dataprof.ProfileReport.from_dict(report.to_dict())

            assert reloaded.quality_status == expected
            assert reloaded.quality_error == report.quality_error

    def test_python_dialect_without_the_field_still_validates(self, csv_path):
        """The field was dropped from *two* `required` lists, one per dialect.

        The Rust half is guarded in `tests/profile_report_schema.rs`; this is
        the other half, so `quality_status` becoming required again in the
        Python document cannot slip through on a passing Rust test.
        """
        import json as _json
        from pathlib import Path

        import jsonschema

        schema_path = (
            Path(__file__).resolve().parents[2]
            / "docs"
            / "schema"
            / "profile-report.v1.schema.json"
        )
        validator = jsonschema.Draft202012Validator(
            _json.loads(schema_path.read_text(encoding="utf-8"))
        )

        document = dataprof.profile(csv_path).to_dict()
        validator.validate(document)

        del document["quality_status"]
        validator.validate(document)

    def test_documents_written_before_the_field_read_back_honestly(self, csv_path):
        """A stored assessment proves the computation ran. Its absence proves
        nothing, so the reason is `unrecorded` rather than an invented one."""
        document = dataprof.profile(csv_path).to_dict()
        del document["quality_status"]

        assert dataprof.ProfileReport.from_dict(document).quality_status == "computed"

        document["quality"] = None
        assert dataprof.ProfileReport.from_dict(document).quality_status == "unrecorded"


class TestMalformedDocuments:
    """`from_dict` used to accept any status document it was handed.

    Two of these re-serialized into a document the committed schema rejects, so
    a report could round-trip into an invalid one; the other two state a verdict
    their own contents contradict. The Rust reader already refused the first
    pair, so accepting them here also made the same file load in one language
    and fail in the other.
    """

    @pytest.mark.parametrize(
        ("status", "message"),
        [
            ({"state": "made_up"}, "unknown quality_status state"),
            ({"state": "failed"}, "must carry a string `error`"),
            ({"state": "computed", "error": "x"}, "must not carry an `error`"),
            ({"state": "not_requested"}, "contradicts the report"),
            ("computed", "must be an object"),
            # An absent key is a pre-0.12 document; an explicit null is a
            # malformed current one, which the committed schema also rejects.
            (None, "must be an object"),
            # An unhashable state used to raise TypeError out of the membership
            # test, which is not the contract this function promises.
            ({"state": []}, "unknown quality_status state"),
            ({"state": 3}, "unknown quality_status state"),
        ],
    )
    def test_rejected(self, csv_path, status, message):
        document = dataprof.profile(csv_path).to_dict()
        document["quality_status"] = status

        with pytest.raises(ValueError, match=message):
            dataprof.ProfileReport.from_dict(document)

    def test_a_status_that_agrees_with_the_report_is_accepted(self, csv_path):
        document = dataprof.profile(csv_path).to_dict()
        document["quality"] = None
        document["quality_status"] = {"state": "failed", "error": "boom"}

        report = dataprof.ProfileReport.from_dict(document)

        assert report.quality_status == "failed"
        assert report.quality_error == "boom"
