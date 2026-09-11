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


class TestSerialization:
    def test_both_dialects_agree(self, csv_path):
        for report in (
            dataprof.profile(csv_path),
            dataprof.profile(csv_path, metrics=["schema"]),
        ):
            assert report.to_dict()["quality_status"] == _native_status(report)

    def test_document_shape(self, csv_path):
        report = dataprof.profile(csv_path, metrics=["schema"])

        # One object, one fact: no `error` key unless there was an error.
        assert report.to_dict()["quality_status"] == {"state": "not_requested"}
        assert json.loads(report.to_json())["quality_status"] == {"state": "not_requested"}

    def test_round_trip(self, csv_path):
        for report in (
            dataprof.profile(csv_path),
            dataprof.profile(csv_path, metrics=["schema"]),
        ):
            reloaded = dataprof.ProfileReport.from_dict(report.to_dict())

            assert reloaded.quality_status == report.quality_status
            assert reloaded.quality_error == report.quality_error

    def test_documents_written_before_the_field_read_back_honestly(self, csv_path):
        """A stored assessment proves the computation ran. Its absence proves
        nothing, so the reason is `unrecorded` rather than an invented one."""
        document = dataprof.profile(csv_path).to_dict()
        del document["quality_status"]

        assert dataprof.ProfileReport.from_dict(document).quality_status == "computed"

        document["quality"] = None
        assert dataprof.ProfileReport.from_dict(document).quality_status == "unrecorded"
