"""Per-column evidence that a column defeated type inference (#561).

`data_type` cannot carry this: below the inference thresholds a half-numeric
column is typed `string`, identical in a schema listing to a column of names,
and `invalid_count` is absent on string columns by contract. `type_homogeneity`
carries the counts, and `to_llm_context()` turns them into a flag — which is
acceptance criterion 4 of #544, the half an agent actually reads.
"""

from __future__ import annotations

import json
import pathlib

import pytest
from jsonschema import Draft202012Validator

try:
    import dataprof as dp
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )


def _profile_column(tmp_path, values: list[str], name: str = "v", **kwargs):
    path = tmp_path / "column.csv"
    path.write_text(f"{name}\n" + "\n".join(values) + "\n", encoding="utf-8")
    return dp.profile(str(path), **kwargs)


def _numeric_with_junk(junk: int, total: int = 1000) -> list[str]:
    """The shape reported in #544: `junk` non-numeric values padded with integers."""
    return [str(1000 + i) for i in range(total - junk)] + [f"junk{i}" for i in range(junk)]


def _flags(report) -> list[str]:
    """The flag lines of a report's LLM context, without the bullet."""
    context = report.to_llm_context()
    if "\nflags (" not in context:
        return []
    section = context.split("\nflags (", 1)[1]
    body = section.split("\n\n", 1)[0]
    return [line[2:] for line in body.splitlines() if line.startswith("- ")]


class TestTypeHomogeneity:
    """The field itself."""

    def test_counts_every_non_null_value_by_class(self, tmp_path):
        report = _profile_column(tmp_path, ["1", "2.5", "2024-01-15", "true", "junk", ""])

        assert report["v"].type_homogeneity == {
            "boolean": 1,
            "date": 1,
            "numeric": 2,
            "text": 1,
        }

    def test_a_clean_column_reports_one_class_rather_than_absence(self, tmp_path):
        # Absence has to keep meaning "not classified". A clean column is
        # `Some` with everything in one class, never a missing field.
        report = _profile_column(tmp_path, [str(i) for i in range(10)])

        assert report["v"].type_homogeneity == {
            "boolean": 0,
            "date": 0,
            "numeric": 10,
            "text": 0,
        }

    def test_an_all_null_column_is_classified_and_holds_nothing(self, tmp_path):
        # "Analyzed, found nothing" is four zero counts — not None, which would
        # claim the classification never ran.
        report = _profile_column(tmp_path, ["", "", ""])

        assert report["v"].type_homogeneity == {
            "boolean": 0,
            "date": 0,
            "numeric": 0,
            "text": 0,
        }

    def test_counts_survive_the_document_round_trip(self, tmp_path):
        report = _profile_column(tmp_path, _numeric_with_junk(200))
        native = report["v"].type_homogeneity
        assert native == {"boolean": 0, "date": 0, "numeric": 800, "text": 200}

        restored = dp.ProfileReport.from_json(report.to_json())

        assert restored["v"].type_homogeneity == native
        assert json.loads(report.to_json())["columns"][0]["type_homogeneity"] == native

    def test_absence_does_not_read_back_as_zero_counts(self):
        # A document written before this field existed must reload as "not
        # classified", not as a column whose values were all classified away.
        document = {
            "schema_version": dp.REPORT_SCHEMA_VERSION,
            "source": "legacy.csv",
            "source_type": "file",
            "execution": {"rows_processed": 1, "columns_detected": 1},
            "columns": [{"name": "v", "data_type": "string", "total_count": 1, "null_count": 0}],
            "quality": None,
        }

        restored = dp.ProfileReport.from_dict(document)

        assert restored["v"].type_homogeneity is None
        assert "type_homogeneity" not in restored.to_dict()["columns"][0]

    def test_the_published_schema_rejects_what_the_loader_discards(self):
        # A partial mapping validating against the schema while the loader
        # treats it as absence would let a document be "valid" and still lose
        # the field on reload. The Python dialect is described by the same
        # four-key definition the Rust dialect is.
        schema_path = (
            pathlib.Path(__file__).resolve().parents[2]
            / "docs"
            / "schema"
            / "profile-report.v1.schema.json"
        )
        validator = Draft202012Validator(json.loads(schema_path.read_text(encoding="utf-8")))

        document = {
            "schema_version": dp.REPORT_SCHEMA_VERSION,
            "source": "hand-edited.csv",
            "source_type": "file",
            "execution": {
                "engine": None,
                "rows_processed": 1,
                "columns_detected": 1,
                "scan_time_ms": 0,
                "source_exhausted": True,
                "truncation_reason": None,
                "bytes_consumed": None,
                "throughput_rows_sec": None,
                "memory_peak_mb": None,
                "error_count": 0,
                "ragged_row_count": 0,
                "sampling_applied": False,
                "sampling_ratio": None,
            },
            "columns": [
                {
                    "name": "v",
                    "data_type": "string",
                    "total_count": 1,
                    "null_count": 0,
                    "null_percentage": 0.0,
                    "unique_count": 1,
                    "unique_count_is_approximate": False,
                    "uniqueness_ratio": 1.0,
                    "type_homogeneity": {"numeric": 0, "date": 0, "boolean": 0, "text": 1},
                }
            ],
            "quality": None,
        }
        validator.validate(document)

        document["columns"][0]["type_homogeneity"] = {"numeric": 3}
        assert list(validator.iter_errors(document)), (
            "a partial count map must not validate, because the loader drops it"
        )

    def test_a_malformed_mapping_reads_as_absence(self):
        # An invented count is worse than a gap: a partial mapping cannot be
        # completed with zeros, because zero is a claim about the data.
        document = {
            "schema_version": dp.REPORT_SCHEMA_VERSION,
            "source": "hand-edited.csv",
            "source_type": "file",
            "execution": {"rows_processed": 1, "columns_detected": 1},
            "columns": [
                {
                    "name": "v",
                    "data_type": "string",
                    "total_count": 1,
                    "null_count": 0,
                    "type_homogeneity": {"numeric": 3},
                }
            ],
            "quality": None,
        }

        assert dp.ProfileReport.from_dict(document)["v"].type_homogeneity is None

    def test_the_flat_exports_carry_the_derived_scalars(self, tmp_path):
        # to_dataframe()/save(".csv") are flat and cannot hold the count map, so
        # they carry the dominant class and its share instead.
        report = _profile_column(tmp_path, _numeric_with_junk(200))

        record = report._records()[0]

        assert record["dominant_type"] == "numeric"
        assert record["dominant_type_share"] == 0.8


class TestMixedTypeFlag:
    """What `to_llm_context()` does with it."""

    def test_a_column_that_defeated_inference_is_flagged(self, tmp_path):
        # The acceptance criterion: 20% junk types the column `string` and used
        # to render exactly like a column of names.
        report = _profile_column(tmp_path, _numeric_with_junk(200))

        assert report["v"].data_type == "string"
        assert _flags(report) == ["v: mixed types (80% numeric, 20% text)"]

    def test_a_textual_column_is_not_flagged(self, tmp_path):
        # The control the flag exists to be distinguishable from. This column is
        # also `string`, and it is fine.
        report = _profile_column(tmp_path, [f"person{i}" for i in range(100)], name="name")

        assert report["name"].data_type == "string"
        assert _flags(report) == []

    def test_the_flag_states_the_mixture_rather_than_the_type(self, tmp_path):
        report = _profile_column(
            tmp_path,
            ["1"] * 60 + ["2024-01-15"] * 10 + ["junk"] * 30,
        )

        assert _flags(report) == ["v: mixed types (60% numeric, 30% text, 10% date)"]

    def test_a_stray_value_does_not_earn_a_flag(self, tmp_path):
        # Display threshold: below 5% outside the dominant class the column is
        # not "mixed", and a flag there would spend budget an agent asked for.
        assert _flags(_profile_column(tmp_path, _numeric_with_junk(3))) == []

    def test_the_threshold_fires_where_it_is_documented(self, tmp_path):
        report = _profile_column(tmp_path, _numeric_with_junk(50))

        assert _flags(report) == ["v: mixed types (95% numeric, 5% text)"]

    def test_a_share_too_small_to_round_is_not_printed_as_zero(self, tmp_path):
        # 1 date in 1000 is 0.1%; a third class rounding to "0%" would state
        # something untrue beside a real mixture.
        values = _numeric_with_junk(100, total=999) + ["2024-01-15"]
        report = _profile_column(tmp_path, values)

        assert _flags(report) == ["v: mixed types (89.9% numeric, 10% text, 0.1% date)"]

    def test_a_sampled_count_discloses_its_scope(self, tmp_path):
        # The counts come from the engine's 10k reservoir. A share taken over
        # 10k values must not read as a fact about 50k.
        report = _profile_column(tmp_path, _numeric_with_junk(12_500, total=50_000))
        flags = _flags(report)

        assert len(flags) == 1
        assert flags[0].startswith("v: mixed types (")
        assert "; sampled 10,000 of 50,000 values)" in flags[0]

    def test_an_unsampled_count_claims_no_scope(self, tmp_path):
        report = _profile_column(tmp_path, _numeric_with_junk(200))

        assert "sampled" not in _flags(report)[0]

    def test_identifier_columns_may_mix_forms_without_a_flag(self, tmp_path):
        # An ID scheme mixing "A1" and "123" is intended, not a defect — the
        # same exemption the consistency dimension makes.
        values = [f"A{i}" for i in range(50)] + [str(i) for i in range(50)]
        report = _profile_column(
            tmp_path, values, name="customer_id", identifier_columns=["customer_id"]
        )

        assert report["customer_id"].data_type == "identifier"
        assert _flags(report) == []

    def test_the_flag_survives_the_document_round_trip(self, tmp_path):
        report = _profile_column(tmp_path, _numeric_with_junk(200))

        restored = dp.ProfileReport.from_json(report.to_json())

        assert restored.to_llm_context() == report.to_llm_context()
        assert _flags(restored) == ["v: mixed types (80% numeric, 20% text)"]
