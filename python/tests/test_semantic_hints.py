"""Semantic hints, hint validation, and per-column binding evidence."""

from __future__ import annotations

import pytest

try:
    import dataprof
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop --features python",
        allow_module_level=True,
    )


class TestSemanticHints:
    def test_positive_columns_drive_negative_values_metric(self, tmp_path):
        path = tmp_path / "pressure.csv"
        path.write_text("pressure,temperature_delta\n101325,1\n-500,-2\n100900,3\n")

        without_hint = dataprof.profile(str(path), engine="incremental")
        assert without_hint.quality is not None
        assert without_hint.quality.accuracy is not None
        assert without_hint.quality.accuracy["negative_values_in_positive"] == 0

        with_hint = dataprof.profile(
            str(path),
            engine="incremental",
            positive_columns=["pressure"],
        )
        assert with_hint.quality is not None
        assert with_hint.quality.accuracy is not None
        assert with_hint.quality.accuracy["negative_values_in_positive"] == 1

    def test_identifier_columns_omit_numeric_stats(self, tmp_path):
        path = tmp_path / "orders.csv"
        path.write_text("order_id\n1\n2\n3\n10000\n")

        report = dataprof.profile(
            str(path),
            engine="incremental",
            identifier_columns=["order_id"],
        )
        order_id = report["order_id"]
        assert order_id.data_type == "identifier"
        assert order_id.mean is None
        assert order_id.outlier_count is None
        assert report.quality is not None
        assert report.quality.accuracy is not None
        assert report.quality.accuracy["outlier_ratio"] == 0.0

    def test_dataframe_hints(self):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({"order_id": [1, 2, 3], "pressure": [1, -1, 2]})
        report = dataprof.profile(
            df,
            identifier_columns=["order_id"],
            positive_columns=["pressure"],
        )
        assert report["order_id"].data_type == "identifier"
        assert report.quality is not None
        assert report.quality.accuracy is not None
        assert report.quality.accuracy["negative_values_in_positive"] == 1


class TestSemanticHintValidation:
    """Hints must bind or fail loudly, never vanish silently."""

    def test_unknown_positive_hint_name_raises_valueerror(self):
        with pytest.raises(ValueError) as exc:
            dataprof.profile({"pressure": ["1", "2", "3"]}, positive_columns=["presure"])
        msg = str(exc.value)
        assert "presure" in msg
        assert "positive_columns" in msg

    def test_unknown_temporal_hint_name_raises_valueerror(self):
        with pytest.raises(ValueError):
            dataprof.profile(
                {"observed_on": ["2020-01-01", "2021-01-01"]},
                temporal_columns=["not_a_column"],
            )

    def test_unknown_identifier_hint_name_raises_valueerror(self):
        with pytest.raises(ValueError):
            dataprof.profile({"code": ["A", "B", "C"]}, identifier_columns=["id"])

    def test_positive_hint_on_text_column_raises_valueerror(self):
        with pytest.raises(ValueError) as exc:
            dataprof.profile({"name": ["alice", "bob", "carol"]}, positive_columns=["name"])
        assert "name" in str(exc.value)

    def test_temporal_hint_on_non_date_column_raises_valueerror(self):
        with pytest.raises(ValueError):
            dataprof.profile({"name": ["alice", "bob", "carol"]}, temporal_columns=["name"])

    def test_valid_positive_hint_records_binding(self):
        report = dataprof.profile(
            {"pressure": ["101325", "-500", "100900"]},
            positive_columns=["pressure"],
        )
        bindings = report.semantic_hint_bindings
        assert len(bindings) == 1
        binding = bindings[0]
        assert binding["column"] == "pressure"
        assert binding["kind"] == "positive"
        assert binding["matched_values"] == 3
        assert binding["exact"] is True
        # to_dict() carries the same evidence.
        assert report.to_dict()["semantic_hint_bindings"] == bindings

    def test_mixed_temporal_column_binds_without_error(self):
        report = dataprof.profile(
            {"event": ["2020-01-01", "not-a-date", "2022-06-15"]},
            temporal_columns=["event"],
        )
        binding = next(b for b in report.semantic_hint_bindings if b["column"] == "event")
        assert binding["checked_values"] == 3
        assert binding["matched_values"] == 2

    def test_identifier_hint_binds_on_text_column(self):
        report = dataprof.profile({"code": ["X", "Y", "Z"]}, identifier_columns=["code"])
        binding = next(b for b in report.semantic_hint_bindings if b["column"] == "code")
        assert binding["kind"] == "identifier"
        assert binding["matched_values"] == binding["checked_values"]

    def test_hint_free_report_has_no_bindings(self):
        report = dataprof.profile({"pressure": ["1", "2", "3"]})
        assert report.semantic_hint_bindings == []
        assert "semantic_hint_bindings" not in report.to_dict()
