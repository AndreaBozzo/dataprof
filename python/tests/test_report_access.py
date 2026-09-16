"""ProfileReport accessors, mapping, rounding, column dictionaries, and repr."""

from __future__ import annotations

import builtins
import json
import os
import tempfile

import pytest
from conftest import CSV_FILE

try:
    import dataprof
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop --features python",
        allow_module_level=True,
    )


class TestProfileReport:
    def test_properties(self, report):
        assert isinstance(report.source, str)
        assert isinstance(report.source_type, str)
        assert isinstance(report.rows, int)
        assert isinstance(report.columns, int)
        assert isinstance(report.execution_time_ms, int)
        assert isinstance(report.source_exhausted, bool)
        assert isinstance(report.sampling_applied, bool)

    def test_column_profile_fields(self, report):
        col = report.column_profiles["name"]
        assert hasattr(col, "name")
        assert hasattr(col, "data_type")
        assert hasattr(col, "total_count")
        assert hasattr(col, "null_count")
        assert hasattr(col, "null_percentage")

    def test_column_profile_repr(self, report):
        r = repr(report["name"])
        assert "ColumnProfile" in r
        assert "name='name'" in r
        assert "type=" in r
        assert "nulls=" in r

    def test_quality_repr(self, report):
        r = repr(report.quality)
        assert "DataQualityMetrics" in r
        assert "score=" in r
        # `assessed=` names the dimensions the score is made of. It used to list
        # the metric structs that exist, which is all seven whenever quality ran.
        assert f"assessed=[{', '.join(report.quality.assessed_dimensions())}]" in r

    def test_csv_duplicate_rows_cover_full_stream_and_expose_provenance(self, tmp_path):
        path = tmp_path / "duplicates.csv"
        path.write_text("a,b\nx,1\nx,1\ny,2\n", encoding="utf-8")

        report = dataprof.profile(str(path), quality_dimensions=["uniqueness"])
        quality = report.quality
        assert quality is not None
        uniqueness = quality.uniqueness
        assert uniqueness is not None

        assert uniqueness["duplicate_rows"] == 1
        assert uniqueness["rows_checked"] == 3
        assert uniqueness["duplicate_rows_approximate"] is False

    def test_inferred_date_columns_drive_timeliness_score(self, tmp_path):
        path = tmp_path / "events.csv"
        path.write_text(
            "observed_on,value\n2020-01-01,1\n2021-01-01,2\n",
            encoding="utf-8",
        )

        without_hint = dataprof.profile(str(path))
        without_quality = without_hint.quality
        assert without_quality is not None
        assert without_quality.dimension_scores()["timeliness"] is not None

        with_hint = dataprof.profile(str(path), temporal_columns=["observed_on"])
        with_quality = with_hint.quality
        assert with_quality is not None
        assert with_quality.dimension_scores()["timeliness"] is not None
        timeliness = with_quality.timeliness
        assert timeliness is not None
        assert timeliness["date_values_checked"] == 2

    def test_invalid_calendar_date_is_visible_in_timeliness(self, tmp_path):
        path = tmp_path / "invalid_date.csv"
        path.write_text(
            "observed_on\n"
            "2024-01-01\n2024-02-02\n2024-03-03\n2024-04-04\n"
            "2024-05-05\n2024-06-06\n2024-07-07\n2024-08-08\n2024-13-45\n",
            encoding="utf-8",
        )

        report = dataprof.profile(str(path))
        assert report["observed_on"].data_type == "date"
        assert report["observed_on"].invalid_count == 1
        quality = report.quality
        assert quality is not None
        timeliness = quality.timeliness
        assert timeliness is not None
        assert timeliness["date_values_checked"] == 9
        assert timeliness["invalid_date_values"] == 1
        timeliness_score = quality.dimension_scores()["timeliness"]
        assert timeliness_score is not None
        assert timeliness_score < 100.0

    def test_validity_and_precision_dimensions(self, tmp_path):
        path = tmp_path / "semantic_values.csv"
        rows = [f"user{i}@example.com,{i}.25" for i in range(1, 9)] + [
            "invalid-email,9.2",
            "also-invalid,10.3",
        ]
        path.write_text("email,amount\n" + "\n".join(rows) + "\n", encoding="utf-8")

        report = dataprof.profile(str(path), quality_dimensions=["validity", "precision"])
        quality = report.quality
        assert quality is not None
        assert quality.validity is not None
        assert quality.precision is not None

        assert quality.validity["values_checked"] == 10
        assert quality.validity["invalid_values"] == 2
        assert quality.validity["valid_values_ratio"] == 80.0
        assert quality.precision["numeric_values_checked"] == 10
        assert quality.precision["inconsistent_precision_values"] == 2
        assert quality.precision["decimal_places_consistency"] == 80.0

    @pytest.mark.parametrize("restore", ["native", "dict", "json", "file"])
    @pytest.mark.parametrize("dimensions", [None, ["validity"]])
    @pytest.mark.parametrize(
        ("dimension", "names"),
        [
            ("completeness", ("missing_values_ratio", "complete_records_ratio", "null_columns")),
            ("consistency", ("data_type_consistency", "format_violations", "encoding_issues")),
            ("uniqueness", ("duplicate_rows", "key_uniqueness", "high_cardinality_warning")),
            ("accuracy", ("outlier_ratio", "range_violations", "negative_values_in_positive")),
            (
                "timeliness",
                (
                    "future_dates_count",
                    "stale_data_ratio",
                    "temporal_violations",
                    "invalid_date_values",
                ),
            ),
        ],
    )
    def test_removed_quality_accessors_name_replacement(
        self, tmp_path, restore, dimensions, dimension, names
    ):
        report = dataprof.profile(CSV_FILE, quality_dimensions=dimensions)
        document = report.to_dict()
        if restore == "dict":
            report = dataprof.ProfileReport.from_dict(document)
        elif restore == "json":
            report = dataprof.ProfileReport.from_json(report.to_json())
        elif restore == "file":
            path = tmp_path / "report.json"
            report.save(path)
            report = dataprof.ProfileReport.load(path)
        quality = report.quality
        assert quality is not None
        if dimensions is not None:
            assert getattr(quality, dimension) is None
        for name in names:
            assert name not in dir(quality)
            assert not hasattr(quality, name)
            with pytest.raises(AttributeError) as caught:
                getattr(quality, name)
            assert str(caught.value) == (
                f"DataQualityMetrics.{name} was removed in 0.12; "
                f'use quality.{dimension}["{name}"] instead '
                f"(check quality.{dimension} is not None)."
            )
        # Rejected lookups must not materialize defaults for absent dimensions
        # or otherwise change the persisted report.
        assert report.to_dict() == document

    @pytest.mark.parametrize("restored", [False, True])
    def test_unknown_quality_accessor_raises_attribute_error(self, report, restored):
        if restored:
            report = dataprof.ProfileReport.from_dict(report.to_dict())
        quality = report.quality
        assert quality is not None
        with pytest.raises(AttributeError, match="has no attribute.*unknown_metric"):
            getattr(quality, "unknown_metric")

    @pytest.mark.parametrize(
        ("dims", "present", "absent"),
        [
            (["completeness"], "completeness", ["uniqueness", "accuracy"]),
            (["uniqueness"], "uniqueness", ["completeness", "accuracy"]),
            (["accuracy"], "accuracy", ["completeness", "uniqueness"]),
        ],
    )
    def test_quality_dimensions_nested_none_semantics(self, dims, present, absent):
        report = dataprof.profile(CSV_FILE, quality_dimensions=dims)
        q = report.quality
        assert q is not None
        assert getattr(q, present) is not None
        for dimension in absent:
            assert getattr(q, dimension) is None

    @pytest.mark.parametrize("source", ["file", "dict"])
    def test_empty_quality_dimensions_means_not_analyzed(self, source, tmp_path):
        """An empty selection requests no dimension, which is the same as not
        asking for quality — so nothing was analyzed and there is no quality to
        report. Must agree for file and in-memory inputs, which reach the gate
        by different routes."""
        data = {"id": [1, 2, 3], "name": ["a", "b", None]}
        if source == "file":
            path = tmp_path / "d.csv"
            path.write_text("id,name\n1,a\n2,b\n3,\n")
            target = str(path)
        else:
            target = data

        report = dataprof.profile(target, quality_dimensions=[])
        assert report.quality is None
        assert report.quality_score is None
        assert report.to_dict().get("quality") is None

    @pytest.mark.parametrize("source", ["file", "dict"])
    def test_empty_quality_dimensions_matches_deselecting_the_pack(self, source, tmp_path):
        """`quality_dimensions=[]` and `metrics=["schema"]` both say "no quality"
        and must not disagree about how that looks."""
        if source == "file":
            path = tmp_path / "d.csv"
            path.write_text("id,name\n1,a\n2,b\n3,\n")
            target = str(path)
        else:
            target = {"id": [1, 2, 3], "name": ["a", "b", None]}

        empty_dims = dataprof.profile(target, quality_dimensions=[])
        no_pack = dataprof.profile(target, metrics=["schema"])
        assert empty_dims.quality is no_pack.quality is None
        assert empty_dims.quality_score is no_pack.quality_score is None

    def test_empty_selection_still_profiles_the_data(self):
        """Dropping quality must not drop the profile with it."""
        report = dataprof.profile(CSV_FILE, quality_dimensions=[])
        assert report.rows > 0
        assert report.columns > 0
        assert report.quality is None

    def test_analyzed_but_vacuous_is_not_reported_as_unanalyzed(self):
        """The distinction the whole contract rests on: asking for quality and
        finding nothing assessable is *not* the same as never asking. The first
        keeps a quality object saying so; only the second is None."""
        report = dataprof.profile({"a": []})
        assert report.quality is not None
        assert report.quality_score is None
        assert report.quality.assessed_dimensions() == []

        not_asked = dataprof.profile({"a": []}, quality_dimensions=[])
        assert not_asked.quality is None

    def test_str_omits_dimensions_that_were_not_assessed(self):
        """str() read the flat accessors, which substitute a perfect 100.0 for an
        absent dimension — so an unassessed dimension printed as 100%, a
        reassuring number with nothing behind it."""
        q = dataprof.profile(CSV_FILE, quality_dimensions=["completeness"]).quality
        assert q is not None
        rendered = str(q)
        assert "completeness=" in rendered
        assert "consistency=" not in rendered
        assert "uniqueness=" not in rendered

    def test_str_reports_dimensions_that_were_assessed(self):
        q = dataprof.profile(CSV_FILE).quality
        assert q is not None
        rendered = str(q)
        for dimension in ("completeness=", "consistency=", "uniqueness="):
            assert dimension in rendered

    def test_str_says_so_when_nothing_was_assessed(self):
        q = dataprof.profile({"a": []}).quality
        assert q is not None
        assert str(q) == "DataQualityMetrics(not assessed)"

    def test_to_dict(self, report):
        d = report.to_dict()
        assert "source" in d
        assert "columns" in d
        assert "execution" in d
        assert report.engine in {"incremental", "columnar"}
        assert d["execution"]["engine"] in {"incremental", "columnar"}
        assert isinstance(d["columns"], list)

    def test_to_json(self, report):
        j = report.to_json()
        parsed = json.loads(j)
        assert "source" in parsed

    def test_to_dataframe(self, report):
        pytest.importorskip("pandas")
        df = report.to_dataframe()
        assert len(df) == report.columns

    def test_to_dataframe_missing_pandas_has_install_hint(self, report, monkeypatch):
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "pandas":
                raise ImportError("blocked pandas")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        with pytest.raises(
            ImportError,
            match=r"pandas is required for to_dataframe\(\).*uv pip install pandas",
        ):
            report.to_dataframe()

    def test_save_json(self, report):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            result = report.save(path)
            assert result is report  # fluent API
            with open(path) as f:
                parsed = json.loads(f.read())
            assert "source" in parsed
        finally:
            os.unlink(path)

    def test_save_unsupported_raises(self, report):
        with pytest.raises(ValueError, match="Unsupported format"):
            report.save("/tmp/test.xlsx")

    def test_save_html(self, report):
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as f:
            path = f.name
        try:
            assert report.save(path) is report  # fluent API
            with open(path, encoding="utf-8") as f:
                content = f.read()
            assert content == report.to_html()
            assert "<table" in content
        finally:
            os.unlink(path)

    def test_save_markdown(self, report):
        for suffix in (".md", ".markdown"):
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
                path = f.name
            try:
                assert report.save(path) is report
                with open(path, encoding="utf-8") as f:
                    content = f.read()
                assert content == report.to_markdown()
            finally:
                os.unlink(path)

    def test_repr(self, report):
        r = repr(report)
        assert "ProfileReport" in r
        assert "rows=" in r

    def test_repr_html(self, report):
        html = report._repr_html_()
        assert "<table" in html
        assert "ProfileReport" in html


class TestProfileReportMapping:
    def test_getitem(self, report):
        col = report["name"]
        assert col.name == "name"

    def test_getitem_missing_raises(self, report):
        with pytest.raises(KeyError):
            report["nonexistent_column"]

    def test_getitem_non_string_raises_type_error(self, report):
        with pytest.raises(TypeError, match="keys must be strings"):
            report[0]  # type: ignore[index]

    def test_contains(self, report):
        assert "name" in report
        assert "nonexistent_column" not in report

    def test_iter(self, report):
        names = list(report)
        assert len(names) == report.columns
        assert all(isinstance(n, str) for n in names)

    def test_len(self, report):
        assert len(report) == report.columns


class TestRounding:
    def test_to_dict_percentages_rounded(self, report):
        d = report.to_dict()
        for col in d["columns"]:
            np = col["null_percentage"]
            if np is not None:
                # Should have at most 2 decimal places
                assert np == round(np, 2), f"{col['name']}: null_percentage not rounded"

    def test_to_dict_stats_rounded(self, report):
        d = report.to_dict()
        for col in d["columns"]:
            stats = col.get("stats", {})
            for key in ("mean", "std_dev", "variance", "median"):
                v = stats.get(key)
                if v is not None:
                    assert v == round(v, 4), f"{col['name']}: {key} not rounded"

    def test_to_dict_execution_rounded(self, report):
        d = report.to_dict()
        tp = d["execution"]["throughput_rows_sec"]
        if tp is not None:
            assert tp == round(tp, 4)

    def test_quality_score_rounded(self, report):
        qs = report.quality_score
        if qs is not None:
            assert qs == round(qs, 2)


class TestReprImproved:
    def test_repr_multiline(self, report):
        r = repr(report)
        assert "ProfileReport" in r
        assert "Columns:" in r
        # Should show at least one column name from the dataset
        assert any(col_name in r for col_name in report.column_profiles)

    def test_repr_html_enriched(self, report):
        html = report._repr_html_()
        assert "Unique" in html
        assert "Pattern" in html
        assert "Stats" in html


class TestColumnToDict:
    def test_column_to_dict_shape_matches_report(self, tmp_path):
        path = tmp_path / "data.csv"
        path.write_text("x\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n")
        r = dataprof.profile(str(path))
        col = r["x"]
        d = dataprof.column_to_dict(col)
        from_report = r.to_dict()["columns"][0]
        assert d == from_report

    def test_column_to_dict_is_exported_from_both_namespaces(self, tmp_path):
        """#514 settled this: the name is public, so __all__ declares it.

        It was reachable, documented in the stub and used in
        docs/guides/examples.md while __all__ omitted it — public in every way
        except the one that counts.
        """
        import dataprof.interop as interop

        path = tmp_path / "data.csv"
        path.write_text("x\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n")
        r = dataprof.profile(str(path))

        assert hasattr(dataprof, "column_to_dict")
        assert "column_to_dict" in dataprof.__all__
        assert "column_to_dict" in interop.__all__
        assert interop.column_to_dict(r["x"]) == dataprof.column_to_dict(r["x"])
