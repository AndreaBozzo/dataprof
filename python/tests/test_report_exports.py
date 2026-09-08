"""Report rendering, comparison, persistence, and dataframe or Arrow exports."""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path

import pytest
from conftest import CSV_FILE, REPO_ROOT
from jsonschema import Draft202012Validator

try:
    import dataprof
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop --features python",
        allow_module_level=True,
    )


class TestReportErgonomics:
    def test_profiles_yields_profile_objects(self, report):
        # column_profiles is a mapping (iterating it yields names); profiles is
        # the ordered list of ColumnProfile objects.
        names = list(report.column_profiles)
        profiles = report.profiles
        assert [c.name for c in profiles] == names
        for col in profiles:  # the natural loop must not raise
            assert isinstance(col.name, str)

    def test_to_html_matches_repr_html(self, report):
        assert report.to_html() == report._repr_html_()
        assert "<table" in report.to_html()

    def test_to_markdown_structure(self, report):
        md = report.to_markdown()
        lines = md.splitlines()
        header_idx = next(i for i, line in enumerate(lines) if line.startswith("| Column |"))
        assert lines[header_idx + 1].startswith("|---")
        data_rows = [line for line in lines[header_idx + 2 :] if line.startswith("|")]
        assert len(data_rows) == report.columns
        assert "**Source:**" in lines[0]

    def test_to_markdown_escapes_pipe(self):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({"a|b": [1, 2, 3], "c": [4, 5, 6]})
        md = dataprof.profile(df).to_markdown()
        assert "a\\|b" in md
        # No unescaped pipe leaks into a cell and breaks the table
        assert "| a|b |" not in md

    def test_to_markdown_keeps_newlines_inside_cells(self):
        md = dataprof.profile({"line\nbreak": [1, 2]}).to_markdown()
        assert "line\\nbreak" in md
        assert "| line\nbreak |" not in md

    def test_from_json_round_trip_quality_score(self, report):
        reloaded = dataprof.ProfileReport.from_json(report.to_json())
        assert reloaded.quality_score == report.quality_score

    def test_quality_score_weights_are_exposed_and_round_trip(self, report):
        expected = {
            "completeness": 0.25,
            "consistency": 0.20,
            "uniqueness": 0.15,
            "accuracy": 0.15,
            "timeliness": 0.10,
            "validity": 0.10,
            "precision": 0.05,
        }
        assert report.quality.score_weights == expected

        reloaded = dataprof.ProfileReport.from_json(report.to_json())
        assert reloaded.quality is not None
        assert reloaded.quality.score_weights == expected
        assert reloaded.quality.score_weights is reloaded.quality.score_weights

    def test_from_dict_round_trip_idempotent(self, report):
        reloaded = dataprof.ProfileReport.from_dict(report.to_dict())
        assert reloaded.to_dict() == report.to_dict()

    def test_low_sample_warning_round_trips_both_states(self, tmp_path):
        # Fewer than 10 rows raises the warning; it must survive to_dict/from_dict.
        small = tmp_path / "small.csv"
        small.write_text("a,b\n" + "\n".join(f"{i},{i * 2}" for i in range(3)) + "\n")
        r_small = dataprof.profile(small)
        assert r_small.quality is not None
        d_small = r_small.to_dict()
        assert d_small["quality"]["low_sample_warning"] is True
        assert r_small.low_sample_warning is True
        assert dataprof.ProfileReport.from_dict(d_small).low_sample_warning is True

        # With an adequate sample the flag is False — and still emitted, since a
        # non-optional bool should never require the reader to infer absence.
        big = tmp_path / "big.csv"
        big.write_text("a,b\n" + "\n".join(f"{i},{i * 2}" for i in range(50)) + "\n")
        d_big = dataprof.profile(big).to_dict()
        assert d_big["quality"]["low_sample_warning"] is False
        assert dataprof.ProfileReport.from_dict(d_big).low_sample_warning is False

    def test_reloaded_report_supports_exports(self, report):
        reloaded = dataprof.ProfileReport.from_json(report.to_json())
        # Rendering + mapping access work on the read-only view
        assert reloaded.to_markdown() == report.to_markdown()
        assert "name" in reloaded
        assert reloaded["name"].name == "name"
        assert reloaded.columns == report.columns

    def test_reloaded_report_to_dataframe(self, report):
        pytest.importorskip("pandas")
        reloaded = dataprof.ProfileReport.from_json(report.to_json())
        df = reloaded.to_dataframe()
        assert len(df) == report.columns

    def test_from_dict_legacy_report_defaults_ragged_row_count(self):
        # A report written before ragged_row_count existed omits the key; it
        # must read back as 0 (not None), matching the Rust serde default and
        # the additive-field compatibility promise.
        legacy = {"source": "old.csv", "columns": [], "execution": {"rows_processed": 5}}
        reloaded = dataprof.ProfileReport.from_dict(legacy)
        assert reloaded.ragged_row_count == 0

    def test_from_dict_rejects_malformed(self):
        with pytest.raises(ValueError, match="to_dict"):
            dataprof.ProfileReport.from_dict({"not": "a report"})

    @pytest.mark.parametrize(
        "bad",
        [
            {"source": "x", "columns": [], "execution": []},  # execution not a mapping
            {"source": "x", "columns": None, "execution": {}},  # columns not a list
            {"source": "x", "columns": [1, 2], "execution": {}},  # columns not mappings
        ],
    )
    def test_from_dict_rejects_wrong_types(self, bad):
        with pytest.raises(ValueError):
            dataprof.ProfileReport.from_dict(bad)

    def test_from_json_rejects_invalid_json(self):
        with pytest.raises(ValueError, match="invalid JSON"):
            dataprof.ProfileReport.from_json("{not valid json")

    def test_load_json_round_trip(self, report):
        # The 0.9.0 acceptance snippet: save → load → same quality_score.
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            report.save(path)
            loaded = dataprof.ProfileReport.load(path)
            assert loaded.quality_score == report.quality_score
            assert loaded.to_dict() == report.to_dict()
        finally:
            os.unlink(path)

    def test_load_accepts_pathlike(self, report):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = Path(f.name)
        try:
            report.save(str(path))
            loaded = dataprof.ProfileReport.load(path)
            assert loaded.quality_score == report.quality_score
        finally:
            os.unlink(path)

    @pytest.mark.parametrize("suffix", [".csv", ".parquet"])
    def test_load_profiles_only_formats_raise(self, suffix):
        with pytest.raises(ValueError, match=r"\.json"):
            dataprof.ProfileReport.load(f"report{suffix}")

    def test_load_unsupported_ext_raises(self):
        with pytest.raises(ValueError, match="Unsupported format"):
            dataprof.ProfileReport.load("report.html")

    def test_load_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            dataprof.ProfileReport.load("does_not_exist_12345.json")

    def test_from_dict_ignores_unknown_stat_keys(self, report):
        d = report.to_dict()
        d["columns"][0].setdefault("stats", {})["__evil__"] = "nope"
        reloaded = dataprof.ProfileReport.from_dict(d)
        col = reloaded[d["columns"][0]["name"]]
        assert not hasattr(col, "__evil__")

    # -- Report schema versioning (compatibility contract) --

    def test_to_dict_carries_schema_version(self, report):
        d = report.to_dict()
        assert d["schema_version"] == dataprof.REPORT_SCHEMA_VERSION
        assert json.loads(report.to_json())["schema_version"] == dataprof.REPORT_SCHEMA_VERSION

    def test_save_load_preserves_schema_version(self, report):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            report.save(path)
            with open(path, encoding="utf-8") as fh:
                on_disk = json.load(fh)
            assert on_disk["schema_version"] == dataprof.REPORT_SCHEMA_VERSION
            loaded = dataprof.ProfileReport.load(path)
            assert loaded.to_dict()["schema_version"] == dataprof.REPORT_SCHEMA_VERSION
        finally:
            os.unlink(path)

    def test_python_serialization_dialect_validates_against_published_schema(self, report):
        schema_path = REPO_ROOT / "docs" / "schema" / "profile-report.v1.schema.json"
        with schema_path.open(encoding="utf-8") as fh:
            schema = json.load(fh)
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema)

        validator.validate(report.to_dict())
        validator.validate(json.loads(report.to_json()))

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            report.save(path)
            with open(path, encoding="utf-8") as fh:
                validator.validate(json.load(fh))
        finally:
            os.unlink(path)

    def test_from_dict_accepts_legacy_unversioned_document(self, report):
        # A 0.9-era save() document has no schema_version field; it must
        # still load through the legacy compatibility path.
        legacy = report.to_dict()
        del legacy["schema_version"]
        reloaded = dataprof.ProfileReport.from_dict(legacy)
        assert reloaded.columns == report.columns

    def test_from_dict_ignores_additive_top_level_fields(self, report):
        # A newer writer on the same schema version may add fields; a
        # compatible reader must not break on them.
        d = report.to_dict()
        d["a_future_additive_field"] = {"anything": True}
        reloaded = dataprof.ProfileReport.from_dict(d)
        assert reloaded.columns == report.columns

    def test_from_dict_rejects_newer_schema_version(self, report):
        d = report.to_dict()
        d["schema_version"] = dataprof.REPORT_SCHEMA_VERSION + 1
        with pytest.raises(ValueError, match="Upgrade dataprof"):
            dataprof.ProfileReport.from_dict(d)

    @pytest.mark.parametrize("bad", ["1", 1.0, True, {}, None])
    def test_from_dict_rejects_non_integer_schema_version(self, report, bad):
        d = report.to_dict()
        d["schema_version"] = bad
        with pytest.raises(ValueError, match="schema_version"):
            dataprof.ProfileReport.from_dict(d)

    def test_newer_schema_version_fails_before_partial_decoding(self):
        # The version gate must run before structural validation: an
        # incompatible document is rejected outright, not half-parsed.
        with pytest.raises(ValueError, match="Upgrade dataprof"):
            dataprof.ProfileReport.from_dict({"schema_version": dataprof.REPORT_SCHEMA_VERSION + 1})

    def test_compare_identical_is_zero(self, report):
        reloaded = dataprof.ProfileReport.from_json(report.to_json())
        delta = report.compare(reloaded)
        assert delta["quality_score"]["abs"] == 0
        assert delta["schema"]["added"] == []
        assert delta["schema"]["removed"] == []
        assert set(delta["schema"]["common"]) == set(report.column_profiles)
        for col in delta["columns"].values():
            assert col["null_pct_delta"] == 0

    def test_compare_schema_diff(self):
        pd = pytest.importorskip("pandas")
        a = dataprof.profile(pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}))
        b = dataprof.profile(pd.DataFrame({"x": [1, 2, 3], "z": [7, 8, 9]}))
        delta = a.compare(b)
        assert delta["schema"]["added"] == ["z"]
        assert delta["schema"]["removed"] == ["y"]
        assert delta["schema"]["common"] == ["x"]
        assert "quality_score" in delta
        assert set(delta["dimensions"]) == {
            "completeness",
            "consistency",
            "uniqueness",
            "accuracy",
            "timeliness",
            "validity",
            "precision",
        }


class TestToDataframeEnriched:
    def test_enriched_columns(self):
        pytest.importorskip("pandas")
        r = dataprof.profile(CSV_FILE)
        df = r.to_dataframe()
        assert len(df) == r.columns
        expected_cols = {
            "name",
            "data_type",
            "total_count",
            "null_count",
            "null_percentage",
            "unique_count",
            "uniqueness_ratio",
            "min",
            "max",
            "mean",
            "std_dev",
            "variance",
            "median",
            "mode",
            "skewness",
            "kurtosis",
            "coefficient_of_variation",
            "q1",
            "q2",
            "q3",
            "iqr",
            "is_approximate",
            "min_length",
            "max_length",
            "avg_length",
            "top_pattern",
            "top_pattern_pct",
        }
        assert expected_cols.issubset(set(df.columns)), (
            f"Missing columns: {expected_cols - set(df.columns)}"
        )

    def test_values_are_rounded(self):
        pd = pytest.importorskip("pandas")
        r = dataprof.profile(CSV_FILE)
        df = r.to_dataframe()
        for _, row in df.iterrows():
            np_val = row["null_percentage"]
            if np_val is not None and pd.notna(np_val):
                assert np_val == round(np_val, 2)


class TestToPolars:
    def test_to_polars(self):
        pytest.importorskip("polars")
        r = dataprof.profile(CSV_FILE)
        df = r.to_polars()
        assert len(df) == r.columns
        assert "name" in df.columns
        assert "mean" in df.columns


class TestToArrow:
    def test_to_arrow(self):
        pa = pytest.importorskip("pyarrow")
        r = dataprof.profile(CSV_FILE)
        table = r.to_arrow()
        assert isinstance(table, pa.Table)
        assert table.num_rows == r.columns
        assert "name" in table.column_names


class TestDescribe:
    def test_describe_returns_dataframe(self):
        pd = pytest.importorskip("pandas")
        r = dataprof.profile(CSV_FILE)
        desc = r.describe()
        assert isinstance(desc, pd.DataFrame)
        assert "count" in desc.index
        assert "null%" in desc.index
        assert "mean" in desc.index

    def test_describe_without_pandas(self):
        r = dataprof.profile(CSV_FILE)
        # describe() falls back to dict-of-dicts if pandas is missing,
        # but since pandas is installed in test env, just verify it works
        desc = r.describe()
        assert desc is not None

    def test_describe_50pct_falls_back_to_median(self):
        # Small samples compute a median without full quartiles; describe()
        # must not show 50% as missing while the median exists.
        r = dataprof.profile({"age": [29, 31, 42]})
        col = r["age"]
        assert col.median is not None
        desc = r.describe()
        try:
            fifty = desc["age"]["50%"]
        except TypeError:
            fifty = desc.loc["50%", "age"]
        assert fifty == pytest.approx(col.median, abs=0.01)


class TestSaveFormats:
    def test_save_csv(self, report):
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name
        try:
            result = report.save(path)
            assert result is report
            with open(path) as f:
                content = f.read()
            assert "name" in content
            assert "data_type" in content
        finally:
            os.unlink(path)

    def test_save_accepts_pathlike(self, report, tmp_path):
        path = tmp_path / "report.json"
        result = report.save(path)
        assert result is report
        assert dataprof.ProfileReport.load(path).to_dict() == report.to_dict()

    def test_save_empty_report_csv_creates_artifact(self, tmp_path):
        report = dataprof.profile({})
        path = tmp_path / "empty.csv"

        assert report.save(path) is report
        assert path.exists()
        assert path.read_bytes() == b""

    def test_save_and_load_extensions_are_case_insensitive(self, report, tmp_path):
        path = tmp_path / "REPORT.JSON"

        assert report.save(path) is report
        assert dataprof.ProfileReport.load(path).to_dict() == report.to_dict()

    def test_save_parquet(self, report):
        pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        try:
            result = report.save(path)
            assert result is report
            table = pq.read_table(path)
            assert table.num_rows == report.columns
        finally:
            os.unlink(path)


def test_to_json_is_byte_stable_for_unchanged_input(tmp_path):
    """Profiling one unchanged file twice must produce identical JSON bytes
    once the three timing-dependent execution fields are masked (gh #546).

    The comparison is deliberately on the raw strings. Parsing to dicts first,
    or sorting keys while comparing, erases the very thing at issue: mapping
    fields backed by a Rust ``HashMap`` came out in a different order on every
    run, and a dict comparison cannot see that.
    """
    p = tmp_path / "stable.csv"
    p.write_text("name,age,salary\nAlice,30,50000\nBob,25,60000\nCarol,35,70000\n")

    # Only the measured-duration fields may differ between two runs of the same
    # input. Everything else, values included, is expected to be reproducible.
    volatile = re.compile(r'"(scan_time_ms|throughput_rows_sec|memory_peak_mb)": [^,\n}]+')

    assert volatile.search(dataprof.profile(str(p)).to_json()), (
        "the mask matched nothing; the execution field names changed and this "
        "test would compare unmasked timings"
    )

    # Five runs, not two: HashMap ordering is randomized per instance, so a
    # single pair can coincide by luck.
    rendered = {
        volatile.sub(r'"\1": <masked>', dataprof.profile(str(p)).to_json()) for _ in range(5)
    }

    assert len(rendered) == 1, f"to_json() rendered {len(rendered)} different documents"
