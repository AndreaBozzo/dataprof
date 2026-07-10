"""Minimal regression test suite for the dataprof Python API.

Run after building the extension:
    maturin develop --features python
    pytest python/tests/test_python_api.py -v
"""

from __future__ import annotations

import builtins
import io
import json
import os
import tempfile
from pathlib import Path

import pytest

# Resolve fixture paths relative to repo root
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
FIXTURES = REPO_ROOT / "examples" / "test_datasets"
CSV_FILE = str(FIXTURES / "small_comma.csv")
CSV_LARGE_FILE = str(FIXTURES / "large_dataset.csv")
JSON_FILE = str(FIXTURES / "users.json")
JSONL_FILE = str(FIXTURES / "logs.jsonl")
PARQUET_FILE = str(REPO_ROOT / "examples" / "test_data" / "simple.parquet")
SEMICOLON_FILE = str(FIXTURES / "employees_semicolon.csv")

# ── Import guard ──

try:
    import dataprof
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop --features python",
        allow_module_level=True,
    )


# ─────────────────────────────────────────────────
#  1. Core profile() dispatch
# ─────────────────────────────────────────────────


class TestProfileFile:
    def test_csv(self):
        r = dataprof.profile(CSV_FILE)
        assert r.rows > 0
        assert r.columns > 0
        assert len(r.column_profiles) == r.columns

    def test_json(self):
        if not os.path.exists(JSON_FILE):
            pytest.skip("fixture missing")
        r = dataprof.profile(JSON_FILE)
        assert r.rows > 0

    def test_jsonl(self):
        if not os.path.exists(JSONL_FILE):
            pytest.skip("fixture missing")
        r = dataprof.profile(JSONL_FILE)
        assert r.rows > 0

    def test_parquet(self):
        if not os.path.exists(PARQUET_FILE):
            pytest.skip("fixture missing")
        r = dataprof.profile(PARQUET_FILE)
        assert r.rows > 0

    def test_parquet_nulls_excluded_from_numeric_stats(self, tmp_path):
        """A null slot carries a physical value; it must not enter the statistics."""
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")
        path = tmp_path / "nullable.parquet"
        pq.write_table(pa.table({"x": pa.array([100.0, None, 1.0, None, 2.0])}), path)

        col = dataprof.profile(str(path))["x"]
        assert col.null_count == 2
        assert col.unique_count == 3
        assert col.min == 1.0  # not 0.0, the buffer's value under the null
        assert col.mean == pytest.approx(34.333333, rel=1e-6)  # (100 + 1 + 2) / 3

    def test_path_object(self):
        r = dataprof.profile(Path(CSV_FILE))
        assert r.rows > 0

    def test_profile_file_matches_profile_for_paths(self):
        via_profile = dataprof.profile(CSV_FILE, max_rows=3)
        via_profile_file = dataprof.profile_file(CSV_FILE, max_rows=3)

        assert via_profile_file.rows == via_profile.rows
        assert via_profile_file.columns == via_profile.columns
        assert via_profile_file.to_dict()["columns"] == via_profile.to_dict()["columns"]

    def test_missing_file_raises_file_not_found(self):
        missing = str(FIXTURES / "does_not_exist.csv")
        with pytest.raises(FileNotFoundError, match="does_not_exist.csv") as excinfo:
            dataprof.profile(missing)
        assert excinfo.value.filename == missing

    def test_unsupported_type_raises(self):
        with pytest.raises(TypeError, match="Unsupported source type"):
            dataprof.profile(42)


class TestProfileDataFrame:
    def test_pandas(self):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        r = dataprof.profile(df)
        assert r.rows == 3
        assert r.columns == 2


class TestInterop:
    def test_analyze_file_path_object(self):
        import dataprof.interop as interop

        report = interop.analyze_file(Path(CSV_FILE))
        assert report.rows_processed > 0
        assert report.columns_detected > 0

    def test_analyze_csv_to_arrow_path_object(self):
        import dataprof.interop as interop

        batch = interop.analyze_csv_to_arrow(Path(CSV_FILE))
        assert batch.num_rows > 0
        assert batch.num_columns > 0

    def test_analyze_parquet_to_arrow_path_object(self):
        import dataprof.interop as interop

        if not os.path.exists(PARQUET_FILE):
            pytest.skip("fixture missing")
        batch = interop.analyze_parquet_to_arrow(Path(PARQUET_FILE))
        assert batch.num_rows > 0
        assert batch.num_columns > 0

    def test_polars(self):
        pl = pytest.importorskip("polars")
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        r = dataprof.profile(df)
        assert r.rows == 3
        assert r.columns == 2


class TestProfileAdHocInputs:
    """Ad-hoc inputs are part of the base-wheel contract: no pandas, ever.

    These paths must never call `_require_pandas`, so none of these tests may
    `importorskip("pandas")` -- doing so would let a regression to a pandas-only
    implementation pass unnoticed in the environments that matter.
    """

    def test_dict_input(self):
        r = dataprof.profile({"a": [1, 2, 3]})
        assert r.rows == 3
        assert r.columns == 1
        assert r["a"].name == "a"

    def test_list_of_dicts_input(self):
        r = dataprof.profile([{"a": 1}, {"a": 2}])
        assert r.rows == 2
        assert r.columns == 1
        assert r["a"].name == "a"

    def test_bytesio_csv_input(self):
        r = dataprof.profile(io.BytesIO(b"a,b\n1,2\n"), format="csv")
        assert r.rows == 1
        assert r.columns == 2

    def test_bytes_input_requires_format(self):
        with pytest.raises(ValueError, match="dataprof.asyncio.profile_bytes"):
            dataprof.profile(b"a,b\n1,2\n")

    def test_ad_hoc_inputs_do_not_import_pandas(self, monkeypatch):
        """The base wheel has no dependencies; profiling must not reach for one."""

        def explode(feature):
            raise AssertionError(f"ad-hoc path called _require_pandas({feature!r})")

        monkeypatch.setattr(dataprof, "_require_pandas", explode)
        dataprof.profile({"a": [1, 2]})
        dataprof.profile([{"a": 1}, {"a": 2}])
        dataprof.profile(b"a\n1\n2\n", format="csv")
        dataprof.profile(b'{"a": [1, 2]}', format="json")
        dataprof.profile(b'{"a": 1}\n{"a": 2}\n', format="jsonl")

    def test_dict_preserves_column_order(self):
        """Column order follows the input, so reports are reproducible."""
        r = dataprof.profile({"z": [1], "a": [2], "m": [3]})
        assert list(r.column_profiles) == ["z", "a", "m"]

    def test_dict_does_not_upcast_integers_with_nulls(self):
        """Unlike a pandas round-trip, a null does not turn an int column float."""
        r = dataprof.profile({"age": [31, 42, None, 29]})
        assert r["age"].data_type == "integer"
        assert r["age"].null_count == 1
        assert r["age"].unique_count == 3

    def test_dict_treats_null_like_tokens_as_missing(self):
        """`""`, `"null"` and NaN are missing, matching the CSV and Arrow paths."""
        r = dataprof.profile({"x": ["a", "", None, "null", float("nan")]})
        assert r["x"].null_count == 4
        assert r["x"].unique_count == 1

    def test_dict_rejects_ragged_columns(self):
        with pytest.raises(ValueError, match="differing lengths"):
            dataprof.profile({"a": [1, 2], "b": [1]})

    def test_dict_rejects_scalar_column(self):
        with pytest.raises(TypeError, match="must be a list or tuple"):
            dataprof.profile({"a": 1})

    def test_raw_extension_rejects_ragged_columns(self):
        """`profile_columns` is importable directly, so it validates its own input.

        `dp.profile()` screens ragged dicts first, but the extension symbol is
        reachable without it, and slicing past a short column would panic across
        the FFI boundary instead of raising.
        """
        from dataprof import _dataprof

        with pytest.raises(ValueError, match="same number of cells"):
            _dataprof.profile_columns([("a", ["1", "2"]), ("b", ["1"])], "x", None, None)

        # A short *first* column must raise too, not silently truncate the rest.
        with pytest.raises(ValueError, match="same number of cells"):
            _dataprof.profile_columns([("a", ["1"]), ("b", ["1", "2"])], "x", None, None)

    def test_list_of_dicts_fills_missing_keys_with_nulls(self):
        r = dataprof.profile([{"a": 1}, {"b": 2}])
        assert list(r.column_profiles) == ["a", "b"]
        assert r["a"].null_count == 1
        assert r["b"].null_count == 1

    def test_csv_bytes_treat_empty_field_as_null(self):
        r = dataprof.profile(b"a,b\n1,\n2,x\n", format="csv")
        assert r["b"].null_count == 1

    def test_csv_bytes_honour_delimiter(self):
        r = dataprof.profile(b"a;b\n1;2\n", format="csv", csv_delimiter=";")
        assert r.columns == 2

    def test_csv_bytes_reject_ragged_rows(self):
        with pytest.raises(ValueError, match="row 2 has 3 fields"):
            dataprof.profile(b"a,b\n1,2,3\n", format="csv")

    def test_json_bytes_accept_columns_or_records(self):
        by_column = dataprof.profile(b'{"a": [1, 2]}', format="json")
        by_record = dataprof.profile(b'[{"a": 1}, {"a": 2}]', format="json")
        assert by_column.rows == by_record.rows == 2

    def test_jsonl_bytes_input(self):
        r = dataprof.profile(b'{"a": 1}\n{"a": 2}\n', format="jsonl")
        assert r.rows == 2
        assert r["a"].data_type == "integer"

    def test_ad_hoc_report_matches_pandas_report(self):
        """The native path must not quietly disagree with the DataFrame path."""
        pd = pytest.importorskip("pandas")
        data = {"city": ["Rome", "Milan", "Rome", ""], "score": [1.5, 2.0, 3.25, 4.0]}

        native = dataprof.profile(data)
        via_pandas = dataprof.profile(pd.DataFrame(data))

        assert native.rows == via_pandas.rows
        assert native.quality_score == via_pandas.quality_score
        for col in ("city", "score"):
            assert native[col].data_type == via_pandas[col].data_type
            assert native[col].null_count == via_pandas[col].null_count
            assert native[col].unique_count == via_pandas[col].unique_count


# ─────────────────────────────────────────────────
#  2. ProfileReport accessors & exports
# ─────────────────────────────────────────────────


class TestProfileReport:
    @pytest.fixture()
    def report(self):
        return dataprof.profile(CSV_FILE)

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
        assert "dimensions=" in r

    @pytest.mark.parametrize(
        ("attr", "dimension", "key", "default"),
        [
            ("missing_values_ratio", "completeness", "missing_values_ratio", 0.0),
            ("complete_records_ratio", "completeness", "complete_records_ratio", 100.0),
            ("null_columns", "completeness", "null_columns", []),
            ("data_type_consistency", "consistency", "data_type_consistency", 100.0),
            ("format_violations", "consistency", "format_violations", 0),
            ("encoding_issues", "consistency", "encoding_issues", 0),
            ("duplicate_rows", "uniqueness", "duplicate_rows", 0),
            ("key_uniqueness", "uniqueness", "key_uniqueness", 100.0),
            ("high_cardinality_warning", "uniqueness", "high_cardinality_warning", False),
            ("outlier_ratio", "accuracy", "outlier_ratio", 0.0),
            ("range_violations", "accuracy", "range_violations", 0),
            ("negative_values_in_positive", "accuracy", "negative_values_in_positive", 0),
            ("future_dates_count", "timeliness", "future_dates_count", 0),
            ("stale_data_ratio", "timeliness", "stale_data_ratio", 0.0),
            ("temporal_violations", "timeliness", "temporal_violations", 0),
        ],
    )
    def test_quality_flat_accessors_warn_and_match_nested(
        self, report, attr, dimension, key, default
    ):
        q = report.quality
        assert q is not None
        nested = getattr(q, dimension)
        assert nested is not None

        with pytest.warns(DeprecationWarning, match=f"DataQualityMetrics\\.{attr}"):
            value = getattr(q, attr)

        assert value == nested.get(key, default)

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

    def test_skipped_flat_accessor_warns_and_returns_default(self):
        report = dataprof.profile(CSV_FILE, quality_dimensions=["completeness"])
        q = report.quality
        assert q is not None
        assert q.uniqueness is None

        with pytest.warns(DeprecationWarning, match="DataQualityMetrics\\.key_uniqueness"):
            assert q.key_uniqueness == 100.0

    def test_reloaded_quality_flat_accessors_warn(self, report):
        reloaded = dataprof.ProfileReport.from_json(report.to_json())
        q = reloaded.quality
        assert q is not None
        assert q.completeness is not None

        with pytest.warns(DeprecationWarning, match="DataQualityMetrics\\.missing_values_ratio"):
            assert q.missing_values_ratio == q.completeness["missing_values_ratio"]

    def test_to_dict(self, report):
        d = report.to_dict()
        assert "source" in d
        assert "columns" in d
        assert "execution" in d
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
            report.save("/tmp/test.html")

    def test_repr(self, report):
        r = repr(report)
        assert "ProfileReport" in r
        assert "rows=" in r

    def test_repr_html(self, report):
        html = report._repr_html_()
        assert "<table" in html
        assert "ProfileReport" in html


# ─────────────────────────────────────────────────
#  2b. Report ergonomics: to_html / to_markdown / round-trip / compare
# ─────────────────────────────────────────────────


class TestReportErgonomics:
    @pytest.fixture()
    def report(self):
        return dataprof.profile(CSV_FILE)

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

    def test_from_json_round_trip_quality_score(self, report):
        reloaded = dataprof.ProfileReport.from_json(report.to_json())
        assert reloaded.quality_score == report.quality_score

    def test_from_dict_round_trip_idempotent(self, report):
        reloaded = dataprof.ProfileReport.from_dict(report.to_dict())
        assert reloaded.to_dict() == report.to_dict()

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
        }


# ─────────────────────────────────────────────────
#  2c. Agent output contract: to_llm_context
# ─────────────────────────────────────────────────


class TestToLlmContext:
    @pytest.fixture()
    def report(self):
        return dataprof.profile(CSV_FILE)

    @pytest.fixture()
    def messy(self, tmp_path):
        """A dataset with a null-heavy column, a constant column, and a pattern."""
        path = tmp_path / "messy.csv"
        rows = ["email,amount,note,const"]
        for i in range(100):
            amount = "" if i % 4 == 0 else str(i)
            note = "" if i % 5 else "x"
            rows.append(f"u{i}@example.com,{amount},{note},K")
        path.write_text("\n".join(rows) + "\n", encoding="utf-8")
        return dataprof.profile(str(path))

    def test_header_reports_shape(self, report):
        out = report.to_llm_context()
        assert out.startswith("dataset: ")
        assert f"columns: {report.columns}" in out
        assert f"rows: {report.rows:,}" in out

    def test_derives_quality_flags(self, messy):
        out = messy.to_llm_context()
        assert "note: null-heavy" in out
        assert "amount: null-heavy" in out
        assert "const: constant (1 distinct value)" in out

    def test_flags_ranked_by_severity(self, messy):
        out = messy.to_llm_context()
        # note (80% null) outranks amount (25% null)
        assert out.index("note: null-heavy") < out.index("amount: null-heavy")

    def test_null_heavy_suppresses_redundant_constant_flag(self, messy):
        # `note` is 80% null with one distinct value; it must not be double-flagged
        assert "note: constant" not in messy.to_llm_context()

    def test_reports_detected_pattern_names(self, messy):
        assert "email: Email" in messy.to_llm_context()

    @staticmethod
    def _header_tokens(report):
        """Cost of the always-emitted header, which is the effective budget floor."""
        return dataprof._estimate_tokens(report.to_llm_context(max_tokens=1))

    @pytest.mark.parametrize("over_floor", [0, 5, 20, 60, 150, 400])
    def test_stays_within_budget(self, messy, over_floor):
        budget = self._header_tokens(messy) + over_floor
        out = messy.to_llm_context(max_tokens=budget)
        assert dataprof._estimate_tokens(out) <= budget

    def test_header_always_emitted_below_budget(self, messy):
        # Documented floor: identity survives even an unsatisfiable budget
        out = messy.to_llm_context(max_tokens=1)
        assert out.startswith("dataset: ")
        assert "\n\n" not in out  # header only, no sections

    def test_truncation_emits_more_tail(self, messy):
        out = messy.to_llm_context(max_tokens=self._header_tokens(messy) + 20)
        assert "... +" in out and " more" in out

    def test_no_section_header_without_items(self, messy):
        """A section must never be a bare header followed by '... +N more'."""
        budget = self._header_tokens(messy) + 12
        for block in messy.to_llm_context(max_tokens=budget).split("\n\n")[1:]:
            lines = block.splitlines()
            assert not (len(lines) > 1 and lines[1].startswith("... +"))

    def test_never_shows_patterns_without_flags(self, messy):
        """A starved high-priority section must suppress lower-priority ones.

        `messy` has flags. If a budget renders `patterns:` but no `flags`, a
        reader would infer the dataset is clean -- a false negative.
        """
        floor = self._header_tokens(messy)
        for budget in range(floor, floor + 120):
            out = messy.to_llm_context(max_tokens=budget)
            if "patterns:" in out:
                assert "flags (" in out, f"patterns without flags at {budget=}"

    def test_clean_dataset_still_shows_patterns(self, tmp_path):
        """The suppression rule must not hide patterns when there are no flags."""
        path = tmp_path / "clean.csv"
        rows = ["email,n"] + [f"u{i}@example.com,{i}" for i in range(100)]
        path.write_text("\n".join(rows) + "\n", encoding="utf-8")
        out = dataprof.profile(str(path)).to_llm_context()
        assert "flags (" not in out
        assert "patterns:" in out

    def test_deterministic(self, messy):
        assert len({messy.to_llm_context(max_tokens=200) for _ in range(5)}) == 1

    def test_redacts_raw_values_by_default(self, tmp_path):
        path = tmp_path / "secret.csv"
        path.write_text("salary\n999777333\n123\n456\n", encoding="utf-8")
        out = dataprof.profile(str(path)).to_llm_context()
        assert "999777333" not in out

    def test_include_samples_surfaces_extremes(self, tmp_path):
        path = tmp_path / "readings.csv"
        path.write_text("reading\n999.75\n123.5\n456.25\n", encoding="utf-8")
        out = dataprof.profile(str(path)).to_llm_context(include_samples=True)
        assert "999.75" in out

    def test_include_samples_withholds_sensitive_pattern_extremes(self, tmp_path):
        path = tmp_path / "ssn.csv"
        path.write_text(
            "ssn\n123456789\n124456789\n125456789\n126456789\n127456789\n",
            encoding="utf-8",
        )
        out = dataprof.profile(str(path)).to_llm_context(include_samples=True)
        assert "SSN (US)" in out
        assert "123456789" not in out
        assert "127456789" not in out

    @pytest.fixture()
    def cards(self, tmp_path):
        """A sensitive integer column beside an innocuous one.

        Both infer as `integer` and so carry extrema; only `card` matches a
        sensitive pattern. The pair distinguishes redaction from over-redaction.
        """
        path = tmp_path / "cards.csv"
        rows = ["card,qty"] + [f"411111111111{1000 + i},{i}" for i in range(10)]
        path.write_text("\n".join(rows) + "\n", encoding="utf-8")
        return str(path)

    def test_include_samples_withholds_extremes_when_patterns_not_scanned(self, cards):
        """Redaction must fail closed when pattern detection never ran.

        A `metrics=` selection without the "patterns" pack leaves every column
        with `patterns is None`. Reading that as "nothing sensitive found" would
        echo raw card numbers into an agent's context on an unrelated opt-out.
        """
        report = dataprof.profile(cards, metrics=["schema", "statistics", "quality"])
        assert report["card"].patterns is None  # detection skipped, not "no match"

        out = report.to_llm_context(include_samples=True)
        assert "4111111111111000" not in out
        assert "4111111111111009" not in out

    def test_include_samples_withholds_extremes_when_reload_lacks_evidence(self, cards, tmp_path):
        """A payload saved without pattern evidence redacts once reloaded."""
        saved = tmp_path / "report.json"
        dataprof.profile(cards, metrics=["schema", "statistics"]).save(str(saved))

        reloaded = dataprof.ProfileReport.load(str(saved))
        assert reloaded["card"].patterns is None  # the key was never written

        out = reloaded.to_llm_context(include_samples=True)
        assert "4111111111111000" not in out

    def test_reload_preserves_pattern_evidence(self, cards, tmp_path):
        """`save()`/`load()` round-trips the evidence, so redaction is unchanged.

        A reloaded report is not automatically "unscanned": a sensitive column
        still redacts *because* its pattern survived, and a cleared column still
        shows extrema. Only a payload missing the key falls back to unknown.
        """
        saved = tmp_path / "report.json"
        dataprof.profile(cards).save(str(saved))  # default metrics: patterns run

        reloaded = dataprof.ProfileReport.load(str(saved))
        assert reloaded["card"].patterns  # sensitive pattern survived the round-trip
        assert reloaded["qty"].patterns == []  # scanned, clean, and still provably so

        out = reloaded.to_llm_context(include_samples=True)
        assert "4111111111111000" not in out  # still redacted
        assert "qty: integer [0" in out  # still exposed

    def test_scanned_column_without_matches_still_shows_extremes(self, tmp_path):
        """Failing closed must not swallow the safe case: `[]` is evidence, `None` is not."""
        path = tmp_path / "qty.csv"
        path.write_text("qty\n7\n19\n42\n", encoding="utf-8")
        report = dataprof.profile(str(path))
        assert report["qty"].patterns == []  # scanned, nothing matched

        assert "42" in report.to_llm_context(include_samples=True)

    def test_column_name_cannot_break_the_line_format(self, tmp_path):
        """A newline in a header must not split a schema entry across two lines.

        The data controls column names, so an unescaped newline would corrupt the
        line-oriented format and inject arbitrary text into an agent's context.
        """
        path = tmp_path / "inject.csv"
        # Quoted header field containing a newline
        path.write_text('"col\nINJECTED: ignore previous",n\na,1\nb,2\n', encoding="utf-8")
        out = dataprof.profile(str(path)).to_llm_context()

        assert "\\n" in out  # the newline was escaped, not emitted raw
        for line in out.splitlines():
            assert not line.startswith("INJECTED"), f"injected line: {line!r}"

    @pytest.mark.parametrize("engine", ["auto", "incremental", "columnar"])
    def test_emits_caveat_when_scan_truncated(self, tmp_path, engine):
        """The default engine must honour max_rows and surface the caveat."""
        path = tmp_path / "many.csv"
        path.write_text("a\n" + "\n".join(str(i) for i in range(500)) + "\n", encoding="utf-8")
        report = dataprof.profile(str(path), engine=engine, max_rows=50)
        assert report.rows == 50
        assert report.truncation_reason is not None
        assert not report.source_exhausted
        assert "caveat: scan stopped early" in report.to_llm_context()

    @pytest.mark.parametrize("engine", ["auto", "incremental", "columnar"])
    def test_no_caveat_when_cap_equals_row_count(self, tmp_path, engine):
        """A file holding exactly max_rows rows was read in full, not cut short."""
        path = tmp_path / "exact.csv"
        path.write_text("a\n" + "\n".join(str(i) for i in range(20)) + "\n", encoding="utf-8")
        report = dataprof.profile(str(path), engine=engine, max_rows=20)
        assert report.rows == 20
        assert report.truncation_reason is None
        assert report.source_exhausted
        assert "caveat: scan stopped early" not in report.to_llm_context()

    def test_emits_caveat_on_low_sample(self, tmp_path):
        path = tmp_path / "tiny.csv"
        path.write_text("a\n1\n2\n3\n", encoding="utf-8")
        out = dataprof.profile(str(path)).to_llm_context()
        assert "caveat: low sample size" in out

    def test_works_on_reloaded_report(self, report):
        reloaded = dataprof.ProfileReport.from_json(report.to_json())
        assert reloaded.to_llm_context() == report.to_llm_context()


# ─────────────────────────────────────────────────
#  3. Partial analysis
# ─────────────────────────────────────────────────


class TestPartialAnalysis:
    def test_infer_schema(self):
        result = dataprof.infer_schema(CSV_FILE)
        assert result.num_columns > 0
        assert len(result.column_names) == result.num_columns
        assert result.rows_sampled > 0

    def test_infer_schema_path_object(self):
        result = dataprof.infer_schema(Path(CSV_FILE))
        assert result.num_columns > 0

    def test_quick_row_count(self):
        result = dataprof.quick_row_count(CSV_FILE)
        assert result.count > 0
        assert isinstance(result.exact, bool)
        assert isinstance(result.method, str)

    def test_quick_row_count_path_object(self):
        result = dataprof.quick_row_count(Path(CSV_FILE))
        assert result.count > 0

    def test_analyze_structure_path_object(self):
        result = dataprof.analyze_structure(Path(CSV_FILE))
        assert result.source.endswith("small_comma.csv")
        assert result.format == "csv"
        assert result.row_count.count > 0
        assert result.rows_sampled > 0
        assert result.source_exhausted is True
        assert result.truncated is False
        assert result.delimiter == ","
        assert result.columns
        first = result.columns[0]
        assert first.name
        assert first.data_type in {
            "integer",
            "float",
            "string",
            "identifier",
            "date",
            "boolean",
        }
        assert first.provenance == "sample"

    def test_analyze_structure_default_max_rows(self, tmp_path):
        path = tmp_path / "many.csv"
        path.write_text("x\n" + "\n".join(str(i) for i in range(1001)) + "\n")

        result = dataprof.analyze_structure(path)
        assert result.row_count.count == 1001
        assert result.rows_sampled == 1000
        assert result.source_exhausted is False
        assert result.truncated is True
        assert result.truncation_reason == "max_rows(1000)"
        assert "structure_sample_truncated" in result.warnings

    def test_analyze_structure_none_max_rows_uses_default(self, tmp_path):
        path = tmp_path / "many.csv"
        path.write_text("x\n" + "\n".join(str(i) for i in range(1001)) + "\n")

        result = dataprof.analyze_structure(path, max_rows=None)
        assert result.rows_sampled == 1000
        assert result.truncated is True
        assert result.truncation_reason == "max_rows(1000)"

    def test_analyze_structure_column_summaries(self, tmp_path):
        path = tmp_path / "summary.csv"
        path.write_text("name,age\nAlice,30\nBob,\nCharlie,40\n")

        result = dataprof.analyze_structure(path, max_rows=10)
        age = next(col for col in result.columns if col.name == "age")
        assert age.data_type == "integer"
        assert age.total_count == 3
        assert age.null_count == 1
        assert age.null_ratio is not None
        assert abs(age.null_ratio - (1 / 3)) < 0.001
        assert age.unique_count is not None
        assert age.uniqueness_ratio is not None
        assert age.distinct_count_approximate is False

    def test_missing_file_raises_file_not_found(self):
        missing = str(FIXTURES / "does_not_exist.csv")
        with pytest.raises(FileNotFoundError, match="does_not_exist.csv") as excinfo:
            dataprof.infer_schema(missing)
        assert excinfo.value.filename == missing
        with pytest.raises(FileNotFoundError, match="does_not_exist.csv") as excinfo:
            dataprof.quick_row_count(missing)
        assert excinfo.value.filename == missing
        with pytest.raises(FileNotFoundError, match="does_not_exist.csv") as excinfo:
            dataprof.analyze_structure(missing)
        assert excinfo.value.filename == missing


# ─────────────────────────────────────────────────
#  4. Configuration
# ─────────────────────────────────────────────────


class TestProfilerConfig:
    def test_basic_config(self):
        cfg = dataprof.ProfilerConfig()
        assert cfg.engine == "auto"
        assert cfg.chunk_size is None
        assert cfg.max_rows is None

    def test_engine_override(self):
        cfg = dataprof.ProfilerConfig(engine="incremental")
        assert cfg.engine == "incremental"

    def test_semantic_hint_config(self):
        cfg = dataprof.ProfilerConfig(
            positive_columns=["pressure"],
            identifier_columns=["order_id", "customer_id"],
        )
        assert cfg.positive_columns == ["pressure"]
        assert cfg.identifier_columns == ["order_id", "customer_id"]

    def test_max_rows(self):
        if not os.path.exists(CSV_LARGE_FILE):
            pytest.skip("fixture missing")
        r = dataprof.profile(CSV_LARGE_FILE, max_rows=100, engine="incremental")
        # Stop condition is evaluated per-chunk, so rows may exceed max_rows
        # by up to one chunk. The key assertion: fewer rows than the full file.
        assert r.rows < 10000
        assert not r.source_exhausted

    def test_csv_delimiter(self):
        if not os.path.exists(SEMICOLON_FILE):
            pytest.skip("fixture missing")
        r = dataprof.profile(SEMICOLON_FILE, csv_delimiter=";")
        assert r.rows > 0
        assert r.columns > 1

    def test_format_override(self):
        r = dataprof.profile(CSV_FILE, format="csv")
        assert r.rows > 0

    def test_invalid_engine_raises(self):
        with pytest.raises(ValueError):
            dataprof.ProfilerConfig(engine="invalid")

    def test_max_rows_and_stop_condition_conflict(self):
        with pytest.raises(ValueError, match="Cannot specify both"):
            dataprof.ProfilerConfig(
                max_rows=100,
                stop_condition=dataprof.StopCondition.max_rows(200),
            )


# ─────────────────────────────────────────────────
#  5. Sampling strategies
# ─────────────────────────────────────────────────


class TestSamplingStrategy:
    def test_none(self):
        s = dataprof.SamplingStrategy.none()
        assert s is not None

    def test_random(self):
        s = dataprof.SamplingStrategy.random(100)
        assert s is not None

    def test_reservoir(self):
        s = dataprof.SamplingStrategy.reservoir(100)
        assert s is not None

    def test_systematic(self):
        s = dataprof.SamplingStrategy.systematic(10)
        assert s is not None

    def test_adaptive(self):
        s = dataprof.SamplingStrategy.adaptive()
        assert s is not None

    def test_profile_with_sampling(self):
        r = dataprof.profile(CSV_FILE, sampling=dataprof.SamplingStrategy.random(2))
        assert r.rows > 0


# ─────────────────────────────────────────────────
#  6. Stop conditions
# ─────────────────────────────────────────────────


class TestStopCondition:
    def test_max_rows(self):
        sc = dataprof.StopCondition.max_rows(100)
        assert sc is not None

    def test_max_bytes(self):
        sc = dataprof.StopCondition.max_bytes(1000)
        assert sc is not None

    def test_never(self):
        sc = dataprof.StopCondition.never()
        assert sc is not None

    def test_or_composition(self):
        sc = dataprof.StopCondition.max_rows(100) | dataprof.StopCondition.max_bytes(1000)
        assert sc is not None

    def test_and_composition(self):
        sc = dataprof.StopCondition.max_rows(100) & dataprof.StopCondition.max_bytes(1000)
        assert sc is not None

    def test_presets(self):
        assert dataprof.StopCondition.schema_inference() is not None
        assert dataprof.StopCondition.quality_sample() is not None

    def test_profile_with_stop_condition(self):
        if not os.path.exists(CSV_LARGE_FILE):
            pytest.skip("fixture missing")
        r = dataprof.profile(
            CSV_LARGE_FILE,
            stop_condition=dataprof.StopCondition.max_rows(100),
            engine="incremental",
        )
        # Stop condition is checked per-chunk; rows may slightly exceed target.
        assert r.rows < 10000
        assert not r.source_exhausted


# ─────────────────────────────────────────────────
#  7. Progress events
# ─────────────────────────────────────────────────


class TestProgress:
    def test_progress_callback(self):
        events = []
        r = dataprof.profile(
            CSV_FILE,
            engine="incremental",
            on_progress=lambda e: events.append(e.kind),
            progress_interval_ms=0,
        )
        assert r.rows > 0
        # Small file may not emit chunk events, but should at least start/finish
        # We just verify the callback was invoked without error


# ─────────────────────────────────────────────────
#  8. Namespace & exports
# ─────────────────────────────────────────────────


class TestNamespace:
    def test_all_exports(self):
        expected = {
            "profile",
            "profile_file",
            "Profiler",
            "ProfileReport",
            "ProfilerConfig",
            "ColumnProfile",
            "DataQualityMetrics",
            "SamplingStrategy",
            "StopCondition",
            "ProgressEvent",
            "list_patterns",
            "infer_schema",
            "quick_row_count",
            "analyze_structure",
            "SchemaResult",
            "RowCountEstimate",
            "StructureColumnSummary",
            "StructureReport",
            "RecordBatch",
            "asyncio",
            "__version__",
            # Database helpers: exported unconditionally. Without a `database`
            # feature build they are stubs that raise ImportError on call.
            "analyze_database_async",
            "count_table_rows_async",
            "get_table_schema_async",
            "test_connection_async",
        }
        assert expected == set(dataprof.__all__), (
            f"__all__ drift detected. "
            f"Missing: {expected - set(dataprof.__all__)}. "
            f"Unexpected: {set(dataprof.__all__) - expected}."
        )

    def test_all_exports_accessible(self):
        """Every name in __all__ must be importable from the package."""
        for name in dataprof.__all__:
            assert hasattr(dataprof, name), f"{name!r} in __all__ but not accessible"

    def test_database_helpers_exported_at_top_level(self):
        """The documented call path is dp.<fn>, not dp._dataprof.<fn>."""
        for name in (
            "analyze_database_async",
            "count_table_rows_async",
            "get_table_schema_async",
            "test_connection_async",
        ):
            assert callable(getattr(dataprof, name))

    def test_database_helpers_fail_loudly_without_feature(self):
        """On the published wheels the stubs must explain the rebuild, not AttributeError."""
        if dataprof._HAS_DATABASE:
            pytest.skip("built with database support; stubs not installed")
        with pytest.raises(ImportError, match="requires database support"):
            # The ImportError stub raises at call time, before any coroutine
            # exists; .close() only runs (and is a no-op) on a real build.
            dataprof.test_connection_async("sqlite:x.db").close()

    def test_asyncio_discoverable(self):
        assert hasattr(dataprof, "asyncio")
        assert dataprof.asyncio.__name__ == "dataprof.asyncio"

    def test_list_patterns_shape_and_locale_filter(self):
        patterns = dataprof.list_patterns()
        assert len(patterns) == 35
        assert set(patterns[0]) == {"name", "regex", "category", "locale", "min_threshold"}
        assert patterns[0]["name"] == "Email"
        assert patterns[0]["category"] == "contact"
        assert patterns[0]["locale"] is None

        it_patterns = dataprof.list_patterns(locale="it")
        names = {pattern["name"] for pattern in it_patterns}
        assert "Email" in names
        assert "Phone (IT)" in names
        assert "Phone (US)" not in names
        assert all(pattern["locale"] in {None, "IT"} for pattern in it_patterns)

    def test_version(self):
        assert isinstance(dataprof.__version__, str)
        assert "." in dataprof.__version__

    def test_profile_signature(self):
        """Guard against accidental signature changes to profile()."""
        import inspect

        sig = inspect.signature(dataprof.profile)
        expected_params = {
            "source",
            "engine",
            "chunk_size",
            "memory_limit_mb",
            "format",
            "max_rows",
            "name",
            "csv_delimiter",
            "csv_flexible",
            "sampling",
            "stop_condition",
            "on_progress",
            "progress_interval_ms",
            "quality_dimensions",
            "metrics",
            "locale",
            "positive_columns",
            "identifier_columns",
        }
        actual_params = set(sig.parameters.keys())
        assert actual_params == expected_params, (
            f"profile() signature drift. "
            f"Missing: {expected_params - actual_params}. "
            f"Unexpected: {actual_params - expected_params}."
        )
        # source is positional; all others are keyword-only
        assert sig.parameters["source"].kind == inspect.Parameter.POSITIONAL_OR_KEYWORD
        assert sig.parameters["engine"].default == "auto"

    def test_profile_report_properties(self):
        """Key ProfileReport properties must exist on the class."""
        expected_props = [
            "source",
            "source_type",
            "rows",
            "columns",
            "column_profiles",
            "quality_score",
            "quality",
            "execution_time_ms",
            "throughput",
            "memory_peak_mb",
            "truncation_reason",
            "source_exhausted",
            "sampling_applied",
            "sampling_ratio",
        ]
        for prop in expected_props:
            assert hasattr(dataprof.ProfileReport, prop), (
                f"ProfileReport missing expected property: {prop!r}"
            )

    def test_profile_report_methods(self):
        """New export methods must exist on the class."""
        expected_methods = [
            "to_dict",
            "to_json",
            "to_dataframe",
            "to_polars",
            "to_arrow",
            "describe",
            "quality_summary",
            "save",
            "to_html",
            "to_markdown",
            "compare",
            "from_dict",
            "from_json",
            "load",
            "__getitem__",
            "__contains__",
            "__iter__",
            "__len__",
        ]
        for method in expected_methods:
            assert hasattr(dataprof.ProfileReport, method), (
                f"ProfileReport missing expected method: {method!r}"
            )

    def test_stub_all_matches_runtime(self):
        """The __all__ list in __init__.pyi must match the runtime __all__."""
        import ast

        stub_path = Path(__file__).resolve().parent.parent / "dataprof" / "__init__.pyi"
        if not stub_path.exists():
            pytest.skip("Type stub not found")
        tree = ast.parse(stub_path.read_text())
        stub_all = None
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "__all__":
                        if isinstance(node.value, ast.List):
                            stub_all = {
                                elt.value
                                for elt in node.value.elts
                                if isinstance(elt, ast.Constant)
                            }
        assert stub_all is not None, "__all__ not found in __init__.pyi"
        runtime_all = set(dataprof.__all__)
        assert stub_all == runtime_all, (
            f"Stub/runtime __all__ mismatch. "
            f"In stub only: {stub_all - runtime_all}. "
            f"In runtime only: {runtime_all - stub_all}."
        )


# ─────────────────────────────────────────────────
#  9. CSV config across engines
# ─────────────────────────────────────────────────


class TestCsvConfigEngines:
    """Verify csv_delimiter works with all engine types."""

    @pytest.fixture(autouse=True)
    def _check_fixture(self):
        if not os.path.exists(SEMICOLON_FILE):
            pytest.skip("semicolon fixture missing")

    def test_csv_delimiter_incremental(self):
        r = dataprof.profile(SEMICOLON_FILE, csv_delimiter=";", engine="incremental")
        assert r.rows > 0
        assert r.columns > 1, "delimiter not applied — got single column"

    def test_csv_delimiter_columnar(self):
        r = dataprof.profile(SEMICOLON_FILE, csv_delimiter=";", engine="columnar")
        assert r.rows > 0
        assert r.columns > 1, "delimiter not applied — got single column"

    def test_csv_delimiter_auto(self):
        r = dataprof.profile(SEMICOLON_FILE, csv_delimiter=";")
        assert r.rows > 0
        assert r.columns > 1, "delimiter not applied — got single column"


# ─────────────────────────────────────────────────
#  10. Mapping protocol (dict-like access)
# ─────────────────────────────────────────────────


class TestProfileReportMapping:
    @pytest.fixture()
    def report(self):
        return dataprof.profile(CSV_FILE)

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


# ─────────────────────────────────────────────────
#  11. Rounding
# ─────────────────────────────────────────────────


class TestRounding:
    @pytest.fixture()
    def report(self):
        return dataprof.profile(CSV_FILE)

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


# ─────────────────────────────────────────────────
#  12. Enriched to_dataframe
# ─────────────────────────────────────────────────


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


# ─────────────────────────────────────────────────
#  13. to_polars
# ─────────────────────────────────────────────────


class TestToPolars:
    def test_to_polars(self):
        pytest.importorskip("polars")
        r = dataprof.profile(CSV_FILE)
        df = r.to_polars()
        assert len(df) == r.columns
        assert "name" in df.columns
        assert "mean" in df.columns


# ─────────────────────────────────────────────────
#  14. to_arrow
# ─────────────────────────────────────────────────


class TestToArrow:
    def test_to_arrow(self):
        pa = pytest.importorskip("pyarrow")
        r = dataprof.profile(CSV_FILE)
        table = r.to_arrow()
        assert isinstance(table, pa.Table)
        assert table.num_rows == r.columns
        assert "name" in table.column_names


# ─────────────────────────────────────────────────
#  15. describe
# ─────────────────────────────────────────────────


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


# ─────────────────────────────────────────────────
#  16. quality_summary
# ─────────────────────────────────────────────────


class TestQualitySummary:
    def test_quality_summary_keys(self):
        r = dataprof.profile(CSV_FILE)
        qs = r.quality_summary()
        assert isinstance(qs, dict)
        expected_keys = {
            "source",
            "rows",
            "quality_score",
            "completeness",
            "consistency",
            "uniqueness",
            "accuracy",
            "timeliness",
            "execution_time_ms",
        }
        assert expected_keys == set(qs.keys())

    def test_quality_summary_values(self):
        r = dataprof.profile(CSV_FILE)
        qs = r.quality_summary()
        assert qs["rows"] == r.rows
        assert isinstance(qs["execution_time_ms"], int)
        assert qs["execution_time_ms"] >= 0

    def test_quality_summary_score_ranges(self):
        r = dataprof.profile(CSV_FILE)
        qs = r.quality_summary()
        score_keys = (
            "quality_score",
            "completeness",
            "consistency",
            "uniqueness",
            "accuracy",
            "timeliness",
        )
        for key in score_keys:
            v = qs[key]
            if v is not None:
                assert isinstance(v, (int, float)), f"{key} should be numeric"
                assert 0 <= v <= 100, f"{key}={v} should be in the 0-100 range"


# ─────────────────────────────────────────────────
#  17. save formats
# ─────────────────────────────────────────────────


class TestSaveFormats:
    @pytest.fixture()
    def report(self):
        return dataprof.profile(CSV_FILE)

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


# ─────────────────────────────────────────────────
#  18. Improved repr
# ─────────────────────────────────────────────────


class TestReprImproved:
    @pytest.fixture()
    def report(self):
        return dataprof.profile(CSV_FILE)

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


# ─────────────────────────────────────────────────
#  Boolean column support
# ─────────────────────────────────────────────────


class TestBooleanColumns:
    @pytest.fixture()
    def boolean_csv(self, tmp_path):
        p = tmp_path / "booleans.csv"
        lines = ["active,verified,count"]
        for i in range(100):
            active = "true" if i % 2 == 0 else "false"
            verified = "True" if i % 3 == 0 else "False"
            lines.append(f"{active},{verified},{i}")
        p.write_text("\n".join(lines))
        return str(p)

    @pytest.fixture()
    def report(self, boolean_csv):
        return dataprof.profile(boolean_csv)

    def test_boolean_detection(self, report):
        active = report["active"]
        assert active.data_type == "boolean"

    def test_boolean_stats_properties(self, report):
        active = report["active"]
        assert active.true_count == 50
        assert active.false_count == 50
        assert active.true_ratio is not None
        assert abs(active.true_ratio - 0.5) < 0.01

    def test_boolean_in_to_dict(self, report):
        d = report.to_dict()
        active_col = next(c for c in d["columns"] if c["name"] == "active")
        assert "stats" in active_col
        assert active_col["stats"]["true_count"] == 50
        assert active_col["stats"]["false_count"] == 50
        assert active_col["stats"]["true_ratio"] is not None

    def test_boolean_in_to_json(self, report):
        j = json.loads(report.to_json())
        active_col = next(c for c in j["columns"] if c["name"] == "active")
        assert "stats" in active_col
        assert "true_count" in active_col["stats"]

    def test_boolean_in_column_record(self, report):
        df = report.to_dataframe()
        active_row = df[df["name"] == "active"].iloc[0]
        assert active_row["true_count"] == 50
        assert active_row["false_count"] == 50

    def test_boolean_repr_shows_stats(self, report):
        r = repr(report)
        assert "true=" in r

    def test_integer_not_boolean(self, report):
        """Pure integer column should NOT be detected as boolean."""
        count_col = report["count"]
        assert count_col.data_type == "integer"

    def test_yes_no_stays_string(self, tmp_path):
        path = tmp_path / "yes_no.csv"
        path.write_text("subscribed\nyes\nno\nyes\nno\n")
        r = dataprof.profile(str(path))
        assert r["subscribed"].data_type == "string"

    def test_mixed_case_boolean_with_null_like_tokens(self, tmp_path):
        path = tmp_path / "booleans_nulls.csv"
        path.write_text("flag,label\ntrue,a\nFALSE,b\nTRUE,c\nfalse,d\nnull,e\nNULL,f\nnan,g\n,h\n")
        r = dataprof.profile(str(path))
        flag = r["flag"]
        assert flag.data_type == "boolean"
        assert flag.null_count == 4
        assert flag.true_count == 2
        assert flag.false_count == 2


# ─────────────────────────────────────────────────
#  16. Profiler builder class
# ─────────────────────────────────────────────────


class TestProfilerBuilder:
    def test_basic_csv(self):
        r = dataprof.Profiler().profile(CSV_FILE)
        assert r.rows > 0
        assert r.columns > 0

    def test_chaining(self):
        r = dataprof.Profiler().engine("auto").max_rows(10).profile(CSV_FILE)
        assert r.rows <= 10

    def test_with_dataframe(self):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        r = dataprof.Profiler().name("test_df").profile(df)
        assert r.rows == 3
        assert r.columns == 2

    def test_stop_when_string(self):
        r = dataprof.Profiler().stop_when("schema_stable").profile(CSV_FILE)
        assert r.rows > 0

    def test_stop_when_object(self):
        r = dataprof.Profiler().stop_when(dataprof.StopCondition.max_rows(5)).profile(CSV_FILE)
        assert r.rows <= 5

    def test_stop_when_invalid_string(self):
        with pytest.raises(ValueError, match="Unknown stop_when shorthand"):
            dataprof.Profiler().stop_when("nonexistent")

    def test_metrics_all_packs(self):
        r = (
            dataprof.Profiler()
            .metrics(["schema", "statistics", "patterns", "quality"])
            .profile(CSV_FILE)
        )
        assert r.quality_score is not None

    def test_metrics_skip_quality(self):
        r = dataprof.Profiler().metrics(["schema", "statistics", "patterns"]).profile(CSV_FILE)
        assert r.quality_score is None

    def test_metrics_schema_only(self):
        r = dataprof.Profiler().metrics(["schema"]).profile(CSV_FILE)
        assert r.quality_score is None

    def test_metrics_invalid_pack(self):
        with pytest.raises(ValueError, match="Unknown metric packs"):
            dataprof.Profiler().metrics(["schema", "bogus"])

    def test_returns_self(self):
        p = dataprof.Profiler()
        assert p.engine("auto") is p
        assert p.max_rows(10) is p
        assert p.csv_delimiter(",") is p
        assert p.quality_dimensions(["completeness"]) is p
        assert p.positive_columns(["pressure"]) is p
        assert p.identifier_columns(["order_id"]) is p

    def test_csv_delimiter(self):
        if not os.path.exists(SEMICOLON_FILE):
            pytest.skip("fixture missing")
        r = dataprof.Profiler().csv_delimiter(";").profile(SEMICOLON_FILE)
        assert r.columns > 1

    def test_repr(self):
        p = dataprof.Profiler().engine("incremental").max_rows(100)
        r = repr(p)
        assert "Profiler(" in r
        assert "engine='incremental'" in r
        assert "max_rows=100" in r


class TestMetricPacks:
    """Test metric pack selection via profile() function and Profiler builder."""

    def test_schema_only(self):
        r = dataprof.profile(CSV_FILE, metrics=["schema"])
        assert r.quality_score is None

    def test_schema_and_statistics(self):
        r = dataprof.profile(CSV_FILE, metrics=["schema", "statistics"])
        assert r.quality_score is None

    def test_all_packs(self):
        r = dataprof.profile(CSV_FILE, metrics=["schema", "statistics", "patterns", "quality"])
        assert r.quality_score is not None

    def test_none_means_all(self):
        r = dataprof.profile(CSV_FILE)
        assert r.quality_score is not None

    def test_builder_metrics(self):
        r = dataprof.Profiler().metrics(["schema", "quality"]).profile(CSV_FILE)
        assert r.quality_score is not None

    def test_metrics_with_dataframe(self):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        r = dataprof.profile(df, metrics=["schema"])
        assert r.quality_score is None

    def test_metrics_with_arrow(self):
        pa = pytest.importorskip("pyarrow")
        table = pa.table({"a": [1, 2, 3]})
        r = dataprof.profile(table, metrics=["schema", "statistics"])
        assert r.quality_score is None


class TestColumnLevelOutliers:
    """Regression: per-column `outlier_count` should surface IQR outliers."""

    def test_outlier_count_flags_spike(self, tmp_path):
        path = tmp_path / "spiky.csv"
        # 9 baseline rows around 22 + one obvious spike at 999.9
        path.write_text(
            "value\n"
            + "\n".join(["22.5", "23.1", "22.8", "23.2", "22.9", "23.0", "22.7", "23.1", "999.9"])
            + "\n"
        )
        r = dataprof.profile(str(path))
        assert r["value"].outlier_count is not None
        assert r["value"].outlier_count >= 1

    def test_outlier_count_zero_on_uniform_column(self, tmp_path):
        path = tmp_path / "flat.csv"
        path.write_text("value\n" + "\n".join(["10.0"] * 12) + "\n")
        r = dataprof.profile(str(path))
        assert r["value"].outlier_count == 0


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


class TestColumnToDict:
    def test_column_to_dict_shape_matches_report(self, tmp_path):
        path = tmp_path / "data.csv"
        path.write_text("x\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n")
        r = dataprof.profile(str(path))
        col = r["x"]
        d = dataprof.column_to_dict(col)
        from_report = r.to_dict()["columns"][0]
        assert d == from_report

    def test_column_to_dict_discoverable_from_interop_not_all(self, tmp_path):
        import dataprof.interop as interop

        path = tmp_path / "data.csv"
        path.write_text("x\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n")
        r = dataprof.profile(str(path))

        assert hasattr(dataprof, "column_to_dict")
        assert "column_to_dict" not in dataprof.__all__
        assert interop.column_to_dict(r["x"]) == dataprof.column_to_dict(r["x"])


class TestLowSampleWarning:
    def test_low_sample_warning_set_on_tiny_csv(self, tmp_path):
        path = tmp_path / "tiny.csv"
        path.write_text("x\n1\n2\n3\n")
        r = dataprof.profile(str(path))
        assert r.low_sample_warning is True
        assert r.to_dict()["quality"].get("low_sample_warning") is True

    def test_low_sample_warning_clear_on_normal_csv(self, tmp_path):
        path = tmp_path / "fine.csv"
        path.write_text("x\n" + "\n".join(str(i) for i in range(50)) + "\n")
        r = dataprof.profile(str(path))
        assert r.low_sample_warning is False
        assert "low_sample_warning" not in r.to_dict()["quality"]
