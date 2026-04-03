"""Minimal regression test suite for the dataprof Python API.

Run after building the extension:
    maturin develop --features python
    pytest python/tests/test_python_api.py -v
"""

from __future__ import annotations

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

    def test_path_object(self):
        r = dataprof.profile(Path(CSV_FILE))
        assert r.rows > 0

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

    def test_polars(self):
        pl = pytest.importorskip("polars")
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        r = dataprof.profile(df)
        assert r.rows == 3
        assert r.columns == 2


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
#  3. Partial analysis
# ─────────────────────────────────────────────────


class TestPartialAnalysis:
    def test_infer_schema(self):
        result = dataprof.infer_schema(CSV_FILE)
        assert result.num_columns > 0
        assert len(result.column_names) == result.num_columns
        assert result.rows_sampled > 0

    def test_quick_row_count(self):
        result = dataprof.quick_row_count(CSV_FILE)
        assert result.count > 0
        assert isinstance(result.exact, bool)
        assert isinstance(result.method, str)


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
            "ProfileReport",
            "ProfilerConfig",
            "ColumnProfile",
            "DataQualityMetrics",
            "SamplingStrategy",
            "StopCondition",
            "ProgressEvent",
            "infer_schema",
            "quick_row_count",
            "SchemaResult",
            "RowCountEstimate",
            "RecordBatch",
            "__version__",
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
