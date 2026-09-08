"""Profiler configuration, sampling, stop conditions, progress, and metric packs."""

from __future__ import annotations

import os

import pytest
from conftest import CSV_FILE, CSV_LARGE_FILE, SEMICOLON_FILE

try:
    import dataprof
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop --features python",
        allow_module_level=True,
    )


class TestProfilerConfig:
    def test_basic_config(self):
        cfg = dataprof.ProfilerConfig()
        assert cfg.engine == "auto"
        assert cfg.chunk_size is None
        assert cfg.max_rows is None

    @pytest.mark.parametrize(
        ("engine", "canonical"),
        [
            ("auto", "auto"),
            ("incremental", "incremental"),
            ("streaming", "incremental"),
            ("columnar", "columnar"),
            ("arrow", "columnar"),
        ],
    )
    def test_engine_override(self, engine, canonical):
        for spelling in (engine, engine.upper()):
            cfg = dataprof.ProfilerConfig(engine=spelling)
            assert cfg.engine == canonical

    @pytest.mark.parametrize("entry_point", ["profile", "profile_file", "builder"])
    @pytest.mark.parametrize("engine", ["auto", "incremental", "streaming", "columnar", "arrow"])
    @pytest.mark.parametrize("uppercase", [False, True], ids=["lowercase", "uppercase"])
    def test_engine_spellings_profile_csv(self, tmp_path, entry_point, engine, uppercase):
        path = tmp_path / "values.csv"
        path.write_text("value\n1\n2\n3\n", encoding="utf-8")
        spelling = engine.upper() if uppercase else engine
        if entry_point == "builder":
            report = dataprof.Profiler().engine(spelling).profile(path)
        else:
            report = getattr(dataprof, entry_point)(path, engine=spelling)

        assert report.rows == 3
        canonical = {"streaming": "incremental", "arrow": "columnar"}.get(engine, engine)
        if canonical == "auto":
            assert report.engine in {"incremental", "columnar"}
        else:
            assert report.engine == canonical
        assert report.to_dict()["execution"]["engine"] == report.engine

    def test_semantic_hint_config(self):
        cfg = dataprof.ProfilerConfig(
            positive_columns=["pressure"],
            identifier_columns=["order_id", "customer_id"],
            temporal_columns=["observed_on"],
        )
        assert cfg.positive_columns == ["pressure"]
        assert cfg.identifier_columns == ["order_id", "customer_id"]
        assert cfg.temporal_columns == ["observed_on"]

    def test_positional_order_remains_backward_compatible(self):
        # `columns` was added after this positional surface already existed.
        # Keep every established slot stable and append new options at the end.
        cfg = dataprof.ProfilerConfig(
            "auto",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "it-IT",
            ["pressure"],
            ["order_id"],
            ["observed_on"],
            ["amount"],
        )

        assert cfg.locale == "IT"
        assert cfg.positive_columns == ["pressure"]
        assert cfg.identifier_columns == ["order_id"]
        assert cfg.temporal_columns == ["observed_on"]
        assert cfg.columns == ["amount"]

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

    @pytest.mark.parametrize("entry_point", ["config", "profile", "profile_file", "builder"])
    def test_invalid_engine_raises(self, tmp_path, entry_point):
        path = tmp_path / "values.csv"
        path.write_text("value\n1\n", encoding="utf-8")
        with pytest.raises(ValueError) as excinfo:
            if entry_point == "config":
                dataprof.ProfilerConfig(engine="invalid")
            elif entry_point == "builder":
                dataprof.Profiler().engine("invalid").profile(path)
            else:
                getattr(dataprof, entry_point)(path, engine="invalid")
        assert str(excinfo.value) == (
            "Unknown engine 'invalid'. Valid: auto, incremental (alias: streaming), "
            "columnar (alias: arrow)"
        )

    def test_max_rows_and_stop_condition_conflict(self):
        with pytest.raises(ValueError, match="Cannot specify both"):
            dataprof.ProfilerConfig(
                max_rows=100,
                stop_condition=dataprof.StopCondition.max_rows(200),
            )


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
        assert p.columns(["id"]) is p
        assert p.positive_columns(["pressure"]) is p
        assert p.identifier_columns(["order_id"]) is p
        assert p.temporal_columns(["observed_on"]) is p

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
