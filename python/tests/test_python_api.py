"""Capabilities, public namespace, and file or in-memory profiling inputs."""

from __future__ import annotations

import builtins
import dataclasses
import datetime
import importlib.util
import io
import os
import subprocess
import sys
from pathlib import Path

import pytest
from conftest import CSV_FILE, FIXTURES, JSON_FILE, JSONL_FILE, PARQUET_FILE

try:
    import dataprof
    from dataprof import _capabilities, _database
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop --features python",
        allow_module_level=True,
    )


class TestCapabilities:
    def test_default_snapshot_shape(self):
        snapshot = dataprof.capabilities()

        assert isinstance(snapshot, dataprof.Capabilities)
        assert snapshot.version == dataprof.__version__
        assert snapshot.local_csv
        assert snapshot.local_json
        assert snapshot.local_jsonl
        assert snapshot.local_parquet
        assert snapshot.pandas_interop
        assert snapshot.polars_interop
        assert snapshot.arrow_interop
        assert snapshot.pandas_installed is (importlib.util.find_spec("pandas") is not None)
        assert snapshot.polars_installed is (importlib.util.find_spec("polars") is not None)
        assert snapshot.pyarrow_installed is (importlib.util.find_spec("pyarrow") is not None)
        assert "Capabilities(" in repr(snapshot)

    def test_compiled_features_match_native_metadata(self):
        from dataprof._dataprof import _compiled_capabilities

        snapshot = dataprof.capabilities()
        compiled = _compiled_capabilities

        assert snapshot.async_streaming is compiled["async_streaming"]
        assert snapshot.url_profiling is compiled["async_streaming"]
        assert snapshot.remote_parquet is compiled["parquet_async"]
        assert snapshot.database is compiled["database"]
        assert snapshot.database_connectors == tuple(
            name
            for name in ("postgres", "mysql", "sqlite")
            if compiled["database"] and compiled[name]
        )

    def test_connectors_are_hidden_without_database_api(self, monkeypatch):
        from dataprof._dataprof import _compiled_capabilities

        monkeypatch.setitem(_compiled_capabilities, "database", False)
        monkeypatch.setitem(_compiled_capabilities, "sqlite", True)

        snapshot = dataprof.capabilities()
        assert not snapshot.database
        assert snapshot.database_connectors == ()

    def test_snapshot_is_immutable(self):
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(dataprof.capabilities(), "database", True)

    def test_discovery_does_not_import_optional_dependencies(self):
        code = """
import sys
import dataprof

optional = {"pandas", "polars", "pyarrow"}
before = optional.intersection(sys.modules)
dataprof.capabilities()
after = optional.intersection(sys.modules)
assert after == before, after - before
"""
        subprocess.run([sys.executable, "-c", code], check=True)


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

    def test_csv_preserves_column_order(self, tmp_path):
        path = tmp_path / "ordered.csv"
        path.write_text("num,cat,when,big\n1.5,a,2024-01-01,1099511627776\n")

        report = dataprof.profile(path)

        assert [column["name"] for column in report.to_dict()["columns"]] == [
            "num",
            "cat",
            "when",
            "big",
        ]

    def test_missing_file_raises_file_not_found(self):
        missing = str(FIXTURES / "does_not_exist.csv")
        with pytest.raises(FileNotFoundError, match="does_not_exist.csv") as excinfo:
            dataprof.profile(missing)
        assert excinfo.value.filename == missing

    def test_unsupported_type_raises(self):
        with pytest.raises(TypeError, match="Unsupported source type"):
            dataprof.profile(42)

    def test_unsupported_format_raises_value_error(self, tmp_path):
        # A genuinely unknown extension is user error, not an internal failure:
        # the lightweight entry points surface it as ValueError, and the
        # supported-format list must match what this build can read.
        bogus = tmp_path / "sheet.xlsx"
        bogus.write_text("not really a spreadsheet")
        with pytest.raises(ValueError, match="Unsupported file format") as excinfo:
            dataprof.infer_schema(str(bogus))
        assert "CSV" in str(excinfo.value)


class TestProfileDataFrame:
    @staticmethod
    def assert_column_order(report, backend):
        expected = ["num", "cat", "when", "big"]
        assert [column["name"] for column in report.to_dict()["columns"]] == expected
        if backend == "pandas":
            assert report.to_dataframe()["name"].tolist() == expected
        else:
            assert report.to_polars()["name"].to_list() == expected

        described = report.describe()
        described_columns = (
            list(described) if isinstance(described, dict) else described.columns.tolist()
        )
        assert described_columns == expected
        assert list(report.compare(report)["columns"]) == expected

    def test_pandas(self):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame(
            {
                "num": [1.5, 2.5],
                "cat": ["a", "b"],
                "when": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "big": [2**40, 2**40 + 1],
            }
        )
        r = dataprof.profile(df)
        assert r.rows == 2
        assert r.columns == 4
        self.assert_column_order(r, "pandas")


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

    def test_arrow_uniqueness_ratio_is_unit_scale(self):
        # uniqueness_ratio must be a 0..1 ratio, matching
        # ColumnProfile.uniqueness_ratio and the docs — not a 0..100 percentage.
        pa = pytest.importorskip("pyarrow")
        import dataprof.interop as interop

        batch = pa.record_batch(interop.analyze_csv_to_arrow(CSV_FILE))
        uniq = batch.column("unique_count").to_pylist()
        totals = batch.column("total_count").to_pylist()
        ratios = batch.column("uniqueness_ratio").to_pylist()
        for uc, tot, ratio in zip(uniq, totals, ratios):
            if ratio is None:
                continue
            assert 0.0 <= ratio <= 1.0
            assert ratio == pytest.approx(uc / tot, abs=1e-9)

    def test_analyze_parquet_to_arrow_path_object(self):
        import dataprof.interop as interop

        if not os.path.exists(PARQUET_FILE):
            pytest.skip("fixture missing")
        batch = interop.analyze_parquet_to_arrow(Path(PARQUET_FILE))
        assert batch.num_rows > 0
        assert batch.num_columns > 0

    def test_polars(self, monkeypatch):
        pl = pytest.importorskip("polars")
        df = pl.DataFrame(
            {
                "num": [1.5, 2.5],
                "cat": ["a", "b"],
                "when": ["2024-01-01", "2024-01-02"],
                "big": [2**40, 2**40 + 1],
            }
        )
        r = dataprof.profile(df)
        assert r.rows == 2
        assert r.columns == 4

        original_import = builtins.__import__

        def import_without_pandas(name, *args, **kwargs):
            if name == "pandas":
                raise ImportError("blocked pandas")
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", import_without_pandas)
        TestProfileDataFrame.assert_column_order(r, "polars")


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

    def test_nested_container_cells_use_deterministic_compact_json(self):
        r = dataprof.profile([{"nested": {"b": [2, 3], "a": 1}, "array": [1, 2]}])
        assert r["nested"].min_length == len('{"a":1,"b":[2,3]}')
        assert r["array"].min_length == len("[1,2]")

    def test_container_cell_with_unserialisable_contents_falls_back_to_str(self):
        # A dict/list holding a non-JSON-serialisable value must not abort
        # profiling; it degrades to str() like any other opaque cell.
        cell = {"when": datetime.datetime(2020, 1, 2)}
        r = dataprof.profile([{"c": cell}])
        assert r.rows == 1
        assert r["c"].min_length == len(str(cell))

    def test_bytesio_csv_input(self):
        r = dataprof.profile(io.BytesIO(b"a,b\n1,2\n"), format="csv")
        assert r.rows == 1
        assert r.columns == 2

    def test_bytes_input_requires_format(self):
        with pytest.raises(ValueError, match="dataprof.asyncio.profile_bytes"):
            dataprof.profile(b"a,b\n1,2\n")

    def test_sync_bytes_reject_controls_they_cannot_apply(self):
        data = b"a,b\n1,2\n3,4\n"
        unsupported = [
            lambda: dataprof.profile(data, format="csv", engine="incremental"),
            lambda: dataprof.profile(data, format="csv", engine="not-an-engine"),
            lambda: dataprof.profile(data, format="csv", chunk_size=1024),
            lambda: dataprof.profile(data, format="csv", memory_limit_mb=1),
            lambda: dataprof.profile(
                data,
                format="csv",
                stop_condition=dataprof.StopCondition.max_rows(1),
            ),
            lambda: dataprof.profile(data, format="csv", on_progress=lambda _event: None),
            lambda: dataprof.profile(data, format="csv", progress_interval_ms=1),
            lambda: dataprof.profile(data, format="csv", csv_flexible=True),
        ]
        for call in unsupported:
            with pytest.raises(ValueError, match="cannot apply"):
                call()

    def test_ad_hoc_inputs_do_not_import_pandas(self, monkeypatch):
        """The base wheel has no dependencies; profiling must not reach for one."""

        def explode(feature):
            raise AssertionError(f"ad-hoc path called _require_pandas({feature!r})")

        monkeypatch.setattr(_capabilities, "_require_pandas", explode)
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

    def test_dict_infers_unsigned_values_beyond_i64_as_integer(self):
        r = dataprof.profile({"x": [2**64 - 1, 2**64 - 2]})
        assert r["x"].data_type == "integer"

    def test_dict_keeps_non_finite_numeric_values_in_a_float_column(self):
        r = dataprof.profile({"x": [1.0, float("inf"), float("-inf"), 2.0]})
        assert r["x"].data_type == "float"
        assert r["x"].invalid_count == 2
        assert r["x"].min == 1.0
        assert r["x"].max == 2.0

    def test_dict_rejects_ragged_columns(self):
        with pytest.raises(ValueError, match="differing lengths"):
            dataprof.profile({"a": [1, 2], "b": [1]})

    def test_dict_rejects_scalar_column(self):
        with pytest.raises(TypeError, match="must be a list or tuple"):
            dataprof.profile({"a": 1})

    def test_dict_rejects_keys_that_collide_after_string_conversion(self):
        with pytest.raises(ValueError, match="collide after string conversion"):
            dataprof.profile({1: [1, 2], "1": [3, 4]})

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

    def test_raw_extension_carries_a_row_count_without_columns(self):
        """`row_count` is how a fieldless-record source states its row count.

        With no columns there are no cells to derive it from, so the count would
        otherwise collapse to zero and erase the rows.
        """
        from dataprof import _dataprof

        report = dataprof.ProfileReport(_dataprof.profile_columns([], "x", None, None, 0, 3))
        assert (report.rows, report.columns) == (3, 0)

    def test_raw_extension_rejects_a_row_count_the_columns_contradict(self):
        """Where the cells already carry the row count, a disagreeing
        `row_count` is a caller bug and must not be quietly ignored."""
        from dataprof import _dataprof

        with pytest.raises(ValueError, match="row_count is 5"):
            _dataprof.profile_columns([("a", ["1", "2"])], "x", None, None, 0, 5)

    def test_list_of_dicts_fills_missing_keys_with_nulls(self):
        r = dataprof.profile([{"a": 1}, {"b": 2}])
        assert list(r.column_profiles) == ["a", "b"]
        assert r["a"].null_count == 1
        assert r["b"].null_count == 1

    def test_list_of_dicts_max_rows_does_not_discover_later_columns(self):
        r = dataprof.profile([{"a": 1}, {"a": 2, "later": 3}], max_rows=1)
        assert list(r.column_profiles) == ["a"]
        assert r.rows == 1
        assert not r.source_exhausted

    def test_list_of_dicts_max_rows_zero_yields_no_rows_not_error(self):
        # max_rows=0 asks for an empty analysis; that must not be mistaken for
        # the "rows but no columns" error, which is about the analysed subset.
        r = dataprof.profile([{"a": 1}, {"a": 2}], max_rows=0)
        assert r.rows == 0

    def test_list_of_dicts_rejects_colliding_normalized_keys(self):
        with pytest.raises(ValueError, match="collide after string conversion"):
            dataprof.profile([{1: "a", "1": "b"}])

    def test_non_empty_list_of_empty_records_is_not_reported_as_zero_rows(self):
        # Records with no fields are rows against no columns, the same shape the
        # file scanner reports for `[{}]`. Reporting zero rows would erase them.
        r = dataprof.profile([{}])
        assert (r.rows, r.columns) == (1, 0)

    def test_csv_bytes_treat_empty_field_as_null(self):
        r = dataprof.profile(b"a,b\n1,\n2,x\n", format="csv")
        assert r["b"].null_count == 1

    def test_csv_bytes_honour_delimiter(self):
        r = dataprof.profile(b"a;b\n1;2\n", format="csv", csv_delimiter=";")
        assert r.columns == 2

    def test_csv_bytes_auto_detect_delimiter_like_file_input(self):
        r = dataprof.profile(b"a;b\n1;2\n", format="csv")
        assert list(r.column_profiles) == ["a", "b"]

    def test_csv_bytes_strip_utf8_bom_from_first_header(self):
        r = dataprof.profile(b"\xef\xbb\xbfa,b\n1,2\n", format="csv")
        assert list(r.column_profiles) == ["a", "b"]

    def test_csv_bytes_reject_ragged_rows(self):
        with pytest.raises(ValueError, match="row 2 has 3 fields"):
            dataprof.profile(b"a,b\n1,2,3\n", format="csv")

    def test_csv_bytes_reject_duplicate_headers(self):
        # Would otherwise report 3 columns but shadow one 'x' at mapping access.
        with pytest.raises(ValueError, match="duplicate column name"):
            dataprof.profile(b"x,x,y\n1,2,a\n3,4,b\n", format="csv")

    def test_csv_file_reject_duplicate_headers(self, tmp_path):
        path = tmp_path / "dup.csv"
        path.write_text("x,x,y\n1,2,a\n3,4,b\n")
        with pytest.raises(ValueError) as excinfo:
            dataprof.profile(str(path))
        msg = str(excinfo.value)
        assert "'x'" in msg  # names the offender
        assert "1" not in msg  # never echoes cell values

    def test_arrow_reject_duplicate_columns(self):
        pa = pytest.importorskip("pyarrow")
        # Two fields named "x"; a name-keyed analyzer would merge them.
        table = pa.table(
            [pa.array([1, 3]), pa.array([2, 4]), pa.array([5, 6])],
            schema=pa.schema([("x", pa.int64()), ("x", pa.int64()), ("y", pa.int64())]),
        )
        with pytest.raises(ValueError, match="[Dd]uplicate column name"):
            dataprof.profile(table)

    def test_raw_extension_rejects_duplicate_column_names(self):
        # profile_columns is importable directly, so it enforces uniqueness too.
        from dataprof import _dataprof

        with pytest.raises(ValueError, match="[Dd]uplicate column name"):
            _dataprof.profile_columns([("x", ["1", "3"]), ("x", ["2", "4"])], "t", None, None)

    def test_json_bytes_accept_columns_or_records(self):
        by_column = dataprof.profile(b'{"a": [1, 2]}', format="json")
        by_record = dataprof.profile(b'[{"a": 1}, {"a": 2}]', format="json")
        assert by_column.rows == by_record.rows == 2

    def test_json_bytes_accept_scalar_root_object_as_one_record(self):
        r = dataprof.profile(b'{"a": 1, "active": true}', format="json")
        assert r.rows == 1
        assert list(r.column_profiles) == ["a", "active"]

    def test_json_bytes_max_rows_does_not_discover_later_columns(self):
        r = dataprof.profile(b'[{"a": 1}, {"a": 2, "later": 3}]', format="json", max_rows=1)
        assert list(r.column_profiles) == ["a"]
        assert not r.source_exhausted

    def test_json_bytes_column_order_matches_file_parser(self, tmp_path):
        # Source field order, with a later-only field appended where it first
        # appears (#465). Sorting would give ["a", "later", "z"].
        payload = b'[{"z": 1, "a": 2}, {"later": 3}]'
        path = tmp_path / "order.json"
        path.write_bytes(payload)

        from_bytes = dataprof.profile(payload, format="json")
        from_file = dataprof.profile(str(path))

        assert list(from_bytes.column_profiles) == ["z", "a", "later"]
        assert list(from_file.column_profiles) == list(from_bytes.column_profiles)

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


class TestNamespace:
    def test_all_exports(self):
        expected = {
            "Capabilities",
            "capabilities",
            "REPORT_SCHEMA_VERSION",
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
            "column_to_dict",
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
        if _database._HAS_DATABASE:
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
            "jsonl_on_error",
            "sampling",
            "stop_condition",
            "on_progress",
            "progress_interval_ms",
            "quality_dimensions",
            "metrics",
            "columns",
            "locale",
            "positive_columns",
            "identifier_columns",
            "temporal_columns",
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
