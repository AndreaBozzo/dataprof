"""Each column records the type its source declared (#906).

A report kept only the type dataprof profiled a column as, so an Arrow
``decimal128(12, 2)`` read as ``float`` and a ``timestamp[us, tz=UTC]`` as
``date``, and nothing said the column had kept its declared type. Each column
now carries ``native_type``: the declaration, in the spelling of the type system
it belongs to, or ``None`` where the source declares none.

``tests/column_native_type.rs`` is the Rust twin.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

try:
    import dataprof as dp
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

#: Arrow types, each built as a one-column table and profiled. Every arm of the
#: renderer is here, with the cases where pyarrow adds detail: a non-nullable or
#: renamed child, an ordered dictionary, union type codes, map field names.
ARROW_TYPES: dict[str, Any] = {
    "null": pa.null(),
    "bool": pa.bool_(),
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float16": pa.float16(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "string": pa.string(),
    "large_string": pa.large_string(),
    "string_view": pa.string_view(),
    "binary": pa.binary(),
    "large_binary": pa.large_binary(),
    "binary_view": pa.binary_view(),
    "fixed_size_binary": pa.binary(4),
    "date32": pa.date32(),
    "date64": pa.date64(),
    "time32": pa.time32("s"),
    "time64": pa.time64("ns"),
    "timestamp": pa.timestamp("ms"),
    "timestamp_utc": pa.timestamp("us", tz="UTC"),
    "timestamp_zone": pa.timestamp("ns", tz="Europe/Rome"),
    "timestamp_offset": pa.timestamp("s", tz="+01:00"),
    "duration": pa.duration("ms"),
    "interval": pa.month_day_nano_interval(),
    "decimal32": pa.decimal32(5, 2),
    "decimal64": pa.decimal64(12, 2),
    "decimal128": pa.decimal128(12, 2),
    "decimal256": pa.decimal256(40, -3),
    "list": pa.list_(pa.int64()),
    "list_named_required": pa.list_(pa.field("x", pa.int64(), nullable=False)),
    "large_list": pa.large_list(pa.string()),
    "fixed_size_list": pa.list_(pa.int32(), 3),
    "list_view": pa.list_view(pa.int8()),
    "large_list_view": pa.large_list_view(pa.int8()),
    "struct": pa.struct([("a", pa.int64()), pa.field("b", pa.string(), nullable=False)]),
    "empty_struct": pa.struct([]),
    "nested": pa.list_(pa.struct([("a", pa.list_(pa.int8()))])),
    "map": pa.map_(pa.string(), pa.int64()),
    "map_sorted": pa.map_(pa.string(), pa.int64(), keys_sorted=True),
    "map_named": pa.map_(pa.field("k", pa.string(), nullable=False), pa.field("val", pa.int64())),
    "dictionary": pa.dictionary(pa.int32(), pa.string()),
    "dictionary_ordered": pa.dictionary(pa.int8(), pa.string(), ordered=True),
    "run_end_encoded": pa.run_end_encoded(pa.int32(), pa.string()),
    "sparse_union": pa.sparse_union(
        [pa.field("a", pa.int32()), pa.field("b", pa.string())], type_codes=[5, 7]
    ),
    "dense_union": pa.dense_union([pa.field("a", pa.int32())]),
}


def _native_types(report: dp.ProfileReport) -> dict[str, Any]:
    return {name: column.native_type for name, column in report.column_profiles.items()}


def _table() -> Any:
    """Typed columns whose profiled type is not their declared one."""
    utc = dt.UTC
    return pa.table(
        {
            "amount": pa.array([Decimal("12.34"), Decimal("5.00")], pa.decimal128(12, 2)),
            "ordered_at": pa.array(
                [dt.datetime(2026, 1, 1, tzinfo=utc), None], pa.timestamp("us", tz="UTC")
            ),
            "id": pa.array([1, 2], pa.int64()),
            "name": pa.array(["a", "b"], pa.string()),
            "status": pa.array(["new", "done"]).dictionary_encode(),
        }
    )


#: The declared names of ``_table()``, as pyarrow prints them.
TABLE_TYPES = {
    "amount": "decimal128(12, 2)",
    "ordered_at": "timestamp[us, tz=UTC]",
    "id": "int64",
    "name": "string",
    "status": "dictionary<values=string, indices=int32, ordered=0>",
}


def test_every_arrow_type_is_spelled_as_pyarrow_spells_it() -> None:
    wrong = []
    for name, arrow_type in ARROW_TYPES.items():
        report = dp.profile(pa.table({name: pa.nulls(2, arrow_type)}))
        recorded = report.column_profiles[name].native_type
        expected = {"system": "arrow", "name": str(arrow_type)}
        if recorded != expected:
            wrong.append(f"{name}: {recorded} != {expected}")
    assert not wrong, "\n".join(wrong)


def test_every_arrow_route_records_the_declared_type(tmp_path: Path) -> None:
    table = _table()
    path = tmp_path / "typed.parquet"
    pq.write_table(table, path)
    routes = {
        "arrow table": lambda: dp.profile(table),
        "record batch": lambda: dp.profile(table.to_batches()[0]),
        "batch reader": lambda: dp.profile(
            pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
        ),
        "parquet file": lambda: dp.profile(str(path)),
        "parquet bytes": lambda: dp.profile(path.read_bytes(), format="parquet"),
    }
    expected = {name: {"system": "arrow", "name": spelled} for name, spelled in TABLE_TYPES.items()}
    wrong = []
    for route, run in routes.items():
        recorded = _native_types(run())
        for column, declared in expected.items():
            if recorded.get(column) != declared:
                wrong.append(f"{route}, {column}: {recorded.get(column)}")
    assert not wrong, "\n".join(wrong)


def test_a_polars_frame_records_the_arrow_type_it_exports() -> None:
    pl = pytest.importorskip("polars")
    frame = pl.from_arrow(_table().select(["amount", "ordered_at", "id"]))
    recorded = _native_types(dp.profile(frame))
    exported = frame.to_arrow().schema
    assert recorded == {
        field.name: {"system": "arrow", "name": str(field.type)} for field in exported
    }
    assert recorded["amount"] == {"system": "arrow", "name": "decimal128(12, 2)"}


def test_a_pandas_frame_records_its_dtypes() -> None:
    pd = pytest.importorskip("pandas")
    frame = pd.DataFrame(
        {
            "id": [1, 2],
            "price": [1.5, 2.25],
            # pyarrow infers decimal128 from these values; the frame declared object.
            "amount": [Decimal("1.50"), Decimal("2.25")],
            "mixed": [1, "x"],
            "maybe": pd.array([1, None], dtype="Int64"),
            "level": pd.Categorical(["low", "high"]),
            "at": pd.to_datetime(["2026-01-01", "2026-01-02"]).tz_localize("UTC"),
        }
    )
    wrong = []
    for label, source in {
        "frame": frame,
        # A multi-level index is stored as trailing columns; each takes its level's dtype.
        "indexed": frame.set_index(["id", "level"]),
    }.items():
        recorded = _native_types(dp.profile(source))
        for column, declared in recorded.items():
            dtype = frame[column].dtype if column in frame.columns else None
            expected = {"system": "pandas", "name": str(dtype)}
            if declared != expected:
                wrong.append(f"{label}, {column}: {declared} != {expected}")
        if len(recorded) != len(frame.columns):
            wrong.append(f"{label}: profiled {sorted(recorded)}")
    assert not wrong, "\n".join(wrong)


def test_text_and_python_sources_declare_no_type(tmp_path: Path) -> None:
    csv = tmp_path / "t.csv"
    csv.write_text("id,amount\n1,12.34\n2,5.00\n", encoding="utf-8")
    jsonl = tmp_path / "t.jsonl"
    jsonl.write_text('{"id": 1, "amount": 12.34}\n{"id": 2, "amount": 5.0}\n', encoding="utf-8")
    routes = {
        "csv file": lambda: dp.profile(str(csv)),
        "csv bytes": lambda: dp.profile(csv.read_bytes(), format="csv"),
        "jsonl": lambda: dp.profile(str(jsonl)),
        "dict": lambda: dp.profile({"id": [1, 2], "amount": [Decimal("12.34"), Decimal("5")]}),
        "records": lambda: dp.profile([{"id": 1, "amount": 12.34}, {"id": 2, "amount": 5.0}]),
    }
    wrong = []
    for route, run in routes.items():
        report = run()
        recorded = _native_types(report)
        if set(recorded.values()) != {None}:
            wrong.append(f"{route}: {recorded}")
        for column in json.loads(report.to_json())["column_profiles"]:
            if "native_type" in column:
                wrong.append(f"{route}: {column['name']} serialized {column['native_type']}")
    assert not wrong, "\n".join(wrong)


def test_both_dialects_write_and_read_back_the_declaration() -> None:
    report = dp.profile(_table())
    expected = {name: {"system": "arrow", "name": spelled} for name, spelled in TABLE_TYPES.items()}
    document = json.loads(report.to_json())
    summary = report.to_dict()
    assert {c["name"]: c["native_type"] for c in document["column_profiles"]} == expected
    assert {c["name"]: c["native_type"] for c in summary["columns"]} == expected
    assert list(summary["columns"][0]["native_type"]) == ["system", "name"]
    for label, loaded in {
        "document": dp.ProfileReport.from_dict(document),
        "summary": dp.ProfileReport.from_dict(summary),
    }.items():
        assert _native_types(loaded) == expected, label
        # Written back, through the Python summary writer for a loaded report.
        rewritten = {c["name"]: c.get("native_type") for c in loaded.to_dict()["columns"]}
        assert rewritten == expected, label


def test_a_report_without_the_field_reads_as_not_recorded() -> None:
    summary = dp.profile(_table()).to_dict()
    for column in summary["columns"]:
        del column["native_type"]
    loaded = dp.ProfileReport.from_dict(summary)
    assert set(_native_types(loaded).values()) == {None}
    assert all("native_type" not in c for c in loaded.to_dict()["columns"])


def test_a_malformed_declaration_is_refused() -> None:
    summary = dp.profile(_table()).to_dict()
    summary["columns"][0]["native_type"] = {"system": "arrow"}
    with pytest.raises(ValueError, match=r"columns\[0\]\.native_type"):
        dp.ProfileReport.from_dict(summary)


def test_sqlite_declares_no_type(tmp_path: Path) -> None:
    """sqlx reports a SQLite column's storage class, not its declaration.

    PostgreSQL and MySQL record the server's type; the Rust twin covers them,
    since the Python test environment has no client to create their tables.
    """
    from dataprof import _dataprof

    if not hasattr(_dataprof, "analyze_database_async"):
        pytest.skip("database features not compiled")
    path = tmp_path / "t.db"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE t (id INTEGER, amount DECIMAL(12,2), note TEXT)")
        connection.execute("INSERT INTO t VALUES (1, 12.34, 'a'), (2, 5.00, NULL)")

    async def run() -> Any:
        return await _dataprof.analyze_database_async(
            str(path), "SELECT * FROM t", 10000, None, None
        )

    recorded = _native_types(dp.ProfileReport(asyncio.run(run())))
    assert set(recorded.values()) == {None}, recorded
