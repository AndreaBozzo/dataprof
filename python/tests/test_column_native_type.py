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
_ARROW_TYPE_FACTORIES: dict[str, Any] = {
    "null": lambda: pa.null(),
    "bool": lambda: pa.bool_(),
    "int8": lambda: pa.int8(),
    "int16": lambda: pa.int16(),
    "int32": lambda: pa.int32(),
    "int64": lambda: pa.int64(),
    "uint8": lambda: pa.uint8(),
    "uint16": lambda: pa.uint16(),
    "uint32": lambda: pa.uint32(),
    "uint64": lambda: pa.uint64(),
    "float16": lambda: pa.float16(),
    "float32": lambda: pa.float32(),
    "float64": lambda: pa.float64(),
    "string": lambda: pa.string(),
    "large_string": lambda: pa.large_string(),
    "string_view": lambda: pa.string_view(),
    "binary": lambda: pa.binary(),
    "large_binary": lambda: pa.large_binary(),
    "binary_view": lambda: pa.binary_view(),
    "fixed_size_binary": lambda: pa.binary(4),
    "date32": lambda: pa.date32(),
    "date64": lambda: pa.date64(),
    "time32": lambda: pa.time32("s"),
    "time64": lambda: pa.time64("ns"),
    "timestamp": lambda: pa.timestamp("ms"),
    "timestamp_utc": lambda: pa.timestamp("us", tz="UTC"),
    "timestamp_zone": lambda: pa.timestamp("ns", tz="Europe/Rome"),
    "timestamp_offset": lambda: pa.timestamp("s", tz="+01:00"),
    "duration": lambda: pa.duration("ms"),
    "interval": lambda: pa.month_day_nano_interval(),
    "decimal32": lambda: pa.decimal32(5, 2),
    "decimal64": lambda: pa.decimal64(12, 2),
    "decimal128": lambda: pa.decimal128(12, 2),
    "decimal256": lambda: pa.decimal256(40, -3),
    "list": lambda: pa.list_(pa.int64()),
    "list_named_required": lambda: pa.list_(pa.field("x", pa.int64(), nullable=False)),
    "large_list": lambda: pa.large_list(pa.string()),
    "fixed_size_list": lambda: pa.list_(pa.int32(), 3),
    "list_view": lambda: pa.list_view(pa.int8()),
    "large_list_view": lambda: pa.large_list_view(pa.int8()),
    "struct": lambda: pa.struct([("a", pa.int64()), pa.field("b", pa.string(), nullable=False)]),
    "empty_struct": lambda: pa.struct([]),
    "nested": lambda: pa.list_(pa.struct([("a", pa.list_(pa.int8()))])),
    "map": lambda: pa.map_(pa.string(), pa.int64()),
    "map_sorted": lambda: pa.map_(pa.string(), pa.int64(), keys_sorted=True),
    "map_named": lambda: pa.map_(
        pa.field("k", pa.string(), nullable=False), pa.field("val", pa.int64())
    ),
    "dictionary": lambda: pa.dictionary(pa.int32(), pa.string()),
    "dictionary_ordered": lambda: pa.dictionary(pa.int8(), pa.string(), ordered=True),
    "run_end_encoded": lambda: pa.run_end_encoded(pa.int32(), pa.string()),
    "sparse_union": lambda: pa.sparse_union(
        [pa.field("a", pa.int32()), pa.field("b", pa.string())], type_codes=[5, 7]
    ),
    "dense_union": lambda: pa.dense_union([pa.field("a", pa.int32())]),
}


def _available(factories: dict[str, Any]) -> dict[str, Any]:
    """The types this pyarrow can build.

    The dev floor is pyarrow 14, and the view and decimal32/64 factories came
    later; a missing one leaves its type out instead of failing the module at
    import.
    """
    built = {}
    for name, factory in factories.items():
        try:
            built[name] = factory()
        except AttributeError:
            continue
    return built


ARROW_TYPES = _available(_ARROW_TYPE_FACTORIES)


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


def _extension_table() -> Any:
    """Extension columns, alone and as the child of a list, struct and map."""
    uuids = pa.ExtensionArray.from_storage(pa.uuid(), pa.array([b"\x01" * 16, None], pa.binary(16)))
    return pa.table(
        {
            "uuid": uuids,
            "json": pa.array(['{"a": 1}', None], pa.json_()),
            "bool8": pa.ExtensionArray.from_storage(pa.bool8(), pa.array([1, None], pa.int8())),
            "uuid_list": pa.ListArray.from_arrays(pa.array([0, 1, 2]), uuids),
            "uuid_struct": pa.StructArray.from_arrays([uuids], names=["id"]),
            "uuid_map": pa.MapArray.from_arrays(pa.array([0, 1, 2]), pa.array(["a", "b"]), uuids),
        }
    )


def test_an_extension_type_is_named_as_the_extension_not_its_storage(tmp_path: Path) -> None:
    """``pa.uuid()`` is stored as ``fixed_size_binary[16]``; the declaration is the uuid."""
    if not all(hasattr(pa, name) for name in ("uuid", "json_", "bool8")):
        pytest.skip("pyarrow without the uuid, json and bool8 extension types")
    table = _extension_table()
    path = tmp_path / "extensions.parquet"
    pq.write_table(table, path)
    wrong = []
    in_memory = {field.name: str(field.type) for field in table.schema}
    # pyarrow's Parquet writer names a list's child `element`. Its own reader
    # also renames a map's value after the column, which the Rust reader does
    # not, so the file's names are spelled out rather than read back.
    in_file = {
        **in_memory,
        "uuid_list": "list<element: extension<arrow.uuid>>",
        "uuid_map": "map<string, extension<arrow.uuid>>",
    }
    assert in_memory["uuid"] == "extension<arrow.uuid>"
    for route, report, names in [
        ("arrow table", dp.profile(table), in_memory),
        ("parquet file", dp.profile(str(path)), in_file),
    ]:
        for name, column in report.column_profiles.items():
            expected = {"system": "arrow", "name": names[name]}
            if column.native_type != expected:
                wrong.append(f"{route}, {name}: {column.native_type} != {expected}")
    assert not wrong, "\n".join(wrong)


def test_a_parameterized_extension_is_named_without_its_parameters() -> None:
    """pyarrow parses parameters out of the extension's own metadata; the name is what is kept."""
    if not hasattr(pa, "fixed_shape_tensor"):
        pytest.skip("pyarrow without the fixed_shape_tensor extension type")
    tensor = pa.fixed_shape_tensor(pa.int32(), [2, 2])
    storage = pa.array([[1, 2, 3, 4], None], tensor.storage_type)
    report = dp.profile(pa.table({"t": pa.ExtensionArray.from_storage(tensor, storage)}))
    assert str(tensor).startswith("extension<arrow.fixed_shape_tensor[")
    assert report.column_profiles["t"].native_type == {
        "system": "arrow",
        "name": "extension<arrow.fixed_shape_tensor>",
    }


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
