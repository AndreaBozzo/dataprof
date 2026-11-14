"""compare() lists each column's type on both sides and the column selection (#1031).

The baseline gate reads both and reports a type change or a selection
difference as a diagnostic, but ``compare()`` carried null percentages only.
Each column entry now has ``data_type_a``/``data_type_b`` (the inferred type,
where the side read a value that is not null) and
``native_type_a``/``native_type_b`` (the type the source declared, #906), and
``configuration.columns`` lists each side's selection.

``tests/compare_column_types.rs`` is the Rust twin. Here the routes are the
Python ones: dicts, pandas and Arrow inputs, a report rebuilt from its
``to_dict()`` summary and one saved and loaded.
"""

from __future__ import annotations

from collections.abc import Callable
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
pd = pytest.importorskip("pandas")

# ``blank`` is all null in the baseline, so it establishes no type there;
# ``gone`` and ``new`` exist on one side only.
BASELINE = {
    "id": [1, 2],
    "code": [10, 20],
    "blank": [None, None],
    "gone": ["x", "y"],
}
CURRENT = {"id": [1, 2], "code": ["a1", "b2"], "blank": [3, 4], "new": ["z", "w"]}


def _flat(report: dp.ProfileReport) -> dp.ProfileReport:
    return dp.ProfileReport.from_dict(report.to_dict())


def _routes(tmp_path: Path) -> list[tuple[str, Callable[[dp.ProfileReport], dp.ProfileReport]]]:
    def saved(report: dp.ProfileReport) -> dp.ProfileReport:
        path = tmp_path / f"{id(report)}.json"
        report.save(str(path))
        return dp.ProfileReport.load(str(path))

    return [("report", lambda r: r), ("summary", _flat), ("saved", saved)]


def _deltas(
    a: dp.ProfileReport, b: dp.ProfileReport, tmp_path: Path
) -> list[tuple[str, dict[str, Any]]]:
    routes = _routes(tmp_path)
    return [
        (f"{name_a}/{name_b}", side_a(a).compare(side_b(b)))
        for name_a, side_a in routes
        for name_b, side_b in routes
    ]


def _mismatches(
    deltas: list[tuple[str, dict[str, Any]]],
    keys: tuple[str, str],
    expected: dict[str, tuple[Any, Any]],
) -> list[str]:
    wrong = []
    for route, delta in deltas:
        for column, wants in expected.items():
            entry = delta["columns"][column]
            for key, want in zip(keys, wants, strict=True):
                if entry[key] != want:
                    wrong.append(f"{route}, {column}.{key}: {entry[key]!r} != {want!r}")
    return wrong


@pytest.mark.parametrize(
    "build",
    [
        pytest.param(lambda data: data, id="dict"),
        pytest.param(lambda data: pd.DataFrame(data), id="pandas"),
        pytest.param(lambda data: pa.table(data), id="arrow"),
    ],
)
def test_each_side_carries_the_type_it_established(build, tmp_path):
    a = dp.profile(build(BASELINE))
    b = dp.profile(build(CURRENT))
    expected = {
        "id": ("integer", "integer"),
        "code": ("integer", "string"),
        "blank": (None, "integer"),
        "gone": ("string", None),
        "new": (None, "string"),
    }
    wrong = _mismatches(_deltas(a, b, tmp_path), ("data_type_a", "data_type_b"), expected)
    assert not wrong, "\n".join(wrong)


def test_two_different_types_are_the_change_the_gate_reports():
    a = dp.profile(BASELINE)
    b = dp.profile(CURRENT)
    gate = b.check(max_null_percentage_increase={"id": 0.0}, baseline=a)
    retyped_by_gate = sorted(
        d["column"]
        for d in gate.to_dict()["baseline"]["diagnostics"]
        if d["code"] == "column_type_changed"
    )
    retyped_in_delta = sorted(
        name
        for name, entry in a.compare(b)["columns"].items()
        if None not in (entry["data_type_a"], entry["data_type_b"])
        and entry["data_type_a"] != entry["data_type_b"]
    )
    assert retyped_in_delta == retyped_by_gate == ["code"]


def test_each_side_carries_the_type_its_source_declared(tmp_path):
    # The same profiled type, ``float``, from two declarations; the
    # baseline's decimals are all null, which establishes no inferred type but
    # keeps the declaration. A dict declares nothing.
    decimal = pa.table({"amount": pa.array([None, None], type=pa.decimal128(12, 2))})
    double = pa.table({"amount": pa.array([12.5, 3.0], type=pa.float64())})
    as_pandas = pd.DataFrame({"amount": [12.5, 3.0]})
    a, b = dp.profile(decimal), dp.profile(double)
    plain = dp.profile({"amount": [12.5, 3.0]})

    arrow = {"system": "arrow", "name": "decimal128(12, 2)"}
    wrong = _mismatches(
        _deltas(a, b, tmp_path),
        ("native_type_a", "native_type_b"),
        {"amount": (arrow, {"system": "arrow", "name": "double"})},
    )
    wrong += _mismatches(
        _deltas(a, b, tmp_path), ("data_type_a", "data_type_b"), {"amount": (None, "float")}
    )
    wrong += _mismatches(
        _deltas(dp.profile(as_pandas), plain, tmp_path),
        ("native_type_a", "native_type_b"),
        {"amount": ({"system": "pandas", "name": "float64"}, None)},
    )
    assert not wrong, "\n".join(wrong)


DATA = {"id": [1, 2], "code": ["a", "b"], "note": ["x", "y"]}
ALL = {"selection": "all"}


def _listed(*names: str) -> dict[str, Any]:
    return {"selection": "listed", "names": list(names)}


@pytest.mark.parametrize(
    ("columns_a", "columns_b", "want_a", "want_b", "same"),
    [
        pytest.param(None, None, ALL, ALL, True, id="all/all"),
        # The order a list gives orders the columns and moves no score.
        pytest.param(
            ["id", "code"],
            ["code", "id"],
            _listed("id", "code"),
            _listed("code", "id"),
            True,
            id="listed/reordered",
        ),
        pytest.param(
            ["id", "code"], ["id"], _listed("id", "code"), _listed("id"), False, id="listed/fewer"
        ),
        # Only ``all`` follows the source's columns.
        pytest.param(
            None,
            ["id", "code", "note"],
            ALL,
            _listed("id", "code", "note"),
            False,
            id="all/every-listed",
        ),
    ],
)
def test_configuration_lists_each_sides_column_selection(
    columns_a, columns_b, want_a, want_b, same, tmp_path
):
    a = dp.profile(DATA, columns=columns_a)
    b = dp.profile(DATA, columns=columns_b)
    want = {"a": want_a, "b": want_b, "same": same}
    wrong = [
        f"{route}: {delta['configuration']['columns']!r} != {want!r}"
        for route, delta in _deltas(a, b, tmp_path)
        if delta["configuration"]["columns"] != want
    ]
    assert not wrong, "\n".join(wrong)


def test_a_summary_written_before_0_13_records_no_selection():
    report = dp.profile(DATA)
    summary = report.to_dict()
    del summary["analysis_settings"]
    legacy = dp.ProfileReport.from_dict(summary)
    assert legacy.compare(report)["configuration"]["columns"] == {"a": None, "b": ALL, "same": None}
