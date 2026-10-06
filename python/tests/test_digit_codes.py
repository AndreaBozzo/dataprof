"""Codes written in digits are not quantities (#814).

Zero-padded codes and ``YYYYMMDD`` dates parse as integers, so they were typed
``integer`` and given a mean: postal codes averaged, leading zeros gone from
every statistic. A column with a value that keeps a leading zero, or whose
numeric values are all ``YYYYMMDD`` dates, is now typed ``string``.

``tests/digit_codes.rs`` is the Rust twin.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

try:
    import dataprof as dp
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

NAMES = (
    "leading_zero",
    "es_postal",
    "compact_date",
    "departure_time",
    "quantity",
    "amount_cents",
)

#: The three columns from #814, HHMM times in a column named like a date, then
#: two controls: a count that includes a bare ``0``, and eight-digit amounts that
#: are not calendar dates.
ROWS = [
    ("00123", "28013", "20240115", "0930", "3", "12345678"),
    ("00456", "08001", "20240216", "1415", "12", "23456789"),
    ("00789", "41001", "20240317", "0805", "0", "34567891"),
    ("01234", "46001", "20240418", "2210", "25", "45678912"),
    ("05678", "48001", "20240519", "1130", "7", "56789123"),
    ("09999", "50001", "20240620", "0645", "100", "67891234"),
]

EXPECTED = {
    "leading_zero": "string",
    "es_postal": "string",
    "compact_date": "string",
    "departure_time": "string",
    "quantity": "integer",
    "amount_cents": "integer",
}


def _columns() -> dict[str, list[str]]:
    return {name: [row[i] for row in ROWS] for i, name in enumerate(NAMES)}


@pytest.fixture
def csv_path(tmp_path: Path) -> Path:
    path = tmp_path / "codes.csv"
    lines = [",".join(NAMES), *(",".join(row) for row in ROWS)]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _assert_typed(report: dp.ProfileReport, label: str) -> None:
    for name, expected in EXPECTED.items():
        column = report[name]
        assert column.data_type == expected, f"[{label}] {name}"
        if expected == "string":
            assert column.mean is None, f"[{label}] {name} has a mean"
        else:
            assert column.mean is not None, f"[{label}] {name} lost its mean"
    # `compact_date` and `departure_time` are held to date forms by their names.
    # They were scored as numbers before, and retyping them costs no consistency.
    assert report.quality is not None
    assert report.quality.dimension_scores()["consistency"] == 100.0, label


@pytest.mark.parametrize("engine", ["auto", "incremental", "columnar"])
def test_every_engine_types_digit_codes_as_text(csv_path: Path, engine: str):
    _assert_typed(dp.profile(str(csv_path), engine=engine), engine)


def test_dict_of_strings():
    _assert_typed(dp.profile(_columns()), "dict")


def test_pandas():
    pd = pytest.importorskip("pandas")
    _assert_typed(dp.profile(pd.DataFrame(_columns())), "pandas")


def test_polars():
    pl = pytest.importorskip("polars")
    _assert_typed(dp.profile(pl.DataFrame(_columns())), "polars")


def test_pyarrow():
    pa = pytest.importorskip("pyarrow")
    _assert_typed(dp.profile(pa.table(_columns())), "pyarrow")


def test_jsonl(tmp_path: Path):
    path = tmp_path / "codes.jsonl"
    records = [dict(zip(NAMES, row, strict=True)) for row in ROWS]
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")
    _assert_typed(dp.profile(str(path)), "jsonl")


def test_a_declared_integer_column_keeps_its_type():
    """A source that declares integers is not re-inferred from its values.

    An Arrow ``int64`` column cannot hold a leading zero, and an ``int64`` of
    ``YYYYMMDD`` values is the source's own typing. Recognizing those as date
    candidates belongs to #815; this pins the boundary until then.
    """
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"date_key": pa.array([20240115, 20240216, 20240317], pa.int64())})
    column = dp.profile(table)["date_key"]
    assert column.data_type == "integer"
    assert column.mean is not None


@pytest.mark.parametrize("layout", ["flat", "canonical"])
def test_the_type_survives_the_document_round_trip(csv_path: Path, layout: str):
    report = dp.profile(str(csv_path))
    document = report.to_dict() if layout == "flat" else json.loads(report.to_json())
    _assert_typed(dp.ProfileReport.from_dict(document), layout)
