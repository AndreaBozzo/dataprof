"""Numbers written with a decimal comma or digit grouping (#433).

dataprof does not parse ``1.234,56`` as a number, so a column of them is typed
``string`` with no numeric statistics, and nothing said why. The profile counts
them in ``locale_number_count``; the ``locale_numbers`` finding and a
``to_llm_context()`` flag name the column.

``tests/locale_numbers.rs`` is the Rust twin.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

try:
    import dataprof as dp
    from dataprof._columns import _column_record
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

#: Three columns as an Italian spreadsheet export writes them.
ROWS = [
    ("1.234,56", "10,50", "consegna rapida"),
    ("2.345,67", "20,00", "1,5"),
    ("9.876,54", "7,25", "fragile"),
    ("12.000,00", "3,10", "ok"),
    ("500,00", "1,99", "da verificare"),
    ("1.000.000,01", "0,99", "urgente"),
]
NAMES = ("importo", "prezzo", "note")

#: Per column: the count, and whether the finding reports it. ``note`` holds one
#: ``1,5`` among free text, too few of its text values to report.
EXPECTED = {"importo": (6, True), "prezzo": (6, True), "note": (1, False)}


def _columns() -> dict[str, list[str]]:
    return {name: [row[i] for row in ROWS] for i, name in enumerate(NAMES)}


@pytest.fixture
def csv_path(tmp_path: Path) -> Path:
    path = tmp_path / "export.csv"
    lines = [";".join(NAMES), *(";".join(row) for row in ROWS)]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _locale_findings(report: dp.ProfileReport) -> list[str]:
    return [f.column for f in report.findings() if f.code == "locale_numbers" and f.column]


def _assert_counted(report: dp.ProfileReport, label: str) -> None:
    for name, (count, _) in EXPECTED.items():
        column = report[name]
        assert column.data_type == "string", f"[{label}] {name}"
        assert column.locale_number_count == count, f"[{label}] {name}"
        assert column.type_homogeneity is not None
        assert count <= column.type_homogeneity["text"], f"[{label}] {name}"
    reported = [name for name, (_, found) in EXPECTED.items() if found]
    assert sorted(_locale_findings(report)) == sorted(reported), label


@pytest.mark.parametrize("engine", ["auto", "incremental", "columnar"])
def test_every_engine_counts_the_same_locale_numbers(csv_path: Path, engine: str):
    _assert_counted(dp.profile(str(csv_path), engine=engine), engine)


def test_pandas():
    pd = pytest.importorskip("pandas")
    _assert_counted(dp.profile(pd.DataFrame(_columns())), "pandas")


def test_polars():
    pl = pytest.importorskip("polars")
    _assert_counted(dp.profile(pl.DataFrame(_columns())), "polars")


def test_pyarrow():
    pa = pytest.importorskip("pyarrow")
    _assert_counted(dp.profile(pa.table(_columns())), "pyarrow")


def test_parquet(tmp_path: Path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "export.parquet"
    pq.write_table(pa.table(_columns()), path)
    _assert_counted(dp.profile(str(path)), "parquet")


def test_jsonl(tmp_path: Path):
    path = tmp_path / "export.jsonl"
    records = [dict(zip(NAMES, row, strict=True)) for row in ROWS]
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")
    _assert_counted(dp.profile(str(path)), "jsonl")


@pytest.mark.parametrize("layout", ["flat", "canonical"])
def test_the_count_survives_the_document_round_trip(csv_path: Path, layout: str):
    report = dp.profile(str(csv_path))
    document = report.to_dict() if layout == "flat" else json.loads(report.to_json())
    restored = dp.ProfileReport.from_dict(document)

    _assert_counted(restored, layout)
    assert restored.findings().to_dict() == report.findings().to_dict()


@pytest.mark.parametrize("layout", ["flat", "canonical"])
def test_a_report_from_before_the_count_does_not_read_as_clean(csv_path: Path, layout: str):
    report = dp.profile(str(csv_path))
    document: dict[str, Any] = (
        report.to_dict() if layout == "flat" else json.loads(report.to_json())
    )
    key = "columns" if layout == "flat" else "column_profiles"
    for column in document[key]:
        del column["locale_number_count"]

    legacy = dp.ProfileReport.from_dict(document)

    assert legacy["importo"].locale_number_count is None
    assert _locale_findings(legacy) == []
    assert {
        "code": "locale_numbers",
        "reason": "unrecorded",
        "columns": list(NAMES),
    } in legacy.findings().not_evaluated


def test_an_identifier_column_is_exempt(csv_path: Path):
    report = dp.profile(str(csv_path), identifier_columns=["importo"])

    assert report["importo"].data_type == "identifier"
    assert report["importo"].locale_number_count == 6
    assert _locale_findings(report) == ["prezzo"]


def test_half_of_the_text_values_is_enough(tmp_path: Path):
    path = tmp_path / "half.csv"
    path.write_text("v\n1,5\nabc\n", encoding="utf-8")
    assert _locale_findings(dp.profile(str(path))) == ["v"]

    path.write_text("v\n1,5\nabc\ndef\n", encoding="utf-8")
    assert _locale_findings(dp.profile(str(path))) == []


def test_a_numeric_column_reports_the_decimal_commas_it_left_out(tmp_path: Path):
    path = tmp_path / "peso.csv"
    path.write_text("peso\n1.5\n2.5\n3,5\n4.5\n5.5\n6.5\n", encoding="utf-8")

    report = dp.profile(str(path))

    assert report["peso"].data_type == "float"
    assert report["peso"].invalid_count == 1
    assert report["peso"].locale_number_count == 1
    assert _locale_findings(report) == ["peso"]


def test_the_flat_exports_carry_the_count(csv_path: Path):
    # The record behind to_dataframe(), to_polars(), to_arrow() and save(".csv").
    record = _column_record(dp.profile(str(csv_path))["importo"])
    assert record["locale_number_count"] == 6


def test_the_llm_context_flags_the_column(csv_path: Path):
    context = dp.profile(str(csv_path)).to_llm_context()
    flags = context.split("\nflags (", 1)[1].split("\n\n", 1)[0].splitlines()[1:]

    assert flags == [
        "- importo: 6 of 6 values are numbers with a decimal comma or digit "
        "grouping, left out of numeric stats",
        "- prezzo: 6 of 6 values are numbers with a decimal comma or digit "
        "grouping, left out of numeric stats",
    ]
    # Counts only: no cell value reaches the agent-facing summary.
    assert "1.234,56" not in context


def test_both_summary_backings_write_the_count(csv_path: Path):
    """``to_dict()`` comes from the Rust summary on a native report and from
    ``column_to_dict`` on one loaded from a flat document; both must carry it.
    """
    report = dp.profile(str(csv_path))
    native = report.to_dict()
    reloaded = dp.ProfileReport.from_dict(native).to_dict()

    assert native["columns"][0]["locale_number_count"] == 6
    assert reloaded == native
    assert dp.column_to_dict(report["importo"])["locale_number_count"] == 6
