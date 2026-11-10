"""A column declared a decimal is read as one whether or not statistics run
(#1268).

The accuracy pass told a decimal column apart only by the exact block of its
statistics, so a run with ``metrics=["quality"]`` read an Arrow decimal as
text, and the float fences missed an outlier the exact ones find: the same
data reported an ``outlier_ratio`` of 0 without statistics and 12.5 with them.
The column's declared type, recorded since #906, now decides both runs.

``CENTS`` is ``9999999999999 + cents / 100``; its lowest value lies 0.00375
below the lower fence, past what float fences resolve. Each route is read in
both dialects: ``to_dict()`` and the ``to_json()`` document.

``tests/declared_decimals.rs`` is the Rust twin, and
``crates/dataprof-db/tests/declared_decimals.rs`` covers PostgreSQL ``NUMERIC``
and MySQL ``DECIMAL``.
"""

from __future__ import annotations

import json
from collections.abc import Callable
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

CENTS = [
    Decimal("9999999999999") + Decimal(c) / 100 for c in (80, 173, 195, 209, 222, 255, 284, 331)
]

PACKS: list[tuple[str, dict[str, Any]]] = [
    ("every pack", {}),
    ("quality only", {"metrics": ["quality"]}),
]


def _ratios(report: Any) -> dict[str, Any]:
    document = json.loads(report.to_json())
    return {
        "to_dict": report.to_dict()["quality"]["accuracy"]["outlier_ratio"],
        "document": document["quality"]["metrics"]["accuracy"]["outlier_ratio"],
    }


def _routes(tmp_path: Path) -> dict[str, Callable[[], Any]]:
    import pyarrow.parquet as pq

    def table(width: int) -> Any:
        return pa.table({"v": pa.array(CENTS, getattr(pa, f"decimal{width}")(16, 2))})

    def parquet_file() -> str:
        path = tmp_path / "cents.parquet"
        pq.write_table(table(128), path)
        return str(path)

    routes: dict[str, Callable[[], Any]] = {
        f"arrow decimal{width}": (lambda width=width: table(width)) for width in (64, 128, 256)
    }
    routes["parquet"] = parquet_file
    try:
        import pandas as pd

        routes["pandas decimal[pyarrow]"] = lambda: pd.DataFrame(
            {"v": pd.array(CENTS, dtype=pd.ArrowDtype(pa.decimal128(16, 2)))}
        )
    except ImportError:
        pass
    try:
        import polars as pl

        routes["polars"] = lambda: pl.DataFrame({"v": pl.Series(CENTS, dtype=pl.Decimal(16, 2))})
    except ImportError:
        pass
    return routes


def test_a_decimal_column_reads_as_a_decimal_with_or_without_statistics(tmp_path: Path):
    wrong = []
    for route, source in _routes(tmp_path).items():
        for packs, options in PACKS:
            for dialect, ratio in _ratios(dp.profile(source(), **options)).items():
                if ratio != 12.5:
                    wrong.append(f"[{route}, {packs}, {dialect}] {ratio}")
    assert not wrong, "\n".join(wrong)


def test_a_pandas_object_column_of_decimals_declares_no_decimal():
    # Pins a known gap, tracked on #884: an `object` column declares no type, though
    # pyarrow reads its `Decimal` values into a decimal, which the statistics
    # read exactly. A run without statistics has no declaration to go by and
    # reads the values as text, so the float fences miss the outlier.
    pd = pytest.importorskip("pandas")
    frame = pd.DataFrame({"v": pd.Series(CENTS, dtype=object)})
    observed = {packs: _ratios(dp.profile(frame, **options))["to_dict"] for packs, options in PACKS}
    assert observed == {"every pack": 12.5, "quality only": 0.0}
