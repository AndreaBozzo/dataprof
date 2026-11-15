"""A sampled accuracy score's bounds cover a full scan's outlier fences (#1304).

`tests/sampled_outlier_fences.rs` is the Rust twin. Here every route that
keeps a sample (file engines, bytes, pandas, Arrow) is held to the verdict of
the same values in a dict, which keeps every value, and so is a report read
back from ``to_json()``.

A column of zeros and fives with a share of zeros near 75% has its third
quartile at 0 or 5 depending on a handful of values, so a sample can land on
either side of the full scan.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import dataprof as dp
import pytest

pa = pytest.importorskip("pyarrow")
pd = pytest.importorskip("pandas")

ROWS = 40_000
# Each lays the values out in another order: primes, coprime with ROWS.
ORDERS = (7_919, 104_729, 1_299_709)
# (zeros, value added to each): near the boundary on both sides, and half
# zeros far from it; the same past 2**53, where fences compare exactly.
CASES = [(zeros, base) for base in (0, 2**60) for zeros in (29_992, 30_008, 20_000)]


def _values(zeros: int, base: int, order: int) -> list[int]:
    return [base + (0 if (row * order) % ROWS < zeros else 5) for row in range(ROWS)]


def _csv(values: list[int]) -> str:
    return "v\n" + "\n".join(map(str, values)) + "\n"


def _routes(tmp_path: Path) -> dict[str, Callable[[list[int]], dp.ProfileReport]]:
    def file(engine: str, **kwargs: Any) -> Callable[[list[int]], dp.ProfileReport]:
        def run(values: list[int]) -> dp.ProfileReport:
            path = tmp_path / f"{engine}.csv"
            path.write_text(_csv(values))
            return dp.profile(str(path), engine=engine, **kwargs)

        return run

    return {
        "csv auto": file("auto"),
        "csv incremental": file("incremental"),
        "csv columnar": file("columnar"),
        "csv auto quality only": file("auto", metrics=["quality"]),
        "csv bytes": lambda values: dp.profile(_csv(values).encode(), format="csv"),
        "pandas": lambda values: dp.profile(pd.DataFrame({"v": values})),
        "arrow": lambda values: dp.profile(pa.table({"v": values})),
    }


def _verdict(report: dp.ProfileReport) -> str:
    return report.check(min_dimension_scores={"accuracy": 90}).verdict


def test_every_sampled_route_gives_the_verdict_of_the_dict_or_none(tmp_path: Path) -> None:
    wrong = []
    for zeros, base in CASES:
        for order in ORDERS:
            values = _values(zeros, base, order)
            expected = _verdict(dp.profile({"v": values}))
            # More than 75% zeros: the quartiles coincide, the column is not
            # tested and accuracy is unassessed; otherwise it scores 100.
            assert expected == ("inconclusive" if zeros > 30_000 else "pass"), (zeros, base)
            for route, run in _routes(tmp_path).items():
                report = run(values)
                saved = dp.ProfileReport.from_json(report.to_json())
                for form, got in (("run", _verdict(report)), ("saved", _verdict(saved))):
                    near_boundary = zeros != 20_000
                    if got != expected and not (near_boundary and got == "inconclusive"):
                        case = f"{zeros} zeros, base {base}, order {order}"
                        wrong.append(f"{case} [{route}, {form}]: {got}, dict {expected}")
    assert not wrong, "\n".join(wrong)


def test_a_column_whose_quartiles_coincide_is_not_tested_for_outliers() -> None:
    # No other component applies, so the dimension is unassessed.
    quality = json.loads(dp.profile({"v": _values(30_008, 0, ORDERS[0])}).to_json())["quality"]
    assert "accuracy" not in quality["metrics"]
    assert quality["scores"]["dimension_scores"]["accuracy"] is None
    # One zero fewer than 75% and the column is tested, with no outlier.
    quality = json.loads(dp.profile({"v": _values(29_999, 0, ORDERS[0])}).to_json())["quality"]
    assert quality["metrics"]["accuracy"]["outlier_values_checked"] == ROWS
    assert quality["scores"]["dimension_scores"]["accuracy"] == 100.0
    semantics = dp.profile({"v": [1, 2]}).metric_semantics
    assert semantics is not None
    assert semantics["outlier_spread"] == "nonzero_iqr"
