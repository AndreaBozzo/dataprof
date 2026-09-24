"""The same input profiles to the same report, on every input path.

A column with more than 10,000 numeric values takes its median, quartiles,
mode and shape statistics from a reservoir sample. The in-memory paths (bytes
and row dicts) drew that sample from an unseeded generator, so the same data
gave a different report on every run while the file paths did not.
"""

from __future__ import annotations

import json

import dataprof as dp
import pytest

ROWS = 15_000


def _csv_bytes() -> bytes:
    lines = ["id,amount"]
    lines += [f"{n},{(n * 7919) % 10_007 / 100}" for n in range(ROWS)]
    return ("\n".join(lines) + "\n").encode()


def _rows() -> list[dict[str, float]]:
    return [{"id": n, "amount": (n * 7919) % 10_007 / 100} for n in range(ROWS)]


def _serialized(report: dp.ProfileReport) -> str:
    document = report.to_dict()
    document.pop("execution")
    return json.dumps(document, sort_keys=True)


@pytest.mark.parametrize(
    "make",
    [
        pytest.param(lambda: dp.profile(_csv_bytes(), format="csv"), id="csv-bytes"),
        pytest.param(lambda: dp.profile(_rows()), id="row-dicts"),
    ],
)
def test_the_same_input_gives_the_same_report(make):
    first = make()
    # The sampled statistics are what differed; make sure this input reaches them.
    assert first["amount"].is_approximate is True
    assert all(_serialized(make()) == _serialized(first) for _ in range(3))
