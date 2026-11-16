"""The gate's per-column uniqueness rule, ``unique_columns`` (#904).

`tests/unique_columns_gate.rs` is the Rust twin. Here the extra routes are the
Python ones: dicts, pandas, polars and Arrow inputs, CSV bytes, a report read
back from ``to_json()``, and the ``python -m dataprof.check`` flag and
policy-file key.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import dataprof as dp
import pytest
from dataprof.check import __main__ as entrypoint

pa = pytest.importorskip("pyarrow")
pd = pytest.importorskip("pandas")
pl = pytest.importorskip("polars")

ROWS = 20
#: Rows from this index reuse the keys of the first rows.
REUSED_FROM = 17
NULL_NOTES = (3, 7)


def _columns() -> dict[str, list[Any]]:
    keys = [i - REUSED_FROM if i >= REUSED_FROM else i for i in range(ROWS)]
    return {
        "row_id": list(range(ROWS)),
        "order_id": [f"O{key}" for key in keys],
        "order_no": [key + 100 for key in keys],
        "note": [None if i in NULL_NOTES else f"n{i}" for i in range(ROWS)],
        "amount": [i % 7 for i in range(ROWS)],
    }


def _csv() -> bytes:
    columns = _columns()
    lines = [",".join(columns)]
    for i in range(ROWS):
        lines.append(",".join("" if v[i] is None else str(v[i]) for v in columns.values()))
    return ("\n".join(lines) + "\n").encode()


ROUTES: dict[str, Callable[[], dp.ProfileReport]] = {
    "dict": lambda: dp.profile(_columns()),
    "pandas": lambda: dp.profile(pd.DataFrame(_columns())),
    "polars": lambda: dp.profile(pl.DataFrame(_columns())),
    "arrow": lambda: dp.profile(pa.table(_columns())),
    "csv bytes": lambda: dp.profile(_csv(), format="csv"),
}

#: Each column with the status and observed repeats the rule must reach.
#: ``note`` holds two nulls and no repeated value.
EXPECTED = [
    ("note", "passed", 0),
    ("order_id", "failed", ROWS - REUSED_FROM),
    ("order_no", "failed", ROWS - REUSED_FROM),
    ("row_id", "passed", 0),
]


def _status(check: dp.QualityCheck) -> str:
    return check.status if check.reason is None else check.reason["reason"]


def test_a_reused_key_fails_on_every_route_while_whole_rows_stay_distinct():
    wrong = []
    for route, build in ROUTES.items():
        report = build()
        # The case the ticket is about: no whole row repeats.
        rows = report.check(max_duplicate_rows=0)
        if rows.verdict != "pass":
            wrong.append(f"{route}: max_duplicate_rows {rows.verdict}")
        for form, candidate in (
            ("live", report),
            ("to_json", dp.ProfileReport.from_json(report.to_json())),
        ):
            result = candidate.check(unique_columns=["row_id", "order_id", "order_no", "note"])
            got = [
                (check.code, check.column, _status(check), check.observed)
                for check in result.checks
            ]
            expected = [("unique_column", *entry) for entry in EXPECTED]
            if got != expected:
                wrong.append(f"{route}/{form}: {got}")
            if result.verdict != "fail":
                wrong.append(f"{route}/{form}: verdict {result.verdict}")
    assert not wrong, "\n".join(wrong)


def test_the_ticket_reproduction_fails():
    """The reproduction in #904, without DuckDB: 1000 rows, ten keys reused."""
    order_id = [f"O{i - 990 if i >= 990 else i}" for i in range(1000)]
    report = dp.profile(
        pa.table({"order_id": order_id, "amount": [float(i % 7) for i in range(1000)]})
    )
    assert report.check(max_duplicate_rows=0).verdict == "pass"
    result = report.check(unique_columns=["order_id"])
    assert result.verdict == "fail"
    assert result.checks[0].observed == 10


def test_an_estimated_or_unrecorded_count_is_never_decided():
    report = ROUTES["csv bytes"]()
    wrong = []
    for case in ("estimated", "unrecorded"):
        for column in ("order_id", "row_id"):
            document = json.loads(report.to_json())
            (profile,) = [c for c in document["column_profiles"] if c["name"] == column]
            if case == "estimated":
                profile["unique_count_is_approximate"] = True
            else:
                profile.pop("unique_count_is_approximate")
            loaded = dp.ProfileReport.from_json(json.dumps(document))
            (check,) = loaded.check(unique_columns=[column]).checks
            if _status(check) != "approximate_count" or check.observed is not None:
                wrong.append(f"{case}/{column}: {_status(check)}, {check.observed}")
    assert not wrong, "\n".join(wrong)


def test_validation_names_the_keyword():
    report = ROUTES["dict"]()
    for bad in ("order_id", [1], {"order_id": 0}):
        with pytest.raises(ValueError, match="unique_columns must be a list of column names"):
            report.check(unique_columns=bad)  # ty: ignore[invalid-argument-type]
    # Naming a column is a requirement on its own.
    assert report.check(unique_columns=["row_id"]).verdict == "pass"


def test_the_check_entrypoint_flag_and_policy_key(tmp_path: Path):
    source = tmp_path / "orders.csv"
    source.write_bytes(_csv())
    assert entrypoint.main([str(source), "--unique-column", "order_id"]) == 1
    assert entrypoint.main([str(source), "--unique-column", "row_id"]) == 0
    assert entrypoint.main([str(source), "--max-duplicate-rows", "0"]) == 0
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"unique_columns": ["note", "order_no"]}), encoding="utf-8")
    assert entrypoint.main([str(source), "--policy", str(policy)]) == 1
    # A flag replaces the policy-file key in full.
    assert entrypoint.main([str(source), "--policy", str(policy), "--unique-column", "note"]) == 0
    policy.write_text(json.dumps({"unique_columns": "order_id"}), encoding="utf-8")
    assert entrypoint.main([str(source), "--policy", str(policy)]) == 3
