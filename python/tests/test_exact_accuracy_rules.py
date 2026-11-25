"""Accuracy rules and value-driven semantic-hint bindings over every scanned value (#788 step 3).

At 40,000 rows with range/sign violations and rare hint-binding matches placed
past the 10,000-value quality sample, every route (dict, bytes, CSV engines,
JSON/JSONL, Parquet file/bytes, Arrow, Arrow C stream, pandas, polars)
serializes equal accuracy rule counts (``rule_values_checked``,
``rule_violating_values``, ``range_violations``,
``negative_values_in_positive``, ``numeric_values_checked``) and equal
``semantic_hint_bindings``.

When no outlier-eligible numeric column is sampled, ``accuracy`` moves from
``sampled_dimensions`` to ``exact_dimensions`` and every route serializes the
same exact accuracy score.

``tests/exact_accuracy_rules.rs`` is the Rust twin.
"""

from __future__ import annotations

import json
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

ROWS = 40_000
POSITIVE_COLUMNS = ["discount_percent", "balance", "mixed_positive"]
TEMPORAL_COLUMNS = ["event_when"]


def _columns() -> dict[str, list[str]]:
    # 1. Name-based age rule (0..=150): 200 violations past row 39,800.
    customer_age = [str(20 + (i % 40)) for i in range(ROWS)]
    for i in range(39_800, 39_900):
        customer_age[i] = "-5"
    for i in range(39_900, ROWS):
        customer_age[i] = "200"

    # 2. Overlapping name-based percent rule (0..=100) + positive_columns hint:
    #    100 negative values (-10 -> breaks both rules, 1 violating value) and
    #    100 values just above 100 (100.000000000000000001 -> exact decimal
    #    bound comparison) placed past row 39,800.
    discount_percent = [str(10 + (i % 80)) for i in range(ROWS)]
    for i in range(39_800, 39_900):
        discount_percent[i] = "-10"
    for i in range(39_900, ROWS):
        discount_percent[i] = "100.000000000000000001"

    # 3. Name-based count rule (>= 0): 150 negative values past row 39,850.
    item_count = [str(i % 500) for i in range(ROWS)]
    for i in range(39_850, ROWS):
        item_count[i] = "-1"

    # 4. Name-based year rule (1900..=2100): 100 out-of-range values past 39,900.
    birth_year = [str(1950 + (i % 70)) for i in range(ROWS)]
    for i in range(39_900, 39_950):
        birth_year[i] = "1850"
    for i in range(39_950, ROWS):
        birth_year[i] = "2150"

    # 5. Sign-only positive_columns hint: 50 subnormal negative values past 39,950.
    balance = [str(100 + (i % 800)) for i in range(ROWS)]
    for i in range(39_950, ROWS):
        balance[i] = "-1e-400"

    # 6. Unruled numeric column: contributes to numeric_values_checked only.
    price = [str(10 + (i % 90)) for i in range(ROWS)]

    # 7. Mixed text column hinted positive_columns: only 5 numeric values (all
    #    negative) placed past row 39,995. Must bind (not raise inert) and add
    #    5 checked / 5 violating values across every route.
    mixed_positive = ["not_a_number"] * ROWS
    for i in range(39_995, ROWS):
        mixed_positive[i] = "-5"

    # 8. Mixed text column hinted temporal_columns: only 5 dates placed past
    #    row 39,995. Must bind with matched_values=5, exact=True across routes.
    event_when = ["not_a_date"] * ROWS
    for i in range(39_995, ROWS):
        event_when[i] = "2024-06-15"

    return {
        "customer_age": customer_age,
        "discount_percent": discount_percent,
        "item_count": item_count,
        "birth_year": birth_year,
        "balance": balance,
        "price": price,
        "mixed_positive": mixed_positive,
        "event_when": event_when,
    }


def _csv_text(columns: dict[str, list[str]]) -> str:
    names = list(columns)
    rows = zip(*(columns[name] for name in names), strict=True)
    return ",".join(names) + "\n" + "".join(",".join(row) + "\n" for row in rows)


def _json_lines(columns: dict[str, list[str]]) -> list[str]:
    names = list(columns)
    return [
        json.dumps(dict(zip(names, row, strict=True)), ensure_ascii=False)
        for row in zip(*(columns[name] for name in names), strict=True)
    ]


def _routes(
    tmp_path: Path,
    columns: dict[str, list[str]],
    *,
    positive_columns: list[str],
    temporal_columns: list[str],
) -> dict[str, Callable[[], dp.ProfileReport]]:
    csv_text = _csv_text(columns)
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(csv_text, encoding="utf-8")

    lines = _json_lines(columns)
    jsonl_text = "\n".join(lines) + "\n"
    json_text = "[" + ",".join(lines) + "]"
    jsonl_path = tmp_path / "data.jsonl"
    jsonl_path.write_text(jsonl_text, encoding="utf-8")
    json_path = tmp_path / "data.json"
    json_path.write_text(json_text, encoding="utf-8")

    hints: dict[str, Any] = {
        "positive_columns": positive_columns,
        "temporal_columns": temporal_columns,
    }
    routes: dict[str, Callable[[], dp.ProfileReport]] = {
        "dict": lambda: dp.profile(columns, **hints),
        "csv_bytes": lambda: dp.profile(csv_text.encode("utf-8"), format="csv", **hints),
        "jsonl_bytes": lambda: dp.profile(jsonl_text.encode("utf-8"), format="jsonl", **hints),
        "json_bytes": lambda: dp.profile(json_text.encode("utf-8"), format="json", **hints),
        "file_json": lambda: dp.profile(str(json_path), **hints),
        "file_jsonl": lambda: dp.profile(str(jsonl_path), **hints),
    }
    for engine in ("auto", "incremental", "columnar"):
        routes[f"file_{engine}"] = lambda engine=engine: dp.profile(
            str(csv_path), engine=engine, **hints
        )

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.table(columns)
        pq_path = tmp_path / "data.parquet"
        pq.write_table(table, pq_path)
        pq_bytes = pq_path.read_bytes()
        routes["parquet"] = lambda: dp.profile(str(pq_path), **hints)
        routes["parquet_bytes"] = lambda: dp.profile(pq_bytes, format="parquet", **hints)
        routes["arrow"] = lambda: dp.profile(table, **hints)
        routes["arrow_stream"] = lambda: dp.profile(
            pa.RecordBatchReader.from_batches(table.schema, table.to_batches()), **hints
        )
    except ImportError:
        pass

    try:
        import pandas as pd

        routes["pandas"] = lambda: dp.profile(pd.DataFrame(columns), **hints)
    except ImportError:
        pass

    try:
        import polars as pl

        routes["polars"] = lambda: dp.profile(pl.DataFrame(columns), **hints)
    except ImportError:
        pass

    return routes


def test_every_route_matches_accuracy_rules_and_hint_bindings_at_40k_rows(
    tmp_path: Path,
) -> None:
    wrong: list[str] = []
    expected_rules = {
        # 5 ruled numeric columns * 40,000 + 5 numeric cells in mixed_positive.
        "rule_values_checked": 200_005,
        # 200 (age) + 200 (percent, -10 counted once) + 150 (count) + 100 (year)
        # + 50 (balance) + 5 (mixed_positive).
        "rule_violating_values": 705,
        # 200 (age) + 200 (percent) + 150 (count) + 100 (year).
        "range_violations": 650,
        # 100 (discount_percent) + 50 (balance) + 5 (mixed_positive).
        "negative_values_in_positive": 155,
        # 6 numeric columns * 40,000 + 5 numeric cells in mixed_positive.
        "numeric_values_checked": 240_005,
    }
    expected_bindings = [
        {
            "column": "discount_percent",
            "kind": "positive",
            "checked_values": ROWS,
            "matched_values": ROWS,
            "exact": True,
        },
        {
            "column": "balance",
            "kind": "positive",
            "checked_values": ROWS,
            "matched_values": ROWS,
            "exact": True,
        },
        {
            "column": "mixed_positive",
            "kind": "positive",
            "checked_values": ROWS,
            "matched_values": 5,
            "exact": True,
        },
        {
            "column": "event_when",
            "kind": "temporal",
            "checked_values": ROWS,
            "matched_values": 5,
            "exact": True,
        },
    ]

    for label, run in _routes(
        tmp_path,
        _columns(),
        positive_columns=POSITIVE_COLUMNS,
        temporal_columns=TEMPORAL_COLUMNS,
    ).items():
        report = run()
        doc = json.loads(report.to_json())
        summary = report.to_dict()
        restored = dp.ProfileReport.from_dict(summary)

        semantics = report.metric_semantics or {}
        if semantics.get("value_facts") != "every_value_consistency_precision_accuracy_rules":
            wrong.append(f"[{label}] value_facts={semantics.get('value_facts')!r}")

        acc = doc["quality"]["metrics"]["accuracy"]
        got_rules = {k: acc[k] for k in expected_rules}
        if got_rules != expected_rules:
            wrong.append(f"[{label}] rules={got_rules} != {expected_rules}")

        for form, checked_report in (("live", report), ("summary", restored)):
            if checked_report.semantic_hint_bindings != expected_bindings:
                got_b = checked_report.semantic_hint_bindings
                wrong.append(f"[{label}:{form}] bindings={got_b} != {expected_bindings}")

    assert not wrong, "\n".join(wrong)


def test_ruled_only_non_numeric_columns_move_accuracy_to_exact_dimensions(
    tmp_path: Path,
) -> None:
    """When no column is eligible for the outlier test, accuracy has no sampled
    component and leaves ``sampled_dimensions`` across every route."""
    mixed_positive = ["not_a_number"] * ROWS
    for i in range(39_900, 39_950):
        mixed_positive[i] = "10"
    for i in range(39_950, ROWS):
        mixed_positive[i] = "-5"

    columns = {"mixed_positive": mixed_positive}
    wrong: list[str] = []
    expected_accuracy_score = 50.0

    for label, run in _routes(
        tmp_path,
        columns,
        positive_columns=["mixed_positive"],
        temporal_columns=[],
    ).items():
        report = run()
        doc = json.loads(report.to_json())
        acc = doc["quality"]["metrics"]["accuracy"]
        score = doc["quality"]["scores"]["dimension_scores"]["accuracy"]

        if (acc["rule_values_checked"], acc["rule_violating_values"]) != (100, 50):
            wrong.append(
                f"[{label}] rules={(acc['rule_values_checked'], acc['rule_violating_values'])}"
            )
        if score != expected_accuracy_score:
            wrong.append(f"[{label}] accuracy_score={score} != {expected_accuracy_score}")

        sampled = report.quality_sampled_dimensions or []
        exact = report.to_dict()["quality"].get("exact_dimensions", [])
        if "accuracy" in sampled:
            wrong.append(f"[{label}] accuracy still in sampled_dimensions={sampled}")
        if "accuracy" not in exact:
            wrong.append(f"[{label}] accuracy missing from exact_dimensions={exact}")

    assert not wrong, "\n".join(wrong)


def test_truncated_run_marks_hint_bindings_non_exact_on_dict_and_file(tmp_path: Path) -> None:
    """Truncating with ``max_rows`` leaves unread rows, so neither dict nor
    file routes mark a zero-match hint binding as proven inert."""
    rows = ["text"] * 10 + ["1"]
    csv_path = tmp_path / "trunc.csv"
    csv_path.write_text("value\n" + "\n".join(rows) + "\n", encoding="utf-8")

    for label, report in (
        ("dict", dp.profile({"value": rows}, max_rows=5, positive_columns=["value"])),
        ("file", dp.profile(str(csv_path), max_rows=5, positive_columns=["value"])),
    ):
        bindings = report.semantic_hint_bindings
        assert len(bindings) == 1, label
        assert bindings[0] == {
            "column": "value",
            "kind": "positive",
            "checked_values": 5,
            "matched_values": 0,
            "exact": False,
        }, label
