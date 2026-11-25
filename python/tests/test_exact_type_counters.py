"""A column's type and lexical forms are counted over every value read (#788).

A file, pandas and Arrow keep 10,000 values of each column; a dict and CSV
bytes hold every value. A text column used to be typed and classified from the
kept values, so one zero-padded code outside them, or a share at a type
threshold, typed the same data otherwise by route. Every route now reports the
type, ``type_homogeneity``, ``locale_number_count`` and ``zero_padded_count`` of
every value read. Zero-padded values make digit codes from 1% of the numbers,
so one stray code no longer retypes a column of quantities.

``tests/exact_type_counters.rs`` is the Rust twin.
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

ROWS = 40_000
#: Laid out in blocks, then read in this order: coprime with ``ROWS``.
ORDER = 7_919


def _placed(blocks: list[tuple[str, int]]) -> list[str]:
    values = [value for value, count in blocks for _ in range(count)]
    assert len(values) == ROWS
    return [values[(row * ORDER) % ROWS] for row in range(ROWS)]


def _forms(numeric: int = 0, date: int = 0, boolean: int = 0, text: int = 0) -> dict[str, int]:
    return {"numeric": numeric, "date": date, "boolean": boolean, "text": text}


def _columns() -> dict[str, list[str]]:
    codes = [str(1 + row % 900) for row in range(ROWS)]
    codes[30_001] = "007"
    notes = [f"item {row}" for row in range(ROWS)]
    notes[35_000] = "1.234,56"
    notes[39_999] = "10,50"
    return {
        "code": codes,
        "postcode": _placed([("10121", 39_600), ("00184", 400)]),
        "numeric_share": _placed([("12", 32_000), ("n/a?", 8_000)]),
        "date_share": _placed([("2024-03-01", 28_000), ("soon", 12_000)]),
        "boolean_share": _placed([("true", 36_000), ("maybe", 4_000)]),
        "note": notes,
    }


#: Per column: the type, the lexical forms, the locale numbers and the
#: zero-padded numbers of every value. One stray ``007`` leaves the integers
#: integers; 1% zero-padded postal codes are codes; exactly 80% numbers and
#: exactly 70% dates fall short of their types; exactly 90% booleans reach
#: theirs.
EXPECTED: dict[str, tuple[str, dict[str, int], int, int]] = {
    "code": ("integer", _forms(numeric=ROWS), 0, 1),
    "postcode": ("string", _forms(numeric=ROWS), 0, 400),
    "numeric_share": ("string", _forms(numeric=32_000, text=8_000), 0, 0),
    "date_share": ("string", _forms(date=28_000, text=12_000), 0, 0),
    "boolean_share": ("boolean", _forms(boolean=36_000, text=4_000), 0, 0),
    "note": ("string", _forms(text=ROWS), 2, 0),
}


def _csv(columns: dict[str, list[str]]) -> str:
    names = list(columns)
    rows = zip(*(columns[name] for name in names), strict=True)
    return ";".join(names) + "\n" + "".join(";".join(row) + "\n" for row in rows)


def _routes(tmp_path: Path) -> dict[str, Callable[[], Any]]:
    columns = _columns()
    text = _csv(columns)
    path = tmp_path / "data.csv"
    path.write_text(text, encoding="utf-8")
    routes: dict[str, Callable[[], Any]] = {
        "dict": lambda: dp.profile(columns),
        "bytes": lambda: dp.profile(text.encode(), format="csv"),
    }
    for engine in ("auto", "incremental", "columnar"):
        routes[f"file {engine}"] = lambda engine=engine: dp.profile(str(path), engine=engine)
    try:
        import pandas as pd

        routes["pandas"] = lambda: dp.profile(pd.DataFrame(columns))
    except ImportError:
        pass
    try:
        import pyarrow as pa

        routes["arrow"] = lambda: dp.profile(pa.table(columns))
    except ImportError:
        pass
    return routes


def test_every_route_types_and_classifies_every_value(tmp_path: Path) -> None:
    wrong = []
    for label, run in _routes(tmp_path).items():
        report = run()
        semantics = report.metric_semantics or {}
        if semantics.get("value_facts") != "every_value_consistency_precision_accuracy_rules":
            wrong.append(f"[{label}] value_facts {semantics.get('value_facts')!r}")
        saved = dp.ProfileReport.from_dict(report.to_dict())
        for name, want in EXPECTED.items():
            for form, got_report in (("run", report), ("saved", saved)):
                column = got_report[name]
                got = (
                    column.data_type,
                    column.type_homogeneity,
                    column.locale_number_count,
                    column.zero_padded_count,
                )
                if got != want:
                    wrong.append(f"[{label} {form}] {name}: {got}, want {want}")
    assert not wrong, "\n".join(wrong)


def test_mixed_types_is_decided_on_a_file_read_in_full(tmp_path: Path) -> None:
    # The forms now cover every value, so a full scan of a file larger than
    # the sample decides the rule instead of leaving it open (#788).
    path = tmp_path / "data.csv"
    path.write_text(_csv(_columns()), encoding="utf-8")
    report = dp.profile(str(path))
    for code in ("mixed_types", "locale_numbers"):
        verdict = report.check(forbid_findings=[code]).verdict
        assert verdict in {"pass", "fail"}, (code, verdict)
    # Patterns still come from the sample, so that rule stays open.
    assert report.check(forbid_findings=["sensitive_pattern"]).verdict == "inconclusive"


def test_the_patterns_own_count_decides_sensitive_pattern(tmp_path: Path) -> None:
    # #1399: in-memory routes detect patterns over every value and record it,
    # so the rule decides past 10,000 values; a file keeps a 10,000-value
    # sample, and the rule stays open there.
    emails = [f"user{i}@example.org" for i in range(20_000)]
    codes = [f"A{i}" for i in range(20_000)]

    def verdict(report: Any) -> str:
        return report.check(forbid_findings=["sensitive_pattern"]).verdict

    found = dp.profile({"contact": emails})
    assert found["contact"].pattern_values_checked == 20_000
    assert verdict(found) == "fail"
    assert verdict(dp.profile({"contact": emails[:9_000]})) == "fail"
    assert verdict(dp.profile({"code": codes})) == "pass"

    path = tmp_path / "contacts.csv"
    path.write_text("contact\n" + "\n".join(emails) + "\n", encoding="utf-8")
    sampled = dp.profile(str(path))
    assert sampled["contact"].pattern_values_checked == 10_000
    assert verdict(sampled) == "inconclusive"
    saved = dp.ProfileReport.from_json(sampled.to_json())
    assert saved["contact"].pattern_values_checked == 10_000
    assert verdict(saved) == "inconclusive"
    # The flat summary carries the count too.
    summary = dp.ProfileReport.from_dict(sampled.to_dict())
    assert summary["contact"].pattern_values_checked == 10_000
