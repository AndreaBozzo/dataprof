"""A sampled route's verdict is the verdict of a route that reads every value (#1307).

A file keeps a sample of each column's values past 10,000; a dict holds every
value. The two groups are each compared within themselves elsewhere, and the
fuzz targets stop at 32 rows, so nothing held one group to the other above the
sample size, where the score bounds, the estimated scores and the sampled
quality path decide. Three wrong verdicts lived there (#1293, #1304, #1305).

One property, per check of the policy:

    For the same data and the same policy, the verdict of the file route is
    the verdict of the dict route, or it is inconclusive.

Each case places a few values where a sample can choose differently from the
source: a quartile at the boundary between two values, the one slash date that
settles the order, one zero-padded number, a share of numbers, dates or
booleans at a type threshold. The reservoir is seeded, so one dataset gives one
sample; the row order is what varies.

A case that disagrees until its owner lands is listed in ``KNOWN`` by name and
owner, and the test fails when it stops disagreeing, so the list stays true.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import dataprof as dp

ROWS = 40_000
# Each lays the values out in another order: primes, coprime with ROWS.
ORDERS = (7_919, 104_729, 1_299_709)
REFERENCE = "2024-06-01"
DIMENSIONS = (
    "completeness",
    "consistency",
    "uniqueness",
    "accuracy",
    "timeliness",
    "validity",
    "precision",
)
# The finding rules that read the quality block.
QUALITY_FINDINGS = ("duplicate_rows", "future_dates", "key_duplicates", "temporal_order_violations")

Columns = dict[str, list[str]]


def _placed(order: int, blocks: list[tuple[str, int]]) -> list[str]:
    """``blocks`` laid end to end, then read in the order ``order`` gives."""
    values = [value for value, count in blocks for _ in range(count)]
    assert len(values) == ROWS
    return [values[(row * order) % ROWS] for row in range(ROWS)]


def _one_late(common: str, settling: str) -> list[tuple[str, int]]:
    return [(common, ROWS - 1), (settling, 1)]


# name: (columns for a row order, profile options)
CASES: dict[str, tuple[Callable[[int], Columns], dict[str, Any]]] = {
    # Zeros and fives with zeros a few values from 25% or 75%: a quartile is
    # 0 or 5 depending on a handful of values, so the quartiles differ and
    # nothing is an outlier, or they coincide and the column is not tested.
    # Before #1304 coinciding quartiles made every zero or every five an
    # outlier, and a sample could land on the other side of the full scan.
    "first quartile above": (lambda o: {"v": _placed(o, [("0", 10_008), ("5", 29_992)])}, {}),
    "first quartile below": (lambda o: {"v": _placed(o, [("0", 9_992), ("5", 30_008)])}, {}),
    "third quartile above": (lambda o: {"v": _placed(o, [("0", 30_008), ("5", 9_992)])}, {}),
    # One value settles a slash column month-first; the others read either
    # way (#1305). Read month-first, 12/01/2024 lies after the reference date
    # and 01/12/2024 before it.
    "slash date future": (
        lambda o: {"event_date": _placed(o, _one_late("12/01/2024", "12/31/2023"))},
        {},
    ),
    "slash date past": (
        lambda o: {"event_date": _placed(o, _one_late("01/12/2024", "12/31/2023"))},
        {},
    ),
    # Month-first every pair runs forwards; day-first every pair is inverted.
    "slash date pair": (
        lambda o: {
            "start_date": _placed(o, _one_late("01/05/2024", "12/30/2023")),
            "end_date": _placed(o, _one_late("02/03/2024", "12/31/2023")),
        },
        {},
    ),
    # One zero-padded value makes the whole column digit codes, so text; the
    # hundreds are outliers once it is numeric.
    "zero padded": (
        lambda o: {"n": _placed(o, [("10", 20_000), ("20", 19_000), ("900", 999), ("007", 1)])},
        {},
    ),
    # Shares exactly at each type threshold (numbers need more than 80%,
    # booleans at least 90%, dates more than 70%), so a sample a few values
    # either side types the column otherwise.
    "numeric share": (lambda o: {"n": _placed(o, [("12", 32_000), ("n/a?", 8_000)])}, {}),
    "boolean share": (lambda o: {"b": _placed(o, [("true", 36_000), ("maybe", 4_000)])}, {}),
    "date share": (lambda o: {"d": _placed(o, [("2024-03-01", 28_000), ("soon", 12_000)])}, {}),
    # A declared key with one repeated value, wherever the order puts it.
    "key duplicate": (
        lambda o: {
            "order_id": [str(row) for row in range(ROWS - 1)] + [str(o % (ROWS - 1))],
            "v": _placed(o, [("1", 20_000), ("2", 20_000)]),
        },
        {"identifier_columns": ["order_id"]},
    ),
}

# Cases that disagree today, with the issue that owns the fix and each
# disagreement as (check group, file verdict, dict verdict). A sampled route
# types a column from its sample, so a share at a threshold or one zero-padded
# value can type it differently from the dict. A case must show exactly these:
# another disagreement fails, and so does one of these going away.
KNOWN: dict[str, tuple[str, set[tuple[str, str, str]]]] = {
    "zero padded": ("#788", {("overall", "pass", "fail"), ("accuracy", "pass", "inconclusive")}),
    "numeric share": (
        "#788",
        {("overall", "pass", "fail"), ("precision", "pass", "inconclusive")},
    ),
    "date share": ("#788", {("timeliness", "pass", "inconclusive")}),
}

# What the dict must decide, so a case keeps meaning what its comment says.
ORACLE: dict[str, dict[str, str]] = {
    "first quartile above": {"accuracy": "pass"},
    "first quartile below": {"accuracy": "inconclusive"},
    "third quartile above": {"accuracy": "inconclusive"},
    "slash date future": {"forbid future_dates": "fail"},
    "slash date past": {"forbid future_dates": "pass"},
    "slash date pair": {"forbid temporal_order_violations": "pass"},
    "zero padded": {"accuracy": "inconclusive"},
    "key duplicate": {"forbid key_duplicates": "fail"},
}


def _csv(columns: Columns) -> str:
    names = list(columns)
    rows = zip(*(columns[name] for name in names), strict=True)
    return ",".join(names) + "\n" + "".join(",".join(row) + "\n" for row in rows)


def _thresholds(score: float | None) -> list[float]:
    """The dict's score and a hundredth either side: the place a sample errs."""
    if score is None:
        return [50.0]
    around = (score - 0.01, score, score + 0.01)
    return sorted({round(t, 2) for t in around if 0 <= t <= 100})


def _checks(oracle: dp.ProfileReport) -> Iterator[tuple[str, str, dict[str, Any]]]:
    """(group, label, policy) for each check, one requirement per policy."""
    scores = json.loads(oracle.to_json())["quality"]["scores"]
    for t in _thresholds(scores["overall_score"]):
        yield "overall", f"min_quality_score {t}", {"min_quality_score": t}
    for dimension in DIMENSIONS:
        for t in _thresholds(scores["dimension_scores"][dimension]):
            policy = {"min_dimension_scores": {dimension: t}}
            yield dimension, f"{dimension} {t}", policy
    for code in QUALITY_FINDINGS:
        yield f"forbid {code}", f"forbid {code}", {"forbid_findings": [code]}


def test_the_file_route_gives_the_verdict_of_the_dict_or_none(tmp_path: Path) -> None:
    # Each disagreement as its (group, file verdict, dict verdict) and a line.
    wrong: dict[str, list[tuple[tuple[str, str, str], str]]] = {name: [] for name in CASES}
    oracle_wrong = []
    dict_verdicts: set[str] = set()
    decided = 0
    for name, (build, options) in CASES.items():
        for order in ORDERS:
            columns = build(order)
            path = tmp_path / "data.csv"
            path.write_text(_csv(columns), encoding="utf-8")
            oracle = dp.profile(columns, reference_date=REFERENCE, **options)
            report = dp.profile(str(path), reference_date=REFERENCE, **options)
            saved = dp.ProfileReport.from_json(report.to_json())
            for group, label, policy in _checks(oracle):
                expected = oracle.check(**policy).verdict
                dict_verdicts.add(expected)
                want = ORACLE.get(name, {}).get(group)
                if want is not None and expected != want:
                    oracle_wrong.append(f"{name}, order {order}: dict {label} {expected}")
                for form, got in (("run", report), ("saved", saved)):
                    verdict = got.check(**policy).verdict
                    decided += verdict != "inconclusive"
                    if verdict not in {expected, "inconclusive"}:
                        line = f"order {order} [{form}] {label}: {verdict}, dict {expected}"
                        wrong[name].append(((group, verdict, expected), line))

    unexpected = []
    settled = []
    for name in CASES:
        owner, known = KNOWN.get(name, ("", set()))
        unexpected += [f"{name}: {line}" for entry, line in wrong[name] if entry not in known]
        seen = {entry for entry, _ in wrong[name]}
        settled += [f"{name} {entry} ({owner})" for entry in sorted(known - seen)]
    assert not unexpected, "\n".join(unexpected)
    assert not settled, f"no longer disagree, remove from KNOWN: {', '.join(settled)}"

    assert not oracle_wrong, "\n".join(oracle_wrong)
    # The comparison means something only if the dict decides both ways and
    # the file route decides some checks rather than leaving all open.
    assert dict_verdicts == {"pass", "fail", "inconclusive"}
    assert decided
