"""A sampled dimension's score estimates the score over every scanned value (#1293).

Each column keeps a reservoir of at most 10,000 non-null values, and the
sampled dimensions were scored over those reservoirs pooled together, so a
sparse column read in full weighed as much as a dense one sampled from many
more values. The reported score could fall outside its own ``score_bounds``.

``tests/sampled_score_estimate.rs`` is the Rust twin.
"""

import json

import dataprof as dp
import pytest

ROWS = 25_000


COLUMNS = ("dense", "sparse", "qty", "age", "rate", "amount")


def _rows() -> list[tuple[str, ...]]:
    """Dense columns hold a value in every row: clean ISO dates, small
    integers, two-decimal amounts. Sparse columns hold one in ten rows and
    fail often: a slash date in one of three, an impossible age of 400 in one
    of five, a one-decimal amount in one of four."""
    rows = []
    for row in range(ROWS):
        dense = f"2024-03-{1 + row % 28:02d}"
        qty = str(20 + row % 40)
        rate = f"{row % 50}.{row % 10}{row % 7}"
        sparse = age = amount = ""
        if row % 10 == 0:
            k = row // 10
            sparse = f"{1 + k % 28:02d}/03/2024" if k % 3 == 0 else f"2024-04-{1 + k % 28:02d}"
            age = "400" if k % 5 == 0 else str(20 + k % 40)
            amount = f"{k % 50}.5" if k % 4 == 0 else f"{k % 50}.{k % 7}{k % 3}"
        rows.append((dense, sparse, qty, age, rate, amount))
    return rows


def _full_source_consistency() -> float:
    rows = _rows()
    checked = sum(1 for row in rows for value in row if value)
    slash = sum(1 for row in rows if "/" in row[1])
    return 100 - 100 * slash / checked


def _csv_bytes() -> bytes:
    lines = [",".join(COLUMNS), *(",".join(row) for row in _rows())]
    return "".join(f"{line}\n" for line in lines).encode()


def _jsonl_bytes() -> bytes:
    records = (
        json.dumps({name: value or None for name, value in zip(COLUMNS, row, strict=True)})
        for row in _rows()
    )
    return "".join(f"{record}\n" for record in records).encode()


@pytest.fixture(scope="module")
def reports(tmp_path_factory) -> list[tuple[str, dp.ProfileReport]]:
    root = tmp_path_factory.mktemp("sampled")
    csv = root / "dates.csv"
    csv.write_bytes(_csv_bytes())
    jsonl = root / "dates.jsonl"
    jsonl.write_bytes(_jsonl_bytes())
    reference = "2024-06-01"
    out = [
        ("csv file", dp.profile(str(csv), reference_date=reference)),
        ("csv bytes", dp.profile(_csv_bytes(), format="csv", reference_date=reference)),
        ("jsonl file", dp.profile(str(jsonl), reference_date=reference)),
    ]
    for engine in ("incremental", "columnar"):
        out.append((f"csv {engine}", dp.profile(str(csv), engine=engine, reference_date=reference)))
    return out


def _document_scores(report: dp.ProfileReport) -> tuple[dict, dict | None]:
    quality = json.loads(report.to_json())["quality"]
    return quality["scores"], quality.get("score_bounds")


def test_every_sampled_score_lies_within_its_own_bounds(reports):
    wrong = []
    bounded = 0
    for label, report in reports:
        scores, bounds = _document_scores(report)
        if bounds is None:
            continue
        bounded += 1
        pairs = [
            (name, score, bounds["dimension_scores"].get(name))
            for name, score in scores["dimension_scores"].items()
        ]
        pairs.append(("overall", scores["overall_score"], bounds["overall_score"]))
        for name, score, interval in pairs:
            if score is None or interval is None:
                continue
            if not interval["lower"] <= score <= interval["upper"]:
                wrong.append(f"[{label}] {name} {score} outside {interval}")
        # The summary dialect reports the same scores.
        summary = report.to_dict()["quality"]
        if summary["dimension_scores"] != scores["dimension_scores"]:
            wrong.append(f"[{label}] summary {summary['dimension_scores']}")
    assert bounded > 0, "no route sampled the fixture"
    assert wrong == []


def test_the_consistency_score_estimates_the_full_source_one(reports):
    exact = _full_source_consistency()
    wrong = []
    for label, report in reports:
        score = report.quality.dimension_scores()["consistency"]
        if abs(score - exact) > 0.01:
            wrong.append(f"[{label}] {score} against {exact}")
    assert wrong == []


def test_the_estimator_is_recorded(reports):
    for _, report in reports:
        assert report.metric_semantics["sampled_score_estimator"] == "population_weighted"
