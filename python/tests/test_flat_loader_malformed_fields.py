"""A present but malformed field fails ``from_dict()`` instead of reading as a default (#763).

The flat loader turned a value of the wrong type into a believable one: a
``stats`` list into "not analyzed", a ``"false"`` flag into ``True``, a ``null``
count into 0. #762 fixed ``quality.score_weights``; these are the other fields
read the same way. A missing key still takes its documented compatibility
default, and ``null`` still means "not analyzed" where the schema allows it.
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from typing import Any

import dataprof as dp
import pytest


@pytest.fixture(scope="module")
def summary() -> dict[str, Any]:
    report = dp.profile(
        {"id": [1, 2, 3, 4], "city": ["Rome", "Milan", "Turin", "Rome"]},
        identifier_columns=["id"],
    )
    document = report.to_dict()
    assert document["quality"] is not None
    assert document["semantic_hint_bindings"]
    return document


def _set(*path: Any) -> Callable[[dict[str, Any], Any], None]:
    def apply(document: dict[str, Any], value: Any) -> None:
        target = document
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value

    return apply


def _pattern_entry(document: dict[str, Any], value: Any) -> None:
    document["columns"][1]["patterns"] = [value]


def _no_quality_status(document: dict[str, Any], value: Any) -> None:
    del document["quality_status"]
    document["quality"] = value


#: (field path in the error, how to place the value, malformed values)
MALFORMED = [
    ("columns[0].stats", _set("columns", 0, "stats"), [[1], "x", 1]),
    ("columns[1].patterns", _pattern_entry, [1, "x", None]),
    ("columns[1].patterns", _set("columns", 1, "patterns"), ["x", {}]),
    ("quality.low_sample_warning", _set("quality", "low_sample_warning"), ["false", 0, None]),
    ("quality.assessed_dimensions", _set("quality", "assessed_dimensions"), [None, {}, "x"]),
    ("quality.dimension_scores", _set("quality", "dimension_scores"), [[1], None, "x"]),
    ("quality.score_bounds", _set("quality", "score_bounds"), ["x", [1]]),
    ("quality", _no_quality_status, ["x", [1]]),
    ("execution.ragged_row_count", _set("execution", "ragged_row_count"), [None, -1, "0", True]),
    ("execution.sampling_applied", _set("execution", "sampling_applied"), ["false", 0, None]),
    ("semantic_hint_bindings", _set("semantic_hint_bindings"), ["x", [1], {}]),
]


@pytest.mark.parametrize(
    ("path", "place", "value"),
    [(path, place, value) for path, place, values in MALFORMED for value in values],
    ids=[f"{path}={value!r}" for path, _, values in MALFORMED for value in values],
)
def test_a_malformed_field_is_refused_with_its_path(summary, path, place, value):
    document = copy.deepcopy(summary)
    place(document, value)
    with pytest.raises(ValueError, match=rf"'{__import__('re').escape(path)}' must be"):
        dp.ProfileReport.from_dict(document)


#: (where the key lives, key, how the reloaded report reads it, the default)
ABSENT = [
    (("execution",), "ragged_row_count", lambda r: r.ragged_row_count, 0),
    (("execution",), "sampling_applied", lambda r: r.sampling_applied, False),
    ((), "semantic_hint_bindings", lambda r: r.semantic_hint_bindings, []),
    (("quality",), "low_sample_warning", lambda r: r.quality.low_sample_warning, False),
    (("quality",), "assessed_dimensions", lambda r: r.quality.assessed_dimensions(), []),
    (("columns", 0), "stats", lambda r: r["id"].min, None),
    (("columns", 1), "patterns", lambda r: r["city"].patterns, None),
]


@pytest.mark.parametrize(
    ("section", "key", "read", "default"), ABSENT, ids=[key for _, key, _, _ in ABSENT]
)
def test_a_missing_key_keeps_its_compatibility_default(summary, section, key, read, default):
    document = copy.deepcopy(summary)
    target = document
    for part in section:
        target = target[part]
    del target[key]
    assert read(dp.ProfileReport.from_dict(document)) == default


def test_null_still_means_not_analyzed_where_the_schema_allows_it(summary):
    document = copy.deepcopy(summary)
    document["columns"][0]["stats"] = None
    document["columns"][1]["patterns"] = None
    document["quality"]["score_bounds"] = None
    report = dp.ProfileReport.from_dict(document)
    assert report["id"].min is None
    assert report["city"].patterns is None


def test_a_well_formed_summary_round_trips(summary):
    assert dp.ProfileReport.from_dict(copy.deepcopy(summary)).to_dict() == summary
