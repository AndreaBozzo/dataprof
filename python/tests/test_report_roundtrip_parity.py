"""Round-trip parity between native and dict-backed reports (#512).

A ``ProfileReport`` has two backings. One wraps the native report returned by
the extension; the other (``_DictBackedReport`` and friends) reconstructs the
same surface from a plain dict, and is what ``from_dict()``, ``from_json()``
and ``load()`` return. Nothing asserted the two agreed, and they had already
drifted — the deprecation set is maintained separately in each (#509), and
``semantic_hint_bindings`` was written by ``to_dict()`` but never read back.

Saving and reloading a report must not change what it reports.

The member lists here are driven by introspection, not written by hand, so a
new accessor is covered the day it is added rather than the day someone
remembers to extend this file.

Rounding: the dict backing carries values the serializer already rounded (2dp
for 0..100 percentages, 4dp for statistics and 0..1 ratios), so a float read
back is not required to be bit-identical to the native one — it is required to
be that native value rounded at one of the two documented precisions. Which
*algorithm* does the rounding is #513's decision and deliberately not pinned
here, so both candidates are accepted.
"""

from __future__ import annotations

import json
import math
import warnings
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import dataprof
import pytest

# Members that cannot participate in a value-parity sweep, each for a reason
# that is not "it disagrees":
#   - constructors return a new report rather than a value
#   - save() writes a file and needs a path
#   - compare() needs a second report
#   - the dataframe exports need pandas/polars/pyarrow, which are optional
_UNCOMPARABLE_REPORT_MEMBERS = frozenset(
    {
        "from_dict",
        "from_json",
        "load",
        "save",
        "compare",
        "to_dataframe",
        "to_polars",
        "to_arrow",
        "describe",
    }
)

# Known divergence, pinned by test_llm_context_flags_survive_roundtrip below
# rather than silenced here: `to_llm_context` recomputes its flags from
# `null_percentage`, comparing the threshold against the unrounded value while
# rendering the rounded one, so a column at 19.998% null renders "20.0% null"
# yet is not flagged -- until a round-trip stores 20.0 and it is. Fixing it
# changes which flags agent-facing output emits, so it is tracked separately.
_KNOWN_DIVERGENT_REPORT_MEMBERS = frozenset({"to_llm_context"})

# Rounding precisions the report serializer documents: 2dp for 0..100
# percentages, 4dp for statistics and 0..1 ratios.
_SERIALIZED_PRECISIONS = (2, 4)


def _rich_csv(path, rows: int = 10_500) -> None:
    """A fixture broad enough to exercise the shapes the issue calls for.

    ``id`` crosses the 10k exact-cardinality threshold so its unique count is
    approximate; ``email`` and ``when`` carry detected patterns; ``amount`` and
    ``notes`` carry nulls; the trailing row is a numeric outlier.
    """
    lines = ["id,email,amount,active,notes,when"]
    for i in range(rows):
        email = f"user{i}@example.com" if i % 3 else ""
        amount = "" if i % 7 == 0 else f"{i * 1.37:.4f}"
        active = "true" if i % 2 else "false"
        notes = "" if i % 5 == 0 else f"note {i % 11}"
        lines.append(f"{i},{email},{amount},{active},{notes},2026-0{(i % 9) + 1}-15")
    lines.append("99999,huge@example.com,999999999.5,true,outlier,2026-01-01")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


@pytest.fixture(scope="module")
def rich_report(tmp_path_factory):
    csv = tmp_path_factory.mktemp("parity") / "rich.csv"
    _rich_csv(csv)
    return dataprof.profile_file(csv)


@pytest.fixture(scope="module")
def hinted_report(tmp_path_factory):
    """A report carrying semantic hint bindings, which are additive in to_dict()."""
    csv = tmp_path_factory.mktemp("parity") / "hinted.csv"
    csv.write_text(
        "id,amount,when\n"
        + "".join(f"{i},{i * 2 + 1}.5,2026-0{(i % 9) + 1}-15\n" for i in range(30)),
        encoding="utf-8",
    )
    return (
        dataprof.Profiler()
        .positive_columns(["amount"])
        .identifier_columns(["id"])
        .temporal_columns(["when"])
        .profile(csv)
    )


@pytest.fixture(scope="module")
def sparse_report(tmp_path_factory):
    """A tiny single-column report: few rows, no dates, no numeric spread.

    Exercises unassessed dimensions (score ``None``) and the low-sample warning,
    so the absence rule is checked on values that are genuinely absent.
    """
    csv = tmp_path_factory.mktemp("parity") / "sparse.csv"
    csv.write_text("label\na\nb\n\nc\n", encoding="utf-8")
    return dataprof.profile_file(csv)


def _round_candidates(value: float) -> set[float]:
    """Every value the serializer could legitimately have written for ``value``.

    Covers both rounding algorithms at both documented precisions — see #513,
    which owns the choice between them. Passing ``value`` through unrounded is
    also allowed, for fields the serializer does not round at all.
    """
    candidates = {value}
    for ndigits in _SERIALIZED_PRECISIONS:
        # Decimal half-up on the shortest repr — today's Python behaviour.
        candidates.add(
            float(Decimal(str(value)).quantize(Decimal(f"1e-{ndigits}"), rounding=ROUND_HALF_UP))
        )
        # Binary multiply/round/divide — today's Rust behaviour.
        scale = 10.0**ndigits
        candidates.add(round(value * scale) / scale)
    return candidates


def _public_names(obj: object) -> set[str]:
    return {name for name in dir(obj) if not name.startswith("_")}


def _read(obj: object, name: str) -> Any:
    """Read a member, calling it if it is a zero-argument method.

    Deprecation warnings from the flat quality accessors are suppressed: this
    test asserts the *values* agree. That the two backings warn consistently is
    #509's territory.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        value = getattr(obj, name)
        if callable(value):
            value = value()
    # to_json() differs only in the key order the two backings happen to build
    # their dicts in; compare the decoded documents, not the text.
    if name == "to_json" and isinstance(value, str):
        return json.loads(value)
    return value


def _assert_parity(native: Any, restored: Any, path: str) -> None:
    """Assert two values agree, recursing into containers and profile objects."""
    # Absence rule: None means "not analyzed" and must never read back as 0,
    # 0.0, False or "". Checked before any value comparison.
    if native is None or restored is None:
        assert native is None and restored is None, (
            f"{path}: absence lost - native={native!r}, round-tripped={restored!r}"
        )
        return

    # bool before int: bool is an int subclass and True == 1.
    if isinstance(native, bool) or isinstance(restored, bool):
        assert isinstance(native, bool) and isinstance(restored, bool), (
            f"{path}: type changed - native={native!r}, round-tripped={restored!r}"
        )
        assert native == restored, f"{path}: native={native!r}, round-tripped={restored!r}"
        return

    if isinstance(native, float) or isinstance(restored, float):
        assert isinstance(native, (int, float)) and isinstance(restored, (int, float)), (
            f"{path}: type changed - native={native!r}, round-tripped={restored!r}"
        )
        if math.isnan(native) or math.isnan(restored):
            assert math.isnan(native) and math.isnan(restored), (
                f"{path}: native={native!r}, round-tripped={restored!r}"
            )
            return
        assert restored in _round_candidates(float(native)), (
            f"{path}: round-tripped value is not the native value at any "
            f"documented precision - native={native!r}, round-tripped={restored!r}"
        )
        return

    if isinstance(native, (int, str)):
        assert type(native) is type(restored), (
            f"{path}: type changed - native={native!r}, round-tripped={restored!r}"
        )
        assert native == restored, f"{path}: native={native!r}, round-tripped={restored!r}"
        return

    if isinstance(native, dict):
        assert isinstance(restored, dict), (
            f"{path}: type changed - native={native!r}, round-tripped={restored!r}"
        )
        # Keys are sorted by repr: they are typed as `object` here, and report
        # documents mix key types across nested dicts.
        assert set(native) == set(restored), (
            f"{path}: keys differ - only native: "
            f"{sorted(map(repr, set(native) - set(restored)))}, "
            f"only round-tripped: {sorted(map(repr, set(restored) - set(native)))}"
        )
        for key in native:
            _assert_parity(native[key], restored[key], f"{path}[{key!r}]")
        return

    if isinstance(native, (list, tuple)):
        assert isinstance(restored, (list, tuple)), (
            f"{path}: type changed - native={native!r}, round-tripped={restored!r}"
        )
        assert len(native) == len(restored), (
            f"{path}: length differs - native={len(native)}, round-tripped={len(restored)}"
        )
        for index, (left, right) in enumerate(zip(native, restored, strict=True)):
            _assert_parity(left, right, f"{path}[{index}]")
        return

    # Anything else is a profile object (ColumnProfile, Pattern,
    # DataQualityMetrics). The two backings use different classes by design, so
    # compare the union of their public members rather than the objects.
    _assert_members_agree(native, restored, path)


def _assert_members_agree(native: Any, restored: Any, path: str, skip: frozenset = frozenset()):
    names = (_public_names(native) | _public_names(restored)) - skip
    assert names, f"{path}: no public members found to compare"
    for name in sorted(names):
        _assert_parity(_read(native, name), _read(restored, name), f"{path}.{name}")


def _roundtrips(report, tmp_path):
    """The three documented ways back from a serialized report."""
    saved = tmp_path / "report.json"
    report.save(saved)
    return {
        "from_dict": dataprof.ProfileReport.from_dict(report.to_dict()),
        "from_json": dataprof.ProfileReport.from_json(report.to_json()),
        "load": dataprof.ProfileReport.load(saved),
    }


@pytest.mark.parametrize("via", ["from_dict", "from_json", "load"])
def test_report_members_survive_roundtrip(rich_report, tmp_path, via):
    """Every public ProfileReport member agrees across the two backings."""
    restored = _roundtrips(rich_report, tmp_path)[via]
    _assert_members_agree(
        rich_report,
        restored,
        "ProfileReport",
        skip=_UNCOMPARABLE_REPORT_MEMBERS | _KNOWN_DIVERGENT_REPORT_MEMBERS,
    )


@pytest.mark.parametrize("via", ["from_dict", "from_json", "load"])
def test_quality_members_survive_roundtrip(rich_report, tmp_path, via):
    """Every DataQualityMetrics member agrees, nested dimension dicts key by key."""
    restored = _roundtrips(rich_report, tmp_path)[via]
    native_quality = rich_report.quality
    assert native_quality is not None, "fixture must produce quality metrics"
    _assert_members_agree(native_quality, restored.quality, "DataQualityMetrics")


@pytest.mark.parametrize("via", ["from_dict", "from_json", "load"])
def test_column_members_survive_roundtrip(rich_report, tmp_path, via):
    """Every ColumnProfile member agrees, including nested patterns."""
    restored = _roundtrips(rich_report, tmp_path)[via]
    assert [c.name for c in restored.profiles] == [c.name for c in rich_report.profiles]
    for native_col, restored_col in zip(rich_report.profiles, restored.profiles, strict=True):
        _assert_members_agree(native_col, restored_col, f"ColumnProfile[{native_col.name!r}]")


def test_semantic_hint_bindings_survive_roundtrip(hinted_report, tmp_path):
    """Hint bindings are written by to_dict() and must be read back (#512).

    They were dropped silently: `_DictBackedReport` never read the key, so a
    reloaded report reported no bindings — indistinguishable from a report
    profiled without hints at all.
    """
    assert hinted_report.semantic_hint_bindings, "fixture must produce hint bindings"
    for name, restored in _roundtrips(hinted_report, tmp_path).items():
        _assert_parity(
            hinted_report.semantic_hint_bindings,
            restored.semantic_hint_bindings,
            f"{name}: ProfileReport.semantic_hint_bindings",
        )


@pytest.mark.parametrize("via", ["from_dict", "from_json", "load"])
def test_sparse_report_survives_roundtrip(sparse_report, tmp_path, via):
    """Unassessed dimensions and the low-sample warning round-trip unchanged."""
    restored = _roundtrips(sparse_report, tmp_path)[via]
    native_quality = sparse_report.quality
    assert native_quality is not None, "fixture must produce quality metrics"
    scores = native_quality.dimension_scores()
    assert any(score is None for score in scores.values()), (
        "fixture must leave at least one dimension unassessed"
    )
    _assert_members_agree(
        sparse_report,
        restored,
        "ProfileReport",
        skip=_UNCOMPARABLE_REPORT_MEMBERS | _KNOWN_DIVERGENT_REPORT_MEMBERS,
    )
    _assert_members_agree(native_quality, restored.quality, "DataQualityMetrics")


def test_unassessed_dimension_does_not_read_back_as_zero(sparse_report, tmp_path):
    """The absence rule, stated directly rather than only via the sweep."""
    restored = _roundtrips(sparse_report, tmp_path)["load"]
    native_scores = sparse_report.quality.dimension_scores()
    restored_scores = restored.quality.dimension_scores()
    assert set(native_scores) == set(restored_scores)
    for dimension, score in native_scores.items():
        if score is None:
            assert restored_scores[dimension] is None, (
                f"{dimension}: unassessed dimension read back as {restored_scores[dimension]!r}"
            )


@pytest.mark.xfail(
    strict=True,
    reason="Known: to_llm_context thresholds flags on the unrounded "
    "null_percentage but renders the rounded one, so a round-trip can add a "
    "flag. Fixing it changes agent-facing output; tracked separately.",
)
def test_llm_context_flags_survive_roundtrip(rich_report):
    """Pins the divergence excluded from the sweep, so it cannot be forgotten.

    ``notes`` is 19.998% null: native renders it as "20.0% null" without the
    null-heavy flag, while the round-tripped report stores 20.0 and flags it.
    When the threshold and the rendering are reconciled this test starts
    passing, and strict xfail turns that into a failure demanding its removal.
    """
    restored = dataprof.ProfileReport.from_dict(rich_report.to_dict())
    assert rich_report.to_llm_context() == restored.to_llm_context()


def test_small_uniqueness_ratio_is_not_rounded_to_zero(rich_report):
    """A 0..1 ratio needs 4dp to carry a 2dp percentage's resolution (#512).

    ``uniqueness_ratio`` was rounded to 2dp, so every column below 0.5%
    uniqueness serialized as a flat ``0.0`` — indistinguishable from a column
    with no distinct values at all. The generic parity sweep cannot catch this:
    ``0.0`` *is* the faithful 2dp rounding, so the two backings agree. The
    defect is the choice of precision, not a disagreement about it.
    """
    document = rich_report.to_dict()
    columns = {col["name"]: col for col in document["columns"]}

    small = columns["notes"]
    exact = small["unique_count"] / small["total_count"]
    assert 0.0 < exact < 0.005, "fixture must have a sub-0.5% uniqueness column"
    assert small["uniqueness_ratio"] > 0.0, (
        f"nonzero uniqueness ratio {exact!r} serialized as "
        f"{small['uniqueness_ratio']!r} - precision lost"
    )
    assert small["uniqueness_ratio"] == pytest.approx(exact, abs=5e-5)

    # Every column, not just the small one, must survive at 4dp resolution.
    for name, col in columns.items():
        if col["unique_count"] is None or not col["total_count"]:
            continue
        assert col["uniqueness_ratio"] == pytest.approx(
            col["unique_count"] / col["total_count"], abs=5e-5
        ), f"{name}: uniqueness_ratio lost more than 4dp of precision"


def test_small_uniqueness_ratio_survives_the_record_export(rich_report):
    """The dataframe/arrow/save export rounds the same ratio the same way."""
    pytest.importorskip("pandas")
    frame = rich_report.to_dataframe()
    row = frame.loc[frame["name"] == "notes"].iloc[0]
    exact = row["unique_count"] / row["total_count"]
    assert row["uniqueness_ratio"] > 0.0, (
        f"nonzero uniqueness ratio {exact!r} exported as {row['uniqueness_ratio']!r}"
    )
    assert row["uniqueness_ratio"] == pytest.approx(exact, abs=5e-5)


def test_roundtrip_is_idempotent(rich_report, tmp_path):
    """A second round-trip changes nothing a first one left intact."""
    once = dataprof.ProfileReport.from_dict(rich_report.to_dict())
    twice = dataprof.ProfileReport.from_dict(once.to_dict())
    assert json.dumps(once.to_dict(), sort_keys=True) == json.dumps(twice.to_dict(), sort_keys=True)


def test_sweep_covers_the_documented_surface(rich_report):
    """Guard the guard: the sweep must actually reach the public accessors.

    A filter bug that emptied the member set would make every parity assertion
    above vacuous, so pin that the names being compared include the ones the
    package declares.
    """
    swept = _public_names(rich_report) - _UNCOMPARABLE_REPORT_MEMBERS
    for expected in ("rows", "columns", "quality_score", "profiles", "to_dict"):
        assert expected in swept, f"{expected} dropped out of the parity sweep"
    quality_names = _public_names(rich_report.quality)
    for expected in ("completeness", "dimension_scores", "score_weights"):
        assert expected in quality_names, f"{expected} dropped out of the quality sweep"
