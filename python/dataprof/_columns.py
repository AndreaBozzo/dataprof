"""Column serialization and shared column evidence helpers."""

from __future__ import annotations as _annotations

from typing import TYPE_CHECKING as _TYPE_CHECKING, Any as _Any

from ._dataprof import ColumnProfile
from ._rounding import _r2, _r4, _round_quartiles

if _TYPE_CHECKING:
    from ._dataprof import Pattern as _NativePattern
    from ._report_backing import _DictPattern
else:
    _NativePattern = _Any


# ---------------------------------------------------------------------------
# Shared column record builder
# ---------------------------------------------------------------------------


#: The lexical classes ``type_homogeneity`` counts, in the order ties resolve
#: in. Mirrors the declaration order the Rust classifier uses to pick a dominant
#: class, so both layers name the same winner when two classes hold equal counts.
#: Serialization keys are sorted instead — see :func:`_homogeneity_counts`.
_LEXICAL_CLASS_ORDER = ("numeric", "date", "boolean", "text")


def _homogeneity_counts(value: _Any) -> dict[str, int] | None:
    """Normalize a ``type_homogeneity`` mapping, or ``None`` when there is none.

    Absence is preserved rather than filled in: ``None`` means the
    classification did not run, and must never read back as four zero counts,
    which mean "classified, and there was nothing to classify". A malformed or
    incomplete mapping is treated as absence for the same reason -- an invented
    count is worse than a gap.

    Keys come back sorted, matching the order the native extension builds them
    in, so the two report backings serialize byte-identical documents.
    """
    if not isinstance(value, dict):
        return None
    counts: dict[str, int] = {}
    for name in sorted(_LEXICAL_CLASS_ORDER):
        count = value.get(name)
        # bool before int: bool is an int subclass, and True == 1.
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            return None
        counts[name] = count
    return counts


def _type_mixture(col: ColumnProfile) -> list[tuple[str, int, float]]:
    """Lexical classes holding at least one value, largest first.

    Each entry is ``(class, count, share)`` with ``share`` in ``0..1``. Empty
    when the column was not classified, or was classified and held no non-null
    values -- the caller cannot tell those apart from here, and must read
    ``type_homogeneity`` itself if the difference matters.
    """
    counts = _homogeneity_counts(col.type_homogeneity)
    if counts is None:
        return []
    total = sum(counts.values())
    if total == 0:
        return []
    present = [(name, n) for name, n in counts.items() if n]
    present.sort(key=lambda item: (-item[1], _LEXICAL_CLASS_ORDER.index(item[0])))
    return [(name, n, n / total) for name, n in present]


def column_to_dict(col: ColumnProfile) -> dict[str, _Any]:
    """Convert a single ColumnProfile to the nested dict layout used by
    :meth:`ProfileReport.to_dict`.

    The shape matches one element of ``report.to_dict()["columns"]``:

    .. code-block:: python

        {
          "name": ..., "data_type": ..., "total_count": ..., "null_count": ...,
          "null_percentage": ..., "unique_count": ...,
          "unique_count_is_approximate": ..., "uniqueness_ratio": ...,
          "stats": {"min": ..., "max": ..., ...},      # numeric / text / boolean
          "patterns": [{"name": ..., "regex": ..., ...}, ...]
        }

    Floating-point values are rounded: 2dp for ``0..100`` percentages, 4dp for
    statistics and for ``0..1`` ratios such as ``uniqueness_ratio``, so that a
    ratio carries the same resolution as the equivalent percentage.
    """
    col_data: dict[str, _Any] = {
        "name": col.name,
        "data_type": col.data_type,
        "total_count": col.total_count,
        "null_count": col.null_count,
        "null_percentage": _r2(col.null_percentage),
        "unique_count": col.unique_count,
        "unique_count_is_approximate": col.unique_count_is_approximate,
        # 4dp, not 2dp: this is a 0..1 ratio, so 2dp gave it a hundredth of the
        # resolution the 0..100 percentages get, and rounded every column below
        # 0.5% uniqueness to a flat 0.0 — a plausible-looking wrong number.
        # Matches the sibling ratio `true_ratio` (#512).
        "uniqueness_ratio": _r4(col.uniqueness_ratio),
    }
    # The key is omitted entirely when the numeric/date check did not run
    # (another column type, or statistics skipped), mirroring the Rust
    # serialization; a clean checked column serializes 0.
    if col.invalid_count is not None:
        col_data["invalid_count"] = col.invalid_count
    # Same omit-when-absent rule: the key is missing when the classification did
    # not run, so a reloaded report cannot mistake absence for four zero counts.
    homogeneity = _homogeneity_counts(col.type_homogeneity)
    if homogeneity is not None:
        col_data["type_homogeneity"] = homogeneity
    if col.min is not None:
        col_data["stats"] = {
            k: v
            for k, v in {
                "min": _r4(col.min),
                "max": _r4(col.max),
                "mean": _r4(col.mean),
                "std_dev": _r4(col.std_dev),
                "variance": _r4(col.variance),
                "median": _r4(col.median),
                "mode": _r4(col.mode),
                "skewness": _r4(col.skewness),
                "kurtosis": _r4(col.kurtosis),
                # A 0..100 percentage (std_dev/mean * 100), so 2dp like every
                # other percentage — not 4dp like the statistics around it.
                "coefficient_of_variation": _r2(col.coefficient_of_variation),
                "quartiles": _round_quartiles(col.quartiles),
                "is_approximate": col.is_approximate,
                "outlier_count": col.outlier_count,
            }.items()
            if v is not None
        }
    if col.min_length is not None:
        if "stats" not in col_data:
            col_data["stats"] = {}
        col_data["stats"].update(
            {
                k: v
                for k, v in {
                    "min_length": col.min_length,
                    "max_length": col.max_length,
                    "avg_length": _r4(col.avg_length),
                }.items()
                if v is not None
            }
        )
    if col.true_count is not None:
        bool_stats: dict[str, _Any] = col_data.setdefault("stats", {})
        bool_stats["true_count"] = col.true_count
        bool_stats["false_count"] = col.false_count
        bool_stats["true_ratio"] = _r4(col.true_ratio)

    if col.patterns is not None:
        col_data["patterns"] = [
            {
                "name": p.name,
                "regex": p.regex,
                "match_count": p.match_count,
                "match_percentage": _r2(p.match_percentage),
                "category": p.category,
                "confidence": round(p.confidence, 4),
            }
            for p in col.patterns
        ]
    return col_data


def _column_record(col: ColumnProfile) -> dict[str, _Any]:
    """Build a flat dict of all column stats with proper rounding.

    Used by to_dataframe(), to_polars(), to_arrow(), describe(), save().
    """
    q = col.quartiles
    rq = _round_quartiles(q)
    top_pattern = None
    top_pattern_pct = None
    top_pattern_category = None
    top_pattern_confidence = None
    best = _dominant_pattern(col)
    if best is not None:
        top_pattern = best.name
        top_pattern_pct = _r2(best.match_percentage)
        top_pattern_category = best.category
        top_pattern_confidence = round(best.confidence, 4)

    # These exports are flat -- a CSV column cannot hold the four-way count map
    # -- so the record carries the two scalars derived from it. Both are None
    # when the column was not classified, or held nothing to classify.
    mixture = _type_mixture(col)
    dominant_type = mixture[0][0] if mixture else None
    dominant_type_share = _r4(mixture[0][2]) if mixture else None

    return {
        "name": col.name,
        "data_type": col.data_type,
        "total_count": col.total_count,
        "null_count": col.null_count,
        "null_percentage": _r2(col.null_percentage),
        "unique_count": col.unique_count,
        "unique_count_is_approximate": col.unique_count_is_approximate,
        # 4dp for the same reason as in column_to_dict() — see the note there.
        "uniqueness_ratio": _r4(col.uniqueness_ratio),
        "invalid_count": col.invalid_count,
        "dominant_type": dominant_type,
        "dominant_type_share": dominant_type_share,
        "min": _r4(col.min),
        "max": _r4(col.max),
        "mean": _r4(col.mean),
        "std_dev": _r4(col.std_dev),
        "variance": _r4(col.variance),
        "median": _r4(col.median),
        "mode": _r4(col.mode),
        "skewness": _r4(col.skewness),
        "kurtosis": _r4(col.kurtosis),
        # A 0..100 percentage (std_dev/mean * 100), so 2dp like every other
        # percentage — not 4dp like the statistics around it.
        "coefficient_of_variation": _r2(col.coefficient_of_variation),
        "q1": rq["q1"] if rq else None,
        "q2": rq["q2"] if rq else None,
        "q3": rq["q3"] if rq else None,
        "iqr": rq["iqr"] if rq else None,
        "is_approximate": col.is_approximate,
        "outlier_count": col.outlier_count,
        "min_length": col.min_length,
        "max_length": col.max_length,
        "avg_length": _r4(col.avg_length),
        "true_count": col.true_count,
        "false_count": col.false_count,
        "true_ratio": _r4(col.true_ratio),
        "top_pattern": top_pattern,
        "top_pattern_pct": top_pattern_pct,
        "top_pattern_category": top_pattern_category,
        "top_pattern_confidence": top_pattern_confidence,
    }


#: A pattern below this confidence remains available in detailed evidence but
#: is not strong enough to become a report-level semantic claim. This mirrors
#: the threshold used by the validity quality dimension.
_MIN_SUMMARY_PATTERN_CONFIDENCE = 0.5


def _dominant_pattern(col: ColumnProfile) -> _NativePattern | _DictPattern | None:
    """Return the strongest pattern that clears the summary evidence threshold."""
    if not col.patterns:
        return None
    candidates = [
        pattern for pattern in col.patterns if pattern.confidence >= _MIN_SUMMARY_PATTERN_CONFIDENCE
    ]
    # Guard the empty case rather than passing `default=None` to max(): that
    # overload makes the key callable's parameter include None, so the lambda
    # no longer type-checks.
    if not candidates:
        return None
    return max(candidates, key=lambda pattern: pattern.confidence)


# Preserve the public import path for introspection and pickled API objects.
column_to_dict.__module__ = "dataprof"
