"""Read-only adapters for reports restored from serialized documents."""

from __future__ import annotations as _annotations

import warnings as _warnings
from typing import Any as _Any

from ._columns import _homogeneity_counts
from ._report_schema import _QUALITY_DIMENSIONS

# ---------------------------------------------------------------------------
# Read-only proxy backing ProfileReport.from_dict() / from_json().
#
# The native ProfileReport can't be constructed from Python, so these classes
# mimic the attribute surface the export methods read (self._report.<attr> and
# self._report.column_profiles) closely enough that every ProfileReport method
# works unchanged on a reloaded report.
# ---------------------------------------------------------------------------


class _DictPattern:
    """Read-only stand-in for a native Pattern, built from a to_dict() entry."""

    def __init__(self, d: dict[str, _Any]):
        self.name = d.get("name")
        self.regex = d.get("regex")
        self.match_count = d.get("match_count")
        self.match_percentage = d.get("match_percentage")
        self.category = d.get("category")
        self.confidence = d.get("confidence", 0.0)


class _DictColumn:
    """Read-only stand-in for a native ColumnProfile, built from to_dict()."""

    # Optional stat attributes, all defaulting to None; a subset is overlaid
    # from the nested "stats" dict depending on the column's data type.
    _STAT_ATTRS = (
        "min",
        "max",
        "mean",
        "std_dev",
        "variance",
        "median",
        "mode",
        "skewness",
        "kurtosis",
        "coefficient_of_variation",
        "quartiles",
        "is_approximate",
        "outlier_count",
        "min_length",
        "max_length",
        "avg_length",
        "true_count",
        "false_count",
        "true_ratio",
    )

    def __init__(self, d: dict[str, _Any]):
        for attr in self._STAT_ATTRS:
            setattr(self, attr, None)
        self.name = d.get("name")
        self.data_type = d.get("data_type")
        self.total_count = d.get("total_count")
        self.null_count = d.get("null_count")
        self.null_percentage = d.get("null_percentage")
        self.unique_count = d.get("unique_count")
        self.unique_count_is_approximate = d.get("unique_count_is_approximate")
        self.uniqueness_ratio = d.get("uniqueness_ratio")
        self.invalid_count = d.get("invalid_count")
        # Normalized on the way in, so a hand-edited or truncated mapping reads
        # back as "not classified" rather than as counts that were never taken.
        self.type_homogeneity = _homogeneity_counts(d.get("type_homogeneity"))
        # Overlay only known stat attributes — never setattr arbitrary keys from
        # (possibly malformed) input, which could inject unexpected/dunder names.
        stats = d.get("stats")
        if isinstance(stats, dict):
            allowed = set(self._STAT_ATTRS)
            for key, value in stats.items():
                if key in allowed:
                    setattr(self, key, value)
        patterns = d.get("patterns")
        self.patterns = (
            [_DictPattern(p) for p in patterns if isinstance(p, dict)]
            if isinstance(patterns, list)
            else None
        )


class _DictQuality:
    """Read-only stand-in for native DataQualityMetrics, built from to_dict()."""

    _DEFAULT_SCORE_WEIGHTS = {
        "completeness": 0.25,
        "consistency": 0.20,
        "uniqueness": 0.15,
        "accuracy": 0.15,
        "timeliness": 0.10,
        "validity": 0.10,
        "precision": 0.05,
    }

    def __init__(self, d: dict[str, _Any]):
        self._d = d
        self.low_sample_warning = bool(d.get("low_sample_warning", False))

    def _warn_flat_accessor(self, name: str) -> None:
        _warnings.warn(
            f"DataQualityMetrics.{name} is deprecated; use the nested dimension "
            "properties such as completeness, consistency, uniqueness, accuracy, "
            "or timeliness instead.",
            DeprecationWarning,
            stacklevel=3,
        )

    def _dimension_value(self, dimension: str, key: str, default: _Any) -> _Any:
        value = self._d.get(dimension)
        if isinstance(value, dict):
            return value.get(key, default)
        return default

    @property
    def missing_values_ratio(self) -> float:
        self._warn_flat_accessor("missing_values_ratio")
        return self._dimension_value("completeness", "missing_values_ratio", 0.0)

    @property
    def complete_records_ratio(self) -> float:
        self._warn_flat_accessor("complete_records_ratio")
        return self._dimension_value("completeness", "complete_records_ratio", 100.0)

    @property
    def null_columns(self) -> list[str]:
        self._warn_flat_accessor("null_columns")
        return self._dimension_value("completeness", "null_columns", [])

    @property
    def data_type_consistency(self) -> float:
        self._warn_flat_accessor("data_type_consistency")
        return self._dimension_value("consistency", "data_type_consistency", 100.0)

    @property
    def format_violations(self) -> int:
        self._warn_flat_accessor("format_violations")
        return self._dimension_value("consistency", "format_violations", 0)

    @property
    def encoding_issues(self) -> int:
        self._warn_flat_accessor("encoding_issues")
        return self._dimension_value("consistency", "encoding_issues", 0)

    @property
    def duplicate_rows(self) -> int:
        self._warn_flat_accessor("duplicate_rows")
        return self._dimension_value("uniqueness", "duplicate_rows", 0)

    @property
    def key_uniqueness(self) -> float:
        self._warn_flat_accessor("key_uniqueness")
        return self._dimension_value("uniqueness", "key_uniqueness", 100.0)

    @property
    def high_cardinality_warning(self) -> bool:
        self._warn_flat_accessor("high_cardinality_warning")
        return self._dimension_value("uniqueness", "high_cardinality_warning", False)

    @property
    def outlier_ratio(self) -> float:
        self._warn_flat_accessor("outlier_ratio")
        return self._dimension_value("accuracy", "outlier_ratio", 0.0)

    @property
    def range_violations(self) -> int:
        self._warn_flat_accessor("range_violations")
        return self._dimension_value("accuracy", "range_violations", 0)

    @property
    def negative_values_in_positive(self) -> int:
        self._warn_flat_accessor("negative_values_in_positive")
        return self._dimension_value("accuracy", "negative_values_in_positive", 0)

    @property
    def future_dates_count(self) -> int:
        self._warn_flat_accessor("future_dates_count")
        return self._dimension_value("timeliness", "future_dates_count", 0)

    @property
    def stale_data_ratio(self) -> float:
        self._warn_flat_accessor("stale_data_ratio")
        return self._dimension_value("timeliness", "stale_data_ratio", 0.0)

    @property
    def temporal_violations(self) -> int:
        self._warn_flat_accessor("temporal_violations")
        return self._dimension_value("timeliness", "temporal_violations", 0)

    @property
    def invalid_date_values(self) -> int:
        self._warn_flat_accessor("invalid_date_values")
        return self._dimension_value("timeliness", "invalid_date_values", 0)

    @property
    def completeness(self) -> dict[str, _Any] | None:
        return self._d.get("completeness")

    @property
    def consistency(self) -> dict[str, _Any] | None:
        return self._d.get("consistency")

    @property
    def uniqueness(self) -> dict[str, _Any] | None:
        return self._d.get("uniqueness")

    @property
    def accuracy(self) -> dict[str, _Any] | None:
        return self._d.get("accuracy")

    @property
    def timeliness(self) -> dict[str, _Any] | None:
        return self._d.get("timeliness")

    @property
    def validity(self) -> dict[str, _Any] | None:
        return self._d.get("validity")

    @property
    def precision(self) -> dict[str, _Any] | None:
        return self._d.get("precision")

    @property
    def score_weights(self) -> dict[str, float]:
        weights = self._d.get("score_weights")
        if isinstance(weights, dict):
            return weights
        return self._DEFAULT_SCORE_WEIGHTS

    def overall_quality_score(self) -> float | None:
        """Overall score (0-100), None when no dimension was assessable."""
        return self._d.get("overall_score")

    def assessed_dimensions(self) -> list[str]:
        """Dimensions that had data to assess. Empty for reports serialized
        before denominators existed — no score is fabricated for them."""
        return self._d.get("assessed_dimensions") or []

    def dimension_scores(self) -> dict[str, float | None]:
        """Per-dimension scores (0-100), None when not assessable."""
        scores = self._d.get("dimension_scores")
        if isinstance(scores, dict):
            return scores
        return dict.fromkeys(_QUALITY_DIMENSIONS)


# The states a `quality_status` document may declare, and whether each one
# comes with a quality assessment. `computed` is the only state that does.
_QUALITY_STATES = {
    "computed": True,
    "not_requested": False,
    "no_data": False,
    "withheld_by_projection": False,
    "failed": False,
    "unrecorded": False,
}


_MISSING = object()


def _read_quality_status(status: _Any, assessed: bool) -> tuple[str, str | None]:
    """Read `quality_status` back, refusing a document that contradicts itself.

    The Rust reader already rejects an unknown state and a `failed` with no
    message, so accepting them here would make the same file load in one
    language and fail in the other. Worse, both re-serialize to a document the
    committed schema rejects: a report would round-trip into an invalid one.

    A state that disagrees with the presence of `quality` is refused for the
    reason the field exists at all -- a gate must not be handed an assessment
    by a report that says the computation never finished.

    Only an *absent* key is legacy. An explicit ``null`` is a malformed current
    document, which the committed schema also rejects, so it is not repaired
    into a reason the writer never recorded.
    """
    if status is _MISSING:
        # Written before the field existed. An assessment proves the
        # computation ran; its absence proves nothing.
        return ("computed" if assessed else "unrecorded"), None

    if not isinstance(status, dict):
        raise ValueError(f"quality_status must be an object, got {type(status).__name__}")

    state = status.get("state")
    # Check the type before the membership test: an unhashable value such as a
    # list raises TypeError out of `in`, which is not the ValueError this
    # function promises for a malformed document.
    if not isinstance(state, str) or state not in _QUALITY_STATES:
        raise ValueError(
            f"unknown quality_status state {state!r}; expected one of "
            f"{', '.join(sorted(_QUALITY_STATES))}"
        )

    error = status.get("error")
    if state == "failed":
        if not isinstance(error, str):
            raise ValueError("quality_status `failed` must carry a string `error`")
    elif error is not None:
        raise ValueError(f"quality_status {state!r} must not carry an `error`")

    if _QUALITY_STATES[state] != assessed:
        raise ValueError(
            f"quality_status {state!r} contradicts the report: quality is "
            f"{'present' if assessed else 'absent'}"
        )
    return state, error if state == "failed" else None


class _DictBackedReport:
    """Read-only stand-in for the native ProfileReport, built from to_dict()."""

    def __init__(self, d: dict[str, _Any]):
        execution = d.get("execution") or {}
        self.source = d.get("source")
        self.source_type = d.get("source_type")
        self.engine = execution.get("engine")
        self.rows_processed = execution.get("rows_processed")
        self.columns_detected = execution.get("columns_detected")
        self.scan_time_ms = execution.get("scan_time_ms")
        self.source_exhausted = execution.get("source_exhausted")
        self.truncation_reason = execution.get("truncation_reason")
        self.bytes_consumed = execution.get("bytes_consumed")
        self.throughput_rows_sec = execution.get("throughput_rows_sec")
        self.memory_peak_mb = execution.get("memory_peak_mb")
        self.error_count = execution.get("error_count")
        # Additive field: reports written before it existed omit the key and
        # must read back as 0 (not None), matching the Rust serde default.
        self.ragged_row_count = execution.get("ragged_row_count") or 0
        self.sampling_applied = bool(execution.get("sampling_applied", False))
        self.sampling_ratio = execution.get("sampling_ratio")
        self.sampled_row_ranges = execution.get("sampled_row_ranges")
        # Additive field, written by to_dict() only when hints were supplied.
        # It was never read back, so a reloaded report reported no bindings —
        # indistinguishable from a run profiled without hints at all (#512).
        bindings = d.get("semantic_hint_bindings")
        self.semantic_hint_bindings = list(bindings) if isinstance(bindings, list) else []
        quality = d.get("quality")
        self.quality = _DictQuality(quality) if isinstance(quality, dict) else None
        self.quality_score = quality.get("overall_score") if isinstance(quality, dict) else None
        # Additive field. A document written before it exists cannot say why
        # quality is absent, but one carrying an assessment proves it was
        # computed -- the same rule the Rust deserializer applies.
        self.quality_status, self.quality_error = _read_quality_status(
            d.get("quality_status", _MISSING), self.quality is not None
        )
        self.column_profiles = [_DictColumn(c) for c in d.get("columns", [])]
