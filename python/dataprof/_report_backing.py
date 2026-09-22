"""Normalize saved report layouts into the shared accessor protocol.

This module only decodes the document's layout and legacy defaults. Public
properties and methods live in _accessors, exactly as for native reports.
"""

from __future__ import annotations

import math
from typing import Any

from ._accessors import ColumnProfile, _Field, _MappingAccessor, _ReportView
from ._columns import _homogeneity_counts
from ._report_schema import _QUALITY_DIMENSIONS

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


def _read_quality_status(status: Any, assessed: bool) -> tuple[str, str | None]:
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


_DEFAULT_SCORE_WEIGHTS = {
    "completeness": 0.25,
    "consistency": 0.20,
    "uniqueness": 0.15,
    "accuracy": 0.15,
    "timeliness": 0.10,
    "validity": 0.10,
    "precision": 0.05,
}


def _column_accessor(document: dict[str, Any]) -> _MappingAccessor:
    values = dict(document)
    stats = document.get("stats")
    # The shared accessor declares its persisted location. There is no second
    # stat-attribute list to drift, and unknown keys cannot override core fields.
    for name, field in vars(ColumnProfile).items():
        if isinstance(field, _Field) and field._document_section == "stats":
            values[name] = stats.get(name) if isinstance(stats, dict) else None
    values["type_homogeneity"] = _homogeneity_counts(document.get("type_homogeneity"))
    patterns = document.get("patterns")
    values["patterns"] = (
        [
            _MappingAccessor({**p, "confidence": p.get("confidence", 0.0)})
            for p in patterns
            if isinstance(p, dict)
        ]
        if isinstance(patterns, list)
        else None
    )
    return _MappingAccessor(values)


def _quality_accessor(document: dict[str, Any]) -> _MappingAccessor:
    values = dict(document)
    values["overall_quality_score"] = document.get("overall_score")
    values["low_sample_warning"] = bool(document.get("low_sample_warning", False))
    values["assessed_dimensions"] = document.get("assessed_dimensions") or []
    scores = document.get("dimension_scores")
    values["dimension_scores"] = (
        scores if isinstance(scores, dict) else dict.fromkeys(_QUALITY_DIMENSIONS)
    )
    values["score_weights"] = _read_score_weights(document)
    return _MappingAccessor(values)


def _read_score_weights(document: dict[str, Any]) -> dict[str, float]:
    """Read the weights as the Rust reader reads `QualityScoreWeights`.

    Only a missing key takes the defaults. A mapping fills its missing
    dimensions from them and ignores unknown ones; anything else is an error,
    since substituted weights would sit next to a score they did not produce.
    Weights must be finite: JSON cannot carry NaN or infinity, so the Rust
    reader never accepts them, and neither does an integer beyond f64 range.
    """
    if "score_weights" not in document:
        return dict(_DEFAULT_SCORE_WEIGHTS)
    weights = document["score_weights"]
    if not isinstance(weights, dict):
        raise ValueError(
            f"from_dict(): 'quality.score_weights' must be a mapping of numbers, got {weights!r}."
        )
    read = {}
    for name, default in _DEFAULT_SCORE_WEIGHTS.items():
        value = weights.get(name, default)
        number = math.nan
        if not isinstance(value, bool) and isinstance(value, int | float):
            try:
                number = float(value)
            except OverflowError:
                number = math.inf
        if not math.isfinite(number):
            raise ValueError(
                f"from_dict(): 'quality.score_weights.{name}' must be a finite number, "
                f"got {value!r}."
            )
        read[name] = number
    return read


def _report_from_dict(document: dict[str, Any]) -> _ReportView:
    execution = document.get("execution") or {}
    values = dict(execution)
    values["source"] = document.get("source")
    values["source_type"] = document.get("source_type")
    # Only these additive fields have legacy defaults. Unknown measurements
    # stay absent; in particular an unknown history is not an empty history.
    values["ragged_row_count"] = execution.get("ragged_row_count") or 0
    values["sampling_applied"] = bool(execution.get("sampling_applied", False))
    bindings = document.get("semantic_hint_bindings")
    values["semantic_hint_bindings"] = list(bindings) if isinstance(bindings, list) else []
    quality = document.get("quality")
    values["quality"] = _quality_accessor(quality) if isinstance(quality, dict) else None
    values["quality_score"] = quality.get("overall_score") if isinstance(quality, dict) else None
    values["quality_sampled_dimensions"] = (
        quality.get("sampled_dimensions") if isinstance(quality, dict) else None
    )
    values["quality_status"], values["quality_error"] = _read_quality_status(
        document.get("quality_status", _MISSING), values["quality"] is not None
    )
    values["column_profiles"] = [_column_accessor(c) for c in document.get("columns", [])]
    return _ReportView(_MappingAccessor(values))
