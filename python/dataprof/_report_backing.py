"""Normalize saved report layouts into the shared accessor protocol.

This module only decodes the document's layout and legacy defaults. Public
properties and methods live in _accessors, exactly as for native reports.
"""

from __future__ import annotations

import math
from typing import Any

from ._accessors import ColumnProfile, _Field, _MappingAccessor, _ReportView
from ._columns import _LEXICAL_CLASS_ORDER, _homogeneity_counts
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


def _read_field(
    document: dict[str, Any],
    key: str,
    path: str,
    expected: str,
    accepts: Any,
    default: Any,
) -> Any:
    """Read one field as the committed schema types it (#763).

    Only a missing key takes `default`, the documented compatibility value. A
    value of the wrong type is refused with the field's path: read as the
    default it would become a believable flag, count, "not analyzed" or
    "analyzed, nothing found" that the writer never recorded.
    """
    if key not in document:
        return default
    value = document[key]
    if not accepts(value):
        raise ValueError(f"from_dict(): {path!r} must be {expected}, got {value!r}.")
    return value


def _is_bool(value: Any) -> bool:
    return isinstance(value, bool)


def _is_count(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _is_finite_number(value: Any) -> bool:
    # bool before int: bool is an int subclass, and True == 1.
    if not isinstance(value, int | float) or isinstance(value, bool):
        return False
    try:
        return math.isfinite(float(value))
    except (OverflowError, ValueError):
        return False


def _is_list_of_mappings(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(entry, dict) for entry in value)


def _is_list_of_dimension_names(value: Any) -> bool:
    return isinstance(value, list) and all(
        isinstance(entry, str) and entry in _QUALITY_DIMENSIONS for entry in value
    )


def _is_homogeneity_counts(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    return all(name in value and _is_count(value[name]) for name in _LEXICAL_CLASS_ORDER)


def _column_accessor(document: dict[str, Any], path: str) -> _MappingAccessor:
    values = dict(document)
    # `null` is the schema's "not analyzed", the same answer as a missing key.
    stats = _read_field(
        document,
        "stats",
        f"{path}.stats",
        "a mapping or null",
        lambda value: value is None or isinstance(value, dict),
        None,
    )
    # The shared accessor declares its persisted location. There is no second
    # stat-attribute list to drift, and unknown keys cannot override core fields.
    for name, field in vars(ColumnProfile).items():
        if isinstance(field, _Field) and field._document_section == "stats":
            values[name] = stats.get(name) if stats is not None else None
    homogeneity = _read_field(
        document,
        "type_homogeneity",
        f"{path}.type_homogeneity",
        "a four-count mapping or null",
        lambda value: value is None or _is_homogeneity_counts(value),
        None,
    )
    values["type_homogeneity"] = (
        _homogeneity_counts(homogeneity) if homogeneity is not None else None
    )
    patterns = _read_field(
        document,
        "patterns",
        f"{path}.patterns",
        "a list of mappings or null",
        lambda value: value is None or _is_list_of_mappings(value),
        None,
    )
    values["patterns"] = (
        [
            _MappingAccessor(
                {
                    **p,
                    "confidence": _read_field(
                        p,
                        "confidence",
                        f"{path}.patterns[{index}].confidence",
                        "a finite number",
                        _is_finite_number,
                        0.0,
                    ),
                }
            )
            for index, p in enumerate(patterns)
        ]
        if patterns is not None
        else None
    )
    return _MappingAccessor(values)


def _quality_accessor(document: dict[str, Any]) -> _MappingAccessor:
    values = dict(document)
    overall_score = _read_field(
        document,
        "overall_score",
        "quality.overall_score",
        "a finite number or null",
        lambda value: value is None or _is_finite_number(value),
        None,
    )
    values["overall_score"] = overall_score
    values["overall_quality_score"] = float(overall_score) if overall_score is not None else None
    values["low_sample_warning"] = _read_field(
        document,
        "low_sample_warning",
        "quality.low_sample_warning",
        "a boolean",
        _is_bool,
        False,
    )
    values["assessed_dimensions"] = list(
        _read_field(
            document,
            "assessed_dimensions",
            "quality.assessed_dimensions",
            "a list of known dimension names",
            _is_list_of_dimension_names,
            [],
        )
    )
    dimension_scores = _read_field(
        document,
        "dimension_scores",
        "quality.dimension_scores",
        "a mapping",
        lambda value: isinstance(value, dict),
        dict.fromkeys(_QUALITY_DIMENSIONS),
    )
    read_scores = dict(dimension_scores)
    for dim, score in dimension_scores.items():
        if score is not None and not _is_finite_number(score):
            raise ValueError(
                f"from_dict(): 'quality.dimension_scores.{dim}' must be a finite number or null, "
                f"got {score!r}."
            )
        read_scores[dim] = float(score) if score is not None else None
    values["dimension_scores"] = read_scores
    values["score_weights"] = _read_score_weights(document)
    # Whether the summary named its weights. Before 0.12 a summary dropped
    # custom weights, so the defaults filled in above may not be the ones
    # behind its stored score, and a re-export must not claim they are.
    values["score_weights_recorded"] = "score_weights" in document
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


# Each named metric definition and the values a document may record for it.
_METRIC_SEMANTICS = {
    "text_length_unit": ("unicode_scalar",),
    "null_tokens": ("common_markers",),
    "pattern_match_count": ("validated",),
    "shape_moments": ("adjusted_sample",),
}


#: The values `analysis_settings.pattern_locale` may hold, as the Rust
#: `PatternLocale` serializes them: no locale, or a supported locale's code.
_PATTERN_LOCALES = ("none", "CA", "DE", "FR", "GB", "IT", "US")


def _read_analysis_settings(document: dict[str, Any]) -> dict[str, str] | None:
    """Read `analysis_settings` as the Rust reader reads `AnalysisSettings`.

    A missing key is a document written before dataprof recorded it, so the
    settings are unknown: None, never the defaults. Unknown settings from a
    later writer are ignored; a malformed or missing `pattern_locale` is an
    error, as it is in Rust.
    """
    if "analysis_settings" not in document:
        return None
    settings = document["analysis_settings"]
    if not isinstance(settings, dict):
        raise ValueError(f"from_dict(): 'analysis_settings' must be a mapping, got {settings!r}.")
    locale = settings.get("pattern_locale")
    if locale not in _PATTERN_LOCALES:
        raise ValueError(
            "from_dict(): 'analysis_settings.pattern_locale' must be one of "
            f"{', '.join(_PATTERN_LOCALES)}, got {locale!r}."
        )
    return {"pattern_locale": locale}


def _read_metric_semantics(document: dict[str, Any]) -> dict[str, str] | None:
    """Read `metric_semantics` as the Rust reader reads `MetricSemantics`.

    A missing key is a document written before dataprof recorded it, so its
    definitions are unknown: None, never the current ones. Unknown definition
    names from a later writer are ignored; a malformed value is an error.
    """
    if "metric_semantics" not in document:
        return None
    semantics = document["metric_semantics"]
    if not isinstance(semantics, dict):
        raise ValueError(f"from_dict(): 'metric_semantics' must be a mapping, got {semantics!r}.")
    read = {}
    for name, allowed in _METRIC_SEMANTICS.items():
        if name not in semantics:
            continue
        value = semantics[name]
        if value not in allowed:
            raise ValueError(
                f"from_dict(): 'metric_semantics.{name}' must be one of "
                f"{', '.join(allowed)}, got {value!r}."
            )
        read[name] = value
    return read


def _report_from_dict(document: dict[str, Any]) -> _ReportView:
    execution = document.get("execution") or {}
    values = dict(execution)
    # Flat summaries deliberately omit identity; additive unknown keys cannot
    # turn a summary into a canonical report.
    values["id"] = None
    values["timestamp"] = None
    values["source"] = document.get("source")
    values["source_type"] = document.get("source_type")
    # Only these additive fields have legacy defaults. Unknown measurements
    # stay absent; in particular an unknown history is not an empty history.
    values["ragged_row_count"] = _read_field(
        execution,
        "ragged_row_count",
        "execution.ragged_row_count",
        "a non-negative integer",
        _is_count,
        0,
    )
    # Absent means a pre-0.10 document, written before versioning. Callers
    # that must not read a defaulted field as a measurement branch on it.
    values["schema_version"] = document.get("schema_version", 0)
    values["sampling_applied"] = _read_field(
        execution,
        "sampling_applied",
        "execution.sampling_applied",
        "a boolean",
        _is_bool,
        False,
    )
    values["semantic_hint_bindings"] = list(
        _read_field(
            document,
            "semantic_hint_bindings",
            "semantic_hint_bindings",
            "a list of mappings",
            _is_list_of_mappings,
            [],
        )
    )
    values["metric_semantics"] = _read_metric_semantics(document)
    values["analysis_settings"] = _read_analysis_settings(document)
    quality = _read_field(
        document,
        "quality",
        "quality",
        "a mapping or null",
        lambda value: value is None or isinstance(value, dict),
        None,
    )
    values["quality"] = _quality_accessor(quality) if quality is not None else None
    values["quality_score"] = (
        values["quality"].get("overall_quality_score") if values["quality"] is not None else None
    )
    values["quality_sampled_dimensions"] = (
        quality.get("sampled_dimensions") if quality is not None else None
    )
    # A flat summary records no exact list, so no component can be found
    # missing from both lists: "not sampled" reads as exact, as in Rust.
    values["quality_unlisted_components"] = []
    values["quality_score_bounds"] = (
        _read_field(
            quality,
            "score_bounds",
            "quality.score_bounds",
            "a mapping or null",
            lambda value: value is None or isinstance(value, dict),
            None,
        )
        if quality is not None
        else None
    )
    values["quality_status"], values["quality_error"] = _read_quality_status(
        document.get("quality_status", _MISSING), values["quality"] is not None
    )
    values["column_profiles"] = [
        _column_accessor(column, f"columns[{index}]")
        for index, column in enumerate(document.get("columns", []))
    ]
    return _ReportView(_MappingAccessor(values))
