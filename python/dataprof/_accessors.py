"""Shared read-only report views over native or restored values.

Backings only supply values through ``_Accessor.get``. Public accessors, nested
object construction, and removed-name handling are implemented here once.
Native readers never serialize: their floats keep the extension's precision.
"""

from __future__ import annotations

from typing import Any, Generic, NoReturn, Protocol, TypeVar, overload

from ._dataprof import DataQualityMetrics as _NativeQuality


class _Accessor(Protocol):
    """Read a named value; nested records are accessors, never public views.

    ``None`` is a value, not a missing-value sentinel. Backings handle their
    own layout and legacy defaults before supplying values to a view.
    """

    def get(self, name: str) -> Any: ...


class _NativeAccessor:
    """Read the extension without rounding, copying documents, or recomputing metrics."""

    def __init__(self, value: Any):
        self._value = value

    def get(self, name: str) -> Any:
        value = getattr(self._value, name)
        if name in ("overall_quality_score", "assessed_dimensions", "dimension_scores"):
            return value()
        if name == "quality":
            return None if value is None else _NativeAccessor(value)
        if name in ("column_profiles", "patterns"):
            return None if value is None else [_NativeAccessor(item) for item in value]
        return value


def _copy_collections(value: Any) -> Any:
    """Detach JSON containers while preserving nested accessor objects."""
    if isinstance(value, dict):
        return {key: _copy_collections(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copy_collections(item) for item in value]
    return value


class _MappingAccessor:
    """Read normalized stored values; unknown fields are not exposed as attributes."""

    def __init__(self, values: dict[str, Any]):
        self._values = _copy_collections(values)

    def get(self, name: str) -> Any:
        return _copy_collections(self._values.get(name))


class _View:
    __slots__ = ("_accessor",)

    def __init__(self, accessor: _Accessor):
        self._accessor = accessor


_T = TypeVar("_T")


class _Field(Generic[_T]):
    """A read-only field whose implementation is independent of its backing."""

    def __init__(self, *, section: str | None = None):
        self._document_section = section

    def __set_name__(self, owner: type, name: str) -> None:
        self._name = name

    @overload
    def __get__(self, instance: None, owner: type | None = None) -> _Field[_T]: ...
    @overload
    def __get__(self, instance: _View, owner: type | None = None) -> _T: ...
    def __get__(self, instance: _View | None, owner: type | None = None) -> _T | _Field[_T]:
        return self if instance is None else instance._accessor.get(self._name)

    def __set__(self, instance: _View, value: Any) -> NoReturn:
        raise AttributeError(f"attribute {self._name!r} is read-only")


class Pattern(_View):
    """Detected pattern evidence shared by live and restored column views."""

    __slots__ = ()

    name = _Field[str]()
    regex = _Field[str]()
    match_count = _Field[int]()
    match_percentage = _Field[float]()
    category = _Field[str]()
    confidence = _Field[float]()


class ColumnProfile(_View):
    """Column statistics, with the same read-only accessors for either backing.

    Native floats retain full precision. Restored values retain the precision
    of their serialized document; absent measurements remain ``None``.
    """

    __slots__ = ()

    name = _Field[str]()
    data_type = _Field[str]()
    total_count = _Field[int]()
    null_count = _Field[int]()
    unique_count = _Field[int | None]()
    unique_count_is_approximate = _Field[bool | None]()
    invalid_count = _Field[int | None]()
    type_homogeneity = _Field[dict[str, int] | None]()
    null_percentage = _Field[float | None]()
    uniqueness_ratio = _Field[float | None]()
    min = _Field[float | None](section="stats")
    max = _Field[float | None](section="stats")
    mean = _Field[float | None](section="stats")
    std_dev = _Field[float | None](section="stats")
    variance = _Field[float | None](section="stats")
    median = _Field[float | None](section="stats")
    mode = _Field[float | None](section="stats")
    skewness = _Field[float | None](section="stats")
    kurtosis = _Field[float | None](section="stats")
    coefficient_of_variation = _Field[float | None](section="stats")
    quartiles = _Field[dict[str, float] | None](section="stats")
    is_approximate = _Field[bool | None](section="stats")
    outlier_count = _Field[int | None](section="stats")
    min_length = _Field[int | None](section="stats")
    max_length = _Field[int | None](section="stats")
    avg_length = _Field[float | None](section="stats")
    true_count = _Field[int | None](section="stats")
    false_count = _Field[int | None](section="stats")
    true_ratio = _Field[float | None](section="stats")

    @property
    def patterns(self) -> list[Pattern] | None:
        patterns = self._accessor.get("patterns")
        return None if patterns is None else [Pattern(pattern) for pattern in patterns]

    def __repr__(self) -> str:
        nulls = "n/a" if self.null_percentage is None else f"{self.null_percentage:.1f}%"
        parts = [f"name='{self.name}'", f"type='{self.data_type}'", f"nulls={nulls}"]
        if self.unique_count is not None:
            parts.append(f"unique={self.unique_count}")
        if self.mean is not None:
            parts.append(f"mean={self.mean:.4f}")
        elif self.avg_length is not None:
            parts.append(f"avg_len={self.avg_length:.4f}")
        elif self.true_count is not None:
            parts.append(f"true={self.true_count}({(self.true_ratio or 0.0) * 100:.0f}%)")
        return f"ColumnProfile({', '.join(parts)})"


class DataQualityMetrics(_View):
    """Quality evidence and scores read through one backing-independent surface."""

    __slots__ = ()

    completeness = _Field[dict[str, Any] | None]()
    consistency = _Field[dict[str, Any] | None]()
    uniqueness = _Field[dict[str, Any] | None]()
    accuracy = _Field[dict[str, Any] | None]()
    timeliness = _Field[dict[str, Any] | None]()
    validity = _Field[dict[str, Any] | None]()
    precision = _Field[dict[str, Any] | None]()
    low_sample_warning = _Field[bool]()
    score_weights = _Field[dict[str, float]]()

    def __getattr__(self, name: str) -> NoReturn:
        # #509's removed names and migration hints remain owned by the native
        # type. Both public backings use this one path; no parallel name list.
        _NativeQuality._attribute_error(name)

    def overall_quality_score(self) -> float | None:
        return self._accessor.get("overall_quality_score")

    def assessed_dimensions(self) -> list[str]:
        return self._accessor.get("assessed_dimensions")

    def dimension_scores(self) -> dict[str, float | None]:
        return self._accessor.get("dimension_scores")

    def __str__(self) -> str:
        score = self.overall_quality_score()
        if score is None:
            return "DataQualityMetrics(not assessed)"
        parts = [f"score={score:.1f}%"]
        scores = self.dimension_scores()
        for dimension in ("completeness", "consistency", "uniqueness"):
            value = scores.get(dimension)
            if value is not None:
                parts.append(f"{dimension}={value:.1f}%")
        return f"DataQualityMetrics({', '.join(parts)})"

    def __repr__(self) -> str:
        value = self.overall_quality_score()
        score = "n/a" if value is None else f"{value:.1f}%"
        return (
            f"DataQualityMetrics(score={score}, "
            f"assessed=[{', '.join(self.assessed_dimensions())}], "
            f"low_sample_warning={str(self.low_sample_warning).lower()})"
        )


class _ReportView(_View):
    """The sole attribute surface consumed by ProfileReport's public methods."""

    __slots__ = ()

    source = _Field[str]()
    source_type = _Field[str]()
    engine = _Field[str | None]()
    rows_processed = _Field[int]()
    columns_detected = _Field[int]()
    scan_time_ms = _Field[int]()
    source_exhausted = _Field[bool]()
    truncation_reason = _Field[str | None]()
    bytes_consumed = _Field[int | None]()
    throughput_rows_sec = _Field[float | None]()
    memory_peak_mb = _Field[float | None]()
    error_count = _Field[int]()
    ragged_row_count = _Field[int]()
    unterminated_quote = _Field[bool | None]()
    schema_version = _Field[int]()
    sampling_applied = _Field[bool]()
    sampling_ratio = _Field[float | None]()
    sampled_row_ranges = _Field[list[list[int]] | None]()
    recovery_events = _Field[list[dict[str, str]] | None]()
    semantic_hint_bindings = _Field[list[dict[str, Any]]]()
    metric_semantics = _Field[dict[str, str] | None]()
    quality_score = _Field[float | None]()
    quality_sampled_dimensions = _Field[list[str] | None]()
    quality_score_bounds = _Field[dict[str, Any] | None]()
    quality_status = _Field[str]()
    quality_error = _Field[str | None]()

    @property
    def quality(self) -> DataQualityMetrics | None:
        quality = self._accessor.get("quality")
        return None if quality is None else DataQualityMetrics(quality)

    @property
    def column_profiles(self) -> list[ColumnProfile]:
        return [ColumnProfile(column) for column in self._accessor.get("column_profiles")]


ColumnProfile.__module__ = "dataprof"
DataQualityMetrics.__module__ = "dataprof"
