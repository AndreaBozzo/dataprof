"""Argument checks that give profiling options a readable error.

The native extension converts every option itself, and its own failures read as
conversion internals ("Can't extract `str` to `Vec`", "can't convert negative
int to unsigned", "'set' object is not an instance of 'Sequence'", "'float'
object cannot be interpreted as an integer"). These checks run first and name
the parameter and the form it accepts. They never change what a valid value
does.
"""

from __future__ import annotations as _annotations

import sys as _sys
from collections.abc import Mapping as _Mapping
from typing import Any as _Any

_U64_MAX = 2**64 - 1
_USIZE_MAX = _sys.maxsize * 2 + 1

# Option name -> largest value the native side accepts.
_UNSIGNED_LIMITS = {
    "max_rows": _U64_MAX,
    "chunk_size": _USIZE_MAX,
    "memory_limit_mb": _USIZE_MAX,
    "progress_interval_ms": _U64_MAX,
    "duplicate_rows_exact_limit": _USIZE_MAX,
}

# Option name -> what its list holds, for the error message.
_NAME_LISTS = {
    "quality_dimensions": "quality dimension names",
    "metrics": "metric pack names",
    "columns": "column names",
    "positive_columns": "column names",
    "identifier_columns": "column names",
    "temporal_columns": "column names",
}


def _check_unsigned(name: str, value: _Any) -> None:
    if value is None:
        return
    limit = _UNSIGNED_LIMITS[name]
    # A bool is an int. True and False stay on the integer path, as they do
    # when the native conversion accepts them. A float, including 2.0, is the
    # wrong type. The constructors raise TypeError and name that type.
    if isinstance(value, float):
        raise TypeError(
            f"{name} must be an integer between 0 and {limit}, got {type(value).__name__}."
        )
    if not isinstance(value, int):
        return
    if not 0 <= value <= limit:
        raise ValueError(f"{name} must be an integer between 0 and {limit}, got {value}.")


def _non_sequence(value: _Any) -> bool:
    """A set, a mapping, or another iterable that has no sequence protocol.

    An object with ``__getitem__`` is left to the native conversion. A pandas
    Index or a numpy array of names is a sequence there, and rejecting it here
    would change a value that already works.
    """
    if isinstance(value, (set, frozenset, _Mapping)):
        return True
    return not hasattr(value, "__getitem__")


def _check_name_list(name: str, value: _Any) -> None:
    if value is None:
        return
    held = _NAME_LISTS[name]
    if isinstance(value, (str, bytes)):
        kind = type(value).__name__
        suggestion = f"[{value!r}]"
        raise TypeError(f"{name} must be a list of {held}, got {kind}; did you mean {suggestion}?")
    if isinstance(value, (list, tuple)):
        for item in value:
            if not isinstance(item, str):
                raise TypeError(
                    f"{name} must be a list of {held}, got {type(item).__name__} {item!r}."
                )
        return
    if _non_sequence(value):
        raise TypeError(f"{name} must be a list of {held}, got {type(value).__name__}.")


def _check_temporal_pairs(value: _Any) -> None:
    if value is None:
        return
    if isinstance(value, (str, bytes)):
        kind = type(value).__name__
        raise TypeError(
            f"temporal_pairs must be a list of (start, end) column pairs, got {kind}; "
            "did you mean [('start_column', 'end_column')]?"
        )
    if isinstance(value, tuple) and len(value) == 2 and all(isinstance(v, str) for v in value):
        raise TypeError(
            "temporal_pairs must be a list of (start, end) column pairs, got a single pair; "
            f"did you mean [{value!r}]?"
        )
    if isinstance(value, (list, tuple)):
        for entry in value:
            if isinstance(entry, (str, bytes)):
                raise TypeError(
                    "temporal_pairs entries must be (start, end) column pairs, "
                    f"got {type(entry).__name__} {entry!r}; did you mean "
                    "[('start_column', 'end_column')]?"
                )
            if not isinstance(entry, (list, tuple)) and _non_sequence(entry):
                raise TypeError(
                    "temporal_pairs entries must be (start, end) column pairs, "
                    f"got {type(entry).__name__} {entry!r}."
                )
        return
    if _non_sequence(value):
        raise TypeError(
            "temporal_pairs must be a list of (start, end) column pairs, "
            f"got {type(value).__name__}."
        )


def _check_unique_count_exact_limits(value: _Any) -> None:
    if value is None:
        return
    if not isinstance(value, dict):
        raise TypeError("unique_count_exact_limits must be a dict from column names to limits.")
    for name, limit in value.items():
        if not isinstance(name, str) or not isinstance(limit, int):
            raise TypeError("unique_count_exact_limits must map string names to integer limits.")
        if not 1 <= limit <= _USIZE_MAX:
            raise ValueError(
                f"unique_count_exact_limits[{name!r}] must be between 1 and {_USIZE_MAX}."
            )


def _check_profile_options(options: _Mapping[str, _Any]) -> None:
    """Reject a bad list option or integer option before native conversion."""
    for name in _NAME_LISTS:
        _check_name_list(name, options.get(name))
    _check_temporal_pairs(options.get("temporal_pairs"))
    _check_unique_count_exact_limits(options.get("unique_count_exact_limits"))
    for name in _UNSIGNED_LIMITS:
        _check_unsigned(name, options.get(name))
