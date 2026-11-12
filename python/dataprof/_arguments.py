"""Argument checks that give profiling options a readable error.

The native extension converts every option itself, and its own failures read as
conversion internals ("Can't extract `str` to `Vec`", "can't convert negative
int to unsigned"). These checks run first and name the parameter and the form
it accepts. They never change what a valid value does.
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
    if value is None or not isinstance(value, int):
        return
    limit = _UNSIGNED_LIMITS[name]
    if not 0 <= value <= limit:
        raise ValueError(f"{name} must be an integer between 0 and {limit}, got {value}.")


def _check_name_list(name: str, value: _Any) -> None:
    if isinstance(value, (str, bytes)):
        kind = type(value).__name__
        suggestion = f"[{value!r}]"
        raise TypeError(
            f"{name} must be a list of {_NAME_LISTS[name]}, got {kind}; did you mean {suggestion}?"
        )


def _check_temporal_pairs(value: _Any) -> None:
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


def _check_profile_options(options: _Mapping[str, _Any]) -> None:
    """Reject list options given as one string and unsigned options out of range."""
    for name in _NAME_LISTS:
        _check_name_list(name, options.get(name))
    _check_temporal_pairs(options.get("temporal_pairs"))
    for name in _UNSIGNED_LIMITS:
        _check_unsigned(name, options.get(name))
