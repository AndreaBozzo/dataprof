"""Type stubs for _columns."""

from __future__ import annotations

from typing import Any, Any as _Any

from ._accessors import ColumnProfile, Pattern as _Pattern
from ._dataprof import ColumnProfile as _NativeColumn

def column_to_dict(col: ColumnProfile | _NativeColumn) -> dict[str, Any]:
    """Convert a ColumnProfile to the nested dict layout used in ``report.to_dict()['columns']``."""
    ...

def _homogeneity_counts(value: _Any) -> dict[str, int] | None: ...
def _type_mixture(col: ColumnProfile) -> list[tuple[str, int, float]]: ...
def _column_record(col: ColumnProfile) -> dict[str, _Any]: ...
def _dominant_pattern(col: ColumnProfile) -> _Pattern | None: ...
