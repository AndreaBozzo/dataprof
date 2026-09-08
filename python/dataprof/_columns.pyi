"""Type stubs for _columns."""

from __future__ import annotations

from typing import Any, Any as _Any

from ._dataprof import ColumnProfile, Pattern as _NativePattern
from ._report_backing import _DictPattern

def column_to_dict(col: ColumnProfile) -> dict[str, Any]:
    """Convert a ColumnProfile to the nested dict layout used in ``report.to_dict()['columns']``."""
    ...

def _homogeneity_counts(value: _Any) -> dict[str, int] | None: ...
def _type_mixture(col: ColumnProfile) -> list[tuple[str, int, float]]: ...
def _column_record(col: ColumnProfile) -> dict[str, _Any]: ...
def _dominant_pattern(col: ColumnProfile) -> _NativePattern | _DictPattern | None: ...
