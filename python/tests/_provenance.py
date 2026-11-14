"""Strip provenance before comparing serialized columns across routes."""

from __future__ import annotations

from typing import Any


def without_native_type(columns: Any) -> Any:
    """Serialized columns without each column's declared type.

    ``native_type`` records what the source declared (#906): an Arrow type for
    Arrow, polars and Parquet, the dtype for pandas, none for text inputs. Two
    routes over the same data declare different types by definition, so the
    field is provenance and stays outside the serialized-value parity
    comparison. Takes a list of column mappings, or a mapping of them by name.
    """
    if isinstance(columns, dict):
        return {name: _strip(column) for name, column in columns.items()}
    return [_strip(column) for column in columns]


def _strip(column: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in column.items() if key != "native_type"}
