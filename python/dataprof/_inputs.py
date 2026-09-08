"""Dependency-free input decoding into named columns."""

from __future__ import annotations as _annotations

import csv as _csv
import io as _io
import json as _json
import math as _math
from typing import Any as _Any


def _is_list_of_dicts(source: object) -> bool:
    return isinstance(source, list) and all(isinstance(row, dict) for row in source)


def _bytes_buffer(source: bytes | bytearray | memoryview | _io.BytesIO) -> _io.BytesIO:
    if isinstance(source, _io.BytesIO):
        return _io.BytesIO(source.getvalue())
    return _io.BytesIO(bytes(source))


class _NonStandardJsonConstant(ValueError):
    """A JavaScript numeric constant that RFC 8259 JSON does not permit."""

    def __init__(self, value: str) -> None:
        super().__init__(value)
        self.value = value


def _reject_nonstandard_json_constant(value: str) -> None:
    raise _NonStandardJsonConstant(value)


def _strict_json_loads(text: str) -> _Any:
    """Decode RFC 8259 JSON, rejecting Python's NaN/Infinity extensions."""
    return _json.loads(text, parse_constant=_reject_nonstandard_json_constant)


# --- Dependency-free columnar inputs (see _dataprof.profile_columns) ---
#
# dict, list-of-dicts, and decoded byte buffers all reduce to named columns of
# optional strings, which the Rust core types and profiles directly. Keeping
# these off pandas is what lets the base wheel honour its documented contract.

#: One column handed to the core: its name and its cells, `None` for null.
_Column = tuple[str, list[str | None]]


def _cell_to_str(value: object) -> str | None:
    """Render one Python cell as the string the core will type-infer.

    ``None`` and float NaN become nulls here; the core additionally treats
    null-like tokens (``""``, ``"null"``, ``"nan"``) as missing, so the result
    matches what the CSV and Arrow paths report for the same data.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and _math.isnan(value):
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list)):
        try:
            return _json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        except (TypeError, ValueError):
            # Contents aren't JSON-serialisable (datetime, set, circular ref, ...);
            # fall back to str() so one odd cell doesn't abort profiling.
            return str(value)
    return str(value)


def _columns_from_dict(source: dict[_Any, _Any]) -> list[_Column]:
    """Convert a dict of equal-length sequences into columns."""
    columns: list[_Column] = []
    normalized_names: set[str] = set()
    for key, values in source.items():
        name = str(key)
        if name in normalized_names:
            raise ValueError(
                f"dict input: column keys collide after string conversion at {name!r}. "
                "Use distinct string column names."
            )
        normalized_names.add(name)
        if isinstance(values, (str, bytes)) or not isinstance(values, (list, tuple)):
            raise TypeError(
                f"dict input: column {key!r} must be a list or tuple of cells, "
                f"got {type(values).__name__}. For a single row, pass [{{...}}]."
            )
        columns.append((name, [_cell_to_str(v) for v in values]))

    lengths = {len(cells) for _, cells in columns}
    if len(lengths) > 1:
        widths = ", ".join(f"{n}={len(c)}" for n, c in columns)
        raise ValueError(f"dict input: columns have differing lengths ({widths}).")
    return columns


def _columns_from_records(
    rows: list[dict[_Any, _Any]],
    max_rows: int | None = None,
) -> tuple[list[_Column], int]:
    """Convert a list of row dicts into columns, keyed in first-seen order.

    Returns the columns and the source row count. The row count is carried
    separately because records with no fields at all produce no columns to hold
    it: ``[{}, {}]`` is two rows against zero columns, the same shape the file
    scanner reports, not an empty input.

    Rows need not share keys; a row missing a key contributes a null there.

    ``max_rows`` caps *key discovery* at the first ``max_rows`` rows, so a column
    that only appears past the cap is never surfaced. Cells are materialised for
    one row beyond the cap: the Rust profiler ignores that extra row (it analyses
    only ``max_rows`` of them) but uses its presence to detect that the source was
    truncated, without us stringifying every dropped row.
    """
    key_rows = rows if max_rows is None else rows[:max_rows]
    cell_rows = rows if max_rows is None else rows[: max_rows + 1]
    keys = list(dict.fromkeys(key for row in key_rows for key in row))

    normalized_names = [str(key) for key in keys]
    if len(set(normalized_names)) != len(normalized_names):
        collisions = sorted(
            name for name in set(normalized_names) if normalized_names.count(name) > 1
        )
        raise ValueError(
            "list-of-dicts input: column keys collide after string conversion: "
            f"{collisions!r}. Use distinct string column names."
        )

    columns = [
        (name, [_cell_to_str(row.get(key)) for row in cell_rows])
        for key, name in zip(keys, normalized_names, strict=True)
    ]
    return columns, len(cell_rows)


def _columns_from_csv_bytes(buffer: _io.BytesIO, delimiter: str | None) -> list[_Column]:
    """Parse CSV bytes into columns, treating an empty field as null.

    This follows the file-based CSV engine, where an empty field is missing data
    rather than an empty string.
    """
    # Match the Rust CSV readers: a UTF-8 BOM marks the encoding and is not part
    # of the first column's name.
    text = buffer.getvalue().decode("utf-8-sig")
    if delimiter is None:
        try:
            delimiter = _csv.Sniffer().sniff(text[:8192], delimiters=",;\t|").delimiter
        except _csv.Error:
            delimiter = ","
    reader = _csv.reader(_io.StringIO(text, newline=""), delimiter=delimiter)
    try:
        # Rust's CSV readers ignore blank physical records, including those
        # before the header. A quoted empty field is still a real record.
        header = next(row for row in reader if row)
    except StopIteration:
        return []

    # Reject duplicate headers before profiling: name-keyed mapping access would
    # otherwise shadow one column and silently drop its profile. Single pass,
    # first-seen order, matching the Rust-side validate_unique_column_names.
    seen: set[str] = set()
    collisions: list[str] = []
    for name in header:
        if name in seen:
            if name not in collisions:
                collisions.append(name)
        else:
            seen.add(name)
    if collisions:
        raise ValueError(
            "csv bytes input: duplicate column name(s): "
            f"{collisions!r}. Column names must be unique."
        )

    cells: list[list[str | None]] = [[] for _ in header]
    for row in reader:
        if not row:
            continue
        if len(row) != len(header):
            raise ValueError(
                f"csv bytes: row {reader.line_num} has {len(row)} fields, "
                f"expected {len(header)}. Write the data to a file to use "
                f"the flexible CSV engine (csv_flexible=True)."
            )
        for i, field in enumerate(row):
            cells[i].append(field if field != "" else None)
    return [(str(name), cells[i]) for i, name in enumerate(header)]


def _json_kind(value: object) -> str:
    """Name a decoded JSON value's type the way the Rust scanners do."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    return "object"


def _scan_jsonl_records(text: str, on_error: str) -> tuple[list[dict], int]:
    """Scan JSONL text into row objects, honoring the malformed-record policy.

    Mirrors the file/async JSONL scanners: blank lines are ignored, and only
    object records become rows. A record that is either malformed or valid JSON
    that is not an object is skipped-and-counted ("skip") or raised on
    ("strict") — a non-object record is never silently discarded, because that
    would turn the source into a smaller clean-looking dataset. Error messages
    carry the 1-based line number, never the record contents. Input that yields
    no valid record but had skipped ones fails.
    """
    rows: list[dict] = []
    skipped = 0
    for lineno, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = _strict_json_loads(line)
        except _json.JSONDecodeError as exc:
            if on_error == "strict":
                # ``lineno`` is the record's position in the input; ``exc.colno``
                # locates the fault within that line. The record text is never
                # included.
                raise ValueError(
                    f"jsonl bytes: malformed JSON record on line {lineno}, column {exc.colno}."
                ) from None
            skipped += 1
            continue
        except _NonStandardJsonConstant as exc:
            if on_error == "strict":
                column = line.find(exc.value) + 1
                raise ValueError(
                    f"jsonl bytes: malformed JSON record on line {lineno}, column {column} "
                    f"(non-standard numeric constant {exc.value!r})."
                ) from None
            skipped += 1
            continue
        if isinstance(value, dict):
            rows.append(value)
            continue
        if on_error == "strict":
            raise ValueError(
                f"jsonl bytes: non-object JSON record on line {lineno}: expected an "
                f"object with fields to profile, found {_json_kind(value)}."
            )
        skipped += 1
    if not rows and skipped:
        raise ValueError(
            "jsonl bytes: no valid JSON records found "
            "(every record was malformed or not a JSON object)."
        )
    return rows, skipped


def _scan_json_array_records(values: list, on_error: str) -> tuple[list[dict], int]:
    """Apply the same record policy to the elements of a JSON array document.

    Only objects are rows; any other element is counted ("skip") or raised on
    ("strict") rather than dropped. Positions are 1-based over the elements.
    """
    rows: list[dict] = []
    skipped = 0
    for position, value in enumerate(values, start=1):
        if isinstance(value, dict):
            rows.append(value)
            continue
        if on_error == "strict":
            raise ValueError(
                f"json bytes: non-object JSON record at position {position}: expected an "
                f"object with fields to profile, found {_json_kind(value)}."
            )
        skipped += 1
    if not rows and skipped:
        raise ValueError(
            "json bytes: no valid JSON records found "
            "(every record was malformed or not a JSON object)."
        )
    return rows, skipped
