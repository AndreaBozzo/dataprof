"""Dependency-free input decoding into named columns."""

from __future__ import annotations as _annotations

import csv as _csv
import decimal as _decimal
import io as _io
import json as _json
import math as _math
import re as _re
from typing import Any as _Any

from ._dataprof import _detect_csv_delimiter


def _is_list_of_dicts(source: object) -> bool:
    return isinstance(source, list) and all(isinstance(row, dict) for row in source)


def _bytes_buffer(source: bytes | bytearray | memoryview | _io.BytesIO) -> _io.BytesIO:
    if isinstance(source, _io.BytesIO):
        return _io.BytesIO(source.getvalue())
    return _io.BytesIO(bytes(source))


class _NonStandardJsonConstant(ValueError):
    """A JSON token the readers refuse: a JavaScript numeric constant that RFC
    8259 does not permit, an integer outside the 64-bit range, a number that
    overflows a float, or a string escape that encodes a lone surrogate.

    ``description`` names the problem for an error message.
    """

    def __init__(self, value: str, description: str | None = None) -> None:
        super().__init__(value)
        self.value = value
        self.description = description or f"non-standard numeric constant {value!r}"


def _reject_nonstandard_json_constant(value: str) -> None:
    raise _NonStandardJsonConstant(value)


_INT64_MIN = -(2**63)
_UINT64_MAX = 2**64 - 1


def _parse_json_int(token: str) -> int:
    # The file and async readers hold an integer outside the 64-bit range as
    # the nearest float, so they refuse it rather than merge distinct values;
    # bytes refuse it too, so every route reads the same input alike (#1103).
    # The value stays out of the message, as with every record error.
    # A token over 20 digits is out of range whatever its value, and is refused
    # before int(), which raises its own ValueError past Python's digit limit.
    value = int(token) if len(token.lstrip("-")) <= 20 else None
    if value is None or not _INT64_MIN <= value <= _UINT64_MAX:
        raise _NonStandardJsonConstant(
            token,
            "an integer outside the 64-bit range; write such integers as strings to profile them",
        )
    return value


class _FloatLiteral(float):
    """A JSON float whose literal names a value the float does not hold.

    ``1000000000000000.01`` reads as ``1000000000000000.0``, so distinct
    literals became one value. A cell profiles as the literal instead, as the
    file and async readers do for a field (#1156). Inside a container it stays
    the float: ``json.dumps`` writes a float subclass as the float.
    """

    literal: str


def _parse_json_float(token: str) -> float:
    # json.loads turns a finite token past the float range, like 1e400, into
    # infinity, which then profiles as one invalid value with no parse error.
    # The file and async readers refuse it as out of range (#1086).
    value = float(token)
    if _math.isinf(value):
        raise _NonStandardJsonConstant(token, "a number outside the 64-bit float range")
    # repr writes the shortest digits that read back as the same float.
    if token != repr(value) and _decimal.Decimal(token) != _decimal.Decimal(repr(value)):
        lossy = _FloatLiteral(value)
        lossy.literal = token
        return lossy
    return value


#: A ``\uXXXX`` escape in the surrogate range, paired or not.
_SURROGATE_ESCAPE = _re.compile(r"\\u[dD][89a-fA-F][0-9a-fA-F]{2}")
_SURROGATE = _re.compile("[\ud800-\udfff]")


def _reject_lone_surrogates(text: str, value: _Any) -> None:
    # json.loads joins an escaped pair into one character but keeps a lone
    # surrogate escape as a code point no UTF-8 string can hold, so the record
    # crashed later with UnicodeEncodeError. The file and async readers refuse
    # it while decoding (#1142). Decoded UTF-8 holds no surrogate of its own,
    # so one found here came from an escape, in a key or in a string value.
    if not _SURROGATE_ESCAPE.search(text):
        return
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, str):
            found = _SURROGATE.search(item)
            if found:
                # The code point came from an escape, so the text spells it.
                escape = _re.compile(rf"\\u{ord(found.group()):04x}", _re.IGNORECASE)
                spelling = escape.search(text)
                assert spelling is not None
                raise _NonStandardJsonConstant(
                    spelling.group(), "a lone surrogate escape, which is not valid Unicode"
                )
        elif isinstance(item, dict):
            stack.extend(item)
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)


def _strict_json_loads(text: str) -> _Any:
    """Decode RFC 8259 JSON, rejecting Python's NaN/Infinity extensions,
    integers outside the 64-bit range, numbers that overflow a float and lone
    surrogate escapes."""
    value = _json.loads(
        text,
        parse_constant=_reject_nonstandard_json_constant,
        parse_int=_parse_json_int,
        parse_float=_parse_json_float,
    )
    _reject_lone_surrogates(text, value)
    return value


# --- Dependency-free columnar inputs (see _dataprof.profile_columns) ---
#
# dict, list-of-dicts, and decoded byte buffers all reduce to named columns of
# optional strings, which the Rust core types and profiles directly. Keeping
# these off pandas is what lets the base wheel honour its documented contract.

#: One column handed to the core: its name, its cells (`None` for null), and
#: the ascending row indices of the cells that were a dict or a list. A
#: container crosses as JSON text; the indices are what tell the core it was
#: not a string, so a column of containers profiles as ``nested`` (#637).
_Column = tuple[str, list[str | None], list[int]]


def _is_container(value: object) -> bool:
    """Whether a cell is one `_cell_to_str` renders as a container."""
    return isinstance(value, (dict, list))


def _column(name: str, values: _Any) -> _Column:
    """Render one column's cells and record which of them were containers."""
    values = list(values)
    containers = [row for row, value in enumerate(values) if _is_container(value)]
    return (name, [_cell_to_str(v) for v in values], containers)


def _cell_to_str(value: object) -> str | None:
    """Render one Python cell as the string the core will type-infer.

    ``None`` and float NaN become nulls here; the core additionally treats
    null-like tokens (``""``, ``"null"``, ``"nan"``, ``"NA"``, ``"None"``, ...) as
    missing, so the result matches what the CSV and Arrow paths report for the
    same data.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and _math.isnan(value):
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, _FloatLiteral):
        return value.literal
    if _is_container(value):
        try:
            return _json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        except (TypeError, ValueError):
            # Contents aren't JSON-serialisable (datetime, set, circular ref, ...);
            # fall back to str() so one odd cell doesn't abort profiling.
            return str(value)
    return str(value)


def _pandas_to_pyarrow_table(df: _Any, pyarrow: _Any, *, container_markers: bool = False) -> _Any:
    """Convert a pandas DataFrame to a pyarrow Table, handling mixed object columns.

    Fast path: delegates directly to ``pyarrow.Table.from_pandas(df)``.
    Fallback path: for object and categorical columns whose values Arrow cannot
    cast into a single type (e.g. numeric with placeholder text or mixed
    booleans/dates), converts their cells via ``_cell_to_str`` into string arrays
    so they can be profiled with ``type_homogeneity`` rather than aborting (#976).

    When requested, return the table and container flags collected during that
    same fallback conversion. Flags use original column positions and cover all
    source rows; native analysis applies its row cap and column projection.
    """
    try:
        table = pyarrow.Table.from_pandas(df)
        return (table, {}) if container_markers else table
    except (pyarrow.ArrowInvalid, pyarrow.ArrowTypeError):
        # If columns have duplicate labels, pyarrow Table.from_pandas fails with
        # ValueError; duplicate column frames cannot be converted.
        if hasattr(df.columns, "is_unique") and not df.columns.is_unique:
            raise

        failing_cols: dict[int, _Any] = {}
        for i in range(len(df.columns)):
            col = df.iloc[:, i]
            # CategoricalDtype is an extension dtype whose name is "category".
            # It can also hold mixed Python values that Arrow cannot cast.
            if col.dtype != object and getattr(col.dtype, "name", None) != "category":
                continue
            name = df.columns[i]
            try:
                pyarrow.Array.from_pandas(col)
            except (pyarrow.ArrowInvalid, pyarrow.ArrowTypeError):
                failing_cols[i] = name

        if not failing_cols:
            raise

        df_fixed = df.copy(deep=False)
        markers: dict[int, list[bool]] = {}
        for index, name in failing_cols.items():
            col = df[name]
            try:
                cells = zip(col.isna(), col) if hasattr(col, "isna") else ((False, v) for v in col)
                rendered = []
                flags = []
                for is_missing, value in cells:
                    rendered.append(None if is_missing else _cell_to_str(value))
                    if container_markers:
                        flags.append(not is_missing and _is_container(value))
                df_fixed[name] = rendered
                if any(flags):
                    markers[index] = flags
            except Exception as exc:
                raise ValueError(
                    f"pandas DataFrame: column {name!r} could not be converted for profiling: {exc}"
                ) from exc

        try:
            table = pyarrow.Table.from_pandas(df_fixed)
            return (table, markers) if container_markers else table
        except Exception as exc:
            for name in failing_cols.values():
                try:
                    pyarrow.Array.from_pandas(df_fixed[name])
                except (pyarrow.ArrowInvalid, pyarrow.ArrowTypeError) as col_exc:
                    raise ValueError(
                        f"pandas DataFrame: column {name!r} could not be converted for profiling: "
                        f"{col_exc}"
                    ) from col_exc
            raise ValueError(f"pandas DataFrame conversion failed: {exc}") from exc


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
        columns.append(_column(name, values))

    lengths = {len(cells) for _, cells, _ in columns}
    if len(lengths) > 1:
        widths = ", ".join(f"{n}={len(c)}" for n, c, _ in columns)
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
        _column(name, (row.get(key) for row in cell_rows))
        for key, name in zip(keys, normalized_names, strict=True)
    ]
    return columns, len(cell_rows)


def _unclosed_quote_at(text: str, delimiter: str, quote: str = '"') -> int | None:
    """Offset of a quote that opens a field and is never closed, if any.

    ``csv.reader`` outside strict mode, like the Rust CSV readers, reads a quote
    that is never closed as a field running to the end of the input, swallowing
    every following row (#782). The rules match theirs: a quote opens a field
    only at the start of one, and a doubled quote inside a field is a literal.
    Only quote characters are visited.
    """
    opened: int | None = None
    index = text.find(quote)
    while index >= 0:
        if opened is not None:
            if text.startswith(quote, index + 1):
                index = text.find(quote, index + 2)
                continue
            opened = None
        elif index == 0 or text[index - 1] in (delimiter, "\r", "\n"):
            opened = index
        index = text.find(quote, index + 1)
    return opened


def _columns_from_csv_bytes(buffer: _io.BytesIO, delimiter: str | None) -> list[_Column]:
    """Parse CSV bytes into columns, treating an empty field as null.

    This follows the file-based CSV engine, where an empty field is missing data
    rather than an empty string.
    """
    # Match the Rust CSV readers: a UTF-8 BOM marks the encoding and is not part
    # of the first column's name.
    raw = buffer.getvalue()
    text = raw.decode("utf-8-sig")
    if delimiter is None:
        # The detector every file and async route uses, so the same bytes read
        # the same way. csv.Sniffer silently became comma on a sample it could
        # not resolve, which hid a column and passed a gate (#1095).
        delimiter = _detect_csv_delimiter(raw)
    # This reader has no flexible mode to record the problem in, so it refuses,
    # as it refuses a ragged row.
    unclosed = _unclosed_quote_at(text, delimiter)
    if unclosed is not None:
        before = text[:unclosed]
        line = before.count("\n") + before.count("\r") - before.count("\r\n") + 1
        raise ValueError(
            f"csv bytes: the quote opened on line {line} is never closed, so every "
            f"row after it would be read into one field. Close the quote, or write "
            f"the data to a file to use the flexible CSV engine (csv_flexible=True), "
            f"which profiles it and sets unterminated_quote."
        )
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
    return [(str(name), cells[i], []) for i, name in enumerate(header)]


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


#: What the file and async JSONL readers trim from a line: Rust's ``str::trim``,
#: the Unicode White_Space set. ``str.strip()`` would also remove U+001C to
#: U+001F, which they keep, and so read a line of them as blank.
_RECORD_WHITESPACE = "".join(
    map(
        chr,
        (
            0x09,
            0x0A,
            0x0B,
            0x0C,
            0x0D,
            0x20,
            0x85,
            0xA0,
            0x1680,
            *range(0x2000, 0x200B),
            0x2028,
            0x2029,
            0x202F,
            0x205F,
            0x3000,
        ),
    )
)


def _scan_jsonl_records(text: str, on_error: str) -> tuple[list[dict], int]:
    """Scan JSONL text into row objects, honoring the malformed-record policy.

    Mirrors the file/async JSONL scanners: a record is a line ended by ``\\n``
    and nothing else, trimmed of Unicode whitespace as they trim it, blank lines
    are ignored, and only object records become rows. ``str.splitlines()``
    would also break at U+2028, U+0085 and other characters a JSON string may
    hold, cutting one record in two (#1087). A record that is either malformed
    or valid JSON that is not an object is skipped-and-counted ("skip") or
    raised on ("strict") — a non-object record is never silently discarded,
    because that would turn the source into a smaller clean-looking dataset.
    Error messages carry the 1-based line number, never the record contents.
    Input that yields no valid record but had skipped ones fails.
    """
    rows: list[dict] = []
    skipped = 0
    for lineno, line in enumerate(text.split("\n"), start=1):
        record = line.strip(_RECORD_WHITESPACE)
        if not record:
            continue
        try:
            value = _strict_json_loads(record)
        except _json.JSONDecodeError as exc:
            if on_error == "strict":
                # ``lineno`` is the record's position in the input; ``exc.colno``
                # locates the fault within the trimmed record, so the trimmed
                # prefix is added back. The record text is never included.
                column = exc.colno + len(line) - len(line.lstrip(_RECORD_WHITESPACE))
                raise ValueError(
                    f"jsonl bytes: malformed JSON record on line {lineno}, column {column}."
                ) from None
            skipped += 1
            continue
        except _NonStandardJsonConstant as exc:
            if on_error == "strict":
                column = line.find(exc.value) + 1
                raise ValueError(
                    f"jsonl bytes: malformed JSON record on line {lineno}, column {column} "
                    f"({exc.description})."
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
            "(every record was malformed, not a JSON object, or held an integer "
            "outside the 64-bit range)."
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
            "(every record was malformed, not a JSON object, or held an integer "
            "outside the 64-bit range)."
        )
    return rows, skipped
