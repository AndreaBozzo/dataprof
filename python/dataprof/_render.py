"""Formatting, quality flags, redaction, and bounded summary helpers."""

from __future__ import annotations as _annotations

from ._columns import _dominant_pattern, _type_mixture
from ._dataprof import ColumnProfile
from ._rounding import _r2, _r4


def _pct_str(value: float | None) -> str:
    """Format a percentage for display, or an em dash when absent.

    A percentage that was never measured (``None`` — e.g. a zero-row column)
    renders as ``—`` rather than ``0.0%``, which would falsely read as a
    measured value.
    """
    r = _r2(value)
    return f"{r:.1f}%" if r is not None else "—"


def _stats_cell(col: ColumnProfile) -> str:
    """Compact stats summary for a column, shared by the HTML/markdown/text views.

    Numeric columns show mean/std/median; boolean columns show true count and
    ratio; text columns show average length. Returns "" when none apply.
    """
    parts: list[str] = []
    if col.mean is not None:
        parts.append(f"mean={_r4(col.mean)}")
        if col.std_dev is not None:
            parts.append(f"std={_r4(col.std_dev)}")
        if col.median is not None:
            parts.append(f"median={_r4(col.median)}")
    elif col.true_count is not None:
        pct = _r2(col.true_ratio * 100) if col.true_ratio is not None else 0
        parts.append(f"true={col.true_count} ({pct:.0f}%)")
    elif col.avg_length is not None:
        parts.append(f"avg_len={_r4(col.avg_length)}")
    return ", ".join(parts)


def _pattern_cell(col: ColumnProfile) -> str:
    """Highest-confidence reportable pattern, formatted as "Name (pct%)"."""
    best = _dominant_pattern(col)
    if best is not None:
        return f"{best.name} ({_r2(best.match_percentage):.0f}%)"
    return ""


# --- LLM context helpers (see ProfileReport.to_llm_context) ---

#: Characters per token. A deliberately rough, dependency-free approximation --
#: real tokenizers vary by model. It runs slightly conservative for prose, which
#: is the safe direction for a budget that must not be exceeded.
_CHARS_PER_TOKEN = 4

#: A column is flagged ``null-heavy`` at or above this null percentage.
_NULL_HEAVY_PCT = 20.0

#: A column is flagged ``mixed types`` once at least this percentage of its
#: classified values falls outside its dominant lexical class. Display only: it
#: changes no score, and exists so that one stray ``N/A`` in a clean numeric
#: column does not spend budget competing with the findings an agent asked for.
_MIXED_TYPE_PCT = 5.0

#: Pattern categories whose concrete values should stay out of agent-facing
#: output even when ``include_samples=True`` asks for numeric extrema.
_SENSITIVE_PATTERN_CATEGORIES = {
    "contact",
    "identifier",
    "financial",
    "geographic",
    "network",
    "file_path",
}


def _estimate_tokens(text: str) -> int:
    """Estimate the token count of ``text`` as ``ceil(len(text) / 4)``.

    Deterministic and dependency-free. See :data:`_CHARS_PER_TOKEN`.
    """
    return -(-len(text) // _CHARS_PER_TOKEN)


_ESCAPES = {"\n": "\\n", "\r": "\\r", "\t": "\\t"}

#: Cap for free-text carried in a report and echoed into the agent header.
#: The header is emitted outside the ``max_tokens`` budget, so an unbounded
#: string from a loaded document would inflate the prompt without limit.
_MAX_BORROWED_TEXT = 200


def _bounded(text: str, limit: int = _MAX_BORROWED_TEXT) -> str:
    """Truncate borrowed free text, saying so rather than trailing off."""
    if len(text) <= limit:
        return text
    return f"{text[:limit]}... (+{len(text) - limit} chars)"


def _one_line(value: object) -> str:
    """Render ``value`` so it cannot break the line-oriented LLM context.

    Column names and cell values come from the data, so a newline in a CSV
    header would otherwise split one schema entry across two lines -- corrupting
    the format and letting the source inject arbitrary text into an
    agent-facing summary.
    """
    text = str(value)
    return "".join(
        _ESCAPES[ch] if ch in _ESCAPES else (ch if ch.isprintable() else f"\\x{ord(ch):02x}")
        for ch in text
    )


def _may_expose_values(col: ColumnProfile) -> bool:
    """Return True only when ``col``'s raw values are *provably* safe to echo.

    Safety here is a positive claim, not the absence of a negative one. A column
    qualifies only if pattern detection actually ran (``patterns is not None``)
    and matched nothing sensitive. When detection was skipped -- ``fast_mode``,
    or a ``metrics=`` selection without the ``"patterns"`` pack -- we cannot
    tell a credit-card column from a quantity column, so we fail closed.

    Deserialized reports whose ``patterns`` key was absent also arrive as
    ``None`` and are likewise treated as unknown.
    """
    if col.patterns is None:
        return False
    return not any(
        (getattr(pattern, "category", "") or "").lower() in _SENSITIVE_PATTERN_CATEGORIES
        for pattern in col.patterns
    )


def _share_pct(share: float) -> str:
    """Render a ``0..1`` share as a percentage that never rounds down to zero.

    A class holding one value in ten thousand is 0.01%, and printing that as
    "0% date" beside a real mixture states something untrue about the data.
    """
    pct = round(share * 100, 1)
    if pct == 0.0 and share > 0.0:
        return "<0.1"
    return f"{pct:g}"


def _mixed_type_flag(col: ColumnProfile) -> tuple[float, str] | None:
    """Flag a column whose values do not agree on one lexical class.

    This is the signal ``data_type`` cannot carry: below the inference
    thresholds a half-numeric column is typed ``string``, identical in the
    schema listing to a column of names. The flag states the mixture actually
    found rather than restating the type.

    Suppressed for ``identifier`` columns, where mixing forms ("A1", "123") is
    what an ID scheme does rather than a defect -- the same exemption the
    consistency dimension makes.
    """
    if col.data_type == "identifier":
        return None
    mixture = _type_mixture(col)
    if len(mixture) < 2:
        return None
    outside_pct = 100.0 * (1.0 - mixture[0][2])
    if outside_pct < _MIXED_TYPE_PCT:
        return None

    name = _one_line(col.name)
    shares = ", ".join(f"{_share_pct(share)}% {cls}" for cls, _, share in mixture)

    # The counts cover what the profiler retained, which on a large source is a
    # bounded sample of the column. Disclose that where it is true, rather than
    # letting a share taken over 10k values read as a fact about 10M.
    classified = sum(count for _, count, _ in mixture)
    non_null = (col.total_count or 0) - (col.null_count or 0)
    scope = f"; sampled {classified:,} of {non_null:,} values" if classified < non_null else ""

    return (outside_pct, f"{name}: mixed types ({shares}{scope})")


def _column_flags(col: ColumnProfile) -> list[tuple[float, str]]:
    """Derive ``(severity, text)`` quality flags for one column.

    Higher severity sorts first. Only high-signal, deterministic flags are
    emitted -- a flag that fires on almost every column is noise, not signal.
    """
    flags: list[tuple[float, str]] = []
    rounded_null_pct = _r2(col.null_percentage)
    null_pct_for_flags = rounded_null_pct if rounded_null_pct is not None else 0.0
    total = col.total_count or 0
    name = _one_line(col.name)

    # Do not infer "all-null" from the rounded percentage: a non-null value
    # can legitimately round 99.996% up to 100.0%. Counts preserve the exact
    # structural distinction, while the threshold and rendered percentage use
    # the same serialized precision so native and round-tripped reports agree.
    is_all_null = total > 0 and col.null_count is not None and col.null_count == total
    if is_all_null:
        flags.append((100.0, f"{name}: all-null"))
    elif rounded_null_pct is not None and rounded_null_pct >= _NULL_HEAVY_PCT:
        flags.append((rounded_null_pct, f"{name}: null-heavy ({rounded_null_pct:.1f}% null)"))

    # A single distinct value carries no information. Suppress when the column
    # is already reported as null-heavy, where it is a restatement, not a flag.
    if col.unique_count == 1 and null_pct_for_flags < _NULL_HEAVY_PCT and total > 1:
        flags.append((60.0, f"{name}: constant (1 distinct value)"))

    outliers = col.outlier_count or 0
    if outliers > 0 and total > 0:
        pct = 100.0 * outliers / total
        flags.append((min(50.0, pct), f"{name}: {outliers} outliers ({pct:.1f}%)"))

    mixed = _mixed_type_flag(col)
    if mixed is not None:
        flags.append(mixed)

    return flags


def _section_min_cost(header: str, items: list[str]) -> int:
    """Tokens needed to show ``header`` plus at least one item (and a tail).

    A section is worth nothing below this cost, so priority allocation reserves
    it before handing budget to lower-priority sections.
    """
    if not items:
        return 0
    cost = _estimate_tokens(header) + _estimate_tokens(items[0])
    if len(items) > 1:
        cost += _estimate_tokens(f"... +{len(items) - 1} more")
    return cost


def _fit_section(header: str, items: list[str], budget: int) -> tuple[list[str], int]:
    """Fit as many ``items`` as ``budget`` tokens allow under ``header``.

    Returns ``(lines, tokens_used)``. Omitted items are summarized by a trailing
    ``... +N more``, whose own cost is reserved before any item is admitted, so
    the result never exceeds ``budget``. Emits nothing if the header alone
    cannot fit.
    """
    if not items:
        return [], 0

    header_cost = _estimate_tokens(header)
    if header_cost > budget:
        return [], 0

    lines = [header]
    used = header_cost
    admitted = 0

    for i, item in enumerate(items):
        remaining = len(items) - i
        item_cost = _estimate_tokens(item)
        # Reserve room for the tail we would need if this item does not fit.
        tail_cost = _estimate_tokens(f"... +{remaining} more") if remaining else 0

        if used + item_cost + (tail_cost if remaining > 1 else 0) <= budget:
            lines.append(item)
            used += item_cost
            admitted += 1
        else:
            tail = f"... +{remaining} more"
            if used + _estimate_tokens(tail) <= budget:
                lines.append(tail)
                used += _estimate_tokens(tail)
            break

    # A section header with no item beneath it spends tokens to say nothing --
    # its "+N more" tail only restates the count already in the header.
    if admitted == 0:
        return [], 0
    return lines, used
