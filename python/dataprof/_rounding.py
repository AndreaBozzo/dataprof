"""Numeric rounding shared by Python report surfaces."""

from __future__ import annotations as _annotations

import decimal as _decimal
import math as _math
from typing import Any as _Any

# ---------------------------------------------------------------------------
# Rounding helpers — match the convention in dataprof-core serde helpers.
# ---------------------------------------------------------------------------


def _half_up(v: float, ndigits: int) -> float:
    """Round ``v`` at ``ndigits``, ties away from zero, as the Rust layer does.

    This mirrors ``(v * 10^n).round() / 10^n`` from
    ``crates/dataprof-core/src/serde_helpers.rs`` step for step: scale in binary,
    round the scaled value half away from zero (which is what ``f64::round()``
    does), then scale back.

    It rounds the *stored* float rather than the shortest decimal string that
    prints for it, and that distinction is the whole point (#513). Rounding
    ``Decimal(str(v))`` instead would take ``23 / 4000 * 100`` — which prints as
    ``0.575`` but is stored just below it — up to ``0.58``, where the Rust
    serializer emits ``0.57``. Two layers, one number: the layers must agree,
    and the engine's answer is the one that wins.
    """
    scale = 10.0**ndigits
    with _decimal.localcontext() as ctx:
        ctx.rounding = _decimal.ROUND_HALF_UP
        try:
            # Decimal(float) is the exact binary value, so quantizing it to an
            # integer under ROUND_HALF_UP reproduces f64::round() exactly.
            rounded = float(_decimal.Decimal(v * scale).quantize(_decimal.Decimal(1)))
        except _decimal.InvalidOperation:
            # Very large or very small numbers (e.g. variance ~1e+29) exceed the
            # decimal context's precision — return as-is, since a value that big
            # is already integral at this scale and rounding cannot move it.
            return v
    return rounded / scale


def _r2(v: float | None) -> float | None:
    """Round to 2 decimal places (0..100 percentages). None/NaN → None.

    Ratios on a 0..1 scale use :func:`_r4` instead, so that both carry the same
    resolution — 2dp on a ratio is a hundredth of 2dp on a percentage.
    """
    if v is None or not _math.isfinite(v):
        return None
    return _half_up(v, 2)


def _r4(v: float | None) -> float | None:
    """Round to 4 decimal places (statistics, 0..1 ratios). None/NaN → None."""
    if v is None or not _math.isfinite(v):
        return None
    return _half_up(v, 4)


def _round_quartiles(q: dict[str, float] | None) -> dict[str, float] | None:
    """Round quartile values to 2 decimal places."""
    if q is None:
        return None
    return {k: _half_up(v, 2) for k, v in q.items()}


def _round_dimension(values: dict[str, _Any]) -> dict[str, _Any]:
    """Round a quality dimension dict the way the Rust serializer does.

    Every float across the seven dimension structs is a ``0..100`` percentage
    carrying ``round_2`` on the Rust side, so rounding floats by type matches it
    field for field without a per-field list to keep in sync. Counts, flags and
    column names pass through untouched.
    """
    return {k: _r2(v) if isinstance(v, float) else v for k, v in values.items()}
