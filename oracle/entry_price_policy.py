"""Canonical reasons for an oracle entry price that cannot be divided by.

``oracle_predictions.entry_price`` is the spot that was measured at publish
time, or NULL when nothing measured it (D-M32). Neither NULL nor a
non-positive value can produce a return: ``(actual - entry) / entry`` raises
TypeError on the first and ZeroDivisionError on the second.

So every reader that wants a return has to decide *before* it divides, and
say which case it hit. The cases are different findings and never share a
string:

* **NULL** — nobody took the measurement. Not scorable; not a 0% return.
* **0 or negative** — a measurement exists and it cannot serve as a basis. A
  percentage return off a zero basis is not a big number, it is undefined.

Nothing here repairs a row, back-fills a price or invents a return. The only
product is the reason, which the caller persists (``score_notes``) or returns
(``tracking_pnl_basis``) so the gap is visible instead of being a silent skip.
"""

from __future__ import annotations

import math
from typing import Any

# ── score_notes (persisted by the scorers) ─────────────────────────────────
SCORE_NOTE_ENTRY_NULL = "No entry price was measured at publish time"
SCORE_NOTE_ENTRY_ZERO = "Entry price is 0, return not computable"
SCORE_NOTE_ENTRY_NEGATIVE = "Entry price is negative, return not computable"

# ── tracking_pnl_basis (returned by /api/v1/oracle/predictions) ────────────
PNL_BASIS_ENTRY_NULL = "unavailable: no entry price was measured at publish time"
PNL_BASIS_ENTRY_ZERO = "unavailable: entry price is 0, return not computable"
PNL_BASIS_ENTRY_NEGATIVE = "unavailable: entry price is negative, return not computable"
PNL_BASIS_NO_SPOT = "unavailable: no spot price has been observed for this ticker"
PNL_BASIS_SPOT_LOOKUP_FAILED = "unavailable: spot price lookup failed"
PNL_BASIS_MEASURED = "measured: (spot - entry_price) / entry_price"

_NULL_PAIR = (SCORE_NOTE_ENTRY_NULL, PNL_BASIS_ENTRY_NULL)
_ZERO_PAIR = (SCORE_NOTE_ENTRY_ZERO, PNL_BASIS_ENTRY_ZERO)
_NEGATIVE_PAIR = (SCORE_NOTE_ENTRY_NEGATIVE, PNL_BASIS_ENTRY_NEGATIVE)


def _classify(entry_price: Any) -> tuple[str, str] | None:
    """Return the (score note, pnl basis) pair, or None if the entry divides.

    A value that is not a number at all is treated as unmeasured rather than
    coerced: ``float("")`` raising is not evidence that the entry was zero.
    """
    if entry_price is None:
        return _NULL_PAIR
    try:
        value = float(entry_price)
    except (TypeError, ValueError):
        return _NULL_PAIR
    if math.isnan(value) or math.isinf(value):
        # A measurement that says nothing, and an infinity is not a price.
        return _NULL_PAIR
    if value == 0:
        return _ZERO_PAIR
    if value < 0:
        return _NEGATIVE_PAIR
    return None


def entry_price_score_note(entry_price: Any) -> str | None:
    """The ``score_notes`` reason this entry is unscorable, else None.

    ``None`` means the entry price is a positive number and the caller may
    divide by it.
    """
    pair = _classify(entry_price)
    return None if pair is None else pair[0]


def entry_price_pnl_basis(entry_price: Any) -> str | None:
    """The ``pnl_basis`` reason no return is computable, else None."""
    pair = _classify(entry_price)
    return None if pair is None else pair[1]


def is_divisible_entry_price(entry_price: Any) -> bool:
    """True only for a positive, finite, numeric entry price."""
    return _classify(entry_price) is None
