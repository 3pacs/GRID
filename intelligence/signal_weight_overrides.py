"""Per-signal conviction multipliers derived from the auto-improve corpus.

The 89K trade_postmortems table tells us empirically which signal sources
were on the right side of failed predictions (their warnings were
correct but ignored) and which were on the wrong side (aligned with the
failed call). When a signal's right:wrong ratio is extreme — either very
high or very low across hundreds of postmortems — that's a calibration
signal the conviction stack should act on.

This module ships a hand-curated override table derived from the
2026-05-13 auto-improve advisory. Each entry maps a ``signal_source``
to a conviction multiplier:

  * **vix_exposure** (1826 right / 0 wrong): boost to 1.40×. Empirically
    perfect over the corpus. Conservative — capped well below 2× so a
    single signal can't dominate.
  * **credit_cycle** (0 right / 1826 wrong): cut to 0.20×. Perfect
    mirror of vix_exposure. Very likely sign-inverted at the source;
    until that's fixed, mute it. Don't fully zero — leave room for the
    signal to recover if the bug is corrected.
  * **feature:equity** (4.67× ratio): 1.20× boost.
  * **feature:sentiment** (3× ratio): 1.15× boost.
  * **feature:rates** (2× ratio): 1.10× boost.
  * **feature:vol** (0 right / 348 wrong): 0.30× cut.
  * **news_intel** (0.5 ratio): 0.60× cut.

Excluded from overrides:
  * Per-ticker features (aapl_full, avgo_full, etc.) — their ratios are
    a tautology of postmortems-only-on-failures.
  * actor:qq_* (gov_contracts, insider_trading, off_exchange) — these
    are real-world events whose appearance in signals_wrong reflects
    oracle ignoring them, not the signal being bad. Already handled by
    edge_signals + anti-signal veto.
  * Sanity-pass signals (sanity_DATA_QUALITY_passed, etc.) — boolean
    gates, not predictive signals.

Apply via ``signal_provenance.compute_aggregate_conviction``. Master
switch ``GRID_SIGNAL_OVERRIDES_ENABLED`` (default ON since these are
data-derived and conservative).
"""

from __future__ import annotations

import os
from typing import Any

from loguru import logger as log


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "")
    if not raw.strip():
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on", "enabled")


# Master switch. Default ON because the overrides are conservatively
# capped and derived from observed corpus data, not heuristics. Flip
# to false via env to A/B-test against the prior calibration.
SIGNAL_OVERRIDES_ENABLED: bool = _env_bool("GRID_SIGNAL_OVERRIDES_ENABLED", True)


# (signal_source name → multiplier on the conviction contribution)
#
# IMPORTANT — namespace match: the keys MUST match the
# ``signal_evidence.signal_source`` strings that
# ``compute_aggregate_conviction`` iterates. That's the bare-key form
# from ``signals.signal_contributions`` (after the layer-1 nested
# fix #123) — i.e. ``equity`` / ``vol`` / ``sentiment``, NOT
# ``feature:equity`` / ``feature:vol``.
#
# The trade_postmortems records signals_wrong/right under the
# **prefixed** name (``feature:vol``, ``alpha_research:vix_exposure``).
# So when reading the auto-improve advisory you'll see the long form;
# strip the prefix to get the override key.
#
# Signals not reachable from this layer (e.g. ``alpha_research:*``,
# which live in ``signals.items[]`` but don't appear in
# ``signal_contributions``) cannot be overridden here. They need a
# different wiring point (oracle/engine.py at signal-roll-up time).
# Track those in the auto_improve advisory; don't lie about them in
# this table.
#
# Conservative ranges: boosts capped at 1.40×, cuts floor at 0.20×.
SIGNAL_WEIGHT_OVERRIDES: dict[str, float] = {
    # STARS — appear in signal_evidence as bare asset-family names.
    # Right:wrong ratios from 2026-05-13 advisory:
    #   equity     757r / 162w  (4.67×)
    #   sentiment  552r / 184w  (3.00×)
    #   rates      306r / 153w  (2.00×)
    #   commodity   87r /   0w  (∞)
    "equity": 1.20,
    "sentiment": 1.15,
    "rates": 1.10,
    "commodity": 1.10,

    # BAD — bare names. Right:wrong ratios:
    #   vol  0r / 348w  (0)
    #   fx   0r /  87w  (0)
    "vol": 0.30,
    "fx": 0.40,
}


# Signals that DO appear in trade_postmortems but NOT in
# signal_evidence at this layer — listed here so the next wiring pass
# (at oracle/engine.py signal roll-up) can pick them up. Documented in
# code so it doesn't get lost.
DEFERRED_SIGNAL_OVERRIDES: dict[str, float] = {
    "alpha_research:vix_exposure": 1.40,
    "alpha_research:credit_cycle": 0.20,
    "news_intel": 0.60,
}


# Hard bounds — a future automated tuner can't push any single
# override outside this range without explicit operator action.
SIGNAL_OVERRIDE_MIN: float = 0.20
SIGNAL_OVERRIDE_MAX: float = 1.40


def get_override(signal_source: Any) -> float:
    """Return the multiplier for ``signal_source`` (1.0 = no effect).

    Returns 1.0 when the master switch is off, the signal isn't in the
    override table, or the input isn't a usable string.
    """
    if not SIGNAL_OVERRIDES_ENABLED:
        return 1.0
    if not isinstance(signal_source, str) or not signal_source.strip():
        return 1.0
    m = SIGNAL_WEIGHT_OVERRIDES.get(signal_source.strip())
    if m is None:
        return 1.0
    return max(SIGNAL_OVERRIDE_MIN, min(SIGNAL_OVERRIDE_MAX, float(m)))


def set_enabled(value: bool) -> None:
    """Runtime toggle (mostly for tests / REPL)."""
    global SIGNAL_OVERRIDES_ENABLED
    SIGNAL_OVERRIDES_ENABLED = bool(value)
