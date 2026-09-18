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

Apply via ``signal_provenance.compute_aggregate_conviction`` (and,
for the DEFERRED_SIGNAL_OVERRIDES set, via ``get_effective_overrides()``
at ``oracle/engine.py``'s signal-roll-up step).

SAFEGUARDS (2026-09-18, GRID W7)
---------------------------------
These overrides are derived from a broken signal meter (see the vault
audit, verified live 2026-09-17): the underlying trade_postmortems
right/wrong ratios come from a corpus with contaminated scoring
(oracle prediction scoring stopped 2026-05-16; a synthetic batch from
April 2026 dominates the scored predictions used to compute those
ratios). Tainted labels must not silently drive live weights.

This workstream (GRID W7b) deliberately separates that *safeguard
capability* from any *operational policy decision* about whether to
use it. This module, on its own, changes nothing about current
production behaviour:

1. ``GRID_SIGNAL_OVERRIDES_ENABLED`` keeps its pre-existing default of
   **True** — the master switch behaves exactly as it did before this
   workstream. A deployment that leaves the env var unset continues to
   apply the override table exactly as before.
2. A NEW, independent knob, ``GRID_SIGNAL_OVERRIDES_REQUIRE_LEDGER``,
   defaults to **False**. It controls whether a
   ``governance.promotion_ledger`` approval is *required* before an
   enabled override table is allowed to affect a live weight:

   * ``require_ledger=False`` (default): legacy behaviour exactly —
     overrides are applied whenever the master switch is on, with no
     ledger check. Because this leaves tainted-corpus-derived weights
     live with no auditable approval, this module logs **one**
     WARNING (at first use) naming ``GRID_SIGNAL_OVERRIDES_REQUIRE_LEDGER``
     and stating that overrides are being applied without a
     promotion-ledger approval.
   * ``require_ledger=True``: overrides are applied only if
     ``governance.promotion_ledger`` has an **approved** record for
     this exact override set: ``kind="weight_override"``, a
     ``subject_hash`` over ``{SIGNAL_WEIGHT_OVERRIDES ∪
     DEFERRED_SIGNAL_OVERRIDES, EVALUATION_VERSION}``, and the matching
     ``evaluation_version``. No ledger match => log once at WARNING
     and apply nothing (multiplier 1.0 / empty table).

``config.py`` does not define either env var as a pydantic-settings
field (both are read directly from ``os.environ`` here, as before)
and is out of scope for this change — **the deploy owner must check
production's effective value of both env vars directly** (`.env`,
systemd unit environment, etc.) to know which path is active. Actually
flipping to the safer defaults (``SIGNAL_OVERRIDES_ENABLED=False``,
``REQUIRE_LEDGER=True``) is a held operational policy change tracked
separately — see ``docs/reference/LEARNING_PROMOTION_PROTOCOL.md``'s
controller-decision list and the ``fable/overrides-policy-20260918``
branch. The override *values* themselves are untouched by any of
this — only whether/when they're allowed to affect a live weight.

See ``get_override()`` and ``get_effective_overrides()`` below, and
``governance/promotion_ledger.py`` for the ledger API.
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any

from loguru import logger as log


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "")
    if not raw.strip():
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on", "enabled")


# Master switch. Default True — the pre-existing production default,
# preserved as-is by this workstream (see SAFEGUARDS note above). This
# is NOT an endorsement that applying a contaminated-corpus-derived
# override table is safe; it means this module does not silently
# change current production behaviour. Whether to flip this off is a
# held operational decision — see GRID_SIGNAL_OVERRIDES_REQUIRE_LEDGER
# below and docs/handoffs/2026-09-18/fable-w7-held-policy-flip.md on
# the fable/overrides-policy-20260918 branch.
SIGNAL_OVERRIDES_ENABLED: bool = _env_bool("GRID_SIGNAL_OVERRIDES_ENABLED", True)

# Ledger-enforcement knob. Default False — legacy behaviour (no ledger
# check) is preserved by default. Set to true to require an approved
# governance.promotion_ledger record (matching this table's
# subject_hash + EVALUATION_VERSION) before an enabled override table
# is allowed to affect a live weight. See SAFEGUARDS note above.
SIGNAL_OVERRIDES_REQUIRE_LEDGER: bool = _env_bool(
    "GRID_SIGNAL_OVERRIDES_REQUIRE_LEDGER", False
)


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


# Deferred overrides — signals that appear in trade_postmortems but NOT
# in compute_aggregate_conviction's signal_evidence loop. They live
# in oracle/engine.py's ``signals.items[]`` instead. Applied there at
# the per-signal weight multiplication step (see engine.py around
# line 1372 — the bull/bear score calc).
#
# AUDIT NOTE (2026-05-13): alpha_research:vix_exposure and
# alpha_research:credit_cycle are NEUTRAL-direction REGIME signals
# since the 2026-04-28 fix in
# alpha_research/adapters/signal_adapter.py:156-180. Their old
# directional output produced the 1826r/0w pattern in pre-2026-04-28
# postmortems, which made them look like perfect predictors. They are
# NOT directional bets — credit_cycle routes signal-family weights via
# _get_credit_cycle_routing (already wired at engine.py:1242), and
# vix_exposure scales position size, not direction. Removed from the
# deferred set; the auto_improve advisory now filters its corpus to
# postmortems >= 2026-04-28 so these don't re-surface.
#
# news_intel is the remaining one — directional, 102r/204w, mild cut.
DEFERRED_SIGNAL_OVERRIDES: dict[str, float] = {
    "news_intel": 0.60,
}


# Hard bounds — a future automated tuner can't push any single
# override outside this range without explicit operator action.
SIGNAL_OVERRIDE_MIN: float = 0.20
SIGNAL_OVERRIDE_MAX: float = 1.40


# Identifies *which derivation* of the override table this is, for the
# promotion ledger. Bump this string (and get a fresh promotion) any
# time SIGNAL_WEIGHT_OVERRIDES or DEFERRED_SIGNAL_OVERRIDES changes —
# the subject_hash below is recomputed from it automatically, so a
# stale ledger entry will simply stop matching rather than silently
# covering a different table.
EVALUATION_VERSION: str = "postmortem-advisory-2026-05-13"


def compute_subject_hash(
    overrides: dict[str, float] | None = None,
    evaluation_version: str | None = None,
) -> str:
    """Deterministic sha256 hex digest identifying an override set.

    Defaults to hashing the module's own merged table
    (``SIGNAL_WEIGHT_OVERRIDES`` ∪ ``DEFERRED_SIGNAL_OVERRIDES``) and
    ``EVALUATION_VERSION``. Exposed with explicit params so callers
    (and the promotion ledger, when recommending a change) can compute
    the hash for a *candidate* table before it's live.
    """
    table = overrides if overrides is not None else {
        **SIGNAL_WEIGHT_OVERRIDES,
        **DEFERRED_SIGNAL_OVERRIDES,
    }
    version = evaluation_version if evaluation_version is not None else EVALUATION_VERSION
    payload = json.dumps(
        {"overrides": table, "evaluation_version": version},
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# Computed once at import time from the table shipped in this file.
# NOT recomputed per-call — if you edit the tables above, restart the
# process (or call compute_subject_hash() explicitly) to pick up the
# new hash; the point is that a stale in-memory hash from before an
# edit must NOT accidentally keep matching an old ledger approval.
_SUBJECT_HASH: str = compute_subject_hash()

_warned_missing_promotion: bool = False
_warned_legacy_no_ledger: bool = False


def _warn_legacy_no_ledger_once() -> None:
    """Log ONE warning (at startup/first use) that overrides are being
    applied without a promotion-ledger approval, naming the knob that
    would change that. Only reached when
    ``SIGNAL_OVERRIDES_REQUIRE_LEDGER`` is False (legacy path)."""
    global _warned_legacy_no_ledger
    if _warned_legacy_no_ledger:
        return
    log.warning(
        "GRID_SIGNAL_OVERRIDES_ENABLED is True and "
        "GRID_SIGNAL_OVERRIDES_REQUIRE_LEDGER is False: signal weight "
        "overrides derived from the trade_postmortems advisory "
        "(evaluation_version={v}) are being applied to live weights "
        "WITHOUT a promotion_ledger approval. Set "
        "GRID_SIGNAL_OVERRIDES_REQUIRE_LEDGER=true to require an "
        "approved governance.promotion_ledger record before these "
        "overrides take effect.",
        v=EVALUATION_VERSION,
    )
    _warned_legacy_no_ledger = True


def _overrides_gate_open() -> bool:
    """True iff current knobs allow the override table to be applied.

    - Master switch off => always False.
    - Master switch on, require_ledger False (legacy/default) => True,
      after logging the one-time "no ledger approval" warning.
    - Master switch on, require_ledger True => delegate to the ledger
      check (which does its own one-time warning on a miss).
    """
    if not SIGNAL_OVERRIDES_ENABLED:
        return False
    if not SIGNAL_OVERRIDES_REQUIRE_LEDGER:
        _warn_legacy_no_ledger_once()
        return True
    return _is_override_set_promoted()


def _is_override_set_promoted() -> bool:
    """True iff governance.promotion_ledger has an approved record for
    this exact override set (kind='weight_override', subject_hash and
    evaluation_version matching the table shipped in this module).

    Fails safe: any error reaching the ledger (no DB configured, no
    promotion_ledger table yet, etc.) is treated as "not promoted",
    exactly like a real absence — this module must never apply an
    override it can't positively verify was promoted.
    """
    global _warned_missing_promotion
    try:
        from db import get_engine
        from governance.promotion_ledger import is_approved

        promoted = is_approved(
            get_engine(),
            kind="weight_override",
            subject_hash=_SUBJECT_HASH,
            evaluation_version=EVALUATION_VERSION,
        )
    except Exception as exc:  # noqa: BLE001 — defensive, no DB in dev/test/CI
        log.debug(
            "signal_weight_overrides: promotion ledger check failed ({e}); "
            "treating as not promoted",
            e=str(exc),
        )
        promoted = False

    if not promoted and not _warned_missing_promotion:
        log.warning(
            "GRID_SIGNAL_OVERRIDES_ENABLED is True but no promotion_ledger "
            "entry approves this override set (kind=weight_override, "
            "subject_hash={h}, evaluation_version={v}). Applying NO signal "
            "weight overrides. Use governance.promotion_ledger.recommend() "
            "+ approve() to activate this table.",
            h=_SUBJECT_HASH,
            v=EVALUATION_VERSION,
        )
        _warned_missing_promotion = True
    return promoted


def get_override(signal_source: Any) -> float:
    """Return the multiplier for ``signal_source`` (1.0 = no effect).

    Returns 1.0 when the master switch is off; when
    ``GRID_SIGNAL_OVERRIDES_REQUIRE_LEDGER`` is True and the override
    set lacks a matching promotion_ledger approval; when the signal
    isn't in the override table; or when the input isn't a usable
    string. When the master switch is on and require_ledger is False
    (the legacy/default path), applies the override table and logs one
    warning that no ledger approval was checked.
    """
    if not _overrides_gate_open():
        return 1.0
    if not isinstance(signal_source, str) or not signal_source.strip():
        return 1.0
    m = SIGNAL_WEIGHT_OVERRIDES.get(signal_source.strip())
    if m is None:
        return 1.0
    return max(SIGNAL_OVERRIDE_MIN, min(SIGNAL_OVERRIDE_MAX, float(m)))


def get_effective_overrides() -> dict[str, float]:
    """Return the merged override table (bare + deferred) gated the
    same way as ``get_override()``: empty unless the master switch is
    on, and (when ``GRID_SIGNAL_OVERRIDES_REQUIRE_LEDGER`` is True) a
    matching promotion_ledger approval exists.

    This is the entry point ``oracle/engine.py`` uses at the
    per-signal weight multiplication step — it must never read
    ``SIGNAL_WEIGHT_OVERRIDES`` / ``DEFERRED_SIGNAL_OVERRIDES``
    directly, or it bypasses this gate entirely.
    """
    if not _overrides_gate_open():
        return {}
    return {**SIGNAL_WEIGHT_OVERRIDES, **DEFERRED_SIGNAL_OVERRIDES}


def set_enabled(value: bool) -> None:
    """Runtime toggle (mostly for tests / REPL)."""
    global SIGNAL_OVERRIDES_ENABLED
    SIGNAL_OVERRIDES_ENABLED = bool(value)


def set_require_ledger(value: bool) -> None:
    """Runtime toggle (mostly for tests / REPL)."""
    global SIGNAL_OVERRIDES_REQUIRE_LEDGER
    SIGNAL_OVERRIDES_REQUIRE_LEDGER = bool(value)


def reset_promotion_warning_state() -> None:
    """Test/REPL helper: clear both "warned once" latches (the
    require_ledger=True missing-promotion warning and the
    require_ledger=False legacy-path warning) so the next call logs
    again. Production code has no reason to call this."""
    global _warned_missing_promotion, _warned_legacy_no_ledger
    _warned_missing_promotion = False
    _warned_legacy_no_ledger = False
