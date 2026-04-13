"""ALPHA-13 / task #116 — Per-regime submodel router.

Splits the oracle into 5 regime-specific sub-oracles routed by
``intelligence.liquidity_regime.classify_current_regime``. Each sub-oracle
shares the same underlying model heads (flow_momentum, regime_contrarian,
options_flow, …) but multiplies their horizon-bucket weight by a
per-regime weight stored in ``oracle_models.regime_buckets``.

The canonical five regime states come from
``intelligence.liquidity_regime``:

    CRISIS, TIGHTENING, NEUTRAL, EXPANSION, EXPANSION_STRONG

This module is the thin read/write layer that owns the regime_buckets
JSONB column. Regime classification itself lives in
``intelligence/liquidity_regime.py`` and is NOT duplicated here — we just
call ``classify_current_regime`` and route on the returned state string.

Phase 0 closer. Stacks multiplicatively with every other shipped
Tier A multiplier (ALPHA-1/2/3/4/5/6/7/8/9/10/11/12).
"""
from __future__ import annotations

import json
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Reuse the canonical state tuple + clamp bounds from their owning modules
# instead of redefining them — source-of-truth invariants.
from intelligence.liquidity_regime import ALL_STATES

# Mirror ALPHA-3's clamp bounds so a nudge can never escape the same
# interval ModelRegistry uses for horizon buckets.
MIN_WEIGHT: float = 0.1
MAX_WEIGHT: float = 5.0


# ── Defaults ────────────────────────────────────────────────────────────────


# The canonical regime list in the order used by the migration + tests.
REGIME_STATES: tuple[str, ...] = ALL_STATES


# In-memory fallback for rows where regime_buckets is missing (un-migrated
# DB). Also exposed as a module constant so downstream callers can assert
# the baseline neutral multiplier set.
REGIME_WEIGHTS_DEFAULT: dict[str, dict[str, float]] = {
    state: {
        "weight": 1.0,
        "hits": 0,
        "misses": 0,
        "partials": 0,
        "scored": 0,
        "brier": 0.0,
        "ece": 0.0,
    }
    for state in REGIME_STATES
}


def _default_regime_buckets() -> dict[str, dict[str, float]]:
    """Return a freshly initialised regime_buckets dict for a new model.

    Every state starts at ``weight=1.0`` with zero counters, matching the
    migration 0045 seed payload and ALPHA-3's ``_default_horizon_buckets``
    shape. The event path stays mathematically identical to the
    horizon-only baseline until the first ``PredictionScored`` event with
    a populated regime tag lands.
    """
    return {
        state: {
            "weight": 1.0,
            "hits": 0,
            "misses": 0,
            "partials": 0,
            "scored": 0,
            "brier": 0.0,
            "ece": 0.0,
        }
        for state in REGIME_STATES
    }


def parse_regime_buckets(raw: Any) -> dict[str, dict[str, float]]:
    """Coerce a raw JSONB / dict / JSON-string payload into the canonical
    regime_buckets shape. Missing states are seeded from the default
    factory; malformed fields fall back to their neutral values.

    Mirrors ALPHA-3's ``_parse_horizon_buckets`` — single entry point for
    trusting untrusted persistence data so the rest of the router can
    rely on dict-shaped buckets.
    """
    if raw is None:
        return _default_regime_buckets()
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return _default_regime_buckets()
    if not isinstance(raw, dict):
        return _default_regime_buckets()
    defaults = _default_regime_buckets()
    parsed: dict[str, dict[str, float]] = {}
    for state_key in REGIME_STATES:
        stored = raw.get(state_key)
        if not isinstance(stored, dict):
            parsed[state_key] = dict(defaults[state_key])
            continue
        merged = dict(defaults[state_key])
        for field_name in merged:
            if field_name in stored:
                try:
                    if field_name in {"weight", "brier", "ece"}:
                        merged[field_name] = float(stored[field_name])
                    else:
                        merged[field_name] = int(stored[field_name])
                except (TypeError, ValueError):
                    pass
        parsed[state_key] = merged
    return parsed


def _canonical_regime(state: str | None) -> str:
    """Snap an arbitrary regime string to the nearest canonical state.

    Unknown / empty states fall back to ``NEUTRAL`` so the router never
    returns a weight of zero for a legitimately predicted ticker just
    because the classifier is in a degraded state.
    """
    if not state:
        return "NEUTRAL"
    upper = str(state).strip().upper()
    if upper in REGIME_STATES:
        return upper
    return "NEUTRAL"


# ── Router ──────────────────────────────────────────────────────────────────


class RegimeRouter:
    """Thin read/write layer over ``oracle_models.regime_buckets``.

    The router caches nothing across calls — every public method opens a
    short-lived connection on ``engine`` — so it is safe to instantiate on
    every ``EnsemblePredictor.predict`` invocation without leaking state
    or pinning rows.
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    # ── Regime classification passthrough ────────────────────────────────

    def current_regime(self, as_of: Any = None) -> str:
        """Return the current liquidity regime state string.

        Passthrough to ``intelligence.liquidity_regime.classify_current_regime``
        so the predict path has a single entry point for both the ALPHA-5
        confidence dampener AND the ALPHA-13 weight router. Falls back to
        ``NEUTRAL`` when the classifier raises (missing history, DB down,
        mocked engine) so the router never takes down the predict path.
        """
        try:
            from intelligence.liquidity_regime import classify_current_regime

            regime = classify_current_regime(self.engine)
            return _canonical_regime(getattr(regime, "state", None))
        except Exception as exc:  # pragma: no cover — defensive
            log.debug("RegimeRouter.current_regime failed: {e}", e=str(exc))
            return "NEUTRAL"

    # ── Weight reads ──────────────────────────────────────────────────────

    def model_regime_weight(self, model_name: str, regime: str) -> float:
        """Return ``oracle_models.regime_buckets[regime].weight`` for
        ``model_name``, defaulting to 1.0 when the row, column, or bucket
        is missing / zero / malformed.

        This is the hot path called once per (model, event) pair inside
        ``EnsemblePredictor.predict``. SELECTs a single JSONB column and
        returns immediately — no additional I/O.
        """
        state = _canonical_regime(regime)
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT regime_buckets FROM oracle_models "
                        "WHERE name = :n"
                    ),
                    {"n": model_name},
                ).fetchone()
        except Exception as exc:
            log.debug(
                "RegimeRouter.model_regime_weight SELECT failed {m}: {e}",
                m=model_name, e=str(exc),
            )
            return 1.0
        if not row or row[0] is None:
            return 1.0
        parsed = parse_regime_buckets(row[0])
        bucket = parsed.get(state) or {}
        try:
            w = float(bucket.get("weight", 1.0) or 1.0)
        except (TypeError, ValueError):
            return 1.0
        if w <= 0.0:
            return 1.0
        return w

    # ── Weight writes ─────────────────────────────────────────────────────

    def nudge_regime_weight(
        self, model_name: str, regime: str, delta: float,
    ) -> float:
        """Additively nudge ``regime_buckets[regime].weight`` by ``delta``.

        Reads the current bucket, applies ``delta``, clamps to
        ``[MIN_WEIGHT, MAX_WEIGHT]``, and writes back via ``jsonb_set``.
        Used by the ModelRegistry per-event nudge path; callers that want
        Bayesian-target semantics should compute the delta themselves and
        pass it in (the router is intentionally dumb to keep the nudge
        math in one place — ``ModelRegistry._nudge_single_model``).

        Returns the clamped new weight. Falls back silently (returns
        1.0) when the row is missing or the UPDATE fails — the contract
        handler is non-fatal by design.
        """
        state = _canonical_regime(regime)
        try:
            with self.engine.begin() as conn:
                row = conn.execute(
                    text(
                        "SELECT regime_buckets FROM oracle_models "
                        "WHERE name = :n"
                    ),
                    {"n": model_name},
                ).fetchone()
                parsed = parse_regime_buckets(row[0] if row else None)
                bucket = dict(parsed.get(state) or {})
                try:
                    cur_w = float(bucket.get("weight", 1.0) or 1.0)
                except (TypeError, ValueError):
                    cur_w = 1.0
                new_w = max(MIN_WEIGHT, min(MAX_WEIGHT, cur_w + float(delta)))
                bucket["weight"] = round(new_w, 6)

                conn.execute(
                    text(
                        "UPDATE oracle_models "
                        "SET regime_buckets = jsonb_set("
                        "    COALESCE(regime_buckets, '{}'::jsonb), "
                        "    :path, CAST(:bucket AS JSONB), true) "
                        "WHERE name = :name"
                    ),
                    {
                        "path": "{" + state + "}",
                        "bucket": json.dumps(bucket),
                        "name": model_name,
                    },
                )
                return new_w
        except Exception as exc:
            log.debug(
                "RegimeRouter.nudge_regime_weight failed {m}/{r}: {e}",
                m=model_name, r=state, e=str(exc),
            )
            return 1.0

    # ── Reporting ─────────────────────────────────────────────────────────

    def summary(self) -> dict[str, dict[str, float]]:
        """Return a per-model × per-regime weight matrix for the report layer.

        Shape: ``{model_name: {CRISIS: 1.0, TIGHTENING: 1.0, …}}``.
        Missing rows are skipped entirely — the report layer should treat
        an absent model as 1.0 across the board.
        """
        out: dict[str, dict[str, float]] = {}
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT name, regime_buckets FROM oracle_models "
                        "ORDER BY name"
                    )
                ).fetchall()
        except Exception as exc:
            log.debug("RegimeRouter.summary SELECT failed: {e}", e=str(exc))
            return out
        for r in rows or []:
            name = r[0]
            parsed = parse_regime_buckets(r[1])
            out[name] = {
                state: float(parsed.get(state, {}).get("weight", 1.0) or 1.0)
                for state in REGIME_STATES
            }
        return out
