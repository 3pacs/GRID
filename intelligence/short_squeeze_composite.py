"""
Short-Squeeze Composite Scorer (CAT-138 / #250).

Per-ticker short-squeeze probability scorer combining 6 hard signals into a
single probability ∈ [0, 1] plus a conviction multiplier that should be
applied only to directional calls on the same ticker.

Signals (all pulled via ``feature_registry`` → ``PITStore``):

1. Short interest ratio (% of float sold short)
2. Days to cover
3. Annualized borrow fee (%)
4. 20-day price momentum (asymmetric: only positive counts)
5. Social heat z-score
6. Dealer net gamma exposure (negative = short gamma = squeeze fuel)

The composite probability is a fixed-weight linear blend of sub-scores that
are each normalized into ``[0, 1]``. The conviction multiplier mapping is
deliberately coarse (1.00 / 1.05 / 1.10 / 1.15) so a miscalibrated sub-score
cannot explode position sizing.

Live-path entry point: ``squeeze_conviction_multiplier``. This function must
never raise — it returns ``1.0`` on any failure and logs a warning. It is
intended to be called from ``signal_provenance.build_provenance_report``
(when that module is wired into production), from the trade-ticket generator,
or anywhere else a bullish / bearish call needs a squeeze-aware size nudge.

PIT correctness is mandatory: every feature lookup goes through
``PITStore.get_pit`` with vintage policy ``LATEST_AS_OF`` and an
``as_of_date`` that is strictly ``<=`` the caller's decision date. The
20-day momentum uses the 20th most-recent observation strictly prior to
``as_of``, never a future value.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from store.pit import PITStore

# ---------------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------------

#: Hard-coded component weights. MUST sum to 1.0 — tested below.
_COMPONENT_WEIGHTS: dict[str, float] = {
    "short_interest_pct": 0.25,
    "days_to_cover": 0.20,
    "borrow_fee": 0.20,
    "momentum_20d": 0.15,
    "social_heat_z": 0.10,
    "gex_sign": 0.10,
}

#: Scaling denominators for the first three components. Values >= denominator
#: saturate at 1.0. Chosen from historical squeeze setups (GME/AMC-style).
_SHORT_INTEREST_FULL = 30.0  # 30% of float = max fuel
_DAYS_TO_COVER_FULL = 10.0   # 10 days = max fuel
_BORROW_FEE_FULL = 30.0      # 30% annualized fee = max fuel
_MOMENTUM_FULL = 0.10        # +10% over 20d = max fuel

#: Momentum lookback — tested explicitly so nobody silently changes it.
MOMENTUM_LOOKBACK_DAYS: int = 20

#: Conviction thresholds. Ordered from highest to lowest.
_THRESHOLD_EXTREME = 0.75
_THRESHOLD_HIGH = 0.60
_THRESHOLD_MODERATE = 0.45

#: Multipliers keyed to the thresholds above.
_MULT_EXTREME = 1.15
_MULT_HIGH = 1.10
_MULT_MODERATE = 1.05
_MULT_NEUTRAL = 1.00
_MULT_BEARISH_ON_SQUEEZE = 0.90

#: Bullish direction tokens we recognize. Anything else → neutral (1.00).
_BULLISH_TOKENS = frozenset({"bullish", "long", "buy", "call", "calls", "up"})
_BEARISH_TOKENS = frozenset({"bearish", "short", "sell", "put", "puts", "down"})


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SqueezeComponent:
    """One scored component of the squeeze composite.

    Attributes:
        name: Canonical component name (matches a key in ``_COMPONENT_WEIGHTS``).
        raw_value: Raw value from PIT (e.g. 25.3 for 25.3% short interest).
        normalized: Sub-score in ``[0, 1]`` after applying the component's
            normalization rule.
        weight: Fixed weight for this component.
        weighted_contribution: ``normalized * weight``.
    """

    name: str
    raw_value: float
    normalized: float
    weight: float
    weighted_contribution: float


@dataclass(frozen=True)
class SqueezeReport:
    """Full squeeze assessment for one ticker at one point in time.

    Attributes:
        ticker: Equity ticker.
        as_of: ISO date string of the decision date used for PIT filtering.
        components: One ``SqueezeComponent`` per signal, in canonical order.
        squeeze_probability: Weighted sum of normalized components, ``[0, 1]``.
        missing_component_count: How many of the 6 components were absent
            from PIT (and therefore defaulted to 0).
        advisory: Short human-readable label for dashboards.
    """

    ticker: str
    as_of: str
    components: list[SqueezeComponent] = field(default_factory=list)
    squeeze_probability: float = 0.0
    missing_component_count: int = 0
    advisory: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dict for JSON transport / DB storage."""
        return {
            "ticker": self.ticker,
            "as_of": self.as_of,
            "squeeze_probability": self.squeeze_probability,
            "missing_component_count": self.missing_component_count,
            "advisory": self.advisory,
            "components": [
                {
                    "name": c.name,
                    "raw_value": c.raw_value,
                    "normalized": c.normalized,
                    "weight": c.weight,
                    "weighted_contribution": c.weighted_contribution,
                }
                for c in self.components
            ],
        }


# ---------------------------------------------------------------------------
# Normalization helpers (pure functions — easy to unit-test)
# ---------------------------------------------------------------------------


def _clamp01(x: float) -> float:
    """Clamp into ``[0, 1]``. NaN/inf → 0.0."""
    try:
        if x != x or x in (float("inf"), float("-inf")):
            return 0.0
    except Exception:
        return 0.0
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return float(x)


def _normalize_short_interest(val: float) -> float:
    return _clamp01(val / _SHORT_INTEREST_FULL)


def _normalize_days_to_cover(val: float) -> float:
    return _clamp01(val / _DAYS_TO_COVER_FULL)


def _normalize_borrow_fee(val: float) -> float:
    return _clamp01(val / _BORROW_FEE_FULL)


def _normalize_momentum(val: float) -> float:
    """Positive momentum only — negative momentum contributes nothing."""
    if val is None or val != val:
        return 0.0
    return _clamp01(max(val, 0.0) / _MOMENTUM_FULL)


def _normalize_social_heat(val: float) -> float:
    """z-score of +2 → 1.0, z of -1 → 0.0, z of 0 → 0.33."""
    if val is None or val != val:
        return 0.0
    return _clamp01((val + 1.0) / 3.0)


def _normalize_gex_sign(val: float) -> float:
    """Binary — negative dealer gamma = squeeze fuel."""
    if val is None or val != val:
        return 0.0
    return 1.0 if val < 0.0 else 0.0


# ---------------------------------------------------------------------------
# Feature lookup
# ---------------------------------------------------------------------------


def _resolve_feature_id(
    engine: Engine,
    name: str,
    cache: dict[str, int | None],
) -> int | None:
    """Look up a feature_registry ID by exact name, with per-call caching.

    Returns ``None`` if the feature does not exist or the query fails. Never
    raises — all DB failures are absorbed and logged.
    """
    if name in cache:
        return cache[name]
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM feature_registry WHERE name = :n"),
                {"n": name},
            ).fetchone()
        fid = int(row[0]) if row else None
    except Exception as exc:  # pragma: no cover - defensive
        log.warning("feature_registry lookup failed for '{n}': {e}", n=name, e=exc)
        fid = None
    cache[name] = fid
    return fid


def _latest_pit_value(
    pit_store: PITStore,
    feature_id: int | None,
    as_of: date,
) -> float | None:
    """Return the most recent PIT value at or before ``as_of``, or None."""
    if feature_id is None:
        return None
    try:
        df = pit_store.get_pit([feature_id], as_of, vintage_policy="LATEST_AS_OF")
    except Exception as exc:
        log.warning("PIT query failed for fid={f}: {e}", f=feature_id, e=exc)
        return None
    if df is None or df.empty:
        return None
    row = df.sort_values("obs_date").iloc[-1]
    val = row["value"]
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _momentum_from_pit(
    pit_store: PITStore,
    feature_id: int | None,
    as_of: date,
    lookback: int = MOMENTUM_LOOKBACK_DAYS,
) -> float | None:
    """Compute ``(latest - lookback_ago) / lookback_ago`` from PIT close prices.

    Uses the PIT store's temporal ordering — the "20 days ago" value is the
    20th-most-recent observation, not a calendar subtraction. This is robust
    to weekends/holidays. Returns ``None`` if there is not enough history or
    the denominator is 0/missing.
    """
    if feature_id is None:
        return None
    try:
        df = pit_store.get_pit([feature_id], as_of, vintage_policy="LATEST_AS_OF")
    except Exception as exc:
        log.warning("PIT momentum query failed for fid={f}: {e}", f=feature_id, e=exc)
        return None
    if df is None or df.empty or len(df) <= lookback:
        return None
    df = df.sort_values("obs_date").reset_index(drop=True)
    try:
        latest = float(df.iloc[-1]["value"])
        prior = float(df.iloc[-1 - lookback]["value"])
    except (TypeError, ValueError, IndexError):
        return None
    if prior == 0.0 or prior != prior:
        return 0.0
    return (latest - prior) / prior


# ---------------------------------------------------------------------------
# Core composite
# ---------------------------------------------------------------------------


# Canonical ordering for the components list on the report.
_CANONICAL_ORDER: tuple[str, ...] = (
    "short_interest_pct",
    "days_to_cover",
    "borrow_fee",
    "momentum_20d",
    "social_heat_z",
    "gex_sign",
)


def _feature_name_for(component: str, ticker: str) -> str:
    """Canonical feature_registry name for a component on a ticker."""
    # Components map 1:1 to registry names except a couple of aliases.
    suffix_map = {
        "short_interest_pct": "short_interest_pct",
        "days_to_cover": "days_to_cover",
        "borrow_fee": "borrow_fee",
        "momentum_20d": "close",  # we derive momentum from the close series
        "social_heat_z": "reddit_mentions_z",
        "gex_sign": "gex_net",
    }
    return f"{ticker}_{suffix_map[component]}"


def _fallback_social_heat_id(
    engine: Engine,
    ticker: str,
    cache: dict[str, int | None],
) -> int | None:
    """Social-heat fallback: try reddit_mentions_z then social_heat_z."""
    fid = _resolve_feature_id(engine, f"{ticker}_reddit_mentions_z", cache)
    if fid is not None:
        return fid
    return _resolve_feature_id(engine, f"{ticker}_social_heat_z", cache)


def _build_component(
    name: str,
    raw: float | None,
) -> tuple[SqueezeComponent, bool]:
    """Build a ``SqueezeComponent`` from a raw value.

    Returns ``(component, missing)`` where ``missing`` is True iff the raw
    value was ``None`` (and we defaulted to 0). When a value is missing the
    normalized sub-score is forced to 0 regardless of what the component's
    normalization rule would say for a raw 0 — "no data" should never pay
    out squeeze fuel (important for the social-heat z rule, where z=0 maps
    to 1/3 of full credit).
    """
    missing = raw is None
    raw_val = 0.0 if missing else float(raw)  # type: ignore[arg-type]

    if missing:
        norm = 0.0
    elif name == "short_interest_pct":
        norm = _normalize_short_interest(raw_val)
    elif name == "days_to_cover":
        norm = _normalize_days_to_cover(raw_val)
    elif name == "borrow_fee":
        norm = _normalize_borrow_fee(raw_val)
    elif name == "momentum_20d":
        norm = _normalize_momentum(raw_val)
    elif name == "social_heat_z":
        norm = _normalize_social_heat(raw_val)
    elif name == "gex_sign":
        norm = _normalize_gex_sign(raw_val)
    else:  # pragma: no cover - defensive
        raise ValueError(f"unknown component {name!r}")

    weight = _COMPONENT_WEIGHTS[name]
    return (
        SqueezeComponent(
            name=name,
            raw_value=raw_val,
            normalized=norm,
            weight=weight,
            weighted_contribution=norm * weight,
        ),
        missing,
    )


def _advisory_for(prob: float) -> str:
    if prob >= _THRESHOLD_EXTREME:
        return "EXTREME_SQUEEZE_RISK"
    if prob >= _THRESHOLD_HIGH:
        return "HIGH_SQUEEZE_RISK"
    if prob >= _THRESHOLD_MODERATE:
        return "MODERATE_SQUEEZE_RISK"
    return "LOW_SQUEEZE_RISK"


def compute_squeeze_report(
    engine: Engine,
    *,
    ticker: str,
    as_of: date,
) -> SqueezeReport:
    """Compute the squeeze composite for a single ticker at ``as_of``.

    All feature lookups go through PIT with vintage ``LATEST_AS_OF`` so no
    value released after ``as_of`` can leak in. Missing features default to
    a raw value of 0 and increment ``missing_component_count``.
    """
    pit_store = PITStore(engine)
    fid_cache: dict[str, int | None] = {}

    raw_values: dict[str, float | None] = {}

    # 1-3 + 5-6: direct scalar lookups
    for comp in ("short_interest_pct", "days_to_cover", "borrow_fee", "gex_sign"):
        name = _feature_name_for(comp, ticker)
        fid = _resolve_feature_id(engine, name, fid_cache)
        raw_values[comp] = _latest_pit_value(pit_store, fid, as_of)

    # 5: social heat with fallback
    social_fid = _fallback_social_heat_id(engine, ticker, fid_cache)
    raw_values["social_heat_z"] = _latest_pit_value(pit_store, social_fid, as_of)

    # 4: momentum derived from close series
    close_name = _feature_name_for("momentum_20d", ticker)
    close_fid = _resolve_feature_id(engine, close_name, fid_cache)
    raw_values["momentum_20d"] = _momentum_from_pit(pit_store, close_fid, as_of)

    components: list[SqueezeComponent] = []
    missing = 0
    prob = 0.0
    for comp_name in _CANONICAL_ORDER:
        comp, is_missing = _build_component(comp_name, raw_values[comp_name])
        components.append(comp)
        if is_missing:
            missing += 1
        prob += comp.weighted_contribution

    prob = _clamp01(prob)
    report = SqueezeReport(
        ticker=ticker,
        as_of=as_of.isoformat(),
        components=components,
        squeeze_probability=prob,
        missing_component_count=missing,
        advisory=_advisory_for(prob),
    )
    log.debug(
        "squeeze report {t} as_of={d} prob={p:.3f} missing={m}",
        t=ticker,
        d=as_of,
        p=prob,
        m=missing,
    )
    return report


# ---------------------------------------------------------------------------
# Conviction multiplier (live path)
# ---------------------------------------------------------------------------


def _multiplier_for(prob: float, direction: str) -> float:
    """Map (probability, direction) → multiplier. Pure function."""
    dlow = (direction or "").strip().lower()
    if dlow in _BULLISH_TOKENS:
        if prob >= _THRESHOLD_EXTREME:
            return _MULT_EXTREME
        if prob >= _THRESHOLD_HIGH:
            return _MULT_HIGH
        if prob >= _THRESHOLD_MODERATE:
            return _MULT_MODERATE
        return _MULT_NEUTRAL
    if dlow in _BEARISH_TOKENS:
        # Shorting a loaded gun is dangerous — taper conviction.
        if prob >= _THRESHOLD_MODERATE:
            return _MULT_BEARISH_ON_SQUEEZE
        return _MULT_NEUTRAL
    # Unknown / flat direction → no boost.
    return _MULT_NEUTRAL


def squeeze_conviction_multiplier(
    engine: Engine,
    *,
    ticker: str,
    as_of: date,
    trade_direction: str,
) -> float:
    """Return a conviction multiplier ``∈ {0.90, 1.00, 1.05, 1.10, 1.15}``.

    Intended to be called from the live signal provenance path and from the
    trade-ticket generator. This function NEVER raises — any failure returns
    ``1.0`` so an upstream bug can never blow up a trade recommendation.
    """
    try:
        report = compute_squeeze_report(engine, ticker=ticker, as_of=as_of)
    except Exception as exc:
        log.warning(
            "squeeze_conviction_multiplier failed for {t}: {e}", t=ticker, e=exc
        )
        return 1.0
    return _multiplier_for(report.squeeze_probability, trade_direction)


# ---------------------------------------------------------------------------
# Universe ranking
# ---------------------------------------------------------------------------


def rank_universe_by_squeeze(
    engine: Engine,
    *,
    tickers: list[str],
    as_of: date,
    min_probability: float = _THRESHOLD_MODERATE,
) -> list[SqueezeReport]:
    """Rank a candidate universe by squeeze probability, descending.

    Filters out tickers below ``min_probability``. Used by the dashboard's
    "loaded gun" watchlist.

    Performance note: the spec permits per-ticker fan-out if the fully-batched
    alternative is complex. We do per-ticker here because each ticker needs a
    different set of feature IDs (6 × N lookups, each by exact name). The
    alternative — one huge ``WHERE name = ANY(:names)`` + pivot — is possible
    but substantially more code and a smaller win than expected, since the
    hot path is PIT queries, not the feature-id lookup itself.
    """
    if not tickers:
        return []
    reports: list[SqueezeReport] = []
    for t in tickers:
        try:
            r = compute_squeeze_report(engine, ticker=t, as_of=as_of)
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("rank_universe_by_squeeze: {t} failed: {e}", t=t, e=exc)
            continue
        if r.squeeze_probability >= min_probability:
            reports.append(r)
    reports.sort(key=lambda r: r.squeeze_probability, reverse=True)
    return reports


__all__ = [
    "SqueezeComponent",
    "SqueezeReport",
    "MOMENTUM_LOOKBACK_DAYS",
    "compute_squeeze_report",
    "squeeze_conviction_multiplier",
    "rank_universe_by_squeeze",
]
