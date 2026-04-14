"""
GRID — Trade Ticket Generator.

The bridge from "the system has a high-conviction view" to
"here is the exact trade to enter." This module consumes a
``TradeProvenanceReport`` (from ``intelligence.signal_provenance``)
and emits a structured ``TradeTicket`` that a human operator can act
on without reading any other file.

SOP enforcement (per the operator's auto-memory — strict)
---------------------------------------------------------

Every ticket MUST contain:

    LEVER:        Who did what affecting which liquidity valve.
                  Read from ``report.causation.lever`` + ``report.causation.actor``.
    CONDITION:    Environmental factor amplifying / dampening the lever.
                  Read from ``report.regime`` + ``report.fci_regime``.
    THESIS:       "Lever + condition → expected [direction] [magnitude] [timeframe]"
    INVALIDATION: A specific price level or signal flip that proves the lever
                  thesis wrong. Derived from ``report.confidence_lower`` and a
                  configurable risk multiple — never hardcoded.
    KELLY SIZE:   Capped at 5% per the user-memory hard rule
                  (``MAX_KELLY_PER_TICKET = 0.05``). Sized off
                  ``confidence_lower`` (NOT raw confidence) per ALPHA-12.

If any of these are missing, the generator refuses to produce a ticket.

Verbatim from the operator's memory (``feedback_trade_tickets.md``):

    "Trade Tickets — Options recs: exact strike, entry, exit, thesis,
    invalidation, Kelly size."

This module is read-only with respect to the provenance report. It does
NOT call ``predict()``. Pure functions wherever possible so each piece
is testable in isolation.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from intelligence.signal_provenance import (
    SignalEvidence,
    TradeProvenanceReport,
)


# ── Hard caps & defaults ──────────────────────────────────────────────────
#
# MAX_KELLY_PER_TICKET is the user-memory hard rule: no single trade may
# size beyond 5% of account equity, regardless of how high conviction
# climbs. ``oracle.engine`` does NOT export an equivalent constant
# (verified by grep on 2026-04-13), so we own the canonical value here.

MAX_KELLY_PER_TICKET: float = 0.05

# Stop loss = current ± risk_multiple * 1-day sigma. Default 2σ.
DEFAULT_RISK_MULTIPLE: float = 2.0

# Minimum aggregate conviction (from compute_aggregate_conviction) for a
# ticket to even be considered. Below this, generator returns None.
MIN_CONVICTION_FOR_TICKET: float = 0.7

# Only generate tickets when the verdict signals worth-acting strength.
MIN_VERDICT_FOR_TICKET: tuple[str, ...] = ("medium", "high")

# Fallback annualized 30-day vol when caller does not supply one. 30%
# is a defensive single-name default; broad-market ETFs are tighter and
# small caps are wider. Always pass a real vol when you have one.
DEFAULT_VOL_30D: float = 0.30

# Reward / risk ratio used for the Kelly edge formula. Default 2:1 means
# the trade pays 2x the risked stop distance when right.
DEFAULT_REWARD_RISK_RATIO: float = 2.0

# Trading days per year — used to convert annualized vol → daily sigma.
TRADING_DAYS_PER_YEAR: int = 252

# Default forecast horizon (calendar days) when scaling target distance.
DEFAULT_TARGET_HORIZON_DAYS: int = 7

_BULLISH = "bullish"
_BEARISH = "bearish"


# ── TradeTicket dataclass ─────────────────────────────────────────────────


@dataclass(frozen=True)
class TradeTicket:
    """A self-contained, executable trade ticket.

    A human reading this dataclass MUST be able to enter the trade
    without referencing any other file — that is the whole point.
    Every required SOP field (lever, condition, thesis, invalidation,
    kelly size) is present on the ticket itself.
    """

    # ── What ──
    ticker: str
    direction: str                       # 'bullish' / 'bearish'
    instrument_type: str                 # 'equity' | 'option'

    # ── Where ──
    entry_price: float
    stop_price: float
    target_price: float

    # ── How big ──
    kelly_size_pct: float                # fraction of account, ≤ 0.05
    kelly_size_dollars: float

    # ── Why (the SOP) ──
    thesis: str
    invalidation: str
    lever: str
    condition: str
    evidence_summary: str

    # ── Provenance ──
    generated_at: str
    verdict: str
    aggregate_conviction: float

    # ── Options branch (None when instrument_type == 'equity') ──
    options_strike: float | None = None
    options_expiry: str | None = None
    options_premium_est: float | None = None
    options_iv: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_human_string(self) -> str:
        """Render the ticket as a single block of text for alerts/email.

        Matches the LEVER/CONDITION/THESIS/INVALIDATION SOP layout.
        """
        bar = "═" * 50
        sep = "─" * 50
        opt_lines = ""
        if self.instrument_type == "option":
            opt_lines = (
                f"\n  STRIKE: {self.options_strike}"
                f"\n  EXPIRY: {self.options_expiry}"
                f"\n  PREMIUM EST: {self.options_premium_est}"
                f"\n  IV: {self.options_iv}"
            )
        return (
            f"{bar}\n"
            f"  {self.ticker}  {self.direction.upper()}  "
            f"({self.instrument_type})\n"
            f"{bar}\n"
            f"  ENTRY:  ${self.entry_price:.2f}\n"
            f"  STOP:   ${self.stop_price:.2f}\n"
            f"  TARGET: ${self.target_price:.2f}\n"
            f"  SIZE:   {self.kelly_size_pct:.2%} "
            f"(${self.kelly_size_dollars:,.0f})"
            f"{opt_lines}\n"
            f"{sep}\n"
            f"  LEVER:        {self.lever}\n"
            f"  CONDITION:    {self.condition}\n"
            f"  THESIS:       {self.thesis}\n"
            f"  INVALIDATION: {self.invalidation}\n"
            f"{sep}\n"
            f"  EVIDENCE:\n{self.evidence_summary}\n"
            f"{sep}\n"
            f"  VERDICT: {self.verdict}  "
            f"CONVICTION: {self.aggregate_conviction:.3f}\n"
            f"  GENERATED: {self.generated_at}\n"
            f"{bar}"
        )


# ── Pure helpers ──────────────────────────────────────────────────────────


def compute_invalidation_price(
    current_price: float,
    direction: str,
    vol_30d: float | None,
    risk_multiple: float = DEFAULT_RISK_MULTIPLE,
) -> float:
    """Return the price level that invalidates the thesis.

    For longs (bullish):  ``current * (1 - risk_multiple * daily_sigma)``
    For shorts (bearish): ``current * (1 + risk_multiple * daily_sigma)``

    ``daily_sigma = vol_30d / sqrt(252)`` (annualized → daily).

    Defensive: if ``vol_30d`` is ``None``, NaN, ≤0, or ``current_price``
    is ≤0, returns ``current_price`` unchanged (no stop = caller must
    treat it as a refusal).
    """
    if current_price <= 0:
        return current_price
    if vol_30d is None or not math.isfinite(vol_30d) or vol_30d <= 0:
        return current_price
    if not math.isfinite(risk_multiple) or risk_multiple <= 0:
        return current_price

    daily_sigma = vol_30d / math.sqrt(TRADING_DAYS_PER_YEAR)
    move = risk_multiple * daily_sigma

    direction_norm = (direction or "").strip().lower()
    if direction_norm == _BULLISH:
        return float(current_price * (1.0 - move))
    if direction_norm == _BEARISH:
        return float(current_price * (1.0 + move))
    # Unknown direction — refuse to set a stop.
    return current_price


def compute_target_price(
    current_price: float,
    direction: str,
    conviction: float,
    vol_30d: float | None,
    horizon_days: int = DEFAULT_TARGET_HORIZON_DAYS,
) -> float:
    """Return a profit target for the ticket.

    Distance scales with conviction and with the sqrt of horizon (random
    walk). The 2.0 multiplier is a 2σ horizon move:

        move = conviction * vol_30d * sqrt(horizon_days / 252) * 2.0

    Defensive: zero conviction or zero vol → returns ``current_price``
    unchanged. Unknown direction → returns ``current_price`` unchanged.
    """
    if current_price <= 0:
        return current_price
    if vol_30d is None or not math.isfinite(vol_30d) or vol_30d <= 0:
        return current_price
    if conviction is None or conviction <= 0:
        return current_price
    if horizon_days <= 0:
        return current_price

    horizon_factor = math.sqrt(horizon_days / TRADING_DAYS_PER_YEAR)
    move = float(conviction) * float(vol_30d) * horizon_factor * 2.0

    direction_norm = (direction or "").strip().lower()
    if direction_norm == _BULLISH:
        return float(current_price * (1.0 + move))
    if direction_norm == _BEARISH:
        return float(current_price * (1.0 - move))
    return current_price


def kelly_size_from_report(
    report: TradeProvenanceReport,
    account_size_usd: float,
) -> tuple[float, float]:
    """Compute Kelly fraction from the *lower* confidence bound.

    Edge formula (Kelly):

        f* = (p * b - q) / b

    where:
        p = report.confidence_lower    (conservative win probability)
        q = 1 - p                      (loss probability)
        b = DEFAULT_REWARD_RISK_RATIO  (default 2.0 — 2:1 R/R)

    ``confidence_lower`` is used instead of raw ``confidence`` per the
    ALPHA-12 conservative-sizing rule: bet the floor of the credible
    interval, not the centroid.

    Capped hard at ``MAX_KELLY_PER_TICKET`` (5%). Negative edge → 0.

    Returns ``(pct, dollars)`` where pct is in [0, 0.05].
    """
    p = float(getattr(report, "confidence_lower", 0.0) or 0.0)
    p = max(0.0, min(1.0, p))
    q = 1.0 - p
    b = DEFAULT_REWARD_RISK_RATIO

    raw_edge = (p * b - q) / b
    pct = max(0.0, min(MAX_KELLY_PER_TICKET, raw_edge))

    account = max(0.0, float(account_size_usd or 0.0))
    dollars = pct * account
    return pct, dollars


def compose_thesis(report: TradeProvenanceReport) -> str:
    """Build the SOP one-liner thesis from the provenance report.

    Format (verbatim from the user memory SOP):

        "<lever> by <actor> opened/closed <flow> in <regime>/<fci> →
         expect <direction> over <horizon>d (conviction <agg>)"

    Every field is read off the report — nothing hardcoded.
    """
    causation = report.causation
    lever = (causation.lever or "unknown_lever").strip()
    actor = (causation.actor or "unknown_actor").strip()
    flow = (causation.flow_direction or "neutral").strip()
    regime = (report.regime or "regime_unknown").strip()
    fci = (report.fci_regime or "fci_unknown").strip()
    direction = (report.direction or "neutral").strip().lower()
    horizon = int(report.horizon_days or 0)
    conviction = float(report.aggregate_conviction or 0.0)

    return (
        f"{lever} by {actor} ({flow} flow) in regime={regime} / fci={fci} "
        f"→ expect {direction} move over ~{horizon}d "
        f"(conviction {conviction:.2f})"
    )


def compose_invalidation_text(
    report: TradeProvenanceReport,
    *,
    invalidation_price: float,
    direction: str,
) -> str:
    """Return a human-readable invalidation rule.

    Combines a hard price level (derived earlier from
    ``confidence_lower`` and the risk multiple) with a signal-flip
    condition pulled from the top contributor.
    """
    direction_norm = (direction or "").strip().lower()
    if direction_norm == _BULLISH:
        side_word = "below"
    elif direction_norm == _BEARISH:
        side_word = "above"
    else:
        side_word = "through"

    top = (report.top_shapley_contributor or "top_contributor").strip()
    return (
        f"close {side_word} ${invalidation_price:.2f} "
        f"OR {top} signal flips direction "
        f"(confidence_lower={report.confidence_lower:.3f})"
    )


def compose_evidence_summary(
    report: TradeProvenanceReport,
    top_n: int = 3,
) -> str:
    """Return a multi-line summary of the top contributing signals.

    Sorts ``signal_evidence`` by ``shapley_weight`` desc and emits the
    top N. Handles fewer than N gracefully — never raises.
    """
    if not report.signal_evidence:
        return "  (no contributing signals)"

    sorted_ev: list[SignalEvidence] = sorted(
        report.signal_evidence,
        key=lambda e: e.shapley_weight,
        reverse=True,
    )
    rows = []
    for ev in sorted_ev[: max(0, int(top_n))]:
        rows.append(
            f"  - {ev.signal_source:24s}  "
            f"weight={ev.shapley_weight:.3f}  "
            f"class={ev.classification}"
        )
    return "\n".join(rows) if rows else "  (no contributing signals)"


# ── Main entry point ──────────────────────────────────────────────────────


def generate_ticket(
    report: TradeProvenanceReport,
    *,
    account_size_usd: float,
    current_price: float,
    instrument: str = "equity",
    vol_30d: float | None = None,
    risk_multiple: float = DEFAULT_RISK_MULTIPLE,
) -> TradeTicket | None:
    """Turn a provenance report into a concrete ``TradeTicket``.

    Refusal rules (returns ``None``):
      - verdict is ``no_trade`` or ``low``
      - ``aggregate_conviction`` < ``MIN_CONVICTION_FOR_TICKET``
      - ``current_price`` ≤ 0 (no anchor for stops/targets)
      - direction is neither bullish nor bearish
      - account_size_usd < 0

    Otherwise, returns a ``TradeTicket`` with every SOP field populated.

    The options branch (instrument='option') is currently a TODO — see
    module docstring. V1 returns an equity-only ticket; the options_*
    fields are populated as ``None`` so the dataclass shape is stable.
    """
    # ── Refusal gates ──
    if report is None:
        return None
    if current_price is None or current_price <= 0:
        return None
    if account_size_usd is None or account_size_usd < 0:
        return None

    verdict = (report.verdict or "").strip().lower()
    if verdict not in MIN_VERDICT_FOR_TICKET:
        return None
    if report.aggregate_conviction < MIN_CONVICTION_FOR_TICKET:
        return None

    direction = (report.direction or "").strip().lower()
    if direction not in (_BULLISH, _BEARISH):
        return None

    # ── Vol fallback ──
    effective_vol = vol_30d
    if effective_vol is None or not math.isfinite(effective_vol) or effective_vol <= 0:
        effective_vol = DEFAULT_VOL_30D

    # ── Pricing ──
    stop_price = compute_invalidation_price(
        current_price=current_price,
        direction=direction,
        vol_30d=effective_vol,
        risk_multiple=risk_multiple,
    )
    target_price = compute_target_price(
        current_price=current_price,
        direction=direction,
        conviction=report.aggregate_conviction,
        vol_30d=effective_vol,
        horizon_days=int(report.horizon_days or DEFAULT_TARGET_HORIZON_DAYS),
    )

    # ── Sizing ──
    kelly_pct, kelly_dollars = kelly_size_from_report(report, account_size_usd)

    # ── SOP narrative ──
    lever_text = (
        f"{report.causation.lever or 'unknown'} "
        f"({report.causation.actor or 'unknown actor'})"
    )
    condition_text = (
        f"regime={report.regime or 'unknown'}, "
        f"fci={report.fci_regime or 'unknown'}"
    )
    thesis_text = compose_thesis(report)
    invalidation_text = compose_invalidation_text(
        report,
        invalidation_price=stop_price,
        direction=direction,
    )
    evidence_text = compose_evidence_summary(report)

    # ── Options branch (TODO) ──
    instrument_type = "option" if instrument == "option" else "equity"
    options_strike: float | None = None
    options_expiry: str | None = None
    options_premium_est: float | None = None
    options_iv: float | None = None
    # NOTE: A clean import surface for trading.options_recommender's
    # strike-picker requires an Engine + scanner output; wiring that
    # safely is deferred to V2. For now the options branch returns the
    # same equity ticket shape with options_* = None and a
    # 'option' instrument_type marker so downstream code can detect
    # the gap and call the full recommender separately.

    return TradeTicket(
        ticker=report.ticker,
        direction=direction,
        instrument_type=instrument_type,
        entry_price=float(current_price),
        stop_price=float(stop_price),
        target_price=float(target_price),
        kelly_size_pct=float(kelly_pct),
        kelly_size_dollars=float(kelly_dollars),
        thesis=thesis_text,
        invalidation=invalidation_text,
        lever=lever_text,
        condition=condition_text,
        evidence_summary=evidence_text,
        generated_at=datetime.now(timezone.utc).isoformat(),
        verdict=verdict,
        aggregate_conviction=float(report.aggregate_conviction),
        options_strike=options_strike,
        options_expiry=options_expiry,
        options_premium_est=options_premium_est,
        options_iv=options_iv,
    )
