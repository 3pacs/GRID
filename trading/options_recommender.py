"""
GRID — Options trade recommendation engine.

Takes scanner output + GEX profile + dealer regime and produces specific,
actionable trade recommendations with multi-layer sanity gating.

Pipeline:
  1. Run options scanner for each ticker (discovery/options_scanner.py)
  2. For each opportunity scoring >= 6:
     - Pull GEX profile from DealerGammaEngine (gamma flip, walls, vanna/charm)
     - Optimize strike: best gamma/premium ratio within direction
     - Pick expiry: 2-6 weeks out, avoid weekly OpEx, prefer monthly
     - Compute entry from bid/ask mid, target from gamma wall distance,
       stop from gamma flip point
     - Size via Kelly criterion (signal_executor pattern)
     - Run 5-layer sanity pipeline (hundredx_digest pattern)
  3. Return only recommendations passing all sanity layers
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Canonical pricing/sizing utilities ──────────────────────────────
#
# These module-level helpers are the single source of truth for Kelly
# sizing, strike selection, expiry selection, and premium estimation
# across every pipeline that emits options tickets (scanner-driven,
# contagion-driven, signal-driven).  Other trading modules should
# import from here rather than rolling their own.

# Cap any single ticket at this fraction of portfolio, regardless of Kelly math.
MAX_KELLY_PER_TICKET: float = 0.05

# Default payout ratio when no explicit R:R is supplied (rule-of-thumb for
# options tickets: 3:1 target:risk).
DEFAULT_PAYOUT_RATIO: float = 3.0

# Expiry window tunables used by ``pick_expiry`` — at least 14 days, and
# scale with the magnitude of the shock/edge.
MIN_DTE: int = 14
DTE_SCALE_BY_IMPACT: float = 180.0

# Estimated entry / target / stop premium fractions used by ``estimate_premium``.
# IV_atm (annualised) × sqrt(dte/365) × S ≈ 1σ straddle move; entry ≈ half.
ESTIMATE_ENTRY_SIGMA_FRAC: float = 0.5
ESTIMATE_TARGET_MULT: float = 2.0
ESTIMATE_STOP_MULT: float = 0.5

# Tag written onto any premium produced by ``estimate_premium`` so the ticket
# reader can tell a modelled number from a quoted one.
PREMIUM_BASIS_MODELLED: str = "modelled_1sigma"

# ── Empirical win-probability lookup (audit C-H6) ───────────────────
#
# Win probability is READ FROM OUTCOMES or it is ``None``.  There is no
# formula mapping a scanner score onto a hit rate: the previous
# ``0.30 + (score - 5) * 0.06`` was a tuning constant wearing the name of
# a measurement, and it sized real Kelly bets.
#
# The replacement buckets closed recommendations by the scanner score
# they were generated from and returns the realised win rate for the
# bucket the candidate falls in — but only once the bucket holds at least
# ``WIN_PROB_MIN_SAMPLE`` resolved trades.  Below that the answer is
# ``None`` and every number derived from it (Kelly fraction, suggested
# contracts, expected return) is ``None`` too.
WIN_PROB_MIN_SAMPLE: int = 20
WIN_PROB_SCORE_BUCKET: float = 1.0

# Risk-free rate used by the Black-Scholes fallback pricer.  Echoed into
# every recommendation it prices so the reader can see the assumption.
MODEL_RISK_FREE_RATE: float = 0.05


def compute_kelly_fraction(
    accuracy: float,
    payout_ratio: float = DEFAULT_PAYOUT_RATIO,
    cap: float = MAX_KELLY_PER_TICKET,
) -> float:
    """Kelly fraction with a hard cap — canonical module-level version.

    ``f* = (p × b − q) / b`` where ``p = accuracy``, ``q = 1 − p``,
    ``b = payout_ratio``.

    - A 0.5 accuracy and 3:1 payout gives f* = 0.333 — capped at ``cap``.
    - A 0.25 accuracy and 3:1 payout gives 0 — we honour it and return 0.
    - NaN / infinite inputs return 0.
    - Callers treat 0 as "skip this ticket".

    This is the full-Kelly formula (not half-Kelly) because the cap is the
    risk governor.  The class method ``OptionsRecommender._compute_kelly``
    applies a half-Kelly shrink on top of this for its scanner pipeline;
    both live in this file so the reconciliation is explicit.
    """
    if payout_ratio <= 0 or not math.isfinite(payout_ratio):
        return 0.0
    if not math.isfinite(accuracy):
        return 0.0
    p = max(0.0, min(1.0, accuracy))
    q = 1.0 - p
    f_star = (p * payout_ratio - q) / payout_ratio
    if f_star <= 0:
        return 0.0
    return round(min(cap, f_star), 4)


# ALPHA-12 / task #115 — Kelly-with-error-bars + tail adjustment.
#
# The canonical compute_kelly_fraction above takes a point accuracy. Real
# predictions come with a confidence interval (ALPHA-11) — the lower bound
# is the conservative sizing input, and the width captures epistemic
# uncertainty we can't collapse by adding more data.
#
# The error-bar Kelly uses the LOWER bound of the CI (so sizing shrinks when
# the ensemble is split) AND applies a tail adjustment that shrinks Kelly
# further as the bound approaches the break-even accuracy (1 / (1 + payout)).
# The shrink is quadratic: predictions near the break-even get Kelly near 0
# even if the point estimate looks profitable.

_TAIL_BREAK_EVEN_BUFFER = 0.05  # How close to BE we start tail-shrinking


def compute_kelly_with_bounds(
    accuracy_lower: float,
    accuracy_upper: float,
    payout_ratio: float = DEFAULT_PAYOUT_RATIO,
    cap: float = MAX_KELLY_PER_TICKET,
) -> float:
    """Kelly sized on the lower CI bound with tail shrinkage.

    Parameters
    ----------
    accuracy_lower, accuracy_upper:
        Confidence interval bounds on the hit rate. Typically from
        ``oracle.uncertainty.compute_confidence_interval``.
    payout_ratio, cap:
        Same meaning as ``compute_kelly_fraction``.

    Behavior
    --------
    - Uses the LOWER bound as the primary Kelly input (conservative).
    - Computes a "tail factor" in ``[0, 1]`` based on how far the lower
      bound is from the break-even accuracy. When the lower bound is at
      break-even or below, tail_factor → 0 and Kelly → 0.
    - Wider intervals → more tail shrink — because a wide interval means
      we don't trust either bound.

    Returns the tail-adjusted, capped Kelly fraction in [0, cap].
    """
    if payout_ratio <= 0 or not math.isfinite(payout_ratio):
        return 0.0
    if not (math.isfinite(accuracy_lower) and math.isfinite(accuracy_upper)):
        return 0.0

    lower = max(0.0, min(1.0, accuracy_lower))
    upper = max(0.0, min(1.0, accuracy_upper))
    if upper < lower:
        lower, upper = upper, lower

    # Break-even accuracy where Kelly turns positive
    break_even = 1.0 / (1.0 + payout_ratio)

    # If even the LOWER bound is below break-even, skip the trade
    if lower <= break_even:
        return 0.0

    # Base Kelly from the lower bound
    base = compute_kelly_fraction(lower, payout_ratio=payout_ratio, cap=cap)
    if base <= 0:
        return 0.0

    # Tail factor — how much edge we have above break-even
    buffer = lower - break_even
    if buffer <= 0:
        tail_factor = 0.0
    else:
        # Quadratic ramp: full Kelly at 5pp above BE, zero at BE.
        tail_factor = min(1.0, (buffer / _TAIL_BREAK_EVEN_BUFFER) ** 2)

    # Width penalty — wider CI → less trust
    width = upper - lower
    width_factor = max(0.2, 1.0 - width)  # Never below 20% even if width is huge

    adjusted = base * tail_factor * width_factor
    return round(min(cap, max(0.0, adjusted)), 4)


def round_to_nickel(value: float) -> float:
    """Round a strike to a realistic listed increment.

    Rules of thumb: <$25 use $0.50, <$200 use $1, >=$200 use $5.  Good enough
    for defensibility — real chains get a closest-strike snap on the frontend.
    """
    if value <= 0 or not math.isfinite(value):
        return 0.0
    if value < 25:
        step = 0.5
    elif value < 200:
        step = 1.0
    else:
        step = 5.0
    return round(round(value / step) * step, 2)


# ── Null-safe display helpers ───────────────────────────────────────
#
# Every one of these renders a missing value as "n/a" rather than as 0,
# 0.0% or $0.00.  A report that prints "$0.00 target" for a trade whose
# target could not be computed is the same defect as the placeholder it
# replaced, only at the presentation layer.

def _fmt_money(value: float | None) -> str:
    return "n/a" if value is None else f"${value:.2f}"


def _fmt_pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1%}"


def _fmt_signed_pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value:+.0f}%"


def _fmt_count(value: int | None) -> str:
    return "n/a" if value is None else str(value)


def _fmt_ratio(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1f}x"


def _db_scalar(value: Any) -> Any:
    """Convert pandas/numpy scalar values before handing them to SQLAlchemy."""
    if value is None or isinstance(value, (str, bytes, dict, list, tuple)):
        return value
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    item = getattr(value, "item", None)
    if callable(item):
        try:
            value = item()
        except Exception:
            pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def pick_strike(
    spot: float,
    direction: str,
    gamma_context: dict[str, Any] | None,
    max_pain: float | None,
) -> float:
    """Choose the strike that acts as the price magnet.

    Canonical module-level strike picker used by contagion → ticket and
    any other pipeline that lacks a full options chain snapshot.  When a
    chain IS available, use ``OptionsRecommender._optimize_strike`` which
    additionally considers open interest and BS gamma.

    Direction is "short"/"long" (contagion convention) or "PUT"/"CALL"
    (scanner convention) — both are accepted.

    Priority order:
      1. put_wall / call_wall in the direction of the move
      2. gamma_wall (absolute max gamma strike)
      3. max_pain from options_daily_signals
      4. 2% OTM fallback based on spot
    """
    if spot <= 0:
        return 0.0

    is_short = direction.lower() in ("short", "put")

    candidates: list[float] = []
    if gamma_context:
        if is_short:
            wall = gamma_context.get("put_wall") or gamma_context.get("gamma_wall")
        else:
            wall = gamma_context.get("call_wall") or gamma_context.get("gamma_wall")
        if wall and wall > 0:
            candidates.append(float(wall))
    if max_pain and max_pain > 0:
        candidates.append(float(max_pain))

    for c in candidates:
        if is_short and c < spot * 1.02:
            return round_to_nickel(c)
        if (not is_short) and c > spot * 0.98:
            return round_to_nickel(c)

    offset = 0.02
    raw = spot * (1 - offset) if is_short else spot * (1 + offset)
    return round_to_nickel(raw)


def pick_expiry(
    simulated_at: datetime | None,
    margin_impact_pct: float,
    min_dte: int = MIN_DTE,
    scale: float = DTE_SCALE_BY_IMPACT,
) -> tuple[str, int]:
    """Select an expiry ISO date and DTE from a shock magnitude.

    The target DTE is ``max(min_dte, |margin_impact| × scale)``.  We then
    snap to the next Friday ≥ target — simple, deterministic, and close
    enough to a listed monthly cycle for a ticket card.

    When a full chain IS available, ``OptionsRecommender._pick_expiry``
    selects amongst actual listed expiries with OI gating.
    """
    from datetime import timezone as _tz

    base = simulated_at or datetime.now(_tz.utc)
    if isinstance(base, datetime):
        base_d = base.date()
    else:
        base_d = date.today()

    target_dte = max(int(min_dte), int(round(abs(margin_impact_pct) * scale)))
    target_dte = max(target_dte, 1)
    target_d = base_d + timedelta(days=target_dte)
    days_to_fri = (4 - target_d.weekday()) % 7
    expiry_d = target_d + timedelta(days=days_to_fri)
    actual_dte = (expiry_d - base_d).days
    return expiry_d.isoformat(), actual_dte


def estimate_premium(
    spot: float, iv_atm: float, dte: int
) -> tuple[float, float, float]:
    """Rough entry / target / stop premium estimates from a 1σ straddle.

    Not a substitute for a live chain quote — but a defensible starting
    point when the chain is missing.  One-sigma move over ``dte`` days at
    annualised ``iv_atm``.  Used by contagion → ticket and any other
    pipeline that doesn't have bid/ask snapshots.

    Callers MUST tag the resulting premiums ``premium_basis:
    PREMIUM_BASIS_MODELLED`` in whatever payload they emit — these are
    model output, not prices anyone quoted.  ``iv_atm`` must be a real
    measured ATM IV; passing a placeholder produces a placeholder premium
    with no way for the reader to tell.
    """
    if spot <= 0 or iv_atm <= 0 or dte <= 0:
        return 0.0, 0.0, 0.0
    sigma_move = spot * iv_atm * math.sqrt(max(dte, 1) / 365.0)
    entry = max(0.05, round(sigma_move * ESTIMATE_ENTRY_SIGMA_FRAC, 2))
    target = round(entry * ESTIMATE_TARGET_MULT, 2)
    stop = round(entry * ESTIMATE_STOP_MULT, 2)
    return entry, target, stop


def _split_signals_by_direction(
    signals: dict[str, Any] | None,
    trade_direction: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split a scanner signals dict into supporting / opposing buckets.

    Each signal is a `{score, direction?, value?}` triple keyed by name.
    Aligns the scanner's `direction` field ("bullish"/"bearish") against
    the trade's `direction` ("CALL"/"PUT") to decide which bucket. Signals
    without a direction or with score==0 are dropped — they didn't drive
    the decision and aren't useful in the postmortem.

    The output preserves the signal name and full payload so the
    postmortem can render contribution scores and raw values without a
    second DB lookup.
    """
    if not signals:
        return [], []

    bullish_dirs = {"bullish", "bull", "long", "up", "call"}
    bearish_dirs = {"bearish", "bear", "short", "down", "put"}
    supports = bullish_dirs if trade_direction == "CALL" else bearish_dirs
    opposes = bearish_dirs if trade_direction == "CALL" else bullish_dirs

    supporting: list[dict[str, Any]] = []
    opposing: list[dict[str, Any]] = []
    for name, payload in signals.items():
        if not isinstance(payload, dict):
            continue
        score = payload.get("score") or 0
        if not score:
            continue
        entry = {"name": name, **payload}
        sig_dir = str(payload.get("direction", "")).lower()
        if sig_dir in opposes:
            opposing.append(entry)
        else:
            supporting.append(entry)

    supporting.sort(key=lambda e: e.get("score") or 0, reverse=True)
    opposing.sort(key=lambda e: e.get("score") or 0, reverse=True)
    return supporting, opposing


# ── Recommendation dataclass ─────────────────────────────────────────


@dataclass
class OptionsRecommendation:
    """A complete, executable trade ticket.

    Every recommendation must contain enough information to execute
    the trade without any additional research. If you can't trade it
    directly from reading this, it's not complete enough.
    """

    # ── What to trade ──
    ticker: str
    direction: str                  # "CALL" or "PUT"
    strike: float                   # exact strike price
    expiry: str                     # ISO date (YYYY-MM-DD)

    # ── Entry ──
    entry_price: float              # bid-ask mid, last trade, or modelled price
    entry_bid: float | None = None  # bid at recommendation time (None if unquoted)
    entry_ask: float | None = None  # ask at recommendation time (None if unquoted)
    # How ``entry_price`` was obtained — never leave a modelled price
    # indistinguishable from a quoted one:
    #   "quote"      → mid of a real bid/ask in options_snapshots
    #   "last_trade" → snapshot row exists but only last_price was populated
    #   "model"      → no snapshot row; Black-Scholes at the echoed sigma/rate
    entry_price_basis: str = "unknown"
    entry_model_sigma: float | None = None  # sigma used when basis == "model"
    entry_model_rate: float | None = None   # r used when basis == "model"
    entry_snapshot_date: str | None = None  # snap_date the quote came from
    entry_by_date: str = ""        # enter by this date or cancel
    underlying_price: float = 0.0  # stock price at recommendation time

    # ── Exit targets ──
    # target/stop are None when the GEX profile that defines them is absent —
    # a 2x-entry target was never a measurement of anything (audit C-H8).
    target_price: float | None = None       # option target price (profit exit)
    target_return_pct: float | None = None  # expected % return on the option
    target_basis: str | None = None         # "gamma_wall" when derived, else None
    stop_loss: float | None = None          # option stop loss price
    stop_basis: str | None = None           # "gamma_flip" when derived, else None
    stop_loss_stock: float = 0.0   # stock price that invalidates (easier to watch)
    time_stop_date: str = ""       # exit by this date if thesis hasn't played out
    max_risk: float | None = None  # total dollars at risk across the position
    expected_return: float | None = None  # win_prob * gain - loss_prob * loss

    # ── Sizing ──
    # All three are None unless an empirical win probability exists. A null
    # here must never be read as 0 by a consumer that sorts or sizes on it.
    kelly_fraction: float | None = None    # Kelly-optimal fraction of portfolio
    suggested_contracts: int | None = None  # contracts at ``self.capital``
    max_portfolio_pct: float = 0.02  # never more than 2% of portfolio per trade

    # ── Win probability provenance (audit C-H6) ──
    win_probability: float | None = None   # realised hit rate for the score bucket
    win_probability_n: int = 0             # resolved trades behind it
    win_probability_basis: str = "unavailable"  # empirical_score_bucket | insufficient_history | query_failed
    scanner_score: float | None = None     # the score the bucket was keyed on

    # ── Thesis (lever + condition framework) ──
    confidence: float = 0.0        # 0-1 overall confidence
    thesis: str = ""               # complete thesis: lever + condition → outcome
    lever: str = ""                # CAUSE: who did what affecting which liquidity valve
    lever_actor: str = ""          # who is pulling the lever (Fed, whale, insider, etc.)
    lever_direction: str = ""      # opening or closing the valve
    conditions: list = field(default_factory=list)  # amplifiers/dampeners (NOT causes)
    catalyst: str = ""             # specific catalyst event (earnings, FOMC, flow)
    catalyst_date: str = ""        # when the catalyst happens
    whats_priced_in: str = ""      # what the market already expects
    whats_not_priced_in: str = ""  # the edge — what we see that others don't
    dealer_context: str = ""       # GEX/vanna/charm positioning

    # ── Invalidation ──
    invalidation: str = ""         # exact condition that kills the thesis
    invalidation_price: float = 0.0  # stock price that invalidates

    # ── Signals ──
    supporting_signals: list = field(default_factory=list)  # what signals agree
    opposing_signals: list = field(default_factory=list)     # what signals disagree
    signal_agreement_pct: float = 0.0  # % of signals that agree with direction

    # ── Meta ──
    sanity_status: dict = field(default_factory=dict)
    generated_at: str = ""
    model_version: str = "grid-options-v2"

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "direction": self.direction,
            "strike": self.strike,
            "expiry": self.expiry,
            "entry_price": self.entry_price,
            "entry_bid": self.entry_bid,
            "entry_ask": self.entry_ask,
            "entry_price_basis": self.entry_price_basis,
            "entry_model_sigma": self.entry_model_sigma,
            "entry_model_rate": self.entry_model_rate,
            "entry_snapshot_date": self.entry_snapshot_date,
            "entry_by_date": self.entry_by_date,
            "underlying_price": self.underlying_price,
            "target_price": self.target_price,
            "target_return_pct": self.target_return_pct,
            "target_basis": self.target_basis,
            "stop_loss": self.stop_loss,
            "stop_basis": self.stop_basis,
            "stop_loss_stock": self.stop_loss_stock,
            "time_stop_date": self.time_stop_date,
            "max_risk": self.max_risk,
            "kelly_fraction": self.kelly_fraction,
            "suggested_contracts": self.suggested_contracts,
            "win_probability": self.win_probability,
            "win_probability_n": self.win_probability_n,
            "win_probability_basis": self.win_probability_basis,
            "scanner_score": self.scanner_score,
            "confidence": self.confidence,
            "thesis": self.thesis,
            "lever": self.lever,
            "lever_actor": self.lever_actor,
            "lever_direction": self.lever_direction,
            "conditions": self.conditions,
            "catalyst": self.catalyst,
            "catalyst_date": self.catalyst_date,
            "whats_priced_in": self.whats_priced_in,
            "whats_not_priced_in": self.whats_not_priced_in,
            "dealer_context": self.dealer_context,
            "invalidation": self.invalidation,
            "invalidation_price": self.invalidation_price,
            "supporting_signals": self.supporting_signals,
            "opposing_signals": self.opposing_signals,
            "signal_agreement_pct": self.signal_agreement_pct,
            "sanity_status": self.sanity_status,
            "generated_at": self.generated_at,
            "model_version": self.model_version,
        }

    def to_trade_ticket(self) -> str:
        """Format as a human-readable trade ticket for alerts/email."""
        rr = self.risk_reward_ratio
        quote = (
            f"bid ${self.entry_bid:.2f} / ask ${self.entry_ask:.2f}"
            if self.entry_bid is not None and self.entry_ask is not None
            else f"basis {self.entry_price_basis}, unquoted"
        )
        return (
            f"{'═' * 50}\n"
            f"  {self.ticker} {self.strike}{self.direction[0]} {self.expiry}\n"
            f"{'═' * 50}\n"
            f"  ENTRY:  ${self.entry_price:.2f} ({quote})\n"
            f"  STOCK:  ${self.underlying_price:.2f} at time of rec\n"
            f"  TARGET: {_fmt_money(self.target_price)} ({_fmt_signed_pct(self.target_return_pct)})\n"
            f"  STOP:   {_fmt_money(self.stop_loss)} (or stock {'below' if self.direction == 'PUT' else 'above'} ${self.stop_loss_stock:.2f})\n"
            f"  TIME:   Exit by {self.time_stop_date} if no move\n"
            f"  SIZE:   {_fmt_count(self.suggested_contracts)} contracts ({_fmt_pct(self.kelly_fraction)} Kelly)\n"
            f"  R/R:    {_fmt_ratio(rr)} | Confidence: {self.confidence:.0%}\n"
            f"{'─' * 50}\n"
            f"  LEVER:  {self.lever}\n"
            f"  ACTOR:  {self.lever_actor} ({self.lever_direction})\n"
            f"  CONDITIONS: {'; '.join(self.conditions) if self.conditions else 'none identified'}\n"
            f"{'─' * 50}\n"
            f"  THESIS: {self.thesis}\n"
            f"  CATALYST: {self.catalyst} ({self.catalyst_date})\n"
            f"  EDGE: {self.whats_not_priced_in}\n"
            f"  INVALIDATION: {self.invalidation}\n"
            f"  DEALER: {self.dealer_context}\n"
            f"{'─' * 50}\n"
            f"  SIGNALS FOR:  {', '.join(self.supporting_signals[:3])}\n"
            f"  SIGNALS AGAINST: {', '.join(self.opposing_signals[:3])}\n"
            f"  AGREEMENT: {self.signal_agreement_pct:.0f}%\n"
            f"{'═' * 50}"
        )

    @property
    def risk_reward_ratio(self) -> float | None:
        """Target gain as a fraction of entry — ``None`` without a target."""
        if self.target_price is None or self.entry_price <= 0:
            return None
        return (self.target_price - self.entry_price) / self.entry_price

    @property
    def all_sanity_passed(self) -> bool:
        """True only if every sanity layer returned PASS or SKIP."""
        return all(
            v.get("status") in ("PASS", "SKIP")
            for v in self.sanity_status.values()
        )


# ── Weekly OpEx dates (third Friday of each month is monthly) ────────

def _third_friday(year: int, month: int) -> date:
    """Return the third Friday of a given month (standard monthly OpEx)."""
    first_day = date(year, month, 1)
    # weekday(): Monday=0 ... Friday=4
    days_until_friday = (4 - first_day.weekday()) % 7
    first_friday = first_day + timedelta(days=days_until_friday)
    return first_friday + timedelta(weeks=2)


def _is_monthly_opex(d: date) -> bool:
    """Check whether a date is a standard monthly options expiration (3rd Friday)."""
    return d == _third_friday(d.year, d.month)


def _is_weekly_opex(d: date) -> bool:
    """True if a Friday expiry is NOT the monthly — i.e. it's a weekly."""
    if d.weekday() != 4:  # not a Friday
        return False
    return not _is_monthly_opex(d)


# ── Recommendation Engine ────────────────────────────────────────────


class OptionsRecommender:
    """Generates actionable options trade recommendations.

    Combines:
    - options_scanner: mispricing signal detection (10 signals — incl. ALPHA-1 vol_surface, ALPHA-2 vanna+charm)
    - DealerGammaEngine: GEX regime, gamma flip, walls, vanna/charm
    - Kelly sizing: position sizing from win probability + payoff
    - 5-layer sanity pipeline: data quality, dealer flow, cross-asset,
      LLM review, historical analog

    Parameters:
        db_engine: SQLAlchemy engine for database access.
        min_score: Minimum scanner composite score to consider (default 6.0).
        capital: Notional capital for risk calculations.
        max_kelly: Cap on Kelly fraction (half-Kelly default).
    """

    # Sane defaults for strike optimization
    _CALL_OTM_MIN_PCT = 0.02   # at least 2% OTM
    _CALL_OTM_MAX_PCT = 0.12   # at most 12% OTM
    _PUT_OTM_MIN_PCT = 0.02
    _PUT_OTM_MAX_PCT = 0.12

    # Expiry preferences — adaptive to available data.
    # Prefer longer DTE when available but work with what the data provides.
    _MIN_DTE = 3     # minimum 3 days out (avoid expiry-day theta crush)
    _MAX_DTE = 120   # at most 4 months
    _IDEAL_DTE_LO = 14   # prefer 2-4 weeks (best gamma/theta balance)
    _IDEAL_DTE_HI = 45
    _EARNINGS_MIN_DTE = 3  # earnings plays can go very short
    _MIN_OI_FOR_EXPIRY = 500   # minimum open interest at expiry

    # Sanity thresholds
    _IV_MIN = 0.05
    _IV_MAX = 2.00
    _MIN_OI = 100
    _MAX_SPREAD_PCT = 0.25   # bid-ask spread < 25% of mid

    def __init__(
        self,
        db_engine: Engine,
        min_score: float = 1.5,
        capital: float = 10_000.0,
        max_kelly: float = 0.25,
    ) -> None:
        self.engine = db_engine
        self.min_score = min_score
        self.capital = capital
        self.max_kelly = max_kelly
        log.info(
            "OptionsRecommender initialised — min_score={s}, capital=${c:,.0f}",
            s=min_score, c=capital,
        )

    # ── Public API ───────────────────────────────────────────────────

    def generate_recommendations(
        self,
        engine: Engine | None = None,
        tickers: list[str] | None = None,
    ) -> list[OptionsRecommendation]:
        """Generate actionable options trade recommendations.

        Parameters:
            engine: Optional override SQLAlchemy engine (defaults to self.engine).
            tickers: Restrict scan to these tickers (default: all available).

        Returns:
            list[OptionsRecommendation]: Only recommendations passing all 5
            sanity layers, sorted by expected return descending.
        """
        db = engine or self.engine

        # 1. Run options scanner
        from discovery.options_scanner import OptionsScanner

        scanner = OptionsScanner(db_engine=db)
        opportunities = scanner.scan_all(tickers=tickers, min_score=self.min_score)

        log.info(
            "Scanner returned {n} opportunities (score >= {s})",
            n=len(opportunities), s=self.min_score,
        )

        if not opportunities:
            return []

        # 2. Load GEX engine
        from physics.dealer_gamma import DealerGammaEngine

        gex_engine = DealerGammaEngine(db_engine=db)

        recommendations: list[OptionsRecommendation] = []

        for opp in opportunities:
            try:
                rec = self._build_recommendation(opp, gex_engine, db)
                if rec is not None:
                    recommendations.append(rec)
            except Exception as exc:
                log.warning(
                    "Recommendation build failed for {t}: {e}",
                    t=opp.ticker, e=str(exc),
                )

        # Sort by expected return descending, unsized recommendations last.
        # An unknown expected return is NOT a zero: it must not outrank a
        # measured negative one, and it must not be treated as one either.
        recommendations.sort(
            key=lambda r: (
                r.expected_return is None,
                -(r.expected_return if r.expected_return is not None else 0.0),
            )
        )

        log.info(
            "Generated {n} actionable recommendations from {t} opportunities",
            n=len(recommendations), t=len(opportunities),
        )

        # Persist recommendations to database
        if recommendations:
            persisted = self._persist_recommendations(db, recommendations)
            log.info("Persisted {n} recommendations to options_recommendations", n=persisted)

        # Push each recommendation to connected WebSocket clients
        try:
            from api.main import broadcast_event
            for rec in recommendations:
                broadcast_event("recommendation", rec.to_dict())
        except Exception:
            pass  # graceful degradation if API module not loaded

        return recommendations

    def _persist_recommendations(
        self, db: Engine, recommendations: list[OptionsRecommendation]
    ) -> int:
        """Insert new recommendations into options_recommendations, skipping duplicates.

        Duplicates are identified by (ticker, strike, expiry).
        Returns the number of newly inserted rows.
        """
        inserted = 0
        with db.begin() as conn:
            for rec in recommendations:
                exists = conn.execute(
                    text(
                        "SELECT 1 FROM options_recommendations "
                        "WHERE ticker = :ticker AND strike = :strike AND expiry = :expiry "
                        "LIMIT 1"
                    ),
                    {"ticker": rec.ticker, "strike": _db_scalar(rec.strike), "expiry": rec.expiry},
                ).fetchone()
                if exists:
                    continue

                conn.execute(
                    text(
                        "INSERT INTO options_recommendations "
                        "(ticker, direction, strike, expiry, entry_price, target_price, "
                        "stop_loss, expected_return, kelly_fraction, confidence, thesis, "
                        "dealer_context, sanity_status, signals, opposing_signals, "
                        "scanner_score, generated_at) "
                        "VALUES (:ticker, :direction, :strike, :expiry, :entry_price, "
                        ":target_price, :stop_loss, :expected_return, :kelly_fraction, "
                        ":confidence, :thesis, :dealer_context, :sanity_status, "
                        ":signals, :opposing_signals, :scanner_score, :generated_at)"
                    ),
                    {
                        "ticker": rec.ticker,
                        "direction": rec.direction,
                        "strike": _db_scalar(rec.strike),
                        "expiry": rec.expiry,
                        "entry_price": _db_scalar(rec.entry_price),
                        "target_price": _db_scalar(rec.target_price),
                        "stop_loss": _db_scalar(rec.stop_loss),
                        "expected_return": _db_scalar(rec.expected_return),
                        "kelly_fraction": _db_scalar(rec.kelly_fraction),
                        "confidence": _db_scalar(rec.confidence),
                        # Persisted so the empirical win-rate lookup has a
                        # score to bucket future outcomes by (audit C-H6).
                        "scanner_score": _db_scalar(rec.scanner_score),
                        "thesis": rec.thesis,
                        "dealer_context": rec.dealer_context,
                        "sanity_status": json.dumps(rec.sanity_status),
                        "signals": json.dumps(rec.supporting_signals or [], default=str),
                        "opposing_signals": json.dumps(rec.opposing_signals or [], default=str),
                        "generated_at": rec.generated_at or datetime.utcnow().isoformat() + "Z",
                    },
                )
                inserted += 1
        return inserted

    # ── Core build logic ─────────────────────────────────────────────

    def _build_recommendation(
        self,
        opp,  # MispricingOpportunity from options_scanner
        gex_engine,  # DealerGammaEngine
        db: Engine,
    ) -> OptionsRecommendation | None:
        """Build a single recommendation from a scanner opportunity.

        Returns None if the recommendation fails any required sanity layer.
        """
        ticker = opp.ticker
        direction = opp.direction
        spot = opp.spot_price

        if not spot or spot <= 0:
            log.debug("Skipping {t}: no spot price", t=ticker)
            return None

        # Get GEX profile
        gex_profile = gex_engine.compute_gex_profile(ticker)
        if "error" in gex_profile:
            log.debug("Skipping {t}: GEX error — {e}", t=ticker, e=gex_profile["error"])
            # Continue without GEX — graceful degradation
            gex_profile = {}

        # Load options chain for strike/expiry selection
        chain_df = self._load_chain(db, ticker)
        if chain_df.empty:
            log.debug("Skipping {t}: no options chain data", t=ticker)
            return None

        # Optimize strike
        strike = self._optimize_strike(chain_df, direction, spot, gex_profile)
        if strike is None:
            log.debug("Skipping {t}: no suitable strike", t=ticker)
            return None

        # Pick expiry
        expiry = self._pick_expiry(chain_df, spot)
        if expiry is None:
            log.debug("Skipping {t}: no suitable expiry", t=ticker)
            return None

        # Compute entry price — quoted mid where one exists, otherwise a
        # model price that says so.
        entry = self._get_entry_price(db, ticker, strike, expiry, direction)
        if entry is None or entry["price"] is None or entry["price"] <= 0:
            log.debug("Skipping {t}: no valid entry price at K={k}", t=ticker, k=strike)
            return None
        entry_price = float(entry["price"])

        # Target from GEX expected move (gamma wall distance).  ``None`` when
        # no GEX profile defines one — audit C-H8.
        target_price, target_basis = self._compute_target_price(
            entry_price, spot, strike, direction, gex_profile,
        )

        # Stop loss from gamma flip point — ``None`` when there is no flip.
        stop_loss, stop_basis = self._compute_stop_loss(
            entry_price, spot, strike, direction, gex_profile,
        )

        # Win probability read from realised outcomes, or None (audit C-H6).
        win_prob, win_prob_n, win_prob_basis = self._empirical_win_probability(
            db, opp.score,
        )

        # Payoff ratio needs BOTH exit levels; without them there is no
        # Kelly input and therefore no size.
        risk_per_contract = (
            entry_price - stop_loss if stop_loss is not None else None
        )
        if (
            target_price is not None
            and risk_per_contract is not None
            and risk_per_contract > 0
        ):
            payoff_ratio = (target_price - entry_price) / risk_per_contract
        else:
            payoff_ratio = None

        # Sizing is null unless every input is real.  A null must never be
        # silently read as 0 or 0.5 downstream — it means "not sized".
        kelly_fraction: float | None = None
        expected_return: float | None = None
        suggested_contracts: int | None = None
        max_risk: float | None = None
        if win_prob is not None and payoff_ratio is not None and payoff_ratio > 0:
            kelly_fraction = self._compute_kelly(win_prob, payoff_ratio)
            expected_return = (
                win_prob * (target_price - entry_price)
                - (1 - win_prob) * risk_per_contract
            )
            suggested_contracts = (
                max(1, int(self.capital * kelly_fraction / (entry_price * 100)))
                if kelly_fraction > 0
                else 0
            )
            max_risk = suggested_contracts * entry_price * 100

        target_return_pct = (
            (target_price / entry_price - 1.0) * 100.0
            if target_price is not None
            else None
        )

        # Confidence from scanner score + GEX alignment
        confidence = self._compute_confidence(opp, gex_profile, direction)

        # Dealer context string
        dealer_context = self._format_dealer_context(gex_profile, direction)

        # Thesis from scanner
        thesis = opp.thesis

        # Decompose the scanner signal payload into supporting / opposing
        # buckets so the postmortem can attribute failures to specific
        # contributors. opp.signals is a free-form dict from the scanner;
        # we treat the items as supporting evidence and only flag entries
        # whose stored direction explicitly contradicts ours as opposing.
        supporting, opposing = _split_signals_by_direction(opp.signals, direction)

        # Build preliminary recommendation
        rec = OptionsRecommendation(
            ticker=ticker,
            direction=direction,
            strike=round(strike, 2),
            expiry=str(expiry),
            entry_price=round(entry_price, 4),
            entry_bid=entry["bid"],
            entry_ask=entry["ask"],
            entry_price_basis=entry["basis"],
            entry_model_sigma=entry["sigma"],
            entry_model_rate=entry["rate"],
            entry_snapshot_date=entry["snap_date"],
            underlying_price=round(float(spot), 4),
            target_price=round(target_price, 4) if target_price is not None else None,
            target_return_pct=(
                round(target_return_pct, 4) if target_return_pct is not None else None
            ),
            target_basis=target_basis,
            stop_loss=(
                round(max(stop_loss, 0.01), 4) if stop_loss is not None else None
            ),
            stop_basis=stop_basis,
            expected_return=(
                round(expected_return, 4) if expected_return is not None else None
            ),
            max_risk=round(max_risk, 2) if max_risk is not None else None,
            kelly_fraction=(
                round(kelly_fraction, 4) if kelly_fraction is not None else None
            ),
            suggested_contracts=suggested_contracts,
            win_probability=(
                round(win_prob, 4) if win_prob is not None else None
            ),
            win_probability_n=win_prob_n,
            win_probability_basis=win_prob_basis,
            scanner_score=float(opp.score) if opp.score is not None else None,
            confidence=round(confidence, 4),
            thesis=thesis,
            dealer_context=dealer_context,
            sanity_status={},
            supporting_signals=supporting,
            opposing_signals=opposing,
            generated_at=datetime.utcnow().isoformat() + "Z",
        )

        # Run 5-layer sanity pipeline
        sanity = self._run_sanity_pipeline(rec, db, opp, gex_profile)
        rec.sanity_status = sanity

        # Only return if all layers pass or skip
        if not rec.all_sanity_passed:
            failed = [
                k for k, v in sanity.items() if v.get("status") == "FAIL"
            ]
            log.info(
                "Recommendation rejected for {t}: failed layers={f}",
                t=ticker, f=failed,
            )
            return None

        return rec

    # ── Strike Optimization ──────────────────────────────────────────

    def _optimize_strike(
        self,
        chain_df: pd.DataFrame,
        direction: str,
        spot: float,
        gex_profile: dict,
    ) -> float | None:
        """Pick the strike with best gamma/premium ratio within direction.

        For CALL: strike above spot where call gamma is high but premium
        is reasonable. For PUT: strike below spot where put gamma is high.
        Avoids strikes beyond gamma wall (likely resistance/support).
        """
        from physics.dealer_gamma import bs_gamma

        call_wall = gex_profile.get("call_wall") or spot * 1.15
        put_wall = gex_profile.get("put_wall") or spot * 0.85

        if direction == "CALL":
            lo = spot * (1 + self._CALL_OTM_MIN_PCT)
            hi = min(spot * (1 + self._CALL_OTM_MAX_PCT), call_wall)
            opt_type = "call"
        else:
            hi = spot * (1 - self._PUT_OTM_MIN_PCT)
            lo = max(spot * (1 - self._PUT_OTM_MAX_PCT), put_wall)
            opt_type = "put"

        # Filter to relevant strikes and option type
        candidates = chain_df[
            (chain_df["strike"] >= lo)
            & (chain_df["strike"] <= hi)
            & (chain_df["opt_type"] == opt_type)
            & (chain_df["open_interest"] > self._MIN_OI)
        ].copy()

        if candidates.empty:
            # Relax OI filter
            candidates = chain_df[
                (chain_df["strike"] >= lo)
                & (chain_df["strike"] <= hi)
                & (chain_df["opt_type"] == opt_type)
            ].copy()

        if candidates.empty:
            return None

        # Score each strike: gamma / (implied cost proxy)
        # Higher gamma + lower IV = better value
        best_strike = None
        best_score = -1.0

        for _, row in candidates.iterrows():
            K = float(row["strike"])
            T = float(row["dte"]) / 365.0
            if T <= 0:
                continue
            iv = float(row["implied_volatility"]) if row["implied_volatility"] > 0 else 0.25
            oi = float(row["open_interest"])

            gamma = bs_gamma(spot, K, T, 0.05, iv)
            # Cost proxy: IV * sqrt(T) * spot (higher = more expensive)
            cost_proxy = iv * math.sqrt(T) * spot if iv > 0 else 1.0
            # OI liquidity bonus (log scale)
            oi_bonus = math.log1p(oi) / 10.0

            score = (gamma * spot * 100 / max(cost_proxy, 0.01)) + oi_bonus
            if score > best_score:
                best_score = score
                best_strike = K

        return best_strike

    # ── Expiry Selection ─────────────────────────────────────────────

    def _pick_expiry(
        self,
        chain_df: pd.DataFrame,
        spot: float,
        is_earnings_play: bool = False,
    ) -> date | None:
        """Pick optimal expiry: 2-4 months out, avoid weekly OpEx, prefer monthly.

        For earnings plays with huge disparity, can go as short as 2 weeks.
        Requires sufficient OI (>500 contracts) at the chosen expiry.
        """
        date.today()

        # Get distinct expiries with aggregate OI
        expiry_stats = (
            chain_df.groupby("expiry")
            .agg(total_oi=("open_interest", "sum"), dte=("dte", "first"))
            .reset_index()
        )

        # Earnings plays can use shorter DTE for event-driven trades
        min_dte = self._EARNINGS_MIN_DTE if is_earnings_play else self._MIN_DTE

        # Filter to acceptable DTE range
        expiry_stats = expiry_stats[
            (expiry_stats["dte"] >= min_dte)
            & (expiry_stats["dte"] <= self._MAX_DTE)
        ]

        if expiry_stats.empty:
            return None

        # Score each expiry
        best_expiry = None
        best_score = -1.0

        for _, row in expiry_stats.iterrows():
            exp = row["expiry"]
            dte = int(row["dte"])
            total_oi = float(row["total_oi"])

            # Parse expiry to date if needed
            if isinstance(exp, str):
                try:
                    exp_date = date.fromisoformat(exp)
                except ValueError:
                    continue
            elif isinstance(exp, date):
                exp_date = exp
            else:
                continue

            # OI threshold
            if total_oi < self._MIN_OI_FOR_EXPIRY:
                continue

            score = 0.0

            # Prefer ideal DTE range (3-4 weeks)
            if self._IDEAL_DTE_LO <= dte <= self._IDEAL_DTE_HI:
                score += 5.0
            elif self._MIN_DTE <= dte < self._IDEAL_DTE_LO:
                score += 3.0  # acceptable but shorter
            else:
                score += 2.0  # acceptable but longer

            # Prefer monthly OpEx over weekly (more liquidity, less dealer unwind noise)
            if _is_monthly_opex(exp_date):
                score += 3.0
            elif _is_weekly_opex(exp_date):
                score -= 1.0  # penalise weekly OpEx

            # OI liquidity bonus
            score += math.log1p(total_oi) / 5.0

            if score > best_score:
                best_score = score
                best_expiry = exp_date

        return best_expiry

    # ── Entry / Target / Stop ────────────────────────────────────────

    def _get_entry_price(
        self,
        db: Engine,
        ticker: str,
        strike: float,
        expiry: date,
        direction: str,
    ) -> dict[str, Any] | None:
        """Resolve the entry price AND how it was obtained.

        Returns a dict ``{price, bid, ask, basis, sigma, rate, snap_date}``
        or ``None`` when no price can be established at all.

        ``basis`` is one of:
          - ``"quote"``      — mid of a real bid/ask in ``options_snapshots``
          - ``"last_trade"`` — snapshot row exists but only ``last_price`` did
          - ``"model"``      — no snapshot row; Black-Scholes at ``sigma``/``rate``

        Audit C-H7: the old version returned a bare float, so a
        Black-Scholes price computed at a hardcoded σ=0.25 was served under
        the same ``entry_price`` field as a live bid/ask mid and nothing in
        the payload distinguished them.
        """
        opt_type = "call" if direction == "CALL" else "put"

        with db.connect() as conn:
            row = conn.execute(text("""
                SELECT bid, ask, last_price, snap_date
                FROM options_snapshots
                WHERE ticker = :ticker
                  AND strike = :strike
                  AND expiry = :expiry
                  AND opt_type = :opt_type
                ORDER BY snap_date DESC
                LIMIT 1
            """), {
                "ticker": ticker,
                "strike": strike,
                "expiry": expiry,
                "opt_type": opt_type,
            }).fetchone()

        if row is None:
            # No snapshot at all — price it, and say that we priced it.
            return self._estimate_premium(db, ticker, strike, expiry, direction)

        bid = float(row[0]) if row[0] and row[0] > 0 else None
        ask = float(row[1]) if row[1] and row[1] > 0 else None
        last = float(row[2]) if row[2] and row[2] > 0 else None
        snap_date = str(row[3]) if len(row) > 3 and row[3] else None

        if bid is not None and ask is not None:
            return {
                "price": (bid + ask) / 2.0,
                "bid": bid,
                "ask": ask,
                "basis": "quote",
                "sigma": None,
                "rate": None,
                "snap_date": snap_date,
            }
        if last is not None:
            # A real trade print, but not a two-sided quote: say which.
            return {
                "price": last,
                "bid": bid,
                "ask": ask,
                "basis": "last_trade",
                "sigma": None,
                "rate": None,
                "snap_date": snap_date,
            }
        return None

    def _estimate_premium(
        self,
        db: Engine,
        ticker: str,
        strike: float,
        expiry: date,
        direction: str,
    ) -> dict[str, Any] | None:
        """Black-Scholes premium when no snapshot exists, with its inputs.

        Returns ``None`` when the model's own inputs are missing — there is
        no default sigma.  The previous ``sigma = self._get_atm_iv(...) or
        0.25`` (audit C-H7) meant a ticker with no measured ATM IV was
        priced off a 25% constant and the result was published as
        ``entry_price`` with nothing marking it as invented.
        """
        from physics.dealer_gamma import _d1, _d2
        from scipy.stats import norm

        spot = self._get_spot(db, ticker)
        if spot <= 0:
            log.debug("No model premium for {t}: no spot price", t=ticker)
            return None

        sigma = self._get_atm_iv(db, ticker)
        if sigma is None or sigma <= 0:
            log.debug(
                "No model premium for {t}: no measured ATM IV to price with",
                t=ticker,
            )
            return None

        T = max((expiry - date.today()).days, 1) / 365.0
        r = MODEL_RISK_FREE_RATE

        d1 = _d1(spot, strike, T, r, sigma)
        d2 = _d2(spot, strike, T, r, sigma)

        if direction == "CALL":
            price = spot * norm.cdf(d1) - strike * math.exp(-r * T) * norm.cdf(d2)
        else:
            price = strike * math.exp(-r * T) * norm.cdf(-d2) - spot * norm.cdf(-d1)

        return {
            "price": max(float(price), 0.01),
            "bid": None,
            "ask": None,
            "basis": "model",
            "sigma": float(sigma),
            "rate": r,
            "snap_date": None,
        }

    def _compute_target_price(
        self,
        entry_price: float,
        spot: float,
        strike: float,
        direction: str,
        gex_profile: dict,
    ) -> tuple[float | None, str | None]:
        """Set target from GEX expected move (gamma wall distance).

        Returns ``(target, basis)``.  Without a gamma wall in the trade's
        direction there is no expected move to target, so the answer is
        ``(None, None)``.

        Audit C-H8: this used to return ``entry_price * 2.0`` whenever the
        GEX profile was missing — a doubling constant published as
        ``target_price``, and then as ``target_return_pct`` (+100%) and as
        the gain leg of ``expected_return``.  The docstring said the target
        came from the GEX expected move; for every ticker without a GEX
        profile it came from the number 2.
        """
        if not gex_profile:
            return None, None

        if direction == "CALL":
            call_wall = gex_profile.get("call_wall") or 0
            if call_wall and call_wall > spot:
                # Expected move: spot -> call wall
                move_pct = (call_wall - spot) / spot
                # Option delta ~0.3 for OTM call; option moves ~ delta * underlying move
                # But with gamma acceleration, the option move is amplified
                option_move_mult = max(1.5, move_pct / max(entry_price / spot, 0.001))
                return entry_price * (1 + option_move_mult), "gamma_call_wall"
        else:
            put_wall = gex_profile.get("put_wall") or 0
            if put_wall and put_wall < spot:
                move_pct = (spot - put_wall) / spot
                option_move_mult = max(1.5, move_pct / max(entry_price / spot, 0.001))
                return entry_price * (1 + option_move_mult), "gamma_put_wall"

        # A GEX profile exists but has no wall on our side — still nothing
        # measured to target.
        return None, None

    def _compute_stop_loss(
        self,
        entry_price: float,
        spot: float,
        strike: float,
        direction: str,
        gex_profile: dict,
    ) -> tuple[float | None, str | None]:
        """Set stop from gamma flip point.

        Returns ``(stop, basis)``.  The gamma flip is what defines the stop:
        when spot crosses it, dealer flows reverse and the thesis breaks.
        With no flip level there is no derived stop, so the answer is
        ``(None, None)`` rather than a 50%-of-entry constant — that constant
        fed ``payoff_ratio`` and therefore the Kelly size.
        """
        if not gex_profile:
            return None, None

        gamma_flip = gex_profile.get("gamma_flip")
        if gamma_flip is None:
            return None, None

        if direction == "CALL":
            # For calls: if spot drops below gamma flip, thesis is broken
            if gamma_flip < spot:
                # Estimate how much the option loses if spot drops to gamma flip
                drop_pct = (spot - gamma_flip) / spot
                # Option loses roughly delta * drop_pct * spot
                option_loss_pct = min(0.70, drop_pct * 3.0)  # amplified by leverage
                return entry_price * (1 - option_loss_pct), "gamma_flip"
        else:
            # For puts: if spot rallies above gamma flip, thesis breaks
            if gamma_flip > spot:
                rally_pct = (gamma_flip - spot) / spot
                option_loss_pct = min(0.70, rally_pct * 3.0)
                return entry_price * (1 - option_loss_pct), "gamma_flip"

        # Flip sits on the wrong side of spot — it does not define a stop here.
        return None, None

    # ── Kelly and Probability ────────────────────────────────────────

    def _empirical_win_probability(
        self, db: Engine, score: float | None,
    ) -> tuple[float | None, int, str]:
        """Realised win rate for this candidate's scanner-score bucket.

        Returns ``(probability, n, basis)``:

        - ``("empirical_score_bucket")`` — at least ``WIN_PROB_MIN_SAMPLE``
          resolved (WIN/LOSS) recommendations were generated at a scanner
          score inside the same ``WIN_PROB_SCORE_BUCKET``-wide bucket, and
          the probability is wins/resolved for that bucket.
        - ``(None, n, "insufficient_history")`` — fewer than the minimum;
          ``n`` says how many there were so the reader can see the gap.
        - ``(None, 0, "no_score")`` / ``"query_failed"`` — we cannot look.

        This replaces the affine score→probability map flagged as audit
        C-H6.  That map was a tuning constant: no part of it was ever
        checked against an outcome, yet it fed ``kelly_fraction``,
        ``suggested_contracts`` and ``expected_return`` — i.e. it decided
        position size.
        """
        if score is None:
            return None, 0, "no_score"

        bucket = max(WIN_PROB_SCORE_BUCKET, 0.01)
        lo = math.floor(float(score) / bucket) * bucket
        hi = lo + bucket

        try:
            with db.connect() as conn:
                row = conn.execute(text("""
                    SELECT
                        COUNT(*) FILTER (WHERE outcome = 'WIN') AS wins,
                        COUNT(*) AS resolved
                    FROM options_recommendations
                    WHERE outcome IN ('WIN', 'LOSS')
                      AND scanner_score IS NOT NULL
                      AND scanner_score >= :lo
                      AND scanner_score < :hi
                """), {"lo": lo, "hi": hi}).fetchone()
        except Exception as exc:  # noqa: BLE001 — any DB failure means "unknown"
            # A failed lookup is not a low win rate and it is certainly not
            # an average one. It is an absence, and it is reported as one.
            log.debug("win-probability history query failed: {e}", e=str(exc))
            return None, 0, "query_failed"

        if row is None:
            return None, 0, "insufficient_history"

        wins = int(row[0] or 0)
        resolved = int(row[1] or 0)
        if resolved < WIN_PROB_MIN_SAMPLE:
            return None, resolved, "insufficient_history"

        return wins / resolved, resolved, "empirical_score_bucket"

    def _compute_kelly(self, win_prob: float, payoff_ratio: float) -> float:
        """Kelly criterion with half-Kelly shrink, capped at ``self.max_kelly``.

        Delegates the raw Kelly math to the canonical module-level
        ``compute_kelly_fraction`` and then applies a half-Kelly shrink on
        top (scanner pipeline is more aggressive than contagion — it has
        the full sanity pipeline behind it).  Follows the pattern from
        ``trading/paper_engine.py``.
        """
        if payoff_ratio <= 0 or win_prob <= 0:
            return 0.0
        # Use the canonical full-Kelly, then shrink to half-Kelly and cap.
        full_kelly = compute_kelly_fraction(
            accuracy=win_prob,
            payout_ratio=payoff_ratio,
            cap=1.0,  # no cap here — caller applies max_kelly below
        )
        half_kelly = full_kelly / 2.0
        return max(0.0, min(half_kelly, self.max_kelly))

    def _compute_confidence(
        self, opp, gex_profile: dict, direction: str,
    ) -> float:
        """Compute 0-1 confidence from scanner score + GEX alignment."""
        # Base: normalize scanner score to 0-1
        base = opp.score / 10.0

        # GEX alignment bonus
        regime = gex_profile.get("regime", "")
        if regime == "SHORT_GAMMA":
            base += 0.05  # amplification regime supports directional bets
        elif regime == "NEUTRAL":
            pass
        elif regime == "LONG_GAMMA":
            base -= 0.05  # dampening works against us

        # Confidence level from scanner
        if opp.confidence == "HIGH":
            base += 0.10
        elif opp.confidence == "MEDIUM":
            base += 0.05

        return max(0.0, min(1.0, base))

    # ── 5-Layer Sanity Pipeline ──────────────────────────────────────

    def _run_sanity_pipeline(
        self,
        rec: OptionsRecommendation,
        db: Engine,
        opp,
        gex_profile: dict,
    ) -> dict:
        """Run 5-layer sanity check on a recommendation.

        Layers:
          1. DATA_QUALITY: IV in 5-200%, OI > 100, spread < 25% of mid
          2. DEALER_FLOW: GEX regime supports direction
          3. CROSS_ASSET: momentum + news energy aligned with direction
          4. LLM_REVIEW: structured prompt to LLM, graceful SKIP if unavailable
          5. HISTORICAL_ANALOG: query past scanner results with similar setups

        Each returns {status: "PASS"/"FAIL"/"SKIP", reason: "..."}
        """
        sanity: dict[str, dict[str, str]] = {}

        sanity["DATA_QUALITY"] = self._sanity_data_quality(rec, db)
        sanity["DEALER_FLOW"] = self._sanity_dealer_flow(rec, gex_profile)
        sanity["CROSS_ASSET"] = self._sanity_cross_asset(rec, db)
        sanity["LLM_REVIEW"] = self._sanity_llm_review(rec, opp, gex_profile)
        sanity["HISTORICAL_ANALOG"] = self._sanity_historical_analog(rec, db)

        passed = sum(1 for v in sanity.values() if v["status"] == "PASS")
        skipped = sum(1 for v in sanity.values() if v["status"] == "SKIP")
        failed = sum(1 for v in sanity.values() if v["status"] == "FAIL")
        log.info(
            "Sanity pipeline for {t} {d}: {p} PASS, {s} SKIP, {f} FAIL",
            t=rec.ticker, d=rec.direction, p=passed, s=skipped, f=failed,
        )

        return sanity

    def _sanity_data_quality(
        self, rec: OptionsRecommendation, db: Engine,
    ) -> dict[str, str]:
        """Layer 1: Data quality checks.

        - IV in 5-200%
        - OI > 100
        - Spread < 25% of mid
        """
        opt_type = "call" if rec.direction == "CALL" else "put"

        with db.connect() as conn:
            row = conn.execute(text("""
                SELECT implied_vol, open_interest, bid, ask
                FROM options_snapshots
                WHERE ticker = :ticker
                  AND strike = :strike
                  AND opt_type = :opt_type
                ORDER BY snap_date DESC
                LIMIT 1
            """), {
                "ticker": rec.ticker,
                "strike": rec.strike,
                "opt_type": opt_type,
            }).fetchone()

        if row is None:
            return {
                "status": "SKIP",
                "reason": (
                    "No options_snapshots row for this strike — IV/OI/spread "
                    f"unchecked; entry_price_basis={rec.entry_price_basis}"
                ),
            }

        iv = float(row[0]) if row[0] else 0
        oi = int(row[1]) if row[1] else 0
        bid = float(row[2]) if row[2] else 0
        ask = float(row[3]) if row[3] else 0

        issues = []

        if iv < self._IV_MIN or iv > self._IV_MAX:
            issues.append(f"IV {iv:.1%} outside sane range ({self._IV_MIN:.0%}-{self._IV_MAX:.0%})")

        if oi < self._MIN_OI:
            issues.append(f"OI {oi} below minimum {self._MIN_OI}")

        if bid > 0 and ask > 0:
            mid = (bid + ask) / 2.0
            spread_pct = (ask - bid) / mid if mid > 0 else 1.0
            if spread_pct > self._MAX_SPREAD_PCT:
                issues.append(f"Spread {spread_pct:.1%} exceeds {self._MAX_SPREAD_PCT:.0%} of mid")

        if issues:
            return {"status": "FAIL", "reason": "; ".join(issues)}

        return {"status": "PASS", "reason": "IV, OI, and spread within acceptable bounds"}

    def _sanity_dealer_flow(
        self, rec: OptionsRecommendation, gex_profile: dict,
    ) -> dict[str, str]:
        """Layer 2: Dealer flow alignment.

        GEX regime should support our direction:
        - SHORT_GAMMA amplifies moves -> supports directional bets (PASS)
        - LONG_GAMMA dampens moves -> works against directional bets (FAIL for high-conviction only)
        - NEUTRAL -> no strong signal (PASS)
        """
        if not gex_profile or "regime" not in gex_profile:
            return {"status": "SKIP", "reason": "No GEX data available"}

        regime = gex_profile["regime"]
        direction = rec.direction

        if regime == "SHORT_GAMMA":
            return {
                "status": "PASS",
                "reason": f"Short gamma regime amplifies {direction} directional move",
            }

        if regime == "LONG_GAMMA":
            # Long gamma dampens, but doesn't prevent moves; warn but pass
            # unless confidence is very low
            if rec.confidence < 0.4:
                return {
                    "status": "FAIL",
                    "reason": f"Long gamma dampens moves + low confidence ({rec.confidence:.1%})",
                }
            return {
                "status": "PASS",
                "reason": f"Long gamma dampens moves, but confidence ({rec.confidence:.1%}) is adequate",
            }

        # NEUTRAL
        return {"status": "PASS", "reason": "Neutral gamma regime — no strong headwind or tailwind"}

    def _sanity_cross_asset(
        self, rec: OptionsRecommendation, db: Engine,
    ) -> dict[str, str]:
        """Layer 3: Cross-asset alignment (momentum + news energy).

        Checks recent price momentum and decision journal sentiment
        to see if they align with the recommended direction.
        """
        try:
            with db.connect() as conn:
                # Price momentum: 5-day return
                feature_name = f"{rec.ticker.lower()}_close"
                prices = conn.execute(text("""
                    SELECT rs.value
                    FROM resolved_series rs
                    JOIN feature_registry fr ON rs.feature_id = fr.id
                    WHERE fr.name = :fname
                    ORDER BY rs.obs_date DESC
                    LIMIT 6
                """), {"fname": feature_name}).fetchall()

                if len(prices) >= 2:
                    latest = float(prices[0][0])
                    older = float(prices[-1][0])
                    if older > 0:
                        momentum = (latest - older) / older
                    else:
                        momentum = 0.0
                else:
                    momentum = 0.0

                # News energy from decision journal (latest regime)
                journal_row = conn.execute(text("""
                    SELECT inferred_state, grid_recommendation
                    FROM decision_journal
                    ORDER BY decision_timestamp DESC
                    LIMIT 1
                """)).fetchone()

                news_aligned = True  # default: no contradiction
                if journal_row:
                    state = journal_row[0] or ""
                    # CRISIS / FRAGILE state + CALL direction = misalignment
                    if state in ("CRISIS", "FRAGILE") and rec.direction == "CALL":
                        news_aligned = False
                    elif state == "GROWTH" and rec.direction == "PUT":
                        news_aligned = False

            # Evaluate
            if rec.direction == "CALL":
                momentum_aligned = momentum >= -0.02  # not in sharp decline
            else:
                momentum_aligned = momentum <= 0.02   # not in sharp rally

            if momentum_aligned and news_aligned:
                return {
                    "status": "PASS",
                    "reason": f"Momentum ({momentum:+.2%}) and regime aligned with {rec.direction}",
                }
            elif not momentum_aligned and not news_aligned:
                return {
                    "status": "FAIL",
                    "reason": f"Momentum ({momentum:+.2%}) and regime both contradict {rec.direction}",
                }
            else:
                # One aligned, one not — pass with warning
                return {
                    "status": "PASS",
                    "reason": f"Partial alignment — momentum={momentum:+.2%}, news_aligned={news_aligned}",
                }

        except Exception as exc:
            log.debug("Cross-asset check failed: {e}", e=str(exc))
            return {"status": "SKIP", "reason": f"Cross-asset data unavailable: {str(exc)[:80]}"}

    def _sanity_llm_review(
        self,
        rec: OptionsRecommendation,
        opp,
        gex_profile: dict,
    ) -> dict[str, str]:
        """Layer 4: LLM structured review.

        Sends all context to local LLM for PASS/FAIL judgment.
        Graceful degradation: returns SKIP if LLM is unavailable.
        """
        try:
            import requests as req

            # Check if LLM is available
            from config import settings
            props = req.get(f"{settings.LLAMACPP_BASE_URL}/props", timeout=3)
            if props.status_code != 200:
                return {"status": "SKIP", "reason": "LLM not available for review"}
        except Exception:
            return {"status": "SKIP", "reason": "LLM not available for review"}

        prompt = f"""You are a senior options strategist reviewing an automated trade recommendation.
Respond with ONLY a JSON object: {{"verdict": "PASS"|"FAIL", "reason": "one sentence"}}

PASS = plausible trade setup worth executing
FAIL = data quality issue, incoherent thesis, or fundamentally flawed logic

Recommendation:
- Ticker: {rec.ticker} {rec.direction}
- Strike: ${rec.strike:.2f}, Expiry: {rec.expiry}
- Entry: ${rec.entry_price:.4f} (basis: {rec.entry_price_basis}), Target: {_fmt_money(rec.target_price)}, Stop: {_fmt_money(rec.stop_loss)}
- Expected Return: {_fmt_money(rec.expected_return)}, Max Risk: {_fmt_money(rec.max_risk)}
- Kelly Fraction: {_fmt_pct(rec.kelly_fraction)}
- Win probability: {_fmt_pct(rec.win_probability)} (n={rec.win_probability_n}, basis={rec.win_probability_basis})
- Confidence: {rec.confidence:.1%}
- Thesis: {rec.thesis}
- Dealer Context: {rec.dealer_context}
- Scanner Score: {opp.score:.1f}/10

Rules for your review:
- Entry price near zero or negative → FAIL
- Target < entry → FAIL (no upside)
- Kelly fraction > 20% → FAIL (oversized)
- Confidence < 20% with Kelly > 10% → FAIL (overbet on low conviction)
- Thesis makes no logical sense → FAIL
- Everything else → PASS
- "n/a" means the value could not be measured. That is an acceptable
  state, NOT a reason to FAIL — judge the thesis and the data quality.

Respond with ONLY the JSON object."""

        try:
            resp = req.post(
                f"{settings.LLAMACPP_BASE_URL}/v1/chat/completions",
                json={
                    "model": "default",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.1,
                    "max_tokens": 200,
                },
                timeout=30,
            )
            if resp.status_code == 200:
                content = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
                import re
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    verdict = json.loads(json_match.group())
                    status = verdict.get("verdict", "SKIP")
                    reason = verdict.get("reason", "No reason provided")
                    if status in ("PASS", "FAIL"):
                        return {"status": status, "reason": reason}

            return {"status": "SKIP", "reason": "LLM response unparseable"}

        except Exception as exc:
            log.debug("LLM review failed: {e}", e=str(exc))
            return {"status": "SKIP", "reason": f"LLM review error: {str(exc)[:60]}"}

    def _sanity_historical_analog(
        self, rec: OptionsRecommendation, db: Engine,
    ) -> dict[str, str]:
        """Layer 5: Historical analog check.

        Query past scanner results with similar setups and check outcomes.
        Similar = same ticker + same direction + score within 1.5 points.

        The score comes from ``rec.scanner_score`` — the actual scanner
        output the recommendation was built from.  It used to be
        back-derived as ``rec.confidence * 10``, which silently made the
        analog window a function of the confidence heuristic rather than
        of the score the analogs were themselves stored under.
        """
        if rec.scanner_score is None:
            return {
                "status": "SKIP",
                "reason": "No scanner score on the recommendation to match analogs by",
            }
        score = float(rec.scanner_score)
        try:
            with db.connect() as conn:
                # Check if mispricing scans table exists and has data
                rows = conn.execute(text("""
                    SELECT score, payoff_multiple, direction, confidence, scan_date
                    FROM options_mispricing_scans
                    WHERE ticker = :ticker
                      AND direction = :direction
                      AND score >= :lo_score
                      AND score <= :hi_score
                    ORDER BY scan_date DESC
                    LIMIT 20
                """), {
                    "ticker": rec.ticker,
                    "direction": rec.direction,
                    "lo_score": score - 1.5,
                    "hi_score": score + 1.5,
                }).fetchall()

            if not rows:
                return {"status": "SKIP", "reason": "No historical analogs found for this setup"}

            # Check if past similar setups had reasonable outcomes
            n = len(rows)
            avg_score = sum(float(r[0]) for r in rows) / n
            high_confidence = sum(1 for r in rows if r[3] in ("HIGH", "MEDIUM"))

            if n >= 5 and high_confidence / n >= 0.5:
                return {
                    "status": "PASS",
                    "reason": f"{n} historical analogs found, {high_confidence}/{n} were medium/high confidence (avg score {avg_score:.1f})",
                }
            elif n >= 3:
                return {
                    "status": "PASS",
                    "reason": f"{n} analogs found (avg score {avg_score:.1f}) — limited history",
                }
            else:
                return {
                    "status": "SKIP",
                    "reason": f"Only {n} analog(s) — insufficient for pattern matching",
                }

        except Exception as exc:
            log.debug("Historical analog check failed: {e}", e=str(exc))
            return {"status": "SKIP", "reason": f"Historical data unavailable: {str(exc)[:80]}"}

    # ── Dealer Context Formatting ────────────────────────────────────

    def _format_dealer_context(self, gex_profile: dict, direction: str) -> str:
        """Build human-readable dealer context string from GEX profile."""
        if not gex_profile:
            return "GEX data unavailable"

        parts = []

        regime = gex_profile.get("regime", "UNKNOWN")
        parts.append(f"Regime: {regime}")

        gex = gex_profile.get("gex_aggregate", 0)
        parts.append(f"GEX: {gex:,.0f}")

        gamma_flip = gex_profile.get("gamma_flip")
        if gamma_flip:
            parts.append(f"Gamma flip: ${gamma_flip:,.2f}")

        gex_profile.get("spot", 0)
        call_wall = gex_profile.get("call_wall")
        put_wall = gex_profile.get("put_wall")
        if call_wall:
            parts.append(f"Call wall: ${call_wall:,.2f}")
        if put_wall:
            parts.append(f"Put wall: ${put_wall:,.2f}")

        vanna = gex_profile.get("vanna_exposure", 0)
        charm = gex_profile.get("charm_exposure", 0)
        if vanna != 0:
            parts.append(f"Vanna: {vanna:,.0f}")
        if charm != 0:
            parts.append(f"Charm: {charm:,.0f}")

        # Interpretation
        if regime == "SHORT_GAMMA":
            parts.append(f"Dealers are short gamma — will amplify {direction} move")
        elif regime == "LONG_GAMMA":
            parts.append("Dealers are long gamma — will dampen moves")

        return " | ".join(parts)

    # ── Database Helpers ─────────────────────────────────────────────

    def _load_chain(self, db: Engine, ticker: str) -> pd.DataFrame:
        """Load options chain from database (latest snap_date)."""
        with db.connect() as conn:
            rows = conn.execute(text("""
                SELECT strike, opt_type, open_interest, implied_vol AS implied_volatility,
                       expiry, (expiry - snap_date) AS dte, bid, ask
                FROM options_snapshots
                WHERE ticker = :ticker
                  AND snap_date = (
                      SELECT MAX(snap_date) FROM options_snapshots WHERE ticker = :ticker
                  )
                  AND open_interest > 0
                  AND expiry > CURRENT_DATE
                ORDER BY expiry, strike
            """), {"ticker": ticker}).fetchall()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=[
            "strike", "opt_type", "open_interest", "implied_volatility",
            "expiry", "dte", "bid", "ask",
        ])
        df["dte"] = df["dte"].apply(lambda x: x.days if hasattr(x, "days") else int(x))
        return df[df["dte"] > 0]

    def _get_spot(self, db: Engine, ticker: str) -> float:
        """Get latest spot price for a ticker."""
        with db.connect() as conn:
            row = conn.execute(text("""
                SELECT rs.value
                FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name = :fname OR fr.name = :fname2
                ORDER BY rs.obs_date DESC
                LIMIT 1
            """), {
                "fname": f"{ticker.lower()}_close",
                "fname2": ticker.lower(),
            }).fetchone()
        return float(row[0]) if row else 0.0

    def _get_atm_iv(self, db: Engine, ticker: str) -> float | None:
        """Get ATM implied volatility from latest options signals."""
        with db.connect() as conn:
            row = conn.execute(text("""
                SELECT iv_atm
                FROM options_daily_signals
                WHERE ticker = :ticker
                ORDER BY signal_date DESC
                LIMIT 1
            """), {"ticker": ticker}).fetchone()
        return float(row[0]) if row and row[0] else None

    # ── Formatting ───────────────────────────────────────────────────

    def format_report(self, recommendations: list[OptionsRecommendation]) -> str:
        """Format recommendations into a readable report."""
        if not recommendations:
            return "No actionable options recommendations generated."

        lines = [
            "=" * 80,
            "GRID OPTIONS TRADE RECOMMENDATIONS",
            f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
            f"Total Recommendations: {len(recommendations)}",
            "=" * 80,
            "",
        ]

        for i, rec in enumerate(recommendations, 1):
            sanity_summary = ", ".join(
                f"{k}:{v['status']}" for k, v in rec.sanity_status.items()
            )
            lines.extend([
                f"#{i}  {rec.ticker} {rec.direction}  |  Confidence: {rec.confidence:.1%}",
                f"     Strike: ${rec.strike:,.2f}  |  Expiry: {rec.expiry}",
                (
                    f"     Entry: ${rec.entry_price:.4f} ({rec.entry_price_basis})  |  "
                    f"Target: {_fmt_money(rec.target_price)}  |  "
                    f"Stop: {_fmt_money(rec.stop_loss)}"
                ),
                (
                    f"     Expected Return: {_fmt_money(rec.expected_return)}  |  "
                    f"Max Risk: {_fmt_money(rec.max_risk)}"
                ),
                (
                    f"     Win prob: {_fmt_pct(rec.win_probability)} "
                    f"(n={rec.win_probability_n}, {rec.win_probability_basis})"
                ),
                f"     Kelly: {_fmt_pct(rec.kelly_fraction)}  |  R:R: {_fmt_ratio(rec.risk_reward_ratio)}",
                f"     Thesis: {rec.thesis}",
                f"     Dealer: {rec.dealer_context}",
                f"     Sanity: [{sanity_summary}]",
                "",
            ])

        return "\n".join(lines)


# ── CLI entrypoint ───────────────────────────────────────────────────

if __name__ == "__main__":
    from db import get_engine

    engine = get_engine()
    recommender = OptionsRecommender(engine)

    recs = recommender.generate_recommendations(engine)
    print(recommender.format_report(recs))

    if recs:
        log.info("Top recommendation: {t} {d} ${k} exp {e}",
                 t=recs[0].ticker, d=recs[0].direction,
                 k=recs[0].strike, e=recs[0].expiry)
