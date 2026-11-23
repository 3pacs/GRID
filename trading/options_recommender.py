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
from ingestion.market_calendar import last_trading_day
from store.feature_ids import feature_ids
from store.options_provenance import VISIBLE_DAILY_SIGNALS, VISIBLE_SNAPSHOTS, visible_sql
from trading.trade_ticket_generator import MAX_KELLY_PER_TICKET


# ── Canonical pricing/sizing utilities ──────────────────────────────
#
# These module-level helpers are the single source of truth for Kelly
# sizing, strike selection, expiry selection, and premium estimation
# across every pipeline that emits options tickets (scanner-driven,
# contagion-driven, signal-driven).  Other trading modules should
# import from here rather than rolling their own.
#
# ``MAX_KELLY_PER_TICKET`` (0.05) is imported from
# ``trading.trade_ticket_generator`` so all ticket/recommendation paths share
# one single hard per-ticket Kelly cap constant.

# Per-contract broker/exchange fee per leg ($0.65 per contract = $0.0065 per share).
OPTION_FEE_PER_CONTRACT: float = 0.65
CONTRACT_MULTIPLIER: int = 100
FEE_PER_SHARE: float = OPTION_FEE_PER_CONTRACT / CONTRACT_MULTIPLIER

# Risk-free rate and target horizon fraction used for Black-Scholes target repricing.
RISK_FREE_RATE: float = 0.05
TARGET_HORIZON_DTE_FRACTION: float = 0.5

# Sanity pipeline check classification.
# Required checks guard pricing, data quality, liquidity, or freshness and MUST PASS.
REQUIRED_CHECKS: frozenset[str] = frozenset({"DATA_QUALITY"})
# Genuinely optional checks may return SKIP without rejecting a recommendation.
OPTIONAL_CHECKS: frozenset[str] = frozenset({
    "DEALER_FLOW",
    "CROSS_ASSET",
    "LLM_REVIEW",
    "HISTORICAL_ANALOG",
})


def min_fresh_snap_date(run_date: date) -> date:
    """Return the oldest acceptable snapshot date (previous trading session).

    Uses ``ingestion.market_calendar.last_trading_day`` so weekends and US
    equity market holidays are handled accurately.
    """
    current_session = last_trading_day(run_date)
    return last_trading_day(current_session - timedelta(days=1))


def _positive_finite(value: Any) -> float | None:
    """Return float(value) when positive and finite, otherwise None."""
    if value is None or isinstance(value, bool):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return out if math.isfinite(out) and out > 0 else None


def _to_date(value: Any) -> date | None:
    """Coerce a date, datetime, or ISO date string into a ``date``."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None

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
    entry_price: float              # effective entry cost per share (ask + leg fee)
    entry_bid: float = 0.0         # bid at recommendation time
    entry_ask: float = 0.0         # ask at recommendation time
    entry_mid: float = 0.0         # displayed mid of bid-ask at recommendation time
    entry_by_date: str = ""        # enter by this date or cancel
    underlying_price: float = 0.0  # stock price at recommendation time

    # ── Exit targets ──
    target_price: float = 0.0      # option target price (profit exit)
    target_return_pct: float = 0.0 # expected % return on the option
    stop_loss: float = 0.0         # option stop loss price
    stop_loss_stock: float = 0.0   # stock price that invalidates (easier to watch)
    time_stop_date: str = ""       # exit by this date if thesis hasn't played out
    max_risk: float = 0.0          # total dollars at risk per contract
    expected_return: float = 0.0   # expected return per contract (win_prob * gain - loss_prob * loss)

    # ── Sizing ──
    kelly_fraction: float = 0.0    # Kelly-optimal fraction of portfolio
    suggested_contracts: int = 0   # for a $100K portfolio, how many contracts
    max_portfolio_pct: float = 0.02  # never more than 2% of portfolio per trade

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
            "entry_mid": self.entry_mid,
            "entry_by_date": self.entry_by_date,
            "underlying_price": self.underlying_price,
            "target_price": self.target_price,
            "target_return_pct": self.target_return_pct,
            "stop_loss": self.stop_loss,
            "stop_loss_stock": self.stop_loss_stock,
            "time_stop_date": self.time_stop_date,
            "max_risk": self.max_risk,
            "kelly_fraction": self.kelly_fraction,
            "suggested_contracts": self.suggested_contracts,
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
        return (
            f"{'═' * 50}\n"
            f"  {self.ticker} {self.strike}{self.direction[0]} {self.expiry}\n"
            f"{'═' * 50}\n"
            f"  ENTRY:  ${self.entry_price:.2f} (bid ${self.entry_bid:.2f} / ask ${self.entry_ask:.2f})\n"
            f"  STOCK:  ${self.underlying_price:.2f} at time of rec\n"
            f"  TARGET: ${self.target_price:.2f} ({self.target_return_pct:+.0f}%)\n"
            f"  STOP:   ${self.stop_loss:.2f} (or stock {'above' if self.direction == 'PUT' else 'below'} ${self.stop_loss_stock:.2f})\n"
            f"  TIME:   Exit by {self.time_stop_date} if no move\n"
            f"  SIZE:   {self.suggested_contracts} contracts ({self.kelly_fraction:.1%} Kelly)\n"
            f"  R/R:    {rr:.1f}x | Confidence: {self.confidence:.0%}\n"
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
    def risk_reward_ratio(self) -> float:
        """Target profit / max risk."""
        if self.max_risk <= 0:
            return 0.0
        return (self.target_price - self.entry_price) / self.entry_price

    @property
    def all_sanity_passed(self) -> bool:
        """True only if every required sanity check PASSed and no check FAILed.

        SKIP is accepted only for checks explicitly listed in ``OPTIONAL_CHECKS``.
        An empty ``sanity_status`` or a missing/SKIPped required check fails closed.
        """
        if not self.sanity_status:
            return False
        for req_name in REQUIRED_CHECKS:
            entry = self.sanity_status.get(req_name)
            if not isinstance(entry, dict) or entry.get("status") != "PASS":
                return False
        for name, entry in self.sanity_status.items():
            if not isinstance(entry, dict):
                return False
            status = entry.get("status")
            if status == "PASS":
                continue
            if status == "SKIP" and name in OPTIONAL_CHECKS:
                continue
            return False
        return True


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
        max_kelly: float = MAX_KELLY_PER_TICKET,
        *,
        allow_kelly_cap_override: bool = False,
    ) -> None:
        if (
            not isinstance(max_kelly, (int, float))
            or isinstance(max_kelly, bool)
            or not math.isfinite(max_kelly)
            or max_kelly < 0
        ):
            raise ValueError(
                f"max_kelly must be a finite non-negative number, got {max_kelly!r}"
            )
        if max_kelly > MAX_KELLY_PER_TICKET and not allow_kelly_cap_override:
            raise ValueError(
                f"max_kelly={max_kelly} exceeds MAX_KELLY_PER_TICKET={MAX_KELLY_PER_TICKET}; "
                "pass allow_kelly_cap_override=True to override"
            )
        self.engine = db_engine
        self.min_score = min_score
        self.capital = capital
        self.max_kelly = float(max_kelly)
        self._allow_kelly_cap_override = bool(allow_kelly_cap_override)
        self.last_rejection_counts: dict[str, int] = {}
        self.last_run_summary: dict[str, Any] = {}
        log.info(
            "OptionsRecommender initialised — min_score={s}, capital=${c:,.0f}, max_kelly={k:.2%}",
            s=min_score, c=capital, k=self.max_kelly,
        )

    def _record_rejection(self, ticker: str, reason: str, detail: str = "") -> None:
        """Increment rejection counter and emit a structured rejection log entry."""
        self.last_rejection_counts[reason] = self.last_rejection_counts.get(reason, 0) + 1
        suffix = f" ({detail})" if detail else ""
        log.info(
            "Recommendation rejected for {t}: reason={r}{s}",
            t=ticker, r=reason, s=suffix,
        )

    # ── Public API ───────────────────────────────────────────────────

    def generate_recommendations(
        self,
        engine: Engine | None = None,
        tickers: list[str] | None = None,
        run_date: date | None = None,
    ) -> list[OptionsRecommendation]:
        """Generate actionable options trade recommendations.

        Parameters:
            engine: Optional override SQLAlchemy engine (defaults to self.engine).
            tickers: Restrict scan to these tickers (default: all available).
            run_date: Effective run date for freshness and DTE (default: today).

        Returns:
            list[OptionsRecommendation]: Only recommendations passing all required
            sanity layers and positive EV/Kelly checks, sorted by expected return
            descending. Per-reason rejection counts are exposed on
            ``self.last_rejection_counts`` and ``self.last_run_summary``.
        """
        db = engine or self.engine
        effective_run_date = run_date or date.today()
        self.last_rejection_counts = {}
        self.last_run_summary = {}

        # 1. Run options scanner
        from discovery.options_scanner import OptionsScanner

        scanner = OptionsScanner(db_engine=db)
        opportunities = scanner.scan_all(tickers=tickers, min_score=self.min_score)

        log.info(
            "Scanner returned {n} opportunities (score >= {s})",
            n=len(opportunities), s=self.min_score,
        )

        if not opportunities:
            self.last_run_summary = {
                "run_date": effective_run_date.isoformat(),
                "opportunities": 0,
                "emitted": 0,
                "rejected": 0,
                "rejection_counts": {},
            }
            return []

        # 2. Load GEX engine
        from physics.dealer_gamma import DealerGammaEngine

        gex_engine = DealerGammaEngine(db_engine=db)

        recommendations: list[OptionsRecommendation] = []

        for opp in opportunities:
            try:
                rec = self._build_recommendation(
                    opp, gex_engine, db, run_date=effective_run_date,
                )
                if rec is not None:
                    recommendations.append(rec)
            except Exception as exc:
                self._record_rejection(opp.ticker, "build_error", str(exc))

        # Sort by expected return descending
        recommendations.sort(key=lambda r: r.expected_return, reverse=True)

        self.last_run_summary = {
            "run_date": effective_run_date.isoformat(),
            "opportunities": len(opportunities),
            "emitted": len(recommendations),
            "rejected": sum(self.last_rejection_counts.values()),
            "rejection_counts": dict(self.last_rejection_counts),
        }

        log.info(
            "Generated {n} actionable recommendations from {t} opportunities (rejections={r})",
            n=len(recommendations), t=len(opportunities), r=self.last_rejection_counts,
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
                        "dealer_context, sanity_status, signals, opposing_signals, generated_at) "
                        "VALUES (:ticker, :direction, :strike, :expiry, :entry_price, "
                        ":target_price, :stop_loss, :expected_return, :kelly_fraction, "
                        ":confidence, :thesis, :dealer_context, :sanity_status, "
                        ":signals, :opposing_signals, :generated_at)"
                    ),
                    {
                        "ticker": rec.ticker,
                        "direction": rec.direction,
                        "strike": _db_scalar(rec.strike),
                        "expiry": rec.expiry,
                        "entry_price": _db_scalar(rec.entry_price),
                        "target_price": _db_scalar(rec.target_price),
                        "stop_loss": _db_scalar(rec.stop_loss),
                        "expected_return": _db_scalar(getattr(rec, "expected_return", 0)),
                        "kelly_fraction": _db_scalar(rec.kelly_fraction),
                        "confidence": _db_scalar(rec.confidence),
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
        run_date: date | None = None,
    ) -> OptionsRecommendation | None:
        """Build a single recommendation from a scanner opportunity.

        Fails closed: returns None and records a rejection reason code whenever
        the contract cannot be priced on a fresh, existing chain row or fails
        EV, Kelly, or required sanity gates.
        """
        ticker = opp.ticker
        direction = opp.direction
        effective_run_date = run_date or date.today()
        min_snap = min_fresh_snap_date(effective_run_date)

        spot = _positive_finite(opp.spot_price)
        if spot is None:
            self._record_rejection(ticker, "no_spot")
            return None

        # Get GEX profile
        gex_profile = gex_engine.compute_gex_profile(ticker)
        if not isinstance(gex_profile, dict) or "error" in gex_profile:
            log.debug("GEX unavailable for {t} — continuing without GEX", t=ticker)
            gex_profile = {}

        # Load fresh options chain (bounded to current or previous trading session)
        chain_df = self._load_chain(db, ticker, run_date=effective_run_date)
        if chain_df.empty:
            self._record_rejection(ticker, "stale_or_missing_chain")
            return None

        # Defense-in-depth freshness check on snapshot date
        snap_d = _to_date(chain_df.attrs.get("snap_date"))
        if snap_d is None and "snap_date" in chain_df.columns and not chain_df["snap_date"].isna().all():
            snap_dates = [_to_date(v) for v in chain_df["snap_date"] if _to_date(v) is not None]
            snap_d = max(snap_dates) if snap_dates else None
        if snap_d is not None and (snap_d < min_snap or snap_d > effective_run_date):
            self._record_rejection(
                ticker, "stale_chain", f"snap_date={snap_d} < min={min_snap}",
            )
            return None

        # Jointly select an existing (strike, expiry) row from the same snapshot
        opp_iv_atm = _positive_finite(getattr(opp, "iv_atm", None))
        contract = self._select_contract(
            chain_df,
            direction,
            spot,
            gex_profile,
            run_date=effective_run_date,
            fallback_iv=opp_iv_atm,
        )
        if contract is None:
            self._record_rejection(ticker, "contract_not_in_chain")
            return None

        strike = float(contract["strike"])
        expiry = _to_date(contract["expiry"])
        if expiry is None:
            self._record_rejection(ticker, "contract_not_in_chain", "unparseable expiry")
            return None

        dte = (expiry - effective_run_date).days
        if dte < self._MIN_DTE or dte > self._MAX_DTE:
            self._record_rejection(
                ticker, "contract_not_in_chain", f"dte={dte} outside [{self._MIN_DTE},{self._MAX_DTE}]",
            )
            return None

        # Real two-sided quote from the selected snapshot row
        bid = _positive_finite(contract.get("bid"))
        ask = _positive_finite(contract.get("ask"))
        if bid is None or ask is None or ask < bid:
            self._record_rejection(
                ticker, "invalid_quote", f"K={strike} exp={expiry} bid={contract.get('bid')} ask={contract.get('ask')}",
            )
            return None

        entry_mid = (bid + ask) / 2.0
        half_spread = (ask - bid) / 2.0
        # Entry cost for EV and Kelly: pay the ask plus per-contract leg fee
        entry_cost = ask + FEE_PER_SHARE

        # Resolve implied volatility: contract's own IV -> repo ATM IV fallback -> reject no_iv
        iv = (
            _positive_finite(contract.get("implied_volatility"))
            or opp_iv_atm
            or _positive_finite(self._get_atm_iv(db, ticker, run_date=effective_run_date))
        )
        if iv is None:
            self._record_rejection(ticker, "no_iv", f"K={strike} exp={expiry}")
            return None

        # Target exit value: Black-Scholes repricing at target underlying & remaining time,
        # minus half the entry spread and minus the exit leg fee
        horizon_days = dte * TARGET_HORIZON_DTE_FRACTION
        target_price = self._compute_target_price(
            entry_cost,
            spot,
            strike,
            direction,
            gex_profile,
            iv=iv,
            dte=dte,
            horizon_days=horizon_days,
            half_spread=half_spread,
            fee_per_share=FEE_PER_SHARE,
        )
        if target_price is None or not math.isfinite(target_price):
            self._record_rejection(ticker, "no_iv", "target pricing unavailable")
            return None

        # Conservative 50% premium stop; net exit at stop also accounts for half-spread + exit fee
        stop_loss = self._compute_stop_loss(
            entry_cost, spot, strike, direction, gex_profile,
        )
        stop_exit = max(0.0, stop_loss - half_spread - FEE_PER_SHARE)

        gain = target_price - entry_cost
        loss = entry_cost - stop_exit
        win_prob = self._estimate_win_probability(opp, gex_profile)

        # Expected return (per share) and payoff ratio
        expected_return = win_prob * gain - (1.0 - win_prob) * loss
        if gain <= 0 or loss <= 0 or not math.isfinite(expected_return) or expected_return <= 0:
            self._record_rejection(
                ticker, "nonpositive_ev", f"ev={expected_return:.4f} gain={gain:.4f}",
            )
            return None

        payoff_ratio = gain / loss
        kelly_fraction = self._compute_kelly(win_prob, payoff_ratio)
        if not math.isfinite(kelly_fraction) or kelly_fraction <= 0:
            self._record_rejection(ticker, "zero_kelly", f"payoff={payoff_ratio:.2f}")
            return None

        # Contract sizing: never fabricate a contract. If the capped Kelly budget
        # cannot buy one contract, the recommendation is not emitted (fail closed).
        contracts = int(self.capital * kelly_fraction / (entry_cost * CONTRACT_MULTIPLIER))
        if contracts < 1:
            self._record_rejection(
                ticker, "below_one_contract",
                f"budget={self.capital * kelly_fraction:.2f} contract_cost={entry_cost * CONTRACT_MULTIPLIER:.2f}",
            )
            return None
        max_risk = contracts * entry_cost * CONTRACT_MULTIPLIER
        target_return_pct = ((target_price - entry_cost) / entry_cost) * 100.0
        time_stop_date = (
            effective_run_date + timedelta(days=max(1, int(round(horizon_days))))
        ).isoformat()

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
            expiry=expiry.isoformat(),
            entry_price=round(entry_cost, 4),
            entry_bid=round(bid, 4),
            entry_ask=round(ask, 4),
            entry_mid=round(entry_mid, 4),
            underlying_price=round(spot, 4),
            target_price=round(target_price, 4),
            target_return_pct=round(target_return_pct, 2),
            stop_loss=round(max(stop_loss, 0.01), 4),
            time_stop_date=time_stop_date,
            expected_return=round(expected_return, 4),
            max_risk=round(max_risk, 2),
            kelly_fraction=round(kelly_fraction, 4),
            suggested_contracts=contracts,
            confidence=round(confidence, 4),
            thesis=thesis,
            dealer_context=dealer_context,
            sanity_status={},
            supporting_signals=supporting,
            opposing_signals=opposing,
            generated_at=datetime.utcnow().isoformat() + "Z",
        )

        # Run 5-layer sanity pipeline
        sanity = self._run_sanity_pipeline(
            rec, db, opp, gex_profile, run_date=effective_run_date, resolved_iv=iv,
        )
        rec.sanity_status = sanity

        # Fail closed on any FAIL or on SKIP/absence of a required check
        if not rec.all_sanity_passed:
            skipped_required = [
                k for k, v in sanity.items()
                if isinstance(v, dict) and v.get("status") == "SKIP" and k not in OPTIONAL_CHECKS
            ]
            missing_required = [k for k in sorted(REQUIRED_CHECKS) if k not in sanity]
            failed = [
                k for k, v in sanity.items()
                if not isinstance(v, dict) or v.get("status") not in ("PASS", "SKIP")
            ]
            if skipped_required:
                self._record_rejection(ticker, f"required_check_skipped:{skipped_required[0]}")
            elif missing_required:
                self._record_rejection(ticker, f"required_check_skipped:{missing_required[0]}")
            elif failed:
                self._record_rejection(ticker, f"sanity_failed:{failed[0]}")
            else:
                self._record_rejection(ticker, "sanity_failed")
            return None

        return rec

    # ── Joint Strike & Expiry Selection ──────────────────────────────

    def _score_expiries(
        self,
        chain_df: pd.DataFrame,
        run_date: date,
        is_earnings_play: bool = False,
    ) -> list[tuple[float, date, int]]:
        """Rank eligible listed expiries in descending preference order."""
        work = chain_df.copy()
        work["_exp_date"] = work["expiry"].apply(_to_date)
        work = work[work["_exp_date"].notna()].copy()
        if work.empty:
            return []
        work["_dte"] = work["_exp_date"].apply(lambda d: (d - run_date).days)

        min_dte = self._EARNINGS_MIN_DTE if is_earnings_play else self._MIN_DTE
        expiry_stats = (
            work.groupby("_exp_date")
            .agg(total_oi=("open_interest", "sum"), dte=("_dte", "first"))
            .reset_index()
        )
        expiry_stats = expiry_stats[
            (expiry_stats["dte"] >= min_dte)
            & (expiry_stats["dte"] <= self._MAX_DTE)
            & (expiry_stats["total_oi"] >= self._MIN_OI_FOR_EXPIRY)
        ]
        if expiry_stats.empty:
            return []

        ranked: list[tuple[float, date, int]] = []
        for _, row in expiry_stats.iterrows():
            exp_date = row["_exp_date"]
            dte = int(row["dte"])
            total_oi = float(row["total_oi"])

            score = 0.0
            if self._IDEAL_DTE_LO <= dte <= self._IDEAL_DTE_HI:
                score += 5.0
            elif self._MIN_DTE <= dte < self._IDEAL_DTE_LO:
                score += 3.0
            else:
                score += 2.0

            if _is_monthly_opex(exp_date):
                score += 3.0
            elif _is_weekly_opex(exp_date):
                score -= 1.0

            score += math.log1p(max(total_oi, 0.0)) / 5.0
            ranked.append((score, exp_date, dte))

        ranked.sort(key=lambda item: item[0], reverse=True)
        return ranked

    def _select_contract(
        self,
        chain_df: pd.DataFrame,
        direction: str,
        spot: float,
        gex_profile: dict,
        *,
        run_date: date | None = None,
        is_earnings_play: bool = False,
        fallback_iv: float | None = None,
    ) -> pd.Series | None:
        """Select an existing (strike, expiry) contract row jointly from one snapshot.

        Ranks eligible expiries first (measuring DTE from ``run_date``) and then
        picks the best listed strike within the highest-ranked expiry that has at
        least one matching contract in the target OTM/wall window. Never fabricates
        a (strike, expiry) pair that is absent from ``chain_df``.
        """
        from physics.dealer_gamma import bs_gamma

        if chain_df.empty:
            return None

        effective_run_date = run_date or date.today()
        work = chain_df.copy()

        # Restrict to a single snapshot date if multiple are present
        if "snap_date" in work.columns:
            work["_snap_date"] = work["snap_date"].apply(_to_date)
            valid_snaps = work["_snap_date"].dropna()
            if not valid_snaps.empty:
                latest_snap = valid_snaps.max()
                work = work[work["_snap_date"] == latest_snap].copy()

        work["_exp_date"] = work["expiry"].apply(_to_date)
        work = work[work["_exp_date"].notna()].copy()
        if work.empty:
            return None
        work["_dte"] = work["_exp_date"].apply(lambda d: (d - effective_run_date).days)
        work = work[work["_dte"] > 0].copy()
        if work.empty:
            return None

        ranked_expiries = self._score_expiries(work, effective_run_date, is_earnings_play=is_earnings_play)
        if not ranked_expiries:
            return None

        call_wall = _positive_finite(gex_profile.get("call_wall")) if gex_profile else None
        put_wall = _positive_finite(gex_profile.get("put_wall")) if gex_profile else None

        if direction == "CALL":
            lo = spot * (1 + self._CALL_OTM_MIN_PCT)
            hi = min(spot * (1 + self._CALL_OTM_MAX_PCT), call_wall or spot * 1.15)
            opt_type = "call"
        else:
            hi = spot * (1 - self._PUT_OTM_MIN_PCT)
            lo = max(spot * (1 - self._PUT_OTM_MAX_PCT), put_wall or spot * 0.85)
            opt_type = "put"

        if lo > hi:
            return None

        fb_iv = _positive_finite(fallback_iv)

        for _, exp_date, dte in ranked_expiries:
            exp_rows = work[
                (work["_exp_date"] == exp_date)
                & (work["opt_type"] == opt_type)
                & (work["strike"] >= lo)
                & (work["strike"] <= hi)
            ]
            if exp_rows.empty:
                continue

            liquid = exp_rows[exp_rows["open_interest"] >= self._MIN_OI]
            candidates = liquid if not liquid.empty else exp_rows

            # Prefer contracts with valid two-sided bid/ask quotes when available
            if "bid" in candidates.columns and "ask" in candidates.columns:
                quoted_mask = candidates.apply(
                    lambda r: (
                        _positive_finite(r.get("bid")) is not None
                        and _positive_finite(r.get("ask")) is not None
                        and float(r.get("ask")) >= float(r.get("bid"))
                    ),
                    axis=1,
                )
                if quoted_mask.any():
                    candidates = candidates[quoted_mask]

            T = float(dte) / 365.0
            best_row: pd.Series | None = None
            best_score = -1.0
            fallback_no_iv_row: pd.Series | None = None
            fallback_no_iv_oi = -1.0

            for _, row in candidates.iterrows():
                K = float(row["strike"])
                oi = float(row["open_interest"]) if pd.notna(row["open_interest"]) else 0.0
                iv = _positive_finite(row.get("implied_volatility")) or fb_iv
                if iv is None:
                    # Keep track of existing row with missing IV so caller can reject with 'no_iv'
                    if oi > fallback_no_iv_oi:
                        fallback_no_iv_oi = oi
                        fallback_no_iv_row = row
                    continue

                gamma = bs_gamma(spot, K, T, RISK_FREE_RATE, iv)
                cost_proxy = iv * math.sqrt(T) * spot
                oi_bonus = math.log1p(max(oi, 0.0)) / 10.0
                score = (gamma * spot * 100 / max(cost_proxy, 0.01)) + oi_bonus
                if score > best_score:
                    best_score = score
                    best_row = row

            chosen = best_row if best_row is not None else fallback_no_iv_row
            if chosen is not None:
                return chosen

        return None

    # ── Strike Optimization ──────────────────────────────────────────

    def _optimize_strike(
        self,
        chain_df: pd.DataFrame,
        direction: str,
        spot: float,
        gex_profile: dict,
        fallback_iv: float | None = None,
    ) -> float | None:
        """Pick the strike with best gamma/premium ratio within direction."""
        from physics.dealer_gamma import bs_gamma

        call_wall = _positive_finite(gex_profile.get("call_wall")) if gex_profile else None
        put_wall = _positive_finite(gex_profile.get("put_wall")) if gex_profile else None

        if direction == "CALL":
            lo = spot * (1 + self._CALL_OTM_MIN_PCT)
            hi = min(spot * (1 + self._CALL_OTM_MAX_PCT), call_wall or spot * 1.15)
            opt_type = "call"
        else:
            hi = spot * (1 - self._PUT_OTM_MIN_PCT)
            lo = max(spot * (1 - self._PUT_OTM_MAX_PCT), put_wall or spot * 0.85)
            opt_type = "put"

        if lo > hi:
            return None

        # Filter to relevant strikes and option type
        candidates = chain_df[
            (chain_df["strike"] >= lo)
            & (chain_df["strike"] <= hi)
            & (chain_df["opt_type"] == opt_type)
            & (chain_df["open_interest"] >= self._MIN_OI)
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

        best_strike = None
        best_score = -1.0
        fb_iv = _positive_finite(fallback_iv)

        for _, row in candidates.iterrows():
            K = float(row["strike"])
            T = float(row["dte"]) / 365.0
            if T <= 0:
                continue
            iv = _positive_finite(row.get("implied_volatility")) or fb_iv
            if iv is None:
                continue
            oi = float(row["open_interest"])

            gamma = bs_gamma(spot, K, T, RISK_FREE_RATE, iv)
            cost_proxy = iv * math.sqrt(T) * spot
            oi_bonus = math.log1p(max(oi, 0.0)) / 10.0

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
        run_date: date | None = None,
    ) -> date | None:
        """Pick optimal expiry measuring DTE from ``run_date``."""
        ranked = self._score_expiries(
            chain_df,
            run_date or date.today(),
            is_earnings_play=is_earnings_play,
        )
        return ranked[0][1] if ranked else None

    # ── Entry / Target / Stop ────────────────────────────────────────

    def _get_entry_price(
        self,
        db: Engine,
        ticker: str,
        strike: float,
        expiry: date,
        direction: str,
        run_date: date | None = None,
    ) -> float | None:
        """Get effective entry cost (ask + leg fee) from a fresh options snapshot.

        Fails closed: returns None if the contract is absent from fresh snapshots
        or lacks a valid two-sided bid/ask quote (never falls back to BS or last).
        """
        opt_type = "call" if direction == "CALL" else "put"
        effective_run_date = run_date or date.today()
        min_snap = min_fresh_snap_date(effective_run_date)
        expiry_d = _to_date(expiry)
        if expiry_d is None:
            return None

        with db.connect() as conn:
            row = conn.execute(visible_sql("""
                SELECT bid, ask, last_price, snap_date
                FROM """ + VISIBLE_SNAPSHOTS + """ s
                WHERE ticker = :ticker
                  AND strike = :strike
                  AND expiry = :expiry
                  AND opt_type = :opt_type
                  AND snap_date >= :min_snap_date
                  AND snap_date <= :run_date
                ORDER BY snap_date DESC
                LIMIT 1
            """), {
                "ticker": ticker,
                "strike": strike,
                "expiry": expiry_d,
                "opt_type": opt_type,
                "min_snap_date": min_snap,
                "run_date": effective_run_date,
            }).fetchone()

        if row is None:
            return None

        if len(row) >= 4 and row[3] is not None:
            snap_d = _to_date(row[3])
            if snap_d is None or snap_d < min_snap or snap_d > effective_run_date:
                return None

        bid = _positive_finite(row[0])
        ask = _positive_finite(row[1])
        if bid is None or ask is None or ask < bid:
            return None

        return ask + FEE_PER_SHARE

    def _compute_target_price(
        self,
        entry_price: float,
        spot: float,
        strike: float,
        direction: str,
        gex_profile: dict,
        *,
        iv: float | None = None,
        dte: float | None = None,
        horizon_days: float | None = None,
        half_spread: float = 0.0,
        fee_per_share: float = FEE_PER_SHARE,
        risk_free_rate: float = RISK_FREE_RATE,
    ) -> float | None:
        """Reprice the option at target underlying & remaining time via Black-Scholes.

        Reuses ``analysis.vol_surface._bs_call_price`` / ``_bs_put_price`` (which
        delegate to ``physics.greeks.black_scholes.d1`` via ``physics.dealer_gamma``).
        Returns the net exit value at target: repriced premium minus half the entry
        spread minus the exit leg fee. Returns None if ``iv`` or ``dte`` is missing.
        """
        from analysis.vol_surface import _bs_call_price, _bs_put_price

        sigma = _positive_finite(iv)
        dte_f = _positive_finite(dte)
        spot_f = _positive_finite(spot)
        strike_f = _positive_finite(strike)
        if sigma is None or dte_f is None or spot_f is None or strike_f is None:
            return None

        if horizon_days is None:
            horizon = dte_f * TARGET_HORIZON_DTE_FRACTION
        else:
            horizon = max(0.0, min(float(horizon_days), dte_f))

        t_rem = max(0.0, (dte_f - horizon) / 365.0)
        t_horizon = max(horizon, 1.0) / 365.0

        if direction == "CALL":
            call_wall = _positive_finite(gex_profile.get("call_wall")) if gex_profile else None
            if call_wall is not None and call_wall > spot_f:
                target_spot = call_wall
            else:
                target_spot = spot_f * math.exp(sigma * math.sqrt(t_horizon))
            theo_price = _bs_call_price(target_spot, strike_f, t_rem, risk_free_rate, sigma)
        else:
            put_wall = _positive_finite(gex_profile.get("put_wall")) if gex_profile else None
            if put_wall is not None and put_wall < spot_f:
                target_spot = put_wall
            else:
                target_spot = spot_f * math.exp(-sigma * math.sqrt(t_horizon))
            theo_price = _bs_put_price(target_spot, strike_f, t_rem, risk_free_rate, sigma)

        return max(0.0, theo_price - max(0.0, half_spread) - max(0.0, fee_per_share))

    def _compute_stop_loss(
        self,
        entry_price: float,
        spot: float,
        strike: float,
        direction: str,
        gex_profile: dict,
    ) -> float:
        """Use the existing 50% premium stop until a validated stop model exists.

        A nearest zero crossing cannot establish the direction of modeled
        GEX on either side, much less a realized dealer hedge reversal.
        Keep the signature for existing recommendation callers.
        """
        return entry_price * 0.50

    # ── Kelly and Probability ────────────────────────────────────────

    def _estimate_win_probability(self, opp, gex_profile: dict) -> float:
        """Estimate win probability from scanner score and GEX alignment.

        Score 6/10 -> ~35% base win rate
        Score 8/10 -> ~50%
        Score 10/10 -> ~60% (capped — markets are efficient)
        GEX alignment bonus: +5-10%
        """
        score = opp.score
        # Linear mapping: score 5->30%, 10->60%
        base_prob = 0.30 + (score - 5.0) * 0.06

        # GEX alignment bonus
        regime = gex_profile.get("regime", "")
        direction = opp.direction
        if regime == "SHORT_GAMMA":
            # Short gamma amplifies moves — good for directional bets
            base_prob += 0.05
        elif regime == "LONG_GAMMA" and direction in ("CALL", "PUT"):
            # Long gamma dampens moves — harder for directional bets
            base_prob -= 0.03

        # Vanna/charm adjustment
        vanna = gex_profile.get("vanna_exposure", 0)
        if direction == "CALL" and vanna < 0:
            # Negative vanna + IV drop -> dealers buy underlying -> bullish
            base_prob += 0.03
        elif direction == "PUT" and vanna > 0:
            # Positive vanna + IV rise -> dealers sell underlying -> bearish
            base_prob += 0.03

        return max(0.10, min(0.65, base_prob))

    def _compute_kelly(self, win_prob: float, payoff_ratio: float) -> float:
        """Kelly criterion with half-Kelly shrink, capped at ``self.max_kelly``.

        Enforces ``MAX_KELLY_PER_TICKET`` (0.05) unless ``allow_kelly_cap_override=True``
        was explicitly passed at construction.
        """
        if payoff_ratio <= 0 or win_prob <= 0:
            return 0.0
        effective_cap = self.max_kelly
        if not self._allow_kelly_cap_override:
            effective_cap = min(effective_cap, MAX_KELLY_PER_TICKET)
        if effective_cap <= 0 or not math.isfinite(effective_cap):
            return 0.0
        full_kelly = compute_kelly_fraction(
            accuracy=win_prob,
            payout_ratio=payoff_ratio,
            cap=min(1.0, effective_cap * 2.0),
        )
        half_kelly = full_kelly / 2.0
        return round(max(0.0, min(half_kelly, effective_cap)), 4)

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
        *,
        run_date: date | None = None,
        resolved_iv: float | None = None,
    ) -> dict:
        """Run 5-layer sanity check on a recommendation.

        Layers:
          1. DATA_QUALITY (REQUIRED): fresh snapshot, IV in 5-200%, OI >= 100, valid spread <= 25% of mid
          2. DEALER_FLOW (OPTIONAL): GEX regime supports direction
          3. CROSS_ASSET (OPTIONAL): momentum + news energy aligned with direction
          4. LLM_REVIEW (OPTIONAL): structured prompt to LLM, graceful SKIP if unavailable
          5. HISTORICAL_ANALOG (OPTIONAL): query past scanner results with similar setups

        Each returns {status: "PASS"/"FAIL"/"SKIP", reason: "..."}
        """
        sanity: dict[str, dict[str, str]] = {}

        sanity["DATA_QUALITY"] = self._sanity_data_quality(
            rec, db, run_date=run_date, resolved_iv=resolved_iv,
        )
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
        self,
        rec: OptionsRecommendation,
        db: Engine,
        *,
        run_date: date | None = None,
        resolved_iv: float | None = None,
    ) -> dict[str, str]:
        """Layer 1 (REQUIRED): Contract-specific data quality, liquidity, and freshness checks.

        Filters by exact (ticker, strike, expiry, opt_type) and fresh snap_date window
        using bound parameters:
        - Snapshot from current or previous trading session
        - IV in 5-200%
        - OI >= 100
        - Valid two-sided bid/ask quote with spread <= 25% of mid
        """
        opt_type = "call" if rec.direction == "CALL" else "put"
        effective_run_date = run_date or date.today()
        min_snap = min_fresh_snap_date(effective_run_date)
        expiry_d = _to_date(rec.expiry)
        if expiry_d is None:
            return {"status": "FAIL", "reason": f"Invalid recommended expiry {rec.expiry!r}"}

        with db.connect() as conn:
            row = conn.execute(visible_sql("""
                SELECT implied_vol, open_interest, bid, ask, snap_date
                FROM """ + VISIBLE_SNAPSHOTS + """ s
                WHERE ticker = :ticker
                  AND strike = :strike
                  AND expiry = :expiry
                  AND opt_type = :opt_type
                  AND snap_date >= :min_snap_date
                  AND snap_date <= :run_date
                ORDER BY snap_date DESC
                LIMIT 1
            """), {
                "ticker": rec.ticker,
                "strike": rec.strike,
                "expiry": expiry_d,
                "opt_type": opt_type,
                "min_snap_date": min_snap,
                "run_date": effective_run_date,
            }).fetchone()

        if row is None:
            return {
                "status": "FAIL",
                "reason": "No fresh snapshot data for recommended contract",
            }

        if len(row) >= 5 and row[4] is not None:
            snap_d = _to_date(row[4])
            if snap_d is None or snap_d < min_snap or snap_d > effective_run_date:
                return {
                    "status": "FAIL",
                    "reason": f"Snapshot date {row[4]} is outside fresh window [{min_snap}, {effective_run_date}]",
                }

        row_iv = _positive_finite(row[0])
        iv = row_iv if row_iv is not None else _positive_finite(resolved_iv)
        oi = int(row[1]) if row[1] is not None else 0
        bid = _positive_finite(row[2])
        ask = _positive_finite(row[3])

        issues = []

        if iv is None:
            issues.append("Implied volatility missing")
        elif iv < self._IV_MIN or iv > self._IV_MAX:
            issues.append(f"IV {iv:.1%} outside sane range ({self._IV_MIN:.0%}-{self._IV_MAX:.0%})")

        if oi < self._MIN_OI:
            issues.append(f"OI {oi} below minimum {self._MIN_OI}")

        if bid is None or ask is None or ask < bid:
            issues.append(f"Invalid bid/ask quote (bid={row[2]}, ask={row[3]})")
        else:
            mid = (bid + ask) / 2.0
            spread_pct = (ask - bid) / mid if mid > 0 else 1.0
            if spread_pct > self._MAX_SPREAD_PCT:
                issues.append(f"Spread {spread_pct:.1%} exceeds {self._MAX_SPREAD_PCT:.0%} of mid")

        if issues:
            return {"status": "FAIL", "reason": "; ".join(issues)}

        return {"status": "PASS", "reason": "Fresh snapshot with IV, OI, and spread within acceptable bounds"}

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
- Entry: ${rec.entry_price:.4f}, Target: ${rec.target_price:.4f}, Stop: ${rec.stop_loss:.4f}
- Expected Return: ${rec.expected_return:.4f}, Max Risk: ${rec.max_risk:.2f}
- Kelly Fraction: {rec.kelly_fraction:.2%}
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
        """
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
                    "lo_score": rec.confidence * 10 - 1.5,  # approximate score
                    "hi_score": rec.confidence * 10 + 1.5,
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

    def _load_chain(
        self, db: Engine, ticker: str, run_date: date | None = None,
    ) -> pd.DataFrame:
        """Load fresh options chain from database (current or previous trading session).

        DTE is measured from ``run_date`` (not ``snap_date``).
        """
        effective_run_date = run_date or date.today()
        min_snap = min_fresh_snap_date(effective_run_date)

        with db.connect() as conn:
            rows = conn.execute(visible_sql("""
                SELECT strike, opt_type, open_interest, implied_vol AS implied_volatility,
                       expiry, bid, ask, snap_date
                FROM """ + VISIBLE_SNAPSHOTS + """ s
                WHERE ticker = :ticker
                  AND snap_date = (
                      SELECT MAX(snap_date)
                      FROM """ + VISIBLE_SNAPSHOTS + """ s_latest
                      WHERE ticker = :ticker
                        AND snap_date >= :min_snap_date
                        AND snap_date <= :run_date
                  )
                  AND snap_date >= :min_snap_date
                  AND snap_date <= :run_date
                  AND open_interest > 0
                  AND expiry > :run_date
                ORDER BY expiry, strike
            """), {
                "ticker": ticker,
                "min_snap_date": min_snap,
                "run_date": effective_run_date,
            }).fetchall()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=[
            "strike", "opt_type", "open_interest", "implied_volatility",
            "expiry", "bid", "ask", "snap_date",
        ])
        df["snap_date"] = df["snap_date"].apply(_to_date)
        df["expiry"] = df["expiry"].apply(_to_date)
        df = df[
            df["snap_date"].notna()
            & df["expiry"].notna()
            & (df["snap_date"] >= min_snap)
            & (df["snap_date"] <= effective_run_date)
        ].copy()
        if df.empty:
            return pd.DataFrame()

        latest_snap = df["snap_date"].max()
        df = df[df["snap_date"] == latest_snap].copy()
        df["dte"] = df["expiry"].apply(lambda exp: (exp - effective_run_date).days)
        df = df[df["dte"] > 0].copy()
        df.attrs["snap_date"] = latest_snap
        df.attrs["run_date"] = effective_run_date
        return df

    def _get_spot(self, db: Engine, ticker: str) -> float:
        """Get latest spot price for a ticker."""
        with db.connect() as conn:
            # Bound ids, each feature's newest row through LATERAL
            # (store.feature_ids); the name join read every vintage first.
            fids = feature_ids(conn, [f"{ticker.lower()}_close", ticker.lower()])
            row = None if not fids else conn.execute(text("""
                SELECT x.value
                FROM unnest(CAST(:fids AS integer[])) AS f(id)
                CROSS JOIN LATERAL (
                    SELECT rs.value, rs.obs_date FROM resolved_series rs
                    WHERE rs.feature_id = f.id
                    ORDER BY rs.obs_date DESC LIMIT 1
                ) x
                ORDER BY x.obs_date DESC
                LIMIT 1
            """), {"fids": fids}).fetchone()
        return float(row[0]) if row else 0.0

    def _get_atm_iv(
        self, db: Engine, ticker: str, run_date: date | None = None,
    ) -> float | None:
        """Get ATM implied volatility from latest fresh options signals."""
        effective_run_date = run_date or date.today()
        min_snap = min_fresh_snap_date(effective_run_date)
        with db.connect() as conn:
            row = conn.execute(visible_sql("""
                SELECT iv_atm
                FROM """ + VISIBLE_DAILY_SIGNALS + """ ods
                WHERE ticker = :ticker
                  AND signal_date >= :min_snap_date
                  AND signal_date <= :run_date
                ORDER BY signal_date DESC
                LIMIT 1
            """), {
                "ticker": ticker,
                "min_snap_date": min_snap,
                "run_date": effective_run_date,
            }).fetchone()
        return _positive_finite(row[0]) if row else None

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
                f"     Entry: ${rec.entry_price:.4f}  |  Target: ${rec.target_price:.4f}  |  Stop: ${rec.stop_loss:.4f}",
                f"     Expected Return: ${rec.expected_return:.4f}  |  Max Risk: ${rec.max_risk:,.2f}",
                f"     Kelly: {rec.kelly_fraction:.2%}  |  R:R: {rec.risk_reward_ratio:.1f}x",
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
