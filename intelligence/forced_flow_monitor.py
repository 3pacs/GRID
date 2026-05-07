"""
GRID Intelligence — Forced Flow Monitor (Waterfall Early Warning System).

Implements the discipline documented in docs/playbooks/opex_waterfall.md.
Every morning Hermes runs this and emits a briefing answering five questions:

    1. REGIME:    Is the dealer long-gamma (rubber band) or short-gamma
                  (slingshot) at current spot? Where is the flip?
    2. CALENDAR:  What forced-flow dates land in the next 10 trading days?
                  (Quarterly OPEX, monthly OPEX, JHEQX roll, FOMC, autocall
                  observation dates.)
    3. THRESHOLDS: How close is spot to (a) the gamma flip, (b) the put
                  wall, (c) the call wall? Is realized vol > implied?
    4. WATERFALL RISK SCORE: count of independent forced-flow conditions
                  currently tripped (0-5). Score >= 2 fires a waterfall
                  warning into alerts/waterfall_watch.py.
    5. POSTURE:   Recommended standing discipline for the day in
                  LEVER / CONDITION / INVALIDATION format.

The module is deliberately thin — it reads existing dealer_gamma output
and decorates it with the calendar + threshold logic. It does NOT recompute
GEX (that lives in physics/dealer_gamma.py).

Designed to be called by hermes_operator.run_intelligence_tasks once per
trading day at ~06:30 ET (pre-open briefing window).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ────────────────────────────────────────────────────────────


GAMMA_FLIP_PROXIMITY_PCT = 1.5      # spot within X% of flip = threshold tripped
WALL_PROXIMITY_PCT = 2.0            # spot within X% of put/call wall
REALIZED_IMPLIED_PREMIUM_PCT = 20.0  # realized > implied by this much = short-gamma confirm
OPEX_WEEK_LOOKAHEAD_DAYS = 10       # how far out to scan the calendar
WATERFALL_ALERT_THRESHOLD = 2       # >= N tripped conditions triggers alert


# 2026 FOMC statement release dates (approximate, update annually from fed.gov).
# Source: Federal Reserve 2026 FOMC meeting calendar.
FOMC_DATES_2026: list[date] = [
    date(2026, 1, 28),
    date(2026, 3, 18),
    date(2026, 4, 29),
    date(2026, 6, 17),
    date(2026, 7, 29),
    date(2026, 9, 16),
    date(2026, 10, 28),
    date(2026, 12, 9),
]


# ── Data Classes ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class GammaRegimeSnapshot:
    """Current dealer positioning regime and proximity levels."""

    regime: str                     # LONG_GAMMA / SHORT_GAMMA / UNKNOWN
    spot: float
    gamma_flip: float | None
    put_wall: float | None
    call_wall: float | None
    aggregate_gex: float
    snapshot_date: str

    def flip_distance_pct(self) -> float | None:
        if self.gamma_flip is None or self.spot <= 0:
            return None
        return abs(self.spot - self.gamma_flip) / self.spot * 100.0


@dataclass(frozen=True)
class CalendarEvent:
    """A known forced-flow date within the lookahead window."""

    event_date: date
    kind: str                       # QUARTERLY_OPEX / MONTHLY_OPEX / JHEQX_ROLL /
                                    # FOMC / AUTOCALL_OBS
    label: str
    trading_days_out: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_date": self.event_date.isoformat(),
            "kind": self.kind,
            "label": self.label,
            "trading_days_out": self.trading_days_out,
        }


@dataclass(frozen=True)
class ForcedFlowThreshold:
    """A single forced-flow threshold check."""

    name: str
    description: str
    current_value: float | None
    threshold_value: float
    tripped: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class MorningBriefing:
    """The full morning briefing artifact, persisted and optionally emailed."""

    generated_at: str
    regime: GammaRegimeSnapshot
    upcoming_events: list[CalendarEvent]
    thresholds: list[ForcedFlowThreshold]
    waterfall_risk_score: int
    posture: dict[str, str]         # LEVER / CONDITION / THESIS / INVALIDATION

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "regime": asdict(self.regime),
            "upcoming_events": [e.to_dict() for e in self.upcoming_events],
            "thresholds": [t.to_dict() for t in self.thresholds],
            "waterfall_risk_score": self.waterfall_risk_score,
            "posture": self.posture,
        }


# ── Table Setup ──────────────────────────────────────────────────────────


def _ensure_tables(engine: Engine) -> None:
    """Create forced_flow_briefings table if it does not exist."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS forced_flow_briefings (
                id SERIAL PRIMARY KEY,
                generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                regime TEXT NOT NULL,
                spot DOUBLE PRECISION,
                gamma_flip DOUBLE PRECISION,
                waterfall_risk_score INTEGER NOT NULL,
                tripped_conditions TEXT[],
                upcoming_events JSONB,
                posture JSONB,
                full_briefing JSONB NOT NULL
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_forced_flow_briefings_generated_at
            ON forced_flow_briefings (generated_at DESC)
        """))


# ── Calendar Computation ─────────────────────────────────────────────────


def _third_friday(year: int, month: int) -> date:
    """Return the 3rd Friday of the given month (monthly OPEX convention)."""
    first = date(year, month, 1)
    # weekday(): Mon=0, Fri=4
    first_friday_offset = (4 - first.weekday()) % 7
    first_friday = first + timedelta(days=first_friday_offset)
    return first_friday + timedelta(days=14)


def _last_business_day(year: int, month: int) -> date:
    """Return the last business day of the given month (approx JHEQX roll)."""
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    last = next_month - timedelta(days=1)
    while last.weekday() >= 5:  # Saturday/Sunday
        last -= timedelta(days=1)
    return last


def _trading_days_between(start: date, end: date) -> int:
    """Count weekdays between two dates (holiday-naive approximation)."""
    if end < start:
        return -1
    days = 0
    cur = start
    while cur < end:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            days += 1
    return days


def upcoming_calendar_events(
    ref_date: date | None = None,
    lookahead_days: int = OPEX_WEEK_LOOKAHEAD_DAYS,
) -> list[CalendarEvent]:
    """Enumerate forced-flow dates within the lookahead window.

    Returns events sorted by date. Includes monthly OPEX, quarterly OPEX,
    JHEQX roll (last biz day of quarter), FOMC statement days, and autocall
    observation dates (approximated as quarterly OPEX + 0).
    """
    if ref_date is None:
        ref_date = datetime.now(timezone.utc).date()

    horizon = ref_date + timedelta(days=lookahead_days + 7)  # pad for weekends
    events: list[CalendarEvent] = []

    # Monthly and quarterly OPEX (3rd Friday)
    cur_year, cur_month = ref_date.year, ref_date.month
    for _ in range(3):  # this month + 2 ahead covers 10-day lookahead
        opex = _third_friday(cur_year, cur_month)
        if ref_date <= opex <= horizon:
            is_quarterly = cur_month in (3, 6, 9, 12)
            kind = "QUARTERLY_OPEX" if is_quarterly else "MONTHLY_OPEX"
            label = (
                f"Quarterly OPEX ({opex.strftime('%b %Y')})"
                if is_quarterly
                else f"Monthly OPEX ({opex.strftime('%b %Y')})"
            )
            events.append(CalendarEvent(
                event_date=opex,
                kind=kind,
                label=label,
                trading_days_out=_trading_days_between(ref_date, opex),
            ))
            # Autocall observation cluster on quarterly OPEX
            if is_quarterly:
                events.append(CalendarEvent(
                    event_date=opex,
                    kind="AUTOCALL_OBS",
                    label="Autocall quarterly observation cluster (SX5E/KOSPI/NKY)",
                    trading_days_out=_trading_days_between(ref_date, opex),
                ))
        # Advance to next month
        if cur_month == 12:
            cur_year += 1
            cur_month = 1
        else:
            cur_month += 1

    # JHEQX quarterly roll = last business day of Mar/Jun/Sep/Dec
    for qm in (3, 6, 9, 12):
        for y in (ref_date.year, ref_date.year + 1):
            roll = _last_business_day(y, qm)
            if ref_date <= roll <= horizon:
                events.append(CalendarEvent(
                    event_date=roll,
                    kind="JHEQX_ROLL",
                    label="JHEQX + buffer ETF quarterly collar roll",
                    trading_days_out=_trading_days_between(ref_date, roll),
                ))

    # FOMC statement days
    for fomc in FOMC_DATES_2026:
        if ref_date <= fomc <= horizon:
            events.append(CalendarEvent(
                event_date=fomc,
                kind="FOMC",
                label="FOMC statement",
                trading_days_out=_trading_days_between(ref_date, fomc),
            ))

    events.sort(key=lambda e: (e.event_date, e.kind))
    return events


# ── Regime + Threshold Checks ────────────────────────────────────────────


def check_gamma_regime(engine: Engine) -> GammaRegimeSnapshot:
    """Read current dealer positioning from physics/dealer_gamma."""
    try:
        from physics.dealer_gamma import DealerGammaEngine
    except Exception as exc:
        log.warning("dealer_gamma import failed: {e}", e=str(exc))
        return GammaRegimeSnapshot(
            regime="UNKNOWN", spot=0.0, gamma_flip=None,
            put_wall=None, call_wall=None, aggregate_gex=0.0,
            snapshot_date=datetime.now(timezone.utc).date().isoformat(),
        )

    try:
        dge = DealerGammaEngine(db_engine=engine)
        summary = dge.get_market_gex_summary()
    except Exception as exc:
        log.warning("get_market_gex_summary failed: {e}", e=str(exc))
        summary = {"error": str(exc)}

    if "error" in summary or not summary.get("tickers"):
        return GammaRegimeSnapshot(
            regime="UNKNOWN", spot=0.0, gamma_flip=None,
            put_wall=None, call_wall=None, aggregate_gex=0.0,
            snapshot_date=datetime.now(timezone.utc).date().isoformat(),
        )

    spy = next((t for t in summary["tickers"] if t["ticker"] == "SPY"), None)
    spot = 0.0
    if spy and spy.get("spot"):
        spot = float(spy["spot"])
    elif summary.get("spy_spot"):
        spot = float(summary["spy_spot"])
    elif spy:
        # Best-effort fallback for older dealer_gamma summaries.
        pw = summary.get("spy_put_wall") or 0.0
        cw = summary.get("spy_call_wall") or 0.0
        if pw and cw:
            spot = (pw + cw) / 2.0

    return GammaRegimeSnapshot(
        regime=summary.get("market_regime", "UNKNOWN"),
        spot=spot,
        gamma_flip=summary.get("spy_gamma_flip"),
        put_wall=summary.get("spy_put_wall"),
        call_wall=summary.get("spy_call_wall"),
        aggregate_gex=float(summary.get("aggregate_gex", 0.0)),
        snapshot_date=summary.get("snap_date", datetime.now(timezone.utc).date().isoformat()),
    )


def scan_thresholds(
    regime: GammaRegimeSnapshot,
    events: list[CalendarEvent],
) -> list[ForcedFlowThreshold]:
    """Evaluate the five forced-flow threshold conditions.

    The conditions are chosen so that >= 2 simultaneously tripped indicates
    meaningful tail risk of a short-gamma cascade ("waterfall").
    """
    thresholds: list[ForcedFlowThreshold] = []

    # 1. Gamma flip proximity — spot near the transition between regimes
    flip_dist = regime.flip_distance_pct()
    thresholds.append(ForcedFlowThreshold(
        name="gamma_flip_proximity",
        description=(
            f"SPY spot within {GAMMA_FLIP_PROXIMITY_PCT}% of gamma flip "
            f"level (regime boundary)"
        ),
        current_value=flip_dist,
        threshold_value=GAMMA_FLIP_PROXIMITY_PCT,
        tripped=(flip_dist is not None and flip_dist <= GAMMA_FLIP_PROXIMITY_PCT),
    ))

    # 2. Short-gamma regime active (regardless of flip proximity)
    thresholds.append(ForcedFlowThreshold(
        name="short_gamma_regime",
        description="Dealers currently in short-gamma (slingshot) mode",
        current_value=regime.aggregate_gex,
        threshold_value=0.0,
        tripped=(regime.regime == "SHORT_GAMMA"),
    ))

    # 3. Put wall proximity — below spot, structural support but slingshot risk if broken
    put_wall_dist = None
    if regime.put_wall and regime.spot > 0:
        put_wall_dist = abs(regime.spot - regime.put_wall) / regime.spot * 100.0
    thresholds.append(ForcedFlowThreshold(
        name="put_wall_proximity",
        description=f"Spot within {WALL_PROXIMITY_PCT}% of SPY put wall (break = short-gamma)",
        current_value=put_wall_dist,
        threshold_value=WALL_PROXIMITY_PCT,
        tripped=(put_wall_dist is not None and put_wall_dist <= WALL_PROXIMITY_PCT
                 and regime.put_wall is not None and regime.spot < regime.put_wall),
    ))

    # 4. Quarterly catalyst in window (OPEX, JHEQX, FOMC, autocall obs)
    high_impact_kinds = {"QUARTERLY_OPEX", "JHEQX_ROLL", "FOMC", "AUTOCALL_OBS"}
    nearest_high = next(
        (e for e in events if e.kind in high_impact_kinds and e.trading_days_out <= 5),
        None,
    )
    thresholds.append(ForcedFlowThreshold(
        name="high_impact_catalyst_within_5d",
        description="Quarterly OPEX / JHEQX roll / FOMC / autocall obs within 5 trading days",
        current_value=float(nearest_high.trading_days_out) if nearest_high else None,
        threshold_value=5.0,
        tripped=nearest_high is not None,
    ))

    # 5. Short-gamma regime AND catalyst in window (compounded risk)
    compound_risk = (
        regime.regime == "SHORT_GAMMA"
        and nearest_high is not None
        and nearest_high.trading_days_out <= 3
    )
    thresholds.append(ForcedFlowThreshold(
        name="compound_regime_catalyst",
        description="Short-gamma regime AND high-impact catalyst within 3 trading days",
        current_value=1.0 if compound_risk else 0.0,
        threshold_value=1.0,
        tripped=compound_risk,
    ))

    return thresholds


def build_posture(
    regime: GammaRegimeSnapshot,
    events: list[CalendarEvent],
    thresholds: list[ForcedFlowThreshold],
) -> dict[str, str]:
    """Generate LEVER / CONDITION / THESIS / INVALIDATION per the GRID SOP."""
    tripped = [t for t in thresholds if t.tripped]
    nearest = events[0] if events else None

    if regime.regime == "SHORT_GAMMA":
        lever = (
            "Dealers short gamma at current spot — forced to sell weakness "
            "and buy strength. Slingshot regime."
        )
        thesis = "Trade the trend, not the fade. Moves accelerate; pinning trades are unsafe."
        condition = (
            f"{nearest.label} in {nearest.trading_days_out} trading days"
            if nearest else "No high-impact catalyst in lookahead window"
        )
        invalidation = (
            "SPY reclaims gamma flip level and holds for 2 consecutive sessions "
            "AND aggregate GEX crosses back above zero"
        )
    elif regime.regime == "LONG_GAMMA":
        lever = (
            "Dealers long gamma — suppressing realized vol. Rubber band regime; "
            "pinning forces dominate."
        )
        thesis = (
            "Pinning trades are in season but size for the slingshot. Own cheap "
            "tail hedges. Exit all pinning trades before high-impact catalysts."
        )
        condition = (
            f"{nearest.label} in {nearest.trading_days_out} trading days — "
            f"pinning force dissolves at close of that day"
            if nearest else "Quiet calendar; structural pinning dominant"
        )
        invalidation = (
            "SPY closes below gamma flip for 2 sessions OR VIX term structure "
            "inverts OR realized 5d vol exceeds implied 5d vol by >20%"
        )
    else:
        lever = "Regime unknown — dealer_gamma data unavailable or stale."
        thesis = "Reduce size until regime can be determined."
        condition = "N/A"
        invalidation = "N/A"

    if any(t.name == "compound_regime_catalyst" and t.tripped for t in thresholds):
        thesis = (
            "WATERFALL ALERT: compound short-gamma + catalyst setup. "
            "No new short vol. Carry tail hedges. " + thesis
        )

    return {
        "lever": lever,
        "condition": condition,
        "thesis": thesis,
        "invalidation": invalidation,
        "tripped_count": str(len(tripped)),
    }


# ── Public Entry Points ──────────────────────────────────────────────────


def build_morning_briefing(engine: Engine) -> MorningBriefing:
    """Orchestrate the full morning briefing."""
    regime = check_gamma_regime(engine)
    events = upcoming_calendar_events()
    thresholds = scan_thresholds(regime, events)
    score = sum(1 for t in thresholds if t.tripped)
    posture = build_posture(regime, events, thresholds)

    return MorningBriefing(
        generated_at=datetime.now(timezone.utc).isoformat(),
        regime=regime,
        upcoming_events=events,
        thresholds=thresholds,
        waterfall_risk_score=score,
        posture=posture,
    )


def persist_briefing(engine: Engine, briefing: MorningBriefing) -> int:
    """Store the briefing in forced_flow_briefings and return its row id."""
    _ensure_tables(engine)
    tripped_names = [t.name for t in briefing.thresholds if t.tripped]
    payload = briefing.to_dict()

    with engine.begin() as conn:
        row = conn.execute(
            text("""
                INSERT INTO forced_flow_briefings (
                    generated_at, regime, spot, gamma_flip,
                    waterfall_risk_score, tripped_conditions,
                    upcoming_events, posture, full_briefing
                ) VALUES (
                    :generated_at, :regime, :spot, :gamma_flip,
                    :risk, :tripped,
                    CAST(:events AS JSONB), CAST(:posture AS JSONB),
                    CAST(:full AS JSONB)
                )
                RETURNING id
            """),
            {
                "generated_at": briefing.generated_at,
                "regime": briefing.regime.regime,
                "spot": briefing.regime.spot or None,
                "gamma_flip": briefing.regime.gamma_flip,
                "risk": briefing.waterfall_risk_score,
                "tripped": tripped_names,
                "events": json.dumps([e.to_dict() for e in briefing.upcoming_events]),
                "posture": json.dumps(briefing.posture),
                "full": json.dumps(payload),
            },
        ).fetchone()
    return int(row[0]) if row else 0


def run_forced_flow_cycle(engine: Engine) -> dict[str, Any]:
    """Hermes entry point: build briefing, persist, and fire alert if needed.

    Returns a compact dict suitable for Hermes task_status logging.
    """
    try:
        briefing = build_morning_briefing(engine)
    except Exception as exc:
        log.warning("Forced flow briefing failed: {e}", e=str(exc))
        return {"status": "failed", "error": str(exc)}

    try:
        row_id = persist_briefing(engine, briefing)
    except Exception as exc:
        log.warning("persist_briefing failed: {e}", e=str(exc))
        row_id = 0

    # Fire waterfall alert if enough conditions tripped
    if briefing.waterfall_risk_score >= WATERFALL_ALERT_THRESHOLD:
        try:
            from alerts.waterfall_watch import send_waterfall_alert
            send_waterfall_alert(briefing)
        except Exception as exc:
            log.warning("waterfall_watch alert failed: {e}", e=str(exc))

    log.info(
        "Forced flow briefing: regime={r} score={s} events={e}",
        r=briefing.regime.regime,
        s=briefing.waterfall_risk_score,
        e=len(briefing.upcoming_events),
    )

    return {
        "status": "ok",
        "briefing_id": row_id,
        "regime": briefing.regime.regime,
        "waterfall_risk_score": briefing.waterfall_risk_score,
        "tripped_conditions": [t.name for t in briefing.thresholds if t.tripped],
        "upcoming_event_count": len(briefing.upcoming_events),
    }
