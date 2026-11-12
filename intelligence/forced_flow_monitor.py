"""
GRID Intelligence — Forced Flow Monitor (Waterfall Early Warning System).

Implements the discipline documented in docs/playbooks/opex_waterfall.md.
Every morning Hermes runs this and emits a briefing answering five questions:

    1. REGIME:    Is the dealer long-gamma (rubber band) or short-gamma
                  (slingshot) at current spot? Where is the flip?
    2. CALENDAR:  What candidate dates land in the next 10 US trading days?
                  (Quarterly OPEX, monthly OPEX, JHEQX roll, FOMC, autocall
                  observation dates require separate source evidence.)
    3. THRESHOLDS: How close is spot to (a) the gamma flip, (b) the put
                  wall, (c) the call wall? Is realized vol > implied?
    4. WATERFALL RULE COUNT: count of tripped evidence groups (0-2),
                  not independent votes or a calibrated probability.
                  Score >= 2 fires a waterfall warning. The built-in
                  calendar contains estimates, not admitted schedules,
                  and cannot by itself supply the second group.
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
import math
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.market_calendar import is_market_open, last_trading_day, trading_days_between


# ── Constants ────────────────────────────────────────────────────────────


GAMMA_FLIP_PROXIMITY_PCT = 1.5      # spot within X% of flip = threshold tripped
WALL_PROXIMITY_PCT = 2.0            # spot within X% of put/call wall
REALIZED_IMPLIED_PREMIUM_PCT = 20.0  # realized > implied by this much = short-gamma confirm
OPEX_WEEK_LOOKAHEAD_DAYS = 10       # how far out to scan the calendar
WATERFALL_ALERT_THRESHOLD = 2       # both evidence groups must be tripped
GROUPED_SCORE_METHOD = "grouped_rule_count_v1"
SCORE_GROUPS = {
    "dealer_positioning": frozenset({
        "gamma_flip_proximity", "short_gamma_regime", "put_wall_proximity",
    }),
    "calendar_catalyst": frozenset({"high_impact_catalyst_within_5d"}),
}
HIGH_IMPACT_KINDS = frozenset({"QUARTERLY_OPEX", "JHEQX_ROLL", "FOMC", "AUTOCALL_OBS"})
_EQUITY_TZ = ZoneInfo("America/New_York")


# Legacy static dates: no source receipt, revision history or known-at clock.
# Display only, not admitted schedule evidence. Coverage ends in 2026.
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
    """A candidate date, with schedule evidence distinct from observed flow.

    An upstream caller may supply ``source_backed_schedule`` only after
    verifying the specific event against its source. ``event_id`` must identify
    that event (including the product/contract for a roll or observation), not
    just its category. ``known_at`` is when that exact schedule revision became
    available, not the event date or a backdated publication time. This module
    checks the receipt's presence and decision-time bound; it does not verify
    the external source. No built-in estimate satisfies this admission contract.
    """

    event_date: date
    kind: str                       # QUARTERLY_OPEX / MONTHLY_OPEX / JHEQX_ROLL /
                                    # FOMC / AUTOCALL_OBS
    label: str
    trading_days_out: int
    evidence_status: str = "unverified"
    source: str | None = None
    event_id: str | None = None
    known_at: datetime | None = None
    evidence_note: str = "Schedule unverified; actual flow is not established."

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_date": self.event_date.isoformat(),
            "kind": self.kind,
            "label": self.label,
            "trading_days_out": self.trading_days_out,
            "evidence_status": self.evidence_status,
            "source": self.source,
            "event_id": self.event_id,
            "known_at": self.known_at.isoformat() if self.known_at else None,
            "evidence_note": self.evidence_note,
            "distance_basis": (
                "ingestion.market_calendar US equity sessions; (New York reference date, event]"
            ),
            "observed_flow_confirmed": False,
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
        payload = asdict(self)
        payload["score_group"] = next(
            (group for group, names in SCORE_GROUPS.items() if self.name in names), None,
        )
        payload["diagnostic_only"] = payload["score_group"] is None
        return payload


@dataclass
class MorningBriefing:
    """The full morning briefing artifact, persisted and optionally emailed."""

    generated_at: str
    regime: GammaRegimeSnapshot
    upcoming_events: list[CalendarEvent]
    thresholds: list[ForcedFlowThreshold]
    waterfall_risk_score: int
    posture: dict[str, str]         # LEVER / CONDITION / THESIS / INVALIDATION
    score_method: str = "unspecified_legacy"

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "generated_at": self.generated_at,
            "regime": asdict(self.regime),
            "upcoming_events": [e.to_dict() for e in self.upcoming_events],
            "thresholds": [t.to_dict() for t in self.thresholds],
            "waterfall_risk_score": self.waterfall_risk_score,
            "waterfall_score_method": self.score_method,
            "posture": self.posture,
        }
        if self.score_method == GROUPED_SCORE_METHOD:
            payload["waterfall_score_max"] = len(SCORE_GROUPS)
            payload["tripped_groups"] = _tripped_score_groups(self.thresholds)
            payload["score_limitations"] = (
                "Grouped rule count, not independent votes or calibrated probability. "
                "Calendar qualification establishes a sourced schedule, not observed flow. "
                "Built-in estimates are excluded; calendar coverage is incomplete."
            )
        return payload


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


def _utc_now() -> datetime:
    """Decision clock, kept separate for synthetic tests."""
    return datetime.now(timezone.utc)


def _session_date(as_of: datetime) -> date:
    """US-market date at an aware instant, including after-hours and DST."""
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
    return as_of.astimezone(_EQUITY_TZ).date()


def _third_friday(year: int, month: int) -> date:
    """Return the 3rd Friday of the given month (monthly OPEX convention)."""
    first = date(year, month, 1)
    # weekday(): Mon=0, Fri=4
    first_friday_offset = (4 - first.weekday()) % 7
    first_friday = first + timedelta(days=first_friday_offset)
    return first_friday + timedelta(days=14)


def _last_business_day(year: int, month: int) -> date:
    """Return the last US equity session, not a verified fund roll date."""
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    return last_trading_day(next_month - timedelta(days=1))


def _trading_days_between(start: date, end: date) -> int:
    """Count canonical US sessions in (start, end]; past dates return -1.

    Standard holidays are covered; extraordinary closures depend on updates
    to ingestion.market_calendar. This is not a foreign-market calendar.
    """
    if end < start:
        return -1
    return len(trading_days_between(start, end)) - int(is_market_open(start))


def upcoming_calendar_events(
    ref_date: date | None = None,
    lookahead_days: int = OPEX_WEEK_LOOKAHEAD_DAYS,
) -> list[CalendarEvent]:
    """List unverified calendar candidates within a US-session window.

    OPEX uses a convention adjusted to the prior US session, JHEQX uses a
    quarter-end proxy, and FOMC uses an unverified static 2026 list. None is
    evidence of actual forced transactions or eligible for the catalyst rule.
    Autocall schedules are contract-specific and cannot be inferred from OPEX.
    A missing entry does not imply an empty calendar, especially after 2026.
    """
    if isinstance(lookahead_days, bool) or not isinstance(lookahead_days, int) or lookahead_days < 0:
        raise ValueError("lookahead_days must be a nonnegative integer of US sessions")
    if ref_date is None:
        ref_date = _session_date(_utc_now())

    horizon = ref_date
    for _ in range(lookahead_days):
        horizon += timedelta(days=1)
        while not is_market_open(horizon):
            horizon += timedelta(days=1)
    events: list[CalendarEvent] = []

    # Conventional date only; product-specific expiration/settlement may differ.
    cur_year, cur_month = ref_date.year, ref_date.month
    while date(cur_year, cur_month, 1) <= horizon:
        opex = last_trading_day(_third_friday(cur_year, cur_month))
        if ref_date <= opex <= horizon:
            is_quarterly = cur_month in (3, 6, 9, 12)
            kind = "QUARTERLY_OPEX" if is_quarterly else "MONTHLY_OPEX"
            label = (
                f"Quarterly OPEX estimate ({opex.strftime('%b %Y')})"
                if is_quarterly
                else f"Monthly OPEX estimate ({opex.strftime('%b %Y')})"
            )
            events.append(CalendarEvent(
                event_date=opex,
                kind=kind,
                label=label,
                trading_days_out=_trading_days_between(ref_date, opex),
                evidence_status="calendar_estimate",
                source="convention:third_friday_prior_US_session",
                evidence_note=(
                    "Unverified expiration convention, not a product-specific exchange schedule; "
                    "does not establish forced flow or autocall observations."
                ),
            ))
        if cur_month in (3, 6, 9, 12):
            roll = _last_business_day(cur_year, cur_month)
            if ref_date <= roll <= horizon:
                events.append(CalendarEvent(
                    event_date=roll,
                    kind="JHEQX_ROLL",
                    label="JHEQX quarter-end roll estimate (unverified)",
                    trading_days_out=_trading_days_between(ref_date, roll),
                    evidence_status="calendar_estimate",
                    source="heuristic:last_US_session_of_quarter",
                    evidence_note=(
                        "Quarter-end proxy, not a confirmed fund roll or buffer ETF schedule; "
                        "execution date, size and direction are unknown."
                    ),
                ))
        # Advance to next month
        if cur_month == 12:
            cur_year += 1
            cur_month = 1
        else:
            cur_month += 1

    # A legacy date list is not a verified, point-in-time schedule receipt.
    for fomc in FOMC_DATES_2026:
        if ref_date <= fomc <= horizon:
            events.append(CalendarEvent(
                event_date=fomc,
                kind="FOMC",
                label="FOMC statement (unverified static 2026 schedule)",
                trading_days_out=_trading_days_between(ref_date, fomc),
                evidence_status="static_unverified",
                source="legacy_static:FOMC_DATES_2026",
                evidence_note=(
                    "No source receipt or schedule-revision known-at timestamp; "
                    "coverage limited to 2026. A policy announcement is not observed dealer flow."
                ),
            ))

    events.sort(key=lambda e: (e.event_date, e.kind))
    return events


# ── Regime + Threshold Checks ────────────────────────────────────────────


def check_gamma_regime(engine: Engine) -> GammaRegimeSnapshot:
    """Read the latest completed SPY model, not observed dealer positions."""
    try:
        from physics.dealer_gamma import DealerGammaEngine
    except Exception as exc:
        log.warning("dealer_gamma import failed: {e}", e=str(exc))
        return GammaRegimeSnapshot(
            regime="UNKNOWN", spot=0.0, gamma_flip=None,
            put_wall=None, call_wall=None, aggregate_gex=0.0,
            snapshot_date=datetime.now(timezone.utc).date().isoformat(),
        )

    # The latest *completed* SPY capture, not today's UTC date: Hermes runs
    # this at 06:00-06:40 UTC and the day's first capture completes ~14:40
    # UTC, so the date-default read found no chain on every run and every
    # briefing since 2026-09-25 was UNKNOWN (37 of 46 in 120 days); four of
    # the five waterfall conditions need the regime, so no alert could fire.
    # SPY is the only ticker with a verified spot receipt, and the market
    # proxy the briefing reports.
    try:
        dge = DealerGammaEngine(db_engine=engine)
        spy = dge.compute_latest_completed_gex_profile("SPY")
    except Exception as exc:
        log.warning("compute_latest_completed_gex_profile failed: {e}", e=str(exc))
        spy = {"error": str(exc)}

    # Preserve the completed-capture reader from #1138, but do not admit a
    # nonfinite, nonpositive or boolean reference spot (or infer one from walls).
    raw_spot = spy.get("spot")
    try:
        spot = float(raw_spot) if not isinstance(raw_spot, bool) else 0.0
    except (TypeError, ValueError, OverflowError):
        spot = 0.0
    if "error" in spy or not math.isfinite(spot) or spot <= 0 or not spy.get("regime"):
        return GammaRegimeSnapshot(
            regime="UNKNOWN", spot=0.0, gamma_flip=None,
            put_wall=None, call_wall=None, aggregate_gex=0.0,
            snapshot_date=datetime.now(timezone.utc).date().isoformat(),
        )

    return GammaRegimeSnapshot(
        regime=spy["regime"],
        spot=spot,
        gamma_flip=spy.get("gamma_flip"),
        put_wall=spy.get("put_wall"),
        call_wall=spy.get("call_wall"),
        aggregate_gex=float(spy.get("gex_aggregate") or 0.0),
        snapshot_date=spy.get("chain_snap_date") or spy.get("snap_date")
        or datetime.now(timezone.utc).date().isoformat(),
    )


def _qualified_catalyst_events(
    events: list[CalendarEvent], as_of: datetime,
) -> list[CalendarEvent]:
    """Admit source-backed schedule receipts available by the decision clock.

    This validates caller-supplied provenance, not the external source itself.
    Integrations must first verify the identified event and exact revision.
    A verified schedule still says nothing about executed flow size/direction.
    """
    decision_date = _session_date(as_of)
    qualified = []
    for event in events:
        if (event.kind not in HIGH_IMPACT_KINDS
                or event.evidence_status != "source_backed_schedule"
                or not isinstance(event.source, str) or not event.source.strip()
                or not isinstance(event.event_id, str) or not event.event_id.strip()
                or not isinstance(event.known_at, datetime)
                or event.known_at.tzinfo is None or event.known_at.utcoffset() is None
                or event.known_at > as_of):
            continue
        # Do not trust a supplied offset to move a past/far event into the window.
        if (type(event.trading_days_out) is not int
                or not 0 <= event.trading_days_out <= 5
                or event.trading_days_out != _trading_days_between(decision_date, event.event_date)):
            continue
        qualified.append(event)
    return sorted(qualified, key=lambda event: (event.trading_days_out, event.event_date, event.kind))


def _tripped_score_groups(thresholds: list[ForcedFlowThreshold]) -> dict[str, list[str]]:
    """Count each shared evidence family at most once; never count conjunctions.

    These groups are conservative overlap controls, not independent factors.
    Unknown condition names are diagnostic-only until explicitly assigned.
    """
    tripped = {threshold.name for threshold in thresholds if threshold.tripped is True}
    return {
        group: sorted(names & tripped)
        for group, names in SCORE_GROUPS.items() if names & tripped
    }


def scan_thresholds(
    regime: GammaRegimeSnapshot,
    events: list[CalendarEvent],
    *,
    as_of: datetime | None = None,
) -> list[ForcedFlowThreshold]:
    """Retain five condition details while qualifying the schedule evidence.

    The first three share dealer-positioning inputs. The fifth is a diagnostic
    conjunction, not another vote. No condition count estimates a probability.
    """
    if as_of is None:
        as_of = _utc_now()
    qualified_events = _qualified_catalyst_events(events, as_of)
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
        description="Assumed dealer-positioning model reports short gamma, not observed flow",
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
        description=f"Spot below and within {WALL_PROXIMITY_PCT}% of modeled SPY put wall",
        current_value=put_wall_dist,
        threshold_value=WALL_PROXIMITY_PCT,
        tripped=(put_wall_dist is not None and put_wall_dist <= WALL_PROXIMITY_PCT
                 and regime.put_wall is not None and regime.spot < regime.put_wall),
    ))

    # 4. Source-backed scheduled catalyst; static/estimated dates never qualify.
    nearest_high = qualified_events[0] if qualified_events else None
    thresholds.append(ForcedFlowThreshold(
        name="high_impact_catalyst_within_5d",
        description=(
            "Source-backed scheduled catalyst within 5 US sessions, known by decision time; "
            "estimated dates excluded, actual flow unconfirmed, coverage incomplete"
        ),
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
        description=(
            "Diagnostic only: short-gamma model AND source-backed scheduled catalyst "
            "within 3 US sessions; adds no score group"
        ),
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
    nearest = min(events, key=lambda event: (event.event_date, event.kind)) if events else None
    calendar_condition = (
        f"Next listed date: {nearest.label} in {nearest.trading_days_out} US sessions; "
        f"evidence={nearest.evidence_status}. {nearest.evidence_note} "
        "A listed date does not establish actual forced flow."
        if nearest else "No listed catalyst; calendar coverage is incomplete and unverified."
    )

    if regime.regime == "SHORT_GAMMA":
        lever = (
            "Modeled dealers short gamma at current spot; hedging may amplify "
            "weakness and strength. Actual dealer flow is unverified."
        )
        thesis = (
            "Research scenario: if short-gamma positioning is validated, monitor whether "
            "hedging reinforces the trend. Acceleration and failure of pinning are not "
            "established; no automatic trade recommendation."
        )
        condition = calendar_condition
        invalidation = (
            "Two consecutive SPY closes with recomputed modeled GEX positive "
            "at the reference spot; crossing the nearest gamma flip alone "
            "does not establish the direction of the regime change"
        )
    elif regime.regime == "LONG_GAMMA":
        lever = (
            "Modeled dealers long gamma; hedging may dampen realized volatility. "
            "Pinning is not guaranteed and actual dealer flow is unverified."
        )
        thesis = (
            "Research scenario: if long-gamma positioning is validated, monitor whether "
            "price behavior is consistent with dampening or pinning. Calendar candidates "
            "alone do not establish hedge value or warrant a trade."
        )
        condition = calendar_condition
        invalidation = (
            "Two consecutive SPY closes with recomputed modeled GEX negative "
            "at the reference spot, or a separately verified volatility "
            "invalidation; crossing the nearest flip alone is insufficient"
        )
    else:
        lever = "Regime unknown — dealer_gamma data unavailable or stale."
        thesis = (
            "Research status: withhold directional conclusions until usable positioning "
            "evidence is available; no automatic trade recommendation."
        )
        condition = "N/A"
        invalidation = "N/A"

    if any(t.name == "compound_regime_catalyst" and t.tripped for t in thresholds):
        thesis = (
            "WATERFALL WATCH: modeled short-gamma + sourced schedule setup, "
            "not confirmed flow or a calibrated probability. "
            "Review both evidence sources before interpreting the scenario. " + thesis
        )

    return {
        "lever": lever,
        "condition": condition,
        "thesis": thesis,
        "invalidation": invalidation,
        "tripped_count": str(len(tripped)),
        "tripped_group_count": str(len(_tripped_score_groups(thresholds))),
        "score_method": GROUPED_SCORE_METHOD,
    }


# ── Public Entry Points ──────────────────────────────────────────────────


def build_morning_briefing(engine: Engine) -> MorningBriefing:
    """Build a grouped-rule briefing, not a calibrated waterfall forecast.

    The legacy output name is retained, but its new versioned count is 0-2,
    not the historical 0-5 raw-check sum. Built-in calendar estimates cannot
    activate the calendar group, so they cannot trigger a two-group alert.
    Future calendar integration needs source/event/revision-known-at admission.
    """
    as_of = _utc_now()
    regime = check_gamma_regime(engine)
    events = upcoming_calendar_events(ref_date=_session_date(as_of))
    thresholds = scan_thresholds(regime, events, as_of=as_of)
    score = len(_tripped_score_groups(thresholds))
    posture = build_posture(regime, events, thresholds)

    return MorningBriefing(
        generated_at=as_of.isoformat(),
        regime=regime,
        upcoming_events=events,
        thresholds=thresholds,
        waterfall_risk_score=score,
        posture=posture,
        score_method=GROUPED_SCORE_METHOD,
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

    # Both evidence groups must trip; overlapping raw checks cannot trigger it.
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
        "waterfall_score_method": briefing.score_method,
        "tripped_groups": _tripped_score_groups(briefing.thresholds),
        "tripped_conditions": [t.name for t in briefing.thresholds if t.tripped],
        "upcoming_event_count": len(briefing.upcoming_events),
    }
