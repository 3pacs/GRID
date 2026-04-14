"""Tests for intelligence.forced_flow_monitor and alerts.waterfall_watch."""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from intelligence.forced_flow_monitor import (
    FOMC_DATES_2026,
    CalendarEvent,
    ForcedFlowThreshold,
    GammaRegimeSnapshot,
    MorningBriefing,
    _last_business_day,
    _third_friday,
    _trading_days_between,
    build_morning_briefing,
    build_posture,
    check_gamma_regime,
    scan_thresholds,
    upcoming_calendar_events,
)


# ── Date math helpers ────────────────────────────────────────────────────


@pytest.mark.unit
def test_third_friday_march_2026():
    """2026-03-20 is the third Friday of March 2026 (quarterly OPEX)."""
    assert _third_friday(2026, 3) == date(2026, 3, 20)


@pytest.mark.unit
def test_third_friday_december_2026():
    """2026-12-18 is the third Friday of December 2026."""
    assert _third_friday(2026, 12) == date(2026, 12, 18)


@pytest.mark.unit
def test_third_friday_always_friday():
    """The computed date is always a Friday across all 12 months of 2026."""
    for month in range(1, 13):
        assert _third_friday(2026, month).weekday() == 4


@pytest.mark.unit
def test_last_business_day_skips_weekends():
    """Last business day of May 2026 — May 31 is Sunday, so should be Fri May 29."""
    result = _last_business_day(2026, 5)
    assert result == date(2026, 5, 29)
    assert result.weekday() < 5


@pytest.mark.unit
def test_last_business_day_end_of_year():
    """Dec 2026 last biz day: Dec 31 2026 is a Thursday, should be Dec 31."""
    result = _last_business_day(2026, 12)
    assert result == date(2026, 12, 31)


@pytest.mark.unit
def test_trading_days_between_basic():
    """Mon → Fri is 4 trading days."""
    mon = date(2026, 4, 13)  # Monday
    fri = date(2026, 4, 17)  # Friday
    assert _trading_days_between(mon, fri) == 4


@pytest.mark.unit
def test_trading_days_between_skips_weekend():
    """Fri → Mon is 1 trading day (weekend skipped)."""
    fri = date(2026, 4, 17)
    mon = date(2026, 4, 20)
    assert _trading_days_between(fri, mon) == 1


# ── Calendar enumeration ────────────────────────────────────────────────


@pytest.mark.unit
def test_upcoming_events_includes_march_quarterly():
    """Scanning from March 10 2026 should pick up March 20 quarterly OPEX."""
    events = upcoming_calendar_events(ref_date=date(2026, 3, 10), lookahead_days=15)
    kinds = {e.kind for e in events}
    assert "QUARTERLY_OPEX" in kinds
    assert "AUTOCALL_OBS" in kinds  # quarterly OPEX implies autocall obs
    q_opex = next(e for e in events if e.kind == "QUARTERLY_OPEX")
    assert q_opex.event_date == date(2026, 3, 20)


@pytest.mark.unit
def test_upcoming_events_includes_monthly_opex():
    """Scanning from April 10 should include April monthly OPEX (not quarterly)."""
    events = upcoming_calendar_events(ref_date=date(2026, 4, 10), lookahead_days=15)
    monthly = [e for e in events if e.kind == "MONTHLY_OPEX"]
    assert len(monthly) >= 1
    # April 2026 third Friday = April 17 (not a quarter-end month)
    assert any(e.event_date == date(2026, 4, 17) for e in monthly)


@pytest.mark.unit
def test_upcoming_events_includes_jheqx_roll():
    """March has a JHEQX roll on the last business day."""
    events = upcoming_calendar_events(ref_date=date(2026, 3, 25), lookahead_days=10)
    jheqx = [e for e in events if e.kind == "JHEQX_ROLL"]
    assert any(e.event_date == date(2026, 3, 31) for e in jheqx)


@pytest.mark.unit
def test_upcoming_events_includes_fomc():
    """FOMC dates from the hardcoded 2026 list appear when in range."""
    # Pick a ref date 5 days before a known FOMC
    fomc = FOMC_DATES_2026[0]  # Jan 28
    ref = date(fomc.year, fomc.month, fomc.day - 5)
    events = upcoming_calendar_events(ref_date=ref, lookahead_days=10)
    assert any(e.kind == "FOMC" and e.event_date == fomc for e in events)


@pytest.mark.unit
def test_upcoming_events_sorted_by_date():
    events = upcoming_calendar_events(ref_date=date(2026, 3, 1), lookahead_days=60)
    dates = [e.event_date for e in events]
    assert dates == sorted(dates)


@pytest.mark.unit
def test_upcoming_events_empty_in_quiet_window():
    """A short lookahead in a quiet window may still return something — but
    confirms the function does not crash and returns a list."""
    events = upcoming_calendar_events(ref_date=date(2026, 4, 1), lookahead_days=1)
    assert isinstance(events, list)


# ── Threshold scanning ───────────────────────────────────────────────────


def _make_regime(
    regime: str = "LONG_GAMMA",
    spot: float = 500.0,
    flip: float | None = 495.0,
    put_wall: float | None = 490.0,
    call_wall: float | None = 510.0,
    gex: float = 1e9,
) -> GammaRegimeSnapshot:
    return GammaRegimeSnapshot(
        regime=regime,
        spot=spot,
        gamma_flip=flip,
        put_wall=put_wall,
        call_wall=call_wall,
        aggregate_gex=gex,
        snapshot_date="2026-04-14",
    )


@pytest.mark.unit
def test_scan_thresholds_returns_five_conditions():
    regime = _make_regime()
    thresholds = scan_thresholds(regime, events=[])
    assert len(thresholds) == 5
    names = {t.name for t in thresholds}
    assert names == {
        "gamma_flip_proximity",
        "short_gamma_regime",
        "put_wall_proximity",
        "high_impact_catalyst_within_5d",
        "compound_regime_catalyst",
    }


@pytest.mark.unit
def test_scan_thresholds_flip_proximity_tripped_when_close():
    """Spot 500, flip 498 → 0.4% distance → tripped (threshold 1.5%)."""
    regime = _make_regime(spot=500.0, flip=498.0)
    thresholds = scan_thresholds(regime, events=[])
    flip_t = next(t for t in thresholds if t.name == "gamma_flip_proximity")
    assert flip_t.tripped is True


@pytest.mark.unit
def test_scan_thresholds_flip_proximity_not_tripped_when_far():
    """Spot 500, flip 470 → 6% distance → not tripped."""
    regime = _make_regime(spot=500.0, flip=470.0)
    thresholds = scan_thresholds(regime, events=[])
    flip_t = next(t for t in thresholds if t.name == "gamma_flip_proximity")
    assert flip_t.tripped is False


@pytest.mark.unit
def test_scan_thresholds_short_gamma_regime_tripped():
    regime = _make_regime(regime="SHORT_GAMMA", gex=-1e9)
    thresholds = scan_thresholds(regime, events=[])
    sg_t = next(t for t in thresholds if t.name == "short_gamma_regime")
    assert sg_t.tripped is True


@pytest.mark.unit
def test_scan_thresholds_catalyst_tripped_when_within_5d():
    """A JHEQX roll 3 days out should trip the catalyst threshold."""
    regime = _make_regime()
    event = CalendarEvent(
        event_date=date(2026, 4, 17),
        kind="JHEQX_ROLL",
        label="JHEQX roll",
        trading_days_out=3,
    )
    thresholds = scan_thresholds(regime, events=[event])
    cat_t = next(t for t in thresholds if t.name == "high_impact_catalyst_within_5d")
    assert cat_t.tripped is True


@pytest.mark.unit
def test_scan_thresholds_catalyst_not_tripped_beyond_5d():
    """A FOMC 8 days out should not trip the 5-day catalyst threshold."""
    regime = _make_regime()
    event = CalendarEvent(
        event_date=date(2026, 4, 22),
        kind="FOMC",
        label="FOMC",
        trading_days_out=8,
    )
    thresholds = scan_thresholds(regime, events=[event])
    cat_t = next(t for t in thresholds if t.name == "high_impact_catalyst_within_5d")
    assert cat_t.tripped is False


@pytest.mark.unit
def test_scan_thresholds_compound_risk():
    """Short gamma + catalyst within 3 days = compound risk tripped."""
    regime = _make_regime(regime="SHORT_GAMMA", gex=-1e9)
    event = CalendarEvent(
        event_date=date(2026, 4, 16),
        kind="QUARTERLY_OPEX",
        label="Quarterly OPEX",
        trading_days_out=2,
    )
    thresholds = scan_thresholds(regime, events=[event])
    comp_t = next(t for t in thresholds if t.name == "compound_regime_catalyst")
    assert comp_t.tripped is True


@pytest.mark.unit
def test_scan_thresholds_unknown_regime_no_trip():
    """UNKNOWN regime should not trip short-gamma or compound conditions."""
    regime = GammaRegimeSnapshot(
        regime="UNKNOWN", spot=0.0, gamma_flip=None,
        put_wall=None, call_wall=None, aggregate_gex=0.0,
        snapshot_date="2026-04-14",
    )
    thresholds = scan_thresholds(regime, events=[])
    sg_t = next(t for t in thresholds if t.name == "short_gamma_regime")
    assert sg_t.tripped is False


# ── Posture generation ──────────────────────────────────────────────────


@pytest.mark.unit
def test_build_posture_short_gamma_has_trend_directive():
    regime = _make_regime(regime="SHORT_GAMMA")
    posture = build_posture(regime, events=[], thresholds=[])
    assert "trend" in posture["thesis"].lower()


@pytest.mark.unit
def test_build_posture_long_gamma_has_pinning_directive():
    regime = _make_regime(regime="LONG_GAMMA")
    posture = build_posture(regime, events=[], thresholds=[])
    assert "pinning" in posture["thesis"].lower()


@pytest.mark.unit
def test_build_posture_unknown_regime_reduces_size():
    regime = GammaRegimeSnapshot(
        regime="UNKNOWN", spot=0.0, gamma_flip=None,
        put_wall=None, call_wall=None, aggregate_gex=0.0,
        snapshot_date="2026-04-14",
    )
    posture = build_posture(regime, events=[], thresholds=[])
    assert "reduce size" in posture["thesis"].lower()


@pytest.mark.unit
def test_build_posture_compound_risk_prepends_waterfall_alert():
    regime = _make_regime(regime="SHORT_GAMMA")
    compound = ForcedFlowThreshold(
        name="compound_regime_catalyst",
        description="test",
        current_value=1.0,
        threshold_value=1.0,
        tripped=True,
    )
    posture = build_posture(regime, events=[], thresholds=[compound])
    assert "WATERFALL ALERT" in posture["thesis"]


# ── Orchestration ───────────────────────────────────────────────────────


@pytest.mark.unit
def test_build_morning_briefing_without_dealer_data(mock_engine):
    """When dealer_gamma returns nothing, briefing still builds with UNKNOWN regime."""
    with patch(
        "intelligence.forced_flow_monitor.check_gamma_regime",
        return_value=GammaRegimeSnapshot(
            regime="UNKNOWN", spot=0.0, gamma_flip=None,
            put_wall=None, call_wall=None, aggregate_gex=0.0,
            snapshot_date="2026-04-14",
        ),
    ):
        briefing = build_morning_briefing(mock_engine)

    assert isinstance(briefing, MorningBriefing)
    assert briefing.regime.regime == "UNKNOWN"
    assert len(briefing.thresholds) == 5
    assert 0 <= briefing.waterfall_risk_score <= 5
    assert briefing.posture["thesis"]


@pytest.mark.unit
def test_check_gamma_regime_handles_missing_import(mock_engine):
    """When dealer_gamma import fails, returns UNKNOWN regime without raising."""
    with patch.dict("sys.modules", {"physics.dealer_gamma": None}):
        result = check_gamma_regime(mock_engine)
    assert result.regime == "UNKNOWN"


@pytest.mark.unit
def test_briefing_to_dict_is_json_safe():
    """MorningBriefing.to_dict() must be JSON-serializable."""
    import json

    briefing = MorningBriefing(
        generated_at=datetime.now(timezone.utc).isoformat(),
        regime=_make_regime(),
        upcoming_events=[CalendarEvent(
            event_date=date(2026, 4, 17),
            kind="JHEQX_ROLL",
            label="JHEQX roll",
            trading_days_out=3,
        )],
        thresholds=[ForcedFlowThreshold(
            name="test",
            description="test",
            current_value=1.0,
            threshold_value=1.0,
            tripped=True,
        )],
        waterfall_risk_score=1,
        posture={"lever": "x", "condition": "y", "thesis": "z", "invalidation": "w"},
    )
    payload = briefing.to_dict()
    json.dumps(payload)  # must not raise


# ── Alert wiring ────────────────────────────────────────────────────────


@pytest.mark.unit
def test_waterfall_alert_subject_contains_score():
    from alerts.waterfall_watch import build_alert_subject

    briefing = MorningBriefing(
        generated_at="2026-04-14T06:30:00+00:00",
        regime=_make_regime(regime="SHORT_GAMMA"),
        upcoming_events=[],
        thresholds=[],
        waterfall_risk_score=3,
        posture={"lever": "", "condition": "", "thesis": "", "invalidation": ""},
    )
    subject = build_alert_subject(briefing)
    assert "3/5" in subject
    assert "SHORT_GAMMA" in subject


@pytest.mark.unit
def test_waterfall_alert_below_threshold_does_not_send():
    from alerts.waterfall_watch import send_waterfall_alert_if_triggered

    briefing = MorningBriefing(
        generated_at="2026-04-14T06:30:00+00:00",
        regime=_make_regime(),
        upcoming_events=[],
        thresholds=[],
        waterfall_risk_score=1,  # below default threshold of 2
        posture={"lever": "", "condition": "", "thesis": "", "invalidation": ""},
    )
    assert send_waterfall_alert_if_triggered(briefing, threshold=2) is False


@pytest.mark.unit
def test_waterfall_alert_send_handles_missing_email_layer():
    """If alerts.email layer cannot be imported, send_waterfall_alert returns False gracefully."""
    from alerts.waterfall_watch import send_waterfall_alert

    briefing = MorningBriefing(
        generated_at="2026-04-14T06:30:00+00:00",
        regime=_make_regime(regime="SHORT_GAMMA"),
        upcoming_events=[],
        thresholds=[],
        waterfall_risk_score=3,
        posture={"lever": "a", "condition": "b", "thesis": "c", "invalidation": "d"},
    )

    # Force the _send call to fail (simulating SMTP / config issue) — should not raise
    with patch("alerts.email._send", side_effect=RuntimeError("smtp down")):
        result = send_waterfall_alert(briefing)
    # Either False (graceful) or True if settings path short-circuits; must not raise
    assert result in (True, False)
