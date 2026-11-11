"""Tests for intelligence.forced_flow_monitor and alerts.waterfall_watch."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from intelligence.forced_flow_monitor import (
    FOMC_DATES_2026,
    GROUPED_SCORE_METHOD,
    CalendarEvent,
    ForcedFlowThreshold,
    GammaRegimeSnapshot,
    MorningBriefing,
    _last_business_day,
    _third_friday,
    _trading_days_between,
    _tripped_score_groups,
    build_morning_briefing,
    build_posture,
    check_gamma_regime,
    persist_briefing,
    run_forced_flow_cycle,
    scan_thresholds,
    upcoming_calendar_events,
)


DECISION = datetime(2026, 4, 14, 6, 30, tzinfo=timezone.utc)


@pytest.fixture
def mock_engine():
    """Synthetic only: never use the repository conftest's live DB fixtures."""
    engine = MagicMock()
    engine.connect.side_effect = AssertionError("real DB access is forbidden")
    engine.begin.side_effect = AssertionError("real DB access is forbidden")
    return engine


@pytest.fixture(autouse=True)
def _synthetic_settings_and_clock():
    """No production .env reads through email settings; fixed decision clock."""
    with patch("alerts.email._get_settings", return_value=SimpleNamespace(ALERT_EMAIL_ENABLED=False)), patch(
        "intelligence.forced_flow_monitor._utc_now", return_value=DECISION,
    ):
        yield


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
    assert "AUTOCALL_OBS" not in kinds  # contract dates cannot be inferred from OPEX
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


def _source_event(**overrides) -> CalendarEvent:
    """A synthetic upstream schedule receipt, never a live/provider event."""
    values = {
        "event_date": date(2026, 4, 17),
        "kind": "JHEQX_ROLL",
        "label": "Synthetic fund roll schedule",
        "trading_days_out": 3,
        "evidence_status": "source_backed_schedule",
        "source": "synthetic:issuer_schedule_fixture",
        "known_at": DECISION - timedelta(days=1),
        "evidence_note": "Synthetic schedule evidence; actual flow is not established.",
    }
    values.update(overrides)
    if "event_id" not in overrides:
        values["event_id"] = f"synthetic:{values['kind']}:{values['event_date'].isoformat()}:revision-1"
    return CalendarEvent(**values)


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
    event = _source_event()
    thresholds = scan_thresholds(regime, events=[event])
    cat_t = next(t for t in thresholds if t.name == "high_impact_catalyst_within_5d")
    assert cat_t.tripped is True


@pytest.mark.unit
def test_scan_thresholds_catalyst_not_tripped_beyond_5d():
    """A FOMC 8 days out should not trip the 5-day catalyst threshold."""
    regime = _make_regime()
    event = _source_event(
        event_date=date(2026, 4, 24),
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
    event = _source_event(
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
def test_build_posture_short_gamma_has_conditional_trend_scenario():
    regime = _make_regime(regime="SHORT_GAMMA")
    posture = build_posture(regime, events=[], thresholds=[])
    assert "trend" in posture["thesis"].lower()
    assert "Research scenario" in posture["thesis"]
    assert "no automatic trade recommendation" in posture["thesis"]


@pytest.mark.unit
def test_build_posture_long_gamma_has_conditional_pinning_scenario():
    regime = _make_regime(regime="LONG_GAMMA")
    posture = build_posture(regime, events=[], thresholds=[])
    assert "pinning" in posture["thesis"].lower()
    assert "Research scenario" in posture["thesis"]
    assert "do not establish hedge value or warrant a trade" in posture["thesis"]


@pytest.mark.unit
def test_build_posture_unknown_regime_withholds_conclusions():
    regime = GammaRegimeSnapshot(
        regime="UNKNOWN", spot=0.0, gamma_flip=None,
        put_wall=None, call_wall=None, aggregate_gex=0.0,
        snapshot_date="2026-04-14",
    )
    posture = build_posture(regime, events=[], thresholds=[])
    assert "withhold directional conclusions" in posture["thesis"].lower()
    assert "no automatic trade recommendation" in posture["thesis"]


@pytest.mark.unit
def test_build_posture_compound_risk_prepends_qualified_waterfall_watch():
    regime = _make_regime(regime="SHORT_GAMMA")
    compound = ForcedFlowThreshold(
        name="compound_regime_catalyst",
        description="test",
        current_value=1.0,
        threshold_value=1.0,
        tripped=True,
    )
    posture = build_posture(regime, events=[], thresholds=[compound])
    assert "WATERFALL WATCH" in posture["thesis"]
    assert "not confirmed flow or a calibrated probability" in posture["thesis"]
    assert "Review both evidence sources" in posture["thesis"]
    assert "Carry tail hedges" not in posture["thesis"]


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
    assert 0 <= briefing.waterfall_risk_score <= 2
    assert briefing.posture["thesis"]


@pytest.mark.unit
def test_check_gamma_regime_handles_missing_import(mock_engine):
    """When dealer_gamma import fails, returns UNKNOWN regime without raising."""
    with patch.dict("sys.modules", {"physics.dealer_gamma": None}):
        result = check_gamma_regime(mock_engine)
    assert result.regime == "UNKNOWN"


@pytest.mark.unit
def test_check_gamma_regime_uses_dealer_gamma_engine_contract(mock_engine):
    """Forced-flow monitor should call DealerGammaEngine with its real constructor."""
    summary = {
        "snap_date": "2026-04-14",
        "aggregate_gex": -123_000_000,
        "market_regime": "SHORT_GAMMA",
        "spy_gamma_flip": 501.0,
        "spy_put_wall": 490.0,
        "spy_call_wall": 512.0,
        "tickers": [{
            "ticker": "SPY",
            "spot": 500.0,
            "gex": -123_000_000,
            "regime": "SHORT_GAMMA",
            "gamma_flip": 501.0,
            "vanna": 0,
            "charm": 0,
        }],
    }

    with patch("physics.dealer_gamma.DealerGammaEngine") as engine_cls:
        engine_cls.return_value.get_market_gex_summary.return_value = summary
        result = check_gamma_regime(mock_engine)

    engine_cls.assert_called_once_with(db_engine=mock_engine)
    assert result.regime == "SHORT_GAMMA"
    assert result.spot == 500.0
    assert result.gamma_flip == 501.0


@pytest.mark.unit
def test_check_gamma_regime_unknown_when_spy_spot_unavailable(mock_engine):
    """get_market_gex_summary reporting no usable SPY row (the shape
    physics.dealer_gamma now returns when no measured spot exists) must map
    to regime UNKNOWN — never a fabricated NEUTRAL/0 regime."""
    summary = {
        "snap_date": "2026-09-24",
        "total_tickers": 0,
        "aggregate_gex": 0,
        "aggregate_vanna": 0,
        "aggregate_charm": 0,
        "long_gamma_count": 0,
        "short_gamma_count": 0,
        "market_regime": "UNKNOWN",
        "spy_spot": None,
        "spy_gamma_flip": None,
        "spy_put_wall": None,
        "spy_call_wall": None,
        "spy_gex": None,
        "tickers": [],
    }
    with patch("physics.dealer_gamma.DealerGammaEngine") as engine_cls:
        engine_cls.return_value.get_market_gex_summary.return_value = summary
        result = check_gamma_regime(mock_engine)

    assert result.regime == "UNKNOWN"
    assert result.gamma_flip is None
    assert result.put_wall is None
    assert result.call_wall is None


@pytest.mark.unit
def test_scan_thresholds_no_trip_when_regime_unknown_from_missing_data():
    """UNKNOWN regime (missing spot) must not trip any threshold that is
    driven by regime/flip/wall data — only a real calendar catalyst may."""
    regime = GammaRegimeSnapshot(
        regime="UNKNOWN", spot=0.0, gamma_flip=None,
        put_wall=None, call_wall=None, aggregate_gex=0.0,
        snapshot_date="2026-09-24",
    )
    thresholds = scan_thresholds(regime, events=[])
    tripped = {t.name for t in thresholds if t.tripped}
    assert tripped == set()


@pytest.mark.unit
def test_waterfall_cycle_no_alert_when_regime_unknown_from_missing_spot(mock_engine):
    """End-to-end: missing spot -> UNKNOWN regime -> zero tripped thresholds
    -> run_forced_flow_cycle never calls send_waterfall_alert."""
    unknown_regime = GammaRegimeSnapshot(
        regime="UNKNOWN", spot=0.0, gamma_flip=None,
        put_wall=None, call_wall=None, aggregate_gex=0.0,
        snapshot_date="2026-09-24",
    )
    with patch(
        "intelligence.forced_flow_monitor.check_gamma_regime",
        return_value=unknown_regime,
    ), patch(
        "intelligence.forced_flow_monitor.upcoming_calendar_events",
        return_value=[],
    ), patch(
        "intelligence.forced_flow_monitor.persist_briefing", return_value=1,
    ), patch(
        "alerts.waterfall_watch.send_waterfall_alert"
    ) as mock_alert:
        result = run_forced_flow_cycle(mock_engine)

    assert result["status"] == "ok"
    assert result["regime"] == "UNKNOWN"
    assert result["waterfall_risk_score"] == 0
    assert result["tripped_conditions"] == []
    mock_alert.assert_not_called()


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
    assert "0/0 checked" in subject
    assert "3 reported" in subject
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



def _email_briefing(regime=None, events=None):
    regime = regime or _make_regime(regime="SHORT_GAMMA")
    return MorningBriefing(
        generated_at="2026-04-14T06:30:00+00:00", regime=regime,
        upcoming_events=events or [], thresholds=scan_thresholds(regime, events or []),
        waterfall_risk_score=3,
        posture={"lever": "hedge <risk>", "condition": "x", "thesis": "all detail", "invalidation": "y"},
    )


def test_waterfall_summary_distinguishes_missing_from_clear_conditions():
    from alerts.waterfall_watch import _build_html_body
    unknown = GammaRegimeSnapshot("UNKNOWN", 0, None, None, None, 0, "2026-04-14")
    html = _build_html_body(_email_briefing(regime=unknown))
    assert "0/1 checked conditions tripped" in html
    assert "4 missing-input conditions" in html
    assert "not a probability" in html
    assert "Aggregate GEX:</strong> n/a" in html
    assert "MISSING" in html
    assert "hedge &lt;risk&gt;" in html


def test_waterfall_keeps_all_events_and_labels_old_unconfirmed_dates():
    from alerts.waterfall_watch import _build_html_body
    events = [CalendarEvent(date(2026, 4, 14+i), "FOMC", f"Event <{i}>", i) for i in range(8)]
    html = _build_html_body(_email_briefing(events=events))
    assert "Event &lt;7&gt;" in html
    assert "2026-04-21" in html
    assert "past date; outcome unconfirmed" in html
    assert "Snapshot age:" in html
    assert "not rechecked" in html
    assert "font-size:32px" in html


def test_waterfall_send_uses_same_subject_as_public_builder():
    from alerts.waterfall_watch import build_alert_subject, send_waterfall_alert
    from types import SimpleNamespace
    briefing = _email_briefing()
    with patch("alerts.email._get_settings", return_value=SimpleNamespace(ALERT_EMAIL_ENABLED=True)), patch("alerts.email._send") as send:
        assert send_waterfall_alert(briefing)
    assert send.call_args.args[0] == build_alert_subject(briefing)
    assert "checked conditions tripped" in send.call_args.args[1][0]["body"]



@pytest.mark.parametrize("value", [None, float("nan"), float("inf"), True, "bad"])
def test_waterfall_nonfinite_measurement_is_missing_not_passed(value):
    from alerts.waterfall_watch import _format_threshold_line
    line = _format_threshold_line({"name": "test <name>", "description": "<detail>",
                                   "current_value": value, "threshold_value": 1, "tripped": False})
    assert "MISSING" in line
    assert "&lt;name&gt;" in line and "&lt;detail&gt;" in line


def test_waterfall_future_event_remains_scheduled_and_dated():
    from alerts.waterfall_watch import _format_event_line
    from datetime import timedelta
    day = datetime.now(timezone.utc).date() + timedelta(days=3)
    line = _format_event_line({"event_date": day.isoformat(), "label": "FOMC", "trading_days_out": 3})
    assert day.isoformat() in line
    assert "scheduled; occurrence unconfirmed" in line
    assert "T+3 trading days from briefing" in line


def test_waterfall_missing_event_date_does_not_become_today():
    from alerts.waterfall_watch import _format_event_line
    line = _format_event_line({"label": "<event>"})
    assert "date unavailable" in line and "today" not in line
    assert "&lt;event&gt;" in line


def test_waterfall_disabled_does_not_send():
    from alerts.waterfall_watch import send_waterfall_alert
    from types import SimpleNamespace
    with patch("alerts.email._get_settings", return_value=SimpleNamespace(ALERT_EMAIL_ENABLED=False)), patch("alerts.email._send") as send:
        assert send_waterfall_alert(_email_briefing()) is False
    send.assert_not_called()



def test_waterfall_future_snapshot_and_upcoming_recap():
    from alerts.waterfall_watch import _build_html_body, _date, _format_threshold_line
    from dataclasses import replace
    from datetime import timedelta
    now = datetime.now(timezone.utc)
    future = now.date() + timedelta(days=3)
    assert _date(now) == now.date()
    regime = replace(_make_regime(), snapshot_date=future.isoformat())
    events = [CalendarEvent(future, "FOMC", "Next <event>", 3)]
    html = _build_html_body(_email_briefing(regime=regime, events=events))
    assert "Snapshot date is in the future" in html
    assert f"Next listed date: Next &lt;event&gt; on {future}" in html
    assert "MISSING" in _format_threshold_line({"name": "test", "current_value": 1,
                                                "threshold_value": None, "tripped": True})


# ── Calendar evidence and overlap regressions (synthetic only) ───────────


@pytest.mark.parametrize("start,end,expected", [
    (date(2026, 11, 25), date(2026, 11, 27), 1),  # Thanksgiving
    (date(2026, 6, 18), date(2026, 6, 22), 1),  # Juneteenth + weekend
    (date(2026, 12, 24), date(2026, 12, 28), 1),  # Christmas + weekend
    (date(2026, 4, 17), date(2026, 4, 17), 0),
    (date(2026, 4, 18), date(2026, 4, 18), 0),
    (date(2026, 4, 17), date(2026, 4, 16), -1),
])
def test_session_distances_use_canonical_holidays(start, end, expected):
    assert _trading_days_between(start, end) == expected


def test_quarter_end_estimate_skips_good_friday():
    assert _last_business_day(2024, 3) == date(2024, 3, 28)


@pytest.mark.parametrize("reference,expected", [
    (date(2025, 4, 14), date(2025, 4, 17)),  # third Friday is Good Friday
    (date(2026, 6, 15), date(2026, 6, 18)),  # third Friday is Juneteenth
])
def test_opex_holiday_adjustment_remains_an_estimate(reference, expected):
    events = upcoming_calendar_events(reference, 5)
    opex = next(event for event in events if event.kind.endswith("OPEX"))
    assert opex.event_date == expected
    assert opex.evidence_status == "calendar_estimate"
    assert "estimate" in opex.label
    assert opex.known_at is None


def test_calendar_window_is_exact_sessions_not_padded_calendar_days():
    assert upcoming_calendar_events(date(2026, 4, 13), 3) == []
    events = upcoming_calendar_events(date(2026, 4, 13), 4)
    assert [(event.kind, event.trading_days_out) for event in events] == [("MONTHLY_OPEX", 4)]
    assert upcoming_calendar_events(date(2026, 4, 17), 0)[0].trading_days_out == 0


def test_calendar_crosses_year_and_is_not_limited_to_three_months():
    events = upcoming_calendar_events(date(2026, 11, 2), 100)
    assert any(event.event_date == date(2027, 3, 19) for event in events)
    assert all(0 <= event.trading_days_out <= 100 for event in events)
    assert not any(event.kind == "FOMC" and event.event_date.year == 2027 for event in events)
    assert len({(event.event_date, event.kind) for event in events}) == len(events)


@pytest.mark.parametrize("lookahead", [-1, True, 1.5])
def test_invalid_calendar_window_rejected(lookahead):
    with pytest.raises(ValueError, match="nonnegative integer"):
        upcoming_calendar_events(DECISION.date(), lookahead)


@pytest.mark.parametrize("reference", [date(2026, 3, 16), date(2026, 3, 30), date(2026, 12, 7)])
def test_builtin_calendar_cannot_activate_catalyst_or_compound(reference):
    events = upcoming_calendar_events(reference, 10)
    assert events
    assert all(event.evidence_status in {"calendar_estimate", "static_unverified"} for event in events)
    assert all(event.source and event.evidence_note for event in events)
    assert not any(event.kind == "AUTOCALL_OBS" for event in events)
    as_of = datetime.combine(reference, DECISION.time(), tzinfo=timezone.utc)
    thresholds = scan_thresholds(_make_regime(regime="SHORT_GAMMA"), events, as_of=as_of)
    assert all(not threshold.tripped for threshold in thresholds if "catalyst" in threshold.name)


@pytest.mark.parametrize("changes", [
    {"evidence_status": "unverified"},
    {"evidence_status": "calendar_estimate"},
    {"evidence_status": "verified"},
    {"source": None}, {"source": ""}, {"source": "  "},
    {"event_id": None}, {"event_id": ""}, {"event_id": "  "},
    {"known_at": None},
    {"known_at": DECISION.replace(tzinfo=None)},
    {"known_at": DECISION + timedelta(microseconds=1)},
    {"trading_days_out": -1}, {"trading_days_out": 0}, {"trading_days_out": True},
    {"event_date": DECISION.date() - timedelta(days=1), "trading_days_out": 0},
    {"kind": "UNRECOGNIZED_EVENT"},
])
def test_unadmitted_schedule_or_untrusted_offset_cannot_trip(changes):
    event = replace(_source_event(), **changes)
    thresholds = scan_thresholds(_make_regime(regime="SHORT_GAMMA"), [event], as_of=DECISION)
    assert all(not threshold.tripped for threshold in thresholds if "catalyst" in threshold.name)


@pytest.mark.parametrize("known_at", [
    DECISION,
    DECISION.astimezone(timezone(timedelta(hours=5, minutes=30))),
])
def test_schedule_known_exactly_at_decision_is_admitted(known_at):
    event = _source_event(known_at=known_at)
    thresholds = scan_thresholds(_make_regime(regime="SHORT_GAMMA"), [event], as_of=DECISION)
    assert all(threshold.tripped for threshold in thresholds if "catalyst" in threshold.name)
    assert event.to_dict()["observed_flow_confirmed"] is False


def test_naive_decision_clock_is_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        scan_thresholds(_make_regime(), [_source_event()], as_of=DECISION.replace(tzinfo=None))


def test_nearest_qualified_event_selected_independently_of_input_order():
    later = _source_event(event_date=date(2026, 4, 21), trading_days_out=5)
    sooner = _source_event(event_date=date(2026, 4, 16), trading_days_out=2)
    thresholds = scan_thresholds(_make_regime(regime="SHORT_GAMMA"), [later, sooner], as_of=DECISION)
    catalyst = next(threshold for threshold in thresholds if threshold.name == "high_impact_catalyst_within_5d")
    compound = next(threshold for threshold in thresholds if threshold.name == "compound_regime_catalyst")
    assert catalyst.current_value == 2
    assert compound.tripped
    assert compound.to_dict()["diagnostic_only"] is True
    assert compound.to_dict()["score_group"] is None


def test_calendar_evidence_serialization_retains_receipt_and_limits():
    import json

    payload = _source_event().to_dict()
    assert payload["source"] == "synthetic:issuer_schedule_fixture"
    assert payload["event_id"] == "synthetic:JHEQX_ROLL:2026-04-17:revision-1"
    assert payload["known_at"] == (DECISION - timedelta(days=1)).isoformat()
    assert payload["evidence_status"] == "source_backed_schedule"
    assert "US equity sessions" in payload["distance_basis"]
    assert "flow is not established" in payload["evidence_note"]
    json.dumps(payload, allow_nan=False)


def test_legacy_event_constructor_is_unverified_and_nonactivating():
    event = CalendarEvent(date(2026, 4, 17), "JHEQX_ROLL", "Old caller", 3)
    assert event.to_dict()["evidence_status"] == "unverified"
    thresholds = scan_thresholds(_make_regime(regime="SHORT_GAMMA"), [event])
    assert not any(threshold.tripped for threshold in thresholds if "catalyst" in threshold.name)


def test_score_groups_are_bounded_and_conjunction_adds_nothing():
    from itertools import product

    names = [threshold.name for threshold in scan_thresholds(_make_regime(), [])]
    for states in product((False, True), repeat=len(names)):
        thresholds = [ForcedFlowThreshold(name, name, 1, 1, tripped)
                      for name, tripped in zip(names, states)]
        groups = _tripped_score_groups(thresholds)
        assert len(groups) == int(any(states[:3])) + int(states[3])
        assert len(_tripped_score_groups(thresholds + thresholds)) == len(groups)
    assert _tripped_score_groups([ForcedFlowThreshold("unknown", "test", 1, 1, True)]) == {}


@pytest.mark.parametrize("with_source_schedule,expected_score,expected_raw", [(False, 1, 3), (True, 2, 5)])
def test_briefing_groups_shared_positioning_and_preserves_details(
    mock_engine, with_source_schedule, expected_score, expected_raw,
):
    regime = _make_regime(regime="SHORT_GAMMA", flip=498, put_wall=501)
    events = [_source_event()] if with_source_schedule else []
    with patch("intelligence.forced_flow_monitor.check_gamma_regime", return_value=regime), patch(
        "intelligence.forced_flow_monitor.upcoming_calendar_events", return_value=events,
    ):
        briefing = build_morning_briefing(mock_engine)
    assert briefing.waterfall_risk_score == expected_score
    assert sum(threshold.tripped for threshold in briefing.thresholds) == expected_raw
    assert len(briefing.thresholds) == 5
    assert briefing.posture["tripped_count"] == str(expected_raw)
    assert briefing.posture["tripped_group_count"] == str(expected_score)
    payload = briefing.to_dict()
    assert payload["waterfall_score_method"] == GROUPED_SCORE_METHOD
    assert payload["waterfall_score_max"] == 2
    assert len(payload["tripped_groups"]) == expected_score
    assert "not independent votes or calibrated probability" in payload["score_limitations"]


@pytest.mark.parametrize("source_backed", [False, True])
def test_cycle_only_alerts_with_both_groups_not_raw_overlap(mock_engine, source_backed):
    regime = _make_regime(regime="SHORT_GAMMA", flip=498, put_wall=501)
    event = _source_event() if source_backed else replace(_source_event(), evidence_status="calendar_estimate")
    with patch("intelligence.forced_flow_monitor.check_gamma_regime", return_value=regime), patch(
        "intelligence.forced_flow_monitor.upcoming_calendar_events", return_value=[event],
    ), patch("intelligence.forced_flow_monitor.persist_briefing", return_value=1), patch(
        "alerts.waterfall_watch.send_waterfall_alert",
    ) as send:
        result = run_forced_flow_cycle(mock_engine)
    assert result["waterfall_risk_score"] == 1 + source_backed
    assert result["waterfall_score_method"] == GROUPED_SCORE_METHOD
    assert send.call_count == int(source_backed)


def test_posture_does_not_turn_estimate_or_omission_into_confirmed_flow():
    regime = _make_regime()
    events = upcoming_calendar_events(date(2026, 3, 30), 2)
    condition = build_posture(regime, events, [])["condition"]
    assert "calendar_estimate" in condition
    assert "not a confirmed fund roll" in condition
    assert "pinning force dissolves" not in condition
    assert "coverage is incomplete" in build_posture(regime, [], [])["condition"]


def test_legacy_briefing_score_is_not_relabelled_as_grouped():
    payload = _email_briefing().to_dict()
    assert payload["waterfall_score_method"] == "unspecified_legacy"
    assert "waterfall_score_max" not in payload
    assert "tripped_groups" not in payload


def test_persist_retains_version_groups_and_calendar_receipt_in_existing_json(mock_engine):
    import json

    regime = _make_regime(regime="SHORT_GAMMA", flip=498, put_wall=501)
    with patch("intelligence.forced_flow_monitor.check_gamma_regime", return_value=regime), patch(
        "intelligence.forced_flow_monitor.upcoming_calendar_events", return_value=[_source_event()],
    ):
        briefing = build_morning_briefing(mock_engine)
    # Entire engine is an in-memory call recorder, including DDL. No DB connection.
    synthetic_engine = MagicMock()
    connection = synthetic_engine.begin.return_value.__enter__.return_value
    connection.execute.return_value.fetchone.return_value = (42,)
    assert persist_briefing(synthetic_engine, briefing) == 42
    statement, params = connection.execute.call_args.args
    assert ":full" in str(statement)
    assert params["risk"] == 2
    assert len(params["tripped"]) == 5
    payload = json.loads(params["full"])
    assert payload["waterfall_score_method"] == GROUPED_SCORE_METHOD
    assert len(payload["tripped_groups"]) == 2
    assert payload["upcoming_events"][0]["event_id"] == _source_event().event_id
    assert json.loads(params["events"])[0]["known_at"] == _source_event().known_at.isoformat()


def _grouped_email_briefing(count: int) -> MorningBriefing:
    regime = (_make_regime(regime="SHORT_GAMMA", flip=498, put_wall=501)
              if count else _make_regime(flip=470, put_wall=490))
    events = [_source_event()] if count == 2 else []
    thresholds = scan_thresholds(regime, events, as_of=DECISION)
    return MorningBriefing(
        generated_at=DECISION.isoformat(), regime=regime, upcoming_events=events,
        thresholds=thresholds, waterfall_risk_score=count,
        posture=build_posture(regime, events, thresholds), score_method=GROUPED_SCORE_METHOD,
    )


@pytest.mark.parametrize("count,raw", [(0, 0), (1, 3), (2, 5)])
def test_grouped_renderer_separates_group_count_from_raw_diagnostics(count, raw):
    from alerts.waterfall_watch import _build_html_body, build_alert_subject

    briefing = _grouped_email_briefing(count)
    before = briefing.to_dict()
    html = _build_html_body(briefing)
    subject = build_alert_subject(briefing)
    assert f"{count}/2 evidence groups tripped" in html
    assert f"Raw diagnostics: {raw}/5 checked conditions tripped" in html
    assert f"{count}/2 evidence groups" in subject
    assert f"{raw}/5 raw checks tripped" in subject
    assert "Raw Condition Diagnostics" in html
    assert "Reported score differs" not in html
    assert "shared" in html.lower() and "compound check adds no group" in html
    assert "not independent voting or a calibrated probability" in html
    assert "Built-in calendar estimates cannot activate a two-group alert" in html
    assert "different scale and are not directly comparable" in html
    if count < 2:
        assert "NO QUALIFIED SCHEDULE" in html
        assert "NO LISTED EVENT" not in html
    assert briefing.to_dict() == before  # presentation never rescored/mutated history


@pytest.mark.parametrize("count,accent,color", [(0, "amber", "#F59E0B"), (1, "amber", "#F59E0B"), (2, "red", "#EF4444")])
def test_grouped_renderer_styles_on_its_own_two_group_scale(count, accent, color):
    from alerts.waterfall_watch import send_waterfall_alert

    with patch("alerts.email._get_settings", return_value=SimpleNamespace(ALERT_EMAIL_ENABLED=True)), patch(
        "alerts.email._send",
    ) as send:
        assert send_waterfall_alert(_grouped_email_briefing(count)) is True
    section = send.call_args.args[1][0]
    assert section["accent"] == accent
    assert f"font-size:32px;font-weight:800;color:{color};" in section["body"]


@pytest.mark.parametrize("method", ["unspecified_legacy", None, "absent"])
def test_legacy_renderer_keeps_raw_headline_mismatch_and_style(method):
    from alerts.waterfall_watch import _build_html_body, build_alert_subject, send_waterfall_alert

    briefing = SimpleNamespace(**vars(_email_briefing()))
    if method == "absent":
        del briefing.score_method
    else:
        briefing.score_method = method
    html = _build_html_body(briefing)
    assert "2/5 checked conditions tripped" in html
    assert "Reported score differs" in html
    assert "Raw Condition Diagnostics" not in html
    assert "evidence groups tripped" not in html
    assert "2/5 checked tripped" in build_alert_subject(briefing)
    assert "3 reported" in build_alert_subject(briefing)
    with patch("alerts.email._get_settings", return_value=SimpleNamespace(ALERT_EMAIL_ENABLED=True)), patch(
        "alerts.email._send",
    ) as send:
        assert send_waterfall_alert(briefing) is True
    assert send.call_args.args[1][0]["accent"] == "red"


@pytest.mark.parametrize("value", [-1, 3, True, None, float("nan")])
def test_invalid_grouped_count_is_unavailable_not_displayed_as_legacy(value):
    from alerts.waterfall_watch import _alert_accent, _build_html_body, build_alert_subject

    briefing = replace(_grouped_email_briefing(1), waterfall_risk_score=value)
    html = _build_html_body(briefing)
    assert "Evidence-group count unavailable" in html
    assert "reported grouped count is invalid or missing" in html
    assert "Reported score differs" not in html
    assert "group count unavailable" in build_alert_subject(briefing)
    assert _alert_accent(briefing) == "amber"


def test_grouped_presentation_does_not_activate_below_threshold_or_disabled_email():
    from alerts.waterfall_watch import send_waterfall_alert, send_waterfall_alert_if_triggered

    with patch("alerts.email._send") as send:
        assert send_waterfall_alert_if_triggered(_grouped_email_briefing(1)) is False
        assert send_waterfall_alert(_grouped_email_briefing(2)) is False
    send.assert_not_called()


@pytest.mark.parametrize("missing_spot", [None, 0, -1, True, float("nan"), float("inf"), "bad"])
def test_nonzero_walls_cannot_fabricate_missing_spot(mock_engine, missing_spot):
    summary = {
        "snap_date": "2026-04-14", "market_regime": "SHORT_GAMMA", "aggregate_gex": -1e9,
        "spy_gamma_flip": 500.0, "spy_put_wall": 490.0, "spy_call_wall": 510.0,
        "tickers": [{"ticker": "SPY", "spot": missing_spot}],
    }
    with patch("physics.dealer_gamma.DealerGammaEngine") as engine_cls:
        engine_cls.return_value.get_market_gex_summary.return_value = summary
        regime = check_gamma_regime(mock_engine)
    assert regime.regime == "UNKNOWN"
    assert regime.spot == 0.0  # unavailable sentinel, never a quote at midpoint 500
    assert regime.gamma_flip is None and regime.put_wall is None and regime.call_wall is None
    thresholds = scan_thresholds(regime, [_source_event()], as_of=DECISION)
    assert _tripped_score_groups(thresholds) == {"calendar_catalyst": ["high_impact_catalyst_within_5d"]}
    assert "Regime unknown" in build_posture(regime, [], thresholds)["lever"]


@pytest.mark.parametrize("row_spot,summary_spot,expected", [
    (502.0, 503.0, 502.0), (None, 503.0, 503.0), (float("nan"), 503.0, 503.0),
    (502.0, None, 502.0), ("502.0", None, 502.0),
])
def test_explicit_spot_sources_remain_usable(mock_engine, row_spot, summary_spot, expected):
    summary = {
        "snap_date": "2026-04-14", "market_regime": "SHORT_GAMMA", "aggregate_gex": -1e9,
        "spy_gamma_flip": 500.0, "spy_put_wall": 490.0, "spy_call_wall": 510.0,
        "spy_spot": summary_spot, "tickers": [{"ticker": "SPY", "spot": row_spot}],
    }
    with patch("physics.dealer_gamma.DealerGammaEngine") as engine_cls:
        engine_cls.return_value.get_market_gex_summary.return_value = summary
        regime = check_gamma_regime(mock_engine)
    assert regime.spot == expected
    assert regime.regime == "SHORT_GAMMA"


@pytest.mark.parametrize("instant,new_york_date,event_day,offset_hours", [
    (datetime(2026, 1, 15, 1, 30, tzinfo=timezone.utc), date(2026, 1, 14), date(2026, 1, 16), -5),
    (datetime(2026, 7, 15, 0, 30, tzinfo=timezone.utc), date(2026, 7, 14), date(2026, 7, 16), -4),
    (datetime(2026, 3, 9, 0, 30, tzinfo=timezone.utc), date(2026, 3, 8), date(2026, 3, 10), -4),
])
def test_schedule_admission_and_briefing_share_new_york_date(
    mock_engine, instant, new_york_date, event_day, offset_hours,
):
    from zoneinfo import ZoneInfo

    assert instant.date() != new_york_date
    assert instant.astimezone(ZoneInfo("America/New_York")).utcoffset() == timedelta(hours=offset_hours)
    event = _source_event(event_date=event_day, trading_days_out=2, known_at=instant - timedelta(days=2))
    regime = _make_regime(regime="SHORT_GAMMA")
    with patch("intelligence.forced_flow_monitor._utc_now", return_value=instant), patch(
        "intelligence.forced_flow_monitor.check_gamma_regime", return_value=regime,
    ), patch("intelligence.forced_flow_monitor.upcoming_calendar_events", return_value=[event]) as calendar:
        briefing = build_morning_briefing(mock_engine)
    calendar.assert_called_once_with(ref_date=new_york_date)
    assert briefing.waterfall_risk_score == 2
    utc_offset_event = replace(event, trading_days_out=1)
    thresholds = scan_thresholds(regime, [utc_offset_event], as_of=instant)
    assert not any(threshold.tripped for threshold in thresholds if "catalyst" in threshold.name)


def test_default_calendar_reference_uses_new_york_after_utc_midnight():
    instant = datetime(2026, 4, 18, 0, 30, tzinfo=timezone.utc)  # Friday evening in NY
    with patch("intelligence.forced_flow_monitor._utc_now", return_value=instant):
        events = upcoming_calendar_events(lookahead_days=0)
    assert [(event.kind, event.event_date, event.trading_days_out) for event in events] == [
        ("MONTHLY_OPEX", date(2026, 4, 17), 0),
    ]
