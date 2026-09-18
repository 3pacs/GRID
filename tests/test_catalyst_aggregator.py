"""ALPHA-4 — tests for intelligence/catalyst_aggregator.py.

The DB-touching paths (_read_earnings_events, _read_clinical_events) are
mocked here. Pure-function paths (OPEX dates, proximity_score math, dedupe,
sort order) are tested directly.

Audit C-H3 removed the module's hand-typed FOMC calendar: GRID ingests no
macro event calendar, so no FOMC event is seeded any more. The tests that
used to lean on those dates now use OPEX (computed from the expiry
convention) or a mocked earnings row instead — see
tests/test_derivatives_catalysts_honesty.py for the honesty guards.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock


from intelligence.catalyst_aggregator import (
    CATALYST_EARNINGS,
    CATALYST_FOMC,
    CATALYST_OPEX_MONTHLY,
    CATALYST_OPEX_QUARTERLY,
    _opex_dates_for_year,
    _seeded_market_events,
    _third_friday,
    events_for_window,
    nearest_catalyst,
    proximity_score,
    upcoming_catalysts_summary,
)


# ── Pure-function helpers ──────────────────────────────────────────────────


class TestThirdFriday:
    def test_january_2026(self):
        # 2026-01-01 is Thursday → first Friday Jan 2 → third Friday Jan 16
        assert _third_friday(2026, 1) == date(2026, 1, 16)

    def test_april_2026(self):
        # 2026-04-01 is Wednesday → first Friday Apr 3 → third Friday Apr 17
        assert _third_friday(2026, 4) == date(2026, 4, 17)

    def test_december_2026(self):
        # 2026-12-01 is Tuesday → first Friday Dec 4 → third Friday Dec 18
        assert _third_friday(2026, 12) == date(2026, 12, 18)


class TestOpexDates:
    def test_year_has_twelve_dates(self):
        dates = _opex_dates_for_year(2026)
        assert len(dates) == 12

    def test_quarterly_tagged(self):
        dates = _opex_dates_for_year(2026)
        quarterlies = [d for d, kind in dates if kind == CATALYST_OPEX_QUARTERLY]
        assert len(quarterlies) == 4
        # Months 3, 6, 9, 12
        months = sorted(d.month for d in quarterlies)
        assert months == [3, 6, 9, 12]

    def test_monthly_count(self):
        dates = _opex_dates_for_year(2026)
        monthlies = [d for d, kind in dates if kind == CATALYST_OPEX_MONTHLY]
        assert len(monthlies) == 8


class TestSeededMarketEvents:
    def test_no_fomc_is_seeded(self):
        """C-H3: the hand-typed Fed calendar is gone, nothing replaced it."""
        events = _seeded_market_events(start=date(2026, 1, 1), end=date(2026, 12, 31))
        assert [e for e in events if e.event_type == CATALYST_FOMC] == []

    def test_window_with_no_opex_is_empty(self):
        events = _seeded_market_events(start=date(2026, 5, 1), end=date(2026, 5, 5))
        assert events == []

    def test_opex_present(self):
        events = _seeded_market_events(start=date(2026, 1, 1), end=date(2026, 12, 31))
        opex = [e for e in events if e.event_type in (CATALYST_OPEX_MONTHLY, CATALYST_OPEX_QUARTERLY)]
        assert len(opex) == 12

    def test_market_wide_ticker_is_none(self):
        events = _seeded_market_events(start=date(2026, 4, 1), end=date(2026, 4, 30))
        for e in events:
            assert e.ticker is None
            assert e.is_market_wide


# ── DB-mocked aggregator ───────────────────────────────────────────────────


def _mock_engine(earnings_rows=None, clinical_rows=None):
    """Build a MagicMock engine that returns the specified rows."""
    eng = MagicMock()

    def execute_router(query, **kwargs):
        sql = str(query)
        result = MagicMock()
        if "earnings_calendar" in sql:
            result.fetchall.return_value = earnings_rows or []
        elif "catalyst_calendar" in sql:
            result.fetchall.return_value = clinical_rows or []
        else:
            result.fetchall.return_value = []
        return result

    conn = MagicMock()
    conn.execute = execute_router
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    eng.connect.return_value = conn
    return eng


class TestEventsForWindow:
    def test_empty_window(self):
        eng = _mock_engine()
        events = events_for_window(
            eng, start=date(2026, 5, 1), end=date(2026, 5, 5),
        )
        # No FOMC + no OPEX in this window → empty
        assert events == []

    def test_includes_market_wide_for_ticker_filter(self):
        # Earnings row mock: (ticker, earnings_date, fiscal_quarter)
        eng = _mock_engine(earnings_rows=[
            ("AAPL", date(2026, 4, 25), "FQ2"),
        ])
        events = events_for_window(
            eng, start=date(2026, 4, 1), end=date(2026, 4, 30),
            ticker="AAPL",
        )
        # Should include the earnings row + April OPEX (market-wide).
        # No macro event: that calendar is not ingested.
        assert any(e.ticker == "AAPL" for e in events)
        assert any(e.event_type == CATALYST_OPEX_MONTHLY for e in events)
        assert all(e.event_type != CATALYST_FOMC for e in events)

    def test_ticker_filter_excludes_other_tickers(self):
        eng = _mock_engine(earnings_rows=[
            ("AAPL", date(2026, 4, 25), "FQ2"),
            ("MSFT", date(2026, 4, 24), "FQ3"),
        ])
        events = events_for_window(
            eng, start=date(2026, 4, 20), end=date(2026, 4, 30),
            ticker="AAPL",
        )
        tickers = {e.ticker for e in events if e.ticker is not None}
        assert "AAPL" in tickers
        assert "MSFT" not in tickers

    def test_event_type_whitelist(self):
        eng = _mock_engine(earnings_rows=[
            ("AAPL", date(2026, 4, 25), "FQ2"),
        ])
        events = events_for_window(
            eng, start=date(2026, 4, 1), end=date(2026, 4, 30),
            event_types=[CATALYST_EARNINGS],
        )
        assert all(e.event_type == CATALYST_EARNINGS for e in events)

    def test_sort_order_priority(self):
        # Same-day quarterly OPEX and earnings → earnings ranks higher
        eng = _mock_engine(earnings_rows=[
            ("AAPL", date(2026, 3, 20), "FQ2"),  # same day as March OPEX
        ])
        events = events_for_window(
            eng, start=date(2026, 3, 20), end=date(2026, 3, 20),
        )
        assert [e.event_type for e in events] == [
            CATALYST_EARNINGS, CATALYST_OPEX_QUARTERLY,
        ]


# ── proximity_score ────────────────────────────────────────────────────────


class TestProximityScore:
    def test_no_events_zero_score(self):
        eng = _mock_engine()
        result = proximity_score(eng, "ZZZ", as_of=date(2026, 5, 1), horizon_days=5)
        assert result["score"] == 0.0
        assert result["nearest"] is None
        assert result["days_to_event"] is None
        assert result["window_density"] == 0

    def test_imminent_catalyst_high_score(self):
        eng = _mock_engine(earnings_rows=[
            ("AAPL", date(2026, 5, 5), "FQ2"),
        ])
        result = proximity_score(eng, "AAPL", as_of=date(2026, 5, 5), horizon_days=7)
        # Earnings at d=0, impact 0.85 → score is the raw impact
        assert result["score"] >= 0.80
        assert result["catalyst_type"] == CATALYST_EARNINGS
        assert result["days_to_event"] == 0

    def test_decay_with_distance(self):
        eng = _mock_engine(earnings_rows=[
            ("AAPL", date(2026, 5, 9), "FQ2"),
        ])
        # Five days before the earnings date; no OPEX inside the window
        result = proximity_score(eng, "AAPL", as_of=date(2026, 5, 4), horizon_days=5)
        # 0.85 × exp(-1.0) ≈ 0.313
        assert 0.25 < result["score"] < 0.40

    def test_density_bump(self):
        # Two earnings rows + nothing else → window_density bumps the score
        eng = _mock_engine(earnings_rows=[
            ("AAPL", date(2026, 5, 5), "FQ2"),
            ("AAPL", date(2026, 5, 6), "FQ3"),  # silly but valid for the test
        ])
        result = proximity_score(eng, "AAPL", as_of=date(2026, 5, 4), horizon_days=10)
        # Density bump of (2 - 1) * 0.05 = 0.05 added on top
        assert result["window_density"] == 2

    def test_score_capped_at_one(self):
        eng = _mock_engine(earnings_rows=[
            ("AAPL", date(2026, 4, 17), "FQ2"),  # same day as April OPEX
            ("AAPL", date(2026, 4, 20), "FQ3"),
        ])
        result = proximity_score(eng, "AAPL", as_of=date(2026, 4, 17), horizon_days=10)
        assert result["score"] <= 1.0


# ── nearest_catalyst ───────────────────────────────────────────────────────


class TestNearestCatalyst:
    def test_returns_first_event(self):
        eng = _mock_engine(earnings_rows=[
            ("AAPL", date(2026, 4, 25), "FQ2"),
        ])
        nearest = nearest_catalyst(eng, "AAPL", as_of=date(2026, 4, 20), horizon_days=10)
        assert nearest is not None
        assert nearest.event_type == CATALYST_EARNINGS

    def test_none_when_empty(self):
        eng = _mock_engine()
        nearest = nearest_catalyst(eng, "ZZZ", as_of=date(2026, 5, 1), horizon_days=3)
        assert nearest is None


# ── upcoming_catalysts_summary ─────────────────────────────────────────────


class TestUpcomingSummary:
    def test_summary_shape(self):
        eng = _mock_engine(earnings_rows=[
            ("AAPL", date(2026, 4, 25), "FQ2"),
            ("MSFT", date(2026, 4, 26), "FQ3"),
        ])
        summary = upcoming_catalysts_summary(eng, as_of=date(2026, 4, 20), horizon_days=14)
        assert "total" in summary
        assert "by_type" in summary
        assert "next" in summary
        assert summary["horizon_days"] == 14
