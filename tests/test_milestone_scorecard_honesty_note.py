"""GET /intelligence/milestones/scorecard must not silently imply its
AlphaVantage inputs (av:earnings:*:eps, av:income:*) are fresh.

Both AlphaVantage sources are inactive in source_catalog (ids 6, 65 — Wave 3
triage report item #13), so ``scan_all_tickers`` scans a live-but-frozen
input every time this GET is hit. This test pins the honest ``note`` field
the route now adds, following the house convention in
``api/routers/intelligence_causation.py`` (``generated``/``as_of``/``note``
style fields instead of silence).
"""

from __future__ import annotations

import asyncio
from datetime import date
from unittest.mock import MagicMock, patch

from api.routers import intelligence_actors as ia


def _engine_with_max_obs_date(max_obs_date):
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchone.return_value = (max_obs_date,)
    return engine


class TestMilestoneScorecardHonestyNote:
    def test_note_reports_the_max_obs_date_of_the_inactive_av_sources(self):
        engine = _engine_with_max_obs_date(date(2026, 4, 11))
        with patch.object(ia, "get_db_engine", return_value=engine), \
             patch("intelligence.milestone_tracker.scan_all_tickers", return_value=[]):
            out = asyncio.run(ia.get_milestone_scorecard(_token="t"))

        assert out["note"] == "AlphaVantage inputs inactive as of 2026-04-11"

    def test_note_is_honest_when_no_successful_pull_exists(self):
        engine = _engine_with_max_obs_date(None)
        with patch.object(ia, "get_db_engine", return_value=engine), \
             patch("intelligence.milestone_tracker.scan_all_tickers", return_value=[]):
            out = asyncio.run(ia.get_milestone_scorecard(_token="t"))

        assert "no successful pull on record" in out["note"]

    def test_note_degrades_gracefully_if_the_freshness_query_fails(self):
        engine = MagicMock()
        engine.connect.side_effect = RuntimeError("db down")
        with patch.object(ia, "get_db_engine", return_value=engine), \
             patch("intelligence.milestone_tracker.scan_all_tickers", return_value=[]):
            out = asyncio.run(ia.get_milestone_scorecard(_token="t"))

        assert "could not be determined" in out["note"]
        # The scorecard itself must still be served, not swallowed as an error.
        assert out["companies"] == []
        assert out["count"] == 0

    def test_scorecard_still_returns_companies_and_count(self):
        engine = _engine_with_max_obs_date(date(2026, 4, 11))
        fake_companies = [{"ticker": "AAPL", "grade": "B"}]
        with patch.object(ia, "get_db_engine", return_value=engine), \
             patch("intelligence.milestone_tracker.scan_all_tickers", return_value=fake_companies):
            out = asyncio.run(ia.get_milestone_scorecard(_token="t"))

        assert out["companies"] == fake_companies
        assert out["count"] == 1
