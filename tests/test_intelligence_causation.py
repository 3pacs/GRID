"""Tests for GET /api/v1/intelligence/causal-links (Timeline / Causal Map overlay).

Since slice N2 the route only reads persisted, provenance-bearing rows through
``intelligence.causal_links.read_links_payload``. The real SQL is exercised
against PostgreSQL in ``tests/test_causal_links_pg.py``.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

from intelligence import causal_links as cl


def _engine():
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


def _row(**over):
    row = {
        "id": 7, "edge_key": "k" * 64, "signal_id": 42, "actor": "Jane Doe", "ticker": "AAPL",
        "action": "SELL", "action_channel": "form4", "action_date": date(2026, 9, 15),
        "action_known_at": datetime(2026, 9, 17, tzinfo=timezone.utc),
        "action_known_at_basis": "filing", "cause_type": "earnings",
        "probable_cause": "Earnings beat released 2026-09-10 (1.10 vs 1.00 est)",
        "event_kind": "earnings", "event_key": "earnings:AAPL:2026-09-10",
        "event_date": date(2026, 9, 10),
        "event_known_at": datetime(2026, 9, 11, tzinfo=timezone.utc),
        "event_known_at_basis": "release_date",
        "known_at": datetime(2026, 9, 17, tzinfo=timezone.utc), "lead_time_days": 4.0,
        "probability": 0.647, "score_method": cl.SCORE_METHOD,
        "evidence": '[{"type": "earnings"}]', "run_id": "r2", "first_run_id": "r1",
        "code_sha": "abc123", "computed_at": datetime(2026, 9, 27, 7, 41, tzinfo=timezone.utc),
    }
    row.update(over)
    return row


_RUN = {
    "run_id": "r2", "as_of": "2026-09-27T07:40:00+00:00",
    "finished_at": "2026-09-27T07:41:00+00:00", "code_sha": "abc123",
    "edges_written": 1, "tickers_processed": 1,
}


def _call(ticker="aapl", days=90, schema=True, run=_RUN, rows=()):
    from api.routers.intelligence_causation import get_causal_links

    engine, _ = _engine()
    with patch("api.routers.intelligence_causation.get_db_engine", return_value=engine), \
         patch.object(cl, "schema_ready", return_value=schema), \
         patch.object(cl, "latest_run", return_value=run), \
         patch.object(cl, "read_links", return_value=list(rows)) as read:
        out = get_causal_links(ticker=ticker, days=days, _token="t")
    return out, read


class TestCausalLinksEndpoint:
    def test_router_uses_facade_relative_prefix(self):
        from api.routers.intelligence_causation import router

        assert router.prefix == ""

    def test_schema_missing_is_honest_not_generated(self):
        out, read = _call(schema=False)
        assert out["generated"] is False
        assert out["links"] == []
        assert out["as_of"] is None
        assert "not scheduled" in out["reason"]
        read.assert_not_called()

    def test_no_finished_run_is_not_generated(self):
        out, _ = _call(run=None, rows=[_row()])
        assert out["generated"] is False
        assert out["links"] == []

    def test_links_carry_as_of_and_provenance(self):
        out, read = _call(rows=[_row()])
        assert out["ticker"] == "AAPL"
        assert read.call_args.kwargs["ticker"] == "AAPL"
        assert out["generated"] is True
        assert out["as_of"] == _RUN["as_of"]
        assert out["last_run"]["code_sha"] == "abc123"
        link = out["links"][0]
        # Arrow runs from the public event to the later trade.
        assert link["cause_date"] == "2026-09-10"
        assert link["effect_date"] == "2026-09-15"
        assert link["effect_description"] == "Jane Doe SELL AAPL"
        assert link["known_at"].startswith("2026-09-17")
        assert link["event_known_at_basis"] == "release_date"
        assert link["run_id"] == "r2" and link["first_run_id"] == "r1"
        assert link["score"] == 0.647
        assert link["score_is_probability"] is False
        assert link["evidence"] == [{"type": "earnings"}]
        assert "Price reaction" not in str(out)

    def test_db_error_returns_graceful_error(self):
        from api.routers.intelligence_causation import get_causal_links

        engine = MagicMock()
        engine.connect.side_effect = RuntimeError("db down")
        with patch("api.routers.intelligence_causation.get_db_engine", return_value=engine):
            out = get_causal_links(ticker="SPY", days=30, _token="t")
        assert out["links"] == [] and out["generated"] is False
        assert out["error"] == "causal links unavailable"


class TestCausationGet:
    def test_causation_get_reads_persisted_links_without_narrative(self):
        from api.routers import intelligence_forensics as f

        engine, _ = _engine()
        with patch.object(f, "get_db_engine", return_value=engine), \
             patch.object(cl, "schema_ready", return_value=True), \
             patch.object(cl, "latest_run", return_value=_RUN), \
             patch.object(cl, "read_links", return_value=[_row()]):
            out = f.get_causation(ticker="aapl", days=30, _token="t")
        assert out["ticker"] == "AAPL"
        assert out["narrative"] is None
        assert out["as_of"] == _RUN["as_of"]
        assert out["total_causes"] == 1
        cause = out["causes"][0]
        assert cause["ticker"] == "AAPL" and cause["probable_cause"].startswith("Earnings beat")
        assert cause["score_method"] == cl.SCORE_METHOD
