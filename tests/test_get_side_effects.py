"""GET routes must not write: briefing, thesis, causation, forensics, watchlist.

Unit-level pins (no PostgreSQL). The real-PostgreSQL SELECT-only proof lives
in ``tests/test_get_side_effects_readonly_pg.py``.
"""

from __future__ import annotations

import asyncio
import os
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("DB_PASSWORD", "testpass")

from api.auth import require_auth  # noqa: E402


def _client(router, prefix: str = "") -> TestClient:
    app = FastAPI()
    app.include_router(router, prefix=prefix)
    app.dependency_overrides[require_auth] = lambda: "test"
    return TestClient(app)


# ── flows/briefing ────────────────────────────────────────────────────────


class TestFlowBriefing:
    def _client(self):
        from api.routers import flows
        return _client(flows.router)

    def test_get_never_generates_and_reports_not_generated(self):
        forbid = MagicMock(side_effect=AssertionError("GET must not generate"))
        with patch("intelligence.audio_briefing.get_latest_briefing", return_value=None), \
             patch("intelligence.audio_briefing.generate_briefing_audio", forbid), \
             patch("intelligence.audio_briefing.generate_briefing_script", forbid):
            response = self._client().get("/api/v1/flows/briefing?audio=true")
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "not_generated"
        assert body["briefing"] is None
        forbid.assert_not_called()

    def test_get_returns_latest_saved_briefing(self):
        from intelligence.audio_briefing import BriefingResult

        latest = BriefingResult(
            script_text="saved", audio_path="/x/briefing_2026-09-25.mp3",
            briefing_date="2026-09-25", generated_at="2026-09-25T12:00:00+00:00",
        )
        forbid = MagicMock(side_effect=AssertionError("GET must not generate"))
        with patch("intelligence.audio_briefing.get_latest_briefing", return_value=latest), \
             patch("intelligence.audio_briefing.generate_briefing_audio", forbid):
            body = self._client().get("/api/v1/flows/briefing").json()
        assert body["status"] == "SUCCESS"
        assert body["briefing"]["briefing_date"] == "2026-09-25"
        forbid.assert_not_called()

    def test_post_is_the_explicit_generator(self):
        from intelligence.audio_briefing import BriefingResult

        made = BriefingResult(script_text="new", audio_path="/x/briefing_new.mp3")
        with patch("intelligence.audio_briefing.generate_briefing_audio", return_value=made) as gen, \
             patch("api.routers.flows.get_db_engine", return_value=MagicMock()):
            body = self._client().post("/api/v1/flows/briefing").json()
        assert body["status"] == "SUCCESS"
        assert body["briefing"]["audio_path"] == "/x/briefing_new.mp3"
        gen.assert_called_once()


# ── intelligence/thesis ───────────────────────────────────────────────────


def test_thesis_get_does_not_snapshot():
    from api.routers import intelligence_thesis

    intelligence_thesis._thesis_cache.clear()
    thesis = {
        "direction": "NEUTRAL", "bull_pct": 0, "bear_pct": 0, "active_models": 0,
        "models": [], "score": 0, "conviction": 0,
    }
    with patch("analysis.thesis_scorer.score_thesis", return_value=thesis), \
         patch("analysis.thesis_scorer.snapshot_thesis") as snap, \
         patch("analysis.thesis_scorer._build_narrative", return_value="n"), \
         patch.object(intelligence_thesis, "get_db_engine", return_value=MagicMock()):
        body = asyncio.run(intelligence_thesis.get_unified_thesis(_token="t"))
    intelligence_thesis._thesis_cache.clear()
    assert body["overall_direction"] == "NEUTRAL"
    snap.assert_not_called()


# ── causation / causal chains ─────────────────────────────────────────────


def _engine_with_rows(rows):
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = rows
    return engine


class TestCausationPersistFlag:
    def test_batch_find_causes_read_only_when_not_persisting(self):
        from intelligence import causation_scoring as cs

        summary = MagicMock(edges=[], actions_processed=0, status="succeeded")
        with patch.object(cs._cl, "run_causal_links", return_value=summary) as run:
            out = cs.batch_find_causes(MagicMock(), days=30, persist=False)
        assert out == []
        assert run.call_args.kwargs["dry_run"] is True

    def test_batch_find_causes_default_still_persists(self):
        from intelligence import causation_scoring as cs

        summary = MagicMock(edges=[], actions_processed=0, status="succeeded")
        with patch.object(cs._cl, "run_causal_links", return_value=summary) as run:
            cs.batch_find_causes(MagicMock(), days=30)
        assert run.call_args.kwargs["dry_run"] is False

    def test_trace_and_detect_skip_ddl_when_read_only(self):
        from intelligence import causation_graph as cg

        with patch.object(cg, "ensure_table") as ensure, \
             patch.object(cg, "_store_chains") as store:
            cg.trace_causal_chain(_engine_with_rows([]), "AAA", persist=False)
            cg.detect_chain_in_progress(_engine_with_rows([]))
        ensure.assert_not_called()
        store.assert_not_called()

    def test_find_longest_chains_threads_persist(self):
        from intelligence import causation_graph as cg

        with patch.object(cg, "ensure_table") as ensure, \
             patch.object(cg, "trace_causal_chain", return_value=[]) as trace:
            cg.find_longest_chains(_engine_with_rows([("AAA", 2, 5)]), days=30, persist=False)
        ensure.assert_not_called()
        assert trace.call_args.kwargs["persist"] is False

    def test_get_routes_pass_persist_false(self):
        from api.routers import intelligence_forensics as f

        payload = {"links": [], "generated": False, "as_of": None, "last_run": None}
        with patch.object(f, "get_db_engine", return_value=MagicMock()), \
             patch("intelligence.causal_links.read_links_payload", return_value=dict(payload)) as read, \
             patch("intelligence.causal_links.run_causal_links") as run, \
             patch("intelligence.causation.trace_causal_chain", return_value=[]) as trace, \
             patch("intelligence.causation.find_longest_chains", return_value=[]) as longest:
            out = f.get_causation(ticker=None, days=30, _token="t")
            asyncio.run(f.get_causal_chains(ticker="AAA", hops=5, days=180, _token="t"))
            asyncio.run(f.get_causal_chains(ticker=None, hops=5, days=180, _token="t"))
        # The causation GET reads persisted rows; it never computes or writes.
        read.assert_called_once()
        run.assert_not_called()
        assert out["generated"] is False and out["causes"] == []
        assert trace.call_args.kwargs["persist"] is False
        assert longest.call_args.kwargs["persist"] is False

    def test_refresh_posts_are_admin_only_writers(self):
        from api.routers import intelligence_forensics as f

        paths = {
            (getattr(r, "path", None), tuple(sorted(getattr(r, "methods", ()) or ())))
            for r in f.router.routes
        }
        assert ("/causation/refresh", ("POST",)) in paths
        assert ("/causal-chains/refresh", ("POST",)) in paths
        summary = MagicMock(edges_written=2)
        summary.to_dict.return_value = {"run_id": "r1", "status": "succeeded"}
        with patch.object(f, "get_db_engine", return_value=MagicMock()), \
             patch("intelligence.causal_links.resolve_code_sha", return_value="abc"), \
             patch("intelligence.causal_links.run_causal_links", return_value=summary) as run:
            out = f.refresh_causation(days=30, max_tickers=50, _token="t")
        assert out["status"] == "stored" and out["stored"] == 2
        assert run.call_args.kwargs["max_tickers"] == 50
        assert run.call_args.kwargs.get("dry_run", False) is False


# ── forensics ─────────────────────────────────────────────────────────────


class TestForensicsReadOnly:
    def test_summary_builds_reports_without_storing(self):
        from intelligence import forensics

        with patch.object(forensics, "batch_forensics", return_value=[]) as batch:
            forensics.generate_forensic_summary(MagicMock(), "aaa", days=30)
        assert batch.call_args.kwargs["persist"] is False

    def test_batch_threads_persist_to_analyze_move(self):
        from intelligence import forensics

        moves = [{"date": "2026-09-20", "pct_change": 2.0, "direction": "up"}]
        with patch.object(forensics, "find_significant_moves", return_value=moves), \
             patch.object(forensics, "analyze_move", return_value=None) as analyze:
            forensics.batch_forensics(MagicMock(), "AAA", days=30, persist=False)
        assert analyze.call_args.kwargs["persist"] is False

    def test_analyze_move_read_only_does_not_touch_tables(self):
        from intelligence import forensics

        with patch.object(forensics, "_ensure_tables") as ensure, \
             patch.object(forensics, "_store_report") as store, \
             patch.object(forensics, "_get_price_moves", return_value=[]):
            assert forensics.analyze_move(MagicMock(), "AAA", "2026-09-20", persist=False) is None
        ensure.assert_not_called()
        store.assert_not_called()

    def test_load_reports_has_no_ddl(self):
        from intelligence import forensics

        with patch.object(forensics, "_ensure_tables") as ensure:
            assert forensics.load_forensic_reports(_engine_with_rows([]), "AAA") == []
        ensure.assert_not_called()


def test_detect_convergence_has_no_ddl():
    from intelligence import trust_scorer

    with patch.object(trust_scorer, "_ensure_tables") as ensure:
        trust_scorer.detect_convergence(_engine_with_rows([]))
    ensure.assert_not_called()


# ── watchlist GETs ────────────────────────────────────────────────────────


class TestWatchlistGetsHaveNoDDL:
    def _client(self):
        from api.routers import watchlist
        return _client(watchlist.router)

    @pytest.mark.parametrize("path", ["/api/v1/watchlist/", "/api/v1/watchlist/enriched",
                                      "/api/v1/watchlist/preload"])
    def test_get_never_initializes_table(self, path):
        engine = _engine_with_rows([])
        engine.connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (0,)
        with patch("api.routers.watchlist_core._init_table") as init, \
             patch("api.routers.watchlist_helpers._init_table") as helper_init, \
             patch("api.routers.watchlist_core.get_db_engine", return_value=engine):
            response = self._client().get(path)
        assert response.status_code == 200, response.text
        init.assert_not_called()
        helper_init.assert_not_called()

    @pytest.mark.parametrize("path", ["/api/v1/watchlist/", "/api/v1/watchlist/enriched",
                                      "/api/v1/watchlist/preload"])
    def test_missing_table_is_unavailable_not_created(self, path):
        engine = MagicMock()
        engine.connect.side_effect = RuntimeError('relation "watchlist" does not exist')
        with patch("api.routers.watchlist_core._init_table") as init, \
             patch("api.routers.watchlist_core.get_db_engine", return_value=engine):
            response = self._client().get(path)
        assert response.status_code == 503
        assert response.json() == {"detail": "Watchlist data is unavailable"}
        init.assert_not_called()

    def test_enriched_live_fallback_is_not_written_back(self):
        """The GET's yfinance fallback used to INSERT into raw_series."""
        import inspect

        from api.routers import watchlist_core

        assert not hasattr(watchlist_core, "_cache_price_to_db")
        assert "_cache_price_to_db" not in inspect.getsource(watchlist_core.list_watchlist_enriched)
        # The explicit POST writer keeps its init path.
        assert "_init_table()" in inspect.getsource(watchlist_core.refresh_watchlist_prices)
