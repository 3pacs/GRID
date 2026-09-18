"""Fake-data audit batch 3a — options recommender honesty (C-H6/H7/H8, C-M1, C-L4).

Every test here pins a *missing* measurement to an explicit unavailable
state. The defects these replace all had the same shape: a constant
(0.30 + score*0.06, sigma=0.25, entry*2.0) published under a field name
that asserts a measurement, and then used to size a real position.

Nothing here touches Postgres — the engine is a MagicMock whose
``execute`` dispatches on the SQL text, the same pattern as
``tests/test_contagion_tickets.py``.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from trading.options_recommender import (
    WIN_PROB_MIN_SAMPLE,
    OptionsRecommendation,
    OptionsRecommender,
)

# ── Fixtures ────────────────────────────────────────────────────────────────


class _FakeOpp:
    """Stand-in for discovery.options_scanner.MispricingOpportunity."""

    def __init__(self, ticker="AAPL", direction="CALL", score=7.0, spot=180.0):
        self.ticker = ticker
        self.direction = direction
        self.score = score
        self.spot_price = spot
        self.thesis = "test thesis"
        self.signals = {}
        self.confidence = "MEDIUM"


def _chain_df(spot: float = 180.0) -> pd.DataFrame:
    """A minimal options chain that satisfies strike/expiry selection."""
    expiry = date.today() + timedelta(days=30)
    rows = []
    for strike in (185.0, 190.0, 195.0):
        for opt_type in ("call", "put"):
            rows.append({
                "strike": strike,
                "opt_type": opt_type,
                "open_interest": 5000,
                "implied_volatility": 0.35,
                "expiry": expiry,
                "dte": 30,
                "bid": 2.0,
                "ask": 2.2,
            })
    return pd.DataFrame(rows)


def _make_engine(
    *,
    snapshot_row=None,
    atm_iv=0.35,
    spot=180.0,
    win_history: tuple[int, int] | None = None,
    history_raises: bool = False,
):
    """Mock engine dispatching on SQL text.

    ``snapshot_row`` is the ``(bid, ask, last_price, snap_date)`` tuple the
    entry-price lookup finds, or ``None`` for "no snapshot at all".
    ``win_history`` is ``(wins, resolved)`` for the score-bucket lookup.
    """
    engine = MagicMock()
    conn = MagicMock()

    def execute(sql, params=None):
        sql_str = str(sql).lower()
        result = MagicMock()
        result.fetchone.return_value = None
        result.fetchall.return_value = []

        if "count(*) filter (where outcome = 'win')" in sql_str:
            if history_raises:
                raise RuntimeError("relation options_recommendations does not exist")
            result.fetchone.return_value = win_history
        elif "from options_snapshots" in sql_str and "bid, ask, last_price" in sql_str:
            result.fetchone.return_value = snapshot_row
        elif "from options_daily_signals" in sql_str:
            result.fetchone.return_value = (atm_iv,) if atm_iv is not None else None
        elif "from resolved_series" in sql_str:
            result.fetchone.return_value = (spot,) if spot else None
        return result

    conn.execute.side_effect = execute
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine


def _build(recommender, engine, opp, gex_profile):
    """Run _build_recommendation with the sanity pipeline stubbed to PASS."""
    gex_engine = MagicMock()
    gex_engine.compute_gex_profile.return_value = gex_profile
    with patch.object(recommender, "_load_chain", return_value=_chain_df()), \
         patch.object(recommender, "_run_sanity_pipeline", return_value={}):
        return recommender._build_recommendation(opp, gex_engine, engine)


_GEX = {
    "regime": "SHORT_GAMMA",
    "call_wall": 200.0,
    "put_wall": 170.0,
    "gamma_flip": 175.0,
    "gex_aggregate": 1.0,
}


# ── C-H6: win probability comes from outcomes or it is null ────────────────


class TestEmpiricalWinProbability:
    def test_zero_outcome_history_yields_all_nulls(self):
        """No resolved rows → no probability, and therefore no size at all."""
        engine = _make_engine(snapshot_row=(2.0, 2.2, 2.1, date.today()),
                              win_history=(0, 0))
        rec = _build(OptionsRecommender(engine), engine, _FakeOpp(), _GEX)

        assert rec is not None
        assert rec.win_probability is None
        assert rec.kelly_fraction is None
        assert rec.suggested_contracts is None
        assert rec.expected_return is None
        assert rec.win_probability_n == 0
        assert rec.win_probability_basis == "insufficient_history"

    def test_partial_history_below_minimum_is_still_null(self):
        """A handful of outcomes is not a hit rate — and n is reported."""
        engine = _make_engine(snapshot_row=(2.0, 2.2, 2.1, date.today()),
                              win_history=(4, WIN_PROB_MIN_SAMPLE - 1))
        rec = _build(OptionsRecommender(engine), engine, _FakeOpp(), _GEX)

        assert rec.win_probability is None
        assert rec.kelly_fraction is None
        assert rec.suggested_contracts is None
        assert rec.win_probability_n == WIN_PROB_MIN_SAMPLE - 1
        assert rec.win_probability_basis == "insufficient_history"

    def test_sufficient_history_gives_the_realised_rate(self):
        engine = _make_engine(snapshot_row=(2.0, 2.2, 2.1, date.today()),
                              win_history=(15, 30))
        rec = _build(OptionsRecommender(engine), engine, _FakeOpp(), _GEX)

        assert rec.win_probability == pytest.approx(0.5)
        assert rec.win_probability_n == 30
        assert rec.win_probability_basis == "empirical_score_bucket"
        # With a real probability the sizing chain runs end to end.
        assert rec.kelly_fraction is not None
        assert rec.suggested_contracts is not None
        assert rec.expected_return is not None

    def test_failed_history_query_is_unknown_not_average(self):
        engine = _make_engine(snapshot_row=(2.0, 2.2, 2.1, date.today()),
                              history_raises=True)
        recommender = OptionsRecommender(engine)
        prob, n, basis = recommender._empirical_win_probability(engine, 7.0)

        assert prob is None
        assert n == 0
        assert basis == "query_failed"

    def test_score_affine_formula_is_gone(self):
        """The old map returned 0.42 for score 7. Nothing may return it now."""
        engine = _make_engine(win_history=(0, 0))
        recommender = OptionsRecommender(engine)
        assert not hasattr(recommender, "_estimate_win_probability")


# ── C-H7: a modelled entry price must say it is modelled ───────────────────


class TestEntryPriceBasis:
    def test_quote_basis_when_bid_ask_present(self):
        engine = _make_engine(snapshot_row=(2.0, 2.2, 2.1, date(2026, 9, 16)),
                              win_history=(0, 0))
        rec = _build(OptionsRecommender(engine), engine, _FakeOpp(), _GEX)

        assert rec.entry_price_basis == "quote"
        assert rec.entry_bid == 2.0
        assert rec.entry_ask == 2.2
        assert rec.entry_price == pytest.approx(2.1)
        assert rec.entry_model_sigma is None
        assert rec.entry_snapshot_date == "2026-09-16"

    def test_model_basis_echoes_sigma_and_rate_when_no_snapshot(self):
        engine = _make_engine(snapshot_row=None, atm_iv=0.42, win_history=(0, 0))
        rec = _build(OptionsRecommender(engine), engine, _FakeOpp(), _GEX)

        assert rec is not None
        assert rec.entry_price_basis == "model"
        assert rec.entry_bid is None
        assert rec.entry_ask is None
        assert rec.entry_model_sigma == pytest.approx(0.42)
        assert rec.entry_model_rate is not None

    def test_no_snapshot_and_no_measured_iv_emits_nothing(self):
        """There is no default sigma — an unpriceable contract is dropped."""
        engine = _make_engine(snapshot_row=None, atm_iv=None, win_history=(0, 0))
        rec = _build(OptionsRecommender(engine), engine, _FakeOpp(), _GEX)

        assert rec is None

    def test_last_trade_basis_is_not_labelled_a_quote(self):
        engine = _make_engine(snapshot_row=(None, None, 2.4, date(2026, 9, 16)),
                              win_history=(0, 0))
        rec = _build(OptionsRecommender(engine), engine, _FakeOpp(), _GEX)

        assert rec.entry_price_basis == "last_trade"
        assert rec.entry_bid is None
        assert rec.entry_ask is None

    def test_stale_snapshot_date_is_reported_not_hidden(self):
        """A month-old quote still serves, but the payload dates it."""
        stale = date.today() - timedelta(days=30)
        engine = _make_engine(snapshot_row=(2.0, 2.2, 2.1, stale),
                              win_history=(0, 0))
        rec = _build(OptionsRecommender(engine), engine, _FakeOpp(), _GEX)

        assert rec.entry_snapshot_date == stale.isoformat()


# ── C-H8: no GEX profile means no target, not a doubling ───────────────────


class TestTargetAndStop:
    def test_no_gex_profile_nulls_the_whole_exit_chain(self):
        engine = _make_engine(snapshot_row=(2.0, 2.2, 2.1, date.today()),
                              win_history=(15, 30))
        rec = _build(OptionsRecommender(engine), engine, _FakeOpp(), {})

        assert rec is not None
        assert rec.target_price is None
        assert rec.target_return_pct is None
        assert rec.expected_return is None
        assert rec.target_basis is None
        # And with no exits there is no payoff ratio, so no size.
        assert rec.stop_loss is None
        assert rec.kelly_fraction is None
        assert rec.suggested_contracts is None

    def test_entry_times_two_is_never_returned(self):
        recommender = OptionsRecommender(MagicMock())
        target, basis = recommender._compute_target_price(
            3.0, 180.0, 190.0, "CALL", {},
        )
        assert target is None
        assert basis is None

    def test_target_derived_from_gamma_wall_is_labelled(self):
        recommender = OptionsRecommender(MagicMock())
        target, basis = recommender._compute_target_price(
            3.0, 180.0, 190.0, "CALL", _GEX,
        )
        assert target is not None
        assert basis == "gamma_call_wall"

    def test_stop_without_gamma_flip_is_null(self):
        recommender = OptionsRecommender(MagicMock())
        stop, basis = recommender._compute_stop_loss(
            3.0, 180.0, 190.0, "CALL", {"regime": "NEUTRAL"},
        )
        assert stop is None
        assert basis is None


# ── Downstream: a null must not be sorted or rendered as a zero ────────────


class TestNullSafeDownstream:
    def test_unsized_recommendations_sort_last_not_as_zero(self):
        """A null expected return must not outrank a measured negative one."""
        sized_pos = OptionsRecommendation(
            ticker="A", direction="CALL", strike=1, expiry="2026-10-16",
            entry_price=1.0, expected_return=0.5,
        )
        sized_neg = OptionsRecommendation(
            ticker="B", direction="CALL", strike=1, expiry="2026-10-16",
            entry_price=1.0, expected_return=-0.5,
        )
        unsized = OptionsRecommendation(
            ticker="C", direction="CALL", strike=1, expiry="2026-10-16",
            entry_price=1.0, expected_return=None,
        )
        recs = [unsized, sized_neg, sized_pos]
        recs.sort(
            key=lambda r: (
                r.expected_return is None,
                -(r.expected_return if r.expected_return is not None else 0.0),
            )
        )
        assert [r.ticker for r in recs] == ["A", "B", "C"]

    def test_report_renders_na_not_zero_for_missing_numbers(self):
        rec = OptionsRecommendation(
            ticker="AAPL", direction="CALL", strike=190.0, expiry="2026-10-16",
            entry_price=2.10, entry_price_basis="model",
        )
        recommender = OptionsRecommender(MagicMock())
        report = recommender.format_report([rec])

        assert "n/a" in report
        assert "$0.00" not in report
        # The ticket formatter must survive the same nulls.
        ticket = rec.to_trade_ticket()
        assert "n/a" in ticket

    def test_to_dict_carries_the_provenance_fields(self):
        rec = OptionsRecommendation(
            ticker="AAPL", direction="CALL", strike=190.0, expiry="2026-10-16",
            entry_price=2.10,
        )
        payload = rec.to_dict()
        for key in (
            "entry_price_basis", "entry_bid", "entry_ask", "entry_model_sigma",
            "entry_model_rate", "target_basis", "stop_basis",
            "win_probability", "win_probability_n", "win_probability_basis",
            "suggested_contracts", "scanner_score",
        ):
            assert key in payload, key
        assert payload["win_probability"] is None
        assert payload["kelly_fraction"] is None
        assert payload["suggested_contracts"] is None
        assert payload["target_price"] is None

    def test_risk_reward_is_null_without_a_target(self):
        rec = OptionsRecommendation(
            ticker="AAPL", direction="CALL", strike=190.0, expiry="2026-10-16",
            entry_price=2.10,
        )
        assert rec.risk_reward_ratio is None


# ── C-M1 / C-L4: the router actually calls the recommender ─────────────────


class TestRouterWiring:
    def test_refresh_runs_the_recommender_and_returns_a_fresh_stamp(self):
        """POST /options/recommendations/refresh must scan, not serve rows."""
        import asyncio

        from api.routers import options as options_router

        rec = OptionsRecommendation(
            ticker="AAPL", direction="CALL", strike=190.0, expiry="2026-10-16",
            entry_price=2.10, entry_price_basis="quote",
        )
        fake_recommender = MagicMock()
        fake_recommender.generate_recommendations.return_value = [rec]
        fake_cls = MagicMock(return_value=fake_recommender)

        before = datetime.now(timezone.utc)
        with patch("trading.options_recommender.OptionsRecommender", fake_cls), \
             patch.object(options_router, "get_db_engine", return_value=MagicMock()), \
             patch.object(options_router, "_persist_recommendations", return_value=0):
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(
                    options_router.refresh_recommendations(_token="tok")
                )
            finally:
                loop.close()

        fake_recommender.generate_recommendations.assert_called_once()
        assert result["scan_summary"]["fresh_scan"] is True
        assert result["scan_summary"]["source"] == "live_scan"
        assert len(result["recommendations"]) == 1
        stamp = datetime.fromisoformat(result["generated_at"])
        assert stamp >= before

    def test_broken_import_symbol_is_gone_from_the_router(self):
        """The module-level symbol never existed; the import must not either."""
        from pathlib import Path

        src = Path("api/routers/options.py").read_text(encoding="utf-8")
        assert "from trading.options_recommender import generate_recommendations" not in src
        # The one allowed occurrence of the sentence is the constant itself.
        assert src.count("Options recommender module is not installed") == 1

    def test_db_failure_reason_names_the_query_not_the_module(self):
        from api.routers import options as options_router

        engine = MagicMock()
        engine.connect.side_effect = RuntimeError("connection refused")

        _recs, summary, _stamp = options_router._load_saved_recommendations(
            engine,
            engine_unavailable_reason="engine skipped for test",
        )

        assert summary["fresh_scan"] is False
        assert summary["source"] == "unavailable"
        assert "connection refused" in summary["reason"]
        assert "persisted options_recommendations query also failed" in summary["reason"]
        assert summary["persisted_age_seconds"] is None

    def test_persisted_branch_reports_the_rows_age(self):
        from api.routers import options as options_router

        old = datetime.now(timezone.utc) - timedelta(hours=30)
        row = (
            "AAPL", "CALL", 190.0, date(2026, 10, 16), 2.1, None, None,
            None, None, 0.7, "thesis", None, "dealer", old, None,
        )
        engine = MagicMock()
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [row]
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        _recs, summary, stamp = options_router._load_saved_recommendations(
            engine,
            engine_unavailable_reason=options_router.ENGINE_MISSING_REASON,
        )

        assert summary["fresh_scan"] is False
        assert summary["source"] == "persisted"
        assert summary["total_scanned"] == 0  # nothing was scanned
        assert summary["persisted_row_count"] == 1
        assert summary["persisted_age_seconds"] == pytest.approx(30 * 3600, rel=0.01)
        assert "old" in summary["reason"]
        # The envelope is stamped with the row's own time, never now().
        assert stamp == old.isoformat()
