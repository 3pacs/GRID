"""Tests for trading.contagion_to_ticket.

Covers:
  1. generate_tickets_for_prediction with a synthetic prediction row
  2. Skip reason when a victim has no options_daily_signals data
  3. Kelly sizing edge cases (0.5 accuracy, zero accuracy, perfect accuracy)
  4. Direction derivation from margin sign
  5. Journal writer happy path (stubbed DecisionJournal)
  6. /api/v1/trade-tickets/recent endpoint response shape

We mock the DB layer aggressively — none of these tests hit Postgres.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from trading import contagion_to_ticket as ctt


# ── Synthetic fixtures ──────────────────────────────────────────────────────


def _synthetic_prediction_row():
    """Return a row tuple matching the SELECT in ``_load_prediction``."""
    summary = {
        "total_actors_affected": 3,
        "worst_case_tier1": "aapl",
        "worst_case_ticker": "aapl",
    }
    ranked = [
        {
            "id": "aapl",
            "tier": 1,
            "margin_impact_pct": -0.023,  # -2.3% → SHORT, PUT
            "edge_count": 2,
            "revenue_at_risk_usd": 9e9,
            "path": ["tsmc", "aapl"],
        },
        {
            "id": "nvda",
            "tier": 1,
            "margin_impact_pct": -0.015,  # -1.5% → SHORT, PUT
            "edge_count": 1,
            "revenue_at_risk_usd": 4e9,
            "path": ["tsmc", "nvda"],
        },
        {
            "id": "noise",
            "tier": 2,
            "margin_impact_pct": -0.002,  # below threshold → skip
            "edge_count": 0,
            "revenue_at_risk_usd": 0,
            "path": ["tsmc", "noise"],
        },
    ]
    return (
        12,                                     # id
        "tsmc",                                 # shock_node
        "supply_disruption",                    # shock_type
        0.30,                                   # magnitude
        4,                                      # max_depth
        datetime(2026, 4, 11, 2, 0, tzinfo=timezone.utc),  # simulated_at
        json.dumps(summary),                    # summary (JSON str form)
        json.dumps(ranked),                     # ranked_impact
    )


def _signal_row_for(ticker: str) -> tuple:
    """Matches SELECT in ``_load_options_signal``."""
    spot = {"AAPL": 180.0, "NVDA": 900.0}[ticker]
    return (
        ticker,                                 # ticker
        datetime(2026, 4, 11).date(),           # signal_date
        0.9,                                    # put_call_ratio
        spot * 0.99,                            # max_pain slightly below spot
        0.05,                                   # iv_skew
        spot,                                   # spot_price
        0.30,                                   # iv_atm
        datetime(2026, 5, 16).date(),           # near_expiry
    )


def _make_engine(
    *,
    prediction_row=None,
    recent_rows=None,
    signals: dict[str, tuple] | None = None,
    accuracy: tuple[float, int] = (0.6, 10),
    model_version_id: int | None = 1,
    options_present: set[str] | None = None,
):
    """Build a mock engine whose ``execute`` inspects SQL text and returns rows.

    ``options_present`` is the set of uppercase tickers that have options_daily_signals.
    When None, every ticker in ``signals`` is considered present.
    """
    signals = signals or {}
    options_present = options_present or set(signals.keys())

    engine = MagicMock()
    conn = MagicMock()

    def execute(sql, params=None):  # noqa: ARG001
        sql_str = str(sql).lower()
        result = MagicMock()
        result.fetchone.return_value = None
        result.fetchall.return_value = []

        if "from contagion_predictions" in sql_str and "where id =" in sql_str:
            result.fetchone.return_value = prediction_row
        elif (
            "from contagion_predictions" in sql_str
            and "simulated_at >=" in sql_str
        ):
            result.fetchall.return_value = recent_rows or []
        elif "from options_daily_signals" in sql_str:
            t = (params or {}).get("t", "").upper()
            if t in options_present and t in signals:
                result.fetchone.return_value = signals[t]
            else:
                result.fetchone.return_value = None
        elif "from contagion_backtest_results" in sql_str:
            result.fetchone.return_value = accuracy
        elif "from model_registry" in sql_str:
            if model_version_id is not None:
                result.fetchone.return_value = (model_version_id,)
        return result

    conn.execute.side_effect = execute
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine


# ── Tests ───────────────────────────────────────────────────────────────────


class TestKellySizing:
    """compute_kelly_fraction math edge cases."""

    def test_coinflip_with_three_to_one_payout(self):
        # Kelly = (0.5*3 - 0.5) / 3 = 0.333 → capped at 0.05
        f = ctt.compute_kelly_fraction(0.5, payout_ratio=3.0)
        assert f == pytest.approx(ctt.MAX_KELLY_PER_TICKET)

    def test_zero_edge_is_zero(self):
        # At 0.25 accuracy with 3:1 payout, Kelly = 0.0 exactly.
        assert ctt.compute_kelly_fraction(0.25, payout_ratio=3.0) == 0.0

    def test_negative_edge_clamps_to_zero(self):
        assert ctt.compute_kelly_fraction(0.1, payout_ratio=3.0) == 0.0

    def test_perfect_accuracy_hits_cap(self):
        assert ctt.compute_kelly_fraction(1.0) == pytest.approx(
            ctt.MAX_KELLY_PER_TICKET
        )

    def test_nan_accuracy_returns_zero(self):
        assert ctt.compute_kelly_fraction(float("nan")) == 0.0

    def test_cap_honoured(self):
        # Even with 0.9 accuracy and 10x payout, result must <= cap.
        f = ctt.compute_kelly_fraction(0.9, payout_ratio=10.0, cap=0.05)
        assert 0 < f <= 0.05


class TestStrikePicker:
    def test_short_snaps_to_put_wall_when_below_spot(self):
        ctx = {"put_wall": 178.0, "gamma_wall": 175.0}
        assert ctt.pick_strike(180.0, "short", ctx, max_pain=None) == 178.0

    def test_long_snaps_to_call_wall_above_spot(self):
        ctx = {"call_wall": 190.0, "gamma_wall": 175.0}
        assert ctt.pick_strike(180.0, "long", ctx, max_pain=None) == 190.0

    def test_fallback_two_pct_otm_when_no_context(self):
        s = ctt.pick_strike(100.0, "short", None, max_pain=None)
        assert s == pytest.approx(98.0, abs=1.0)

    def test_zero_spot_returns_zero(self):
        assert ctt.pick_strike(0.0, "short", None, None) == 0.0


class TestExpiryPicker:
    def test_minimum_dte_floor(self):
        base = datetime(2026, 4, 11, tzinfo=timezone.utc)
        expiry, dte = ctt.pick_expiry(base, margin_impact_pct=-0.001)
        assert dte >= ctt.MIN_DTE

    def test_dte_scales_with_impact(self):
        base = datetime(2026, 4, 11, tzinfo=timezone.utc)
        _, small = ctt.pick_expiry(base, margin_impact_pct=-0.01)
        _, big = ctt.pick_expiry(base, margin_impact_pct=-0.40)
        assert big > small


class TestPremiumEstimate:
    def test_reasonable_shape(self):
        entry, target, stop = ctt.estimate_premium(spot=100, iv_atm=0.3, dte=30)
        assert entry > 0
        assert target > entry
        assert 0 < stop < entry

    def test_zero_inputs_return_zero(self):
        assert ctt.estimate_premium(0, 0.3, 30) == (0.0, 0.0, 0.0)


class TestGenerateTicketsForPrediction:
    def test_full_happy_path(self):
        engine = _make_engine(
            prediction_row=_synthetic_prediction_row(),
            signals={
                "AAPL": _signal_row_for("AAPL"),
                "NVDA": _signal_row_for("NVDA"),
            },
            accuracy=(0.6, 10),
        )
        # Stub dealer gamma to return a clean context.
        with patch.object(
            ctt, "_load_dealer_gamma_context",
            return_value={
                "gamma_wall": 175.0, "put_wall": 178.0,
                "call_wall": 182.0, "flip_level": 176.0, "regime": "SHORT_GAMMA",
                "spot": 180.0,
            },
        ):
            tickets = ctt.generate_tickets_for_prediction(
                engine, prediction_id=12, journal=False
            )

        # Noise row (below threshold) is skipped.
        assert len(tickets) == 2
        tickers = {t["ticker"] for t in tickets}
        assert tickers == {"aapl", "nvda"}
        # Direction derivation from negative margin.
        assert all(t["direction"] == "short" for t in tickets)
        assert all(t["instrument"] == "put" for t in tickets)
        # Thesis contains the full LEVER/CONDITION/THESIS/INVALIDATION.
        aapl = next(t for t in tickets if t["ticker"] == "aapl")
        for keyword in ("LEVER:", "CONDITION:", "THESIS:", "INVALIDATION:"):
            assert keyword in aapl["thesis"]
        assert aapl["flow_thesis"] == ctt.FLOW_THESIS_TAG
        assert aapl["confidence"] == pytest.approx(0.6)
        assert 0 < aapl["kelly_size"] <= ctt.MAX_KELLY_PER_TICKET
        # Dealer context is present.
        assert aapl["dealer_gamma_context"]["put_wall"] == 178.0

    def test_skip_when_no_options_data(self):
        engine = _make_engine(
            prediction_row=_synthetic_prediction_row(),
            signals={"AAPL": _signal_row_for("AAPL")},  # nvda missing
            accuracy=(0.6, 10),
        )
        with patch.object(ctt, "_load_dealer_gamma_context", return_value=None):
            tickets = ctt.generate_tickets_for_prediction(
                engine, prediction_id=12, journal=False
            )
        tickers = {t["ticker"] for t in tickets}
        assert "nvda" not in tickers
        assert "aapl" in tickers

    def test_direction_flips_on_positive_margin(self):
        prediction = list(_synthetic_prediction_row())
        ranked = [
            {
                "id": "aapl",
                "tier": 1,
                "margin_impact_pct": 0.03,  # positive → LONG / CALL
                "path": ["tsmc", "aapl"],
            }
        ]
        prediction[7] = json.dumps(ranked)
        engine = _make_engine(
            prediction_row=tuple(prediction),
            signals={"AAPL": _signal_row_for("AAPL")},
            accuracy=(0.6, 10),
        )
        with patch.object(ctt, "_load_dealer_gamma_context", return_value=None):
            tickets = ctt.generate_tickets_for_prediction(
                engine, prediction_id=12, journal=False
            )
        assert len(tickets) == 1
        assert tickets[0]["direction"] == "long"
        assert tickets[0]["instrument"] == "call"

    def test_missing_prediction_returns_empty(self):
        engine = _make_engine(prediction_row=None)
        tickets = ctt.generate_tickets_for_prediction(
            engine, prediction_id=999, journal=False
        )
        assert tickets == []


class TestJournalWriter:
    def test_writes_decision_row(self):
        engine = _make_engine(
            prediction_row=_synthetic_prediction_row(),
            signals={"AAPL": _signal_row_for("AAPL")},
            accuracy=(0.6, 10),
        )

        fake_journal = MagicMock()
        fake_journal.log_decision.return_value = 4242

        fake_cls = MagicMock(return_value=fake_journal)

        with patch.object(ctt, "_load_dealer_gamma_context", return_value=None):
            with patch("journal.log.DecisionJournal", fake_cls):
                tickets = ctt.generate_tickets_for_prediction(
                    engine, prediction_id=12, journal=True
                )

        aapl = next(t for t in tickets if t["ticker"] == "aapl")
        assert aapl["journal_id"] == 4242
        fake_journal.log_decision.assert_called()
        kwargs = fake_journal.log_decision.call_args.kwargs
        assert kwargs["model_version_id"] == 1
        assert kwargs["inferred_state"].startswith("CONTAGION_")
        assert "LEVER:" in kwargs["counterfactual"]
        assert "PUT" in kwargs["action_taken"]


class TestEndpointShape:
    def test_recent_endpoint_returns_ticket_list(self):
        from api.routers import trade_tickets as tt_router

        fake_tickets = [{"ticker": "aapl", "direction": "short"}]
        with patch.object(
            tt_router, "generate_tickets_for_recent_predictions",
            return_value=fake_tickets,
        ):
            import asyncio
            # Use a fresh loop rather than asyncio.get_event_loop(), which
            # raises on Python 3.9 when a prior test closed the global loop.
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(
                    tt_router.recent_tickets(
                        since_hours=24, write_journal=False, _token="tok",
                    )
                )
            finally:
                loop.close()
        assert result["count"] == 1
        assert result["since_hours"] == 24
        assert result["journaled"] is False
        assert result["tickets"] == fake_tickets
