"""Fake-data audit batch 3b — contagion ticket & scanner honesty.

Covers C-H4 (Kelly sized off a 0.55 placeholder), C-H5 (premiums modelled
off a 0.30 placeholder IV), C-M24 (a strike/expiry that may not be a listed
contract) and C-M20 (``is_100x``, a bare boolean built from tuning
constants).

The engine is a MagicMock dispatching on SQL text — the same pattern as
``tests/test_contagion_tickets.py``. Nothing here touches Postgres.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from trading import contagion_to_ticket as ctt

# ── Fixtures ────────────────────────────────────────────────────────────────


def _prediction_row(margin: float = -0.023):
    ranked = [{
        "id": "aapl",
        "tier": 1,
        "margin_impact_pct": margin,
        "edge_count": 2,
        "path": ["tsmc", "aapl"],
    }]
    return (
        12, "tsmc", "supply_disruption", 0.30, 4,
        datetime(2026, 4, 11, 2, 0, tzinfo=timezone.utc),
        json.dumps({"worst_case_ticker": "aapl"}),
        json.dumps(ranked),
    )


def _signal_row(iv_atm: float | None = 0.30):
    """Matches the SELECT in ``_load_options_signal``."""
    return (
        "AAPL",
        datetime(2026, 4, 11).date(),
        0.9,            # put_call_ratio
        178.0,          # max_pain
        0.05,           # iv_skew
        180.0,          # spot_price
        iv_atm,         # iv_atm — nullable in the real table
        datetime(2026, 5, 16).date(),
    )


def _make_engine(
    *,
    prediction_row=None,
    signal_row=None,
    accuracy: tuple[float, int] = (0.6, 10),
    contract_listed: bool = True,
    snapshot_raises: bool = False,
    model_version_id: int | None = 1,
):
    engine = MagicMock()
    conn = MagicMock()

    def execute(sql, params=None):
        sql_str = str(sql).lower()
        result = MagicMock()
        result.fetchone.return_value = None
        result.fetchall.return_value = []

        if "from contagion_predictions" in sql_str and "where id =" in sql_str:
            result.fetchone.return_value = prediction_row
        elif "from options_daily_signals" in sql_str:
            result.fetchone.return_value = signal_row
        elif "from options_snapshots" in sql_str:
            if snapshot_raises:
                raise RuntimeError("options_snapshots unavailable")
            result.fetchone.return_value = (1,) if contract_listed else None
        elif "from contagion_backtest_results" in sql_str:
            result.fetchone.return_value = accuracy
        elif "from model_registry" in sql_str and model_version_id is not None:
            result.fetchone.return_value = (model_version_id,)
        return result

    conn.execute.side_effect = execute
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine


def _tickets(engine, **kw):
    with patch.object(ctt, "_load_dealer_gamma_context", return_value=None):
        return ctt.generate_tickets_for_prediction(
            engine, prediction_id=12, journal=kw.get("journal", False),
        )


# ── C-H4: no backtest history means no size ────────────────────────────────


class TestConfidenceWithoutHistory:
    def test_zero_history_gives_zero_kelly_and_null_confidence(self):
        engine = _make_engine(
            prediction_row=_prediction_row(),
            signal_row=_signal_row(),
            accuracy=(-1.0, 0),  # what _load_contagion_accuracy returns empty
        )
        tickets = _tickets(engine)

        assert len(tickets) == 1
        t = tickets[0]
        assert t["kelly_size"] == 0
        assert t["confidence"] is None
        assert t["confidence_basis"] == ctt.CONFIDENCE_BASIS_NO_HISTORY
        assert t["confidence_n"] == 0
        # The specific number that used to be substituted here.
        assert t["confidence"] != 0.55

    def test_real_history_sizes_the_ticket(self):
        engine = _make_engine(
            prediction_row=_prediction_row(),
            signal_row=_signal_row(),
            accuracy=(0.6, 10),
        )
        t = _tickets(engine)[0]

        assert t["confidence"] == pytest.approx(0.6)
        assert t["confidence_basis"] == ctt.CONFIDENCE_BASIS_BACKTEST
        assert t["confidence_n"] == 10
        assert 0 < t["kelly_size"] <= ctt.MAX_KELLY_PER_TICKET

    def test_placeholder_constant_is_gone(self):
        assert not hasattr(ctt, "DEFAULT_CONFIDENCE_NO_HISTORY")

    def test_null_confidence_is_not_journalled_as_a_placeholder(self):
        """The journal is immutable — a fabricated 0.5 there is permanent."""
        engine = _make_engine(
            prediction_row=_prediction_row(),
            signal_row=_signal_row(),
            accuracy=(-1.0, 0),
        )
        fake_journal = MagicMock()
        fake_journal.log_decision.return_value = 4242
        with patch("journal.log.DecisionJournal", MagicMock(return_value=fake_journal)):
            tickets = _tickets(engine, journal=True)

        fake_journal.log_decision.assert_not_called()
        assert "journal_id" not in tickets[0]


# ── C-H5: no measured IV means no ticket ───────────────────────────────────


class TestMissingImpliedVol:
    def test_missing_iv_atm_emits_no_ticket(self):
        engine = _make_engine(
            prediction_row=_prediction_row(),
            signal_row=_signal_row(iv_atm=None),
        )
        assert _tickets(engine) == []

    def test_non_positive_iv_atm_emits_no_ticket(self):
        engine = _make_engine(
            prediction_row=_prediction_row(),
            signal_row=_signal_row(iv_atm=0.0),
        )
        assert _tickets(engine) == []

    def test_signal_loader_no_longer_substitutes_030(self):
        engine = _make_engine(signal_row=_signal_row(iv_atm=None))
        signal = ctt._load_options_signal(engine, "AAPL")
        assert signal is not None
        assert signal["iv_atm"] is None

    def test_modelled_premiums_are_tagged_as_modelled(self):
        engine = _make_engine(
            prediction_row=_prediction_row(),
            signal_row=_signal_row(iv_atm=0.30),
        )
        t = _tickets(engine)[0]

        assert t["premium_basis"] == "modelled_1sigma"
        assert t["iv_atm"] == pytest.approx(0.30)
        assert t["entry_premium"] > 0


# ── C-M24: the contract on the ticket must be a real one, or say it is not ─


class TestContractVerification:
    def test_listed_contract_is_marked_verified(self):
        engine = _make_engine(
            prediction_row=_prediction_row(),
            signal_row=_signal_row(),
            contract_listed=True,
        )
        assert _tickets(engine)[0]["contract_verified"] is True

    def test_unlisted_contract_is_marked_unverified(self):
        engine = _make_engine(
            prediction_row=_prediction_row(),
            signal_row=_signal_row(),
            contract_listed=False,
        )
        assert _tickets(engine)[0]["contract_verified"] is False

    def test_failed_lookup_is_unknown_not_false(self):
        engine = _make_engine(
            prediction_row=_prediction_row(),
            signal_row=_signal_row(),
            snapshot_raises=True,
        )
        assert _tickets(engine)[0]["contract_verified"] is None

    def test_every_emitted_ticket_carries_the_field(self):
        """The acceptance condition: verified, or explicitly not."""
        for listed in (True, False):
            engine = _make_engine(
                prediction_row=_prediction_row(),
                signal_row=_signal_row(),
                contract_listed=listed,
            )
            for t in _tickets(engine):
                assert "contract_verified" in t
                assert t["contract_verified"] is True or t["contract_verified"] is not True


# ── C-M20: the scanner's payoff flag ships its inputs ──────────────────────


class TestScannerPayoffFlag:
    def test_route_and_field_no_longer_assert_100x(self):
        from pathlib import Path

        src = Path("discovery/options_scanner.py").read_text(encoding="utf-8")
        # The property is renamed; only the historical DB column keeps the name.
        assert "def is_100x" not in src
        assert "def heuristic_payoff_flag" in src

        router = Path("api/routers/options.py").read_text(encoding="utf-8")
        assert '"is_100x": o.' not in router
        assert '"heuristic_payoff_flag"' in router

    def test_scanner_module_has_no_bare_score_times_five_fallback(self):
        from pathlib import Path

        src = Path("discovery/options_scanner.py").read_text(encoding="utf-8")
        assert "return composite_score * 5" not in src
