"""Tests for trading/robinhood_risk_store.py's InMemoryRiskStore.

No database: InMemoryRiskStore is the default RiskStore every
RobinhoodCryptoTrader gets unless one is explicitly wired in (see
trading/robinhood.py::get_robinhood_trader()). Postgres-specific behavior —
the real SQL, against the schema migrations/versions/robinhood_guards_20260924.py
creates — is covered separately in tests/test_robinhood_risk_store_pg.py,
gated on a live database the same way every other ``*_pg.py`` file in this
suite is.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from trading.robinhood_risk_store import InMemoryRiskStore, RiskState, utc_today


class TestTouch:
    def test_first_touch_seeds_peak_and_day_start_from_equity(self):
        store = InMemoryRiskStore()
        today = utc_today()
        state = store.touch("robinhood", 1000.0, today)
        assert state == RiskState("robinhood", 1000.0, 1000.0, today, 0)

    def test_peak_only_ever_rises(self):
        store = InMemoryRiskStore()
        today = utc_today()
        store.touch("robinhood", 1000.0, today)
        store.touch("robinhood", 500.0, today)  # a loss must not lower the peak
        state = store.touch("robinhood", 1200.0, today)  # a new high does raise it
        assert state.peak_equity == 1200.0

    def test_day_rollover_resets_day_start_and_order_count_but_not_peak(self):
        store = InMemoryRiskStore()
        yesterday = utc_today() - timedelta(days=1)
        store.touch("robinhood", 1000.0, yesterday, increment_order=True)
        store.touch("robinhood", 1000.0, yesterday, increment_order=True)
        today = utc_today()
        state = store.touch("robinhood", 900.0, today)
        assert state.peak_equity == 1000.0  # unchanged — a high-water mark never resets
        assert state.day_start_equity == 900.0  # today's own opening reading
        assert state.orders_today == 0

    def test_increment_order_counts_within_the_same_day(self):
        store = InMemoryRiskStore()
        today = utc_today()
        store.touch("robinhood", 1000.0, today, increment_order=True)
        store.touch("robinhood", 1000.0, today, increment_order=True)
        state = store.touch("robinhood", 1000.0, today)
        assert state.orders_today == 2

    def test_venues_are_independent(self):
        store = InMemoryRiskStore()
        today = utc_today()
        store.touch("robinhood", 1000.0, today, increment_order=True)
        state = store.touch("other_venue", 5000.0, today)
        assert state.orders_today == 0 and state.peak_equity == 5000.0

    def test_clock_skew_never_rolls_the_day_backward(self):
        store = InMemoryRiskStore()
        today = utc_today()
        store.touch("robinhood", 1000.0, today, increment_order=True)
        earlier = today - timedelta(days=1)
        state = store.touch("robinhood", 999.0, earlier)  # a stale/skewed clock
        assert state.day_start_date == today  # kept the later, already-stored day
        assert state.orders_today == 1  # not reset by the earlier date

    def test_get_state_returns_none_before_any_touch(self):
        assert InMemoryRiskStore().get_state("robinhood") is None

    def test_get_state_mirrors_touch(self):
        store = InMemoryRiskStore()
        today = utc_today()
        store.touch("robinhood", 1000.0, today)
        assert store.get_state("robinhood") == RiskState("robinhood", 1000.0, 1000.0, today, 0)


class TestIdempotencyLog:
    def test_unseen_key_is_not_a_duplicate(self):
        assert InMemoryRiskStore().is_duplicate("robinhood", "never-seen") is False

    def test_a_dry_run_log_entry_is_a_duplicate(self):
        store = InMemoryRiskStore()
        store.log_order("robinhood", "k1", status="dry_run", ticker="BTC-USD", side="buy")
        assert store.is_duplicate("robinhood", "k1") is True

    def test_a_submitted_log_entry_is_a_duplicate(self):
        store = InMemoryRiskStore()
        store.log_order("robinhood", "k1", status="submitted", ticker="BTC-USD", side="buy")
        assert store.is_duplicate("robinhood", "k1") is True

    @pytest.mark.parametrize("status", ["blocked", "rejected", "error", "duplicate"])
    def test_a_non_consumed_status_is_not_a_duplicate(self, status):
        store = InMemoryRiskStore()
        store.log_order("robinhood", "k1", status=status, ticker="BTC-USD", side="buy")
        assert store.is_duplicate("robinhood", "k1") is False

    def test_keys_are_scoped_per_venue(self):
        store = InMemoryRiskStore()
        store.log_order("robinhood", "k1", status="dry_run", ticker="BTC-USD", side="buy")
        assert store.is_duplicate("other_venue", "k1") is False


class TestAverageCost:
    def test_no_history_returns_none(self):
        assert InMemoryRiskStore().average_cost("robinhood", "BTC-USD") is None

    def test_single_buy_is_its_own_average(self):
        store = InMemoryRiskStore()
        store.log_order("robinhood", "buy-1", status="dry_run", ticker="BTC-USD", side="buy",
                        quantity="0.1", fill_price=60000.0)
        assert store.average_cost("robinhood", "BTC-USD") == pytest.approx(60000.0)

    def test_volume_weighted_across_two_buys(self):
        store = InMemoryRiskStore()
        store.log_order("robinhood", "buy-1", status="dry_run", ticker="BTC-USD", side="buy",
                        quantity="1.0", fill_price=100.0)
        store.log_order("robinhood", "buy-2", status="dry_run", ticker="BTC-USD", side="buy",
                        quantity="3.0", fill_price=200.0)
        # (1*100 + 3*200) / 4 = 175
        assert store.average_cost("robinhood", "BTC-USD") == pytest.approx(175.0)

    def test_only_history_since_the_last_sell_counts(self):
        store = InMemoryRiskStore()
        store.log_order("robinhood", "buy-1", status="dry_run", ticker="BTC-USD", side="buy",
                        quantity="1.0", fill_price=100.0)
        store.log_order("robinhood", "sell-1", status="dry_run", ticker="BTC-USD", side="sell",
                        quantity="1.0", fill_price=150.0)
        store.log_order("robinhood", "buy-2", status="dry_run", ticker="BTC-USD", side="buy",
                        quantity="1.0", fill_price=300.0)
        assert store.average_cost("robinhood", "BTC-USD") == pytest.approx(300.0)

    def test_rejected_buys_do_not_count(self):
        store = InMemoryRiskStore()
        store.log_order("robinhood", "buy-1", status="blocked", ticker="BTC-USD", side="buy",
                        quantity="1.0", fill_price=100.0)
        assert store.average_cost("robinhood", "BTC-USD") is None

    def test_tickers_are_independent(self):
        store = InMemoryRiskStore()
        store.log_order("robinhood", "buy-1", status="dry_run", ticker="BTC-USD", side="buy",
                        quantity="1.0", fill_price=100.0)
        assert store.average_cost("robinhood", "ETH-USD") is None
