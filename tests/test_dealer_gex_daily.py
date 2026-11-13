"""physics.dealer_gex_daily: the dealer_gex_daily writer (one SPY row per session, end-of-day cutoff, insert-only)."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from physics import dealer_gex_daily as G


def _chain():
    rows = []
    for dte in (0, 3, 20):
        for k in (95.0, 100.0, 105.0):
            rows.append({"strike": k, "opt_type": "call", "open_interest": 100.0 * (1 + dte), "implied_volatility": 0.2,
                         "expiry": None, "dte": timedelta(days=dte)})
            rows.append({"strike": k, "opt_type": "put", "open_interest": 300.0, "implied_volatility": 0.25,
                         "expiry": None, "dte": timedelta(days=dte)})
    return pd.DataFrame(rows)


def test_day_cutoff_is_end_of_utc_day():
    assert G.day_cutoff(date(2026, 11, 12)) == datetime(2026, 11, 13, tzinfo=timezone.utc)


def test_max_pain_minimises_settlement_payout():
    # payout(K) = 10*(K-90)+ for the calls + 30*(100-K)+ for the puts: 300 at 90, 200 at 95, 100 at 100.
    c = pd.DataFrame([{"strike": 90.0, "opt_type": "call", "open_interest": 10.0},
                      {"strike": 100.0, "opt_type": "put", "open_interest": 30.0},
                      {"strike": 95.0, "opt_type": "call", "open_interest": 0.0}])
    assert G.max_pain(c) == 100.0


def test_chain_extras_use_the_nearest_expiry_out_and_the_atm_band():
    ex = G.chain_extras(_chain(), 100.0)
    assert ex["put_call_oi_ratio"] == pytest.approx((300 * 9) / ((100 + 400 + 2100) * 3))
    assert ex["atm_iv"] == pytest.approx((0.2 + 0.25) / 2)        # only the 20-day 100 strike is 7-45 days, within 1 %
    assert ex["max_pain_strike"] in (95.0, 100.0, 105.0)


class _FakeEngine:
    def __init__(self, sel, prof):
        self.sel, self.prof = sel, prof

    def __call__(self, engine):
        return self

    def latest_completed_capture_batch(self, ticker, d, cutoff):
        self.cutoff = cutoff
        return self.sel

    def compute_gex_profile(self, ticker, d, capture_batch_id=None, cutoff=None):
        return self.prof

    def _load_chain(self, ticker, d, capture_batch_id=None, cutoff=None):
        return _chain()

    def _compute_per_strike(self, chain, spot):
        return [{"call_gex": 2e8, "put_gex": -5e7}, {"call_gex": 1e8, "put_gex": -1e8}]


def _patch(monkeypatch, fake):
    import physics.dealer_gamma as DG

    monkeypatch.setattr(DG, "DealerGammaEngine", fake)


def test_capture_from_another_day_is_never_used(monkeypatch):
    fake = _FakeEngine((date(2026, 11, 11), "b"), {})
    _patch(monkeypatch, fake)
    assert "skip" in G.compute_row(object(), date(2026, 11, 12))
    assert fake.cutoff == G.day_cutoff(date(2026, 11, 12))


def test_no_flip_is_skipped_not_written(monkeypatch):
    _patch(monkeypatch, _FakeEngine((date(2026, 11, 12), "b"), {"spot": 100.0, "gamma_flip": None, "gex_aggregate": 1.0}))
    assert G.compute_row(object(), date(2026, 11, 12))["skip"].startswith("no GEX zero crossing")


def test_row_units_are_millions_per_one_percent(monkeypatch):
    prof = {"spot": 100.0, "gamma_flip": 98.0, "gex_aggregate": 4e9, "regime": "positive_gamma",
            "chain_capture_completed_at": "2026-11-12T22:30:00+00:00"}
    _patch(monkeypatch, _FakeEngine((date(2026, 11, 12), "b"), prof))
    row = G.compute_row(object(), date(2026, 11, 12))
    assert row["net_gex_usd_m"] == pytest.approx(4e9 * 100 * 0.01 / 1e6)
    assert row["call_gex_usd_m"] == pytest.approx(3e8 * 1.0 / 1e6)
    assert row["spot_to_flip_pct"] == pytest.approx(2.0)
    assert row["gex_regime"] == "positive_gamma" and row["capture_batch_id"] == "b"
