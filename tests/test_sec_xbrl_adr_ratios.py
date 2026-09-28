"""ADR ratio table for the SEC XBRL shares puller (ticker_metrics_daily).

``shares_outstanding`` in ticker_metrics_daily is stored per US-listed unit
(ordinary shares / ratio) and ``market_cap_usd = shares * close`` where the
close is YF:{TICKER}:close from raw_series, which is stored UNADJUSTED. So the
ratio must be the one in force on each obs_date.

Sources for the corrected entries (2026-09-28):
  * SNY  — Sanofi 20-F FY2025 exhibit 2.2: each ADS represents one-half of one
           ordinary share (JPMorgan depositary). 0.5, not 2.
  * AZN  — 6-K 2015-06-26: 1 ADS : 1 share -> 2 ADSs : 1 share from 2015-07-27.
           6-K 2026-01-20: ADSs leave Nasdaq 2026-01-30, ordinary shares trade
           on the NYSE from 2026-02-02 (ratio 1 from then on).
  * HEINY — Heineken ADR page: two ADRs represent one ordinary share. 0.5.
"""

from __future__ import annotations

from datetime import date

import pytest

import ingestion.altdata.sec_xbrl_shares as mod
from ingestion.altdata.sec_xbrl_shares import (
    _ADR_RATIO_HISTORY,
    _ADR_RATIOS,
    _adr_ratio_for,
)

# ── Static table ──

def test_sny_is_half_a_share_per_ads():
    assert _adr_ratio_for("SNY") == 0.5
    assert _adr_ratio_for("SNY", date(2026, 9, 25)) == 0.5
    assert _adr_ratio_for("sny ", date(2003, 1, 2)) == 0.5


def test_heiny_is_half_a_share_per_adr():
    assert _adr_ratio_for("HEINY") == 0.5


def test_azn_current_ratio_is_one_after_direct_listing():
    assert _adr_ratio_for("AZN") == 1.0


@pytest.mark.parametrize(
    "obs, expected",
    [
        (date(2010, 1, 4), 1.0),
        (date(2015, 7, 24), 1.0),   # last session before the 2015 change
        (date(2015, 7, 27), 0.5),   # 2 ADSs : 1 share effective
        (date(2020, 6, 1), 0.5),
        (date(2026, 1, 30), 0.5),   # last Nasdaq ADS session
        (date(2026, 2, 2), 1.0),    # first NYSE ordinary-share session
        (date(2026, 9, 25), 1.0),
    ],
)
def test_azn_ratio_is_point_in_time(obs, expected):
    assert _adr_ratio_for("AZN", obs) == expected


def test_undated_tickers_ignore_obs_date_and_unknown_defaults_to_one():
    assert _adr_ratio_for("TSM", date(2001, 1, 2)) == 5.0
    assert _adr_ratio_for("TSM", date(2026, 9, 25)) == 5.0
    assert _adr_ratio_for("AAPL", date(2026, 9, 25)) == 1.0
    assert _adr_ratio_for("") == 1.0


def test_ratio_history_invariants():
    for ticker, history in _ADR_RATIO_HISTORY.items():
        dates = [d for d, _r in history]
        assert dates[0] == date.min, ticker
        assert dates == sorted(dates) and len(set(dates)) == len(dates), ticker
        assert all(r > 0 for _d, r in history), ticker
        # The undated/current table must agree with the latest history entry.
        assert float(_ADR_RATIOS[ticker]) == float(history[-1][1]), ticker


def test_all_ratios_positive():
    assert all(float(r) > 0 for r in _ADR_RATIOS.values())


# ── Puller applies the ratio per obs_date ──

class _FakeEngine:  # never touched: every DB helper is monkeypatched
    pass


def _run_puller(monkeypatch, ticker, ordinary_shares, closes):
    written: dict[str, list[dict]] = {}
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    monkeypatch.setattr(mod, "_fetch_ticker_to_cik_map", lambda: {ticker: "0000000001"})
    monkeypatch.setattr(mod, "_fetch_company_facts", lambda _cik: {"facts": {}})
    monkeypatch.setattr(
        mod, "_extract_shares_entries",
        lambda _facts: [(date(2015, 1, 1), ordinary_shares)],
    )
    monkeypatch.setattr(mod, "_fetch_close_prices", lambda *_a, **_k: closes)

    def _capture(_engine, t, rows):
        written[t] = rows
        return len(rows)

    monkeypatch.setattr(mod, "_write_rows", _capture)
    mod.SECXBRLSharesPuller(_FakeEngine()).pull_all(
        tickers=[ticker], backfill_days=4000,
    )
    return {r["obs_date"]: r for r in written[ticker]}


def test_puller_uses_azn_ratio_in_force_on_each_date(monkeypatch):
    # Real YF:AZN:close values: ADS price on 01-30, ordinary-share price on 02-02.
    rows = _run_puller(
        monkeypatch, "AZN", 1_549_000_000,
        [(date(2026, 1, 30), 92.77), (date(2026, 2, 2), 188.41)],
    )
    ads = rows[date(2026, 1, 30)]
    ords = rows[date(2026, 2, 2)]
    assert ads["shares"] == 3_098_000_000          # 1.549B / 0.5 ADS-equivalents
    assert ords["shares"] == 1_549_000_000         # direct ordinary shares
    assert ads["mcap"] == pytest.approx(3_098_000_000 * 92.77)
    assert ords["mcap"] == pytest.approx(1_549_000_000 * 188.41)
    # Both sides of the switch describe the same company value (~$287-292B),
    # not a 2x step.
    assert ords["mcap"] / ads["mcap"] == pytest.approx(1.0, rel=0.05)


def test_puller_sny_market_cap_is_ads_equivalent(monkeypatch):
    rows = _run_puller(
        monkeypatch, "SNY", 1_219_427_096, [(date(2026, 9, 25), 41.11)],
    )
    row = rows[date(2026, 9, 25)]
    assert row["shares"] == 2_438_854_192
    # ~$100B (public references ~$99-100B), not the old ~$25B.
    assert row["mcap"] == pytest.approx(2_438_854_192 * 41.11)
    assert 90e9 < row["mcap"] < 110e9
