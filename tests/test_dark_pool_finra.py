"""Tests for the FINRA dark-pool (ATS) puller.

Two concerns are covered offline (mocked HTTP):

1. Request construction — FINRA's weeklySummary endpoint returns HTTP 400
   ("Sorting is allowed only if all partition keys ...") when ``sortFields``
   is used. The puller must instead use ``dateRangeFilters`` on weekStartDate
   plus ``domainFilters`` on issueSymbolIdentifier (verified live: that combo
   returns 200).

2. Record parsing — weeklySummary returns four summaryTypeCode partitions per
   ticker/week (ATS_W_SMBL_FIRM, ATS_W_SMBL, OTC_W_SMBL_FIRM, OTC_W_SMBL).
   Summing all of them (the previous behaviour) inflated dark-pool volume ~4x
   by double-counting firm-vs-aggregate and mixing in non-ATS OTC flow. The
   parser must keep ATS only and use a single summary level.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

from ingestion.altdata.dark_pool import (
    _ATS_AGG_SUMMARY_CODE,
    _ATS_FIRM_SUMMARY_CODE,
    DarkPoolPuller,
)


def _puller() -> DarkPoolPuller:
    """Build a puller without touching BasePuller.__init__ (no DB)."""
    p = DarkPoolPuller.__new__(DarkPoolPuller)
    p.engine = MagicMock()
    p.source_id = 1
    return p


# ── Request construction ──

def test_fetch_weekly_page_uses_daterange_not_sortfields():
    p = _puller()
    captured: dict = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        resp = MagicMock(status_code=200)
        resp.json = lambda: []
        return resp

    with patch("ingestion.altdata.dark_pool.requests.post", side_effect=fake_post):
        p._fetch_weekly_page(
            offset=0,
            limit=100,
            tickers=["SPY", "QQQ"],
            start_date=date(2025, 1, 1),
            end_date=date(2025, 6, 1),
        )

    body = captured["json"]
    # Must NOT use sortFields (that triggers the partition-key 400).
    assert "sortFields" not in body
    # Must use a date range filter on weekStartDate.
    drf = body["dateRangeFilters"]
    assert drf[0]["fieldName"] == "weekStartDate"
    assert drf[0]["startDate"] == "2025-01-01"
    assert drf[0]["endDate"] == "2025-06-01"
    # Must filter tickers via domainFilters on issueSymbolIdentifier.
    dom = body["domainFilters"]
    assert dom[0]["fieldName"] == "issueSymbolIdentifier"
    assert dom[0]["values"] == ["SPY", "QQQ"]
    assert body["offset"] == 0 and body["limit"] == 100
    assert captured["headers"]["Content-Type"] == "application/json"


def test_fetch_weekly_page_handles_204_no_content():
    p = _puller()
    resp = MagicMock(status_code=204)
    with patch("ingestion.altdata.dark_pool.requests.post", return_value=resp):
        assert p._fetch_weekly_page(tickers=["SPY"]) == []


def test_fetch_weekly_page_omits_domainfilters_without_tickers():
    p = _puller()
    captured: dict = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        captured["json"] = json
        resp = MagicMock(status_code=200)
        resp.json = lambda: []
        return resp

    with patch("ingestion.altdata.dark_pool.requests.post", side_effect=fake_post):
        p._fetch_weekly_page(tickers=None)
    assert "domainFilters" not in captured["json"]


# ── Record parsing (ATS-only, no double counting) ──

def _firm(ticker, week, vol, trades, code=_ATS_FIRM_SUMMARY_CODE):
    return {
        "issueSymbolIdentifier": ticker,
        "weekStartDate": week,
        "totalWeeklyShareQuantity": vol,
        "totalWeeklyTradeCount": trades,
        "summaryTypeCode": code,
    }


def test_parse_keeps_ats_aggregate_and_drops_otc_and_firm_dupes():
    # Real-shape week for SPY: per-firm ATS (30M+20M) == aggregate (50M);
    # OTC rows of similar size must be excluded entirely.
    records = [
        _firm("SPY", "2026-05-04", 30_000_000, 300, _ATS_FIRM_SUMMARY_CODE),
        _firm("SPY", "2026-05-04", 20_000_000, 200, _ATS_FIRM_SUMMARY_CODE),
        _firm("SPY", "2026-05-04", 50_000_000, 500, _ATS_AGG_SUMMARY_CODE),
        _firm("SPY", "2026-05-04", 49_000_000, 490, "OTC_W_SMBL_FIRM"),
        _firm("SPY", "2026-05-04", 49_000_000, 490, "OTC_W_SMBL"),
    ]
    out = _puller()._parse_weekly_records(records)
    spy = out["SPY"][date(2026, 5, 4)]
    # Aggregate wins -> 50M, NOT 50M+50M(firms)+49M+49M = ~198M.
    assert spy["volume"] == 50_000_000
    assert spy["trades"] == 500


def test_parse_falls_back_to_firm_sum_when_no_aggregate():
    records = [
        _firm("QQQ", "2026-05-04", 10_000_000, 100, _ATS_FIRM_SUMMARY_CODE),
        _firm("QQQ", "2026-05-04", 5_000_000, 50, _ATS_FIRM_SUMMARY_CODE),
        # OTC present but must be ignored.
        _firm("QQQ", "2026-05-04", 99_000_000, 990, "OTC_W_SMBL"),
    ]
    out = _puller()._parse_weekly_records(records)
    qqq = out["QQQ"][date(2026, 5, 4)]
    assert qqq["volume"] == 15_000_000  # sum of the two firm rows, OTC excluded
    assert qqq["trades"] == 150


def test_parse_excludes_pure_otc_ticker_entirely():
    records = [_firm("IWM", "2026-05-04", 7_000_000, 70, "OTC_W_SMBL")]
    out = _puller()._parse_weekly_records(records)
    assert "IWM" not in out


def test_parse_skips_bad_dates_and_empty_symbols():
    records = [
        _firm("", "2026-05-04", 1_000_000, 10, _ATS_AGG_SUMMARY_CODE),
        _firm("SPY", "not-a-date", 1_000_000, 10, _ATS_AGG_SUMMARY_CODE),
        _firm("SPY", "2026-05-04", 2_000_000, 20, _ATS_AGG_SUMMARY_CODE),
    ]
    out = _puller()._parse_weekly_records(records)
    assert list(out.keys()) == ["SPY"]
    assert out["SPY"][date(2026, 5, 4)]["volume"] == 2_000_000
