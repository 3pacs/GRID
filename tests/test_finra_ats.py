"""Tests for the FINRA ATS puller's weeklySummary request (finra_ats.py).

FINRA_ATS failed every scheduled run with ``400 Client Error: Bad Request``
for ``otcMarket/name/weeklyDownloadDetails``. That dataset name is not in
FINRA's API catalog or OTC Transparency API spec, and the request also sent
``sortFields`` without the EQUAL partition compareFilters FINRA requires for
sorting. These tests pin the corrected request (the same shape
``dark_pool.py`` uses for the same dataset) and the ATS-only aggregation.
All HTTP and DB access is mocked.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import requests

from ingestion.altdata import finra_ats
from ingestion.altdata.finra_ats import FINRAATSPuller


def _puller() -> FINRAATSPuller:
    """Build a puller without touching BasePuller.__init__ (no DB)."""
    p = FINRAATSPuller.__new__(FINRAATSPuller)
    p.engine = MagicMock()
    p.source_id = 1
    return p


def _rec(symbol: str, week: str, code: str, volume: float) -> dict:
    return {
        "issueSymbolIdentifier": symbol,
        "weekStartDate": week,
        "summaryTypeCode": code,
        "totalWeeklyShareQuantity": volume,
        "totalWeeklyTradeCount": 1,
    }


# ── Request construction ──

def test_ats_url_is_weekly_summary_not_weekly_download_details():
    assert finra_ats._ATS_VOLUME_URL == (
        "https://api.finra.org/data/group/otcMarket/name/weeklySummary"
    )
    assert "weeklyDownloadDetails" not in finra_ats._ATS_VOLUME_URL


def test_fetch_ats_page_uses_daterange_and_domain_filters_without_sort():
    p = _puller()
    captured: dict = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        captured.update(url=url, json=json, headers=headers)
        resp = MagicMock(status_code=200)
        resp.json = list
        return resp

    with patch("ingestion.altdata.finra_ats.requests.post", side_effect=fake_post):
        p._fetch_ats_page(
            offset=5000,
            limit=100,
            tickers=["SPY", "QQQ"],
            start_date=date(2026, 1, 5),
            end_date=date(2026, 10, 19),
        )

    body = captured["json"]
    assert captured["url"] == finra_ats._ATS_VOLUME_URL
    assert "sortFields" not in body
    assert body["dateRangeFilters"] == [
        {"fieldName": "weekStartDate", "startDate": "2026-01-05", "endDate": "2026-10-19"}
    ]
    assert body["domainFilters"] == [
        {"fieldName": "issueSymbolIdentifier", "values": ["SPY", "QQQ"]}
    ]
    assert body["offset"] == 5000 and body["limit"] == 100
    assert captured["headers"]["Content-Type"] == "application/json"


def test_fetch_ats_page_defaults_to_rolling_window():
    p = _puller()
    captured: dict = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        captured["json"] = json
        resp = MagicMock(status_code=200)
        resp.json = list
        return resp

    with patch("ingestion.altdata.finra_ats.requests.post", side_effect=fake_post):
        p._fetch_ats_page(tickers=["SPY"])

    drf = captured["json"]["dateRangeFilters"][0]
    end = date.fromisoformat(drf["endDate"])
    start = date.fromisoformat(drf["startDate"])
    assert end == date.today()
    assert end - start == timedelta(days=finra_ats._ATS_WINDOW_DAYS)


def test_fetch_ats_page_handles_204_no_content():
    p = _puller()
    resp = MagicMock(status_code=204)
    with patch("ingestion.altdata.finra_ats.requests.post", return_value=resp):
        assert p._fetch_ats_page(tickers=["SPY"]) == []
    resp.raise_for_status.assert_not_called()


# ── Parsing ──

def test_parse_keeps_ats_only_and_prefers_symbol_aggregate():
    p = _puller()
    records = [
        # SPY week 1: aggregate present -> firm rows must not be added on top.
        _rec("SPY", "2026-09-28", "ATS_W_SMBL", 1000),
        _rec("SPY", "2026-09-28", "ATS_W_SMBL_FIRM", 600),
        _rec("SPY", "2026-09-28", "ATS_W_SMBL_FIRM", 400),
        # Non-ATS OTC volume is not dark-pool volume.
        _rec("SPY", "2026-09-28", "OTC_W_SMBL", 99999),
        _rec("SPY", "2026-09-28", "OTC_W_SMBL_FIRM", 88888),
        # QQQ week 1: no aggregate -> fall back to the firm-row sum.
        _rec("QQQ", "2026-09-28", "ATS_W_SMBL_FIRM", 250),
        _rec("QQQ", "2026-09-28", "ATS_W_SMBL_FIRM", 50),
        # SPY week 2.
        _rec("SPY", "2026-10-05", "ATS_W_SMBL", 700),
    ]

    by_week = p._parse_ats_records(records)

    assert set(by_week) == {date(2026, 9, 28), date(2026, 10, 5)}
    assert by_week[date(2026, 9, 28)]["ats_volume"] == 1300.0  # 1000 + 300
    assert by_week[date(2026, 9, 28)]["raw_records"] == 2  # SPY, QQQ
    assert by_week[date(2026, 10, 5)]["ats_volume"] == 700.0
    # weeklySummary has no consolidated volume, so no dark_pct is derivable.
    for agg in by_week.values():
        assert agg["total_volume"] == 0.0
        assert agg["dark_pct"] == 0.0


def test_parse_skips_bad_dates_and_missing_volume():
    p = _puller()
    records = [
        _rec("SPY", "not-a-date", "ATS_W_SMBL", 10),
        {"issueSymbolIdentifier": "SPY", "weekStartDate": "2026-09-28",
         "summaryTypeCode": "ATS_W_SMBL"},
        _rec("SPY", "2026-10-05", "ATS_W_SMBL", 5),
    ]
    by_week = p._parse_ats_records(records)
    assert list(by_week) == [date(2026, 10, 5)]
    assert by_week[date(2026, 10, 5)]["ats_volume"] == 5.0


# ── pull_ats_volume ──

def _wire_db(p: FINRAATSPuller, existing: set[date] | None = None) -> list[dict]:
    inserted: list[dict] = []

    @contextmanager
    def begin():
        yield MagicMock()

    p.engine.begin = begin
    p._get_existing_dates = lambda sid, conn: set(existing or ())
    p._insert_raw = lambda **kw: inserted.append(kw)
    return inserted


def test_pull_ats_volume_clamps_window_and_stores_volume_only():
    p = _puller()
    inserted = _wire_db(p, existing={date(2026, 9, 28)})
    calls: list[dict] = []

    def fake_fetch(**kw):
        calls.append(kw)
        return [
            _rec("SPY", "2026-09-28", "ATS_W_SMBL", 1000),
            _rec("SPY", "2026-10-05", "ATS_W_SMBL", 700),
            _rec("SPY", "2026-10-05", "OTC_W_SMBL", 5000),
        ]

    p._fetch_ats_page = fake_fetch
    result = p.pull_ats_volume(start_date="2020-01-01", tickers=["SPY"])

    assert len(calls) == 1  # short page -> no second request
    assert calls[0]["end_date"] == date.today()
    assert calls[0]["start_date"] == date.today() - timedelta(days=finra_ats._ATS_WINDOW_DAYS)
    assert calls[0]["tickers"] == ["SPY"]

    assert result["status"] == "SUCCESS"
    # 2026-09-28 already stored; only 2026-10-05 volume is new. No
    # ats_dark_pct row, because total volume is unknown.
    assert result["per_feature"] == {"ats_total_volume": 1, "ats_dark_pct": 0}
    assert result["rows_inserted"] == 1
    assert [(r["series_id"], r["obs_date"], r["value"]) for r in inserted] == [
        ("finra.ats_total_volume", date(2026, 10, 5), 700.0)
    ]
    assert inserted[0]["raw_payload"]["source_url"] == finra_ats._ATS_VOLUME_URL


def test_pull_ats_volume_reports_failed_on_http_error():
    p = _puller()
    _wire_db(p)

    def boom(**kw):
        raise requests.HTTPError("400 Client Error: Bad Request")

    p._fetch_ats_page = boom
    result = p.pull_ats_volume(tickers=["SPY"])
    assert result["status"] == "FAILED"
    assert result["rows_inserted"] == 0
    assert "400" in result["error"]
