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

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

from ingestion.altdata.dark_pool import (
    _ATS_AGG_SUMMARY_CODE,
    _ATS_FIRM_SUMMARY_CODE,
    _FETCH_OVERLAP_WEEKS,
    _FETCH_WINDOW_WEEKS,
    _MAX_FETCH_WEEKS,
    _MAX_PAGES,
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


def test_fetch_weekly_page_omits_ticker_filter_without_tickers():
    p = _puller()
    captured: dict = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        captured["json"] = json
        resp = MagicMock(status_code=200)
        resp.json = lambda: []
        return resp

    with patch("ingestion.altdata.dark_pool.requests.post", side_effect=fake_post):
        p._fetch_weekly_page(tickers=None)
    fields = [f["fieldName"] for f in captured["json"]["domainFilters"]]
    assert fields == ["summaryTypeCode"]


def test_fetch_weekly_page_requests_only_ats_partitions():
    """OTC_* rows are dropped by the parser, so they are never requested."""
    p = _puller()
    captured: dict = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        captured["json"] = json
        resp = MagicMock(status_code=200)
        resp.json = lambda: []
        return resp

    with patch("ingestion.altdata.dark_pool.requests.post", side_effect=fake_post):
        p._fetch_weekly_page(tickers=["SPY"])
    by_field = {f["fieldName"]: f["values"] for f in captured["json"]["domainFilters"]}
    assert by_field["summaryTypeCode"] == [_ATS_AGG_SUMMARY_CODE, _ATS_FIRM_SUMMARY_CODE]


# ── Fetch window (2026-11-07 TIMEOUT: every run asked for 2020-01-01..today) ──


def _resp(records: list, total: int | None) -> MagicMock:
    resp = MagicMock(status_code=200)
    resp.json = lambda: records
    resp.headers = {"Record-Total": str(total)} if total is not None else {}
    return resp


def _window_puller(latest: date | None) -> tuple[DarkPoolPuller, list[dict]]:
    p = _puller()
    p._latest_stored_week = lambda ticker: latest  # type: ignore[method-assign]
    return p, []


def _run(p: DarkPoolPuller, pages: list[MagicMock], **kwargs) -> tuple[dict, list[dict]]:
    bodies: list[dict] = []
    queue = list(pages)

    def fake_post(url, json=None, headers=None, timeout=None):
        bodies.append(json)
        return queue.pop(0)

    with patch("ingestion.altdata.dark_pool.requests.post", side_effect=fake_post), \
            patch("ingestion.altdata.dark_pool.time.sleep"):
        out = p.pull_weekly(**kwargs)
    return out, bodies


def test_scheduled_run_never_requests_full_history():
    """The default call must not ask FINRA for 2020-01-01 (296,280 records)."""
    today = date.today()
    p, _ = _window_puller(latest=today - timedelta(weeks=6))
    _, bodies = _run(p, [_resp([], 0)])
    start = date.fromisoformat(bodies[0]["dateRangeFilters"][0]["startDate"])
    assert start == today - timedelta(weeks=_FETCH_WINDOW_WEEKS)


def test_scheduled_run_widens_window_to_cover_a_stored_gap():
    today = date.today()
    latest = today - timedelta(weeks=30)
    p, _ = _window_puller(latest=latest)
    _, bodies = _run(p, [_resp([], 0)])
    start = date.fromisoformat(bodies[0]["dateRangeFilters"][0]["startDate"])
    assert start == latest - timedelta(weeks=_FETCH_OVERLAP_WEEKS)


def test_scheduled_run_window_is_capped_at_a_year():
    today = date.today()
    for latest in (None, today - timedelta(weeks=300)):
        p, _ = _window_puller(latest=latest)
        _, bodies = _run(p, [_resp([], 0)])
        start = date.fromisoformat(bodies[0]["dateRangeFilters"][0]["startDate"])
        assert start == today - timedelta(weeks=_MAX_FETCH_WEEKS)


def test_explicit_start_date_is_a_manual_backfill():
    p, _ = _window_puller(latest=date.today())
    _, bodies = _run(p, [_resp([], 0)], start_date="2025-01-06")
    assert bodies[0]["dateRangeFilters"][0]["startDate"] == "2025-01-06"


def test_pagination_stops_at_record_total():
    """A short page with more records left is not the end; Record-Total is."""
    p, _ = _window_puller(latest=date.today() - timedelta(weeks=2))
    rec = _firm("SPY", (date.today() - timedelta(weeks=1)).isoformat(), 10, 1, _ATS_AGG_SUMMARY_CODE)
    pages = [_resp([rec] * 3, 5), _resp([rec] * 2, 5)]
    with patch.object(DarkPoolPuller, "_get_existing_dates", return_value=set()), \
            patch.object(DarkPoolPuller, "_insert_raw"), \
            patch.object(DarkPoolPuller, "_emit_spike_signal"):
        out, bodies = _run(p, pages)
    assert [b["offset"] for b in bodies] == [0, 3]
    assert out["status"] == "SUCCESS"


def test_page_budget_exhaustion_is_partial_not_success():
    p, _ = _window_puller(latest=date.today() - timedelta(weeks=2))
    rec = _firm("SPY", (date.today() - timedelta(weeks=1)).isoformat(), 10, 1, _ATS_AGG_SUMMARY_CODE)
    pages = [_resp([rec], 10_000) for _ in range(_MAX_PAGES)]
    with patch.object(DarkPoolPuller, "_get_existing_dates", return_value=set()), \
            patch.object(DarkPoolPuller, "_insert_raw"), \
            patch.object(DarkPoolPuller, "_emit_spike_signal"):
        out, bodies = _run(p, pages)
    assert len(bodies) == _MAX_PAGES
    assert out["status"] == "PARTIAL"
    assert "page budget" in out["error"]


def test_smart_scheduler_gives_dark_pool_headroom_over_its_window():
    from ingestion.smart_scheduler import PULLER_REGISTRY

    entry = next(p for p in PULLER_REGISTRY if p["name"] == "dark_pool")
    assert entry["method"] == "pull_weekly"
    assert entry["timeout_s"] >= 120


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
