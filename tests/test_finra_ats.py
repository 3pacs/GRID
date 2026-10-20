"""Tests for the FINRA ATS puller's weeklySummary request (finra_ats.py).

FINRA_ATS failed every scheduled run with ``400 Client Error: Bad Request``
for ``otcMarket/name/weeklyDownloadDetails``. That dataset name is not in
FINRA's API catalog or OTC Transparency API spec, and the request also sent
``sortFields`` without the EQUAL partition compareFilters FINRA requires for
sorting. These tests pin the corrected request (the same shape
``dark_pool.py`` uses for the same dataset) and the ATS-only aggregation.

``pull_short_interest`` (``otcMarket/name/consolidatedShortInterest``) had
the same defect: ``sortFields ["-settlementDate"]`` with no partition filter,
answered live on 2026-10-20 with HTTP 400 "Sorting is allowed only if all
partitions keys are specified in EQUAL CompareFilter. Partition keys missing
or not using EQUAL CompareFilter: settlementDate". The short-interest tests
pin the corrected request (``dateRangeFilters`` on settlementDate, verified
200 by one anonymous 1-row probe), 204-as-empty, the segmented window walk,
and ``pull_all``'s per-sub-pull outcome tagging.
All HTTP and DB access is mocked.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, timedelta
from itertools import pairwise
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
    resp = MagicMock(status_code=204, headers={})
    with patch("ingestion.altdata.finra_ats.requests.post", return_value=resp):
        page = p._fetch_ats_page(tickers=["SPY"])
    assert page.records == []
    assert page.total is None
    resp.raise_for_status.assert_not_called()


def test_fetch_ats_page_returns_validated_record_total():
    p = _puller()
    for header, expected in (({"Record-Total": "20176"}, 20176),
                             ({"record-total": " 7 "}, None),  # plain dict: exact name only
                             ({"Record-Total": "6k"}, None),
                             ({"Record-Total": "-1"}, None),
                             ({}, None)):
        resp = MagicMock(status_code=200, headers=header)
        resp.json = list
        with patch("ingestion.altdata.finra_ats.requests.post", return_value=resp):
            page = p._fetch_ats_page(tickers=["SPY"])
        assert page.records == []
        assert page.total == expected, header


def test_fetch_ats_page_reads_total_case_insensitively_from_requests_headers():
    from requests.structures import CaseInsensitiveDict

    p = _puller()
    resp = MagicMock(status_code=200, headers=CaseInsensitiveDict({"record-total": "20176"}))
    resp.json = list
    with patch("ingestion.altdata.finra_ats.requests.post", return_value=resp):
        assert p._fetch_ats_page(tickers=["SPY"]).total == 20176


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
        return finra_ats._ATSPage([
            _rec("SPY", "2026-09-28", "ATS_W_SMBL", 1000),
            _rec("SPY", "2026-10-05", "ATS_W_SMBL", 700),
            _rec("SPY", "2026-10-05", "OTC_W_SMBL", 5000),
        ], 3)

    p._fetch_ats_page = fake_fetch
    result = p.pull_ats_volume(start_date="2020-01-01", tickers=["SPY"])

    assert len(calls) == 1  # Record-Total 3 reached -> no second request
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


# ── pull_ats_volume pagination (HTTP-level fake; no network, no DB) ──
#
# FINRA's synchronous Query API can return fewer rows than ``limit`` while
# more remain (response-size cap), and reports the full match count in the
# ``Record-Total`` response header. A short page is therefore not the last
# page; only offset == Record-Total proves complete coverage.

class _Resp:
    def __init__(self, status_code: int, body: list | None, headers: dict):
        self.status_code = status_code
        self._body = body
        self.headers = headers

    def json(self):
        return self._body

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Client Error")


class _FinraServer:
    """Fake weeklySummary: serves records[offset:offset + min(limit, cap)]."""

    def __init__(self, records, cap, *, totals=None, ignore_offset=False,
                 empty_at=None, empty_status=204):
        self.records = records
        self.cap = cap
        self.totals = totals  # callable(call_index) -> header value | None
        self.ignore_offset = ignore_offset
        self.empty_at = empty_at
        self.empty_status = empty_status
        self.offsets: list[int] = []

    def post(self, url, json=None, headers=None, timeout=None):
        offset, limit = json["offset"], json["limit"]
        call = len(self.offsets)
        self.offsets.append(offset)
        total = self.totals(call) if self.totals else str(len(self.records))
        hdrs = {} if total is None else {"Record-Total": total}
        if offset == self.empty_at:
            return _Resp(self.empty_status, [] if self.empty_status == 200 else None, hdrs)
        start = 0 if self.ignore_offset else offset
        page = self.records[start:start + min(limit, self.cap)]
        if not page:
            return _Resp(204, None, hdrs)
        return _Resp(200, page, hdrs)


def _weekly_records(weeks: int, per_week: int = 100) -> list[dict]:
    """``weeks`` distinct weeks x ``per_week`` tickers, ordered by week."""
    first = date(2025, 1, 6)
    return [
        _rec(f"T{t:03d}", (first + timedelta(weeks=w)).isoformat(), "ATS_W_SMBL", 1)
        for w in range(weeks)
        for t in range(per_week)
    ]


def _pull_with(server: _FinraServer):
    p = _puller()
    inserted = _wire_db(p)
    with patch("ingestion.altdata.finra_ats.requests.post", side_effect=server.post), \
            patch("ingestion.altdata.finra_ats.time.sleep"):
        result = p.pull_ats_volume(start_date="2020-01-01", tickers=["SPY"])
    return result, inserted


def test_pagination_capped_short_page_continues_to_record_total():
    server = _FinraServer(_weekly_records(60), cap=3000)  # 6000 rows, limit 5000
    result, inserted = _pull_with(server)
    assert server.offsets == [0, 3000]
    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 60  # one ats_total_volume row per week
    assert result.get("records_fetched") == 6000
    assert result.get("record_total") == 6000
    assert len(inserted) == 60


def test_pagination_exact_total_stops_without_extra_request():
    server = _FinraServer(_weekly_records(100), cap=5000)  # 10000 rows
    result, _ = _pull_with(server)
    assert server.offsets == [0, 5000]
    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 100


def test_pagination_partial_final_page_under_cap():
    server = _FinraServer(_weekly_records(70), cap=3000)  # 3000 + 3000 + 1000
    result, _ = _pull_with(server)
    assert server.offsets == [0, 3000, 6000]
    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 70


def test_pagination_204_on_empty_dataset_is_success_with_no_rows():
    server = _FinraServer([], cap=5000, totals=lambda call: None)
    result, inserted = _pull_with(server)
    assert server.offsets == [0]
    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 0
    assert inserted == []


def test_pagination_premature_204_fails_and_stores_nothing():
    server = _FinraServer(_weekly_records(60), cap=3000, empty_at=3000, empty_status=204)
    result, inserted = _pull_with(server)
    assert result["status"] == "FAILED"
    assert inserted == []


def test_pagination_no_progress_empty_200_fails_and_stores_nothing():
    server = _FinraServer(_weekly_records(60), cap=3000, empty_at=3000, empty_status=200)
    result, inserted = _pull_with(server)
    assert result["status"] == "FAILED"
    assert inserted == []


def test_pagination_missing_total_on_short_page_fails():
    server = _FinraServer(_weekly_records(60), cap=3000, totals=lambda call: None)
    result, inserted = _pull_with(server)
    assert result["status"] == "FAILED"
    assert "Record-Total" in result["error"]
    assert inserted == []


def test_pagination_malformed_total_fails():
    server = _FinraServer(_weekly_records(60), cap=3000, totals=lambda call: "6k")
    result, inserted = _pull_with(server)
    assert result["status"] == "FAILED"
    assert "Record-Total" in result["error"]
    assert inserted == []


def test_pagination_changing_total_fails():
    server = _FinraServer(_weekly_records(60), cap=3000,
                          totals=lambda call: "6000" if call == 0 else "6500")
    result, inserted = _pull_with(server)
    assert result["status"] == "FAILED"
    assert inserted == []


def test_pagination_total_below_rows_returned_fails():
    server = _FinraServer(_weekly_records(60), cap=3000, totals=lambda call: "2000")
    result, inserted = _pull_with(server)
    assert result["status"] == "FAILED"
    assert inserted == []


def test_pagination_repeated_page_fails():
    server = _FinraServer(_weekly_records(60), cap=3000, ignore_offset=True)
    result, inserted = _pull_with(server)
    assert server.offsets[:2] == [0, 3000]
    assert result["status"] == "FAILED"
    assert inserted == []


def test_pagination_page_budget_is_bounded(monkeypatch):
    monkeypatch.setattr(finra_ats, "_MAX_ATS_PAGES", 2, raising=False)
    server = _FinraServer(_weekly_records(60), cap=1000)  # needs 6 pages
    result, inserted = _pull_with(server)
    assert len(server.offsets) <= 2
    assert result["status"] == "FAILED"
    assert "page budget" in result["error"]
    assert inserted == []


# ── Short interest: request construction ──

# Exact FINRA body from the 2026-10-20 executor receipt (Phase C, short
# interest): the old request's sortFields on the partition key.
_SI_400_TEXT = (
    "Sorting is allowed only if all partitions keys are specified in EQUAL "
    "CompareFilter. Partition keys missing or not using EQUAL CompareFilter: "
    "settlementDate"
)


def _si_rec(symbol: str, settlement: str, qty: float) -> dict:
    return {
        "symbolCode": symbol,
        "settlementDate": settlement,
        "currentShortPositionQuantity": qty,
        "marketClassCode": "ARCA",
    }


def test_short_interest_url_is_consolidated_short_interest():
    assert finra_ats._SHORT_INTEREST_URL == (
        "https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest"
    )


def test_fetch_short_interest_page_uses_daterange_and_domain_filters_without_sort():
    p = _puller()
    captured: dict = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        captured.update(url=url, json=json, headers=headers)
        resp = MagicMock(status_code=200)
        resp.json = list
        return resp

    with patch("ingestion.altdata.finra_ats.requests.post", side_effect=fake_post):
        p._fetch_short_interest_page(
            offset=5000,
            limit=100,
            tickers=["SPY", "QQQ"],
            start_date=date(2025, 10, 20),
            end_date=date(2026, 10, 20),
        )

    body = captured["json"]
    assert captured["url"] == finra_ats._SHORT_INTEREST_URL
    assert "sortFields" not in body
    assert body["dateRangeFilters"] == [
        {"fieldName": "settlementDate", "startDate": "2025-10-20", "endDate": "2026-10-20"}
    ]
    assert body["domainFilters"] == [
        {"fieldName": "symbolCode", "values": ["SPY", "QQQ"]}
    ]
    assert body["offset"] == 5000 and body["limit"] == 100
    assert captured["headers"]["Content-Type"] == "application/json"


def test_fetch_short_interest_page_defaults_to_si_range():
    p = _puller()
    captured: dict = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        captured["json"] = json
        resp = MagicMock(status_code=200)
        resp.json = list
        return resp

    with patch("ingestion.altdata.finra_ats.requests.post", side_effect=fake_post):
        p._fetch_short_interest_page(tickers=["SPY"])

    drf = captured["json"]["dateRangeFilters"][0]
    assert drf["fieldName"] == "settlementDate"
    end = date.fromisoformat(drf["endDate"])
    start = date.fromisoformat(drf["startDate"])
    assert end == date.today()
    assert end - start == timedelta(days=finra_ats._SI_RANGE_DAYS)


def test_fetch_short_interest_page_omits_domainfilters_without_tickers():
    p = _puller()
    captured: dict = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        captured["json"] = json
        resp = MagicMock(status_code=200)
        resp.json = list
        return resp

    with patch("ingestion.altdata.finra_ats.requests.post", side_effect=fake_post):
        p._fetch_short_interest_page(tickers=None)

    assert "domainFilters" not in captured["json"]
    assert "sortFields" not in captured["json"]


def test_fetch_short_interest_page_handles_204_no_content():
    p = _puller()
    resp = MagicMock(status_code=204)
    with patch("ingestion.altdata.finra_ats.requests.post", return_value=resp):
        assert p._fetch_short_interest_page(tickers=["SPY"]) == []
    resp.raise_for_status.assert_not_called()
    resp.json.assert_not_called()


def test_fetch_short_interest_page_does_not_trigger_partition_sort_400():
    """A FINRA stand-in that 400s exactly like the live API did on sortFields."""
    p = _puller()
    record = _si_rec("SPY", "2025-10-31", 108765577)

    def finra(url, json=None, headers=None, timeout=None):
        if "sortFields" in json:
            resp = MagicMock(status_code=400)
            resp.raise_for_status.side_effect = requests.HTTPError(
                f"400 Client Error: Bad Request for url: {url} :: {_SI_400_TEXT}"
            )
            return resp
        resp = MagicMock(status_code=200)
        resp.json = lambda: [record]
        return resp

    with patch("ingestion.altdata.finra_ats.requests.post", side_effect=finra):
        page = p._fetch_short_interest_page(
            tickers=["SPY"],
            start_date=date(2025, 10, 20),
            end_date=date(2026, 10, 20),
        )

    assert page == [record]


# ── Short interest: window walk and storage ──

def test_short_interest_segments_cover_window_without_overlap():
    segs = FINRAATSPuller._short_interest_segments(date(2024, 1, 1), date(2026, 10, 20))

    assert segs[0][0] == date(2024, 1, 1)
    assert segs[-1][1] == date(2026, 10, 20)
    assert len(segs) == 3
    for a, b in segs:
        assert a <= b
        assert (b - a).days <= finra_ats._SI_RANGE_DAYS
    for (_, b1), (a2, _) in pairwise(segs):
        assert a2 == b1 + timedelta(days=1)

    assert FINRAATSPuller._short_interest_segments(date(2026, 10, 1), date(2026, 10, 20)) == [
        (date(2026, 10, 1), date(2026, 10, 20))
    ]
    assert FINRAATSPuller._short_interest_segments(date(2026, 10, 21), date(2026, 10, 20)) == []


def test_pull_short_interest_walks_window_in_segments_and_stores():
    p = _puller()
    inserted = _wire_db(p, existing={date(2025, 10, 31)})
    calls: list[dict] = []
    rows = [
        ("SPY", "2025-10-31", 100.0),
        ("QQQ", "2025-10-31", 50.0),
        ("SPY", "2026-09-30", 120.0),
    ]

    def fake_fetch(**kw):
        calls.append(kw)
        return [
            _si_rec(sym, d, q)
            for sym, d, q in rows
            if kw["start_date"] <= date.fromisoformat(d) <= kw["end_date"]
        ]

    p._fetch_short_interest_page = fake_fetch
    with patch("ingestion.altdata.finra_ats.time.sleep") as slept:
        result = p.pull_short_interest(start_date="2024-01-01", tickers=["SPY", "QQQ"])

    expected = FINRAATSPuller._short_interest_segments(date(2024, 1, 1), date.today())
    assert [(c["start_date"], c["end_date"]) for c in calls] == expected
    assert all(c["tickers"] == ["SPY", "QQQ"] and c["offset"] == 0 for c in calls)
    # Rate-limit pause between requests only, never before the first.
    assert slept.call_count == len(calls) - 1

    assert result["status"] == "SUCCESS"
    assert result["per_feature"] == {"short_interest_total": 1}
    assert result["rows_inserted"] == 1
    # 2025-10-31 already stored; 2026-09-30 is new.
    assert [(r["series_id"], r["obs_date"], r["value"]) for r in inserted] == [
        ("finra.short_interest_total", date(2026, 9, 30), 120.0)
    ]
    payload = inserted[0]["raw_payload"]
    assert payload["symbols"] == ["SPY"]
    assert payload["raw_records"] == 1
    assert payload["source_url"] == finra_ats._SHORT_INTEREST_URL


def test_pull_short_interest_paginates_within_a_segment():
    p = _puller()
    inserted = _wire_db(p)
    calls: list[dict] = []
    today = date.today()
    d1 = (today - timedelta(days=5)).isoformat()
    d2 = (today - timedelta(days=3)).isoformat()

    def fake_fetch(**kw):
        calls.append(kw)
        if kw["offset"] == 0:
            return [_si_rec("SPY", d1, 10.0), _si_rec("QQQ", d1, 5.0)]
        return [_si_rec("SPY", d2, 7.0)]

    p._fetch_short_interest_page = fake_fetch
    with patch.object(finra_ats, "_PAGE_SIZE", 2), \
            patch("ingestion.altdata.finra_ats.time.sleep"):
        result = p.pull_short_interest(start_date=today - timedelta(days=10), tickers=["SPY", "QQQ"])

    assert [c["offset"] for c in calls] == [0, 2]
    assert all(c["limit"] == 2 for c in calls)
    assert result["rows_inserted"] == 2
    assert [(r["obs_date"], r["value"]) for r in inserted] == [
        (date.fromisoformat(d1), 15.0),
        (date.fromisoformat(d2), 7.0),
    ]


def test_pull_short_interest_reports_failed_on_partition_400():
    p = _puller()
    _wire_db(p)

    def boom(**kw):
        raise requests.HTTPError(f"400 Client Error: Bad Request :: {_SI_400_TEXT}")

    p._fetch_short_interest_page = boom
    result = p.pull_short_interest(tickers=["SPY"])
    assert result["status"] == "FAILED"
    assert result["rows_inserted"] == 0
    assert "settlementDate" in result["error"]


# ── pull_all: per-sub-pull outcomes ──

def _ok(rows: int, **per_feature) -> dict:
    return {"status": "SUCCESS", "rows_inserted": rows, "per_feature": per_feature}


def _failed(error: str) -> dict:
    return {"status": "FAILED", "rows_inserted": 0, "error": error}


def test_pull_all_tags_sub_pulls_and_scheduler_classifies_per_item():
    from ingestion.smart_scheduler import (
        OUTCOME_FAILED,
        OUTCOME_PARTIAL,
        OUTCOME_SUCCESS,
        _classify_outcome,
    )

    p = _puller()
    p.pull_ats_volume = lambda **kw: _ok(50, ats_total_volume=50, ats_dark_pct=0)
    p.pull_short_interest = lambda **kw: _failed(f"400 Client Error :: {_SI_400_TEXT}")
    with patch("ingestion.altdata.finra_ats.time.sleep"):
        results = p.pull_all()

    assert [r["source"] for r in results] == ["ats_volume", "short_interest"]
    assert results[0]["status"] == "SUCCESS" and "error" not in results[0]
    assert results[1]["error"].startswith("short_interest: 400")

    # The ATS half succeeding must not be reported as a whole-source FAILED,
    # and the SI half failing must not be hidden behind a SUCCESS.
    outcome, rows, note = _classify_outcome(results)
    assert outcome == OUTCOME_PARTIAL
    assert rows == 50
    assert "short_interest" in note and "settlementDate" in note

    # Both halves failing is a FAILED run.
    p.pull_ats_volume = lambda **kw: _failed("boom")
    with patch("ingestion.altdata.finra_ats.time.sleep"):
        both_failed = p.pull_all()
    assert _classify_outcome(both_failed)[0] == OUTCOME_FAILED
    assert [r["error"] for r in both_failed] == [
        "ats_volume: boom",
        f"short_interest: 400 Client Error :: {_SI_400_TEXT}",
    ]

    # Both halves succeeding is a SUCCESS with the summed row count.
    p.pull_ats_volume = lambda **kw: _ok(50, ats_total_volume=50, ats_dark_pct=0)
    p.pull_short_interest = lambda **kw: _ok(12, short_interest_total=12)
    with patch("ingestion.altdata.finra_ats.time.sleep"):
        both_ok = p.pull_all()
    assert _classify_outcome(both_ok)[:2] == (OUTCOME_SUCCESS, 62)
