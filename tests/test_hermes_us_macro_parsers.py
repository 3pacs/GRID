"""US macro parser fixes from the 2026-11-04 Hermes log-hygiene wave.

Every network call is mocked. Payload shapes are trimmed copies of the live
responses probed from grid-svr on 2026-11-04 (FRED series/observations, BLS
v2 timeseries, NY Fed Markets API SOMA summary + Treasury operations, the NY
Fed Staff Nowcast download workbook, TreasuryDirect securities/auctioned,
NY Fed GSCPI workbook).
"""

from __future__ import annotations

import io
import json
import sys
import warnings
from contextlib import contextmanager
from datetime import date, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests
from loguru import logger

from ingestion import bls as bls_mod
from ingestion.altdata import nyfed as nyfed_mod
from ingestion.altdata.fed_liquidity import FED_LIQUIDITY_SERIES, FedLiquidityPuller
from ingestion.altdata.h8_bank_balance import H8BankBalancePuller
from ingestion.altdata.nyfed import NYFedPermanentError, NYFedPuller
from ingestion.altdata.nyfed_gscpi import _parse_gscpi_date
from ingestion.altdata.repo_market import RepoMarketPuller
from ingestion.altdata.treasury_auction import TreasuryAuctionPuller
from ingestion.bls import BLSPuller
from ingestion.fred import FREDPuller
from ingestion.smart_scheduler import _classify_outcome


# ── shared helpers ────────────────────────────────────────────────────────


@pytest.fixture
def logs():
    """Capture loguru records as (level, message)."""
    records: list[tuple[str, str]] = []
    sink_id = logger.add(lambda m: records.append((m.record["level"].name, m.record["message"])),
                         level="DEBUG")
    yield records
    logger.remove(sink_id)


def _fedfred_frame(observations: list[dict[str, str]]) -> pd.DataFrame:
    """Replica of fedfred.helpers.FredHelpers.to_pd_df (fedfred 3.0.0)."""
    df = pd.DataFrame(observations)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df


def _obs(day: str, value: str) -> dict[str, str]:
    return {"realtime_start": "2026-11-03", "realtime_end": "2026-11-03",
            "date": day, "value": value}


# SOFR around Columbus Day 2026-10-12 ("." on FRED) and ending on a "."
# latest observation, the exact case the brief names.
SOFR_OBS = [
    _obs("2026-10-09", "4.01"),
    _obs("2026-10-12", "."),
    _obs("2026-10-13", "3.99"),
    _obs("2026-11-02", "3.95"),
    _obs("2026-11-03", "."),
]


def _install_fake_fred(monkeypatch, frame: pd.DataFrame) -> SimpleNamespace:
    calls = SimpleNamespace(count=0)

    class _FakeFred:
        def __init__(self, _key):
            pass

        def get_series_observations(self, *_a, **_kw):
            calls.count += 1
            return frame

    monkeypatch.setitem(sys.modules, "fedfred", SimpleNamespace(FredAPI=_FakeFred))
    monkeypatch.setattr("time.sleep", lambda _s: None)
    return calls


def _recording_insert(store: list[tuple[str, date, float]]):
    def _insert_raw(conn, series_id, obs_date, value, raw_payload, return_result=False):
        store.append((series_id, obs_date, value))
        return SimpleNamespace(rowcount=1)
    return _insert_raw


# ── 1. FRED "." markers (repo_market, fed_liquidity) ──────────────────────


def _repo_puller() -> RepoMarketPuller:
    p = RepoMarketPuller.__new__(RepoMarketPuller)
    p._api_key = "test"
    p.source_id = 1
    return p


def test_repo_market_keeps_valid_rows_around_dot_markers(monkeypatch, logs):
    calls = _install_fake_fred(monkeypatch, _fedfred_frame(SOFR_OBS))

    frame = _repo_puller()._fetch_fred_series("SOFR", date(2026, 10, 9), date(2026, 11, 3))

    assert calls.count == 1
    assert list(frame["date"].dt.date) == [date(2026, 10, 9), date(2026, 10, 13), date(2026, 11, 2)]
    assert list(frame["value"]) == [4.01, 3.99, 3.95]
    # Two holiday markers out of five rows (40%) -> above the 25% share.
    skipped = [lvl for lvl, msg in logs if "skipped 2 missing" in msg]
    assert skipped == ["WARNING"]
    assert not any("coerced to NaN" in msg for _, msg in logs)


def test_repo_market_holiday_markers_log_at_debug(monkeypatch, logs):
    rows = [_obs(f"2026-10-{d:02d}", "4.0") for d in range(1, 31)] + [_obs("2026-10-31", ".")]
    _install_fake_fred(monkeypatch, _fedfred_frame(rows))

    frame = _repo_puller()._fetch_fred_series("SOFR", date(2026, 10, 1), date(2026, 10, 31))

    assert len(frame) == 30
    assert [lvl for lvl, msg in logs if "skipped 1 missing" in msg] == ["DEBUG"]


def test_repo_market_all_missing_fails_once_without_retry(monkeypatch, logs):
    calls = _install_fake_fred(monkeypatch, _fedfred_frame([_obs("2026-10-12", ".")] * 2))
    p = _repo_puller()

    with pytest.raises(ValueError, match="Invalid FRED observation date or value"):
        p._fetch_fred_series("SOFR", date(2026, 10, 12), date(2026, 10, 12))
    assert calls.count == 1, "deterministic parse failure must not be retried"
    assert not any("retrying" in msg for _, msg in logs)


def test_repo_market_pull_raw_series_stores_only_observed(monkeypatch):
    _install_fake_fred(monkeypatch, _fedfred_frame(SOFR_OBS))
    p = _repo_puller()
    p.engine = MagicMock()
    p._row_exists = lambda *_a: False
    stored: list[tuple[str, date, float]] = []
    p._insert_raw = _recording_insert(stored)

    result = p._pull_raw_series("sofr_rate", "SOFR", date(2026, 10, 9), date(2026, 11, 3))

    assert result == {"feature": "sofr_rate", "status": "SUCCESS", "rows_inserted": 3}
    assert [v for _, _, v in stored] == [4.01, 3.99, 3.95]
    assert _classify_outcome([result])[0] == "SUCCESS"


def test_repo_market_transport_error_still_retried(monkeypatch):
    calls = SimpleNamespace(count=0)

    class _Flaky:
        def __init__(self, _key):
            pass

        def get_series_observations(self, *_a, **_kw):
            calls.count += 1
            if calls.count == 1:
                raise ConnectionError("reset")
            return _fedfred_frame(SOFR_OBS)

    monkeypatch.setitem(sys.modules, "fedfred", SimpleNamespace(FredAPI=_Flaky))
    monkeypatch.setattr("time.sleep", lambda _s: None)

    frame = _repo_puller()._fetch_fred_series("SOFR", date(2026, 10, 9), date(2026, 11, 3))
    assert calls.count == 2 and len(frame) == 3


def _fed_puller() -> FedLiquidityPuller:
    p = FedLiquidityPuller.__new__(FedLiquidityPuller)
    p._api_key = "test"
    p.source_id = 1
    return p


def test_fed_liquidity_dot_marker_no_longer_fatal(monkeypatch):
    rrp = [_obs("2026-10-09", "4.1"), _obs("2026-10-12", "."), _obs("2026-10-13", "0.4")]
    _install_fake_fred(monkeypatch, _fedfred_frame(rrp))

    frame = _fed_puller()._fetch_fred_series("RRPONTSYD", date(2026, 10, 9), date(2026, 10, 13))

    assert list(frame["date"].dt.date) == [date(2026, 10, 9), date(2026, 10, 13)]
    assert list(frame["value"]) == [4.1, 0.4]


def test_fed_liquidity_incremental_window_is_per_series():
    p = _fed_puller()
    latest = {
        "RRPONTSYD": date(2026, 11, 3),     # daily
        "WSHOSHO": date(2026, 10, 29),      # weekly
        "SWPT": date(2026, 10, 29),         # weekly
        "H8B1023NCBCMG": date(2026, 9, 1),  # monthly
        "TOTRESNS": date(2026, 9, 1),       # monthly
    }
    p._get_latest_date = lambda sid: latest[sid]
    windows: dict[str, date] = {}

    def _raw(sid, start, end):
        windows[sid] = start
        return {"series": sid, "status": "SUCCESS", "rows_inserted": 0}

    p._pull_raw_series = _raw
    derived: list[date] = []
    p._compute_derived = lambda start, end: derived.append(start) or []
    with patch("ingestion.altdata.fed_liquidity.time.sleep"):
        out = p.pull_all(start_date="2013-01-01")

    assert set(windows) == set(FED_LIQUIDITY_SERIES)
    for sid, start in windows.items():
        # The latest stored observation is always inside its own window, so
        # FRED returns it and an off-cycle run is a clean 0-row check.
        assert start <= latest[sid], sid
    assert windows["H8B1023NCBCMG"] == date(2026, 9, 1) - timedelta(days=35)
    assert windows["RRPONTSYD"] == date(2026, 10, 27)
    assert derived == [date(2026, 10, 27)]
    assert _classify_outcome(out)[0] == "NO_NEW_DATA"


# ── 1b. FRED main puller: repair window always includes latest obs ───────


def test_fred_incremental_window_contains_latest_even_when_start_is_later():
    p = FREDPuller.__new__(FREDPuller)
    p._get_latest_date = lambda sid: date(2026, 9, 1)
    seen: list[str] = []
    p.pull_series = lambda sid, start, end: seen.append(start) or {
        "series_id": sid, "rows_inserted": 0, "rows_failed": 0,
        "status": "SUCCESS", "errors": [],
    }

    out = p.pull_all(["CPIAUCSL"], start_date="2026-10-13")  # hermes repair window

    assert seen == ["2026-08-25"]
    assert _classify_outcome(out)[0] == "NO_NEW_DATA"


def test_fred_cold_series_keeps_caller_start():
    p = FREDPuller.__new__(FREDPuller)
    p._get_latest_date = lambda sid: None
    seen: list[Any] = []
    p.pull_series = lambda sid, start, end: seen.append(start) or {
        "series_id": sid, "rows_inserted": 1, "rows_failed": 0,
        "status": "SUCCESS", "errors": [],
    }
    p.pull_all(["NEWSERIES"], start_date="2026-10-13")
    assert seen == ["2026-10-13"]


# ── 2. BLS "-" no-data marker, bad series id ─────────────────────────────


class _BLSEngine:
    def __init__(self):
        self.inserts: list[dict[str, Any]] = []

    @contextmanager
    def begin(self):
        yield self

    def execute(self, stmt, params=None):
        if "INSERT INTO raw_series" in str(stmt):
            self.inserts.append(params)
        return SimpleNamespace(fetchall=lambda: [], rowcount=1)


def _bls_response(payload: dict[str, Any]) -> requests.Response:
    out = requests.Response()
    out.status_code = 200
    out._content = json.dumps(payload).encode()
    return out


LNS_PAYLOAD = {
    "status": "REQUEST_SUCCEEDED",
    "message": ["Series does not exist for Series JTS00000000JOL"],
    "Results": {"series": [{"seriesID": "LNS14000000", "data": [
        {"year": "2026", "period": "M09", "periodName": "September", "value": "4.2",
         "footnotes": [{}]},
        {"year": "2025", "period": "M10", "periodName": "October", "value": "-",
         "footnotes": [{"code": "9",
                        "text": "Data unavailable due to the 2025 lapse in appropriations."}]},
    ]}]},
}


def _bls_puller() -> BLSPuller:
    p = BLSPuller.__new__(BLSPuller)
    p.engine = _BLSEngine()
    p.source_id = 3
    p.api_key = None
    p.query_count = 0
    p._max_queries = 25
    p._get_existing_dates = lambda sid, conn: set()
    return p


def test_bls_dash_is_a_quiet_no_data_marker(monkeypatch, logs):
    monkeypatch.setattr(bls_mod, "_REPORTED_ONCE", set())
    for run in range(2):
        p = _bls_puller()
        with patch.object(bls_mod.requests, "post", return_value=_bls_response(LNS_PAYLOAD)):
            result = p.pull_series(["LNS14000000"], start_year=2025, end_year=2026)
        assert result["status"] == "SUCCESS" and result["rows_inserted"] == 1
        assert [i["val"] for i in p.engine.inserts] == [4.2], run

    assert not any("Invalid BLS value" in msg for _, msg in logs)
    gap = [lvl for lvl, msg in logs if "2025/M10: no data published" in msg]
    assert gap == ["INFO", "DEBUG"], "first sighting INFO, repeats DEBUG"
    api_msgs = [lvl for lvl, msg in logs if "Series does not exist" in msg]
    assert api_msgs == ["WARNING", "DEBUG"]


def test_bls_garbage_value_still_warns(monkeypatch, logs):
    monkeypatch.setattr(bls_mod, "_REPORTED_ONCE", set())
    payload = json.loads(json.dumps(LNS_PAYLOAD))
    payload["message"] = []
    payload["Results"]["series"][0]["data"][1]["value"] = "n/a"
    with patch.object(bls_mod.requests, "post", return_value=_bls_response(payload)):
        _bls_puller().pull_series(["LNS14000000"], start_year=2025, end_year=2026)
    assert [lvl for lvl, msg in logs if "Invalid BLS value" in msg] == ["WARNING"]


def test_bls_request_failure_is_warning_not_error(logs):
    with patch.object(bls_mod.requests, "post", side_effect=requests.ConnectionError("down")):
        result = _bls_puller().pull_series(["LNS14000000"], start_year=2026, end_year=2026)
    assert result["status"] == "FAILED"
    assert not any(lvl == "ERROR" for lvl, _ in logs)


def test_bls_jolts_series_id_is_a_real_bls_id():
    assert "JTS00000000JOL" not in bls_mod.BLS_SERIES_LIST
    assert "JTS000000000000000JOL" in bls_mod.BLS_SERIES_LIST


# ── 3. NY Fed: SOMA, Treasury ops, Nowcast, repeat-blacklist logging ─────


def _soma_entry(day: str, **over: str) -> dict[str, str]:
    entry = {"asOfDate": day, "mbs": "1876203782906.70", "cmbs": "7318049754.50",
             "tips": "277655585900", "frn": "20045553800",
             "tipsInflationCompensation": "106611083624.33",
             "notesbonds": "3600742227800", "bills": "571527926700",
             "agencies": "2347000000", "total": "6355840126861.20"}
    entry.update(over)
    return entry


class _NYEngine:
    @contextmanager
    def begin(self):
        yield self


def _ny_puller(json_by_url: dict[str, Any] | None = None) -> tuple[NYFedPuller, list]:
    p = NYFedPuller.__new__(NYFedPuller)
    p.engine = _NYEngine()
    p.source_id = 274
    p._get_existing_dates = lambda sid, conn: {date(2026, 10, 21)}
    stored: list[tuple[str, date, float]] = []
    p._insert_raw = _recording_insert(stored)
    if json_by_url is not None:
        p._fetch_json = lambda url: json_by_url[url]
    return p, stored


def _soma_urls(summary: list[dict[str, str]], asof: str = "2026-10-28") -> dict[str, Any]:
    return {nyfed_mod._SOMA_ASOF_URL: {"soma": {"asOfDates": [asof]}},
            nyfed_mod._SOMA_SUMMARY_URL: {"soma": {"summary": summary}}}


def test_soma_current_shape_parsed_and_backfilled():
    summary = [_soma_entry("2003-07-09", mbs="0.00", cmbs="0.00", frn="",
                           total="5978155822000.20"),
               _soma_entry("2026-10-21"), _soma_entry("2026-10-28")]
    p, stored = _ny_puller(_soma_urls(summary))

    out = p._pull_soma()

    assert [r["status"] for r in out] == ["SUCCESS"] * 3
    # 2003 is outside the backfill window; 2026-10-21 already stored.
    assert [r["rows_inserted"] for r in out] == [1, 1, 1]
    vals = {sid: v for sid, d, v in stored}
    assert {d for _, d, _ in stored} == {date(2026, 10, 28)}
    assert vals["nyfed.soma_total_par_bn"] == pytest.approx(6355.840126861)
    assert vals["nyfed.soma_treasury_par_bn"] == pytest.approx(4469.9712942)
    assert vals["nyfed.soma_mbs_par_bn"] == pytest.approx(1876.2037829067)
    assert _classify_outcome(out)[0] == "SUCCESS"


@pytest.mark.parametrize("summary,why", [
    ([_soma_entry("2026-10-28", total="1")], "components do not sum"),
    ([_soma_entry("2026-10-21")], "lacks the latest as-of snapshot"),
    ([_soma_entry("2026-10-28", bills="abc")], "Invalid SOMA bills"),
    ([_soma_entry("2026-10-28", mbs="-5")], "Invalid SOMA mbs"),
])
def test_soma_current_shape_fails_closed(summary, why):
    p, stored = _ny_puller(_soma_urls(summary))
    out = p._pull_soma()
    assert stored == []
    assert all(r["status"] == "FAILED" and r["rows_inserted"] == 0 for r in out)
    assert why in out[0]["error"]


def test_treasury_ops_current_url_and_amount_key():
    assert nyfed_mod._TSY_OPS_URL.endswith("/tsy/all/results/summary/lastTwoWeeks.json")
    ops = {"treasury": {"auctions": [
        {"operationDate": "2026-11-03", "operationDirection": "S", "totalParAmtAccepted": "50000000"},
        {"operationDate": "2026-10-29", "operationDirection": "P", "totalParAmtAccepted": "1827000000"},
    ]}}
    p, stored = _ny_puller({nyfed_mod._TSY_OPS_URL: ops})
    p._get_existing_dates = lambda sid, conn: set()
    out = p._pull_treasury_ops()
    assert out[0]["status"] == "SUCCESS" and out[0]["rows_inserted"] == 2
    assert sorted(v for _, _, v in stored) == pytest.approx([0.05, 1.827])


def _nowcast_workbook(rows: list[list[Any]]) -> bytes:
    """Build the real workbook layout: notes sheet + banner rows + header."""
    banner = [[None] * 5 for _ in range(5)]
    banner[2][1] = "NEW YORK FED  ECONOMIC RESEARCH"
    header = [["Forecast date", "Reference quarter", "Backcast (previous quarter)",
               "Nowcast (current quarter)", "Forecast (next quarter)"]]
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xw:
        pd.DataFrame([[None, "notes"]]).to_excel(xw, sheet_name="New York Fed Nowcast",
                                                  header=False, index=False)
        pd.DataFrame(banner + header + rows).to_excel(
            xw, sheet_name="Forecasts By Horizon", header=False, index=False)
    return buf.getvalue()


def test_nowcast_horizon_sheet_parsed():
    xlsx = _nowcast_workbook([
        [pd.Timestamp("2022-12-02"), "2022:Q4", None, None, 1.79],
        [pd.Timestamp("2026-10-23"), "2026:Q3", None, 2.67, 2.81],
        [pd.Timestamp("2026-10-30"), "2026:Q3", None, 2.67, 2.9],
    ])
    p, stored = _ny_puller()
    p._get_existing_dates = lambda sid, conn: {date(2026, 10, 23)}
    p._fetch_bytes = lambda url: xlsx

    out = p._pull_nowcast()

    assert [(r["status"], r["rows_inserted"]) for r in out] == [("SUCCESS", 1), ("SUCCESS", 2)]
    assert ("nyfed.nowcast_gdp_q1", date(2026, 10, 30), 2.67) in stored
    assert ("nyfed.nowcast_gdp_q2", date(2026, 10, 30), 2.9) in stored
    assert ("nyfed.nowcast_gdp_q2", date(2022, 12, 2), 1.79) in stored
    assert not any(sid == "nyfed.nowcast_gdp_q1" and d == date(2022, 12, 2) for sid, d, _ in stored)


def test_nowcast_horizon_sheet_rejects_text_forecast():
    xlsx = _nowcast_workbook([[pd.Timestamp("2026-10-30"), "2026:Q3", None, "n/a", 2.9]])
    p, stored = _ny_puller()
    p._fetch_bytes = lambda url: xlsx
    out = p._pull_nowcast()
    assert stored == [] and all(r["status"] == "FAILED" for r in out)


def test_nowcast_url_is_current_download():
    assert nyfed_mod._NOWCAST_URL.endswith(
        "/NowCast/downloads/New-York-Fed-Staff-Nowcast_download_data.xlsx")


def test_repeat_blacklisted_endpoint_logs_debug(monkeypatch, logs):
    monkeypatch.setattr(nyfed_mod, "_DEAD_ENDPOINTS", {nyfed_mod._TSY_OPS_URL})
    p, _ = _ny_puller()
    p._session = MagicMock()
    out = p._pull_treasury_ops()
    assert out[0]["status"] == "SKIPPED"
    assert [lvl for lvl, msg in logs if "Treasury ops skipped" in msg] == ["DEBUG"]
    p._session.get.assert_not_called()


def test_first_permanent_failure_still_warns(monkeypatch, logs):
    monkeypatch.setattr(nyfed_mod, "_DEAD_ENDPOINTS", set())
    p, _ = _ny_puller()
    p._session = MagicMock()
    p._session.get.return_value = SimpleNamespace(status_code=400)
    p._pull_treasury_ops()
    assert [lvl for lvl, msg in logs if "Treasury ops skipped" in msg] == ["WARNING"]
    assert isinstance(NYFedPermanentError("x"), RuntimeError)


# ── 4. Stale-source pullers ───────────────────────────────────────────────


def test_treasury_auction_current_feed_shape():
    today = date.today()
    row = {
        "cusip": "91282CRV5", "type": "Note", "securityTerm": "7-Year",
        "competitiveAuctionCloseDate": f"{today.isoformat()}T00:00:00",
        "bidToCoverRatio": "2.46", "highYield": "4.894", "highRate": "",
        "indirectBiddersCompetitiveParAwarded": "25211878000",
        "directBiddersCompetitiveParAwarded": "14321000000",
        "primaryDealersCompetitiveParAwarded": "4107456000",
    }
    bill = dict(row, cusip="912797WF0", type="Bill", securityTerm="8-Week",
                highYield="", highRate="4.050")
    p = TreasuryAuctionPuller.__new__(TreasuryAuctionPuller)
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = [row, bill]
    with patch("ingestion.altdata.treasury_auction.requests.get", return_value=resp):
        auctions = p._fetch_auctions()
    assert len(auctions) == 2

    metrics = {r.metric: r for r in p._parse_auction(row)}
    assert metrics["stop_yield"].value == 4.894
    assert metrics["stop_yield"].obs_date == today and metrics["stop_yield"].term == "7-Year"
    assert metrics["indirect_pct"].value == pytest.approx(57.771963890)
    assert sum(metrics[m].value for m in ("indirect_pct", "direct_pct", "primary_dealer_pct")) \
        == pytest.approx(100.0)
    assert {r.metric: r.value for r in p._parse_auction(bill)}["stop_yield"] == 4.05


def test_h8_fetch_failure_never_logs_api_key(logs):
    p = H8BankBalancePuller.__new__(H8BankBalancePuller)
    p.api_key = "SECRETKEY123"
    err = requests.HTTPError(
        "400 Client Error: Bad Request for url: https://api.stlouisfed.org/fred/series/"
        "observations?series_id=TOTCI&api_key=SECRETKEY123&file_type=json")
    with patch("ingestion.altdata.h8_bank_balance.requests.get", side_effect=err):
        assert p._fetch_series("TOTCI") == []
    joined = " ".join(msg for _, msg in logs)
    # The module's own api_key=REDACTED, or the central log patcher's
    # "?[REDACTED]" once config is imported: either way the key is gone.
    assert "SECRETKEY123" not in joined and "REDACTED" in joined


def test_gscpi_text_dates_parse_without_inference_warning():
    col = pd.Series(["Date", None, "31-Jan-1998", "30-Sep-2026", pd.Timestamp("2026-10-31")])
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        parsed = col.map(_parse_gscpi_date)
    assert pd.isna(parsed[0]) and pd.isna(parsed[1])
    assert parsed[2] == pd.Timestamp("1998-01-31")
    assert parsed[3] == pd.Timestamp("2026-09-30")
    assert parsed[4] == pd.Timestamp("2026-10-31")
