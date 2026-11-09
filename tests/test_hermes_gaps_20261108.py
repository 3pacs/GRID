"""Four Hermes failed-source gaps found in the 2026-11-08 journal audit.

* TIINGO_FUNDAMENTALS: 89 of 138 default tickers answer 400 "Free and Power
  plans are limited to the DOW 30"; that was reported as a failed pull every
  run because the paywall check looked at str(HTTPError), which has no body.
* OppInsights: "Employment - National - Daily.csv" is gone upstream (404);
  the weekly file uses ``emp`` and ``day_endofweek``. The 1-hour
  ``_row_exists`` window re-inserted the frozen spend history every pull.
* SEC_EDGAR: 13F re-parsed every stored filing (4 min 38 s vs a 180 s
  timeout) and re-inserted ~830 rows a run.
* sec_xbrl_shares: limit=400 always took the first 400 of 1,268 tickers; a
  ticker with no SEC CIK counted as a failed ticker.

No network, no database.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest
import requests

from ingestion.smart_scheduler import PULLER_REGISTRY, _classify_outcome


# ── Tiingo fundamentals ─────────────────────────────────────────────────

PLAN_BODY = (b'{"detail":"Error: Free and Power plans are limited to the DOW 30. '
             b'If you would like access to all supported tickers, upgrade."}')


def _response(status: int, body: bytes) -> requests.Response:
    r = requests.Response()
    r.status_code = status
    r._content = body
    r.url = "https://api.tiingo.com/tiingo/fundamentals/X/daily"
    return r


class _TiingoEngine:
    """Answers the default-universe query; inserts report 0 new rows."""

    def __init__(self, universe: list[str]):
        self.universe = universe

    @contextmanager
    def connect(self):
        yield self

    @contextmanager
    def begin(self):
        yield self

    def execute(self, sql, params=None):
        if "feature_registry" in str(sql):
            return SimpleNamespace(fetchall=lambda: [(t,) for t in self.universe])
        return SimpleNamespace(rowcount=0, fetchall=lambda: [])


@pytest.fixture
def tiingo(monkeypatch):
    from ingestion import tiingo_fundamentals_pull as tf

    in_plan = {"AAPL", "KO"}

    def fake_get(url, params=None, headers=None, timeout=None):
        ticker = url.rsplit("/", 2)[-2]
        if ticker in in_plan:
            r = _response(200, b"[]")
        elif ticker == "WEIRD":
            r = _response(400, b'{"detail":"Error: startDate is malformed"}')
        else:
            r = _response(400, PLAN_BODY)
        return r

    monkeypatch.setattr(tf, "_TIINGO_API_KEY", "SYNTHETIC_TEST_KEY")
    monkeypatch.setattr(tf, "_RATE_LIMIT_DELAY", 0)
    monkeypatch.setattr(tf.requests, "get", fake_get)

    def make(universe):
        p = tf.TiingoFundamentalsPuller.__new__(tf.TiingoFundamentalsPuller)
        p.engine = _TiingoEngine(universe)
        p.source_id = 565
        p._get_latest_date = lambda series_id: date(2026, 11, 1)
        return p

    return make


def test_plan_limited_400_is_paywalled_not_failed(tiingo):
    res = tiingo([]).pull_ticker("NVDA", date(2026, 11, 1), date(2026, 11, 7))
    assert res["status"] == "PAYWALLED"
    assert "limited to the DOW 30" not in str(res)  # body never persisted


def test_other_400_still_fails(tiingo):
    res = tiingo([]).pull_ticker("WEIRD", date(2026, 11, 1), date(2026, 11, 7))
    assert res["status"] == "FAILED"
    assert res["errors"] == ["Tiingo HTTP request failed (status 400)"]


def test_default_universe_drops_out_of_plan_tickers(tiingo):
    p = tiingo(["AAPL", "NVDA", "KO", "SPY"])
    out = p.pull_all(should_continue=lambda: True)
    assert [r["ticker"] for r in out["results"]] == ["AAPL", "KO"]
    assert out["tickers_not_attempted"] == []
    assert _classify_outcome(out)[:2] == ("NO_NEW_DATA", 0)


def test_default_universe_keeps_real_failures(tiingo):
    p = tiingo(["AAPL", "NVDA", "WEIRD"])
    out = p.pull_all(should_continue=lambda: True)
    assert [r["ticker"] for r in out["results"]] == ["AAPL", "WEIRD"]
    assert _classify_outcome(out)[0] == "PARTIAL"


def test_explicit_ticker_list_keeps_paywalled_result(tiingo):
    out = tiingo([]).pull_all(["NVDA"])
    assert [r["status"] for r in out] == ["PAYWALLED"]


# ── Opportunity Insights ────────────────────────────────────────────────


class _RowsEngine:
    """raw_series stand-in: DISTINCT ON reads and INSERTs on one dict."""

    def __init__(self, stored: dict[str, dict[date, float]]):
        self.stored = stored
        self.inserts: list[dict] = []

    @contextmanager
    def begin(self):
        yield self

    connect = begin

    def execute(self, sql, params=None):
        sql = str(sql)
        if sql.startswith("SELECT DISTINCT ON"):
            rows = sorted(self.stored.get(params["sid"], {}).items())
            return SimpleNamespace(fetchall=lambda: rows)
        if sql.startswith("INSERT"):
            self.inserts.append(params)
            self.stored.setdefault(params["sid"], {})[params["od"]] = params["val"]
            return SimpleNamespace(rowcount=1)
        raise AssertionError(f"unexpected SQL: {sql[:60]}")


def _oi(engine):
    from ingestion.altdata.opportunity import OppInsightsPuller

    p = OppInsightsPuller.__new__(OppInsightsPuller)
    p.engine = engine
    p.source_id = 347
    return p


def test_employment_points_at_the_weekly_file():
    from ingestion.altdata.opportunity import OI_FILES

    emp = next(f for f in OI_FILES if f["feature"] == "oi_employment_overall")
    assert emp["filename"] == "Employment - National - Weekly.csv"
    assert (emp["value_col"], emp["day_col"]) == ("emp", "day_endofweek")


def test_weekly_employment_dates_use_day_endofweek():
    p = _oi(_RowsEngine({}))
    df = pd.DataFrame({"year": [2026, 2026], "month": [9, 9],
                       "day_endofweek": [12, 19], "emp": [0.0156, 0.0201]})
    p._download_csv = MagicMock(return_value=df)
    res = p.pull_file("Employment - National - Weekly.csv", "date", "emp",
                      "oi_employment_overall", "day_endofweek")
    assert res["status"] == "SUCCESS" and res["rows_inserted"] == 2
    assert [i["od"] for i in p.engine.inserts] == [date(2026, 9, 12), date(2026, 9, 19)]


def test_unchanged_history_is_not_reinserted_and_revisions_are():
    stored = {"oi_consumer_spend": {date(2024, 6, 9): 0.18, date(2024, 6, 16): 0.184}}
    p = _oi(_RowsEngine(stored))
    df = pd.DataFrame({"year": [2024, 2024], "month": [6, 6], "day": [9, 16],
                       "spend_all": [0.18, 0.19]})
    p._download_csv = MagicMock(return_value=df)
    res = p.pull_file("Affinity - National - Daily.csv", "date", "spend_all",
                      "oi_consumer_spend")
    assert res["rows_inserted"] == 1
    assert [(i["od"], i["val"]) for i in p.engine.inserts] == [(date(2024, 6, 16), 0.19)]


def test_k_shape_is_not_reinserted():
    d = date(2024, 6, 16)
    stored = {"oi_spend_high_income": {d: 0.3}, "oi_spend_low_income": {d: 0.1}}
    p = _oi(_RowsEngine(stored))
    assert p.compute_k_shape()["rows_inserted"] == 1
    assert p.compute_k_shape()["rows_inserted"] == 0


# ── EDGAR 13F ───────────────────────────────────────────────────────────


def test_13f_skips_stored_filings_before_download(monkeypatch):
    edgar_mod = pytest.importorskip("ingestion.edgar")
    monkeypatch.setattr(edgar_mod.time, "sleep", lambda *_a, **_k: None)

    stored_filing = MagicMock(filing_date=date(2026, 8, 14))
    new_filing = MagicMock(filing_date=date(2026, 11, 7))
    new_filing.obj.return_value.infotable = pd.DataFrame({"Value": [5.0, 3.0]})
    company = MagicMock()
    company.return_value.get_filings.return_value = [new_filing, stored_filing]
    monkeypatch.setattr(edgar_mod, "Company", company)

    p = edgar_mod.EDGARPuller.__new__(edgar_mod.EDGARPuller)
    p.engine = MagicMock()
    p.source_id = 61
    p._row_exists = lambda *_a, **_k: False
    p._stored_13f_dates = lambda cik: {date(2026, 8, 14)}

    out = p.pull_13f_holdings(cik_list=["0001067983"])
    stored_filing.obj.assert_not_called()
    new_filing.obj.assert_called_once()
    assert out["rows_inserted"] == 3


# ── sec_xbrl_shares ─────────────────────────────────────────────────────


def test_ticker_without_cik_is_no_data_not_failure(monkeypatch):
    import ingestion.altdata.sec_xbrl_shares as mod

    monkeypatch.setattr(mod.time, "monotonic", lambda: 100)
    monkeypatch.setattr(mod.time, "sleep", lambda _: None)
    monkeypatch.setattr(mod, "_fetch_ticker_to_cik_map", lambda: {"TSM": "1"})
    monkeypatch.setattr(mod, "_fetch_company_facts", lambda _: {"facts": {}})
    out = mod.SECXBRLSharesPuller(object()).pull_all(tickers=["KRE", "TSM"])
    assert [(r.get("ticker"), r.get("reason")) for r in out[:2]] == [
        ("KRE", "no_cik"), ("TSM", "no_shares")]
    assert _classify_outcome(out)[:2] == ("NO_NEW_DATA", 0)


def _entry(name: str) -> dict:
    return next(e for e in PULLER_REGISTRY if e["name"] == name)


def test_registry_budgets():
    assert "limit" not in _entry("sec_xbrl_shares").get("kwargs", {})
    assert _entry("edgar")["timeout_s"] >= 300
    assert _entry("opportunity")["timeout_s"] >= 120
