"""Honest pull_log outcomes for configuration-limited sources (2026-11-09).

Evidence from grid-svr pull_log, 2026-11-03..09:
  * CryptoQuant: "PullerReportedFailure: auth_failed" every run. The key is
    set and accepted, but every metric answers 403 "This data (window: day)
    is only available on the Professional plan and above" -> SKIPPED.
  * OpenSecrets: "PullerReportedFailure: no_api_key" -> SKIPPED.
  * WorldNews: FAILED/PARTIAL on HTTP 402 (50-point daily quota) -> SKIPPED
    when the run's first request is refused, PARTIAL only after a pulled day.
  * Tiingo news: "7 of 51 items failed or partial" -- quiet ETFs with no
    articles in 7 days (NO_DATA, not PARTIAL), and BTC-USD / ETH-USD /
    DX-Y.NYB, which Tiingo's news index tags as btc / eth / nothing.
  * SEC XBRL shares: "2 of 1268 items failed: facts_unavailable" -- SEC 404
    for issuers that never filed XBRL (BYDDF, RBGLY) -> NO_NEW_DATA.

No network: every HTTP call is mocked.
"""
from __future__ import annotations

from datetime import date
import json
from unittest.mock import MagicMock

import pytest
import requests

from ingestion.smart_scheduler import (
    OUTCOME_FAILED,
    OUTCOME_NO_NEW_DATA,
    OUTCOME_PARTIAL,
    OUTCOME_SKIPPED,
    OUTCOME_SUCCESS,
    _classify_outcome,
)


class _Resp:
    def __init__(self, status: int, body=None, text: str = "") -> None:
        self.status_code = status
        self._body = body
        self.content = json.dumps(body).encode("utf-8") if body is not None else b""
        self.text = text

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            err = requests.HTTPError(f"HTTP {self.status_code}")
            err.response = self  # type: ignore[assignment]
            raise err


# ── CryptoQuant ──────────────────────────────────────────────────────────

_CQ_403 = {"result": {}, "status": {
    "code": 403, "message": "forbidden",
    "description": "403 Forbidden: This data (window: day) is only available on the "
                   "Professional plan and above.",
}}
_CQ_401 = {"result": {}, "status": {
    "code": 401, "message": "unauthorized",
    "description": "401 Unauthorized: Token does not exists.",
}}
_CQ_OK = {"result": {"data": [{"date": "2026-11-07", "value": 1.5}]}}


@pytest.fixture
def cq(monkeypatch):
    from ingestion.altdata import cryptoquant_puller as mod

    monkeypatch.setattr(mod.time, "sleep", lambda *_: None)
    monkeypatch.setattr("ingestion.base.time.sleep", lambda *_: None)
    obj = mod.CryptoQuantPuller.__new__(mod.CryptoQuantPuller)
    obj.api_key = "test-key"
    obj.engine = MagicMock()
    obj.source_id = 1
    obj.inserted = []
    monkeypatch.setattr(obj, "_get_existing_dates", lambda *_a, **_k: set(), raising=False)
    monkeypatch.setattr(obj, "_insert_raw", lambda conn, **kw: obj.inserted.append(kw), raising=False)
    return mod, obj


def _cq_http(monkeypatch, mod, responses):
    calls: list[str] = []

    def fake_get(url, **_kw):
        resp = responses[min(len(calls), len(responses) - 1)]
        calls.append(url)
        if isinstance(resp, Exception):
            raise resp
        return resp

    monkeypatch.setattr(mod.requests, "get", fake_get)
    return calls


@pytest.mark.unit
class TestCryptoQuant:
    def test_no_key_is_skipped(self, cq):
        _, obj = cq
        obj.api_key = ""
        out = obj.pull()
        assert _classify_outcome(out) == (OUTCOME_SKIPPED, 0, "CRYPTOQUANT_API_KEY not configured")

    def test_plan_403_is_skipped_with_reason_and_one_request(self, cq, monkeypatch):
        mod, obj = cq
        calls = _cq_http(monkeypatch, mod, [_Resp(403, _CQ_403)])
        outcome, rows, note = _classify_outcome(obj.pull())
        assert (outcome, rows) == (OUTCOME_SKIPPED, 0)
        assert "plan not entitled (HTTP 403)" in note and "Professional plan" in note
        assert len(calls) == 1  # terminal: no request after the first 403

    def test_rejected_401_is_skipped_renew(self, cq, monkeypatch):
        mod, obj = cq
        _cq_http(monkeypatch, mod, [_Resp(401, _CQ_401)])
        outcome, rows, note = _classify_outcome(obj.pull())
        assert outcome == OUTCOME_SKIPPED and rows == 0
        assert "CRYPTOQUANT_API_KEY rejected (HTTP 401)" in note and "renew" in note
        assert "test-key" not in note

    def test_403_after_committed_rows_is_partial(self, cq, monkeypatch):
        mod, obj = cq
        _cq_http(monkeypatch, mod, [_Resp(200, _CQ_OK), _Resp(403, _CQ_403)])
        outcome, rows, _ = _classify_outcome(obj.pull())
        assert (outcome, rows) == (OUTCOME_PARTIAL, 1)

    def test_server_errors_stay_failed(self, cq, monkeypatch):
        mod, obj = cq
        _cq_http(monkeypatch, mod, [_Resp(503, None)])
        out = obj.pull()
        assert out["rows_inserted"] == 0
        assert _classify_outcome(out)[0] == OUTCOME_FAILED

    def test_network_errors_stay_failed(self, cq, monkeypatch):
        mod, obj = cq
        _cq_http(monkeypatch, mod, [requests.ConnectionError("down")])
        assert _classify_outcome(obj.pull())[0] == OUTCOME_FAILED

    def test_full_success_reports_committed_rows(self, cq, monkeypatch):
        mod, obj = cq
        _cq_http(monkeypatch, mod, [_Resp(200, _CQ_OK)])
        out = obj.pull()
        n = len(mod.BTC_METRICS + mod.ETH_METRICS + mod.STABLE_METRICS)
        assert out["rows_inserted"] == n == len(obj.inserted)
        assert _classify_outcome(out) == (OUTCOME_SUCCESS, n, None)


# ── OpenSecrets ──────────────────────────────────────────────────────────

@pytest.mark.unit
def test_opensecrets_no_key_is_skipped():
    from ingestion.altdata.opensecrets_puller import OpenSecretsPuller

    obj = OpenSecretsPuller.__new__(OpenSecretsPuller)
    obj.api_key = ""
    assert _classify_outcome(obj.pull()) == (
        OUTCOME_SKIPPED, 0, "OPENSECRETS_API_KEY not configured",
    )


# ── WorldNews ────────────────────────────────────────────────────────────

@pytest.fixture
def wn(monkeypatch):
    from ingestion.altdata import world_news as mod

    monkeypatch.setattr(mod, "_payment_required_until", 0.0)
    monkeypatch.setattr(mod.time, "sleep", lambda *_: None)
    obj = mod.WorldNewsPuller.__new__(mod.WorldNewsPuller)
    obj.api_key = "test-key"
    obj._api_expired = False
    obj.source_id = 1
    obj.engine = MagicMock()
    monkeypatch.setattr(obj, "_ensure_features", lambda: None)
    monkeypatch.setattr(obj, "_row_exists", lambda *a, **k: False)
    monkeypatch.setattr(obj, "_insert_raw", lambda **kw: None)
    monkeypatch.setattr(obj, "_captured_days", lambda *_a: set())
    return mod, obj


def _wn_http(monkeypatch, mod, statuses):
    calls: list[int] = []

    def fake_get(*_a, **_k):
        status = statuses[min(len(calls), len(statuses) - 1)]
        calls.append(status)
        return _Resp(status, {"news": [{"sentiment": 0.1, "source_country": "us"}]})

    monkeypatch.setattr(mod.requests, "get", fake_get)
    return calls


@pytest.mark.unit
class TestWorldNews402:
    def test_first_request_402_is_skipped_quota(self, wn, monkeypatch):
        mod, obj = wn
        _wn_http(monkeypatch, mod, [402])
        outcome, rows, note = _classify_outcome(obj.pull_all(days_back=2))
        assert (outcome, rows) == (OUTCOME_SKIPPED, 0)
        assert "402" in note and "quota" in note

    def test_402_after_captured_days_only_is_skipped(self, wn, monkeypatch):
        # The 2026-11-06 20:31Z run: day 1 already captured, day 2 -> 402.
        # It fetched nothing new because of the quota: SKIPPED, not PARTIAL.
        mod, obj = wn
        yesterday = date.fromordinal(date.today().toordinal() - 1)  # noqa: DTZ011 - the puller uses local date.today()
        before = date.fromordinal(yesterday.toordinal() - 1)
        monkeypatch.setattr(obj, "_captured_days", lambda *_a: {before})
        _wn_http(monkeypatch, mod, [402])
        out = obj.pull_all(days_back=2)
        outcome, rows, note = _classify_outcome(out)
        assert (outcome, rows) == (OUTCOME_SKIPPED, 0)
        assert "1 earlier day(s) already captured" in note

    def test_402_after_a_pulled_day_is_partial_naming_the_quota(self, wn, monkeypatch):
        mod, obj = wn
        _wn_http(monkeypatch, mod, [200] * 10 + [402])
        outcome, rows, note = _classify_outcome(obj.pull_all(days_back=2))
        assert (outcome, rows) == (OUTCOME_PARTIAL, 33)
        assert "402 Payment Required" in note

    def test_breaker_hold_is_skipped(self, wn, monkeypatch):
        mod, obj = wn
        calls = _wn_http(monkeypatch, mod, [402])
        obj.pull_all(days_back=1)
        fresh = mod.WorldNewsPuller.__new__(mod.WorldNewsPuller)
        fresh.api_key, fresh._api_expired = "test-key", False
        assert _classify_outcome(fresh.pull_all(days_back=7))[0] == OUTCOME_SKIPPED
        assert calls == [402]

    def test_server_error_is_not_skipped(self, wn, monkeypatch):
        mod, obj = wn
        monkeypatch.setattr("ingestion.base.time.sleep", lambda *_: None)
        _wn_http(monkeypatch, mod, [500])
        assert _classify_outcome(obj.pull_all(days_back=1))[0] != OUTCOME_SKIPPED


# ── Tiingo news ──────────────────────────────────────────────────────────

@pytest.fixture
def tn(monkeypatch):
    from ingestion import tiingo_news_pull as mod

    monkeypatch.setattr(mod.time, "sleep", lambda *_: None)
    monkeypatch.setattr(mod, "_bandwidth_paused_until", lambda: None)
    obj = mod.TiingoNewsPuller.__new__(mod.TiingoNewsPuller)
    obj.source_id = 7
    engine = MagicMock()
    conn = engine.begin.return_value.__enter__.return_value
    conn.execute.return_value.rowcount = 1
    obj.engine = engine
    return mod, obj, conn


def _tn_http(monkeypatch, by_symbol):
    import requests as rq

    seen: list[str] = []

    def fake_get(url, headers=None, params=None, timeout=None):
        sym = params["tickers"]
        seen.append(sym)
        resp = by_symbol.get(sym, [])
        if isinstance(resp, _Resp):
            return resp
        return _Resp(200, resp)

    monkeypatch.setattr(rq, "get", fake_get)
    return seen


_ARTICLE = {"title": "t", "url": "https://x/1", "publishedDate": "2026-11-07T10:00:00Z"}


@pytest.mark.unit
class TestTiingoNews:
    def test_default_tickers_drop_untaggable_symbols(self, tn):
        mod, _, _ = tn
        tickers = mod._default_news_tickers()
        assert "DX-Y.NYB" not in tickers
        assert "BTC-USD" in tickers and "ETH-USD" in tickers
        assert not any(t.startswith("^") or "=" in t for t in tickers)

    def test_crypto_queries_tiingo_tag_but_keeps_grid_series_id(self, tn, monkeypatch):
        _, obj, conn = tn
        seen = _tn_http(monkeypatch, {"btc": [_ARTICLE]})
        out = obj.pull_ticker_news("BTC-USD")
        assert seen == ["btc"]
        assert out["status"] == "SUCCESS" and out["rows_inserted"] == 2
        sids = [c.args[1]["sid"] for c in conn.execute.call_args_list]
        assert all(s.startswith("TIINGO_NEWS:BTC-USD:") for s in sids)

    def test_no_articles_is_no_new_data_not_partial(self, tn, monkeypatch):
        _, obj, _ = tn
        _tn_http(monkeypatch, {})
        out = obj.pull_ticker_news("XLI")
        assert out["status"] == "NO_DATA"
        assert _classify_outcome(out)[0] == OUTCOME_NO_NEW_DATA

    def test_run_with_quiet_tickers_is_success(self, tn, monkeypatch):
        _, obj, _ = tn
        _tn_http(monkeypatch, {"SPY": [_ARTICLE]})
        out = obj.pull_all(["SPY", "XLI", "SLV", "BKLN"])
        assert _classify_outcome(out) == (OUTCOME_SUCCESS, 2, None)

    def test_all_quiet_run_is_no_new_data(self, tn, monkeypatch):
        _, obj, _ = tn
        _tn_http(monkeypatch, {})
        assert _classify_outcome(obj.pull_all(["XLI", "SLV"]))[0] == OUTCOME_NO_NEW_DATA

    def test_http_error_still_fails_the_ticker(self, tn, monkeypatch):
        _, obj, _ = tn
        _tn_http(monkeypatch, {"SPY": [_ARTICLE], "QQQ": _Resp(500, None)})
        outcome, rows, _ = _classify_outcome(obj.pull_all(["SPY", "QQQ"]))
        assert (outcome, rows) == (OUTCOME_PARTIAL, 2)

    def test_untaggable_ticker_is_not_requested(self, tn, monkeypatch):
        _, obj, _ = tn
        seen = _tn_http(monkeypatch, {})
        out = obj.pull_ticker_news("DX-Y.NYB")
        assert seen == [] and out["status"] == "SKIPPED"


# ── SEC XBRL shares ──────────────────────────────────────────────────────

@pytest.fixture
def sec(monkeypatch):
    import ingestion.altdata.sec_xbrl_financials as fin
    import ingestion.altdata.sec_xbrl_shares as mod

    monkeypatch.setattr(fin, "_COMPANY_FACTS_NOT_FOUND", set())
    monkeypatch.setattr(mod.time, "monotonic", lambda: 100)
    monkeypatch.setattr(mod.time, "sleep", lambda _: None)
    monkeypatch.setattr(mod, "_fetch_ticker_to_cik_map",
                        lambda: {"BYDDF": "0001445162", "AAPL": "0000320193"})
    monkeypatch.setattr(mod, "_extract_shares_entries", lambda _: [(date(2026, 1, 1), 10)])
    monkeypatch.setattr(mod, "_fetch_close_prices", lambda *_: [(date(2026, 9, 1), 2)])
    monkeypatch.setattr(mod, "_write_rows", lambda *_a, **_k: 1)
    return fin, mod


def _sec_http(monkeypatch, fin, by_cik):
    def fake_get(url, headers=None, timeout=None):
        for cik, status in by_cik.items():
            if cik in url:
                return _Resp(status, {"facts": {}} if status == 200 else None)
        raise AssertionError(url)

    monkeypatch.setattr(fin.requests, "get", fake_get)


@pytest.mark.unit
class TestSecCompanyFacts404:
    def test_404_is_no_xbrl_facts_not_a_failure(self, sec, monkeypatch):
        fin, mod = sec
        _sec_http(monkeypatch, fin, {"0001445162": 404, "0000320193": 200})
        out = mod.SECXBRLSharesPuller(object()).pull_all(tickers=["BYDDF", "AAPL"])
        by = {r.get("ticker"): r for r in out}
        assert by["BYDDF"]["status"] == "NO_NEW_DATA"
        assert by["BYDDF"]["reason"] == "no_xbrl_facts"
        assert out[-1]["failed"] == 0 and out[-1]["no_data"] == 1
        assert _classify_outcome(out)[:2] == (OUTCOME_SUCCESS, 1)

    @pytest.mark.parametrize("status", [403, 500, 503])
    def test_other_http_failures_still_fail(self, sec, monkeypatch, status):
        fin, mod = sec
        _sec_http(monkeypatch, fin, {"0001445162": status})
        out = mod.SECXBRLSharesPuller(object()).pull_all(tickers=["BYDDF"])
        assert out[0]["status"] == "FAILED" and out[0]["error"] == "facts_unavailable"
        assert _classify_outcome(out)[0] == OUTCOME_FAILED

    def test_a_later_200_clears_the_404_mark(self, sec, monkeypatch):
        fin, _ = sec
        _sec_http(monkeypatch, fin, {"0001445162": 404})
        assert fin._fetch_company_facts("0001445162") is None
        assert fin.company_facts_not_found("0001445162")
        _sec_http(monkeypatch, fin, {"0001445162": 200})
        assert fin._fetch_company_facts("0001445162") == {"facts": {}}
        assert not fin.company_facts_not_found("0001445162")


# ── Health markers ───────────────────────────────────────────────────────

@pytest.mark.unit
def test_owner_side_key_limits_count_as_not_configured():
    from scripts.hermes_health import is_not_configured_reason

    assert is_not_configured_reason(
        "SKIPPED: CRYPTOQUANT_API_KEY plan not entitled (HTTP 403) on btc/x -- upgrade")
    assert is_not_configured_reason("SKIPPED: CRYPTOQUANT_API_KEY rejected (HTTP 401) -- renew")
    assert is_not_configured_reason(
        "SKIPPED: all 1 items skipped: WorldNewsAPI 402 Payment Required (plan expired ...)")
    assert not is_not_configured_reason("CryptoQuant 3 of 25 metrics failed")
