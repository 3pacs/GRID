"""Write-count contract for the pullers stale on 2026-11-04 (workstream B).

Every puller here was in the "35 sources past freshness window" list because
its return value failed ``smart_scheduler._classify_outcome``'s fail-closed
write-count contract (no committed count, an unknown status, FAILED for an
unconfigured optional source, or PARTIAL coverage that could never be
complete). Each test drives the real puller method with network and DB
mocked, then classifies its return value exactly as the schedulers do.
"""

from __future__ import annotations

import io
import os
from datetime import date, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("FRED_API_KEY", "test")

from ingestion.smart_scheduler import _classify_outcome

SECRET = "sekret-key-0123456789"


# ── fakes ───────────────────────────────────────────────────────────────


class FakeResult:
    def __init__(self, rowcount: int = 1, rows: list | None = None, one: Any = None) -> None:
        self.rowcount = rowcount
        self._rows = rows or []
        self._one = one

    def fetchall(self) -> list:
        return self._rows

    def fetchone(self) -> Any:
        return self._one


class FakeConn:
    """Records statements; INSERT/UPSERT rowcount 1, SELECTs return nothing."""

    def __init__(self, rowcount: int = 1) -> None:
        self.statements: list[tuple[str, dict]] = []
        self.rowcount = rowcount

    def execute(self, stmt: Any, params: dict | None = None) -> FakeResult:
        sql = str(stmt)
        self.statements.append((sql, dict(params or {})))
        if sql.lstrip().upper().startswith("SELECT"):
            return FakeResult(rowcount=0, rows=[], one=None)
        return FakeResult(rowcount=self.rowcount)

    def begin_nested(self) -> MagicMock:
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=self)
        cm.__exit__ = MagicMock(return_value=False)
        return cm

    def inserts(self, table: str = "raw_series") -> list[dict]:
        return [p for sql, p in self.statements if f"INSERT INTO {table}" in sql]


class FakeEngine:
    def __init__(self, conn: FakeConn | None = None) -> None:
        self.conn = conn or FakeConn()

    def _cm(self) -> MagicMock:
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=self.conn)
        cm.__exit__ = MagicMock(return_value=False)
        return cm

    def begin(self) -> MagicMock:
        return self._cm()

    def connect(self) -> MagicMock:
        return self._cm()


def _bare(cls: type, engine: FakeEngine | None = None, **attrs: Any) -> Any:
    obj = cls.__new__(cls)
    obj.engine = engine or FakeEngine()
    obj.source_id = 7
    for k, v in attrs.items():
        setattr(obj, k, v)
    return obj


class Resp:
    def __init__(self, status: int = 200, json_data: Any = None, text: str = "",
                 headers: dict | None = None, content: bytes = b"") -> None:
        self.status_code = status
        self._json = json_data
        self.text = text
        self.headers = headers or {}
        self.content = content

    def json(self) -> Any:
        if self._json is None:
            raise ValueError("no json")
        return self._json

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            import requests

            raise requests.exceptions.HTTPError(f"HTTP {self.status_code}")


# ── Polygon ─────────────────────────────────────────────────────────────


class TestPolygon:
    def _puller(self, **kw: Any) -> Any:
        from ingestion.altdata.polygon_puller import PolygonPuller

        return _bare(PolygonPuller, **{"api_key": SECRET, **kw})

    def test_not_entitled_is_skipped_after_one_call_without_the_30_ticker_loop(self) -> None:
        import ingestion.altdata.polygon_puller as mod

        calls: list[dict] = []

        def fake_get(url: str, params: Any = None, headers: Any = None, timeout: Any = None) -> Resp:
            calls.append({"url": url, "params": params, "headers": headers})
            return Resp(403, {"status": "NOT_AUTHORIZED", "message": "upgrade"})

        sleeps: list[float] = []
        with patch.object(mod.requests, "get", side_effect=fake_get), \
             patch.object(mod.time, "sleep", side_effect=sleeps.append):
            out = self._puller().pull()
        assert out["status"] == "SKIPPED"
        assert "NOT_AUTHORIZED" in out["skipped_reason"]
        assert len(calls) == 1 and sleeps == []
        assert _classify_outcome(out)[0] == "SKIPPED"
        # Key travels in the header only, never in the URL/query string.
        assert SECRET not in calls[0]["url"] and SECRET not in str(calls[0]["params"])
        assert calls[0]["headers"]["Authorization"] == f"Bearer {SECRET}"

    def test_success_reports_committed_rows(self) -> None:
        import ingestion.altdata.polygon_puller as mod

        chain = [
            {"details": {"contract_type": "call"}, "open_interest": 100, "day": {"volume": 10},
             "implied_volatility": 0.2, "greeks": {"delta": 0.5, "gamma": 0.01}},
            {"details": {"contract_type": "put"}, "open_interest": 50, "day": {"volume": 5},
             "implied_volatility": 0.3, "greeks": {"delta": -0.4, "gamma": 0.02}},
        ]
        engine = FakeEngine()
        with patch.object(mod, "OPTIONS_TICKERS", ["SPY", "QQQ"]), \
             patch.object(mod.requests, "get", return_value=Resp(200, {"results": chain})), \
             patch.object(mod.time, "sleep"):
            out = self._puller(engine=engine).pull()
        assert out["status"] == "SUCCESS"
        assert out["rows_inserted"] == 12 == len(engine.conn.inserts())
        assert _classify_outcome(out) == ("SUCCESS", 12, None)

    def test_no_key_is_skipped(self) -> None:
        out = self._puller(api_key="").pull()
        assert _classify_outcome(out)[0] == "SKIPPED"


# ── FMP ─────────────────────────────────────────────────────────────────


class TestFMP:
    def _puller(self, **kw: Any) -> Any:
        from ingestion.altdata.fmp_puller import FMPPuller

        return _bare(FMPPuller, **{"api_key": SECRET, **kw})

    def _router(self, today: date, calls: list) -> Any:
        recent = (today - timedelta(days=2)).isoformat()
        upcoming = (today + timedelta(days=9)).isoformat()

        def fake_get(url: str, params: Any = None, headers: Any = None, timeout: Any = None) -> Resp:
            calls.append({"url": url, "params": params, "headers": headers})
            ep = url.rsplit("/", 1)[-1]
            if ep == "earnings-calendar":
                return Resp(200, [
                    {"symbol": "AAPL", "date": recent, "epsActual": 2.0, "epsEstimated": 1.6,
                     "revenueActual": 10.0, "revenueEstimated": 9.0},
                    {"symbol": "MSFT", "date": upcoming, "epsActual": None, "epsEstimated": 3.0},
                    {"symbol": "ZZZZ", "date": recent, "epsActual": 1.0, "epsEstimated": 1.0},
                ])
            if ep == "sector-performance-snapshot":
                return Resp(200, [{"date": today.isoformat(), "sector": "Energy",
                                   "exchange": params.get("exchange"), "averageChange": 1.5}])
            if ep == "quote":
                if params["symbol"] == "BRK-B":
                    return Resp(402, None, text="Premium Query Parameter")
                return Resp(200, [{"symbol": params["symbol"], "price": 10.0,
                                   "changePercentage": 1.0, "volume": 5, "marketCap": 9.0}])
            return Resp(404, None)

        return fake_get

    def test_stable_api_success_with_honest_counts(self) -> None:
        import ingestion.altdata.fmp_puller as mod

        today = date.today()
        calls: list[dict] = []
        engine = FakeEngine()
        with patch.object(mod.requests, "get", side_effect=self._router(today, calls)), \
             patch.object(mod.time, "sleep"):
            out = self._puller(engine=engine).pull(tickers=["AAPL", "MSFT", "BRK-B"])
        assert out["status"] == "SUCCESS", out
        assert out["restricted_symbols"] == ["BRK-B"]
        inserted = engine.conn.inserts()
        assert out["rows_inserted"] == len(inserted) > 0
        assert _classify_outcome(out) == ("SUCCESS", out["rows_inserted"], None)
        sids = {p["sid"] for p in inserted}
        assert {"earnings:AAPL:eps_actual", "earnings:AAPL:surprise_pct",
                "earnings:calendar:MSFT", "fmp:AAPL:price"} <= sids
        assert not any(s.startswith("earnings:ZZZZ") for s in sids)  # outside the universe
        # Upcoming report: observed today, value = days until it; no future obs_date.
        cal = next(p for p in inserted if p["sid"] == "earnings:calendar:MSFT")
        assert cal["od"] == today and cal["val"] == 9.0
        assert all(p["od"] <= today for p in inserted)
        # /stable API, key in the header only.
        assert all("/stable/" in c["url"] and SECRET not in c["url"] + str(c["params"]) for c in calls)
        assert all(c["headers"] == {"apikey": SECRET} for c in calls)

    def test_rejected_key_is_failed(self) -> None:
        import ingestion.altdata.fmp_puller as mod

        with patch.object(mod.requests, "get", return_value=Resp(401, {"Error Message": "Invalid API KEY"})), \
             patch.object(mod.time, "sleep"):
            out = self._puller().pull(tickers=["AAPL"])
        assert out["status"] == "FAILED"
        assert _classify_outcome(out)[0] == "FAILED"

    def test_limit_reached_mid_run_is_partial(self) -> None:
        import ingestion.altdata.fmp_puller as mod

        today = date.today()
        calls: list[dict] = []
        base = self._router(today, calls)

        def fake_get(url: str, params: Any = None, headers: Any = None, timeout: Any = None) -> Resp:
            if url.endswith("/quote") and params["symbol"] == "MSFT":
                return Resp(429, {"Error Message": "Limit Reach"})
            return base(url, params, headers, timeout)

        with patch.object(mod.requests, "get", side_effect=fake_get), patch.object(mod.time, "sleep"):
            out = self._puller().pull(tickers=["AAPL", "MSFT", "NVDA"])
        assert out["status"] == "PARTIAL"
        assert _classify_outcome(out)[0] == "PARTIAL"


# ── NASA FIRMS ──────────────────────────────────────────────────────────


AVAILABILITY = (
    "data_id,min_date,max_date\n"
    "VIIRS_NOAA20_NRT,2026-08-01,{today}\n"
    "VIIRS_SNPP_NRT,2026-08-01,2026-11-02\n"
)
HEADER = "latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,satellite,instrument,confidence,version,bright_ti5,frp,daynight"


class TestNASAFirms:
    def _puller(self, **kw: Any) -> Any:
        from ingestion.altdata.nasa_firms_puller import NASAFirmsPuller

        return _bare(NASAFirmsPuller, **{"token": "jwt", "map_key": SECRET, **kw})

    def test_map_key_in_path_current_satellite_and_counts(self) -> None:
        import ingestion.altdata.nasa_firms_puller as mod

        urls: list[str] = []
        today = date.today()

        def fake_get(url: str, **_: Any) -> Resp:
            urls.append(url)
            if "/data_availability/" in url:
                return Resp(200, text=AVAILABILITY.format(today=today.isoformat()))
            return Resp(200, text=HEADER + "\n29.7,-95.4,330,0.5,0.5,2026-11-03,100,N20,VIIRS,n,2,290,4,D\n")

        engine = FakeEngine()
        with patch.object(mod.requests, "get", side_effect=fake_get), patch.object(mod.time, "sleep"):
            out = self._puller(engine=engine).pull()
        assert out["status"] == "SUCCESS" and out["satellite"] == "VIIRS_NOAA20_NRT"
        # 9 region counts + 1 infra alert (Houston fire), all committed.
        assert out["rows_inserted"] == 10 == len(engine.conn.inserts())
        assert _classify_outcome(out) == ("SUCCESS", 10, None)
        area = [u for u in urls if "/api/area/csv/" in u]
        assert area and all(f"/api/area/csv/{SECRET}/VIIRS_NOAA20_NRT/" in u for u in area)

    def test_invalid_map_key_is_failed_and_never_logs_the_key(self) -> None:
        import ingestion.altdata.nasa_firms_puller as mod

        today = date.today()

        def fake_get(url: str, **_: Any) -> Resp:
            if "/data_availability/" in url:
                return Resp(200, text=AVAILABILITY.format(today=today.isoformat()))
            return Resp(400, text=f"Invalid MAP_KEY {SECRET}.")

        messages: list[str] = []
        with patch.object(mod.requests, "get", side_effect=fake_get), patch.object(mod.time, "sleep"), \
             patch.object(mod.log, "warning", side_effect=lambda m, **kw: messages.append(m.format(**kw))):
            out = self._puller().pull()
        assert out["status"] == "FAILED" and out["rows_inserted"] == 0
        assert _classify_outcome(out)[0] == "FAILED"
        assert messages and not any(SECRET in m for m in messages)

    def test_no_current_product_is_failed_not_zero_fires(self) -> None:
        import ingestion.altdata.nasa_firms_puller as mod

        stale = "data_id,min_date,max_date\nVIIRS_NOAA20_NRT,2026-01-01,2026-01-02\n"
        engine = FakeEngine()
        with patch.object(mod.requests, "get", return_value=Resp(200, text=stale)):
            out = self._puller(engine=engine).pull()
        assert out["status"] == "FAILED" and engine.conn.inserts() == []

    def test_missing_map_key_is_skipped(self) -> None:
        out = self._puller(map_key="").pull()
        assert _classify_outcome(out)[0] == "SKIPPED"


# ── ADS ─────────────────────────────────────────────────────────────────


def _ads_xlsx(rows: list[tuple[str, float]]) -> bytes:
    import pandas as pd

    buf = io.BytesIO()
    pd.DataFrame({"Date": [d for d, _ in rows], "ADS_Index": [v for _, v in rows],
                  "RECBARS": [0] * len(rows)}).to_excel(buf, index=False)
    return buf.getvalue()


class TestADS:
    def _puller(self, engine: FakeEngine | None = None) -> Any:
        from ingestion.altdata.ads_index import ADSIndexPuller

        p = _bare(ADSIndexPuller, engine)
        p._session = MagicMock()
        return p

    def test_colon_dates_parse_and_rows_are_counted(self) -> None:
        recent = date.today() - timedelta(days=3)
        data = _ads_xlsx([("1960:03:01", -0.58), (recent.strftime("%Y:%m:%d"), 0.08)])
        engine = FakeEngine()
        p = self._puller(engine)
        p._session.get.return_value = Resp(200, content=data)
        out = p.pull_all()
        assert out[0]["status"] == "SUCCESS" and out[0]["rows_inserted"] == 2
        assert {i["od"] for i in engine.conn.inserts()} == {date(1960, 3, 1), recent}
        assert _classify_outcome(out) == ("SUCCESS", 2, None)

    def test_nothing_new_is_unchanged(self) -> None:
        data = _ads_xlsx([("2026:10:24", 0.08)])
        p = self._puller()
        p._session.get.return_value = Resp(200, content=data)
        with patch.object(type(p), "_get_existing_dates", return_value={date(2026, 10, 24)}):
            out = p.pull_all()
        assert out[0]["status"] == "UNCHANGED"
        assert _classify_outcome(out)[0] == "NO_NEW_DATA"

    def test_unparseable_file_is_failed_not_no_data(self) -> None:
        data = _ads_xlsx([("garbage", 0.1), ("also garbage", 0.2)])
        p = self._puller()
        p._session.get.return_value = Resp(200, content=data)
        out = p.pull_all()
        assert out[0]["status"] == "FAILED"
        assert _classify_outcome(out)[0] == "FAILED"

    def test_history_rows_skip_the_five_year_warning(self) -> None:
        p = self._puller()
        with patch("ingestion.base.log.warning") as warn:
            assert p.validate_row("ads.business_conditions_index", date(1960, 3, 1), -0.5) == []
        warn.assert_not_called()


# ── Social_Smart_Money ──────────────────────────────────────────────────


def test_smart_money_all_unconfigured_is_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    import ingestion.altdata.smart_money as sm

    monkeypatch.setattr("ingestion.altdata.reddit_oauth.reddit_credentials_configured", lambda: False)
    p = _bare(sm.SmartMoneyPuller)
    p._load_trust_scores = lambda: None
    out = p.pull_all()
    assert [r["status"] for r in out] == ["SKIPPED", "SKIPPED"]
    outcome, rows, note = _classify_outcome(out)
    assert outcome == "SKIPPED" and rows == 0 and "reddit not configured" in note


# ── Asset_Registries / OpenCorporates ───────────────────────────────────


def test_asset_registries_without_icij_inputs_is_skipped() -> None:
    from ingestion.altdata.asset_registries import AssetRegistryPuller

    out = _bare(AssetRegistryPuller).pull_all()
    assert out["status"] == "SKIPPED" and "ICIJ" in out["skipped_reason"]
    assert _classify_outcome(out)[0] == "SKIPPED"


class TestOpenCorporates:
    def _puller(self, token: str = SECRET, engine: FakeEngine | None = None) -> Any:
        from ingestion.altdata.opencorporates import OpenCorporatesPuller, _RateLimiter

        return _bare(OpenCorporatesPuller, engine, _limiter=_RateLimiter(0), _search_calls_used=0,
                     _api_token=token, _failed_requests=0, _auth_failed=False,
                     _officers_available=0, _officers_searched=0, _rows_inserted=0)

    def test_no_token_is_skipped_without_db_or_network(self) -> None:
        import ingestion.altdata.opencorporates as mod

        with patch.object(mod.requests, "get") as get:
            out = self._puller(token="").pull_all()
        get.assert_not_called()
        assert out["status"] == "SKIPPED" and "OPENCORPORATES_API_TOKEN" in out["skipped_reason"]
        assert _classify_outcome(out)[0] == "SKIPPED"

    def test_no_icij_inputs_is_skipped(self) -> None:
        out = self._puller().pull_all()
        assert out["status"] == "SKIPPED" and "ICIJ" in out["skipped_reason"]

    def test_rejected_token_is_failed_and_stops(self) -> None:
        import ingestion.altdata.opencorporates as mod

        conn = FakeConn()
        conn.execute = MagicMock(return_value=FakeResult(rows=[("Jane Roe", "a1"), ("John Doe", "a2")]))
        p = self._puller(engine=FakeEngine(conn))
        with patch.object(mod.requests, "get", return_value=Resp(401, {"error": {}})) as get:
            out = p.pull_all()
        assert out["status"] == "FAILED" and get.call_count == 1
        assert _classify_outcome(out)[0] == "FAILED"
        assert get.call_args.kwargs["params"]["api_token"] == SECRET

    def test_matches_report_committed_rows(self) -> None:
        import ingestion.altdata.opencorporates as mod

        conn = FakeConn()
        orig = conn.execute

        def execute(stmt: Any, params: dict | None = None) -> FakeResult:
            if "OFFSHORE:%" in str(stmt):
                return FakeResult(rows=[("Jane Roe", "a1")])
            return orig(stmt, params)

        conn.execute = execute
        hit = {"results": {"officers": [{"officer": {
            "name": "JANE ROE", "position": "director",
            "company": {"name": "X Ltd", "company_number": "123", "jurisdiction_code": "gb"}}}]}}
        with patch.object(mod.requests, "get", return_value=Resp(200, hit)):
            out = self._puller(engine=FakeEngine(conn)).pull_all()
        assert out["status"] == "SUCCESS" and out["rows_inserted"] == 1
        assert _classify_outcome(out) == ("SUCCESS", 1, None)


# ── Wikidata persons ────────────────────────────────────────────────────


def _wd_puller(engine: FakeEngine | None = None) -> Any:
    from ingestion.altdata.wikidata_persons import WikidataPersonPuller, _RateLimiter

    return _bare(WikidataPersonPuller, engine, _limiter=_RateLimiter(0))


def _search_hit(qid: str) -> Resp:
    return Resp(200, {"search": [{"id": qid}]})


def _sparql_person(qid: str, label: str) -> Resp:
    return Resp(200, {"results": {"bindings": [{
        "person": {"value": f"http://www.wikidata.org/entity/{qid}"},
        "personLabel": {"value": label}}]}})


def _sparql_links(*links: tuple[str, str, str]) -> Resp:
    return Resp(200, {"results": {"bindings": [{
        "p": {"value": f"http://www.wikidata.org/prop/{pid}"},
        "target": {"value": f"http://www.wikidata.org/entity/{tq}"},
        "targetLabel": {"value": label}} for pid, tq, label in links]}})


class TestWikidata:
    ACTORS = {"a1": {"name": "Jim Simons (Estate / Renaissance)", "category": "fund"},
              "a2": {"name": "Ray Dalio", "category": "fund"}}

    def test_counts_upserts_and_follows_links_by_qid_not_label_search(self) -> None:
        import ingestion.altdata.wikidata_persons as mod

        searches: list[str] = []

        def fake_get(url: str, params: Any = None, headers: Any = None, timeout: Any = None) -> Resp:
            if url == mod._SEARCH_URL:
                searches.append(params["search"])
                return _search_hit("Q1" if params["search"] == "Jim Simons" else "Q2")
            q = params["query"]
            if "wdt:P31 wd:Q5" in q:
                return _sparql_person("Q1" if "wd:Q1 " in q else "Q2", "x")
            if "wd:Q1 ?p" in q:
                return _sparql_links(("P39", "Q100", "delegate"), ("P26", "Q200", "Spouse Name"))
            return _sparql_links()

        engine = FakeEngine()
        with patch.object(mod.requests, "get", side_effect=fake_get):
            out = _wd_puller(engine).pull_all(actors=self.ACTORS)
        # Seed names are cleaned; connection labels ('delegate') never searched.
        assert searches == ["Jim Simons", "Ray Dalio"]
        assert out["status"] == "SUCCESS"
        assert out["rows_upserted"] == len(engine.conn.inserts("wikidata_persons")) + len(
            engine.conn.inserts("wikidata_connections")) > 0
        assert _classify_outcome(out) == ("SUCCESS", out["rows_upserted"], None)

    def test_retry_after_is_honoured_then_request_succeeds(self) -> None:
        import ingestion.altdata.wikidata_persons as mod

        replies = [Resp(429, headers={"Retry-After": "7"}), _search_hit("Q9")]
        sleeps: list[float] = []
        p = _wd_puller()
        with patch.object(mod.requests, "get", side_effect=lambda *a, **k: replies.pop(0)), \
             patch.object(mod.time, "sleep", side_effect=sleeps.append):
            resp = p._http_get(mod._SEARCH_URL, {}, {}, 15)
        assert resp.status_code == 200 and 7.0 in sleeps

    def test_sustained_429_stops_the_batch_as_partial_with_counts(self) -> None:
        import ingestion.altdata.wikidata_persons as mod

        calls = {"n": 0}

        def fake_get(url: str, params: Any = None, headers: Any = None, timeout: Any = None) -> Resp:
            calls["n"] += 1
            return Resp(429, headers={"Retry-After": "1"})

        with patch.object(mod.requests, "get", side_effect=fake_get), patch.object(mod.time, "sleep"):
            out = _wd_puller().pull_all(actors=self.ACTORS)
        assert calls["n"] == mod._MAX_CONSECUTIVE_429  # stopped, did not hammer actor 2
        assert out["status"] == "PARTIAL" and out["stopped_by_rate_limit"] is True
        assert out["rows_upserted"] == 0
        assert _classify_outcome(out)[0] == "PARTIAL"

    def test_retry_after_above_cap_stops_immediately(self) -> None:
        import ingestion.altdata.wikidata_persons as mod

        with patch.object(mod.requests, "get", return_value=Resp(429, headers={"Retry-After": "3600"})) as get, \
             patch.object(mod.time, "sleep") as sleep:
            with pytest.raises(mod.WikidataRateLimited):
                _wd_puller()._http_get(mod._SEARCH_URL, {}, {}, 15)
        assert get.call_count == 1
        sleep.assert_not_called()

    def test_clean_actor_name(self) -> None:
        from ingestion.altdata.wikidata_persons import _clean_actor_name

        assert _clean_actor_name("George Soros / Soros Fund Management") == "George Soros"
        assert _clean_actor_name("Jim Simons (Estate / Renaissance)") == "Jim Simons"


# ── yfinance_options ────────────────────────────────────────────────────


class TestOptionsCoverage:
    def _results(self, extra: list[dict]) -> Any:
        from ingestion.options import EQUITY_TICKERS, OptionsPullResults

        items = [{"ticker": t, "status": "SUCCESS", "rows_inserted": 10} for t in EQUITY_TICKERS]
        return OptionsPullResults(items + extra, full_universe=True)

    def test_catalyst_tickers_without_a_chain_are_accounted_for(self) -> None:
        out = self._results([
            {"ticker": "TKPHF", "status": "SKIPPED", "rows_inserted": 0, "reason": "no expirations"},
            {"ticker": "NUOX", "status": "SKIPPED", "rows_inserted": 0, "reason": "no chain listed"},
        ]).summary
        assert out["status"] == "SUCCESS" and out["tickers_no_chain"] == 2
        assert _classify_outcome(out)[0] == "SUCCESS"

    def test_core_ticker_without_a_chain_is_still_partial(self) -> None:
        from ingestion.options import EQUITY_TICKERS, OptionsPullResults

        items = [{"ticker": t, "status": "SUCCESS", "rows_inserted": 10} for t in EQUITY_TICKERS[1:]]
        items.append({"ticker": EQUITY_TICKERS[0], "status": "SKIPPED", "rows_inserted": 0,
                      "reason": "no expirations"})
        out = OptionsPullResults(items, full_universe=True).summary
        assert out["status"] == "PARTIAL" and EQUITY_TICKERS[0] in out["reason"]

    def test_real_failures_still_partial(self) -> None:
        out = self._results([
            {"ticker": "CRNX", "status": "FAILED", "rows_inserted": 0, "error": "no data from Yahoo"},
        ]).summary
        assert out["status"] == "PARTIAL"

    def test_empty_chain_result_marks_no_chain(self) -> None:
        from ingestion.options import NO_CHAIN, YahooOptionsClient

        client = YahooOptionsClient.__new__(YahooOptionsClient)
        client.crumb = "c"
        client.session = SimpleNamespace(get=lambda *a, **k: Resp(200, {"optionChain": {"result": [], "error": None}}))
        # A well-formed empty result is the falsy NO_CHAIN sentinel (merged
        # with the no-listed-options cache), never a transport failure.
        assert client.get_options("NUOX") is NO_CHAIN and not client.get_options("NUOX")
        client.session = SimpleNamespace(get=lambda *a, **k: Resp(500, None))
        assert client.get_options("SPY") is None and client.last_no_chain is False
