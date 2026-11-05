"""Write-count contract for the seven sources Hermes listed on 2026-11-05.

grid-hermes (grid-svr, 2026-11-05 11:23Z) listed these as failing with
``puller did not report committed write count``:

    FARA_DOJ, smart:social_sentiment, Asset_Registries, OpenCorporates,
    polygon, wikidata_persons, wikipedia_pageviews

Polygon, OpenCorporates, Asset_Registries and wikidata_persons were fixed
by 7de10c2c (their listed failures predate that deploy). FARA_DOJ,
SocialSentiment and wikipedia_pageviews (``wikipedia_text.WikipediaPuller``,
the grid-scheduler ``Wikipedia_Pageviews`` puller) are fixed here.

Every test drives the real puller method with network and DB mocked, then
classifies its return value exactly as both schedulers do
(``smart_scheduler._classify_outcome``). ``test_journal_source_happy_path``
is the regression guard: it fails if any of the seven reverts to an opaque
return on a run that committed rows.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("FRED_API_KEY", "test")

from ingestion.smart_scheduler import (
    PULLER_REGISTRY,
    SMART_PULL_LOG_PREFIX,
    _classify_outcome,
)

OPAQUE = "puller did not report committed write count"
VALID_OUTCOMES = {"SUCCESS", "NO_NEW_DATA", "PARTIAL", "SKIPPED", "FAILED"}


# ── fakes (no DB, no network) ───────────────────────────────────────────


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
    """Records statements. INSERT rowcount 1; SELECT answers via ``selects``.

    ``selects`` maps a SQL substring to a FakeResult; unmatched SELECTs
    return no rows.
    """

    def __init__(self, selects: dict[str, FakeResult] | None = None) -> None:
        self.statements: list[tuple[str, dict]] = []
        self.selects = selects or {}

    def execute(self, stmt: Any, params: dict | None = None) -> FakeResult:
        sql = str(stmt)
        self.statements.append((sql, dict(params or {})))
        if sql.lstrip().upper().startswith("SELECT"):
            for needle, result in self.selects.items():
                if needle in sql:
                    return result
            return FakeResult(rowcount=0)
        return FakeResult(rowcount=1)

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
        self.begins = 0

    def _cm(self) -> MagicMock:
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=self.conn)
        cm.__exit__ = MagicMock(return_value=False)
        return cm

    def begin(self) -> MagicMock:
        self.begins += 1
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
    def __init__(self, status: int = 200, json_data: Any = None, headers: dict | None = None) -> None:
        self.status_code = status
        self._json = json_data
        self.headers = headers or {}
        self.text = ""
        self.content = b""

    def json(self) -> Any:
        if self._json is None:
            raise ValueError("no json")
        return self._json

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            import requests

            raise requests.exceptions.HTTPError(f"{self.status_code} Client Error")


def _no_sleep() -> Any:
    """Silence every sleep the pullers and retry decorator use."""
    from ingestion import base

    return patch.object(base.time, "sleep")


# ── FARA_DOJ ────────────────────────────────────────────────────────────

_TODAY = date.today()


def _mmddyyyy(d: date) -> str:
    return d.strftime("%m/%d/%Y")


def _fara_active(*regs: tuple[int, str, date]) -> dict:
    # Shape of GET /api/v1/Registrants/json/Active (probed 2026-11-05).
    return {"REGISTRANTS_ACTIVE": {"ROW": [
        {"Registration_Number": n, "Name": name, "Registration_Date": _mmddyyyy(d),
         "Address_1": "1701 Pennsylvania Avenue, NW", "City": "Washington", "State": "DC"}
        for n, name, d in regs
    ]}}


def _fara_principals(*principals: tuple[str, str]) -> dict:
    # Shape of GET /api/v1/ForeignPrincipals/json/Active/{n}: ROW is a dict
    # for one principal, a list for several.
    rows = [{"FP_NAME": name, "COUNTRY_NAME": country, "REG_NUMBER": 1} for name, country in principals]
    return {"ROWSET": {"ROW": rows[0] if len(rows) == 1 else rows}}


class FaraHTTP:
    """Routes FARA URLs; records every URL requested."""

    def __init__(self, active: Resp, principals: dict[str, Resp] | None = None) -> None:
        self.active = active
        self.principals = principals or {}
        self.urls: list[str] = []

    def __call__(self, url: str, params: Any = None, timeout: Any = None, headers: Any = None) -> Resp:
        import ingestion.altdata.fara as mod

        self.urls.append(url)
        if url == mod._FARA_ACTIVE_REGISTRANTS_URL:
            return self.active
        if url.startswith(mod._FARA_PRINCIPALS_URL):
            return self.principals.get(url.rsplit("/", 1)[1], Resp(404))
        return Resp(404)


def _run_fara(http: FaraHTTP, engine: FakeEngine | None = None, days_back: int = 30) -> dict:
    import ingestion.altdata.fara as mod

    puller = _bare(mod.FARAPuller, engine)
    with patch.object(mod.requests, "get", side_effect=http), \
         patch.object(mod.time, "sleep"), _no_sleep():
        return puller.pull_all(days_back=days_back)


class TestFARA:
    def test_live_endpoint_reports_committed_rows(self) -> None:
        import ingestion.altdata.fara as mod

        http = FaraHTTP(
            Resp(200, _fara_active(
                (7791, "Javelin Advisors LLC", _TODAY - timedelta(days=5)),
                (7792, "Vernick, Victor", _TODAY - timedelta(days=1)),
                (100, "Old Registrant", _TODAY - timedelta(days=400)),
            )),
            {"7791": Resp(200, _fara_principals(("Prosecutor General's Office", "Kazakhstan"))),
             "7792": Resp(200, _fara_principals(("Ministry A", "QATAR"), ("Firm B", "CHINA")))},
        )
        engine = FakeEngine()
        out = _run_fara(http, engine)

        assert out["status"] == "SUCCESS"
        assert out["rows_inserted"] == 3 == len(engine.conn.inserts())
        assert out["registrants"]["registrants_fetched"] == 3
        assert out["registrants"]["registrants_recent"] == 2
        assert "activity-report endpoint" in out["activities_unavailable"]
        assert _classify_outcome(out) == ("SUCCESS", 3, None)
        # Only the live endpoints; principals only for recent registrants;
        # never the dead search endpoints.
        assert http.urls[0] == mod._FARA_ACTIVE_REGISTRANTS_URL
        assert sorted(http.urls[1:]) == [mod._FARA_PRINCIPALS_URL + "7791", mod._FARA_PRINCIPALS_URL + "7792"]
        assert not any("search" in u for u in http.urls)
        series = sorted(p["sid"] for p in engine.conn.inserts())
        assert series == [
            "FARA:CHINA:VERNICK_VICTOR:REGISTRATION",
            "FARA:KAZAKHSTAN:JAVELIN_ADVISORS_LLC:REGISTRATION",
            "FARA:QATAR:VERNICK_VICTOR:REGISTRATION",
        ]

    def test_registrant_list_404_is_failed_with_reason_not_opaque(self) -> None:
        engine = FakeEngine()
        out = _run_fara(FaraHTTP(Resp(404)), engine)
        assert out["status"] == "FAILED" and out["rows_inserted"] == 0
        assert "active registrants" in out["error"] and "404" in out["error"]
        outcome, rows, note = _classify_outcome(out)
        assert (outcome, rows) == ("FAILED", 0) and note != OPAQUE and "404" in note
        assert engine.begins == 0

    def test_unexpected_shape_is_failed_not_empty_success(self) -> None:
        out = _run_fara(FaraHTTP(Resp(200, {"SOMETHING_ELSE": {}})))
        assert _classify_outcome(out)[0] == "FAILED"
        assert "unexpected response shape" in out["error"]

    def test_failed_principal_lookup_is_partial_with_committed_rows(self) -> None:
        http = FaraHTTP(
            Resp(200, _fara_active(
                (7791, "Javelin Advisors LLC", _TODAY - timedelta(days=5)),
                (7792, "Vernick, Victor", _TODAY - timedelta(days=1)),
            )),
            {"7791": Resp(200, _fara_principals(("Office", "KAZAKHSTAN")))},  # 7792 -> 404
        )
        out = _run_fara(http)
        assert out["status"] == "PARTIAL" and out["rows_inserted"] == 1
        assert out["registrants"]["principal_lookups_failed"] == 1
        assert _classify_outcome(out)[:2] == ("PARTIAL", 1)

    def test_no_recent_registrations_is_no_new_data(self) -> None:
        http = FaraHTTP(Resp(200, _fara_active((100, "Old Registrant", _TODAY - timedelta(days=400)))))
        out = _run_fara(http)
        assert out["status"] == "SUCCESS" and out["rows_inserted"] == 0
        import ingestion.altdata.fara as mod

        assert http.urls == [mod._FARA_ACTIVE_REGISTRANTS_URL]
        assert _classify_outcome(out) == ("NO_NEW_DATA", 0, "run completed, 0 rows written")

    def test_lookup_cap_reports_unattempted_as_partial(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import ingestion.altdata.fara as mod

        monkeypatch.setattr(mod, "_MAX_PRINCIPAL_LOOKUPS", 1)
        http = FaraHTTP(
            Resp(200, _fara_active(
                (7791, "Javelin Advisors LLC", _TODAY - timedelta(days=5)),
                (7792, "Vernick, Victor", _TODAY - timedelta(days=1)),
            )),
            {"7791": Resp(200, _fara_principals(("Office", "KAZAKHSTAN")))},
        )
        out = _run_fara(http)
        assert out["registrants"]["principal_lookups_unattempted"] == 1
        assert _classify_outcome(out)[:2] == ("PARTIAL", 1)

    def test_dead_activities_endpoint_is_reported_failed_when_called_directly(self) -> None:
        import ingestion.altdata.fara as mod

        puller = _bare(mod.FARAPuller)
        with patch.object(mod.requests, "get", return_value=Resp(404)), \
             patch.object(mod.time, "sleep"), _no_sleep():
            out = puller.pull_activities(days_back=30)
        assert out["status"] == "FAILED" and out["rows_inserted"] == 0
        assert "404" in out["error"]
        assert _classify_outcome(out)[0] == "FAILED"


# ── wikipedia_pageviews (wikipedia_text.WikipediaPuller) ────────────────


def _views(n: int = 60, spike: bool = False) -> dict:
    start = _TODAY - timedelta(days=n)
    items = [{"timestamp": (start + timedelta(days=i)).strftime("%Y%m%d00"), "views": 100 + (i % 3)}
             for i in range(n)]
    if spike:
        items[-1]["views"] = 100_000
    return {"items": items}


def _run_wiki(get: Callable[..., Resp], engine: FakeEngine | None = None,
              watchlist: dict[str, str] | None = None) -> dict:
    import ingestion.altdata.wikipedia_text as mod

    puller = _bare(mod.WikipediaPuller, engine)
    wl = watchlist or {"Apple": "Apple_Inc.", "Nvidia": "Nvidia"}
    with patch.object(mod, "WATCHLIST", wl), \
         patch.object(mod.requests, "get", side_effect=get), _no_sleep():
        return puller.pull()


class TestWikipediaPageviews:
    def test_success_reports_committed_raw_rows(self) -> None:
        engine = FakeEngine()
        out = _run_wiki(lambda *a, **k: Resp(200, _views(60)), engine)
        assert out["status"] == "SUCCESS"
        assert out["rows_inserted"] == 120 == len(engine.conn.inserts())
        assert _classify_outcome(out) == ("SUCCESS", 120, None)

    def test_anomalies_still_detected_and_stored(self) -> None:
        engine = FakeEngine()
        out = _run_wiki(lambda *a, **k: Resp(200, _views(60, spike=True)), engine)
        assert out["anomalies"] and len(engine.conn.inserts("attention_anomaly")) == len(out["anomalies"])
        assert _classify_outcome(out)[0] == "SUCCESS"

    def test_one_article_failing_is_partial_with_rows(self) -> None:
        def get(url: str, **kw: Any) -> Resp:
            return Resp(429) if "Nvidia" in url else Resp(200, _views(60))

        out = _run_wiki(get)
        assert out["status"] == "PARTIAL" and out["rows_inserted"] == 60
        assert "1 of 2 lookups failed" in out["error"]
        assert _classify_outcome(out)[:2] == ("PARTIAL", 60)

    def test_every_article_failing_is_failed(self) -> None:
        out = _run_wiki(lambda *a, **k: Resp(429))
        assert out["status"] == "FAILED" and out["rows_inserted"] == 0
        assert _classify_outcome(out)[:2] == ("FAILED", 0)

    def test_nothing_new_is_no_new_data(self) -> None:
        data = _views(60)
        stored = [(date(int(i["timestamp"][:4]), int(i["timestamp"][4:6]), int(i["timestamp"][6:8])),)
                  for i in data["items"]]
        engine = FakeEngine(FakeConn({"SELECT DISTINCT obs_date FROM raw_series": FakeResult(rows=stored)}))
        out = _run_wiki(lambda *a, **k: Resp(200, data), engine)
        assert out["rows_inserted"] == 0 and engine.conn.inserts() == []
        assert _classify_outcome(out) == ("NO_NEW_DATA", 0, "run completed, 0 rows written")

    def test_short_history_is_checked_not_failed(self) -> None:
        out = _run_wiki(lambda *a, **k: Resp(200, _views(10)))
        assert out["entities_short_history"] == 2
        assert _classify_outcome(out)[0] == "NO_NEW_DATA"


# ── smart:social_sentiment ──────────────────────────────────────────────


def _social_engine(already_stored: bool = False) -> FakeEngine:
    return FakeEngine(FakeConn({
        "SELECT id FROM source_catalog": FakeResult(one=(42,)),
        "SELECT 1 FROM raw_series": FakeResult(one=(1,) if already_stored else None),
    }))


_POSTS = [{"title": "NVDA calls to the moon, bull breakout", "score": 10, "comments": 1,
           "upvote_ratio": 0.9, "created": 0, "subreddit": "stocks"}]


def _run_social(
    engine: Any,
    reddit: Callable[[str], list],
    bluesky: Callable[[str], list],
    mentions: Callable[[str], list] = lambda flt: [],
) -> dict:
    import ingestion.social_sentiment as mod

    puller = mod.SocialSentimentPuller(db_engine=engine)
    # One combined RSS request returns every board; the stub serves r/stocks.
    puller._reddit_hot_all = lambda subs: {"stocks": reddit("stocks")}
    puller._reddit_mentions = mentions
    puller._bluesky_search = bluesky
    puller._google_trends = lambda kw: {}
    with patch.object(mod.time, "sleep"):
        return puller.pull_all()


def _http403(*_a: Any) -> list:
    import requests

    raise requests.exceptions.HTTPError("403 Client Error: Blocked")


class TestSocialSentiment:
    def test_success_persists_scan_and_reports_one_row(self) -> None:
        engine = _social_engine()
        out = _run_social(engine, lambda sub: list(_POSTS), lambda term: [])
        assert out["status"] == "SUCCESS" and out["rows_inserted"] == 1
        rows = engine.conn.inserts("raw_series")
        assert len(rows) == 1 and rows[0]["sid"] == f"social_sentiment_{_TODAY.isoformat()}"
        # The stored payload is the scan, not the write-contract envelope.
        assert '"status"' not in rows[0]["payload"]
        assert _classify_outcome(out) == ("SUCCESS", 1, None)

    def test_already_stored_today_is_no_new_data(self) -> None:
        engine = _social_engine(already_stored=True)
        out = _run_social(engine, lambda sub: list(_POSTS), lambda term: [])
        assert out["rows_inserted"] == 0 and engine.conn.inserts("raw_series") == []
        assert _classify_outcome(out) == ("NO_NEW_DATA", 0, "run completed, 0 rows written")

    def test_every_source_403_is_failed_with_reason_and_no_write(self) -> None:
        # 1 Reddit RSS + 2 ApeWisdom + 5 Bluesky requests, all refused.
        engine = _social_engine()
        out = _run_social(engine, _http403, _http403, _http403)
        assert out["status"] == "FAILED" and out["rows_inserted"] == 0
        assert "8 of 8 social requests failed" in out["error"] and "403" in out["error"]
        assert engine.begins == 0
        outcome, rows, note = _classify_outcome(out)
        assert (outcome, rows) == ("FAILED", 0) and "403" in note

    def test_reddit_blocked_bluesky_alive_is_partial_with_row(self) -> None:
        engine = _social_engine()
        bsky = [{"text": "SPY rally strong", "likes": 1, "reposts": 0, "replies": 0,
                 "created": "", "source": "bluesky"}]
        out = _run_social(engine, _http403, lambda term: list(bsky))
        assert out["status"] == "PARTIAL" and out["rows_inserted"] == 1
        assert _classify_outcome(out)[:2] == ("PARTIAL", 1)

    def test_store_failure_is_failed(self) -> None:
        engine = MagicMock()
        engine.begin.side_effect = RuntimeError("db down")
        out = _run_social(engine, lambda sub: list(_POSTS), lambda term: [])
        assert out["status"] == "FAILED" and "store failed" in out["error"]
        assert _classify_outcome(out)[:2] == ("FAILED", 0)

    def test_no_engine_is_skipped(self) -> None:
        out = _run_social(None, lambda sub: list(_POSTS), lambda term: [])
        assert _classify_outcome(out)[0] == "SKIPPED"

    def test_save_to_db_stays_idempotent_after_pull_all(self) -> None:
        # intelligence/scheduler.py calls save_to_db(result) after pull_all().
        import ingestion.social_sentiment as mod

        engine = _social_engine(already_stored=True)
        assert mod.SocialSentimentPuller(db_engine=engine).save_to_db(
            {"date": _TODAY.isoformat(), "ticker_sentiment": {"NVDA": {"mentions": 1}}}) is True
        assert engine.conn.inserts("raw_series") == []


# ── regression guard: all seven journal sources ─────────────────────────


def _happy_fara() -> tuple[dict, int]:
    engine = FakeEngine()
    http = FaraHTTP(
        Resp(200, _fara_active((7791, "Javelin Advisors LLC", _TODAY - timedelta(days=2)))),
        {"7791": Resp(200, _fara_principals(("Office", "KAZAKHSTAN")))},
    )
    return _run_fara(http, engine), len(engine.conn.inserts())


def _happy_social() -> tuple[dict, int]:
    engine = _social_engine()
    out = _run_social(engine, lambda sub: list(_POSTS), lambda term: [])
    return out, len(engine.conn.inserts("raw_series"))


def _happy_wikipedia() -> tuple[dict, int]:
    engine = FakeEngine()
    out = _run_wiki(lambda *a, **k: Resp(200, _views(40)), engine)
    return out, len(engine.conn.inserts())


def _happy_polygon() -> tuple[dict, int]:
    import ingestion.altdata.polygon_puller as mod

    chain = [
        {"details": {"contract_type": "call"}, "open_interest": 100, "day": {"volume": 10},
         "implied_volatility": 0.2, "greeks": {"delta": 0.5, "gamma": 0.01}},
        {"details": {"contract_type": "put"}, "open_interest": 50, "day": {"volume": 5},
         "implied_volatility": 0.3, "greeks": {"delta": -0.4, "gamma": 0.02}},
    ]
    engine = FakeEngine()
    with patch.object(mod, "OPTIONS_TICKERS", ["SPY"]), \
         patch.object(mod.requests, "get", return_value=Resp(200, {"results": chain})), \
         patch.object(mod.time, "sleep"):
        out = _bare(mod.PolygonPuller, engine, api_key="k").pull()
    return out, len(engine.conn.inserts())


def _happy_opencorporates() -> tuple[dict, int]:
    import ingestion.altdata.opencorporates as mod

    engine = FakeEngine(FakeConn({"OFFSHORE:%": FakeResult(rows=[("Jane Roe", "a1")])}))
    hit = {"results": {"officers": [{"officer": {
        "name": "JANE ROE", "position": "director",
        "company": {"name": "X Ltd", "company_number": "123", "jurisdiction_code": "gb"}}}]}}
    puller = _bare(mod.OpenCorporatesPuller, engine, _limiter=mod._RateLimiter(0),
                   _search_calls_used=0, _api_token="t", _failed_requests=0, _auth_failed=False,
                   _officers_available=0, _officers_searched=0, _rows_inserted=0)
    with patch.object(mod.requests, "get", return_value=Resp(200, hit)):
        out = puller.pull_all()
    return out, len(engine.conn.inserts())


def _happy_asset_registries() -> tuple[dict, int]:
    from ingestion.altdata.asset_registries import AssetRegistryPuller

    engine = FakeEngine(FakeConn({"ICIJ_OFFSHORE": FakeResult(rows=[("Jane Roe", "Jane Roe", "a1")])}))
    puller = _bare(AssetRegistryPuller, engine)
    puller.search_aircraft = lambda name: [{"n_number": "N123", "registrant_name": name}]
    puller.search_vessels = lambda name: []
    out = puller.pull_all()
    return out, len(engine.conn.inserts())


def _happy_wikidata() -> tuple[dict, int]:
    import ingestion.altdata.wikidata_persons as mod

    def get(url: str, params: Any = None, headers: Any = None, timeout: Any = None) -> Resp:
        if url == mod._SEARCH_URL:
            return Resp(200, {"search": [{"id": "Q1"}]})
        if "wdt:P31 wd:Q5" in params["query"]:
            return Resp(200, {"results": {"bindings": [{
                "person": {"value": "http://www.wikidata.org/entity/Q1"},
                "personLabel": {"value": "Ray Dalio"}}]}})
        return Resp(200, {"results": {"bindings": [{
            "p": {"value": "http://www.wikidata.org/prop/P108"},
            "target": {"value": "http://www.wikidata.org/entity/Q100"},
            "targetLabel": {"value": "Bridgewater"}}]}})

    engine = FakeEngine()
    puller = _bare(mod.WikidataPersonPuller, engine, _limiter=mod._RateLimiter(0))
    with patch.object(mod.requests, "get", side_effect=get):
        out = puller.pull_all(actors={"a1": {"name": "Ray Dalio", "category": "fund"}})
    writes = len(engine.conn.inserts("wikidata_persons")) + len(engine.conn.inserts("wikidata_connections"))
    return out, writes


_SCHEDULER_SRC = Path(__file__).resolve().parents[1].joinpath("ingestion", "scheduler.py").read_text()


def _smart_entry(name: str) -> dict:
    return next(e for e in PULLER_REGISTRY if e["name"] == name)


def _source_name(mod: str, cls: str) -> str:
    import importlib

    return getattr(importlib.import_module(mod), cls).SOURCE_NAME


def _wiring_smart(registry_name: str, mod: str, cls: str, method: str, journal: str) -> None:
    """SmartScheduler runs mod.cls.method; the journal names its source."""
    entry = _smart_entry(registry_name)
    assert (entry["mod"], entry["cls"], entry["method"]) == (mod, cls, method)
    if journal.startswith(SMART_PULL_LOG_PREFIX):
        # pull_log name of a puller with no BasePuller SOURCE_NAME.
        assert journal == SMART_PULL_LOG_PREFIX + registry_name
    else:
        assert _source_name(mod, cls) == journal


def _wiring_grid_scheduler(label: str, mod: str, cls: str, method: str, journal: str) -> None:
    """grid-scheduler run_pull_group lists (label, cls(db_engine), method)."""
    assert f'("{label}", {cls}(db_engine), "{method}"' in _SCHEDULER_SRC
    assert _source_name(mod, cls) == journal


# (journal source name, wiring check, happy-path run)
JOURNAL_SOURCES: list[tuple[str, Callable[[], None], Callable[[], tuple[dict, int]]]] = [
    ("FARA_DOJ",
     lambda: _wiring_smart("fara", "ingestion.altdata.fara", "FARAPuller", "pull_all", "FARA_DOJ"),
     _happy_fara),
    ("smart:social_sentiment",
     lambda: _wiring_smart("social_sentiment", "ingestion.social_sentiment",
                           "SocialSentimentPuller", "pull_all", "smart:social_sentiment"),
     _happy_social),
    ("Asset_Registries",
     lambda: _wiring_smart("asset_registries", "ingestion.altdata.asset_registries",
                           "AssetRegistryPuller", "pull_all", "Asset_Registries"),
     _happy_asset_registries),
    ("OpenCorporates",
     lambda: _wiring_smart("opencorporates", "ingestion.altdata.opencorporates",
                           "OpenCorporatesPuller", "pull_all", "OpenCorporates"),
     _happy_opencorporates),
    ("polygon",
     lambda: _wiring_grid_scheduler("Polygon", "ingestion.altdata.polygon_puller",
                                    "PolygonPuller", "pull", "polygon"),
     _happy_polygon),
    ("wikidata_persons",
     lambda: _wiring_smart("wikidata_persons", "ingestion.altdata.wikidata_persons",
                           "WikidataPersonPuller", "pull_all", "wikidata_persons"),
     _happy_wikidata),
    ("wikipedia_pageviews",
     lambda: _wiring_grid_scheduler("Wikipedia_Pageviews", "ingestion.altdata.wikipedia_text",
                                    "WikipediaPuller", "pull", "wikipedia_pageviews"),
     _happy_wikipedia),
]


@pytest.mark.parametrize("source,wiring,happy", JOURNAL_SOURCES, ids=[s[0] for s in JOURNAL_SOURCES])
def test_journal_source_happy_path(source: str, wiring: Callable[[], None],
                                   happy: Callable[[], tuple[dict, int]]) -> None:
    """A run that committed rows must classify SUCCESS with exactly that count."""
    wiring()
    out, writes = happy()
    outcome, rows, note = _classify_outcome(out)
    assert note != OPAQUE, f"{source} regressed to an opaque return: {out!r}"[:500]
    assert outcome in VALID_OUTCOMES
    assert writes > 0, f"{source} happy path wrote nothing"
    assert (outcome, rows) == ("SUCCESS", writes), f"{source}: {outcome} {rows} {note}"


def test_all_seven_journal_sources_are_covered() -> None:
    assert sorted(s[0] for s in JOURNAL_SOURCES) == sorted([
        "FARA_DOJ", "smart:social_sentiment", "Asset_Registries", "OpenCorporates",
        "polygon", "wikidata_persons", "wikipedia_pageviews",
    ])
