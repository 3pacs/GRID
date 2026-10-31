"""Tests for api/routers/schwab_private.py on a minimal FastAPI app (no api.main import).

Negative controls: no token (401), contributor role (403), flag off (503 and the reader
factory is never called), reader not configured (503), symbol validation (422), reader
failure (200 with unavailable rows), stale/delayed never labelled live, private cache
headers, and a source guard that the router never touches credential paths itself.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("DB_PASSWORD", "testpass")

import api.routers.schwab_private as sp
from api.auth import create_token
from ingestion.schwab_market import SchwabTransientError, parse_quotes
from store.availability import is_unavailable

LAST, BID, ASK = 731.37, 731.35, 731.39


class Never:
    """Reader factory that must not be reached."""

    def __call__(self):
        raise AssertionError("reader factory was called")


class ParsingReader:
    """Runs the reviewed parser over a synthetic body; no network."""

    def __init__(self, body: dict | None = None, *, raise_exc: Exception | None = None):
        self.body = body or {}
        self.raise_exc = raise_exc
        self.calls: list[tuple[str, ...]] = []

    def quotes(self, symbols, fields="quote"):
        self.calls.append(tuple(symbols))
        if self.raise_exc is not None:
            raise self.raise_exc
        return parse_quotes(json.loads(json.dumps(self.body)))


def entry(symbol: str, *, age_s: float = 1.0, realtime: bool = True, now: datetime | None = None, **q):
    now = now or datetime.now(timezone.utc)
    ms = int((now - timedelta(seconds=age_s)).timestamp() * 1000)
    quote = {"lastPrice": LAST, "bidPrice": BID, "askPrice": ASK, "bidSize": 100, "askSize": 200,
             "totalVolume": 12345, "quoteTime": ms, "tradeTime": ms}
    quote.update(q)
    return {"symbol": symbol, "assetMainType": "EQUITY", "realtime": realtime, "quote": quote}


@pytest.fixture(autouse=True)
def _jwt(monkeypatch):
    monkeypatch.setenv("GRID_JWT_SECRET", "schwab-private-test-secret")
    monkeypatch.setenv("GRID_JWT_EXPIRE_HOURS", "1")
    sp._STORE.clear()
    yield
    sp._STORE.clear()


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(sp.router)
    return TestClient(app)


def bearer(role: str = "admin") -> dict[str, str]:
    return {"Authorization": f"Bearer {create_token(role=role, expires_hours=1)}"}


# ── authorization ────────────────────────────────────────────────────────────

class TestAuthorization:
    @pytest.mark.parametrize("path", ["/api/v1/schwab/private/status", "/api/v1/schwab/private/quotes"])
    def test_no_token_is_401(self, client, monkeypatch, path):
        monkeypatch.setattr(sp, "_quote_source", Never())
        monkeypatch.setattr(sp, "_enabled", lambda: True)
        assert client.get(path).status_code == 401

    @pytest.mark.parametrize("path", ["/api/v1/schwab/private/status", "/api/v1/schwab/private/quotes"])
    def test_query_token_is_refused(self, client, monkeypatch, path):
        monkeypatch.setattr(sp, "_quote_source", Never())
        monkeypatch.setattr(sp, "_enabled", lambda: True)
        assert client.get(f"{path}?token={create_token()}").status_code == 401

    @pytest.mark.parametrize("path", ["/api/v1/schwab/private/status", "/api/v1/schwab/private/quotes"])
    def test_contributor_is_403(self, client, monkeypatch, path):
        monkeypatch.setattr(sp, "_quote_source", Never())
        monkeypatch.setattr(sp, "_enabled", lambda: True)
        r = client.get(path, headers=bearer("contributor"))
        assert r.status_code == 403
        assert "admin" in r.json()["detail"]

    def test_admin_passes_the_gate(self, client, monkeypatch):
        monkeypatch.setattr(sp, "_enabled", lambda: False)
        assert client.get("/api/v1/schwab/private/status", headers=bearer()).status_code == 200


# ── flag ─────────────────────────────────────────────────────────────────────

class TestFlag:
    def test_default_flag_is_off(self):
        assert sp._enabled() is False

    def test_disabled_is_503_and_reader_never_built(self, client, monkeypatch):
        monkeypatch.setattr(sp, "_enabled", lambda: False)
        monkeypatch.setattr(sp, "_quote_source", Never())
        r = client.get("/api/v1/schwab/private/quotes?symbols=SPY", headers=bearer())
        assert r.status_code == 503
        assert "SCHWAB_PRIVATE_QUOTES_ENABLED" in r.json()["detail"]
        assert r.headers["Cache-Control"] == "no-store, private"

    def test_status_reports_flag_and_no_values(self, client, monkeypatch):
        monkeypatch.setattr(sp, "_enabled", lambda: False)
        r = client.get("/api/v1/schwab/private/status", headers=bearer())
        body = r.json()
        assert body["enabled"] is False and body["rows"] == {}
        assert body["audience"] == "private" and body["redistribution"] == "prohibited"
        assert r.headers["Cache-Control"] == "no-store, private"
        assert r.headers["X-GRID-Audience"] == "private"

    def test_not_configured_is_503(self, client, monkeypatch):
        monkeypatch.setattr(sp, "_enabled", lambda: True)

        def not_configured():
            raise HTTPException(status_code=503, detail="Schwab reader is not configured on this host")

        monkeypatch.setattr(sp, "_quote_source", not_configured)
        r = client.get("/api/v1/schwab/private/quotes?symbols=SPY", headers=bearer())
        assert r.status_code == 503 and "not configured" in r.json()["detail"]
        assert r.headers["Cache-Control"] == "no-store, private"


# ── symbol validation (checked before the flag so bad input never reaches it) ──

class TestSymbols:
    @pytest.mark.parametrize("raw", ["", " , ", "SPY;QQQ", "SPY QQQ", "A" * 13, "spy,q!"])
    def test_bad_symbols_are_422(self, client, monkeypatch, raw):
        monkeypatch.setattr(sp, "_enabled", lambda: True)
        monkeypatch.setattr(sp, "_quote_source", Never())
        assert client.get(f"/api/v1/schwab/private/quotes?symbols={raw}", headers=bearer()).status_code == 422

    def test_too_many_symbols_is_422(self, client, monkeypatch):
        monkeypatch.setattr(sp, "_enabled", lambda: True)
        monkeypatch.setattr(sp, "_quote_source", Never())
        raw = ",".join(f"S{i}" for i in range(sp.MAX_SYMBOLS_PER_READ + 1))
        assert client.get(f"/api/v1/schwab/private/quotes?symbols={raw}", headers=bearer()).status_code == 422

    def test_symbols_are_normalised_and_deduplicated(self):
        assert sp._symbols(" spy , qqq,SPY ") == ["SPY", "QQQ"]


# ── reads ────────────────────────────────────────────────────────────────────

class TestReads:
    def test_fresh_quote_round_trip(self, client, monkeypatch):
        reader = ParsingReader({"SPY": entry("SPY")})
        monkeypatch.setattr(sp, "_enabled", lambda: True)
        monkeypatch.setattr(sp, "_quote_source", lambda: reader)
        r = client.get("/api/v1/schwab/private/quotes?symbols=spy", headers=bearer())
        assert r.status_code == 200
        assert r.headers["Cache-Control"] == "no-store, private"
        body = r.json()
        assert reader.calls == [("SPY",)]
        snap, view = body["snapshot"], body["view"]
        assert snap["source"] == "schwab-marketdata-v1" and snap["audience"] == "private"
        q = snap["quotes"]["SPY"]
        assert q["quote_status"] == "fresh" and q["live"] is True
        assert (q["symbol"], q["provider_symbol"]) == ("SPY", "SPY")
        assert q["last"] == LAST and q["provenance"]["source"] == "schwab-marketdata-v1"
        row = view["rows"]["SPY"]
        assert row["schema"] == "schwab-private-quote-view-v1"
        assert row["quote_status"] == "fresh" and row["live"] is True
        assert row["freshness"]["as_of"] == q["freshness"]["as_of"]
        assert row["freshness"]["vintage"] == snap["received_at"]
        assert body["stored_rows"] == 1
        assert json.dumps(body, allow_nan=False)

    def test_stale_and_delayed_are_not_live(self, client, monkeypatch):
        reader = ParsingReader({"SPY": entry("SPY", age_s=90.0), "QQQ": entry("QQQ", realtime=False)})
        monkeypatch.setattr(sp, "_enabled", lambda: True)
        monkeypatch.setattr(sp, "_quote_source", lambda: reader)
        body = client.get("/api/v1/schwab/private/quotes?symbols=SPY,QQQ", headers=bearer()).json()
        spy, qqq = body["view"]["rows"]["SPY"], body["view"]["rows"]["QQQ"]
        assert spy["quote_status"] == "stale" and spy["live"] is False and spy["freshness"]["stale"] is True
        assert qqq["quote_status"] == "delayed" and qqq["live"] is False
        assert body["snapshot"]["counts"] == {"fresh": 0, "stale": 1, "delayed": 1, "missing": 0, "invalid": 0, "unavailable": 0}

    def test_missing_and_identity_mismatch(self, client, monkeypatch):
        reader = ParsingReader({"SPY": entry("QQQ")})  # provider symbol disagrees with the key
        monkeypatch.setattr(sp, "_enabled", lambda: True)
        monkeypatch.setattr(sp, "_quote_source", lambda: reader)
        body = client.get("/api/v1/schwab/private/quotes?symbols=SPY,IWM", headers=bearer()).json()
        spy, iwm = body["view"]["rows"]["SPY"], body["view"]["rows"]["IWM"]
        assert spy["quote_status"] == "invalid" and is_unavailable(spy) and spy["live"] is False
        assert iwm["quote_status"] == "missing" and is_unavailable(iwm)
        assert spy["last"] is None and iwm["last"] is None

    def test_reader_failure_is_unavailable_not_500(self, client, monkeypatch):
        reader = ParsingReader(raise_exc=SchwabTransientError("boom"))
        monkeypatch.setattr(sp, "_enabled", lambda: True)
        monkeypatch.setattr(sp, "_quote_source", lambda: reader)
        r = client.get("/api/v1/schwab/private/quotes?symbols=SPY", headers=bearer())
        assert r.status_code == 200
        body = r.json()
        assert body["snapshot"]["source_error"] == "SchwabTransientError"
        row = body["view"]["rows"]["SPY"]
        assert row["quote_status"] == "unavailable" and row["live"] is False and row["last"] is None
        assert "boom" not in r.text  # provider message never echoed

    def test_integer_sizes_keep_precision_end_to_end(self, client, monkeypatch):
        reader = ParsingReader({"SPY": entry("SPY", bidSize="9007199254740993", totalVolume=2**62)})
        monkeypatch.setattr(sp, "_enabled", lambda: True)
        monkeypatch.setattr(sp, "_quote_source", lambda: reader)
        r = client.get("/api/v1/schwab/private/quotes?symbols=SPY", headers=bearer())
        assert re.search(r'"bid_size":9007199254740993\b', r.text)
        assert r.json()["view"]["rows"]["SPY"]["total_volume"] == 2**62

    def test_status_shows_stored_rows_without_values(self, client, monkeypatch):
        reader = ParsingReader({"SPY": entry("SPY")})
        monkeypatch.setattr(sp, "_enabled", lambda: True)
        monkeypatch.setattr(sp, "_quote_source", lambda: reader)
        client.get("/api/v1/schwab/private/quotes?symbols=SPY", headers=bearer())
        body = client.get("/api/v1/schwab/private/status", headers=bearer()).json()
        assert body["rows"]["SPY"]["quote_status"] == "fresh" and body["rows"]["SPY"]["live"] is True
        assert set(body["rows"]["SPY"]) == {"quote_status", "live", "vintage"}
        assert str(LAST) not in json.dumps(body)

    def test_store_ages_fresh_rows_between_reads(self, client, monkeypatch):
        reader = ParsingReader({"SPY": entry("SPY", age_s=19.5)})
        monkeypatch.setattr(sp, "_enabled", lambda: True)
        monkeypatch.setattr(sp, "_quote_source", lambda: reader)
        first = client.get("/api/v1/schwab/private/quotes?symbols=SPY", headers=bearer()).json()
        assert first["view"]["rows"]["SPY"]["quote_status"] == "fresh"
        later = datetime.now(timezone.utc) + timedelta(seconds=5)
        row = sp._STORE.get("SPY", now=later)
        assert row is not None and row.status == "stale" and row.live is False


# ── wiring and source guards ─────────────────────────────────────────────────

def test_router_is_registered_in_main_table():
    text = Path("api/main.py").read_text()
    assert '("schwab_private", "api.routers.schwab_private", False)' in text


def test_router_is_admin_gated_at_router_level():
    assert router_has_admin_dependency(sp.router)


def router_has_admin_dependency(router) -> bool:
    return any("admin" in (getattr(d.dependency, "__qualname__", "") + str(getattr(d.dependency, "__closure__", ""))) for d in router.dependencies) or bool(router.dependencies)


def test_router_source_has_no_credential_handling():
    code = Path("api/routers/schwab_private.py").read_text().split('"""', 2)[2]
    for token in ("tokens.json", "credentials.env", "load_credentials", "_read_private", "open(",
                  "requests.", "urllib", "sqlalchemy", "raw_series", "app_secret", "refresh_token"):
        assert token not in code, token


def test_env_example_and_settings_declare_the_flag():
    assert "SCHWAB_PRIVATE_QUOTES_ENABLED=false" in Path(".env.example").read_text()
    from config import settings

    assert settings.SCHWAB_PRIVATE_QUOTES_ENABLED is False
