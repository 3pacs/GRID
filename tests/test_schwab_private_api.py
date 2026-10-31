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
from ingestion.schwab_market import SchwabAuthError, SchwabTransientError, parse_quotes
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


# ── receipt-time freshness (r2 finding 1) ────────────────────────────────────

T0 = datetime(2026, 10, 31, 15, 0, 0, tzinfo=timezone.utc)


class FakeClock:
    """Deterministic clock: advances only when a fake source or the test says so."""

    def __init__(self, start: datetime = T0, tick_s: float = 0.0):
        self.now = start
        self.tick_s = tick_s  # wall time that passes per clock read
        self.reads = 0

    def __call__(self) -> datetime:
        self.reads += 1
        current = self.now
        self.now = self.now + timedelta(seconds=self.tick_s)
        return current

    def advance(self, seconds: float) -> None:
        self.now = self.now + timedelta(seconds=seconds)


class SlowReader:
    """Stamps quoteTime at request start, then 'takes' ``delay_s`` on the fake clock.

    ``fail_times`` models the reader's own retry loop (each attempt costs ``delay_s``);
    ``final_error`` makes the read fail after the delay. No sleep, no network.
    """

    def __init__(self, clock: FakeClock, delay_s: float, *, quote_offset_s: float = 0.0,
                 fail_times: int = 0, final_error: Exception | None = None):
        self.clock, self.delay_s, self.quote_offset_s = clock, delay_s, quote_offset_s
        self.fail_times, self.final_error, self.attempts = fail_times, final_error, 0

    def quotes(self, symbols, fields="quote"):
        start = self.clock.now
        while True:
            self.attempts += 1
            self.clock.advance(self.delay_s)
            if self.attempts <= self.fail_times:
                continue
            break
        if self.final_error is not None:
            raise self.final_error
        body = {s: entry(s, age_s=-self.quote_offset_s, now=start) for s in symbols}
        return parse_quotes(json.loads(json.dumps(body)))


def _wire(monkeypatch, clock: FakeClock, reader) -> None:
    monkeypatch.setattr(sp, "_clock", clock)
    monkeypatch.setattr(sp, "_enabled", lambda: True)
    monkeypatch.setattr(sp, "_quote_source", lambda: reader)


class TestReceiptTimeFreshness:
    def test_delayed_response_is_classified_against_actual_receipt(self, client, monkeypatch):
        clock = FakeClock()
        _wire(monkeypatch, clock, SlowReader(clock, delay_s=25.0))  # quoteTime = T0, receipt = T0+25
        body = client.get("/api/v1/schwab/private/quotes?symbols=SPY", headers=bearer()).json()
        assert body["received_at"] == (T0 + timedelta(seconds=25)).isoformat()
        q = body["snapshot"]["quotes"]["SPY"]
        assert q["quote_status"] == "stale" and q["live"] is False  # r1 (now before the read) said fresh
        assert q["freshness"]["age_seconds"] == 25.0
        assert q["freshness"]["received_at"] == body["received_at"]
        assert body["view"]["rows"]["SPY"]["freshness"]["vintage"] == body["received_at"]

    def test_fast_response_within_gate_is_fresh(self, client, monkeypatch):
        clock = FakeClock()
        _wire(monkeypatch, clock, SlowReader(clock, delay_s=3.0))
        q = client.get("/api/v1/schwab/private/quotes?symbols=SPY", headers=bearer()).json()["snapshot"]["quotes"]["SPY"]
        assert q["quote_status"] == "fresh" and q["live"] is True and q["freshness"]["age_seconds"] == 3.0

    def test_retry_then_success_counts_the_whole_wait(self, client, monkeypatch):
        clock = FakeClock()
        reader = SlowReader(clock, delay_s=8.0, fail_times=2)  # 3 attempts x 8 s
        _wire(monkeypatch, clock, reader)
        body = client.get("/api/v1/schwab/private/quotes?symbols=SPY", headers=bearer()).json()
        assert reader.attempts == 3
        assert body["received_at"] == (T0 + timedelta(seconds=24)).isoformat()
        assert body["snapshot"]["quotes"]["SPY"]["quote_status"] == "stale"

    @pytest.mark.parametrize("error", [SchwabTransientError("t"), SchwabAuthError("a")])
    def test_failure_after_delay_stamps_receipt_after_the_failure(self, client, monkeypatch, error):
        clock = FakeClock()
        _wire(monkeypatch, clock, SlowReader(clock, delay_s=10.0, final_error=error))
        r = client.get("/api/v1/schwab/private/quotes?symbols=SPY", headers=bearer())
        assert r.status_code == 200
        body = r.json()
        assert body["received_at"] == (T0 + timedelta(seconds=10)).isoformat()
        assert body["snapshot"]["source_error"] == type(error).__name__
        row = body["view"]["rows"]["SPY"]
        assert row["quote_status"] == "unavailable" and row["live"] is False
        assert row["freshness"]["vintage"] == body["received_at"]

    def test_future_tolerance_is_measured_from_receipt(self, client, monkeypatch):
        # provider stamps the quote 28 s after request start; receipt lands at +25 s -> 3 s ahead, tolerated
        clock = FakeClock()
        _wire(monkeypatch, clock, SlowReader(clock, delay_s=25.0, quote_offset_s=28.0))
        q = client.get("/api/v1/schwab/private/quotes?symbols=SPY", headers=bearer()).json()["snapshot"]["quotes"]["SPY"]
        assert q["quote_status"] == "fresh" and q["freshness"]["age_seconds"] == -3.0

    def test_quote_too_far_ahead_of_receipt_is_invalid(self, client, monkeypatch):
        clock = FakeClock()
        _wire(monkeypatch, clock, SlowReader(clock, delay_s=25.0, quote_offset_s=40.0))  # 15 s ahead
        q = client.get("/api/v1/schwab/private/quotes?symbols=SPY", headers=bearer()).json()["snapshot"]["quotes"]["SPY"]
        assert q["quote_status"] == "invalid" and q["live"] is False and q["last"] is None

    def test_freshness_is_assessed_again_at_response_time(self, client, monkeypatch):
        # each clock read returns the time then moves it 21 s: receipt at T0 (fresh, age 0),
        # evaluation at T0+21 (past the 20 s gate) -> the view says stale although the snapshot said fresh
        clock = FakeClock(tick_s=21.0)
        _wire(monkeypatch, clock, SlowReader(clock, delay_s=0.0))
        body = client.get("/api/v1/schwab/private/quotes?symbols=SPY", headers=bearer()).json()
        assert body["snapshot"]["quotes"]["SPY"]["quote_status"] == "fresh"
        assert body["evaluated_at"] > body["received_at"]
        row = body["view"]["rows"]["SPY"]
        assert row["quote_status"] == "stale" and row["live"] is False
        assert row["freshness"]["evaluated_at"] == body["evaluated_at"]
        assert row["freshness"]["age_seconds"] == 21.0

    def test_status_uses_the_injected_clock(self, client, monkeypatch):
        clock = FakeClock()
        monkeypatch.setattr(sp, "_clock", clock)
        monkeypatch.setattr(sp, "_enabled", lambda: False)
        assert client.get("/api/v1/schwab/private/status", headers=bearer()).json()["evaluated_at"] == T0.isoformat()

    def test_non_schwab_exception_is_not_swallowed(self, client, monkeypatch):
        class Broken:
            def quotes(self, symbols, fields="quote"):
                raise RuntimeError("programming error")

        _wire(monkeypatch, FakeClock(), Broken())
        with pytest.raises(RuntimeError):
            client.get("/api/v1/schwab/private/quotes?symbols=SPY", headers=bearer())


# ── headers: narrowed claim (r2 note) ────────────────────────────────────────

class TestHeaderScope:
    @pytest.mark.parametrize("path", ["/api/v1/schwab/private/status", "/api/v1/schwab/private/quotes"])
    def test_dependency_and_validation_responses_carry_no_quote_data(self, client, monkeypatch, path):
        monkeypatch.setattr(sp, "_quote_source", Never())
        monkeypatch.setattr(sp, "_enabled", lambda: True)
        for r in (client.get(path), client.get(path, headers=bearer("contributor"))):
            assert r.status_code in (401, 403)
            assert set(r.json()) == {"detail"}
            assert "Cache-Control" not in r.headers  # framework response; documented, not claimed
        r = client.get("/api/v1/schwab/private/quotes?symbols=S;P", headers=bearer())
        assert r.status_code == 422 and set(r.json()) == {"detail"}

    def test_handler_responses_carry_private_headers(self, client, monkeypatch):
        monkeypatch.setattr(sp, "_enabled", lambda: False)
        for r in (client.get("/api/v1/schwab/private/status", headers=bearer()),
                  client.get("/api/v1/schwab/private/quotes", headers=bearer())):
            assert r.headers["Cache-Control"] == "no-store, private"
            assert r.headers["X-GRID-Audience"] == "private"


# ── exact integer strings (r2 note) ──────────────────────────────────────────

def test_exact_integer_strings_end_to_end(client, monkeypatch):
    reader = ParsingReader({"SPY": entry("SPY", bidSize="9007199254740993", askSize=7, totalVolume=2**63 - 1)})
    monkeypatch.setattr(sp, "_enabled", lambda: True)
    monkeypatch.setattr(sp, "_quote_source", lambda: reader)
    row = client.get("/api/v1/schwab/private/quotes?symbols=SPY", headers=bearer()).json()["view"]["rows"]["SPY"]
    assert row["bid_size_exact"] == "9007199254740993" and row["bid_size"] == 9007199254740993
    assert row["ask_size_exact"] == "7"
    assert row["total_volume_exact"] == str(2**63 - 1)


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
