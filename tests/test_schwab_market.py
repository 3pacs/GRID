"""Tests for ``ingestion/schwab_market.py`` — read-only Schwab market-data client.

All HTTP is faked through an injected session object; nothing here touches
the network, the operator's real token file or ``~/.config/grid/schwab``.
"""

from __future__ import annotations

import inspect
import json
import os
import stat
from pathlib import Path
from typing import Any

import pytest
import requests
from loguru import logger

from ingestion import schwab_market as sm
from ingestion.schwab_market import (
    MARKET_BASE,
    READ_ONLY_PATHS,
    TOKEN_URL,
    SchwabAuthError,
    SchwabConfigError,
    SchwabError,
    SchwabMarketClient,
    SchwabQuote,
    SchwabTransientError,
    load_credentials,
    parse_quotes,
)

FAKE_KEY = "fake-app-key-ABC123"
FAKE_SECRET = "fake-app-secret-XYZ789"
FAKE_ACCESS = "fake-access-token-aaaaaaaa"
FAKE_REFRESH = "fake-refresh-token-rrrrrrrr"
NOW = 1_800_000_000.0

SPY_BODY = {
    "SPY": {
        "assetMainType": "EQUITY",
        "assetSubType": "ETF",
        "quoteType": "NBBO",
        "realtime": True,
        "ssid": 1281357,
        "symbol": "SPY",
        "quote": {
            "askPrice": 601.23,
            "askSize": 5,
            "bidPrice": 601.21,
            "bidSize": 7,
            "lastPrice": 601.22,
            "totalVolume": 41234567,
            "quoteTime": 1761762000123,
            "tradeTime": 1761762000100,
        },
    }
}


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeResponse:
    def __init__(self, status: int, body: Any = None, headers: dict | None = None) -> None:
        self.status_code = status
        self._body = body if body is not None else {}
        self.headers = headers or {}
        self.text = json.dumps(self._body)

    def json(self) -> Any:
        return self._body


class FakeSession:
    """Records every call; serves queued responses per (method, url)."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.get_queue: list[Any] = []
        self.post_queue: list[Any] = []

    def _next(self, queue: list[Any]) -> FakeResponse:
        if not queue:
            raise AssertionError("no fake response queued")
        item = queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def get(self, url: str, params=None, headers=None, timeout=None) -> FakeResponse:
        self.calls.append({"method": "GET", "url": url, "params": dict(params or {}),
                           "headers": dict(headers or {}), "timeout": timeout})
        return self._next(self.get_queue)

    def post(self, url: str, data=None, auth=None, headers=None, timeout=None) -> FakeResponse:
        self.calls.append({"method": "POST", "url": url, "data": dict(data or {}),
                           "auth": auth, "headers": dict(headers or {}), "timeout": timeout})
        return self._next(self.post_queue)


def write_token_file(path: Path, *, access_expires_at: float, refresh_expires_at: float,
                     mode: int = 0o600, refresh: str | None = FAKE_REFRESH) -> None:
    payload = {
        "access_token": FAKE_ACCESS,
        "token_type": "Bearer",
        "expires_in": 1800,
        "scope": "api",
        "obtained_at": int(NOW) - 100,
        "access_expires_at": access_expires_at,
        "refresh_expires_at": refresh_expires_at,
    }
    if refresh is not None:
        payload["refresh_token"] = refresh
    path.write_text(json.dumps(payload))
    os.chmod(path, mode)


@pytest.fixture
def token_file(tmp_path: Path) -> Path:
    p = tmp_path / "tokens.json"
    write_token_file(p, access_expires_at=NOW + 1500, refresh_expires_at=NOW + 5 * 86400)
    return p


@pytest.fixture
def session() -> FakeSession:
    return FakeSession()


@pytest.fixture
def sleeps() -> list[float]:
    return []


@pytest.fixture
def client(token_file: Path, session: FakeSession, sleeps: list[float]) -> SchwabMarketClient:
    return SchwabMarketClient(
        app_key=FAKE_KEY, app_secret=FAKE_SECRET, token_file=token_file,
        session=session, sleep=sleeps.append, now=lambda: NOW,
        backoff_base=0.5, backoff_cap=4.0, max_retries=3,
    )


@pytest.fixture
def log_lines() -> list[str]:
    lines: list[str] = []
    hid = logger.add(lambda m: lines.append(str(m)), level="DEBUG", format="{message}")
    yield lines
    logger.remove(hid)


# ---------------------------------------------------------------------------
# Credentials / config
# ---------------------------------------------------------------------------

def test_explicit_credentials_win(tmp_path: Path) -> None:
    assert load_credentials(FAKE_KEY, FAKE_SECRET, tmp_path / "nope.env") == (FAKE_KEY, FAKE_SECRET)


def test_credentials_fallback_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sm.settings, "SCHWAB_APP_KEY", "")
    monkeypatch.setattr(sm.settings, "SCHWAB_APP_SECRET", "")
    cred = tmp_path / "credentials.env"
    cred.write_text(
        "# comment\n"
        f"SCHWAB_APP_KEY={FAKE_KEY}\n"
        f'SCHWAB_APP_SECRET="{FAKE_SECRET}"\n'
        "SCHWAB_CALLBACK_URL=https://127.0.0.1\n"
    )
    os.chmod(cred, 0o600)
    assert load_credentials(credentials_file=cred) == (FAKE_KEY, FAKE_SECRET)


def test_credentials_fallback_file_refuses_world_readable(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(sm.settings, "SCHWAB_APP_KEY", "")
    monkeypatch.setattr(sm.settings, "SCHWAB_APP_SECRET", "")
    cred = tmp_path / "credentials.env"
    cred.write_text(f"SCHWAB_APP_KEY={FAKE_KEY}\nSCHWAB_APP_SECRET={FAKE_SECRET}\n")
    os.chmod(cred, 0o644)
    with pytest.raises(SchwabConfigError, match="not private"):
        load_credentials(credentials_file=cred)


def test_missing_credentials_is_config_error_not_crash(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(sm.settings, "SCHWAB_APP_KEY", "")
    monkeypatch.setattr(sm.settings, "SCHWAB_APP_SECRET", "")
    with pytest.raises(SchwabConfigError):
        load_credentials(credentials_file=tmp_path / "absent.env")


def test_is_configured_warns_not_raises(tmp_path: Path, monkeypatch, log_lines) -> None:
    monkeypatch.setattr(sm.settings, "SCHWAB_APP_KEY", "")
    monkeypatch.setattr(sm.settings, "SCHWAB_APP_SECRET", "")
    monkeypatch.setattr(sm.settings, "SCHWAB_CREDENTIALS_FILE", str(tmp_path / "absent.env"))
    monkeypatch.setattr(sm.settings, "SCHWAB_TOKEN_FILE", str(tmp_path / "absent.json"))
    assert sm.is_configured() is False
    assert any("Schwab market data disabled" in line for line in log_lines)


def test_settings_fields_default_empty() -> None:
    from config import Settings
    fields = Settings.model_fields
    assert fields["SCHWAB_APP_KEY"].default == ""
    assert fields["SCHWAB_APP_SECRET"].default == ""
    assert fields["SCHWAB_TOKEN_FILE"].default == "~/.config/grid/schwab/tokens.json"


# ---------------------------------------------------------------------------
# Token file handling
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("mode", [0o644, 0o640, 0o604, 0o660])
def test_token_file_refused_unless_0600(tmp_path: Path, session: FakeSession, mode: int) -> None:
    tf = tmp_path / "tokens.json"
    write_token_file(tf, access_expires_at=NOW + 1500, refresh_expires_at=NOW + 86400, mode=mode)
    client = SchwabMarketClient(app_key=FAKE_KEY, app_secret=FAKE_SECRET, token_file=tf,
                                session=session, now=lambda: NOW)
    with pytest.raises(SchwabConfigError, match="not private"):
        client.quotes("SPY")
    assert session.calls == []  # refused before any HTTP


def test_token_file_0600_accepted(client: SchwabMarketClient, session: FakeSession) -> None:
    session.get_queue.append(FakeResponse(200, SPY_BODY))
    assert "SPY" in client.quotes("SPY")


def test_missing_token_file(tmp_path: Path, session: FakeSession) -> None:
    client = SchwabMarketClient(app_key=FAKE_KEY, app_secret=FAKE_SECRET,
                                token_file=tmp_path / "none.json", session=session)
    with pytest.raises(SchwabConfigError, match="missing"):
        client.quotes("SPY")


# ---------------------------------------------------------------------------
# Refresh path
# ---------------------------------------------------------------------------

def test_refresh_when_access_token_expiring(tmp_path: Path, session: FakeSession) -> None:
    tf = tmp_path / "tokens.json"
    write_token_file(tf, access_expires_at=NOW + 30, refresh_expires_at=NOW + 5 * 86400)
    client = SchwabMarketClient(app_key=FAKE_KEY, app_secret=FAKE_SECRET, token_file=tf,
                                session=session, now=lambda: NOW)
    session.post_queue.append(FakeResponse(200, {
        "access_token": "fake-new-access-bbbbbbbb", "token_type": "Bearer",
        "expires_in": 1800, "scope": "api", "refresh_token": "fake-new-refresh-ssssssss",
    }))
    session.get_queue.append(FakeResponse(200, SPY_BODY))

    client.quotes("SPY")

    post = session.calls[0]
    assert post["method"] == "POST" and post["url"] == TOKEN_URL
    assert post["data"] == {"grant_type": "refresh_token", "refresh_token": FAKE_REFRESH}
    assert isinstance(post["auth"], requests.auth.HTTPBasicAuth)
    assert (post["auth"].username, post["auth"].password) == (FAKE_KEY, FAKE_SECRET)
    assert post["timeout"] is not None

    get = session.calls[1]
    assert get["method"] == "GET"
    assert get["headers"]["Authorization"] == "Bearer fake-new-access-bbbbbbbb"

    # token file rewritten atomically, still 0600, no leftover tmp
    assert stat.S_IMODE(tf.stat().st_mode) == 0o600
    saved = json.loads(tf.read_text())
    assert saved["access_token"] == "fake-new-access-bbbbbbbb"
    assert saved["refresh_token"] == "fake-new-refresh-ssssssss"
    assert saved["access_expires_at"] == int(NOW) + 1800
    assert saved["refresh_expires_at"] == int(NOW) + 7 * 86400
    assert not list(tmp_path.glob("*.tmp"))


def test_refresh_keeps_old_refresh_token_when_not_returned(tmp_path: Path, session) -> None:
    tf = tmp_path / "tokens.json"
    write_token_file(tf, access_expires_at=NOW - 10, refresh_expires_at=NOW + 86400)
    client = SchwabMarketClient(app_key=FAKE_KEY, app_secret=FAKE_SECRET, token_file=tf,
                                session=session, now=lambda: NOW)
    session.post_queue.append(FakeResponse(200, {"access_token": "fake-new", "expires_in": 1800}))
    client.refresh()
    saved = json.loads(tf.read_text())
    assert saved["refresh_token"] == FAKE_REFRESH
    assert saved["refresh_expires_at"] == NOW + 86400


def test_no_refresh_when_access_token_fresh(client: SchwabMarketClient, session) -> None:
    session.get_queue.append(FakeResponse(200, SPY_BODY))
    client.quotes("SPY")
    assert [c["method"] for c in session.calls] == ["GET"]


def test_expired_refresh_token_requires_login(tmp_path: Path, session) -> None:
    tf = tmp_path / "tokens.json"
    write_token_file(tf, access_expires_at=NOW - 10, refresh_expires_at=NOW - 1)
    client = SchwabMarketClient(app_key=FAKE_KEY, app_secret=FAKE_SECRET, token_file=tf,
                                session=session, now=lambda: NOW)
    with pytest.raises(SchwabAuthError, match="login helper"):
        client.quotes("SPY")
    assert session.calls == []


def test_refresh_rejected_401_is_auth_error(tmp_path: Path, session) -> None:
    tf = tmp_path / "tokens.json"
    write_token_file(tf, access_expires_at=NOW - 10, refresh_expires_at=NOW + 86400)
    client = SchwabMarketClient(app_key=FAKE_KEY, app_secret=FAKE_SECRET, token_file=tf,
                                session=session, now=lambda: NOW)
    session.post_queue.append(FakeResponse(401, {"error": "invalid_grant"}))
    with pytest.raises(SchwabAuthError):
        client.quotes("SPY")
    assert json.loads(tf.read_text())["access_token"] == FAKE_ACCESS  # untouched


def test_401_on_get_triggers_single_reauth(client: SchwabMarketClient, session) -> None:
    session.get_queue += [FakeResponse(401), FakeResponse(401)]
    session.post_queue.append(FakeResponse(200, {"access_token": "fake-new", "expires_in": 1800}))
    with pytest.raises(SchwabError, match="HTTP 401"):
        client.quotes("SPY")
    assert [c["method"] for c in session.calls] == ["GET", "POST", "GET"]


# ---------------------------------------------------------------------------
# Backoff
# ---------------------------------------------------------------------------

def test_429_backoff_then_success(client: SchwabMarketClient, session, sleeps) -> None:
    session.get_queue += [
        FakeResponse(429, headers={"Retry-After": "2"}),
        FakeResponse(429),
        FakeResponse(200, SPY_BODY),
    ]
    out = client.quotes("SPY")
    assert out["SPY"].last_price == 601.22
    assert sleeps == [2.0, 1.0]  # Retry-After honoured, then base*2**1
    assert sum(1 for c in session.calls if c["method"] == "GET") == 3


def test_5xx_backoff_exhausted(client: SchwabMarketClient, session, sleeps, log_lines) -> None:
    session.get_queue += [FakeResponse(503)] * 4
    with pytest.raises(SchwabTransientError, match="HTTP 503"):
        client.quotes("SPY")
    assert sleeps == [0.5, 1.0, 2.0]  # 3 retries, capped exponential
    assert all("| ERROR" not in line for line in log_lines)
    assert sum("WARNING" in line or "retry" in line for line in log_lines) >= 3


def test_network_error_retried(client: SchwabMarketClient, session, sleeps) -> None:
    session.get_queue += [requests.ConnectionError("boom"), FakeResponse(200, SPY_BODY)]
    assert "SPY" in client.quotes("SPY")
    assert sleeps == [0.5]


def test_4xx_not_retried(client: SchwabMarketClient, session, sleeps) -> None:
    session.get_queue.append(FakeResponse(400, {"errors": ["bad"]}))
    with pytest.raises(SchwabError, match="HTTP 400"):
        client.quotes("SPY")
    assert sleeps == []


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def test_parse_quotes_spy_shape() -> None:
    out = parse_quotes(SPY_BODY)
    q = out["SPY"]
    assert isinstance(q, SchwabQuote)
    assert q.symbol == "SPY"
    assert q.asset_main_type == "EQUITY"
    assert q.realtime is True
    assert (q.last_price, q.bid_price, q.ask_price) == (601.22, 601.21, 601.23)
    assert (q.bid_size, q.ask_size, q.total_volume) == (7, 5, 41234567)
    assert q.quote_time_ms == 1761762000123 and q.trade_time_ms == 1761762000100
    assert q.raw is SPY_BODY["SPY"]
    import dataclasses
    with pytest.raises(dataclasses.FrozenInstanceError):
        q.last_price = 1.0  # type: ignore[misc]


def test_parse_quotes_skips_errors_and_missing_quote(log_lines) -> None:
    body = {
        "SPY": SPY_BODY["SPY"],
        "errors": {"invalidSymbols": ["NOPE"]},
        "QQQ": {"assetMainType": "EQUITY", "realtime": True},  # no quote map
    }
    out = parse_quotes(body)
    assert set(out) == {"SPY", "QQQ"}
    assert out["QQQ"].last_price is None
    assert any("errors for some symbols" in line for line in log_lines)
    assert parse_quotes({}) == {} and parse_quotes(None) == {}  # type: ignore[arg-type]


def test_quotes_params_and_symbol_normalisation(client: SchwabMarketClient, session) -> None:
    session.get_queue.append(FakeResponse(200, SPY_BODY))
    client.quotes(["spy", " qqq "])
    call = session.calls[-1]
    assert call["url"] == MARKET_BASE + "/quotes"
    assert call["params"] == {"symbols": "SPY,QQQ", "fields": "quote"}
    assert call["timeout"] == sm.DEFAULT_TIMEOUT
    assert client.quotes([]) == {}


# ---------------------------------------------------------------------------
# Read-only surface
# ---------------------------------------------------------------------------

EXPECTED_PUBLIC = {"quotes", "option_chain", "expiration_chain", "price_history",
                   "market_hours", "refresh", "token_status"}


def test_public_surface_is_exactly_the_read_only_methods() -> None:
    public = {n for n, _ in inspect.getmembers(SchwabMarketClient, inspect.isfunction)
              if not n.startswith("_")}
    assert public == EXPECTED_PUBLIC


def test_every_data_method_is_get_on_a_market_data_path(client: SchwabMarketClient, session) -> None:
    session.get_queue += [FakeResponse(200, {}) for _ in range(5)]
    client.quotes("SPY")
    client.option_chain("SPY", contractType="ALL", strikeCount=10)
    client.expiration_chain("SPY")
    client.price_history("SPY", periodType="day", period=5)
    client.market_hours(["EQUITY", "option"], date="2026-10-30")

    assert len(session.calls) == 5
    paths = []
    for call in session.calls:
        assert call["method"] == "GET"
        assert call["url"].startswith(MARKET_BASE + "/")
        paths.append(call["url"][len(MARKET_BASE):])
    assert paths == ["/quotes", "/chains", "/expirationchain", "/pricehistory", "/markets"]
    assert set(paths) == set(READ_ONLY_PATHS)
    assert session.calls[1]["params"] == {"symbol": "SPY", "contractType": "ALL", "strikeCount": 10}
    assert session.calls[3]["params"] == {"symbol": "SPY", "periodType": "day", "period": 5}
    assert session.calls[4]["params"] == {"markets": "equity,option", "date": "2026-10-30"}


def test_internal_get_refuses_non_market_paths(client: SchwabMarketClient, session) -> None:
    for bad in ("/accounts", "/orders", "/trader/v1/accounts", "/quotes/../accounts"):
        with pytest.raises(SchwabError, match="not a read-only"):
            client._get(bad)
    assert session.calls == []


def test_only_post_in_module_is_the_token_endpoint() -> None:
    src = inspect.getsource(sm)
    assert src.count("_session.post(") == 1
    assert "/trader/" not in src and "accounts" not in src.lower().replace("account-side", "")
    for verb in ("put(", "delete(", "patch("):
        assert f"_session.{verb}" not in src


# ---------------------------------------------------------------------------
# Secrecy
# ---------------------------------------------------------------------------

def test_no_secret_ever_logged_or_in_exceptions(tmp_path: Path, session, log_lines) -> None:
    tf = tmp_path / "tokens.json"
    write_token_file(tf, access_expires_at=NOW - 10, refresh_expires_at=NOW + 86400)
    client = SchwabMarketClient(app_key=FAKE_KEY, app_secret=FAKE_SECRET, token_file=tf,
                                session=session, now=lambda: NOW, sleep=lambda s: None,
                                max_retries=1)
    session.post_queue.append(FakeResponse(200, {"access_token": "fake-new-access-cccc",
                                                 "expires_in": 1800}))
    session.get_queue += [FakeResponse(429), FakeResponse(503)]
    messages: list[str] = []
    try:
        client.quotes("SPY")
    except SchwabError as exc:
        messages.append(str(exc))
    session.post_queue.append(FakeResponse(401, {"error": "invalid_client"}))
    try:
        client.refresh()
    except SchwabError as exc:
        messages.append(str(exc))

    blob = "\n".join(log_lines + messages)
    for secret in (FAKE_KEY, FAKE_SECRET, FAKE_ACCESS, FAKE_REFRESH, "fake-new-access-cccc"):
        assert secret not in blob
    assert "Authorization" not in blob and "Bearer" not in blob
