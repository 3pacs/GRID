"""Tests for ``ingestion/schwab_market.py`` — read-only Schwab market-data client.

All HTTP is faked through an injected session object; nothing here touches
the network, the operator's real token file or ``~/.config/grid/schwab``.
"""

from __future__ import annotations

import fcntl
import inspect
import json
import os
import stat
import threading
import traceback
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
    assert any("returned errors: invalidSymbols=1" in line for line in log_lines)
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


# ---------------------------------------------------------------------------
# Independent review of ecb23e66 (PR #931) — adversarial controls.
# Every case uses disposable files under tmp_path and fabricated markers;
# the operator's real token path is never touched.
# ---------------------------------------------------------------------------

MARKER = "FAKE-SECRET-MARKER-9f3a1c"


def _tb(exc: BaseException) -> str:
    return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))


def _client_needing_refresh(tmp_path: Path, session: FakeSession, **kw) -> tuple[SchwabMarketClient, Path]:
    tf = tmp_path / "tokens.json"
    write_token_file(tf, access_expires_at=NOW - 10, refresh_expires_at=NOW + 86400)
    kw.setdefault("now", lambda: NOW)
    kw.setdefault("sleep", lambda s: None)
    client = SchwabMarketClient(app_key=FAKE_KEY, app_secret=FAKE_SECRET, token_file=tf,
                                session=session, **kw)
    return client, tf


def _refresh_ok(session: FakeSession, access: str = "fake-new-access", **extra) -> None:
    body = {"access_token": access, "expires_in": 1800}
    body.update(extra)
    session.post_queue.append(FakeResponse(200, body))


# -- P1: temp-file write -------------------------------------------------------

@pytest.mark.parametrize("link", ["hard", "sym"])
def test_p1_preplaced_link_at_old_tmp_path_is_never_written(tmp_path: Path, session, link) -> None:
    client, tf = _client_needing_refresh(tmp_path, session)
    dummy = tmp_path / "victim.txt"
    dummy.write_text("untouched\n")
    os.chmod(dummy, 0o644)
    old_tmp = tf.with_name(tf.name + ".tmp")  # the deterministic path ecb23e66 used
    if link == "hard":
        os.link(dummy, old_tmp)
    else:
        os.symlink(dummy, old_tmp)
    _refresh_ok(session, "fake-new-access-" + MARKER)

    client.refresh()

    assert dummy.read_text() == "untouched\n"
    assert stat.S_IMODE(dummy.stat().st_mode) == 0o644
    assert MARKER not in old_tmp.read_text()  # link target untouched
    assert not tf.is_symlink() and tf.is_file()
    assert stat.S_IMODE(tf.stat().st_mode) == 0o600
    assert json.loads(tf.read_text())["access_token"] == "fake-new-access-" + MARKER
    leftovers = [p.name for p in tmp_path.iterdir()
                 if p.name.startswith(".tokens-") or (p.suffix == ".tmp" and p != old_tmp)]
    assert leftovers == []


def test_p1_temp_file_is_unique_exclusive_and_0600(tmp_path: Path, session, monkeypatch) -> None:
    """The temp file is created by mkstemp (O_EXCL, random name, 0600) and fsynced."""
    client, _tf = _client_needing_refresh(tmp_path, session)
    seen: dict[str, Any] = {}
    real_mkstemp = sm.tempfile.mkstemp

    def spy(*a, **k):
        fd, name = real_mkstemp(*a, **k)
        seen["name"] = name
        seen["mode"] = stat.S_IMODE(os.fstat(fd).st_mode)
        seen["dir"] = k.get("dir")
        return fd, name

    monkeypatch.setattr(sm.tempfile, "mkstemp", spy)
    fsyncs: list[int] = []
    monkeypatch.setattr(sm.os, "fsync", lambda fd: fsyncs.append(fd))
    _refresh_ok(session)
    client.refresh()
    assert Path(seen["dir"]) == tmp_path
    assert Path(seen["name"]).parent == tmp_path and Path(seen["name"]).name.startswith(".tokens-")
    assert seen["mode"] == 0o600
    assert not Path(seen["name"]).exists()  # renamed over the destination
    assert len(fsyncs) >= 1


def test_p1_refuses_to_replace_symlinked_destination(tmp_path: Path, session) -> None:
    real = tmp_path / "real-tokens.json"
    write_token_file(real, access_expires_at=NOW - 10, refresh_expires_at=NOW + 86400)
    link = tmp_path / "tokens.json"
    os.symlink(real, link)
    client = SchwabMarketClient(app_key=FAKE_KEY, app_secret=FAKE_SECRET, token_file=link,
                                session=session, now=lambda: NOW)
    client._tokens = json.loads(real.read_text())  # bypass the read guard to reach the writer
    with pytest.raises(SchwabConfigError, match="not a regular file"):
        client._write_tokens({"access_token": "x"})
    assert json.loads(real.read_text())["access_token"] == FAKE_ACCESS
    assert link.is_symlink()


def test_p1_refuses_group_or_other_writable_directory(tmp_path: Path, session) -> None:
    d = tmp_path / "shared"
    d.mkdir(mode=0o700)
    tf = d / "tokens.json"
    write_token_file(tf, access_expires_at=NOW - 10, refresh_expires_at=NOW + 86400)
    client = SchwabMarketClient(app_key=FAKE_KEY, app_secret=FAKE_SECRET, token_file=tf,
                                session=session, now=lambda: NOW)
    os.chmod(d, 0o770)
    try:
        with pytest.raises(SchwabConfigError, match="group/other writable"):
            client.quotes("SPY")
    finally:
        os.chmod(d, 0o700)
    assert session.calls == []


# -- P2: fd-bound private reads -----------------------------------------------

def test_p2_symlink_to_private_token_file_is_refused(tmp_path: Path, session) -> None:
    real = tmp_path / "real.json"
    write_token_file(real, access_expires_at=NOW + 1500, refresh_expires_at=NOW + 86400)
    link = tmp_path / "tokens.json"
    os.symlink(real, link)
    client = SchwabMarketClient(app_key=FAKE_KEY, app_secret=FAKE_SECRET, token_file=link,
                                session=session, now=lambda: NOW)
    with pytest.raises(SchwabConfigError, match="symbolic link"):
        client.quotes("SPY")
    assert session.calls == []


def test_p2_symlink_to_private_credentials_file_is_refused(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(sm.settings, "SCHWAB_APP_KEY", "")
    monkeypatch.setattr(sm.settings, "SCHWAB_APP_SECRET", "")
    real = tmp_path / "real.env"
    real.write_text(f"SCHWAB_APP_KEY={FAKE_KEY}\nSCHWAB_APP_SECRET={FAKE_SECRET}\n")
    os.chmod(real, 0o600)
    link = tmp_path / "credentials.env"
    os.symlink(real, link)
    with pytest.raises(SchwabConfigError, match="symbolic link"):
        load_credentials(credentials_file=link)


def test_p2_non_regular_file_is_refused(tmp_path: Path, session) -> None:
    fifo = tmp_path / "tokens.json"
    os.mkfifo(fifo, 0o600)
    client = SchwabMarketClient(app_key=FAKE_KEY, app_secret=FAKE_SECRET, token_file=fifo,
                                session=session, now=lambda: NOW)
    with pytest.raises(SchwabConfigError, match="not a regular file"):
        client.quotes("SPY")


@pytest.mark.skipif(not hasattr(os, "geteuid") or os.geteuid() == 0, reason="needs non-root POSIX")
def test_p2_foreign_owner_is_refused_via_fstat(tmp_path: Path, session, monkeypatch) -> None:
    """Ownership cannot be changed without root, so the uid check is driven
    through the fstat seam with the real file otherwise untouched."""
    client, _tf = _client_needing_refresh(tmp_path, session)
    monkeypatch.setattr(sm, "_expected_uid", lambda: os.geteuid() + 1)
    with pytest.raises(SchwabConfigError, match="owned by uid"):
        client.quotes("SPY")
    assert session.calls == []


def test_p2_read_uses_validated_descriptor_not_a_second_open(client, session, monkeypatch) -> None:
    """If the read went through Path.read_text()/Path.stat() a swap between
    check and read would be possible; forbid both and prove the read still works."""
    def boom(*a, **k):
        raise AssertionError("path re-opened after validation")
    monkeypatch.setattr(Path, "read_text", boom)
    monkeypatch.setattr(Path, "stat", boom)
    session.get_queue.append(FakeResponse(200, SPY_BODY))
    assert client.quotes("SPY")["SPY"].last_price == 601.22


def test_p2_check_and_read_share_one_fd(tmp_path: Path, monkeypatch) -> None:
    """Swap the file between fstat validation and the read: the data must come
    from the descriptor that was validated, not from the swapped-in path."""
    tf = tmp_path / "tokens.json"
    tf.write_text('{"access_token": "validated"}')
    os.chmod(tf, 0o600)
    real_check = sm._check_private_fd

    def swap_then_check(fd, path):
        st = real_check(fd, path)
        attacker = tmp_path / "attacker.json"
        attacker.write_text('{"access_token": "swapped"}')
        os.chmod(attacker, 0o644)
        os.replace(attacker, tf)  # replace the path after validation
        return st

    monkeypatch.setattr(sm, "_check_private_fd", swap_then_check)
    assert json.loads(sm._read_private(tf))["access_token"] == "validated"


# -- P2: exception chaining -----------------------------------------------------

def test_p2_transport_exception_text_absent_from_traceback_on_get(tmp_path: Path, session) -> None:
    client, _tf = _client_needing_refresh(tmp_path, session, max_retries=0)
    _refresh_ok(session)
    session.get_queue.append(requests.ConnectionError(f"proxy auth {MARKER} https://u:{MARKER}@x"))
    with pytest.raises(SchwabTransientError) as ei:
        client.quotes("SPY")
    assert MARKER not in _tb(ei.value)
    assert ei.value.__cause__ is None and ei.value.__suppress_context__


def test_p2_transport_exception_text_absent_from_traceback_on_refresh(tmp_path: Path, session) -> None:
    client, _tf = _client_needing_refresh(tmp_path, session)
    session.post_queue.append(requests.ConnectionError(f"basic {MARKER}"))
    with pytest.raises(SchwabTransientError) as ei:
        client.refresh()
    assert MARKER not in _tb(ei.value)
    assert ei.value.__cause__ is None and ei.value.__suppress_context__


def test_p2_non_json_bodies_do_not_leak_text(tmp_path: Path, session) -> None:
    class BadJSON(FakeResponse):
        def json(self):
            raise ValueError(f"Expecting value: {MARKER}")

    client, _tf = _client_needing_refresh(tmp_path, session)
    session.post_queue.append(BadJSON(200, {}))
    with pytest.raises(SchwabTransientError) as ei:
        client.refresh()
    assert MARKER not in _tb(ei.value)
    _refresh_ok(session)
    session.get_queue.append(BadJSON(200, {}))
    with pytest.raises(SchwabTransientError) as ei2:
        client.quotes("SPY")
    assert MARKER not in _tb(ei2.value)


# -- P2: concurrency / lock -----------------------------------------------------

def test_p2_lock_file_is_private_and_exclusive(client, session, tmp_path: Path) -> None:
    session.get_queue.append(FakeResponse(200, SPY_BODY))
    client.quotes("SPY")
    lock_path = tmp_path / "tokens.json.lock"
    assert lock_path.is_file() and stat.S_IMODE(lock_path.stat().st_mode) == 0o600

    held = threading.Event()
    release = threading.Event()

    def holder():
        with client._lock():
            held.set()
            release.wait(5)

    t = threading.Thread(target=holder)
    t.start()
    assert held.wait(5)
    fd = os.open(lock_path, os.O_RDWR)
    try:
        with pytest.raises(BlockingIOError):
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        os.close(fd)
        release.set()
        t.join(5)
    fd = os.open(lock_path, os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # released after exit
    finally:
        os.close(fd)


def test_p2_second_client_reuses_refresh_done_by_first(tmp_path: Path) -> None:
    """Two clients share one token file and both hold stale caches. Only one
    refresh happens and both caches equal the final file."""
    sa, sb = FakeSession(), FakeSession()
    tf = tmp_path / "tokens.json"
    write_token_file(tf, access_expires_at=NOW - 10, refresh_expires_at=NOW + 86400)
    mk = lambda s: SchwabMarketClient(app_key=FAKE_KEY, app_secret=FAKE_SECRET, token_file=tf,
                                      session=s, now=lambda: NOW)
    a, b = mk(sa), mk(sb)
    a._load_tokens(); b._load_tokens()  # both cache the stale state
    _refresh_ok(sa, "fake-access-from-A")
    sa.get_queue.append(FakeResponse(200, SPY_BODY))
    sb.get_queue.append(FakeResponse(200, SPY_BODY))

    a.quotes("SPY")
    b.quotes("SPY")

    assert [c["method"] for c in sa.calls] == ["POST", "GET"]
    assert [c["method"] for c in sb.calls] == ["GET"]  # no second refresh
    assert sb.calls[0]["headers"]["Authorization"] == "Bearer fake-access-from-A"
    final = json.loads(tf.read_text())
    assert a._tokens == final == b._tokens
    assert [p.name for p in tmp_path.iterdir() if p.name.startswith(".tokens-")] == []


def test_p2_stale_401_does_not_refresh_when_file_already_rotated(tmp_path: Path) -> None:
    """A 401 for a token that is no longer the one on disk re-reads the file
    instead of refreshing again (another holder already refreshed)."""
    sa, sb = FakeSession(), FakeSession()
    tf = tmp_path / "tokens.json"
    write_token_file(tf, access_expires_at=NOW + 1500, refresh_expires_at=NOW + 86400)
    a = SchwabMarketClient(app_key=FAKE_KEY, app_secret=FAKE_SECRET, token_file=tf,
                           session=sa, now=lambda: NOW)
    b = SchwabMarketClient(app_key=FAKE_KEY, app_secret=FAKE_SECRET, token_file=tf,
                           session=sb, now=lambda: NOW)

    def a_get(url, params=None, headers=None, timeout=None):
        sa.calls.append({"method": "GET", "url": url, "params": dict(params or {}),
                         "headers": dict(headers or {}), "timeout": timeout})
        if len(sa.calls) == 1:
            _refresh_ok(sb, "fake-access-from-B")
            b.refresh()  # someone else rotates the file between A's attempts
            return FakeResponse(401)
        return FakeResponse(200, SPY_BODY)

    sa.get = a_get  # type: ignore[method-assign]
    a.quotes("SPY")
    assert [c["method"] for c in sa.calls] == ["GET", "GET"]  # no POST from A
    assert sa.calls[1]["headers"]["Authorization"] == "Bearer fake-access-from-B"


# -- P2: refresh-token expiry policy ------------------------------------------

def test_p2_unchanged_refresh_token_keeps_old_expiry(tmp_path: Path, session) -> None:
    client, tf = _client_needing_refresh(tmp_path, session)
    _refresh_ok(session, refresh_token=FAKE_REFRESH)  # same token echoed back
    client.refresh()
    saved = json.loads(tf.read_text())
    assert saved["refresh_token"] == FAKE_REFRESH
    assert saved["refresh_expires_at"] == NOW + 86400  # not extended


def test_p2_omitted_refresh_token_keeps_old_expiry(tmp_path: Path, session) -> None:
    client, tf = _client_needing_refresh(tmp_path, session)
    _refresh_ok(session)
    client.refresh()
    saved = json.loads(tf.read_text())
    assert saved["refresh_token"] == FAKE_REFRESH
    assert saved["refresh_expires_at"] == NOW + 86400


def test_p2_changed_refresh_token_gets_new_seven_day_expiry(tmp_path: Path, session) -> None:
    client, tf = _client_needing_refresh(tmp_path, session)
    _refresh_ok(session, refresh_token="fake-rotated-refresh")
    client.refresh()
    saved = json.loads(tf.read_text())
    assert saved["refresh_token"] == "fake-rotated-refresh"
    assert saved["refresh_expires_at"] == int(NOW) + 7 * 86400


# -- P2: provider error objects are not logged raw ----------------------------

def test_p2_errors_object_values_never_reach_logs(log_lines) -> None:
    huge_key = "k" * 500
    body = {
        "SPY": SPY_BODY["SPY"],
        "errors": {
            "invalidSymbols": [MARKER, "NOPE"],
            "other": {"detail": MARKER},
            huge_key: "x" * 10_000,
            **{f"cat{i}": [i] for i in range(20)},
        },
    }
    out = parse_quotes(body)
    assert set(out) == {"SPY"}
    err_lines = [line for line in log_lines if "returned errors" in line]
    assert len(err_lines) == 1
    line = err_lines[0]
    assert MARKER not in line and "x" * 50 not in line
    assert "invalidSymbols=2" in line and "other_keys=22" in line
    assert "other=" not in line and huge_key[:40] not in line
    assert len(line) < 200


@pytest.mark.parametrize("errors", [["a", MARKER], MARKER, 42])
def test_p2_non_mapping_errors_objects_are_summarised(log_lines, errors) -> None:
    parse_quotes({"errors": errors})
    line = next(line for line in log_lines if "returned errors" in line)
    assert MARKER not in line
    assert line.rstrip().endswith(("list=2", "unrecognised"))


# Second independent review (d522aa27): provider-controlled *keys* must not
# reach the log either, truncated or not.
@pytest.mark.parametrize("key", [
    MARKER,                              # bare fake secret as the key
    "invalidSymbols" + MARKER,           # allowlisted prefix + secret suffix
    MARKER + "invalidSymbols",           # secret prefix + allowlisted suffix
    "invalidsymbols",                    # case variant of an allowlisted name
    "Bearer " + MARKER,
    "k" * 39 + MARKER,                   # survives a 40-char truncation
])
def test_p2_errors_object_keys_never_reach_logs(log_lines, key) -> None:
    body = {"SPY": SPY_BODY["SPY"], "errors": {key: ["NOPE"], "invalidCusips": ["x", "y"]}}
    out = parse_quotes(body)
    assert set(out) == {"SPY"}
    err_lines = [line for line in log_lines if "returned errors" in line]
    assert len(err_lines) == 1
    line = err_lines[0]
    assert MARKER not in line and MARKER[:8] not in line and key not in line
    assert "invalidCusips=2" in line and "other_keys=1" in line


def test_p2_error_summary_emits_only_allowlisted_literals(log_lines) -> None:
    """Every token before '=' in the summary is an allowlist literal or 'other_keys'."""
    body = {"errors": {"invalidSymbols": ["A"], "invalidSSIDs": [1, 2, 3],
                       "zzz": 1, MARKER: {"deep": MARKER}, 7: None}}
    parse_quotes(body)
    line = next(line for line in log_lines if "returned errors" in line)
    summary = line.split("returned errors: ", 1)[1].strip()
    names = [part.split("=", 1)[0] for part in summary.split(", ")]
    assert set(names) <= set(sm.ERROR_CATEGORY_ALLOWLIST) | {"other_keys"}
    assert summary == "invalidSymbols=1, invalidSSIDs=3, other_keys=3"
    assert sm._summarise_errors({}) == "empty"


# -- P3: missing expires_in -----------------------------------------------------

@pytest.mark.parametrize("expires_in", [None, "garbage", 0, -5])
def test_p3_missing_or_bad_expires_in_is_normalised_before_persist(tmp_path: Path, session,
                                                                   expires_in, log_lines) -> None:
    client, tf = _client_needing_refresh(tmp_path, session)
    body = {"access_token": "fake-new"}
    if expires_in is not None:
        body["expires_in"] = expires_in
    session.post_queue.append(FakeResponse(200, body))
    client.refresh()  # must not raise after the file has been written
    saved = json.loads(tf.read_text())
    assert saved["expires_in"] == sm.DEFAULT_ACCESS_LIFETIME_SECONDS
    assert saved["access_expires_at"] == int(NOW) + sm.DEFAULT_ACCESS_LIFETIME_SECONDS
    assert any("expires in 1800s" in line for line in log_lines)


def test_refresh_with_empty_access_token_does_not_persist(tmp_path: Path, session) -> None:
    client, tf = _client_needing_refresh(tmp_path, session)
    session.post_queue.append(FakeResponse(200, {"access_token": "", "expires_in": 1800}))
    with pytest.raises(SchwabAuthError):
        client.refresh()
    assert json.loads(tf.read_text())["access_token"] == FAKE_ACCESS


def test_posix_requirement_is_fail_closed(client, session, monkeypatch) -> None:
    monkeypatch.setattr(sm, "fcntl", None)
    with pytest.raises(SchwabConfigError, match="POSIX"):
        client.quotes("SPY")
    assert session.calls == []
