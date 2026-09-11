"""Secret handling in the wave-loader scripts and the trial-gem-hunter harness.

``scripts/load_wave2.py`` and ``scripts/load_wave3.py`` are straight-line
modules that connect to Postgres and hit a dozen public APIs at import time,
so they are executed here with ``runpy`` against stubbed ``psycopg2`` /
``requests`` / ``time.sleep``. The tests pin three properties:

* with the key env vars unset, every keyed section is skipped with a warning
  and no request reaches the keyed endpoint (graceful degradation);
* with keys set, the key travels in ``params`` / headers, never inside the
  URL string;
* the rest of the script still runs either way.
"""

from __future__ import annotations

import datetime
import importlib.util
import runpy
import time
from pathlib import Path
from types import ModuleType

import psycopg2
import pytest
import requests
from loguru import logger as log

from config import settings

ROOT = Path(__file__).resolve().parent.parent
WAVE2 = ROOT / "scripts" / "load_wave2.py"
WAVE3 = ROOT / "scripts" / "load_wave3.py"
HARNESS = ROOT / "tasks" / "trial-gem-hunter" / "tests" / "test.py"


class _FakeCursor:
    def execute(self, *_args, **_kwargs) -> None:
        pass

    def fetchone(self) -> tuple[int]:
        return (1,)

    def fetchall(self) -> list:
        return []

    def close(self) -> None:
        pass


class _FakeConn:
    autocommit = False

    def cursor(self, **_kwargs) -> _FakeCursor:
        return _FakeCursor()

    def close(self) -> None:
        pass


class _FakeResponse:
    ok = True
    status_code = 200
    text = ""

    def json(self) -> dict:
        return {}

    def raise_for_status(self) -> None:
        pass


@pytest.fixture
def recorded_requests(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    """Stub the network, the database, and the rate-limit sleeps; record every GET."""
    calls: list[dict] = []

    def fake_get(url: str, params=None, headers=None, timeout=None, **_kwargs) -> _FakeResponse:
        calls.append({"url": url, "params": dict(params or {}), "headers": dict(headers or {})})
        return _FakeResponse()

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(psycopg2, "connect", lambda **_kwargs: _FakeConn())
    monkeypatch.setattr(time, "sleep", lambda *_args, **_kwargs: None)
    return calls


@pytest.fixture
def captured_warnings() -> list[str]:
    messages: list[str] = []
    handler = log.add(lambda message: messages.append(message.record["message"]), level="WARNING")
    yield messages
    log.remove(handler)


def _run(script: Path) -> None:
    runpy.run_path(str(script), run_name="__main__")


def _hits(calls: list[dict], host: str) -> list[dict]:
    return [call for call in calls if host in call["url"]]


def _assert_key_never_in_url(calls: list[dict], *secrets: str) -> None:
    for call in calls:
        for secret in secrets:
            assert secret not in call["url"], f"secret leaked into URL: {call['url']}"


# ── scripts/load_wave2.py ────────────────────────────────────────────────────


@pytest.mark.unit
def test_wave2_skips_keyed_sources_without_keys(
    monkeypatch: pytest.MonkeyPatch, recorded_requests: list[dict], captured_warnings: list[str]
) -> None:
    monkeypatch.setattr(settings, "ALPHAVANTAGE_API_KEY", "")
    monkeypatch.setattr(settings, "NEWSAPI_KEY", "")

    _run(WAVE2)

    assert not _hits(recorded_requests, "alphavantage.co")
    assert not _hits(recorded_requests, "newsapi.org")
    assert any("ALPHAVANTAGE_API_KEY not set" in m for m in captured_warnings)
    assert any("NEWSAPI_KEY not set" in m for m in captured_warnings)
    # the keyless sections still ran
    assert _hits(recorded_requests, "mempool.space")
    assert _hits(recorded_requests, "open-meteo.com")


@pytest.mark.unit
def test_wave2_sends_keys_as_params_never_in_url(
    monkeypatch: pytest.MonkeyPatch, recorded_requests: list[dict]
) -> None:
    monkeypatch.setattr(settings, "ALPHAVANTAGE_API_KEY", "av-test-key")
    monkeypatch.setattr(settings, "NEWSAPI_KEY", "news-test-key")

    _run(WAVE2)

    av_calls = _hits(recorded_requests, "alphavantage.co")
    assert len(av_calls) == 4
    for call in av_calls:
        assert call["url"] == "https://www.alphavantage.co/query"
        assert call["params"]["apikey"] == "av-test-key"
        assert call["params"]["function"] in {"RSI", "MACD"}
        assert call["params"]["interval"] == "daily"
    rsi_periods = {c["params"].get("time_period") for c in av_calls if c["params"]["function"] == "RSI"}
    assert rsi_periods == {14}
    assert not any("time_period" in c["params"] for c in av_calls if c["params"]["function"] == "MACD")

    news_calls = _hits(recorded_requests, "newsapi.org")
    assert len(news_calls) == 8
    assert all(c["params"]["apiKey"] == "news-test-key" for c in news_calls)

    _assert_key_never_in_url(recorded_requests, "av-test-key", "news-test-key")


# ── scripts/load_wave3.py ────────────────────────────────────────────────────


@pytest.mark.unit
def test_wave3_skips_keyed_sources_without_keys(
    monkeypatch: pytest.MonkeyPatch, recorded_requests: list[dict], captured_warnings: list[str]
) -> None:
    monkeypatch.setattr(settings, "EIA_API_KEY", "")
    monkeypatch.setattr(settings, "NOAA_TOKEN", "")

    _run(WAVE3)

    assert not _hits(recorded_requests, "api.eia.gov")
    assert not _hits(recorded_requests, "ncei.noaa.gov")
    assert any("EIA_API_KEY not set" in m for m in captured_warnings)
    assert any("NOAA_TOKEN not set" in m for m in captured_warnings)
    assert _hits(recorded_requests, "open-meteo.com")
    assert _hits(recorded_requests, "db.nomics.world")


@pytest.mark.unit
def test_wave3_sends_keys_as_params_or_headers_never_in_url(
    monkeypatch: pytest.MonkeyPatch, recorded_requests: list[dict]
) -> None:
    monkeypatch.setattr(settings, "EIA_API_KEY", "eia-test-key")
    monkeypatch.setattr(settings, "NOAA_TOKEN", "noaa-test-token")

    _run(WAVE3)

    eia_calls = _hits(recorded_requests, "api.eia.gov")
    assert len(eia_calls) == 8
    for call in eia_calls:
        assert call["url"].startswith("https://api.eia.gov/v2/seriesid/")
        assert "?" not in call["url"]
        assert call["params"]["api_key"] == "eia-test-key"
        assert call["params"]["frequency"] == "weekly"

    noaa_calls = _hits(recorded_requests, "ncei.noaa.gov")
    assert len(noaa_calls) == 4  # 1 HDD-normals call + 3 station calls
    assert all(c["headers"]["token"] == "noaa-test-token" for c in noaa_calls)

    _assert_key_never_in_url(recorded_requests, "eia-test-key", "noaa-test-token")


# ── tasks/trial-gem-hunter/tests/test.py ─────────────────────────────────────


def _load_harness(monkeypatch: pytest.MonkeyPatch, env: dict[str, str]) -> ModuleType:
    for name in ("ALPHAVANTAGE_API_KEY", "ALPHA_VANTAGE_KEY"):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    spec = importlib.util.spec_from_file_location("trial_gem_harness", HARNESS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.unit
def test_harness_av_fallback_disabled_without_key(
    monkeypatch: pytest.MonkeyPatch, recorded_requests: list[dict]
) -> None:
    harness = _load_harness(monkeypatch, {})
    assert harness.ALPHA_VANTAGE_KEY == ""
    assert harness._av_forward_return("ABCD", datetime.date(2026, 1, 5)) is None
    assert recorded_requests == []


@pytest.mark.unit
@pytest.mark.parametrize("env_name", ["ALPHAVANTAGE_API_KEY", "ALPHA_VANTAGE_KEY"])
def test_harness_av_fallback_sends_key_as_param(
    monkeypatch: pytest.MonkeyPatch, recorded_requests: list[dict], env_name: str
) -> None:
    harness = _load_harness(monkeypatch, {env_name: "av-test-key"})
    harness._av_forward_return("ABCD", datetime.date(2026, 1, 5))

    (call,) = recorded_requests
    assert call["url"] == "https://www.alphavantage.co/query"
    assert call["params"]["apikey"] == "av-test-key"
    assert call["params"]["symbol"] == "ABCD"
    assert call["params"]["function"] == "TIME_SERIES_DAILY_ADJUSTED"
    _assert_key_never_in_url(recorded_requests, "av-test-key")
