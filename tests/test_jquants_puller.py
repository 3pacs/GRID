"""Regression tests for the J-Quants V2 puller.

Production audit 2026-11-05: JQuants failed every run since 2026-09-30 with
``RetryError[... raised ValueError]``. The scheduler builds
``JQuantsPuller(db_engine)`` without credentials, the puller never read
``settings``, and the deterministic ``ValueError`` was retried three times.
The client also spoke the V1 token flow, which J-Quants discontinued on
2026-06-01. The puller is now a V2 client: API key in the ``x-api-key``
header, ``GET /v2/indices/bars/daily/topix``, rows under ``"data"`` with
short columns (``C`` = Close), still written as ``japan_topix``.

Fixtures follow the response samples in the official V2 docs
(https://jpx-jquants.com/en/spec/idx-bars-daily-topix,
https://jpx-jquants.com/en/spec/response-status). All HTTP is mocked.
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import requests
from loguru import logger

from ingestion.international import jquants
from ingestion.international.jquants import JQuantsNotConfigured, JQuantsPuller
from ingestion.smart_scheduler import OUTCOME_FAILED, OUTCOME_SKIPPED, _classify_outcome
from ingestion.source_freshness import error_class

API_KEY = "jq-test-key-0123456789abcdef"
TOPIX_PATH = "/v2/indices/bars/daily/topix"

# Shape of the 200 sample on the V2 TOPIX page.
TOPIX_PAGE_1 = {
    "data": [
        {"Date": "2022-06-28", "O": 1885.52, "H": 1907.38, "L": 1885.32, "C": 1907.38},
    ],
    "pagination_key": "value1.value2.",
}
TOPIX_PAGE_2 = {
    "data": [
        {"Date": "2022-06-29", "O": 1907.0, "H": 1910.1, "L": 1890.2, "C": 1895.75},
    ],
}
# The 403 body the response-status page documents for a bad key.
FORBIDDEN_BODY = {"message": "The incoming api key is invalid or expired."}


def _engine() -> tuple[MagicMock, MagicMock]:
    """Mock engine: source_catalog id 213; returns (engine, write conn)."""
    engine = MagicMock()
    read_conn = engine.connect.return_value.__enter__.return_value
    read_conn.execute.return_value.fetchone.return_value = (213,)
    write_conn = engine.begin.return_value.__enter__.return_value
    return engine, write_conn


def _resp(status: int, payload: dict) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = payload
    return resp


def _puller(api_key: str | None = API_KEY) -> tuple[JQuantsPuller, MagicMock]:
    engine, write_conn = _engine()
    puller = JQuantsPuller(engine, api_key=api_key)
    puller._row_exists = MagicMock(return_value=False)
    return puller, write_conn


@pytest.fixture
def captured_logs():
    lines: list[str] = []
    sink = logger.add(lambda m: lines.append(str(m)), level="DEBUG")
    yield lines
    logger.remove(sink)


@pytest.fixture(autouse=True)
def _no_sleep():
    with patch.object(jquants.time, "sleep"), patch.object(JQuantsPuller._get.retry, "sleep"):
        yield


def test_api_key_defaults_from_settings() -> None:
    engine, _ = _engine()
    with patch("config.settings", SimpleNamespace(JQUANTS_API_KEY=API_KEY)):
        puller = JQuantsPuller(engine)
    assert puller.is_configured


def test_key_sent_only_in_header_and_v2_path() -> None:
    puller, _ = _puller()
    with patch.object(jquants.requests, "get", return_value=_resp(200, TOPIX_PAGE_2)) as get, \
            patch.object(jquants.requests, "post") as post:
        result = puller.pull_all()

    post.assert_not_called()  # no V1 auth_user / auth_refresh
    url = get.call_args.args[0]
    assert url == f"https://api.jquants.com{TOPIX_PATH}"
    assert get.call_args.kwargs["headers"] == {"x-api-key": API_KEY}
    assert API_KEY not in url
    assert API_KEY not in str(get.call_args.kwargs["params"])
    assert set(get.call_args.kwargs["params"]) == {"from"}
    assert result["status"] == "SUCCESS"


def test_series_mapping_and_pagination() -> None:
    puller, write_conn = _puller()
    with patch.object(jquants.requests, "get",
                      side_effect=[_resp(200, TOPIX_PAGE_1), _resp(200, TOPIX_PAGE_2)]) as get:
        result = puller.pull_all()

    assert get.call_count == 2
    assert get.call_args_list[1].kwargs["params"]["pagination_key"] == "value1.value2."
    inserts = [c.args[1] for c in write_conn.execute.call_args_list]
    assert inserts == [
        {"sid": "japan_topix", "src": 213, "od": date(2022, 6, 28), "val": 1907.38},
        {"sid": "japan_topix", "src": 213, "od": date(2022, 6, 29), "val": 1895.75},
    ]
    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 2


def test_row_without_close_is_skipped_not_zero() -> None:
    puller, write_conn = _puller()
    body = {"data": [{"Date": "2022-06-28", "O": 1.0, "H": 1.0, "L": 1.0}]}
    with patch.object(jquants.requests, "get", return_value=_resp(200, body)):
        result = puller.pull_all()
    write_conn.execute.assert_not_called()
    assert result["rows_inserted"] == 0


@pytest.mark.parametrize("settings_key", ["", None])
def test_missing_key_skips_without_network(settings_key: str | None) -> None:
    engine, _ = _engine()
    with patch("config.settings", SimpleNamespace(JQUANTS_API_KEY=settings_key)):
        puller = JQuantsPuller(engine)
    with patch.object(jquants.requests, "get") as get, \
            patch.object(jquants.requests, "post") as post:
        result = puller.pull_all()

    get.assert_not_called()
    post.assert_not_called()
    assert result["status"] == "SKIPPED"
    assert result["skipped_reason"] == "JQUANTS_API_KEY not configured"
    outcome, rows, note = _classify_outcome(result)
    assert (outcome, rows) == (OUTCOME_SKIPPED, 0)
    assert error_class(note) == "CONFIG"


def test_missing_key_is_not_retried() -> None:
    puller, _ = _puller(api_key="")
    with patch.object(jquants.requests, "get") as get, pytest.raises(JQuantsNotConfigured):
        puller._get(jquants._TOPIX_ENDPOINT, {})
    get.assert_not_called()


@pytest.mark.parametrize("status", [401, 403])
def test_auth_error_fails_config_class_without_retry(status: int, captured_logs: list[str]) -> None:
    puller, _ = _puller()
    with patch.object(jquants.requests, "get", return_value=_resp(status, FORBIDDEN_BODY)) as get:
        result = puller.pull_all()

    assert get.call_count == 1
    assert result["status"] == "FAILED"
    assert result["errors"][0].startswith(f"auth_failed: HTTP {status} on {TOPIX_PATH}")
    outcome, _rows, note = _classify_outcome(result)
    assert outcome == OUTCOME_FAILED
    assert error_class(note) == "CONFIG"
    failed = [line for line in captured_logs if "J-Quants pull failed" in line]
    assert failed and all("WARNING" in line for line in failed)


def test_rate_limit_is_not_retried() -> None:
    puller, _ = _puller()
    with patch.object(jquants.requests, "get", return_value=_resp(429, {})) as get:
        result = puller.pull_all()
    assert get.call_count == 1
    assert result["errors"] == [f"HTTP 429 on {TOPIX_PATH}"]


def test_server_error_is_retried() -> None:
    puller, _ = _puller()
    with patch.object(jquants.requests, "get",
                      side_effect=[_resp(500, {}), _resp(200, TOPIX_PAGE_2)]) as get:
        result = puller.pull_all()
    assert get.call_count == 2
    assert result["status"] == "SUCCESS"


def test_key_never_logged(captured_logs: list[str]) -> None:
    puller, _ = _puller()
    with patch.object(jquants.requests, "get",
                      side_effect=[_resp(200, TOPIX_PAGE_1), _resp(403, FORBIDDEN_BODY)]):
        result = puller.pull_all()
    assert result["status"] == "FAILED"
    assert captured_logs
    assert not any(API_KEY in line for line in captured_logs)
    assert API_KEY not in str(result)


def test_query_string_never_logged_or_stored(captured_logs: list[str]) -> None:
    """requests exceptions embed the full URL; its query string must not leak."""
    puller, _ = _puller()
    leaky = "https://api.jquants.com/v2/indices/bars/daily/topix?from=2022-06-01&token=sekrit-q-value"
    resp = MagicMock(status_code=502, url=leaky)
    exc_http = requests.HTTPError(f"502 Server Error: Bad Gateway for url: {leaky}", response=resp)
    exc_conn = requests.ConnectionError(f"Max retries exceeded with url: {leaky}")

    for exc in (exc_http, exc_conn):
        with patch.object(jquants.requests, "get", side_effect=exc):
            result = puller.pull_all()
        assert result["status"] == "FAILED"
        assert "sekrit-q-value" not in str(result["errors"])
        assert "?" not in result["errors"][0]

    assert result["errors"][0].startswith(
        "ConnectionError: Max retries exceeded with url: https://api.jquants.com/v2/")
    assert not any("sekrit-q-value" in line for line in captured_logs)
    assert puller._safe_error(exc_http) == f"HTTP 502 on {TOPIX_PATH}"
