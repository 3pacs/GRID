"""Tests for the EIA weekly Cushing crude oil stocks *request contract*
(W5c, 2026-09-18). See ingestion/altdata/eia_puller.py's module docstring
for the documentation this was built from.

Pure Python: no network call is ever made against the live EIA API in
this test file (or anywhere in this pass) -- ``fetch_weekly_stocks`` is
never exercised end-to-end here, only:
  - the request builder (``build_weekly_stocks_request`` / ``as_display_url``),
  - the response parser (``parse_weekly_stocks_response``) against a
    constructed response in the documented envelope, and
  - the fail-closed guard when ``EIA_API_KEY`` is unset.
"""

from __future__ import annotations

from datetime import date

import pytest

from ingestion.altdata.eia_puller import (
    DEFAULT_WEEKLY_STOCKS_SERIES,
    _redact_api_key,
    as_display_url,
    build_weekly_stocks_request,
    fetch_weekly_stocks,
    parse_weekly_stocks_response,
)


# ── Request builder ─────────────────────────────────────────────────────


def test_build_weekly_stocks_request_matches_documented_shape(monkeypatch):
    import config

    monkeypatch.setattr(config.settings, "EIA_API_KEY", "sk_real_secret_value")
    monkeypatch.setattr(config.settings, "EIA_BASE_URL", "https://api.eia.gov/v2/")

    req = build_weekly_stocks_request()

    assert req["url"] == "https://api.eia.gov/v2/petroleum/stoc/wstk/data/"
    assert req["params"] == {
        "api_key": "sk_real_secret_value",
        "frequency": "weekly",
        "data[0]": "value",
        "facets[series][]": DEFAULT_WEEKLY_STOCKS_SERIES,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": 5,
    }


def test_build_weekly_stocks_request_honors_series_id_and_length():
    req = build_weekly_stocks_request("SOME_OTHER_SERIES", length=12, api_key="x")
    assert req["params"]["facets[series][]"] == "SOME_OTHER_SERIES"
    assert req["params"]["length"] == 12


def test_as_display_url_masks_api_key_never_leaks_real_value():
    req = build_weekly_stocks_request(api_key="sk_should_never_appear_in_logs")
    displayed = as_display_url(req)

    assert "sk_should_never_appear_in_logs" not in displayed
    assert "api_key=***" in displayed
    # The rest of the documented shape must still be visible/uncorrupted.
    assert "petroleum/stoc/wstk/data/" in displayed
    assert "frequency=weekly" in displayed
    assert f"facets%5Bseries%5D%5B%5D={DEFAULT_WEEKLY_STOCKS_SERIES}" in displayed


def test_redact_api_key_helper_masks_value_only():
    url = "https://api.eia.gov/v2/x/data/?api_key=abc123&frequency=weekly"
    assert _redact_api_key(url) == (
        "https://api.eia.gov/v2/x/data/?api_key=***&frequency=weekly"
    )


# ── Response parser (constructed response, documented envelope) ────────


def test_parse_weekly_stocks_response_documented_envelope():
    payload = {
        "response": {
            "total": "2",
            "dateFormat": "YYYY-MM-DD",
            "data": [
                {
                    "period": "2026-09-05",
                    "series": "W_EPC0_SAX_YCUOK_MBBL",
                    "series-description": "Weekly Cushing, OK Ending Stocks of Crude Oil (Thousand Barrels)",
                    "value": "24531",
                    "units": "MBBL",
                },
                {
                    "period": "2026-08-29",
                    "series": "W_EPC0_SAX_YCUOK_MBBL",
                    "series-description": "Weekly Cushing, OK Ending Stocks of Crude Oil (Thousand Barrels)",
                    "value": "24102",
                    "units": "MBBL",
                },
            ],
        },
        "request": {"command": "/v2/petroleum/stoc/wstk/data/"},
        "apiVersion": "2.1.8",
    }

    rows = parse_weekly_stocks_response(payload)

    assert len(rows) == 2
    assert rows[0] == {
        "period": date(2026, 9, 5),
        "value": 24531.0,
        "units": "MBBL",
        "series": "W_EPC0_SAX_YCUOK_MBBL",
    }
    assert rows[1]["period"] == date(2026, 8, 29)
    assert rows[1]["value"] == 24102.0


def test_parse_weekly_stocks_response_skips_missing_period_or_value():
    payload = {
        "response": {
            "data": [
                {"period": "2026-09-05", "value": "24531", "units": "MBBL"},
                {"period": None, "value": "1", "units": "MBBL"},
                {"period": "2026-08-29", "value": None, "units": "MBBL"},
                {"period": "2026-08-22", "value": "not-a-number", "units": "MBBL"},
            ],
        }
    }

    rows = parse_weekly_stocks_response(payload)

    assert len(rows) == 1
    assert rows[0]["period"] == date(2026, 9, 5)


def test_parse_weekly_stocks_response_empty_data_returns_empty_list():
    assert parse_weekly_stocks_response({"response": {"data": []}}) == []
    assert parse_weekly_stocks_response({"response": {}}) == []
    assert parse_weekly_stocks_response({}) == []


# ── fetch_weekly_stocks() fails closed without a key; never called live ──


def test_fetch_weekly_stocks_fails_closed_without_api_key(monkeypatch):
    import config

    monkeypatch.setattr(config.settings, "EIA_API_KEY", "")
    with pytest.raises(RuntimeError, match="EIA_API_KEY"):
        fetch_weekly_stocks()


def test_fetch_weekly_stocks_never_called_without_mocking_network(monkeypatch):
    """Guard against accidentally exercising a real network call: if
    EIA_API_KEY is set but requests.get is not monkeypatched, calling
    fetch_weekly_stocks() must not silently succeed against a real
    connection in this offline-only test file -- we assert it raises
    (either a connection error or is intercepted) rather than returning
    real data, and we never assert on network reachability either way."""
    import config

    monkeypatch.setattr(config.settings, "EIA_API_KEY", "sk_test_only")

    def _fail(*args, **kwargs):
        raise AssertionError(
            "fetch_weekly_stocks must never be called against the live "
            "EIA API in this test pass -- monkeypatch requests.get."
        )

    monkeypatch.setattr("ingestion.altdata.eia_puller.requests.get", _fail)
    with pytest.raises(AssertionError, match="must never be called"):
        fetch_weekly_stocks()
