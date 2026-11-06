"""LobbyingPuller against the current LDA API (lda.gov).

2026-11-06: lda.senate.gov/api/v1/ 301-redirects to lda.gov/api/v1/, and the
old ``filing_type=REGISTRATION,REPORT`` filter is not an LDA filing-type code
(the API answers HTTP 400), so every run fetched 0 filings and still reported
SUCCESS. HTTP is mocked; no test touches the network.
"""

from __future__ import annotations

from typing import Any

import pytest
import requests

from ingestion.altdata import lobbying

FILING = {
    "filing_uuid": "b4a023c6-9d76-4a73-aabe-194fc132e760",
    "filing_type": "Q3",
    "filing_year": 2026,
    "filing_period": "third_quarter",
    "income": "70000.00",
    "expenses": None,
    "dt_posted": "2026-10-07T06:37:30-04:00",
    "registrant": {"id": 401, "name": "Acme Advocacy LLC"},
    "client": {"id": 77, "name": "Exxon Mobil"},
    "lobbying_activities": [
        {"general_issue_code": "ENG", "description": "Energy permitting, H.R. 1234 and S. 99"},
    ],
}


class _Resp:
    def __init__(self, status: int, body: Any) -> None:
        self.status_code = status
        self._body = body

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Client Error")

    def json(self) -> Any:
        return self._body


@pytest.fixture
def puller(monkeypatch: pytest.MonkeyPatch) -> lobbying.LobbyingPuller:
    obj = lobbying.LobbyingPuller.__new__(lobbying.LobbyingPuller)
    obj.opensecrets_key = ""
    monkeypatch.setattr(lobbying.time, "sleep", lambda _s: None)
    return obj


def _record(monkeypatch: pytest.MonkeyPatch, *responses: _Resp) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    queue = list(responses)

    def _get(url: str, **kwargs: Any) -> _Resp:
        calls.append({"url": url, **kwargs})
        return queue.pop(0)

    monkeypatch.setattr(lobbying.requests, "get", _get)
    return calls


def test_lda_host_is_lda_gov_and_redirects_are_followed(
    puller: lobbying.LobbyingPuller, monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _record(monkeypatch, _Resp(200, {"results": [FILING], "next": None}))
    filings = puller._fetch_lda_filings(days_back=30)
    assert len(filings) == 1
    call = calls[0]
    assert call["url"] == "https://lda.gov/api/v1/filings/"
    assert call["allow_redirects"] is True
    params = call["params"]
    assert "filing_type" not in params  # "REGISTRATION,REPORT" -> HTTP 400
    assert params["ordering"] == "-dt_posted"
    assert params["page_size"] == 25  # API maximum
    assert "filing_dt_posted_after" in params
    assert lobbying.LobbyingPuller.SOURCE_CONFIG["base_url"] == "https://lda.gov/api/v1/"


def test_parse_current_filing_shape(puller: lobbying.LobbyingPuller) -> None:
    parsed = puller._parse_lda_filing(FILING)
    assert parsed is not None
    assert parsed["client_name"] == "Exxon Mobil"
    assert parsed["amount"] == 70000.0
    assert str(parsed["obs_date"]) == "2026-10-07"
    assert parsed["issue_codes"] == ["ENG"]
    assert parsed["bills_lobbied"] == ["H.R. 1234", "S. 99"]


def test_pagination_follows_next_and_is_bounded(
    puller: lobbying.LobbyingPuller, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(lobbying, "_MAX_PAGES", 2)
    calls = _record(
        monkeypatch,
        _Resp(200, {"results": [FILING], "next": "https://lda.gov/api/v1/filings/?page=2"}),
        _Resp(200, {"results": [FILING], "next": "https://lda.gov/api/v1/filings/?page=3"}),
    )
    assert len(puller._fetch_lda_filings()) == 2
    assert [c["params"]["page"] for c in calls] == [1, 2]
    assert puller._lda_fetch_error is None


class _Conn:
    def __enter__(self) -> _Conn:
        return self

    def __exit__(self, *_a: object) -> None:
        return None

    def execute(self, *_a: Any, **_k: Any) -> None:
        return None


class _Engine:
    def begin(self) -> _Conn:
        return _Conn()


def test_failed_first_page_is_failed_not_zero_row_success(
    puller: lobbying.LobbyingPuller, monkeypatch: pytest.MonkeyPatch,
) -> None:
    puller.engine = _Engine()
    monkeypatch.setattr(lobbying, "_MAX_PAGES", 1)
    _record(monkeypatch, *[_Resp(400, {"detail": "bad filter"})] * 3)
    summary = puller.pull_all(days_back=30)
    assert summary["status"] == "FAILED"
    assert summary["error"].startswith("page 1:")
    assert summary["rows_inserted"] == 0


def test_successful_pull_reports_committed_rows(
    puller: lobbying.LobbyingPuller, monkeypatch: pytest.MonkeyPatch,
) -> None:
    puller.engine = _Engine()
    _record(monkeypatch, _Resp(200, {"results": [FILING], "next": None}))
    monkeypatch.setattr(puller, "_row_exists", lambda *_a, **_k: False, raising=False)
    inserted: list[str] = []
    monkeypatch.setattr(puller, "_insert_raw", lambda **kw: inserted.append(kw["series_id"]), raising=False)
    monkeypatch.setattr(puller, "_compute_spending_trend", lambda *_a: {"trend": "FLAT"})
    monkeypatch.setattr(puller, "_emit_signal", lambda *_a, **_k: None)
    summary = puller.pull_all(days_back=30)
    assert summary["status"] == "SUCCESS" and summary["error"] is None
    assert summary["rows_inserted"] == summary["stored"] == 1
    assert inserted and inserted[0].startswith("LOBBYING:")


def test_partial_when_a_later_page_fails(
    puller: lobbying.LobbyingPuller, monkeypatch: pytest.MonkeyPatch,
) -> None:
    puller.engine = _Engine()
    monkeypatch.setattr(lobbying, "_MAX_PAGES", 2)
    _record(
        monkeypatch,
        _Resp(200, {"results": [FILING], "next": "https://lda.gov/api/v1/filings/?page=2"}),
        *[_Resp(500, {})] * 3,
    )
    monkeypatch.setattr(puller, "_row_exists", lambda *_a, **_k: True, raising=False)
    summary = puller.pull_all(days_back=30)
    assert summary["status"] == "PARTIAL"
    assert summary["error"].startswith("page 2:")
