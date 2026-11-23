"""A ticker Twelve Data refuses is skipped with its reason; it does not fail the run.

2026-11-22: the only four targets left for ``scripts/td_backfill_universe.py``
were symbols Twelve Data refuses (GLEN, SOL, US: HTTP 404; WBD: HTTP 400). No
fetch could succeed, the script exited 1 and
``grid-td-universe-backfill.service`` sat in failed state. The two error
bodies used below are the ones Twelve Data returned on 2026-11-23.
"""
from __future__ import annotations

from typing import Any

import pytest

from tests.test_td_backfill_universe_credentials import (
    _default_env,
    _install_fake_db,
    _load_module,
)

PLAN_404 = {
    "code": 404,
    "message": "This symbol is available starting with the Grow or Venture plan. "
               "Consider upgrading now at https://twelvedata.com/pricing",
    "status": "error",
}
NOT_FOUND_404 = {
    "code": 404,
    "message": "**symbol** not found: SOL. Please specify it correctly according to API Documentation.",
    "status": "error",
}
NO_DATA_400 = {
    "code": "400",
    "message": "No data is available on the specified dates. Try setting different start/end dates.",
    "status": "error",
}
BAR = {"date": "2026-11-20", "close": 1.0}


class _Resp:
    def __init__(self, status_code: int, body: Any) -> None:
        self.status_code = status_code
        self._body = body

    def json(self) -> Any:
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


def _module(monkeypatch):
    mod = _load_module(monkeypatch, _default_env(TWELVEDATA_API_KEY="super-secret-key"))
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    return mod


def _rows(*tickers: str) -> list[dict[str, str]]:
    return [{"ticker": t, "bucket": "STALE_7_30"} for t in tickers]


@pytest.mark.parametrize(
    ("status", "body", "expected_prefix"),
    [
        (404, PLAN_404, "HTTP 404: This symbol is available starting with the Grow"),
        (404, NOT_FOUND_404, "HTTP 404: **symbol** not found: SOL"),
        (400, NO_DATA_400, "HTTP 400: No data is available on the specified dates"),
        # Twelve Data also answers HTTP 200 with the code in the body.
        (200, NOT_FOUND_404, "HTTP 404: **symbol** not found: SOL"),
    ],
)
def test_symbol_refusals_are_rejections_with_twelve_datas_reason(monkeypatch, status, body, expected_prefix):
    mod = _module(monkeypatch)

    reason = mod.provider_rejection(status, body)

    assert reason is not None and reason.startswith(expected_prefix)


@pytest.mark.parametrize(
    ("status", "body"),
    [
        (401, {"code": 401, "message": "**apikey** parameter is incorrect", "status": "error"}),
        (403, {"code": 403, "message": "symbol requires a higher plan", "status": "error"}),
        (429, {"code": 429, "message": "You have run out of API credits for the symbol day.", "status": "error"}),
        (500, {"code": 500, "message": "Internal error for symbol", "status": "error"}),
        (400, {"code": 400, "message": "**interval** parameter is missing or invalid", "status": "error"}),
        (404, {"code": 404, "message": "Endpoint not found", "status": "error"}),
        (404, None),                      # a 404 page that is not a Twelve Data error body
        (404, ["not", "a", "dict"]),
        (404, {"code": 404, "message": "symbol not found"}),            # no status: not an error body
        (200, {"code": "n/a", "message": "symbol", "status": "error"}),
        (200, {"values": [], "status": "ok"}),
    ],
)
def test_everything_else_stays_a_failed_fetch(monkeypatch, status, body):
    mod = _module(monkeypatch)

    assert mod.provider_rejection(status, body) is None


def test_fetch_td_raises_for_a_refused_symbol_and_returns_empty_for_a_real_failure(monkeypatch, capsys):
    mod = _module(monkeypatch)
    answers = {"GLEN": _Resp(404, PLAN_404), "WBD": _Resp(400, NO_DATA_400),
               "DOWN": _Resp(503, ValueError("not json")),
               "LIMIT": _Resp(429, {"code": 429, "message": "run out of API credits", "status": "error"})}
    monkeypatch.setattr(mod.requests, "get", lambda _url, params, timeout: answers[params["symbol"]])
    window = (mod.date(2026, 9, 24), mod.date(2026, 11, 23))

    with pytest.raises(mod.ProviderRejected, match="Grow or Venture plan"):
        mod.fetch_td("GLEN", *window)
    with pytest.raises(mod.ProviderRejected, match="No data is available"):
        mod.fetch_td("WBD", *window)
    assert mod.fetch_td("DOWN", *window) == []
    assert mod.fetch_td("LIMIT", *window) == []
    err = capsys.readouterr().err
    assert "DOWN: HTTP 503" in err and "LIMIT: HTTP 429" in err


def test_rejection_reason_never_carries_the_api_key(monkeypatch):
    mod = _module(monkeypatch)
    body = {"code": 404, "status": "error",
            "message": "**symbol** not found: X (apikey=super-secret-key) " + "y" * 400}

    reason = mod.provider_rejection(404, body)

    assert reason is not None and "super-secret-key" not in reason
    assert len(reason) <= len("HTTP 404: ") + 200


def test_run_of_only_refused_symbols_exits_zero_and_names_each_reason(monkeypatch, capsys):
    """The 2026-11-22 run: four targets, all refused by Twelve Data."""
    mod = _module(monkeypatch)
    fake_conn = _install_fake_db(monkeypatch, work_rows=_rows("GLEN", "SOL", "US", "WBD"))
    reasons = {"GLEN": "HTTP 404: plan", "SOL": "HTTP 404: not found",
               "US": "HTTP 404: not found", "WBD": "HTTP 400: no data"}

    def _refuse(ticker, _start, _end):
        raise mod.ProviderRejected(reasons[ticker])

    monkeypatch.setattr(mod, "fetch_td", _refuse)

    assert mod.main() == 0

    out = capsys.readouterr()
    assert "fetched_ok=0/4" in out.out and "failed=0 skipped=4" in out.out
    for ticker, reason in reasons.items():
        assert f"skipped {ticker}: {reason}" in out.out
    assert "total failure" not in out.err
    # Nothing was fetched, so nothing is written or stamped as pulled.
    assert not any("INSERT INTO ticker_metrics_daily" in sql for sql in fake_conn.cursor_obj.executed)
    assert not any("UPDATE source_catalog" in sql for sql in fake_conn.cursor_obj.executed)


def test_refused_symbols_do_not_hide_real_fetch_failures(monkeypatch, capsys):
    mod = _module(monkeypatch)
    _install_fake_db(monkeypatch, work_rows=_rows("GLEN", "AAA", "BBB"))

    def _fetch(ticker, _start, _end):
        if ticker == "GLEN":
            raise mod.ProviderRejected("HTTP 404: plan")
        return []

    monkeypatch.setattr(mod, "fetch_td", _fetch)

    assert mod.main() == 1

    out = capsys.readouterr()
    assert "failed=2 skipped=1" in out.out
    assert "failed tickers: ['AAA', 'BBB']" in out.out
    assert "total failure: 0/2 attempted tickers had a successful fetch (1 skipped)" in out.err


def test_refused_symbols_beside_successful_fetches_exit_zero_and_write_the_bars(monkeypatch, capsys):
    mod = _module(monkeypatch)
    fake_conn = _install_fake_db(monkeypatch, work_rows=_rows("AAA", "WBD", "BBB"))

    def _fetch(ticker, _start, _end):
        if ticker == "WBD":
            raise mod.ProviderRejected("HTTP 400: no data")
        return [BAR] if ticker == "AAA" else []

    monkeypatch.setattr(mod, "fetch_td", _fetch)

    assert mod.main() == 0

    out = capsys.readouterr().out
    assert "fetched_ok=1/3" in out and "failed=1 skipped=1" in out
    assert "failed tickers: ['BBB']" in out and "skipped WBD: HTTP 400: no data" in out
    assert sum("INSERT INTO ticker_metrics_daily" in sql for sql in fake_conn.cursor_obj.executed) == 1
    assert any("UPDATE source_catalog" in sql for sql in fake_conn.cursor_obj.executed)
