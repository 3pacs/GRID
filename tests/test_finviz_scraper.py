"""Tests for ingestion/altdata/finviz_scraper.py.

tests/fixtures/finviz/stock_AAPL_20260929.html is the real
https://finviz.com/stock?t=AAPL page recorded from grid-svr on 2026-09-29
(one request, honest User-Agent). No test touches the network.
"""

from __future__ import annotations

import time
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

import ingestion.altdata.finviz_scraper as mod
from ingestion.altdata.finviz_scraper import (
    DEFAULT_TICKERS,
    FinvizScraperPuller,
    parse_snapshot_table,
)

AAPL_HTML = (
    Path(__file__).parent / "fixtures" / "finviz" / "stock_AAPL_20260929.html"
).read_text(encoding="utf-8")


def test_parse_current_layout() -> None:
    pairs = parse_snapshot_table(AAPL_HTML)
    assert pairs["P/E"] == "38.79"
    assert pairs["EPS (ttm)"] == "8.72"
    assert pairs["Market Cap"] == "4938.67B"
    assert pairs["Sales"] == "466.82B"
    # value wrapped in a colour <span>
    assert pairs["ROE"] == "148.75%"
    assert pairs["Debt/Eq"] == "0.78"
    assert pairs["Beta"] == "1.07"
    # value followed by a <small> percentage: first token only
    assert pairs["52W High"] == "345.34"


def test_parse_legacy_layout_still_supported() -> None:
    legacy = (
        '<td class="snapshot-td2 cursor-pointer w-[7%]" align="left">P/E</td>'
        '<td class="snapshot-td2 w-[8%]" align="left"><b>31.2</b></td>'
    )
    assert parse_snapshot_table(legacy) == {"P/E": "31.2"}


def test_parse_returns_empty_on_unknown_layout() -> None:
    assert parse_snapshot_table("<html><body>Just a moment...</body></html>") == {}


# ---------------------------------------------------------------------------
# Puller behaviour with a fake session + mock engine
# ---------------------------------------------------------------------------


class _Resp:
    def __init__(self, text: str, status: int = 200) -> None:
        self.text = text
        self.status_code = status
        self.response = self

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            err = requests.HTTPError(f"{self.status_code} Client Error")
            err.response = self  # type: ignore[attr-defined]
            raise err


class _Session:
    def __init__(self, resp_for) -> None:  # noqa: ANN001
        self.resp_for = resp_for
        self.headers: dict[str, str] = {}
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, params: dict | None = None, timeout: int = 0) -> _Resp:  # noqa: ARG002
        self.calls.append((url, dict(params or {})))
        return self.resp_for(params["t"])


def _engine() -> MagicMock:
    engine = MagicMock()
    cconn = MagicMock()
    cconn.execute.return_value.fetchone.return_value = (731,)
    engine.connect.return_value.__enter__ = MagicMock(return_value=cconn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    bconn = MagicMock()
    bconn.execute.return_value.fetchall.return_value = []
    bconn.execute.return_value.fetchone.return_value = None
    engine.begin.return_value.__enter__ = MagicMock(return_value=bconn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    engine._bconn = bconn
    return engine


def _inserts(engine: MagicMock) -> list[dict]:
    return [
        c.args[1]
        for c in engine._bconn.execute.call_args_list
        if "INSERT INTO raw_series" in str(c.args[0])
    ]


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    monkeypatch.setattr(time, "sleep", lambda s: None)


def test_pull_ticker_writes_only_numeric_fields() -> None:
    engine = _engine()
    session = _Session(lambda t: _Resp(AAPL_HTML))
    puller = FinvizScraperPuller(engine, session=session)
    result = puller.pull_ticker("AAPL")

    assert session.calls[0][0] == "https://finviz.com/stock"
    assert session.calls[0][1] == {"t": "AAPL"}
    assert result["status"] == "SUCCESS"
    rows = _inserts(engine)
    by_sid = {r["sid"]: r["val"] for r in rows}
    assert by_sid == {
        "finviz.AAPL.pe_ratio": 38.79,
        "finviz.AAPL.eps_ttm": 8.72,
        "finviz.AAPL.market_cap": pytest.approx(4938.67e9),
        "finviz.AAPL.revenue": pytest.approx(466.82e9),
        "finviz.AAPL.roe": 148.75,
        "finviz.AAPL.debt_equity": 0.78,
        "finviz.AAPL.beta": 1.07,
    }
    for r in rows:
        assert r["od"] == date.today()
        assert r["status"] == "SUCCESS"
        assert "pull_timestamp" not in r  # column default = fetch time
    # No placeholder 0.0 for text fields.
    assert not any(s.endswith((".sector", ".industry")) for s in by_sid)


def test_pull_raises_when_no_ticker_parses() -> None:
    """0-row runs used to return SUCCESS; now the group runner sees an exception."""
    engine = _engine()
    session = _Session(lambda t: _Resp("<html>layout changed</html>"))
    puller = FinvizScraperPuller(engine, session=session)
    with pytest.raises(RuntimeError, match=r"0/\d+ tickers parsed"):
        puller.pull()
    assert _inserts(engine) == []


def test_pull_raises_when_every_fetch_is_blocked() -> None:
    engine = _engine()
    session = _Session(lambda t: _Resp("forbidden", 403))
    puller = FinvizScraperPuller(engine, session=session)
    with pytest.raises(RuntimeError):
        puller.pull()
    assert _inserts(engine) == []
    # 403 is non-retryable: exactly one request per ticker.
    assert len(session.calls) == len(DEFAULT_TICKERS)


def test_pull_partial_when_some_tickers_fail() -> None:
    engine = _engine()
    session = _Session(lambda t: _Resp(AAPL_HTML) if t == "AAPL" else _Resp("x", 404))
    puller = FinvizScraperPuller(engine, session=session)
    result = puller.pull()
    assert result["status"] == "PARTIAL"
    assert result["tickers_ok"] == 1
    assert result["rows_inserted"] == 7
