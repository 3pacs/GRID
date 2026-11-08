"""analysis.sec_valuation: trailing P/E from filed SEC EPS + Tiingo raw close.

Replaces Tiingo fundamentals' pe_ratio (retired 2026-11-08). Shapes match
rows written by ingestion.altdata.sec_edgar_company (10-Q rows are fiscal
year-to-date; raw_payload.filed is the known-at date). No database.
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from types import SimpleNamespace

import pytest

from analysis import sec_valuation as sv


def _row(end: str, start: str, value: float, filed: str):
    payload = json.dumps({"period_start": start, "period_end": end, "filed": filed})
    return (date.fromisoformat(end), value, payload, None)


# AAPL-shaped fiscal calendar (year ends late September).
AAPL = [
    _row("2024-09-28", "2023-10-01", 6.08, "2024-11-01"),
    _row("2025-06-28", "2024-09-29", 5.62, "2025-08-01"),
    _row("2025-09-27", "2024-09-29", 7.46, "2025-10-31"),
    _row("2025-12-27", "2025-09-28", 2.84, "2026-01-30"),
    _row("2026-03-28", "2025-09-28", 4.85, "2026-05-01"),
    _row("2026-06-27", "2025-09-28", 6.88, "2026-07-31"),
    _row("2026-09-26", "2025-09-28", 8.98, "2026-11-03"),
]


class _Conn:
    def __init__(self, rows):
        self.rows = rows
        self.params = []

    def execute(self, sql, params):
        self.params.append(params)
        rows = [r for r in self.rows if r[0] <= params["as_of"]]
        return SimpleNamespace(fetchall=lambda: rows)


@pytest.fixture
def price(monkeypatch):
    seen = {}

    def fake(conn, series_id, *, source=None, as_of=None, as_of_ts=None):
        seen.update(series_id=series_id, source=source, as_of=as_of)
        return SimpleNamespace(obs_date=as_of - timedelta(days=1), value=342.37)

    monkeypatch.setattr(sv, "read_latest", fake)
    return seen


def test_fiscal_year_end_uses_the_10k_value(price):
    pe = sv.trailing_pe(_Conn(AAPL), "AAPL", date(2026, 11, 8))
    assert pe.eps_ttm == pytest.approx(8.98) and pe.period_end == date(2026, 9, 26)
    assert pe.pe == pytest.approx(342.37 / 8.98)
    assert price == {"series_id": "YF:AAPL:close", "source": "TIINGO", "as_of": date(2026, 11, 8)}


def test_mid_year_is_fy_plus_ytd_minus_prior_ytd(price):
    # Before the 2026-11-03 10-K: latest known period is the Q3 YTD.
    eps, period = sv.trailing_eps(_Conn(AAPL), "AAPL", date(2026, 10, 15))
    assert period.end == date(2026, 6, 27)
    assert eps == pytest.approx(7.46 + 6.88 - 5.62)


def test_filing_not_yet_known_is_ignored():
    # The FY2026 row exists but was filed after as_of: point in time.
    eps, period = sv.trailing_eps(_Conn(AAPL), "AAPL", date(2026, 11, 2))
    assert period.end == date(2026, 6, 27)


def test_missing_prior_year_ytd_is_none():
    rows = [r for r in AAPL if r[0] != date(2025, 6, 28)]
    assert sv.trailing_eps(_Conn(rows), "AAPL", date(2026, 10, 15)) is None


def test_stale_latest_period_is_none():
    assert sv.trailing_eps(_Conn(AAPL), "AAPL", date(2027, 6, 1)) is None


def test_loss_has_no_pe(price):
    rows = [_row("2026-09-26", "2025-09-28", -1.2, "2026-11-03")]
    assert sv.trailing_pe(_Conn(rows), "X", date(2026, 11, 8)) is None


def test_stale_price_has_no_pe(monkeypatch):
    monkeypatch.setattr(sv, "read_latest", lambda *a, **k: SimpleNamespace(
        obs_date=date(2026, 10, 1), value=300.0))
    assert sv.trailing_pe(_Conn(AAPL), "AAPL", date(2026, 11, 8)) is None


def test_no_rows_is_none(price):
    assert sv.trailing_pe(_Conn([]), "V", date(2026, 11, 8)) is None


def test_valuation_compression_reads_sec_pe(monkeypatch):
    from analysis import thesis_scorer as ts

    pes = {"AAPL": 38.0, "MSFT": 28.0, "AMZN": 21.0, "JPM": 14.0}

    def fake(conn, ticker, as_of):
        v = pes.get(ticker)
        return None if v is None else SimpleNamespace(pe=v)

    monkeypatch.setattr(ts, "trailing_pe", fake)

    class Engine:
        def connect(self):
            class C:
                def __enter__(self):
                    return object()

                def __exit__(self, *a):
                    return False
            return C()

    out = ts._score_valuation_compression(Engine(), 0.5)
    assert out["status"] != "no_data"
    assert "avg PE 25.2x" in out["data_point"] and "4 stocks" in out["data_point"]
