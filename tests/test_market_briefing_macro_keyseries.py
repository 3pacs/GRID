"""key_series ids in MarketBriefingEngine._gather_market_snapshot.

FRED series are stored bare in raw_series (``DFF``, not ``FRED:DFF``), so the
``rates`` lookups used to miss and the rates/macro sections rendered empty.
Offline: a fake connection records the ``sid`` of every raw_series lookup.
"""
from __future__ import annotations

import os
from datetime import date

os.environ.setdefault("DB_PASSWORD", "test-password")

from ollama.market_briefing import MarketBriefingEngine

RATES = ["DFF", "T10Y2Y", "T10Y3M", "DGS2", "DGS5"]
MACRO = ["CPIAUCSL", "PCEPILFE", "UNRATE", "RSAFS", "UMCSENT", "T5YIE"]


class _Result:
    def __init__(self, one=None, many=None):
        self._one, self._many = one, many or []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many


class _Conn:
    def __init__(self, rows):
        self.rows = rows
        self.sids: list[str] = []

    def execute(self, stmt, params=None):
        sql, params = str(stmt), params or {}
        if "FROM raw_series" in sql and "sid" in params:
            self.sids.append(params["sid"])
            return _Result(one=self.rows.get(params["sid"]))
        return _Result()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Engine:
    def __init__(self, conn):
        self._conn = conn

    def connect(self):
        return self._conn


class _Ollama:
    is_available = False


def _engine(conn, tmp_path):
    e = MarketBriefingEngine(ollama_client=_Ollama(), db_engine=_Engine(conn))
    e.output_dir = tmp_path
    return e


def test_key_series_uses_bare_fred_ids_and_includes_macro(tmp_path):
    conn = _Conn({})
    _engine(conn, tmp_path)._gather_market_snapshot()
    for sid in RATES + MACRO:
        assert sid in conn.sids, sid
    assert not any(s.startswith("FRED:") for s in conn.sids)
    assert "DGS10" not in conn.sids  # not in FRED_SERIES_LIST


def test_bare_fred_rows_populate_rates_and_macro(tmp_path):
    rows = {sid: (4.5, date(2026, 10, 16)) for sid in RATES + MACRO}
    snap = _engine(_Conn(rows), tmp_path)._gather_market_snapshot()
    assert set(snap["rates"]) == set(RATES)
    assert set(snap["macro"]) == set(MACRO)
    assert snap["macro"]["CPIAUCSL"] == {"value": 4.5, "date": "2026-10-16"}


def test_macro_series_are_in_fred_series_list():
    from ingestion.fred import FRED_SERIES_LIST

    for sid in RATES + MACRO:
        assert sid in FRED_SERIES_LIST, sid


def _snapshot_with_macro():
    return {
        "timestamp": "2026-10-19T06:00:00", "date": "2026-10-19",
        "equities": {}, "equity_volume": {}, "rates": {}, "credit": {},
        "volatility": {}, "commodities": {}, "fx": {},
        "macro": {"UNRATE": {"value": 4.3, "date": "2026-09-01"}},
    }


def test_build_data_context_renders_macro(tmp_path):
    ctx = _engine(_Conn({}), tmp_path)._build_data_context(_snapshot_with_macro())
    assert "### MACRO" in ctx
    assert "macro:UNRATE" in ctx and "freshness=unknown" in ctx


def test_fallback_briefing_renders_macro(tmp_path):
    text = _engine(_Conn({}), tmp_path)._generate_fallback_briefing(_snapshot_with_macro())
    assert "## Macro" in text
    assert "macro:UNRATE" in text and "freshness=unknown" in text
