"""Tiingo fundamentals retired 2026-11-08 (owner decision).

The plan covers only the DOW 30 (89 of 138 tickers refused with 400). Market
cap and shares come from sec_xbrl_shares (ticker_metrics_daily) for the full
universe. The pull is held in both schedulers, staleness is not alerted, and
readers no longer serve its last rows as current values. No network, no DB.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, timedelta
from types import SimpleNamespace

from ingestion.smart_scheduler import PULLER_REGISTRY, hold_reason_for

AS_OF = date(2026, 11, 8)


def test_pull_is_held_in_the_smart_scheduler():
    entry = next(e for e in PULLER_REGISTRY if e["name"] == "tiingo_fundamentals")
    assert entry["hold_reason"].startswith("Retired 2026-11-08 (owner)")
    assert hold_reason_for("tiingo_fundamentals") == entry["hold_reason"]
    # Tiingo prices and news stay scheduled.
    assert hold_reason_for("tiingo") is None
    assert hold_reason_for("tiingo_news") is None


def test_legacy_scheduler_checks_the_hold():
    from pathlib import Path

    src = Path(__file__).resolve().parents[1].joinpath("ingestion", "scheduler.py").read_text()
    block = src[src.index("# Tiingo fundamentals (daily, paid API)"):]
    block = block[:block.index("except Exception")]
    assert 'hold_reason_for("tiingo_fundamentals")' in block
    assert block.index("hold_reason_for") < block.index("TiingoFundamentalsPuller(db_engine)")


def test_staleness_alert_excludes_it():
    from alerts.cadence import is_excluded

    assert is_excluded("TIINGO_FUNDAMENTALS")


class _Conn:
    def __init__(self, answers):
        self.answers = answers
        self.executed = []

    def execute(self, sql, params=None):
        sql = str(sql)
        self.executed.append((sql, params))
        for needle, row in self.answers.items():
            if needle in sql or needle in str(params):
                return SimpleNamespace(fetchone=lambda r=row: r, fetchall=lambda: [])
        return SimpleNamespace(fetchone=lambda: None, fetchall=lambda: [])


class _Engine:
    def __init__(self, conn):
        self.conn = conn

    @contextmanager
    def connect(self):
        yield self.conn


def test_spot_price_never_uses_a_market_cap(monkeypatch):
    from valuation import derivatives_support as ds

    conn = _Conn({"options_daily_signals": (182.5,)})
    eng = ds.DerivativesSupportEngine(_Engine(conn))
    assert eng._get_spot_price("AAPL", AS_OF) == 182.5
    assert not any("TIINGO_FUND" in str(p) for _, p in conn.executed)
    spot_sql, params = next((s, p) for s, p in conn.executed if "options_daily_signals" in s)
    assert "signal_date <= :as_of" in spot_sql and params["since"] == AS_OF - timedelta(days=ds.SPOT_MAX_AGE_DAYS)


def test_spot_price_falls_back_to_recent_raw_tiingo_close(monkeypatch):
    from valuation import derivatives_support as ds

    seen = {}

    def fake_read_latest(conn, series_id, *, source=None, as_of=None, as_of_ts=None):
        seen.update(series_id=series_id, source=source, as_of=as_of)
        return SimpleNamespace(obs_date=AS_OF - timedelta(days=1), value=181.0)

    monkeypatch.setattr(ds, "read_latest", fake_read_latest)
    eng = ds.DerivativesSupportEngine(_Engine(_Conn({})))
    assert eng._get_spot_price("AAPL", AS_OF) == 181.0
    assert seen == {"series_id": "YF:AAPL:close", "source": "TIINGO", "as_of": AS_OF}

    monkeypatch.setattr(ds, "read_latest", lambda *a, **k: SimpleNamespace(
        obs_date=AS_OF - timedelta(days=30), value=150.0))
    assert eng._get_spot_price("AAPL", AS_OF) is None  # too old to be a spot


def test_intrinsic_market_cap_comes_from_recent_sec_metrics():
    from valuation.intrinsic import MARKET_CAP_MAX_AGE_DAYS, IntrinsicValueEngine

    conn = _Conn({"ticker_metrics_daily": (2.5e12,), "fmp:AAPL:equity": (6.0e10, AS_OF)})
    eng = IntrinsicValueEngine.__new__(IntrinsicValueEngine)
    eng.engine = _Engine(conn)
    inputs = eng.gather_inputs("AAPL", AS_OF)
    assert inputs is not None and inputs.market_cap == 2.5e12
    assert not any("TIINGO_FUND" in str(p) for _, p in conn.executed)
    sql, params = next((s, p) for s, p in conn.executed if "ticker_metrics_daily" in s)
    assert params["since"] == AS_OF - timedelta(days=MARKET_CAP_MAX_AGE_DAYS)
