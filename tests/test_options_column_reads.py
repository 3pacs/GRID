"""Reads that named columns the schema never had (review of #1322, 2026-11-21).

Five reads failed on PostgreSQL before and after #1322 because they selected
columns that do not exist, and every caller swallowed the error, so the
features returned nothing:

* ``api/routers/derivatives.py`` vol surface and skew: ``implied_volatility``
  on the ``options_snapshots`` view (the column is ``implied_vol``);
* ``analysis/money_flow.py`` whale flow: ``pcr`` (``put_call_ratio``);
* ``api/routers/chat.py`` options context: ``signal_type`` / ``direction`` /
  ``confidence`` / ``detail`` on ``options_daily_signals`` (none exist);
* ``intelligence/news_impact.py`` moves and returns: ``obs_date`` /
  ``close_price`` on ``options_daily_signals`` (now read from the price log).

The tables here are built from ``schema.sql`` itself, never a hand copy (the
SQLite fixture of ``tests/test_options_provenance.py`` once aliased
``implied_vol AS implied_volatility`` into its view, which is how the bad
column passed). Each test calls the real reader and fails if it comes back
empty or with an error, so a wrong column name fails here.

Two backends: SQLite always (offline), and PostgreSQL 15 when
``GRID_TEST_DB_URL`` points at a local disposable database, where the
``schema.sql`` blocks run verbatim. ``REQUIRE_OPTIONS_PROVENANCE_PG=1`` turns a
missing URL into a failure (CI). No provider, network or LLM access.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = (ROOT / "schema.sql").read_text(encoding="utf-8")
_REQUIRE_ENV = "REQUIRE_OPTIONS_PROVENANCE_PG"

PUBLIC_DAY = date(2026, 11, 20)   # Cboe capture: every viewer
SCHWAB_DAY = date(2026, 11, 23)   # Schwab capture: admin requests only


# ── schema.sql, as written ─────────────────────────────────────────────


def _block(start: str, end: str) -> str:
    i = SCHEMA.index(start)
    return SCHEMA[i:SCHEMA.index(end, i) + len(end)]


def _pg_ddl() -> list[str]:
    """The schema.sql statements themselves (the options store is its own DO block)."""
    return [
        _block("CREATE TABLE IF NOT EXISTS source_catalog (", "\n);"),
        _block("CREATE TABLE IF NOT EXISTS raw_series (", "\n);"),
        _block("DO $options_store$", "$options_store$;"),
        _block("CREATE TABLE IF NOT EXISTS options_daily_signals (", "\n);"),
    ]


_SQLITE_TYPES = {"BIGSERIAL": "INTEGER", "SERIAL": "INTEGER", "JSONB": "TEXT", "DOUBLE": "REAL"}
_NOT_A_COLUMN = {"CHECK", "CONSTRAINT", "UNIQUE", "PRIMARY", "FOREIGN"}


def _split_top_level(body: str) -> list[str]:
    parts, depth, cur = [], 0, []
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    parts.append("".join(cur))
    return parts


def _sqlite_table(create: str) -> str:
    """A schema.sql CREATE TABLE with its column names and types, constraints dropped."""
    create = re.sub(r"--[^\n]*", "", create)
    name = re.search(r"CREATE TABLE (?:IF NOT EXISTS )?(\w+)", create).group(1)
    body = create[create.index("(") + 1:create.rindex(")")]
    cols = []
    for item in _split_top_level(body):
        tokens = item.split()
        if not tokens or tokens[0].upper() in _NOT_A_COLUMN:
            continue
        col, typ = tokens[0], tokens[1].upper()
        cols.append(f"{col} {_SQLITE_TYPES.get(typ, typ)}"
                    + (" PRIMARY KEY" if typ in {"BIGSERIAL", "SERIAL"} else ""))
    return f"CREATE TABLE {name} ({', '.join(cols)})"


def _sqlite_ddl() -> list[str]:
    store = _block("DO $options_store$", "$options_store$;")
    view = re.search(r"CREATE VIEW options_snapshots AS.*?\n    \);", store, re.DOTALL).group(0)
    return [
        _sqlite_table(_block("CREATE TABLE IF NOT EXISTS source_catalog (", "\n);")),
        _sqlite_table(_block("CREATE TABLE IF NOT EXISTS raw_series (", "\n);")),
        _sqlite_table(re.search(r"CREATE TABLE options_snapshots_all \(.*?\n    \);", store, re.DOTALL).group(0)),
        _sqlite_table(re.search(r"CREATE TABLE options_capture_batches \(.*?\n    \);", store, re.DOTALL).group(0)),
        view.replace("IS DISTINCT FROM", "IS NOT").rstrip(";"),
        _sqlite_table(_block("CREATE TABLE IF NOT EXISTS options_daily_signals (", "\n);")),
    ]


def test_schema_has_the_columns_the_fixed_reads_use() -> None:
    """The shape the readers rely on, read from schema.sql (no database needed)."""
    store = _block("DO $options_store$", "$options_store$;")
    view = re.search(r"CREATE VIEW options_snapshots AS(.*?)FROM", store, re.DOTALL).group(1)
    view_cols = set(re.findall(r"s\.(\w+)", view))
    assert {"implied_vol", "open_interest", "volume", "opt_type", "strike", "expiry"} <= view_cols
    assert "implied_volatility" not in view_cols
    signals = _block("CREATE TABLE IF NOT EXISTS options_daily_signals (", "\n);")
    signal_cols = {i.split()[0] for i in _split_top_level(signals[signals.index("(") + 1:-2]) if i.split()}
    assert {"put_call_ratio", "iv_atm", "iv_skew", "max_pain", "spot_price", "signal_date"} <= signal_cols
    assert not {"pcr", "signal_type", "direction", "confidence", "detail", "obs_date",
                "close_price"} & signal_cols


# ── Backends ───────────────────────────────────────────────────────────


def _sqlite_engine():
    sqlite3.register_converter("DATE", lambda raw: date.fromisoformat(raw.decode()))
    sqlite3.register_converter("TIMESTAMPTZ", lambda raw: datetime.fromisoformat(raw.decode()))
    engine = create_engine("sqlite://", connect_args={"detect_types": sqlite3.PARSE_DECLTYPES})
    with engine.begin() as conn:
        for ddl in _sqlite_ddl():
            conn.execute(text(ddl))
    return engine, None


def _pg_engine():
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        if os.environ.get(_REQUIRE_ENV) == "1":
            pytest.fail(f"GRID_TEST_DB_URL is required ({_REQUIRE_ENV}=1)")
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("options column PG test requires a local disposable test database")
    schema = "opt_cols_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text("CREATE SCHEMA " + schema))  # generated identifier, not input
    engine = create_engine(url, connect_args={"options": "-csearch_path=" + schema})
    with engine.begin() as conn:
        for ddl in _pg_ddl():
            conn.execute(text(ddl))

    def drop() -> None:
        with admin.begin() as c:
            c.execute(text("DROP SCHEMA " + schema + " CASCADE"))
        admin.dispose()
    return engine, drop


def _json(conn: Any) -> str:
    return "CAST(:m AS JSONB)" if conn.dialect.name == "postgresql" else ":m"


def _at(day: date, hour: int) -> datetime:
    return datetime(day.year, day.month, day.day, hour, tzinfo=UTC)


def _batch(conn: Any, batch_id: str, day: date, provider: str, iv: float) -> None:
    conn.execute(text(
        "INSERT INTO options_capture_batches (capture_batch_id, ticker, snap_date, capture_ordinal, "
        "capture_started_at, capture_completed_at, row_count, spot_price, provider_regular_market_at, "
        "spot_provider, clock_metadata, capture_source, backfilled, registered_at) "
        "VALUES (:id, 'SPY', :d, 1, :s, :c, 4, 600, :q, :p, " + _json(conn) + ", 'daily_scheduler', "
        ":bf, :c)"),
        {"id": batch_id, "d": day, "s": _at(day, 21), "c": _at(day, 22), "q": _at(day, 20), "p": provider,
         "m": json.dumps({"provider": provider}), "bf": False})
    for expiry_days in (7, 30):
        for opt_type, skew in (("call", 0.0), ("put", 0.05)):
            conn.execute(text(
                "INSERT INTO options_snapshots_all (ticker, snap_date, expiry, opt_type, strike, volume, "
                "open_interest, implied_vol, capture_batch_id, capture_ordinal, capture_started_at, "
                "capture_completed_at, provider_regular_market_at) VALUES ('SPY', :d, :e, :ot, 600, 10, "
                "500, :iv, :id, 1, :s, :c, :q)"),
                {"d": day, "e": day + timedelta(days=expiry_days), "ot": opt_type, "iv": iv + skew,
                 "id": batch_id, "s": _at(day, 21), "c": _at(day, 22), "q": _at(day, 20)})


def _signal(conn: Any, day: date, pcr: float, source: str) -> None:
    conn.execute(text(
        "INSERT INTO options_daily_signals (ticker, signal_date, put_call_ratio, max_pain, iv_skew, "
        "total_oi, total_volume, spot_price, iv_atm, wing_iv_metadata) "
        "VALUES ('SPY', :d, :pcr, 595, 0.03, 5000, 900, 600, 0.2, " + _json(conn) + ")"),
        {"d": day, "pcr": pcr, "m": json.dumps({"source": source})})


SOURCES = {"TIINGO": 1, "yfinance": 2}


def _seed_sources(conn: Any) -> None:
    for name, sid in SOURCES.items():
        conn.execute(text(
            "INSERT INTO source_catalog (id, name, base_url, cost_tier, latency_class, pit_available, "
            "revision_behavior, trust_score, priority_rank, active, uptime_score) "
            "VALUES (:id, :n, 'https://example.invalid', 'FREE', 'EOD', :f, 'RARE', 'HIGH', :id, :t, 1.0)"),
            {"id": sid, "n": name, "f": False, "t": True})


def _close(conn: Any, series_id: str, source: str, day: date, value: float,
           status: str = "SUCCESS", pulled_hour: int = 22) -> None:
    conn.execute(text(
        "INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, pull_status) "
        "VALUES (:s, :src, :d, :p, :v, :st)"),
        {"s": series_id, "src": SOURCES[source], "d": day, "p": _at(day, pulled_hour), "v": value,
         "st": status})


@pytest.fixture(params=["sqlite", "pg"])
def db(request):
    engine, drop = _sqlite_engine() if request.param == "sqlite" else _pg_engine()
    with engine.begin() as conn:
        _batch(conn, "spy-cboe", PUBLIC_DAY, "cboe", 0.20)
        _batch(conn, "spy-schwab", SCHWAB_DAY, "schwab", 0.50)
        _signal(conn, PUBLIC_DAY, 0.4, "cboe")
        _signal(conn, SCHWAB_DAY, 2.0, "schwab")
        _seed_sources(conn)
    yield engine
    engine.dispose()
    if drop is not None:
        drop()


# ── api/routers/derivatives.py ─────────────────────────────────────────


def test_vol_surface_reads_implied_vol_and_hides_schwab_days(db, monkeypatch) -> None:
    from api.routers import derivatives

    monkeypatch.setattr(derivatives, "get_db_engine", lambda: db)
    public = derivatives.get_vol_surface("spy", include_schwab=False)
    assert "error" not in public, public
    assert public["snap_date"] == PUBLIC_DAY.isoformat() and public["count"] == 4
    assert sorted(p["iv"] for p in public["surface"]) == pytest.approx([0.20, 0.20, 0.25, 0.25])
    admin = derivatives.get_vol_surface("spy", include_schwab=True)
    assert "error" not in admin, admin
    assert admin["snap_date"] == SCHWAB_DAY.isoformat()
    assert sorted(p["iv"] for p in admin["surface"]) == pytest.approx([0.50, 0.50, 0.55, 0.55])


def test_skew_reads_implied_vol_and_hides_schwab_days(db, monkeypatch) -> None:
    from api.routers import derivatives

    monkeypatch.setattr(derivatives, "get_db_engine", lambda: db)
    public = derivatives.get_skew("spy", include_schwab=False)
    assert "error" not in public, public
    assert public["snap_date"] == PUBLIC_DAY.isoformat() and len(public["skew"]) == 2
    for expiry in public["skew"]:
        assert expiry["strikes"] == [{"strike": 600.0, "call_iv": pytest.approx(0.20),
                                      "put_iv": pytest.approx(0.25)}]
    admin = derivatives.get_skew("spy", include_schwab=True)
    assert admin["snap_date"] == SCHWAB_DAY.isoformat()
    assert admin["skew"][0]["strikes"][0]["call_iv"] == pytest.approx(0.50)


# ── analysis/money_flow.py ─────────────────────────────────────────────


def test_whale_flow_reads_put_call_ratio_point_in_time_and_never_a_schwab_day(db) -> None:
    from analysis.money_flow import _get_whale_flow

    # The Schwab day (PCR 2.0, "put_heavy") is the latest; every reader of the
    # money-flow map gets the Cboe day (PCR 0.4) instead.
    assert _get_whale_flow(db, "SPY", SCHWAB_DAY + timedelta(days=1)) == "call_heavy"
    assert _get_whale_flow(db, "SPY", PUBLIC_DAY) == "call_heavy"
    # Nothing dated after as_of is read.
    assert _get_whale_flow(db, "SPY", PUBLIC_DAY - timedelta(days=1)) is None


# ── api/routers/chat.py ────────────────────────────────────────────────


def test_chat_options_context_reads_measured_values_and_never_a_schwab_day(db, monkeypatch) -> None:
    from api.routers import chat

    monkeypatch.setattr(chat, "_get_db_engine", lambda: db)
    for ticker in ("spy", None):
        block, source = chat._gather_options_context(ticker)
        assert source == "options_signals", block
        assert f"SPY {PUBLIC_DAY.isoformat()}: put/call 0.4, ATM IV 0.2, IV skew 0.03, max pain 595" in block
        assert SCHWAB_DAY.isoformat() not in block and "put/call 2" not in block


def test_chat_options_value_format() -> None:
    from api.routers.chat import _fmt_options_value

    assert _fmt_options_value(None) == "n/a"
    assert _fmt_options_value(float("nan")) == "n/a"
    assert _fmt_options_value(float("inf")) == "n/a"
    assert _fmt_options_value(0.912345) == "0.9123"


# ── intelligence/news_impact.py ────────────────────────────────────────


def _decomposer(db):
    from intelligence.news_impact import CatalystClassifier, PriceDecomposer

    return PriceDecomposer(db, CatalystClassifier(db))


def test_news_impact_moves_read_tiingo_adjusted_closes_not_failed_zeros(db) -> None:
    d = [date.today() - timedelta(days=9 - i) for i in range(5)]
    with db.begin() as conn:
        for day, value in zip(d, (100.0, 103.0, 103.5, 99.0, 99.5)):
            _close(conn, "YF:NVDA:adj_close", "TIINGO", day, value)
        _close(conn, "YF:NVDA:adj_close", "TIINGO", d[2], 0.0, status="FAILED", pulled_hour=23)
        # A later vintage of d[4] wins over the first.
        _close(conn, "YF:NVDA:adj_close", "TIINGO", d[4], 101.0, pulled_hour=23)
    moves = _decomposer(db)._find_significant_moves("NVDA", 30, 0.02)
    assert moves == [(d[1], 0.03), (d[3], round(99.0 / 103.5 - 1, 5)), (d[4], round(101.0 / 99.0 - 1, 5))]


def test_news_impact_falls_back_to_yfinance_and_applies_a_recorded_split(db) -> None:
    d = [date.today() - timedelta(days=9 - i) for i in range(4)]
    with db.begin() as conn:
        # As-traded history across a 2-for-1 on d[2]: 100 -> 100.4 -> 50.5 -> 53.0.
        for day, value in zip(d, (100.0, 100.4, 50.5, 53.0)):
            _close(conn, "YF:AMD:close", "yfinance", day, value)
        _close(conn, "YF:AMD:split_factor", "yfinance", d[2], 2.0)
    moves = _decomposer(db)._find_significant_moves("AMD", 30, 0.02)
    # The split is not a -50% move; the +4.95% day after it is.
    assert moves == [(d[3], round(53.0 / 50.5 - 1, 5))]


def test_news_impact_return_is_that_days_close_and_reads_nothing_later(db) -> None:
    day = date(2026, 11, 18)
    with db.begin() as conn:
        for dd, value in ((date(2026, 11, 16), 600.0), (date(2026, 11, 17), 606.0),
                          (day, 612.06), (date(2026, 11, 19), 1.0)):
            _close(conn, "YF:SPY:adj_close", "TIINGO", dd, value)
    dec = _decomposer(db)
    assert dec._get_return("SPY", day) == pytest.approx(0.01)
    assert dec._get_return("SPY", date(2026, 11, 15)) is None   # no close that day
    assert dec._get_return("QQQ", day) is None                  # no closes at all
