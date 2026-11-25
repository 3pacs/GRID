"""Schwab-row filter on real PostgreSQL (store/options_provenance.py, #1322).

The fragments that SQLite cannot run (``LATERAL`` for each ticker's latest
non-Schwab day) and the GEX read that picks a batch and then loads its chain
are exercised here end to end, in a disposable schema. Skips unless
GRID_TEST_DB_URL points at a local throwaway database; with
REQUIRE_OPTIONS_PROVENANCE_PG=1 a missing URL fails instead (CI).
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

from store import options_provenance as op

_REQUIRE_ENV = "REQUIRE_OPTIONS_PROVENANCE_PG"
UTC = timezone.utc
FRI = date(2026, 11, 20)     # Cboe capture only
MON = date(2026, 11, 23)     # Cboe capture, then a Schwab capture
CUTOFF = datetime(2026, 11, 24, 2, 0, tzinfo=UTC)

DDL = (
    """CREATE TABLE options_capture_batches (
        capture_batch_id TEXT PRIMARY KEY, ticker TEXT NOT NULL, snap_date DATE NOT NULL,
        capture_ordinal BIGINT NOT NULL, capture_started_at TIMESTAMPTZ NOT NULL,
        capture_completed_at TIMESTAMPTZ NOT NULL, row_count INTEGER NOT NULL,
        spot_price DOUBLE PRECISION, provider_regular_market_at TIMESTAMPTZ, spot_provider TEXT,
        clock_metadata JSONB, capture_source TEXT NOT NULL DEFAULT 'daily_scheduler',
        backfilled BOOLEAN NOT NULL DEFAULT false, registered_at TIMESTAMPTZ NOT NULL,
        UNIQUE (ticker, snap_date, capture_ordinal))""",
    """CREATE TABLE options_snapshots_all (
        id BIGSERIAL PRIMARY KEY, ticker TEXT NOT NULL, snap_date DATE NOT NULL, expiry DATE NOT NULL,
        opt_type TEXT NOT NULL, strike DOUBLE PRECISION NOT NULL, last_price DOUBLE PRECISION,
        bid DOUBLE PRECISION, ask DOUBLE PRECISION, volume INTEGER, open_interest INTEGER,
        implied_vol DOUBLE PRECISION, in_the_money BOOLEAN, created_at TIMESTAMPTZ DEFAULT NOW(),
        capture_batch_id TEXT, capture_ordinal BIGINT, capture_started_at TIMESTAMPTZ,
        capture_completed_at TIMESTAMPTZ, provider_regular_market_at TIMESTAMPTZ, clock_metadata JSONB)""",
    # As in migrations/versions/options_append_only_20260930.py.
    """CREATE VIEW options_snapshots AS
        SELECT s.id, s.ticker, s.snap_date, s.expiry, s.opt_type, s.strike, s.last_price, s.bid, s.ask,
               s.volume, s.open_interest, s.implied_vol, s.in_the_money, s.created_at,
               s.capture_batch_id, s.capture_ordinal, s.capture_started_at, s.capture_completed_at,
               s.provider_regular_market_at
        FROM options_snapshots_all s
        WHERE NOT EXISTS (SELECT 1 FROM options_capture_batches b
            WHERE b.ticker = s.ticker AND b.snap_date = s.snap_date
              AND b.capture_batch_id IS DISTINCT FROM s.capture_batch_id
              AND b.capture_ordinal >= COALESCE(s.capture_ordinal, 0))""",
    """CREATE TABLE options_daily_signals (
        id BIGSERIAL PRIMARY KEY, ticker TEXT NOT NULL, signal_date DATE NOT NULL,
        put_call_ratio DOUBLE PRECISION, max_pain DOUBLE PRECISION, iv_skew DOUBLE PRECISION,
        total_oi BIGINT, total_volume BIGINT, near_expiry DATE, spot_price DOUBLE PRECISION,
        iv_atm DOUBLE PRECISION, wing_iv_metadata JSONB, created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (ticker, signal_date))""",
)


@pytest.fixture(scope="module")
def pg():
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        if os.environ.get(_REQUIRE_ENV) == "1":
            pytest.fail(f"GRID_TEST_DB_URL is required ({_REQUIRE_ENV}=1)")
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("options provenance PG test requires a local disposable test database")
    schema = "schwab_prov_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text("CREATE SCHEMA " + schema))  # generated identifier, not input
    engine = create_engine(url, connect_args={"options": "-csearch_path=" + schema})
    with engine.begin() as conn:
        for ddl in DDL:
            conn.execute(text(ddl))
        _seed(conn)
    yield engine
    engine.dispose()
    with admin.begin() as conn:
        conn.execute(text("DROP SCHEMA " + schema + " CASCADE"))
    admin.dispose()


def _at(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=UTC)


def _batch(conn, batch_id: str, ticker: str, day: date, ordinal: int, provider: str | None) -> None:
    started, completed = _at(day, 21, 20), _at(day, 21, 30)
    quote = _at(day, 20, 59)  # 15:59 New York
    conn.execute(text(
        "INSERT INTO options_capture_batches (capture_batch_id, ticker, snap_date, capture_ordinal, "
        "capture_started_at, capture_completed_at, row_count, spot_price, provider_regular_market_at, "
        "spot_provider, clock_metadata, registered_at) VALUES (:id, :t, :d, :o, :s, :c, 4, 600, :q, :p, "
        "CAST(:m AS JSONB), :c)"),
        {"id": batch_id, "t": ticker, "d": day, "o": ordinal, "s": started, "c": completed, "q": quote,
         "p": provider, "m": json.dumps({"provider": provider}) if provider else None})
    for opt_type, strike, oi in (("call", 610.0, 900 + ordinal), ("call", 620.0, 500),
                                 ("put", 590.0, 800), ("put", 580.0, 400)):
        conn.execute(text(
            "INSERT INTO options_snapshots_all (ticker, snap_date, expiry, opt_type, strike, open_interest, "
            "implied_vol, created_at, capture_batch_id, capture_ordinal, capture_started_at, "
            "capture_completed_at, provider_regular_market_at) VALUES (:t, :d, :e, :ot, :k, :oi, 0.2, :cr, "
            ":id, :o, :s, :c, :q)"),
            {"t": ticker, "d": day, "e": day + timedelta(days=14), "ot": opt_type, "k": strike, "oi": oi,
             "cr": _at(day, 21, 25), "id": batch_id, "o": ordinal, "s": started, "c": completed, "q": quote})


def _signal(conn, ticker: str, day: date, spot: float, source: str | None) -> None:
    conn.execute(text(
        "INSERT INTO options_daily_signals (ticker, signal_date, put_call_ratio, total_oi, spot_price, "
        "iv_atm, wing_iv_metadata) VALUES (:t, :d, 0.9, 5000, :spot, 0.2, CAST(:w AS JSONB))"),
        {"t": ticker, "d": day, "spot": spot, "w": json.dumps({"source": source}) if source else None})


BATCH_FRI = "11111111-1111-4111-8111-111111111111"
BATCH_MON_CBOE = "22222222-2222-4222-8222-222222222222"
BATCH_MON_SCHWAB = "33333333-3333-4333-8333-333333333333"


def _seed(conn) -> None:
    _batch(conn, BATCH_FRI, "SPY", FRI, 1, "cboe")
    _batch(conn, BATCH_MON_CBOE, "SPY", MON, 1, "cboe")
    _batch(conn, BATCH_MON_SCHWAB, "SPY", MON, 2, "schwab")
    _signal(conn, "SPY", FRI, 590.0, "cboe")
    _signal(conn, "SPY", MON, 600.0, "schwab")
    _signal(conn, "NUOX", MON, 7.0, "yahoo")
    # No batch and no wing metadata, dated after the cutoff: written by a
    # writer that registers no batch (scripts/pull_surfacer_options.py), so
    # it is not Schwab and stays visible.
    _signal(conn, "QQQ", MON, 500.0, None)


def _rows(pg, sql: str, include: bool) -> list[tuple]:
    with pg.connect() as conn:
        return [tuple(r) for r in conn.execute(op.visible_sql(sql, include)).fetchall()]


def test_latest_day_cross_section_gives_each_ticker_its_latest_non_schwab_day(pg) -> None:
    sql = "SELECT ticker, signal_date, spot_price FROM " + op.LATEST_DAILY_SIGNALS + " ods ORDER BY ticker"
    assert _rows(pg, sql, True) == [("NUOX", MON, 7.0), ("QQQ", MON, 500.0), ("SPY", MON, 600.0)]
    assert _rows(pg, sql, False) == [("NUOX", MON, 7.0), ("QQQ", MON, 500.0), ("SPY", FRI, 590.0)]


def test_latest_row_per_ticker_skips_schwab_days(pg) -> None:
    sql = ("SELECT DISTINCT ON (ticker) ticker, signal_date FROM " + op.LATEST_SIGNAL_EACH_TICKER
           + " ods ORDER BY ticker, signal_date DESC")
    assert _rows(pg, sql, True) == [("NUOX", MON), ("QQQ", MON), ("SPY", MON)]
    assert _rows(pg, sql, False) == [("NUOX", MON), ("QQQ", MON), ("SPY", FRI)]


def test_signal_rows_without_a_batch_stay_visible_unless_metadata_names_schwab(pg) -> None:
    sql = "SELECT ticker, signal_date FROM " + op.VISIBLE_DAILY_SIGNALS + " ods ORDER BY ticker, signal_date"
    assert _rows(pg, sql, False) == [("NUOX", MON), ("QQQ", MON), ("SPY", FRI)]
    with pg.begin() as conn:
        _signal(conn, "IWM", MON, 230.0, "schwab")
    try:
        assert ("IWM", MON) not in _rows(pg, sql, False)
        assert ("IWM", MON) in _rows(pg, sql, True)
    finally:
        with pg.begin() as conn:
            conn.execute(text("DELETE FROM options_daily_signals WHERE ticker = 'IWM'"))


def test_admin_statement_is_the_plain_table() -> None:
    sql = "SELECT spot_price FROM " + op.VISIBLE_DAILY_SIGNALS + " ods WHERE ticker = :t"
    assert op.visible_sql_text(sql, True) == "SELECT spot_price FROM options_daily_signals ods WHERE ticker = :t"


def test_latest_gex_selects_public_batch_on_mixed_day_and_skips_schwab_only_day(pg, monkeypatch) -> None:
    from physics import dealer_gamma
    from physics.dealer_gamma import DealerGammaEngine

    monkeypatch.setattr(dealer_gamma, "_utc_now", lambda: CUTOFF)

    def receipt(self, ticker, chain_completed_at):
        day = chain_completed_at.astimezone(UTC).date() - timedelta(days=1)
        return {"price": 600.0, "obs_date": day, "available_at": _at(day, 22),
                "receipt_created_at": _at(day, 22, 5), "release_date": day, "vintage_date": day,
                "receipt_id": "r-" + day.isoformat()}

    monkeypatch.setattr(DealerGammaEngine, "_get_spot_receipt", receipt)

    # On MON (mixed Cboe + Schwab), admin sees the latest Schwab batch, while
    # public readers select and load the verified same-day Cboe batch (never Schwab).
    admin = DealerGammaEngine(pg, include_schwab=True).compute_latest_completed_gex_profile("SPY")
    public = DealerGammaEngine(pg).compute_latest_completed_gex_profile("SPY")

    assert not admin.get("error"), admin
    assert (admin["chain_snap_date"], admin["chain_batch_id"]) == (MON.isoformat(), BATCH_MON_SCHWAB)
    assert not public.get("error"), public
    assert (public["chain_snap_date"], public["chain_batch_id"]) == (MON.isoformat(), BATCH_MON_CBOE)
    assert public["gex_aggregate"] is not None

    # Explicit replay of the Schwab batch still fails closed for public readers.
    denied = DealerGammaEngine(pg).compute_gex_profile("SPY", MON, capture_batch_id=BATCH_MON_SCHWAB, cutoff=CUTOFF)
    assert denied.get("error")

    # On a Schwab-only day (TUE), public readers skip TUE and fall back to MON's Cboe batch.
    tue = date(2026, 11, 24)
    batch_tue_schwab = "44444444-4444-4444-8444-444444444444"
    tue_cutoff = datetime(2026, 11, 25, 2, 0, tzinfo=UTC)
    monkeypatch.setattr(dealer_gamma, "_utc_now", lambda: tue_cutoff)
    with pg.begin() as conn:
        _batch(conn, batch_tue_schwab, "SPY", tue, 1, "schwab")
    try:
        admin_tue = DealerGammaEngine(pg, include_schwab=True).compute_latest_completed_gex_profile("SPY")
        public_tue = DealerGammaEngine(pg).compute_latest_completed_gex_profile("SPY")
        assert (admin_tue["chain_snap_date"], admin_tue["chain_batch_id"]) == (tue.isoformat(), batch_tue_schwab)
        assert (public_tue["chain_snap_date"], public_tue["chain_batch_id"]) == (MON.isoformat(), BATCH_MON_CBOE)
    finally:
        with pg.begin() as conn:
            conn.execute(text("DELETE FROM options_snapshots_all WHERE capture_batch_id = :b"), {"b": batch_tue_schwab})
            conn.execute(text("DELETE FROM options_capture_batches WHERE capture_batch_id = :b"), {"b": batch_tue_schwab})
