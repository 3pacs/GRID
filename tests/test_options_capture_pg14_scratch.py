"""Opt-in PostgreSQL 14 check for the staged options capture migration.

Run only against a disposable local database named ``grid_gex_scratch_*``.
The test creates and drops a random schema; it never uses production tables.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from ipaddress import IPv4Address, ip_interface
from threading import Event, Thread
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import URL, make_url

from ingestion import options
from migrations.versions import options_capture_batch_20260924 as migration
from physics.dealer_gamma import DealerGammaEngine


def _yahoo(expirations: list[int], strikes: list[float]):
    class Yahoo:
        def get_options(self, _ticker, _expiry=None):
            rows = [{
                "strike": strike, "volume": 3, "openInterest": 10,
                "impliedVolatility": 0.2, "lastPrice": 2.0,
                "bid": 1.0, "ask": 3.0, "inTheMoney": False,
            } for strike in strikes]
            return {
                "quote": {"regularMarketPrice": 100.0},
                "expirations": expirations, "calls": rows, "puts": rows,
            }

    return Yahoo()


def _puller(engine, yahoo):
    puller = options.OptionsPuller.__new__(options.OptionsPuller)
    puller.engine = engine
    puller._yahoo = yahoo
    puller._push_to_resolved = lambda *_args: None
    return puller


def _scratch_url(dsn: str) -> URL:
    """Require an unambiguous TCP loopback route before libpq can connect."""
    url = make_url(dsn)
    if (url.drivername != "postgresql+psycopg2"
            or url.host != "127.0.0.1"
            or url.port is None
            or url.query
            or not (url.database or "").startswith("grid_gex_scratch_")):
        raise ValueError(
            "GRID_GEX_SCRATCH_DSN requires explicit 127.0.0.1:PORT, "
            "a grid_gex_scratch_* database, and no URL query parameters"
        )
    return url


def _create_scratch_engine(url: URL, **kwargs):
    # Explicit libpq connection values outrank PGHOSTADDR/PGSERVICE defaults.
    return create_engine(url, connect_args={
        "host": "127.0.0.1", "hostaddr": "127.0.0.1",
        "port": url.port, "dbname": url.database,
    }, **kwargs)


def _is_expected_server_address(value: str | None) -> bool:
    """PostgreSQL inet text includes a prefix, e.g. 127.0.0.1/32."""
    try:
        return ip_interface(value).ip == IPv4Address("127.0.0.1")
    except (TypeError, ValueError):
        return False


@pytest.mark.parametrize(("server_addr", "expected"), [
    ("127.0.0.1/32", True),
    ("127.0.0.1", True),
    ("127.0.0.2/32", False),
    ("10.0.0.1/32", False),
    ("::1/128", False),
    (None, False),
])
def test_scratch_server_address_accepts_only_expected_loopback(
    server_addr: str | None, expected: bool,
) -> None:
    assert _is_expected_server_address(server_addr) is expected


@pytest.mark.parametrize("dsn", [
    "postgresql+psycopg2:///grid_gex_scratch_test",
    "postgresql+psycopg2://scratch@remote:55432/grid_gex_scratch_test",
    "postgresql+psycopg2://scratch@127.0.0.1/grid_gex_scratch_test",
    "postgresql+psycopg2://scratch@127.0.0.1:55432/grid_prod",
    "postgresql+psycopg2://scratch@127.0.0.1:55432/grid_gex_scratch_test?hostaddr=10.0.0.5",
    "postgresql+psycopg2://scratch@127.0.0.1:55432/grid_gex_scratch_test?service=production",
    "postgresql+psycopg2://scratch@127.0.0.1:55432/grid_gex_scratch_test?host=/tmp",
    "postgresql+psycopg2://scratch@127.0.0.1:55432/grid_gex_scratch_test?options=-csearch_path%3Dpublic",
])
def test_scratch_dsn_rejects_ambiguous_or_nonlocal_routes(dsn: str) -> None:
    with pytest.raises(ValueError, match="GRID_GEX_SCRATCH_DSN"):
        _scratch_url(dsn)


def test_scratch_dsn_accepts_explicit_loopback_without_overrides() -> None:
    url = _scratch_url(
        "postgresql+psycopg2://scratch@127.0.0.1:55432/grid_gex_scratch_test"
    )
    assert url.host == "127.0.0.1"
    assert url.port == 55432
    assert not url.query


def test_scratch_engine_pins_libpq_route(monkeypatch) -> None:
    seen = {}

    def capture(_url, **kwargs):
        seen.update(kwargs)
        return object()

    monkeypatch.setattr(sys.modules[__name__], "create_engine", capture)
    url = _scratch_url(
        "postgresql+psycopg2://scratch@127.0.0.1:55432/grid_gex_scratch_test"
    )
    _create_scratch_engine(url, pool_pre_ping=True)
    assert seen["connect_args"] == {
        "host": "127.0.0.1", "hostaddr": "127.0.0.1",
        "port": 55432, "dbname": "grid_gex_scratch_test",
    }


@pytest.fixture
def scratch_pg14(monkeypatch):
    dsn = os.getenv("GRID_GEX_SCRATCH_DSN")
    if not dsn:
        pytest.skip("set GRID_GEX_SCRATCH_DSN for a disposable local PG14 database")
    url = _scratch_url(dsn)

    admin = _create_scratch_engine(url, pool_pre_ping=True)
    schema = f"gex_capture_{uuid4().hex}"
    with admin.begin() as conn:
        server_addr = conn.exec_driver_sql("SELECT inet_server_addr()::text").scalar_one()
        if not _is_expected_server_address(server_addr):
            pytest.fail(f"scratch server did not accept a loopback connection: {server_addr}")
        version = int(conn.exec_driver_sql("SHOW server_version_num").scalar_one())
        if version // 10000 != 14:
            pytest.fail(f"scratch server must be PostgreSQL 14, got {version}")
        conn.exec_driver_sql(f'CREATE SCHEMA "{schema}"')

    engine = _create_scratch_engine(url, pool_size=3, max_overflow=0, pool_pre_ping=True)

    @event.listens_for(engine, "connect")
    def _set_search_path(dbapi_conn, _record):
        with dbapi_conn.cursor() as cur:
            cur.execute(f'SET search_path TO "{schema}"')
        dbapi_conn.commit()

    try:
        with engine.begin() as conn:
            conn.exec_driver_sql("""
                CREATE TABLE options_snapshots (
                    ticker TEXT NOT NULL, snap_date DATE NOT NULL,
                    expiry DATE NOT NULL, opt_type TEXT NOT NULL,
                    strike DOUBLE PRECISION NOT NULL,
                    last_price DOUBLE PRECISION, bid DOUBLE PRECISION,
                    ask DOUBLE PRECISION, volume INTEGER,
                    open_interest INTEGER, implied_vol DOUBLE PRECISION,
                    in_the_money BOOLEAN, created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (ticker, snap_date, expiry, opt_type, strike)
                )
            """)
            conn.exec_driver_sql("""
                CREATE TABLE options_daily_signals (
                    ticker TEXT NOT NULL, signal_date DATE NOT NULL,
                    put_call_ratio DOUBLE PRECISION, max_pain DOUBLE PRECISION,
                    iv_skew DOUBLE PRECISION, total_oi BIGINT, total_volume BIGINT,
                    near_expiry DATE, spot_price DOUBLE PRECISION,
                    iv_atm DOUBLE PRECISION, iv_25d_put DOUBLE PRECISION,
                    iv_25d_call DOUBLE PRECISION, term_structure_slope DOUBLE PRECISION,
                    oi_concentration DOUBLE PRECISION, UNIQUE (ticker, signal_date)
                )
            """)
        with engine.begin() as conn:
            monkeypatch.setattr(migration, "op", SimpleNamespace(execute=conn.exec_driver_sql))
            migration.upgrade()
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.exec_driver_sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.dispose()


def test_pg14_migration_replacement_rollback_and_overlap(scratch_pg14, monkeypatch):
    engine = scratch_pg14
    now = datetime.now(timezone.utc)
    day = now.date()
    expirations = [int((now + timedelta(days=n)).timestamp()) for n in (10, 20)]
    monkeypatch.setattr(options, "compute_max_pain", lambda *_args: 100.0)
    monkeypatch.setattr(options, "compute_iv_skew", lambda *_args: 0.0)
    monkeypatch.setattr(options, "_compute_atm_iv", lambda *_args: 0.2)
    monkeypatch.setattr(options, "_compute_wing_iv", lambda *_args, **_kwargs: 0.2)
    monkeypatch.setattr(options, "_compute_oi_concentration", lambda *_args: 0.5)

    with engine.begin() as conn:
        columns = {row[0] for row in conn.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = 'options_snapshots'
        """))}
        assert {"capture_batch_id", "capture_ordinal", "capture_started_at",
                "capture_completed_at"} <= columns
        assert conn.exec_driver_sql(
            "SELECT to_regclass('options_capture_ordinal_seq')"
        ).scalar_one() is not None
        conn.execute(text("""
            INSERT INTO options_snapshots (ticker, snap_date, expiry, opt_type, strike,
                                           open_interest, implied_vol)
            VALUES ('SPY', :day, :expiry, 'call', 999, 10, 0.2)
        """), {"day": day, "expiry": day + timedelta(days=10)})
    assert DealerGammaEngine(engine)._load_chain("SPY", day).empty

    assert _puller(engine, _yahoo(expirations, [100.0, 110.0]))._pull_ticker(
        "SPY", day.isoformat(),
    )["status"] == "SUCCESS"
    assert _puller(engine, _yahoo(expirations, [100.0, 120.0]))._pull_ticker(
        "SPY", day.isoformat(),
    )["status"] == "SUCCESS"
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT strike, capture_batch_id, capture_ordinal, capture_started_at,
                   capture_completed_at, created_at FROM options_snapshots
            WHERE ticker = 'SPY' AND snap_date = :day
        """), {"day": day}).fetchall()
    assert len(rows) == 8
    assert {row[0] for row in rows} == {100.0, 120.0}
    assert len({row[1] for row in rows}) == 1
    assert len({row[2] for row in rows}) == 1 and rows[0][2] > 0
    assert all(row[3] <= row[4] for row in rows)
    assert not DealerGammaEngine(engine)._load_chain("SPY", day).empty

    with engine.begin() as conn:
        conn.exec_driver_sql("""
            CREATE FUNCTION reject_130() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.strike = 130 THEN RAISE EXCEPTION 'scratch insert failure'; END IF;
                RETURN NEW;
            END $$
        """)
        conn.exec_driver_sql("""
            CREATE TRIGGER reject_130 BEFORE INSERT ON options_snapshots
            FOR EACH ROW EXECUTE FUNCTION reject_130()
        """)
    assert _puller(engine, _yahoo(expirations, [100.0, 130.0]))._pull_ticker(
        "SPY", day.isoformat(),
    )["status"] == "FAILED"
    with engine.connect() as conn:
        strikes = {row[0] for row in conn.execute(text(
            "SELECT strike FROM options_snapshots WHERE ticker = 'SPY' AND snap_date = :day"
        ), {"day": day})}
    assert strikes == {100.0, 120.0}
    with engine.begin() as conn:
        conn.exec_driver_sql("DROP TRIGGER reject_130 ON options_snapshots")
        conn.exec_driver_sql("DROP FUNCTION reject_130()")

    old = _yahoo(expirations, [90.0])
    old_get = old.get_options
    entered, release, newer_done = Event(), Event(), Event()
    results = {}

    def blocked_get(ticker, expiry=None):
        if not entered.is_set():
            entered.set()
            assert release.wait(10)
        return old_get(ticker, expiry)

    old.get_options = blocked_get

    def run(name, yahoo):
        results[name] = _puller(engine, yahoo)._pull_ticker("SPY", day.isoformat())
        if name == "newer":
            newer_done.set()

    a = Thread(target=run, args=("older", old), daemon=True)
    b = Thread(target=run, args=("newer", _yahoo(expirations, [140.0])), daemon=True)
    a.start()
    try:
        assert entered.wait(5)
        b.start()
        assert newer_done.wait(10), "older provider request held the DB lock"
    finally:
        release.set()
        a.join(10)
        if b.ident is not None:
            b.join(10)
    assert not a.is_alive() and not b.is_alive()
    assert results["newer"]["status"] == "SUCCESS"
    assert results["older"]["status"] == "SKIPPED"
    with engine.connect() as conn:
        strikes = {row[0] for row in conn.execute(text(
            "SELECT strike FROM options_snapshots WHERE ticker = 'SPY' AND snap_date = :day"
        ), {"day": day})}
    assert strikes == {140.0}
