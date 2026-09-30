"""Real-PostgreSQL proof for migrations/versions/options_append_only_20260930.py.

Runs in a throwaway schema (random name, dropped afterwards) on the shared
``pg_engine`` fixture (``GRID_TEST_DB_URL``); CI runs it in its own step and
fails on any skip. Proves, against a real database:

* upgrade keeps every legacy row, registers existing consistent batches, turns
  ``options_snapshots`` into a view and drops the per-day uniqueness;
* two same-day captures through the real ``OptionsPuller`` writer both survive
  in ``options_snapshots_all``; the view (and so every default reader) shows
  only the latest complete batch;
* UPDATE / DELETE / TRUNCATE are refused, and so are rows without batch
  metadata or without a registered batch;
* the frozen GEX-levels v1 chain selection and tested-wall loader read exactly
  one batch (no summing of same-day batches);
* the live v1 ``run_preopen`` still records the latest batch, while
  ``paper_log.gex_batch_replay`` replays the earlier batch end to end, and a
  mismatched (batch, ordinal) pair fails closed;
* a capture outside the NY session date (#653) is refused before any write;
* downgrade refuses while any ticker/day holds two batches.
"""

from __future__ import annotations

import importlib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

from ingestion import options

_MIGRATION = "migrations.versions.options_append_only_20260930"
SNAP_DAY = date(2026, 9, 25)  # Friday session
GEM_START = datetime(2026, 9, 25, 14, 5, tzinfo=timezone.utc)
LATE_START = datetime(2026, 9, 25, 19, 0, tzinfo=timezone.utc)
PREOPEN_RUN = datetime(2026, 9, 28, 12, 45, tzinfo=timezone.utc)  # Mon 08:45 NY

_LEGACY_DDL = """
CREATE TABLE options_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    ticker          TEXT NOT NULL,
    snap_date       DATE NOT NULL,
    expiry          DATE NOT NULL,
    opt_type        TEXT NOT NULL CHECK (opt_type IN ('call', 'put')),
    strike          DOUBLE PRECISION NOT NULL,
    last_price      DOUBLE PRECISION,
    bid             DOUBLE PRECISION,
    ask             DOUBLE PRECISION,
    volume          INTEGER,
    open_interest   INTEGER,
    implied_vol     DOUBLE PRECISION,
    in_the_money    BOOLEAN,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    capture_batch_id TEXT,
    capture_ordinal BIGINT,
    capture_started_at TIMESTAMPTZ,
    capture_completed_at TIMESTAMPTZ,
    provider_regular_market_at TIMESTAMPTZ,
    UNIQUE (ticker, snap_date, expiry, opt_type, strike)
);
CREATE INDEX idx_opts_snap_ticker_date ON options_snapshots (ticker, snap_date);
"""


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"opts_append_only_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    url = pg_engine.url.update_query_dict({"options": f"-csearch_path={schema}"})
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.exec_driver_sql(_LEGACY_DDL)
    try:
        yield engine, url
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _run(engine: Engine, fn_name: str) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(_MIGRATION)
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            real_op = migration.op
            migration.op = Operations(MigrationContext.configure(conn))
            try:
                getattr(migration, fn_name)()
            finally:
                migration.op = real_op
            trans.commit()
        except Exception:
            trans.rollback()
            raise


def _seed_legacy(engine: Engine) -> str:
    """One NULL-provenance day and one pre-append-only batched day."""
    batch = str(uuid4())
    with engine.begin() as conn:
        for strike in (100.0, 101.0):
            conn.execute(text(
                "INSERT INTO options_snapshots (ticker, snap_date, expiry, opt_type, strike, "
                "open_interest, implied_vol) VALUES ('SPY', '2026-09-22', '2026-10-16', 'call', :k, 5, 0.2)"
            ), {"k": strike})
        for strike in (100.0, 101.0, 102.0):
            conn.execute(text(
                "INSERT INTO options_snapshots (ticker, snap_date, expiry, opt_type, strike, "
                "open_interest, implied_vol, capture_batch_id, capture_ordinal, capture_started_at, "
                "capture_completed_at, provider_regular_market_at) VALUES ('SPY', '2026-09-24', "
                "'2026-10-16', 'put', :k, 5, 0.2, :b, 7, '2026-09-24 14:00Z', '2026-09-24 14:02Z', "
                "'2026-09-24 13:59Z')"
            ), {"k": strike, "b": batch})
    return batch


class _Yahoo:
    """Two expiries; per-strike open interest makes each batch's walls distinct."""

    def __init__(self, oi_by_strike: dict[float, int], quote_at: datetime) -> None:
        self.oi = oi_by_strike
        self.quote_at = quote_at

    def get_options(self, _ticker, _expiry=None):
        rows = [{"strike": k, "volume": 3, "openInterest": oi, "impliedVolatility": 0.2,
                 "lastPrice": 2.0, "bid": 1.0, "ask": 3.0, "inTheMoney": False}
                for k, oi in self.oi.items()]
        expirations = [int(datetime(2026, 10, day, 20, tzinfo=timezone.utc).timestamp())
                       for day in (5, 15)]
        return {"quote": {"regularMarketPrice": 100.0,
                          "regularMarketTime": int(self.quote_at.timestamp())},
                "expirations": expirations, "calls": rows, "puts": rows}


def _capture(engine: Engine, monkeypatch, started: datetime, oi: dict[float, int],
             source: str = "options_puller") -> dict:
    """Run the real writer once, with the capture clock pinned to ``started``."""
    completed = started + timedelta(minutes=2)
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "ALTER TABLE options_snapshots_all ALTER COLUMN created_at "
            f"SET DEFAULT TIMESTAMPTZ '{completed.isoformat()}'")

    def fixed_clock(_conn, _cursor, statement, parameters, _context, _many):
        if statement == "SELECT txid_current(), clock_timestamp()":
            return f"SELECT txid_current(), TIMESTAMPTZ '{started.isoformat()}'", parameters
        if statement == "SELECT clock_timestamp()":
            return f"SELECT TIMESTAMPTZ '{completed.isoformat()}'", parameters
        return statement, parameters

    monkeypatch.setattr(options, "_utc_now", lambda: started)
    monkeypatch.setattr(options.time, "sleep", lambda _s: None)
    puller = options.OptionsPuller.__new__(options.OptionsPuller)
    puller.engine = engine
    puller._ensure_tables()
    puller._yahoo = _Yahoo(oi, started - timedelta(minutes=1))
    puller._push_to_resolved = lambda *_a, **_k: 0
    event.listen(engine, "before_cursor_execute", fixed_clock, retval=True)
    try:
        return puller._pull_ticker("SPY", SNAP_DAY.isoformat(), capture_source=source)
    finally:
        event.remove(engine, "before_cursor_execute", fixed_clock)


_EARLY_OI = {95.0: 500, 97.0: 10, 103.0: 10, 105.0: 500}
_LATE_OI = {95.0: 10, 97.0: 500, 103.0: 500, 105.0: 10}


@pytest.fixture()
def two_batches(scratch, monkeypatch):
    engine, url = scratch
    _run(engine, "upgrade")
    first = _capture(engine, monkeypatch, GEM_START, _EARLY_OI, source="gem")
    second = _capture(engine, monkeypatch, LATE_START, _LATE_OI)
    assert first["status"] == second["status"] == "SUCCESS", (first, second)
    return engine, url, first, second


def test_upgrade_keeps_legacy_rows_and_registers_existing_batches(scratch) -> None:
    engine, _ = scratch
    legacy_batch = _seed_legacy(engine)
    _run(engine, "upgrade")
    with engine.connect() as conn:
        kind = conn.execute(text(
            "SELECT relkind FROM pg_class WHERE oid = to_regclass('options_snapshots')")).scalar()
        assert kind == "v"
        assert conn.execute(text("SELECT COUNT(*) FROM options_snapshots_all")).scalar() == 5
        assert conn.execute(text("SELECT COUNT(*) FROM options_snapshots")).scalar() == 5
        batches = conn.execute(text(
            "SELECT capture_batch_id, capture_ordinal, row_count, backfilled, capture_source "
            "FROM options_capture_batches")).fetchall()
        assert [tuple(b) for b in batches] == [(legacy_batch, 7, 3, True, "pre_append_only")]
        assert conn.execute(text(
            "SELECT COUNT(*) FROM pg_constraint WHERE conname = "
            "'options_snapshots_ticker_snap_date_expiry_opt_type_strike_key'")).scalar() == 0


def test_two_same_day_batches_survive_and_default_readers_see_latest(two_batches) -> None:
    engine, _, first, second = two_batches
    assert first["capture_ordinal"] < second["capture_ordinal"]
    assert first["latest_batch"] is True and second["latest_batch"] is True
    with engine.connect() as conn:
        stored = conn.execute(text(
            "SELECT capture_batch_id, COUNT(*) FROM options_snapshots_all "
            "GROUP BY capture_batch_id ORDER BY MIN(capture_ordinal)")).fetchall()
        assert [tuple(r) for r in stored] == [
            (first["capture_batch_id"], 16), (second["capture_batch_id"], 16)]
        registered = conn.execute(text(
            "SELECT capture_batch_id, capture_source, row_count FROM options_capture_batches "
            "ORDER BY capture_ordinal")).fetchall()
        assert [tuple(r) for r in registered] == [
            (first["capture_batch_id"], "gem", 16),
            (second["capture_batch_id"], "options_puller", 16)]
        visible = conn.execute(text(
            "SELECT DISTINCT capture_batch_id FROM options_snapshots")).scalars().all()
        assert visible == [second["capture_batch_id"]]

    from physics.dealer_gamma import DealerGammaEngine

    gex = DealerGammaEngine(engine)
    latest = gex._load_chain("SPY", SNAP_DAY)
    assert latest.attrs["batch_id"] == second["capture_batch_id"] and len(latest) == 16
    earlier = gex._load_chain("SPY", SNAP_DAY, capture_batch_id=first["capture_batch_id"])
    assert earlier.attrs["batch_id"] == first["capture_batch_id"] and len(earlier) == 16
    assert gex._load_chain("QQQ", SNAP_DAY, capture_batch_id=first["capture_batch_id"]).empty
    assert gex._load_chain("SPY", SNAP_DAY, capture_batch_id=str(uuid4())).empty


@pytest.mark.parametrize("statement", [
    "UPDATE options_snapshots_all SET open_interest = 1",
    "DELETE FROM options_snapshots_all",
    "TRUNCATE options_snapshots_all",
    "DELETE FROM options_snapshots WHERE ticker = 'SPY'",
    "UPDATE options_capture_batches SET row_count = 1",
    "DELETE FROM options_capture_batches",
    "TRUNCATE options_capture_batches CASCADE",
])
def test_mutation_is_refused(two_batches, statement: str) -> None:
    engine = two_batches[0]
    with pytest.raises(DBAPIError, match="append-only"):
        with engine.begin() as conn:
            conn.exec_driver_sql(statement)


def test_rows_without_registered_batch_are_refused(two_batches) -> None:
    engine = two_batches[0]
    with pytest.raises(DBAPIError, match="options_snapshots_all_batch_required"):
        with engine.begin() as conn:
            conn.exec_driver_sql(
                "INSERT INTO options_snapshots_all (ticker, snap_date, expiry, opt_type, strike) "
                "VALUES ('SPY', '2026-09-25', '2026-10-16', 'call', 1.0)")
    with pytest.raises(DBAPIError, match="options_snapshots_all_batch_fk"):
        with engine.begin() as conn:
            conn.exec_driver_sql(
                "INSERT INTO options_snapshots_all (ticker, snap_date, expiry, opt_type, strike, "
                "capture_batch_id, capture_ordinal, capture_started_at, capture_completed_at, "
                "provider_regular_market_at) VALUES ('SPY', '2026-09-25', '2026-10-16', 'call', "
                "1.0, 'unregistered', 99999999, now(), now(), now())")


def test_frozen_v1_readers_see_exactly_one_batch(two_batches) -> None:
    engine = two_batches[0]
    from paper_log.gex_levels.chain import select_chain_snapshot
    from paper_log.gex_levels.tested_walls import _load_full_chain_rows

    chain = select_chain_snapshot(engine, "SPY", PREOPEN_RUN)
    assert chain.snap_date == SNAP_DAY
    assert chain.created_at == LATE_START + timedelta(minutes=2)
    # 4 strikes x call/put x 2 expiries = one batch; two batches would be 32.
    assert len(_load_full_chain_rows(engine, "SPY", SNAP_DAY)) == 16


def _receipt():
    at = datetime(2026, 9, 25, 1, tzinfo=timezone.utc)
    return {"price": 100.0, "obs_date": date(2026, 9, 24), "available_at": at,
            "receipt_created_at": at, "release_date": date(2026, 9, 24),
            "vintage_date": date(2026, 9, 24), "receipt_id": 1}


def test_live_preopen_keeps_latest_and_replay_uses_earlier_batch(
    two_batches, monkeypatch, tmp_path: Path,
) -> None:
    _, url, first, second = two_batches
    from paper_log import gex_batch_replay as replay
    from paper_log.gex_levels import preopen
    from paper_log.gex_levels.db import build_readonly_engine
    from paper_log.gex_levels.market_data import PricePoint
    from physics.dealer_gamma import DealerGammaEngine

    monkeypatch.setattr(DealerGammaEngine, "_get_spot_receipt", lambda *_a, **_k: _receipt())
    p0 = PricePoint(price=100.0, as_of_date=SNAP_DAY, fetched_at=PREOPEN_RUN)
    vix = PricePoint(price=15.0, as_of_date=SNAP_DAY, fetched_at=PREOPEN_RUN)
    monkeypatch.setattr(preopen, "fetch_previous_close",
                        lambda ticker, *_a, **_k: vix if ticker == "^VIX" else p0)
    readonly = build_readonly_engine(url.render_as_string(hide_password=False))
    try:
        live = preopen.run_preopen(log_dir=tmp_path, db_engine=readonly, code_sha="test",
                                   now_fn=lambda: PREOPEN_RUN)
        replayed = replay.replay_preopen_for_batch(
            db_engine=readonly, ticker="SPY", capture_batch_id=first["capture_batch_id"],
            capture_ordinal=first["capture_ordinal"], p0=p0, vix=vix, code_sha="test")
        with pytest.raises(replay.BatchReplayError, match="ordinal"):
            replay.replay_preopen_for_batch(
                db_engine=readonly, ticker="SPY", capture_batch_id=first["capture_batch_id"],
                capture_ordinal=second["capture_ordinal"], p0=p0, vix=vix, code_sha="test")
        with pytest.raises(replay.BatchReplayError, match="not registered"):
            replay.resolve_batch(readonly, "SPY", str(uuid4()), first["capture_ordinal"])
        with pytest.raises(replay.BatchReplayError, match="different ticker"):
            replay.resolve_batch(readonly, "QQQ", first["capture_batch_id"],
                                 first["capture_ordinal"])
    finally:
        readonly.dispose()

    assert live["excluded"] is False, live
    assert live["kind"] == "preopen"
    # The appended record comes back JSON-serialized by the v1 store.
    assert live["chain"]["created_at"] == (LATE_START + timedelta(minutes=2)).isoformat()
    assert (live["levels"]["real"]["put_wall"], live["levels"]["real"]["call_wall"]) == (97.0, 103.0)

    assert replayed["excluded"] is False, replayed
    assert replayed["kind"] == "preopen_replay" and replayed["live"] is False
    assert replayed["session_date"] == date(2026, 9, 28)
    assert replayed["chain"]["capture_batch_id"] == first["capture_batch_id"]
    assert replayed["chain"]["capture_source"] == "gem"
    assert (replayed["levels"]["real"]["put_wall"],
            replayed["levels"]["real"]["call_wall"]) == (95.0, 105.0)
    # Nothing was written to the live paper-log store by the replay.
    assert sum(1 for _ in (tmp_path).rglob("*.jsonl")) >= 1
    assert all("preopen_replay" not in p.read_text() for p in tmp_path.rglob("*.jsonl"))


def test_capture_after_ny_session_date_is_refused_before_any_write(scratch, monkeypatch) -> None:
    """#653 kept: 00:15 UTC Friday is still Thursday 20:15 in New York; both
    days are sessions, so only the UTC/New York date mismatch refuses it."""
    engine, _ = scratch
    _run(engine, "upgrade")
    monkeypatch.setattr(options, "_utc_now",
                        lambda: datetime(2026, 9, 25, 0, 15, tzinfo=timezone.utc))
    monkeypatch.setattr(options, "YahooOptionsClient",
                        lambda: pytest.fail("non-session capture must not contact provider"))
    puller = options.OptionsPuller.__new__(options.OptionsPuller)
    puller.engine = engine
    result = puller.pull_all(tickers=["SPY"], max_expirations=6)
    assert [r["status"] for r in result] == ["SKIPPED"]
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM options_capture_batches")).scalar() == 0


def test_downgrade_refuses_two_batches_and_restores_single_batch_layout(
    two_batches, pg_engine,
) -> None:
    engine = two_batches[0]
    with pytest.raises(DBAPIError, match="downgrade refused"):
        _run(engine, "downgrade")
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM options_snapshots_all")).scalar() == 32


def test_downgrade_restores_table_when_one_batch_per_day(scratch) -> None:
    engine, _ = scratch
    _seed_legacy(engine)
    _run(engine, "upgrade")
    _run(engine, "downgrade")
    with engine.connect() as conn:
        assert conn.execute(text(
            "SELECT relkind FROM pg_class WHERE oid = to_regclass('options_snapshots')")).scalar() == "r"
        assert conn.execute(text("SELECT COUNT(*) FROM options_snapshots")).scalar() == 5
        assert conn.execute(text("SELECT to_regclass('options_capture_batches')")).scalar() is None


def test_same_day_legacy_rows_are_kept_but_hidden_once_a_batch_lands(scratch, monkeypatch) -> None:
    engine, _ = scratch
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO options_snapshots (ticker, snap_date, expiry, opt_type, strike, "
            "open_interest, implied_vol) VALUES ('SPY', :d, '2026-10-16', 'call', 999.0, 5, 0.2)"
        ), {"d": SNAP_DAY})
    _run(engine, "upgrade")
    with engine.connect() as conn:
        assert conn.execute(text("SELECT strike FROM options_snapshots")).scalars().all() == [999.0]
    result = _capture(engine, monkeypatch, GEM_START, _EARLY_OI)
    assert result["status"] == "SUCCESS"
    with engine.connect() as conn:
        visible = conn.execute(text("SELECT DISTINCT capture_batch_id FROM options_snapshots")).scalars().all()
        assert visible == [result["capture_batch_id"]]
        assert conn.execute(text(
            "SELECT COUNT(*) FROM options_snapshots_all WHERE capture_batch_id IS NULL")).scalar() == 1


def _schema_sql_options_block() -> str:
    source = (Path(__file__).resolve().parents[1] / "schema.sql").read_text(encoding="utf-8")
    start = source.index("DO $options_store$")
    end = source.index("$options_store$;", start) + len("$options_store$;")
    return source[start:end]


def test_schema_sql_fresh_install_matches_migration_guards(pg_engine) -> None:
    schema = f"opts_schema_sql_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url.update_query_dict({"options": f"-csearch_path={schema}"}))
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(_schema_sql_options_block())
            conn.exec_driver_sql(_schema_sql_options_block())  # idempotent no-op
        with engine.connect() as conn:
            assert conn.execute(text(
                "SELECT relkind FROM pg_class WHERE oid = to_regclass('options_snapshots')")).scalar() == "v"
        with pytest.raises(DBAPIError, match="options_snapshots_all_batch_required"):
            with engine.begin() as conn:
                conn.exec_driver_sql(
                    "INSERT INTO options_snapshots_all (ticker, snap_date, expiry, opt_type, strike) "
                    "VALUES ('SPY', '2026-09-25', '2026-10-16', 'call', 1.0)")
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def test_schema_sql_block_is_a_no_op_on_a_pre_migration_database(scratch) -> None:
    engine, _ = scratch
    with engine.begin() as conn:
        conn.exec_driver_sql(_schema_sql_options_block())
        assert conn.execute(text(
            "SELECT relkind FROM pg_class WHERE oid = to_regclass('options_snapshots')")).scalar() == "r"
        assert conn.execute(text("SELECT to_regclass('options_capture_batches')")).scalar() is None
