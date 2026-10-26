"""Real-PostgreSQL proof for resolved_series retractions.

Covers ``migrations/versions/resolved_retractions_20260927.py`` and every
reader that honours it (``store/pit.py`` and the direct PIT readers migrated
with it). Runs in a throwaway schema (random name, dropped afterwards) holding
a minimal ``feature_registry`` and a ``resolved_series`` with production's
unique key ``uq_resolved_series_composite``. Proves:

* upgrade creates the table, its key (with ``retracted_at`` INCLUDEd for the
  reader anti-join), the FK to the retracted row, and the append-only guard;
  the SET LOCAL timeouts stay inside the migration's transaction;
* a retracted row is hidden from reads with as_of on/after the retraction
  and still visible to as_of dates before the retraction's UTC date (those
  replays are reproduced exactly; as_of = retraction day already hides it);
* LATEST_AS_OF / FIRST_RELEASE pick among the remaining vintages, and a cell
  with none left returns no row (never a zero, never another feature);
* non-retracted rows and other features are unaffected;
* retractions cannot be backdated, updated or deleted, must name a real row,
  and a retracted resolved row cannot be deleted;
* downgrade refuses while retractions exist.

Uses the shared ``pg_engine`` fixture's URL (``GRID_TEST_DB_URL``); skips when
no PostgreSQL is reachable (the CI step fails on a skip).
"""

from __future__ import annotations

import importlib
from datetime import date, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

from store.pit import PITStore, retraction_cutoff

_MIGRATION_MODULE = "migrations.versions.resolved_retractions_20260927"

_FEATURE_REGISTRY_DDL = """
CREATE TABLE feature_registry (
    id    SERIAL PRIMARY KEY,
    name  TEXT NOT NULL UNIQUE
)
"""

# schema.sql's resolved_series (FK to source_catalog omitted: irrelevant here).
_RESOLVED_SERIES_DDL = """
CREATE TABLE resolved_series (
    id                    BIGSERIAL PRIMARY KEY,
    feature_id            INTEGER NOT NULL REFERENCES feature_registry(id),
    obs_date              DATE NOT NULL,
    release_date          DATE NOT NULL,
    vintage_date          DATE NOT NULL,
    value                 DOUBLE PRECISION NOT NULL,
    source_priority_used  INTEGER NOT NULL,
    conflict_flag         BOOLEAN NOT NULL DEFAULT FALSE,
    conflict_detail       JSONB,
    resolution_version    INTEGER NOT NULL DEFAULT 1
)
"""
_RESOLVED_SERIES_KEY = (
    "CREATE UNIQUE INDEX uq_resolved_series_composite "
    "ON resolved_series (feature_id, obs_date, vintage_date)"
)

D_OBS = date(2024, 1, 10)
D_OBS2 = date(2024, 1, 11)
V1 = date(2024, 1, 15)
V2 = date(2024, 1, 20)


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"retract_mig_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(
        pg_engine.url, connect_args={"options": f"-csearch_path={schema}"},
    )
    try:
        with engine.begin() as conn:
            conn.execute(text(_FEATURE_REGISTRY_DDL))
            conn.execute(text(_RESOLVED_SERIES_DDL))
            conn.execute(text(_RESOLVED_SERIES_KEY))
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _run(engine: Engine, fn_name: str) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(_MIGRATION_MODULE)
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


def _feature(engine: Engine, name: str) -> int:
    with engine.begin() as conn:
        return conn.execute(
            text("INSERT INTO feature_registry (name) VALUES (:n) RETURNING id"),
            {"n": name},
        ).scalar_one()


def _resolved(engine: Engine, fid: int, obs: date, vintage: date, value: float) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO resolved_series "
                "(feature_id, obs_date, release_date, vintage_date, value, source_priority_used) "
                "VALUES (:f, :o, :v, :v, :val, 1)"
            ),
            {"f": fid, "o": obs, "v": vintage, "val": value},
        )


def _retract(engine: Engine, fid: int, obs: date, vintage: date) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO resolved_series_retractions "
                "(feature_id, obs_date, vintage_date, reason, run_tag) "
                "VALUES (:f, :o, :v, 'no_clean_raw', 'test_run')"
            ),
            {"f": fid, "o": obs, "v": vintage},
        )


def _db_today(engine: Engine) -> date:
    with engine.connect() as conn:
        return conn.execute(text("SELECT (now() AT TIME ZONE 'UTC')::date")).scalar_one()


def _cells(df) -> dict[tuple[int, date], float]:
    return {(int(r.feature_id), r.obs_date): float(r.value) for r in df.itertuples()}


@pytest.fixture()
def seeded(scratch: Engine):
    """Two features; spy has a clean-then-wrong cell, a wrong-only cell and a
    wrong-then-clean cell; gld has an untouched cell on the same obs_date."""
    _run(scratch, "upgrade")
    spy = _feature(scratch, "spy_full")
    gld = _feature(scratch, "gld_full")
    _resolved(scratch, spy, D_OBS, V1, 100.0)    # clean first vintage
    _resolved(scratch, spy, D_OBS, V2, 101.0)    # wrong latest vintage
    _resolved(scratch, spy, D_OBS2, V1, 102.0)   # wrong, only vintage
    _resolved(scratch, gld, D_OBS, V1, 180.0)    # different feature, same date
    return scratch, spy, gld


# ---------------------------------------------------------------------------
# Migration shape
# ---------------------------------------------------------------------------

def test_upgrade_creates_key_fk_and_guard(scratch):
    _run(scratch, "upgrade")
    with scratch.connect() as conn:
        cons = dict(conn.execute(text(
            "SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid = 'resolved_series_retractions'::regclass"
        )).fetchall())
        trig = conn.execute(text(
            "SELECT tgname FROM pg_trigger "
            "WHERE tgrelid = 'resolved_series_retractions'::regclass AND NOT tgisinternal"
        )).scalars().all()
        nullable = dict(conn.execute(text(
            "SELECT column_name, is_nullable FROM information_schema.columns "
            "WHERE table_name = 'resolved_series_retractions' "
            "AND table_schema = current_schema()"
        )).fetchall())
    key = cons["uq_resolved_series_retractions_key"]
    assert "UNIQUE (feature_id, obs_date, vintage_date)" in key
    assert "INCLUDE (retracted_at)" in key
    fk = cons["fk_resolved_series_retractions_row"]
    assert "REFERENCES resolved_series(feature_id, obs_date, vintage_date)" in fk
    assert trig == ["trg_resolved_series_retractions_guard"]
    assert all(nullable[c] == "NO" for c in (
        "feature_id", "obs_date", "vintage_date", "retracted_at", "reason", "run_tag"))


def test_upgrade_timeouts_are_scoped_to_its_own_transaction(scratch):
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(_MIGRATION_MODULE)
    with scratch.connect() as conn:
        baseline = (
            conn.execute(text("SHOW lock_timeout")).scalar(),
            conn.execute(text("SHOW statement_timeout")).scalar(),
        )
        conn.rollback()
        trans = conn.begin()
        real_op = migration.op
        migration.op = Operations(MigrationContext.configure(conn))
        try:
            migration.upgrade()
        finally:
            migration.op = real_op
        assert conn.execute(text("SHOW lock_timeout")).scalar() == migration._LOCK_TIMEOUT
        assert conn.execute(text("SHOW statement_timeout")).scalar() == migration._STATEMENT_TIMEOUT
        trans.commit()
        assert (
            conn.execute(text("SHOW lock_timeout")).scalar(),
            conn.execute(text("SHOW statement_timeout")).scalar(),
        ) == baseline


def test_upgrade_is_idempotent(scratch):
    _run(scratch, "upgrade")
    _run(scratch, "upgrade")
    with scratch.connect() as conn:
        n = conn.execute(text(
            "SELECT count(*) FROM pg_trigger "
            "WHERE tgrelid = 'resolved_series_retractions'::regclass AND NOT tgisinternal"
        )).scalar_one()
    assert n == 1


# ---------------------------------------------------------------------------
# PIT semantics (store/pit.py)
# ---------------------------------------------------------------------------

def test_no_retractions_leaves_pit_unchanged(seeded):
    engine, spy, gld = seeded
    today = _db_today(engine)
    pit = PITStore(engine)
    latest = _cells(pit.get_pit([spy, gld], today, "LATEST_AS_OF"))
    first = _cells(pit.get_pit([spy, gld], today, "FIRST_RELEASE"))
    assert latest == {(spy, D_OBS): 101.0, (spy, D_OBS2): 102.0, (gld, D_OBS): 180.0}
    assert first == {(spy, D_OBS): 100.0, (spy, D_OBS2): 102.0, (gld, D_OBS): 180.0}


def test_latest_as_of_hides_retracted_rows_from_retraction_on(seeded):
    engine, spy, gld = seeded
    _retract(engine, spy, D_OBS, V2)     # wrong latest vintage; clean V1 remains
    _retract(engine, spy, D_OBS2, V1)    # only vintage: cell becomes unavailable
    today = _db_today(engine)
    pit = PITStore(engine)

    after = _cells(pit.get_pit([spy, gld], today, "LATEST_AS_OF"))
    # Remaining vintage wins; the fully retracted cell has NO row (not 0, not
    # gld's value); gld on the same obs_date is untouched.
    assert after == {(spy, D_OBS): 100.0, (gld, D_OBS): 180.0}

    # Replays for as_of dates before the retraction's UTC date are unchanged.
    before = _cells(pit.get_pit([spy, gld], today - timedelta(days=1), "LATEST_AS_OF"))
    assert before == {(spy, D_OBS): 101.0, (spy, D_OBS2): 102.0, (gld, D_OBS): 180.0}


def test_first_release_uses_earliest_non_retracted_vintage(seeded):
    engine, spy, gld = seeded
    _retract(engine, spy, D_OBS, V1)     # wrong first vintage; V2 remains
    _retract(engine, spy, D_OBS2, V1)    # only vintage
    today = _db_today(engine)
    pit = PITStore(engine)

    after = _cells(pit.get_pit([spy, gld], today, "FIRST_RELEASE"))
    assert after == {(spy, D_OBS): 101.0, (gld, D_OBS): 180.0}

    before = _cells(pit.get_pit([spy, gld], today - timedelta(days=1), "FIRST_RELEASE"))
    assert before == {(spy, D_OBS): 100.0, (spy, D_OBS2): 102.0, (gld, D_OBS): 180.0}


def test_all_vintages_retracted_cell_is_unavailable_in_both_policies(seeded):
    engine, spy, gld = seeded
    _retract(engine, spy, D_OBS, V1)
    _retract(engine, spy, D_OBS, V2)
    today = _db_today(engine)
    pit = PITStore(engine)
    for policy in ("LATEST_AS_OF", "FIRST_RELEASE"):
        cells = _cells(pit.get_pit([spy, gld], today, policy))
        assert (spy, D_OBS) not in cells
        assert cells[(gld, D_OBS)] == 180.0


def test_feature_matrix_shows_retracted_cell_as_missing_not_zero(seeded):
    engine, spy, gld = seeded
    _retract(engine, spy, D_OBS2, V1)
    today = _db_today(engine)
    matrix = PITStore(engine).get_feature_matrix(
        [spy, gld], D_OBS, D_OBS2, today, vintage_policy="LATEST_AS_OF",
    )
    import pandas as pd

    # D_OBS2 had only spy's row; once retracted the date drops out entirely.
    assert list(matrix.index) == [pd.Timestamp(D_OBS)]
    assert matrix.loc[pd.Timestamp(D_OBS), spy] == 101.0


def test_timestamp_cutoff_is_inclusive_at_retracted_at(seeded):
    """as_of_ts >= retracted_at hides the row; one microsecond earlier shows it."""
    engine, spy, _ = seeded
    _retract(engine, spy, D_OBS2, V1)
    probe = text("""
        SELECT count(*) FROM resolved_series rs
        WHERE rs.feature_id = :f AND rs.obs_date = :o
          AND NOT EXISTS (
              SELECT 1 FROM resolved_series_retractions rr
              WHERE rr.feature_id = rs.feature_id
                AND rr.obs_date = rs.obs_date
                AND rr.vintage_date = rs.vintage_date
                AND rr.retracted_at <= :retraction_cutoff
          )
    """)
    with engine.connect() as conn:
        ts = conn.execute(text("SELECT retracted_at FROM resolved_series_retractions")).scalar_one()
        at = conn.execute(probe, {"f": spy, "o": D_OBS2,
                                  "retraction_cutoff": retraction_cutoff(ts)}).scalar_one()
        just_before = conn.execute(probe, {
            "f": spy, "o": D_OBS2,
            "retraction_cutoff": retraction_cutoff(ts - timedelta(microseconds=1)),
        }).scalar_one()
    assert (at, just_before) == (0, 1)


# ---------------------------------------------------------------------------
# Direct PIT readers migrated with this change
# ---------------------------------------------------------------------------

def test_direct_pit_readers_honour_retractions(seeded):
    from alpha_research import conviction_scorer as cs
    from alpha_research.signals import credit_cycle
    from oracle import prediction_context

    engine, spy, _ = seeded
    today = _db_today(engine)
    yesterday = today - timedelta(days=1)
    _retract(engine, spy, D_OBS, V2)
    _retract(engine, spy, D_OBS2, V1)

    with engine.connect() as conn:
        # Latest obs_date D_OBS2 is fully retracted -> falls to D_OBS's clean 100.
        assert cs._load_latest(conn, "spy_full", as_of_date=today) == 100.0
        assert cs._load_latest(conn, "spy_full", as_of_date=yesterday) == 102.0
        price_now = cs._load_price(conn, "SPY", as_of_date=today)
        price_then = cs._load_price(conn, "SPY", as_of_date=yesterday)
    assert list(price_now.values) == [100.0]
    assert list(price_then.values) == [101.0, 102.0]  # latest vintage per obs_date

    series_now = credit_cycle._get_feature_series(engine, spy, D_OBS, today)
    series_then = credit_cycle._get_feature_series(engine, spy, D_OBS, yesterday)
    assert sorted(series_now.tolist()) == [100.0]
    assert sorted(series_then.tolist()) == [100.0, 101.0, 102.0]

    lookback = (today - D_OBS).days + 1
    assert prediction_context._latest_feature_value(
        engine, ["spy_full"], today, lookback_days=lookback) == 100.0
    assert prediction_context._latest_feature_value(
        engine, ["spy_full"], yesterday, lookback_days=lookback) == 102.0


def test_volume_panel_honours_retractions(scratch):
    from alpha_research.data import panel_builder

    _run(scratch, "upgrade")
    vol = _feature(scratch, "btc_avg_volume")
    _resolved(scratch, vol, D_OBS, V1, 7.0)
    _resolved(scratch, vol, D_OBS2, V1, 8.0)
    _retract(scratch, vol, D_OBS2, V1)
    today = _db_today(scratch)

    now = panel_builder.build_volume_panel(
        scratch, ["BTC"], start_date=D_OBS, end_date=D_OBS2, as_of_date=today)
    then = panel_builder.build_volume_panel(
        scratch, ["BTC"], start_date=D_OBS, end_date=D_OBS2,
        as_of_date=today - timedelta(days=1))
    assert now.stack().tolist() == [7.0]
    assert then.stack().tolist() == [7.0, 8.0]


# ---------------------------------------------------------------------------
# Database guarantees
# ---------------------------------------------------------------------------

def test_retractions_are_append_only_and_not_backdatable(seeded):
    engine, spy, _ = seeded
    _retract(engine, spy, D_OBS, V2)

    with pytest.raises(DBAPIError, match="append-only"):
        with engine.begin() as conn:
            conn.execute(text("UPDATE resolved_series_retractions SET reason = 'x'"))
    with pytest.raises(DBAPIError, match="append-only"):
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM resolved_series_retractions"))
    with pytest.raises(DBAPIError, match="cannot be backdated"):
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO resolved_series_retractions "
                "(feature_id, obs_date, vintage_date, retracted_at, reason, run_tag) "
                "VALUES (:f, :o, :v, now() - interval '1 day', 'r', 't')"
            ), {"f": spy, "o": D_OBS, "v": V1})
    with pytest.raises(DBAPIError):  # duplicate key
        _retract(engine, spy, D_OBS, V2)
    with pytest.raises(DBAPIError):  # blank reason
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO resolved_series_retractions "
                "(feature_id, obs_date, vintage_date, reason, run_tag) "
                "VALUES (:f, :o, :v, '  ', 't')"
            ), {"f": spy, "o": D_OBS, "v": V1})


def test_retraction_must_name_a_real_row_and_pins_it(seeded):
    engine, spy, _ = seeded
    with pytest.raises(DBAPIError, match="fk_resolved_series_retractions_row"):
        _retract(engine, spy, D_OBS, date(2024, 1, 16))  # no such vintage

    _retract(engine, spy, D_OBS, V2)
    with pytest.raises(DBAPIError, match="fk_resolved_series_retractions_row"):
        with engine.begin() as conn:
            conn.execute(text(
                "DELETE FROM resolved_series WHERE feature_id = :f AND obs_date = :o "
                "AND vintage_date = :v"
            ), {"f": spy, "o": D_OBS, "v": V2})
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM resolved_series")).scalar_one() == 4


def test_downgrade_refuses_while_retractions_exist(seeded):
    engine, spy, _ = seeded
    _retract(engine, spy, D_OBS, V2)
    with pytest.raises(DBAPIError, match="un-retract"):
        _run(engine, "downgrade")
    with engine.connect() as conn:
        assert conn.execute(text(
            "SELECT count(*) FROM resolved_series_retractions")).scalar_one() == 1


def test_downgrade_drops_empty_table_and_guard(scratch):
    _run(scratch, "upgrade")
    _run(scratch, "downgrade")
    with scratch.connect() as conn:
        assert conn.execute(text(
            "SELECT to_regclass('resolved_series_retractions')")).scalar() is None
        assert conn.execute(text(
            "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
            "WHERE p.proname = 'resolved_series_retractions_guard' "
            "AND n.nspname = current_schema()")).scalar_one() == 0
    _run(scratch, "downgrade")  # no table: still a clean no-op
