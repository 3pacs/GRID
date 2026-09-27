"""Real-PostgreSQL proof for migrations/versions/godview_writers_20260926.py (god view G2).

Runs in a throwaway schema (random name, dropped afterwards). The three pillar
tables and the ``market_god_view_daily`` matview are built by running the real
``god_view_market_tables_20260918`` upgrade there, on top of minimal
``market_briefings`` / ``insider_trades`` tables, so the starting point has the
exact production DDL and the real matview dependency. Proves:

* upgrade keeps legacy rows byte-for-byte and leaves them ``provenance IS NULL``;
* the relaxed columns accept NULL, the natural keys and raw legs stay NOT NULL;
* the matview still refreshes after the upgrade (the dependency is untouched);
* the row guards: a provenance row must carry the full receipt, GEX rows are
  only ``modeled``, run_id must exist in ``godview_runs`` at commit (deferred);
* upgrade is idempotent and its SET LOCAL timeouts are transaction-scoped;
* downgrade refuses once writer rows exist, and otherwise restores the exact
  original column set and nullability.

Uses the shared ``pg_engine`` fixture's URL (``GRID_TEST_DB_URL``); skips when
no PostgreSQL is reachable (the dedicated CI step fails on a skip).
"""

from __future__ import annotations

import importlib
import json
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

_MIGRATION_MODULE = "migrations.versions.godview_writers_20260926"
_BASE_MODULE = "migrations.versions.god_view_market_tables_20260918"
_TABLES = ("cftc_positioning_daily", "fed_net_liquidity_daily", "dealer_gex_daily")

_PREREQ_DDL = (
    """
    CREATE TABLE market_briefings (
        id              SERIAL PRIMARY KEY,
        briefing_type   TEXT NOT NULL,
        briefing_date   DATE NOT NULL,
        content         TEXT NOT NULL,
        snapshot_data   JSONB,
        created_at      TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE insider_trades (
        id          SERIAL PRIMARY KEY,
        trade_date  DATE,
        trade_type  TEXT
    )
    """,
)

_LEGACY_ROWS = (
    """
    INSERT INTO cftc_positioning_daily (
        report_date, contract_code, contract_name, asset_class, total_open_interest,
        commercial_long, commercial_short, commercial_net, noncommercial_long,
        noncommercial_short, noncommercial_net, spec_net_pct_oi, z_score_1y, z_score_3y,
        percentile_3y, crowding_regime)
    VALUES ('2026-09-08', 'ES', 'E-mini S&P 500', 'equity', 2000000, 900000, 1000000,
            -100000, 400000, 300000, 100000, 0.05, -1.2, -2.11, 0.03, 'NEUTRAL')
    """,
    """
    INSERT INTO fed_net_liquidity_daily (
        obs_date, fed_assets_walcl, treasury_tga_wtregen, reverse_repo_rrp,
        net_liquidity_usd_m, liquidity_regime)
    VALUES ('2026-09-16', 6780000, 790000, 5.375, 5990000, 'STABLE')
    """,
    """
    INSERT INTO dealer_gex_daily (
        obs_date, ticker, spot_price, net_gex_usd_m, call_gex_usd_m, put_gex_usd_m,
        gamma_flip_strike, spot_to_flip_pct, gex_regime, max_pain_strike,
        put_call_oi_ratio, atm_iv)
    VALUES ('2026-09-18', 'SPY', 750, 1.5, 2.0, -0.5, 300, 0.6, 'LONG_GAMMA', 745, 1.1, 0.25)
    """,
    """
    INSERT INTO market_briefings (briefing_type, briefing_date, content)
    VALUES ('hourly', '2026-09-18', 'x')
    """,
)

_COLUMNS_SQL = text(
    "SELECT table_name, column_name, data_type, is_nullable, column_default "
    "FROM information_schema.columns "
    "WHERE table_schema = current_schema() AND table_name = ANY(:tables) "
    "ORDER BY table_name, ordinal_position"
)


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"godview_g2_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        with engine.begin() as conn:
            for ddl in _PREREQ_DDL:
                conn.execute(text(ddl))
        _run(engine, _BASE_MODULE, "upgrade")
        with engine.begin() as conn:
            for sql in _LEGACY_ROWS:
                conn.execute(text(sql))
            conn.execute(text("REFRESH MATERIALIZED VIEW market_god_view_daily"))
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _run(engine: Engine, module: str, fn_name: str) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(module)
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


def _columns(engine: Engine) -> list[tuple]:
    with engine.connect() as conn:
        return [tuple(r) for r in conn.execute(_COLUMNS_SQL, {"tables": list(_TABLES)})]


def _nullable(engine: Engine) -> dict[tuple[str, str], bool]:
    return {(r[0], r[1]): r[3] == "YES" for r in _columns(engine)}


def _legacy_values(engine: Engine) -> dict[str, list[tuple]]:
    out = {}
    with engine.connect() as conn:
        for table in _TABLES:
            out[table] = [tuple(r) for r in conn.execute(text(f"SELECT * FROM {table} ORDER BY id"))]
    return out


def _insert_run(conn, run_id: str, pillar: str) -> None:
    conn.execute(
        text(
            "INSERT INTO godview_runs (run_id, pillar, status, finished_at, rows_written, code_sha) "
            "VALUES (:r, :p, 'complete', NOW(), 1, 'abc1234')"
        ),
        {"r": run_id, "p": pillar},
    )


def _insert_fed(conn, run_id: str | None, *, obs_date: str = "2026-09-23", provenance: str = "derived",
                source_ref: object = None) -> None:
    ref = {"inputs": ["WALCL", "WTREGEN", "RRPONTSYD"]} if source_ref is None else source_ref
    conn.execute(
        text(
            "INSERT INTO fed_net_liquidity_daily (obs_date, fed_assets_walcl, treasury_tga_wtregen, "
            "reverse_repo_rrp, net_liquidity_usd_m, release_at, available_at, availability_basis, "
            "provenance, source_ref, run_id, code_sha, updated_at) "
            "VALUES (:d, 6700000, 800000, 10, 5890000, '2026-09-24 20:30Z', '2026-09-24 21:02Z', "
            "'observed_acquisition', :prov, CAST(:ref AS JSONB), CAST(:run AS UUID), 'abc1234', NOW())"
        ),
        {"d": obs_date, "prov": provenance, "ref": json.dumps(ref), "run": run_id},
    )


# --------------------------------------------------------------------- upgrade


def test_upgrade_keeps_legacy_rows_and_marks_them_unverified(scratch):
    before = _legacy_values(scratch)
    _run(scratch, _MIGRATION_MODULE, "upgrade")
    after = _legacy_values(scratch)

    for table in _TABLES:
        assert len(after[table]) == len(before[table]) == 1
        # Existing columns come first (ADD COLUMN appends), so the prefix is the legacy row.
        assert after[table][0][: len(before[table][0])] == before[table][0]
        assert all(v is None for v in after[table][0][len(before[table][0]):])
    with scratch.connect() as conn:
        for table in _TABLES:
            assert conn.execute(text(f"SELECT count(*) FROM {table} WHERE provenance IS NULL")).scalar() == 1


def test_upgrade_relaxes_only_the_fabrication_forcing_not_nulls(scratch):
    _run(scratch, _MIGRATION_MODULE, "upgrade")
    nullable = _nullable(scratch)

    migration = importlib.import_module(_MIGRATION_MODULE)
    assert nullable[("cftc_positioning_daily", "crowding_regime")]
    assert nullable[("fed_net_liquidity_daily", "liquidity_regime")]
    for col in migration.GEX_LEGACY_VALUE_COLUMNS:
        assert nullable[("dealer_gex_daily", col)], col

    for key in [
        ("cftc_positioning_daily", "report_date"), ("cftc_positioning_daily", "contract_code"),
        ("cftc_positioning_daily", "total_open_interest"), ("cftc_positioning_daily", "noncommercial_net"),
        ("fed_net_liquidity_daily", "obs_date"), ("fed_net_liquidity_daily", "fed_assets_walcl"),
        ("fed_net_liquidity_daily", "net_liquidity_usd_m"),
        ("dealer_gex_daily", "obs_date"), ("dealer_gex_daily", "ticker"),
    ]:
        assert not nullable[key], f"{key} must stay NOT NULL"


def test_matview_still_refreshes_after_upgrade(scratch):
    _run(scratch, _MIGRATION_MODULE, "upgrade")
    with scratch.begin() as conn:
        conn.execute(text("REFRESH MATERIALIZED VIEW market_god_view_daily"))
        row = conn.execute(text(
            "SELECT es_spec_zscore, spy_gamma_flip FROM market_god_view_daily"
        )).one()
    assert row.es_spec_zscore == pytest.approx(-2.11)
    assert row.spy_gamma_flip == pytest.approx(300)


def test_upgrade_is_idempotent(scratch):
    _run(scratch, _MIGRATION_MODULE, "upgrade")
    first = _columns(scratch)
    _run(scratch, _MIGRATION_MODULE, "upgrade")
    assert _columns(scratch) == first


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


# ------------------------------------------------------------------ row guards


def test_writer_row_with_full_receipt_commits_with_ledger_row_after_it(scratch):
    _run(scratch, _MIGRATION_MODULE, "upgrade")
    run_id = str(uuid4())
    with scratch.begin() as conn:
        _insert_fed(conn, run_id)  # row first: the FK is deferred to commit
        _insert_run(conn, run_id, "fed_liquidity")
    with scratch.connect() as conn:
        assert conn.execute(text(
            "SELECT count(*) FROM fed_net_liquidity_daily WHERE provenance = 'derived' AND liquidity_regime IS NULL"
        )).scalar() == 1


def test_run_id_must_exist_in_the_ledger_at_commit(scratch):
    _run(scratch, _MIGRATION_MODULE, "upgrade")
    with pytest.raises(DBAPIError), scratch.begin() as conn:
        _insert_fed(conn, str(uuid4()))


@pytest.mark.parametrize("column", ["run_id", "code_sha", "source_ref", "release_at", "available_at",
                                    "availability_basis"])
def test_provenance_row_without_its_receipt_is_rejected(scratch, column):
    _run(scratch, _MIGRATION_MODULE, "upgrade")
    run_id = str(uuid4())
    with pytest.raises(DBAPIError), scratch.begin() as conn:
        _insert_run(conn, run_id, "fed_liquidity")
        _insert_fed(conn, run_id)
        conn.execute(text(f"UPDATE fed_net_liquidity_daily SET {column} = NULL WHERE run_id IS NOT NULL"))


@pytest.mark.parametrize("bad", [
    {"provenance": "observed"},
    {"source_ref": ["not", "an", "object"]},
])
def test_domain_checks(scratch, bad):
    _run(scratch, _MIGRATION_MODULE, "upgrade")
    run_id = str(uuid4())
    with pytest.raises(DBAPIError), scratch.begin() as conn:
        _insert_run(conn, run_id, "fed_liquidity")
        _insert_fed(conn, run_id, **bad)


def test_gex_rows_are_modeled_and_need_capture_and_spot_receipts(scratch):
    _run(scratch, _MIGRATION_MODULE, "upgrade")
    insert = text(
        "INSERT INTO dealer_gex_daily (obs_date, ticker, release_at, available_at, availability_basis, "
        "provenance, source_ref, run_id, code_sha, gex_aggregate, regime, chain_capture_batch_id, "
        "chain_capture_completed_at, spot_receipt_id) "
        "VALUES ('2026-09-28', 'SPY', '2026-09-28 20:31Z', '2026-09-28 20:31Z', 'observed_acquisition', "
        ":prov, '{}'::jsonb, CAST(:run AS UUID), 'abc1234', 1.0e9, 'LONG_GAMMA', :batch, "
        "'2026-09-28 20:31Z', :receipt)"
    )
    for prov, batch, receipt in [("measured", "b1", 7), ("modeled", None, 7), ("modeled", "b1", None)]:
        run_id = str(uuid4())
        with pytest.raises(DBAPIError), scratch.begin() as conn:
            _insert_run(conn, run_id, "dealer_gex")
            conn.execute(insert, {"prov": prov, "run": run_id, "batch": batch, "receipt": receipt})

    run_id = str(uuid4())
    with scratch.begin() as conn:
        _insert_run(conn, run_id, "dealer_gex")
        conn.execute(insert, {"prov": "modeled", "run": run_id, "batch": "b1", "receipt": 7})
    with scratch.connect() as conn:
        row = conn.execute(text(
            "SELECT spot_price, net_gex_usd_m, gamma_flip_strike FROM dealer_gex_daily "
            "WHERE provenance = 'modeled'"
        )).one()
    assert tuple(row) == (None, None, None), "new rows leave the legacy value columns NULL"


def test_ledger_status_and_lifecycle_checks(scratch):
    _run(scratch, _MIGRATION_MODULE, "upgrade")
    bad_rows = [
        "INSERT INTO godview_runs (pillar, status, code_sha) VALUES ('fed', 'running', 'abc')",
        "INSERT INTO godview_runs (pillar, status, code_sha) VALUES ('cftc', 'bogus', 'abc')",
        "INSERT INTO godview_runs (pillar, status, code_sha) VALUES ('cftc', 'complete', 'abc')",
        "INSERT INTO godview_runs (pillar, status, finished_at, code_sha) VALUES ('cftc', 'running', NOW(), 'abc')",
        "INSERT INTO godview_runs (pillar, status, code_sha) VALUES ('cftc', 'running', '')",
        "INSERT INTO godview_runs (pillar, status, rows_written, code_sha) VALUES ('cftc', 'running', -1, 'abc')",
    ]
    for sql in bad_rows:
        with pytest.raises(DBAPIError), scratch.begin() as conn:
            conn.execute(text(sql))
    with scratch.begin() as conn:
        run_id = conn.execute(text(
            "INSERT INTO godview_runs (pillar, code_sha) VALUES ('cftc', 'abc') RETURNING run_id"
        )).scalar()
        conn.execute(text(
            "UPDATE godview_runs SET status = 'inputs_stale', finished_at = NOW() WHERE run_id = :r"
        ), {"r": run_id})


# ------------------------------------------------------------------- downgrade


def test_downgrade_restores_the_original_schema(scratch):
    original = _columns(scratch)
    _run(scratch, _MIGRATION_MODULE, "upgrade")
    _run(scratch, _MIGRATION_MODULE, "downgrade")
    assert _columns(scratch) == original
    with scratch.connect() as conn:
        assert conn.execute(text("SELECT to_regclass('godview_runs')")).scalar() is None
        conn.execute(text("REFRESH MATERIALIZED VIEW market_god_view_daily"))


def test_downgrade_refuses_once_writer_rows_exist(scratch):
    _run(scratch, _MIGRATION_MODULE, "upgrade")
    run_id = str(uuid4())
    with scratch.begin() as conn:
        _insert_run(conn, run_id, "fed_liquidity")
        _insert_fed(conn, run_id)

    with pytest.raises(DBAPIError, match="downgrade refused"):
        _run(scratch, _MIGRATION_MODULE, "downgrade")
    assert ("fed_net_liquidity_daily", "provenance") in _nullable(scratch)  # rolled back

    with scratch.begin() as conn:  # stands in for archive + removal by the owner
        conn.execute(text("DELETE FROM fed_net_liquidity_daily WHERE provenance IS NOT NULL"))
    with pytest.raises(DBAPIError, match="downgrade refused"):  # the ledger row still blocks
        _run(scratch, _MIGRATION_MODULE, "downgrade")
    with scratch.begin() as conn:
        conn.execute(text("DELETE FROM godview_runs"))
    _run(scratch, _MIGRATION_MODULE, "downgrade")
    assert ("fed_net_liquidity_daily", "provenance") not in _nullable(scratch)
