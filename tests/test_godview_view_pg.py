"""PostgreSQL contract for the G6 view (``godview_view_v2_20260927``).

Runs only against ``GRID_TEST_DB_URL`` (CI's "Godview G6 view PostgreSQL
contract" step fails when this file is skipped). Each test builds a throwaway
schema from the real ``god_view_market_tables_20260918`` (which creates the
legacy matview), ``godview_writers_20260926`` (G2) and this revision's
``upgrade()``.

The spine is relative to ``now()`` in America/New_York, so the fixture dates
are relative to today too.
"""

from __future__ import annotations

import importlib
import os
import uuid
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

pytestmark = pytest.mark.integration

ET = ZoneInfo("America/New_York")
_BASE = "migrations.versions.god_view_market_tables_20260918"
_G2 = "migrations.versions.godview_writers_20260926"
_G6 = "migrations.versions.godview_view_v2_20260927"
_SHA = "test-sha-godview-g6"

_PREREQ_DDL = (
    """
    CREATE TABLE market_briefings (
        id SERIAL PRIMARY KEY, briefing_type TEXT NOT NULL, briefing_date DATE NOT NULL,
        content TEXT NOT NULL, snapshot_data JSONB, created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    "CREATE TABLE insider_trades (id SERIAL PRIMARY KEY, trade_date DATE, trade_type TEXT)",
)


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


@pytest.fixture
def engine():
    db_url = os.environ.get("GRID_TEST_DB_URL")
    if not db_url:
        pytest.skip("GRID_TEST_DB_URL not set")
    root = create_engine(db_url, pool_pre_ping=True)
    try:
        with root.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001
        root.dispose()
        pytest.skip(f"GRID_TEST_DB_URL set but unreachable: {exc}")
    schema = f"godview_g6_{uuid.uuid4().hex[:12]}"
    with root.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    eng = create_engine(db_url, pool_size=1, max_overflow=0, connect_args={"options": f"-csearch_path={schema}"})
    try:
        with eng.begin() as conn:
            for ddl in _PREREQ_DDL:
                conn.execute(text(ddl))
        _run(eng, _BASE, "upgrade")
        _run(eng, _G2, "upgrade")
        yield eng
    finally:
        eng.dispose()
        with root.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        root.dispose()


def _relkind(conn) -> str | None:
    return conn.execute(
        text("SELECT relkind FROM pg_class WHERE oid = to_regclass('market_god_view_daily')")
    ).scalar()


def _today_et() -> date:
    return datetime.now(ET).date()


def _anchor_wednesday() -> date:
    """A Wednesday 20-26 days ago: far enough back that every fixture date and
    the +10 day stale checks fall inside the view's 366-day spine and before today."""
    d = _today_et() - timedelta(days=20)
    while d.weekday() != 2:
        d -= timedelta(days=1)
    return d


def _utc(d: date, hh: int, mm: int = 0) -> datetime:
    return datetime.combine(d, time(hh, mm), tzinfo=timezone.utc)


def _ledger(conn, pillar: str) -> str:
    run_id = str(uuid.uuid4())
    conn.execute(
        text(
            "INSERT INTO godview_runs (run_id, pillar, started_at, finished_at, status, code_sha) "
            "VALUES (CAST(:r AS UUID), :p, now() - interval '1 minute', now(), 'complete', :sha)"
        ),
        {"r": run_id, "p": pillar, "sha": _SHA},
    )
    return run_id


def _seed(conn, w: date) -> dict:
    fed_run = _ledger(conn, "fed_liquidity")
    cftc_run = _ledger(conn, "cftc")
    gex_run = _ledger(conn, "dealer_gex")
    # Fed: H.4.1 Wednesday w, published Thursday, acquired Thursday evening.
    conn.execute(
        text(
            "INSERT INTO fed_net_liquidity_daily (obs_date, fed_assets_walcl, treasury_tga_wtregen, reverse_repo_rrp, "
            "net_liquidity_usd_m, delta_1w_m, liquidity_regime, release_at, available_at, availability_basis, "
            "provenance, source_ref, run_id, code_sha) VALUES (:d, 6746548, 877028, 5375, 5864145, -1000, "
            "'insufficient_history', :rel, :avail, 'observed_acquisition', 'measured', CAST('{}' AS JSONB), "
            "CAST(:run AS UUID), :sha)"
        ),
        {"d": w, "rel": _utc(w + timedelta(days=1), 20, 30), "avail": _utc(w + timedelta(days=1), 21, 5),
         "run": fed_run, "sha": _SHA},
    )
    # A legacy fed row a week later (the incident's fabricated constant): never shown.
    conn.execute(
        text(
            "INSERT INTO fed_net_liquidity_daily (obs_date, fed_assets_walcl, treasury_tga_wtregen, reverse_repo_rrp, "
            "net_liquidity_usd_m, liquidity_regime) VALUES (:d, 6780000, 790000, 5375, 5984625, 'STABLE')"
        ),
        {"d": w + timedelta(days=7)},
    )
    # CFTC ES: Tuesday report t = w - 1, released Friday t + 3.
    t = w - timedelta(days=1)
    conn.execute(
        text(
            "INSERT INTO cftc_positioning_daily (report_date, contract_code, contract_name, asset_class, "
            "total_open_interest, commercial_long, commercial_short, commercial_net, noncommercial_long, "
            "noncommercial_short, noncommercial_net, spec_net_pct_oi, z_score_3y, percentile_3y, crowding_regime, "
            "release_at, available_at, availability_basis, provenance, source_ref, run_id, code_sha, "
            "cftc_market_code, market_name) VALUES (:d, 'ES', 'E-mini', 'EQUITY', 1000, 400, 500, -100, 300, 100, "
            "200, 20, 1.25, 88, 'ELEVATED_LONG', :rel, :avail, 'observed_acquisition', 'measured', "
            "CAST('{}' AS JSONB), CAST(:run AS UUID), :sha, '13874A', 'E-mini S&P 500 (CME)')"
        ),
        {"d": t, "rel": _utc(t + timedelta(days=3), 19, 30), "avail": _utc(t + timedelta(days=3), 20, 30),
         "run": cftc_run, "sha": _SHA},
    )
    # A legacy ES row a week earlier (market-mixed z-score): never shown.
    conn.execute(
        text(
            "INSERT INTO cftc_positioning_daily (report_date, contract_code, contract_name, asset_class, "
            "total_open_interest, commercial_long, commercial_short, commercial_net, noncommercial_long, "
            "noncommercial_short, noncommercial_net, spec_net_pct_oi, z_score_3y, crowding_regime) "
            "VALUES (:d, 'ES', 'S&P 500 Consolidated', 'EQUITY', 1, 1, 1, 0, 1, 1, 0, 0, -2.11, 'NEUTRAL')"
        ),
        {"d": t - timedelta(days=7)},
    )
    # GEX SPY (modeled): Monday session s = w - 2, captured 20:31Z that day.
    s = w - timedelta(days=2)
    conn.execute(
        text(
            "INSERT INTO dealer_gex_daily (obs_date, ticker, spot, gex_aggregate, gamma_flip, regime, model_basis, "
            "chain_capture_batch_id, chain_capture_completed_at, spot_receipt_id, release_at, available_at, "
            "availability_basis, provenance, source_ref, run_id, code_sha) VALUES (:d, 'SPY', 661.5, -2.5e9, NULL, "
            "'SHORT_GAMMA', 'bs_own_dte', 'batch-1', :c, 7, :c, :c, 'observed_acquisition', 'modeled', "
            "CAST('{}' AS JSONB), CAST(:run AS UUID), :sha)"
        ),
        {"d": s, "c": _utc(s, 20, 31), "run": gex_run, "sha": _SHA},
    )
    # A legacy GEX row (impossible flip at 300): never shown.
    conn.execute(
        text(
            "INSERT INTO dealer_gex_daily (obs_date, ticker, spot_price, net_gex_usd_m, call_gex_usd_m, put_gex_usd_m, "
            "gamma_flip_strike, spot_to_flip_pct, gex_regime, max_pain_strike, put_call_oi_ratio, atm_iv) "
            "VALUES (:d, 'SPY', 750, 1, 1, 1, 300, 1, 'LONG_GAMMA', 1, 1, 0.25)"
        ),
        {"d": w + timedelta(days=1)},
    )
    return {"w": w, "t": t, "s": s}


def _day(conn, d: date):
    return conn.execute(
        text("SELECT * FROM market_god_view_daily WHERE as_of_date = :d"), {"d": d}
    ).mappings().one()


def test_upgrade_replaces_the_matview_with_a_plain_view(engine):
    with engine.begin() as conn:
        assert _relkind(conn) == "m"
    _run(engine, _G6, "upgrade")
    with engine.begin() as conn:
        assert _relkind(conn) == "v"
        n, n_distinct, latest = conn.execute(text(
            "SELECT count(*), count(DISTINCT as_of_date), max(as_of_date) FROM market_god_view_daily"
        )).one()
        assert n == n_distinct == 366
        assert latest == conn.execute(text("SELECT (now() AT TIME ZONE 'America/New_York')::date")).scalar()
        cols = set(conn.execute(text("SELECT * FROM market_god_view_daily LIMIT 0")).keys())
    for gone in ("sp500_close", "cushing_crude_m_bbl", "insider_buys", "spy_net_gex"):
        assert gone not in cols
    for present in ("fed_available_at", "fed_basis", "fed_stale", "es_release_at", "zn_z_score_3y",
                    "spy_gex_provenance", "spy_gex_stale", "known_before"):
        assert present in cols


def test_empty_pillars_read_null_not_fresh(engine):
    _run(engine, _G6, "upgrade")
    with engine.begin() as conn:
        row = _day(conn, _today_et())
    assert row["fed_obs_date"] is None and row["fed_stale"] is None
    assert row["es_report_date"] is None and row["es_stale"] is None
    assert row["spy_gex_obs_date"] is None and row["spy_gex_stale"] is None


def test_point_in_time_join_and_legacy_exclusion(engine):
    w = _anchor_wednesday()
    with engine.begin() as conn:
        dates = _seed(conn, w)
    _run(engine, _G6, "upgrade")
    t, s = dates["t"], dates["s"]
    with engine.begin() as conn:
        # Fed: not on its observation Wednesday, only once acquired Thursday.
        assert _day(conn, w)["fed_obs_date"] is None
        thu = _day(conn, w + timedelta(days=1))
        assert thu["fed_obs_date"] == w and thu["fed_net_liquidity_usd_m"] == 5864145
        assert thu["fed_basis"] == "observed_acquisition" and thu["fed_provenance"] == "measured"
        assert thu["fed_stale"] is False
        # The later legacy row never replaces it.
        assert _day(conn, w + timedelta(days=8))["fed_net_liquidity_usd_m"] == 5864145
        assert _day(conn, w + timedelta(days=9))["fed_stale"] is False
        assert _day(conn, w + timedelta(days=10))["fed_stale"] is True

        # CFTC ES: Tuesday's report is not visible Tuesday..Thursday (the old
        # matview joined it on Tuesday), and the legacy -2.11 never appears.
        for offset in (0, 1, 2):
            r = _day(conn, t + timedelta(days=offset))
            assert r["es_report_date"] is None and r["es_z_score_3y"] is None
        fri = _day(conn, t + timedelta(days=3))
        assert fri["es_report_date"] == t and float(fri["es_z_score_3y"]) == 1.25
        assert fri["es_crowding_regime"] == "ELEVATED_LONG" and fri["es_stale"] is False
        assert fri["zn_report_date"] is None and fri["zn_stale"] is None
        # Stale once the end of the day is more than 10 days after the Friday
        # 19:30Z release: day t+12 ends ~9d 8.5h after it, day t+13 ~10d 8.5h.
        assert _day(conn, t + timedelta(days=12))["es_stale"] is False
        assert _day(conn, t + timedelta(days=13))["es_stale"] is True

        # GEX SPY: modeled row visible on its session day; stale once a later
        # weekday passes; the legacy flip at 300 never appears.
        mon = _day(conn, s)
        assert mon["spy_gex_obs_date"] == s and mon["spy_gex_provenance"] == "modeled"
        assert mon["spy_gamma_flip"] is None and mon["spy_gex_stale"] is False
        assert _day(conn, s + timedelta(days=1))["spy_gex_stale"] is False
        later = _day(conn, s + timedelta(days=3))
        assert later["spy_gex_stale"] is True and later["spy_gex_obs_date"] == s
        flips = conn.execute(text(
            "SELECT count(*) FROM market_god_view_daily WHERE spy_gamma_flip = 300"
        )).scalar()
        assert flips == 0


def test_downgrade_restores_the_legacy_matview_and_upgrade_reruns(engine):
    _run(engine, _G6, "upgrade")
    _run(engine, _G6, "downgrade")
    with engine.begin() as conn:
        assert _relkind(conn) == "m"
        cols = set(conn.execute(text("SELECT * FROM market_god_view_daily LIMIT 0")).keys())
        assert {"sp500_close", "es_spec_zscore", "spy_gamma_flip", "insider_buys"} <= cols
        assert conn.execute(text("SELECT to_regclass('idx_god_view_as_of')")).scalar() is not None
    _run(engine, _G6, "upgrade")
    with engine.begin() as conn:
        assert _relkind(conn) == "v"


def test_upgrade_refuses_when_something_depends_on_the_matview(engine):
    with engine.begin() as conn:
        conn.execute(text("CREATE VIEW gv_dependent AS SELECT as_of_date FROM market_god_view_daily"))
    with pytest.raises(Exception, match="depend"):
        _run(engine, _G6, "upgrade")
    with engine.begin() as conn:
        assert _relkind(conn) == "m"  # rolled back whole
