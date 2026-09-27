"""DB-gated tests for godview/cftc_positioning.py (G4) and the G7 runner's
transaction wrapper (scripts/run_godview_writers.py).

Runs only against ``GRID_TEST_DB_URL`` (a disposable PostgreSQL; CI:
``postgresql://grid:testpass@localhost:5432/griddb_test``). Each test gets its
own throwaway schema built by the real ``god_view_market_tables_20260918`` and
``godview_writers_20260926`` (#674, G2) migrations, the same fixture shape as
``test_fed_liquidity_writer_pg.py`` (G3). CI fails the step if anything here is
skipped.
"""

from __future__ import annotations

import argparse
import importlib
import os
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

import godview.cftc_positioning as cp
from ingestion.altdata.cftc_markets import compute_release, series_id
from scripts import run_godview_writers as runner

pytestmark = pytest.mark.integration

_CODE_SHA = "test-sha-godview-g4"
_BASE_MIGRATION = "migrations.versions.god_view_market_tables_20260918"
_G2_MIGRATION = "migrations.versions.godview_writers_20260926"

ES = "13874A"
GC = "088691"

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
    """
    CREATE TABLE source_catalog (
        id                SERIAL PRIMARY KEY,
        name              TEXT NOT NULL UNIQUE,
        base_url          TEXT NOT NULL DEFAULT 'https://publicreporting.cftc.gov',
        cost_tier         TEXT NOT NULL DEFAULT 'FREE',
        latency_class     TEXT NOT NULL DEFAULT 'WEEKLY',
        pit_available     BOOLEAN NOT NULL DEFAULT TRUE,
        revision_behavior TEXT NOT NULL DEFAULT 'NEVER',
        trust_score       TEXT NOT NULL DEFAULT 'HIGH',
        priority_rank     INTEGER NOT NULL DEFAULT 30
    )
    """,
    """
    CREATE TABLE raw_series (
        id                BIGSERIAL PRIMARY KEY,
        series_id         TEXT NOT NULL,
        source_id         INTEGER NOT NULL REFERENCES source_catalog(id),
        obs_date          DATE NOT NULL,
        pull_timestamp    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        value             DOUBLE PRECISION NOT NULL,
        raw_payload       JSONB,
        pull_status       TEXT NOT NULL CHECK (pull_status IN ('SUCCESS', 'PARTIAL', 'FAILED'))
    )
    """,
    "INSERT INTO source_catalog (name) VALUES ('CFTC_COT'), ('FRED'), ('OTHER_PULLER')",
)


def _run_migration(engine: Engine, module: str, fn_name: str) -> None:
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
def gv():
    db_url = os.environ.get("GRID_TEST_DB_URL")
    if not db_url:
        pytest.skip("GRID_TEST_DB_URL not set")
    root_engine = create_engine(db_url, pool_pre_ping=True)
    try:
        with root_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001
        root_engine.dispose()
        pytest.skip(f"GRID_TEST_DB_URL set but unreachable: {exc}")

    schema = f"godview_cftc_{uuid.uuid4().hex[:12]}"
    with root_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(
        db_url, pool_size=1, max_overflow=0, connect_args={"options": f"-csearch_path={schema}"}
    )
    try:
        with engine.begin() as conn:
            for ddl in _PREREQ_DDL:
                conn.execute(text(ddl))
        _run_migration(engine, _BASE_MIGRATION, "upgrade")
        _run_migration(engine, _G2_MIGRATION, "upgrade")
        yield engine
    finally:
        engine.dispose()
        with root_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        root_engine.dispose()


# ── fixture data helpers ─────────────────────────────────────────────────

BACKFILL_PULL = datetime(2026, 9, 27, 3, 0, tzinfo=timezone.utc)
FIRST_TUESDAY = date(2023, 1, 3)


def _insert(conn, sid: str, d: date, value: float, pull: datetime, source: str = "CFTC_COT", status: str = "SUCCESS"):
    conn.execute(
        text(
            "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_timestamp, pull_status) "
            "VALUES (:sid, (SELECT id FROM source_catalog WHERE name = :src), :d, :v, :p, :st)"
        ),
        {"sid": sid, "src": source, "d": d, "v": value, "p": pull, "st": status},
    )


def _insert_week(
    conn,
    code: str,
    d: date,
    *,
    net: int = 0,
    pull: datetime = BACKFILL_PULL,
    skip: tuple[str, ...] = (),
    oi: int = 100_000,
) -> None:
    ncl, ncs = 20_000 + net, 20_000
    values = {
        "commercial_long": 30_000,
        "commercial_short": 35_000,
        "noncommercial_long": ncl,
        "noncommercial_short": ncs,
        "total_open_interest": oi,
        "net_speculative": ncl - ncs,
    }
    for metric, v in values.items():
        if metric not in skip:
            _insert(conn, series_id(code, metric), d, float(v), pull)


def _insert_history(conn, code: str, n_weeks: int, *, start: date = FIRST_TUESDAY, pull: datetime = BACKFILL_PULL):
    for i in range(n_weeks):
        _insert_week(conn, code, start + timedelta(days=7 * i), net=((i * 37) % 101) - 50, pull=pull)


def _row(conn, d: date, contract_code: str = "ES"):
    return conn.execute(
        text("SELECT * FROM cftc_positioning_daily WHERE report_date = :d AND contract_code = :c"),
        {"d": d, "c": contract_code},
    ).mappings().fetchone()


def _ledger(conn, run_id: str):
    return conn.execute(
        text("SELECT * FROM godview_runs WHERE run_id = CAST(:r AS UUID)"), {"r": run_id}
    ).mappings().fetchone()


_COUNT_SQL = {
    "cftc_positioning_daily": text("SELECT COUNT(*) FROM cftc_positioning_daily"),
    "fed_net_liquidity_daily": text("SELECT COUNT(*) FROM fed_net_liquidity_daily"),
    "godview_runs": text("SELECT COUNT(*) FROM godview_runs"),
}


def _count(conn, table: str) -> int:
    return conn.execute(_COUNT_SQL[table]).scalar()


NOW = datetime(2026, 9, 27, 12, 0, tzinfo=timezone.utc)


# ── happy path, provenance, ledger ────────────────────────────────────────


def test_backfilled_history_writes_provenance_rows_as_inferred_schedule(gv):
    with gv.begin() as conn:
        _insert_history(conn, ES, 160)
    res = cp.materialize_cftc_positioning(gv, code_sha=_CODE_SHA, as_of_ts=NOW, market_codes=[ES])
    assert res.status == cp.STATUS_SUCCESS
    assert res.rows_written == 160

    last = FIRST_TUESDAY + timedelta(days=7 * 159)
    with gv.begin() as conn:
        row = _row(conn, last)
        ledger = _ledger(conn, res.run_id)
    assert row["provenance"] == "measured"
    assert row["cftc_market_code"] == ES
    assert row["contract_code"] == "ES" and row["asset_class"] == "EQUITY"
    assert row["code_sha"] == _CODE_SHA and str(row["run_id"]) == res.run_id
    assert row["release_at"] == compute_release(last).release_at
    assert row["available_at"] == BACKFILL_PULL  # pulled long after release
    assert row["availability_basis"] == "inferred_schedule"
    assert row["noncommercial_net"] == row["noncommercial_long"] - row["noncommercial_short"]
    assert row["spec_net_pct_oi"] == pytest.approx(row["noncommercial_net"] / 100_000 * 100)
    assert row["z_score_1y"] is not None and row["z_score_3y"] is not None
    assert row["crowding_regime"] in cp.CROWDING_REGIMES
    assert row["source_ref"]["market_code"] == ES
    assert ledger["pillar"] == "cftc" and ledger["status"] == "complete" and ledger["rows_written"] == 160

    with gv.begin() as conn:
        early = _row(conn, FIRST_TUESDAY)
    assert early["z_score_1y"] is None and early["crowding_regime"] is None  # never a default NEUTRAL


def test_live_pull_on_release_friday_is_observed_acquisition(gv):
    live = date(2026, 9, 29)  # after the history backfill pull
    release_at = compute_release(live).release_at
    live_pull = release_at + timedelta(minutes=20)
    with gv.begin() as conn:
        _insert_history(conn, ES, 159, start=live - timedelta(days=7 * 159))
        _insert_week(conn, ES, live, net=5, pull=live_pull)
    res = cp.materialize_cftc_positioning(
        gv, code_sha=_CODE_SHA, as_of_ts=release_at + timedelta(hours=1), market_codes=[ES], start=live
    )
    assert res.status == cp.STATUS_SUCCESS and res.rows_written == 1
    with gv.begin() as conn:
        row = _row(conn, live)
    # The last input of the whole window is this row's own Friday pull.
    assert BACKFILL_PULL < live_pull
    assert row["available_at"] == live_pull
    assert row["availability_basis"] == "observed_acquisition"
    assert row["source_ref"]["window"]["n_obs_3y"] == 156


def test_rerun_with_unchanged_inputs_is_a_noop(gv):
    with gv.begin() as conn:
        _insert_history(conn, ES, 60)
    first = cp.materialize_cftc_positioning(gv, code_sha=_CODE_SHA, as_of_ts=NOW, market_codes=[ES])
    second = cp.materialize_cftc_positioning(gv, code_sha="other-sha", as_of_ts=NOW, market_codes=[ES])
    assert first.rows_written == 60
    assert second.status == cp.STATUS_SUCCESS_NOOP and second.rows_written == 0
    with gv.begin() as conn:
        assert _ledger(conn, second.run_id)["status"] == "noop"
        assert _row(conn, FIRST_TUESDAY)["code_sha"] == _CODE_SHA  # untouched


# ── legacy rows are never touched ─────────────────────────────────────────


def test_legacy_row_is_skipped_untouched_and_run_is_partial(gv):
    d = FIRST_TUESDAY + timedelta(days=7 * 10)
    with gv.begin() as conn:
        _insert_history(conn, ES, 20)
        conn.execute(
            text(
                "INSERT INTO cftc_positioning_daily (report_date, contract_code, contract_name, asset_class, "
                "total_open_interest, commercial_long, commercial_short, commercial_net, noncommercial_long, "
                "noncommercial_short, noncommercial_net, spec_net_pct_oi, z_score_3y, crowding_regime) "
                "VALUES (:d, 'ES', 'E-mini S&P 500', 'EQUITY', 1, 0, 0, 0, 0, 0, 0, 0.0, -2.11, 'NEUTRAL')"
            ),
            {"d": d},
        )
    res = cp.materialize_cftc_positioning(gv, code_sha=_CODE_SHA, as_of_ts=NOW, market_codes=[ES])
    assert res.status == cp.STATUS_PARTIAL_BLOCKED_BY_LEGACY
    assert res.rows_written == 19
    assert [r.reason for r in res.rows if r.report_date == d] == [cp.SKIP_BLOCKED_BY_LEGACY_ROW]
    with gv.begin() as conn:
        legacy = _row(conn, d)
        ledger = _ledger(conn, res.run_id)
    assert legacy["provenance"] is None and legacy["z_score_3y"] == -2.11 and legacy["total_open_interest"] == 1
    assert ledger["status"] == "partial_blocked_by_legacy"
    assert ledger["reasons"][cp.SKIP_BLOCKED_BY_LEGACY_ROW] == 1


# ── fail closed ───────────────────────────────────────────────────────────


def test_missing_leg_writes_no_row(gv):
    d = FIRST_TUESDAY
    with gv.begin() as conn:
        _insert_week(conn, ES, d, skip=("commercial_short",))
    res = cp.materialize_cftc_positioning(gv, code_sha=_CODE_SHA, as_of_ts=NOW, market_codes=[ES])
    assert res.rows_written == 0
    assert [(r.reason, r.detail) for r in res.rows] == [(cp.SKIP_MISSING_LEG, "commercial_short")]
    with gv.begin() as conn:
        assert _row(conn, d) is None


def test_failed_pull_marker_and_other_sources_are_never_observations(gv):
    d = FIRST_TUESDAY
    with gv.begin() as conn:
        _insert_week(conn, ES, d)
        # A FAILED zero marker dated later, and a second puller under the same id.
        _insert(conn, series_id(ES, "total_open_interest"), d + timedelta(days=7), 0.0, NOW, status="FAILED")
    res = cp.materialize_cftc_positioning(gv, code_sha=_CODE_SHA, as_of_ts=NOW, market_codes=[ES])
    assert res.rows_written == 1  # the FAILED marker week contributes nothing
    with gv.begin() as conn:
        _insert(conn, series_id(ES, "commercial_long"), d, 1.0, NOW, source="OTHER_PULLER")
    again = cp.materialize_cftc_positioning(gv, code_sha=_CODE_SHA, as_of_ts=NOW, market_codes=[ES])
    assert again.status == cp.STATUS_SUCCESS_NOOP  # source="CFTC_COT" pins the read


def test_legacy_name_keyed_ids_are_never_read(gv):
    with gv.begin() as conn:
        for metric in cp.ALL_METRICS:
            _insert(conn, "cftc.SP500." + metric, FIRST_TUESDAY, 10.0, BACKFILL_PULL)
    res = cp.materialize_cftc_positioning(gv, code_sha=_CODE_SHA, as_of_ts=NOW, market_codes=[ES])
    assert res.status == cp.STATUS_EMPTY
    with gv.begin() as conn:
        assert _count(conn, "cftc_positioning_daily") == 0
        assert _ledger(conn, res.run_id)["status"] == "inputs_missing"


def test_non_tuesday_report_date_is_skipped_but_counts_as_history(gv):
    monday = date(2018, 12, 24)
    next_tuesday = date(2019, 1, 8)
    with gv.begin() as conn:
        _insert_week(conn, ES, date(2018, 12, 18), net=1)
        _insert_week(conn, ES, monday, net=2)
        _insert_week(conn, ES, next_tuesday, net=3)
    res = cp.materialize_cftc_positioning(gv, code_sha=_CODE_SHA, as_of_ts=NOW, market_codes=[ES])
    assert res.no_release_rule_dates == {ES: (monday,)}
    assert [r.reason for r in res.rows if r.report_date == monday] == [cp.SKIP_NO_RELEASE_RULE]
    with gv.begin() as conn:
        assert _row(conn, monday) is None
        after = _row(conn, next_tuesday)
        assert _ledger(conn, res.run_id)["reasons"][cp.SKIP_NO_RELEASE_RULE] == 1
    assert after["source_ref"]["window"]["n_obs_1y"] == 3


def test_before_release_gate_and_pull_bound(gv):
    d = date(2026, 9, 22)
    release_at = compute_release(d).release_at
    with gv.begin() as conn:
        _insert_week(conn, ES, d, pull=release_at - timedelta(hours=2))  # an early (leaked) pull
    early = cp.materialize_cftc_positioning(
        gv, code_sha=_CODE_SHA, as_of_ts=release_at - timedelta(minutes=1), market_codes=[ES]
    )
    assert [r.reason for r in early.rows] == [cp.SKIP_BEFORE_RELEASE]
    before_pull = cp.materialize_cftc_positioning(
        gv, code_sha=_CODE_SHA, as_of_ts=release_at - timedelta(hours=3), market_codes=[ES]
    )
    assert before_pull.status == cp.STATUS_EMPTY  # the pull is after as_of_ts: invisible
    ok = cp.materialize_cftc_positioning(gv, code_sha=_CODE_SHA, as_of_ts=release_at, market_codes=[ES])
    assert ok.rows_written == 1
    with gv.begin() as conn:
        assert _row(conn, d)["available_at"] == release_at  # clamped: never before release


def test_later_reports_do_not_change_an_earlier_rows_statistics(gv):
    """PIT windows: row R computed with all data equals row R computed with only data <= R."""
    target = FIRST_TUESDAY + timedelta(days=7 * 150)
    with gv.begin() as conn:
        _insert_history(conn, ES, 151)
    cp.materialize_cftc_positioning(gv, code_sha=_CODE_SHA, as_of_ts=NOW, market_codes=[ES])
    with gv.begin() as conn:
        before = dict(_row(conn, target))
        _insert_history(conn, ES, 40, start=target + timedelta(days=7))
    res = cp.materialize_cftc_positioning(gv, code_sha=_CODE_SHA, as_of_ts=NOW, market_codes=[ES])
    assert res.rows_written == 40  # only the new weeks; the target row is a no-op
    with gv.begin() as conn:
        after = dict(_row(conn, target))
    for col in ("z_score_1y", "z_score_3y", "percentile_3y", "crowding_regime", "available_at"):
        assert after[col] == before[col]


def test_start_limits_written_rows_but_windows_see_history(gv):
    with gv.begin() as conn:
        _insert_history(conn, ES, 160)
    start = FIRST_TUESDAY + timedelta(days=7 * 155)
    res = cp.materialize_cftc_positioning(gv, code_sha=_CODE_SHA, as_of_ts=NOW, start=start, market_codes=[ES])
    assert res.rows_written == 5
    with gv.begin() as conn:
        row = _row(conn, start)
    assert row["source_ref"]["window"]["n_obs_3y"] == 156 and row["z_score_3y"] is not None


def test_markets_are_independent(gv):
    with gv.begin() as conn:
        _insert_history(conn, ES, 10)
    res = cp.materialize_cftc_positioning(gv, code_sha=_CODE_SHA, as_of_ts=NOW, market_codes=[ES, GC])
    assert res.rows_written == 10 and res.markets_without_observations == (GC,)
    with gv.begin() as conn:
        assert _ledger(conn, res.run_id)["input_watermarks"][GC] is None


# ── G7 runner: dry run persists nothing, live run commits ─────────────────


def _args(**kw) -> argparse.Namespace:
    base = {"pillar": "cftc", "code_sha": "abcdef1", "code_sha_from_git": False, "dry_run": False,
            "start": None, "as_of_ts": NOW}
    base.update(kw)
    return argparse.Namespace(**base)


def test_runner_dry_run_rolls_back_rows_and_ledger(gv):
    with gv.begin() as conn:
        _insert_history(conn, ES, 5)
    code, summary = runner.run(_args(dry_run=True), gv)
    assert code == runner.EXIT_OK
    assert summary["dry_run"] is True and summary["persisted"] is False
    assert summary["rows_written"] == 5  # what a live run would write
    with gv.begin() as conn:
        assert _count(conn, "cftc_positioning_daily") == 0
        assert _count(conn, "godview_runs") == 0


def test_runner_live_run_commits_with_the_given_sha(gv):
    with gv.begin() as conn:
        _insert_history(conn, ES, 5)
    code, summary = runner.run(_args(), gv)
    assert code == runner.EXIT_OK and summary["status"] == cp.STATUS_SUCCESS
    with gv.begin() as conn:
        assert _count(conn, "cftc_positioning_daily") == 5
        assert conn.execute(text("SELECT code_sha FROM godview_runs")).scalar() == "abcdef1"


def test_runner_fed_dry_run_persists_nothing(gv):
    wed = date(2026, 9, 16)
    with gv.begin() as conn:
        for sid, v in (("WALCL", 7_000_000.0), ("WTREGEN", 800_000.0), ("RRPONTSYD", 5.0)):
            _insert(conn, sid, wed, v, datetime(2026, 9, 17, 21, 0, tzinfo=timezone.utc), source="FRED")
    code, summary = runner.run(_args(pillar="fed", dry_run=True), gv)
    assert code == runner.EXIT_OK and summary["rows_written"] == 1
    with gv.begin() as conn:
        assert _count(conn, "fed_net_liquidity_daily") == 0
        assert _count(conn, "godview_runs") == 0


def test_runner_sets_transaction_timeouts(gv):
    tx = runner.TransactionEngine(gv, commit=False)
    with tx.begin() as conn:
        assert conn.execute(text("SHOW lock_timeout")).scalar() == runner.LOCK_TIMEOUT
        assert conn.execute(text("SHOW statement_timeout")).scalar() == "1min"


def test_runner_empty_upstream_exit_code(gv):
    code, summary = runner.run(_args(), gv)
    assert code == runner.EXIT_EMPTY and summary["status"] == cp.STATUS_EMPTY
