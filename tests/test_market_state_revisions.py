"""market_state_daily keeps every version it replaces (#1191, migrations/versions/market_state_revs_20261125.py).

The schema-parity, replacement-argument and CLI tests run everywhere. The rest need real PostgreSQL (the trigger is
plpgsql): they run in a throwaway schema on the shared ``pg_engine`` fixture (``GRID_TEST_DB_URL``), and CI runs
this file in its own step that fails on any skip. Against a real database they prove:

* the migration keeps existing rows, creates the table on a database that never had it, and installs the trigger;
* a default write for a date that has a row changes nothing, and the trigger, not the writer, stamps
  ``computed_at`` in UTC (a writer's own value is ignored, and a session in another time zone cannot shift it);
* a replacement keeps both versions: the old one with its clock, when it was superseded, the reason and the run;
  the new one starts exactly when the old one ends; an identical replacement writes nothing;
* a change without a reason (a hand UPDATE or DELETE) is archived as unattributed; an UPDATE of ``computed_at``
  alone is ignored; TRUNCATE and as_of_date changes are refused; revisions and the epoch are append-only;
* a replacement on a database without the trigger refuses and leaves the row as it was;
* the desk brief, replayed between the original write and the revision, shows the original;
* downgrade refuses while revisions exist.
"""

from __future__ import annotations

import importlib
import json
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

from analysis import desk_brief as B
from analysis import desk_brief_sources as S
from intelligence.regime import market_state as MS
from scripts import run_market_state as W

ROOT = Path(__file__).resolve().parents[1]
_MIGRATION = "migrations.versions.market_state_revs_20261125"
DAY = date(2026, 11, 20)
OLD_DAY = date(2026, 11, 12)
MON = B.SessionTimes(date(2026, 11, 23), datetime(2026, 11, 23, 14, 30, tzinfo=UTC),
                     datetime(2026, 11, 23, 21, 0, tzinfo=UTC))

# market_state_daily as griddb has it: created by ensure_table at runtime, never declared in schema.sql before.
_LEGACY_DDL = MS._TABLE_SQL


def _squash(sql: str) -> str:
    return re.sub(r"\(\s+", "(", re.sub(r"\s+\)", ")", re.sub(r"\s+", " ", sql))).strip()


def test_schema_sql_declares_exactly_what_the_migration_installs():
    m = importlib.import_module(_MIGRATION)
    schema = _squash((ROOT / "schema.sql").read_text())
    for statement in (m.MARKET_STATE_TABLE, m.EPOCH_TABLE, m.EPOCH_ROW, m.REVISIONS_TABLE, m.REVISIONS_INDEX,
                      m.KEEP_REVISIONS_FUNCTION, m.APPEND_ONLY_FUNCTION, *m.TRIGGERS):
        assert _squash(statement) + ";" in schema, statement.strip().splitlines()[0]


def test_the_migration_creates_market_state_daily_with_the_ddl_ensure_table_runs():
    m = importlib.import_module(_MIGRATION)
    assert _squash(m.MARKET_STATE_TABLE) == _squash(MS._TABLE_SQL)
    assert m.revision == MS.REVISIONS_MIGRATION


def _state(label: str, day: date = DAY) -> MS.MarketState:
    state = MS.MarketState(day)
    state.variables = {"vix": {"status": "ok", "label": label, "robust_z": 2.5, "obs_date": day.isoformat()}}
    return state


REPLACE = MS.MarketStateReplacement("dealer gamma backfilled", "run_market_state.run:test-A")


@pytest.mark.parametrize("reason, run_tag", [("", "run"), ("why", " "), (None, "run")])
def test_a_replacement_names_a_reason_and_a_run(reason, run_tag):
    with pytest.raises(ValueError, match="non-empty"):
        MS.MarketStateReplacement(reason, run_tag)


def test_a_bare_replace_flag_is_refused_before_the_table_is_touched(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'ms.db'}")
    with pytest.raises(ValueError, match="MarketStateReplacement"):
        MS.store_market_state(engine, _state("low"), replace=True)
    assert not inspect(engine).has_table("market_state_daily")


@pytest.mark.parametrize("argv", [["--replace"], ["--replace", "--reason", "  "], ["--reason", "why"],
                                  ["--provisional", "--reason", "why"]])
def test_the_writer_cli_needs_reason_exactly_with_replace(argv, capsys):
    assert W.main(["--no-refresh", *argv], engine=object()) == 2
    assert "--reason" in capsys.readouterr().err


# ── real PostgreSQL ──────────────────────────────────────────────────────────


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"ms_rev_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url.update_query_dict({"options": f"-csearch_path={schema} -cTimeZone=UTC"}))
    with engine.begin() as conn:
        conn.exec_driver_sql(_LEGACY_DDL)
    try:
        yield engine
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


def _engine_with(engine: Engine, **query) -> Engine:
    return create_engine(engine.url.update_query_dict({**engine.url.query, **query}))


def _row(engine: Engine, day: date = DAY):
    with engine.connect() as conn:
        return conn.execute(text("SELECT rule, record, computed_at FROM market_state_daily WHERE as_of_date = :d"),
                            {"d": day}).one()


def _revisions(engine: Engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT as_of_date, rule, record, computed_at, computed_at_by_trigger, "
                                 "superseded_at, operation, reason, run_tag, attributed "
                                 "FROM market_state_daily_revisions ORDER BY id")).all()


def _db_utc_now(engine: Engine) -> datetime:
    with engine.connect() as conn:
        return conn.execute(text("SELECT clock_timestamp() AT TIME ZONE 'UTC'")).scalar_one()


def _insert_legacy(engine: Engine, day: date, label: str, computed: str) -> None:
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO market_state_daily (as_of_date, rule, record, computed_at) "
                          "VALUES (:d, 'market-state/3', CAST(:r AS JSONB), :c)"),
                     {"d": day, "r": json.dumps(_state(label, day).to_dict()), "c": computed})


def test_the_migration_creates_the_table_on_a_database_that_never_had_it(pg_engine: Engine):
    schema = f"ms_rev_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url.update_query_dict({"options": f"-csearch_path={schema} -cTimeZone=UTC"}))
    try:
        _run(engine, "upgrade")
        assert MS.store_market_state(engine, _state("low")) is True
        assert MS.store_market_state(engine, _state("high"), replace=REPLACE) is True
        assert len(_revisions(engine)) == 1
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def test_migration_keeps_existing_rows_and_a_default_write_changes_nothing(scratch):
    _insert_legacy(scratch, OLD_DAY, "normal", "2026-11-13 12:12:14")
    _run(scratch, "upgrade")
    assert _row(scratch, OLD_DAY)[2] == datetime(2026, 11, 13, 12, 12, 14)

    assert MS.store_market_state(scratch, _state("low")) is True
    first = _row(scratch)
    assert MS.store_market_state(scratch, _state("high")) is False
    assert _row(scratch) == first and _revisions(scratch) == []


def test_the_trigger_stamps_computed_at_in_utc_whatever_the_writer_or_session_says(scratch):
    _run(scratch, "upgrade")
    before = _db_utc_now(scratch)
    _insert_legacy(scratch, DAY, "low", "2001-01-01 00:00:00")      # a writer's own clock is ignored
    elsewhere = _engine_with(scratch, options=scratch.url.query["options"].replace("UTC", "America/New_York"))
    try:
        with elsewhere.begin() as conn:
            assert conn.execute(text("SHOW TimeZone")).scalar_one() == "America/New_York"
            conn.execute(text("INSERT INTO market_state_daily (as_of_date, rule, record) "
                              "VALUES (:d, 'market-state/3', CAST(:r AS JSONB))"),
                         {"d": OLD_DAY, "r": json.dumps(_state("low", OLD_DAY).to_dict())})
    finally:
        elsewhere.dispose()
    after = _db_utc_now(scratch)
    for day in (DAY, OLD_DAY):
        assert before <= _row(scratch, day)[2] <= after


def test_a_replacement_keeps_both_versions(scratch):
    _insert_legacy(scratch, OLD_DAY, "normal", "2026-11-13 12:12:14")
    _run(scratch, "upgrade")
    assert MS.store_market_state(scratch, _state("low")) is True
    old = _row(scratch)
    assert MS.store_market_state(scratch, _state("high"), replace=REPLACE) is True
    new = _row(scratch)
    assert new[1]["variables"]["vix"]["label"] == "high"
    (rev,) = _revisions(scratch)
    assert (rev.as_of_date, rev.rule, rev.record, rev.computed_at) == (DAY, old[0], old[1], old[2])
    assert rev.computed_at_by_trigger is True and rev.operation == "UPDATE" and rev.attributed is True
    assert (rev.reason, rev.run_tag) == (REPLACE.reason, REPLACE.run_tag)
    assert rev.superseded_at == new[2] and new[2] > old[2]       # the new version starts as the old one ends

    # A version written before the migration keeps the old writer's clock, and says so.
    assert MS.store_market_state(scratch, _state("high", OLD_DAY), replace=REPLACE) is True
    legacy = _revisions(scratch)[-1]
    assert legacy.computed_at == datetime(2026, 11, 13, 12, 12, 14) and legacy.computed_at_by_trigger is False


def test_an_identical_replacement_writes_nothing(scratch):
    _run(scratch, "upgrade")
    MS.store_market_state(scratch, _state("low"))
    first = _row(scratch)
    assert MS.store_market_state(scratch, _state("low"), replace=REPLACE) is False
    assert _row(scratch) == first and _revisions(scratch) == []


def test_a_replacement_of_a_missing_row_inserts_and_archives_nothing(scratch):
    _run(scratch, "upgrade")
    assert MS.store_market_state(scratch, _state("low"), replace=REPLACE) is True
    assert _revisions(scratch) == []


def test_an_unattributed_change_is_archived_and_computed_at_cannot_be_set(scratch):
    _run(scratch, "upgrade")
    MS.store_market_state(scratch, _state("low"))
    first = _row(scratch)
    tagged = _engine_with(scratch, application_name="hand-edit")
    try:
        with tagged.begin() as conn:            # a hand backdate: ignored, nothing archived
            conn.execute(text("UPDATE market_state_daily SET computed_at = '2001-01-01 00:00:00'"))
        assert _row(scratch) == first and _revisions(scratch) == []
        with tagged.begin() as conn:
            conn.execute(text("UPDATE market_state_daily SET record = CAST(:r AS JSONB), "
                              "computed_at = '2001-01-01 00:00:00'"), {"r": json.dumps(_state("high").to_dict())})
    finally:
        tagged.dispose()
    (rev,) = _revisions(scratch)
    assert rev.attributed is False and rev.reason.startswith("unattributed")
    assert rev.run_tag == "unattributed: application_name hand-edit"
    assert _row(scratch)[2] == rev.superseded_at > first[2]


def test_a_plain_delete_is_archived_and_truncate_and_date_changes_are_refused(scratch):
    _run(scratch, "upgrade")
    MS.store_market_state(scratch, _state("low"))
    with pytest.raises(DBAPIError, match="cannot change"):
        with scratch.begin() as conn:
            conn.execute(text("UPDATE market_state_daily SET as_of_date = as_of_date + 1"))
    with scratch.begin() as conn:
        conn.execute(text("DELETE FROM market_state_daily"))
    (rev,) = _revisions(scratch)
    assert (rev.as_of_date, rev.operation, rev.attributed) == (DAY, "DELETE", False)
    MS.store_market_state(scratch, _state("high"))
    with pytest.raises(DBAPIError, match="TRUNCATE refused"):
        with scratch.begin() as conn:
            conn.execute(text("TRUNCATE market_state_daily"))
    assert _row(scratch)[1]["variables"]["vix"]["label"] == "high"


@pytest.mark.parametrize("statement", [
    "UPDATE market_state_daily_revisions SET reason = 'x'",
    "DELETE FROM market_state_daily_revisions",
    "TRUNCATE market_state_daily_revisions",
    "UPDATE market_state_daily_revision_epoch SET revisions_from = revisions_from - interval '1 day'",
    "DELETE FROM market_state_daily_revision_epoch",
    "TRUNCATE market_state_daily_revision_epoch",
])
def test_revisions_and_the_epoch_are_append_only(scratch, statement):
    _run(scratch, "upgrade")
    MS.store_market_state(scratch, _state("low"))
    MS.store_market_state(scratch, _state("high"), replace=REPLACE)
    with pytest.raises(DBAPIError, match="append-only"):
        with scratch.begin() as conn:
            conn.execute(text(statement))
    assert len(_revisions(scratch)) == 1


def test_a_replacement_without_the_archive_refuses_and_leaves_the_row(scratch):
    MS.store_market_state(scratch, _state("low"))                  # no migration: no revisions table
    first = _row(scratch)
    with pytest.raises(DBAPIError):
        MS.store_market_state(scratch, _state("high"), replace=REPLACE)
    assert _row(scratch) == first


def test_a_replacement_without_the_trigger_refuses_and_leaves_the_row(scratch):
    _run(scratch, "upgrade")
    MS.store_market_state(scratch, _state("low"))
    first = _row(scratch)
    with scratch.begin() as conn:
        conn.execute(text("DROP TRIGGER market_state_daily_keep_revisions ON market_state_daily"))
    with pytest.raises(RuntimeError, match="left 0 revision rows"):
        MS.store_market_state(scratch, _state("high"), replace=REPLACE)
    assert _row(scratch) == first and _revisions(scratch) == []


def test_the_brief_replayed_before_the_revision_shows_the_original(scratch):
    _run(scratch, "upgrade")
    MS.store_market_state(scratch, _state("low"))
    MS.store_market_state(scratch, _state("high"), replace=REPLACE)
    (rev,) = _revisions(scratch)
    between = (rev.computed_at + (rev.superseded_at - rev.computed_at) / 2).replace(tzinfo=UTC)
    replayed = S.market_state(scratch, "post_close", MON, between)
    assert replayed.status == "ok" and replayed.facts["record_date"] == DAY.isoformat()
    assert any("vix low" in line for line in replayed.lines)
    assert replayed.facts["superseded_at"] == B.iso(rev.superseded_at.replace(tzinfo=UTC))
    after = S.market_state(scratch, "post_close", MON, rev.superseded_at.replace(tzinfo=UTC))
    assert any("vix high" in line for line in after.lines) and "superseded_at" not in after.facts
    early = S.market_state(scratch, "post_close", MON, rev.computed_at.replace(tzinfo=UTC) - timedelta(seconds=1))
    assert early.status == "empty"                               # neither version existed yet


def test_interleaved_sessions_archive_with_ordered_clocks(scratch):
    """A begins, B replaces and commits, then A replaces B's version. With now() (the old CURRENT_TIMESTAMP) A would
    archive B's version with a superseded_at before B's computed_at; the mutation clock is read under the row lock."""
    _run(scratch, "upgrade")
    MS.store_market_state(scratch, _state("normal"))
    conn_a = scratch.connect()
    trans_a = conn_a.begin()
    try:
        conn_a.execute(text("SELECT now()"))                   # A's transaction clock is fixed here
        MS.store_market_state(scratch, _state("low"), replace=MS.MarketStateReplacement("B", "run-B"))
        b_version = _row(scratch)[2]
        conn_a.execute(text("SELECT set_config('grid.market_state_reason', 'A', true), "
                            "set_config('grid.market_state_run', 'run-A', true)"))
        conn_a.execute(text("UPDATE market_state_daily SET record = CAST(:r AS JSONB) WHERE as_of_date = :d"),
                       {"r": json.dumps(_state("high").to_dict()), "d": DAY})
        trans_a.commit()
    finally:
        conn_a.close()
    first, second = _revisions(scratch)
    labels = [r.record["variables"]["vix"]["label"] for r in (first, second)]
    assert labels == ["normal", "low"]
    assert second.computed_at == b_version == first.superseded_at
    assert second.superseded_at > second.computed_at            # B's version ends after it began
    assert _row(scratch)[2] == second.superseded_at             # and A's begins where B's ends
    with scratch.connect() as conn:
        xids = conn.execute(text("SELECT superseded_by_xid FROM market_state_daily_revisions ORDER BY id")).scalars().all()
    assert len(set(xids)) == 2 and all(xids)


def test_the_write_lock_is_taken_before_the_epoch_is_stamped():
    """The lesson of #1302's review (Codex 6011125969): stamping the epoch before blocking writers would let a legacy
    row written in between pass as trigger-stamped. Only the timeouts and the table's own CREATE IF NOT EXISTS come
    before the lock, which lasts until the triggers exist (same transaction)."""
    m = importlib.import_module(_MIGRATION)
    calls: list[str] = []

    class _Op:
        def execute(self, statement):
            calls.append(str(statement).strip())

    real_op = m.op
    m.op = _Op()
    try:
        m.upgrade()
    finally:
        m.op = real_op
    lock = calls.index(m.WRITE_LOCK)
    assert m.WRITE_LOCK == "LOCK TABLE market_state_daily IN SHARE ROW EXCLUSIVE MODE"
    assert calls[:lock] == ["SET LOCAL lock_timeout = '10s'", "SET LOCAL statement_timeout = '60s'",
                            m.MARKET_STATE_TABLE.strip()]
    first_trigger = next(i for i, c in enumerate(calls)
                         if c.startswith("CREATE TRIGGER market_state_daily_keep_revisions"))
    assert lock < calls.index(m.EPOCH_ROW.strip()) < first_trigger


def test_the_migration_boundary_with_concurrent_legacy_writers(scratch):
    """The migration paused right after it stamps the epoch, before any trigger exists. A legacy writer that tries to
    commit a row in that window is blocked: otherwise its computed_at, later than the epoch, would pass as
    trigger-stamped. Once the migration commits, the same write goes through with the trigger's clock."""
    import threading

    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    m = importlib.import_module(_MIGRATION)
    paused, release = threading.Event(), threading.Event()
    errors: list[BaseException] = []

    def _upgrade_pausing_after_the_epoch():
        try:
            with scratch.connect() as conn:
                trans = conn.begin()
                ops = Operations(MigrationContext.configure(conn))

                class _Pausing:
                    def execute(self, statement, *args, **kwargs):
                        ops.execute(statement, *args, **kwargs)
                        if str(statement).strip() == m.EPOCH_ROW.strip():
                            paused.set()
                            release.wait(20)

                real_op = m.op
                m.op = _Pausing()
                try:
                    m.upgrade()
                finally:
                    m.op = real_op
                trans.commit()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    legacy = ("INSERT INTO market_state_daily (as_of_date, rule, record, computed_at) "
              "VALUES (:d, 'market-state/3', CAST(:r AS JSONB), '2000-01-01')")
    params = {"d": DAY, "r": json.dumps(_state("low").to_dict())}
    migration = threading.Thread(target=_upgrade_pausing_after_the_epoch)
    migration.start()
    try:
        assert paused.wait(20)                                 # epoch stamped, no trigger yet
        with pytest.raises(DBAPIError, match="lock timeout"):
            with scratch.begin() as conn:
                conn.execute(text("SET LOCAL lock_timeout = '300ms'"))
                conn.execute(text(legacy), params)
    finally:
        release.set()
        migration.join(timeout=30)
    assert not migration.is_alive() and errors == []
    with scratch.begin() as conn:
        conn.execute(text(legacy), params)
        epoch = conn.execute(text("SELECT revisions_from FROM market_state_daily_revision_epoch")).scalar_one()
    assert _row(scratch)[2] > epoch                             # the trigger's clock, never the writer's


def test_downgrade_refuses_while_revisions_exist(scratch):
    _run(scratch, "upgrade")
    MS.store_market_state(scratch, _state("low"))
    _run(scratch, "downgrade")                                    # nothing archived yet: allowed
    assert not inspect(scratch).has_table("market_state_daily_revisions")
    assert _row(scratch)[1]["variables"]["vix"]["label"] == "low"
    _run(scratch, "upgrade")
    MS.store_market_state(scratch, _state("high"), replace=REPLACE)
    with pytest.raises(DBAPIError, match="downgrade refused"):
        _run(scratch, "downgrade")
    assert len(_revisions(scratch)) == 1
