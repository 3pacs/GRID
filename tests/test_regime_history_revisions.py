"""regime_history keeps every version it replaces (#1302).

The schema-parity test runs everywhere. The rest need real PostgreSQL (the trigger is plpgsql): they run in a
throwaway schema on the shared ``pg_engine`` fixture (``GRID_TEST_DB_URL``), and CI runs this file in its own step
that fails on any skip. Against a real database they prove:

* the migration keeps existing rows and installs the revision trigger;
* a second default write for the same obs_date leaves the row unchanged;
* a change without a reason (the pinned scheduler's legacy upsert, a plain UPDATE or DELETE) goes through and is
  archived as unattributed; an UPDATE that changes nothing writes no revision; TRUNCATE and obs_date changes are
  refused;
* an explicit replacement keeps both versions: the old one in regime_history_revisions with when it was written,
  when it was superseded, the reason and the run; the new one stamped with its own created_at;
* an identical replacement writes nothing, and revisions are append-only;
* the chat regime context and trial_signal's query read the current label, and the HMM reader returns what it
  returned before;
* a replacement on a database without the trigger refuses and leaves the row as it was;
* downgrade refuses while revisions exist.
"""

from __future__ import annotations

import importlib
import re
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

from scripts.auto_regime import RegimeReplacement, persist_regime_history

ROOT = Path(__file__).resolve().parents[1]
_MIGRATION = "migrations.versions.regime_history_revs_20261122"
DAY = date.today() - timedelta(days=3)

# regime_history as griddb has it: created by hand before schema.sql declared it, then given data_as_of.
_LEGACY_DDL = """
CREATE TABLE regime_history (
    obs_date    DATE PRIMARY KEY,
    regime      TEXT NOT NULL,
    confidence  DOUBLE PRECISION,
    source      TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    data_as_of  DATE
);
"""


def _squash(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def test_schema_sql_declares_exactly_what_the_migration_installs():
    m = importlib.import_module(_MIGRATION)
    schema = _squash((ROOT / "schema.sql").read_text())
    for statement in (m.EPOCH_TABLE, m.EPOCH_ROW, m.REVISIONS_TABLE, m.REVISIONS_INDEX, m.KEEP_REVISIONS_FUNCTION,
                      m.APPEND_ONLY_FUNCTION, *m.TRIGGERS):
        assert _squash(statement) + ";" in schema, statement.strip().splitlines()[0]


# ── real PostgreSQL ──────────────────────────────────────────────────────────


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"regime_rev_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url.update_query_dict({"options": f"-csearch_path={schema}"}))
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


def _row(engine: Engine, day: date = DAY):
    with engine.connect() as conn:
        return conn.execute(text("SELECT regime, confidence, source, data_as_of, created_at "
                                 "FROM regime_history WHERE obs_date = :d"), {"d": day}).one()


def _revisions(engine: Engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT obs_date, regime, confidence, data_as_of, created_at, superseded_at, "
                                 "operation, reason, run_tag, attributed FROM regime_history_revisions "
                                 "ORDER BY id")).all()


def test_migration_keeps_existing_rows_and_a_second_default_write_changes_nothing(scratch):
    with scratch.begin() as conn:
        conn.execute(text("INSERT INTO regime_history (obs_date, regime, confidence, source) "
                          "VALUES (:d, 'NEUTRAL', 0.5, 'decision_journal')"), {"d": DAY - timedelta(days=30)})
    _run(scratch, "upgrade")
    assert _row(scratch, DAY - timedelta(days=30))[:3] == ("NEUTRAL", 0.5, "decision_journal")

    assert persist_regime_history(scratch, DAY, "GROWTH", 0.8, data_as_of=DAY) is True
    first = _row(scratch)
    assert persist_regime_history(scratch, DAY, "CRISIS", 0.9, data_as_of=DAY) is False
    assert _row(scratch) == first
    assert _revisions(scratch) == []


# The statement grid-scheduler's pinned release (235e9467) runs, verbatim: it names no reason.
_PINNED_SCHEDULER_UPSERT = (
    "INSERT INTO regime_history (obs_date, regime, confidence, source, data_as_of) "
    "VALUES (:obs_date, :regime, :confidence, :source, :data_as_of) "
    "ON CONFLICT (obs_date) DO UPDATE SET "
    "regime = EXCLUDED.regime, "
    "confidence = EXCLUDED.confidence, "
    "source = EXCLUDED.source, "
    "data_as_of = EXCLUDED.data_as_of"
)


def test_the_pinned_scheduler_upsert_keeps_working_and_every_version_is_kept(scratch):
    _run(scratch, "upgrade")
    tagged = create_engine(scratch.url.update_query_dict(
        {"options": scratch.url.query["options"], "application_name": "grid-scheduler"}))
    try:
        def upsert(regime, conf):
            with tagged.begin() as conn:
                conn.execute(text(_PINNED_SCHEDULER_UPSERT), {"obs_date": DAY, "regime": regime, "confidence": conf,
                                                               "source": "auto_regime", "data_as_of": DAY})
        upsert("NEUTRAL", 0.5)                       # 13:xx run: insert
        first = _row(scratch)
        upsert("NEUTRAL", 0.5)                       # 16:xx run, same values: nothing to keep
        assert _row(scratch) == first and _revisions(scratch) == []
        upsert("FRAGILE", 0.7)                       # 22:xx run: label changed, as on 11-17
    finally:
        tagged.dispose()
    assert _row(scratch)[:2] == ("FRAGILE", 0.7)
    (rev,) = _revisions(scratch)
    assert (rev.regime, rev.confidence, rev.created_at, rev.operation) == ("NEUTRAL", 0.5, first[4], "UPDATE")
    assert rev.attributed is False and rev.reason.startswith("unattributed")
    assert rev.run_tag == "unattributed: application_name grid-scheduler"


def test_a_plain_delete_is_archived_and_truncate_is_refused(scratch):
    _run(scratch, "upgrade")
    persist_regime_history(scratch, DAY, "GROWTH", 0.8)
    with scratch.begin() as conn:
        conn.execute(text("DELETE FROM regime_history"))
    (rev,) = _revisions(scratch)
    assert (rev.regime, rev.operation, rev.attributed) == ("GROWTH", "DELETE", False)
    persist_regime_history(scratch, DAY, "NEUTRAL", 0.5)
    with pytest.raises(DBAPIError, match="TRUNCATE refused"):
        with scratch.begin() as conn:
            conn.execute(text("TRUNCATE regime_history"))
    assert _row(scratch)[0] == "NEUTRAL"


def test_obs_date_cannot_change_even_with_a_reason(scratch):
    _run(scratch, "upgrade")
    persist_regime_history(scratch, DAY, "GROWTH", 0.8)
    with pytest.raises(DBAPIError, match="cannot change"):
        with scratch.begin() as conn:
            conn.execute(text("SELECT set_config('grid.regime_history_reason', 'x', true), "
                              "set_config('grid.regime_history_run', 'y', true)"))
            conn.execute(text("UPDATE regime_history SET obs_date = obs_date + 1"))


def test_a_replacement_keeps_both_versions(scratch):
    _run(scratch, "upgrade")
    persist_regime_history(scratch, DAY, "NEUTRAL", 0.5, data_as_of=DAY - timedelta(days=1))
    old = _row(scratch)
    replace = RegimeReplacement("operator re-run after the resolver fix", "run-A")
    assert persist_regime_history(scratch, DAY, "FRAGILE", 0.7, data_as_of=DAY, replace=replace) is True

    new = _row(scratch)
    assert new[:4] == ("FRAGILE", 0.7, "auto_regime", DAY)
    (rev,) = _revisions(scratch)
    assert rev.obs_date == DAY and (rev.regime, rev.confidence, rev.data_as_of) == ("NEUTRAL", 0.5, old[3])
    assert rev.created_at == old[4]                      # when the superseded version was written
    assert rev.superseded_at == new[4] > old[4]          # its lifetime ends where the new version's begins
    assert (rev.operation, rev.reason, rev.run_tag) == ("UPDATE", "operator re-run after the resolver fix", "run-A")
    assert rev.attributed is True

    # What regime_history said for DAY at any instant: the revision before superseded_at, the row after.
    with scratch.connect() as conn:
        said = conn.execute(text(
            "SELECT regime FROM regime_history_revisions WHERE obs_date = :d "
            "AND created_at <= :t AND superseded_at > :t "
            "UNION ALL SELECT regime FROM regime_history WHERE obs_date = :d AND created_at <= :t"),
            {"d": DAY, "t": old[4]}).scalars().all()
    assert said == ["NEUTRAL"]


def test_an_identical_replacement_writes_nothing_and_revisions_are_append_only(scratch):
    _run(scratch, "upgrade")
    persist_regime_history(scratch, DAY, "NEUTRAL", 0.5)
    replace = RegimeReplacement("same-day refresh", "run-B")
    assert persist_regime_history(scratch, DAY, "NEUTRAL", 0.5, replace=replace) is False
    assert _revisions(scratch) == []
    persist_regime_history(scratch, DAY, "GROWTH", 0.6, replace=replace)
    for statement in ("UPDATE regime_history_revisions SET reason = 'edited'",
                      "DELETE FROM regime_history_revisions", "TRUNCATE regime_history_revisions"):
        with pytest.raises(DBAPIError, match="append-only"):
            with scratch.begin() as conn:
                conn.execute(text(statement))
    assert len(_revisions(scratch)) == 1


def test_readers_see_the_current_label(scratch):
    """The chat regime context and trial_signal's query read the replaced label; the HMM reader returns exactly
    what it returned before (it selects regime_state/snapshot_date, columns regime_history has never had)."""
    from api.routers import chat
    from intelligence.hmm_regime_transitions import read_regime_history

    _run(scratch, "upgrade")
    hmm_before = read_regime_history(scratch)
    persist_regime_history(scratch, DAY, "NEUTRAL", 0.5)
    persist_regime_history(scratch, DAY, "CRISIS", 0.9, replace=RegimeReplacement("re-run", "run-C"))
    with patch.object(chat, "_get_db_engine", return_value=scratch):
        framing, source = chat._gather_regime_context()
    assert source == "regime_history" and "Current regime: CRISIS" in framing
    with scratch.connect() as conn:    # grid/signals/trial_signal.py::_get_regime, verbatim
        assert conn.execute(text("SELECT regime FROM regime_history ORDER BY obs_date DESC LIMIT 1")).scalar() == "CRISIS"
    assert read_regime_history(scratch) == hmm_before


def test_a_replacement_without_the_trigger_refuses_and_changes_nothing(scratch):
    persist_regime_history(scratch, DAY, "NEUTRAL", 0.5)          # legacy table, migration not applied
    before = _row(scratch)
    with pytest.raises(Exception):
        persist_regime_history(scratch, DAY, "CRISIS", 0.9, replace=RegimeReplacement("re-run", "run-D"))
    assert _row(scratch) == before


def test_downgrade_refuses_while_revisions_exist(scratch):
    _run(scratch, "upgrade")
    _run(scratch, "downgrade")                                     # empty: allowed
    _run(scratch, "upgrade")
    persist_regime_history(scratch, DAY, "NEUTRAL", 0.5)
    persist_regime_history(scratch, DAY, "CRISIS", 0.9, replace=RegimeReplacement("re-run", "run-E"))
    with pytest.raises(DBAPIError, match="downgrade refused"):
        _run(scratch, "downgrade")
    assert len(_revisions(scratch)) == 1


def test_writers_cannot_set_or_rewrite_a_version_clock(scratch):
    _run(scratch, "upgrade")
    with scratch.begin() as conn:
        conn.execute(text("INSERT INTO regime_history (obs_date, regime, confidence, source, created_at) "
                          "VALUES (:d, 'NEUTRAL', 0.5, 'auto_regime', '2000-01-01')"), {"d": DAY})
    stamped = _row(scratch)[4]
    assert stamped.year >= 2026                                # the trigger's clock, not the writer's
    with scratch.begin() as conn:                              # a timestamp-only rewrite changes nothing
        conn.execute(text("UPDATE regime_history SET created_at = '2000-01-01' WHERE obs_date = :d"), {"d": DAY})
    assert _row(scratch)[4] == stamped and _revisions(scratch) == []


def test_interleaved_sessions_archive_with_ordered_clocks(scratch):
    """Codex's case: A begins, B replaces and commits, then A replaces B's version. With now() A would archive
    B's version with a superseded_at before B's created_at; the mutation clock is read under the row lock."""
    _run(scratch, "upgrade")
    persist_regime_history(scratch, DAY, "NEUTRAL", 0.5)
    conn_a = scratch.connect()
    trans_a = conn_a.begin()
    try:
        conn_a.execute(text("SELECT now()"))                   # A's transaction clock is fixed here
        persist_regime_history(scratch, DAY, "GROWTH", 0.6, replace=RegimeReplacement("B", "run-B"))
        b_version = _row(scratch)[4]
        conn_a.execute(text("SELECT set_config('grid.regime_history_reason', 'A', true), "
                            "set_config('grid.regime_history_run', 'run-A', true)"))
        conn_a.execute(text("UPDATE regime_history SET regime = 'CRISIS', confidence = 0.9 WHERE obs_date = :d"),
                       {"d": DAY})
        trans_a.commit()
    finally:
        conn_a.close()
    first, second = _revisions(scratch)
    assert (first.regime, second.regime) == ("NEUTRAL", "GROWTH")
    assert second.created_at == b_version == first.superseded_at
    assert second.superseded_at > second.created_at            # B's version ends after it began
    assert _row(scratch)[4] == second.superseded_at            # and A's begins where B's ends
    with scratch.connect() as conn:
        xids = conn.execute(text("SELECT superseded_by_xid FROM regime_history_revisions ORDER BY id")).scalars().all()
    assert len(set(xids)) == 2 and all(xids)


def test_versions_from_before_the_migration_have_an_unknown_start(scratch):
    with scratch.begin() as conn:                              # the old upsert never moved created_at
        conn.execute(text("INSERT INTO regime_history (obs_date, regime, confidence, source, created_at) "
                          "VALUES (:d, 'NEUTRAL', 0.5, 'auto_regime', '2026-03-30')"), {"d": DAY})
    _run(scratch, "upgrade")
    persist_regime_history(scratch, DAY, "FRAGILE", 0.7, replace=RegimeReplacement("re-run", "run-F"))
    persist_regime_history(scratch, DAY, "CRISIS", 0.9, replace=RegimeReplacement("re-run", "run-G"))
    legacy, known = _revisions(scratch)
    assert legacy.regime == "NEUTRAL" and legacy.created_at is None
    assert known.regime == "FRAGILE" and known.created_at == legacy.superseded_at


def test_the_write_lock_is_taken_before_the_epoch_is_stamped():
    """Codex (6011125969): stamping the epoch before blocking writers let a legacy row written in between pass as
    trigger-managed. The lock must come first and last until the triggers exist (same transaction)."""
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
    assert calls[0] == m.WRITE_LOCK == "LOCK TABLE regime_history IN SHARE ROW EXCLUSIVE MODE"
    first_trigger = next(i for i, c in enumerate(calls) if c.startswith("CREATE TRIGGER regime_history_keep_revisions"))
    assert calls.index(m.EPOCH_ROW.strip()) < first_trigger


def test_the_migration_boundary_with_concurrent_legacy_writers(scratch):
    """Two sessions at the boundary Codex named (6011125969). The migration is paused right after it stamps the
    epoch, before any trigger exists. A legacy writer that tries to commit a row in that window must be blocked:
    otherwise its created_at, later than the epoch, would pass as trigger-managed. Once the migration commits, the
    same write goes through with the trigger's clock."""
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

    migration = threading.Thread(target=_upgrade_pausing_after_the_epoch)
    migration.start()
    try:
        assert paused.wait(20)                                 # epoch stamped, no trigger yet
        with pytest.raises(DBAPIError, match="lock timeout"):
            with scratch.begin() as conn:
                conn.execute(text("SET LOCAL lock_timeout = '300ms'"))
                conn.execute(text("INSERT INTO regime_history (obs_date, regime, confidence, source) "
                                  "VALUES (:d, 'NEUTRAL', 0.5, 'auto_regime')"), {"d": DAY})
    finally:
        release.set()
        migration.join(timeout=30)
    assert not migration.is_alive() and errors == []
    with scratch.begin() as conn:
        conn.execute(text("INSERT INTO regime_history (obs_date, regime, confidence, source, created_at) "
                          "VALUES (:d, 'NEUTRAL', 0.5, 'auto_regime', '2000-01-01')"), {"d": DAY})
        epoch = conn.execute(text("SELECT revisions_from FROM regime_history_revision_epoch")).scalar_one()
    assert _row(scratch)[4] > epoch                             # the trigger's clock, never the writer's
