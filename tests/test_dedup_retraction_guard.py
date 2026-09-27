"""Fake-engine tests for the resolved_series_retractions FK guard on the
resolved_series dedup delete paths.

PR #683 (feat/resolved-series-retractions-20260927, not yet on main) adds
``resolved_series_retractions`` with an FK to the retracted
``resolved_series`` row keyed on ``(feature_id, obs_date, vintage_date)``,
plus an append-only trigger. Its review found two dedup delete paths that
would start hitting FK violations on a retracted row:

* ``intelligence/resolution_audit.py::auto_fix_issues`` (the "duplicate"
  finding branch)
* ``scripts/hermes_fixers.py::_run_data_quality_fix`` (Hermes'
  ``FIX_DATA_QUALITY`` dedup loop) -- which additionally had no per-row
  isolation: one FK violation would abort the single transaction shared by
  the whole function, so the bare ``try/except`` around each delete caught
  the immediate exception but every later statement on that same connection
  (including the rest of the loop and the implicit commit) failed too,
  silently rolling back every other dedup in the batch.

These tests exercise both fixed functions against a fake SQLAlchemy
engine/connection -- no real Postgres, so they also run before PR #683's
migration exists. They prove:

* with no ``resolved_series_retractions`` table (today's main), both
  functions fall back to the exact pre-#683 delete, unguarded;
* with the table present, both functions anti-join retracted rows out of
  the delete and run a skip-count query first;
* in ``scripts/hermes_fixers.py``, each dupe-group delete runs inside its
  own ``conn.begin_nested()`` (SAVEPOINT), and one group's delete raising
  does not stop the loop from reaching the next group's delete.

The real transactional guarantee -- that a SAVEPOINT truly confines a
Postgres FK-violation rollback to one row, which a fake connection cannot
demonstrate -- is proved against live PostgreSQL by
tests/test_dedup_retraction_guard_pg.py.
"""

from __future__ import annotations

import contextlib
from datetime import date
from types import SimpleNamespace
from typing import Any

from intelligence import resolution_audit
from scripts import hermes_fixers


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeResult:
    def __init__(self, scalar: Any = None, rows: list | None = None) -> None:
        self._scalar = scalar
        self._rows = rows or []

    def scalar(self) -> Any:
        return self._scalar

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class _FakeTxn:
    """Stands in for ``engine.begin()``'s context manager."""

    def __init__(self, conn: "_FakeConn") -> None:
        self._conn = conn

    def __enter__(self) -> "_FakeConn":
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeConn:
    """Records every statement text/params; answers just enough to drive
    resolution_audit.auto_fix_issues and hermes_fixers._run_data_quality_fix
    through their real code paths.
    """

    def __init__(
        self,
        table_exists: bool,
        dupe_rows: list[tuple[str, Any, int]] | None = None,
        skip_counts: dict[str, int] | None = None,
        fail_delete_for: set[str] | None = None,
    ) -> None:
        self.table_exists = table_exists
        self.dupe_rows = dupe_rows or []
        self.skip_counts = skip_counts or {}
        self.fail_delete_for = fail_delete_for or set()
        self.executed: list[tuple[str, dict]] = []
        self.nested_calls = 0

    def begin_nested(self):
        self.nested_calls += 1
        return contextlib.nullcontext(self)

    def execute(self, stmt: Any, params: dict | None = None) -> _FakeResult:
        sql = str(stmt)
        params = params or {}
        self.executed.append((sql, dict(params)))

        if "to_regclass" in sql:
            return _FakeResult(scalar=self.table_exists)
        if "null_count" in sql:
            return _FakeResult(rows=[])
        if "HAVING COUNT(*) > 1" in sql:
            return _FakeResult(rows=self.dupe_rows)
        if "ABS(rs.value)" in sql:
            return _FakeResult(rows=[])
        if "SELECT COUNT(*)" in sql and "resolved_series_retractions" in sql:
            fname = params.get("fname")
            return _FakeResult(scalar=self.skip_counts.get(fname, 0))
        if sql.strip().upper().startswith("DELETE"):
            fname = params.get("fname")
            if fname in self.fail_delete_for:
                raise RuntimeError(f"simulated FK violation for {fname!r}")
            return _FakeResult()
        return _FakeResult()


class _FakeEngine:
    """A single shared connection, like a real engine's connection pool
    would hand out for one ``with engine.begin() as conn:`` block -- both
    functions under test only ever open one.
    """

    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn

    def begin(self) -> _FakeTxn:
        return _FakeTxn(self.conn)

    def connect(self) -> _FakeTxn:
        return _FakeTxn(self.conn)


# ---------------------------------------------------------------------------
# intelligence/resolution_audit.py::auto_fix_issues
# ---------------------------------------------------------------------------


def _duplicate_finding(feature: str = "fred_test", obs_date: str = "2026-09-20"):
    return resolution_audit.AuditFinding(
        check_type="duplicate",
        severity="warning",
        feature=feature,
        description="dup",
        evidence={"obs_date": obs_date},
    )


def test_resolution_audit_dedup_plain_delete_when_retractions_table_absent():
    conn = _FakeConn(table_exists=False)
    engine = _FakeEngine(conn)

    result = resolution_audit.auto_fix_issues(engine, [_duplicate_finding()], dry_run=False)

    assert result["duplicates_fixed"] == 1
    sqls = [sql for sql, _ in conn.executed]
    assert any("to_regclass" in s for s in sqls)
    deletes = [s for s in sqls if s.strip().upper().startswith("DELETE")]
    assert len(deletes) == 1
    assert "resolved_series_retractions" not in deletes[0]
    # No skip-count probe when the table doesn't exist.
    assert not any("resolved_series_retractions" in s for s in sqls)


def test_resolution_audit_dedup_anti_joins_retractions_when_table_present():
    conn = _FakeConn(table_exists=True, skip_counts={"fred_test": 1})
    engine = _FakeEngine(conn)

    result = resolution_audit.auto_fix_issues(engine, [_duplicate_finding()], dry_run=False)

    assert result["duplicates_fixed"] == 1
    sqls = [sql for sql, _ in conn.executed]
    skip_probes = [s for s in sqls if s.strip().upper().startswith("SELECT COUNT(*)")]
    assert len(skip_probes) == 1
    assert "resolved_series_retractions" in skip_probes[0]
    assert "EXISTS" in skip_probes[0]

    deletes = [s for s in sqls if s.strip().upper().startswith("DELETE")]
    assert len(deletes) == 1
    assert "resolved_series_retractions" in deletes[0]
    assert "AND NOT" in deletes[0] and "EXISTS (" in deletes[0]


def test_resolution_audit_dedup_still_fixes_when_no_rows_are_retracted():
    """table present, but nothing for this feature/date is retracted."""
    conn = _FakeConn(table_exists=True, skip_counts={})
    engine = _FakeEngine(conn)

    result = resolution_audit.auto_fix_issues(engine, [_duplicate_finding()], dry_run=False)

    assert result["duplicates_fixed"] == 1
    assert "details" in result  # unaffected shape


# ---------------------------------------------------------------------------
# scripts/hermes_fixers.py::_run_data_quality_fix
# ---------------------------------------------------------------------------


def _state() -> Any:
    return SimpleNamespace(cycle_count=1)


def _patch_log_issue(monkeypatch) -> list[dict]:
    """FIX_DATA_QUALITY calls log_issue(engine, ...) when it finds anything --
    stub it out so the fake engine doesn't also need to answer
    operator_issues bookkeeping queries unrelated to this fix."""
    calls: list[dict] = []

    def _fake_log_issue(engine, **kwargs):
        calls.append(kwargs)
        return None

    monkeypatch.setattr(hermes_fixers, "log_issue", _fake_log_issue)
    return calls


def test_hermes_dedup_plain_delete_when_retractions_table_absent(monkeypatch):
    _patch_log_issue(monkeypatch)
    conn = _FakeConn(
        table_exists=False,
        dupe_rows=[("fred_a", date(2026, 9, 20), 2)],
    )
    engine = _FakeEngine(conn)

    result = hermes_fixers._run_data_quality_fix(engine, None, _state())

    assert result["duplicates_fixed"] == 1
    # 1 SAVEPOINT for the to_regclass existence check + 1 for the one dupe row.
    assert conn.nested_calls == 2
    deletes = [sql for sql, _ in conn.executed if sql.strip().upper().startswith("DELETE")]
    assert len(deletes) == 1
    assert "resolved_series_retractions" not in deletes[0]


def test_hermes_dedup_anti_joins_retractions_when_table_present(monkeypatch):
    _patch_log_issue(monkeypatch)
    conn = _FakeConn(
        table_exists=True,
        dupe_rows=[("fred_a", date(2026, 9, 20), 2)],
        skip_counts={"fred_a": 1},
    )
    engine = _FakeEngine(conn)

    result = hermes_fixers._run_data_quality_fix(engine, None, _state())

    assert result["duplicates_fixed"] == 1
    assert conn.nested_calls == 2
    sqls = [sql for sql, _ in conn.executed]
    skip_probes = [s for s in sqls if s.strip().upper().startswith("SELECT COUNT(*)")]
    assert len(skip_probes) == 1 and "resolved_series_retractions" in skip_probes[0]
    deletes = [s for s in sqls if s.strip().upper().startswith("DELETE")]
    assert len(deletes) == 1 and "resolved_series_retractions" in deletes[0]


def test_hermes_dedup_one_failed_savepoint_does_not_block_the_next_row(monkeypatch):
    """The regression this fix closes: before it, one FK violation aborted
    the whole shared transaction and every later statement failed too. This
    fake connection can't reproduce Postgres' real abort semantics (see the
    module docstring), but it does prove the code now opens one SAVEPOINT
    per dupe-group and keeps going after a failure -- fred_a's delete raises,
    fred_b's still runs and succeeds.
    """
    _patch_log_issue(monkeypatch)
    conn = _FakeConn(
        table_exists=True,
        dupe_rows=[
            ("fred_a", date(2026, 9, 20), 2),
            ("fred_b", date(2026, 9, 21), 2),
        ],
        skip_counts={"fred_a": 1, "fred_b": 0},
        fail_delete_for={"fred_a"},
    )
    engine = _FakeEngine(conn)

    result = hermes_fixers._run_data_quality_fix(engine, None, _state())

    # Only fred_b's delete succeeded.
    assert result["duplicates_fixed"] == 1
    # One SAVEPOINT for the to_regclass check + one per dupe-group (2 rows) --
    # never one shared SAVEPOINT for the whole loop.
    assert conn.nested_calls == 3

    delete_fnames = [
        params.get("fname")
        for sql, params in conn.executed
        if sql.strip().upper().startswith("DELETE")
    ]
    assert delete_fnames == ["fred_a", "fred_b"]
