"""Fake-engine tests proving the resolved_series "dedup" paths never delete
a vintage row.

PR #688 review (2026-09-27) reversed an earlier, incorrect fix. The earlier
version treated rows that share (feature_id, obs_date) as duplicates and
deleted all but one of them, guarded only against deleting a row PR #683's
``resolved_series_retractions`` had already retracted. That guard was not
enough: resolved_series' real unique index is
``(feature_id, obs_date, vintage_date)``, so any two rows sharing
(feature_id, obs_date) are -- always -- distinct vintages, not duplicates.
An exact duplicate (same feature_id, obs_date, vintage_date, AND value) is
impossible; the index forbids it. Deleting "extra" rows per
(feature_id, obs_date) collapses point-in-time history that
``store/pit.py``'s FIRST_RELEASE and LATEST_AS_OF policies read
intentionally -- measured against production, ~52% of eligible groups
would have lost their FIRST_RELEASE row.

Both delete paths are gone:

* ``intelligence/resolution_audit.py::auto_fix_issues`` -- the "duplicate"
  and NaN/Infinity branches are now report-only: they count and log, and
  never execute a DELETE against resolved_series.
* ``scripts/hermes_fixers.py::_run_data_quality_fix`` -- the
  ``FIX_DATA_QUALITY`` dedup loop is now report-only in the same way; there
  is no delete SQL left in the module at all.

These tests exercise both functions against a fake SQLAlchemy
engine/connection that raises if anything ever issues a DELETE against
resolved_series -- the thing this review requires never happens again --
and prove the report-only counting logic (multi-vintage group/row counts,
and how many are already retracted, purely for operator context) still
runs without error whether or not resolved_series_retractions exists.
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
    """Stands in for ``engine.begin()``'s/``engine.connect()``'s context manager."""

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

    Raises immediately if anything ever executes a DELETE against
    resolved_series -- the regression this test module guards against.
    """

    def __init__(
        self,
        table_exists: bool,
        dupe_rows: list[tuple[str, Any, int]] | None = None,
        vintage_counts: dict[str, int] | None = None,
        retracted_counts: dict[str, int] | None = None,
    ) -> None:
        self.table_exists = table_exists
        self.dupe_rows = dupe_rows or []
        self.vintage_counts = vintage_counts or {}
        self.retracted_counts = retracted_counts or {}
        self.executed: list[tuple[str, dict]] = []
        self.nested_calls = 0

    def begin_nested(self):
        self.nested_calls += 1
        return contextlib.nullcontext(self)

    def execute(self, stmt: Any, params: dict | None = None) -> _FakeResult:
        sql = str(stmt)
        params = params or {}
        self.executed.append((sql, dict(params)))

        upper = sql.strip().upper()
        if upper.startswith("DELETE") and "RESOLVED_SERIES" in upper:
            raise AssertionError(
                f"a vintage row was about to be deleted -- forbidden by "
                f"PR #688: {sql.strip()[:200]!r}"
            )

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
            return _FakeResult(scalar=self.retracted_counts.get(fname, 0))
        if "SELECT COUNT(*)" in sql:
            fname = params.get("fname")
            return _FakeResult(scalar=self.vintage_counts.get(fname, 0))
        return _FakeResult()


class _FakeEngine:
    """A single shared connection, like a real engine's connection pool
    would hand out for one transaction block -- both functions under test
    only ever open one at a time.
    """

    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn

    def begin(self) -> _FakeTxn:
        return _FakeTxn(self.conn)

    def connect(self) -> _FakeTxn:
        return _FakeTxn(self.conn)


# ---------------------------------------------------------------------------
# intelligence/resolution_audit.py::auto_fix_issues -- "duplicate" (really:
# multi-vintage) findings
# ---------------------------------------------------------------------------


def _duplicate_finding(feature: str = "fred_test", obs_date: str = "2026-09-20"):
    return resolution_audit.AuditFinding(
        check_type="duplicate",
        severity="warning",
        feature=feature,
        description="dup",
        evidence={"obs_date": obs_date},
    )


def test_resolution_audit_duplicate_branch_never_deletes_when_retractions_table_absent():
    conn = _FakeConn(table_exists=False, vintage_counts={"fred_test": 2})
    engine = _FakeEngine(conn)

    result = resolution_audit.auto_fix_issues(engine, [_duplicate_finding()], dry_run=False)

    assert result["duplicates_fixed"] == 0
    assert result["multi_vintage_groups_reported"] == 1
    sqls = [sql for sql, _ in conn.executed]
    assert not any(sql.strip().upper().startswith("DELETE") for sql in sqls)
    assert any("to_regclass" in s for s in sqls)


def test_resolution_audit_duplicate_branch_never_deletes_when_retractions_table_present():
    conn = _FakeConn(
        table_exists=True, vintage_counts={"fred_test": 2}, retracted_counts={"fred_test": 1},
    )
    engine = _FakeEngine(conn)

    result = resolution_audit.auto_fix_issues(engine, [_duplicate_finding()], dry_run=False)

    assert result["duplicates_fixed"] == 0
    assert result["multi_vintage_groups_reported"] == 1
    sqls = [sql for sql, _ in conn.executed]
    assert not any(sql.strip().upper().startswith("DELETE") for sql in sqls)
    count_probes = [s for s in sqls if s.strip().upper().startswith("SELECT COUNT(*)")]
    assert any("resolved_series_retractions" in s for s in count_probes)


def test_resolution_audit_duplicate_branch_report_only_regardless_of_dry_run():
    """The report-only branches have no destructive path to gate, so
    dry_run must not change their behaviour."""
    conn_dry = _FakeConn(table_exists=False, vintage_counts={"fred_test": 3})
    engine_dry = _FakeEngine(conn_dry)
    result_dry = resolution_audit.auto_fix_issues(engine_dry, [_duplicate_finding()], dry_run=True)

    conn_live = _FakeConn(table_exists=False, vintage_counts={"fred_test": 3})
    engine_live = _FakeEngine(conn_live)
    result_live = resolution_audit.auto_fix_issues(engine_live, [_duplicate_finding()], dry_run=False)

    assert result_dry["multi_vintage_groups_reported"] == result_live["multi_vintage_groups_reported"] == 1
    assert result_dry["duplicates_fixed"] == result_live["duplicates_fixed"] == 0


# ---------------------------------------------------------------------------
# intelligence/resolution_audit.py::auto_fix_issues -- NaN/Infinity findings
# ---------------------------------------------------------------------------


def _nan_finding(feature: str = "fred_nan", obs_date: str = "2026-09-20"):
    return resolution_audit.AuditFinding(
        check_type="sanity",
        severity="warning",
        feature=feature,
        description="NaN detected",
        evidence={"obs_date": obs_date},
    )


def test_resolution_audit_nan_branch_never_deletes():
    conn = _FakeConn(table_exists=False)
    engine = _FakeEngine(conn)

    result = resolution_audit.auto_fix_issues(engine, [_nan_finding()], dry_run=False)

    assert result["nan_removed"] == 0
    assert result["nan_values_reported"] == 1
    assert not conn.executed  # never even opens a connection -- nothing to query


def test_resolution_audit_nan_branch_report_only_regardless_of_dry_run():
    conn_dry = _FakeConn(table_exists=False)
    result_dry = resolution_audit.auto_fix_issues(_FakeEngine(conn_dry), [_nan_finding()], dry_run=True)

    conn_live = _FakeConn(table_exists=False)
    result_live = resolution_audit.auto_fix_issues(_FakeEngine(conn_live), [_nan_finding()], dry_run=False)

    assert result_dry["nan_values_reported"] == result_live["nan_values_reported"] == 1
    assert result_dry["nan_removed"] == result_live["nan_removed"] == 0


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


def test_hermes_dedup_never_deletes_when_retractions_table_absent(monkeypatch):
    _patch_log_issue(monkeypatch)
    conn = _FakeConn(
        table_exists=False,
        dupe_rows=[("fred_a", date(2026, 9, 20), 2)],
    )
    engine = _FakeEngine(conn)

    result = hermes_fixers._run_data_quality_fix(engine, None, _state())

    assert result["duplicates_fixed"] == 0
    assert result["multi_vintage_groups_found"] == 1
    sqls = [sql for sql, _ in conn.executed]
    assert not any(sql.strip().upper().startswith("DELETE") for sql in sqls)
    # table absent -> no retracted-count probe is even attempted.
    assert not any("resolved_series_retractions" in s for s in sqls)


def test_hermes_dedup_never_deletes_when_retractions_table_present(monkeypatch):
    _patch_log_issue(monkeypatch)
    conn = _FakeConn(
        table_exists=True,
        dupe_rows=[("fred_a", date(2026, 9, 20), 3)],
        retracted_counts={"fred_a": 1},
    )
    engine = _FakeEngine(conn)

    result = hermes_fixers._run_data_quality_fix(engine, None, _state())

    assert result["duplicates_fixed"] == 0
    assert result["multi_vintage_groups_found"] == 1
    sqls = [sql for sql, _ in conn.executed]
    assert not any(sql.strip().upper().startswith("DELETE") for sql in sqls)
    assert any("resolved_series_retractions" in s for s in sqls)


def test_hermes_dedup_multiple_groups_all_survive(monkeypatch):
    """Several multi-vintage groups in one cycle -- every one of them must
    be left alone, not just the first."""
    _patch_log_issue(monkeypatch)
    conn = _FakeConn(
        table_exists=True,
        dupe_rows=[
            ("fred_a", date(2026, 9, 20), 2),
            ("fred_b", date(2026, 9, 21), 5),
        ],
        retracted_counts={"fred_a": 0, "fred_b": 2},
    )
    engine = _FakeEngine(conn)

    result = hermes_fixers._run_data_quality_fix(engine, None, _state())

    assert result["duplicates_fixed"] == 0
    assert result["multi_vintage_groups_found"] == 2
    sqls = [sql for sql, _ in conn.executed]
    assert not any(sql.strip().upper().startswith("DELETE") for sql in sqls)


def test_hermes_dedup_reports_zero_groups_when_none_found(monkeypatch):
    _patch_log_issue(monkeypatch)
    conn = _FakeConn(table_exists=False, dupe_rows=[])
    engine = _FakeEngine(conn)

    result = hermes_fixers._run_data_quality_fix(engine, None, _state())

    assert result["duplicates_fixed"] == 0
    assert result["multi_vintage_groups_found"] == 0
