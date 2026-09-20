"""No-DB, fake-engine tests for compute_ttm's durable watermark tracking.

fable-daily-intel-sql-tasks (2026-09-20 follow-up): ``TTM_LOOKBACK_DAYS =
3`` was removed from ``intelligence/company_financial_rollups.py`` and
replaced with a durable, restart-safe watermark (``as_of`` high-water
mark, persisted on ``OperatorState.capital_flow_ttm_watermark`` — see
that module's docstring for the corrected-row signal finding and the
advance-after-write contract). These tests exercise ``compute_ttm`` and
``run_all`` against a small hand-rolled fake SQLAlchemy engine — no
Postgres, no ``pg_engine`` fixture — so they always run, everywhere.

Covers (fable-daily-intel-sql-tasks controller brief, section 2):
  (a) downtime beyond three days: an old watermark is passed through to
      the SQL unbound by any fixed day-count — there is no reintroduced
      lookback constant anywhere in the call path.
  (b) late correction to an OLD fiscal period: the ``changed_actors`` CTE
      that governs the recompute set has no ``fiscal_period`` predicate
      at all — only ``as_of`` vs. the watermark — so a correction to any
      period, old or new, is caught the same way a brand new row is
      (real-PG proof of the actual recompute lives in
      ``tests/test_capital_flow_rollups_pg.py``).
  (c) failed-then-retried: a raising UPSERT never yields a new watermark;
      ``run_all`` keeps the caller's watermark unchanged on failure, so a
      retry with the same watermark recomputes the identical actor set.
  (d) restart hydration of the tracking state (``OperatorState``).
"""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from intelligence import company_financial_rollups as cfr
from scripts.hermes_health import OperatorState

# ── Fake engine ──────────────────────────────────────────────────────


class _FakeResult:
    def __init__(self, rowcount: int = 0, fetchone_value: Any = None) -> None:
        self.rowcount = rowcount
        self._fetchone_value = fetchone_value

    def fetchone(self) -> Any:
        return self._fetchone_value


class _FakeConn:
    """Records every ``execute`` call and dispatches a canned response by
    statement identity — ``compute_ttm`` always issues the UPSERT
    (``_TTM_UPSERT_SQL``) first, then the watermark query
    (``_TTM_NEW_WATERMARK_SQL``), inside the same transaction."""

    def __init__(
        self,
        *,
        upsert_rowcount: int = 0,
        upsert_raises: Exception | None = None,
        new_watermark_value: Any = None,
    ) -> None:
        self.calls: list[tuple[Any, dict]] = []
        self._upsert_rowcount = upsert_rowcount
        self._upsert_raises = upsert_raises
        self._new_watermark_value = new_watermark_value

    def execute(self, stmt: Any, params: dict | None = None) -> _FakeResult:
        self.calls.append((stmt, dict(params or {})))
        if stmt is cfr._TTM_UPSERT_SQL:
            if self._upsert_raises is not None:
                raise self._upsert_raises
            return _FakeResult(rowcount=self._upsert_rowcount)
        if stmt is cfr._TTM_NEW_WATERMARK_SQL:
            fetchone = (
                (self._new_watermark_value,)
                if self._new_watermark_value is not None
                else None
            )
            return _FakeResult(fetchone_value=fetchone)
        raise AssertionError(f"unexpected statement: {stmt}")


class _FakeEngine:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def begin(self):
        conn = self._conn

        class _Ctx:
            def __enter__(self_inner):
                return conn

            def __exit__(self_inner, exc_type, exc, tb):
                return False  # never swallow — real engine.begin() rolls back and re-raises

        return _Ctx()


# ── (a) downtime beyond three days: no reintroduced day-count cap ─────


def test_watermark_passed_through_unbound_by_any_fixed_day_window():
    """A watermark 10 days old is passed to the SQL VERBATIM — nothing in
    compute_ttm clamps or reinterprets it as a bounded N-day lookback.
    This is the fake-engine-level proof that TTM_LOOKBACK_DAYS's fixed
    3-day cap is gone, not reintroduced under a new name."""
    old_watermark = "2026-09-10T00:00:00+00:00"  # 10 days before NOW=2026-09-20
    conn = _FakeConn(upsert_rowcount=5, new_watermark_value="2026-09-20T02:00:00+00:00")
    engine = _FakeEngine(conn)

    result = cfr.compute_ttm(engine, old_watermark)

    upsert_call = next(c for c in conn.calls if c[0] is cfr._TTM_UPSERT_SQL)
    assert upsert_call[1]["watermark"] == old_watermark
    # No lookback_days-shaped parameter survives anywhere in the call.
    assert "lookback_days" not in upsert_call[1]
    assert result.rows_written == 5


def test_none_watermark_means_unconditional_full_recompute():
    """No persisted watermark (first-ever run) -> every actor qualifies,
    unconditionally -- not bounded by any implicit day window either."""
    conn = _FakeConn(upsert_rowcount=42, new_watermark_value="2026-09-20T02:00:00+00:00")
    engine = _FakeEngine(conn)

    result = cfr.compute_ttm(engine, None)

    upsert_call = next(c for c in conn.calls if c[0] is cfr._TTM_UPSERT_SQL)
    assert upsert_call[1]["watermark"] is None
    assert result.rows_written == 42
    assert result.watermark == "2026-09-20T02:00:00+00:00"


# ── (b) late correction to an OLD fiscal period ────────────────────────


def test_changed_actors_cte_has_no_fiscal_period_predicate():
    """Structural drift guard: the changed_actors CTE that decides WHICH
    actors get recomputed keys ONLY on as_of vs. the watermark -- it does
    not (and must not) filter on fiscal_period. That's what makes a
    correction to an OLD fiscal period indistinguishable, at this gate,
    from a brand-new quarterly row -- both just move as_of forward past
    the watermark. (The actual recompute-set behaviour on real data is
    proven end-to-end in tests/test_capital_flow_rollups_pg.py.)"""
    full_sql = str(cfr._TTM_UPSERT_SQL)
    start = full_sql.index("changed_actors AS (")
    end = full_sql.index("q_ranked AS (")
    cte_body = full_sql[start:end]
    assert "fiscal_period" not in cte_body
    assert "as_of" in cte_body
    assert "watermark" in cte_body


def test_new_watermark_query_also_has_no_fiscal_period_predicate():
    wm_sql = str(cfr._TTM_NEW_WATERMARK_SQL)
    assert "fiscal_period" not in wm_sql
    assert "as_of" in wm_sql


# ── (c) failed-then-retried ────────────────────────────────────────────


def test_failed_upsert_raises_and_does_not_yield_a_watermark():
    conn = _FakeConn(upsert_raises=RuntimeError("statement timeout"))
    engine = _FakeEngine(conn)

    with pytest.raises(RuntimeError, match="statement timeout"):
        cfr.compute_ttm(engine, "2026-09-15T00:00:00+00:00")

    # Only the UPSERT was attempted -- the watermark query never ran,
    # because the transaction rolled back before compute_ttm could get
    # there.
    assert len(conn.calls) == 1
    assert conn.calls[0][0] is cfr._TTM_UPSERT_SQL


def test_run_all_keeps_watermark_unchanged_when_ttm_fails_then_retry_recomputes_same_set(
    monkeypatch,
):
    """The (no-DB) equivalent of "TTM write raises -> tracking not
    advanced -> the next call recomputes exactly the same set, and on
    success, advances it" -- exercised against run_all, which is what
    scripts/hermes_operator.py actually calls."""
    seen_watermarks: list[str | None] = []

    def _fake_compute_ttm(engine, watermark=None):
        seen_watermarks.append(watermark)
        if len(seen_watermarks) == 1:
            raise RuntimeError("cancelled")
        return cfr.TtmResult(rows_written=7, watermark="2026-09-20T02:00:00+00:00")

    monkeypatch.setattr(cfr, "compute_ttm", _fake_compute_ttm)
    monkeypatch.setattr(cfr, "fold_announcements", lambda engine: 3)

    original_watermark = "2026-09-15T00:00:00+00:00"

    stats1 = cfr.run_all(MagicMock(), ttm_watermark=original_watermark)
    assert stats1["ttm_ok"] is False
    assert stats1["ttm_watermark"] == original_watermark  # unchanged on failure
    assert stats1["ok"] is False

    # Retry: caller passes the SAME (unchanged) watermark back in.
    stats2 = cfr.run_all(MagicMock(), ttm_watermark=stats1["ttm_watermark"])
    assert stats2["ttm_ok"] is True
    assert stats2["ttm_watermark"] == "2026-09-20T02:00:00+00:00"
    assert stats2["ok"] is True

    # Both attempts recomputed the identical set: same watermark in.
    assert seen_watermarks == [original_watermark, original_watermark]


def test_run_all_reports_ok_false_on_partial_failure_ttm_only():
    """compute_ttm fails, fold_announcements succeeds -> run_all itself
    never raises, but stats["ok"] is False (the caller in
    scripts/hermes_operator.py is what turns this into a ledger
    FAILURE -- see tests/test_hermes_daily_intel_resumable.py::
    TestCapitalFlowRollupsPartialFailure)."""
    conn = _FakeConn(upsert_raises=RuntimeError("boom"))
    engine = _FakeEngine(conn)

    # fold_announcements needs its own fake — patch it directly since
    # this test only cares about run_all's aggregation logic, not its SQL.
    import intelligence.company_financial_rollups as real_cfr

    orig_fold = real_cfr.fold_announcements
    real_cfr.fold_announcements = lambda e: 9
    try:
        stats = real_cfr.run_all(engine, ttm_watermark="2026-09-15T00:00:00+00:00")
    finally:
        real_cfr.fold_announcements = orig_fold

    assert stats["ttm_ok"] is False
    assert stats["fold_ok"] is True
    assert stats["rolled_rows"] == 9
    assert stats["ok"] is False
    assert stats["ttm_watermark"] == "2026-09-15T00:00:00+00:00"


# ── (d) restart hydration of the tracking state ────────────────────────


class _FakeHydrateEngine:
    """Minimal stand-in for the engine hydrate_from_snapshot() connects
    to: one row containing a JSON payload with an operator_state dict."""

    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def connect(self):
        payload = self._payload

        class _Conn:
            def execute(self_inner, *a, **kw):
                class _Result:
                    def fetchone(self_r):
                        return (payload,)

                return _Result()

            def __enter__(self_inner):
                return self_inner

            def __exit__(self_inner, *a):
                return False

        return _Conn()


def test_hydration_restores_watermark_when_currently_unset():
    payload = {
        "operator_state": {
            "capital_flow_ttm_watermark": "2026-09-19T02:00:00+00:00",
        }
    }
    state = OperatorState()
    assert state.capital_flow_ttm_watermark is None

    hydrated = state.hydrate_from_snapshot(_FakeHydrateEngine(payload))

    assert hydrated is True
    assert state.capital_flow_ttm_watermark == "2026-09-19T02:00:00+00:00"


def test_hydration_never_overwrites_a_watermark_already_set_this_process():
    payload = {
        "operator_state": {
            "capital_flow_ttm_watermark": "2026-09-01T00:00:00+00:00",
        }
    }
    state = OperatorState()
    state.capital_flow_ttm_watermark = "2026-09-19T02:00:00+00:00"  # live, in-process value

    state.hydrate_from_snapshot(_FakeHydrateEngine(payload))

    # The live in-process value wins -- hydration only fills in what's
    # currently unset, same rule as every other daily-intel ledger field.
    assert state.capital_flow_ttm_watermark == "2026-09-19T02:00:00+00:00"


def test_to_dict_round_trips_watermark():
    state = OperatorState()
    state.capital_flow_ttm_watermark = "2026-09-20T02:00:00+00:00"
    d = state.to_dict()
    assert d["capital_flow_ttm_watermark"] == "2026-09-20T02:00:00+00:00"
