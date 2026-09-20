"""No-DB, fake-engine tests for compute_ttm's dirty-actor tracking.

fable-daily-intel-sql-tasks (2026-09-20, SECOND follow-up): the scalar
``as_of`` watermark this file used to pin (``TTM_LOOKBACK_DAYS=3``'s
replacement from the first follow-up) was established by the controller
to be commit-order UNSAFE — PostgreSQL's ``NOW()`` is transaction-START
time, so a writer that starts before a rollup snapshot but commits after
it can carry an ``as_of`` the rollup's watermark already passed, and
``as_of > watermark`` then skips that row forever (a second, narrower
hole: two equal ``as_of`` values fall on either side of strict ``>``
depending only on write order). See
``intelligence/company_financial_rollups.py``'s module docstring for the
full writeup and ``tests/test_capital_flow_rollups_pg.py`` for the real
two-connection PostgreSQL proofs (this file has no PG fixture and cannot
prove commit-order behavior itself — real concurrency needs a real
database).

``compute_ttm`` now decides which actors are dirty by comparing a content
fingerprint of each actor's current quarterly rows against the
fingerprint durably stored in ``capital_flows_ttm_state`` — see that
function's docstring. This file exercises `compute_ttm` and `run_all`
against a small hand-rolled fake SQLAlchemy engine — no Postgres, no
``pg_engine`` fixture — so it always runs, everywhere. It covers what a
fake engine CAN prove:
  (a) the ``watermark``/``ttm_watermark`` parameter is accepted for
      backward-compatible call signatures only and has no bearing on the
      SQL executed — a structural drift guard, so nobody quietly wires it
      back into a gating predicate;
  (b) compute_ttm issues exactly ONE statement per call (the fingerprint
      comparison, the stale-ttm delete, the per-actor state upsert, and
      the ttm write all live in one `WITH` so they share one query
      snapshot — see that SQL's own comment for why a second statement
      would race a concurrent writer);
  (c) failed-then-retried: a raising UPSERT never yields a new watermark;
      ``run_all`` keeps the caller's watermark unchanged on failure (pure
      pass-through, still useful for callers that persist it for
      telemetry);
  (d) restart hydration of ``OperatorState.capital_flow_ttm_watermark``
      (unchanged — it is still a plain string field, just informational
      now instead of a gating cursor).
"""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from intelligence import company_financial_rollups as cfr
from scripts.hermes_health import OperatorState

# ── Fake engine ──────────────────────────────────────────────────────


class _FakeResult:
    def __init__(self, rowcount: int = 0) -> None:
        self.rowcount = rowcount


class _FakeConn:
    """Records every ``execute`` call and dispatches a canned response.

    compute_ttm now issues exactly ONE statement per call
    (``_TTM_UPSERT_SQL`` — fingerprint comparison, stale-ttm delete,
    per-actor state upsert, and the ttm write, all in one ``WITH``), so
    this fake no longer needs to distinguish a second "what's the new
    watermark" query the way the pre-fingerprint design did.
    """

    def __init__(
        self,
        *,
        upsert_rowcount: int = 0,
        upsert_raises: Exception | None = None,
    ) -> None:
        self.calls: list[tuple[Any, dict]] = []
        self._upsert_rowcount = upsert_rowcount
        self._upsert_raises = upsert_raises

    def execute(self, stmt: Any, params: dict | None = None) -> _FakeResult:
        self.calls.append((stmt, dict(params or {})))
        if stmt is cfr._TTM_UPSERT_SQL:
            if self._upsert_raises is not None:
                raise self._upsert_raises
            return _FakeResult(rowcount=self._upsert_rowcount)
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


# ── (a) watermark is accepted but plays no role in the executed SQL ───


def test_watermark_param_is_not_among_the_sql_bind_parameters():
    """Structural drift guard: whatever ``watermark`` compute_ttm is
    called with, it never shows up in the bound params sent to the DB —
    proof that it cannot gate anything at the SQL level any more, however
    tempting it might look to a future editor to wire it back in as a
    shortcut."""
    conn = _FakeConn(upsert_rowcount=5)
    engine = _FakeEngine(conn)

    cfr.compute_ttm(engine, "2026-09-10T00:00:00+00:00")

    upsert_call = next(c for c in conn.calls if c[0] is cfr._TTM_UPSERT_SQL)
    assert "watermark" not in upsert_call[1]
    assert set(upsert_call[1]) == {"window", "source_filing", "confidence"}


def test_none_watermark_behaves_identically_to_any_other_watermark():
    """``watermark=None`` and an arbitrary non-None ``watermark`` must
    produce the byte-identical bound-params dict — both are equally
    inert. (Distinguishing "first run" from "Nth run" is no longer this
    parameter's job; capital_flows_ttm_state having no row for an actor
    is what marks it dirty, and that is DB content the fake engine here
    cannot model — see tests/test_capital_flow_rollups_pg.py.)"""
    conn_a = _FakeConn(upsert_rowcount=1)
    cfr.compute_ttm(_FakeEngine(conn_a), None)

    conn_b = _FakeConn(upsert_rowcount=1)
    cfr.compute_ttm(_FakeEngine(conn_b), "some-arbitrary-string")

    params_a = next(c for c in conn_a.calls if c[0] is cfr._TTM_UPSERT_SQL)[1]
    params_b = next(c for c in conn_b.calls if c[0] is cfr._TTM_UPSERT_SQL)[1]
    assert params_a == params_b


def test_result_watermark_is_populated_and_informational_only():
    """On success, ``TtmResult.watermark`` is always a non-None string
    (kept for backward-compatible callers that persist it for telemetry
    — see the module docstring), regardless of what rowcount came back."""
    conn = _FakeConn(upsert_rowcount=0)
    result = cfr.compute_ttm(_FakeEngine(conn), None)
    assert result.watermark is not None
    assert result.rows_written == 0


# ── (b) exactly one statement per call, sharing one snapshot ──────────


def test_compute_ttm_issues_exactly_one_statement():
    """The fingerprint comparison, the stale-ttm delete, the per-actor
    state upsert, and the ttm write all live in ONE ``WITH`` statement —
    compute_ttm must not split them into multiple ``conn.execute()``
    calls, which would let a concurrent write land between them and
    durably record a fingerprint the ttm write never actually matched
    (see ``_TTM_UPSERT_SQL``'s own comment)."""
    conn = _FakeConn(upsert_rowcount=3)
    cfr.compute_ttm(_FakeEngine(conn), None)
    assert len(conn.calls) == 1
    assert conn.calls[0][0] is cfr._TTM_UPSERT_SQL


def test_sql_is_fingerprint_based_not_timestamp_based():
    """Structural drift guard: the SQL under test keys its dirty-actor
    set on a durable per-actor content fingerprint
    (``capital_flows_ttm_state`` / ``quarter_fingerprint`` /
    ``IS DISTINCT FROM``), not on any ``as_of``-vs-cursor comparison."""
    full_sql = str(cfr._TTM_UPSERT_SQL)
    assert "capital_flows_ttm_state" in full_sql
    assert "quarter_fingerprint" in full_sql
    assert "IS DISTINCT FROM" in full_sql
    # No bind-parameter placeholder named :watermark anywhere.
    assert ":watermark" not in full_sql


def test_sql_deletes_stale_ttm_rows_for_dirty_actors():
    """Structural drift guard for the case-3 fix: a dirty actor's ttm
    row that no longer has a qualifying 4-quarter window must be
    DELETEd, not just left un-refreshed."""
    full_sql = str(cfr._TTM_UPSERT_SQL)
    assert "deleted_stale" in full_sql
    assert "DELETE FROM capital_flows" in full_sql


# ── (c) failed-then-retried ────────────────────────────────────────────


def test_failed_upsert_raises_and_does_not_yield_a_watermark():
    conn = _FakeConn(upsert_raises=RuntimeError("statement timeout"))
    engine = _FakeEngine(conn)

    with pytest.raises(RuntimeError, match="statement timeout"):
        cfr.compute_ttm(engine, "2026-09-15T00:00:00+00:00")

    # The one statement compute_ttm issues raised, so nothing about a
    # watermark was ever computed (execution never reaches that line).
    assert len(conn.calls) == 1
    assert conn.calls[0][0] is cfr._TTM_UPSERT_SQL


def test_run_all_keeps_watermark_unchanged_when_ttm_fails_then_retry(
    monkeypatch,
):
    """The (no-DB) equivalent of "TTM write raises -> tracking not
    advanced -> the caller's persisted value is unchanged" -- exercised
    against run_all, which is what scripts/hermes_operator.py actually
    calls. (Which ACTORS get recomputed on the retry is now decided by
    capital_flows_ttm_state content, not by this watermark value — see
    tests/test_capital_flow_rollups_pg.py for that proof.)"""
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

    assert seen_watermarks == [original_watermark, original_watermark]


def test_run_all_reports_ok_false_on_partial_failure_ttm_only():
    """compute_ttm fails, fold_announcements succeeds -> run_all itself
    never raises, but stats["ok"] is False (the caller in
    scripts/hermes_operator.py is what turns this into a ledger
    FAILURE -- see tests/test_hermes_daily_intel_resumable.py::
    TestCapitalFlowRollupsPartialFailure)."""
    conn = _FakeConn(upsert_raises=RuntimeError("boom"))
    engine = _FakeEngine(conn)

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
