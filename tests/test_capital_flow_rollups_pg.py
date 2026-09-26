"""PostgreSQL-backed validation of compute_ttm's durable per-actor
dirty tracking (fable-daily-intel-sql-tasks, 2026-09-20 SECOND follow-up).

The controller established that the first follow-up's scalar ``as_of``
watermark is NOT commit-order safe: PostgreSQL's ``NOW()``/
``CURRENT_TIMESTAMP`` (what ``ingestion/altdata/sec_xbrl_financials.py::
_write_rows`` binds into ``as_of``) is TRANSACTION-START time, not commit
time, so a writer transaction that starts before a rollup's snapshot but
commits after it can carry an ``as_of`` the rollup's persisted watermark
already passed — permanently skipped, not just delayed a cycle. Equal
``as_of`` values have a second, narrower hole under strict ``>``. And,
separately, the old design only ever ADDED ttm rows — a deleted or
reclassified quarter row left a stale ttm row with nothing to remove it.

``intelligence/company_financial_rollups.py::compute_ttm`` now decides
which actors are dirty by comparing a durable per-actor content
fingerprint (``capital_flows_ttm_state``) against each actor's CURRENT
committed ``period_type='quarter'`` rows — see that module's docstring
for the full design and why this is commit-order safe by construction.

Uses the shared ``pg_engine`` fixture (tests/conftest.py — skips cleanly
if no Postgres is reachable, and honours ``GRID_TEST_DB_URL`` for a
disposable test database). Writes directly to ``public.capital_flows``
and ``public.capital_flows_ttm_state`` (migration
``capital_flow_ttm_state_20260920``) using unique ``rollup_test_<uuid>``
actor ids and cleans up afterwards. Calls the REAL ``compute_ttm`` — no
SQL text assertions except where noted, only behavior.

The commit-order tests use TWO INDEPENDENT connections/transactions (real
PostgreSQL concurrency — one connection holds a transaction open while
another, separate call runs compute_ttm) rather than sequential seeding,
because sequential seeding cannot exercise "started before, committed
after" at all.
"""
from __future__ import annotations

import uuid
from datetime import date

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

import intelligence.company_financial_rollups as cfr
from intelligence.company_financial_rollups import TTM_SOURCE_FILING, compute_ttm


@pytest.fixture
def test_actor_id() -> str:
    return f"rollup_test_{uuid.uuid4().hex[:10]}"


@pytest.fixture
def test_actor_id_b() -> str:
    return f"rollup_test_{uuid.uuid4().hex[:10]}"


@pytest.fixture(autouse=True)
def cleanup_test_rows(pg_engine: Engine, test_actor_id: str, test_actor_id_b: str):
    yield
    ids = [test_actor_id, test_actor_id_b]
    with pg_engine.begin() as conn:
        conn.execute(
            text("DELETE FROM capital_flows WHERE actor_id = ANY(:ids)").bindparams(
                ids=ids,
            ),
        )
        conn.execute(
            text(
                "DELETE FROM capital_flows_ttm_state WHERE actor_id = ANY(:ids)",
            ).bindparams(ids=ids),
        )


def _insert_quarter_with_as_of(
    engine: Engine,
    actor_id: str,
    fp: date,
    amount: float,
    *,
    as_of_days_ago: float,
    flow_type: str = "revenue",
    source_filing: str = "10-Q test",
) -> None:
    """Insert (or refresh, updating as_of) a quarterly row with an
    explicit, backdated ``as_of``, committed immediately."""
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO capital_flows (
                    actor_id, fiscal_period, period_type, flow_type,
                    direction, amount_usd, counterparty_id, source_filing,
                    confidence, currency, as_of
                ) VALUES (
                    :a, :fp, 'quarter', :ft, 'in', :amt, NULL, :sf,
                    'confirmed', 'USD',
                    NOW() - make_interval(secs => :ago_secs)
                )
                ON CONFLICT (
                    actor_id, fiscal_period, period_type, flow_type,
                    (COALESCE(NULLIF(counterparty_id,''), '__none__')),
                    source_filing
                ) DO UPDATE SET
                    amount_usd = EXCLUDED.amount_usd,
                    as_of = EXCLUDED.as_of
                """,
            ).bindparams(
                a=actor_id, fp=fp, ft=flow_type, amt=amount, sf=source_filing,
                ago_secs=as_of_days_ago * 86400.0,
            ),
        )


def _insert_quarter_explicit_as_of(
    engine: Engine,
    actor_id: str,
    fp: date,
    amount: float,
    as_of: object,
    *,
    flow_type: str = "revenue",
    source_filing: str = "10-Q test",
) -> None:
    """Like ``_insert_quarter_with_as_of`` but binds ``as_of`` directly —
    used to give two DIFFERENT actors' rows the EXACT same timestamp
    (case 2), which a relative "days ago" offset computed at two
    different statement times cannot guarantee."""
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO capital_flows (
                    actor_id, fiscal_period, period_type, flow_type,
                    direction, amount_usd, counterparty_id, source_filing,
                    confidence, currency, as_of
                ) VALUES (
                    :a, :fp, 'quarter', :ft, 'in', :amt, NULL, :sf,
                    'confirmed', 'USD', :as_of
                )
                ON CONFLICT (
                    actor_id, fiscal_period, period_type, flow_type,
                    (COALESCE(NULLIF(counterparty_id,''), '__none__')),
                    source_filing
                ) DO UPDATE SET
                    amount_usd = EXCLUDED.amount_usd,
                    as_of = EXCLUDED.as_of
                """,
            ).bindparams(
                a=actor_id, fp=fp, ft=flow_type, amt=amount, sf=source_filing,
                as_of=as_of,
            ),
        )


def _delete_insert_quarter_uncommitted(
    conn,
    actor_id: str,
    fp: date,
    amount: float,
    *,
    flow_type: str = "revenue",
    source_filing: str = "10-Q test",
) -> None:
    """Mirrors ``ingestion/altdata/sec_xbrl_financials.py::_write_rows``'s
    DELETE-then-plain-INSERT pattern (``as_of = NOW()``, i.e. this
    connection's transaction-start time), executed on an ALREADY-OPEN
    connection/transaction — the caller controls commit/rollback. Used to
    hold a write uncommitted while a second, independent connection runs
    ``compute_ttm``, so the two "started before, committed after" tests
    below exercise real PostgreSQL concurrency instead of sequential
    seeding."""
    conn.execute(
        text(
            """
            DELETE FROM capital_flows
            WHERE actor_id = :a AND fiscal_period = :fp
              AND period_type = 'quarter' AND flow_type = :ft
              AND source_filing = :sf
            """,
        ).bindparams(a=actor_id, fp=fp, ft=flow_type, sf=source_filing),
    )
    conn.execute(
        text(
            """
            INSERT INTO capital_flows (
                actor_id, fiscal_period, period_type, flow_type,
                direction, amount_usd, counterparty_id, source_filing,
                confidence, currency, as_of
            ) VALUES (
                :a, :fp, 'quarter', :ft, 'in', :amt, NULL, :sf,
                'confirmed', 'USD', NOW()
            )
            """,
        ).bindparams(a=actor_id, fp=fp, ft=flow_type, amt=amount, sf=source_filing),
    )


def _fetch_ttm_rows(engine: Engine, actor_id: str, flow_type: str = "revenue") -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT fiscal_period, amount_usd FROM capital_flows "
                "WHERE actor_id = :a AND period_type = 'ttm' "
                "AND source_filing = :sf AND flow_type = :ft "
                "ORDER BY fiscal_period DESC"
            ).bindparams(a=actor_id, sf=TTM_SOURCE_FILING, ft=flow_type),
        ).fetchall()
    return [{"fiscal_period": r[0], "amount_usd": float(r[1])} for r in rows]


def _quarter_fingerprint_state(engine: Engine, actor_id: str) -> str | None:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT quarter_fingerprint FROM capital_flows_ttm_state "
                "WHERE actor_id = :a",
            ).bindparams(a=actor_id),
        ).fetchone()
    return row[0] if row else None


_QUARTERS = [date(2024, 3, 31), date(2024, 6, 30), date(2024, 9, 30), date(2024, 12, 31)]


# ── Baseline behavior (still true under the fingerprint design) ───────


def test_first_time_actor_is_recomputed_regardless_of_watermark_param(
    pg_engine: Engine, test_actor_id: str, test_actor_id_b: str,
):
    """The ``watermark`` parameter compute_ttm still accepts is now
    vestigial — kept only for backward-compatible call signatures (see
    the module docstring). Passing a value that would have excluded BOTH
    actors under the retired scalar-watermark design (a timestamp in the
    FUTURE, newer than every row either actor has) must have ZERO
    effect: both actors are first-time-seen (no capital_flows_ttm_state
    row yet), so both are dirty and both get a ttm row."""
    amounts = [100.0, 110.0, 120.0, 130.0]
    for fp, amt in zip(_QUARTERS, amounts):
        _insert_quarter_with_as_of(pg_engine, test_actor_id, fp, amt, as_of_days_ago=20)
        _insert_quarter_with_as_of(pg_engine, test_actor_id_b, fp, amt, as_of_days_ago=5)

    with pg_engine.connect() as conn:
        future_watermark = conn.execute(
            text("SELECT NOW() + INTERVAL '1 hour'"),
        ).fetchone()[0].isoformat()

    compute_ttm(pg_engine, future_watermark)

    a_rows = _fetch_ttm_rows(pg_engine, test_actor_id)
    b_rows = _fetch_ttm_rows(pg_engine, test_actor_id_b)
    assert a_rows and a_rows[0]["amount_usd"] == pytest.approx(sum(amounts)), a_rows
    assert b_rows and b_rows[0]["amount_usd"] == pytest.approx(sum(amounts)), b_rows


def test_written_ttm_value_equals_independent_computation(
    pg_engine: Engine, test_actor_id: str,
):
    """No prior state (first-ever run for this actor) -> full recompute;
    the written ttm amount must equal a straightforward independent sum
    of the four quarters, nothing derived from the SQL under test."""
    amounts = [100.0, 250.5, 99.25, 400.0]
    for fp, amt in zip(_QUARTERS, amounts):
        _insert_quarter_with_as_of(pg_engine, test_actor_id, fp, amt, as_of_days_ago=0)

    result = compute_ttm(pg_engine, None)
    assert result.rows_written >= 1
    assert result.watermark is not None

    rows = _fetch_ttm_rows(pg_engine, test_actor_id)
    latest = [r for r in rows if r["fiscal_period"] == date(2024, 12, 31)]
    assert len(latest) == 1
    assert latest[0]["amount_usd"] == pytest.approx(sum(amounts))


def test_failed_call_writes_nothing_and_a_later_call_is_unaffected(
    pg_engine: Engine, test_actor_id: str, monkeypatch,
):
    """A call that fails mid-transaction must leave NOTHING committed —
    no ttm row, no capital_flows_ttm_state row — and a later, real call
    must be unaffected. Forced here by monkeypatching the module's SQL
    constant to a statement that raises a genuine PostgreSQL error
    (division by zero) while still consuming the exact same bind
    parameters compute_ttm passes — the retired design's forcing
    mechanism (an invalid ``:watermark`` cast) no longer applies, since
    ``:watermark`` isn't bound by the SQL any more."""
    amounts = [100.0, 110.0, 120.0, 130.0]
    for fp, amt in zip(_QUARTERS, amounts):
        _insert_quarter_with_as_of(pg_engine, test_actor_id, fp, amt, as_of_days_ago=0)

    broken_sql = text(
        "SELECT 1/0 WHERE :window::int IS NOT NULL "
        "AND :source_filing IS NOT NULL AND :confidence IS NOT NULL",
    )
    monkeypatch.setattr(cfr, "_TTM_UPSERT_SQL", broken_sql)
    with pytest.raises(SQLAlchemyError):
        compute_ttm(pg_engine, None)
    monkeypatch.undo()  # restore the real SQL before the next (real) call

    assert _fetch_ttm_rows(pg_engine, test_actor_id) == []
    assert _quarter_fingerprint_state(pg_engine, test_actor_id) is None

    compute_ttm(pg_engine, None)
    rows = _fetch_ttm_rows(pg_engine, test_actor_id)
    latest = [r for r in rows if r["fiscal_period"] == date(2024, 12, 31)]
    assert len(latest) == 1
    assert latest[0]["amount_usd"] == pytest.approx(sum(amounts))


def test_first_run_recomputes_actor_with_only_old_rows(
    pg_engine: Engine, test_actor_id: str,
):
    """An actor whose quarterly rows are all "old" by wall-clock time is
    still recomputed on its first run — content comparison does not care
    how stale a row looks, only whether it differs from what is stored
    (nothing is stored yet). This is the fixed ``TTM_LOOKBACK_DAYS=3``
    regression the first follow-up already fixed, still true here."""
    amounts = [100.0, 110.0, 120.0, 130.0]
    for fp, amt in zip(_QUARTERS, amounts):
        _insert_quarter_with_as_of(pg_engine, test_actor_id, fp, amt, as_of_days_ago=10)

    compute_ttm(pg_engine, None)

    rows = _fetch_ttm_rows(pg_engine, test_actor_id)
    latest = [r for r in rows if r["fiscal_period"] == date(2024, 12, 31)]
    assert len(latest) == 1, (
        f"an actor whose only rows are 10 days old must still be picked "
        f"up on its first run, got {rows}"
    )


# ── Case 1: late-committing earlier as_of (real concurrency) ──────────


def test_late_committing_earlier_as_of_is_recomputed(
    pg_engine: Engine, test_actor_id: str, test_actor_id_b: str,
):
    """Proves the controller's headline correctness gap is closed: a
    writer transaction that STARTS before a compute_ttm run's snapshot
    but COMMITS after it must still be picked up by the NEXT compute_ttm
    call — however much OTHER traffic committed with a LATER as_of in
    between. Under the retired scalar-as_of watermark this could be
    permanently missed: if some OTHER actor's row committed (and got
    folded into the persisted watermark) between the late writer's
    transaction START and its COMMIT, the watermark could advance past
    the late writer's as_of before that row was ever visible, and
    ``as_of > watermark`` would then never match it again. The
    fingerprint design has no such window: dirtiness is decided by
    comparing committed CONTENT against what was last durably recorded,
    never by timestamp ordering."""
    amounts = [100.0, 110.0, 120.0, 130.0]
    for fp, amt in zip(_QUARTERS, amounts):
        _insert_quarter_with_as_of(pg_engine, test_actor_id, fp, amt, as_of_days_ago=20)
    # Baseline: actor X already has a correct ttm row and a recorded
    # fingerprint from a prior, ordinary run.
    compute_ttm(pg_engine, None)
    baseline = _fetch_ttm_rows(pg_engine, test_actor_id)
    assert baseline and baseline[0]["amount_usd"] == pytest.approx(sum(amounts))

    # Connection A: begin a real transaction, correct actor X's Q4 row
    # (mirrors _write_rows's DELETE-then-INSERT), as_of = NOW() = this
    # transaction's own start time (t1). Do NOT commit yet.
    conn_a = pg_engine.connect()
    txn_a = conn_a.begin()
    try:
        _delete_insert_quarter_uncommitted(
            conn_a, test_actor_id, date(2024, 12, 31), 135.0,
        )

        # While A is still open, an UNRELATED actor commits a row with a
        # LATER as_of — the "other traffic" that could leapfrog a scalar
        # watermark past A's still-in-flight transaction start.
        _insert_quarter_with_as_of(
            pg_engine, test_actor_id_b, date(2024, 12, 31), 999.0, as_of_days_ago=0,
        )

        # Connection B: an independent compute_ttm call, snapshot taken
        # AFTER A's transaction started but BEFORE A commits (t2). Must
        # correctly see no change for actor X yet — A's row isn't
        # committed. Whatever "tracking" value it returns is what the
        # caller would persist (it plays no gating role — see below).
        run1 = compute_ttm(pg_engine, None)
        persisted_tracking = run1.watermark
        assert _fetch_ttm_rows(pg_engine, test_actor_id) == baseline, (
            "compute_ttm must not see or act on A's uncommitted correction"
        )

        # NOW commit A (t3 > t2 > t1) — the correction becomes visible.
        txn_a.commit()
    finally:
        conn_a.close()

    # Run again with whatever was "persisted" from run1.
    compute_ttm(pg_engine, persisted_tracking)

    rows = _fetch_ttm_rows(pg_engine, test_actor_id)
    latest = [r for r in rows if r["fiscal_period"] == date(2024, 12, 31)]
    assert len(latest) == 1, f"actor X's late-committing correction must be recomputed, got {rows}"
    assert latest[0]["amount_usd"] == pytest.approx(100.0 + 110.0 + 120.0 + 135.0), (
        f"actor X's TTM must reflect A's corrected Q4 amount (135.0), not "
        f"the stale 130.0, got {latest}"
    )


def test_late_committing_row_with_explicit_older_as_of_is_recomputed(
    pg_engine: Engine, test_actor_id: str,
):
    """Variant of the above with an explicit, deliberately-OLD ``as_of``
    on the correction (older than the actor's own other quarters' as_of,
    and older than any watermark a caller could plausibly hold) — the
    fingerprint design does not read ``as_of`` at all, so this makes no
    difference to whether the correction is picked up, unlike the
    retired scalar-watermark design where a row's ``as_of`` WAS the
    entire gating signal."""
    amounts = [100.0, 110.0, 120.0, 130.0]
    for fp, amt in zip(_QUARTERS, amounts):
        _insert_quarter_with_as_of(pg_engine, test_actor_id, fp, amt, as_of_days_ago=20)
    compute_ttm(pg_engine, None)

    _insert_quarter_with_as_of(
        pg_engine, test_actor_id, date(2024, 12, 31), 777.0, as_of_days_ago=60,
    )

    compute_ttm(pg_engine, None)

    rows = _fetch_ttm_rows(pg_engine, test_actor_id)
    latest = [r for r in rows if r["fiscal_period"] == date(2024, 12, 31)]
    assert len(latest) == 1
    assert latest[0]["amount_usd"] == pytest.approx(100.0 + 110.0 + 120.0 + 777.0), (
        f"a correction with an explicitly OLD as_of must still be picked "
        f"up by content comparison, got {rows}"
    )


# ── Case 2: equal as_of timestamps ─────────────────────────────────────


def test_equal_as_of_timestamps_do_not_skip_the_second_actor(
    pg_engine: Engine, test_actor_id: str, test_actor_id_b: str,
):
    """Two actors whose newest quarter rows carry the EXACT SAME
    ``as_of``: actor P is processed by run 1 (which, under the retired
    scalar design, would have advanced the watermark to exactly that
    timestamp). Actor Q's matching row is inserted AFTER run 1, with the
    identical ``as_of``. Under strict ``as_of > watermark``, Q's row is
    NOT strictly greater than a watermark equal to it — permanently
    skipped. The fingerprint design does not compare ``as_of`` at all,
    so Q (a first-time actor) is picked up regardless."""
    with pg_engine.connect() as conn:
        shared_as_of = conn.execute(text("SELECT NOW()")).fetchone()[0]

    amounts = [100.0, 110.0, 120.0, 130.0]
    for fp, amt in zip(_QUARTERS[:-1], amounts[:-1]):
        _insert_quarter_with_as_of(pg_engine, test_actor_id, fp, amt, as_of_days_ago=1)
    _insert_quarter_explicit_as_of(
        pg_engine, test_actor_id, _QUARTERS[-1], amounts[-1], shared_as_of,
    )

    run1 = compute_ttm(pg_engine, None)
    p_rows = _fetch_ttm_rows(pg_engine, test_actor_id)
    assert p_rows and p_rows[0]["amount_usd"] == pytest.approx(sum(amounts))

    # Actor Q, inserted AFTER run 1, with the identical as_of on its
    # newest quarter.
    for fp, amt in zip(_QUARTERS[:-1], amounts[:-1]):
        _insert_quarter_with_as_of(pg_engine, test_actor_id_b, fp, amt, as_of_days_ago=1)
    _insert_quarter_explicit_as_of(
        pg_engine, test_actor_id_b, _QUARTERS[-1], amounts[-1], shared_as_of,
    )

    compute_ttm(pg_engine, run1.watermark)

    q_rows = _fetch_ttm_rows(pg_engine, test_actor_id_b)
    latest = [r for r in q_rows if r["fiscal_period"] == date(2024, 12, 31)]
    assert len(latest) == 1, (
        f"actor Q's row shares an as_of EQUAL to the previously-processed "
        f"watermark and must not be skipped by a > vs >= gap, got {q_rows}"
    )


# ── Case 3: removal / reclassification ─────────────────────────────────


def test_deleted_quarter_row_without_reinsert_triggers_recompute_and_removes_ttm(
    pg_engine: Engine, test_actor_id: str,
):
    """Actor Y has a full 4-quarter ttm row. One quarterly row is
    DELETEd with no replacement — nothing moves forward, so the retired
    scalar watermark could never have caught this at all. Y no longer
    has 4 qualifying quarters, so per compute_ttm's existing
    ``n_quarters = :window`` rule the group simply does not qualify any
    more, and the now-stale ttm row must not be left behind looking
    current: this design DELETEs it."""
    amounts = [100.0, 110.0, 120.0, 130.0]
    for fp, amt in zip(_QUARTERS, amounts):
        _insert_quarter_with_as_of(pg_engine, test_actor_id, fp, amt, as_of_days_ago=5)
    compute_ttm(pg_engine, None)
    assert _fetch_ttm_rows(pg_engine, test_actor_id) != []

    with pg_engine.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM capital_flows WHERE actor_id = :a AND fiscal_period = :fp "
                "AND period_type = 'quarter'",
            ).bindparams(a=test_actor_id, fp=date(2024, 6, 30)),
        )

    compute_ttm(pg_engine, None)

    rows = _fetch_ttm_rows(pg_engine, test_actor_id)
    assert rows == [], (
        f"actor Y no longer has 4 qualifying quarters after a plain "
        f"delete; the stale ttm row must be removed, not left behind, "
        f"got {rows}"
    )


def test_reclassified_quarter_row_triggers_recompute_and_removes_ttm(
    pg_engine: Engine, test_actor_id: str,
):
    """Same as the delete case, but via ``UPDATE ... SET period_type =
    'annual'`` on one of the four quarter rows instead of a DELETE — the
    row still exists, but no longer counts as a ``period_type='quarter'``
    row, so it drops out of both the fingerprint and the trailing-4
    window the same way a deletion would."""
    amounts = [100.0, 110.0, 120.0, 130.0]
    for fp, amt in zip(_QUARTERS, amounts):
        _insert_quarter_with_as_of(pg_engine, test_actor_id, fp, amt, as_of_days_ago=5)
    compute_ttm(pg_engine, None)
    assert _fetch_ttm_rows(pg_engine, test_actor_id) != []

    with pg_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE capital_flows SET period_type = 'annual' "
                "WHERE actor_id = :a AND fiscal_period = :fp AND period_type = 'quarter'",
            ).bindparams(a=test_actor_id, fp=date(2024, 9, 30)),
        )

    compute_ttm(pg_engine, None)

    rows = _fetch_ttm_rows(pg_engine, test_actor_id)
    assert rows == [], (
        f"a quarter row reclassified away from period_type='quarter' must "
        f"drop its actor's stale ttm row the same way a delete does, got {rows}"
    )


def test_all_quarter_rows_deleted_removes_stale_ttm_row(
    pg_engine: Engine, test_actor_id: str,
):
    """Every one of actor Y's quarter rows is deleted. Y's ttm row from
    the prior run must not survive as if it were still current, and the
    stored fingerprint must go to NULL (not just stop updating) so a
    future re-insertion is still detected as a change."""
    amounts = [100.0, 110.0, 120.0, 130.0]
    for fp, amt in zip(_QUARTERS, amounts):
        _insert_quarter_with_as_of(pg_engine, test_actor_id, fp, amt, as_of_days_ago=5)
    compute_ttm(pg_engine, None)
    assert _fetch_ttm_rows(pg_engine, test_actor_id) != []
    assert _quarter_fingerprint_state(pg_engine, test_actor_id) is not None

    with pg_engine.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM capital_flows WHERE actor_id = :a AND period_type = 'quarter'",
            ).bindparams(a=test_actor_id),
        )

    compute_ttm(pg_engine, None)

    assert _fetch_ttm_rows(pg_engine, test_actor_id) == [], (
        "actor Y has zero quarter rows left; its stale ttm row must be removed"
    )
    assert _quarter_fingerprint_state(pg_engine, test_actor_id) is None, (
        "capital_flows_ttm_state must record NULL (no rows), not leave the "
        "old non-NULL fingerprint in place, so a future re-insertion is "
        "still detected as a change from NULL"
    )


# ── Case 4: replay harmlessness ─────────────────────────────────────────


def test_replay_after_crash_before_watermark_persist_is_a_harmless_noop(
    pg_engine: Engine, test_actor_id: str,
):
    """Run compute_ttm (commits rows, returns a new tracking value);
    simulate a crash by NOT persisting that value, then call again with
    the OLD (pre-run) tracking value. Must be a byte-identical no-op:
    same row, same amount, no duplicate — because the durable per-actor
    state that actually gates recomputation
    (``capital_flows_ttm_state``) already committed atomically with the
    ttm row in run 1's own transaction. There is nothing left for a
    crash between "DB commit" and "operator-state persistence" to race,
    unlike the retired scalar-watermark design, where the caller's
    persisted value WAS the only gate. ``run_all``'s docstring and
    ``scripts/hermes_operator.py::_daily_intel_capital_flow_rollups``
    still persist ``stats["ttm_watermark"]`` at cycle end, after the DB
    commit — that ordering is preserved, it is just no longer load-
    bearing for correctness."""
    amounts = [100.0, 110.0, 120.0, 130.0]
    for fp, amt in zip(_QUARTERS, amounts):
        _insert_quarter_with_as_of(pg_engine, test_actor_id, fp, amt, as_of_days_ago=5)

    old_tracking = None  # what the caller had persisted BEFORE run 1
    run1 = compute_ttm(pg_engine, old_tracking)
    assert run1.rows_written >= 1
    rows_after_run1 = _fetch_ttm_rows(pg_engine, test_actor_id)
    latest_after_run1 = [r for r in rows_after_run1 if r["fiscal_period"] == date(2024, 12, 31)]
    assert len(latest_after_run1) == 1
    assert latest_after_run1[0]["amount_usd"] == pytest.approx(sum(amounts))

    # "Crash" — run1.watermark (the new tracking value) is NEVER
    # persisted. The next call is made with the STALE, pre-run1 value.
    run2 = compute_ttm(pg_engine, old_tracking)

    assert run2.rows_written == 0, (
        "nothing changed since run 1 committed; the replay must be a true "
        "no-op (nothing dirty), not a redundant recompute that merely "
        "happens to produce the same numbers"
    )
    rows_after_run2 = _fetch_ttm_rows(pg_engine, test_actor_id)
    assert rows_after_run2 == rows_after_run1, (
        f"replay must leave the ttm row byte-identical, "
        f"got {rows_after_run2} vs {rows_after_run1}"
    )
    latest_after_run2 = [r for r in rows_after_run2 if r["fiscal_period"] == date(2024, 12, 31)]
    assert len(latest_after_run2) == 1, "no duplicate rows from the replay"
