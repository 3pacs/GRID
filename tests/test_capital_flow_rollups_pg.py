"""PostgreSQL-backed validation of compute_ttm's durable watermark
tracking (fable-daily-intel-sql-tasks, 2026-09-20 follow-up).

Uses the shared ``pg_engine`` fixture (tests/conftest.py — skips cleanly
if no Postgres is reachable, and honours ``GRID_TEST_DB_URL`` for a
disposable test database). Writes directly to ``public.capital_flows``
using unique ``rollup_test_<uuid>`` actor ids and cleans up afterwards —
same isolation pattern as the pre-existing ``tests/
test_capital_flow_rollups.py``. Calls the REAL ``compute_ttm`` — no SQL
text assertions, only behavior.
"""
from __future__ import annotations

import uuid
from datetime import date

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

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
    with pg_engine.begin() as conn:
        conn.execute(
            text("DELETE FROM capital_flows WHERE actor_id = ANY(:ids)").bindparams(
                ids=[test_actor_id, test_actor_id_b],
            ),
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
    explicit, backdated ``as_of`` — the exact signal compute_ttm's
    watermark tracks."""
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


def _current_watermark(engine: Engine) -> str:
    """A watermark timestamp guaranteed to be 'now' at test-data-seed
    time, expressed as an ISO string compute_ttm can bind."""
    with engine.connect() as conn:
        row = conn.execute(text("SELECT NOW()")).fetchone()
    return row[0].isoformat()


_QUARTERS = [date(2024, 3, 31), date(2024, 6, 30), date(2024, 9, 30), date(2024, 12, 31)]


def test_only_actor_with_row_newer_than_watermark_is_recomputed(
    pg_engine: Engine, test_actor_id: str, test_actor_id_b: str,
):
    """Two actors, each with a full 4-quarter trailing window. Actor A
    gets a NEW/CORRECTED row (fresh as_of) after the watermark; actor B's
    rows are all older than the watermark. Only A should get a TTM row —
    proving the watermark, not "does this actor happen to have 4
    quarters", gates the recompute."""
    amounts = [100.0, 110.0, 120.0, 130.0]
    # Both actors fully seeded, backdated well before the watermark.
    for fp, amt in zip(_QUARTERS, amounts):
        _insert_quarter_with_as_of(pg_engine, test_actor_id, fp, amt, as_of_days_ago=20)
        _insert_quarter_with_as_of(pg_engine, test_actor_id_b, fp, amt, as_of_days_ago=20)

    watermark = _current_watermark(pg_engine)

    # Only actor A gets a correction AFTER the watermark — same fiscal
    # period (Q4), corrected amount. This simulates a late correction to
    # an existing quarter, not a brand-new one.
    _insert_quarter_with_as_of(
        pg_engine, test_actor_id, date(2024, 12, 31), 135.0, as_of_days_ago=0,
    )

    compute_ttm(pg_engine, watermark)

    a_rows = _fetch_ttm_rows(pg_engine, test_actor_id)
    b_rows = _fetch_ttm_rows(pg_engine, test_actor_id_b)

    a_latest = [r for r in a_rows if r["fiscal_period"] == date(2024, 12, 31)]
    assert len(a_latest) == 1, f"actor A (row newer than watermark) must be recomputed, got {a_rows}"
    assert a_latest[0]["amount_usd"] == pytest.approx(100.0 + 110.0 + 120.0 + 135.0)

    assert b_rows == [], (
        f"actor B has no row newer than the watermark and must NOT be "
        f"recomputed, got {b_rows}"
    )


def test_written_ttm_value_equals_independent_computation(
    pg_engine: Engine, test_actor_id: str,
):
    """No watermark (first-ever run) -> full recompute; the written ttm
    amount must equal a straightforward independent sum of the four
    quarters, nothing derived from the SQL under test."""
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
    pg_engine: Engine, test_actor_id: str,
):
    """A call that fails mid-batch (forced here via a watermark value
    that fails CAST(... AS timestamptz) inside compute_ttm's own
    transaction) must leave NOTHING committed — no TTM row, and no
    residue that would corrupt a later, valid call. This is the PG-level
    proof of "a failure mid-batch leaves the watermark unchanged": there
    is nothing for the caller to have advanced past, because nothing
    committed."""
    amounts = [100.0, 110.0, 120.0, 130.0]
    for fp, amt in zip(_QUARTERS, amounts):
        _insert_quarter_with_as_of(pg_engine, test_actor_id, fp, amt, as_of_days_ago=0)

    with pytest.raises(SQLAlchemyError):
        compute_ttm(pg_engine, "not-a-valid-timestamp")

    # Nothing was written by the failed attempt.
    assert _fetch_ttm_rows(pg_engine, test_actor_id) == []

    # A subsequent call (full recompute) is unaffected by the earlier
    # failure — same data, correct result.
    compute_ttm(pg_engine, None)
    rows = _fetch_ttm_rows(pg_engine, test_actor_id)
    latest = [r for r in rows if r["fiscal_period"] == date(2024, 12, 31)]
    assert len(latest) == 1
    assert latest[0]["amount_usd"] == pytest.approx(sum(amounts))


def test_null_watermark_recomputes_actor_with_only_old_rows(
    pg_engine: Engine, test_actor_id: str,
):
    """watermark=None (no persisted watermark yet, e.g. first run after
    this deploy, or downtime longer than any fixed window) must recompute
    an actor whose rows are ALL old — the fixed TTM_LOOKBACK_DAYS=3
    regression this whole design replaces."""
    amounts = [100.0, 110.0, 120.0, 130.0]
    for fp, amt in zip(_QUARTERS, amounts):
        _insert_quarter_with_as_of(pg_engine, test_actor_id, fp, amt, as_of_days_ago=10)

    compute_ttm(pg_engine, None)

    rows = _fetch_ttm_rows(pg_engine, test_actor_id)
    latest = [r for r in rows if r["fiscal_period"] == date(2024, 12, 31)]
    assert len(latest) == 1, (
        f"an actor whose only rows are 10 days old must still be picked "
        f"up by a full (watermark=None) recompute, got {rows}"
    )
