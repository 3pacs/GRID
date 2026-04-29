"""Tests for intelligence/capital_flow_rollups.py.

These tests need real PostgreSQL because the rollup module relies on
window functions, ``ON CONFLICT`` against an expression-based unique
index, and the PG-specific ``date_trunc`` semantics. They are skipped
automatically if no test PG instance is reachable (see ``conftest.py``
``pg_engine`` fixture).

Each test inserts rows with a unique actor_id prefix
(``rollup_test_<uuid>``), runs the rollups, asserts on the derived
output, and cleans up. Production data is left untouched.
"""

from __future__ import annotations

import uuid
from datetime import date

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.company_financial_rollups import (
    ROLLED_SOURCE_FILING,
    TTM_SOURCE_FILING,
    compute_ttm,
    fold_announcements,
    run_all,
)


# ── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def pg_engine() -> Engine:
    """Connect to the live griddb via the project's get_engine() helper.

    Tests are skipped if no Postgres is reachable or the
    ``capital_flows`` table is missing (e.g. local dev box without the
    full migration set applied).
    """
    try:
        from db import get_engine
        engine = get_engine()
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT to_regclass('public.capital_flows')"),
            ).fetchone()
            if not row or not row[0]:
                pytest.skip("capital_flows table missing")
    except Exception as exc:
        pytest.skip(f"Postgres not available: {exc}")
    return engine


@pytest.fixture
def test_actor_id() -> str:
    """Unique actor_id per test so rows can't collide with prod data."""
    return f"rollup_test_{uuid.uuid4().hex[:10]}"


@pytest.fixture(autouse=True)
def cleanup_test_rows(pg_engine: Engine, test_actor_id: str):
    """Delete any rows we wrote when the test exits."""
    yield
    with pg_engine.begin() as conn:
        conn.execute(
            text("DELETE FROM capital_flows WHERE actor_id = :a").bindparams(
                a=test_actor_id,
            ),
        )


def _insert_quarter(
    engine: Engine,
    actor_id: str,
    fp: date,
    flow_type: str,
    amount: float,
    *,
    direction: str = "in",
    counterparty: str | None = None,
    source_filing: str = "10-Q test",
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO capital_flows (
                    actor_id, fiscal_period, period_type, flow_type,
                    direction, amount_usd, counterparty_id, source_filing,
                    confidence, currency, as_of
                ) VALUES (
                    :a, :fp, 'quarter', :ft, :d, :amt, :cp, :sf,
                    'confirmed', 'USD', NOW()
                )
                ON CONFLICT (
                    actor_id, fiscal_period, period_type, flow_type,
                    (COALESCE(NULLIF(counterparty_id,''), '__none__')),
                    source_filing
                ) DO UPDATE SET amount_usd = EXCLUDED.amount_usd
                """,
            ).bindparams(
                a=actor_id, fp=fp, ft=flow_type, d=direction,
                amt=amount, cp=counterparty, sf=source_filing,
            ),
        )


def _insert_announcement(
    engine: Engine,
    actor_id: str,
    fp: date,
    flow_type: str,
    amount: float,
    *,
    counterparty: str | None = None,
    source_filing: str = "8-K test",
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO capital_flows (
                    actor_id, fiscal_period, period_type, flow_type,
                    direction, amount_usd, counterparty_id, source_filing,
                    confidence, currency, as_of
                ) VALUES (
                    :a, :fp, 'announcement', :ft, 'out', :amt, :cp, :sf,
                    'derived', 'USD', NOW()
                )
                ON CONFLICT (
                    actor_id, fiscal_period, period_type, flow_type,
                    (COALESCE(NULLIF(counterparty_id,''), '__none__')),
                    source_filing
                ) DO UPDATE SET amount_usd = EXCLUDED.amount_usd
                """,
            ).bindparams(
                a=actor_id, fp=fp, ft=flow_type, amt=amount,
                cp=counterparty, sf=source_filing,
            ),
        )


def _insert_annual(
    engine: Engine,
    actor_id: str,
    fp: date,
    flow_type: str,
    amount: float,
    *,
    counterparty: str | None = None,
    source_filing: str = "10-K test",
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO capital_flows (
                    actor_id, fiscal_period, period_type, flow_type,
                    direction, amount_usd, counterparty_id, source_filing,
                    confidence, currency, as_of
                ) VALUES (
                    :a, :fp, 'annual', :ft, 'out', :amt, :cp, :sf,
                    'confirmed', 'USD', NOW()
                )
                ON CONFLICT (
                    actor_id, fiscal_period, period_type, flow_type,
                    (COALESCE(NULLIF(counterparty_id,''), '__none__')),
                    source_filing
                ) DO UPDATE SET amount_usd = EXCLUDED.amount_usd
                """,
            ).bindparams(
                a=actor_id, fp=fp, ft=flow_type, amt=amount,
                cp=counterparty, sf=source_filing,
            ),
        )


def _fetch_ttm_rows(
    engine: Engine,
    actor_id: str,
    flow_type: str | None = None,
) -> list[dict]:
    sql = (
        "SELECT fiscal_period, flow_type, amount_usd, source_filing "
        "FROM capital_flows "
        "WHERE actor_id = :a AND period_type = 'ttm' "
        "AND source_filing = :sf "
    )
    params: dict = {"a": actor_id, "sf": TTM_SOURCE_FILING}
    if flow_type is not None:
        sql += "AND flow_type = :ft "
        params["ft"] = flow_type
    sql += "ORDER BY fiscal_period DESC"
    with engine.connect() as conn:
        rows = conn.execute(text(sql).bindparams(**params)).fetchall()
    return [
        {
            "fiscal_period": r[0],
            "flow_type": r[1],
            "amount_usd": float(r[2]) if r[2] is not None else None,
            "source_filing": r[3],
        }
        for r in rows
    ]


def _fetch_rolled_rows(
    engine: Engine,
    actor_id: str,
    flow_type: str | None = None,
) -> list[dict]:
    sql = (
        "SELECT fiscal_period, flow_type, amount_usd, counterparty_id "
        "FROM capital_flows "
        "WHERE actor_id = :a AND period_type = 'annual' "
        "AND source_filing = :sf "
    )
    params: dict = {"a": actor_id, "sf": ROLLED_SOURCE_FILING}
    if flow_type is not None:
        sql += "AND flow_type = :ft "
        params["ft"] = flow_type
    sql += "ORDER BY fiscal_period DESC"
    with engine.connect() as conn:
        rows = conn.execute(text(sql).bindparams(**params)).fetchall()
    return [
        {
            "fiscal_period": r[0],
            "flow_type": r[1],
            "amount_usd": float(r[2]) if r[2] is not None else None,
            "counterparty_id": r[3],
        }
        for r in rows
    ]


# ── Tests ────────────────────────────────────────────────────────────


def test_compute_ttm_sums_four_quarters(pg_engine: Engine, test_actor_id: str):
    """Four quarterly rows → one TTM row with the correct sum."""
    quarters = [
        (date(2024, 3, 31), 100.0),
        (date(2024, 6, 30), 110.0),
        (date(2024, 9, 30), 120.0),
        (date(2024, 12, 31), 130.0),
    ]
    for fp, amt in quarters:
        _insert_quarter(pg_engine, test_actor_id, fp, "revenue", amt)

    compute_ttm(pg_engine)

    rows = _fetch_ttm_rows(pg_engine, test_actor_id, "revenue")
    # We get a TTM row for each quarter that has 4 trailing quarters
    # available. Only the latest (Dec 31 2024) has all four.
    latest = [r for r in rows if r["fiscal_period"] == date(2024, 12, 31)]
    assert len(latest) == 1, f"expected 1 TTM row at 2024-Q4, got {rows}"
    assert latest[0]["amount_usd"] == pytest.approx(460.0)
    assert latest[0]["source_filing"] == TTM_SOURCE_FILING


def test_compute_ttm_skips_when_under_four_quarters(
    pg_engine: Engine, test_actor_id: str,
):
    """Only 3 quarters available → no TTM row written."""
    _insert_quarter(pg_engine, test_actor_id, date(2024, 3, 31), "revenue", 100.0)
    _insert_quarter(pg_engine, test_actor_id, date(2024, 6, 30), "revenue", 110.0)
    _insert_quarter(pg_engine, test_actor_id, date(2024, 9, 30), "revenue", 120.0)

    compute_ttm(pg_engine)

    rows = _fetch_ttm_rows(pg_engine, test_actor_id, "revenue")
    assert rows == [], f"expected no TTM rows, got {rows}"


def test_fold_announcements_creates_rolled_annual(
    pg_engine: Engine, test_actor_id: str,
):
    """Announcement row → annual_rolled row in the matching fiscal year."""
    _insert_announcement(
        pg_engine,
        test_actor_id,
        date(2022, 1, 18),
        "acquisitions",
        68_700_000_000.0,
        counterparty="atvi",
    )

    fold_announcements(pg_engine)

    rows = _fetch_rolled_rows(pg_engine, test_actor_id, "acquisitions")
    assert len(rows) == 1, f"expected 1 rolled row, got {rows}"
    assert rows[0]["amount_usd"] == pytest.approx(68_700_000_000.0)
    assert rows[0]["counterparty_id"] == "atvi"
    # Year-end date should be Dec 31 of the announcement year.
    assert rows[0]["fiscal_period"] == date(2022, 12, 31)


def test_idempotency_double_run(pg_engine: Engine, test_actor_id: str):
    """Running the rollups twice does not duplicate or double-count rows."""
    quarters = [
        (date(2024, 3, 31), 100.0),
        (date(2024, 6, 30), 110.0),
        (date(2024, 9, 30), 120.0),
        (date(2024, 12, 31), 130.0),
    ]
    for fp, amt in quarters:
        _insert_quarter(pg_engine, test_actor_id, fp, "revenue", amt)

    _insert_announcement(
        pg_engine,
        test_actor_id,
        date(2023, 5, 10),
        "acquisitions",
        5_000_000_000.0,
        counterparty="targetco",
    )

    run_all(pg_engine)
    run_all(pg_engine)

    ttm_rows = _fetch_ttm_rows(pg_engine, test_actor_id, "revenue")
    latest_ttm = [r for r in ttm_rows if r["fiscal_period"] == date(2024, 12, 31)]
    assert len(latest_ttm) == 1
    assert latest_ttm[0]["amount_usd"] == pytest.approx(460.0)

    rolled = _fetch_rolled_rows(pg_engine, test_actor_id, "acquisitions")
    assert len(rolled) == 1
    assert rolled[0]["amount_usd"] == pytest.approx(5_000_000_000.0)


def test_fold_does_not_duplicate_existing_annual(
    pg_engine: Engine, test_actor_id: str,
):
    """A real 10-K annual row coexists with the rolled row but each is
    keyed by source_filing — they should NOT collapse into one another
    and the rolled row should not appear in the 10-K row's slot.

    The dedup CTE in api/routers/capital_flow.py picks the SEC row over
    the rolled row at read time, so the consumer never double-counts.
    """
    # Real 10-K row for FY2022
    _insert_annual(
        pg_engine,
        test_actor_id,
        date(2022, 12, 31),
        "acquisitions",
        70_000_000_000.0,
        counterparty="atvi",
        source_filing="10-K test",
    )
    # M&A announcement in same year, same counterparty
    _insert_announcement(
        pg_engine,
        test_actor_id,
        date(2022, 1, 18),
        "acquisitions",
        68_700_000_000.0,
        counterparty="atvi",
    )

    fold_announcements(pg_engine)

    # Both rows should exist independently
    with pg_engine.connect() as conn:
        all_annual = conn.execute(
            text(
                "SELECT source_filing, amount_usd FROM capital_flows "
                "WHERE actor_id = :a AND fiscal_period = :fp "
                "AND period_type = 'annual' AND flow_type = 'acquisitions' "
                "ORDER BY source_filing"
            ).bindparams(a=test_actor_id, fp=date(2022, 12, 31)),
        ).fetchall()

    sources = {r[0]: float(r[1]) for r in all_annual}
    assert "10-K test" in sources
    assert ROLLED_SOURCE_FILING in sources
    assert sources["10-K test"] == pytest.approx(70_000_000_000.0)
    assert sources[ROLLED_SOURCE_FILING] == pytest.approx(68_700_000_000.0)
    # Crucially: no third row, no doubling
    assert len(all_annual) == 2


def test_fold_aggregates_multiple_announcements_in_same_year(
    pg_engine: Engine, test_actor_id: str,
):
    """Two announcements in the same fiscal year sum into one rolled row."""
    _insert_announcement(
        pg_engine, test_actor_id, date(2023, 3, 10),
        "buybacks", 1_000_000_000.0, counterparty=None,
        source_filing="8-K mar",
    )
    _insert_announcement(
        pg_engine, test_actor_id, date(2023, 9, 22),
        "buybacks", 2_500_000_000.0, counterparty=None,
        source_filing="8-K sep",
    )

    fold_announcements(pg_engine)

    rows = _fetch_rolled_rows(pg_engine, test_actor_id, "buybacks")
    assert len(rows) == 1
    assert rows[0]["amount_usd"] == pytest.approx(3_500_000_000.0)
    assert rows[0]["fiscal_period"] == date(2023, 12, 31)


def test_run_all_returns_stats(pg_engine: Engine, test_actor_id: str):
    """run_all returns a stats dict with both row counts."""
    _insert_announcement(
        pg_engine, test_actor_id, date(2023, 5, 10),
        "dividends", 100_000_000.0,
    )
    stats = run_all(pg_engine)
    assert "ttm_rows" in stats
    assert "rolled_rows" in stats
    assert stats["rolled_rows"] >= 1
    assert "completed_at" in stats
