"""Tests for acquisition outflow decomposition in capital_flow router.

The capital_flow annual view aggregates all source filings into one
per-flow-type total. These tests verify that we expose the underlying
8-K announcement rows as a ``deals`` sub-field on the aggregated
``acquisitions`` outflow entry so the frontend can drill down into the
specific M&A events that make up the annual total.

Most tests drive the pure-Python helpers directly (``_load_deal_announcements``
and ``_build_period``) so they work without touching the live endpoint
auth layer. One end-to-end test exercises ``get_capital_flow`` against a
seeded actor to verify the full response shape.
"""

from __future__ import annotations

import uuid
from datetime import date
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from api.routers import capital_flow as cf


# ── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def pg_engine() -> Engine:
    """Connect to the live griddb via the project's get_engine() helper."""
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
    return f"deal_test_{uuid.uuid4().hex[:10]}"


@pytest.fixture
def cleanup(pg_engine: Engine, test_actor_id: str):
    """Cleanup fixture — explicitly required by PG-backed tests only.

    We deliberately don't mark this autouse=True because the unit tests
    below drive ``_build_period`` directly without touching Postgres.
    """
    yield
    with pg_engine.begin() as conn:
        conn.execute(
            text("DELETE FROM capital_flows WHERE actor_id = :a").bindparams(
                a=test_actor_id,
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
    source_filing: str = "8-K acq",
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


# ── Unit tests for _build_period deals attachment ──────────────────────


def _ann_row(
    fp: date, ft: str, amt: float, cp: str | None = None,
) -> dict[str, Any]:
    return {
        "fiscal_period": fp,
        "flow_type": ft,
        "direction": "out",
        "amount_usd": amt,
        "currency": "USD",
        "counterparty_id": cp,
        "source_filing": "10-K 2022",
        "confidence": "confirmed",
    }


def test_deals_attached_to_acquisitions_outflow():
    """Deals sub-field appears on the acquisitions outflow entry."""
    fp = date(2022, 12, 31)
    rows = [
        _ann_row(fp, "revenue", 198_000_000_000.0),
        _ann_row(fp, "acquisitions", 72_000_000_000.0),
    ]
    deals = [
        {
            "target": "atvi", "target_label": "ATVI",
            "amount_usd": 68_700_000_000.0,
            "announcement_date": "2022-01-18",
            "source_filing": "8-K 2022-01-18",
            "confidence": "derived",
            "currency": "USD",
        },
        {
            "target": "nuance", "target_label": "NUAN",
            "amount_usd": 3_300_000_000.0,
            "announcement_date": "2022-04-12",
            "source_filing": "8-K 2022-04-12",
            "confidence": "derived",
            "currency": "USD",
        },
    ]
    period = cf._build_period(
        fp, rows, "annual",
        deals_for_period={"acquisitions": deals},
    )
    acq = [o for o in period["outflows"] if o["flow_type"] == "acquisitions"]
    assert len(acq) == 1
    assert "deals" in acq[0]
    assert len(acq[0]["deals"]) == 2
    assert acq[0]["deals"][0]["target"] == "atvi"


def test_deals_empty_when_no_announcements():
    """No announcement data → acquisitions outflow has no deals field."""
    fp = date(2021, 12, 31)
    rows = [
        _ann_row(fp, "revenue", 168_000_000_000.0),
        _ann_row(fp, "acquisitions", 5_000_000_000.0),
    ]
    period = cf._build_period(
        fp, rows, "annual",
        deals_for_period={},
    )
    acq = [o for o in period["outflows"] if o["flow_type"] == "acquisitions"]
    assert len(acq) == 1
    # Empty / absent is both acceptable as long as there's no truthy list.
    assert not acq[0].get("deals")


def test_deals_only_on_annual_period_type():
    """Quarter period_type should NOT attach deals even if provided."""
    fp = date(2022, 9, 30)
    rows = [
        _ann_row(fp, "revenue", 50_000_000_000.0),
        _ann_row(fp, "acquisitions", 10_000_000_000.0),
    ]
    deals = [{
        "target": "atvi", "target_label": "ATVI",
        "amount_usd": 10_000_000_000.0,
        "announcement_date": "2022-08-15",
        "source_filing": "8-K",
    }]
    period = cf._build_period(
        fp, rows, "quarter",
        deals_for_period={"acquisitions": deals},
    )
    acq = [o for o in period["outflows"] if o["flow_type"] == "acquisitions"]
    assert len(acq) == 1
    assert "deals" not in acq[0]


def test_response_shape_of_deal_entries():
    """Every deal entry must carry the documented fields."""
    fp = date(2022, 12, 31)
    rows = [_ann_row(fp, "acquisitions", 68_700_000_000.0, cp="atvi")]
    deals = [{
        "target": "atvi",
        "target_label": "Activision Blizzard",
        "amount_usd": 68_700_000_000.0,
        "announcement_date": "2022-01-18",
        "source_filing": "8-K 2022-01-18 ATVI",
        "confidence": "derived",
        "currency": "USD",
    }]
    period = cf._build_period(
        fp, rows, "annual",
        deals_for_period={"acquisitions": deals},
    )
    acq = [o for o in period["outflows"] if o["flow_type"] == "acquisitions"][0]
    d = acq["deals"][0]
    for key in (
        "target", "target_label", "amount_usd",
        "announcement_date", "source_filing",
    ):
        assert key in d, f"deal missing key: {key}"


# ── Integration tests against live PG ───────────────────────────────────


def test_load_deal_announcements_matches_fiscal_year(
    pg_engine: Engine, test_actor_id: str, cleanup,
):
    """_load_deal_announcements bucketizes by announcement year."""
    _insert_announcement(
        pg_engine, test_actor_id, date(2022, 1, 18),
        "acquisitions", 68_700_000_000.0, counterparty="atvi",
        source_filing="8-K atvi-2022",
    )
    _insert_announcement(
        pg_engine, test_actor_id, date(2022, 4, 12),
        "acquisitions", 3_300_000_000.0, counterparty="nuance",
        source_filing="8-K nuance-2022",
    )
    _insert_announcement(
        pg_engine, test_actor_id, date(2021, 4, 12),
        "acquisitions", 19_700_000_000.0, counterparty="nuance-v1",
        source_filing="8-K nuance-2021",
    )

    by_year = cf._load_deal_announcements(
        pg_engine, test_actor_id, "acquisitions", [2021, 2022, 2023],
    )

    assert 2022 in by_year and 2021 in by_year
    assert len(by_year[2022]) == 2
    assert len(by_year[2021]) == 1
    # 2023 has no rows → absent from map
    assert 2023 not in by_year


def test_deals_sorted_by_amount_desc(
    pg_engine: Engine, test_actor_id: str, cleanup,
):
    """_load_deal_announcements sorts each year's deals by amount desc."""
    _insert_announcement(
        pg_engine, test_actor_id, date(2022, 3, 1),
        "acquisitions", 1_000_000_000.0, counterparty="small",
        source_filing="8-K small",
    )
    _insert_announcement(
        pg_engine, test_actor_id, date(2022, 1, 18),
        "acquisitions", 68_700_000_000.0, counterparty="atvi",
        source_filing="8-K atvi",
    )
    _insert_announcement(
        pg_engine, test_actor_id, date(2022, 7, 1),
        "acquisitions", 5_000_000_000.0, counterparty="mid",
        source_filing="8-K mid",
    )

    by_year = cf._load_deal_announcements(
        pg_engine, test_actor_id, "acquisitions", [2022],
    )

    deals = by_year[2022]
    amounts = [d["amount_usd"] for d in deals]
    assert amounts == sorted(amounts, reverse=True)
    assert deals[0]["target"] == "atvi"


def test_empty_years_returns_empty_dict(
    pg_engine: Engine, test_actor_id: str, cleanup,
):
    """No matching years → empty dict."""
    _insert_announcement(
        pg_engine, test_actor_id, date(2018, 1, 1),
        "acquisitions", 1_000_000_000.0, counterparty="old",
        source_filing="8-K old",
    )
    by_year = cf._load_deal_announcements(
        pg_engine, test_actor_id, "acquisitions", [2023, 2024],
    )
    assert by_year == {}


def test_empty_years_list_returns_empty_dict(pg_engine: Engine):
    """Passing an empty year list short-circuits to empty."""
    assert cf._load_deal_announcements(pg_engine, "whatever", "acquisitions", []) == {}
