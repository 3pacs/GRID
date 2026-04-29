"""Tests for intelligence/holder_deal_overlap.py.

Requires a live PostgreSQL with the ``capital_flows``,
``institutional_holdings``, and ``holder_deal_overlap`` tables.
Tests are skipped automatically if Postgres is not reachable or the
tables are missing (e.g. CI runs without the full migration set).

Each test uses unique ticker prefixes (``HDO_<uuid8>``) and filer
prefixes so rows can't collide with production data. An autouse
cleanup fixture wipes every row the test wrote when it exits.
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.holder_deal_overlap import (
    MIN_POSITION_USD,
    OverlapRow,
    detect_overlap_for_deal,
    fetch_overlaps_for_actor,
    find_deals,
    run,
    upsert_rows,
)


# ── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def live_engine() -> Engine:
    """Real Postgres engine. Skips if the full schema is missing."""
    try:
        from db import get_engine
        engine = get_engine()
        with engine.connect() as conn:
            for t in (
                "public.capital_flows",
                "public.institutional_holdings",
                "public.holder_deal_overlap",
            ):
                row = conn.execute(
                    text("SELECT to_regclass(:n)").bindparams(n=t)
                ).fetchone()
                if not row or not row[0]:
                    pytest.skip(f"missing table: {t}")
    except Exception as exc:
        pytest.skip(f"Postgres not available: {exc}")
    return engine


@pytest.fixture
def ticker_prefix() -> str:
    """Short unique prefix for this test's synthetic tickers."""
    return f"HDO{uuid.uuid4().hex[:6].upper()}"


@pytest.fixture(autouse=True)
def cleanup_test_rows(live_engine: Engine, ticker_prefix: str):
    """Delete everything this test wrote on teardown."""
    yield
    with live_engine.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM holder_deal_overlap "
                "WHERE acquirer_ticker LIKE :p OR target_ticker LIKE :p"
            ).bindparams(p=f"{ticker_prefix}%")
        )
        conn.execute(
            text(
                "DELETE FROM institutional_holdings "
                "WHERE ticker LIKE :p OR holder_name LIKE :p"
            ).bindparams(p=f"{ticker_prefix}%")
        )
        conn.execute(
            text(
                "DELETE FROM capital_flows "
                "WHERE actor_id LIKE :p OR counterparty_id LIKE :p"
            ).bindparams(p=f"{ticker_prefix}%")
        )


# ── Helpers ──────────────────────────────────────────────────────────


def _insert_holding(
    engine: Engine,
    *,
    ticker: str,
    holder: str,
    report_date: date,
    value_usd: float,
    shares: int = 1000,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO institutional_holdings
                    (holder_name, ticker, shares_held, value_usd,
                     report_date, filed_date, source)
                VALUES (:h, :t, :s, :v, :rd, :rd, 'test_hdo')
                ON CONFLICT (holder_name, ticker, report_date)
                DO UPDATE SET shares_held = EXCLUDED.shares_held,
                              value_usd   = EXCLUDED.value_usd
                """
            ).bindparams(
                h=holder, t=ticker, s=shares, v=value_usd, rd=report_date
            )
        )


def _insert_deal_announcement(
    engine: Engine,
    *,
    acquirer: str,
    target: str,
    announcement_date: date,
    amount_usd: float = 1_000_000_000.0,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO capital_flows (
                    actor_id, fiscal_period, period_type, flow_type,
                    direction, amount_usd, counterparty_id,
                    source_filing, confidence, currency, as_of
                ) VALUES (
                    :a, :fp, 'announcement', 'acquisitions', 'out',
                    :amt, :cp, :sf, 'derived', 'USD', NOW()
                )
                ON CONFLICT (
                    actor_id, fiscal_period, period_type, flow_type,
                    (COALESCE(NULLIF(counterparty_id,''), '__none__')),
                    source_filing
                ) DO UPDATE SET amount_usd = EXCLUDED.amount_usd
                """
            ).bindparams(
                a=acquirer, fp=announcement_date, amt=amount_usd,
                cp=target, sf=f"test_hdo_{acquirer}",
            )
        )


# ── Tests ────────────────────────────────────────────────────────────


def test_find_deals_picks_up_new_announcement(
    live_engine: Engine, ticker_prefix: str
) -> None:
    """find_deals returns acquisition announcements with non-null targets."""
    acq = f"{ticker_prefix}A"
    tgt = f"{ticker_prefix}T"
    ann = date(2025, 6, 15)
    _insert_deal_announcement(
        live_engine, acquirer=acq, target=tgt, announcement_date=ann
    )

    deals = find_deals(live_engine)
    match = [d for d in deals if d["acquirer_ticker"] == acq]
    assert len(match) == 1
    assert match[0]["target_ticker"] == tgt
    assert match[0]["announcement_date"] == ann
    assert match[0]["deal_size_usd"] == 1_000_000_000.0


def test_detect_overlap_flags_material_pre_position(
    live_engine: Engine, ticker_prefix: str
) -> None:
    """Filer holding both legs above the floor gets pre_position_flag=True."""
    acq = f"{ticker_prefix}A"
    tgt = f"{ticker_prefix}T"
    filer = f"{ticker_prefix}_FUND_ALPHA"
    ann = date(2025, 6, 15)
    rd = ann - timedelta(days=50)

    _insert_deal_announcement(
        live_engine, acquirer=acq, target=tgt, announcement_date=ann
    )
    _insert_holding(
        live_engine, ticker=acq, holder=filer,
        report_date=rd, value_usd=10_000_000.0,
    )
    _insert_holding(
        live_engine, ticker=tgt, holder=filer,
        report_date=rd, value_usd=2_000_000.0,
    )

    rows = detect_overlap_for_deal(
        live_engine,
        announcement_date=ann,
        acquirer_ticker=acq,
        target_ticker=tgt,
    )
    assert len(rows) == 1
    row = rows[0]
    assert row.filer_name == filer
    assert row.pre_position_flag is True
    assert row.acquirer_position_value_usd == 10_000_000.0
    assert row.target_position_value_usd == 2_000_000.0
    assert row.days_before_announcement == 50
    assert row.holding_report_date == rd
    assert acq in row.narrative and tgt in row.narrative


def test_detect_overlap_skips_immaterial_weak_leg(
    live_engine: Engine, ticker_prefix: str
) -> None:
    """When the weaker leg is below the floor, pre_position_flag=False."""
    acq = f"{ticker_prefix}A"
    tgt = f"{ticker_prefix}T"
    filer = f"{ticker_prefix}_DUSTY_FUND"
    ann = date(2025, 6, 15)
    rd = ann - timedelta(days=30)

    _insert_deal_announcement(
        live_engine, acquirer=acq, target=tgt, announcement_date=ann
    )
    _insert_holding(
        live_engine, ticker=acq, holder=filer,
        report_date=rd, value_usd=50_000_000.0,
    )
    # Weaker leg $100k << $500k floor.
    _insert_holding(
        live_engine, ticker=tgt, holder=filer,
        report_date=rd, value_usd=100_000.0,
    )

    rows = detect_overlap_for_deal(
        live_engine,
        announcement_date=ann,
        acquirer_ticker=acq,
        target_ticker=tgt,
        min_position_usd=MIN_POSITION_USD,
    )
    assert len(rows) == 1
    assert rows[0].pre_position_flag is False
    # Below-threshold overlaps do not trigger the quick-exit probe.
    assert rows[0].quick_exit_flag is False


def test_quick_exit_flag_set_when_next_13f_liquidated(
    live_engine: Engine, ticker_prefix: str
) -> None:
    """Filer who sold the target in the next 13F gets quick_exit_flag=True."""
    acq = f"{ticker_prefix}A"
    tgt = f"{ticker_prefix}T"
    filer = f"{ticker_prefix}_FAST_MONEY"
    ann = date(2025, 6, 15)
    pre_rd = ann - timedelta(days=40)
    post_rd = ann + timedelta(days=60)

    _insert_deal_announcement(
        live_engine, acquirer=acq, target=tgt, announcement_date=ann
    )
    _insert_holding(
        live_engine, ticker=acq, holder=filer,
        report_date=pre_rd, value_usd=5_000_000.0,
    )
    _insert_holding(
        live_engine, ticker=tgt, holder=filer,
        report_date=pre_rd, value_usd=3_000_000.0,
    )
    # Next quarter: acquirer position unchanged, target position GONE
    # (another filer-ticker row exists so next_reports returns a date).
    _insert_holding(
        live_engine,
        ticker=tgt,
        holder=f"{ticker_prefix}_OTHER_HOLDER",
        report_date=post_rd,
        value_usd=1_000_000.0,
    )

    rows = detect_overlap_for_deal(
        live_engine,
        announcement_date=ann,
        acquirer_ticker=acq,
        target_ticker=tgt,
    )
    target_row = next(r for r in rows if r.filer_name == filer)
    assert target_row.pre_position_flag is True
    assert target_row.quick_exit_flag is True
    assert "QUICK EXIT" in target_row.narrative


def test_run_upserts_and_fetch_overlaps_for_actor(
    live_engine: Engine, ticker_prefix: str
) -> None:
    """End-to-end: ``run`` writes rows, ``fetch_overlaps_for_actor`` reads them."""
    acq = f"{ticker_prefix}A"
    tgt = f"{ticker_prefix}T"
    filer = f"{ticker_prefix}_WHALE"
    ann = date(2025, 6, 15)
    rd = ann - timedelta(days=20)

    _insert_deal_announcement(
        live_engine, acquirer=acq, target=tgt, announcement_date=ann
    )
    _insert_holding(
        live_engine, ticker=acq, holder=filer,
        report_date=rd, value_usd=25_000_000.0,
    )
    _insert_holding(
        live_engine, ticker=tgt, holder=filer,
        report_date=rd, value_usd=8_000_000.0,
    )

    stats = run(live_engine)
    assert stats["deals_scanned"] >= 1
    assert stats["overlaps_written"] >= 1
    assert stats["pre_positioned"] >= 1

    # Idempotent: second run reuses the unique-key upsert.
    stats2 = run(live_engine)
    assert stats2["deals_scanned"] >= 1

    # Fetch helper surfaces the pre-positioned row on either leg.
    by_acq = fetch_overlaps_for_actor(live_engine, acq)
    assert any(r["filer_name"] == filer for r in by_acq)
    by_tgt = fetch_overlaps_for_actor(live_engine, tgt)
    assert any(r["filer_name"] == filer for r in by_tgt)

    # Unique constraint holds: only one persisted row per (deal, filer).
    with live_engine.connect() as conn:
        n = conn.execute(
            text(
                "SELECT COUNT(*) FROM holder_deal_overlap "
                "WHERE acquirer_ticker = :a AND target_ticker = :t "
                "AND filer_name = :f"
            ).bindparams(a=acq, t=tgt, f=filer)
        ).scalar()
    assert n == 1


def test_upsert_rows_empty_list_returns_zero(live_engine: Engine) -> None:
    """Safety: empty input is a cheap no-op, not an error."""
    assert upsert_rows(live_engine, []) == 0


def test_overlap_row_dataclass_defaults() -> None:
    """OverlapRow defaults don't require a DB at all (unit-level sanity)."""
    r = OverlapRow(
        deal_announcement_date=date(2025, 1, 1),
        acquirer_ticker="AAA",
        target_ticker="BBB",
        filer_name="Fund X",
        acquirer_position_value_usd=None,
        target_position_value_usd=None,
        holding_report_date=None,
        days_before_announcement=None,
    )
    assert r.pre_position_flag is False
    assert r.quick_exit_flag is False
    assert r.narrative == ""
