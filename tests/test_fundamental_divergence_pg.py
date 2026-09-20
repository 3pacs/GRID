"""PostgreSQL-backed validation of the batched raw_series/capital_flows
readers in intelligence/fundamental_divergence.py (fable-daily-intel-sql-
tasks, 2026-09-20 follow-up).

Uses the shared ``pg_engine`` fixture (tests/conftest.py — skips cleanly
if no Postgres is reachable, honours ``GRID_TEST_DB_URL``). Seeds
``public.raw_series``/``public.capital_flows`` with the real table shapes
(schema.sql) under unique, cleaned-up series_id/actor_id prefixes, and
calls the REAL ``_load_batch_price_cagrs`` / ``_load_batch_fundamentals``
— assertions are on behavior, never SQL text.

_load_batch_price_cagrs/_load_ticker_price_cagr are called with a
CONNECTION (``engine.connect()``), matching how compute_divergence calls
them — not with the engine itself.
"""
from __future__ import annotations

import uuid
from datetime import date, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.fundamental_divergence import (
    PRICE_LOOKBACK_DAYS,
    _load_batch_fundamentals,
    _load_batch_price_cagrs,
    _load_ticker_fundamentals,
    _load_ticker_price_cagr,
)

# ── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
def test_ticker() -> str:
    # Short, uppercase, alnum — mirrors a real ticker shape and keeps
    # the YF:{TICKER}:close series_id unique per test.
    return f"PG{uuid.uuid4().hex[:6].upper()}"


@pytest.fixture
def source_id(pg_engine: Engine) -> int:
    """A throwaway source_catalog row this test owns end-to-end — does
    not assume any pre-seeded 'FRED'/'yfinance' row exists in the
    disposable test database."""
    name = f"pg_test_source_{uuid.uuid4().hex[:10]}"
    with pg_engine.begin() as conn:
        row = conn.execute(
            text(
                """
                INSERT INTO source_catalog (
                    name, base_url, cost_tier, latency_class,
                    pit_available, revision_behavior, trust_score,
                    priority_rank, active
                ) VALUES (
                    :name, 'https://example.invalid', 'FREE', 'EOD',
                    FALSE, 'NEVER', 'MED', 999, TRUE
                )
                RETURNING id
                """,
            ).bindparams(name=name),
        ).fetchone()
        sid = int(row[0])
    yield sid
    with pg_engine.begin() as conn:
        conn.execute(text("DELETE FROM source_catalog WHERE id = :id").bindparams(id=sid))


@pytest.fixture(autouse=True)
def cleanup_raw_series(pg_engine: Engine, test_ticker: str):
    yield
    with pg_engine.begin() as conn:
        conn.execute(
            text("DELETE FROM raw_series WHERE series_id = :sid").bindparams(
                sid=f"YF:{test_ticker}:close",
            ),
        )


@pytest.fixture
def test_actor_id() -> str:
    return f"fd_pg_test_{uuid.uuid4().hex[:10]}"


@pytest.fixture(autouse=True)
def cleanup_capital_flows(pg_engine: Engine, test_actor_id: str):
    yield
    with pg_engine.begin() as conn:
        conn.execute(
            text("DELETE FROM capital_flows WHERE actor_id = :a").bindparams(
                a=test_actor_id,
            ),
        )


# ── Bulk price-history seeding ─────────────────────────────────────────
#
# MIN_PRICE_OBS (500) SUCCESS rows are required just to make a series
# eligible; PRICE_LOOKBACK_DAYS (3y = 1095 days) sets how far back the
# "prior" close is read from. Seed daily rows from (as_of - N_DAYS) to
# as_of so both floors are comfortably covered by ONE bulk INSERT
# (generate_series), not N_DAYS individual round trips.

_N_DAYS = PRICE_LOOKBACK_DAYS + 105  # > MIN_PRICE_OBS and > PRICE_LOOKBACK_DAYS
_DAILY_RATE = 0.0006  # deterministic growth: (1+_DAILY_RATE)^_N_DAYS gives latest/oldest ratio


def _seed_daily_success_rows(
    engine: Engine, ticker: str, source_id: int, as_of: date, *, n_days: int = _N_DAYS,
) -> None:
    """One bulk INSERT of n_days+1 SUCCESS rows, obs_date = as_of - i for
    i in [0, n_days], value = 100 * (1+_DAILY_RATE)^(n_days - i) — value
    increases monotonically as obs_date approaches as_of. pull_timestamp
    is backdated 1h so later individually-inserted rows (FAILED rows /
    competing vintages) always sort as the MORE RECENT pull."""
    series_id = f"YF:{ticker}:close"
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO raw_series (
                    series_id, source_id, obs_date, pull_timestamp, value, pull_status
                )
                SELECT
                    :sid, :src,
                    (CAST(:as_of AS date) - i),
                    NOW() - INTERVAL '1 hour',
                    100.0 * POWER(1.0 + :rate, :n - i),
                    'SUCCESS'
                FROM generate_series(0, :n) AS i
                """,
            ).bindparams(
                sid=series_id, src=source_id, as_of=as_of, rate=_DAILY_RATE, n=n_days,
            ),
        )


def _expected_clean_cagr(n_days: int = _N_DAYS) -> float:
    """Independent computation matching _seed_daily_success_rows exactly:
    latest (i=0) = 100*(1+r)^n; prior (i=PRICE_LOOKBACK_DAYS) =
    100*(1+r)^(n-PRICE_LOOKBACK_DAYS); ratio = (1+r)^PRICE_LOOKBACK_DAYS."""
    ratio = (1.0 + _DAILY_RATE) ** PRICE_LOOKBACK_DAYS
    return ratio ** (1.0 / 3.0) - 1.0


def _insert_raw_series_row(
    engine: Engine,
    ticker: str,
    source_id: int,
    obs_date: date,
    value: float,
    *,
    pull_status: str = "SUCCESS",
    pull_timestamp_offset_s: float = 0.0,
) -> None:
    """A single, individually-timestamped row — used to inject a FAILED
    row or a competing (newer-pull) vintage on top of the bulk baseline.
    pull_timestamp_offset_s is relative to NOW(): 0 means "now" (newer
    than the bulk baseline's NOW() - 1h)."""
    series_id = f"YF:{ticker}:close"
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO raw_series (
                    series_id, source_id, obs_date, pull_timestamp, value, pull_status
                ) VALUES (
                    :sid, :src, :d,
                    NOW() + make_interval(secs => :off),
                    :val, :status
                )
                """,
            ).bindparams(
                sid=series_id, src=source_id, d=obs_date, off=pull_timestamp_offset_s,
                val=value, status=pull_status,
            ),
        )


# ── 1. Failed observations excluded ────────────────────────────────────


def test_failed_observations_excluded_from_cagr(
    pg_engine: Engine, test_ticker: str, source_id: int,
):
    as_of = date.today()
    _seed_daily_success_rows(pg_engine, test_ticker, source_id, as_of)

    target_prior = as_of - timedelta(days=PRICE_LOOKBACK_DAYS)
    # FAILED rows at exactly the "latest" and "prior" obs_dates, with a
    # wildly wrong value and a LATER pull_timestamp than the bulk SUCCESS
    # baseline -- if the pull_status filter were missing, these would win
    # the latest-pull tiebreak and corrupt the CAGR.
    _insert_raw_series_row(
        pg_engine, test_ticker, source_id, as_of, 999_999.0,
        pull_status="FAILED", pull_timestamp_offset_s=10,
    )
    _insert_raw_series_row(
        pg_engine, test_ticker, source_id, target_prior, 0.0000001,
        pull_status="FAILED", pull_timestamp_offset_s=10,
    )

    with pg_engine.connect() as conn:
        result = _load_batch_price_cagrs(conn, [test_ticker], as_of)

    cagr = result[test_ticker]
    assert cagr is not None
    assert cagr == pytest.approx(_expected_clean_cagr(), rel=1e-6), (
        "FAILED rows must be excluded -- the CAGR must equal the "
        "SUCCESS-only computation"
    )
    # Sanity: a computation that let the bogus FAILED value win would be
    # wildly different (many orders of magnitude off).
    assert cagr < 10.0


# ── 2. Measured zero treated as a real observation ─────────────────────


def test_measured_zero_treated_as_real_matches_per_ticker_function(
    pg_engine: Engine, test_ticker: str, source_id: int,
):
    """No FAILED rows in this dataset at all, so _load_ticker_price_cagr
    and _load_batch_price_cagrs must agree exactly (per the controller
    brief: "compare against the pre-batching per-ticker function's
    result on the same data")."""
    as_of = date.today()
    _seed_daily_success_rows(pg_engine, test_ticker, source_id, as_of)
    # Overwrite the LATEST close (obs_date == as_of) with a genuine,
    # measured zero -- via a newer-pull-timestamp SUCCESS row so it wins
    # the latest-pull tiebreak cleanly.
    _insert_raw_series_row(
        pg_engine, test_ticker, source_id, as_of, 0.0,
        pull_status="SUCCESS", pull_timestamp_offset_s=10,
    )

    with pg_engine.connect() as conn:
        batched = _load_batch_price_cagrs(conn, [test_ticker], as_of)[test_ticker]
        per_ticker = _load_ticker_price_cagr(conn, test_ticker, as_of)

    assert batched is not None
    # A measured zero latest close means CAGR == (0/prior)^(1/3) - 1 ==
    # -1.0 exactly -- NOT None (which is what "treated as missing" would
    # produce, since the code would then need to skip it and use an
    # older nonzero close instead).
    assert batched == pytest.approx(-1.0, abs=1e-9)
    assert per_ticker == pytest.approx(batched, rel=1e-9), (
        "the batched loader must match the pre-batching per-ticker "
        "function on the same measured-zero data"
    )


# ── 3. Competing vintages: latest pull wins ─────────────────────────────


def test_competing_vintages_latest_pull_wins_matches_per_ticker_function(
    pg_engine: Engine, test_ticker: str, source_id: int,
):
    """Two SUCCESS rows for the same obs_date (as_of), different
    pull_timestamp -- the newer pull's value must win, in BOTH the
    batched loader and the pre-batching per-ticker function (per the
    controller brief: "matching the per-ticker function")."""
    as_of = date.today()
    _seed_daily_success_rows(pg_engine, test_ticker, source_id, as_of)

    older_value = 111.11
    newer_value = 222.22
    # Older-vintage correction at the SAME obs_date as the bulk baseline's
    # own row -- pull_timestamp still after the bulk baseline's (-1h) but
    # before the "newer" row below.
    _insert_raw_series_row(
        pg_engine, test_ticker, source_id, as_of, older_value,
        pull_status="SUCCESS", pull_timestamp_offset_s=5,
    )
    _insert_raw_series_row(
        pg_engine, test_ticker, source_id, as_of, newer_value,
        pull_status="SUCCESS", pull_timestamp_offset_s=10,
    )

    with pg_engine.connect() as conn:
        batched = _load_batch_price_cagrs(conn, [test_ticker], as_of)[test_ticker]
        per_ticker = _load_ticker_price_cagr(conn, test_ticker, as_of)

    prior_val = 100.0 * (1.0 + _DAILY_RATE) ** (_N_DAYS - PRICE_LOOKBACK_DAYS)
    expected = (newer_value / prior_val) ** (1.0 / 3.0) - 1.0

    assert batched is not None
    assert batched == pytest.approx(expected, rel=1e-6), (
        "the LATEST pull (newer_value) must win, not the older vintage "
        "and not an arbitrary row"
    )
    assert per_ticker == pytest.approx(batched, rel=1e-6), (
        "the pre-batching per-ticker function must resolve the same "
        "competing-vintages tie the same way (deterministic "
        "pull_timestamp DESC tiebreak)"
    )


# ── Bonus: batched fundamentals loader matches the per-ticker one ──────


def _insert_annual_flow(
    engine: Engine, actor_id: str, fp: date, flow_type: str, amount: float,
    *, source_filing: str = "10-K test",
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
                    :a, :fp, 'annual', :ft, 'in', :amt, NULL, :sf,
                    'confirmed', 'USD', NOW()
                )
                ON CONFLICT (
                    actor_id, fiscal_period, period_type, flow_type,
                    (COALESCE(NULLIF(counterparty_id,''), '__none__')),
                    source_filing
                ) DO UPDATE SET amount_usd = EXCLUDED.amount_usd
                """,
            ).bindparams(a=actor_id, fp=fp, ft=flow_type, amt=amount, sf=source_filing),
        )


def test_batched_fundamentals_matches_per_ticker_function(
    pg_engine: Engine, test_actor_id: str,
):
    revenue = {
        date(2022, 12, 31): 1000.0,
        date(2023, 12, 31): 1100.0,
        date(2024, 12, 31): 1210.0,
        date(2025, 12, 31): 1331.0,
    }
    for fp, amt in revenue.items():
        _insert_annual_flow(pg_engine, test_actor_id, fp, "revenue", amt)

    with pg_engine.connect() as conn:
        # _load_batch_fundamentals requires its ticker list already
        # upper-cased (it joins on UPPER(actor_id) = ANY(:tickers) — see
        # its docstring); _load_ticker_fundamentals handles casing itself.
        batched = _load_batch_fundamentals(
            conn, [test_actor_id.upper()],
        )[test_actor_id.upper()]
        per_ticker = _load_ticker_fundamentals(conn, test_actor_id)

    assert batched is not None
    assert per_ticker is not None
    assert batched == per_ticker
    assert batched["revenue_cagr"] == pytest.approx(0.10, abs=1e-9)
