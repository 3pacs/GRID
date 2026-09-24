"""Tests for Amendment 1's tested-wall computation.

`select_tested_walls` and `aggregate_per_strike_gex` are pure functions
(synthetic per-strike data / synthetic chain rows) — most of this file.
`compute_tested_walls_from_db`'s SQL query is exercised once against an
in-memory SQLite sandbox, the same dependency-free technique used for
`chain.py`'s PIT-safety query.
"""

from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace

import pytest
import sqlalchemy as sa

from paper_log.gex_levels.tested_walls import (
    WallSelection,
    aggregate_per_strike_gex,
    compute_tested_walls_from_db,
    select_tested_walls,
)

MIN_DIST = 0.005  # Amendment 1's 0.5%


# ── select_tested_walls (pure) ──────────────────────────────────────────


def test_picks_largest_magnitude_put_and_call_beyond_threshold() -> None:
    p0 = 500.0
    per_strike = {
        490.0: {"call_gex": 0.0, "put_gex": -1_000.0},  # 2% below P0
        480.0: {"call_gex": 0.0, "put_gex": -5_000.0},  # 4% below, larger magnitude -> wins
        510.0: {"call_gex": 2_000.0, "put_gex": 0.0},   # 2% above
        520.0: {"call_gex": 9_000.0, "put_gex": 0.0},   # 4% above, larger -> wins
    }
    walls = select_tested_walls(per_strike, p0, min_distance_pct=MIN_DIST)
    assert walls == WallSelection(put_wall=480.0, call_wall=520.0)


def test_atm_strike_excluded_even_with_largest_exposure() -> None:
    """The exact scenario the 2026-09-24 smoke check found: short-dated
    ATM gamma dominates both sides. The ATM strike must be excluded by the
    0.5% threshold even though it has the largest raw exposure."""
    p0 = 764.9
    per_strike = {
        765.0: {"call_gex": 50_000.0, "put_gex": -50_000.0},  # ATM, ~0.013% away -- excluded both sides
        740.0: {"call_gex": 0.0, "put_gex": -3_000.0},         # ~3.3% below -- qualifies
        790.0: {"call_gex": 4_000.0, "put_gex": 0.0},          # ~3.3% above -- qualifies
    }
    walls = select_tested_walls(per_strike, p0, min_distance_pct=MIN_DIST)
    assert walls.put_wall == 740.0
    assert walls.call_wall == 790.0


def test_missing_wall_when_no_strike_qualifies_on_a_side() -> None:
    p0 = 500.0
    per_strike = {
        498.0: {"call_gex": 0.0, "put_gex": -9_000.0},  # inside the 0.5% band -- doesn't qualify
        520.0: {"call_gex": 3_000.0, "put_gex": 0.0},   # qualifies
    }
    walls = select_tested_walls(per_strike, p0, min_distance_pct=MIN_DIST)
    assert walls.put_wall is None
    assert walls.call_wall == 520.0


def test_both_walls_missing_when_chain_is_entirely_near_the_money() -> None:
    p0 = 500.0
    per_strike = {499.0: {"call_gex": 100.0, "put_gex": -100.0}}
    walls = select_tested_walls(per_strike, p0, min_distance_pct=MIN_DIST)
    assert walls == WallSelection(put_wall=None, call_wall=None)


def test_ties_go_to_the_strike_closer_to_p0() -> None:
    p0 = 500.0
    per_strike = {
        480.0: {"call_gex": 0.0, "put_gex": -5_000.0},  # 20 away from P0
        470.0: {"call_gex": 0.0, "put_gex": -5_000.0},  # tied magnitude, 30 away -- loses
        510.0: {"call_gex": 5_000.0, "put_gex": 0.0},   # 10 away
        525.0: {"call_gex": 5_000.0, "put_gex": 0.0},   # tied magnitude, 25 away -- loses
    }
    walls = select_tested_walls(per_strike, p0, min_distance_pct=MIN_DIST)
    assert walls.put_wall == 480.0
    assert walls.call_wall == 510.0


def test_threshold_boundary_is_inclusive() -> None:
    p0 = 1000.0
    # Exactly 0.5% away on each side -- "at or below" / "at or above".
    per_strike = {
        995.0: {"call_gex": 0.0, "put_gex": -1.0},
        1005.0: {"call_gex": 1.0, "put_gex": 0.0},
    }
    walls = select_tested_walls(per_strike, p0, min_distance_pct=MIN_DIST)
    assert walls == WallSelection(put_wall=995.0, call_wall=1005.0)


def test_no_candidates_at_all() -> None:
    walls = select_tested_walls({}, 500.0, min_distance_pct=MIN_DIST)
    assert walls == WallSelection(put_wall=None, call_wall=None)


# ── aggregate_per_strike_gex (pure) ─────────────────────────────────────


SNAP_DATE = date(2026, 9, 24)


def _row(strike: float, opt_type: str, oi: float, iv: float, dte_days: int) -> SimpleNamespace:
    return SimpleNamespace(
        strike=strike, opt_type=opt_type, open_interest=oi, implied_volatility=iv,
        expiry=SNAP_DATE + timedelta(days=dte_days),
    )


def test_aggregate_signs_calls_positive_and_puts_negative() -> None:
    rows = [
        _row(510.0, "call", 1000.0, 0.20, 30),
        _row(490.0, "put", 1000.0, 0.20, 30),
    ]
    per_strike = aggregate_per_strike_gex(rows, spot=500.0, snap_date=SNAP_DATE)
    assert per_strike[510.0]["call_gex"] > 0
    assert per_strike[510.0]["put_gex"] == 0.0
    assert per_strike[490.0]["put_gex"] < 0
    assert per_strike[490.0]["call_gex"] == 0.0


def test_aggregate_combines_multiple_expiries_at_the_same_strike() -> None:
    rows = [
        _row(510.0, "call", 1000.0, 0.20, 30),
        _row(510.0, "call", 500.0, 0.20, 60),  # same strike, different expiry
    ]
    per_strike = aggregate_per_strike_gex(rows, spot=500.0, snap_date=SNAP_DATE)
    single = aggregate_per_strike_gex([_row(510.0, "call", 1000.0, 0.20, 30)], spot=500.0, snap_date=SNAP_DATE)
    assert per_strike[510.0]["call_gex"] > single[510.0]["call_gex"]


def test_aggregate_skips_non_positive_dte() -> None:
    rows = [_row(510.0, "call", 1000.0, 0.20, 0), _row(510.0, "call", 1000.0, 0.20, -5)]
    per_strike = aggregate_per_strike_gex(rows, spot=500.0, snap_date=SNAP_DATE)
    assert per_strike == {}


def test_aggregate_defaults_zero_iv_to_25_percent() -> None:
    zero_iv = aggregate_per_strike_gex([_row(510.0, "call", 1000.0, 0.0, 30)], spot=500.0, snap_date=SNAP_DATE)
    default_iv = aggregate_per_strike_gex([_row(510.0, "call", 1000.0, 0.25, 30)], spot=500.0, snap_date=SNAP_DATE)
    assert zero_iv[510.0]["call_gex"] == pytest.approx(default_iv[510.0]["call_gex"])


# ── compute_tested_walls_from_db (SQL integration, in-memory SQLite) ────


@pytest.fixture
def sqlite_engine():
    engine = sa.create_engine("sqlite:///:memory:")
    meta = sa.MetaData()
    sa.Table(
        "options_snapshots", meta,
        sa.Column("ticker", sa.Text),
        sa.Column("snap_date", sa.Date),
        sa.Column("expiry", sa.Date),
        sa.Column("strike", sa.Float),
        sa.Column("opt_type", sa.Text),
        sa.Column("open_interest", sa.Integer),
        sa.Column("implied_vol", sa.Float),
    )
    meta.create_all(engine)
    try:
        yield engine
    finally:
        engine.dispose()


def _insert(engine, rows: list[dict]) -> None:
    with engine.begin() as conn:
        conn.execute(sa.text(
            "INSERT INTO options_snapshots (ticker, snap_date, expiry, strike, opt_type, open_interest, implied_vol) "
            "VALUES (:ticker, :snap_date, :expiry, :strike, :opt_type, :open_interest, :implied_vol)"
        ), rows)


def test_compute_tested_walls_from_db_end_to_end(sqlite_engine) -> None:
    snap_date = date(2026, 9, 24)
    expiry = snap_date + timedelta(days=30)
    _insert(sqlite_engine, [
        {"ticker": "SPY", "snap_date": snap_date, "expiry": expiry, "strike": 765.0,
         "opt_type": "call", "open_interest": 50000, "implied_vol": 0.15},
        {"ticker": "SPY", "snap_date": snap_date, "expiry": expiry, "strike": 765.0,
         "opt_type": "put", "open_interest": 50000, "implied_vol": 0.15},
        {"ticker": "SPY", "snap_date": snap_date, "expiry": expiry, "strike": 740.0,
         "opt_type": "put", "open_interest": 5000, "implied_vol": 0.20},
        {"ticker": "SPY", "snap_date": snap_date, "expiry": expiry, "strike": 790.0,
         "opt_type": "call", "open_interest": 4000, "implied_vol": 0.20},
        # a different ticker must never leak in
        {"ticker": "QQQ", "snap_date": snap_date, "expiry": expiry, "strike": 400.0,
         "opt_type": "call", "open_interest": 100000, "implied_vol": 0.20},
    ])

    walls = compute_tested_walls_from_db(
        sqlite_engine, "SPY", snap_date, spot=764.9, p0=764.9, min_distance_pct=MIN_DIST,
    )
    assert walls.put_wall == 740.0
    assert walls.call_wall == 790.0


def test_compute_tested_walls_from_db_filters_zero_oi_and_iv(sqlite_engine) -> None:
    snap_date = date(2026, 9, 24)
    expiry = snap_date + timedelta(days=30)
    _insert(sqlite_engine, [
        {"ticker": "SPY", "snap_date": snap_date, "expiry": expiry, "strike": 740.0,
         "opt_type": "put", "open_interest": 0, "implied_vol": 0.20},  # zero OI -- excluded
        {"ticker": "SPY", "snap_date": snap_date, "expiry": expiry, "strike": 741.0,
         "opt_type": "put", "open_interest": 100, "implied_vol": 0.0},  # zero IV -- excluded
    ])
    walls = compute_tested_walls_from_db(
        sqlite_engine, "SPY", snap_date, spot=764.9, p0=764.9, min_distance_pct=MIN_DIST,
    )
    assert walls.put_wall is None
