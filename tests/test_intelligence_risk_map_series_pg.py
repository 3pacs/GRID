"""``_build_risk_map`` reads its exact series through real SQL, past decoys.

The risk map once read its inputs with ``fr.name ILIKE :pattern ORDER BY
rs.obs_date DESC LIMIT 1``. On griddb (2026-11-23) each pattern matched
several features. HY came from whichever of hy_oas_spread,
hy_spread_3m_chg, hy_spread_proxy and hy_spread was newest. The TGA was
tga_drawdown (a 30-day change) or mortgage_30y. TED was a 2022 reading of a
discontinued series. VIX matched nothing.

This seeds the real inputs alongside the decoys from griddb, each decoy
dated *newer* than the real series. It adds a multi-vintage date, a
retracted vintage, a VIX holiday print and a vintage written after the
cutoff. The builder then runs on a scratch schema through the real
``store.pit.PITStore`` SQL, with the cutoff fixed at Monday 2026-11-23.
The checks:
- the decoys are never read;
- each NYSE session counts once, at its newest unretracted vintage held on
  the cutoff;
- HY / IG are in basis points;
- the month-back readings are anchored to each current print.

Runs on ``pg_engine`` (``GRID_TEST_DB_URL``) and skips without PostgreSQL.
"""

from __future__ import annotations

import sys
import types
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from ingestion.market_calendar import trading_days_between

pytestmark = pytest.mark.xdist_group("postgres")

_DDL = (
    "CREATE TABLE feature_registry (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
    """
    CREATE TABLE resolved_series (
        id                    BIGSERIAL PRIMARY KEY,
        feature_id            INTEGER NOT NULL REFERENCES feature_registry(id),
        obs_date              DATE NOT NULL,
        release_date          DATE NOT NULL,
        vintage_date          DATE NOT NULL,
        value                 DOUBLE PRECISION NOT NULL,
        source_priority_used  INTEGER NOT NULL DEFAULT 1
    )
    """,
    "CREATE UNIQUE INDEX uq_resolved_series_composite ON resolved_series (feature_id, obs_date, vintage_date)",
    """
    CREATE TABLE resolved_series_retractions (
        id            BIGSERIAL PRIMARY KEY,
        feature_id    INTEGER NOT NULL,
        obs_date      DATE NOT NULL,
        vintage_date  DATE NOT NULL,
        retracted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        reason        TEXT NOT NULL,
        run_tag       TEXT NOT NULL,
        UNIQUE (feature_id, obs_date, vintage_date)
    )
    """,
)

CUTOFF = date(2026, 11, 23)  # Monday
D = date
YEAR = trading_days_between(CUTOFF - timedelta(days=364), CUTOFF)

# (obs_date, vintage_date, value). release_date = vintage_date.
REAL = {
    # Every NYSE session in the year at 15.0, the current print 30.0.
    # 11-16 has two vintages (40.0, then 20.0 the next day: newest wins).
    # 11-09 has a newer vintage of 99.0 that is retracted. Labor Day
    # (09-07) has a Cboe holiday print, which is not a session.
    "vix_spot": [(d, d, 15.0) for d in YEAR if d not in (D(2026, 11, 16), D(2026, 11, 9), CUTOFF)]
    + [(D(2026, 11, 16), D(2026, 11, 16), 40.0), (D(2026, 11, 16), D(2026, 11, 17), 20.0),
       (D(2026, 11, 9), D(2026, 11, 9), 19.0), (D(2026, 11, 9), D(2026, 11, 10), 99.0),
       (D(2026, 9, 7), D(2026, 9, 7), 77.0),
       (CUTOFF, CUTOFF, 30.0)],
    # The 11-23 HY row was written on 11-24, after the cutoff: never read.
    "hy_oas_spread": [(D(2026, 10, 20), D(2026, 10, 21), 3.38), (D(2026, 11, 20), D(2026, 11, 23), 3.76),
                      (D(2026, 11, 23), D(2026, 11, 24), 5.00)],
    "ig_oas_spread": [(D(2026, 10, 20), D(2026, 10, 21), 0.91), (D(2026, 11, 20), D(2026, 11, 23), 0.98)],
    "fed_balance_sheet": [(D(2026, 10, 14), D(2026, 10, 15), 6_753_604.0),
                          (D(2026, 10, 21), D(2026, 10, 22), 6_760_000.0),
                          (D(2026, 11, 18), D(2026, 11, 19), 6_741_452.0)],
    "treasury_general_acct": [(D(2026, 10, 14), D(2026, 10, 15), 952_002.0),
                              (D(2026, 11, 18), D(2026, 11, 19), 916_613.0)],
    "reverse_repo_usage": [(D(2026, 10, 23), D(2026, 10, 23), 0.20), (CUTOFF, CUTOFF, 0.35)],
}
# Same-pattern features from griddb, each newer than the real series.
DECOYS = {
    "hy_spread_3m_chg": [(D(2026, 10, 15), D(2026, 10, 15), 0.10), (CUTOFF, CUTOFF, 0.83)],
    "hy_spread_proxy": [(D(2026, 11, 21), D(2026, 11, 21), 3.52)],
    "hy_spread": [(CUTOFF, CUTOFF, 9.99)],
    "ig_spread_proxy": [(CUTOFF, CUTOFF, 5.55)],
    "ted_spread": [(D(2022, 1, 21), D(2022, 1, 21), 0.09)],
    "tga_drawdown": [(D(2026, 10, 21), D(2026, 10, 21), 74_974.0), (CUTOFF, CUTOFF, 33_240.0)],
    "treasury_tga_balance": [(D(2026, 4, 1), D(2026, 4, 2), 847_718.0)],
    "mortgage_30y": [(CUTOFF, CUTOFF, 6.0)],
    "reverse_repo": [(CUTOFF, CUTOFF, 777.0)],
    "vix": [(CUTOFF, CUTOFF, 11.0)],
    "vix_close_decoy": [(CUTOFF, CUTOFF, 88.0)],
}


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"risk_map_series_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema} -ctimezone=UTC"})
    try:
        with engine.begin() as conn:
            for ddl in _DDL:
                conn.execute(text(ddl))
            for name, rows in {**REAL, **DECOYS}.items():
                fid = conn.execute(
                    text("INSERT INTO feature_registry (name) VALUES (:n) RETURNING id"), {"n": name}
                ).scalar_one()
                conn.execute(
                    text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                         "VALUES (:f, :od, :vd, :vd, :v)"),
                    [{"f": fid, "od": od, "vd": vd, "v": v} for od, vd, v in rows],
                )
            conn.execute(
                text("INSERT INTO resolved_series_retractions (feature_id, obs_date, vintage_date, "
                     "retracted_at, reason, run_tag) "
                     "SELECT id, :od, :vd, :at, 'bad print', 'test' "
                     "FROM feature_registry WHERE name = 'vix_spot'"),
                {"od": D(2026, 11, 9), "vd": D(2026, 11, 10),
                 "at": datetime(2026, 11, 20, tzinfo=timezone.utc)},
            )
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


@pytest.fixture()
def risk_map(scratch, monkeypatch):
    from api.routers import intelligence_risk as ir

    mod = types.ModuleType("physics.dealer_gamma")

    class DealerGammaEngine:  # test stub
        def __init__(self, engine):
            pass

        def compute_gex_profile(self, ticker):
            return {"error": "no options chain in the scratch schema"}

    mod.DealerGammaEngine = DealerGammaEngine
    monkeypatch.setitem(sys.modules, "physics.dealer_gamma", mod)
    monkeypatch.setattr(ir, "get_db_engine", lambda: scratch)
    monkeypatch.setattr(ir, "_risk_cutoff", lambda: CUTOFF)
    ir._risk_map_cache.clear()
    try:
        yield ir._build_risk_map()
    finally:
        ir._risk_map_cache.clear()


def test_volatility_reads_vix_spot_one_value_per_session(risk_map):
    vol = risk_map["volatility_risk"]
    assert vol["available"] is True, vol
    assert vol["vix"] == 30.0
    assert vol["vix_as_of"] == "2026-11-23"
    # Every session once, at its newest unretracted vintage: 15s, 20.0, 19.0, 30.0.
    # The superseded 40.0, the retracted 99.0 and the holiday 77.0 would each
    # put 30.0 below the 100th percentile.
    assert vol["vix_percentile_sample"] == vol["vix_sessions_expected"] == len(YEAR)
    assert vol["vix_percentile_1y"] == 100


def test_credit_reads_oas_in_basis_points_not_the_decoys(risk_map):
    credit = risk_map["credit_risk"]
    assert credit["available"] is True, credit
    assert credit["hy_spread"] == 376  # not the 11-23 row written after the cutoff
    assert credit["hy_spread_as_of"] == "2026-11-20"
    assert credit["hy_spread_prior_as_of"] == "2026-10-20"
    assert credit["ig_spread"] == 98
    assert credit["spread_direction"] == "widening"
    assert credit["risk_level"] == "elevated"
    assert "ted_spread" not in credit


def test_liquidity_reads_walcl_wtregen_rrpontsyd_not_the_decoys(risk_map):
    liq = risk_map["liquidity_risk"]
    assert liq["available"] is True, liq
    # Anchored to 11-18: the prior is 10-14 (35 days), not 10-21 (28 days).
    assert liq["fed_total_assets_change_1m_usd"] == -12_152_000_000
    assert liq["fed_total_assets_as_of"] == "2026-11-18"
    assert liq["fed_total_assets_prior_as_of"] == "2026-10-14"
    # tga_drawdown (74,974 -> 33,240) would read "draining".
    assert liq["tga_trend"] == "stable"
    assert liq["reverse_repo_trend"] == "rising"


def test_only_series_subsystems_are_measured(risk_map):
    assert risk_map["available_subsystems"] == 3
    assert sorted(risk_map["unavailable_subsystems"]) == ["Concentration", "Correlation", "Dealer positioning"]
