"""Regression tests for verified defects in intelligence/dollar_flows.py
and intelligence/sentiment_scorer.py.

All tests are offline (in-memory SQLite + mocks; no external DB, no network).
Covers:
  1. 13F normalization: skip 3-part portfolio metrics and unknown actions,
     vintage dedup per (series_id, obs_date) with PIT as_of cutoff, real
     ticker mapping required before sector aggregation.
  2. Unsigned dark-pool and ETF volume: direction='neutral' unless a signed
     measure exists; neutral rows never become inflows or outflows; sentiment
     scorer abstains on unsigned components and renormalizes weights over
     directional/active components.
  3. Put/call ratio: SPY only (not cross-ticker average), Schwab-safe, PIT-safe,
     fail-closed on missing or non-positive SPY PCR.
  4. Yfinance daily-policy consistency: sentiment_scorer uses read_daily_policy_n
     (matching ollama/market_briefing.py) so intraday INCOMPLETE/unmarked pulls
     are never mixed with prior-day completed closes.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    text,
)

from analysis import flow_aggregator
from ingestion.altdata import institutional_flows
from ingestion.market_calendar import trading_days_between
from intelligence import dollar_flows, sentiment_scorer
from ollama.market_briefing import _read_yf_snapshot_value
from price_close_contract import daily_capture_payload, raw_capture_payload

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

TODAY = date.today()
YESTERDAY = TODAY - timedelta(days=1)
FUTURE = TODAY + timedelta(days=5)


def _make_engine(*, preserve_ts_text: bool = False):
    eng = create_engine("sqlite://")
    md = MetaData()
    Table(
        "source_catalog", md,
        Column("id", Integer, primary_key=True),
        Column("name", String, nullable=False),
    )
    Table(
        "raw_series", md,
        Column("series_id", String, nullable=False),
        Column("source_id", Integer, ForeignKey("source_catalog.id"), nullable=False),
        Column("obs_date", Date, nullable=False),
        Column("pull_timestamp", Text if preserve_ts_text else DateTime, nullable=False),
        Column("value", Float, nullable=False),
        Column("raw_payload", Text),
        Column("pull_status", String, nullable=False),
    )
    Table(
        "signal_sources", md,
        Column("id", Integer, primary_key=True),
        Column("source_type", String, nullable=False),
        Column("source_id", String, nullable=False),
        Column("ticker", String),
        Column("signal_date", Date, nullable=False),
        Column("signal_type", String, nullable=False),
        Column("signal_value", Text),
        Column("trust_score", Float),
        UniqueConstraint("source_type", "source_id", "ticker", "signal_date", "signal_type"),
    )
    Table(
        "dollar_flows", md,
        Column("id", Integer, primary_key=True),
        Column("source_type", String, nullable=False),
        Column("actor_name", String),
        Column("ticker", String),
        Column("amount_usd", Float, nullable=False),
        Column("direction", String, nullable=False),
        Column("confidence", String),
        Column("evidence", Text),
        Column("flow_date", Date, nullable=False),
        Column("created_at", DateTime, server_default=text("CURRENT_TIMESTAMP")),
    )
    Table(
        "security_identifiers", md,
        Column("id", Integer, primary_key=True),
        Column("entity_id", String, nullable=False),
        Column("id_scheme", String, nullable=False),
        Column("id_value", String, nullable=False),
        Column("valid_from", Date, nullable=False),
        Column("valid_to", Date),
        Column("is_primary", Integer, nullable=False, default=1),
    )
    Table(
        "options_daily_signals", md,
        Column("ticker", String, nullable=False),
        Column("signal_date", Date, nullable=False),
        Column("put_call_ratio", Float),
        Column("wing_iv_metadata", Text),
    )
    Table(
        "options_capture_batches", md,
        Column("capture_batch_id", String, primary_key=True),
        Column("ticker", String, nullable=False),
        Column("snap_date", Date, nullable=False),
        Column("spot_provider", String),
        Column("clock_metadata", Text),
    )
    md.create_all(eng)
    with eng.begin() as conn:
        conn.execute(text("INSERT INTO source_catalog (id, name) VALUES (1, 'yfinance'), (2, 'edgar')"))
    return eng


def _insert_raw(engine, series_id, obs_date, value, *, source_id=2, pull_ts=None, payload=None, status="SUCCESS"):
    ts = pull_ts if pull_ts is not None else datetime.combine(obs_date, datetime.min.time())
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO raw_series "
                "(series_id, source_id, obs_date, pull_timestamp, value, raw_payload, pull_status) "
                "VALUES (:sid, :src, :od, :ts, :val, :payload, :status)"
            ),
            {
                "sid": series_id,
                "src": source_id,
                "od": obs_date,
                "ts": ts,
                "val": value,
                "payload": json.dumps(payload) if isinstance(payload, dict) else (payload or "{}"),
                "status": status,
            },
        )


def _insert_flow(engine, source_type, actor_name, ticker, amount_usd, direction, flow_date, confidence="confirmed"):
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO dollar_flows "
                "(source_type, actor_name, ticker, amount_usd, direction, confidence, evidence, flow_date) "
                "VALUES (:st, :act, :tkr, :amt, :dir, :conf, '{}', :fd)"
            ),
            {
                "st": source_type,
                "act": actor_name,
                "tkr": ticker,
                "amt": amount_usd,
                "dir": direction,
                "conf": confidence,
                "fd": flow_date,
            },
        )


# ── Defect 1: 13F normalization, vintage dedup, PIT as_of, ticker mapping ─


def test_13f_skips_3part_metrics_and_unknown_actions():
    """Only 4-part 13F:{cik}:{cusip}:{NEW|INCREASED|DECREASED|CLOSED} are
    candidates, and of those only NEW/CLOSED are kept, as non-directional
    observations (13F is non-directional until CUSIP identity and split
    coverage exist).

    Worked synthetic example:
      - 13F:0001067983:TOTAL_VALUE         = $250,000,000,000  (3-part AUM -> skip)
      - 13F:0001067983:NUM_POSITIONS       = 45                (3-part count -> skip)
      - 13F:0001067983:TOP5_CONCENTRATION  = 0.72              (3-part ratio -> skip)
      - 13F:0001067983:037833100:UNKNOWN   = $50,000,000       (unknown action -> skip)
      - 13F:0001067983:037833100:NEW       = $120,000,000      (observed appearance)
      - 13F:0001067983:594918104:CLOSED    = $45,000,000       (observed disappearance)
      - 13F:0001067983:459200101:DECREASED = $30,000,000       (continuing position:
        a split cannot be ruled out -> quarantined)
    Expected observed total = $120M + $45M = $165M (never +$250.125B).
    """
    engine = _make_engine()
    filing_day = YESTERDAY
    _insert_raw(engine, "13F:0001067983:TOTAL_VALUE", filing_day, 250_000_000_000.0,
                payload={"cik": "0001067983", "num_positions": 45})
    _insert_raw(engine, "13F:0001067983:NUM_POSITIONS", filing_day, 45.0,
                payload={"cik": "0001067983"})
    _insert_raw(engine, "13F:0001067983:TOP5_CONCENTRATION", filing_day, 0.72,
                payload={"cik": "0001067983"})
    _insert_raw(engine, "13F:0001067983:037833100:UNKNOWN", filing_day, 50_000_000.0,
                payload={"manager_name": "Berkshire", "cusip": "037833100", "ticker": "AAPL"})
    _insert_raw(engine, "13F:0001067983:037833100:NEW", filing_day, 120_000_000.0,
                payload={
                    "manager_name": "Berkshire", "cusip": "037833100", "issuer_name": "APPLE INC",
                    "ticker": "AAPL", "value_units": "usd", "value_multiplier": 1,
                    "share_type": "SH", "prev_shares": 0, "curr_shares": 600_000,
                    "delta_shares": 600_000, "curr_value_usd": 120_000_000.0,
                    "flow_estimation_method": "delta_shares_x_curr_quarter_end_price",
                    "qualified_for_directional_flow": True,
                })
    # CLOSED: 300k shares at the prior quarter-end implied $150 = $45M outflow.
    _insert_raw(engine, "13F:0001067983:594918104:CLOSED", filing_day, 45_000_000.0,
                payload={
                    "manager_name": "Berkshire", "cusip": "594918104", "issuer_name": "MICROSOFT CORP",
                    "ticker": "MSFT", "value_units": "usd", "value_multiplier": 1,
                    "share_type": "SH", "prev_shares": 300_000, "curr_shares": 0,
                    "delta_shares": -300_000, "curr_value_usd": 0.0,
                    "prev_value_usd": 45_000_000.0,
                    "flow_estimation_method": "delta_shares_x_prev_quarter_end_price",
                    "qualified_for_directional_flow": True,
                })
    # A well-formed 4-part DECREASED row (continuing position) is a flow
    # candidate but never a verified direction.
    _insert_raw(engine, "13F:0001067983:459200101:DECREASED", filing_day, 30_000_000.0,
                payload={
                    "manager_name": "Berkshire", "cusip": "459200101", "issuer_name": "IBM",
                    "ticker": "IBM", "value_units": "usd", "value_multiplier": 1,
                    "share_type": "SH", "prev_shares": 400_000, "curr_shares": 200_000,
                    "delta_shares": -200_000, "curr_value_usd": 30_000_000.0,
                    "prev_value_usd": 60_000_000.0,
                    "flow_estimation_method": "delta_shares_x_curr_quarter_end_price",
                    "qualified_for_directional_flow": True,
                })

    reasons: dict[str, int] = {}
    flows = dollar_flows._normalize_13f_flows(engine, days=30, quarantine_reasons=reasons)

    assert len(flows) == 2
    by_ticker = {f["ticker"]: f for f in flows}
    assert set(by_ticker) == {"AAPL", "MSFT"}
    # 13F is non-directional (owner decision 2026-11-23): NEW/CLOSED are
    # observed filing appearances/disappearances at reference prices.
    assert by_ticker["AAPL"]["direction"] == "observed"
    assert by_ticker["AAPL"]["amount_usd"] == pytest.approx(120_000_000.0)
    assert by_ticker["AAPL"]["evidence"]["observation_kind"] == "filing_appearance"
    assert by_ticker["MSFT"]["direction"] == "observed"
    assert by_ticker["MSFT"]["amount_usd"] == pytest.approx(45_000_000.0)
    assert by_ticker["MSFT"]["evidence"]["observation_kind"] == "filing_disappearance"
    for f in flows:
        assert f["evidence"]["qualified_for_directional_flow"] is False
        assert f["evidence"]["unqualified_reason"] == "no_identity_coverage"
        assert f["evidence"]["measure"] == "observed_filing_holdings_change_at_reference_prices"
    # The 3-part metrics and UNKNOWN action are skipped, not quarantined; the
    # DECREASED row is the only quarantine.
    assert reasons == {"no_corporate_action_coverage": 1}

    # Only the two 4-part NEW/CLOSED rows are observed: $165M in total, never
    # the $250B 3-part AUM, the position count, the ratio or the UNKNOWN row.
    assert sum(f["amount_usd"] for f in flows) == pytest.approx(165_000_000.0)


def test_13f_dedups_vintages_and_respects_pit_as_of():
    """Multiple vintages for (series_id, obs_date) collapse to the latest PIT vintage."""
    engine = _make_engine()
    obs1 = date(2026, 11, 10)
    obs2 = date(2026, 11, 18)

    # Older vintage of obs1 pulled Nov 11 (NEW 50k sh x $200 = $10M), revision
    # pulled Nov 15 (NEW 60k sh x $200 = $12M).
    _insert_raw(
        engine, "13F:0001067983:037833100:NEW", obs1, 10_000_000.0,
        pull_ts=datetime(2026, 11, 11, 10, 0, 0),
        payload={
            "manager_name": "Berkshire", "cusip": "037833100", "ticker": "AAPL",
            "value_units": "usd", "value_multiplier": 1, "share_type": "SH",
            "prev_shares": 0, "curr_shares": 50_000, "delta_shares": 50_000,
            "curr_value_usd": 10_000_000.0, "prev_value_usd": 0.0,
            "flow_estimation_method": "delta_shares_x_curr_quarter_end_price",
            "qualified_for_directional_flow": True,
        },
    )
    _insert_raw(
        engine, "13F:0001067983:037833100:NEW", obs1, 12_000_000.0,
        pull_ts=datetime(2026, 11, 15, 10, 0, 0),
        payload={
            "manager_name": "Berkshire", "cusip": "037833100", "ticker": "AAPL",
            "value_units": "usd", "value_multiplier": 1, "share_type": "SH",
            "prev_shares": 0, "curr_shares": 60_000, "delta_shares": 60_000,
            "curr_value_usd": 12_000_000.0, "prev_value_usd": 0.0,
            "flow_estimation_method": "delta_shares_x_curr_quarter_end_price",
            "qualified_for_directional_flow": True,
        },
    )
    # Later filing obs2 on Nov 18 ($8M CLOSED).
    _insert_raw(
        engine, "13F:0001067983:594918104:CLOSED", obs2, 8_000_000.0,
        pull_ts=datetime(2026, 11, 18, 10, 0, 0),
        payload={
            "manager_name": "Berkshire", "cusip": "594918104", "ticker": "MSFT",
            "value_units": "usd", "value_multiplier": 1, "share_type": "SH",
            "prev_shares": 40_000, "curr_shares": 0, "delta_shares": -40_000,
            "prev_value_usd": 8_000_000.0, "curr_value_usd": 0.0,
            "flow_estimation_method": "delta_shares_x_prev_quarter_end_price",
            "qualified_for_directional_flow": True,
        },
    )

    # As of Nov 20: obs1 deduplicated to latest vintage ($12M), plus obs2 ($8M).
    flows_nov20 = dollar_flows._normalize_13f_flows(engine, days=30, as_of=date(2026, 11, 20))
    assert len(flows_nov20) == 2
    by_t = {f["ticker"]: f for f in flows_nov20}
    assert by_t["AAPL"]["amount_usd"] == pytest.approx(12_000_000.0)
    assert by_t["MSFT"]["amount_usd"] == pytest.approx(8_000_000.0)

    # As of Nov 13: obs2 is in the future (excluded) AND Nov 15 revision is
    # after as_of (excluded), so obs1 must return the Nov 11 vintage ($10M).
    flows_nov13 = dollar_flows._normalize_13f_flows(engine, days=30, as_of=date(2026, 11, 13))
    assert len(flows_nov13) == 1
    assert flows_nov13[0]["ticker"] == "AAPL"
    assert flows_nov13[0]["amount_usd"] == pytest.approx(10_000_000.0)
    # 13F is non-directional: every surviving vintage is an observation.
    assert {f["direction"] for f in flows_nov20 + flows_nov13} == {"observed"}


def test_13f_resolves_cusip_to_ticker_and_requires_real_ticker_for_sector_agg():
    """Never dump issuer_name into ticker or unmapped CUSIPs into sector 'Unknown'."""
    engine = _make_engine()
    filing_day = YESTERDAY

    # Map CUSIP 037833100 -> AAPL in security_identifiers (PIT valid).
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO security_identifiers "
            "(entity_id, id_scheme, id_value, valid_from, valid_to, is_primary) VALUES "
            "('sm_0000320193', 'cusip', '037833100', '2020-01-01', NULL, 1), "
            "('sm_0000320193', 'ticker', 'AAPL', '2020-01-01', NULL, 1)"
        ))

    # Row A: no ticker in payload, only cusip + issuer_name -> resolves via security_identifiers.
    _insert_raw(
        engine, "13F:0001067983:037833100:NEW", filing_day, 120_000_000.0,
        payload={
            "manager_name": "Berkshire", "cusip": "037833100", "issuer_name": "APPLE INC",
            "value_units": "usd", "value_multiplier": 1, "share_type": "SH",
            "prev_shares": 0, "curr_shares": 600_000, "delta_shares": 600_000,
            "curr_value_usd": 120_000_000.0,
            "flow_estimation_method": "delta_shares_x_curr_quarter_end_price",
            "qualified_for_directional_flow": True,
        },
    )
    # Row B: unmapped CUSIP, only issuer_name -> ticker must be None, NOT 'MYSTERY CORP'.
    _insert_raw(
        engine, "13F:0001067983:ZZZ999999:NEW", filing_day, 30_000_000.0,
        payload={
            "manager_name": "Berkshire", "cusip": "ZZZ999999", "issuer_name": "MYSTERY CORP",
            "value_units": "usd", "value_multiplier": 1, "share_type": "SH",
            "prev_shares": 0, "curr_shares": 150_000, "delta_shares": 150_000,
            "curr_value_usd": 30_000_000.0,
            "flow_estimation_method": "delta_shares_x_curr_quarter_end_price",
            "qualified_for_directional_flow": True,
        },
    )

    flows = dollar_flows._normalize_13f_flows(engine, days=30, cusip_map=None)

    assert len(flows) == 2
    mapped = [f for f in flows if f["ticker"] == "AAPL"]
    unmapped = [f for f in flows if f["ticker"] is None]
    assert len(mapped) == 1
    assert len(unmapped) == 1
    assert all(f["ticker"] not in ("APPLE INC", "MYSTERY CORP", "") for f in flows)

    # Also verify CusipTickerMap fallback works with a stub mapper.
    class _StubCusipMap:
        def lookup(self, cusip: str):
            return "MSFT" if cusip == "ZZZ999999" else None

    flows_via_map = dollar_flows._normalize_13f_flows(engine, days=30, cusip_map=_StubCusipMap())
    assert {f["ticker"] for f in flows_via_map} == {"AAPL", "MSFT"}

    # Persist: the 13F rows are recorded as observations, not flows.
    expected_sector = dollar_flows._get_sector("AAPL")
    assert expected_sector != "Unknown"
    dollar_flows._persist_flows(engine, flows)
    with engine.connect() as conn:
        persisted = conn.execute(
            text("SELECT ticker, direction FROM dollar_flows WHERE source_type = :st"),
            {"st": "13f"},
        ).fetchall()
    assert sorted((t or "", d) for t, d in persisted) == [("", "observed"), ("AAPL", "observed")]

    # Non-13F rows: one on a mapped ticker, one with no ticker at all.
    _insert_flow(engine, "insider", "Jane Doe", "AAPL", 1_000_000.0, "inflow", filing_day)
    _insert_flow(engine, "insider", "John Roe", None, 2_000_000.0, "inflow", filing_day)

    # Sector aggregation needs a real ticker (never 'Unknown'), and 13F
    # (non-directional) contributes nothing to any directional total.
    agg = dollar_flows.get_aggregate_flows(engine, days=30)
    assert "Unknown" not in agg["by_sector"]
    assert list(agg["by_sector"].keys()) == [expected_sector]
    assert agg["by_sector"][expected_sector]["inflow"] == pytest.approx(1_000_000.0)
    assert agg["total_inflow"] == pytest.approx(3_000_000.0)
    assert "13f" not in agg["by_source_type"]
    assert agg["quarantined_13f"] == 2

    flow_aggregator._flow_cache.clear()
    try:
        sec_agg = flow_aggregator.aggregate_by_sector(engine, days=30)
    finally:
        flow_aggregator._flow_cache.clear()
    assert "Unknown" not in sec_agg
    assert list(sec_agg.keys()) == [expected_sector]
    assert sec_agg[expected_sector]["inflow"] == pytest.approx(1_000_000.0)


# ── Defect 2: Unsigned dark-pool & ETF volume -> neutral; no +1.00 pegs ───


def test_unsigned_darkpool_and_etf_are_neutral_unless_signed_measure_exists():
    """Unsigned dark-pool and ETF volumes must emit direction='neutral', never 'inflow'."""
    engine = _make_engine()
    _insert_raw(engine, "YF:AAPL:close", YESTERDAY, 200.0, source_id=1)

    # Unsigned dark-pool spike -> neutral
    dp_unsigned, reason = dollar_flows._normalize_darkpool(
        {
            "source_id": "finra_ats",
            "ticker": "AAPL",
            "signal_type": "UNUSUAL_VOLUME",
            "signal_date": YESTERDAY,
            "signal_value": json.dumps({"volume": 50_000, "spike_ratio": 3.1}),
        },
        engine,
    )
    assert reason is None
    assert dp_unsigned is not None
    assert dp_unsigned["direction"] == "neutral"

    # Signed dark-pool row (explicit buy/sell volume) -> directional
    dp_signed, reason = dollar_flows._normalize_darkpool(
        {
            "source_id": "finra_ats",
            "ticker": "AAPL",
            "signal_type": "UNUSUAL_VOLUME",
            "signal_date": YESTERDAY,
            "signal_value": json.dumps({"volume": 50_000, "buy_volume": 10_000, "sell_volume": 40_000}),
        },
        engine,
    )
    assert reason is None
    assert dp_signed is not None
    assert dp_signed["direction"] == "outflow"

    # Unsigned ETF_FLOW:SPY:5d (with duplicate vintage!) -> single neutral row
    _insert_raw(
        engine, "ETF_FLOW:SPY:5d", YESTERDAY, 50_000_000_000.0, source_id=2,
        pull_ts=datetime.combine(YESTERDAY, datetime.min.time()) + timedelta(hours=1),
        payload={"ticker": "SPY", "close": 580.0, "volume": 86_000_000},
    )
    _insert_raw(
        engine, "ETF_FLOW:SPY:5d", YESTERDAY, 52_000_000_000.0, source_id=2,
        pull_ts=datetime.combine(YESTERDAY, datetime.min.time()) + timedelta(hours=2),
        payload={"ticker": "SPY", "close": 581.0, "volume": 89_000_000},
    )
    # Signed ETF_FLOW:QQQ:5d -> directional outflow
    _insert_raw(
        engine, "ETF_FLOW:QQQ:5d", YESTERDAY, 20_000_000_000.0, source_id=2,
        pull_ts=datetime.combine(YESTERDAY, datetime.min.time()) + timedelta(hours=2),
        payload={"ticker": "QQQ", "net_flow": -350_000_000.0},
    )

    etf_flows = dollar_flows._normalize_etf_flows(engine, days=30)
    assert len(etf_flows) == 2
    by_t = {f["ticker"]: f for f in etf_flows}
    assert by_t["SPY"]["direction"] == "neutral"
    assert by_t["SPY"]["amount_usd"] == pytest.approx(52_000_000_000.0 * 0.003)
    assert by_t["QQQ"]["direction"] == "outflow"

    # Neutral rows persisted to dollar_flows must NOT count as inflow OR outflow.
    dollar_flows._persist_flows(engine, [dp_unsigned, by_t["SPY"]])
    agg = dollar_flows.get_aggregate_flows(engine, days=30)
    assert agg["total_inflow"] == 0.0
    assert agg["total_outflow"] == 0.0
    assert agg["net_flow"] == 0.0

    flow_aggregator._flow_cache.clear()
    tiers = flow_aggregator.aggregate_by_actor_tier(engine, days=30)
    assert tiers["institutional"]["inflow"] == 0.0
    assert tiers["institutional"]["outflow"] == 0.0
    assert tiers["institutional"]["net_flow"] == 0.0


def test_sentiment_scorer_abstains_on_unsigned_flows_and_renormalizes_weights(monkeypatch):
    """Unsigned darkpool/ETF flows abstain (no +1.00 peg) and active weights sum to 1.0."""
    engine = _make_engine()
    _insert_flow(engine, "darkpool", "finra_ats", "AAPL", 500_000_000.0, "neutral", YESTERDAY, "estimated")
    _insert_flow(engine, "etf_flow", "ETF:SPY", "SPY", 2_000_000_000.0, "neutral", YESTERDAY, "estimated")

    dp_comp = sentiment_scorer._score_dark_pool(engine)
    etf_comp = sentiment_scorer._score_etf_flows(engine)
    flow_comp = sentiment_scorer._score_flows(engine)

    for comp in (dp_comp, etf_comp, flow_comp):
        assert comp.score == 0.0
        assert comp.detail.endswith("available")

    # End-to-end compute_sentiment with 2 active bearish components + abstained flows.
    monkeypatch.setattr(sentiment_scorer, "_ensure_tables", lambda _e: None)
    monkeypatch.setattr(sentiment_scorer, "_load_weights", lambda _e: (dict(sentiment_scorer.DEFAULT_WEIGHTS), 0))

    no_data = lambda name: sentiment_scorer.SentimentComponent(name, 0.0, 0.0, 0.0, "No data available")
    monkeypatch.setattr(sentiment_scorer, "_score_momentum", lambda _e: sentiment_scorer.SentimentComponent("momentum", -0.025, -0.5, 0.0, "SPY down"))
    monkeypatch.setattr(sentiment_scorer, "_score_volatility", lambda _e: sentiment_scorer.SentimentComponent("volatility", 27.5, -0.5, 0.0, "VIX=27.5"))
    for fn_name, comp_name in [
        ("_score_breadth", "breadth"),
        ("_score_trend", "trend"),
        ("_score_vol_term_structure", "vol_term_structure"),
        ("_score_put_call_ratio", "put_call_ratio"),
        ("_score_signals", "signals"),
        ("_score_insider", "insider"),
        ("_score_congressional", "congressional"),
        ("_score_regime", "regime"),
        ("_score_yield_curve", "yield_curve"),
        ("_score_credit_spread", "credit_spread"),
        ("_score_social_sentiment", "social_sentiment"),
        ("_score_fear_greed", "fear_greed"),
        ("_score_convergence", "convergence"),
    ]:
        monkeypatch.setattr(sentiment_scorer, fn_name, lambda _e, n=comp_name: no_data(n))

    result = sentiment_scorer.compute_sentiment(engine)
    by_name = {c.name: c for c in result.components}

    # Unsigned flow components must have 0 score and 0 weight (never pegged at +1.00).
    for name in ("flows", "dark_pool", "etf_flows"):
        assert by_name[name].score == 0.0
        assert by_name[name].weight == 0.0

    # Active components' weights must be renormalized to sum to 1.0:
    # momentum raw=0.08, volatility raw=0.06 -> 0.08/0.14 and 0.06/0.14.
    assert by_name["momentum"].weight == pytest.approx(0.08 / 0.14)
    assert by_name["volatility"].weight == pytest.approx(0.06 / 0.14)
    assert sum(c.weight for c in result.components) == pytest.approx(1.0)
    assert result.score == pytest.approx(-0.5)
    assert result.label == "BEARISH"


def test_sentiment_scorer_uses_signed_darkpool_and_etf_when_directional():
    """When a genuinely signed measure exists (inflow/outflow), components score it."""
    engine = _make_engine()
    _insert_flow(engine, "darkpool", "finra_ats", "AAPL", 30_000_000.0, "inflow", YESTERDAY)
    _insert_flow(engine, "darkpool", "finra_ats", "AAPL", 70_000_000.0, "outflow", YESTERDAY)
    _insert_flow(engine, "etf_flow", "ETF:SPY", "SPY", 200_000_000.0, "inflow", YESTERDAY)
    _insert_flow(engine, "etf_flow", "ETF:SPY", "SPY", 300_000_000.0, "outflow", YESTERDAY)

    dp = sentiment_scorer._score_dark_pool(engine)
    etf = sentiment_scorer._score_etf_flows(engine)
    fl = sentiment_scorer._score_flows(engine)

    assert dp.raw_value == pytest.approx(-0.4)
    assert dp.score == pytest.approx(-0.8)
    assert etf.raw_value == pytest.approx(-0.2)
    assert etf.score == pytest.approx(-0.4)
    assert fl.raw_value == pytest.approx((230 - 370) / 600)
    assert fl.score < 0.0


# ── Defect 3: Put/call ratio uses SPY only ────────────────────────────────


def test_put_call_ratio_uses_spy_only_not_all_ticker_average():
    """SPY PCR (0.62 -> complacent -0.5) must not be averaged with single-stock PCRs."""
    engine = _make_engine()
    obs_day = date(2026, 11, 20)
    schwab_day = date(2026, 11, 21)

    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO options_daily_signals (ticker, signal_date, put_call_ratio, wing_iv_metadata) VALUES "
            "('SPY', :d, 0.62, '{}'), "
            "('AAPL', :d, 2.80, '{}'), "
            "('TSLA', :d, 3.10, '{}'), "
            "('NVDA', :d, 2.50, '{}'), "
            "('SPY', :sd, 1.90, '{}'), "
            "('SPY', :fd, 2.40, '{}')"
        ), {"d": obs_day, "sd": schwab_day, "fd": FUTURE})
        conn.execute(text(
            "INSERT INTO options_capture_batches (capture_batch_id, ticker, snap_date, spot_provider, clock_metadata) VALUES "
            "('b_cboe', 'SPY', :d, 'cboe', '{\"provider\": \"cboe\"}'), "
            "('b_schwab', 'SPY', :sd, 'schwab', '{\"provider\": \"schwab\"}')"
        ), {"d": obs_day, "sd": schwab_day})

    comp = sentiment_scorer._score_put_call_ratio(engine)
    assert comp.raw_value == pytest.approx(0.62)
    assert comp.score == pytest.approx(-0.5)
    assert "SPY" in comp.detail


def test_put_call_ratio_fails_closed_without_valid_spy_row():
    """Single-stock PCRs or non-positive SPY PCR must abstain (fail closed)."""
    engine = _make_engine()
    obs_day = date(2026, 11, 20)
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO options_daily_signals (ticker, signal_date, put_call_ratio, wing_iv_metadata) VALUES "
            "('AAPL', :d, 2.80, '{}'), "
            "('SPY', :d, 0.0, '{}')"
        ), {"d": obs_day})

    comp = sentiment_scorer._score_put_call_ratio(engine)
    assert comp.score == 0.0
    assert comp.detail == "No data available"


# ── Defect 4: Yfinance reads use read_daily_policy_n like market_briefing ─


def test_sentiment_yf_reads_ignore_intraday_incomplete_and_match_market_briefing(monkeypatch):
    """Intraday INCOMPLETE/unmarked pulls must not override prior policy-eligible closes."""
    engine = _make_engine(preserve_ts_text=True)

    # Pin wall clock to Friday 2026-11-20 16:00 UTC (mid-session in NY).
    pinned_now = datetime(2026, 11, 20, 16, 0, 0, tzinfo=timezone.utc)

    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return pinned_now if tz is not None else pinned_now.replace(tzinfo=None)

    monkeypatch.setattr(sentiment_scorer, "datetime", _FrozenDatetime)

    # Seed 7 completed trading days ending Thursday 2026-11-19 (policy-eligible).
    sessions = [
        date(2026, 11, 11),
        date(2026, 11, 12),
        date(2026, 11, 13),
        date(2026, 11, 16),
        date(2026, 11, 17),
        date(2026, 11, 18),
        date(2026, 11, 19),
    ]
    for idx, d in enumerate(sessions):
        captured = datetime(d.year, d.month, d.day, 6, 0, 0, tzinfo=timezone.utc) + timedelta(days=1)
        _insert_raw(
            engine, "YF:^GSPC:close", d, 5800.0 - idx * 20.0, source_id=1,
            pull_ts=captured.isoformat(sep=" "),
            payload=daily_capture_payload("^GSPC", "close", d, captured),
        )
    # Completed close for 2026-11-19: VIX=26.0, VIX3M=24.0 (backwardation / fear).
    cap_19 = datetime(2026, 11, 20, 6, 0, 0, tzinfo=timezone.utc)
    _insert_raw(
        engine, "YF:^VIX:close", date(2026, 11, 19), 26.0, source_id=1,
        pull_ts=cap_19.isoformat(sep=" "),
        payload=daily_capture_payload("^VIX", "close", date(2026, 11, 19), cap_19),
    )
    _insert_raw(
        engine, "YF:^VIX3M:close", date(2026, 11, 19), 24.0, source_id=1,
        pull_ts=cap_19.isoformat(sep=" "),
        payload=daily_capture_payload("^VIX3M", "close", date(2026, 11, 19), cap_19),
    )

    # Intraday INCOMPLETE pulls for Friday 2026-11-20 at 15:30 UTC (contradicting values).
    intraday_ts = datetime(2026, 11, 20, 15, 30, 0, tzinfo=timezone.utc)
    _insert_raw(
        engine, "YF:^VIX:close", date(2026, 11, 20), 13.0, source_id=1,
        pull_ts=intraday_ts.isoformat(sep=" "),
        payload=raw_capture_payload("^VIX", "close", date(2026, 11, 20), intraday_ts, "1d"),
    )
    _insert_raw(
        engine, "YF:^VIX3M:close", date(2026, 11, 20), 20.0, source_id=1,
        pull_ts=intraday_ts.isoformat(sep=" "),
        payload=raw_capture_payload("^VIX3M", "close", date(2026, 11, 20), intraday_ts, "1d"),
    )
    _insert_raw(
        engine, "YF:^GSPC:close", date(2026, 11, 20), 6500.0, source_id=1,
        pull_ts=intraday_ts.isoformat(sep=" "),
        payload=raw_capture_payload("^GSPC", "close", date(2026, 11, 20), intraday_ts, "1d"),
    )

    # Reference value from ollama/market_briefing.py's reader
    with engine.connect() as conn:
        briefing_vix = _read_yf_snapshot_value(conn, "YF:^VIX:close", pinned_now)
    assert briefing_vix is not None
    assert briefing_vix["value"] == pytest.approx(26.0)
    assert briefing_vix["date"] == "2026-11-19"

    # sentiment_scorer must match market_briefing (2026-11-19 close), never 2026-11-20 intraday.
    vol = sentiment_scorer._score_volatility(engine)
    assert vol.raw_value == pytest.approx(26.0)
    assert "2026-11-19" in vol.detail

    vts = sentiment_scorer._score_vol_term_structure(engine)
    assert vts.raw_value == pytest.approx(26.0 / 24.0)
    assert "VIX/VIX3M spot term-structure slope" in vts.detail
    assert "inverted" in vts.detail

    fg = sentiment_scorer._score_fear_greed(engine)
    expected_fg = 100 - (26.0 - 10) * (100 / 30)
    assert fg.raw_value == pytest.approx(expected_fg)

    mom = sentiment_scorer._score_momentum(engine)
    # Nov 19 close (5680) vs 5 sessions earlier Nov 12 (5780) -> negative, NOT +14% from 6500 intraday.
    assert mom.raw_value < 0.0
    assert mom.score < 0.0


def test_sentiment_yf_reads_fail_closed_when_only_intraday_exists(monkeypatch):
    """If only an INCOMPLETE/unmarked capture exists, yfinance components must abstain."""
    engine = _make_engine(preserve_ts_text=True)
    pinned_now = datetime(2026, 11, 20, 16, 0, 0, tzinfo=timezone.utc)

    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return pinned_now if tz is not None else pinned_now.replace(tzinfo=None)

    monkeypatch.setattr(sentiment_scorer, "datetime", _FrozenDatetime)

    intraday_ts = datetime(2026, 11, 20, 15, 30, 0, tzinfo=timezone.utc)
    _insert_raw(
        engine, "YF:^VIX:close", date(2026, 11, 20), 15.0, source_id=1,
        pull_ts=intraday_ts.isoformat(sep=" "),
        payload=raw_capture_payload("^VIX", "close", date(2026, 11, 20), intraday_ts, "1d"),
    )

    assert sentiment_scorer._score_volatility(engine).detail == "No VIX data available"
    assert sentiment_scorer._score_fear_greed(engine).detail == "No data available"


# ── Follow-up Defect 1: _score_trend with < 200 vs >= 200 policy sessions ─


def _seed_gspc_policy_sessions(engine, values: list[float], end_date: date = date(2026, 11, 20)):
    """Seed ``len(values)`` policy-eligible NYSE trading-day closes ending on ``end_date``."""
    # Start early enough to cover >= 250 NYSE trading days prior to end_date.
    all_days = trading_days_between(end_date - timedelta(days=420), end_date)
    days = all_days[-len(values):]
    assert len(days) == len(values)
    for d, val in zip(days, values):
        captured = datetime(d.year, d.month, d.day, 6, 0, 0, tzinfo=timezone.utc) + timedelta(days=1)
        _insert_raw(
            engine,
            "YF:^GSPC:close",
            d,
            val,
            source_id=1,
            pull_ts=captured.isoformat(sep=" "),
            payload=daily_capture_payload("^GSPC", "close", d, captured),
        )


def _freeze_sentiment_now(monkeypatch, pinned_now: datetime):
    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return pinned_now if tz is not None else pinned_now.replace(tzinfo=None)

    monkeypatch.setattr(sentiment_scorer, "datetime", _FrozenDatetime)


def test_score_trend_140_sessions_scores_50d_only_without_200d_claim(monkeypatch):
    """With 50 <= n < 200 sessions (e.g. 140), score 50d alone (+0.3/-0.3) and make no 200d claim."""
    _freeze_sentiment_now(monkeypatch, datetime(2026, 11, 21, 12, 0, 0, tzinfo=timezone.utc))

    # 140 rising sessions -> latest > ma50
    eng_up = _make_engine(preserve_ts_text=True)
    _seed_gspc_policy_sessions(eng_up, [5000.0 + i * 2.0 for i in range(140)])
    comp_up = sentiment_scorer._score_trend(eng_up)
    assert comp_up.score == pytest.approx(0.3)
    assert "200d MA unavailable (140 sessions)" in comp_up.detail
    assert "below 200d" not in comp_up.detail.lower()
    assert "above 200d" not in comp_up.detail.lower()
    assert "recovery" not in comp_up.detail.lower()
    assert not sentiment_scorer._is_unavailable_component(comp_up)

    # 140 falling sessions -> latest < ma50
    eng_dn = _make_engine(preserve_ts_text=True)
    _seed_gspc_policy_sessions(eng_dn, [6000.0 - i * 2.0 for i in range(140)])
    comp_dn = sentiment_scorer._score_trend(eng_dn)
    assert comp_dn.score == pytest.approx(-0.3)
    assert "200d MA unavailable (140 sessions)" in comp_dn.detail
    assert "below 50d & 200d" not in comp_dn.detail.lower()
    assert "downtrend" not in comp_dn.detail.lower()
    assert not sentiment_scorer._is_unavailable_component(comp_dn)

    # Fewer than 50 sessions -> abstain convention preserved (ends in "available")
    eng_short = _make_engine(preserve_ts_text=True)
    _seed_gspc_policy_sessions(eng_short, [5000.0 + i for i in range(40)])
    comp_short = sentiment_scorer._score_trend(eng_short)
    assert comp_short.score == 0.0
    assert comp_short.detail == "No data available"
    assert sentiment_scorer._is_unavailable_component(comp_short)


def test_score_trend_210_sessions_computes_full_200d_ma(monkeypatch):
    """With 210 policy-eligible sessions (>= 200), _score_trend computes both 50d and 200d MAs."""
    _freeze_sentiment_now(monkeypatch, datetime(2026, 11, 21, 12, 0, 0, tzinfo=timezone.utc))

    # Strong uptrend: above 50d & 200d
    eng_strong = _make_engine(preserve_ts_text=True)
    _seed_gspc_policy_sessions(eng_strong, [5000.0 + i * 2.0 for i in range(210)])
    comp_strong = sentiment_scorer._score_trend(eng_strong)
    assert comp_strong.score == pytest.approx(0.8)
    assert comp_strong.detail == "SPY above 50d & 200d MA (strong uptrend)"

    # Recovery: above 50d, below 200d
    eng_rec = _make_engine(preserve_ts_text=True)
    _seed_gspc_policy_sessions(eng_rec, [6000.0] * 160 + [5000.0] * 49 + [5200.0])
    comp_rec = sentiment_scorer._score_trend(eng_rec)
    assert comp_rec.score == pytest.approx(0.3)
    assert comp_rec.detail == "SPY above 50d but below 200d MA (recovery)"

    # Pullback: below 50d, above 200d
    eng_pb = _make_engine(preserve_ts_text=True)
    _seed_gspc_policy_sessions(eng_pb, [4000.0] * 160 + [6000.0] * 49 + [5500.0])
    comp_pb = sentiment_scorer._score_trend(eng_pb)
    assert comp_pb.score == pytest.approx(-0.2)
    assert comp_pb.detail == "SPY below 50d but above 200d MA (pullback)"

    # Downtrend: below 50d & 200d
    eng_dn = _make_engine(preserve_ts_text=True)
    _seed_gspc_policy_sessions(eng_dn, [6000.0 - i * 2.0 for i in range(210)])
    comp_dn = sentiment_scorer._score_trend(eng_dn)
    assert comp_dn.score == pytest.approx(-0.8)
    assert comp_dn.detail == "SPY below 50d & 200d MA (downtrend)"


# ── Follow-up Defects 2 & 3: 13F unit cutover & delta flow values ─────────


def _make_13f_xml(entries: list[tuple[str, str, int, int]]) -> bytes:
    """Build a minimal SEC Form 13F infotable XML payload."""
    parts = ['<?xml version="1.0" encoding="UTF-8"?><informationTable>']
    for name, cusip, val, shares in entries:
        parts.append(
            "<infoTable>"
            f"<nameOfIssuer>{name}</nameOfIssuer>"
            f"<cusip>{cusip}</cusip>"
            f"<value>{val}</value>"
            f"<shrsOrPrnAmt><sshPrnamt>{shares}</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>"
            "</infoTable>"
        )
    parts.append("</informationTable>")
    return "".join(parts).encode("utf-8")


def _make_13f_cover(period: str, entries: int, value_total: int) -> bytes:
    """Minimal 13F-HR cover page (primary_doc.xml): period and summary totals."""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler">'
        "<headerData><submissionType>13F-HR</submissionType></headerData>"
        f"<formData><coverPage><reportCalendarOrQuarter>{period}</reportCalendarOrQuarter>"
        "<isAmendment>false</isAmendment></coverPage>"
        f"<summaryPage><tableEntryTotal>{entries}</tableEntryTotal>"
        f"<tableValueTotal>{value_total}</tableValueTotal></summaryPage></formData>"
        "</edgarSubmission>"
    ).encode("utf-8")


def _fake_edgar_get(xml_by_accession: dict[str, bytes], cover_by_accession: dict[str, bytes] | None = None):
    """Offline mock for ``requests.get`` serving EDGAR index.json, the cover
    page (when given) and infotable.xml."""
    covers = cover_by_accession or {}

    def _get(url: str, **_kwargs):
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        if url.endswith("/index.json"):
            items = [{"name": "infotable.xml"}]
            if any(acc.replace("-", "") in url for acc in covers):
                items.insert(0, {"name": "primary_doc.xml"})
            resp.json.return_value = {"directory": {"item": items}}
            return resp
        for acc, xml_bytes in xml_by_accession.items():
            if acc.replace("-", "") in url:
                resp.content = covers[acc] if url.endswith("/primary_doc.xml") else xml_bytes
                return resp
        raise AssertionError(f"unexpected URL: {url}")
    return _get


def test_13f_per_filing_multiplier_both_sides_of_2023_cutover(monkeypatch):
    """SEC 13F <value> is in thousands before 2023-01-03 and whole dollars on/after 2023-01-03."""
    puller = institutional_flows.InstitutionalFlowsPuller.__new__(
        institutional_flows.InstitutionalFlowsPuller
    )
    xml_by_acc = {
        "0001067983-22-000010": _make_13f_xml([("APPLE INC", "037833100", 100_000, 500_000)]),
        "0001067983-23-000001": _make_13f_xml([("APPLE INC", "037833100", 100_000, 500_000)]),
        "0001067983-23-000002": _make_13f_xml([("APPLE INC", "037833100", 100_000_000, 500_000)]),
        "0001067983-24-000010": _make_13f_xml([("APPLE INC", "037833100", 100_000_000, 500_000)]),
    }
    monkeypatch.setattr(institutional_flows.requests, "get", _fake_edgar_get(xml_by_acc))

    pre_nov = puller._fetch_13f_holdings("1067983", "0001067983-22-000010", filing_date="2022-11-14")
    pre_jan2 = puller._fetch_13f_holdings("1067983", "0001067983-23-000001", filing_date=date(2023, 1, 2))
    on_cutover = puller._fetch_13f_holdings("1067983", "0001067983-23-000002", filing_date="2023-01-03")
    post_cutover = puller._fetch_13f_holdings("1067983", "0001067983-24-000010", filing_date=date(2024, 2, 14))

    assert pre_nov[0]["value_usd"] == 100_000_000
    assert pre_jan2[0]["value_usd"] == 100_000_000
    assert on_cutover[0]["value_usd"] == 100_000_000
    assert post_cutover[0]["value_usd"] == 100_000_000


def test_13f_straddling_cutover_pair_creates_no_spurious_change_event(monkeypatch):
    """A filing pair straddling 2023-01-03 applies per-accession multipliers without false deltas."""
    engine = _make_engine()
    puller = institutional_flows.InstitutionalFlowsPuller.__new__(
        institutional_flows.InstitutionalFlowsPuller
    )
    puller.engine = engine
    puller.source_id = 2

    curr_acc = "0001067983-23-000010"
    prev_acc = "0001067983-22-000099"
    # Both filings represent AAPL = $100M unchanged; MSFT moves $100M -> $150M (+50%).
    # NVDA is opened in the post-cutover filing ($30M) and IBM is closed out of
    # the pre-cutover filing ($20M, stated as 20,000 thousands).
    # Pre-cutover (2022-11-14) XML is in thousands; post-cutover (2023-02-14) XML is in whole dollars.
    xml_by_acc = {
        curr_acc: _make_13f_xml([
            ("APPLE INC", "037833100", 100_000_000, 500_000),
            ("MICROSOFT CORP", "594918104", 150_000_000, 600_000),
            ("NVIDIA CORP", "67066G104", 30_000_000, 100_000),
        ]),
        prev_acc: _make_13f_xml([
            ("APPLE INC", "037833100", 100_000, 500_000),
            ("MICROSOFT CORP", "594918104", 100_000, 400_000),
            ("IBM", "459200101", 20_000, 100_000),
        ]),
    }

    inserted_raw: list[dict] = []
    monkeypatch.setattr(institutional_flows, "TOP_13F_FILERS", {"1067983": "Berkshire Hathaway Inc"})
    monkeypatch.setattr(institutional_flows.time, "sleep", lambda *_a, **_k: None)
    # Cover pages: the writer compares only filings read in full (entry count
    # matches tableEntryTotal); the periods come from the cover page here.
    covers = {
        curr_acc: _make_13f_cover("12-31-2022", 3, 280_000_000),
        prev_acc: _make_13f_cover("09-30-2022", 3, 220_000),
    }
    monkeypatch.setattr(institutional_flows.requests, "get", _fake_edgar_get(xml_by_acc, covers))
    monkeypatch.setattr(
        puller,
        "_fetch_13f_index",
        lambda _cik, count=2: [
            {"accession": curr_acc, "filing_date": "2023-02-14", "primary_doc": "primary.xml"},
            {"accession": prev_acc, "filing_date": "2022-11-14", "primary_doc": "primary.xml"},
        ],
    )
    monkeypatch.setattr(puller, "_row_exists", lambda *_a, **_k: False)
    monkeypatch.setattr(
        puller,
        "_insert_raw",
        lambda *, conn, series_id, obs_date, value, raw_payload=None: inserted_raw.append({
            "series_id": series_id,
            "obs_date": obs_date,
            "value": value,
            "raw_payload": raw_payload,
        }),
    )

    results = puller._pull_13f_filings()
    assert len(results) == 1
    assert results[0]["status"] == "SUCCESS"
    assert results[0]["changes_detected"] == 3

    # One filing comparison receipt: both sides read in full and compared.
    receipts = [r for r in inserted_raw if r["series_id"].startswith("13F-FILING:")]
    assert [r["series_id"] for r in receipts] == ["13F-FILING:1067983:0001067983-23-000010"]
    receipt = receipts[0]["raw_payload"]
    assert receipt["comparison_status"] == "compared"
    assert receipt["curr_snapshot"]["status"] == receipt["prev_snapshot"]["status"] == "complete"
    assert (receipt["report_date"], receipt["prev_report_date"]) == ("2022-12-31", "2022-09-30")
    assert receipt["qualified_for_directional_flow"] is False

    # AAPL unchanged across cutover -> no spurious event. Every value below is
    # in dollars: a wrong multiplier would read 1000x too small or too large.
    by_series = {r["series_id"]: r for r in inserted_raw if r["series_id"].startswith("13F:")}
    assert set(by_series) == {
        "13F:1067983:67066G104:NEW",
        "13F:1067983:459200101:CLOSED",
        "13F:1067983:594918104:INCREASED",
    }

    new_row = by_series["13F:1067983:67066G104:NEW"]
    assert new_row["value"] == pytest.approx(30_000_000.0)
    assert new_row["raw_payload"]["curr_value_usd"] == 30_000_000
    assert new_row["raw_payload"]["value_multiplier"] == 1

    closed_row = by_series["13F:1067983:459200101:CLOSED"]
    assert closed_row["value"] == pytest.approx(20_000_000.0)
    assert closed_row["raw_payload"]["prev_value_usd"] == 20_000_000
    assert closed_row["raw_payload"]["prev_value_multiplier"] == 1000

    # MSFT is a continuing position: the $50M share-based estimate (200k x $250).
    incr_row = by_series["13F:1067983:594918104:INCREASED"]
    assert incr_row["value"] == pytest.approx(50_000_000.0)
    assert incr_row["raw_payload"]["curr_value_usd"] == 150_000_000
    assert incr_row["raw_payload"]["prev_value_usd"] == 100_000_000

    # 13F is non-directional (owner decision 2026-11-23): every change is an
    # observed filing holdings change, none is qualified as a direction.
    expected_labels = {
        "13F:1067983:67066G104:NEW": ("no_identity_coverage", "filing_appearance"),
        "13F:1067983:459200101:CLOSED": ("no_identity_coverage", "filing_disappearance"),
        "13F:1067983:594918104:INCREASED": ("no_corporate_action_coverage", "filing_holding_change"),
    }
    for series_id, (reason, kind) in expected_labels.items():
        payload = by_series[series_id]["raw_payload"]
        assert payload["qualified_for_directional_flow"] is False, series_id
        assert payload["unqualified_reason"] == reason, series_id
        assert payload["observation_kind"] == kind, series_id
        assert payload["measure"] == "observed_filing_holdings_change_at_reference_prices", series_id

    # No 13F row reaches signal_sources (13F is non-directional): the old
    # 'institutional' / NET_POSITION_DELTA rows fed wealth_tracker and
    # pocket_lining as dollar-valued fund flows. The labelled observations
    # above (raw_series) are the holdings-observation path.
    with engine.connect() as conn:
        sig_count = conn.execute(text("SELECT COUNT(*) FROM signal_sources")).scalar_one()
    assert sig_count == 0


def test_13f_compare_holdings_uses_delta_for_increased_decreased_and_keeps_new_closed():
    """INCREASED/DECREASED emit abs(curr - prev) with curr/prev retained; NEW/CLOSED unchanged."""
    puller = institutional_flows.InstitutionalFlowsPuller.__new__(
        institutional_flows.InstitutionalFlowsPuller
    )
    prev = {
        "CLOSED001": {"name": "EXITED CO", "value_usd": 18_000_000, "shares": "100000"},
        "INCR00001": {"name": "GROWING CO", "value_usd": 100_000_000, "shares": "500000"},
        "DECR00001": {"name": "TRIMMED CO", "value_usd": 100_000_000, "shares": "500000"},
        "FLAT00001": {"name": "STEADY CO", "value_usd": 100_000_000, "shares": "500000"},
    }
    curr = {
        "NEW000001": {"name": "FRESH CO", "value_usd": 25_000_000, "shares": "200000"},
        "INCR00001": {"name": "GROWING CO", "value_usd": 140_000_000, "shares": "700000"},
        "DECR00001": {"name": "TRIMMED CO", "value_usd": 65_000_000, "shares": "325000"},
        "FLAT00001": {"name": "STEADY CO", "value_usd": 110_000_000, "shares": "550000"},
    }

    changes = puller._compare_holdings(prev, curr)
    by_cusip = {c["cusip"]: c for c in changes}

    assert set(by_cusip) == {"NEW000001", "CLOSED001", "INCR00001", "DECR00001"}

    assert by_cusip["NEW000001"]["action"] == "NEW"
    assert by_cusip["NEW000001"]["value_usd"] == 25_000_000

    assert by_cusip["CLOSED001"]["action"] == "CLOSED"
    assert by_cusip["CLOSED001"]["value_usd"] == 18_000_000

    assert by_cusip["INCR00001"]["action"] == "INCREASED"
    assert by_cusip["INCR00001"]["value_usd"] == 40_000_000
    assert by_cusip["INCR00001"]["curr_value_usd"] == 140_000_000
    assert by_cusip["INCR00001"]["prev_value_usd"] == 100_000_000
    assert by_cusip["INCR00001"]["pct_change"] == pytest.approx(0.4)

    assert by_cusip["DECR00001"]["action"] == "DECREASED"
    assert by_cusip["DECR00001"]["value_usd"] == 35_000_000
    assert by_cusip["DECR00001"]["curr_value_usd"] == 65_000_000
    assert by_cusip["DECR00001"]["prev_value_usd"] == 100_000_000
    assert by_cusip["DECR00001"]["pct_change"] == pytest.approx(-0.35)
