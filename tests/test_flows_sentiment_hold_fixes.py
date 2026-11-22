"""Tests closing Codex exact-head review HOLDs on PR #1369 (1883bcec).

Offline only (in-memory SQLite + mocks; no external DB, no network).

Covers every HOLD point:
  1. No price-driven 13F flows: share-based estimate (delta_shares * stated
     quarter-end implied price); constant shares at a higher price -> 0 flow;
     share increase + price jump separates share-based flow from mark-to-market
     gain; share decrease + price rally produces outflow, not inflow.
  2. Quarantine: legacy whole-position rows, unknown/mixed units, missing share
     counts, non-equity/PRN rows, and explicitly disqualified rows are excluded
     from directional flow totals with a logged reason and count (both at
     normalization and at aggregation).
  3. Vintage corrections: 13F-HR/A amendments replace the original filing for
     that filer-quarter (never added on top), including direction flips,
     restatements to unchanged shares, and PIT as_of preservation.
  4. VIX/VIX3M: pair only on the same latest policy-eligible observation date;
     mismatched dates or missing leg abstain with a reason and 0 weight;
     matched dates score with "VIX/VIX3M spot term-structure slope" wording
     (never futures "contango"/"backwardation") and stable output keys.
"""

from __future__ import annotations

import json
import pathlib
import sqlite3
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from loguru import logger as log
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
    create_engine,
    text,
)

from analysis import flow_aggregator
from ingestion.altdata import institutional_flows
from intelligence import dollar_flows, sentiment_scorer
from price_close_contract import daily_capture_payload

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

TODAY = date.today()
YESTERDAY = TODAY - timedelta(days=1)


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
        Column("created_at", DateTime),
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


def _insert_flow(
    engine,
    source_type,
    actor_name,
    ticker,
    amount_usd,
    direction,
    flow_date,
    *,
    confidence="derived",
    evidence=None,
):
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO dollar_flows "
                "(source_type, actor_name, ticker, amount_usd, direction, confidence, evidence, flow_date) "
                "VALUES (:st, :act, :tkr, :amt, :dir, :conf, :ev, :fd)"
            ),
            {
                "st": source_type,
                "act": actor_name,
                "tkr": ticker,
                "amt": amount_usd,
                "dir": direction,
                "conf": confidence,
                "ev": json.dumps(evidence) if isinstance(evidence, dict) else (evidence or "{}"),
                "fd": flow_date,
            },
        )


def _qualified_13f_payload(
    *,
    ticker: str,
    cusip: str,
    prev_shares: int,
    curr_shares: int,
    curr_value_usd: float | None = None,
    prev_value_usd: float | None = None,
    report_date: str | None = "2026-06-30",
    filing_date: str | None = None,
    form_type: str = "13F-HR",
    is_amendment: bool = False,
    filing_accession: str = "0001067983-26-000010",
    manager_name: str = "Berkshire Hathaway Inc",
    value_units: str = "usd",
    value_multiplier: int = 1,
    share_type: str = "SH",
    extra: dict | None = None,
) -> dict:
    delta_shares = curr_shares - prev_shares
    if curr_shares > 0 and curr_value_usd is not None:
        price = curr_value_usd / curr_shares
        method = "delta_shares_x_curr_quarter_end_price"
        basis = "curr_quarter_end_implied"
    elif prev_shares > 0 and prev_value_usd is not None:
        price = prev_value_usd / prev_shares
        method = "delta_shares_x_prev_quarter_end_price"
        basis = "prev_quarter_end_implied"
    else:
        price = None
        method = "delta_shares_x_curr_quarter_end_price"
        basis = "curr_quarter_end_implied"

    payload = {
        "manager_name": manager_name,
        "cusip": cusip,
        "ticker": ticker,
        "prev_shares": prev_shares,
        "curr_shares": curr_shares,
        "shares": curr_shares if curr_shares > 0 else prev_shares,
        "delta_shares": delta_shares,
        "curr_value_usd": curr_value_usd,
        "prev_value_usd": prev_value_usd,
        "valuation_price_usd": price,
        "valuation_price_basis": basis,
        "flow_estimation_method": method,
        "value_units": value_units,
        "value_multiplier": value_multiplier,
        "share_type": share_type,
        "qualified_for_directional_flow": True,
        "report_date": report_date,
        "filing_date": filing_date,
        "form_type": form_type,
        "is_amendment": is_amendment,
        "filing_accession": filing_accession,
    }
    if extra:
        payload.update(extra)
    return payload


def _freeze_sentiment_now(monkeypatch, pinned_now: datetime):
    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return pinned_now if tz is not None else pinned_now.replace(tzinfo=None)

    monkeypatch.setattr(sentiment_scorer, "datetime", _FrozenDatetime)


def _seed_yf_close(engine, symbol: str, obs_date: date, value: float):
    captured = datetime(obs_date.year, obs_date.month, obs_date.day, 6, 0, 0, tzinfo=timezone.utc) + timedelta(days=1)
    _insert_raw(
        engine,
        f"YF:{symbol}:close",
        obs_date,
        value,
        source_id=1,
        pull_ts=captured.isoformat(sep=" "),
        payload=daily_capture_payload(symbol, "close", obs_date, captured),
    )


# ── HOLD 1: No price-driven 13F flows (share-based estimate) ──────────────


def test_13f_unchanged_shares_with_price_increase_produces_zero_flow_in_compare_holdings():
    """Constant shares at a +50% higher price must NOT create an INCREASED flow."""
    puller = institutional_flows.InstitutionalFlowsPuller.__new__(
        institutional_flows.InstitutionalFlowsPuller
    )
    prev = {
        "037833100": {
            "name": "APPLE INC",
            "value_usd": 100_000_000,
            "shares": "500000",
            "share_type": "SH",
            "value_units": "usd",
        },
    }
    curr = {
        "037833100": {
            "name": "APPLE INC",
            "value_usd": 150_000_000,  # +50% price appreciation, same 500k shares
            "shares": "500000",
            "share_type": "SH",
            "value_units": "usd",
        },
    }

    changes = puller._compare_holdings(prev, curr)
    assert changes == []


def test_13f_share_based_estimate_separates_share_delta_from_price_move():
    """Share increase + price jump uses delta_shares * later quarter-end price,
    and share decrease + price rally still classifies as DECREASED (outflow)."""
    puller = institutional_flows.InstitutionalFlowsPuller.__new__(
        institutional_flows.InstitutionalFlowsPuller
    )
    prev = {
        # Bought +150k shares (+30%), price rose $200 -> $240:
        # Marked value delta = +$56M, share-based flow = 150k * $240 = $36M.
        "037833100": {
            "name": "APPLE INC",
            "value_usd": 100_000_000,
            "shares": "500000",
            "share_type": "SH",
            "value_units": "usd",
        },
        # Sold -200k shares (-40%), price rose $200 -> $240:
        # Marked value fell only $28M ($100M -> $72M); share-based flow is $48M OUTFLOW.
        "594918104": {
            "name": "MICROSOFT CORP",
            "value_usd": 100_000_000,
            "shares": "500000",
            "share_type": "SH",
            "value_units": "usd",
        },
    }
    curr = {
        "037833100": {
            "name": "APPLE INC",
            "value_usd": 156_000_000,
            "shares": "650000",
            "share_type": "SH",
            "value_units": "usd",
        },
        "594918104": {
            "name": "MICROSOFT CORP",
            "value_usd": 72_000_000,
            "shares": "300000",
            "share_type": "SH",
            "value_units": "usd",
        },
    }

    changes = puller._compare_holdings(prev, curr)
    by_cusip = {c["cusip"]: c for c in changes}

    assert set(by_cusip) == {"037833100", "594918104"}

    aapl = by_cusip["037833100"]
    assert aapl["action"] == "INCREASED"
    assert aapl["delta_shares"] == 150_000
    assert aapl["valuation_price_usd"] == pytest.approx(240.0)
    assert aapl["valuation_price_basis"] == "curr_quarter_end_implied"
    assert aapl["flow_estimation_method"] == "delta_shares_x_curr_quarter_end_price"
    assert aapl["value_usd"] == pytest.approx(36_000_000.0)
    assert aapl["estimated_flow_usd"] == pytest.approx(36_000_000.0)
    assert aapl["holdings_value_delta_usd"] == pytest.approx(56_000_000.0)
    assert aapl["qualified_for_directional_flow"] is True

    msft = by_cusip["594918104"]
    assert msft["action"] == "DECREASED"
    assert msft["delta_shares"] == -200_000
    assert msft["valuation_price_usd"] == pytest.approx(240.0)
    assert msft["value_usd"] == pytest.approx(48_000_000.0)
    assert msft["holdings_value_delta_usd"] == pytest.approx(-28_000_000.0)


def test_dollar_flows_13f_unchanged_shares_with_price_change_emits_zero_flow():
    """Even if raw_series holds a non-zero marked-value delta, constant shares
    (prev_shares == curr_shares) must produce zero directional flow."""
    engine = _make_engine()
    filing_day = YESTERDAY
    _insert_raw(
        engine,
        "13F:0001067983:037833100:INCREASED",
        filing_day,
        50_000_000.0,  # price-only marked value jump
        payload=_qualified_13f_payload(
            ticker="AAPL",
            cusip="037833100",
            prev_shares=500_000,
            curr_shares=500_000,  # unchanged shares!
            prev_value_usd=100_000_000.0,
            curr_value_usd=150_000_000.0,
            filing_date=filing_day.isoformat(),
        ),
    )

    skips: dict[str, int] = {}
    flows = dollar_flows._normalize_13f_flows(engine, days=30, cusip_map=None, skipped_counts=skips)
    assert flows == []
    assert skips.get("zero_share_delta_13f", 0) == 1


# ── HOLD 2: Quarantine legacy/mixed-units/unqualified 13F rows ────────────


def test_13f_quarantines_legacy_mixed_units_missing_shares_and_unqualified_rows():
    """Legacy whole-position rows, unknown/mixed units, missing shares, PRN/options,
    and explicitly disqualified rows are quarantined with logged reasons and counts."""
    engine = _make_engine()
    filing_day = YESTERDAY

    # 1. Legacy pre-fix whole-position row (no value_units, no prev_shares)
    _insert_raw(
        engine,
        "13F:0001067983:111111111:INCREASED",
        filing_day,
        250_000_000.0,
        payload={"manager_name": "Legacy Fund", "cusip": "111111111", "ticker": "NVDA", "shares": "1000000"},
    )
    # 2. Mixed/unknown units row (value_units='mixed')
    _insert_raw(
        engine,
        "13F:0001067983:222222222:NEW",
        filing_day,
        80_000_000.0,
        payload=_qualified_13f_payload(
            ticker="META",
            cusip="222222222",
            prev_shares=0,
            curr_shares=200_000,
            curr_value_usd=80_000_000.0,
            filing_date=filing_day.isoformat(),
            value_units="mixed",
        ),
    )
    # 3. Post-2023 filing stamped with pre-cutover 1000x multiplier (mixed/corrupted unit)
    _insert_raw(
        engine,
        "13F:0001067983:333333333:NEW",
        filing_day,
        90_000_000_000.0,
        payload=_qualified_13f_payload(
            ticker="GOOGL",
            cusip="333333333",
            prev_shares=0,
            curr_shares=500_000,
            curr_value_usd=90_000_000_000.0,
            filing_date=filing_day.isoformat(),
            value_units="usd",
            value_multiplier=1000,  # illegal on >= 2023-01-03 filing
        ),
    )
    # 4. Missing share counts (value_units='usd', shares=None)
    _insert_raw(
        engine,
        "13F:0001067983:444444444:INCREASED",
        filing_day,
        40_000_000.0,
        payload={
            "manager_name": "NoShares Fund",
            "cusip": "444444444",
            "ticker": "AMZN",
            "value_units": "usd",
            "value_multiplier": 1,
            "curr_value_usd": 140_000_000.0,
            "prev_value_usd": 100_000_000.0,
            "filing_date": filing_day.isoformat(),
            "report_date": "2026-06-30",
        },
    )
    # 5. Non-equity / PRN share type
    _insert_raw(
        engine,
        "13F:0001067983:555555555:NEW",
        filing_day,
        25_000_000.0,
        payload=_qualified_13f_payload(
            ticker="TSLA",
            cusip="555555555",
            prev_shares=0,
            curr_shares=25_000,
            curr_value_usd=25_000_000.0,
            filing_date=filing_day.isoformat(),
            share_type="PRN",
        ),
    )
    # 6. Explicitly disqualified row
    _insert_raw(
        engine,
        "13F:0001067983:666666666:NEW",
        filing_day,
        15_000_000.0,
        payload=_qualified_13f_payload(
            ticker="JPM",
            cusip="666666666",
            prev_shares=0,
            curr_shares=100_000,
            curr_value_usd=15_000_000.0,
            filing_date=filing_day.isoformat(),
            extra={"qualified_for_directional_flow": False},
        ),
    )
    # 7. One VALID qualified share-based row -> ONLY this row must survive!
    _insert_raw(
        engine,
        "13F:0001067983:037833100:INCREASED",
        filing_day,
        30_000_000.0,
        payload=_qualified_13f_payload(
            ticker="AAPL",
            cusip="037833100",
            prev_shares=500_000,
            curr_shares=600_000,
            prev_value_usd=100_000_000.0,
            curr_value_usd=144_000_000.0,  # $200 -> $240/sh -> 100k * $240 = $24M
            filing_date=filing_day.isoformat(),
        ),
    )

    logged: list[str] = []
    hid = log.add(lambda m: logged.append(m.record["message"]), level="WARNING")
    try:
        summary = dollar_flows.normalize_all_flows(engine, days=30, dry_run=True)
    finally:
        log.remove(hid)

    assert len(summary) == 1
    assert summary[0]["ticker"] == "AAPL"
    assert summary[0]["direction"] == "inflow"
    assert summary[0]["amount_usd"] == pytest.approx(24_000_000.0)
    assert summary[0]["confidence"] == "derived"
    assert summary.quarantined_13f == 6
    assert sum(summary.quarantined_13f_reasons.values()) == 6
    assert summary.to_dict()["quarantined_13f"] == 6
    assert any("quarantin" in msg.lower() for msg in logged)


def test_13f_missing_filing_date_fails_closed_instead_of_defaulting_multiplier():
    """Missing/unparseable filing_date must fail closed (None multiplier / unknown units)."""
    assert institutional_flows._13f_value_multiplier(None) is None
    assert institutional_flows._13f_value_multiplier("not-a-date") is None


def test_aggregators_quarantine_legacy_unrebuilt_13f_rows_in_dollar_flows():
    """Legacy pre-fix 13f rows sitting in dollar_flows (confidence='confirmed',
    unqualified evidence) must be excluded from directional totals in both
    dollar_flows.get_aggregate_flows and flow_aggregator."""
    engine = _make_engine()
    # Legacy un-rebuilt 13F row in dollar_flows (confirmed, empty evidence)
    _insert_flow(
        engine, "13f", "Legacy Fund", "AAPL", 999_000_000.0, "inflow", YESTERDAY,
        confidence="confirmed", evidence={"cusip": "037833100", "action": "INCREASED"},
    )
    # Qualified share-based 13F row in dollar_flows (derived, qualified evidence)
    _insert_flow(
        engine, "13f", "Qualified Fund", "MSFT", 25_000_000.0, "inflow", YESTERDAY,
        confidence="derived",
        evidence={
            "cusip": "594918104",
            "action": "INCREASED",
            "value_units": "usd",
            "prev_shares": 100_000,
            "curr_shares": 150_000,
            "delta_shares": 50_000,
            "valuation_price_usd": 500.0,
            "flow_estimation_method": "delta_shares_x_curr_quarter_end_price",
            "qualified_for_directional_flow": True,
        },
    )

    agg = dollar_flows.get_aggregate_flows(engine, days=30)
    assert agg["total_inflow"] == pytest.approx(25_000_000.0)
    assert agg["net_flow"] == pytest.approx(25_000_000.0)
    assert agg["quarantined_13f"] == 1

    flow_aggregator._flow_cache.clear()
    try:
        tiers = flow_aggregator.aggregate_by_actor_tier(engine, days=30)
        assert tiers["institutional"]["inflow"] == pytest.approx(25_000_000.0)
        assert tiers["institutional"]["net_flow"] == pytest.approx(25_000_000.0)
    finally:
        flow_aggregator._flow_cache.clear()


# ── HOLD 3: Vintage corrections (13F-HR/A amendments replace originals) ───


def test_fetch_13f_index_amendment_replaces_original_for_same_report_date(monkeypatch):
    """EDGAR 13F-HR/A replaces the original 13F-HR for the same reportDate so
    quarter-over-quarter comparison compares Q2-amended vs Q1, never Q2-amended vs Q2-orig."""
    puller = institutional_flows.InstitutionalFlowsPuller.__new__(
        institutional_flows.InstitutionalFlowsPuller
    )
    resp = MagicMock()
    resp.status_code = 200
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {
        "filings": {
            "recent": {
                "form": ["13F-HR/A", "13F-HR", "13F-HR"],
                "accessionNumber": [
                    "0001067983-26-000020",
                    "0001067983-26-000010",
                    "0001067983-26-000005",
                ],
                "filingDate": ["2026-08-28", "2026-08-14", "2026-05-15"],
                "reportDate": ["2026-06-30", "2026-06-30", "2026-03-31"],
                "primaryDocument": ["amend.xml", "orig.xml", "q1.xml"],
            }
        }
    }
    monkeypatch.setattr(institutional_flows.requests, "get", lambda *_a, **_k: resp)

    filings = puller._fetch_13f_index("1067983", count=2)
    # EDGAR's submissions index carries no amendment type, so the 13F-HR/A
    # can't be verified as a RESTATEMENT: the original Q2 filing stands, and
    # Q2 is still compared with Q1, never with itself.
    assert len(filings) == 2
    assert filings[0]["accession"] == "0001067983-26-000010"
    assert filings[0]["form"] == "13F-HR"
    assert filings[0]["is_amendment"] is False
    assert filings[0]["report_date"] == "2026-06-30"
    assert filings[1]["accession"] == "0001067983-26-000005"
    assert filings[1]["report_date"] == "2026-03-31"


def test_only_a_verified_restatement_replaces_the_original_13f():
    """A NEW HOLDINGS 13F-HR/A lists only added positions; using it as the
    quarter's holdings would turn every omitted position into a false CLOSED
    outflow (e.g. 5 of 100 holdings -> 95 fake sales). Only a verified
    RESTATEMENT may replace the original."""
    select = institutional_flows._select_latest_filings_by_report_date
    original = {"form": "13F-HR", "accession": "acc-orig", "filing_date": "2026-08-14",
                "report_date": "2026-06-30"}
    prior_q = {"form": "13F-HR", "accession": "acc-q1", "filing_date": "2026-05-15",
               "report_date": "2026-03-31"}
    for amendment_type in (None, "", "NEW HOLDINGS", "new holdings"):
        amend = {"form": "13F-HR/A", "accession": "acc-amend", "filing_date": "2026-08-28",
                 "report_date": "2026-06-30", "amendment_type": amendment_type}
        picked = select([amend, original, prior_q], count=2)
        assert [f["accession"] for f in picked] == ["acc-orig", "acc-q1"], amendment_type

    restated = {"form": "13F-HR/A", "accession": "acc-restated", "filing_date": "2026-08-28",
                "report_date": "2026-06-30", "amendment_type": "RESTATEMENT"}
    picked = select([restated, original, prior_q], count=2)
    assert [f["accession"] for f in picked] == ["acc-restated", "acc-q1"]
    assert picked[0]["is_amendment"] is True
    assert picked[0]["replaces_accession"] == "acc-orig"


def test_13f_amendment_replaces_original_in_dollar_flows_and_respects_pit():
    """A 13F-HR/A restatement for the same (cik, report_date) replaces the original
    filing rather than double counting, while PIT as_of before the amendment sees original."""
    engine = _make_engine()
    orig_day = date(2026, 8, 14)
    amend_day = date(2026, 8, 28)

    # Original 13F-HR (filed Aug 14 for Q2 2026-06-30): AAPL +$50M, MSFT +$20M
    _insert_raw(
        engine,
        "13F:0001067983:037833100:INCREASED",
        orig_day,
        50_000_000.0,
        pull_ts=datetime(2026, 8, 14, 18, 0, 0),
        payload=_qualified_13f_payload(
            ticker="AAPL",
            cusip="037833100",
            prev_shares=500_000,
            curr_shares=750_000,
            prev_value_usd=100_000_000.0,
            curr_value_usd=150_000_000.0,
            report_date="2026-06-30",
            filing_date="2026-08-14",
            form_type="13F-HR",
            is_amendment=False,
            filing_accession="0001067983-26-000010",
        ),
    )
    _insert_raw(
        engine,
        "13F:0001067983:594918104:NEW",
        orig_day,
        20_000_000.0,
        pull_ts=datetime(2026, 8, 14, 18, 0, 1),
        payload=_qualified_13f_payload(
            ticker="MSFT",
            cusip="594918104",
            prev_shares=0,
            curr_shares=50_000,
            curr_value_usd=20_000_000.0,
            report_date="2026-06-30",
            filing_date="2026-08-14",
            form_type="13F-HR",
            is_amendment=False,
            filing_accession="0001067983-26-000010",
        ),
    )
    # Restated 13F-HR/A (filed Aug 28 for SAME Q2 2026-06-30): AAPL revised to +$30M
    _insert_raw(
        engine,
        "13F:0001067983:037833100:INCREASED",
        amend_day,
        30_000_000.0,
        pull_ts=datetime(2026, 8, 28, 18, 0, 0),
        payload=_qualified_13f_payload(
            ticker="AAPL",
            cusip="037833100",
            prev_shares=500_000,
            curr_shares=650_000,
            prev_value_usd=100_000_000.0,
            curr_value_usd=130_000_000.0,
            report_date="2026-06-30",
            filing_date="2026-08-28",
            form_type="13F-HR/A",
            is_amendment=True,
            filing_accession="0001067983-26-000020",
        ),
    )

    # As of Sep 01 (after amendment): ONLY the restated AAPL $30M survives (never $80M, and MSFT replaced).
    skips_after: dict[str, int] = {}
    flows_after = dollar_flows._normalize_13f_flows(
        engine, days=60, as_of=date(2026, 9, 1), cusip_map=None, skipped_counts=skips_after,
    )
    assert len(flows_after) == 1
    assert flows_after[0]["ticker"] == "AAPL"
    assert flows_after[0]["amount_usd"] == pytest.approx(30_000_000.0)
    assert flows_after[0]["evidence"]["is_amendment"] is True
    assert skips_after.get("superseded_13f_vintages", 0) == 2

    # PIT check as of Aug 20 (before amendment was filed): sees the original $50M + $20M, no lookahead.
    flows_before = dollar_flows._normalize_13f_flows(
        engine, days=60, as_of=date(2026, 8, 20), cusip_map=None,
    )
    assert len(flows_before) == 2
    by_ticker = {f["ticker"]: f["amount_usd"] for f in flows_before}
    assert by_ticker == {"AAPL": pytest.approx(50_000_000.0), "MSFT": pytest.approx(20_000_000.0)}


def test_13f_amendment_flipping_direction_or_restating_to_zero_replaces_original():
    """An amendment that flips INCREASED -> DECREASED or restates to unchanged shares
    supersedes the original rather than leaving both rows active."""
    engine = _make_engine()
    orig_day = date(2026, 8, 14)
    amend_day = date(2026, 8, 28)

    # Filer 1: original INCREASED $50M, amendment flips to DECREASED $10M
    _insert_raw(
        engine,
        "13F:0001067983:037833100:INCREASED",
        orig_day,
        50_000_000.0,
        pull_ts=datetime(2026, 8, 14, 18, 0, 0),
        payload=_qualified_13f_payload(
            ticker="AAPL", cusip="037833100",
            prev_shares=500_000, curr_shares=750_000,
            prev_value_usd=100_000_000.0, curr_value_usd=150_000_000.0,
            report_date="2026-06-30", filing_date="2026-08-14",
            form_type="13F-HR", is_amendment=False, filing_accession="acc-1-orig",
        ),
    )
    _insert_raw(
        engine,
        "13F:0001067983:037833100:DECREASED",
        amend_day,
        10_000_000.0,
        pull_ts=datetime(2026, 8, 28, 18, 0, 0),
        payload=_qualified_13f_payload(
            ticker="AAPL", cusip="037833100",
            prev_shares=500_000, curr_shares=450_000,
            prev_value_usd=100_000_000.0, curr_value_usd=90_000_000.0,
            report_date="2026-06-30", filing_date="2026-08-28",
            form_type="13F-HR/A", is_amendment=True, filing_accession="acc-1-amend",
        ),
    )

    # Filer 2: original INCREASED $40M, amendment restates to unchanged shares (0 flow)
    _insert_raw(
        engine,
        "13F:0001336528:594918104:INCREASED",
        orig_day,
        40_000_000.0,
        pull_ts=datetime(2026, 8, 14, 18, 0, 0),
        payload=_qualified_13f_payload(
            ticker="MSFT", cusip="594918104",
            prev_shares=200_000, curr_shares=300_000,
            prev_value_usd=80_000_000.0, curr_value_usd=120_000_000.0,
            report_date="2026-06-30", filing_date="2026-08-14",
            form_type="13F-HR", is_amendment=False, filing_accession="acc-2-orig",
        ),
    )
    _insert_raw(
        engine,
        "13F:0001336528:594918104:INCREASED",
        amend_day,
        0.0,
        pull_ts=datetime(2026, 8, 28, 18, 0, 0),
        payload=_qualified_13f_payload(
            ticker="MSFT", cusip="594918104",
            prev_shares=200_000, curr_shares=200_000,  # restated unchanged!
            prev_value_usd=80_000_000.0, curr_value_usd=100_000_000.0,
            report_date="2026-06-30", filing_date="2026-08-28",
            form_type="13F-HR/A", is_amendment=True, filing_accession="acc-2-amend",
        ),
    )

    flows = dollar_flows._normalize_13f_flows(
        engine, days=60, as_of=date(2026, 9, 1), cusip_map=None,
    )
    assert len(flows) == 1
    assert flows[0]["ticker"] == "AAPL"
    assert flows[0]["direction"] == "outflow"
    assert flows[0]["amount_usd"] == pytest.approx(10_000_000.0)


# ── HOLD 4: VIX/VIX3M date-matched pairing & spot term-structure wording ──


def test_vol_term_structure_abstains_on_mismatched_vix_vix3m_dates(monkeypatch):
    """When latest VIX and VIX3M observation dates differ (even if an older shared
    date exists), vol_term_structure must abstain with a reason and 0 weight."""
    engine = _make_engine(preserve_ts_text=True)
    _freeze_sentiment_now(monkeypatch, datetime(2026, 11, 20, 16, 0, 0, tzinfo=timezone.utc))

    # Prior day (Nov 18): both VIX and VIX3M exist
    _seed_yf_close(engine, "^VIX", date(2026, 11, 18), 18.0)
    _seed_yf_close(engine, "^VIX3M", date(2026, 11, 18), 20.0)
    # Latest day (Nov 19): VIX updated to 26.0, VIX3M missing -> dates differ!
    _seed_yf_close(engine, "^VIX", date(2026, 11, 19), 26.0)

    comp = sentiment_scorer._score_vol_term_structure(engine)
    assert comp.name == "vol_term_structure"
    assert comp.score == 0.0
    assert comp.raw_value == 0.0
    assert comp.abstain_reason == "mismatched_observation_dates"
    assert sentiment_scorer._is_unavailable_component(comp)
    assert "2026-11-19" in comp.detail and "2026-11-18" in comp.detail
    assert "contango" not in comp.detail.lower()
    assert "backwardation" not in comp.detail.lower()

    # In compute_sentiment, abstained vol_term_structure gets weight 0 and remaining active renormalizes to 1.0
    monkeypatch.setattr(sentiment_scorer, "_ensure_tables", lambda _e: None)
    monkeypatch.setattr(sentiment_scorer, "_load_weights", lambda _e: (dict(sentiment_scorer.DEFAULT_WEIGHTS), 0))
    no_data = lambda name: sentiment_scorer.SentimentComponent(name, 0.0, 0.0, 0.0, "No data available")
    monkeypatch.setattr(sentiment_scorer, "_score_momentum", lambda _e: sentiment_scorer.SentimentComponent("momentum", 0.025, 0.5, 0.0, "SPY up"))
    for fn_name, comp_name in [
        ("_score_breadth", "breadth"),
        ("_score_trend", "trend"),
        ("_score_volatility", "volatility"),
        ("_score_put_call_ratio", "put_call_ratio"),
        ("_score_flows", "flows"),
        ("_score_dark_pool", "dark_pool"),
        ("_score_etf_flows", "etf_flows"),
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
    assert by_name["vol_term_structure"].weight == 0.0
    assert by_name["vol_term_structure"].abstain_reason == "mismatched_observation_dates"
    assert by_name["momentum"].weight == pytest.approx(1.0)
    assert result.score == pytest.approx(0.5)

    as_dict = result.to_dict()
    vts_dict = next(c for c in as_dict["components"] if c["name"] == "vol_term_structure")
    assert set(["name", "raw_value", "score", "weight", "detail"]).issubset(vts_dict.keys())
    assert vts_dict["abstain_reason"] == "mismatched_observation_dates"


def test_vol_term_structure_abstains_when_one_leg_missing(monkeypatch):
    """If VIX3M is absent altogether, vol_term_structure must abstain with a reason."""
    engine = _make_engine(preserve_ts_text=True)
    _freeze_sentiment_now(monkeypatch, datetime(2026, 11, 20, 16, 0, 0, tzinfo=timezone.utc))
    _seed_yf_close(engine, "^VIX", date(2026, 11, 19), 21.0)

    comp = sentiment_scorer._score_vol_term_structure(engine)
    assert comp.score == 0.0
    assert comp.abstain_reason == "missing_vix3m"
    assert sentiment_scorer._is_unavailable_component(comp)


def test_vol_term_structure_scores_matched_dates_with_spot_slope_wording(monkeypatch):
    """Matched VIX and VIX3M observation dates score cleanly and use
    'VIX/VIX3M spot term-structure slope' wording (never contango/backwardation)."""
    _freeze_sentiment_now(monkeypatch, datetime(2026, 11, 20, 16, 0, 0, tzinfo=timezone.utc))

    # Upward-sloping spot term structure: VIX=16.0 < VIX3M=20.0 -> ratio=0.80 -> score=+0.60
    eng_up = _make_engine(preserve_ts_text=True)
    _seed_yf_close(eng_up, "^VIX", date(2026, 11, 19), 16.0)
    _seed_yf_close(eng_up, "^VIX3M", date(2026, 11, 19), 20.0)

    up = sentiment_scorer._score_vol_term_structure(eng_up)
    assert up.name == "vol_term_structure"
    assert up.raw_value == pytest.approx(0.80)
    assert up.score == pytest.approx(0.60)
    assert up.abstain_reason is None
    assert up.obs_date == "2026-11-19"
    assert not sentiment_scorer._is_unavailable_component(up)
    assert "VIX/VIX3M spot term-structure slope" in up.detail
    assert "2026-11-19" in up.detail
    assert "contango" not in up.detail.lower()
    assert "backwardation" not in up.detail.lower()

    # Inverted spot term structure: VIX=26.0 > VIX3M=24.0 -> ratio=1.0833 -> score=-0.25
    eng_inv = _make_engine(preserve_ts_text=True)
    _seed_yf_close(eng_inv, "^VIX", date(2026, 11, 19), 26.0)
    _seed_yf_close(eng_inv, "^VIX3M", date(2026, 11, 19), 24.0)

    inv = sentiment_scorer._score_vol_term_structure(eng_inv)
    assert inv.raw_value == pytest.approx(26.0 / 24.0)
    assert inv.score == pytest.approx(-0.25)
    assert inv.abstain_reason is None
    assert inv.obs_date == "2026-11-19"
    assert "VIX/VIX3M spot term-structure slope" in inv.detail
    assert "contango" not in inv.detail.lower()
    assert "backwardation" not in inv.detail.lower()

    # Guard against stale contango/backwardation wording anywhere in sentiment_scorer.py
    src = pathlib.Path("intelligence/sentiment_scorer.py").read_text()
    assert "contango" not in src.lower()
    assert "backwardation" not in src.lower()


# ── Codex HOLD on #1369 head 4b66f327 ──────────────────────────────────────
# (1) a same-CUSIP split must not read as a trade; (2) every served reader of
# 13F dollar_flows rows qualifies them, honours the as-of cutoff and keeps no
# qualified coverage unknown.


def _holding(value_usd: float, shares: int) -> dict:
    return {"name": "SPLITCO", "value_usd": value_usd, "shares": str(shares),
            "share_type": "SH", "value_units": "usd"}


@pytest.mark.parametrize("prev,curr", [
    ((10_000, 100), (10_000, 200)),     # 2-for-1 split, no trade: 100 x $100 -> 200 x $50
    ((5_000, 1_000), (5_000, 100)),     # 1-for-10 reverse split: 1000 x $5 -> 100 x $50
    ((10_000, 100), (12_500, 250)),     # split plus a buy, no adjustment available
    ((10_000, 100), (5_000, 100)),      # split plus a sale leaving the share count flat
], ids=["split_2_for_1", "reverse_1_for_10", "split_plus_buy", "split_flat_shares"])
def test_13f_split_like_transition_is_quarantined_not_read_as_a_trade(prev, curr):
    puller = institutional_flows.InstitutionalFlowsPuller.__new__(institutional_flows.InstitutionalFlowsPuller)
    counts: dict[str, int] = {}
    changes = puller._compare_holdings({"C1": _holding(*prev)}, {"C1": _holding(*curr)}, quarantine_counts=counts)
    assert changes == []
    assert counts == {"possible_split_or_corporate_action": 1}


def test_13f_real_trade_at_a_normal_price_move_still_reads():
    puller = institutional_flows.InstitutionalFlowsPuller.__new__(institutional_flows.InstitutionalFlowsPuller)
    changes = puller._compare_holdings({"C1": _holding(10_000, 100)}, {"C1": _holding(16_500, 150)})
    assert [(c["action"], c["delta_shares"], c["estimated_flow_usd"]) for c in changes] == [("INCREASED", 50, 5_500.0)]
    assert institutional_flows.implied_price_suggests_corporate_action(10_000, 100, 12_400, 100) is False
    for bad in ((0, 100, 1, 1), (1, 0, 1, 1), (float("nan"), 1, 1, 1), ("x", 1, 1, 1)):
        assert institutional_flows.implied_price_suggests_corporate_action(*bad) is True


@pytest.mark.parametrize("prev_value,curr_value,reason", [
    (10_000.0, 10_000.0, "quarantine:possible_split_or_corporate_action"),
    (None, 10_000.0, "quarantine:unverifiable_corporate_action"),
], ids=["split_shaped", "missing_prev_value"])
def test_dollar_flows_estimate_refuses_split_shaped_and_unverifiable_rows(prev_value, curr_value, reason):
    payload = _qualified_13f_payload(
        ticker="SPLT", cusip="999999999", prev_shares=100, curr_shares=200,
        prev_value_usd=prev_value, curr_value_usd=curr_value, filing_date=YESTERDAY.isoformat(),
    )
    meta = dollar_flows._extract_13f_vintage_meta(payload, YESTERDAY)
    estimate, why = dollar_flows._qualify_and_estimate_13f_flow("INCREASED", 5_000.0, payload, YESTERDAY, meta)
    assert estimate is None and why == reason


_QUALIFIED_EV = {
    "value_units": "usd", "delta_shares": 1_000, "qualified_for_directional_flow": True,
    "flow_estimation_method": "delta_shares_x_curr_quarter_end_price",
}


def _insert_flow_at(engine, amount, direction, *, confidence, evidence, created_at, flow_date=YESTERDAY):
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO dollar_flows (source_type, actor_name, ticker, amount_usd, direction, "
                "confidence, evidence, flow_date, created_at) "
                "VALUES ('13f', 'F', 'AAPL', :amt, :dir, :conf, :ev, :fd, :ca)"
            ),
            {"amt": amount, "dir": direction, "conf": confidence,
             "ev": json.dumps(evidence) if evidence is not None else None, "fd": flow_date, "ca": created_at},
        )


def test_institutional_layer_counts_only_qualified_13f_rows_known_by_as_of():
    from analysis.money_flow_engine import layer_institutional

    engine = _make_engine()
    known = datetime.combine(YESTERDAY, datetime.min.time(), timezone.utc) + timedelta(hours=12)
    later = known + timedelta(days=3)
    _insert_flow_at(engine, 10_000_000.0, "inflow", confidence="derived", evidence=_QUALIFIED_EV, created_at=known)
    _insert_flow_at(engine, 4_000_000.0, "outflow", confidence="derived", evidence=_QUALIFIED_EV, created_at=known)
    # legacy rows: 'derived' without qualification evidence, and 'confirmed'
    _insert_flow_at(engine, 900_000_000.0, "inflow", confidence="derived", evidence={"cusip": "x"}, created_at=known)
    _insert_flow_at(engine, 800_000_000.0, "inflow", confidence="confirmed", evidence=_QUALIFIED_EV, created_at=known)
    # qualified, but written after the as-of date
    _insert_flow_at(engine, 50_000_000.0, "inflow", confidence="derived", evidence=_QUALIFIED_EV, created_at=later)

    node = layer_institutional._build_13f_aggregate(engine, YESTERDAY)
    assert node.value == pytest.approx(6_000_000.0)
    assert node.metadata["qualified"] == 2 and node.metadata["unqualified"] == 2
    assert node.metadata["measure"] == "estimated_holdings_position_change_at_reference_prices"
    assert node.metadata["not_observed_cash_flow"] is True
    assert "Estimated Holdings-Position Change" in node.label and "Net Flows" not in node.label

    later_node = layer_institutional._build_13f_aggregate(engine, (later + timedelta(days=1)).date())
    assert later_node.value == pytest.approx(56_000_000.0)


def test_institutional_layer_without_qualified_rows_is_unknown_not_zero():
    from analysis.money_flow_engine import layer_institutional

    engine = _make_engine()
    known = datetime.combine(YESTERDAY, datetime.min.time(), timezone.utc)
    _insert_flow_at(engine, 900_000_000.0, "inflow", confidence="derived", evidence={"cusip": "x"}, created_at=known)
    node = layer_institutional._build_13f_aggregate(engine, YESTERDAY)
    assert node.value is None
    assert node.metadata["coverage"] == "no_qualified_13f_rows"
    empty = layer_institutional._build_13f_aggregate(_make_engine(), YESTERDAY)
    assert empty.value is None and empty.metadata["rows_read"] == 0


def test_sentiment_and_momentum_readers_drop_derived_rows_without_evidence():
    """flow_aggregator (both readers) and sentiment_scorer used to keep any
    13F row tagged 'derived'; a legacy row without share-based evidence is
    now dropped from each, and the qualified row is kept."""
    engine = _make_engine()
    _insert_flow(engine, "13f", "Q", "AAPL", 10_000_000.0, "inflow", YESTERDAY, confidence="derived", evidence=_QUALIFIED_EV)
    _insert_flow(engine, "13f", "L", "AAPL", 900_000_000.0, "outflow", YESTERDAY, confidence="derived", evidence={"cusip": "x"})

    momentum = flow_aggregator.compute_flow_momentum(engine, "AAPL")
    assert momentum["flow_count"] == 1

    flows = flow_aggregator._fetch_flows(engine, 30)
    assert [f["amount_usd"] for f in flows if f["source_type"] == "13f"] == [10_000_000.0]

    component = sentiment_scorer._score_flows(engine, days=7)
    assert component.raw_value == pytest.approx(1.0)  # only the qualified inflow counts
    assert "$0.0B out" in component.detail
