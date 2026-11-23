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
        Column("created_at", DateTime, server_default=text("CURRENT_TIMESTAMP")),
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
    and share decrease + price rally still classifies as DECREASED, not INCREASED.

    Both are continuing positions: the estimate is recorded, but with no
    CUSIP-level corporate-action coverage a split cannot be ruled out, so
    neither is qualified as a directional flow."""
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
    assert aapl["qualified_for_directional_flow"] is False
    assert aapl["unqualified_reason"] == "no_corporate_action_coverage"

    msft = by_cusip["594918104"]
    assert msft["action"] == "DECREASED"
    assert msft["delta_shares"] == -200_000
    assert msft["valuation_price_usd"] == pytest.approx(240.0)
    assert msft["value_usd"] == pytest.approx(48_000_000.0)
    assert msft["holdings_value_delta_usd"] == pytest.approx(-28_000_000.0)
    assert msft["qualified_for_directional_flow"] is False
    assert msft["unqualified_reason"] == "no_corporate_action_coverage"


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
    explicitly disqualified rows and continuing-position (INCREASED/DECREASED)
    rows are quarantined with logged reasons and counts."""
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
    # 7. Well-formed share-based INCREASED (continuing position): a split
    #    cannot be ruled out, so it is quarantined, not an inflow.
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
    # 8. One VALID qualified share-based NEW row -> ONLY this row must survive!
    _insert_raw(
        engine,
        "13F:0001067983:594918104:NEW",
        filing_day,
        24_000_000.0,
        payload=_qualified_13f_payload(
            ticker="MSFT",
            cusip="594918104",
            prev_shares=0,
            curr_shares=100_000,
            curr_value_usd=24_000_000.0,  # 100k * $240 = $24M
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
    assert summary[0]["ticker"] == "MSFT"
    assert summary[0]["direction"] == "inflow"
    assert summary[0]["amount_usd"] == pytest.approx(24_000_000.0)
    assert summary[0]["confidence"] == "derived"
    assert summary.quarantined_13f == 7
    assert sum(summary.quarantined_13f_reasons.values()) == 7
    assert summary.quarantined_13f_reasons["no_corporate_action_coverage"] == 1
    assert summary.quarantined_13f_reasons["failed_qualification"] == 1
    assert summary.to_dict()["quarantined_13f"] == 7
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

    # Original 13F-HR (filed Aug 14 for Q2 2026-06-30): AAPL NEW +$50M, MSFT NEW +$20M
    _insert_raw(
        engine,
        "13F:0001067983:037833100:NEW",
        orig_day,
        50_000_000.0,
        pull_ts=datetime(2026, 8, 14, 18, 0, 0),
        payload=_qualified_13f_payload(
            ticker="AAPL",
            cusip="037833100",
            prev_shares=0,
            curr_shares=250_000,
            curr_value_usd=50_000_000.0,  # 250k x $200
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
    # Restated 13F-HR/A (filed Aug 28 for SAME Q2 2026-06-30): AAPL NEW revised to +$30M
    _insert_raw(
        engine,
        "13F:0001067983:037833100:NEW",
        amend_day,
        30_000_000.0,
        pull_ts=datetime(2026, 8, 28, 18, 0, 0),
        payload=_qualified_13f_payload(
            ticker="AAPL",
            cusip="037833100",
            prev_shares=0,
            curr_shares=150_000,
            curr_value_usd=30_000_000.0,  # 150k x $200
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
    """An amendment that flips the action (INCREASED -> CLOSED, CLOSED -> a
    restated unchanged holding) supersedes the original rather than leaving
    both rows active.

    The directional sides are NEW/CLOSED: a continuing position
    (INCREASED/DECREASED) is never a verified direction. A failed supersession
    would show up either as the original's CLOSED outflow leaking through or as
    the original's INCREASED row being quarantined instead of superseded."""
    engine = _make_engine()
    orig_day = date(2026, 8, 14)
    amend_day = date(2026, 8, 28)

    # Filer 1: original INCREASED (500k -> 750k); the amendment restates the
    # quarter-end holding to zero, flipping it to CLOSED: 500k x $200 = $100M out.
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
        "13F:0001067983:037833100:CLOSED",
        amend_day,
        100_000_000.0,
        pull_ts=datetime(2026, 8, 28, 18, 0, 0),
        payload=_qualified_13f_payload(
            ticker="AAPL", cusip="037833100",
            prev_shares=500_000, curr_shares=0,
            prev_value_usd=100_000_000.0,
            report_date="2026-06-30", filing_date="2026-08-28",
            form_type="13F-HR/A", is_amendment=True, filing_accession="acc-1-amend",
        ),
    )

    # Filer 2: original CLOSED (200k x $400 = $80M out); the amendment restates
    # the holding as still held with unchanged shares (0 flow).
    _insert_raw(
        engine,
        "13F:0001336528:594918104:CLOSED",
        orig_day,
        80_000_000.0,
        pull_ts=datetime(2026, 8, 14, 18, 0, 0),
        payload=_qualified_13f_payload(
            ticker="MSFT", cusip="594918104",
            prev_shares=200_000, curr_shares=0,
            prev_value_usd=80_000_000.0,
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

    skips: dict[str, int] = {}
    reasons: dict[str, int] = {}
    flows = dollar_flows._normalize_13f_flows(
        engine, days=60, as_of=date(2026, 9, 1), cusip_map=None,
        skipped_counts=skips, quarantine_reasons=reasons,
    )
    assert len(flows) == 1
    assert flows[0]["ticker"] == "AAPL"
    assert flows[0]["direction"] == "outflow"
    assert flows[0]["amount_usd"] == pytest.approx(100_000_000.0)
    assert flows[0]["evidence"]["is_amendment"] is True
    # Both originals were superseded (not quarantined, not counted).
    assert skips.get("superseded_13f_vintages", 0) == 2
    assert skips.get("zero_share_delta_13f", 0) == 1
    assert reasons == {}


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


# ── Codex HOLD on #1369 head 4b66f327, REQUEST_CHANGES on 78679e80 ─────────
# (1) a same-CUSIP split must not read as a trade: with no CUSIP-level
# corporate-action coverage, a continuing position's share change
# (INCREASED/DECREASED) is never a verified direction, while NEW/CLOSED
# (split-invariant) stay directional; (2) every served reader of 13F
# dollar_flows rows qualifies them, refuses rows with no capture clock
# (created_at NULL), refuses historical replay, and keeps no qualified
# coverage unknown.


def _holding(value_usd: float, shares: int) -> dict:
    return {"name": "SPLITCO", "value_usd": value_usd, "shares": str(shares),
            "share_type": "SH", "value_units": "usd"}


def _raw_payload_from_change(chg: dict, filing_day: date) -> dict:
    """The raw_series payload the 13F puller writes for one _compare_holdings change."""
    payload = {k: chg.get(k) for k in (
        "cusip", "action", "estimated_flow_usd", "holdings_value_delta_usd",
        "curr_value_usd", "prev_value_usd", "pct_change", "shares", "curr_shares",
        "prev_shares", "delta_shares", "valuation_price_usd", "valuation_price_basis",
        "flow_estimation_method", "value_units", "share_type",
        "qualified_for_directional_flow", "unqualified_reason",
    )}
    payload.update({
        "manager_name": "Split Fund", "issuer_name": chg.get("name", ""),
        "value_multiplier": 1, "filing_accession": "0009999999-26-000001",
        "filing_date": filing_day.isoformat(), "report_date": "2026-09-30",
        "form_type": "13F-HR", "is_amendment": False,
    })
    return payload


_SPLIT_CASES = {
    # (prev value, prev shares) -> (curr value, curr shares)
    "split_2_for_1": ((10_000, 100), (10_000, 200)),            # 100 x $100 -> 200 x $50, no trade
    "reverse_1_for_10": ((5_000, 1_000), (5_000, 100)),         # 1000 x $5 -> 100 x $50, no trade
    "split_plus_buy": ((10_000, 100), (12_500, 250)),           # 2-for-1 plus a 50-share buy
    "split_flat_shares": ((10_000, 100), (5_000, 100)),         # 2-for-1 plus a sale leaving shares flat
    "masked_split_plus_70pct": ((10_000, 100), (17_000, 200)),  # 2-for-1 plus +70%: $100 -> $85
}


@pytest.mark.parametrize("case", list(_SPLIT_CASES), ids=list(_SPLIT_CASES))
def test_13f_split_like_transition_is_quarantined_not_read_as_a_trade(case):
    """A split, a reverse split, a split plus a trade, and a split masked by a
    price move (2-for-1 plus +70% reads $100 -> $85 a share) never yield a
    qualified directional change from _compare_holdings and never produce a
    dollar_flows directional flow. The NEW and CLOSED positions in the same
    filing pair are split-invariant and still flow."""
    prev, curr = _SPLIT_CASES[case]
    puller = institutional_flows.InstitutionalFlowsPuller.__new__(institutional_flows.InstitutionalFlowsPuller)
    counts: dict[str, int] = {}
    changes = puller._compare_holdings(
        {"C1": _holding(*prev), "CLOSED01": _holding(18_000, 100)},
        {"C1": _holding(*curr), "NEW00001": _holding(25_000, 200)},
        quarantine_counts=counts,
    )
    by_cusip = {c["cusip"]: c for c in changes}
    assert counts == {}

    split_chg = by_cusip.get("C1")
    if case == "split_flat_shares":
        assert split_chg is None  # zero share delta: no event at all
    else:
        assert split_chg is not None
        assert split_chg["action"] in ("INCREASED", "DECREASED")
        assert split_chg["qualified_for_directional_flow"] is False
        assert split_chg["unqualified_reason"] == "no_corporate_action_coverage"
    assert by_cusip["NEW00001"]["action"] == "NEW"
    assert by_cusip["NEW00001"]["qualified_for_directional_flow"] is True
    assert by_cusip["CLOSED01"]["action"] == "CLOSED"
    assert by_cusip["CLOSED01"]["qualified_for_directional_flow"] is True
    assert sorted(c["cusip"] for c in changes if c["qualified_for_directional_flow"] is True) == [
        "CLOSED01", "NEW00001",
    ]

    # Through raw_series into dollar_flows: only NEW and CLOSED become flows.
    engine = _make_engine()
    for chg in changes:
        _insert_raw(
            engine, f"13F:0009999999:{chg['cusip']}:{chg['action']}", YESTERDAY,
            float(chg["value_usd"]), payload=_raw_payload_from_change(chg, YESTERDAY),
        )
    reasons: dict[str, int] = {}
    flows = dollar_flows._normalize_13f_flows(engine, days=30, cusip_map=None, quarantine_reasons=reasons)
    assert {(f["evidence"]["cusip"], f["direction"], f["amount_usd"]) for f in flows} == {
        ("NEW00001", "inflow", 25_000.0),
        ("CLOSED01", "outflow", 18_000.0),
    }
    assert reasons == ({} if split_chg is None else {"failed_qualification": 1})

    # A payload that claims qualification (e.g. an older writer) is still refused.
    if split_chg is not None:
        forged = _raw_payload_from_change(split_chg, YESTERDAY)
        forged["qualified_for_directional_flow"] = True
        forged.pop("unqualified_reason")
        meta = dollar_flows._extract_13f_vintage_meta(forged, YESTERDAY)
        estimate, why = dollar_flows._qualify_and_estimate_13f_flow(
            split_chg["action"], split_chg["value_usd"], forged, YESTERDAY, meta,
        )
        assert estimate is None and why == "quarantine:no_corporate_action_coverage"


def test_13f_continuing_position_trade_keeps_its_estimate_but_is_not_directional():
    """A genuine 50-share buy at a normal price move (100 x $100 -> 150 x $110)
    cannot be told apart from a split masked by a price move, so it keeps its
    share-based estimate for the record but is never qualified as a direction."""
    puller = institutional_flows.InstitutionalFlowsPuller.__new__(institutional_flows.InstitutionalFlowsPuller)
    changes = puller._compare_holdings({"C1": _holding(10_000, 100)}, {"C1": _holding(16_500, 150)})
    assert [(c["action"], c["delta_shares"], c["estimated_flow_usd"]) for c in changes] == [("INCREASED", 50, 5_500.0)]
    assert changes[0]["qualified_for_directional_flow"] is False
    assert changes[0]["unqualified_reason"] == "no_corporate_action_coverage"


@pytest.mark.parametrize("action,prev_shares,curr_shares,prev_value,curr_value", [
    ("INCREASED", 100, 200, 10_000.0, 10_000.0),   # 2-for-1 split, no trade
    ("INCREASED", 100, 200, 10_000.0, 17_000.0),   # 2-for-1 masked by +70%: $100 -> $85
    ("INCREASED", 100, 200, None, 10_000.0),       # no prior quarter-end value
    ("INCREASED", 100, 150, 10_000.0, 16_500.0),   # a real buy at a normal price move
    ("DECREASED", 1_000, 100, 5_000.0, 5_000.0),   # 1-for-10 reverse split, no trade
    ("DECREASED", 100, 50, 10_000.0, 5_200.0),     # a real sale at a normal price move
], ids=["split_shaped", "masked_split_plus_70pct", "missing_prev_value", "real_buy",
        "reverse_split", "real_sale"])
def test_dollar_flows_estimate_refuses_split_shaped_and_unverifiable_rows(
    action, prev_shares, curr_shares, prev_value, curr_value,
):
    payload = _qualified_13f_payload(
        ticker="SPLT", cusip="999999999", prev_shares=prev_shares, curr_shares=curr_shares,
        prev_value_usd=prev_value, curr_value_usd=curr_value, filing_date=YESTERDAY.isoformat(),
    )
    meta = dollar_flows._extract_13f_vintage_meta(payload, YESTERDAY)
    raw_val = abs(curr_shares - prev_shares) * curr_value / curr_shares
    estimate, why = dollar_flows._qualify_and_estimate_13f_flow(action, raw_val, payload, YESTERDAY, meta)
    assert estimate is None and why == "quarantine:no_corporate_action_coverage"

    # A payload the writer already flagged unqualified quarantines earlier.
    flagged = dict(payload, qualified_for_directional_flow=False,
                   unqualified_reason="no_corporate_action_coverage")
    assert dollar_flows._qualify_and_estimate_13f_flow(action, raw_val, flagged, YESTERDAY, meta) == (
        None, "quarantine:failed_qualification",
    )


def test_dollar_flows_estimate_still_reads_new_and_closed_positions():
    """NEW and CLOSED value the whole position at one quarter-end, so a split
    cannot change them: both still estimate a directional flow."""
    new = _qualified_13f_payload(
        ticker="SPLT", cusip="999999999", prev_shares=0, curr_shares=200,
        curr_value_usd=17_000.0, filing_date=YESTERDAY.isoformat(),
    )
    est, why = dollar_flows._qualify_and_estimate_13f_flow(
        "NEW", 17_000.0, new, YESTERDAY, dollar_flows._extract_13f_vintage_meta(new, YESTERDAY),
    )
    assert why is None
    assert est["direction"] == "inflow"
    assert est["estimated_flow_usd"] == pytest.approx(17_000.0)
    assert est["valuation_price_usd"] == pytest.approx(85.0)

    closed = _qualified_13f_payload(
        ticker="SPLT", cusip="999999999", prev_shares=100, curr_shares=0,
        prev_value_usd=10_000.0, filing_date=YESTERDAY.isoformat(),
    )
    est, why = dollar_flows._qualify_and_estimate_13f_flow(
        "CLOSED", 10_000.0, closed, YESTERDAY, dollar_flows._extract_13f_vintage_meta(closed, YESTERDAY),
    )
    assert why is None
    assert est["direction"] == "outflow"
    assert est["estimated_flow_usd"] == pytest.approx(10_000.0)


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


def _written_before_now() -> datetime:
    """A tz-aware UTC capture clock a minute before the live read.

    Relative to now, not to a fixed hour: just after 00:00 UTC a fixed
    "01:00 today" is still in the future, and the layer's live cutoff is now.
    """
    return datetime.now(timezone.utc) - timedelta(minutes=1)


def test_institutional_layer_counts_only_qualified_13f_rows_known_by_as_of():
    from analysis.money_flow_engine import layer_institutional

    today = date.today()
    engine = _make_engine()
    known = _written_before_now()
    after_today = datetime.now(timezone.utc) + timedelta(days=1)  # written after the read
    _insert_flow_at(engine, 10_000_000.0, "inflow", confidence="derived", evidence=_QUALIFIED_EV, created_at=known)
    _insert_flow_at(engine, 4_000_000.0, "outflow", confidence="derived", evidence=_QUALIFIED_EV, created_at=known)
    # legacy rows: 'derived' without qualification evidence, and 'confirmed'
    _insert_flow_at(engine, 900_000_000.0, "inflow", confidence="derived", evidence={"cusip": "x"}, created_at=known)
    _insert_flow_at(engine, 800_000_000.0, "inflow", confidence="confirmed", evidence=_QUALIFIED_EV, created_at=known)
    # qualified, but written after the end of the as-of date
    _insert_flow_at(engine, 50_000_000.0, "inflow", confidence="derived", evidence=_QUALIFIED_EV, created_at=after_today)
    # qualified, but with no capture clock at all
    _insert_flow_at(engine, 70_000_000.0, "inflow", confidence="derived", evidence=_QUALIFIED_EV, created_at=None)

    node = layer_institutional._build_13f_aggregate(engine, today)
    assert node.value == pytest.approx(6_000_000.0)  # 10M inflow - 4M outflow
    assert node.metadata["coverage"] == "qualified"
    assert node.metadata["qualified"] == 2 and node.metadata["unqualified"] == 2
    # The late and NULL-clock rows carry the same qualified evidence as the
    # counted rows: they are excluded by their clock, unread.
    assert node.metadata["rows_read"] == 4
    assert node.metadata["measure"] == "estimated_holdings_position_change_at_reference_prices"
    assert node.metadata["not_observed_cash_flow"] is True
    assert "Estimated Holdings-Position Change" in node.label and "Net Flows" not in node.label

    # dollar_flows is rebuilt on refresh, so a past as-of is refused unread.
    past = layer_institutional._build_13f_aggregate(engine, today - timedelta(days=1))
    assert past.value is None and past.change_1m is None
    assert past.metadata["coverage"] == "historical_replay_unavailable"
    assert past.metadata["rows_read"] == 0


def test_institutional_layer_without_qualified_rows_is_unknown_not_zero():
    from analysis.money_flow_engine import layer_institutional

    today = date.today()
    engine = _make_engine()
    _insert_flow_at(engine, 900_000_000.0, "inflow", confidence="derived", evidence={"cusip": "x"},
                    created_at=_written_before_now())
    # A qualified row with no capture clock supplies no coverage either.
    _insert_flow_at(engine, 10_000_000.0, "inflow", confidence="derived", evidence=_QUALIFIED_EV, created_at=None)
    node = layer_institutional._build_13f_aggregate(engine, today)
    assert node.value is None
    assert node.metadata["coverage"] == "no_qualified_13f_rows"
    assert node.metadata["rows_read"] == 1
    empty = layer_institutional._build_13f_aggregate(_make_engine(), today)
    assert empty.value is None and empty.metadata["rows_read"] == 0
    assert empty.metadata["coverage"] == "no_qualified_13f_rows"


def test_aggregate_flows_refuses_historical_13f_replay_and_counts_live_rows():
    """dollar_flows is rebuilt (range delete + re-insert) on each refresh, so
    the 13F rows visible at a past date are not preserved: a historical as_of
    counts every 13F row as quarantined, while the live read counts the
    qualified ones and non-13F rows are read either way. A 13F row with no
    capture clock is not read at all."""
    engine = _make_engine()
    flow_day = date.today() - timedelta(days=5)
    _insert_flow(engine, "13f", "Q", "AAPL", 10_000_000.0, "inflow", flow_day,
                 confidence="derived", evidence=_QUALIFIED_EV)
    _insert_flow(engine, "13f", "Q", "MSFT", 4_000_000.0, "outflow", flow_day,
                 confidence="derived", evidence=_QUALIFIED_EV)
    _insert_flow(engine, "insider", "Jane", "AAPL", 1_000_000.0, "inflow", flow_day,
                 confidence="confirmed", evidence={})
    _insert_flow_at(engine, 70_000_000.0, "inflow", confidence="derived", evidence=_QUALIFIED_EV,
                    created_at=None, flow_date=flow_day)

    live = dollar_flows.get_aggregate_flows(engine, days=30)
    assert live["total_inflow"] == pytest.approx(11_000_000.0)
    assert live["total_outflow"] == pytest.approx(4_000_000.0)
    assert live["by_source_type"]["13f"]["net"] == pytest.approx(6_000_000.0)
    assert live["flow_count"] == 3
    assert live["quarantined_13f"] == 0

    replay = dollar_flows.get_aggregate_flows(engine, days=30, as_of=date.today() - timedelta(days=1))
    assert replay["quarantined_13f"] == 2
    assert "13f" not in replay["by_source_type"]
    assert replay["total_inflow"] == pytest.approx(1_000_000.0)
    assert replay["total_outflow"] == 0.0
    assert replay["flow_count"] == 1


def test_sentiment_and_momentum_readers_drop_derived_rows_without_evidence():
    """flow_aggregator (both readers) and sentiment_scorer used to keep any
    13F row tagged 'derived'; a legacy row without share-based evidence is
    now dropped from each, as is a qualified row with no capture clock
    (created_at NULL), and the qualified row is kept."""
    engine = _make_engine()
    _insert_flow(engine, "13f", "Q", "AAPL", 10_000_000.0, "inflow", YESTERDAY, confidence="derived", evidence=_QUALIFIED_EV)
    _insert_flow(engine, "13f", "L", "AAPL", 900_000_000.0, "outflow", YESTERDAY, confidence="derived", evidence={"cusip": "x"})
    _insert_flow_at(engine, 50_000_000.0, "outflow", confidence="derived", evidence=_QUALIFIED_EV, created_at=None)

    momentum = flow_aggregator.compute_flow_momentum(engine, "AAPL")
    assert momentum["flow_count"] == 1

    # _fetch_flows caches per window across engines; start and end clean.
    flow_aggregator._flow_cache.clear()
    try:
        flows = flow_aggregator._fetch_flows(engine, 30)
    finally:
        flow_aggregator._flow_cache.clear()
    assert [f["amount_usd"] for f in flows if f["source_type"] == "13f"] == [10_000_000.0]

    component = sentiment_scorer._score_flows(engine, days=7)
    assert component.raw_value == pytest.approx(1.0)  # only the qualified inflow counts
    assert "$0.0B out" in component.detail
