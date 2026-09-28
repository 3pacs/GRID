"""Tests for GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927 §4.3: dollar_flows.py
honesty fixes.

Before this fix, ``_normalize_darkpool`` (via the removed
``_get_vwap_estimate``) fabricated a dark-pool row's dollar amount from a
hard-coded ``_DEFAULT_VWAP_ESTIMATE = 50.0`` whenever no real price was
available, and ``normalize_all_flows``/``_persist_flows`` had no upper
bound on ``signal_date``/``flow_date``, so a future-dated row could be
persisted (and, at ingestion time, ``flow_date`` for a dark-pool row could
silently fall back to ``date.today()`` instead of the row's own
``signal_date``).

Runs against an in-memory SQLite ``raw_series``/``source_catalog`` fixture
shaped like ``tests/test_store_observations.py``'s (real SQL, not mocked,
for the VWAP lookup through ``store/observations.py::read_latest``) plus a
minimal ``signal_sources`` table shaped like
``tests/test_causal_links_pg.py``'s. No real/production database is
touched.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta

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
    create_engine,
    text,
)

from intelligence import dollar_flows

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

YF_SRC = 1
TODAY = date.today()
YESTERDAY = TODAY - timedelta(days=1)
FUTURE = TODAY + timedelta(days=5)


@pytest.fixture()
def engine():
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
        Column("pull_timestamp", DateTime, nullable=False),
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
    md.create_all(eng)
    with eng.begin() as conn:
        conn.execute(text("INSERT INTO source_catalog (id, name) VALUES (1, 'yfinance')"))
    return eng


def _insert_yf_close(engine, ticker: str, obs_date: date, value: float) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO raw_series "
                "(series_id, source_id, obs_date, pull_timestamp, value, pull_status) "
                "VALUES (:sid, :src, :od, :ts, :v, 'SUCCESS')"
            ),
            {
                "sid": f"YF:{ticker}:close",
                "src": YF_SRC,
                "od": obs_date,
                "ts": datetime.combine(obs_date, datetime.min.time()),
                "v": value,
            },
        )


def _insert_signal(engine, source_type, source_id, ticker, signal_date, signal_type, signal_value) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO signal_sources "
                "(source_type, source_id, ticker, signal_date, signal_type, signal_value) "
                "VALUES (:st, :sid, :t, :d, :typ, :v)"
            ),
            {
                "st": source_type, "sid": source_id, "t": ticker,
                "d": signal_date, "typ": signal_type, "v": json.dumps(signal_value),
            },
        )


# ── _normalize_darkpool: no fabricated $50 default ─────────────────────────

def test_normalize_darkpool_skips_when_no_vwap_not_fabricated(engine):
    """No YF close exists for this ticker/date — the row must be dropped,
    never priced with the old $50 default."""
    row = {
        "source_id": "finra_ats",
        "ticker": "ZZZZ",
        "signal_type": "VOLUME_SPIKE",
        "signal_date": YESTERDAY,
        "signal_value": json.dumps({"volume": 100_000, "spike_ratio": 3.2}),
    }
    flow, skip_reason = dollar_flows._normalize_darkpool(row, engine)
    assert flow is None
    assert skip_reason == "no_vwap"


def test_normalize_darkpool_uses_real_vwap_and_records_obs_date(engine):
    """A real accepted YF close drives the amount, and its own obs_date is
    recorded in evidence — never a fabricated price, never dated today."""
    _insert_yf_close(engine, "ZZZZ", YESTERDAY, 123.45)

    row = {
        "source_id": "finra_ats",
        "ticker": "ZZZZ",
        "signal_type": "VOLUME_SPIKE",
        "signal_date": YESTERDAY,
        "signal_value": json.dumps({"volume": 1_000, "spike_ratio": 3.2}),
    }
    flow, skip_reason = dollar_flows._normalize_darkpool(row, engine)

    assert skip_reason is None
    assert flow is not None
    assert flow["amount_usd"] == pytest.approx(1_000 * 123.45)
    assert flow["amount_usd"] != pytest.approx(1_000 * 50.0), "must not use the old $50 default"
    assert flow["evidence"]["vwap"] == pytest.approx(123.45)
    assert flow["evidence"]["vwap_obs_date"] == YESTERDAY.isoformat()
    # flow_date must come from the row's own signal_date, never date.today()
    assert flow["flow_date"] == YESTERDAY


def test_normalize_darkpool_missing_signal_date_is_dropped_not_defaulted_to_today(engine):
    """A row with no signal_date must be dropped, never priced/dated using
    date.today() as a stand-in."""
    row = {
        "source_id": "finra_ats",
        "ticker": "ZZZZ",
        "signal_type": "VOLUME_SPIKE",
        "signal_date": None,
        "signal_value": json.dumps({"volume": 1_000}),
    }
    flow, skip_reason = dollar_flows._normalize_darkpool(row, engine)
    assert flow is None
    assert skip_reason is None  # not a VWAP problem — there is no usable date at all


def test_normalize_darkpool_zero_volume_is_not_counted_as_no_vwap(engine):
    """A zero/absent volume is a different reason to skip than "no VWAP" —
    the two must not be conflated in the run's counters."""
    row = {
        "source_id": "finra_ats",
        "ticker": "ZZZZ",
        "signal_type": "VOLUME_SPIKE",
        "signal_date": YESTERDAY,
        "signal_value": json.dumps({"volume": 0}),
    }
    flow, skip_reason = dollar_flows._normalize_darkpool(row, engine)
    assert flow is None
    assert skip_reason is None


# ── _normalize_darkpool: VWAP staleness bound (PR #712 review) ─────────────

def test_normalize_darkpool_skips_stale_vwap_not_priced_off_old_close(engine):
    """A VWAP observation far older than _VWAP_MAX_AGE_TRADING_DAYS must be
    dropped and counted as stale, never used to price the row."""
    stale_date = YESTERDAY - timedelta(days=30)  # far more than 5 trading days back
    _insert_yf_close(engine, "ZZZZ", stale_date, 999.99)

    row = {
        "source_id": "finra_ats",
        "ticker": "ZZZZ",
        "signal_type": "VOLUME_SPIKE",
        "signal_date": YESTERDAY,
        "signal_value": json.dumps({"volume": 1_000}),
    }
    flow, skip_reason = dollar_flows._normalize_darkpool(row, engine)
    assert flow is None
    assert skip_reason == "stale_vwap"


def test_normalize_darkpool_accepts_vwap_within_staleness_bound(engine):
    """A VWAP just inside the staleness bound (a handful of trading days
    back, well under _VWAP_MAX_AGE_TRADING_DAYS) still prices the row —
    the bound must not become a blanket break."""
    recent_date = YESTERDAY - timedelta(days=2)
    _insert_yf_close(engine, "ZZZZ", recent_date, 200.0)

    row = {
        "source_id": "finra_ats",
        "ticker": "ZZZZ",
        "signal_type": "VOLUME_SPIKE",
        "signal_date": YESTERDAY,
        "signal_value": json.dumps({"volume": 1_000}),
    }
    flow, skip_reason = dollar_flows._normalize_darkpool(row, engine)
    assert skip_reason is None
    assert flow is not None
    assert flow["amount_usd"] == pytest.approx(1_000 * 200.0)


def test_vwap_age_trading_days_same_session_is_zero():
    assert dollar_flows._vwap_age_trading_days(YESTERDAY, YESTERDAY) == 0


def test_vwap_max_age_is_a_named_pinned_constant():
    """The staleness bound must be a discoverable constant, not a magic
    number buried in a comparison."""
    assert dollar_flows._VWAP_MAX_AGE_TRADING_DAYS == 5


# ── normalize_all_flows: dry run counts, never fabricates, never persists ──

def test_normalize_all_flows_dry_run_counts_and_never_persists(engine):
    # A valid past-dated congressional signal -> should be kept.
    _insert_signal(
        engine, "congressional", "rep-x", "GRDX", YESTERDAY, "BUY",
        {"amount_midpoint": 50_000, "chamber": "House"},
    )
    # A future-dated signal -> must be dropped, never persisted.
    _insert_signal(
        engine, "congressional", "rep-y", "GRDY", FUTURE, "BUY",
        {"amount_midpoint": 999_000},
    )
    # A dark-pool signal with no matching YF price -> must be dropped, not
    # fabricated at the old $50 default.
    _insert_signal(
        engine, "darkpool", "finra_ats", "NOPRICE", YESTERDAY, "VOLUME_SPIKE",
        {"volume": 5_000},
    )
    # A dark-pool signal whose only YF price is far too old -> must be
    # dropped, never priced off a stale close.
    _insert_yf_close(engine, "STALEPRICE", YESTERDAY - timedelta(days=30), 40.0)
    _insert_signal(
        engine, "darkpool", "finra_ats", "STALEPRICE", YESTERDAY, "VOLUME_SPIKE",
        {"volume": 2_000},
    )

    summary = dollar_flows.normalize_all_flows(engine, days=30, dry_run=True)

    assert summary.dry_run is True
    assert summary.persisted == 0
    assert summary.skipped_future_date == 1
    assert summary.skipped_no_vwap == 1
    assert summary.skipped_stale_vwap == 1
    assert len(summary) == 1  # only the valid congressional row survives
    assert summary[0]["ticker"] == "GRDX"
    assert summary[0]["flow_date"] == YESTERDAY
    assert summary.by_source == {"congressional": 1}

    # A dry run must never touch the dollar_flows table at all.
    with engine.connect() as conn:
        tables = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table'")
        ).fetchall()
    assert not any(t[0] == "dollar_flows" for t in tables)


def test_normalize_all_flows_summary_is_still_a_list(engine):
    """Existing callers (api/routers/intelligence_govflow.py) do
    len(flows)/iterate over the return value — the summary must remain a
    list, not a breaking new type."""
    _insert_signal(
        engine, "congressional", "rep-x", "GRDX", YESTERDAY, "BUY",
        {"amount_midpoint": 50_000},
    )
    summary = dollar_flows.normalize_all_flows(engine, days=30, dry_run=True)
    assert isinstance(summary, list)
    assert len(summary) == 1
    assert [f["ticker"] for f in summary] == ["GRDX"]


def test_normalize_all_flows_future_dated_13f_row_is_dropped(engine):
    """13F/ETF raw_series-derived flows get the same future-date guard as
    signal_sources rows."""
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO raw_series "
                "(series_id, source_id, obs_date, pull_timestamp, value, pull_status, raw_payload) "
                "VALUES ('13F:CIK1:CUSIP1:NEW', :src, :od, :ts, 5000000, 'SUCCESS', :payload)"
            ),
            {
                "src": YF_SRC, "od": FUTURE,
                "ts": datetime.combine(FUTURE, datetime.min.time()),
                "payload": json.dumps({"manager_name": "Acme Capital", "issuer_name": "GRDZ"}),
            },
        )

    summary = dollar_flows.normalize_all_flows(engine, days=30, dry_run=True)
    assert summary.skipped_future_date == 1
    assert len(summary) == 0
