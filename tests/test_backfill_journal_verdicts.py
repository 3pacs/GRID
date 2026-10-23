"""Tests for ``scripts/backfill_journal_verdicts.py``.

Background (griddb, measured 2026-10-23): the scorer read SPY from
``resolved_series`` joined on ``feature_registry.name = 'sp500'``, whose last
``obs_date`` is 2026-03-24, while ``raw_series`` ``YF:^GSPC:close`` kept
updating. Every decision after March therefore had no forward close and was
written ``INSUFFICIENT_DATA`` about 16 hours after it was made -- permanently,
because ``DecisionJournal.record_outcome`` refuses a second write. 170 of the
174 decisions in the trailing 45 days were frozen that way.

These tests run the scorer against an in-memory SQLite ``raw_series`` shaped
like production (same columns and ``source_catalog`` FK as
``tests/test_store_observations.py``) so the SUCCESS-only / latest-vintage
semantics come from ``store/observations.py`` itself, not a mock. The
journal is a fake that records ``record_outcome`` calls.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

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
)

import scripts.backfill_journal_verdicts as bjv

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "backfill_journal_verdicts.py"

YFINANCE_SRC = 1
TIINGO_SRC = 2
T0 = datetime(2026, 10, 1, 22, 0, 0)  # noqa: DTZ001 -- naive UTC, as raw_series stores pull_timestamp

# Trading days around the scenario dates (2026-10-12 is Columbus Day: NYSE open).
#   Mon 2026-10-19, Tue 10-20, Wed 10-21, Thu 10-22, Fri 10-23, Mon 10-26


class FakeJournal:
    """Records record_outcome calls; refuses a second write like the real one."""

    def __init__(self, db_engine=None):
        self.calls: list[dict] = []
        self._written: set[int] = set()

    def record_outcome(self, decision_id, outcome_value, verdict, annotation=None):
        if decision_id in self._written:
            raise ValueError(f"Outcome already recorded for decision {decision_id}. Journal is immutable.")
        self._written.add(decision_id)
        self.calls.append({
            "decision_id": decision_id, "outcome_value": outcome_value,
            "verdict": verdict, "annotation": annotation,
        })
        return True


@pytest.fixture()
def db():
    """In-memory engine with production-shaped raw_series, source_catalog, decision_journal."""
    engine = create_engine("sqlite://")
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
        "decision_journal", md,
        Column("id", Integer, primary_key=True),
        Column("decision_timestamp", DateTime, nullable=False),
        Column("inferred_state", String, nullable=False),
        Column("grid_recommendation", String, nullable=False),
        Column("action_taken", String, nullable=False),
        Column("outcome_recorded_at", DateTime),
    )
    md.create_all(engine)
    with engine.begin() as c:
        c.execute(md.tables["source_catalog"].insert(), [
            {"id": YFINANCE_SRC, "name": "yfinance"},
            {"id": TIINGO_SRC, "name": "tiingo"},
        ])
    return engine, md


def _price_rows(series_id, closes, *, status="SUCCESS", source_id=YFINANCE_SRC, ts_offset_h=0):
    return [
        {
            "series_id": series_id, "source_id": source_id, "obs_date": d,
            "pull_timestamp": T0 + timedelta(days=i, hours=ts_offset_h),
            "value": v, "raw_payload": "{}", "pull_status": status,
        }
        for i, (d, v) in enumerate(closes)
    ]


def _insert_prices(db, rows):
    engine, md = db
    with engine.begin() as c:
        c.execute(md.tables["raw_series"].insert(), rows)


def _insert_decisions(db, decisions):
    engine, md = db
    with engine.begin() as c:
        c.execute(md.tables["decision_journal"].insert(), [
            {
                "id": i, "decision_timestamp": datetime.combine(d, datetime.min.time()) + timedelta(hours=14),
                "inferred_state": state, "grid_recommendation": rec,
                "action_taken": action, "outcome_recorded_at": None,
            }
            for i, (d, state, rec, action) in enumerate(decisions, start=1)
        ])


@pytest.fixture()
def run_scorer(db, monkeypatch):
    """Run ``bjv.run`` against the fixture engine with a fake journal; returns (counts, journal)."""
    engine, _ = db
    journal = FakeJournal()
    monkeypatch.setattr(bjv, "get_engine", lambda: engine)
    monkeypatch.setattr(bjv, "DecisionJournal", lambda db_engine=None: journal)

    def _run(today: date, dry_run: bool = False):
        return bjv.run(dry_run=dry_run, today=today), journal

    return _run


# ── Price loading goes through store.observations ───────────────────────────


def test_benchmark_prices_come_from_raw_series_success_rows_latest_vintage(db):
    engine, _ = db
    _insert_prices(db, _price_rows("YF:^GSPC:close", [
        (date(2026, 10, 19), 6800.0),
        (date(2026, 10, 20), 6810.0),
    ]))
    # Revision re-pull: a second vintage for 10-20 pulled later wins.
    _insert_prices(db, _price_rows("YF:^GSPC:close", [(date(2026, 10, 20), 6812.0)], ts_offset_h=48))
    # A FAILED zero marker dated today and a PARTIAL row are not observations.
    _insert_prices(db, _price_rows("YF:^GSPC:close", [(date(2026, 10, 22), 0.0)], status="FAILED"))
    _insert_prices(db, _price_rows("YF:^GSPC:close", [(date(2026, 10, 21), 6700.0)], status="PARTIAL"))
    # Tiingo writes the same series_id; the scorer names yfinance explicitly.
    _insert_prices(db, _price_rows("YF:^GSPC:close", [(date(2026, 10, 21), 9999.0)], source_id=TIINGO_SRC))

    prices = bjv._load_sp500_prices(engine, start=date(2026, 10, 1), as_of=date(2026, 10, 22))

    assert prices == {date(2026, 10, 19): 6800.0, date(2026, 10, 20): 6812.0}
    assert all(isinstance(k, date) for k in prices)
    assert all(isinstance(v, float) for v in prices.values())


def test_benchmark_read_is_bounded_by_start_and_as_of(db):
    engine, _ = db
    _insert_prices(db, _price_rows("YF:IWM:close", [
        (date(2026, 9, 1), 200.0), (date(2026, 10, 19), 210.0), (date(2026, 10, 23), 211.0),
    ]))
    prices = bjv._load_benchmark_prices(engine, "iwm", start=date(2026, 10, 1), as_of=date(2026, 10, 22))
    assert prices == {date(2026, 10, 19): 210.0}


def test_every_scoring_benchmark_has_an_explicit_yfinance_series():
    for benchmark in set(bjv.INDIVIDUAL_STATE_BENCHMARKS.values()) | {"sp500", "vix"}:
        series_id, source = bjv.BENCHMARK_RAW_SERIES[benchmark]
        assert series_id.startswith("YF:") and series_id.endswith(":close")
        assert source == "yfinance"
    assert bjv.BENCHMARK_RAW_SERIES["sp500"] == ("YF:^GSPC:close", "yfinance")


def test_unmapped_benchmark_is_unavailable_without_touching_the_database():
    engine = MagicMock()
    assert bjv._load_benchmark_prices(engine, "no-such-benchmark") == {}
    engine.connect.assert_not_called()


def test_database_error_propagates_instead_of_reading_as_no_data():
    engine = MagicMock()
    engine.connect.side_effect = RuntimeError("connection refused")
    with pytest.raises(RuntimeError):
        bjv._load_benchmark_prices(engine, "sp500")


# ── Window-pending rule ─────────────────────────────────────────────────────


def test_trading_days_after_counts_weekdays_only():
    assert bjv._trading_days_after(date(2026, 10, 16), date(2026, 10, 19)) == 1   # Fri -> Mon
    assert bjv._trading_days_after(date(2026, 10, 19), date(2026, 10, 22)) == 3   # Mon -> Thu
    assert bjv._trading_days_after(date(2026, 10, 19), date(2026, 10, 23)) == 4
    assert bjv._trading_days_after(date(2026, 10, 22), date(2026, 10, 22)) == 0
    assert bjv._trading_days_after(date(2026, 10, 22), date(2026, 10, 19)) == 0


def test_window_pending_until_window_days_have_elapsed():
    prices = {date(2026, 10, 19): 1.0}
    decision = date(2026, 10, 19)
    assert bjv._window_pending(prices, decision, today=date(2026, 10, 19))
    assert bjv._window_pending(prices, decision, today=date(2026, 10, 22))        # 3 trading days
    # 4th trading day after: window elapsed; feed is at the decision date so still pending.
    assert bjv._window_pending(prices, decision, today=date(2026, 10, 23))


def test_window_pending_when_feed_lags_within_a_window_of_the_decision():
    prices = {date(2026, 10, 19): 1.0}
    assert bjv._window_pending(prices, date(2026, 10, 22), today=date(2026, 11, 20))      # 3 days behind
    assert not bjv._window_pending(prices, date(2026, 10, 23), today=date(2026, 11, 20))  # 4 days behind


def test_window_not_pending_when_elapsed_and_no_feed():
    assert bjv._window_pending({}, date(2026, 10, 19), today=date(2026, 10, 22))
    assert not bjv._window_pending({}, date(2026, 10, 19), today=date(2026, 10, 23))


# ── run(): skip vs write ────────────────────────────────────────────────────


def test_decision_inside_window_is_skipped_not_written(db, run_scorer):
    _insert_prices(db, _price_rows("YF:^GSPC:close", [
        (date(2026, 10, 16), 6790.0), (date(2026, 10, 19), 6800.0),
        (date(2026, 10, 20), 6810.0), (date(2026, 10, 21), 6820.0),
    ]))
    _insert_decisions(db, [(date(2026, 10, 21), "GROWTH", "AGGRESSIVE", "AUTO_AGGRESSIVE")])

    counts, journal = run_scorer(today=date(2026, 10, 22))

    assert journal.calls == []
    assert counts["pending"] == 1
    assert counts["INSUFFICIENT_DATA"] == 0
    assert counts["scored"] == 0


def test_same_decision_is_scored_once_the_forward_close_exists(db, run_scorer):
    _insert_prices(db, _price_rows("YF:^GSPC:close", [
        (date(2026, 10, 19), 6800.0), (date(2026, 10, 20), 6810.0),
        (date(2026, 10, 21), 6820.0), (date(2026, 10, 22), 6900.0),
    ]))
    _insert_decisions(db, [(date(2026, 10, 21), "GROWTH", "AGGRESSIVE", "AUTO_AGGRESSIVE")])

    counts, journal = run_scorer(today=date(2026, 10, 23))

    assert counts["pending"] == 0
    assert counts["HELPED"] == 1 and counts["scored"] == 1
    assert len(journal.calls) == 1
    assert journal.calls[0]["verdict"] == "HELPED"
    assert journal.calls[0]["outcome_value"] == pytest.approx((6900.0 - 6820.0) / 6820.0, abs=1e-6)


def test_decision_past_window_with_no_data_gets_insufficient_data(db, run_scorer):
    # Feed stopped on 10-01; decision on 10-09 is 6 trading days later; scored on 10-22.
    _insert_prices(db, _price_rows("YF:^GSPC:close", [
        (date(2026, 9, 30), 6700.0), (date(2026, 10, 1), 6710.0),
    ]))
    _insert_decisions(db, [(date(2026, 10, 9), "FRAGILE", "DEFENSIVE", "AUTO_DEFENSIVE")])

    counts, journal = run_scorer(today=date(2026, 10, 22))

    assert counts["pending"] == 0
    assert counts["INSUFFICIENT_DATA"] == 1
    assert len(journal.calls) == 1
    assert journal.calls[0]["verdict"] == "INSUFFICIENT_DATA"
    assert journal.calls[0]["annotation"] == "No forward SPY data available for scoring window"


def test_decision_near_a_lagging_feed_stays_pending_after_the_window(db, run_scorer):
    # Feed last at 10-19; decision 10-21 is 2 trading days past it. Even weeks
    # later it is left for the feed to catch up rather than frozen.
    _insert_prices(db, _price_rows("YF:^GSPC:close", [
        (date(2026, 10, 16), 6790.0), (date(2026, 10, 19), 6800.0),
    ]))
    _insert_decisions(db, [(date(2026, 10, 21), "NEUTRAL", "BALANCED", "AUTO_BALANCED")])

    counts, journal = run_scorer(today=date(2026, 11, 20))

    assert journal.calls == []
    assert counts["pending"] == 1


def test_failed_zero_rows_never_become_an_entry_or_exit_price(db, run_scorer):
    _insert_prices(db, _price_rows("YF:^GSPC:close", [
        (date(2026, 10, 19), 6800.0), (date(2026, 10, 20), 6810.0),
    ]))
    # The puller's failure markers for the window dates: value 0, FAILED.
    _insert_prices(db, _price_rows("YF:^GSPC:close", [
        (date(2026, 10, 21), 0.0), (date(2026, 10, 22), 0.0),
    ], status="FAILED"))
    _insert_decisions(db, [(date(2026, 10, 20), "GROWTH", "AGGRESSIVE", "AUTO_AGGRESSIVE")])

    counts, journal = run_scorer(today=date(2026, 10, 22))

    # No SUCCESS close after 10-20 -> no forward return; window open -> pending.
    assert journal.calls == []
    assert counts["pending"] == 1


def test_individual_pick_inside_window_is_pending_and_benchmark_read_once(db, run_scorer, monkeypatch):
    _insert_prices(db, _price_rows("YF:^GSPC:close", [
        (date(2026, 10, 19), 6800.0), (date(2026, 10, 20), 6810.0), (date(2026, 10, 21), 6820.0),
    ]))
    _insert_prices(db, _price_rows("YF:IWM:close", [
        (date(2026, 10, 19), 210.0), (date(2026, 10, 20), 211.0), (date(2026, 10, 21), 212.0),
    ]))
    _insert_decisions(db, [
        (date(2026, 10, 21), "EQUITY_VALUE", "AGGRESSIVE", "AUTO_AGGRESSIVE"),
        (date(2026, 10, 19), "EQUITY_VALUE", "AGGRESSIVE", "AUTO_AGGRESSIVE"),
    ])
    loads: list[str] = []
    real_load = bjv._load_benchmark_prices
    monkeypatch.setattr(
        bjv, "_load_benchmark_prices",
        lambda engine, name, start=None, as_of=None: (loads.append(name), real_load(engine, name, start=start, as_of=as_of))[1],
    )

    counts, journal = run_scorer(today=date(2026, 10, 22))

    assert counts["pending"] == 1
    assert counts["HELPED"] == 1
    assert journal.calls[0]["annotation"].startswith("EQUITY_VALUE AGGRESSIVE correct: iwm")
    assert loads.count("iwm") == 1, "benchmark must be preloaded once per run, not per row"


def test_individual_pick_with_unmapped_benchmark_is_insufficient_data(db, run_scorer, monkeypatch):
    _insert_prices(db, _price_rows("YF:^GSPC:close", [
        (date(2026, 10, 19), 6800.0), (date(2026, 10, 20), 6810.0),
    ]))
    monkeypatch.setitem(bjv.INDIVIDUAL_STATE_BENCHMARKS, "CRYPTO_CORE", "no-such-benchmark")
    _insert_decisions(db, [(date(2026, 10, 19), "CRYPTO_CORE", "AGGRESSIVE", "AUTO_AGGRESSIVE")])

    counts, journal = run_scorer(today=date(2026, 10, 20))

    assert counts["pending"] == 0
    assert counts["INSUFFICIENT_DATA"] == 1
    assert journal.calls[0]["annotation"] == "No benchmark data for no-such-benchmark"


def test_benchmark_cache_is_cleared_each_run(db, run_scorer):
    _insert_prices(db, _price_rows("YF:^GSPC:close", [(date(2026, 10, 19), 6800.0)]))
    _insert_decisions(db, [(date(2026, 10, 19), "GROWTH", "AGGRESSIVE", "AUTO_AGGRESSIVE")])
    bjv._benchmark_cache["iwm"] = {date(2000, 1, 1): 1.0}

    run_scorer(today=date(2026, 10, 20))

    assert "iwm" not in bjv._benchmark_cache
    assert set(bjv._benchmark_cache) == {"sp500", "vix"}


def test_dry_run_writes_nothing_and_still_counts_pending(db, run_scorer):
    _insert_prices(db, _price_rows("YF:^GSPC:close", [
        (date(2026, 10, 16), 6790.0), (date(2026, 10, 19), 6800.0), (date(2026, 10, 20), 6700.0),
    ]))
    _insert_decisions(db, [
        (date(2026, 10, 19), "FRAGILE", "DEFENSIVE", "AUTO_DEFENSIVE"),
        (date(2026, 10, 20), "GROWTH", "AGGRESSIVE", "AUTO_AGGRESSIVE"),
    ])

    counts, journal = run_scorer(today=date(2026, 10, 21), dry_run=True)

    assert journal.calls == []
    assert counts["HELPED"] == 1 and counts["pending"] == 1 and counts["scored"] == 1


def test_no_spy_data_aborts_without_writing(db, run_scorer):
    _insert_decisions(db, [(date(2026, 10, 19), "GROWTH", "AGGRESSIVE", "AUTO_AGGRESSIVE")])
    counts, journal = run_scorer(today=date(2026, 10, 22))
    assert counts == {"error": "no SPY data"}
    assert journal.calls == []


# ── Source guards ───────────────────────────────────────────────────────────


def test_resolved_series_read_is_gone():
    src = SCRIPT.read_text(encoding="utf-8")
    code = "\n".join(
        line for line in src.splitlines() if not line.lstrip().startswith("#")
    )
    # Strip the module docstring: it names the old tables to explain the bug.
    body = code.split('"""', 2)[2]
    assert not re.search(r"\bFROM\s+resolved_series\b", body, re.IGNORECASE)
    assert "feature_registry" not in body
    assert "from store.observations import read_window" in src
    assert "read_window(conn, series_id, source=source" in src


def test_scheduler_registration_is_unchanged():
    src = (ROOT / "intelligence" / "scheduler.py").read_text(encoding="utf-8")
    assert "from scripts.backfill_journal_verdicts import run as _score_journal" in src
    assert '_sched.every().day.at("06:45").do(_journal_verdicts_daily)' in src
