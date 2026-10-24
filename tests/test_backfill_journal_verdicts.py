"""Tests for ``scripts/backfill_journal_verdicts.py``.

Background (griddb, measured 2026-10-23): the scorer read SPY from
``resolved_series`` joined on ``feature_registry.name = 'sp500'``, whose last
``obs_date`` is 2026-03-24, so every decision after March had no forward
close and was written ``INSUFFICIENT_DATA`` about 16 hours after it was
made -- permanently, because ``DecisionJournal.record_outcome`` refuses a
second write. 170 of the 174 decisions in the trailing 45 days were frozen.

The scorer now reads completed-close captures through
``store.observations.read_daily_policy_n``. These tests run it against an
in-memory ``raw_series`` whose payloads are produced by
``price_close_contract`` itself, so what counts as a completed close is
decided by the contract, not by the test. The journal is a fake that records
``record_outcome`` calls and refuses a second write like the real one.

Calendar used throughout (2026): Fri 10-16, Mon 10-19 .. Fri 10-23,
Mon 10-26, Tue 10-27; Thanksgiving Thu 11-26.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

import price_close_contract as contract
import scripts.backfill_journal_verdicts as bjv

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "backfill_journal_verdicts.py"
UTC = timezone.utc


def at(d: date, hh: int, mm: int = 0, ss: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, ss, tzinfo=UTC)


def next_morning(d: date) -> datetime:
    """The 06:02Z re-pull the day after ``d``: when a close becomes a completed capture."""
    return at(d + timedelta(days=1), 6, 2)


def same_evening(d: date) -> datetime:
    """The 22:02Z pull on ``d`` itself: provisional, the label day has not elapsed."""
    return at(d, 22, 2)


class Result:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


class MemoryDB:
    """An in-memory ``raw_series`` that answers ``store.observations``' window SQL."""

    def __init__(self):
        self.rows: list[dict] = []
        self.calls: list[tuple[str, dict]] = []

    def connect(self):
        return self

    def begin(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def add(self, series_id, obs_date, value, pulled_at, *, payload, status="SUCCESS", source="yfinance"):
        self.rows.append({
            "sid": series_id, "od": obs_date, "val": value, "pulled_at": pulled_at,
            "payload": payload, "status": status, "src": source,
        })

    def execute(self, stmt, params=None):
        sql = str(stmt)
        p = params or {}
        self.calls.append((sql, dict(p)))
        if "SELECT r.obs_date" not in sql:
            raise AssertionError("unmodeled SQL: " + sql)
        rows = [
            r for r in self.rows
            if r["sid"] == p["sid"] and r["status"] == p["ok"]
            and (p.get("source") is None or r["src"].lower() == p["source"].lower())
            and (p.get("start") is None or r["od"] >= p["start"])
            and (p.get("as_of") is None or r["od"] <= p["as_of"])
            and (p.get("as_of_ts") is None or r["pulled_at"] <= p["as_of_ts"])
        ]
        rows.sort(key=lambda r: (r["od"], -r["pulled_at"].timestamp()))  # obs ASC, pull DESC
        if "r.raw_payload" in sql:
            return Result([(r["od"], r["val"], r["pulled_at"], r["src"], r["payload"]) for r in rows])
        return Result([(r["od"], r["val"], r["pulled_at"], r["src"]) for r in rows])


class FakeJournal:
    """Records record_outcome calls; refuses a second write like the real one."""

    def __init__(self, prewritten=()):
        self.calls: list[dict] = []
        self._written: set[int] = set(prewritten)

    def record_outcome(self, decision_id, outcome_value, verdict, annotation=None):
        if decision_id in self._written:
            raise ValueError(f"Outcome already recorded for decision {decision_id}. Journal is immutable.")
        self._written.add(decision_id)
        self.calls.append({
            "decision_id": decision_id, "outcome_value": outcome_value,
            "verdict": verdict, "annotation": annotation,
        })
        return True


def _payload(bench: bjv.Benchmark, obs_date: date, pulled_at: datetime) -> dict:
    """Exactly what the yfinance puller writes for this series at this pull time."""
    if bench.series_id == contract.SPY_CLOSE_SERIES:
        marker = contract.capture_payload(obs_date, pulled_at)
        return marker or {  # unmarked provisional row
            "price_basis": contract.SPY_CLOSE_SERIES, "interval": "1d",
            "obs_date": obs_date.isoformat(), "provider_certified_final": False,
        }
    ticker = bench.series_id.split(":")[1]
    return contract.raw_capture_payload(ticker, "close", obs_date, pulled_at, "1d")


def add_close(db, benchmark, obs_date, value, pulled_at, status="SUCCESS"):
    bench = bjv.BENCHMARKS[benchmark]
    db.add(bench.series_id, obs_date, value, pulled_at, payload=_payload(bench, obs_date, pulled_at), status=status)


def add_completed(db, benchmark, closes: dict[date, float]):
    for d, v in closes.items():
        add_close(db, benchmark, d, v, next_morning(d))


def add_provisional(db, benchmark, closes: dict[date, float]):
    for d, v in closes.items():
        add_close(db, benchmark, d, v, same_evening(d))


SPY_CLOSES = {
    date(2026, 10, 16): 670.0, date(2026, 10, 19): 680.0, date(2026, 10, 20): 684.0,
    date(2026, 10, 21): 686.0, date(2026, 10, 22): 690.0, date(2026, 10, 23): 695.0,
}


def ret(a: float, b: float) -> float:
    return (b - a) / a


@pytest.fixture()
def scorer(monkeypatch):
    """Run ``bjv.run`` with an in-memory raw_series, given decisions and a fake journal."""

    def _run(db, decisions, evaluated_at, *, dry_run=False, journal=None):
        journal = journal or FakeJournal()
        monkeypatch.setattr(bjv, "get_engine", lambda: db)
        monkeypatch.setattr(bjv, "DecisionJournal", lambda db_engine=None: journal)
        monkeypatch.setattr(bjv, "_fetch_unscored", lambda engine: [tuple(d) for d in decisions])
        return bjv.run(dry_run=dry_run, evaluated_at=evaluated_at), journal

    return _run


GROWTH = ("GROWTH", "AGGRESSIVE", "AUTO_AGGRESSIVE")


# ── Entry: the last completed close known when the decision was made ────────


def test_same_day_decision_before_the_close_enters_on_the_previous_session(scorer):
    db = MemoryDB()
    add_completed(db, "sp500", SPY_CLOSES)
    add_provisional(db, "sp500", {d: v + 0.5 for d, v in SPY_CLOSES.items()})  # same-evening pulls

    # Tuesday 14:00Z, mid-session: Tuesday's close is pulled at 22:02Z and
    # becomes a completed capture at 06:02Z Wednesday -- it did not exist.
    counts, journal = scorer(db, [(1, at(date(2026, 10, 20), 14), *GROWTH)], at(date(2026, 10, 23), 6, 45))

    assert counts["HELPED"] == 1 and counts["pending"] == 0
    call = journal.calls[0]
    assert call["outcome_value"] == pytest.approx(ret(680.0, 690.0), abs=1e-6)   # 10-19 -> 10-22
    assert call["annotation"].startswith("AGGRESSIVE correct: SPY +1.47%")
    assert call["annotation"].endswith(
        "| basis YF:SPY:close (spy_close_v1, source yfinance): "
        "entry 2026-10-19 close 680.0 pulled 2026-10-20T06:02:00+00:00 known_at 2026-10-20T14:00:00+00:00; "
        "anchor session 2026-10-19; "
        "exit session 2026-10-22 close 690.0 pulled 2026-10-23T06:02:00+00:00; "
        "evaluated_at 2026-10-23T06:45:00+00:00"
    )


def test_decision_after_the_morning_repull_enters_on_that_close(scorer):
    db = MemoryDB()
    add_completed(db, "sp500", SPY_CLOSES)

    # Wednesday 07:00Z: Tuesday's close became a completed capture at 06:02Z.
    decision = (1, at(date(2026, 10, 21), 7), *GROWTH)
    counts, journal = scorer(db, [decision], at(date(2026, 10, 23), 6, 45))
    assert counts["pending"] == 1 and journal.calls == []   # exit session 10-23 not captured yet

    counts, journal = scorer(db, [decision], at(date(2026, 10, 24), 6, 45))
    assert counts["HELPED"] == 1
    assert journal.calls[0]["outcome_value"] == pytest.approx(ret(684.0, 695.0), abs=1e-6)  # 10-20 -> 10-23


def test_entry_skips_a_provisional_capture_and_uses_the_last_completed_one(scorer):
    db = MemoryDB()
    add_completed(db, "sp500", {d: v for d, v in SPY_CLOSES.items() if d != date(2026, 10, 19)})
    add_provisional(db, "sp500", {date(2026, 10, 19): 680.0})   # never re-pulled

    counts, journal = scorer(db, [(1, at(date(2026, 10, 20), 14), *GROWTH)], at(date(2026, 10, 23), 6, 45))

    # Entry falls back to Friday 10-16 (4 days: still within ENTRY_MAX_AGE_DAYS).
    # The horizon is anchored to the decision's session (Monday 10-19), not
    # to the stale entry label, so the exit is still Thursday 10-22.
    assert counts["HELPED"] == 1
    assert journal.calls[0]["outcome_value"] == pytest.approx(ret(670.0, 690.0), abs=1e-6)
    assert "entry 2026-10-16 close 670.0 pulled 2026-10-17T06:02:00+00:00" in journal.calls[0]["annotation"]
    assert "anchor session 2026-10-19; exit session 2026-10-22" in journal.calls[0]["annotation"]


def test_entry_older_than_max_age_is_insufficient_data_at_once(scorer):
    db = MemoryDB()
    add_completed(db, "sp500", {date(2026, 10, 16): 670.0, date(2026, 10, 19): 680.0})

    counts, journal = scorer(db, [(1, at(date(2026, 10, 27), 14), *GROWTH)], at(date(2026, 10, 28), 6, 45))

    assert counts["INSUFFICIENT_DATA"] == 1 and counts["pending"] == 0
    assert journal.calls[0]["outcome_value"] == 0.0
    assert journal.calls[0]["annotation"] == (
        "No forward SPY data available for scoring window "
        "(no completed YF:SPY:close close known at 2026-10-27T14:00:00+00:00 within 4d)"
    )


def test_late_revision_keeps_the_entry_vintage_known_at_decision_time(scorer):
    db = MemoryDB()
    add_completed(db, "sp500", {date(2026, 10, 19): 680.0, date(2026, 10, 20): 684.0, date(2026, 10, 21): 686.0})
    add_close(db, "sp500", date(2026, 10, 19), 681.0, at(date(2026, 10, 21), 6, 2))   # re-pull after the decision
    add_close(db, "sp500", date(2026, 10, 22), 690.0, at(date(2026, 10, 23), 6, 2))
    add_close(db, "sp500", date(2026, 10, 22), 691.0, at(date(2026, 10, 23), 6, 30))  # exit revised
    decision = (1, at(date(2026, 10, 20), 14), *GROWTH)

    # Entry is the vintage GRID had at 14:00Z Tuesday (680, not 681); the exit is
    # the latest vintage known at evaluation time.
    counts, journal = scorer(db, [decision], at(date(2026, 10, 23), 6, 45))
    assert counts["HELPED"] == 1
    assert journal.calls[0]["outcome_value"] == pytest.approx(ret(680.0, 691.0), abs=1e-6)

    counts, journal = scorer(db, [decision], at(date(2026, 10, 23), 6, 10))
    assert counts["HELPED"] == 1
    assert journal.calls[0]["outcome_value"] == pytest.approx(ret(680.0, 690.0), abs=1e-6)


# ── Exit: the exact session, never a provisional capture, never a shifted label


def test_exit_never_uses_provisional_failed_or_quarantined_rows(scorer):
    db = MemoryDB()
    add_completed(db, "sp500", {d: v for d, v in SPY_CLOSES.items() if d != date(2026, 10, 22)})
    add_provisional(db, "sp500", {date(2026, 10, 22): 690.0})                       # label day not elapsed
    add_close(db, "sp500", date(2026, 10, 22), 0.0, next_morning(date(2026, 10, 22)), status="FAILED")
    add_close(db, "sp500", date(2026, 10, 22), 690.0, next_morning(date(2026, 10, 22)), status="QUARANTINED")
    add_close(db, "sp500", date(2026, 10, 22), 690.0, next_morning(date(2026, 10, 22)), status="PARTIAL")
    decision = (1, at(date(2026, 10, 20), 7), *GROWTH)   # entry 10-19, exit session 10-22

    # Within the puller's catch-up window: deferred, not scored on the
    # provisional 10-22 row and not shifted to the completed 10-23 close.
    counts, journal = scorer(db, [decision], at(date(2026, 10, 24), 6, 45))
    assert counts["pending"] == 1 and journal.calls == []

    # Past the window with the feed demonstrably alive after the gap: a gap.
    counts, journal = scorer(db, [decision], at(date(2026, 10, 27), 6, 45))
    assert counts["INSUFFICIENT_DATA"] == 1
    assert journal.calls[0]["annotation"] == (
        "No forward SPY data available for scoring window "
        "(no completed YF:SPY:close close for session 2026-10-22; feed resumed at 2026-10-23)"
    )


def test_monday_is_not_scored_with_friday_when_the_exit_session_is_missing(scorer):
    db = MemoryDB()
    add_completed(db, "sp500", {d: v for d, v in SPY_CLOSES.items() if d != date(2026, 10, 22)})
    decision = (1, at(date(2026, 10, 20), 7), *GROWTH)   # entry Mon 10-19, exit Thu 10-22 (absent)

    counts, journal = scorer(db, [decision], at(date(2026, 10, 24), 6, 45))
    assert counts["pending"] == 1 and journal.calls == []

    counts, journal = scorer(db, [decision], at(date(2026, 10, 27), 6, 45))
    assert journal.calls[0]["verdict"] == "INSUFFICIENT_DATA"
    assert journal.calls[0]["outcome_value"] == 0.0


def test_stalled_feed_stays_pending_rather_than_frozen(scorer):
    db = MemoryDB()
    add_completed(db, "sp500", {date(2026, 10, 19): 680.0, date(2026, 10, 20): 684.0, date(2026, 10, 21): 686.0})
    decision = (1, at(date(2026, 10, 20), 7), *GROWTH)   # exit session 10-22, nothing after

    counts, journal = scorer(db, [decision], at(date(2026, 11, 5), 6, 45))

    assert counts["pending"] == 1 and journal.calls == []


def test_equity_exit_skips_the_thanksgiving_holiday(scorer):
    closes = {
        date(2026, 11, 24): 700.0, date(2026, 11, 25): 702.0,
        date(2026, 11, 27): 704.0, date(2026, 11, 30): 706.0, date(2026, 12, 1): 710.0,
    }
    db = MemoryDB()
    add_completed(db, "sp500", closes)
    # Decision on the holiday itself: entry is Wednesday's close (completed at
    # 06:02Z Thursday); the three sessions after it are Fri, Mon, Tue.
    counts, journal = scorer(db, [(1, at(date(2026, 11, 26), 12), *GROWTH)], at(date(2026, 12, 2), 6, 45))

    assert counts["HELPED"] == 1
    assert journal.calls[0]["outcome_value"] == pytest.approx(ret(702.0, 710.0), abs=1e-6)
    assert "entry 2026-11-25 close 702.0 pulled 2026-11-26T06:02:00+00:00" in journal.calls[0]["annotation"]
    assert "anchor session 2026-11-25; exit session 2026-12-01 close 710.0" in journal.calls[0]["annotation"]


def test_btc_exit_counts_utc_calendar_days_including_the_weekend(scorer):
    closes = {date(2026, 10, 22): 81000.0, date(2026, 10, 23): 82000.0,
              date(2026, 10, 24): 83000.0, date(2026, 10, 25): 84000.0}
    db = MemoryDB()
    add_completed(db, "btc", closes)

    counts, journal = scorer(
        db, [(1, at(date(2026, 10, 23), 12), "CRYPTO_CORE", "AGGRESSIVE", "AUTO_AGGRESSIVE")],
        at(date(2026, 10, 26), 6, 45),
    )

    assert counts["HELPED"] == 1
    call = journal.calls[0]
    assert call["outcome_value"] == pytest.approx(ret(81000.0, 84000.0), abs=1e-6)   # 10-22 -> Sun 10-25
    assert call["annotation"].startswith("CRYPTO_CORE AGGRESSIVE correct: btc +3.70%")
    assert "basis YF:BTC-USD:close (yf_daily_capture_v1, source yfinance): entry 2026-10-22" in call["annotation"]
    assert "anchor session 2026-10-22; exit session 2026-10-25" in call["annotation"]


def test_stale_btc_entry_never_scores_a_window_that_closed_before_the_decision(scorer):
    """Review P1: entry four days stale, exit three days after it -- all pre-decision.

    Decision Friday 2026-10-23T14:00Z; the newest completed BTC close known is
    Oct 19 (feed stale), and Oct 22's close is captured late on Oct 24. The
    old rule (exit = entry + 3 days) would score Oct 19 -> Oct 22, a period
    that ended before the decision. The horizon is anchored to the decision's
    last capturable session (Oct 22), so the exit is Oct 25.
    """
    db = MemoryDB()
    add_completed(db, "btc", {date(2026, 10, 16): 80000.0, date(2026, 10, 17): 80500.0,
                              date(2026, 10, 18): 80800.0, date(2026, 10, 19): 81000.0})
    add_close(db, "btc", date(2026, 10, 22), 83000.0, at(date(2026, 10, 24), 10))   # late capture
    decision = (1, at(date(2026, 10, 23), 14), "CRYPTO_CORE", "AGGRESSIVE", "AUTO_AGGRESSIVE")

    counts, journal = scorer(db, [decision], at(date(2026, 10, 25), 6, 45))
    assert counts["pending"] == 1 and journal.calls == []          # not Oct 19 -> Oct 22

    add_close(db, "btc", date(2026, 10, 25), 84000.0, next_morning(date(2026, 10, 25)))
    counts, journal = scorer(db, [decision], at(date(2026, 10, 26), 6, 45))
    assert counts["HELPED"] == 1
    assert journal.calls[0]["outcome_value"] == pytest.approx(ret(81000.0, 84000.0), abs=1e-6)
    assert "entry 2026-10-19 close 81000.0" in journal.calls[0]["annotation"]
    assert "anchor session 2026-10-22; exit session 2026-10-25" in journal.calls[0]["annotation"]


def test_stale_equity_entry_is_anchored_to_the_decision_session_across_the_holiday(scorer):
    """A weekend decision after Thanksgiving with the feed three sessions behind."""
    db = MemoryDB()
    add_completed(db, "sp500", {date(2026, 11, 24): 700.0, date(2026, 11, 25): 702.0})
    # Decision Sunday 2026-11-29 12:00Z: entry is Wed 11-25 (4 days, still
    # allowed); the last capturable session is Fri 11-27, so the exit is the
    # third session after it: Mon 11-30, Tue 12-01, Wed 12-02 -- not Tue
    # 12-01, which is what entry + 3 sessions would give.
    decision = (1, at(date(2026, 11, 29), 12), *GROWTH)
    # The feed catches up only on Monday: Friday's close is captured after the decision.
    add_close(db, "sp500", date(2026, 11, 27), 704.0, at(date(2026, 11, 30), 10))
    add_completed(db, "sp500", {date(2026, 11, 30): 706.0, date(2026, 12, 1): 708.0})

    counts, journal = scorer(db, [decision], at(date(2026, 12, 2), 6, 45))
    assert counts["pending"] == 1 and journal.calls == []          # 12-02 not captured yet

    add_completed(db, "sp500", {date(2026, 12, 2): 712.0})
    counts, journal = scorer(db, [decision], at(date(2026, 12, 3), 6, 45))
    assert counts["HELPED"] == 1
    assert journal.calls[0]["outcome_value"] == pytest.approx(ret(702.0, 712.0), abs=1e-6)
    assert "anchor session 2026-11-27; exit session 2026-12-02 close 712.0" in journal.calls[0]["annotation"]


def test_annotation_records_the_exact_vintages_selected_at_sub_minute_precision(scorer):
    """Review P2: an evaluation at 06:45:45 selecting a 06:45:30 re-pull must say so."""
    db = MemoryDB()
    add_completed(db, "sp500", {date(2026, 10, 19): 680.0, date(2026, 10, 20): 684.0, date(2026, 10, 21): 686.0})
    add_close(db, "sp500", date(2026, 10, 22), 690.0, at(date(2026, 10, 23), 6, 2, 0))
    add_close(db, "sp500", date(2026, 10, 22), 690.25, at(date(2026, 10, 23), 6, 45, 30))   # re-pull
    decision = (1, at(date(2026, 10, 20), 14, 0, 7), *GROWTH)

    _, early = scorer(db, [decision], at(date(2026, 10, 23), 6, 45, 15))
    _, late = scorer(db, [decision], at(date(2026, 10, 23), 6, 45, 45))

    assert early.calls[0]["outcome_value"] == pytest.approx(ret(680.0, 690.0), abs=1e-6)
    assert late.calls[0]["outcome_value"] == pytest.approx(ret(680.0, 690.25), abs=1e-6)
    assert early.calls[0]["annotation"].endswith(
        "entry 2026-10-19 close 680.0 pulled 2026-10-20T06:02:00+00:00 known_at 2026-10-20T14:00:07+00:00; "
        "anchor session 2026-10-19; "
        "exit session 2026-10-22 close 690.0 pulled 2026-10-23T06:02:00+00:00; "
        "evaluated_at 2026-10-23T06:45:15+00:00"
    )
    assert late.calls[0]["annotation"].endswith(
        "exit session 2026-10-22 close 690.25 pulled 2026-10-23T06:45:30+00:00; "
        "evaluated_at 2026-10-23T06:45:45+00:00"
    )


def test_individual_pick_uses_its_own_benchmark_series(scorer):
    db = MemoryDB()
    add_completed(db, "sp500", SPY_CLOSES)
    add_completed(db, "iwm", {date(2026, 10, 19): 210.0, date(2026, 10, 20): 211.0,
                              date(2026, 10, 21): 212.0, date(2026, 10, 22): 208.0})

    counts, journal = scorer(
        db, [(1, at(date(2026, 10, 20), 7), "EQUITY_VALUE", "AGGRESSIVE", "AUTO_AGGRESSIVE")],
        at(date(2026, 10, 23), 6, 45),
    )

    assert counts["HARMED"] == 1
    assert journal.calls[0]["annotation"].startswith("EQUITY_VALUE AGGRESSIVE wrong: iwm -0.95%")
    assert "basis YF:IWM:close" in journal.calls[0]["annotation"]
    assert all(c[1]["sid"] == "YF:IWM:close" for c in db.calls)


def test_unmapped_benchmark_is_insufficient_data_without_a_read(scorer, monkeypatch):
    db = MemoryDB()
    monkeypatch.setitem(bjv.INDIVIDUAL_STATE_BENCHMARKS, "CRYPTO_AI", "no-such-benchmark")

    counts, journal = scorer(
        db, [(1, at(date(2026, 10, 20), 7), "CRYPTO_AI", "AGGRESSIVE", "AUTO_AGGRESSIVE")],
        at(date(2026, 10, 23), 6, 45),
    )

    assert counts["INSUFFICIENT_DATA"] == 1
    assert journal.calls[0]["annotation"] == (
        "No benchmark data for no-such-benchmark "
        "(no completed-close series mapped for benchmark no-such-benchmark)"
    )
    assert db.calls == []


# ── Journal interaction ──────────────────────────────────────────────────────


def test_dry_run_writes_nothing_and_still_counts_pending(scorer):
    db = MemoryDB()
    add_completed(db, "sp500", SPY_CLOSES)
    decisions = [(1, at(date(2026, 10, 20), 14), *GROWTH), (2, at(date(2026, 10, 22), 14), *GROWTH)]

    counts, journal = scorer(db, decisions, at(date(2026, 10, 23), 6, 45), dry_run=True)

    assert journal.calls == []
    assert counts["HELPED"] == 1 and counts["pending"] == 1 and counts["scored"] == 1


def test_journal_refusal_is_counted_as_skipped(scorer):
    db = MemoryDB()
    add_completed(db, "sp500", SPY_CLOSES)

    counts, journal = scorer(
        db, [(7, at(date(2026, 10, 20), 14), *GROWTH)], at(date(2026, 10, 23), 6, 45),
        journal=FakeJournal(prewritten={7}),
    )

    assert counts["skipped"] == 1 and counts["scored"] == 0 and journal.calls == []


def test_no_unscored_rows_is_a_noop(scorer):
    counts, journal = scorer(MemoryDB(), [], at(date(2026, 10, 23), 6, 45))
    assert counts["scored"] == 0 and counts["pending"] == 0 and journal.calls == []


# ── Declared basis and source guards ─────────────────────────────────────────


def test_every_benchmark_is_a_contract_eligible_completed_close_series():
    assert set(bjv.BENCHMARKS) == {"sp500", "iwm", "hyg", "btc"}
    assert bjv.BENCHMARKS["sp500"].series_id == contract.SPY_CLOSE_SERIES
    assert bjv.BENCHMARKS["sp500"].contract == contract.SPY_CLOSE_CONTRACT
    for name in ("iwm", "hyg"):
        ticker = bjv.BENCHMARKS[name].series_id.split(":")[1]
        assert ticker in contract.US_LISTED_DAILY_TICKERS
        assert bjv.BENCHMARKS[name].sessions == bjv.US_LISTED_SESSIONS
    assert "BTC-USD" in contract.UTC_DAILY_TICKERS
    assert bjv.BENCHMARKS["btc"].sessions == bjv.UTC_24H_SESSIONS
    for bench in bjv.BENCHMARKS.values():
        assert bench.source == "yfinance"
    # Capture qualification does not select a journal scoring benchmark.
    assert "vix" not in bjv.BENCHMARKS
    assert all(bench.series_id != "YF:^VIX:close" for bench in bjv.BENCHMARKS.values())
    obs_date = date(2026, 10, 19)
    pulled_at = next_morning(obs_date)
    payload = contract.raw_capture_payload("^VIX", "close", obs_date, pulled_at, "1d")
    assert payload["capture_state"] == "POLICY_ELIGIBLE"
    assert payload["session_convention"] == "cboe_vol_index_ny_label_day_2026"
    db = MemoryDB()
    db.add("YF:^VIX:close", obs_date, 18.5, pulled_at, payload=payload)
    assert [o.value for o in bjv.read_daily_policy_n(
        db, "YF:^VIX:close", 1, start=obs_date, source="yfinance", as_of_ts=pulled_at,
    )] == [18.5]
    assert set(bjv.INDIVIDUAL_STATE_BENCHMARKS.values()) <= set(bjv.BENCHMARKS)


def test_decision_timestamps_are_normalised_to_utc():
    assert bjv._as_utc(datetime(2026, 10, 20, 14, 0)) == at(date(2026, 10, 20), 14)   # noqa: DTZ001 -- naive = UTC
    assert bjv._as_utc(datetime(2026, 10, 20, 7, 0, tzinfo=timezone(timedelta(hours=-7)))) == at(date(2026, 10, 20), 14)
    assert bjv._as_utc("2026-10-20 14:00:00") == at(date(2026, 10, 20), 14)
    assert bjv._as_utc(date(2026, 10, 20)) == at(date(2026, 10, 20), 0)


def test_sessions_follow_the_declared_calendars():
    assert bjv._sessions_after(date(2026, 10, 19), 3, bjv.US_LISTED_SESSIONS) == [
        date(2026, 10, 20), date(2026, 10, 21), date(2026, 10, 22)]
    assert bjv._sessions_after(date(2026, 10, 22), 3, bjv.US_LISTED_SESSIONS) == [
        date(2026, 10, 23), date(2026, 10, 26), date(2026, 10, 27)]           # weekend skipped
    assert bjv._sessions_after(date(2026, 10, 22), 3, bjv.UTC_24H_SESSIONS) == [
        date(2026, 10, 23), date(2026, 10, 24), date(2026, 10, 25)]           # weekend counted


def test_old_resolved_series_read_is_gone_and_reads_are_completed_closes_only():
    src = SCRIPT.read_text(encoding="utf-8")
    body = src.split('"""', 2)[2]   # past the module docstring, which explains the old bug
    code = "\n".join(line for line in body.splitlines() if not line.lstrip().startswith("#"))
    assert not re.search(r"\bFROM\s+resolved_series\b", code, re.IGNORECASE)
    assert "feature_registry" not in code
    assert "from store.observations import Observation, read_daily_policy_n" in src
    assert "read_window(" not in code


def test_scheduler_registration_is_unchanged():
    src = (ROOT / "intelligence" / "scheduler.py").read_text(encoding="utf-8")
    assert "from scripts.backfill_journal_verdicts import run as _score_journal" in src
    assert '_sched.every().day.at("06:45").do(_journal_verdicts_daily)' in src
