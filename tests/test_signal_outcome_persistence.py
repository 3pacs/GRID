"""Offline replay and forward scoring of ``signal_sources`` (S8, sig-eval-4-sessions).

Background (griddb, read-only, 2026-11-06): ``signal_sources.outcome`` holds
693k WRONG / 85k CORRECT / 125k EXPIRED / 132k null over 1.03M rows because
``trust_scorer.score_pending_signals`` writes WRONG for every case that is not
a >1% move in the signal's direction -- a 0.3% drift, a move the right way
that fell short, a weekend-dated signal priced off Friday's close. The
corrected outcome is appended to ``signal_source_outcomes`` (never the legacy
column), versioned, and only once it is final.

Single-stock closes exist on griddb only as TIINGO EOD pulls written under
``YF:{ticker}:close`` (~17:30 ET on the session day); yfinance pulls the
listed ETFs/indices. The accessor pools both raw-close writers and refuses a
label whose completed vintages disagree.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import evaluation.signal_outcomes as so
from evaluation.prices import (
    AmbiguousPriceError,
    CompletedCloseAccessor,
    UnsupportedInstrumentError,
    us_listed_close_boundary,
)
from store.observations import read_completed_closes

ROOT = Path(__file__).resolve().parent.parent
UTC = timezone.utc
NY = ZoneInfo("America/New_York")


def ny(d: date, hh: int, mm: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, tzinfo=NY)


class Result:
    def __init__(self, rows=(), scalar=None, rowcount=None):
        self.rows = list(rows)
        self._scalar = scalar
        self.rowcount = rowcount

    def fetchall(self):
        return self.rows

    def scalar(self):
        return self._scalar


class FakeDB:
    """Answers the raw_series window SQL, the signal_sources selects and the insert."""

    def __init__(self, signals=(), *, table_exists=True):
        self.raw: list[dict] = []
        self.signals = list(signals)
        self.table_exists = table_exists
        self.existing: set[tuple[int, str]] = set()
        self.inserted: list[dict] = []
        self.statements: list[str] = []
        self.raw_reads: list[dict] = []

    # connection protocol
    def connect(self):
        return self

    def begin(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def rollback(self):
        return None

    def add_close(self, ticker, d, value, pulled, *, source="TIINGO", status="SUCCESS", payload=None):
        self.raw.append({"sid": f"YF:{ticker}:close", "od": d, "val": value, "pulled": pulled,
                         "src": source, "status": status, "payload": payload})

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.statements.append(sql)
        p = params or {}
        if "to_regclass" in sql:
            return Result(scalar="signal_source_outcomes" if self.table_exists else None)
        if "FROM raw_series" in sql:
            self.raw_reads.append(dict(p))
            rows = [r for r in self.raw
                    if r["sid"] == p["sid"] and r["status"] == p["ok"]
                    and r["src"].lower() == p["source"].lower()
                    and (p["start"] is None or r["od"] >= p["start"])
                    and (p["as_of"] is None or r["od"] <= p["as_of"])
                    and (p["as_of_ts"] is None or r["pulled"] <= p["as_of_ts"])]
            rows.sort(key=lambda r: (r["od"], -r["pulled"].timestamp()))
            return Result([(r["od"], r["val"], r["pulled"], r["src"], r["payload"]) for r in rows])
        if "SELECT DISTINCT ticker" in sql:
            return Result(sorted({(s[2],) for s in self.signals if s[2]}))
        if "FROM signal_sources s" in sql:
            rows = [s for s in self.signals if s[2]]
            if "s.ticker = :ticker" in sql:
                rows = [s for s in rows if s[2] == p["ticker"] and s[0] % p["mod"] == 0]
            if "NOT EXISTS" in sql:
                rows = [s for s in rows if (s[0], p["version"]) not in self.existing]
            if "s.signal_date >= :since" in sql and p.get("since") is not None:
                rows = [s for s in rows if s[3] >= p["since"]]
            rows.sort(key=lambda s: s[0])
            if "LIMIT :n" in sql:
                rows = rows[: p["n"]]
            return Result(rows)
        if "INSERT INTO signal_source_outcomes" in sql:
            batch = params if isinstance(params, list) else [params]
            fresh = [b for b in batch if (b["signal_source_id"], b["evaluation_version"]) not in self.existing]
            for b in fresh:
                self.existing.add((b["signal_source_id"], b["evaluation_version"]))
            self.inserted.extend(fresh)
            return Result(rowcount=len(fresh))
        raise AssertionError("unmodeled SQL: " + sql)


# Calendar: Mon 2026-09-14 .. Fri 09-18, Mon 09-21 .. Fri 09-25 (EDT).
CLOSES = {
    date(2026, 9, 14): 100.0, date(2026, 9, 15): 100.0, date(2026, 9, 16): 101.0,
    date(2026, 9, 17): 102.0, date(2026, 9, 18): 103.0, date(2026, 9, 21): 100.3,
    date(2026, 9, 22): 101.5, date(2026, 9, 23): 98.0, date(2026, 9, 24): 99.0,
    date(2026, 9, 25): 104.0,
}
EVAL_AT = ny(date(2026, 11, 6), 9)


def tiingo_eod(db, ticker="AAA", closes=CLOSES):
    for d, v in closes.items():
        db.add_close(ticker, d, v, ny(d, 17, 30))


def signal(id_, ticker, d, signal_type="BUY", *, created=None, source_type="scanner", value=None, legacy="WRONG"):
    created = created or ny(d, 10)
    return (id_, source_type, ticker, d, signal_type, value, created, legacy)


# ── read_completed_closes / boundary / accessor ──────────────────────────────


def test_close_boundary_is_17_et_on_sessions_only_and_dst_aware():
    assert us_listed_close_boundary(date(2026, 9, 15)) == datetime(2026, 9, 15, 21, 0, tzinfo=UTC)   # EDT
    assert us_listed_close_boundary(date(2026, 12, 15)) == datetime(2026, 12, 15, 22, 0, tzinfo=UTC)  # EST
    assert us_listed_close_boundary(date(2026, 9, 19)) is None          # Saturday
    assert us_listed_close_boundary(date(2026, 11, 26)) is None         # Thanksgiving


def test_completed_closes_drop_intraday_and_labelled_incomplete_pulls():
    db = FakeDB()
    d = date(2026, 9, 15)
    db.add_close("AAA", d, 99.0, ny(d, 13), source="yfinance")                       # intraday
    db.add_close("AAA", d, 98.0, ny(d, 18), source="yfinance",
                 payload={"capture_state": "INCOMPLETE"})                           # labelled provisional
    db.add_close("AAA", d, 100.0, ny(d, 17, 30))                                     # TIINGO EOD
    db.add_close("AAA", d, 100.00001, ny(d, 22), source="yfinance")                  # float noise agrees

    out = read_completed_closes(db, "YF:AAA:close", start=d, end=d, boundary=us_listed_close_boundary,
                                source=("yfinance", "TIINGO"), as_of_ts=EVAL_AT)

    cc = out[d]
    assert not cc.ambiguous and len(cc.vintages) == 2
    assert cc.first.value == 100.0 and cc.latest.value == pytest.approx(100.00001)
    assert cc.source == "TIINGO+yfinance"
    assert cc.known_at(ny(d, 17)) is None and cc.known_at(ny(d, 18)).value == 100.0


def test_completed_closes_that_disagree_across_sources_are_ambiguous():
    db = FakeDB()
    d = date(2026, 9, 15)
    db.add_close("AAA", d, 100.0, ny(d, 17, 30))
    db.add_close("AAA", d, 99.2, ny(d, 23), source="yfinance")   # adjusted value under the raw id
    out = read_completed_closes(db, "YF:AAA:close", start=d, end=d, boundary=us_listed_close_boundary,
                                source=("yfinance", "TIINGO"))
    assert out[d].ambiguous


def test_completed_closes_respect_the_evaluation_cutoff_and_refuse_other_sources():
    db = FakeDB()
    d = date(2026, 9, 15)
    db.add_close("AAA", d, 100.0, ny(date(2026, 11, 7), 9))
    assert read_completed_closes(db, "YF:AAA:close", start=d, end=d, boundary=us_listed_close_boundary,
                                 source="TIINGO", as_of_ts=EVAL_AT) == {}
    with pytest.raises(ValueError):
        read_completed_closes(db, "YF:AAA:close", start=d, end=d, boundary=us_listed_close_boundary,
                              source="KAGGLE_BULK")
    with pytest.raises(ValueError):
        read_completed_closes(db, "FRED:SP500", start=d, end=d, boundary=us_listed_close_boundary)


def test_accessor_reads_each_instrument_once_and_refuses_unsupported_or_ambiguous():
    db = FakeDB()
    tiingo_eod(db)
    db.add_close("BBB", date(2026, 9, 15), 50.0, ny(date(2026, 9, 15), 17, 30))
    db.add_close("BBB", date(2026, 9, 15), 52.0, ny(date(2026, 9, 16), 2), source="yfinance")
    acc = CompletedCloseAccessor(db, start=date(2026, 9, 1), evaluated_at=EVAL_AT)

    assert acc("AAA", date(2026, 9, 19)).bar_date == date(2026, 9, 18)   # latest at or before
    assert acc("AAA", date(2026, 9, 16)).price == 101.0
    assert acc("AAA", date(2026, 9, 1)) is None
    assert acc.reads == 1
    with pytest.raises(AmbiguousPriceError):
        acc("BBB", date(2026, 9, 15))
    for bad in ("BTC-USD", "aapl", "7203.T", "ES=F", ""):
        with pytest.raises(UnsupportedInstrumentError):
            acc(bad, date(2026, 9, 15))


# ── evaluate_signal with NYSE sessions ───────────────────────────────────────


def _rec(**kw):
    base = dict(source_type="scanner", instrument="AAA", signal_date=date(2026, 9, 15),
                direction="BUY", horizon_days=7, created_at=ny(date(2026, 9, 15), 10))
    base.update(kw)
    return so.SignalRecord(**base)


def _eval(rec, db=None, today=date(2026, 11, 6)):
    if db is None:
        db = FakeDB()
        tiingo_eod(db)
    acc = CompletedCloseAccessor(db, start=date(2026, 9, 1), evaluated_at=ny(today, 9))
    from ingestion.market_calendar import is_market_open
    return so.evaluate_signal(rec, acc, dead_band_pct=1.0, today=today, session_calendar=is_market_open)


def test_weekend_known_signal_enters_monday_and_never_borrows_an_earlier_exit():
    # Created Saturday 09-19: the legacy scorer priced it off Friday's close.
    rec = _rec(signal_date=date(2026, 9, 19), created_at=ny(date(2026, 9, 19), 9))
    out = _eval(rec)
    # Exit session 09-28 has no close in the fixture -> refusal, never 09-25.
    assert out.evaluation_version == so.SESSION_EVALUATION_VERSION
    assert out.outcome == "INELIGIBLE" and out.eligibility_reason == so.REASON_STALE_EXIT
    assert out.entry_price_date == date(2026, 9, 21)

    db = FakeDB()
    tiingo_eod(db, closes={**CLOSES, date(2026, 9, 28): 104.0})
    out = _eval(rec, db=db)
    assert (out.entry_price_date, out.exit_price_date) == (date(2026, 9, 21), date(2026, 9, 28))
    assert out.outcome == "CORRECT"


def test_holiday_exit_rolls_forward_to_the_next_session():
    db = FakeDB()
    closes = {date(2026, 11, 19): 100.0, date(2026, 11, 27): 103.0}   # Thu 11-26 is Thanksgiving
    tiingo_eod(db, closes=closes)
    out = _eval(_rec(signal_date=date(2026, 11, 19), created_at=ny(date(2026, 11, 19), 10)), db=db,
                today=date(2026, 12, 10))
    assert out.exit_price_date == date(2026, 11, 27) and out.outcome == "CORRECT"


def test_small_move_in_the_right_direction_is_no_move_not_wrong():
    # Legacy: +0.3% for a BUY -> WRONG. Here: dead band -> NO_MOVE.
    out = _eval(_rec(signal_date=date(2026, 9, 14), created_at=ny(date(2026, 9, 14), 10)))
    assert out.raw_return == pytest.approx(0.3) and out.outcome == "NO_MOVE"


def test_without_calendar_the_original_version_and_rules_are_unchanged():
    db = FakeDB()
    tiingo_eod(db)
    acc = CompletedCloseAccessor(db, start=date(2026, 9, 1), evaluated_at=EVAL_AT)
    out = so.evaluate_signal(_rec(signal_date=date(2026, 9, 19), created_at=ny(date(2026, 9, 19), 9)),
                             acc, today=date(2026, 11, 6))
    assert out.evaluation_version == so.EVALUATION_VERSION
    assert out.eligibility_reason == so.REASON_STALE_ENTRY   # Saturday has no bar


# ── row -> record, finality ──────────────────────────────────────────────────


def test_record_uses_legacy_direction_inference_horizon_and_yf_symbol():
    rows = [
        so.SignalSourceRow(1, "options_flow", "aapl", date(2026, 9, 15), "UNUSUAL_OPTIONS",
                           {"direction": "CALL"}, ny(date(2026, 9, 15), 10), "WRONG"),
        so.SignalSourceRow(2, "insider", "BRK.B", date(2026, 9, 15), "insider_sell", None,
                           ny(date(2026, 9, 15), 10), "CORRECT"),
        so.SignalSourceRow(3, "quiverquant:offexchange", "XYZ", date(2026, 9, 15), "off_exchange",
                           '{"dpi": 0.4}', ny(date(2026, 9, 15), 10), None),
    ]
    recs = [so.record_from_row(r) for r in rows]
    assert [r.direction for r in recs] == ["BUY", "SELL", "UNKNOWN"]
    assert [r.horizon_days for r in recs] == [7, 14, 7]
    assert [r.instrument for r in recs] == ["AAPL", "BRK-B", "XYZ"]
    assert recs[0].metadata["legacy_outcome"] == "WRONG"


def test_only_final_outcomes_are_persisted():
    rec = _rec()
    base = dict(signal_source_id=1, source_type="scanner", instrument="AAA", signal_date=rec.signal_date,
                direction="BUY", horizon_days=7, evaluation_version=so.SESSION_EVALUATION_VERSION,
                origin_tag="unknown", dead_band_pct=1.0, cost_bps=0.0, entry_price=None,
                entry_price_date=None, entry_price_basis=None, exit_price=None, exit_price_date=None,
                exit_price_basis=None, raw_return=None, cost_adjusted_return=None)
    unresolved = so.SignalOutcomeRecord(**base, outcome="UNRESOLVED")
    missing = so.SignalOutcomeRecord(**base, outcome="INELIGIBLE", eligibility_reason=so.REASON_MISSING_EXIT)
    unknown = so.SignalOutcomeRecord(**base, outcome="INELIGIBLE", eligibility_reason=so.REASON_UNKNOWN_DIRECTION)
    final_day = rec.signal_date + timedelta(days=7 + so.FINAL_GRACE_DAYS)
    assert not so.is_final(unresolved, rec, date(2027, 1, 1))
    assert not so.is_final(missing, rec, final_day - timedelta(days=1))
    assert so.is_final(missing, rec, final_day)
    assert so.is_final(unknown, rec, rec.signal_date)


# ── replay + forward ─────────────────────────────────────────────────────────


def _signals():
    return [
        signal(10, "AAA", date(2026, 9, 14)),                      # +0.3% -> NO_MOVE (legacy WRONG)
        signal(11, "AAA", date(2026, 9, 16), "SELL"),              # 101 -> 98 -> CORRECT
        signal(12, "AAA", date(2026, 9, 15), "off_exchange"),      # unknown direction
        signal(13, "ZZZ", date(2026, 9, 15)),                      # no prices at all
        signal(14, "AAA", date(2026, 11, 5), created=ny(date(2026, 11, 5), 10)),   # inside horizon
    ]


def _db(**kw):
    db = FakeDB(_signals(), **kw)
    tiingo_eod(db)
    db.add_close("AAA", date(2026, 11, 5), 105.0, ny(date(2026, 11, 5), 17, 30))
    return db


def test_replay_dry_run_projects_without_writing():
    db = _db()
    out = so.replay_signal_sources(db, evaluated_at=EVAL_AT)
    assert out["dry_run"] is True and out["written"] == 0 and db.inserted == []
    assert out["rows"] == 5 and out["tickers"] == 2
    assert out["by_outcome"] == {"INELIGIBLE": 2, "CORRECT": 1, "NO_MOVE": 1, "UNRESOLVED": 1}
    assert out["by_reason"] == {"missing_entry_price": 1, "unknown_direction": 1}
    assert out["legacy_vs_new"]["WRONG->NO_MOVE"] == 1
    assert not any("signal_source_outcomes" in s for s in db.statements)


def test_replay_apply_appends_final_rows_once_and_resumes():
    db = _db()
    first = so.replay_signal_sources(db, apply=True, evaluated_at=EVAL_AT)
    assert first["written"] == 4                     # everything except the UNRESOLVED row
    ids = sorted(r["signal_source_id"] for r in db.inserted)
    assert ids == [10, 11, 12, 13]
    row = next(r for r in db.inserted if r["signal_source_id"] == 11)
    assert row["outcome"] == "CORRECT" and row["legacy_outcome"] == "WRONG"
    assert row["evaluation_version"] == so.SESSION_EVALUATION_VERSION
    second = so.replay_signal_sources(db, apply=True, evaluated_at=EVAL_AT)
    assert second["rows"] == 1 and second["written"] == 0     # only the unresolved row is reread


def test_replay_sample_mod_and_ticker_filter():
    db = FakeDB(_signals())
    tiingo_eod(db)
    out = so.replay_signal_sources(db, evaluated_at=EVAL_AT, sample_mod=2, tickers=["AAA"])
    assert out["rows"] == 3   # ids 10, 12, 14


def test_replay_apply_refuses_without_the_table():
    with pytest.raises(RuntimeError):
        so.replay_signal_sources(FakeDB(_signals(), table_exists=False), apply=True, evaluated_at=EVAL_AT)


def test_forward_scoring_writes_final_recent_rows_and_skips_without_table():
    assert so.score_forward(FakeDB(_signals(), table_exists=False))["skipped"]
    db = _db()
    out = so.score_forward(db, evaluated_at=EVAL_AT, lookback_days=60)
    assert out["written"] == 4 and out["rows"] == 5


def test_trust_cycle_hook_is_flag_gated_and_never_raises(monkeypatch):
    import config
    import intelligence.trust_scorer as ts

    calls = []
    monkeypatch.setattr(so, "score_forward", lambda engine: calls.append(engine) or {"written": 0})
    monkeypatch.setattr(config.settings, "GRID_ENABLE_SIGNAL_OUTCOME_FORWARD_SCORING", False, raising=False)
    assert "skipped" in ts._score_forward_outcomes("engine") and calls == []
    monkeypatch.setattr(config.settings, "GRID_ENABLE_SIGNAL_OUTCOME_FORWARD_SCORING", True, raising=False)
    assert ts._score_forward_outcomes("engine") == {"written": 0} and calls == ["engine"]

    def boom(engine):
        raise RuntimeError("db down")
    monkeypatch.setattr(so, "score_forward", boom)
    assert ts._score_forward_outcomes("engine") == {"error": "db down"}


def test_cli_replay_subcommand_is_a_dry_run_by_default(capsys):
    import scripts.evaluate_signals as cli

    db = FakeDB(_signals())
    tiingo_eod(db)
    assert cli.main_replay(["--ticker", "AAA", "--evaluated-at", EVAL_AT.isoformat()], engine=db) == 0
    assert '"dry_run": true' in capsys.readouterr().out and db.inserted == []
    assert cli.main_replay(["--evaluated-at", "2026-11-06T09:00:00"], engine=db) == 2   # naive refused


# ── Immutability guards ──────────────────────────────────────────────────────


def test_no_code_path_rewrites_legacy_outcomes_or_journal_rows():
    for rel in ("evaluation/signal_outcomes.py", "evaluation/prices.py", "store/observations.py"):
        src = (ROOT / rel).read_text(encoding="utf-8")
        assert not re.search(r"\bUPDATE\s+(signal_sources|decision_journal|signal_source_outcomes)\b", src, re.I), rel
        assert not re.search(r"\bDELETE\s+FROM\b", src, re.I), rel
    assert "ON CONFLICT (signal_source_id, evaluation_version) DO NOTHING" in so._INSERT_OUTCOME_SQL


def test_migration_is_additive_append_only_and_granted():
    src = (ROOT / "migrations" / "versions" / "outcome_rescore_tables_20261106.py").read_text(encoding="utf-8")
    assert 'down_revision = "finra_short_volume_agg_20261105"' in src
    for table in ("signal_source_outcomes", "decision_journal_verdicts"):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in src
        assert f"BEFORE UPDATE OR DELETE ON {table}" in src
        assert f"BEFORE TRUNCATE ON {table}" in src
        assert f"GRANT SELECT, INSERT ON {table} TO grid" in src
        assert f"GRANT USAGE, SELECT ON SEQUENCE {table}_id_seq TO grid" in src
    # Append-only is also a privilege: grid never gets UPDATE/DELETE/TRUNCATE.
    assert not re.search(r"GRANT\s+ALL\b", src, re.I)
    assert not re.search(r"GRANT[^;']*\b(UPDATE|DELETE|TRUNCATE)\b", src, re.I)
    assert not re.search(r"ALTER\s+TABLE\s+(signal_sources|decision_journal)\b", src, re.I)
