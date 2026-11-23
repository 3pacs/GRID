"""Regression tests for verified postmortem, outcome-event, and thesis-monitor defects.

Covers:
1. Unit mixing in intelligence/postmortem.py (option premium vs underlying spot).
2. Staleness check using future expiry instead of decision-time signal age.
3. EXPIRED worthless (-100%) mapping to PARTIAL instead of MISS (loss).
4. PostmortemCompleted stamping producer_module='intelligence.postmortem'
   instead of a key matching ModelRegistry.decay_model_by_source.
5. apply_contagion_feedback using abs(actual_price_move_pct) and reinforcing
   edges on wrong-direction moves.
6. thesis_invalidation_monitor querying non-existent decision_journal columns
   (and ticker_metrics_daily.as_of_date) and swallowing DB errors at log.debug.
7. The entry anchor and scanner context read by date only, so an intraday
   decision could anchor on that day's later close and a later revision could
   move the anchor (Codex HOLD 6011903681): both are now fenced at generated_at.

All tests are offline (synthetic inputs, mocks, or in-memory SQLite).
"""

from __future__ import annotations

import json
import re
import sys
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine, event, text

from contracts.handlers import oracle_weights
from intelligence import postmortem as pm_mod
from intelligence import thesis_invalidation_monitor as tim
from intelligence.postmortem import (
    PostMortem,
    _classify_failure,
    _compute_implied_pct,
    _emit_trade_postmortem,
    _normalize_producer_key,
    _verdict_from_outcome,
    apply_contagion_feedback,
    generate_postmortem,
    generate_prediction_postmortem,
)
from oracle.engine import DEFAULT_MODELS, ModelRegistry
from oracle.trace_evolver import FailurePattern, TargetedMutator, TraceAnalyzer


def _capture_emits(monkeypatch) -> list:
    """Patch ``contracts.emit.emit`` safely (contracts.__init__ re-exports emit)."""
    captured: list = []

    def _fake_emit(contract):
        captured.append(contract)
        return contract.event_id

    emit_mod = sys.modules.get("contracts.emit")
    if emit_mod is None:
        import contracts.emit as _imported  # noqa: F401
        emit_mod = sys.modules["contracts.emit"]
    monkeypatch.setattr(emit_mod, "emit", _fake_emit)
    return captured


# ── Defect 1: Unit mixing (option premium vs underlying spot) ───────────────


class TestDefect1UnitMixing:
    def test_losing_call_does_not_compare_underlying_spot_against_option_premium(self):
        """Option premium $2.50 vs underlying path $180 -> $170 must NOT yield
        +7,100% and mislabel a monotonic losing CALL as right_signal_wrong_timing."""
        cat, root_cause, _wrong, _right, _missed = _classify_failure(
            ticker="AAPL",
            direction="CALL",
            entry_price=2.50,  # option premium paid
            strike=185.0,
            target=5.00,
            expiry=date(2026, 4, 15),
            outcome="EXPIRED",
            actual_return=-1.0,
            price_path=[
                {"date": "2026-04-10", "price": 180.0},
                {"date": "2026-04-13", "price": 178.0},
                {"date": "2026-04-14", "price": 175.0},
                {"date": "2026-04-15", "price": 170.0},
            ],
            data_at_decision={
                "decision_date": "2026-04-10",
                "scanner_signals": {
                    "spot_price": 180.0,
                    "signal_date": "2026-04-10",
                },
            },
            sanity={},
        )
        assert cat == "wrong_signal", f"Expected wrong_signal, got {cat}: {root_cause}"
        assert "7100" not in root_cause

    def test_losing_put_gets_right_signal_wrong_timing_when_underlying_dropped_first(self):
        """PUT bought with underlying at $180, option premium $2.50. Underlying
        dropped -2.22% to $176 before rallying to $185 -> right_signal_wrong_timing."""
        cat, root_cause, _wrong, _right, _missed = _classify_failure(
            ticker="AAPL",
            direction="PUT",
            entry_price=2.50,  # option premium paid
            strike=175.0,
            target=5.00,
            expiry=date(2026, 4, 15),
            outcome="EXPIRED",
            actual_return=-1.0,
            price_path=[
                {"date": "2026-04-10", "price": 180.0},
                {"date": "2026-04-13", "price": 176.0},  # -2.22% favorable for PUT
                {"date": "2026-04-15", "price": 185.0},
            ],
            data_at_decision={
                "decision_date": "2026-04-10",
                "scanner_signals": {
                    "spot_price": 180.0,
                    "signal_date": "2026-04-10",
                },
            },
            sanity={},
        )
        assert cat == "right_signal_wrong_timing", f"Got {cat}: {root_cause}"
        assert "2.2%" in root_cause

    def test_missing_underlying_spot_labels_insufficient_data(self):
        """When no underlying spot at entry is available, fail closed with
        'insufficient_data' rather than dividing by option premium or guessing."""
        cat, root_cause, _wrong, _right, _missed = _classify_failure(
            ticker="AAPL",
            direction="CALL",
            entry_price=2.50,  # option premium only — NOT underlying spot
            strike=185.0,
            target=5.00,
            expiry=date(2026, 4, 15),
            outcome="EXPIRED",
            actual_return=-1.0,
            price_path=[
                {"date": "2026-04-13", "price": 180.0},
                {"date": "2026-04-15", "price": 170.0},
            ],
            data_at_decision={"decision_date": "2026-04-10"},
            sanity={},
        )
        assert cat == "insufficient_data", f"Expected insufficient_data, got {cat}: {root_cause}"

    def test_generate_postmortem_loads_pit_raw_series_close_when_scanner_spot_missing(self, monkeypatch):
        """When options_daily_signals has no row and the price path falls back to
        raw_series, the entry anchor is the completed close known at generated_at
        (the prior session for an 11:00 ET decision), never the option premium
        and never the decision day's own close."""
        gen_ts = _ny(date(2026, 4, 10), 11).astimezone(timezone.utc)  # Friday, intraday
        db = _ClockFakeDB(_trade_row(7, "AAPL", "CALL", date(2026, 4, 24), gen_ts,
                                     _ny(date(2026, 4, 24), 17)))
        # Price path fell monotonically from 180 to 170.
        db.add_close("AAPL", date(2026, 4, 9), 180.0, _ny(date(2026, 4, 9), 17, 30))
        db.add_close("AAPL", date(2026, 4, 10), 180.0, _ny(date(2026, 4, 10), 17, 30))
        db.add_close("AAPL", date(2026, 4, 17), 175.0, _ny(date(2026, 4, 17), 17, 30))
        db.add_close("AAPL", date(2026, 4, 24), 170.0, _ny(date(2026, 4, 24), 17, 30))

        pm = _run_trade_postmortem(monkeypatch, db, 7)
        assert pm is not None
        assert pm.failure_category == "wrong_signal"
        assert pm.actual_return == pytest.approx(-1.0)
        assert pm.data_at_decision["underlying_entry_spot"] == pytest.approx(180.0)
        assert "2026-04-09" in pm.data_at_decision["underlying_entry_spot_basis"]
        # The entry read went through store.observations, fenced at generated_at.
        entry_reads = [p for sql, p in db.statements if "FROM raw_series r" in sql]
        assert entry_reads, "entry anchor must be read through store.observations"
        assert all(p["as_of_ts"] == gen_ts for p in entry_reads)


# ── Decision-clock fence on the entry anchor (Codex HOLD 6011903681) ───────

_NY = ZoneInfo("America/New_York")


def _ny(d: date, hh: int, mm: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, tzinfo=_NY)


class _Res:
    def __init__(self, rows=(), one=None):
        self._rows = list(rows)
        self._one = one

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._one


class _ClockFakeDB:
    """Answers generate_postmortem's trade, options_daily_signals and raw_series reads.

    Rows carry their write clock (``created_at`` / ``pull_timestamp``) and the
    fake applies exactly the filters a statement states, so a read without a
    decision-clock fence sees rows written after the decision, as the real
    tables would show them.
    """

    def __init__(self, trade_row):
        self.trade_row = trade_row
        self.closes: list[dict] = []
        self.signals: list[dict] = []
        self.statements: list[tuple[str, dict]] = []

    def add_close(self, ticker, d, value, pulled, *, source="TIINGO", status="SUCCESS", payload=None):
        self.closes.append({"sid": f"YF:{ticker}:close", "od": d, "val": value,
                            "pulled": pulled.astimezone(timezone.utc), "src": source,
                            "status": status, "payload": payload})

    def add_signal(self, ticker, d, spot, created_at, *, pcr=1.0, iv_skew=0.0):
        self.signals.append({"t": ticker, "sd": d, "spot": spot, "pcr": pcr, "iv_skew": iv_skew,
                             "created": None if created_at is None else created_at.astimezone(timezone.utc)})

    def connect(self):
        return self

    def begin(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, stmt, params=None):
        sql = " ".join(str(stmt).split())
        p = dict(params or {})
        self.statements.append((sql, p))
        if "FROM options_recommendations" in sql:
            return _Res(one=self.trade_row)
        if "options_daily_signals" in sql:
            return self._signal_read(sql, p)
        if "FROM raw_series" in sql:
            return self._raw_read(sql, p)
        return _Res()

    def _signal_read(self, sql, p):
        rows = [s for s in self.signals if s["t"] == p["t"]]
        if "signal_date >= :s" in sql:  # price path
            rows = sorted((s for s in rows if p["s"] <= s["sd"] <= p["e"] and s["spot"] > 0),
                          key=lambda s: s["sd"])
            return _Res([(s["sd"], s["spot"]) for s in rows])
        rows = [s for s in rows if s["sd"] <= p["d"]]
        if "created_at <= :decision_ts" in sql:
            rows = [s for s in rows if s["created"] is not None and s["created"] <= p["decision_ts"]]
        if "spot_price > 0" in sql:
            rows = [s for s in rows if s["spot"] > 0]
        rows.sort(key=lambda s: s["sd"], reverse=True)
        if not rows:
            return _Res()
        s = rows[0]
        if sql.startswith("SELECT spot_price"):
            return _Res(one=(s["spot"],))
        return _Res(one=(s["pcr"], 0.3, s["iv_skew"], None, s["spot"], 1000, -0.01, 0.1,
                         s["sd"], s["created"]))

    def _raw_read(self, sql, p):
        rows = [r for r in self.closes if r["sid"] == p["sid"]]
        if sql.startswith("SELECT obs_date, value FROM raw_series"):  # price path
            rows = [r for r in rows if r["status"] == "SUCCESS" and r["val"] > 0
                    and p["s"] <= r["od"] <= p["e"]]
            rows.sort(key=lambda r: (r["od"], -r["pulled"].timestamp()))
            return _Res([(r["od"], r["val"]) for r in rows])
        if sql.startswith("SELECT value FROM raw_series"):  # obs_date-only entry read
            rows = [r for r in rows if r["status"] == "SUCCESS" and r["val"] > 0 and r["od"] <= p["d"]]
            rows.sort(key=lambda r: (r["od"], r["pulled"]), reverse=True)
            return _Res(one=(rows[0]["val"],) if rows else None)
        if sql.startswith("SELECT r.obs_date, r.value, r.pull_timestamp, sc.name"):  # store.observations
            rows = [r for r in rows if r["status"] == p["ok"]
                    and (p["source"] is None or r["src"].lower() == p["source"].lower())
                    and (p["start"] is None or r["od"] >= p["start"])
                    and (p["as_of"] is None or r["od"] <= p["as_of"])
                    and (p["as_of_ts"] is None or r["pulled"] <= p["as_of_ts"])]
            rows.sort(key=lambda r: (r["od"], -r["pulled"].timestamp()))
            if "r.raw_payload" in sql:
                return _Res([(r["od"], r["val"], r["pulled"], r["src"], r["payload"]) for r in rows])
            return _Res([(r["od"], r["val"], r["pulled"], r["src"]) for r in rows])
        raise AssertionError("unmodeled raw_series SQL: " + sql)


def _trade_row(trade_id, ticker, direction, expiry, gen_ts, closed_ts):
    return (
        trade_id, ticker, direction, 100.0, expiry, 2.50,
        5.00, 1.25, 0.7, f"{direction.lower()} thesis", "{}", gen_ts,
        "EXPIRED", -1.0, closed_ts, "", "[]", "[]",
    )


def _run_trade_postmortem(monkeypatch, db, trade_id):
    monkeypatch.setattr(pm_mod, "_ensure_tables", lambda _e: None)
    monkeypatch.setattr(pm_mod, "_store_postmortem", lambda *a, **k: None)
    monkeypatch.setattr(pm_mod, "_emit_trade_postmortem", lambda **k: None)
    monkeypatch.setattr(
        pm_mod,
        "_get_llm_postmortem",
        lambda **k: {"recommended_fix": k["root_cause"], "what_we_missed": "", "confidence": 0.5},
    )
    return generate_postmortem(db, trade_id=trade_id)


class TestEntryAnchorDecisionClockFence:
    """The entry anchor and scanner context hold only what was known at generated_at.

    Calendar (EDT): Mon 2026-04-13 .. Fri 04-17, Sat 04-18, Mon 04-20, Tue 04-21.
    Every TIINGO close is pulled at 17:30 ET on its session day unless stated.
    """

    WED = date(2026, 4, 15)

    def test_intraday_decision_never_anchors_on_that_days_later_close(self, monkeypatch):
        """11:00 ET decision on Wednesday: Wednesday's close (pulled 17:30 ET) did
        not exist yet. Anchoring on it (104) reads the CALL as never right; the
        known anchor (Tuesday's 100) shows it was +4 % right before expiry."""
        gen_ts = _ny(self.WED, 11).astimezone(timezone.utc)
        db = _ClockFakeDB(_trade_row(11, "AAPL", "CALL", date(2026, 4, 17), gen_ts,
                                     _ny(date(2026, 4, 17), 17)))
        for d, v in ((date(2026, 4, 14), 100.0), (self.WED, 104.0),
                     (date(2026, 4, 16), 103.0), (date(2026, 4, 17), 102.5)):
            db.add_close("AAPL", d, v, _ny(d, 17, 30))

        pm = _run_trade_postmortem(monkeypatch, db, 11)
        assert pm.data_at_decision["underlying_entry_spot"] == pytest.approx(100.0)
        assert "2026-04-14" in pm.data_at_decision["underlying_entry_spot_basis"]
        assert pm.failure_category == "right_signal_wrong_timing", pm.root_cause

    def test_later_revision_of_prior_close_leaves_entry_anchor_unchanged(self, monkeypatch):
        """Saturday decision: Friday's 100 close was known. A re-pull of Friday
        on Monday (110) is a later revision and must not move the anchor."""
        gen_ts = _ny(date(2026, 4, 18), 10).astimezone(timezone.utc)
        fri = date(2026, 4, 17)

        def build():
            db = _ClockFakeDB(_trade_row(12, "AAPL", "CALL", date(2026, 4, 21), gen_ts,
                                         _ny(date(2026, 4, 21), 17)))
            db.add_close("AAPL", fri, 100.0, _ny(fri, 17, 30))
            db.add_close("AAPL", date(2026, 4, 20), 103.0, _ny(date(2026, 4, 20), 17, 30))
            db.add_close("AAPL", date(2026, 4, 21), 102.0, _ny(date(2026, 4, 21), 17, 30))
            return db

        baseline = _run_trade_postmortem(monkeypatch, build(), 12)
        revised_db = build()
        revised_db.add_close("AAPL", fri, 110.0, _ny(date(2026, 4, 20), 9))  # revision after the decision
        revised = _run_trade_postmortem(monkeypatch, revised_db, 12)

        assert baseline.data_at_decision["underlying_entry_spot"] == pytest.approx(100.0)
        assert revised.data_at_decision["underlying_entry_spot"] == pytest.approx(100.0)
        assert revised.failure_category == baseline.failure_category == "right_signal_wrong_timing"

    def test_abstains_when_no_close_was_known_at_decision(self, monkeypatch):
        """Tuesday's close was only backfilled after the decision and Wednesday's
        only capture before the decision was intraday (not a close): nothing was
        provably known, so no anchor and no attribution, with the reason kept."""
        gen_ts = _ny(self.WED, 11).astimezone(timezone.utc)
        db = _ClockFakeDB(_trade_row(13, "AAPL", "CALL", date(2026, 4, 17), gen_ts,
                                     _ny(date(2026, 4, 17), 17)))
        db.add_close("AAPL", date(2026, 4, 14), 100.0, _ny(date(2026, 4, 20), 9))  # backfill
        db.add_close("AAPL", self.WED, 101.0, _ny(self.WED, 10, 30), source="yfinance")  # intraday
        for d, v in ((self.WED, 104.0), (date(2026, 4, 16), 103.0), (date(2026, 4, 17), 102.5)):
            db.add_close("AAPL", d, v, _ny(d, 17, 30))

        pm = _run_trade_postmortem(monkeypatch, db, 13)
        assert "underlying_entry_spot" not in pm.data_at_decision
        reason = pm.data_at_decision["underlying_entry_spot_unavailable"]
        assert gen_ts.isoformat() in reason
        assert pm.failure_category == "insufficient_data"
        assert reason in pm.root_cause

    def test_scanner_lookup_ignores_a_row_written_after_the_decision(self, monkeypatch):
        """Wednesday's options_daily_signals row was written at 16:30 ET, after the
        11:00 ET decision: the scanner context and anchor are Tuesday's row."""
        gen_ts = _ny(self.WED, 11).astimezone(timezone.utc)
        db = _ClockFakeDB(_trade_row(14, "AAPL", "CALL", date(2026, 4, 17), gen_ts,
                                     _ny(date(2026, 4, 17), 17)))
        for d, v, pcr in ((date(2026, 4, 14), 100.0, 0.5), (self.WED, 104.0, 1.5),
                          (date(2026, 4, 16), 103.0, 1.5), (date(2026, 4, 17), 102.5, 1.5)):
            db.add_signal("AAPL", d, v, _ny(d, 16, 30), pcr=pcr)

        pm = _run_trade_postmortem(monkeypatch, db, 14)
        scanner = pm.data_at_decision["scanner_signals"]
        assert scanner["signal_date"] == "2026-04-14"
        assert scanner["put_call_ratio"] == pytest.approx(0.5)
        assert pm.data_at_decision["underlying_entry_spot"] == pytest.approx(100.0)
        assert pm.failure_category == "right_signal_wrong_timing", pm.root_cause
        fenced = [p for sql, p in db.statements
                  if "options_daily_signals" in sql and "signal_date <= :d" in sql]
        assert fenced and all(p["decision_ts"] == gen_ts for p in fenced)

    def test_scanner_row_revised_after_the_decision_is_not_used(self, monkeypatch):
        """Tuesday's row was revised in place on Thursday (its created_at moved past
        the decision), so its value at the decision cannot be shown; Monday's row,
        unchanged since Monday, is the last one provably known."""
        gen_ts = _ny(self.WED, 11).astimezone(timezone.utc)
        db = _ClockFakeDB(_trade_row(15, "AAPL", "CALL", date(2026, 4, 17), gen_ts,
                                     _ny(date(2026, 4, 17), 17)))
        db.add_signal("AAPL", date(2026, 4, 13), 99.0, _ny(date(2026, 4, 13), 16, 30))
        db.add_signal("AAPL", date(2026, 4, 14), 110.0, _ny(date(2026, 4, 16), 9))  # revised later
        for d, v in ((self.WED, 104.0), (date(2026, 4, 16), 103.0), (date(2026, 4, 17), 102.5)):
            db.add_signal("AAPL", d, v, _ny(d, 16, 30))

        pm = _run_trade_postmortem(monkeypatch, db, 15)
        assert pm.data_at_decision["scanner_signals"]["signal_date"] == "2026-04-13"
        assert pm.data_at_decision["underlying_entry_spot"] == pytest.approx(99.0)
        assert pm.failure_category == "right_signal_wrong_timing", pm.root_cause

    def test_scanner_abstains_when_every_row_was_written_after_the_decision(self, monkeypatch):
        """No options_daily_signals row existed by the decision: no scanner
        context, no anchor from the scanner's price source, insufficient_data."""
        gen_ts = _ny(self.WED, 11).astimezone(timezone.utc)
        db = _ClockFakeDB(_trade_row(16, "AAPL", "CALL", date(2026, 4, 17), gen_ts,
                                     _ny(date(2026, 4, 17), 17)))
        db.add_signal("AAPL", date(2026, 4, 14), 100.0, _ny(date(2026, 4, 20), 9))  # backfilled
        for d, v in ((self.WED, 104.0), (date(2026, 4, 16), 103.0), (date(2026, 4, 17), 102.5)):
            db.add_signal("AAPL", d, v, _ny(d, 16, 30))

        pm = _run_trade_postmortem(monkeypatch, db, 16)
        assert "scanner_signals" not in pm.data_at_decision
        assert gen_ts.isoformat() in pm.data_at_decision["scanner_signals_unavailable"]
        assert "underlying_entry_spot" not in pm.data_at_decision
        assert pm.failure_category == "insufficient_data"

    @pytest.mark.parametrize(
        "raw, expected",
        [
            (datetime(2026, 4, 15, 15, 0, tzinfo=timezone.utc), datetime(2026, 4, 15, 15, 0, tzinfo=timezone.utc)),
            (datetime(2026, 4, 15, 15, 0), datetime(2026, 4, 15, 15, 0, tzinfo=timezone.utc)),  # naive = UTC
            (datetime(2026, 4, 15, 11, 0, tzinfo=ZoneInfo("America/New_York")),
             datetime(2026, 4, 15, 15, 0, tzinfo=timezone.utc)),
            ("2026-04-15T15:00:00Z", datetime(2026, 4, 15, 15, 0, tzinfo=timezone.utc)),
            (date(2026, 4, 15), None),     # no time of day: cannot say what was known
            ("2026-04-15", None),
            ("not a time", None),
            (None, None),
        ],
    )
    def test_decision_clock_is_an_aware_instant_or_none(self, raw, expected):
        assert pm_mod._decision_clock(raw) == expected

    def test_bare_date_clock_abstains_without_reading(self, monkeypatch):
        """A decision clock with no time of day cannot fence a same-day close:
        abstain rather than read by date."""
        db = _ClockFakeDB(None)
        db.add_close("AAPL", date(2026, 4, 14), 100.0, _ny(date(2026, 4, 14), 17, 30))
        db.add_signal("AAPL", date(2026, 4, 14), 100.0, _ny(date(2026, 4, 14), 16, 30))
        data = pm_mod._reconstruct_decision_data(db, "AAPL", date(2026, 4, 15), {}, "")
        assert "scanner_signals" not in data
        assert "scanner_signals_unavailable" in data
        spot, note = pm_mod._load_underlying_entry_spot(db, "AAPL", date(2026, 4, 15), data)
        assert spot is None and "timestamp" in note
        assert not [sql for sql, _ in db.statements
                    if "options_daily_signals" in sql or "FROM raw_series" in sql]


# ── Defect 2: Staleness check uses future expiry instead of decision time ──


class TestDefect2Staleness:
    def test_fresh_signal_with_35_dte_expiry_is_not_bad_data(self):
        """Signal observed on the decision date (0 days old) with a 35-DTE expiry
        must NOT be labelled bad_data."""
        cat, root_cause, _w, _r, _m = _classify_failure(
            ticker="NVDA",
            direction="PUT",
            entry_price=3.00,
            strike=120.0,
            target=6.00,
            expiry=date(2026, 5, 15),  # 35 days after decision
            outcome="EXPIRED",
            actual_return=-1.0,
            price_path=[
                {"date": "2026-04-10", "price": 125.0},
                {"date": "2026-05-15", "price": 130.0},
            ],
            data_at_decision={
                "decision_date": "2026-04-10",
                "scanner_signals": {
                    "spot_price": 125.0,
                    "signal_date": "2026-04-10",
                },
            },
            sanity={},
        )
        assert cat == "wrong_signal", f"Fresh signal misclassified as {cat}: {root_cause}"

    def test_genuinely_stale_signal_at_entry_is_bad_data(self):
        """Scanner signal 16 days older than decision time IS stale -> bad_data,
        and root_cause reports age at decision time (16 days), not expiry gap (51 days)."""
        cat, root_cause, _w, _r, _m = _classify_failure(
            ticker="NVDA",
            direction="PUT",
            entry_price=3.00,
            strike=120.0,
            target=6.00,
            expiry=date(2026, 5, 15),  # 51 days after 2026-03-25
            outcome="EXPIRED",
            actual_return=-1.0,
            price_path=[
                {"date": "2026-04-10", "price": 125.0},
                {"date": "2026-05-15", "price": 130.0},
            ],
            data_at_decision={
                "decision_date": "2026-04-10",
                "scanner_signals": {
                    "spot_price": 125.0,
                    "signal_date": "2026-03-25",  # 16 days before 2026-04-10
                },
            },
            sanity={},
        )
        assert cat == "bad_data", f"Expected bad_data, got {cat}: {root_cause}"
        assert "16 days old" in root_cause


# ── Defect 3: EXPIRED worthless maps to MISS (LOSS), not PARTIAL ───────────


class TestDefect3ExpiredMapping:
    def test_expired_outcome_maps_to_miss(self):
        assert _verdict_from_outcome("EXPIRED") == "MISS"
        assert _verdict_from_outcome("expired") == "MISS"
        assert _verdict_from_outcome("LOSS") == "MISS"
        assert _verdict_from_outcome("PARTIAL") == "PARTIAL"
        assert _verdict_from_outcome("WIN") == "HIT"

    def test_expired_options_trade_emits_miss_and_skips_oracle_decay(self, monkeypatch):
        """An EXPIRED worthless options trade must emit verdict='MISS' and
        producer_module='trading.options_recommender', and oracle_weights must
        skip decay (options trades are not attributable to oracle models)."""
        captured = _capture_emits(monkeypatch)
        info_logs: list[str] = []
        monkeypatch.setattr(oracle_weights, "_SKIP_DECAY_LOGGED_EVENT_TYPES", set())
        monkeypatch.setattr(
            oracle_weights.log,
            "info",
            lambda msg, **kw: info_logs.append(msg.format(**kw)),
        )

        pm = PostMortem(
            trade_id=101,
            ticker="AAPL",
            direction="CALL",
            outcome="EXPIRED",
            actual_return=-1.0,
            data_at_decision={
                "decision_signals": [{"name": "iv_skew", "score": 3.0, "direction": "bullish"}],
            },
            thesis_at_decision="call thesis",
            sanity_results_at_decision={},
            what_actually_happened="expired worthless",
            price_path=[],
            failure_category="wrong_signal",
            root_cause="wrong direction",
            which_signals_were_wrong=["iv_skew_flat"],
            which_signals_were_right=[],
            what_we_missed="",
            recommended_fix="",
            confidence_in_analysis=0.7,
            generated_at=datetime(2026, 4, 25, tzinfo=timezone.utc).isoformat(),
        )
        _emit_trade_postmortem(pm=pm, trade_id=101)
        assert len(captured) == 1
        evt = captured[0]
        assert evt.verdict == "MISS"
        assert evt.producer_module == "trading.options_recommender"

        mock_engine = MagicMock()
        with patch("oracle.engine.ModelRegistry") as MockRegistry:
            oracle_weights.on_postmortem_completed(evt, engine=mock_engine)
            oracle_weights.on_postmortem_completed(evt, engine=mock_engine)
            MockRegistry.assert_not_called()
            MockRegistry.return_value.decay_model_by_source.assert_not_called()
            MockRegistry.return_value.decay_model_by_name.assert_not_called()

        skip_msgs = [
            m for m in info_logs
            if "skip decay: outcome not attributable to an oracle model" in m
        ]
        # Logged at INFO once per event type, not repeated on the second event.
        assert len(skip_msgs) == 1


# ── Defect 4: Targeted oracle-model decay vs non-oracle options outcomes ───


class TestDefect4ProducerModuleMatching:
    def _sqlite_oracle_engine(self, extra_models: list[tuple[str, list[str]]] | None = None):
        eng = create_engine("sqlite://")

        @event.listens_for(eng, "connect")
        def _register_funcs(dbapi_conn, _rec):
            dbapi_conn.create_function("NOW", 0, lambda: "2026-04-25 00:00:00")
            dbapi_conn.create_function("GREATEST", 2, lambda a, b: max(a, b))

        with eng.begin() as conn:
            conn.execute(text("""
                CREATE TABLE oracle_models (
                    name TEXT PRIMARY KEY,
                    signal_families TEXT,
                    weight DOUBLE PRECISION DEFAULT 1.0,
                    last_updated TEXT
                )
            """))
            for m in DEFAULT_MODELS:
                conn.execute(
                    text("INSERT INTO oracle_models (name, signal_families, weight) VALUES (:n, :f, 1.0)"),
                    {"n": m.name, "f": json.dumps(m.signal_families)},
                )
            for name, families in extra_models or []:
                conn.execute(
                    text("INSERT INTO oracle_models (name, signal_families, weight) VALUES (:n, :f, 1.0)"),
                    {"n": name, "f": json.dumps(families)},
                )
        return eng

    def test_options_loss_decays_nothing(self, monkeypatch):
        """An options-trade LOSS derived from options_recommendations/scanner
        must emit producer_module='trading.options_recommender' and leave ALL
        oracle model weights untouched (even models carrying vol/sentiment)."""
        captured = _capture_emits(monkeypatch)
        info_logs: list[str] = []
        monkeypatch.setattr(oracle_weights, "_SKIP_DECAY_LOGGED_EVENT_TYPES", set())
        monkeypatch.setattr(
            oracle_weights.log,
            "info",
            lambda msg, **kw: info_logs.append(msg.format(**kw)),
        )

        pm = PostMortem(
            trade_id=202,
            ticker="TSLA",
            direction="CALL",
            outcome="LOSS",
            actual_return=-0.8,
            data_at_decision={
                "decision_signals": [{"name": "iv_skew", "score": 4.0, "direction": "bullish"}],
            },
            thesis_at_decision="skew play",
            sanity_results_at_decision={},
            what_actually_happened="dropped",
            price_path=[],
            failure_category="wrong_signal",
            root_cause="skew failed",
            which_signals_were_wrong=["iv_skew_flat"],
            which_signals_were_right=[],
            what_we_missed="",
            recommended_fix="",
            confidence_in_analysis=0.6,
            generated_at=datetime(2026, 4, 25, tzinfo=timezone.utc).isoformat(),
        )
        _emit_trade_postmortem(pm=pm, trade_id=202)
        assert len(captured) == 1
        evt = captured[0]
        assert evt.verdict == "MISS"
        assert evt.producer_module == "trading.options_recommender"

        eng = self._sqlite_oracle_engine(
            extra_models=[
                ("options_flow_mut_abc123", ["vol", "sentiment"]),
                ("trace_remove_s_xyz789", ["vol", "sentiment", "equity"]),
            ]
        )
        oracle_weights.on_postmortem_completed(evt, engine=eng)

        with eng.connect() as conn:
            weights = dict(conn.execute(text("SELECT name, weight FROM oracle_models")).fetchall())
        assert all(w == pytest.approx(1.0) for w in weights.values()), (
            f"Options LOSS must decay zero oracle models, got {weights}"
        )
        assert any(
            "skip decay: outcome not attributable to an oracle model" in m
            for m in info_logs
        )

    def test_oracle_prediction_loss_with_model_name_decays_only_that_model(self, monkeypatch):
        """An oracle-prediction LOSS carrying a model_name must decay ONLY that
        exact model (bound WHERE name = :name), leaving every other model
        (including models sharing vol/sentiment signal_families) untouched."""
        captured = _capture_emits(monkeypatch)
        created_ts = datetime(2026, 4, 10, 14, 0, tzinfo=timezone.utc)
        scored_ts = datetime(2026, 4, 17, 21, 0, tzinfo=timezone.utc)

        class _FakeConn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, stmt, params=None):
                sql = " ".join(str(stmt).split())
                res = MagicMock()
                if "FROM oracle_predictions" in sql:
                    res.fetchone.return_value = (
                        "pred-42", "NVDA", "CALL", 140.0, 125.0, date(2026, 4, 17),
                        0.72, 12.0, "options_flow_mut_abc123",
                        json.dumps([{"name": "iv_skew", "direction": "bullish"}]),
                        "[]", "{}", "miss", 118.0, -5.6, -0.056, scored_ts, "missed",
                        created_ts,
                    )
                else:
                    res.fetchone.return_value = None
                    res.fetchall.return_value = []
                return res

        class _FakePredEngine:
            def connect(self):
                return _FakeConn()

            def begin(self):
                return _FakeConn()

        monkeypatch.setattr(pm_mod, "_ensure_tables", lambda _e: None)
        monkeypatch.setattr(pm_mod, "_store_postmortem", lambda *a, **k: None)
        monkeypatch.setattr(
            pm_mod,
            "_get_llm_postmortem",
            lambda **k: {"recommended_fix": k["root_cause"], "what_we_missed": "", "confidence": 0.6},
        )

        pm = generate_prediction_postmortem(_FakePredEngine(), prediction_id="pred-42")
        assert pm is not None
        assert len(captured) == 1
        evt = captured[0]
        assert evt.verdict == "MISS"
        assert evt.producer_module == "oracle.model:options_flow_mut_abc123"

        eng = self._sqlite_oracle_engine(
            extra_models=[
                ("options_flow_mut_abc123", ["vol", "sentiment"]),
                ("trace_remove_s_xyz789", ["vol", "sentiment"]),
            ]
        )
        executed_sql: list[str] = []

        @event.listens_for(eng, "before_cursor_execute")
        def _record_sql(_conn, _cursor, statement, _parameters, _context, _executemany):
            executed_sql.append(" ".join(statement.split()))

        oracle_weights.on_postmortem_completed(evt, engine=eng)

        with eng.connect() as conn:
            weights = dict(conn.execute(text("SELECT name, weight FROM oracle_models")).fetchall())

        assert weights["options_flow_mut_abc123"] == pytest.approx(0.9)
        for name, weight in weights.items():
            if name != "options_flow_mut_abc123":
                assert weight == pytest.approx(1.0), (
                    f"Sibling model {name!r} was unexpectedly decayed to {weight}"
                )

        update_sqls = [s for s in executed_sql if s.upper().startswith("UPDATE ORACLE_MODELS")]
        assert len(update_sqls) == 1
        assert "LIKE" not in update_sqls[0].upper()
        assert "signal_families" not in update_sqls[0].lower()
        assert "WHERE name = ?" in update_sqls[0]

    def test_oracle_prediction_loss_without_model_name_skips_decay(self, monkeypatch):
        """An oracle-prediction LOSS with no model_name must NOT decay anything."""
        captured = _capture_emits(monkeypatch)
        info_logs: list[str] = []
        monkeypatch.setattr(oracle_weights, "_SKIP_DECAY_LOGGED_EVENT_TYPES", set())
        monkeypatch.setattr(
            oracle_weights.log,
            "info",
            lambda msg, **kw: info_logs.append(msg.format(**kw)),
        )
        created_ts = datetime(2026, 4, 10, 14, 0, tzinfo=timezone.utc)
        scored_ts = datetime(2026, 4, 17, 21, 0, tzinfo=timezone.utc)

        class _FakeConn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, stmt, params=None):
                sql = " ".join(str(stmt).split())
                res = MagicMock()
                if "FROM oracle_predictions" in sql:
                    res.fetchone.return_value = (
                        "pred-99", "NVDA", "CALL", 140.0, 125.0, date(2026, 4, 17),
                        0.72, 12.0, None, "[]", "[]", "{}", "miss", 118.0, -5.6,
                        -0.056, scored_ts, "missed", created_ts,
                    )
                else:
                    res.fetchone.return_value = None
                    res.fetchall.return_value = []
                return res

        class _FakePredEngine:
            def connect(self):
                return _FakeConn()

            def begin(self):
                return _FakeConn()

        monkeypatch.setattr(pm_mod, "_ensure_tables", lambda _e: None)
        monkeypatch.setattr(pm_mod, "_store_postmortem", lambda *a, **k: None)
        monkeypatch.setattr(
            pm_mod,
            "_get_llm_postmortem",
            lambda **k: {"recommended_fix": k["root_cause"], "what_we_missed": "", "confidence": 0.6},
        )

        pm = generate_prediction_postmortem(_FakePredEngine(), prediction_id="pred-99")
        assert pm is not None
        assert len(captured) == 1
        evt = captured[0]
        assert evt.producer_module == "unknown"

        eng = self._sqlite_oracle_engine()
        oracle_weights.on_postmortem_completed(evt, engine=eng)
        with eng.connect() as conn:
            weights = dict(conn.execute(text("SELECT name, weight FROM oracle_models")).fetchall())
        assert all(w == pytest.approx(1.0) for w in weights.values())
        assert any(
            "skip decay: outcome not attributable to an oracle model" in m
            for m in info_logs
        )

    def test_unmapped_producer_returns_none_and_is_skipped(self, monkeypatch):
        """_normalize_producer_key must return None for unmapped values instead
        of passing them through, and oracle_weights must skip decay cleanly."""
        for unmapped in (
            "some_unmapped_producer",
            "vol",
            "sentiment",
            "supply",
            "iv_skew",
            "intelligence.postmortem",
            "unknown",
            "oracle.engine",
            "oracle.model:",
            "oracle.model:   ",
            "oracle.model:unknown",
            "",
            None,
        ):
            assert _normalize_producer_key(unmapped) is None, (
                f"Expected None for unmapped key {unmapped!r}"
            )

        assert _normalize_producer_key("trading.options_recommender") == "trading.options_recommender"
        assert _normalize_producer_key("options_scanner") == "trading.options_recommender"
        assert _normalize_producer_key("oracle.model:flow_momentum") == "oracle.model:flow_momentum"

        captured = _capture_emits(monkeypatch)
        warnings: list[str] = []
        info_logs: list[str] = []
        monkeypatch.setattr(pm_mod.log, "warning", lambda msg, **kw: warnings.append(msg.format(**kw)))
        monkeypatch.setattr(oracle_weights, "_SKIP_DECAY_LOGGED_EVENT_TYPES", set())
        monkeypatch.setattr(
            oracle_weights.log,
            "info",
            lambda msg, **kw: info_logs.append(msg.format(**kw)),
        )

        pm = PostMortem(
            trade_id=203,
            ticker="TSLA",
            direction="CALL",
            outcome="LOSS",
            actual_return=-0.8,
            data_at_decision={},
            thesis_at_decision="",
            sanity_results_at_decision={},
            what_actually_happened="",
            price_path=[],
            failure_category="wrong_signal",
            root_cause="unknown",
            which_signals_were_wrong=[],
            which_signals_were_right=[],
            what_we_missed="",
            recommended_fix="",
            confidence_in_analysis=0.5,
            generated_at=datetime(2026, 4, 25, tzinfo=timezone.utc).isoformat(),
        )
        _emit_trade_postmortem(pm=pm, trade_id=203, producer_module="some_unmapped_producer")
        assert len(captured) == 1
        evt = captured[0]
        assert evt.producer_module == "unknown"
        assert warnings, "Expected a warning log when producer_module is unmapped"

        eng = self._sqlite_oracle_engine()
        with patch("oracle.engine.ModelRegistry") as MockRegistry:
            oracle_weights.on_postmortem_completed(evt, engine=eng)
            # Also verify a raw event carrying a family or arbitrary unmapped string is skipped
            raw_family_evt = evt.model_copy(update={"producer_module": "vol"})
            raw_unmapped_evt = evt.model_copy(update={"producer_module": "custom.unmapped"})
            oracle_weights.on_postmortem_completed(raw_family_evt, engine=eng)
            oracle_weights.on_postmortem_completed(raw_unmapped_evt, engine=eng)
            MockRegistry.assert_not_called()

        skip_msgs = [
            m for m in info_logs
            if "skip decay: outcome not attributable to an oracle model" in m
        ]
        assert len(skip_msgs) == 1

    def test_decay_model_by_name_is_exact_and_bounded(self):
        """ModelRegistry.decay_model_by_name decays only the named model and respects floor."""
        eng = self._sqlite_oracle_engine(
            extra_models=[("options_flow_v2", ["vol", "sentiment"])]
        )
        reg = ModelRegistry(eng)
        assert reg.decay_model_by_name("options_flow", 0.9) == 1
        assert reg.decay_model_by_name("", 0.9) == 0
        assert reg.decay_model_by_name("options_flow", 0.0) == 0
        assert reg.decay_model_by_name("nonexistent_model", 0.9) == 0

        with eng.connect() as conn:
            w_exact = conn.execute(
                text("SELECT weight FROM oracle_models WHERE name = 'options_flow'")
            ).scalar()
            w_prefix = conn.execute(
                text("SELECT weight FROM oracle_models WHERE name = 'options_flow_v2'")
            ).scalar()
        assert w_exact == pytest.approx(0.9)
        assert w_prefix == pytest.approx(1.0)


# ── Follow-up: TraceEvolver insufficient_data + POSTMORTEM_WRITES_LESSONS ─


class TestTraceEvolverInsufficientDataAndLessonGuard:
    def test_trace_evolver_treats_insufficient_data_as_noop(self):
        """insufficient_data must map to a no-op remedy (not 'Investigate ...')
        and produce zero mutation proposals even if what_we_missed mentions words
        matching signal names like 'time'."""
        eng = MagicMock()
        analyzer = TraceAnalyzer(eng)
        action = analyzer._recommend_action(
            "insufficient_data", [], [], ["time", "spot", "price"]
        )
        assert "no-op" in action.lower() or "ignore" in action.lower()
        assert "consider adding missed signals" not in action.lower()

        mutator = TargetedMutator(eng)
        pattern = FailurePattern(
            category="insufficient_data",
            frequency=5,
            fraction=0.5,
            signals_commonly_wrong=["feature:sentiment"],
            signals_commonly_right=[],
            signals_commonly_missed=["time", "spot"],
            affected_models=["flow_momentum"],
            regime_context="NEUTRAL",
            recommended_action=action,
        )
        assert mutator.propose([pattern]) == []

    def test_postmortem_writes_lessons_remains_false(self):
        assert pm_mod.POSTMORTEM_WRITES_LESSONS is False


# ── Defect 5: Contagion feedback sign on wrong-direction moves ─────────────


class TestDefect5ContagionFeedbackSign:
    def test_compute_implied_pct_wrong_direction_does_not_reinforce(self):
        """Positive cost shock (+30%) expects negative downstream move; an actual
        move of +10% (wrong direction) must yield implied=0.0, not +0.476."""
        implied = _compute_implied_pct(
            actual_price_move_pct=+0.10,
            shock_magnitude=0.30,
            pass_through=0.70,
        )
        assert implied == pytest.approx(0.0)

    def test_compute_implied_pct_positive_prediction_respects_sign(self):
        # Right direction (+move for +prediction)
        assert _compute_implied_pct(
            actual_price_move_pct=+0.021,
            shock_magnitude=0.30,
            pass_through=0.70,
            predicted_margin_impact_pct=+0.05,
        ) == pytest.approx(0.10, rel=1e-3)
        # Wrong direction (-move for +prediction)
        assert _compute_implied_pct(
            actual_price_move_pct=-0.10,
            shock_magnitude=0.30,
            pass_through=0.70,
            predicted_margin_impact_pct=+0.05,
        ) == pytest.approx(0.0)

    def test_apply_contagion_feedback_decays_edge_on_wrong_direction_move(self):
        """Edge at 0.02 with predicted -5% and actual +10% (wrong direction,
        accuracy=0.0) must DECAY toward 0 (0.02 -> 0.014), NEVER reinforce to 0.04."""
        edge_updates: list[dict] = []

        class _Row:
            def __init__(self, vals):
                self._v = vals

            def __getitem__(self, i):
                return self._v[i]

        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, stmt, params=None):
                sql = " ".join(str(stmt).lower().split())
                p = dict(params or {})
                res = MagicMock()
                if "from contagion_backtest_results" in sql:
                    res.fetchall.return_value = [
                        _Row((1, 42, "HSY", -0.05, +0.10, 0.0, "cocoa_beans", 0.30))
                    ]
                elif "from supply_chain_edges" in sql:
                    res.fetchall.return_value = [
                        _Row((101, "cocoa_beans", "HSY", 0.02, "estimated"))
                    ]
                elif "update supply_chain_edges" in sql and "pct_downstream_cogs" in sql:
                    edge_updates.append(p)
                return res

        class _Eng:
            def connect(self):
                return _Conn()

            def begin(self):
                return _Conn()

        summary = apply_contagion_feedback(_Eng(), since_hours=24)
        assert summary["decayed"] == 1
        assert len(edge_updates) == 1
        # 0.7 * 0.02 + 0.3 * 0.0 = 0.014 (strictly decayed, NOT reinforced to 0.04)
        assert edge_updates[0]["v"] == pytest.approx(0.014, abs=1e-6)
        assert edge_updates[0]["v"] < 0.02


# ── Defect 6: thesis_invalidation_monitor schema + error visibility ────────


class TestDefect6ThesisInvalidationMonitor:
    def test_queries_use_real_schema_columns_only(self):
        """decision_journal query must not reference non-existent columns
        (metadata, outcome, logged_at, or decision_journal.ticker), and
        ticker_metrics_daily must order by obs_date (not non-existent as_of_date)."""
        captured_sql: list[tuple[str, dict]] = []
        now = datetime(2026, 4, 13, 16, 0, tzinfo=timezone.utc)

        eng = MagicMock()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False

        def _execute(stmt, params=None):
            sql = " ".join(str(stmt).split())
            captured_sql.append((sql, dict(params or {})))
            res = MagicMock()
            if "FROM decision_journal" in sql:
                res.fetchall.return_value = [
                    (
                        42,
                        "AAPL",
                        {"invalidation": {"type": "price_level", "operator": "below", "threshold": 180.0}},
                        now,
                    )
                ]
            elif "FROM ticker_metrics_daily" in sql:
                res.fetchone.return_value = (175.0,)
            else:
                res.fetchall.return_value = []
                res.fetchone.return_value = None
            return res

        conn.execute = _execute
        eng.connect.return_value = conn

        run = tim.run_monitor(eng, as_of=now)
        assert run.predictions_scanned == 1
        assert run.triggered_count == 1

        dj_sql = next(sql for sql, _ in captured_sql if "FROM decision_journal" in sql)
        # Forbidden non-existent columns on decision_journal
        assert not re.search(r"\bmetadata\b", dj_sql), f"Non-existent column 'metadata' in: {dj_sql}"
        assert not re.search(r"\blogged_at\b", dj_sql), f"Non-existent column 'logged_at' in: {dj_sql}"
        assert not re.search(r"\boutcome\s+IS\s+NULL\b", dj_sql, re.IGNORECASE), (
            f"Non-existent column 'outcome' in: {dj_sql}"
        )
        # Real columns present
        assert "contradiction_flags" in dj_sql
        assert "decision_timestamp" in dj_sql
        assert "verdict IS NULL" in dj_sql
        assert "outcome_recorded_at IS NULL" in dj_sql
        assert "agent_runs" in dj_sql

        tmd_sql, tmd_params = next(
            (sql, p) for sql, p in captured_sql if "FROM ticker_metrics_daily" in sql
        )
        assert not re.search(r"(?<!:)\bas_of_date\b", tmd_sql), (
            f"Non-existent column 'as_of_date' in: {tmd_sql}"
        )
        assert "obs_date" in tmd_sql
        assert tmd_params.get("as_of_date") == date(2026, 4, 13)

        assert run.query_failures == 0
        assert run.had_query_error is False

    def test_generate_postmortem_end_to_end_expired_put_fresh_signal(self, monkeypatch):
        """End-to-end trade postmortem on an EXPIRED PUT with 35 DTE, fresh signal,
        option premium $2.50, and underlying dipping from $180 to $176 before
        rallying to $185 -> right_signal_wrong_timing, verdict=MISS,
        producer='trading.options_recommender'."""
        gen_ts = datetime(2026, 4, 10, 14, 30, tzinfo=timezone.utc)
        closed_ts = datetime(2026, 5, 15, 21, 0, tzinfo=timezone.utc)
        captured = _capture_emits(monkeypatch)

        class _FakeConn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, stmt, params=None):
                sql = " ".join(str(stmt).split())
                res = MagicMock()
                if "FROM options_recommendations" in sql:
                    res.fetchone.return_value = (
                        88, "AAPL", "PUT", 175.0, date(2026, 5, 15), 2.50,
                        5.00, 1.25, 0.65, "bearish put", "{}", gen_ts,
                        "EXPIRED", -1.0, closed_ts, "",
                        json.dumps([{"name": "iv_skew", "score": 3.5, "direction": "bearish"}]),
                        "[]",
                    )
                elif "put_call_ratio" in sql:
                    # Written at 09:00 ET, before the 10:30 ET decision.
                    res.fetchone.return_value = (
                        1.4, 0.32, 0.25, 172.0, 180.0, 50000, -0.04, 0.12, date(2026, 4, 10),
                        datetime(2026, 4, 10, 13, 0, tzinfo=timezone.utc),
                    )
                elif "signal_date >= :s" in sql:
                    res.fetchall.return_value = [
                        (date(2026, 4, 10), 180.0),
                        (date(2026, 4, 20), 176.0),  # -2.22% favorable for PUT
                        (date(2026, 5, 15), 185.0),
                    ]
                else:
                    res.fetchone.return_value = None
                    res.fetchall.return_value = []
                return res

        class _FakeEngine:
            def connect(self):
                return _FakeConn()

            def begin(self):
                return _FakeConn()

        monkeypatch.setattr(pm_mod, "_ensure_tables", lambda _e: None)
        monkeypatch.setattr(pm_mod, "_store_postmortem", lambda *a, **k: None)
        monkeypatch.setattr(
            pm_mod,
            "_get_llm_postmortem",
            lambda **k: {"recommended_fix": k["root_cause"], "what_we_missed": "", "confidence": 0.6},
        )

        pm = generate_postmortem(_FakeEngine(), trade_id=88)
        assert pm is not None
        assert pm.failure_category == "right_signal_wrong_timing"
        assert pm.actual_return == pytest.approx(-1.0)
        assert len(captured) == 1
        assert captured[0].verdict == "MISS"
        assert captured[0].producer_module == "trading.options_recommender"

    def test_broken_query_logs_warning_and_sets_failure_flag(self, monkeypatch):
        """A failing SQL query must log at WARNING (not debug) and surface via
        query_failures / had_query_error / errors in MonitorRun."""
        warnings: list[str] = []
        monkeypatch.setattr(tim.log, "warning", lambda msg, **kw: warnings.append(msg.format(**kw)))

        eng = MagicMock()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        conn.execute.side_effect = RuntimeError('column "metadata" does not exist')
        eng.connect.return_value = conn

        run = tim.run_monitor(eng, as_of=datetime(2026, 4, 13, tzinfo=timezone.utc))
        assert run.predictions_scanned == 0
        assert warnings, "Expected log.warning on failed DB query"
        assert len(run.errors) == 1
        assert "metadata" in run.errors[0]
        assert run.had_query_error is True
        assert run.query_failures == 1
        d = run.to_dict()
        assert d["had_query_error"] is True
        assert d["query_failures"] == 1
