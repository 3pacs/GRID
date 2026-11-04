"""EarningsCalendarPuller raw_series vintage dedupe, same-day collapse, budget.

Regression for the 2026-11-04 Hermes log audit: every pull re-inserted every
yfinance ``earnings_dates`` row into raw_series (9,915 ``>5 years old``
SANITY warnings/day; EARNINGS:AMD:actual held 10,349 rows for 27 dates),
two same-day rows aborted IVA with UniqueViolation on uq_raw_series_composite,
and a 174-ticker pull overran SmartScheduler's 120 s timeout.

All network (yfinance) and DB access is faked.
"""

from __future__ import annotations

import types
from contextlib import contextmanager
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from ingestion.altdata import earnings_calendar as ec
from ingestion.smart_scheduler import _classify_outcome


# ── Fakes ────────────────────────────────────────────────────────────────


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class FakeEngine:
    """Records statements; keeps committed raw_series vintages in memory."""

    def __init__(self, raw=None, last_pulls=None, fail_raw_insert=False):
        # (series_id, obs_date) -> list of values in insertion order
        self.raw: dict[tuple[str, date], list[float]] = {
            k: list(v) for k, v in (raw or {}).items()
        }
        self.last_pulls = last_pulls or {}
        self.fail_raw_insert = fail_raw_insert
        self.statements: list[tuple[str, dict]] = []
        self.calendar_upserts: list[dict] = []
        self.txn_raw_keys: list[list[tuple[str, date]]] = []
        self._pending: list[tuple[str, date, float]] | None = None

    @contextmanager
    def begin(self):
        self._pending = []
        self.txn_raw_keys.append([])
        try:
            yield self
        except Exception:
            self._pending = None
            raise
        for sid, od, val in self._pending:
            self.raw.setdefault((sid, od), []).append(val)
        self._pending = None

    @contextmanager
    def connect(self):
        yield self

    def execute(self, sql, params=None):
        sql = str(sql)
        params = dict(params or {})
        self.statements.append((sql, params))
        if "pg_advisory_xact_lock" in sql:
            return _Rows([])
        if "CROSS JOIN LATERAL" in sql:
            out = []
            for sid, od in zip(params["sids"], params["ods"]):
                vals = self.raw.get((sid, od))
                if vals:
                    out.append((sid, od, vals[-1]))
            return _Rows(out)
        if "INSERT INTO raw_series" in sql:
            if self.fail_raw_insert:
                raise RuntimeError("simulated raw_series failure")
            key = (params["sid"], params["od"])
            # Mirror uq_raw_series_composite: same key twice in one txn
            # (same pull_timestamp) is a UniqueViolation.
            if key in self.txn_raw_keys[-1]:
                raise RuntimeError("duplicate key value violates unique constraint uq_raw_series_composite")
            self.txn_raw_keys[-1].append(key)
            self._pending.append((params["sid"], params["od"], params["val"]))
            return _Rows([])
        if "INSERT INTO earnings_calendar" in sql:
            self.calendar_upserts.append(params)
            return _Rows([])
        if "FROM earnings_calendar" in sql and "GROUP BY ticker" in sql:
            return _Rows([(t, ts) for t, ts in self.last_pulls.items() if t in params["tickers"]])
        raise AssertionError(f"unexpected SQL: {sql[:80]}")

    def lookups(self) -> int:
        return sum("CROSS JOIN LATERAL" in s for s, _ in self.statements)


def _frame(rows):
    """rows: list of (iso timestamp, estimate, reported, surprise)."""
    # yfinance indexes earnings_dates in America/New_York (mixed EST/EDT offsets).
    idx = pd.DatetimeIndex(
        pd.to_datetime([r[0] for r in rows], utc=True).tz_convert("America/New_York"),
        name="Earnings Date",
    )
    return pd.DataFrame(
        {
            "EPS Estimate": [r[1] for r in rows],
            "Reported EPS": [r[2] for r in rows],
            "Surprise(%)": [r[3] for r in rows],
        },
        index=idx,
    )


class FakeYF:
    def __init__(self, frames):
        self.frames = frames
        self.calls: list[str] = []

    def Ticker(self, ticker):
        self.calls.append(ticker)
        return types.SimpleNamespace(earnings_dates=self.frames.get(ticker), calendar=None)


def _puller(engine):
    p = ec.EarningsCalendarPuller.__new__(ec.EarningsCalendarPuller)
    p.engine = engine
    p.source_id = 351
    return p


NAN = float("nan")

# IVA as yfinance returned it on 2026-11-04: two rows at the SAME timestamp.
IVA_ROWS = [
    ("2027-04-01 17:00:00-04:00", NAN, NAN, NAN),
    ("2022-03-10 19:00:00-05:00", -0.39, -0.16, 58.21),
    ("2022-03-10 19:00:00-05:00", -0.48, -0.16, 65.72),
    ("2020-09-16 16:00:00-04:00", -0.45, -0.21, 52.31),
]


# ── _dedupe_earnings_rows ────────────────────────────────────────────────


def test_same_day_rows_collapse_to_one_record_per_date():
    records = ec._dedupe_earnings_rows(_frame(IVA_ROWS))
    dates = [r["earn_date"] for r in records]
    assert dates == [date(2020, 9, 16), date(2022, 3, 10), date(2027, 4, 1)]
    picked = records[1]
    # Same timestamp, both reported: the lower estimate wins, deterministically.
    assert (picked["eps_est"], picked["eps_act"], picked["surprise_pct"]) == (-0.48, -0.16, 65.72)


def test_dedupe_choice_does_not_depend_on_yfinance_row_order():
    forward = ec._dedupe_earnings_rows(_frame(IVA_ROWS))
    backward = ec._dedupe_earnings_rows(_frame(list(reversed(IVA_ROWS))))
    assert forward == backward


def test_reported_row_beats_later_unreported_row_then_latest_timestamp():
    rows = [
        ("2026-05-01 08:00:00-04:00", 1.00, 1.10, NAN),
        ("2026-05-01 16:30:00-04:00", 1.05, NAN, NAN),  # later, but no actual
        ("2026-08-01 08:00:00-04:00", 2.00, NAN, NAN),
        ("2026-08-01 16:30:00-04:00", 2.10, NAN, NAN),  # later timestamp wins
    ]
    records = ec._dedupe_earnings_rows(_frame(rows))
    assert [(r["eps_est"], r["eps_act"]) for r in records] == [(1.00, 1.10), (2.10, None)]


# ── raw_series vintages ──────────────────────────────────────────────────


def _amd_frame(actual_2026=0.92):
    return _frame([
        ("2027-02-03 16:00:00-05:00", 1.05, NAN, NAN),
        ("2026-11-03 16:00:00-05:00", 0.90, actual_2026, 2.2),
        ("2021-10-26 16:00:00-04:00", 0.67, 0.73, 9.0),
    ])


def test_repull_of_unchanged_history_writes_no_raw_rows(monkeypatch):
    engine = FakeEngine()
    monkeypatch.setattr(ec, "yf", FakeYF({"AMD": _amd_frame()}))
    p = _puller(engine)

    first = p.pull_ticker_earnings("AMD")
    assert first["status"] == "SUCCESS"
    assert first["raw_series_inserted"] == 5  # 3 estimates + 2 actuals
    assert first["calendar_rows_upserted"] == 3
    assert first["rows_inserted"] == 8

    second = p.pull_ticker_earnings("AMD")
    assert second["status"] == "SUCCESS"
    assert second["raw_series_inserted"] == 0
    assert second["raw_series_unchanged"] == 5
    assert all(len(v) == 1 for v in engine.raw.values())
    # Exactly one batched lookup per ticker pull, never one per row.
    assert engine.lookups() == 2


def test_revised_actual_is_recorded_as_a_new_vintage(monkeypatch):
    engine = FakeEngine(raw={
        ("EARNINGS:AMD:estimate", date(2027, 2, 3)): [1.05],
        ("EARNINGS:AMD:estimate", date(2026, 11, 3)): [0.90],
        ("EARNINGS:AMD:actual", date(2026, 11, 3)): [0.92],
        ("EARNINGS:AMD:estimate", date(2021, 10, 26)): [0.67],
        ("EARNINGS:AMD:actual", date(2021, 10, 26)): [0.73],
    })
    monkeypatch.setattr(ec, "yf", FakeYF({"AMD": _amd_frame(actual_2026=0.95)}))
    res = _puller(engine).pull_ticker_earnings("AMD")
    assert res["raw_series_inserted"] == 1
    assert res["raw_series_unchanged"] == 4
    assert engine.raw[("EARNINGS:AMD:actual", date(2026, 11, 3))] == [0.92, 0.95]


def test_same_day_duplicates_no_longer_abort_the_ticker(monkeypatch):
    engine = FakeEngine()
    monkeypatch.setattr(ec, "yf", FakeYF({"IVA": _frame(IVA_ROWS)}))
    res = _puller(engine).pull_ticker_earnings("IVA")
    assert res["status"] == "SUCCESS", res["errors"]
    assert res["errors"] == []
    # 2 dated estimates + 2 actuals; the NaN-only upcoming row has no values.
    assert res["raw_series_inserted"] == 4
    keys = [k for txn in engine.txn_raw_keys for k in txn]
    assert len(keys) == len(set(keys))
    assert len(engine.calendar_upserts) == 3


def test_raw_write_takes_ticker_advisory_lock_before_lookup(monkeypatch):
    engine = FakeEngine()
    monkeypatch.setattr(ec, "yf", FakeYF({"AMD": _amd_frame()}))
    _puller(engine).pull_ticker_earnings("AMD")
    kinds = [
        "lock" if "pg_advisory_xact_lock" in s else "lookup" if "CROSS JOIN LATERAL" in s else None
        for s, _ in engine.statements
    ]
    kinds = [k for k in kinds if k]
    assert kinds == ["lock", "lookup"]


def test_lookup_sql_is_parameterized_and_success_only(monkeypatch):
    engine = FakeEngine()
    monkeypatch.setattr(ec, "yf", FakeYF({"AMD": _amd_frame()}))
    _puller(engine).pull_ticker_earnings("AMD")
    sql, params = next((s, p) for s, p in engine.statements if "CROSS JOIN LATERAL" in s)
    assert "pull_status = 'SUCCESS'" in sql
    assert "AMD" not in sql
    assert params["src"] == 351
    assert len(params["sids"]) == len(params["ods"]) == 5


def test_raw_failure_reports_only_committed_calendar_rows(monkeypatch):
    engine = FakeEngine(fail_raw_insert=True)
    monkeypatch.setattr(ec, "yf", FakeYF({"AMD": _amd_frame()}))
    res = _puller(engine).pull_ticker_earnings("AMD")
    assert res["status"] == "FAILED"
    assert res["raw_series_inserted"] == 0
    assert res["rows_inserted"] == res["calendar_rows_upserted"] == 3
    assert engine.raw == {}
    assert _classify_outcome(res)[0] == "FAILED"
    # The committed calendar rows keep a one-ticker run PARTIAL, never SUCCESS.
    assert _classify_outcome([res])[0] == "PARTIAL"


def test_ticker_without_earnings_reports_zero_writes(monkeypatch):
    engine = FakeEngine()
    monkeypatch.setattr(ec, "yf", FakeYF({}))
    res = _puller(engine).pull_ticker_earnings("SPY")
    assert (res["status"], res["rows_inserted"]) == ("SUCCESS", 0)
    assert engine.statements == []


# ── pull_all: budget, contract, rotation ────────────────────────────────


def _budget(n_true):
    calls = {"n": 0}

    def should_continue():
        calls["n"] += 1
        return calls["n"] <= n_true

    return should_continue


def test_budget_stop_returns_partial_with_honest_counts(monkeypatch):
    frames = {t: _amd_frame() for t in ("AAA", "BBB", "CCC", "DDD")}
    monkeypatch.setattr(ec, "yf", FakeYF(frames))
    monkeypatch.setattr(ec, "_BUDGET_STOP_WARNED", False)
    out = _puller(FakeEngine()).pull_all(["AAA", "BBB", "CCC", "DDD"], should_continue=_budget(2))
    assert out["status"] == "PARTIAL"
    assert out["stopped_by_budget"] is True
    assert [r["ticker"] for r in out["results"]] == ["AAA", "BBB"]
    assert out["tickers_not_attempted"] == ["CCC", "DDD"]
    assert out["rows_inserted"] == 16
    assert out["raw_series_inserted"] == 10
    outcome, rows, _ = _classify_outcome(out)
    assert (outcome, rows) == ("PARTIAL", 16)


def test_full_run_with_budget_is_success_and_unchanged_rerun_counts_calendar(monkeypatch):
    monkeypatch.setattr(ec, "yf", FakeYF({"AAA": _amd_frame(), "BBB": _amd_frame()}))
    p = _puller(FakeEngine())
    out = p.pull_all(["AAA", "BBB"], should_continue=lambda: True)
    assert out["status"] == "SUCCESS" and out["tickers_not_attempted"] == []
    assert _classify_outcome(out) == ("SUCCESS", 16, None)
    again = p.pull_all(["AAA", "BBB"], should_continue=lambda: True)
    assert again["raw_series_inserted"] == 0
    assert again["raw_series_unchanged"] == 10
    # earnings_calendar upserts are real committed writes (pull_timestamp=NOW()).
    assert _classify_outcome(again) == ("SUCCESS", 6, None)


def test_one_failed_ticker_makes_the_run_partial(monkeypatch):
    class Boom(FakeYF):
        def Ticker(self, ticker):
            if ticker == "BAD":
                raise RuntimeError("yfinance unavailable")
            return super().Ticker(ticker)

    monkeypatch.setattr(ec, "yf", Boom({"AAA": _amd_frame()}))
    out = _puller(FakeEngine()).pull_all(["AAA", "BAD"], should_continue=lambda: True)
    assert _classify_outcome(out)[0] == "PARTIAL"


def test_legacy_list_shape_without_budget(monkeypatch):
    monkeypatch.setattr(ec, "yf", FakeYF({"AAA": _amd_frame()}))
    out = _puller(FakeEngine()).pull_all(["AAA"])
    assert isinstance(out, list) and out[0]["ticker"] == "AAA"
    assert _classify_outcome(out)[0] == "SUCCESS"


def test_empty_universe_with_budget_is_skipped(monkeypatch):
    p = _puller(FakeEngine())
    monkeypatch.setattr(p, "_get_watchlist_tickers", lambda: [])
    out = p.pull_all(should_continue=lambda: True)
    assert _classify_outcome(out)[0] == "SKIPPED"
    assert p.pull_all() == []


def test_default_universe_rotates_least_recently_pulled_first(monkeypatch):
    engine = FakeEngine(last_pulls={
        "AAA": datetime(2026, 11, 4, 5, 0, tzinfo=timezone.utc),
        "BBB": datetime(2026, 11, 3, 17, 0, tzinfo=timezone.utc),
    })
    fake = FakeYF({})
    monkeypatch.setattr(ec, "yf", fake)
    p = _puller(engine)
    monkeypatch.setattr(p, "_get_watchlist_tickers", lambda: ["AAA", "BBB", "CCC"])
    out = p.pull_all(should_continue=lambda: True)
    # Never-pulled CCC first, then the oldest refresh.
    assert fake.calls == ["CCC", "BBB", "AAA"]
    assert out["status"] == "SUCCESS"


def test_explicit_ticker_list_keeps_caller_order(monkeypatch):
    engine = FakeEngine(last_pulls={"AAA": datetime(2026, 11, 4, tzinfo=timezone.utc)})
    fake = FakeYF({})
    monkeypatch.setattr(ec, "yf", fake)
    _puller(engine).pull_all(["AAA", "BBB"], should_continue=lambda: True)
    assert fake.calls == ["AAA", "BBB"]


def test_scheduler_wires_should_continue_into_pull_all():
    import inspect

    params = inspect.signature(ec.EarningsCalendarPuller.pull_all).parameters
    assert "should_continue" in params and "ticker_list" in params


@pytest.mark.parametrize("a,b,same", [(0.92, 0.92, True), (0.92, 0.9200000000001, True), (0.92, 0.95, False)])
def test_same_value_tolerance(a, b, same):
    assert ec._same_value(a, b) is same
