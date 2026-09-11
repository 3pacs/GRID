"""``intelligence/trial_outcomes.py`` — recording what readouts actually did.

``trial_signals`` shipped with ``fwd_return_30d`` / ``eval_score`` /
``evaluated_at`` and a schema comment claiming a post-hoc writer filled
them. No writer existed: 135 rows, 0 scored on 2026-09-10. These tests pin
the writer that closes that hole, and in particular that it keeps the
signal-anchored and readout-anchored measurements apart.

No live DB — a ``MagicMock`` engine, the ``tests/test_long_plays.py``
pattern.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from intelligence import trial_outcomes as to

AS_OF = date(2026, 9, 10)


def _series(start: date, days: int, start_price: float, end_price: float) -> list[tuple[date, float]]:
    """A straight line from ``start_price`` to ``end_price`` over ``days``."""
    if days <= 0:
        return [(start, start_price)]
    step = (end_price - start_price) / days
    return [(start + timedelta(days=i), start_price + step * i) for i in range(days + 1)]


# ── realized_return ───────────────────────────────────────────────────────


def test_realized_return_measures_the_window() -> None:
    anchor = date(2026, 6, 1)
    points = _series(anchor - timedelta(days=10), 60, 10.0, 16.0)
    # +0.1/day: anchor at 11.0, +30d at 14.0 -> +27.3 %
    ret = to.realized_return(points, anchor)
    assert ret == pytest.approx((14.0 - 11.0) / 11.0, abs=1e-6)


def test_a_readout_that_halved_scores_negative() -> None:
    anchor = date(2026, 6, 1)
    points = [(anchor, 20.0), (anchor + timedelta(days=30), 10.0)]
    assert to.realized_return(points, anchor) == pytest.approx(-0.5)


def test_a_half_measured_window_is_not_a_measurement() -> None:
    anchor = date(2026, 6, 1)
    # start present, end far outside tolerance
    only_start = [(anchor, 10.0), (anchor + timedelta(days=90), 30.0)]
    assert to.realized_return(only_start, anchor) is None
    # end present, start missing
    only_end = [(anchor - timedelta(days=40), 10.0), (anchor + timedelta(days=30), 30.0)]
    assert to.realized_return(only_end, anchor) is None
    assert to.realized_return([], anchor) is None


def test_tolerance_covers_a_long_weekend_but_not_a_month() -> None:
    anchor = date(2026, 6, 1)
    # both ends 3 days off target — inside the 5-day tolerance
    points = [(anchor + timedelta(days=3), 10.0), (anchor + timedelta(days=33), 20.0)]
    assert to.realized_return(points, anchor) == pytest.approx(1.0)
    # 12 days off — refused rather than silently becoming a 42-day return
    stretched = [(anchor + timedelta(days=12), 10.0), (anchor + timedelta(days=42), 20.0)]
    assert to.realized_return(stretched, anchor) is None


def test_nearest_observation_wins_and_bad_prices_are_skipped() -> None:
    anchor = date(2026, 6, 1)
    points = [
        (anchor - timedelta(days=4), 8.0),
        (anchor, 10.0),                      # exact hit must win
        (anchor + timedelta(days=2), 9.0),
        (anchor + timedelta(days=30), 0.0),  # non-positive price is not a price
        (anchor + timedelta(days=31), 25.0),
    ]
    assert to.realized_return(points, anchor) == pytest.approx(1.5)


# ── _as_date ──────────────────────────────────────────────────────────────


def test_as_date_accepts_the_shapes_psycopg2_returns() -> None:
    assert to._as_date(date(2026, 6, 1)) == date(2026, 6, 1)
    assert to._as_date(datetime(2026, 6, 1, 13, 30, tzinfo=timezone.utc)) == date(2026, 6, 1)
    assert to._as_date("2026-06-01") == date(2026, 6, 1)
    assert to._as_date("2026-06-01T13:30:00+00:00") == date(2026, 6, 1)
    assert to._as_date(None) is None
    assert to._as_date("not a date") is None


# ── score_trial_outcomes ──────────────────────────────────────────────────


def _engine(readouts: list[Any], signals: list[Any]) -> tuple[MagicMock, list[tuple[str, Any]]]:
    """Engine whose reads return the two batches and whose writes are recorded."""
    calls: list[tuple[str, Any]] = []

    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)

    def execute(stmt: Any, params: Any = None) -> MagicMock:
        sql = str(stmt)
        calls.append((sql, params))
        result = MagicMock()
        if "FROM trial_signals" in sql and "readout_scored_at IS NULL" in sql:
            result.fetchall.return_value = readouts
        elif "FROM trial_signals" in sql and "fwd_return_30d IS NULL" in sql:
            result.fetchall.return_value = signals
        else:
            result.fetchall.return_value = []
        return result

    conn.execute.side_effect = execute
    engine = MagicMock()
    engine.connect.return_value = conn
    engine.begin.return_value = conn
    return engine, calls


def _readout_row(id_: int, ticker: str, completion: date) -> tuple:
    return (id_, ticker, "PHASE3", "oncology", "BUY", completion,
            datetime(2026, 1, 5, tzinfo=timezone.utc))


def test_scores_the_readout_and_the_signal_from_their_own_anchors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The two windows are different questions and must not be conflated."""
    completion = date(2026, 6, 1)
    signal_date = datetime(2026, 2, 1, tzinfo=timezone.utc)

    # A stock that drifts down after the signal, then doubles on the readout.
    points = (
        _series(date(2026, 1, 1), 120, 20.0, 10.0)          # signal window: falling
        + [(date(2026, 6, 1), 10.0), (date(2026, 7, 1), 30.0)]   # readout window: +200 %
    )
    engine, calls = _engine(
        readouts=[_readout_row(1, "GEMX", completion)],
        signals=[(1, "GEMX", signal_date)],
    )
    monkeypatch.setattr(
        "intelligence.long_plays._load_adj_close",
        lambda eng, tickers, years, as_of: {"GEMX": points},
    )

    out = to.score_trial_outcomes(engine, as_of=AS_OF)
    assert out["readouts_scored"] == 1 and out["signals_scored"] == 1

    writes = {}
    for sql, params in calls:
        if "SET readout_return_30d" in sql:
            writes["readout"] = params
        elif "SET fwd_return_30d" in sql:
            writes["signal"] = params

    # readout doubled and then some; the signal's own 30 days fell
    assert writes["readout"]["ret"] == pytest.approx(2.0, abs=0.01)
    assert writes["signal"]["ret"] < 0
    # the two measurements genuinely differ — that is the whole point
    assert writes["readout"]["ret"] != writes["signal"]["ret"]


def test_only_fully_elapsed_windows_are_considered() -> None:
    """The scorer must never read a price from beyond the day it scores."""
    engine, calls = _engine(readouts=[], signals=[])
    to.score_trial_outcomes(engine, as_of=AS_OF, window_days=30)
    read_params = [p for sql, p in calls if "readout_scored_at IS NULL" in sql]
    assert read_params, "the readout batch must be queried"
    # latest eligible completion date is as_of minus the full window
    assert read_params[0]["latest"] == AS_OF - timedelta(days=30)
    assert read_params[0]["earliest"] < read_params[0]["latest"]


def test_unpriced_readouts_are_counted_not_invented(monkeypatch: pytest.MonkeyPatch) -> None:
    engine, calls = _engine(
        readouts=[_readout_row(1, "NOPX", date(2026, 6, 1))],
        signals=[],
    )
    monkeypatch.setattr(
        "intelligence.long_plays._load_adj_close",
        lambda eng, tickers, years, as_of: {},   # no history for this name
    )
    out = to.score_trial_outcomes(engine, as_of=AS_OF)
    assert out["readouts_considered"] == 1
    assert out["readouts_scored"] == 0
    assert out["unpriced"] == 1
    assert not any("SET readout_return_30d" in sql for sql, _ in calls)


def test_a_price_outage_degrades_the_batch_without_raising(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*a: Any, **k: Any) -> Any:
        raise RuntimeError("db down")

    engine, calls = _engine(readouts=[_readout_row(1, "GEMX", date(2026, 6, 1))], signals=[])
    monkeypatch.setattr("intelligence.long_plays._load_adj_close", boom)
    out = to.score_trial_outcomes(engine, as_of=AS_OF)
    assert out["readouts_scored"] == 0
    assert any("price history unavailable" in n for n in out["notes"])
    assert not any("SET readout_return_30d" in sql for sql, _ in calls)


def test_a_read_failure_returns_an_empty_summary() -> None:
    broken = MagicMock()
    broken.connect.side_effect = RuntimeError("relation does not exist")
    broken.begin.side_effect = RuntimeError("relation does not exist")
    out = to.score_trial_outcomes(broken, as_of=AS_OF)
    assert out["readouts_scored"] == 0 and out["signals_scored"] == 0
    assert any("read failed" in n for n in out["notes"])


def test_writes_never_overwrite_an_existing_score(monkeypatch: pytest.MonkeyPatch) -> None:
    """A scored row keeps its first measurement — the UPDATE is guarded."""
    points = [(date(2026, 6, 1), 10.0), (date(2026, 7, 1), 20.0)]
    engine, calls = _engine(readouts=[_readout_row(1, "GEMX", date(2026, 6, 1))], signals=[])
    monkeypatch.setattr(
        "intelligence.long_plays._load_adj_close",
        lambda eng, tickers, years, as_of: {"GEMX": points},
    )
    to.score_trial_outcomes(engine, as_of=AS_OF)
    readout_sql = [sql for sql, _ in calls if "SET readout_return_30d" in sql][0]
    assert "readout_scored_at IS NULL" in readout_sql


# ── the fitter reads the readout anchor, not the signal anchor ────────────


def test_load_scored_outcomes_feeds_the_readout_move_to_the_fitter() -> None:
    rows = [
        ("PHASE3", "oncology", "BUY", 1.8, -0.12, date(2026, 6, 1)),
        ("PHASE2", "cns", "WATCHLIST", -0.4, 0.05, date(2026, 5, 1)),
    ]
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchall.return_value = rows
    engine = MagicMock()
    engine.connect.return_value = conn

    out = to.load_scored_outcomes(engine, as_of=AS_OF)
    assert len(out) == 2
    # the key the fitter reads carries the READOUT move
    assert out[0]["fwd_return_30d"] == pytest.approx(1.8)
    assert out[0]["readout_return_30d"] == pytest.approx(1.8)
    assert out[0]["signal_return_30d"] == pytest.approx(-0.12)

    # and it drops straight into the base-rate fitter
    from intelligence.catalyst_ev import empirical_phase_outcomes

    fitted, counts = empirical_phase_outcomes(out, min_samples=1)
    assert counts == {"PHASE3": 1, "PHASE2": 1}
    assert fitted == {"PHASE3": 1.0, "PHASE2": 0.0}


def test_load_scored_outcomes_is_empty_before_anything_is_scored() -> None:
    broken = MagicMock()
    broken.connect.side_effect = RuntimeError("column does not exist")
    assert to.load_scored_outcomes(broken) == []


# ── the ingestion windows this depends on ─────────────────────────────────


def test_the_catalyst_window_retains_history_and_reaches_18_months() -> None:
    """Scoring readouts is impossible if their dates are discarded.

    ``DAYS_LOOKBACK = 0`` deleted every readout from catalyst_calendar the
    day after it happened, which is why nothing could be measured.
    """
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1] / "grid/ingestors/trial_ingestor.py").read_text()
    ns: dict[str, Any] = {}
    for line in src.splitlines():
        if line.startswith(("DAYS_LOOKAHEAD", "DAYS_LOOKBACK")):
            exec(line, ns)  # noqa: S102 — two integer literals from our own repo
    assert ns["DAYS_LOOKBACK"] <= -365, "past readouts must be retained to be scorable"
    assert ns["DAYS_LOOKAHEAD"] >= 548, "the catalyst window must cover the 18-month gate"


def test_the_scorer_is_actually_scheduled() -> None:
    """An unscheduled scorer measures nothing — that was the original bug."""
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1] / "intelligence/scheduler.py").read_text()
    assert "from intelligence.trial_outcomes import score_trial_outcomes" in src
    assert '_sched.every().day.at("07:15").do(_trial_outcomes_daily)' in src


def test_options_coverage_reaches_the_catalyst_universe() -> None:
    """The IV upside anchor needs a chain for the names it prices."""
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1] / "ingestion/options.py").read_text()
    assert "def catalyst_options_universe" in src
    assert "include_catalyst_universe: bool = True" in src
    # bounded on both sides and capped — this drives an outbound fetch loop
    assert "expected_date >= CURRENT_DATE" in src
    assert "expected_date <= CURRENT_DATE + :horizon_days" in src
    assert ":max_tickers" in src
