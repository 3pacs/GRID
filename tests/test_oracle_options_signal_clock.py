"""The options-signal row a decision may use (oracle/engine.py, OPTIONS_SIGNAL_POLICY).

``options_daily_signals`` keeps one mutable row per ticker and market day. Every
capture of the day overwrites it in place, and ``created_at`` is the time of
its last write. So the Oracle uses the row only for a decision that is being
made now, only when the row is for the decision's own session and was last
written at or before the read clock, and a replay never reads it: what the row
said at a past clock cannot be reproduced.

Acting or replay is declared by the caller, never inferred from clocks (Codex
review 6089033234 on GRID#1507): at 10:08 a replay of the 10:00 decision and a
fresh 10:08 decision have the same wall clock and see the same overwritten row.

The sequence Codex asked for (review 6088788414 on GRID#1506), in New York
wall-clock terms on 2026-11-25 (UTC is five hours ahead):

    09:59  the row is written                        (put/call 0.9)
    10:00  a decision is made                        -> uses 0.9
    10:07  a correction overwrites the row in place  (put/call 1.3)
    10:08  the 10:00 decision is replayed            -> refused, the table is not even read
    10:08  a new decision is made, same wall clock   -> uses 1.3
    any    a read with no provable timing            -> not used

SQLite runs the engine's real statement through the real visibility fragment.
Offline: no provider, network or PostgreSQL access.
"""

from __future__ import annotations

import sqlite3
import sys
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, text

from oracle import engine as oe

DAY = date(2026, 11, 25)


def _ny(hour: int, minute: int = 0, second: int = 0, day: date = DAY) -> datetime:
    """A New York wall-clock time on ``day`` as UTC (EST, UTC-5, in late November)."""
    return datetime(day.year, day.month, day.day, hour + 5, minute, second, tzinfo=timezone.utc)


@pytest.fixture
def db():
    sqlite3.register_converter("DATE", lambda raw: date.fromisoformat(raw.decode()))
    sqlite3.register_converter("TIMESTAMPTZ", lambda raw: datetime.fromisoformat(raw.decode()))
    engine = create_engine("sqlite://", connect_args={"detect_types": sqlite3.PARSE_DECLTYPES})
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE options_capture_batches (capture_batch_id TEXT, ticker TEXT, snap_date DATE,"
            " spot_provider TEXT, clock_metadata TEXT)"))
        conn.execute(text(
            "CREATE TABLE options_daily_signals (ticker TEXT, signal_date DATE, put_call_ratio REAL,"
            " iv_atm REAL, iv_skew REAL, max_pain REAL, spot_price REAL, total_oi INTEGER,"
            " term_structure_slope REAL, oi_concentration REAL, wing_iv_metadata TEXT,"
            " created_at TIMESTAMPTZ, PRIMARY KEY (ticker, signal_date))"))
    yield engine
    engine.dispose()


def _write(db: Any, pcr: float, written_at: datetime | None, *, day: date = DAY, ticker: str = "SPY") -> None:
    """Write the day's row the way the puller does: in place, one row per ticker and day."""
    with db.begin() as conn:
        conn.execute(text("DELETE FROM options_daily_signals WHERE ticker = :t AND signal_date = :d"),
                     {"t": ticker, "d": day.isoformat()})
        conn.execute(text(
            "INSERT INTO options_daily_signals (ticker, signal_date, put_call_ratio, iv_atm, max_pain,"
            " spot_price, total_oi, created_at) VALUES (:t, :d, :pcr, 0.25, 600, 610, 5000, :at)"),
            {"t": ticker, "d": day.isoformat(), "pcr": pcr,
             "at": written_at.isoformat(sep=" ") if written_at is not None else None})


def _oracle(db: Any) -> oe.OracleEngine:
    oracle = oe.OracleEngine.__new__(oe.OracleEngine)
    oracle.engine = db
    return oracle


def _wall_clock(monkeypatch, now: datetime) -> None:
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz) if tz is not None else now

    monkeypatch.setattr(oe, "datetime", Clock)


ACTING, REPLAY = oe.DECISION_MODE_ACTING, oe.DECISION_MODE_REPLAY


def _decide(monkeypatch, db: Any, at: datetime, mode: str = ACTING):
    """What a decision in ``mode`` gets when the wall clock reads ``at``: the engine's own read, judged."""
    _wall_clock(monkeypatch, at)
    read = _oracle(db)._options_read_for("SPY", mode)
    return oe.OracleEngine.decision_time_options_read(read, mode)


# ── The sequence ──────────────────────────────────────────────────────


def test_a_decision_uses_the_row_as_written_before_it(monkeypatch, db) -> None:
    _write(db, 0.9, _ny(9, 59))
    used, basis = _decide(monkeypatch, db, _ny(10, 0))
    assert used.values[0] == 0.9
    assert basis == {
        "policy": "oracle_options_signal_live_v2", "mode": "acting", "status": "used",
        "session_date": "2026-11-25", "signal_date": "2026-11-25",
        "written_at": "2026-11-25T14:59:00+00:00", "read_at": "2026-11-25T15:00:00+00:00",
    }


def test_at_the_same_wall_clock_a_replay_is_refused_and_a_fresh_decision_uses_the_correction(monkeypatch, db) -> None:
    """Review 6089033234. 10:08: the row was overwritten at 10:07. A replay of the 10:00 decision and
    a new 10:08 decision have the same clock and would read the same row. Only the declared mode
    tells them apart."""
    _write(db, 0.9, _ny(9, 59))
    first, first_basis = _decide(monkeypatch, db, _ny(10, 0))
    frozen = dict(first_basis)
    _write(db, 1.3, _ny(10, 7))                               # the correction, in place

    replayed, replay_basis = _decide(monkeypatch, db, _ny(10, 8), REPLAY)
    fresh, fresh_basis = _decide(monkeypatch, db, _ny(10, 8), ACTING)

    assert replayed.values is None                            # never answered with the 10:07 overwrite
    assert (replay_basis["mode"], replay_basis["status"]) == ("replay", "historical_replay_unavailable")
    assert replay_basis["written_at"] is None and replay_basis["read_at"] is None     # it read nothing
    assert fresh.values[0] == 1.3
    assert (fresh_basis["mode"], fresh_basis["status"], fresh_basis["written_at"], fresh_basis["read_at"]) == (
        "acting", "used", "2026-11-25T15:07:00+00:00", "2026-11-25T15:08:00+00:00")
    # What the 10:00 decision used and recorded is untouched by the correction.
    assert first.values[0] == 0.9 and first_basis == frozen


def test_a_replay_does_not_query_the_table(monkeypatch) -> None:
    oracle = oe.OracleEngine.__new__(oe.OracleEngine)
    oracle.engine = MagicMock()
    oracle._read_options_signal = MagicMock(side_effect=AssertionError("a replay must not read the row"))
    for mode in (REPLAY, "", "live", "ACTING", None):          # anything not exactly "acting" is a replay
        assert oracle._options_read_for("SPY", mode) == oe.OptionsSignalRead(None, False)
    oracle.engine.connect.assert_not_called()


def test_a_replay_is_refused_whatever_read_it_is_handed(monkeypatch, db) -> None:
    """Even a read that an acting decision could use: the mode decides first."""
    _write(db, 0.9, _ny(9, 59))
    _wall_clock(monkeypatch, _ny(10, 0))
    usable = _oracle(db)._read_options_signal("SPY")
    assert oe.OracleEngine.decision_time_options_read(usable, ACTING)[1]["status"] == "used"
    for mode in (REPLAY, "live", "", None):
        refused, basis = oe.OracleEngine.decision_time_options_read(usable, mode)
        assert refused.values is None
        assert (basis["mode"], basis["status"]) == ("replay", "historical_replay_unavailable")


def test_a_row_written_after_the_acting_clock_is_never_used_for_it(monkeypatch, db) -> None:
    """The receipt cutoff of an acting read: its own recorded clock. A write after it is not known to it."""
    _write(db, 1.3, _ny(10, 7))
    _wall_clock(monkeypatch, _ny(10, 30))
    read = replace(_oracle(db)._read_options_signal("SPY"), read_at=_ny(10, 0))
    refused, basis = oe.OracleEngine.decision_time_options_read(read, ACTING)
    assert refused.values is None
    assert basis["status"] == "unavailable_written_after_decision_clock"
    assert (basis["written_at"], basis["read_at"]) == ("2026-11-25T15:07:00+00:00", "2026-11-25T15:00:00+00:00")


def test_a_row_written_in_the_same_instant_as_the_read_is_known(monkeypatch, db) -> None:
    _write(db, 0.9, _ny(10, 0))
    used, basis = _decide(monkeypatch, db, _ny(10, 0))
    assert used.values[0] == 0.9 and basis["status"] == "used"


# ── No provable timing: not used ──────────────────────────────────────

_VALUES = (0.9, 0.25, None, 600.0, 610.0, 5000, None, None)


@pytest.mark.parametrize("read", [
    oe.OptionsSignalRead(_VALUES, False),                                             # caller-built, undated
    oe.OptionsSignalRead(_VALUES, False, DAY),                                        # no write time, no read clock
    oe.OptionsSignalRead(_VALUES, False, DAY, _ny(9, 59)),                            # no read clock
    oe.OptionsSignalRead(_VALUES, False, DAY, None, _ny(10, 0)),                      # no write time
    oe.OptionsSignalRead(_VALUES, False, None, _ny(9, 59), _ny(10, 0)),               # no market day
    oe.OptionsSignalRead(_VALUES, False, DAY, datetime(2026, 11, 25, 14, 59), _ny(10, 0)),   # unzoned write time
])
def test_a_read_without_provable_timing_is_not_used(read: oe.OptionsSignalRead) -> None:
    refused, basis = oe.OracleEngine.decision_time_options_read(read, ACTING)
    assert refused.values is None
    assert basis["status"] == "unavailable_timing_unverifiable"


def test_a_stored_row_with_no_write_time_is_not_used(monkeypatch, db) -> None:
    _write(db, 0.9, None)
    refused, basis = _decide(monkeypatch, db, _ny(10, 0))
    assert refused.values is None and basis["status"] == "unavailable_timing_unverifiable"
    assert basis["signal_date"] == "2026-11-25" and basis["written_at"] is None


# ── Other sessions ────────────────────────────────────────────────────


def test_a_row_from_another_session_is_not_used_and_none_is_substituted(monkeypatch, db) -> None:
    # 2026-11-25 in production: the newest row the Oracle could see was dated 11-19.
    _write(db, 0.9, _ny(16, 5, day=date(2026, 11, 19)), day=date(2026, 11, 19))
    refused, basis = _decide(monkeypatch, db, _ny(10, 43))
    assert refused.values is None
    assert (basis["status"], basis["signal_date"], basis["session_date"]) == (
        "unavailable_other_market_session", "2026-11-19", "2026-11-25")


def test_yesterdays_row_is_another_session_too(monkeypatch, db) -> None:
    _write(db, 0.9, _ny(16, 5, day=date(2026, 11, 24)), day=date(2026, 11, 24))
    refused, basis = _decide(monkeypatch, db, _ny(9, 31))
    assert refused.values is None and basis["status"] == "unavailable_other_market_session"


def test_the_session_is_the_new_york_day_of_the_read() -> None:
    # 00:30Z on the 26th is still the evening of the 25th in New York.
    late = datetime(2026, 11, 26, 0, 30, tzinfo=timezone.utc)
    read = oe.OptionsSignalRead(_VALUES, False, DAY, late - timedelta(minutes=5), late)
    used, basis = oe.OracleEngine.decision_time_options_read(read, ACTING)
    assert used.values == _VALUES and basis["session_date"] == "2026-11-25"


def test_no_row_and_no_read_are_labelled_and_pass_through(monkeypatch, db) -> None:
    _wall_clock(monkeypatch, _ny(10, 0))
    read = _oracle(db)._read_options_signal("SPY")
    assert read.values is None and read.read_at == _ny(10, 0)
    same, basis = oe.OracleEngine.decision_time_options_read(read, ACTING)
    assert same is read and basis["status"] == "no_row"
    none, basis = oe.OracleEngine.decision_time_options_read(None, ACTING)
    assert none is None and basis["status"] == "unread"


def test_a_refused_read_keeps_its_lineage() -> None:
    read = oe.OptionsSignalRead(_VALUES, True, date(2026, 11, 19), _ny(9, 59), _ny(10, 0))
    for mode in (ACTING, REPLAY):
        refused, _basis = oe.OracleEngine.decision_time_options_read(read, mode)
        assert refused.values is None and refused.schwab_private is True
        assert refused.signal_date == date(2026, 11, 19)


def test_the_two_shared_findings_use_the_thesis_scorers_words() -> None:
    from analysis import thesis_scorer

    assert oe.OPTIONS_REPLAY_UNAVAILABLE == thesis_scorer.OPTIONS_REPLAY_UNAVAILABLE
    assert oe.OPTIONS_WRITTEN_AFTER_CLOCK == thesis_scorer.OPTIONS_WRITTEN_AFTER_CLOCK


# ── Through the per-ticker pass ───────────────────────────────────────


def _ticker_oracle(read: oe.OptionsSignalRead, reference: dict[str, Any], seen: list[Any]) -> oe.OracleEngine:
    oracle = oe.OracleEngine.__new__(oe.OracleEngine)
    oracle.engine = MagicMock()
    oracle.models = [oe.OracleModel("test_model", "1.0", "test", ["equity", "vol"])]
    oracle._get_flow_context = lambda ticker: {}
    oracle._get_entry_price_reference = lambda ticker: reference
    oracle._get_journal_feedback = lambda ticker: {}
    oracle._get_convergence_for_ticker = lambda ticker: []
    oracle._read_options_signal = lambda ticker: read
    oracle._schwab_private_inputs = lambda ticker, ref, options_read: bool(seen.append(("tag", options_read)))
    oracle._get_realized_daily_vol = lambda ticker, as_of, window=20: 0.01
    oracle._find_anti_signals = lambda sigs, direction, ticker=None, **kw: []

    def gather(ticker, families, options_read=None):
        seen.append(("gather", options_read))
        return [oe.Signal("vix_spot", "vol", 11.0, -2.5, "bullish", 1.0, 0),
                oe.Signal("reg_bull_neg_z", "equity", -1.5, -1.5, "bullish", 1.0, 0),
                oe.Signal("minor_bear", "breadth", -0.6, -0.6, "bearish", 1.0, 0)]

    oracle._gather_signals = gather
    return oracle


def _quiet_model_extras(monkeypatch) -> None:
    monkeypatch.setattr(oe, "_USE_SIGNAL_REGISTRY", False)
    monkeypatch.setitem(sys.modules, "intelligence.confidence_calibration",
                        SimpleNamespace(calibrate_confidence=lambda v, *a: v))
    monkeypatch.setenv("GRID_SIGNAL_OVERRIDES_ENABLED", "false")
    import intelligence.signal_weight_overrides as swo

    monkeypatch.setattr(swo, "SIGNAL_OVERRIDES_ENABLED", False)


def test_the_models_get_a_usable_row_and_every_prediction_records_the_receipt(monkeypatch) -> None:
    live = oe.OptionsSignalRead(_VALUES, False, DAY, _ny(9, 59), _ny(10, 0))
    seen: list[Any] = []
    oracle = _ticker_oracle(live, {"status": "measured", "price": 100.0}, seen)
    _quiet_model_extras(monkeypatch)
    preds = oracle._oracle_one_ticker(1, "SPY", 1, _ny(10, 0), {}, horizon_days=1, decision_mode=ACTING)
    assert preds and all(read is live for kind, read in seen)
    assert all(p.flow_context["options_signal_basis"]["status"] == "used" for p in preds)
    assert preds[0].flow_context["options_signal_basis"]["written_at"] == "2026-11-25T14:59:00+00:00"


def test_the_models_never_see_a_refused_row_and_the_tag_is_judged_on_the_read_as_made(monkeypatch) -> None:
    stale = oe.OptionsSignalRead(_VALUES, True, date(2026, 11, 19), _ny(9, 59), _ny(10, 43))
    seen: list[Any] = []
    oracle = _ticker_oracle(stale, {"status": "measured", "price": 100.0}, seen)
    _quiet_model_extras(monkeypatch)
    preds = oracle._oracle_one_ticker(1, "SPY", 1, _ny(10, 43), {}, horizon_days=1, decision_mode=ACTING)
    kinds = [kind for kind, _read in seen]
    assert kinds[0] == "tag" and "gather" in kinds
    assert seen[0][1] is stale                                  # the private-use tag sees the read as made
    assert all(read.values is None for kind, read in seen if kind == "gather")
    assert preds and all(p.flow_context["options_signal_basis"]["status"]
                         == "unavailable_other_market_session" for p in preds)


def test_abstentions_carry_the_receipt_too() -> None:
    live = oe.OptionsSignalRead(_VALUES, False, DAY, _ny(9, 59), _ny(10, 0))
    oracle = _ticker_oracle(live, {"status": "unavailable", "reason": "no qualified quote"}, [])
    preds = oracle._oracle_one_ticker(1, "SPY", 1, _ny(10, 0), {}, horizon_days=1, decision_mode=ACTING)
    assert [p.verdict for p in preds] == [oe.Verdict.NO_DATA]
    assert preds[0].flow_context["options_signal_basis"]["policy"] == "oracle_options_signal_live_v2"
    assert preds[0].flow_context["options_signal_basis"]["status"] == "used"


def test_a_pass_that_does_not_declare_itself_acting_is_a_replay_and_reads_nothing(monkeypatch) -> None:
    """The default is fail-closed: no declaration, no options row, and the table is not touched."""
    live = oe.OptionsSignalRead(_VALUES, True, DAY, _ny(9, 59), _ny(10, 0))
    seen: list[Any] = []
    oracle = _ticker_oracle(live, {"status": "measured", "price": 100.0}, seen)
    oracle._read_options_signal = MagicMock(side_effect=AssertionError("a replay must not read the row"))
    _quiet_model_extras(monkeypatch)
    preds = oracle._oracle_one_ticker(1, "SPY", 1, _ny(10, 0), {}, horizon_days=1)
    assert preds
    assert all(read.values is None for _kind, read in seen)
    # Nothing was consumed from the table, so the options side contributes no private-use lineage.
    assert seen[0] == ("tag", oe.OptionsSignalRead(None, False))
    assert all(p.flow_context["options_signal_basis"]["mode"] == "replay"
               and p.flow_context["options_signal_basis"]["status"] == "historical_replay_unavailable"
               for p in preds)


def test_a_failed_read_never_lets_gathering_read_the_table_on_its_own(monkeypatch) -> None:
    """Given None, _gather_signals makes its own read of the row, outside the policy."""
    seen: list[Any] = []
    oracle = _ticker_oracle(None, {"status": "measured", "price": 100.0}, seen)

    def failing(ticker):
        raise RuntimeError("database unavailable")

    oracle._read_options_signal = failing
    _quiet_model_extras(monkeypatch)
    preds = oracle._oracle_one_ticker(1, "SPY", 1, _ny(10, 0), {}, horizon_days=1, decision_mode=ACTING)
    gathered = [read for kind, read in seen if kind == "gather"]
    assert preds and gathered and all(read is not None and read.values is None for read in gathered)
    assert seen[0] == ("tag", None)                             # the failed read still tags private (fail closed)
    assert preds[0].flow_context["options_signal_basis"]["status"] == "unread"


def test_generate_predictions_declares_the_acting_mode(monkeypatch) -> None:
    from oracle import calibration, hallucination_guard

    oracle = oe.OracleEngine.__new__(oe.OracleEngine)
    oracle.engine = object()
    oracle._load_models = lambda: [SimpleNamespace(name="fixture", hits=0, misses=0, partials=0)]
    monkeypatch.setenv("GRID_ORACLE_PARALLELISM", "1")
    monkeypatch.setattr(calibration, "compute_calibration", lambda engine: None)
    monkeypatch.setattr(hallucination_guard, "verify_predictions", lambda preds, **kw: (preds, []))
    modes: list[Any] = []

    def one_ticker(idx, ticker, total, now, cache, **kw):
        modes.append(kw.get("decision_mode"))
        return []

    oracle._oracle_one_ticker = one_ticker
    oracle._store_predictions = lambda preds: []
    oracle._last_store_counts = {}
    oracle.generate_predictions(["SPY", "QQQ"], horizon_days=1)
    assert modes == [oe.DECISION_MODE_ACTING, oe.DECISION_MODE_ACTING]


def test_the_statement_selects_the_market_day_and_the_write_time() -> None:
    assert oe._OPTIONS_SIGNAL_SQL.count("signal_date, created_at") == 1
    assert "ORDER BY signal_date DESC LIMIT 1" in oe._OPTIONS_SIGNAL_SQL
