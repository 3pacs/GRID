"""A priced ticker whose applicable model finds no direction leaves a stated abstention.

Before, `_oracle_one_ticker` skipped such a model with a bare `continue`, so a
captured, priced ticker (SPY on every pass) wrote no row at all and could not
be told apart from a ticker that was never run. A model with fewer than three
signals does not apply to the ticker and still records nothing.
"""
from __future__ import annotations

import json
import sys
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from oracle import engine as oe
from oracle.entry_price_policy import QUOTE_REFERENCE_POLICY

# Friday after Thanksgiving, 10:40 New York: a regular (half-day) session.
NOW = datetime(2026, 11, 27, 15, 40, tzinfo=UTC)
SPOT = 731.175


def _receipt(**overrides) -> dict:
    market = NOW - timedelta(minutes=5)
    receipt = {
        "policy": QUOTE_REFERENCE_POLICY, "status": "measured", "ticker": "SPY",
        "source": "options_capture_batches", "provider": "cboe", "basis": "raw_spot",
        "capture_batch_id": "batch-1", "capture_source": "oracle_forward", "backfilled": False,
        "price": SPOT, "observed_on": "2026-11-27",
        "market_at": market.isoformat(),
        "capture_started_at": (market + timedelta(seconds=10)).isoformat(),
        "capture_completed_at": (market + timedelta(seconds=20)).isoformat(),
        "available_at": (market + timedelta(seconds=30)).isoformat(),
        "read_observed_at": (market + timedelta(seconds=40)).isoformat(),
    }
    receipt.update(overrides)
    return receipt


def _oracle(signals: list[oe.Signal], receipt: dict | None = None) -> oe.OracleEngine:
    reference = receipt or _receipt()
    oracle = oe.OracleEngine.__new__(oe.OracleEngine)
    oracle.engine = MagicMock()
    oracle.models = [oe.OracleModel("test_model", "1.0", "test", ["equity", "vol"])]
    oracle._get_flow_context = lambda ticker: {}
    oracle._get_entry_price_reference = lambda ticker: reference
    oracle._get_journal_feedback = lambda ticker: {}
    oracle._get_convergence_for_ticker = lambda ticker: []
    oracle._read_options_signal = lambda ticker: oe.OptionsSignalRead(None, False)
    oracle._gather_signals = lambda ticker, families, options_read=None: list(signals)
    oracle._get_realized_daily_vol = lambda ticker, as_of, window=20: 0.01
    oracle._find_anti_signals = lambda sigs, direction, ticker=None, **kw: []
    return oracle


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    monkeypatch.setattr(oe, "_USE_SIGNAL_REGISTRY", False)
    monkeypatch.setitem(sys.modules, "intelligence.confidence_calibration",
                        SimpleNamespace(calibrate_confidence=lambda v, *a: v))
    import intelligence.signal_weight_overrides as swo
    monkeypatch.setattr(swo, "SIGNAL_OVERRIDES_ENABLED", False)


def _neutral(name: str) -> oe.Signal:
    return oe.Signal(name, "equity", 0.0, 0.4, "neutral", 1.0, 0)


def _assert_no_direction(pred: oe.OraclePrediction, n_signals: int) -> None:
    assert pred.direction == "NONE"
    assert pred.verdict == oe.Verdict.NO_DATA
    assert pred.confidence == 0.0
    assert pred.score_notes == "No directional evidence at decision time"
    assert pred.flow_context["abstention_kind"] == "no_direction"
    assert pred.flow_context["entry_price_basis"]["price"] == SPOT
    assert len(pred.signals) == n_signals
    assert pred.model_name == "test_model"
    assert pred.horizon_days is not None


def test_only_neutral_signals_write_a_no_direction_abstention() -> None:
    signals = [_neutral("alpha_research:vol_price_divergence") for _ in range(3)]

    preds = _oracle(signals)._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)

    assert len(preds) == 1
    _assert_no_direction(preds[0], 3)


def test_balanced_directional_signals_abstain_too() -> None:
    signals = [
        oe.Signal("up", "equity", 1.0, 1.0, "bullish", 1.0, 0),
        oe.Signal("down", "equity", -1.0, -1.0, "bearish", 1.0, 0),
        _neutral("flat"),
    ]

    preds = _oracle(signals)._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)

    assert len(preds) == 1
    _assert_no_direction(preds[0], 3)


def test_a_model_with_fewer_than_three_signals_does_not_apply_and_writes_nothing() -> None:
    preds = _oracle([_neutral("a"), _neutral("b")])._oracle_one_ticker(
        1, "SPY", 1, NOW, {}, horizon_days=1)

    assert preds == []


def test_an_unqualified_receipt_makes_a_quote_abstention_not_a_no_direction_one() -> None:
    """The store would refuse this receipt; the row must not claim "no direction"."""
    stale = _receipt(market_at=(NOW - timedelta(minutes=25)).isoformat(),
                     capture_started_at=(NOW - timedelta(minutes=24)).isoformat(),
                     capture_completed_at=(NOW - timedelta(minutes=24)).isoformat(),
                     available_at=(NOW - timedelta(minutes=24)).isoformat(),
                     read_observed_at=(NOW - timedelta(minutes=23)).isoformat())
    signals = [_neutral(n) for n in "abc"]

    preds = _oracle(signals, receipt=stale)._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)

    assert len(preds) == 1
    assert preds[0].direction == "NONE"
    assert preds[0].verdict == oe.Verdict.NO_DATA
    assert "abstention_kind" not in preds[0].flow_context
    assert preds[0].score_notes == "quote is older than the 20-minute reference limit"


def test_directional_evidence_still_yields_a_real_prediction() -> None:
    signals = [
        oe.Signal("alpha_research:dual_horizon_equity", "equity", 0.9, 0.9, "bullish", 1.0, 0),
        _neutral("b"),
        _neutral("c"),
    ]

    preds = _oracle(signals, receipt=_receipt(ticker="QQQ"))._oracle_one_ticker(
        1, "QQQ", 1, NOW, {}, horizon_days=1)

    assert len(preds) == 1
    assert preds[0].direction == "CALL"
    assert preds[0].verdict == oe.Verdict.PENDING
    assert "abstention_kind" not in preds[0].flow_context


class _InsertRecorder:
    """A connection that records oracle_predictions INSERTs; ``fresh`` decides RETURNING."""

    def __init__(self, fresh: list[bool]) -> None:
        self.fresh = list(fresh)
        self.inserts: list[dict] = []

    def execute(self, stmt, params=None):
        sql = str(getattr(stmt, "text", stmt))
        result = MagicMock()
        if "INSERT INTO oracle_predictions" in sql:
            self.inserts.append(dict(params))
            result.fetchone.return_value = (params["id"],) if self.fresh.pop(0) else None
        return result


def _store(preds: list[oe.OraclePrediction], fresh: list[bool], monkeypatch) -> tuple:
    from oracle.model_factory import ModelFactory

    conn = _InsertRecorder(fresh)
    begin = MagicMock()
    begin.__enter__ = lambda self: conn
    begin.__exit__ = lambda self, *exc: False
    oracle = oe.OracleEngine.__new__(oe.OracleEngine)
    oracle.engine = MagicMock()
    oracle.engine.begin.return_value = begin
    monkeypatch.setattr(ModelFactory, "active_model_names",
                        staticmethod(lambda conn, names, lock=False: set(names)))
    committed = oracle._store_predictions(preds)
    return committed, conn.inserts, oracle._last_store_counts


def test_a_stored_no_direction_row_is_an_abstention_never_a_qualified_forecast(monkeypatch) -> None:
    abstain = _oracle([_neutral(n) for n in "abc"])._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)
    call = _oracle([
        oe.Signal("alpha_research:dual_horizon_equity", "equity", 0.9, 0.9, "bullish", 1.0, 0),
        _neutral("b"), _neutral("c"),
    ])._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)

    committed, inserts, counts = _store(abstain + call, [True, True], monkeypatch)

    assert [(c.direction, c.verdict) for c in committed] == [
        ("NONE", oe.Verdict.NO_DATA), ("CALL", oe.Verdict.PENDING)]
    assert (counts["abstentions"], counts["qualified"], counts["duplicates"]) == (1, 1, 0)
    row = inserts[0]
    assert (row["d"], row["verdict"], row["ep"], row["conf"]) == ("NONE", "no_data", None, None)
    assert row["score_notes"] == "No directional evidence at decision time"
    assert json.loads(row["fc"])["abstention_kind"] == "no_direction"
    # The forecast identity carries the direction, so NONE and CALL never collide.
    assert row["id"] != inserts[1]["id"]


def test_a_repeated_no_direction_row_is_counted_as_a_duplicate(monkeypatch) -> None:
    abstain = _oracle([_neutral(n) for n in "abc"])._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)

    committed, _, counts = _store(abstain, [False], monkeypatch)

    assert committed == []
    assert (counts["abstentions"], counts["qualified"], counts["duplicates"]) == (0, 0, 1)


def test_the_registry_path_abstains_the_same_way(monkeypatch) -> None:
    """Production reads the signal registry (GRID_SIGNAL_REGISTRY=1)."""
    monkeypatch.setattr(oe, "_USE_SIGNAL_REGISTRY", True)
    oracle = _oracle([])
    oracle._gather_signals_from_registry = lambda ticker, model: [
        _neutral("alpha_research:vol_price_divergence") for _ in range(31)]
    oracle._gather_signals = lambda *a, **k: pytest.fail("legacy path must not run")

    preds = oracle._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)

    assert len(preds) == 1
    _assert_no_direction(preds[0], 31)


def test_a_receipt_that_fails_at_the_decision_clock_turns_no_direction_into_a_quote_abstention(
    monkeypatch,
) -> None:
    """generate_predictions restamps candidates with decision_at before storing them."""
    from dataclasses import replace

    abstain = _oracle([_neutral(n) for n in "abc"])._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)
    assert abstain[0].flow_context["abstention_kind"] == "no_direction"
    late = [replace(p, timestamp=NOW + timedelta(minutes=16)) for p in abstain]  # quote now 21 min old

    _, inserts, counts = _store(late, [True], monkeypatch)

    fc = json.loads(inserts[0]["fc"])
    assert fc["abstention_kind"] == "no_qualified_quote"
    assert inserts[0]["score_notes"] == "quote is older than the 20-minute reference limit"
    assert counts["abstentions"] == 1


def test_a_no_direction_row_after_a_quote_abstention_is_a_duplicate_and_the_pass_reads_abstained_other(
    monkeypatch,
) -> None:
    """Both kinds are NONE under one unique key: the first stored row stays.

    The 14:31 pass stored a quote abstention for the model; the 14:41 pass,
    with a valid quote and no direction, collides with it. The stored row
    keeps the quote reason, while this pass's outcome (what the retry plan
    reads) says the ticker abstained for another reason, not for a quote.
    """
    from dataclasses import replace

    first = _oracle([_neutral(n) for n in "abc"], receipt=_receipt(
        market_at=(NOW - timedelta(minutes=25)).isoformat(),
        capture_started_at=(NOW - timedelta(minutes=24)).isoformat(),
        capture_completed_at=(NOW - timedelta(minutes=24)).isoformat(),
        available_at=(NOW - timedelta(minutes=24)).isoformat(),
        read_observed_at=(NOW - timedelta(minutes=23)).isoformat(),
    ))._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)
    later = NOW + timedelta(minutes=10)
    second = _oracle([_neutral(n) for n in "abc"], receipt=_receipt(
        market_at=(later - timedelta(minutes=5)).isoformat(),
        capture_started_at=(later - timedelta(minutes=4)).isoformat(),
        capture_completed_at=(later - timedelta(minutes=4)).isoformat(),
        available_at=(later - timedelta(minutes=4)).isoformat(),
        read_observed_at=(later - timedelta(minutes=3)).isoformat(),
    ))._oracle_one_ticker(1, "SPY", 1, later, {}, horizon_days=1)
    second = [replace(p, timestamp=later) for p in second]

    _, first_rows, _ = _store(first, [True], monkeypatch)
    _, second_rows, counts = _store(second, [False], monkeypatch)

    key = ("t", "d", "exp", "pt", "mn", "mv", "rp")
    assert [first_rows[0][k] for k in key] == [second_rows[0][k] for k in key]
    assert first_rows[0]["created_at"].date() == second_rows[0]["created_at"].date()
    assert json.loads(first_rows[0]["fc"])["abstention_kind"] == "no_qualified_quote"
    assert json.loads(second_rows[0]["fc"])["abstention_kind"] == "no_direction"
    assert counts["duplicates"] == 1


def test_pass_outcome_of_a_duplicate_no_direction_row(monkeypatch) -> None:
    from oracle.forward_health import TICKER_ABSTAINED_OTHER, pass_outcome

    abstain = _oracle([_neutral(n) for n in "abc"])._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)
    from oracle.model_factory import ModelFactory

    conn = _InsertRecorder([False])
    begin = MagicMock()
    begin.__enter__ = lambda self: conn
    begin.__exit__ = lambda self, *exc: False
    oracle = oe.OracleEngine.__new__(oe.OracleEngine)
    oracle.engine = MagicMock()
    oracle.engine.begin.return_value = begin
    monkeypatch.setattr(ModelFactory, "active_model_names",
                        staticmethod(lambda conn, names, lock=False: set(names)))
    oracle._store_predictions(abstain)

    outcome = pass_outcome(["SPY"], oracle._last_store_detail)

    assert outcome["ticker_outcomes"]["SPY"] == TICKER_ABSTAINED_OTHER
    assert outcome["all_abstained"] is True
