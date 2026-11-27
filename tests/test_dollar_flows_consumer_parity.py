"""What the model that subscribes to ``dollar_flows`` gets, before and after 13F rows leave the adapter.

Codex review 6086917833 asked for this before the changeover is called
qualified. One active model subscribes to ``dollar_flows`` and to nothing
else, with a minimum of three signals and the forecast family as its legacy
inputs. Two readers hand it signals, and they treat a short list differently:

- ``EnsemblePredictor.predict`` skips the model for a ticker when it has
  fewer signals than the model's minimum.
- The Oracle cycle (``OracleEngine._oracle_one_ticker``) uses the registry's
  signals when there is at least one, and only an EMPTY list sends it to the
  model's own families (here the forecast). It then needs three signals.

So taking the 13F rows out is not only "fewer signals". Where a ticker had
nothing but 13F rows the cycle goes back to the forecast family; where one or
two other rows remain it neither votes nor goes back. These cases pin each
registry state on both readers: 13F rows alone (before), empty (after),
surviving neutral rows, a surviving row with no ticker, and the same signal
registered by two consecutive adapter runs, which both readers count twice
(present behaviour, not changed here).

Real: the adapter, ``SignalRegistry.register``, ``get_signals_for_model`` and
its SQL on PostgreSQL, ``SignalAggregator``, and ``_oracle_one_ticker`` up to
the point where the model's signals are chosen. Replaced: the model's spec
lookup (the test table has two columns), the cycle's price, options and
journal reads, and the legacy family reader, which records that it was asked.
No prediction is made: the cycle is stopped where it has chosen the signals.
"""

from __future__ import annotations

import types
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

import oracle.engine as oracle_engine
from intelligence import actor_signal_bridge
from intelligence.adapters.dollar_flows_adapter import DollarFlowsAdapter
from intelligence.signal_registry import SignalRegistry
from oracle.engine import OracleEngine, Signal
from oracle.model_factory import ModelFactory, ModelSpec
from oracle.signal_aggregator import SignalAggregator
from tests import test_dollar_flows_past_date_readers as _readers_suite
from tests.test_dollar_flows_past_date_readers import (
    _MODEL_TABLES,
    _as_the_previous_adapter_registered,
    _write,
    _write_13f,
)

pg_engine = _readers_suite.pg_engine          # the same disposable-schema fixture

MODEL = "timeseries_enhanced_mut"
FAMILIES = ["timeseries_forecast"]
SPEC = ModelSpec(name=MODEL, signal_sources=["dollar_flows"], min_signals=3)
FORECAST = [Signal(f"timesfm_{n}", "timeseries_forecast", 1.0, 1.0, "bullish", 1.0, 0) for n in range(3)]


@pytest.fixture()
def now() -> datetime:
    # The cycle reads the registry at the wall clock, so the rows sit around it.
    return datetime.now(timezone.utc)


@pytest.fixture()
def readers(pg_engine, monkeypatch):
    """readers(ticker) -> (ensemble's view, cycle's view) of the registry now."""
    monkeypatch.setenv("GRID_SIGNAL_REGISTRY", "1")
    monkeypatch.setattr(oracle_engine, "_USE_SIGNAL_REGISTRY", True)
    monkeypatch.setattr(ModelFactory, "__init__", lambda self, engine: setattr(self, "engine", engine))
    monkeypatch.setattr(ModelFactory, "get_model_spec", lambda self, name: SPEC)
    with pg_engine.begin() as conn:
        for ddl in _MODEL_TABLES:
            conn.execute(text(ddl))
        conn.execute(text("INSERT INTO oracle_models (name, active) VALUES (:n, TRUE)"), {"n": MODEL})

    def _ensemble(ticker: str, as_of: datetime) -> dict:
        # EnsemblePredictor.predict's own two lines: the read and the minimum.
        signals = ModelFactory(pg_engine).get_signals_for_model(MODEL, as_of, ticker=ticker)
        if len(signals) < SPEC.min_signals:
            return {"votes": False, "signals": len(signals)}
        vote = SignalAggregator().aggregate(signals, SPEC.weight_config, as_of)
        return {"votes": True, "signals": vote.signal_count, "direction": vote.direction,
                "strength": vote.strength, "confidence": vote.confidence}

    def _cycle(ticker: str) -> dict:
        asked: list[list[str]] = []
        chosen: list[list[tuple[str, str]]] = []

        def _legacy(_ticker, families, **_kw):
            asked.append(list(families))
            return list(FORECAST)

        def _stop_here(signals, _batch):
            chosen.append([(sig.name, sig.direction) for sig in signals])
            signals.clear()                       # fewer than three: the cycle moves on, no prediction
            return {}

        monkeypatch.setattr(oracle_engine, "_inject_actor_evidence", _stop_here)
        monkeypatch.setattr(actor_signal_bridge, "get_actor_evidence_for_ticker", lambda *_a, **_k: {})
        cycle = OracleEngine.__new__(OracleEngine)
        cycle.engine = pg_engine
        cycle.models = [types.SimpleNamespace(name=MODEL, version="1", weight=1.0, signal_families=FAMILIES)]
        cycle._get_flow_context = lambda _t: {}
        cycle._get_entry_price_reference = lambda _t: {"status": "measured", "price": 100.0}
        cycle._read_options_signal = lambda _t: None
        cycle._schwab_private_inputs = lambda *_a: False
        cycle._get_journal_feedback = lambda _t: {}
        cycle._gather_signals = _legacy
        predictions = cycle._oracle_one_ticker(1, ticker, 1, datetime.now(timezone.utc), {}, None)
        assert predictions == [] and len(chosen) == 1
        return {"asked_families": asked, "chosen": chosen[0]}

    def _both(ticker: str, as_of: datetime) -> tuple[dict, dict]:
        return _ensemble(ticker, as_of), _cycle(ticker)

    return _both


def _run_adapter(engine, at: datetime) -> int:
    """One run of the adapter as it is now; returns how many signals it registered."""
    return SignalRegistry.register(DollarFlowsAdapter()._build_signals(engine, at), engine)


def _old_13f_rows(engine, at: datetime, ticker: str) -> None:
    """What the previous adapter registered for a ticker held up by 13F alone:
    the two no-ticker totals, which reach every ticker, and the ticker's own."""
    SignalRegistry.register([
        _as_the_previous_adapter_registered(None, 7_022_705_633_749.0, at),
        _as_the_previous_adapter_registered(None, 3_847_160_143.0, at - timedelta(minutes=1)),
        _as_the_previous_adapter_registered(ticker, 2_283_782_911.0, at),
    ], engine)


# ── Before: 13F rows alone ─────────────────────────────────────────────────


def test_real_types_before_the_change_13f_rows_alone_reach_the_minimum_and_close_the_fallback(
    pg_engine, readers, now
):
    _old_13f_rows(pg_engine, now - timedelta(hours=1), "XOM")

    ensemble, cycle = readers("XOM", now)

    assert ensemble == {"votes": True, "signals": 3, "direction": "neutral",
                        "strength": 0.0, "confidence": pytest.approx(0.5)}
    assert cycle["asked_families"] == []                               # the forecast family is never asked
    assert cycle["chosen"] == [("dollar_flows", "neutral")] * 3


# ── After: nothing left for the ticker ─────────────────────────────────────


def test_real_types_an_empty_registry_sends_the_cycle_to_the_forecast_family(pg_engine, readers, now):
    _old_13f_rows(pg_engine, now - timedelta(hours=5), "XOM")          # expired an hour ago
    _write_13f(pg_engine, 2_283_782_911.0, now - timedelta(days=1), direction="observed",
               action="NEW", ticker="XOM", confidence="derived")
    assert _run_adapter(pg_engine, now - timedelta(minutes=5)) == 0    # the adapter makes nothing of it

    ensemble, cycle = readers("XOM", now)

    assert ensemble == {"votes": False, "signals": 0}                  # the ensemble loses a neutral vote
    assert cycle["asked_families"] == [FAMILIES]                       # asked once, for its own families
    assert cycle["chosen"] == [(sig.name, sig.direction) for sig in FORECAST]


# ── After: other rows remain ───────────────────────────────────────────────


def _other_flows(engine, at: datetime, ticker: str | None, kinds: tuple[str, ...]) -> None:
    for kind in kinds:
        _write(engine, kind, 5_000_000.0, (at - timedelta(days=2)).date(), at - timedelta(days=1), ticker=ticker)


def test_real_types_one_or_two_surviving_rows_mean_no_vote_and_no_fallback(pg_engine, readers, now):
    """The ticker neither reaches the minimum nor goes back to the forecast."""
    _other_flows(pg_engine, now, "NVDA", ("insider", "congressional"))
    assert _run_adapter(pg_engine, now - timedelta(minutes=5)) == 2

    ensemble, cycle = readers("NVDA", now)

    assert ensemble == {"votes": False, "signals": 2}
    assert cycle["asked_families"] == []                               # the registry is not empty
    assert cycle["chosen"] == [("dollar_flows", "neutral")] * 2        # under the cycle's three: no prediction


def test_real_types_three_surviving_rows_vote_the_same_with_and_without_13f(pg_engine, readers, now):
    """A ticker that keeps the minimum: only the count moves."""
    _other_flows(pg_engine, now, "AAPL", ("insider", "congressional", "darkpool"))
    assert _run_adapter(pg_engine, now - timedelta(minutes=5)) == 3
    after, cycle_after = readers("AAPL", now)

    _old_13f_rows(pg_engine, now - timedelta(hours=1), "AAPL")         # as it stood before the change
    before, cycle_before = readers("AAPL", now)

    assert (after["votes"], before["votes"]) == (True, True)
    assert (after["signals"], before["signals"]) == (3, 6)
    assert after["direction"] == before["direction"] == "neutral"
    assert after["strength"] == before["strength"] == 0.0
    assert after["confidence"] == pytest.approx(before["confidence"]) == pytest.approx(0.5)
    assert cycle_after["asked_families"] == cycle_before["asked_families"] == []
    assert (len(cycle_after["chosen"]), len(cycle_before["chosen"])) == (3, 6)


def test_real_types_a_surviving_row_with_no_ticker_reaches_every_ticker(pg_engine, readers, now):
    """One no-ticker flow that is not 13F keeps the fallback closed everywhere."""
    _other_flows(pg_engine, now, None, ("etf",))
    assert _run_adapter(pg_engine, now - timedelta(minutes=5)) == 1

    for ticker in ("XOM", "FLS"):
        ensemble, cycle = readers(ticker, now)
        assert ensemble == {"votes": False, "signals": 1}
        assert cycle["asked_families"] == [] and len(cycle["chosen"]) == 1


# ── The same signal from two runs ──────────────────────────────────────────


def test_real_types_a_signal_registered_by_two_runs_counts_twice_in_both_readers(pg_engine, readers, now):
    """Two flows, two adapter runs an hour apart, both runs still valid: four
    rows. Both readers count four and the model passes a minimum of three on
    two signals. Present behaviour on both sides of the change."""
    _other_flows(pg_engine, now, "AAPL", ("insider", "congressional"))
    assert _run_adapter(pg_engine, now - timedelta(hours=1, minutes=5)) == 2
    assert _run_adapter(pg_engine, now - timedelta(minutes=5)) == 2

    with pg_engine.connect() as conn:
        rows, distinct = conn.execute(text(
            "SELECT count(*), count(DISTINCT signal_id) FROM signal_registry "
            "WHERE source_module = 'dollar_flows' AND valid_until > :t"), {"t": now}).one()
    assert (rows, distinct) == (4, 2)

    ensemble, cycle = readers("AAPL", now)

    assert ensemble["votes"] is True and ensemble["signals"] == 4
    assert (ensemble["direction"], ensemble["strength"]) == ("neutral", 0.0)
    assert cycle["asked_families"] == [] and len(cycle["chosen"]) == 4
