"""What the model that subscribes to ``dollar_flows`` gets, before and after 13F rows leave the adapter.

Codex review 6086917833 on #1502 asked for consumer parity before the
changeover is called qualified, and review 6094174721 on #1514 asked that it
be measured on the real consumers, not on copies of their logic. One active
model subscribes to ``dollar_flows`` and to nothing else, with a minimum of
three signals and the forecast family as its own inputs. Two readers hand it
signals, and here both run for real, end to end:

- ``EnsemblePredictor.predict``: the model's head votes only when it has at
  least ``min_signals`` registry signals; the vote comes from
  ``SignalAggregator``.
- The Oracle cycle (``OracleEngine._oracle_one_ticker``): registry signals
  when there is at least one, the model's own families only when there are
  none; then the actor seam (``_inject_actor_evidence``), the cycle's own
  minimum of three, the bull/bear vote, coherence, confidence, expected move
  and the prediction it returns.

Every case records the registry rows a ticker sees (raw and distinct
``signal_id``), what each reader did with them, and the outcome: a direction,
or a deliberate abstention. The adapter publishes every ``dollar_flows``
signal as neutral (its own 2026-04-28 note), so for adapter-built signals the
truthful outcome is a neutral ensemble vote and a cycle abstention. Two
controls register synthetic directional rows to show both readers turning
them into a direction, so a change to either vote is seen.

Real: the adapter, ``SignalRegistry.register``, ``ModelFactory`` (model row,
spec, roster and the registry SQL) on PostgreSQL, ``EnsemblePredictor.predict``,
``SignalAggregator``, ``OracleEngine._oracle_one_ticker`` and
``_inject_actor_evidence``.

Replaced, each by a fixed synthetic answer: the cycle's environment reads
(flow context, a qualified entry quote at 100.00, journal feedback, private-use
tag, convergence events, supply-shock and regulatory anti-signals, realized
volatility 2% a day), the legacy family reader (records that it was asked and
returns three bullish forecast signals), the actor-evidence reader (a complete
window with no actor rows unless a case supplies one), and the per-model
reliability curve (identity). One clock is pinned for both readers: the
ensemble is asked ``as_of`` it, the cycle is handed it as ``now`` and reads the
registry at it (``oracle.engine.datetime``), the adapter runs are placed
relative to it, and the cycle uses a seven-day horizon so no expiry is read off
the wall clock. (The previous version of this file placed its rows around the
wall clock, and its two-run case split into two dates after midnight UTC.)
"""

from __future__ import annotations

import json
import types
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

import oracle.engine as oracle_engine
from intelligence import actor_signal_bridge, confidence_calibration
from intelligence.adapters.base import sid
from intelligence.adapters.dollar_flows_adapter import DollarFlowsAdapter
from intelligence.signal_registry import RegisteredSignal, SignalRegistry, SignalType
from oracle.engine import EnsemblePredictor, OracleEngine, Signal
from oracle.engine import ABSTENTION_KIND_NO_DIRECTION, NO_DIRECTION_SCORE_NOTE
from oracle.entry_price_policy import ABSTENTION_KIND_KEY, QUOTE_REFERENCE_POLICY
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
PINNED = datetime(2026, 11, 27, 15, 0, tzinfo=timezone.utc)
RUN = PINNED - timedelta(minutes=5)           # the adapter run the readers see
HORIZON_DAYS = 7
FORECAST = [Signal(f"timesfm_{n}", "timeseries_forecast", 1.0, 1.0, "bullish", 1.0, 0) for n in range(3)]
NO_ACTORS = {"complete": True, "reason": None, "as_of": PINNED.isoformat(), "signals": []}
_REAL_INJECT = oracle_engine._inject_actor_evidence   # taken once, before any case wraps it
# The fixture's usual entry quote: a price with no receipt, so it never qualifies.
UNQUALIFIED_QUOTE = {"status": "measured", "price": 100.0, "reason": None}
UNQUALIFIED_REASON = "no qualified entry quote receipt (legacy or unknown provenance)"


def _qualified_quote(ticker: str) -> dict:
    """A receipt that qualifies at PINNED: same session, captured five minutes earlier."""
    market = PINNED - timedelta(minutes=5)
    return {
        "policy": QUOTE_REFERENCE_POLICY, "status": "measured", "ticker": ticker,
        "source": "options_capture_batches", "provider": "cboe", "basis": "raw_spot",
        "capture_batch_id": "batch-1", "capture_source": "oracle_forward", "backfilled": False,
        "price": 100.0, "observed_on": str(PINNED.date()),
        "market_at": market.isoformat(),
        "capture_started_at": (market + timedelta(seconds=10)).isoformat(),
        "capture_completed_at": (market + timedelta(seconds=20)).isoformat(),
        "available_at": (market + timedelta(seconds=30)).isoformat(),
        "read_observed_at": (market + timedelta(seconds=40)).isoformat(),
    }

# The model table with the columns ModelFactory reads (the readers suite's
# two-column table is enough for its own reads, not for the real factory).
_ORACLE_MODELS_DDL = """
CREATE TABLE oracle_models (
    name TEXT PRIMARY KEY, version TEXT, description TEXT,
    signal_sources JSONB, signal_filters JSONB, weight_config_json JSONB,
    prediction_type_col TEXT DEFAULT 'directional', target_horizon_days INTEGER DEFAULT 7,
    min_signals INTEGER DEFAULT 3, active BOOLEAN, created_by TEXT DEFAULT 'human',
    parent_model TEXT, hits INTEGER DEFAULT 0, misses INTEGER DEFAULT 0,
    partials INTEGER DEFAULT 0, horizon_buckets JSONB, weight DOUBLE PRECISION DEFAULT 1.0,
    last_updated TIMESTAMPTZ
)"""
_SIGNAL_REGISTRY_DDL = _MODEL_TABLES[1]


class _PinnedClock(datetime):
    """``oracle.engine.datetime`` with ``now()`` fixed at PINNED."""

    @classmethod
    def now(cls, tz=None):
        return PINNED if tz is None else PINNED.astimezone(tz)


@pytest.fixture()
def readers(pg_engine, monkeypatch):
    """readers(ticker, actors=NO_ACTORS) -> {"registry", "ensemble", "cycle"} at PINNED."""
    monkeypatch.setenv("GRID_SIGNAL_REGISTRY", "1")
    monkeypatch.setattr(oracle_engine, "_USE_SIGNAL_REGISTRY", True)
    with pg_engine.begin() as conn:
        conn.execute(text(_ORACLE_MODELS_DDL))
        conn.execute(text(_SIGNAL_REGISTRY_DDL))
        conn.execute(text(
            "INSERT INTO oracle_models (name, version, signal_sources, min_signals, active) "
            "VALUES (:n, '1', CAST(:src AS JSONB), 3, TRUE)"),
            {"n": MODEL, "src": json.dumps(["dollar_flows"])})

    monkeypatch.setattr(oracle_engine, "datetime", _PinnedClock)
    monkeypatch.setattr(confidence_calibration, "calibrate_confidence",
                        lambda raw, _model, _engine: float(raw))

    def _registry(ticker: str) -> dict:
        with pg_engine.connect() as conn:
            rows, distinct = conn.execute(text(
                "SELECT count(*), count(DISTINCT signal_id) FROM signal_registry "
                "WHERE source_module = 'dollar_flows' AND valid_from <= :t "
                "AND (valid_until IS NULL OR valid_until > :t) "
                "AND (ticker = :tk OR ticker IS NULL OR ticker = '')"), {"t": PINNED, "tk": ticker}).one()
        return {"rows": rows, "distinct": distinct}

    def _ensemble(ticker: str) -> dict:
        prediction = EnsemblePredictor(pg_engine).predict(ticker, as_of=PINNED)
        vote = next((v for v in prediction.model_votes if v["model_name"] == MODEL), None)
        if vote is None:
            return {"votes": False}
        return {"votes": True, "signals": vote["signal_count"], "direction": vote["direction"],
                "strength": vote["strength"], "confidence": vote["confidence"]}

    def _cycle(ticker: str, actors: dict, quote: dict | None) -> dict:
        asked: list[list[str]] = []
        at_threshold: list[int] = []

        def _legacy(_ticker, families, **_kw):
            asked.append(list(families))
            return list(FORECAST)

        def _seen_at_the_actor_seam(signals, batch):
            receipt = _REAL_INJECT(signals, batch)  # the real seam, then count what leaves it
            at_threshold.append(len(signals))
            return receipt

        monkeypatch.setattr(oracle_engine, "_inject_actor_evidence", _seen_at_the_actor_seam)
        monkeypatch.setattr(actor_signal_bridge, "get_actor_evidence_for_ticker",
                            lambda _engine, _ticker, **_kw: dict(actors))
        cycle = OracleEngine.__new__(OracleEngine)
        cycle.engine = pg_engine
        cycle.models = [types.SimpleNamespace(name=MODEL, version="1", weight=1.0, signal_families=FAMILIES)]
        cycle._get_flow_context = lambda _t: {}
        cycle._get_entry_price_reference = lambda _t: dict(quote or UNQUALIFIED_QUOTE)
        cycle._schwab_private_inputs = lambda *_a: False
        cycle._get_journal_feedback = lambda _t: {}
        cycle._gather_signals = _legacy
        cycle._get_convergence_for_ticker = lambda _t: []
        cycle._cross_lens_anti_signals = lambda *_a: []
        cycle._regulatory_anti_signals = lambda *_a: []
        cycle._get_realized_daily_vol = lambda _t, **_kw: 0.02
        predictions = cycle._oracle_one_ticker(1, ticker, 1, PINNED, {}, HORIZON_DAYS)
        mine = [p for p in predictions if p.model_name == MODEL]
        assert len(mine) <= 1
        made = mine[0] if mine else None
        return {
            "asked_families": asked,
            "at_threshold": at_threshold,
            "prediction": None if made is None else {
                "direction": made.direction, "timestamp": made.timestamp,
                "signals": sorted(s.name for s in made.signals),
                **({} if made.direction != "NONE" else {
                    "verdict": made.verdict.value,
                    "abstention_kind": made.flow_context.get(ABSTENTION_KIND_KEY),
                    "reason": made.score_notes,
                }),
            },
        }

    def _all(ticker: str, actors: dict | None = None, quote: dict | None = None) -> dict:
        return {"registry": _registry(ticker), "ensemble": _ensemble(ticker),
                "cycle": _cycle(ticker, NO_ACTORS if actors is None else actors, quote)}

    return _all


def _run_adapter(engine, at: datetime) -> int:
    """One run of the adapter as it is now; returns how many signals it registered."""
    return SignalRegistry.register(DollarFlowsAdapter()._build_signals(engine, at), engine)


def _old_13f_rows(engine, at: datetime, ticker: str) -> None:
    """What the previous adapter registered for a ticker held up by 13F alone:
    the no-ticker total from two runs a minute apart (one logical signal, two
    rows), which reaches every ticker, and the ticker's own."""
    SignalRegistry.register([
        _as_the_previous_adapter_registered(None, 7_022_705_633_749.0, at),
        _as_the_previous_adapter_registered(None, 3_847_160_143.0, at - timedelta(minutes=1)),
        _as_the_previous_adapter_registered(ticker, 2_283_782_911.0, at),
    ], engine)


def _other_flows(engine, ticker: str | None, kinds: tuple[str, ...], written: datetime) -> None:
    for kind in kinds:
        _write(engine, kind, 5_000_000.0, (written - timedelta(days=1)).date(), written, ticker=ticker)


def _directional(engine, ticker: str, n: int, direction: str = "bullish") -> None:
    """Synthetic directional registry rows under the subscribed source (a control)."""
    SignalRegistry.register([
        RegisteredSignal(
            signal_id=sid("dollar_flows", "synthetic", ticker, str(k), str(RUN.date())),
            source_module="dollar_flows", signal_type=SignalType.DIRECTIONAL, ticker=ticker,
            direction=direction, value=1.0, z_score=2.0 if direction == "bullish" else -2.0,
            confidence=0.8, valid_from=RUN, valid_until=RUN + timedelta(hours=4),
            freshness_hours=0.0, metadata={"synthetic": True}, provenance=f"synthetic:{k}",
        ) for k in range(n)
    ], engine)


NEUTRAL_VOTE = {"direction": "neutral", "strength": 0.0, "confidence": pytest.approx(0.5)}
ABSTAINED = None    # fewer than three signals: the model does not apply and the cycle writes no row


def _abstained_on_the_record(n_signals: int, *, qualified: bool = False) -> dict:
    """Three or more signals with no net side (here all neutral): the cycle
    writes a stated abstention (direction NONE, verdict no_data) that keeps the
    signals it saw, instead of no row (GRID#1510). With the fixture's usual
    unqualified quote it is a quote abstention: no kind of its own and the
    receipt's reason. With a qualified receipt it is ``no_direction``."""
    return {"direction": "NONE", "timestamp": PINNED, "signals": ["dollar_flows"] * n_signals,
            "verdict": "no_data",
            "abstention_kind": ABSTENTION_KIND_NO_DIRECTION if qualified else None,
            "reason": NO_DIRECTION_SCORE_NOTE if qualified else UNQUALIFIED_REASON}


# ── Before: 13F rows alone ─────────────────────────────────────────────────


def test_real_types_before_the_change_13f_rows_alone_vote_neutral_and_the_cycle_abstains(pg_engine, readers):
    _old_13f_rows(pg_engine, PINNED - timedelta(hours=1), "XOM")

    seen = readers("XOM")

    assert seen["registry"] == {"rows": 3, "distinct": 2}    # the no-ticker total twice
    assert seen["ensemble"] == {"votes": True, "signals": 3, **NEUTRAL_VOTE}
    assert seen["cycle"]["asked_families"] == []             # the registry was not empty
    assert seen["cycle"]["at_threshold"] == [3]              # through the cycle's minimum of three
    assert seen["cycle"]["prediction"] == _abstained_on_the_record(3)   # three neutral rows: no side to take


# ── After: nothing left for the ticker ─────────────────────────────────────


def test_real_types_an_empty_registry_sends_only_the_cycle_to_the_forecast_family_once(pg_engine, readers):
    _old_13f_rows(pg_engine, PINNED - timedelta(hours=5), "XOM")          # expired an hour ago
    _write_13f(pg_engine, 2_283_782_911.0, PINNED - timedelta(days=1), direction="observed",
               action="NEW", ticker="XOM", confidence="derived")
    assert _run_adapter(pg_engine, RUN) == 0                              # the adapter makes nothing of it

    seen = readers("XOM")

    assert seen["registry"] == {"rows": 0, "distinct": 0}
    assert seen["ensemble"] == {"votes": False}                           # the ensemble has no fallback
    assert seen["cycle"]["asked_families"] == [FAMILIES]                  # asked once, for its own families
    assert seen["cycle"]["at_threshold"] == [3]
    assert seen["cycle"]["prediction"] == {
        "direction": "CALL", "timestamp": PINNED,
        "signals": sorted(sig.name for sig in FORECAST),                  # the forecast, not the registry
    }


# ── After: one or two rows remain ──────────────────────────────────────────


@pytest.mark.parametrize("kinds", [("insider",), ("insider", "congressional")], ids=["one", "two"])
def test_real_types_one_or_two_surviving_rows_mean_no_vote_no_fallback_and_no_prediction(pg_engine, readers, kinds):
    _other_flows(pg_engine, "NVDA", kinds, PINNED - timedelta(days=1))
    assert _run_adapter(pg_engine, RUN) == len(kinds)

    seen = readers("NVDA")

    assert seen["registry"] == {"rows": len(kinds), "distinct": len(kinds)}
    assert seen["ensemble"] == {"votes": False}                           # under the model's minimum
    assert seen["cycle"]["asked_families"] == []                          # not empty, so no fallback
    assert seen["cycle"]["at_threshold"] == [len(kinds)]                  # under the cycle's three
    assert seen["cycle"]["prediction"] is ABSTAINED


# ── After: three rows, three ways ──────────────────────────────────────────


def test_real_types_three_distinct_rows_vote_the_same_with_and_without_13f(pg_engine, readers):
    _other_flows(pg_engine, "AAPL", ("insider", "congressional", "darkpool"), PINNED - timedelta(days=1))
    assert _run_adapter(pg_engine, RUN) == 3
    after = readers("AAPL")

    _old_13f_rows(pg_engine, PINNED - timedelta(hours=1), "AAPL")         # as it stood before the change
    before = readers("AAPL")

    assert (after["registry"], before["registry"]) == ({"rows": 3, "distinct": 3}, {"rows": 6, "distinct": 5})
    assert after["ensemble"] == {"votes": True, "signals": 3, **NEUTRAL_VOTE}
    assert before["ensemble"] == {"votes": True, "signals": 6, **NEUTRAL_VOTE}
    assert (after["cycle"]["at_threshold"], before["cycle"]["at_threshold"]) == ([3], [6])
    assert after["cycle"]["prediction"] == _abstained_on_the_record(3)
    assert before["cycle"]["prediction"] == _abstained_on_the_record(6)
    assert after["cycle"]["asked_families"] == before["cycle"]["asked_families"] == []


def test_real_types_two_signals_and_one_re_registered_copy_pass_like_three_distinct_signals(pg_engine, readers):
    """Two logical signals, one registered again by a later adapter run: three
    rows, two ids. Both readers count rows, so this passes the minimum of three
    exactly as three genuinely distinct signals do. Present behaviour on both
    sides of the change, pinned so a change to it is deliberate."""
    _other_flows(pg_engine, "AAPL", ("insider",), PINNED - timedelta(hours=2))
    _other_flows(pg_engine, "AAPL", ("congressional",), PINNED - timedelta(minutes=30))
    assert _run_adapter(pg_engine, PINNED - timedelta(hours=1)) == 1       # sees the insider flow only
    assert _run_adapter(pg_engine, RUN) == 2                                # sees both: insider again

    copied = readers("AAPL")

    _other_flows(pg_engine, "MSFT", ("insider", "congressional", "darkpool"), PINNED - timedelta(days=1))
    assert _run_adapter(pg_engine, RUN) == 3          # MSFT's three; AAPL's two are already stored at RUN
    distinct = readers("MSFT")

    assert copied["registry"] == {"rows": 3, "distinct": 2}
    assert distinct["registry"] == {"rows": 3, "distinct": 3}
    for seen in (copied, distinct):
        assert seen["ensemble"] == {"votes": True, "signals": 3, **NEUTRAL_VOTE}
        assert seen["cycle"]["at_threshold"] == [3]
        assert seen["cycle"]["prediction"] == _abstained_on_the_record(3)
        assert seen["cycle"]["asked_families"] == []


@pytest.mark.parametrize("kinds", [("insider", "congressional"), ("insider", "congressional", "darkpool")],
                         ids=["two_no_row", "three_no_direction"])
def test_real_types_with_a_qualified_quote_neutral_rows_abstain_as_no_direction_only_from_three(
        pg_engine, readers, kinds):
    """The same neutral rows with a qualified entry receipt: the abstention is
    ``no_direction`` with its own reason, so it is not mistaken for a quote
    abstention a fresh capture could cure. Under three signals there is still
    no row, and the registry being non-empty still means no family fallback."""
    _other_flows(pg_engine, "AAPL", kinds, PINNED - timedelta(days=1))
    assert _run_adapter(pg_engine, RUN) == len(kinds)

    seen = readers("AAPL", quote=_qualified_quote("AAPL"))

    assert seen["cycle"]["at_threshold"] == [len(kinds)]
    assert seen["cycle"]["asked_families"] == []
    if len(kinds) < 3:
        assert seen["cycle"]["prediction"] is ABSTAINED
    else:
        assert seen["cycle"]["prediction"] == _abstained_on_the_record(3, qualified=True)


# ── A row with no ticker ───────────────────────────────────────────────────


def test_real_types_a_surviving_row_with_no_ticker_reaches_every_ticker(pg_engine, readers):
    _other_flows(pg_engine, None, ("etf",), PINNED - timedelta(days=1))
    assert _run_adapter(pg_engine, RUN) == 1

    for ticker in ("XOM", "FLS"):
        seen = readers(ticker)
        assert seen["registry"] == {"rows": 1, "distinct": 1}
        assert seen["ensemble"] == {"votes": False}
        assert seen["cycle"]["asked_families"] == [] and seen["cycle"]["at_threshold"] == [1]
        assert seen["cycle"]["prediction"] is ABSTAINED


# ── Controls: both readers really vote ─────────────────────────────────────


def test_real_types_control_three_directional_rows_give_a_direction_on_both_paths(pg_engine, readers):
    _directional(pg_engine, "AAPL", 3, "bullish")
    bullish = readers("AAPL")
    _directional(pg_engine, "TSLA", 3, "bearish")
    bearish = readers("TSLA")

    assert bullish["ensemble"]["votes"] is True and bullish["ensemble"]["direction"] == "bullish"
    assert bullish["ensemble"]["signals"] == 3
    assert bullish["cycle"]["prediction"]["direction"] == "CALL"
    assert bullish["cycle"]["prediction"]["timestamp"] == PINNED
    assert bearish["ensemble"]["direction"] == "bearish"
    assert bearish["cycle"]["prediction"]["direction"] == "PUT"
    assert bullish["cycle"]["asked_families"] == bearish["cycle"]["asked_families"] == []


def test_real_types_control_the_actor_seam_can_lift_two_rows_over_the_cycles_minimum(pg_engine, readers):
    """The cycle adds qualified actor evidence before its own minimum; the
    ensemble has no such seam. Two directional rows and one synthetic actor
    row: the cycle predicts, the ensemble does not vote."""
    _directional(pg_engine, "AAPL", 2, "bullish")
    actor = {
        "actor": "Synthetic Fund", "signal_type": "insider", "direction": "bullish",
        "actor_trust": 0.9, "actor_influence": 0.9,
        "provenance": {"row_id": "synthetic-actor-1", "observed_at": RUN.isoformat()},
    }

    without = readers("AAPL")
    with_actor = readers("AAPL", {**NO_ACTORS, "signals": [actor]})

    assert without["ensemble"] == with_actor["ensemble"] == {"votes": False}
    assert without["cycle"]["at_threshold"] == [2] and without["cycle"]["prediction"] is ABSTAINED
    assert with_actor["cycle"]["at_threshold"] == [3]
    assert with_actor["cycle"]["prediction"]["direction"] == "CALL"
    assert "actor:Synthetic Fund" in with_actor["cycle"]["prediction"]["signals"]
