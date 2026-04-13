"""Tests for SYNTH Wave C handler wiring (SYNTH-34..42 + SYNTH-45 partial).

Covers:

- ``EdgeValidated`` contract construction + serialisation.
- ``contracts.handlers.edges.on_edge_validated`` downgrades cross_lens rows
  when an edge flips weak.
- ``contracts.handlers.trade_outcomes.on_options_trade_outcome`` updates the
  contagion model head weight via ``ModelRegistry.decay_model_by_source``.
- ``contracts.handlers.journal.on_signal_fired`` inserts a provisional row
  when ``|strength| > 0.7`` and skips when below.
- ``contracts.handlers.oracle_signals.on_signal_fired`` switch now handles
  all five wave-B/C signal types.
- Router integrity: new routes registered for EdgeValidated,
  OptionsTradeOutcome, and SignalFired has TWO handlers.
- Emit helpers in chain_contagion, news_contagion_listener,
  supply_chokepoints, supply_chain_edge_validator, contagion_to_ticket, and
  postmortem all call ``contracts.emit.emit`` without raising and never
  bubble exceptions up to the producer.
- Trust scorer integrity: every new signal key has a matching half-life +
  window.

All tests use ``mock_engine`` from ``conftest.py`` — no live DB calls.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from contracts.router import ROUTES, resolve_handler
from contracts.schemas import (
    ALL_CONTRACTS,
    EdgeValidated,
    OptionsTradeOutcome,
    SignalFired,
)


# ── 1. EdgeValidated contract ───────────────────────────────────────────


class TestEdgeValidatedContract:
    def test_construct_and_serialise(self):
        evt = EdgeValidated(
            producer_module="intelligence.supply_chain_edge_validator",
            correlation_id=uuid4(),
            edge_id=42,
            upstream_id="brent_crude",
            downstream_id="XOM",
            relationship="raw_material",
            validation_correlation=-0.62,
            weak_since=datetime(2026, 3, 1, tzinfo=timezone.utc),
            relationship_weak=True,
            implied_pct_cogs=0.14,
        )
        payload = evt.model_dump(mode="json")
        assert payload["edge_id"] == 42
        assert payload["relationship_weak"] is True
        assert payload["upstream_id"] == "brent_crude"

    def test_registered_in_all_contracts(self):
        assert EdgeValidated in ALL_CONTRACTS


# ── 2. Router integrity ─────────────────────────────────────────────────


class TestRouterIntegrity:
    def test_edge_validated_routed(self):
        assert EdgeValidated in ROUTES
        paths = ROUTES[EdgeValidated]
        assert "contracts.handlers.edges.on_edge_validated" in paths

    def test_options_trade_outcome_routed(self):
        assert OptionsTradeOutcome in ROUTES
        paths = ROUTES[OptionsTradeOutcome]
        assert "contracts.handlers.trade_outcomes.on_options_trade_outcome" in paths

    def test_signal_fired_has_two_handlers(self):
        assert SignalFired in ROUTES
        paths = ROUTES[SignalFired]
        assert "contracts.handlers.oracle_signals.on_signal_fired" in paths
        assert "contracts.handlers.journal.on_signal_fired" in paths
        assert len(paths) >= 2

    def test_all_new_routes_resolve(self):
        for contract_cls in (EdgeValidated, OptionsTradeOutcome, SignalFired):
            for path in ROUTES[contract_cls]:
                handler = resolve_handler(path)
                assert callable(handler), f"{path} is not callable"


# ── 3. edges.on_edge_validated ──────────────────────────────────────────


class TestEdgeValidatedHandler:
    def _evt(self, weak: bool, edge_id: int = 17) -> EdgeValidated:
        return EdgeValidated(
            producer_module="intelligence.supply_chain_edge_validator",
            correlation_id=uuid4(),
            edge_id=edge_id,
            upstream_id="brent_crude",
            downstream_id="XOM",
            relationship="raw_material",
            validation_correlation=0.05 if weak else 0.4,
            weak_since=datetime(2026, 3, 1, tzinfo=timezone.utc) if weak else None,
            relationship_weak=weak,
            implied_pct_cogs=0.10,
        )

    def test_downgrades_when_weak(self, mock_engine):
        from contracts.handlers import edges

        edges.on_edge_validated(self._evt(weak=True), engine=mock_engine)

        # begin() used (parameterized UPDATE)
        assert mock_engine.begin.called
        conn = mock_engine.begin.return_value.__enter__.return_value
        assert conn.execute.called
        args, kwargs = conn.execute.call_args
        params = args[1] if len(args) > 1 else kwargs
        assert params["eid"] == 17
        assert 0.0 < params["factor"] < 1.0

    def test_skips_when_strong(self, mock_engine):
        from contracts.handlers import edges

        edges.on_edge_validated(self._evt(weak=False), engine=mock_engine)

        # No DB write for healthy edges.
        assert not mock_engine.begin.called

    def test_swallows_db_errors(self, mock_engine):
        from contracts.handlers import edges

        conn = mock_engine.begin.return_value.__enter__.return_value
        conn.execute.side_effect = RuntimeError("boom")

        # Must not raise — dispatcher relies on that guarantee.
        edges.on_edge_validated(self._evt(weak=True), engine=mock_engine)


# ── 4. trade_outcomes.on_options_trade_outcome ──────────────────────────


class TestTradeOutcomeHandler:
    def _evt(self, pnl: float, strategy: str = "contagion_long") -> OptionsTradeOutcome:
        return OptionsTradeOutcome(
            producer_module="trading.contagion_to_ticket",
            correlation_id=uuid4(),
            trade_id=99,
            ticker="XOM",
            strategy=strategy,
            pnl=Decimal(str(pnl)),
            signal_mix={"contagion_ranked_impact": 1.0},
            hit_levels={"closed": True},
            duration_s=3600,
        )

    def test_hit_boosts_weight(self, mock_engine):
        from contracts.handlers import trade_outcomes

        with patch("oracle.engine.ModelRegistry") as MockRegistry:
            instance = MockRegistry.return_value
            instance.decay_model_by_source.return_value = 1

            trade_outcomes.on_options_trade_outcome(
                self._evt(pnl=125.0), engine=mock_engine,
            )

            assert instance.decay_model_by_source.called
            kwargs = instance.decay_model_by_source.call_args.kwargs
            assert kwargs["source"] == "contagion"
            assert kwargs["factor"] > 1.0

    def test_miss_decays_weight(self, mock_engine):
        from contracts.handlers import trade_outcomes

        with patch("oracle.engine.ModelRegistry") as MockRegistry:
            instance = MockRegistry.return_value
            instance.decay_model_by_source.return_value = 1

            trade_outcomes.on_options_trade_outcome(
                self._evt(pnl=-50.0), engine=mock_engine,
            )
            kwargs = instance.decay_model_by_source.call_args.kwargs
            assert kwargs["source"] == "contagion"
            assert kwargs["factor"] < 1.0

    def test_non_contagion_strategy_noop(self, mock_engine):
        from contracts.handlers import trade_outcomes

        with patch("oracle.engine.ModelRegistry") as MockRegistry:
            trade_outcomes.on_options_trade_outcome(
                self._evt(pnl=125.0, strategy="holder_overlap_long"),
                engine=mock_engine,
            )
            assert not MockRegistry.return_value.decay_model_by_source.called

    def test_swallows_exceptions(self, mock_engine):
        from contracts.handlers import trade_outcomes

        with patch("oracle.engine.ModelRegistry") as MockRegistry:
            MockRegistry.side_effect = RuntimeError("boom")
            # Must not raise.
            trade_outcomes.on_options_trade_outcome(
                self._evt(pnl=1.0), engine=mock_engine,
            )


# ── 5. journal.on_signal_fired ──────────────────────────────────────────


class TestJournalHandler:
    def _evt(self, strength: float) -> SignalFired:
        return SignalFired(
            producer_module="intelligence.chain_contagion",
            correlation_id=uuid4(),
            signal_id=uuid4(),
            source="chain_contagion:brent_crude",
            signal_type="contagion_ranked_impact",
            strength=strength,
            ticker="XOM",
            raw_row_ids=[],
        )

    def test_strong_signal_inserts_row(self, mock_engine):
        from contracts.handlers import journal

        conn = mock_engine.begin.return_value.__enter__.return_value
        # First execute = SELECT model_registry, second = INSERT
        select_result = MagicMock()
        select_result.fetchone.return_value = (7,)
        insert_result = MagicMock()
        conn.execute.side_effect = [select_result, insert_result]

        journal.on_signal_fired(self._evt(strength=0.9), engine=mock_engine)

        assert mock_engine.begin.called
        # Two execute calls: one SELECT + one INSERT.
        assert conn.execute.call_count == 2

    def test_weak_signal_skipped(self, mock_engine):
        from contracts.handlers import journal

        journal.on_signal_fired(self._evt(strength=0.4), engine=mock_engine)

        # Below threshold — no DB work at all.
        assert not mock_engine.begin.called

    def test_boundary_strength_skipped(self, mock_engine):
        from contracts.handlers import journal

        # Threshold is strict >, so 0.7 exactly is NOT inserted.
        journal.on_signal_fired(self._evt(strength=0.7), engine=mock_engine)
        assert not mock_engine.begin.called

    def test_swallows_db_errors(self, mock_engine):
        from contracts.handlers import journal

        conn = mock_engine.begin.return_value.__enter__.return_value
        conn.execute.side_effect = RuntimeError("boom")
        # Must not raise.
        journal.on_signal_fired(self._evt(strength=0.95), engine=mock_engine)

    def test_no_model_registry_skips_gracefully(self, mock_engine):
        from contracts.handlers import journal

        conn = mock_engine.begin.return_value.__enter__.return_value
        select_result = MagicMock()
        select_result.fetchone.return_value = None
        conn.execute.return_value = select_result
        conn.execute.side_effect = None

        # Should log + return without crashing.
        journal.on_signal_fired(self._evt(strength=0.95), engine=mock_engine)


# ── 6. oracle_signals handles all five signal types ────────────────────


class TestOracleSignalsSwitch:
    def _evt(self, signal_type: str, ticker: str = "XOM") -> SignalFired:
        return SignalFired(
            producer_module="intelligence.test",
            correlation_id=uuid4(),
            signal_id=uuid4(),
            source=f"test:{signal_type}",
            signal_type=signal_type,
            strength=0.6,
            ticker=ticker,
            raw_row_ids=[],
        )

    @pytest.mark.parametrize(
        "signal_type, expected_source_type",
        [
            ("holder_overlap", "holder_overlap"),
            ("fundamental_divergence", "fundamental_divergence"),
            ("contagion_ranked_impact", "contagion"),
            ("chokepoint_crossing", "chokepoint_crossing"),
            ("contagion_trigger", "news_trigger"),
        ],
    )
    def test_handler_routes_signal_type(
        self, signal_type, expected_source_type, mock_engine,
    ):
        from contracts.handlers import oracle_signals

        oracle_signals.on_signal_fired(
            self._evt(signal_type), engine=mock_engine,
        )

        assert mock_engine.begin.called
        conn = mock_engine.begin.return_value.__enter__.return_value
        args, kwargs = conn.execute.call_args
        params = args[1] if len(args) > 1 else kwargs
        assert params["st"] == expected_source_type

    def test_unknown_type_ignored(self, mock_engine):
        from contracts.handlers import oracle_signals

        oracle_signals.on_signal_fired(
            self._evt("not_a_real_signal"), engine=mock_engine,
        )
        assert not mock_engine.begin.called


# ── 7. Producer-side emit helpers are non-fatal ─────────────────────────


class TestProducerEmits:
    """Every emit helper touched by this wave must:

    - successfully call ``contracts.emit.emit`` on the happy path, and
    - swallow any exception raised by ``emit`` so the producer keeps running.
    """

    def _captured_monkeypatch(self, monkeypatch, raise_on_emit: bool = False):
        captured = []

        def _fake_emit(contract):
            if raise_on_emit:
                raise RuntimeError("bus down")
            captured.append(contract)
            return contract.event_id

        import sys

        _emit_mod = sys.modules.get("contracts.emit")
        if _emit_mod is None:
            import contracts.emit as _emit_mod  # noqa: F401
            _emit_mod = sys.modules["contracts.emit"]
        monkeypatch.setattr(_emit_mod, "emit", _fake_emit)
        return captured

    def test_chain_contagion_emits_per_victim(self, monkeypatch):
        from intelligence.chain_contagion import _emit_contagion_signals

        captured = self._captured_monkeypatch(monkeypatch)

        ranked = [
            {"id": "XOM", "margin_impact_pct": -0.12},
            {"id": "SHEL", "margin_impact_pct": -0.09},
            {"id": "BP", "margin_impact_pct": 0.0},  # skipped
        ]
        _emit_contagion_signals(ranked, "brent_crude")
        assert len(captured) == 2
        types = {c.signal_type for c in captured}
        assert types == {"contagion_ranked_impact"}

    def test_chain_contagion_emit_errors_are_non_fatal(self, monkeypatch):
        from intelligence.chain_contagion import _emit_contagion_signals

        self._captured_monkeypatch(monkeypatch, raise_on_emit=True)

        # Must not raise even though emit() throws.
        _emit_contagion_signals(
            [{"id": "XOM", "margin_impact_pct": -0.1}], "brent_crude",
        )

    def test_supply_chokepoints_emits_per_flipped_node(self, monkeypatch):
        from intelligence.supply_chokepoints import _emit_chokepoint_signals

        captured = self._captured_monkeypatch(monkeypatch)

        _emit_chokepoint_signals(
            ["taiwan_semis", "asml"],
            {"taiwan_semis": 0.85, "asml": 0.0},  # second skipped
        )
        assert len(captured) == 1
        assert captured[0].signal_type == "chokepoint_crossing"
        assert captured[0].ticker == "taiwan_semis"

    def test_supply_chokepoints_emit_errors_are_non_fatal(self, monkeypatch):
        from intelligence.supply_chokepoints import _emit_chokepoint_signals

        self._captured_monkeypatch(monkeypatch, raise_on_emit=True)

        _emit_chokepoint_signals(["x"], {"x": 0.9})

    def test_news_listener_emits_trigger(self, monkeypatch):
        from intelligence.news_contagion_listener import (
            Candidate,
            _emit_contagion_trigger,
        )

        captured = self._captured_monkeypatch(monkeypatch)

        cand = Candidate(
            news_id=42,
            url="https://x.test",
            title="Massive oil spill",
            pattern="oil_spill",
            shock_type="supply_disruption",
            magnitude=0.3,
            raw_entity="BP",
            resolved_node="bp",
        )
        _emit_contagion_trigger(cand, prediction_id=7)
        assert len(captured) == 1
        assert captured[0].signal_type == "contagion_trigger"
        assert captured[0].ticker == "bp"

    def test_news_listener_emit_errors_are_non_fatal(self, monkeypatch):
        from intelligence.news_contagion_listener import (
            Candidate,
            _emit_contagion_trigger,
        )

        self._captured_monkeypatch(monkeypatch, raise_on_emit=True)
        cand = Candidate(
            news_id=1,
            url="",
            title="x",
            pattern="p",
            shock_type="price_increase",
            magnitude=0.1,
            raw_entity="r",
            resolved_node="x",
        )
        _emit_contagion_trigger(cand, prediction_id=None)

    def test_edge_validator_emits(self, monkeypatch):
        from intelligence.supply_chain_edge_validator import (
            EdgeRow,
            _emit_edge_validated,
        )

        captured = self._captured_monkeypatch(monkeypatch)

        edge = EdgeRow(
            edge_id=11,
            upstream_id="brent_crude",
            downstream_id="XOM",
            weak_since=None,
            relationship_weak=False,
            relationship="raw_material",
            pct_downstream_cogs=0.14,
        )
        _emit_edge_validated(
            edge=edge,
            correlation=-0.6,
            new_weak_since=date(2026, 3, 1),
            new_weak_flag=True,
        )
        assert len(captured) == 1
        c = captured[0]
        assert c.edge_id == 11
        assert c.relationship_weak is True
        assert c.upstream_id == "brent_crude"

    def test_edge_validator_emit_errors_are_non_fatal(self, monkeypatch):
        from intelligence.supply_chain_edge_validator import (
            EdgeRow,
            _emit_edge_validated,
        )

        self._captured_monkeypatch(monkeypatch, raise_on_emit=True)

        edge = EdgeRow(
            edge_id=1,
            upstream_id="a",
            downstream_id="b",
            weak_since=None,
            relationship_weak=False,
            relationship="raw_material",
        )
        _emit_edge_validated(
            edge=edge,
            correlation=-0.5,
            new_weak_since=date(2026, 3, 1),
            new_weak_flag=True,
        )

    def test_contagion_to_ticket_finalize_emits(self, monkeypatch):
        from trading.contagion_to_ticket import finalize_ticket

        captured = self._captured_monkeypatch(monkeypatch)

        finalize_ticket(
            engine=MagicMock(),
            ticket_id=555,
            pnl=125.0,
            outcome="HIT",
            ticker="XOM",
            strategy="contagion_long",
            signals_used=["contagion_ranked_impact", "dealer_gamma"],
            duration_s=3600,
        )
        assert len(captured) == 1
        evt = captured[0]
        assert evt.trade_id == 555
        assert evt.strategy == "contagion_long"
        assert float(evt.pnl) == pytest.approx(125.0)

    def test_contagion_to_ticket_finalize_non_fatal(self, monkeypatch):
        from trading.contagion_to_ticket import finalize_ticket

        self._captured_monkeypatch(monkeypatch, raise_on_emit=True)

        # Must not raise.
        finalize_ticket(
            engine=MagicMock(),
            ticket_id="abc",
            pnl=-10.0,
            outcome="MISS",
            ticker="XOM",
        )

    def test_postmortem_emit_builds_contract(self, monkeypatch):
        from intelligence.postmortem import PostMortem, _emit_trade_postmortem

        captured = self._captured_monkeypatch(monkeypatch)

        pm = PostMortem(
            trade_id=77,
            ticker="XOM",
            direction="long",
            outcome="LOSS",
            actual_return=-0.12,
            data_at_decision={},
            thesis_at_decision="thesis",
            sanity_results_at_decision={},
            what_actually_happened="underlying dropped",
            price_path=[],
            failure_category="thesis_invalidated",
            root_cause="crude spiked",
            which_signals_were_wrong=["contagion"],
            which_signals_were_right=[],
            what_we_missed="hedge book",
            recommended_fix="tighten stops",
            confidence_in_analysis=0.8,
            generated_at=datetime(2026, 4, 1, tzinfo=timezone.utc).isoformat(),
        )
        _emit_trade_postmortem(pm=pm, trade_id=77)

        assert len(captured) == 1
        c = captured[0]
        assert c.ticker == "XOM"
        assert c.verdict == "MISS"
        assert str(c.root_cause).startswith("crude")

    def test_postmortem_emit_non_fatal(self, monkeypatch):
        from intelligence.postmortem import PostMortem, _emit_trade_postmortem

        self._captured_monkeypatch(monkeypatch, raise_on_emit=True)

        pm = PostMortem(
            trade_id=1,
            ticker="X",
            direction="long",
            outcome="LOSS",
            actual_return=0.0,
            data_at_decision={},
            thesis_at_decision="",
            sanity_results_at_decision={},
            what_actually_happened="",
            price_path=[],
            failure_category="",
            root_cause="",
            which_signals_were_wrong=[],
            which_signals_were_right=[],
            what_we_missed="",
            recommended_fix="",
            confidence_in_analysis=0.5,
            generated_at=datetime(2026, 4, 1, tzinfo=timezone.utc).isoformat(),
        )
        _emit_trade_postmortem(pm=pm, trade_id=1)


# ── 8. Trust scorer integrity (SYNTH-44 precursor, extended) ────────────


class TestTrustScorerIntegrity:
    def test_all_new_keys_have_three_entries(self):
        from intelligence.trust_scorer import (
            EVALUATION_WINDOWS,
            SIGNAL_HALF_LIFE_DAYS,
            SIGNAL_TRUST_DELTA,
        )

        new_keys = {"contagion", "news_trigger"}
        for key in new_keys:
            assert key in SIGNAL_TRUST_DELTA, f"{key} missing from SIGNAL_TRUST_DELTA"
            assert key in SIGNAL_HALF_LIFE_DAYS, f"{key} missing from SIGNAL_HALF_LIFE_DAYS"
            assert key in EVALUATION_WINDOWS, f"{key} missing from EVALUATION_WINDOWS"

    def test_global_integrity(self):
        """Every trust delta key must also have a half-life + window."""
        from intelligence.trust_scorer import (
            EVALUATION_WINDOWS,
            SIGNAL_HALF_LIFE_DAYS,
            SIGNAL_TRUST_DELTA,
        )
        for key in SIGNAL_TRUST_DELTA:
            assert key in SIGNAL_HALF_LIFE_DAYS, f"{key} missing half-life"
            assert key in EVALUATION_WINDOWS, f"{key} missing window"


# ── 9. oracle engine has the contagion head ───────────────────────────


class TestContagionModelHead:
    def test_contagion_model_present(self):
        from oracle.engine import DEFAULT_MODELS

        names = {m.name for m in DEFAULT_MODELS}
        assert "contagion" in names

    def test_contagion_families(self):
        from oracle.engine import DEFAULT_MODELS

        by_name = {m.name: m for m in DEFAULT_MODELS}
        fam = by_name["contagion"].signal_families
        assert "supply" in fam
        assert "macro" in fam
        assert "equity" in fam
