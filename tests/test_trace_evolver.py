"""
Tests for oracle/trace_evolver.py — trace-based self-evolution engine.

Tests the GEPA-inspired pipeline: trace analysis → targeted mutation → gating.
Uses in-memory SQLite-like mocking (parameterized queries only, no PG-specific).
"""

from __future__ import annotations

import json
import pytest
from dataclasses import asdict
from unittest.mock import MagicMock, patch, PropertyMock

from oracle.trace_evolver import (
    TraceAnalyzer, TargetedMutator, EvolutionGate, TraceEvolver,
    FailurePattern, MutationProposal, EvolutionCycleResult,
    _parse_json, MIN_SIGNAL_SOURCES, MAX_SIGNAL_SOURCES,
    MIN_POSTMORTEMS_FOR_ANALYSIS, MIN_SCORED_PREDICTIONS,
)


# ── Fixtures ──────────────────────────────────────────────────────────────

def _make_pattern(
    category: str = "wrong_signal",
    frequency: int = 5,
    fraction: float = 0.5,
    wrong: list[str] | None = None,
    right: list[str] | None = None,
    missed: list[str] | None = None,
    models: list[str] | None = None,
    regime: str | None = "GROWTH",
) -> FailurePattern:
    return FailurePattern(
        category=category,
        frequency=frequency,
        fraction=fraction,
        signals_commonly_wrong=wrong or ["feature:sentiment"],
        signals_commonly_right=right or ["feature:equity"],
        signals_commonly_missed=missed or [],
        affected_models=models or ["flow_momentum"],
        regime_context=regime,
        recommended_action="test action",
    )


def _make_proposal(
    parent: str = "flow_momentum",
    mutation_type: str = "remove_signal",
    params: dict | None = None,
) -> MutationProposal:
    return MutationProposal(
        parent_model=parent,
        mutation_type=mutation_type,
        description="Test mutation",
        rationale="Test rationale",
        params=params or {"signals_to_remove": ["feature:sentiment"]},
    )


class MockEngine:
    """Mock SQLAlchemy engine for testing without a database."""

    def __init__(self):
        self._models = {
            "flow_momentum": {
                "name": "flow_momentum",
                "signal_sources": json.dumps(["feature:equity", "feature:flows", "feature:sentiment", "feature:vol"]),
                "signal_families": json.dumps(["feature:equity", "feature:flows"]),
                "description": "Flow momentum model",
                "parent_model": None,
                "active": True,
                "target_horizon_days": 7,
            },
            "regime_contrarian": {
                "name": "regime_contrarian",
                "signal_sources": json.dumps(["feature:rates", "feature:credit", "feature:vol"]),
                "signal_families": json.dumps(["feature:rates", "feature:credit"]),
                "description": "Regime contrarian model",
                "parent_model": None,
                "active": True,
                "target_horizon_days": 7,
            },
        }
        self._postmortems = []
        self._predictions = []
        self._signal_sources = []
        self._journal = []
        self._active_count = len(self._models)

    def connect(self):
        return MockConnection(self)

    def begin(self):
        return MockConnection(self)


class MockConnection:
    def __init__(self, mock_engine: MockEngine):
        self._engine = mock_engine

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, stmt, params=None):
        query = str(stmt) if not hasattr(stmt, 'text') else stmt.text
        return MockResult(self._engine, query, params or {})


class MockResult:
    def __init__(self, engine: MockEngine, query: str, params: dict):
        self._engine = engine
        self._query = query.lower().strip()
        self._params = params

    def fetchall(self):
        if "trade_postmortems" in self._query and "select" in self._query:
            return [
                (pm["ticker"], pm["outcome"], pm["failure_category"],
                 pm["root_cause"], json.dumps(pm["signals_wrong"]),
                 json.dumps(pm["signals_right"]), pm["what_we_missed"],
                 pm["recommended_fix"], pm["confidence"], pm["generated_at"],
                 pm.get("model_name"))
                for pm in self._engine._postmortems
            ]
        if "oracle_predictions" in self._query and "having" in self._query:
            # Top performer query (_get_top_performer) — must be before scoring match
            return [("flow_momentum", 0.65)]
        if "oracle_predictions" in self._query and "group by model_name" in self._query:
            # Scoring data aggregation
            from collections import Counter
            model_stats = {}
            for p in self._engine._predictions:
                m = p["model_name"]
                if m not in model_stats:
                    model_stats[m] = {"hits": 0, "misses": 0, "partials": 0, "total": 0, "pnl": 0}
                model_stats[m]["total"] += 1
                model_stats[m][p["verdict"] + "s"] = model_stats[m].get(p["verdict"] + "s", 0) + 1
                model_stats[m]["pnl"] += p.get("pnl_pct", 0)
            return [
                (m, s["hits"], s["misses"], s["partials"], s["total"],
                 s["pnl"] / s["total"] if s["total"] else 0)
                for m, s in model_stats.items()
            ]
        if "signal_sources" in self._query and "group by source_type" in self._query:
            from collections import Counter
            stats = {}
            for ss in self._engine._signal_sources:
                st = ss["source_type"]
                if st not in stats:
                    stats[st] = {"correct": 0, "wrong": 0, "total": 0}
                stats[st]["total"] += 1
                if ss["outcome"] == "CORRECT":
                    stats[st]["correct"] += 1
                elif ss["outcome"] == "WRONG":
                    stats[st]["wrong"] += 1
            return [(st, s["correct"], s["wrong"], s["total"]) for st, s in stats.items()]
        if "decision_journal" in self._query:
            if self._engine._journal:
                return [(self._engine._journal[-1]["inferred_state"],)]
            return []
        if "oracle_models" in self._query and "count" in self._query:
            return [(self._engine._active_count,)]
        if "oracle_models" in self._query and "active" in self._query and "signal_sources" in self._query and "select" in self._query:
            return [
                (json.dumps(m["signal_sources"]) if isinstance(m["signal_sources"], list) else m["signal_sources"],)
                for m in self._engine._models.values() if m.get("active", True)
            ]
        if "oracle_models" in self._query and "name = :n" in self._query:
            name = self._params.get("n")
            m = self._engine._models.get(name)
            if m:
                return [(m["name"], m["signal_sources"], m["signal_families"],
                         m["description"], m["parent_model"], m.get("target_horizon_days", 7))]
            return []
        if "signal_registry" in self._query:
            return [("feature:equity",), ("feature:flows",), ("feature:sentiment",),
                    ("feature:vol",), ("feature:rates",), ("feature:credit",),
                    ("feature:macro",), ("feature:event_risk",)]
        return []

    def fetchone(self):
        rows = self.fetchall()
        return rows[0] if rows else None


# ── Test _parse_json ──────────────────────────────────────────────────────

class TestParseJson:
    def test_none(self):
        assert _parse_json(None) == []

    def test_list(self):
        assert _parse_json(["a", "b"]) == ["a", "b"]

    def test_json_string(self):
        assert _parse_json('["x", "y"]') == ["x", "y"]

    def test_invalid_json(self):
        assert _parse_json("not json") == []

    def test_non_list_json(self):
        assert _parse_json('{"a": 1}') == []

    def test_numeric_list(self):
        assert _parse_json([1, 2, 3]) == ["1", "2", "3"]


# ── Test FailurePattern ──────────────────────────────────────────────────

class TestFailurePattern:
    def test_to_dict(self):
        p = _make_pattern()
        d = p.to_dict()
        assert d["category"] == "wrong_signal"
        assert d["frequency"] == 5
        assert "feature:sentiment" in d["signals_commonly_wrong"]

    def test_frozen(self):
        p = _make_pattern()
        with pytest.raises(AttributeError):
            p.category = "bad_data"


# ── Test MutationProposal ────────────────────────────────────────────────

class TestMutationProposal:
    def test_to_dict(self):
        p = _make_proposal()
        d = p.to_dict()
        assert d["parent_model"] == "flow_momentum"
        assert d["mutation_type"] == "remove_signal"


# ── Test TraceAnalyzer ────────────────────────────────────────────────────

class TestTraceAnalyzer:
    def setup_method(self):
        self.engine = MockEngine()
        self.analyzer = TraceAnalyzer(self.engine)

    def test_returns_empty_when_insufficient_postmortems(self):
        """Should skip analysis when fewer than MIN_POSTMORTEMS_FOR_ANALYSIS."""
        self.engine._postmortems = [
            {"ticker": "AAPL", "outcome": "miss", "failure_category": "wrong_signal",
             "root_cause": "test", "signals_wrong": ["feature:sentiment"],
             "signals_right": ["feature:equity"], "what_we_missed": "vol",
             "recommended_fix": "fix", "confidence": 0.7, "generated_at": "2026-04-01",
             "model_name": "flow_momentum"}
        ]
        patterns = self.analyzer.analyze()
        assert patterns == []

    def test_identifies_failure_category_clustering(self):
        """Should find patterns when same failure category repeats."""
        self.engine._postmortems = [
            {"ticker": f"T{i}", "outcome": "miss", "failure_category": "wrong_signal",
             "root_cause": "test", "signals_wrong": ["feature:sentiment"],
             "signals_right": ["feature:equity"], "what_we_missed": "",
             "recommended_fix": "fix", "confidence": 0.7, "generated_at": "2026-04-01",
             "model_name": "flow_momentum"}
            for i in range(6)
        ]
        patterns = self.analyzer.analyze()
        assert len(patterns) >= 1
        assert patterns[0].category == "wrong_signal"
        assert patterns[0].frequency == 6
        assert "feature:sentiment" in patterns[0].signals_commonly_wrong

    def test_identifies_chronic_bad_signal(self):
        """Should flag signals with high error rate from trust_scorer."""
        self.engine._postmortems = [
            {"ticker": f"T{i}", "outcome": "miss", "failure_category": "wrong_signal",
             "root_cause": "test", "signals_wrong": [], "signals_right": [],
             "what_we_missed": "", "recommended_fix": "", "confidence": 0.5,
             "generated_at": "2026-04-01", "model_name": "flow_momentum"}
            for i in range(6)
        ]
        self.engine._signal_sources = [
            {"source_type": "social", "outcome": "WRONG"} for _ in range(8)
        ] + [
            {"source_type": "social", "outcome": "CORRECT"} for _ in range(2)
        ]
        patterns = self.analyzer.analyze()
        chronic = [p for p in patterns if p.category == "chronic_bad_signal"]
        assert len(chronic) >= 1
        assert "social" in chronic[0].signals_commonly_wrong

    def test_get_trace_summary(self):
        self.engine._postmortems = [
            {"ticker": "AAPL", "outcome": "miss", "failure_category": "wrong_signal",
             "root_cause": "test", "signals_wrong": [], "signals_right": [],
             "what_we_missed": "", "recommended_fix": "", "confidence": 0.5,
             "generated_at": "2026-04-01", "model_name": "flow_momentum"}
            for _ in range(3)
        ]
        summary = self.analyzer.get_trace_summary()
        assert summary["postmortems_analyzed"] == 3
        assert "wrong_signal" in summary["failure_distribution"]


# ── Test TargetedMutator ─────────────────────────────────────────────────

class TestTargetedMutator:
    def setup_method(self):
        self.engine = MockEngine()
        self.mutator = TargetedMutator(self.engine)

    def test_proposes_remove_for_wrong_signal(self):
        pattern = _make_pattern(
            category="wrong_signal",
            wrong=["feature:sentiment"],
            models=["flow_momentum"],
        )
        proposals = self.mutator.propose([pattern])
        assert len(proposals) >= 1
        remove_proposals = [p for p in proposals if p.mutation_type == "remove_signal"]
        assert len(remove_proposals) >= 1
        assert "feature:sentiment" in remove_proposals[0].params["signals_to_remove"]

    def test_proposes_horizon_for_timing(self):
        pattern = _make_pattern(
            category="right_signal_wrong_timing",
            models=["flow_momentum"],
        )
        proposals = self.mutator.propose([pattern])
        horizon_proposals = [p for p in proposals if p.mutation_type == "adjust_horizon"]
        assert len(horizon_proposals) >= 1
        assert horizon_proposals[0].params["horizon_multiplier"] == 1.5

    def test_proposes_add_for_external_shock(self):
        pattern = _make_pattern(
            category="external_shock",
            models=["flow_momentum"],
        )
        proposals = self.mutator.propose([pattern])
        add_proposals = [p for p in proposals if p.mutation_type == "add_signal"]
        assert len(add_proposals) >= 1
        assert "feature:event_risk" in add_proposals[0].params["signals_to_add"]

    def test_proposes_crossover_for_underperformance(self):
        pattern = _make_pattern(
            category="model_underperformance",
            models=["regime_contrarian"],
        )
        proposals = self.mutator.propose([pattern])
        cross_proposals = [p for p in proposals if p.mutation_type == "crossover_targeted"]
        assert len(cross_proposals) >= 1
        assert cross_proposals[0].params["donor_model"] == "flow_momentum"

    def test_respects_max_proposals(self):
        patterns = [_make_pattern(models=[f"model_{i}"]) for i in range(10)]
        proposals = self.mutator.propose(patterns, max_proposals=2)
        assert len(proposals) <= 2

    def test_apply_remove_signal(self):
        proposal = _make_proposal(
            parent="flow_momentum",
            mutation_type="remove_signal",
            params={"signals_to_remove": ["feature:sentiment"]},
        )
        name = self.mutator.apply(proposal)
        assert name is not None
        assert name.startswith("trace_remove_s")

    def test_apply_add_signal(self):
        proposal = _make_proposal(
            parent="flow_momentum",
            mutation_type="add_signal",
            params={"signals_to_add": ["feature:event_risk"]},
        )
        name = self.mutator.apply(proposal)
        assert name is not None
        assert name.startswith("trace_add_sign")

    def test_apply_adjust_horizon(self):
        proposal = _make_proposal(
            parent="flow_momentum",
            mutation_type="adjust_horizon",
            params={"horizon_multiplier": 1.5},
        )
        name = self.mutator.apply(proposal)
        assert name is not None

    def test_apply_returns_none_for_missing_parent(self):
        proposal = _make_proposal(parent="nonexistent_model")
        assert self.mutator.apply(proposal) is None

    def test_apply_remove_respects_min_sources(self):
        """Removing too many signals should return None."""
        # regime_contrarian has only 3 sources, removing 2 would leave 1
        proposal = _make_proposal(
            parent="regime_contrarian",
            mutation_type="remove_signal",
            params={"signals_to_remove": ["feature:rates", "feature:credit"]},
        )
        result = self.mutator.apply(proposal)
        assert result is None


# ── Test EvolutionGate ────────────────────────────────────────────────────

class TestEvolutionGate:
    def setup_method(self):
        self.engine = MockEngine()
        self.gate = EvolutionGate(self.engine)

    def test_passes_valid_remove(self):
        proposal = _make_proposal(
            parent="flow_momentum",
            mutation_type="remove_signal",
            params={"signals_to_remove": ["feature:sentiment"]},
        )
        parent_sources = ["feature:equity", "feature:flows", "feature:sentiment", "feature:vol"]
        passed, reason = self.gate.check(proposal, parent_sources)
        assert passed
        assert reason == "passed"

    def test_rejects_below_min_signals(self):
        proposal = _make_proposal(
            parent="regime_contrarian",
            mutation_type="remove_signal",
            params={"signals_to_remove": ["feature:rates", "feature:credit"]},
        )
        parent_sources = ["feature:rates", "feature:credit", "feature:vol"]
        passed, reason = self.gate.check(proposal, parent_sources)
        assert not passed
        assert "min" in reason.lower() or "signals" in reason.lower()

    def test_rejects_missing_parent(self):
        proposal = _make_proposal(parent="nonexistent")
        passed, reason = self.gate.check(proposal, [])
        assert not passed
        assert "not found" in reason.lower()

    def test_rejects_at_population_cap(self):
        self.engine._active_count = 50
        proposal = _make_proposal(parent="flow_momentum")
        parent_sources = ["feature:equity", "feature:flows", "feature:sentiment", "feature:vol"]
        passed, reason = self.gate.check(proposal, parent_sources)
        assert not passed
        assert "cap" in reason.lower()


# ── Test TraceEvolver (Integration) ──────────────────────────────────────

class TestTraceEvolver:
    def setup_method(self):
        self.engine = MockEngine()
        self.evolver = TraceEvolver(self.engine)

    def test_returns_empty_when_no_patterns(self):
        result = self.evolver.evolve_cycle()
        assert result["patterns_found"] == []
        assert result["mutations_applied"] == []

    def test_full_cycle_with_patterns(self):
        self.engine._postmortems = [
            {"ticker": f"T{i}", "outcome": "miss", "failure_category": "wrong_signal",
             "root_cause": "sentiment was wrong", "signals_wrong": ["feature:sentiment"],
             "signals_right": ["feature:equity"], "what_we_missed": "",
             "recommended_fix": "remove sentiment", "confidence": 0.8,
             "generated_at": "2026-04-01", "model_name": "flow_momentum"}
            for i in range(6)
        ]
        result = self.evolver.evolve_cycle()
        assert len(result["patterns_found"]) >= 1
        assert len(result["mutations_proposed"]) >= 1
        # Mutations may be applied or rejected depending on gate checks
        total_outcomes = len(result["mutations_applied"]) + len(result["mutations_rejected"])
        assert total_outcomes >= 1

    def test_cycle_result_has_trace_summary(self):
        self.engine._postmortems = [
            {"ticker": f"T{i}", "outcome": "miss", "failure_category": "wrong_signal",
             "root_cause": "test", "signals_wrong": [], "signals_right": [],
             "what_we_missed": "", "recommended_fix": "", "confidence": 0.5,
             "generated_at": "2026-04-01", "model_name": "flow_momentum"}
            for i in range(6)
        ]
        result = self.evolver.evolve_cycle()
        assert "trace_summary" in result
        assert result["trace_summary"]["postmortems_analyzed"] == 6


# ── Test EvolutionCycleResult ─────────────────────────────────────────────

class TestEvolutionCycleResult:
    def test_to_dict(self):
        r = EvolutionCycleResult()
        r.mutations_applied.append("test_model")
        d = r.to_dict()
        assert d["mutations_applied"] == ["test_model"]
        assert d["errors"] == []
