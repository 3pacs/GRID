"""
Regression tests for verified defects in oracle/model_evolver.py and oracle/trace_evolver.py.

Defects covered:
1. Shadow stage: newly evolved models insert active=FALSE, weight=0.0, tagged
   [shadow] in description, and stay inactive until promoted by an operator.
2. Functional clones & shadow dedup: refuse creating any model whose
   vote-determining fields (signal_families, read by OracleEngine._load_models)
   are empty, unchanged from parent, or equal an existing active or [shadow]
   model's.
3. TraceAnalyzer Pattern 1: rate-based (failure mentions / overall fires across
   winning + losing outcomes); disabled with logged reason when only failures
   are available.
4. Prose matching on what_we_missed: exact case-insensitive whole-token match
   against full signal_registry names, minimum token length 5, stopword filter;
   no substring matching.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from oracle.model_evolver import ModelEvolver
from oracle.trace_evolver import (
    EvolutionGate,
    FailurePattern,
    MutationProposal,
    TargetedMutator,
    TraceAnalyzer,
)


class RecordingConn:
    def __init__(self, engine: "FakeDB") -> None:
        self._engine = engine

    def __enter__(self) -> "RecordingConn":
        return self

    def __exit__(self, *args: Any) -> None:
        return None

    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> "FakeResult":
        sql = stmt.text if hasattr(stmt, "text") else str(stmt)
        bound = dict(params or {})
        self._engine.executed.append((sql, bound))
        return self._engine.handle(sql, bound)


class FakeResult:
    def __init__(self, rows: list[tuple] | None = None, rowcount: int = 1) -> None:
        self._rows = list(rows or [])
        self.rowcount = rowcount

    def fetchall(self) -> list[tuple]:
        return list(self._rows)

    def fetchone(self) -> tuple | None:
        return self._rows[0] if self._rows else None


class FakeDB:
    """Minimal in-memory mock SQLAlchemy Engine for unit testing evolvers."""

    def __init__(self) -> None:
        self.models: dict[str, dict[str, Any]] = {}
        self.registry_sources: list[str] = [
            "feature:credit",
            "feature:equity",
            "feature:event_risk",
            "feature:flows",
            "feature:rates",
            "feature:sentiment",
            "feature:vol",
            "lever_pullers",
            "news_intel",
            "sector_network",
            "sleuth",
        ]
        self.postmortems: list[dict[str, Any]] = []
        self.scored_predictions: list[dict[str, Any]] = []
        self.signal_sources_outcomes: list[dict[str, Any]] = []
        self.hypotheses: list[tuple] = []
        self.executed: list[tuple[str, dict[str, Any]]] = []
        self.inserts: list[tuple[str, dict[str, Any]]] = []
        self.updates: list[tuple[str, dict[str, Any]]] = []
        self.fail_active_models_query: bool = False

    def connect(self) -> RecordingConn:
        return RecordingConn(self)

    def begin(self) -> RecordingConn:
        return RecordingConn(self)

    def handle(self, sql: str, params: dict[str, Any]) -> FakeResult:
        q = " ".join(sql.lower().split())

        if q.startswith("insert into oracle_models"):
            self.inserts.append((sql, params))
            # Parse active / weight literals or params
            is_shadow_false = "false" in q and "0.0" in q
            name = params["name"]
            self.models[name] = {
                "name": name,
                "description": params.get("desc", ""),
                "signal_families": params.get("fam", "[]"),
                "signal_sources": params.get("src", "[]"),
                "parent_model": params.get("parent"),
                "created_by": params.get("cb", ""),
                "target_horizon_days": params.get("horizon", 7),
                "weight": 0.0 if is_shadow_false else 1.0,
                "active": False if is_shadow_false else True,
            }
            return FakeResult(rowcount=1)

        if q.startswith("update oracle_models"):
            self.updates.append((sql, params))
            name = params.get("n")
            if name in self.models:
                if "active = true" in q or "active=true" in q:
                    self.models[name]["active"] = True
                if "active = false" in q or "active=false" in q:
                    self.models[name]["active"] = False
                if "w" in params:
                    self.models[name]["weight"] = params["w"]
                if "desc" in params:
                    self.models[name]["description"] = params["desc"]
                return FakeResult(rowcount=1)
            return FakeResult(rowcount=0)

        if "from signal_registry" in q:
            return FakeResult([(s,) for s in sorted(set(self.registry_sources))])

        if "from discovered_hypotheses" in q:
            return FakeResult(self.hypotheses)

        if "from trade_postmortems" in q:
            return FakeResult([
                (
                    pm["ticker"],
                    pm.get("outcome", "miss"),
                    pm["failure_category"],
                    pm.get("root_cause", ""),
                    json.dumps(pm.get("signals_wrong", [])),
                    json.dumps(pm.get("signals_right", [])),
                    pm.get("what_we_missed", ""),
                    pm.get("recommended_fix", ""),
                    pm.get("confidence", 0.7),
                    pm.get("generated_at", "2026-11-01"),
                    pm.get("model_name"),
                )
                for pm in self.postmortems
            ])

        if "from signal_sources" in q and "group by source_type" in q:
            agg: dict[str, dict[str, int]] = {}
            for row in self.signal_sources_outcomes:
                st = row["source_type"]
                agg.setdefault(st, {"c": 0, "w": 0, "t": 0})
                agg[st]["t"] += 1
                if row["outcome"] == "CORRECT":
                    agg[st]["c"] += 1
                elif row["outcome"] == "WRONG":
                    agg[st]["w"] += 1
            return FakeResult([(st, v["c"], v["w"], v["t"]) for st, v in agg.items()])

        # Signal fire-counts query from oracle_predictions (verdict, signals)
        if "select verdict, signals from oracle_predictions" in q:
            rows = []
            for p in self.scored_predictions:
                if p.get("verdict") in ("hit", "miss", "partial") and p.get("dedup_keep", True):
                    rows.append((p["verdict"], p.get("signals")))
            return FakeResult(rows)

        # Scoring data aggregation on oracle_predictions
        if "from oracle_predictions" in q and "group by model_name" in q:
            stats: dict[str, dict[str, Any]] = {}
            for p in self.scored_predictions:
                if p.get("verdict") not in ("hit", "miss", "partial"):
                    continue
                if not p.get("dedup_keep", True):
                    continue
                mn = p["model_name"]
                stats.setdefault(mn, {"hit": 0, "miss": 0, "partial": 0, "total": 0, "pnl": 0.0})
                stats[mn][p["verdict"]] += 1
                stats[mn]["total"] += 1
                stats[mn]["pnl"] += float(p.get("pnl_pct", 0.0))
            return FakeResult([
                (mn, s["hit"], s["miss"], s["partial"], s["total"], s["pnl"] / s["total"])
                for mn, s in stats.items()
            ])

        if "from oracle_models" in q and "count(*)" in q:
            n = sum(1 for m in self.models.values() if m.get("active") is True)
            return FakeResult([(n,)])

        if "from oracle_models" in q and "where name = :n" in q or "where name=:n" in q:
            m = self.models.get(params.get("n", ""))
            if not m:
                return FakeResult([])
            if "select 1 from oracle_models" in q:
                return FakeResult([(1,)])
            if "select active, signal_families from oracle_models" in q:
                return FakeResult([(m.get("active"), m.get("signal_families"))])
            if "select active from oracle_models" in q:
                return FakeResult([(m.get("active"),)])
            if "select signal_families from oracle_models" in q:
                return FakeResult([(m.get("signal_families"),)])
            if "target_horizon_days" in q:
                return FakeResult([(
                    m["name"], m["signal_sources"], m["signal_families"],
                    m["description"], m.get("parent_model"), m.get("target_horizon_days", 7),
                )])
            return FakeResult([(
                m["name"], m["signal_sources"], m["signal_families"],
                m["description"], m.get("parent_model"),
            )])

        if "from oracle_models" in q and ("active = true" in q or "active=true" in q or "active is true" in q):
            if self.fail_active_models_query:
                raise RuntimeError("simulated DB failure")
            include_shadow = "description like :shadow_prefix" in q
            shadow_prefix: str | None = None
            if include_shadow:
                raw_prefix = params.get("shadow_prefix")
                assert isinstance(raw_prefix, str) and raw_prefix.endswith("%"), (
                    f"Expected bound LIKE parameter ending in '%', got {raw_prefix!r}"
                )
                shadow_prefix = raw_prefix[:-1]

            def _matches(m: dict[str, Any]) -> bool:
                if m.get("active") is True:
                    return True
                if shadow_prefix is not None:
                    return str(m.get("description") or "").startswith(shadow_prefix)
                return False

            if "select signal_families" in q:
                return FakeResult([
                    (m["signal_families"],)
                    for m in self.models.values()
                    if _matches(m)
                ])
            if "select signal_sources" in q:
                return FakeResult([
                    (m["signal_sources"],)
                    for m in self.models.values()
                    if _matches(m)
                ])

        return FakeResult([])


def _seed_two_active_models(db: FakeDB) -> None:
    db.models["flow_momentum"] = {
        "name": "flow_momentum",
        "signal_sources": json.dumps(["feature:equity", "feature:flows", "feature:sentiment", "feature:vol"]),
        "signal_families": json.dumps(["equity", "flows"]),
        "description": "Flow momentum",
        "parent_model": None,
        "target_horizon_days": 7,
        "weight": 1.0,
        "active": True,
    }
    db.models["regime_contrarian"] = {
        "name": "regime_contrarian",
        "signal_sources": json.dumps(["feature:rates", "feature:credit", "feature:vol"]),
        "signal_families": json.dumps(["rates", "credit"]),
        "description": "Regime contrarian",
        "parent_model": None,
        "target_horizon_days": 7,
        "weight": 1.0,
        "active": True,
    }


# ── Defect 1: Shadow stage & shadow de-duplication ────────────────────────

class TestDefect1ShadowStage:
    def test_model_evolver_crossover_inserts_inactive_zero_weight_shadow(self):
        db = FakeDB()
        _seed_two_active_models(db)
        evolver = ModelEvolver(db)  # type: ignore[arg-type]

        name = evolver._crossover("flow_momentum", "regime_contrarian")
        assert name is not None
        assert len(db.inserts) == 1
        sql, params = db.inserts[0]
        sql_norm = " ".join(sql.lower().split())
        assert "0.0, false" in sql_norm, f"Expected weight 0.0 and active FALSE in INSERT: {sql}"
        assert "1.0, true" not in sql_norm
        assert "[shadow]" in params["desc"].lower()

    def test_trace_evolver_apply_inserts_inactive_zero_weight_shadow(self):
        db = FakeDB()
        _seed_two_active_models(db)
        mutator = TargetedMutator(db)  # type: ignore[arg-type]
        proposal = MutationProposal(
            parent_model="flow_momentum",
            mutation_type="crossover_targeted",
            description="Crossover flow_momentum with regime_contrarian",
            rationale="Underperforming model crossed with top performer",
            params={"donor_model": "regime_contrarian", "take_ratio": 0.5},
        )

        name = mutator.apply(proposal)
        assert name is not None
        assert len(db.inserts) == 1
        sql, params = db.inserts[0]
        sql_norm = " ".join(sql.lower().split())
        assert "0.0, false" in sql_norm, f"Expected weight 0.0 and active FALSE in INSERT: {sql}"
        assert "1.0, true" not in sql_norm
        assert "[shadow]" in params["desc"].lower()

    def test_model_evolver_same_crossover_twice_refuses_second(self):
        db = FakeDB()
        _seed_two_active_models(db)
        # An inactive non-shadow (retired) row with the same families must NOT block
        db.models["retired_union"] = {
            "name": "retired_union",
            "signal_sources": json.dumps(["news_intel", "sleuth"]),
            "signal_families": json.dumps(["credit", "equity", "flows", "rates"]),
            "description": "Retired loser union",
            "parent_model": None,
            "weight": 0.0,
            "active": False,
        }
        evolver = ModelEvolver(db)  # type: ignore[arg-type]

        first = evolver._crossover("flow_momentum", "regime_contrarian")
        assert first is not None
        assert len(db.inserts) == 1

        second = evolver._crossover("flow_momentum", "regime_contrarian")
        assert second is None
        assert len(db.inserts) == 1
        sig_queries = [
            (sql, bound)
            for sql, bound in db.executed
            if "select signal_families from oracle_models" in " ".join(sql.lower().split())
            and "description like :shadow_prefix" in " ".join(sql.lower().split())
        ]
        assert sig_queries, "Expected bound LIKE :shadow_prefix query for shadow dedup"
        assert all(bound.get("shadow_prefix") == "[shadow]%" for _, bound in sig_queries)

    def test_trace_evolver_same_crossover_twice_refuses_second(self):
        db = FakeDB()
        _seed_two_active_models(db)
        mutator = TargetedMutator(db)  # type: ignore[arg-type]
        gate = EvolutionGate(db)  # type: ignore[arg-type]
        proposal = MutationProposal(
            parent_model="flow_momentum",
            mutation_type="crossover_targeted",
            description="Crossover flow_momentum with regime_contrarian",
            rationale="Underperforming model crossed with top performer",
            params={"donor_model": "regime_contrarian", "take_ratio": 0.5},
        )
        parent_sources = ["feature:equity", "feature:flows", "feature:sentiment", "feature:vol"]

        passed_first, _ = gate.check(proposal, parent_sources)
        assert passed_first is True
        first = mutator.apply(proposal)
        assert first is not None
        assert len(db.inserts) == 1

        passed_second, reason = gate.check(proposal, parent_sources)
        assert passed_second is False
        assert "clone" in reason.lower() or "signal_families" in reason.lower()
        second = mutator.apply(proposal)
        assert second is None
        assert len(db.inserts) == 1


# ── Defect 2: Functional clones refused ───────────────────────────────────

class TestDefect2FunctionalClones:
    def test_model_evolver_mutate_refuses_unread_signal_sources_only_change(self):
        db = FakeDB()
        _seed_two_active_models(db)
        evolver = ModelEvolver(db)  # type: ignore[arg-type]

        created = evolver._mutate("flow_momentum")
        assert created is None
        assert db.inserts == []

    def test_model_evolver_crossover_refuses_duplicate_active_signal_families(self):
        db = FakeDB()
        _seed_two_active_models(db)
        # Add an active model whose signal_families equals flow_momentum U regime_contrarian
        db.models["already_combined"] = {
            "name": "already_combined",
            "signal_sources": json.dumps(["news_intel", "sleuth"]),
            "signal_families": json.dumps(["vol", "credit", "rates", "flows", "equity"])
            if False else json.dumps(["rates", "flows", "equity", "credit"]),  # different order
            "description": "Existing active union",
            "parent_model": None,
            "weight": 1.0,
            "active": True,
        }
        evolver = ModelEvolver(db)  # type: ignore[arg-type]

        created = evolver._crossover("flow_momentum", "regime_contrarian")
        assert created is None
        assert db.inserts == []

    def test_model_evolver_from_hypothesis_refuses_empty_signal_families(self):
        db = FakeDB()
        _seed_two_active_models(db)
        db.hypotheses = [
            ("hyp-001", "Rates lead equities", json.dumps([{"source": "feature:rates"}]), 0.8),
        ]
        evolver = ModelEvolver(db)  # type: ignore[arg-type]

        created = evolver._from_hypothesis()
        assert created is None
        assert db.inserts == []

    def test_trace_evolver_refuses_adjust_horizon_in_gate_and_apply(self):
        db = FakeDB()
        _seed_two_active_models(db)
        gate = EvolutionGate(db)  # type: ignore[arg-type]
        mutator = TargetedMutator(db)  # type: ignore[arg-type]
        proposal = MutationProposal(
            parent_model="flow_momentum",
            mutation_type="adjust_horizon",
            description="Extend horizon",
            rationale="Timing wrong",
            params={"horizon_multiplier": 1.5},
        )
        parent_sources = ["feature:equity", "feature:flows", "feature:sentiment", "feature:vol"]

        passed, reason = gate.check(proposal, parent_sources)
        assert passed is False
        assert "signal_families" in reason.lower() or "clone" in reason.lower()
        assert mutator.apply(proposal) is None
        assert db.inserts == []

    @pytest.mark.parametrize(
        "mutation_type,params",
        [
            ("remove_signal", {"signals_to_remove": ["feature:sentiment"]}),
            ("add_signal", {"signals_to_add": ["feature:event_risk"]}),
        ],
    )
    def test_trace_evolver_refuses_signal_sources_only_mutations(self, mutation_type, params):
        db = FakeDB()
        _seed_two_active_models(db)
        gate = EvolutionGate(db)  # type: ignore[arg-type]
        mutator = TargetedMutator(db)  # type: ignore[arg-type]
        proposal = MutationProposal(
            parent_model="flow_momentum",
            mutation_type=mutation_type,
            description=f"Test {mutation_type}",
            rationale="Unread signal_sources mutation",
            params=params,
        )
        parent_sources = ["feature:equity", "feature:flows", "feature:sentiment", "feature:vol"]

        passed, _ = gate.check(proposal, parent_sources)
        assert passed is False
        assert mutator.apply(proposal) is None
        assert db.inserts == []

    def test_trace_evolver_gate_refuses_crossover_clone_and_fails_closed_on_db_error(self):
        db = FakeDB()
        _seed_two_active_models(db)
        db.models["existing_union"] = {
            "name": "existing_union",
            "signal_sources": json.dumps(["news_intel", "sleuth"]),
            "signal_families": json.dumps(["credit", "equity", "flows", "rates"]),
            "description": "Existing active union",
            "parent_model": None,
            "target_horizon_days": 7,
            "weight": 1.0,
            "active": True,
        }
        gate = EvolutionGate(db)  # type: ignore[arg-type]
        proposal = MutationProposal(
            parent_model="flow_momentum",
            mutation_type="crossover_targeted",
            description="Cross flow_momentum x regime_contrarian",
            rationale="Underperforming",
            params={"donor_model": "regime_contrarian", "take_ratio": 0.5},
        )
        parent_sources = ["feature:equity", "feature:flows", "feature:sentiment", "feature:vol"]

        passed, reason = gate.check(proposal, parent_sources)
        assert passed is False
        assert "clone" in reason.lower() or "signal_families" in reason.lower()

        # Remove union model, then inject DB failure -> must fail closed
        del db.models["existing_union"]
        db.fail_active_models_query = True
        passed_err, _ = gate.check(proposal, parent_sources)
        assert passed_err is False


# ── Defect 3: Pattern 1 failure-only bias ─────────────────────────────────

class TestDefect3Pattern1BaseRate:
    def test_pattern1_disabled_when_only_failures_exist(self):
        db = FakeDB()
        db.postmortems = [
            {
                "ticker": f"T{i}",
                "outcome": "miss",
                "failure_category": "wrong_signal",
                "signals_wrong": ["feature:equity"],
                "signals_right": [],
                "what_we_missed": "",
                "model_name": "flow_momentum",
            }
            for i in range(6)
        ]
        # No winning outcomes in oracle_predictions (failure-only data)
        db.scored_predictions = [
            {
                "model_name": "flow_momentum",
                "verdict": "miss",
                "dedup_keep": True,
                "signals": {"items": [{"name": "feature:equity", "direction": "bullish"}]},
            }
            for _ in range(6)
        ]
        # Pattern 2 still has both CORRECT and WRONG in signal_sources
        db.signal_sources_outcomes = (
            [{"source_type": "social", "outcome": "WRONG"} for _ in range(8)]
            + [{"source_type": "social", "outcome": "CORRECT"} for _ in range(2)]
        )
        analyzer = TraceAnalyzer(db)  # type: ignore[arg-type]

        patterns = analyzer.analyze()
        p1 = [p for p in patterns if p.category == "wrong_signal"]
        p2 = [p for p in patterns if p.category == "chronic_bad_signal"]
        assert p1 == [], f"Pattern 1 must be disabled without winning+losing denominator, got {p1}"
        assert len(p2) == 1

    def test_pattern1_uses_failure_rate_not_raw_failure_count(self):
        db = FakeDB()
        # 6 failure postmortems:
        # - feature:equity mentioned in all 6 failures, BUT fired 20 times overall
        #   (14 wins + 6 misses -> 30% failure rate <= 40% threshold -> NOT flagged)
        # - feature:sentiment mentioned in 5 failures, fired 8 times overall
        #   (3 wins + 5 misses -> 62.5% failure rate > 40% threshold -> FLAGGED)
        db.postmortems = [
            {
                "ticker": f"T{i}",
                "outcome": "miss",
                "failure_category": "wrong_signal",
                "signals_wrong": ["feature:equity", "feature:sentiment"] if i < 5 else ["feature:equity"],
                "signals_right": [],
                "what_we_missed": "",
                "model_name": "flow_momentum",
            }
            for i in range(6)
        ]
        wins = [
            {
                "model_name": "flow_momentum",
                "verdict": "hit",
                "dedup_keep": True,
                "signals": {
                    "items": (
                        [{"name": "feature:equity"}, {"name": "feature:sentiment"}]
                        if i < 3
                        else [{"name": "feature:equity"}]
                    )
                },
            }
            for i in range(14)
        ]
        losses = [
            {
                "model_name": "flow_momentum",
                "verdict": "miss",
                "dedup_keep": True,
                "signals": {
                    "items": (
                        [{"name": "feature:equity"}, {"name": "feature:sentiment"}]
                        if i < 5
                        else [{"name": "feature:equity"}]
                    )
                },
            }
            for i in range(6)
        ]
        db.scored_predictions = wins + losses
        analyzer = TraceAnalyzer(db)  # type: ignore[arg-type]

        patterns = analyzer.analyze()
        wrong_patterns = [p for p in patterns if p.category == "wrong_signal"]
        assert len(wrong_patterns) == 1
        assert wrong_patterns[0].signals_commonly_wrong == ["feature:sentiment"]
        assert "feature:equity" not in wrong_patterns[0].signals_commonly_wrong
        assert wrong_patterns[0].fraction == pytest.approx(6 / 20, abs=1e-3)

    def test_aggregate_signals_fails_closed_without_denominator(self):
        db = FakeDB()
        analyzer = TraceAnalyzer(db)  # type: ignore[arg-type]
        pms = [{"signals_wrong": ["feature:sentiment"]} for _ in range(5)]
        assert analyzer._aggregate_signals(pms, "signals_wrong") == []


# ── Defect 4: Prose matching ──────────────────────────────────────────────

class TestDefect4ProseMatching:
    def test_aggregate_missed_matches_only_exact_registry_tokens(self):
        db = FakeDB()
        analyzer = TraceAnalyzer(db)  # type: ignore[arg-type]
        pms = [
            {
                "what_we_missed": (
                    "We missed that from work ever edit credit news; should have "
                    "included feature:event_risk and SLEUTH."
                )
            }
            for _ in range(3)
        ]

        missed = analyzer._aggregate_missed(pms)
        assert set(missed) == {"feature:event_risk", "sleuth"}
        for bad in ("that", "from", "work", "ever", "edit", "credit", "news", "should", "missed"):
            assert bad not in missed

    def test_targeted_mutator_does_not_substring_match_missed_words(self):
        db = FakeDB()
        _seed_two_active_models(db)
        mutator = TargetedMutator(db)  # type: ignore[arg-type]
        # "ever" is a substring of "lever_pullers"; "work" of "sector_network";
        # "edit" and "credit" of "feature:credit"; "news" of "news_intel".
        pattern = FailurePattern(
            category="model_error",
            frequency=5,
            fraction=0.25,
            signals_commonly_wrong=[],
            signals_commonly_right=[],
            signals_commonly_missed=[
                "that",
                "from",
                "ever",
                "work",
                "edit",
                "credit",
                "news",
                "should",
                "feature:event_risk",
            ],
            affected_models=["flow_momentum"],
            regime_context="NEUTRAL",
            recommended_action="test",
        )

        proposals = mutator.propose([pattern])
        add_proposals = [p for p in proposals if p.mutation_type == "add_signal"]
        assert len(add_proposals) == 1
        assert add_proposals[0].params["signals_to_add"] == ["feature:event_risk"]
