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
            # Answer the column the query asks for in the last slot: the
            # LEFT JOIN shape (op.model_name, any prediction row) or the bare
            # tp.prediction_id shape.
            joined = "op.model_name" in q
            by_id = {_pred_id(i, p): p for i, p in enumerate(self.scored_predictions)}

            def _tail(pm: dict[str, Any]) -> Any:
                if not joined:
                    return pm.get("prediction_id")
                if pm.get("model_name") is not None:
                    return pm["model_name"]
                pred = by_id.get(pm.get("prediction_id"))
                return pred.get("model_name") if pred else None

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
                    _tail(pm),
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

        # Eligible scored Oracle predictions: verdict IN (hit, miss, partial),
        # dedup_keep = TRUE, id NOT LIKE 'astrogrid:%' (the scored_at window is
        # not modelled; a test pins the SQL text for it instead).
        def _eligible(p: dict[str, Any]) -> bool:
            return (
                p.get("verdict") in ("hit", "miss", "partial")
                and p.get("dedup_keep", True)
                and not str(p.get("id", "")).startswith("astrogrid:")
            )

        if "select id, verdict, model_name, signals from oracle_predictions" in q:
            return FakeResult([
                (_pred_id(i, p), p["verdict"], p.get("model_name"), p.get("signals"))
                for i, p in enumerate(self.scored_predictions)
                if _eligible(p)
            ])

        # Pre-fix fire-counts shape (verdict, signals); kept so the new tests
        # can be shown failing against the pre-fix code.
        if "select verdict, signals from oracle_predictions" in q:
            return FakeResult([
                (p["verdict"], p.get("signals"))
                for p in self.scored_predictions
                if _eligible(p)
            ])

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


def _pred_id(index: int, pred: dict[str, Any]) -> str:
    """oracle_predictions.id for a fake row (explicit, or a stable default)."""
    return str(pred.get("id") or f"op-{index}")


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
        # Each postmortem belongs to one of the 6 scored misses (Codex HOLD
        # 6011905413: both sides come from the same prediction ids).
        db.postmortems = [
            {
                "ticker": f"T{i}",
                "outcome": "miss",
                "failure_category": "wrong_signal",
                "signals_wrong": ["feature:equity", "feature:sentiment"] if i < 5 else ["feature:equity"],
                "signals_right": [],
                "what_we_missed": "",
                "prediction_id": f"loss-{i}",
            }
            for i in range(6)
        ]
        wins = [
            {
                "id": f"win-{i}",
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
                "id": f"loss-{i}",
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
        assert wrong_patterns[0].affected_models == ["flow_momentum"]

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


# ── Codex HOLD 6011905413: Pattern 1 failures and fires from one population ──


def _trade_postmortem(i: int, signals_wrong: list[str]) -> dict[str, Any]:
    """A paper-trade postmortem: trade_id set, no Oracle prediction id."""
    return {
        "ticker": f"TR{i}",
        "outcome": "loss",
        "failure_category": "wrong_signal",
        "signals_wrong": list(signals_wrong),
        "signals_right": [],
        "what_we_missed": "",
        "prediction_id": None,
    }


def _oracle_postmortem(pid: str, signals_wrong: list[str]) -> dict[str, Any]:
    return {
        "ticker": "NVDA",
        "outcome": "miss",
        "failure_category": "wrong_signal",
        "signals_wrong": list(signals_wrong),
        "signals_right": [],
        "what_we_missed": "",
        "prediction_id": pid,
    }


def _scored(pid: str, verdict: str, signals: list[str], **extra: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": pid,
        "model_name": "flow_momentum",
        "verdict": verdict,
        "dedup_keep": True,
        "signals": {"items": [{"name": s} for s in signals]},
    }
    row.update(extra)
    return row


def _wrong_signal_patterns(db: FakeDB) -> list[FailurePattern]:
    patterns = TraceAnalyzer(db).analyze()  # type: ignore[arg-type]
    return [p for p in patterns if p.category == "wrong_signal"]


class TestCodexHold6011905413Pattern1Population:
    X = "feature:sentiment"

    def test_unrelated_postmortems_do_not_make_a_false_failure_rate(self):
        """Codex's example: two postmortems naming X that belong to no scored
        Oracle prediction, three Oracle X fires that did not fail. The old
        code read 2/3 = 67% (> 40%) and flagged X."""
        db = FakeDB()
        db.postmortems = (
            [_trade_postmortem(i, [self.X]) for i in range(2)]
            + [_trade_postmortem(i, []) for i in range(2, 5)]
        )
        db.scored_predictions = [_scored(f"hit-{i}", "hit", [self.X]) for i in range(3)]

        wrong = _wrong_signal_patterns(db)
        assert wrong == [], f"unrelated postmortems produced a failure rate: {wrong}"

    def test_postmortems_of_ineligible_predictions_do_not_count(self):
        """Postmortems of an AstroGrid row, a soft duplicate and a prediction
        scored outside the window are outside the denominator's population."""
        db = FakeDB()
        db.postmortems = [
            _oracle_postmortem("astrogrid:abc", [self.X]),
            _oracle_postmortem("dup-1", [self.X]),
            _oracle_postmortem("scored-before-window", [self.X]),
            _trade_postmortem(3, []),
            _trade_postmortem(4, []),
        ]
        db.scored_predictions = [
            _scored("astrogrid:abc", "miss", [self.X]),
            _scored("dup-1", "miss", [self.X], dedup_keep=False),
        ] + [_scored(f"hit-{i}", "hit", [self.X]) for i in range(3)]

        assert _wrong_signal_patterns(db) == []

    def test_repeated_postmortems_of_one_prediction_count_once(self):
        """Three postmortems of the same failed prediction are one failure
        per (prediction id, signal), not three."""
        db = FakeDB()
        db.postmortems = (
            [_oracle_postmortem("miss-1", [self.X]) for _ in range(3)]
            + [_trade_postmortem(i, []) for i in range(2)]
        )
        db.scored_predictions = (
            [_scored("miss-1", "miss", [self.X])]
            + [_scored(f"hit-{i}", "hit", [self.X]) for i in range(3)]
        )

        # Old code: 3 mentions / 4 fires = 75% -> flagged.
        assert _wrong_signal_patterns(db) == []

    def test_failure_counts_only_where_the_signal_fired(self):
        """A postmortem naming X for a prediction that never fired X is not a
        failure of X (the pair is outside the fire population)."""
        db = FakeDB()
        db.postmortems = (
            [_oracle_postmortem("m1", [self.X]), _oracle_postmortem("m2", [self.X])]
            + [_oracle_postmortem(f"m{i}", []) for i in range(3, 6)]
        )
        db.scored_predictions = (
            [_scored("m1", "miss", ["feature:equity"]), _scored("m2", "miss", [self.X])]
            + [_scored(f"m{i}", "miss", ["feature:equity"]) for i in range(3, 6)]
            + [_scored(f"hit-{i}", "hit", [self.X]) for i in range(2)]
        )

        # Old code: 2 mentions / 3 fires = 67% -> flagged. Now 1 / 3.
        assert _wrong_signal_patterns(db) == []

    def test_eligible_failures_give_the_rate_over_the_same_ids(self):
        """Positive control: three failed eligible X fires out of five, plus two
        unrelated postmortems naming X that must not add to the count."""
        db = FakeDB()
        db.postmortems = (
            [_oracle_postmortem(f"miss-{i}", [self.X]) for i in range(3)]
            + [_trade_postmortem(i, [self.X]) for i in range(2)]
        )
        db.scored_predictions = (
            [_scored(f"miss-{i}", "miss", [self.X]) for i in range(3)]
            + [_scored(f"hit-{i}", "hit", [self.X]) for i in range(2)]
        )

        wrong = _wrong_signal_patterns(db)
        assert len(wrong) == 1
        assert wrong[0].signals_commonly_wrong == [self.X]
        assert wrong[0].frequency == 3          # eligible failed predictions, not 5 postmortems
        assert wrong[0].fraction == pytest.approx(3 / 5, abs=1e-3)
        assert wrong[0].affected_models == ["flow_momentum"]

    def test_signal_failure_counts_join_on_prediction_id_and_signal(self):
        from oracle import trace_evolver as te

        eligible = {
            "miss-1": te.ScoredPrediction("miss-1", "miss", "m", frozenset({self.X})),
            "miss-2": te.ScoredPrediction("miss-2", "miss", "m", frozenset({"feature:equity"})),
            "hit-1": te.ScoredPrediction("hit-1", "hit", "m", frozenset({self.X})),
            "hit-2": te.ScoredPrediction("hit-2", "hit", "m", frozenset({self.X})),
        }
        postmortems = [
            {"prediction_id": "miss-1", "signals_wrong": [self.X, self.X]},
            {"prediction_id": "miss-1", "signals_wrong": [self.X]},       # re-run of the same failure
            {"prediction_id": "miss-2", "signals_wrong": [self.X]},       # X never fired here
            {"prediction_id": "hit-1", "signals_wrong": [self.X]},        # not a failure
            {"prediction_id": None, "signals_wrong": [self.X]},           # paper trade
            {"prediction_id": "not-eligible", "signals_wrong": [self.X]},
        ]

        counts = te._signal_failure_counts(postmortems, "signals_wrong", eligible)
        assert counts[self.X] == (1, 3)
        assert "feature:equity" not in counts or counts["feature:equity"][0] == 0

    def test_queries_pin_the_eligible_population(self):
        db = FakeDB()
        db.postmortems = [_oracle_postmortem(f"miss-{i}", [self.X]) for i in range(5)]
        db.scored_predictions = [_scored(f"miss-{i}", "miss", [self.X]) for i in range(5)] + [
            _scored("hit-1", "hit", [self.X])
        ]
        TraceAnalyzer(db).analyze()  # type: ignore[arg-type]

        norm = [(" ".join(sql.lower().split()), bound) for sql, bound in db.executed]
        eligible = [(q, b) for q, b in norm if q.startswith("select id, verdict, model_name, signals from oracle_predictions")]
        assert len(eligible) == 1, "expected one eligible scored-prediction read"
        q, bound = eligible[0]
        for needle in (
            "scored_at >= :cutoff",
            "verdict in ('hit', 'miss', 'partial')",
            "dedup_keep = true",
            "id not like 'astrogrid:%'",
        ):
            assert needle in q, f"eligible population filter missing: {needle}"
        assert "cutoff" in bound
        pm_queries = [q for q, _ in norm if "from trade_postmortems" in q]
        assert pm_queries and all("tp.prediction_id" in q for q in pm_queries)


# ── Open question for Anik: ModelEvolver still retires active models ──────


class _RosterDB:
    """Active roster for ModelEvolver.evolve_cycle: stats, count, deactivate."""

    def __init__(self, roster: dict[str, tuple[int, int, int]]) -> None:
        self.stats = dict(roster)  # name -> (hits, misses, partials)
        self.active = {name: True for name in roster}
        self.executed: list[tuple[str, dict[str, Any]]] = []
        self.writes: list[str] = []

    def connect(self) -> RecordingConn:
        return RecordingConn(self)  # type: ignore[arg-type]

    def begin(self) -> RecordingConn:
        return RecordingConn(self)  # type: ignore[arg-type]

    def handle(self, sql: str, params: dict[str, Any]) -> FakeResult:
        q = " ".join(sql.lower().split())
        if q.startswith(("update", "insert", "delete")):
            self.writes.append(q)
        if q.startswith("update oracle_models set active=false"):
            self.active[params["n"]] = False
            return FakeResult(rowcount=1)
        if "from oracle_models m left join" in q:
            return FakeResult([
                (name, "[]", "[]", h, m, p)
                for name, (h, m, p) in self.stats.items()
                if self.active[name]
            ])
        if q.startswith("select count(*) from oracle_models where active=true"):
            return FakeResult([(sum(self.active.values()),)])
        return FakeResult([])


class TestModelEvolverRosterPin:
    """Pins ModelEvolver.evolve_cycle's retirement of ACTIVE models as it is at
    this PR (Codex review 6011905413). Whether the cycle should do this to the
    existing roster is an open question for Anik; change it deliberately, and
    update these pins with it."""

    _DEACTIVATE = "update oracle_models set active=false, last_updated=now() where name=:n"

    def test_kill_rule_deactivates_active_models_below_20pct_after_20_scored(self):
        db = _RosterDB({
            "loser": (3, 17, 0),           # 20 scored, hit rate 15% -> retired
            "partial_heavy": (2, 8, 10),   # 20 scored, hit rate 10% (partials are not hits) -> retired
            "at_threshold": (4, 16, 0),    # hit rate exactly 20% -> kept (strict <)
            "young_loser": (0, 19, 0),     # 19 scored -> kept
            "winner": (12, 8, 0),
        })
        result = ModelEvolver(db).evolve_cycle()  # type: ignore[arg-type]

        assert sorted(result["killed"]) == ["loser", "partial_heavy"]
        assert {n for n, a in db.active.items() if not a} == {"loser", "partial_heavy"}
        updates = [w for w in db.writes if w.startswith("update")]
        assert updates == [self._DEACTIVATE] * 2   # only active=FALSE; weight untouched
        assert not any(w.startswith(("delete", "insert into oracle_models")) for w in db.writes)

    def test_cap_deactivates_lowest_scored_active_models_down_to_50(self):
        roster: dict[str, tuple[int, int, int]] = {
            f"strong_{i:02d}": (15, 4, 0) for i in range(48)
        }
        weak = {f"weak_{i}": (i + 1, 18 - i, 0) for i in range(5)}  # 19 scored: below the kill rule
        roster.update(weak)
        roster.update({"fresh_0": (0, 9, 0), "fresh_1": (0, 9, 0)})  # 9 scored: never ranked
        db = _RosterDB(roster)

        result = ModelEvolver(db).evolve_cycle()  # type: ignore[arg-type]

        assert result["active_before"] == 55
        assert sorted(result["killed"]) == sorted(weak)
        assert result["active_after"] == 50
        assert db.active["fresh_0"] and db.active["fresh_1"]

    def test_no_cap_deactivation_at_exactly_50_active(self):
        db = _RosterDB({f"strong_{i:02d}": (15, 4, 0) for i in range(50)})
        result = ModelEvolver(db).evolve_cycle()  # type: ignore[arg-type]

        assert result["killed"] == []
        assert not [w for w in db.writes if w.startswith("update")]


# ── The evolution cycle stays off outside OracleEngine.run_cycle ──────────


class TestEvolutionCycleHeld:
    ROOT = __import__("pathlib").Path(__file__).resolve().parents[1]
    _SKIP = {"tests", "migrations", "notebooks", "pwa", "node_modules", ".git", ".venv",
             "venv", "__pycache__", "site-packages"}

    def _tree(self, rel: str) -> Any:
        import ast

        return ast.parse((self.ROOT / rel).read_text(encoding="utf-8"))

    def test_hermes_oracle_slot_never_runs_the_evolvers(self):
        import ast

        tree = self._tree("scripts/hermes_operator.py")
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                assert node.module not in ("oracle.model_evolver", "oracle.trace_evolver")
            if isinstance(node, ast.Name):
                assert node.id not in ("ModelEvolver", "TraceEvolver")
        forward = [n for n in ast.walk(tree)
                   if isinstance(n, ast.FunctionDef) and n.name == "_run_oracle_forward_pass"]
        assert len(forward) == 1
        called = {n.func.attr for n in ast.walk(forward[0])
                  if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
        assert not called & {"run_cycle", "evolve_weights", "evolve_cycle"}

    def test_evolvers_are_constructed_only_inside_oracle_engine_run_cycle(self):
        import ast
        import warnings

        sites: list[tuple[str, str]] = []
        for path in self.ROOT.rglob("*.py"):
            rel = path.relative_to(self.ROOT)
            if self._SKIP.intersection(rel.parts):
                continue
            try:
                source = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            if "ModelEvolver(" not in source and "TraceEvolver(" not in source:
                continue
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SyntaxWarning)
                tree = ast.parse(source)

            def visit(node: ast.AST, enclosing: str) -> None:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    enclosing = node.name
                if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                        and node.func.id in ("ModelEvolver", "TraceEvolver")):
                    sites.append((rel.as_posix(), enclosing))
                for child in ast.iter_child_nodes(node):
                    visit(child, enclosing)

            visit(tree, "<module>")
        assert sorted(set(sites)) == [("oracle/engine.py", "run_cycle")]

    def test_surfacer_gap_worker_runs_the_oracle_predict_only(self):
        from scripts import drain_surfacer_oracle_gap_worker as worker

        assert "--predict-only" in worker._oracle_cycle_command("SPY")
