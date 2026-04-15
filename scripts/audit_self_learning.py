#!/usr/bin/env python3
"""audit_self_learning.py — who has a loop, who needs one, who doesn't need one?

Walks the module corpus and classifies each .py file into one of three
buckets so we can answer the question "all 739 modules should have
self-learning loops" with an actual plan rather than a vibe.

Buckets:

  * HAS_LOOP — already records → scores → updates → persists somewhere.
    Either imports intelligence.self_learning_loop, or has its own
    bespoke history table + update path (per_signal_brier,
    signal_cooccurrence, trust_scorer, etc.).

  * NEEDS_LOOP — has a scorable output (prediction, score, threshold,
    classification, weight, recommendation, ranking) but no evident
    feedback path. These are the gaps to plug.

  * NO_LOOP_NEEDED — pure utility / data pulling / schema / config. No
    scorable output, so no loop makes sense.

Classification heuristic (grepped, not semantic):

  - If the module imports ``intelligence.self_learning_loop`` or instantiates
    ``SelfLearningLoop`` → HAS_LOOP.
  - Else if it names a well-known legacy learning table (per_signal_brier_history,
    signal_cooccurrence_history, confidence_bucket_history, trust_scores,
    meta_learning_matrix, regime_conditional_brier_history,
    scanner_weights, forensic_journal, oracle_models_history) → HAS_LOOP.
  - Else if it has any function named predict / score / classify / rank /
    recommend / decide / compute_conviction / compute_weight / detect /
    calibrate / fit / update_weights OR a class named *Scorer / *Predictor
    / *Classifier / *Ranker / *Detector / *Model → NEEDS_LOOP.
  - Else → NO_LOOP_NEEDED.

Usage:

    python3 -m scripts.audit_self_learning

Prints counts per bucket plus a sample of NEEDS_LOOP modules so you can
target the highest-leverage ones first.
"""
from __future__ import annotations

import ast
import re
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

SCAN_DIRS = [
    "intelligence",
    "features",
    "discovery",
    "oracle",
    "physics",
    "analysis",
    "inference",
    "trading",
    "ingestion",
    "normalization",
    "store",
    "journal",
    "validation",
    "governance",
    "alerts",
    "agents",
    "api",
    "signals",
    "ingestors",
    "alpha_research",
    "scripts",
]

# Modules we already know have their own learning loops via dedicated
# history tables (not using the shared primitive, but still self-learning).
LEGACY_LEARNING_TABLES: set[str] = {
    "per_signal_brier_history",
    "signal_cooccurrence_history",
    "confidence_bucket_history",
    "trust_scores",
    "meta_learning_matrix",
    "regime_conditional_brier_history",
    "scanner_weights",
    "forensic_journal",
    "oracle_models_history",
    "thesis_postmortems",
    "source_accuracy",
    "model_registry",
    "decision_journal",
    "feature_importance_log",
    "conviction_drift",
    "options_mispricing_scans",
    "validation_results",
    "shadow_scores",
    "agent_runs",
    "hypothesis_registry",
}

# Method / function names that imply a scorable output.
SCORABLE_FUNCTION_NAMES: set[str] = {
    "predict",
    "score",
    "classify",
    "rank",
    "recommend",
    "decide",
    "detect",
    "compute_conviction",
    "compute_weight",
    "compute_score",
    "compute_confidence",
    "calibrate",
    "fit",
    "train",
    "update_weights",
    "compute_multiplier",
    "forecast",
    "project",
    "evaluate",
    "should_trade",
    "generate_ticket",
    "run_prediction",
    "infer",
}

# Class name SUFFIXES that imply a scorable-output producer.
SCORABLE_CLASS_SUFFIXES: tuple[str, ...] = (
    "Scorer",
    "Predictor",
    "Classifier",
    "Ranker",
    "Detector",
    "Recommender",
    "Model",
    "Ensemble",
    "Forecaster",
    "Estimator",
    "Tracker",
    "Analyzer",
    "Scanner",
    "Monitor",
    "Gate",
    "Filter",
    "Calibrator",
    "Selector",
)

# Substring blacklist: any module whose name contains these is treated as
# utility / pulling / config / test scaffolding and marked NO_LOOP_NEEDED
# regardless of what functions it defines.
UTILITY_NAME_PATTERNS: tuple[str, ...] = (
    "ingestion.altdata.",  # data pullers have no decisions to score
    "ingestion.",
    ".test_",
    "schemas.",
    "migrations.",
    "_pycache_",
    "config",
    "settings",
    "constants",
    "types",
    "helpers",
    "__init__",
)


def _scan_modules() -> list[Path]:
    out: list[Path] = []
    for d in SCAN_DIRS:
        root = REPO / d
        if not root.is_dir():
            continue
        for p in root.rglob("*.py"):
            if "__pycache__" in p.parts or "worktrees" in p.parts:
                continue
            if p.name == "__init__.py":
                continue
            out.append(p)
    return out


def _module_name(path: Path) -> str:
    return ".".join(path.relative_to(REPO).with_suffix("").parts)


def _is_utility(modname: str) -> bool:
    for pat in UTILITY_NAME_PATTERNS:
        if pat in modname:
            return True
    return False


def _classify(path: Path) -> tuple[str, list[str]]:
    """Return (bucket, evidence) for one file."""
    modname = _module_name(path)
    try:
        source = path.read_text(errors="ignore")
    except Exception:
        return ("NO_LOOP_NEEDED", ["unreadable"])

    evidence: list[str] = []

    # 1. Primitive user?
    if "self_learning_loop" in source or "SelfLearningLoop" in source:
        evidence.append("uses shared SelfLearningLoop")
        return ("HAS_LOOP", evidence)

    # 2. Legacy history table author?
    for tbl in LEGACY_LEARNING_TABLES:
        if tbl in source:
            evidence.append(f"touches {tbl}")
            return ("HAS_LOOP", evidence)

    # 3. Utility?
    if _is_utility(modname):
        evidence.append("utility / pulling / config path")
        return ("NO_LOOP_NEEDED", evidence)

    # 4. Does it have a scorable surface?
    try:
        tree = ast.parse(source)
    except Exception:
        return ("NO_LOOP_NEEDED", ["unparseable"])

    scorable_fns: list[str] = []
    scorable_classes: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name in SCORABLE_FUNCTION_NAMES:
                scorable_fns.append(node.name)
        elif isinstance(node, ast.ClassDef):
            for suffix in SCORABLE_CLASS_SUFFIXES:
                if node.name.endswith(suffix):
                    scorable_classes.append(node.name)
                    break

    if scorable_fns or scorable_classes:
        if scorable_fns:
            evidence.append(f"fn: {','.join(sorted(set(scorable_fns))[:3])}")
        if scorable_classes:
            evidence.append(f"cls: {','.join(sorted(set(scorable_classes))[:3])}")
        return ("NEEDS_LOOP", evidence)

    return ("NO_LOOP_NEEDED", ["no scorable surface"])


def main() -> int:
    paths = _scan_modules()
    by_bucket: dict[str, list[tuple[str, list[str]]]] = {
        "HAS_LOOP": [],
        "NEEDS_LOOP": [],
        "NO_LOOP_NEEDED": [],
    }
    dir_counts: dict[str, Counter] = {}
    for p in paths:
        bucket, ev = _classify(p)
        name = _module_name(p)
        by_bucket[bucket].append((name, ev))
        top_dir = name.split(".", 1)[0]
        dir_counts.setdefault(top_dir, Counter())[bucket] += 1

    n_total = len(paths)
    print("═" * 80)
    print("  GRID self-learning audit")
    print("═" * 80)
    print(f"\n  Total modules scanned: {n_total}")
    for bucket in ("HAS_LOOP", "NEEDS_LOOP", "NO_LOOP_NEEDED"):
        n = len(by_bucket[bucket])
        pct = 100.0 * n / max(n_total, 1)
        print(f"  {bucket:<18} {n:>4}   ({pct:5.1f}%)")

    print("\n— Per-directory breakdown —")
    print(f"  {'dir':<20}  {'HAS':>5}  {'NEEDS':>6}  {'NONE':>5}")
    for d in sorted(dir_counts):
        c = dir_counts[d]
        print(
            f"  {d:<20}  {c['HAS_LOOP']:>5}  "
            f"{c['NEEDS_LOOP']:>6}  {c['NO_LOOP_NEEDED']:>5}"
        )

    print("\n— Top 40 NEEDS_LOOP modules (target these first) —")
    for name, ev in sorted(by_bucket["NEEDS_LOOP"])[:40]:
        print(f"  ○ {name}  [{'; '.join(ev)}]")

    if len(by_bucket["NEEDS_LOOP"]) > 40:
        print(f"  ... +{len(by_bucket['NEEDS_LOOP']) - 40} more")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
