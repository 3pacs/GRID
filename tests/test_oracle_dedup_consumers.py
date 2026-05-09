"""Regression tests for soft-deduped oracle prediction consumers."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]


CRITICAL_SCORED_HISTORY_FILES = (
    "scripts/walk_forward_validate.py",
    "scripts/bootstrap_per_signal_brier.py",
    "intelligence/confidence_bucket_tracker.py",
    "intelligence/meta_learning_matrix.py",
    "intelligence/signal_cooccurrence.py",
    "features/regime_conditional_brier.py",
    "oracle/model_evolver.py",
    "oracle/trace_evolver.py",
)


def _scored_oracle_query_blocks(source: str) -> list[str]:
    blocks: list[str] = []
    for match in re.finditer(r"FROM oracle_predictions(?P<body>.*?)(?:ORDER BY|GROUP BY|LIMIT|\"\"\")", source, re.S):
        block = match.group(0)
        if re.search(r"verdict\s+IN\s*\(\s*'hit'\s*,\s*'miss'\s*,\s*'partial'\s*\)", block):
            blocks.append(block)
        elif "scored_at" in block and "verdict IS NOT NULL" in block:
            blocks.append(block)
        elif "verdict='hit'" in block and "verdict='miss'" in block and "verdict='partial'" in block:
            blocks.append(block)
        elif "verdict = 'hit'" in block and "verdict = 'miss'" in block and "verdict = 'partial'" in block:
            blocks.append(block)
    return blocks


def test_critical_scored_history_queries_ignore_soft_duplicates() -> None:
    missing: list[str] = []
    for relpath in CRITICAL_SCORED_HISTORY_FILES:
        source = (ROOT / relpath).read_text()
        blocks = _scored_oracle_query_blocks(source)
        assert blocks, f"{relpath} has no scored oracle history query blocks"
        for block in blocks:
            if "dedup_keep" not in block:
                missing.append(relpath)
                break

    assert missing == []


class _FakeResult:
    def fetchall(self) -> list[tuple[float, str]]:
        return [(0.8, "hit"), (0.2, "miss")]


class _FakeConn:
    def __init__(self) -> None:
        self.sql: str | None = None

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *args: Any) -> None:
        return None

    def execute(self, clause: Any, params: dict[str, Any]) -> _FakeResult:
        self.sql = str(clause)
        return _FakeResult()


class _FakeEngine:
    def __init__(self) -> None:
        self.conn = _FakeConn()

    def connect(self) -> _FakeConn:
        return self.conn


def test_compute_calibration_uses_kept_oracle_rows_only() -> None:
    from oracle.calibration import compute_calibration

    engine = _FakeEngine()

    compute_calibration(engine)  # type: ignore[arg-type]

    assert engine.conn.sql is not None
    assert "dedup_keep" in engine.conn.sql


def test_walk_forward_profitability_duplicate_count_uses_kept_rows_only() -> None:
    from scripts import walk_forward_profitability as wfp

    assert "dedup_keep" in str(wfp._DUP_COUNT_QUERY)
