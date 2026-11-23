"""Guard: no new read of ``oracle_predictions`` that goes around the Schwab filter.

Since 2026-11-23 the Oracle engine reads Schwab-captured options rows (the
owner's private use, ``store.options_provenance.ORACLE_READS_SCHWAB``) and tags
what it builds from them in ``flow_context``. Those predictions reach admin
requests only. Every reader that serves people reads through
``VISIBLE_ORACLE_PREDICTIONS`` (or ``oracle_predictions_from(include_schwab)``
in a route that knows its viewer). That covers API routes, the MCP server, chat
and LLM context, postmortem narratives, and tables copied for serving.

This test scans every Python module outside ``tests/`` and ``migrations/`` for
SQL text that names ``oracle_predictions`` after ``FROM`` or ``JOIN``, and
freezes the reads that may stay raw, each with its reason. Counts may only go
down. Modules that serve people (``api/``, ``astrogrid_api/``,
``mcp_server.py``) may have none. A new raw read fails the build: route it
through the fragment, or add it here with a reason that says why no Schwab
value can leave through it.

Same scanner rules and limits as ``tests/test_options_raw_read_guard.py``:
string constants only (docstrings excluded). A table name assembled at run time
is not seen.
"""

from __future__ import annotations

import ast
import pathlib
import re
import warnings

REPO = pathlib.Path(__file__).resolve().parent.parent
SKIP_DIRS = {"tests", "migrations", "notebooks", "pwa", "node_modules", ".git", ".venv", "venv",
             "__pycache__", "site-packages"}
SANCTIONED = {"store/options_provenance.py"}
SERVING = ("api/", "astrogrid_api/", "mcp_server.py")
_GAP = r"(?:\s|/\*.*?\*/|--[^\n]*(?:\n|$))+"
_RAW_READ = re.compile(
    r"\b(?:FROM|JOIN)" + _GAP + r'(?:"?public"?\s*\.\s*)?"?oracle_predictions\b"?', re.I | re.S)

# Raw reads allowed on 2026-11-23. Lower or delete an entry when a file moves
# onto the fragment.
_AGGREGATE = "aggregate statistics only (counts, hit rates, Brier); no per-prediction value or price"
ALLOWED_RAW_READS: dict[str, tuple[int, str]] = {
    # The Oracle's own internals: the owner's private use, writing back to the
    # same rows or to per-model aggregates.
    "oracle/engine.py": (2, "the writer: dedup and scoring of its own rows"),
    "oracle/scoreboard.py": (4, "per-model scoreboard: " + _AGGREGATE),
    "oracle/calibration.py": (3, "per-model calibration: " + _AGGREGATE),
    "oracle/trace_evolver.py": (3, "weight evolution inside the Oracle; stores weights, not predictions"),
    "oracle/model_evolver.py": (1, "model evolution inside the Oracle; stores models, not predictions"),
    "oracle/evidence.py": (1, "per-model bootstrap evidence: " + _AGGREGATE),
    "oracle/publish.py": (1, "reads back the AstroGrid row it just inserted (never Schwab-built)"),
    "scripts/score_oracle_trades.py": (13, "the scorer: writes outcomes back onto the same rows"),
    "scripts/backfill_oracle_prediction_context.py": (1, "enriches the same rows in place"),
    "scripts/backfill_oracle_closes.py": (1, "ticker universe only (DISTINCT ticker)"),
    "ingestion/yfinance_pull.py": (1, "ticker universe only (DISTINCT ticker)"),
    # Aggregate analytics; their stored and served outputs carry no per-prediction value.
    "alpha_research/realized_alpha.py": (1, "the served route reads realized_alpha_daily: " + _AGGREGATE),
    "evaluation/edge_report.py": (2, _AGGREGATE),
    "features/regime_conditional_brier.py": (1, _AGGREGATE),
    "intelligence/confidence_bucket_tracker.py": (1, _AGGREGATE),
    "intelligence/confidence_calibration.py": (1, _AGGREGATE),
    "intelligence/hypothesis_engine.py": (2, "model convergence counts and outcome moves: " + _AGGREGATE),
    "intelligence/meta_learning_matrix.py": (1, _AGGREGATE),
    "intelligence/null_hypothesis_forecaster.py": (2, _AGGREGATE),
    "intelligence/prediction_market_arbitrage.py": (1, _AGGREGATE),
    "intelligence/signal_cooccurrence.py": (1, _AGGREGATE),
    "intelligence/source_quality_ablation.py": (1, _AGGREGATE),
    "scripts/backfill_surfacer_calibration.py": (2, "materialized per-ticker calibration: " + _AGGREGATE),
    "scripts/bootstrap_per_signal_brier.py": (3, "per-signal Brier seed (one count is argparse help text): "
                                                 + _AGGREGATE),
    "scripts/score_boost_log_with_local_llm.py": (1, "max/min/avg outcome move per hypothesis: " + _AGGREGATE),
    # Operator-run offline research reports, not served.
    "scripts/backtest_intelligence.py": (2, "operator-run offline backtest report"),
    "scripts/walk_forward_profitability.py": (3, "operator-run offline walk-forward report"),
    "scripts/walk_forward_validate.py": (2, "operator-run offline walk-forward report"),
}


def _docstring_ids(tree: ast.AST) -> set[int]:
    ids: set[int] = set()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if isinstance(body, list):
            for stmt in body:
                if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant) \
                        and isinstance(stmt.value.value, str):
                    ids.add(id(stmt.value))
    return ids


def raw_reads() -> dict[str, int]:
    found: dict[str, int] = {}
    for path in REPO.rglob("*.py"):
        rel = path.relative_to(REPO)
        if SKIP_DIRS.intersection(rel.parts) or rel.as_posix() in SANCTIONED:
            continue
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SyntaxWarning)
                tree = ast.parse(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        skip = _docstring_ids(tree)
        count = sum(len(_RAW_READ.findall(node.value)) for node in ast.walk(tree)
                    if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in skip)
        if count:
            found[rel.as_posix()] = count
    return found


def test_no_new_raw_oracle_prediction_reads() -> None:
    over = {path: n for path, n in raw_reads().items() if n > ALLOWED_RAW_READS.get(path, (0, ""))[0]}
    assert not over, (
        "Raw read of oracle_predictions (Schwab-built predictions would reach non-admin readers): "
        f"{over}. Read through VISIBLE_ORACLE_PREDICTIONS / oracle_predictions_from(include_schwab), "
        "or add an ALLOWED_RAW_READS entry saying why no Schwab value can leave through it."
    )


def test_allowed_entries_are_still_accurate() -> None:
    found = raw_reads()
    stale = {path: (n, found.get(path, 0)) for path, (n, _why) in ALLOWED_RAW_READS.items()
             if found.get(path, 0) != n}
    assert not stale, f"ALLOWED_RAW_READS out of date (allowed, found): {stale}; lower or delete the entry"


def test_serving_modules_have_no_allowed_raw_reads() -> None:
    serving = [path for path in ALLOWED_RAW_READS if path.startswith(SERVING)]
    assert not serving, f"modules that serve people may not read oracle_predictions raw: {serving}"
    assert all(why for _n, why in ALLOWED_RAW_READS.values())


def test_scanner_sees_aliased_quoted_and_commented_reads() -> None:
    for sql in ("SELECT 1 FROM oracle_predictions", "SELECT 1 FROM oracle_predictions p",
                'SELECT 1 JOIN "oracle_predictions" op', "SELECT 1 FROM /* x */ oracle_predictions",
                "SELECT 1 FROM -- x\n oracle_predictions", "select 1 from public.oracle_predictions"):
        assert _RAW_READ.search(sql), sql
    for sql in ("SELECT 1 FROM oracle_prediction_rescores", "INSERT INTO oracle_predictions VALUES (1)",
                "SELECT 1 FROM my_oracle_predictions",
                "SELECT 1 FROM (SELECT schwab_o.* FROM x schwab_o) oracle_predictions"):
        assert not _RAW_READ.search(sql), sql
