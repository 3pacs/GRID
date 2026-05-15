"""One-shot operational script: reset misclassified ``signal_sources``
rows so the post-#119 ``score_pending_signals`` can re-evaluate them.

Background
----------

Pre-2026-05-13 ``intelligence.trust_scorer.score_pending_signals`` only
recognised literal ``"BUY"`` / ``"SELL"`` signal_types and defaulted
every other type to ``WRONG`` regardless of price action. That produced
the 99.3% WRONG inflation (167,135 WRONG / 1,105 CORRECT) flagged in
the 2026-05-11 backtest. PRs #119 (batch scorer) and #120 (contract-
driven scorer) made both writers direction-aware, but they only affect
**future** scoring — the 167K historical WRONGs stay WRONG.

This script resets those rows to PENDING so the new logic re-scores
them against actual historical prices. Two modes:

  --all-wrong (default, "option B")
      Reset every WRONG row, including BUY/SELL that may already be
      correct. The pre-fix BUY/SELL classifier was right, so those
      rows re-score identically. Cleaner, no judgment calls.

  --surgical ("option A")
      Reset only WRONG rows where signal_type is NOT in ('BUY', 'SELL').
      Preserves the ~1K BUY/SELL outcomes that were already legit.

Dry-run by default — prints what WOULD happen. ``--commit`` makes it
real. ``--rescore`` chains the reset into an immediate
``score_pending_signals`` call (otherwise the operator runs that
separately on their own cadence — recommended for very large resets,
which can take hours of price lookups).
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from loguru import logger as log
from sqlalchemy import text

from db import get_engine


# Statement timeout for the reset UPDATE — large but not unbounded.
_RESET_STATEMENT_TIMEOUT = "10min"


def _count_targets(conn, *, surgical: bool) -> dict[str, int]:
    """Return how many rows the reset WOULD touch, broken out by signal_type."""
    where = "outcome = 'WRONG'"
    if surgical:
        where += " AND signal_type NOT IN ('BUY', 'SELL')"

    total_row = conn.execute(text(f"SELECT COUNT(*) FROM signal_sources WHERE {where}")).fetchone()
    total = int(total_row[0] or 0) if total_row else 0

    by_type = conn.execute(text(f"""
        SELECT signal_type, COUNT(*) AS n
        FROM signal_sources
        WHERE {where}
        GROUP BY signal_type
        ORDER BY n DESC
    """)).fetchall()

    return {
        "total": total,
        "by_signal_type": [(row[0], int(row[1])) for row in by_type],
    }


def _baseline_outcome_distribution(conn) -> dict[str, int]:
    rows = conn.execute(text("""
        SELECT COALESCE(outcome, 'NULL') AS o, COUNT(*) FROM signal_sources GROUP BY 1
    """)).fetchall()
    return {str(r[0]): int(r[1]) for r in rows}


def _do_reset(conn, *, surgical: bool) -> int:
    where = "outcome = 'WRONG'"
    if surgical:
        where += " AND signal_type NOT IN ('BUY', 'SELL')"

    # Local statement timeout so a runaway query can't lock the table.
    conn.execute(text(f"SET LOCAL statement_timeout = '{_RESET_STATEMENT_TIMEOUT}'"))
    result = conn.execute(text(f"""
        UPDATE signal_sources
        SET outcome = NULL,
            outcome_return = NULL,
            scored_at = NULL
        WHERE {where}
    """))
    return int(result.rowcount or 0)


def _print_plan(targets: dict[str, int], baseline: dict[str, int], *, surgical: bool) -> None:
    mode = "surgical (non-BUY/SELL only)" if surgical else "all-wrong"
    total = targets["total"]
    by_type = targets["by_signal_type"]

    print(f"Mode: {mode}")
    print(f"Rows to reset: {total:,}")
    print()
    print(f"Pre-reset outcome distribution:")
    for outcome, count in sorted(baseline.items(), key=lambda kv: -kv[1]):
        print(f"  {outcome:>10s}: {count:>9,}")
    print()
    print(f"Top signal_types in the reset set:")
    for stype, n in by_type[:15]:
        print(f"  {stype:>30s}: {n:>8,}")
    if len(by_type) > 15:
        rest = sum(n for _, n in by_type[15:])
        print(f"  {'(' + str(len(by_type) - 15) + ' more)':>30s}: {rest:>8,}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    mode_grp = ap.add_mutually_exclusive_group()
    mode_grp.add_argument(
        "--all-wrong",
        dest="surgical",
        action="store_false",
        default=False,
        help="Reset every WRONG row, including BUY/SELL (default; 'option B').",
    )
    mode_grp.add_argument(
        "--surgical",
        dest="surgical",
        action="store_true",
        help="Reset only WRONG rows whose signal_type is NOT in ('BUY','SELL') ('option A').",
    )
    ap.add_argument(
        "--commit",
        action="store_true",
        help="Actually perform the reset. Without this, run dry.",
    )
    ap.add_argument(
        "--rescore",
        action="store_true",
        help=(
            "After reset, immediately call score_pending_signals. "
            "Slow for large resets (hours of per-row price lookups) — "
            "by default we just reset and let the regular cron run pick up."
        ),
    )
    args = ap.parse_args(argv)

    started = datetime.now(timezone.utc)
    engine = get_engine()

    with engine.connect() as conn:
        baseline = _baseline_outcome_distribution(conn)
        targets = _count_targets(conn, surgical=args.surgical)

    if targets["total"] == 0:
        print("No rows match — nothing to do.")
        return 0

    _print_plan(targets, baseline, surgical=args.surgical)
    print()

    if not args.commit:
        print("Dry-run. Re-run with --commit to perform the reset.")
        return 0

    print("Committing reset…")
    with engine.begin() as conn:
        affected = _do_reset(conn, surgical=args.surgical)
    print(f"  reset {affected:,} rows in {(datetime.now(timezone.utc) - started).total_seconds():.1f}s")

    if not args.rescore:
        print()
        print("Reset complete. Next steps (run separately on operator schedule):")
        print("  python -c 'from db import get_engine; from intelligence.trust_scorer import score_pending_signals; "
              "print(score_pending_signals(get_engine()))'")
        return 0

    # Chained rescore — only do this when the operator asked.
    print()
    print("Chaining score_pending_signals (this can run for hours on large resets)…")
    from intelligence.trust_scorer import score_pending_signals
    summary = score_pending_signals(engine)
    print(f"  score_pending_signals: {summary}")

    with engine.connect() as conn:
        after = _baseline_outcome_distribution(conn)
    print()
    print("Post-rescore outcome distribution:")
    for outcome in sorted(set(baseline) | set(after)):
        before_n = baseline.get(outcome, 0)
        after_n = after.get(outcome, 0)
        delta = after_n - before_n
        sign = "+" if delta >= 0 else ""
        print(f"  {outcome:>10s}: {before_n:>9,} → {after_n:>9,}  ({sign}{delta:,})")

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print()
    print(f"Total elapsed: {elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
