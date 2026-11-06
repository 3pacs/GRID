#!/usr/bin/env python3
"""Forward edge report and trust-score before/after (S16). Read-only.

Subcommands::

    # Edge report (prune_rule_v1): markdown + JSON + candidate prune list
    python scripts/edge_report.py report --out docs/research/edge-report-forward-20261106.md \\
        --json-out /tmp/edge.json --candidate-out /tmp/prune_candidate.json

    # Trust scores under the legacy vs corrected outcome basis
    python scripts/edge_report.py trust-diff --top 25 --json-out /tmp/trust_diff.json

Every connection is opened with ``default_transaction_read_only=on`` and a
statement timeout, so the script cannot write. The candidate prune list does
nothing until an operator copies it to ``GRID_TRUST_PRUNE_LIST_PATH`` and sets
``GRID_TRUST_PRUNE_NO_EDGE=true``. See
``docs/research/prune-rule-v1-preregistration.md``.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from loguru import logger as log
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool


def read_only_engine(db_url: str | None, timeout_s: int) -> Engine:
    """A NullPool engine whose every transaction is READ ONLY and time-bounded."""
    if db_url is None:
        from config import settings

        db_url = settings.DB_URL
    options = f"-c default_transaction_read_only=on -c statement_timeout={int(timeout_s) * 1000}"
    return create_engine(db_url, poolclass=NullPool, connect_args={"options": options})


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db-url", default=None, help="Override settings.DB_URL (the session is read-only regardless)")
    p.add_argument("--timeout-s", type=int, default=900, help="Per-statement timeout in seconds")
    sub = p.add_subparsers(dest="command", required=True)

    r = sub.add_parser("report", help="Forward edge report (prune_rule_v1)")
    r.add_argument("--out", type=Path, help="Markdown report path (default: stdout)")
    r.add_argument("--json-out", type=Path)
    r.add_argument("--candidate-out", type=Path, help="Write the candidate prune list (not active)")
    r.add_argument("--title-date", default=None, help="Date in the report title (default: today UTC)")
    r.add_argument("--feed-limit", type=int, default=40)
    r.add_argument("--n-boot", type=int, default=None, help="Bootstrap resamples (default: the rule's 1999)")

    t = sub.add_parser("trust-diff", help="Trust scores: legacy vs corrected outcome basis")
    t.add_argument("--top", type=int, default=25)
    t.add_argument("--json-out", type=Path)
    return p


def _write(path: Path | None, text: str) -> None:
    if path is None:
        sys.stdout.write(text + ("\n" if not text.endswith("\n") else ""))
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    log.info("wrote {p}", p=str(path))


def run_report(engine: Engine, args: argparse.Namespace) -> dict[str, Any]:
    from evaluation import edge_report as er

    with engine.connect() as conn:
        loaded = er.load_all(conn)
        conn.rollback()
    report = er.build_report(loaded, n_boot=args.n_boot or er.N_BOOT)
    title = args.title_date or datetime.now(timezone.utc).date().isoformat()
    _write(args.out, er.render_markdown(report, title_date=title, feed_limit=args.feed_limit))
    if args.json_out:
        _write(args.json_out, er.report_json(report))
    if args.candidate_out:
        _write(args.candidate_out, json.dumps(er.candidate_prune_list(report), indent=1, sort_keys=True))
    return report


def run_trust_diff(engine: Engine, args: argparse.Namespace) -> dict[str, Any]:
    from evaluation.edge_report import trust_diff
    from intelligence.trust_scorer import compute_trust_scores

    today = datetime.now(timezone.utc).date()
    legacy = compute_trust_scores(engine, use_corrected=False, today=today)
    corrected = compute_trust_scores(engine, use_corrected=True, today=today)
    diff = trust_diff(legacy, corrected, top=args.top)
    text = json.dumps(diff, indent=1, sort_keys=True, default=str)
    _write(args.json_out, text)
    return diff


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    engine = read_only_engine(args.db_url, args.timeout_s)
    try:
        if args.command == "report":
            run_report(engine, args)
        else:
            run_trust_diff(engine, args)
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
