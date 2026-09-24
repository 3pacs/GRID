"""CLI entry point.

    python -m paper_log.gex_levels {preopen|postclose|status|evaluate} --log-dir <dir>

Read-only against the database (preopen only; postclose/status/evaluate
touch no database at all), no orders, no brokerage/trading API calls —
see ``db.py`` and the pre-registration's "Integrity" section.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger as log

from paper_log.gex_levels.config import TICKER
from paper_log.gex_levels.db import build_readonly_engine
from paper_log.gex_levels.evaluate import format_evaluate, run_evaluate
from paper_log.gex_levels.postclose import run_postclose
from paper_log.gex_levels.preopen import run_preopen
from paper_log.gex_levels.status import compute_status, format_status
from paper_log.gex_levels.storage import resolve_code_sha


def _repo_root() -> Path:
    # this file: <repo_root>/paper_log/gex_levels/__main__.py
    return Path(__file__).resolve().parents[2]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m paper_log.gex_levels",
        description="SPY GEX structural-levels forward paper log v1 (read-only DB, no orders).",
    )
    parser.add_argument("command", choices=["preopen", "postclose", "status", "evaluate"])
    parser.add_argument(
        "--log-dir", required=True, type=Path,
        help="Directory holding gex_levels_v1.jsonl (and its lock file).",
    )
    parser.add_argument(
        "--interim", action="store_true",
        help="evaluate only: run with fewer than 60 valid sessions; labels every output line INTERIM.",
    )
    parser.add_argument(
        "--code-sha", default=None,
        help="Override the recorded code_sha. Dev/local use only — the production "
             "install writes a VERSION file that is used automatically.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.interim and args.command != "evaluate":
        parser.error("--interim is only valid with the evaluate command")

    if args.command == "preopen":
        code_sha = resolve_code_sha(_repo_root(), override=args.code_sha)
        engine = build_readonly_engine()
        try:
            record = run_preopen(log_dir=args.log_dir, db_engine=engine, code_sha=code_sha, ticker=TICKER)
        finally:
            engine.dispose()
        log.info(
            "wrote preopen record session_date={d} excluded={e} reason={r}",
            d=record["session_date"], e=record["excluded"], r=record["exclusion_reason"],
        )
        return 0

    if args.command == "postclose":
        code_sha = resolve_code_sha(_repo_root(), override=args.code_sha)
        record = run_postclose(log_dir=args.log_dir, code_sha=code_sha, ticker=TICKER)
        log.info(
            "wrote postclose record session_date={d} excluded={e} reason={r}",
            d=record["session_date"], e=record["excluded"], r=record["exclusion_reason"],
        )
        return 0

    if args.command == "status":
        report = compute_status(args.log_dir)
        print(format_status(report))
        return 0

    if args.command == "evaluate":
        report = run_evaluate(args.log_dir, interim=args.interim)
        print(format_evaluate(report))
        return 1 if report.refused else 0

    parser.error(f"unknown command: {args.command!r}")
    return 2  # pragma: no cover — argparse.error() already exits


if __name__ == "__main__":
    sys.exit(main())
