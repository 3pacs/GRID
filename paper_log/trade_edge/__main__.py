"""CLI.

    python -m paper_log.trade_edge run    --log-dir DIR [--reports-dir DIR]
    python -m paper_log.trade_edge status --log-dir DIR
    python -m paper_log.trade_edge verify --log-dir DIR [--anchor OFFHOST_COPY]

``run`` opens a read-only DB engine (verified with SHOW), re-reads new
candidate filings from EDGAR (needs ``SEC_USER_AGENT``), fetches yfinance
closes, appends to the hash-chained log and writes the daily reports.
``status`` and ``verify`` touch no database and never import ``config``
(which validates ``DB_PASSWORD`` at import).

The recorded ``code_sha`` has no override: the installed archive's
``VERSION`` file, or ``HEAD`` of a clean checkout.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from paper_log.trade_edge.config import (
    ANCHOR_FILENAME,
    LOCK_FILENAME,
    LOG_FILENAME,
    PREREG_SHA256,
    REPORTS_DIRNAME,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def open_log(log_dir: Path):
    from analysis.research_forward_log import ForwardLog

    return ForwardLog(log_dir, log_filename=LOG_FILENAME, anchor_filename=ANCHOR_FILENAME,
                      lock_filename=LOCK_FILENAME, prereg_sha256=PREREG_SHA256)


def status_from_records(records: list[dict]) -> dict:
    from paper_log.trade_edge.scoreboard import build_scoreboard

    entries, exits, filings = {}, {}, []
    for r in records:
        if r.get("kind") == "entry":
            entries[r["position_id"]] = r
        elif r.get("kind") == "exit":
            exits[(r["position_id"], r["horizon"])] = r
        elif r.get("kind") == "filing":
            filings.append(r)
    late = sum(1 for f in filings for line in f["lines"] if line.get("exclusion") == "late_filing")
    grid_db = sum(1 for f in filings if f["source"] == "grid_db")
    board = build_scoreboard(entries.values(), exits.values(), late, grid_db)
    runs = [r for r in records if r.get("kind") == "run"]
    return {"records": len(records), "last_run": runs[-1] if runs else None,
            "banner": board["banner"], "label": board["label"],
            "primary": board["tables"]["h30_large"]["all"], "missing_labels": board["missing_labels"]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m paper_log.trade_edge",
                                     description="trade_edge tracker v2 (read-only DB, no orders).")
    parser.add_argument("command", choices=["run", "status", "verify"])
    parser.add_argument("--log-dir", required=True, type=Path)
    parser.add_argument("--reports-dir", type=Path, default=None,
                        help=f"run: where the daily JSON/Markdown go (default <log-dir>/{REPORTS_DIRNAME})")
    parser.add_argument("--anchor", type=Path, help="verify: off-host copy of the anchor file")
    args = parser.parse_args(argv)
    log = open_log(args.log_dir)

    if args.command == "verify":
        check = log.verify_chain(args.anchor)
        print(json.dumps(check, indent=2))
        return 0 if check["ok"] else 1

    if args.command == "status":
        check = log.verify_chain()
        print(json.dumps({"chain": check, **status_from_records(log.read_all())}, indent=2, default=str))
        return 0 if check["ok"] else 1

    if not os.environ.get("SEC_USER_AGENT"):
        print("REFUSED: SEC_USER_AGENT is not set (source the GRID .env)", file=sys.stderr)
        return 2

    from analysis.research_forward_log import resolve_code_sha
    from paper_log.gex_levels.db import assert_read_only, build_readonly_engine
    from paper_log.trade_edge.prices import YFinancePrices
    from paper_log.trade_edge.report import build_report, write_reports
    from paper_log.trade_edge.sec import SecReader
    from paper_log.trade_edge.tracker import run_once

    code_sha = resolve_code_sha(_repo_root())
    now = datetime.now(timezone.utc)
    engine = build_readonly_engine()
    try:
        assert_read_only(engine)
        with engine.connect() as conn:
            result = run_once(log, conn=conn, now=now, code_sha=code_sha, sec=SecReader(),
                              prices=YFinancePrices())
    finally:
        engine.dispose()
    rep = build_report(result, code_sha)
    jpath, mpath = write_reports(args.reports_dir or (args.log_dir / REPORTS_DIRNAME), rep)
    counts: dict[str, int] = {}
    for r in result["written"]:
        counts[r["kind"]] = counts.get(r["kind"], 0) + 1
    print(json.dumps({"run_at": now.isoformat(), "written": counts, "label": rep["label"]["label"],
                      "new_signals": len(rep["new_signals"]), "json": str(jpath), "markdown": str(mpath)},
                     sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
