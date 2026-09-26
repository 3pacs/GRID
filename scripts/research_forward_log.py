"""S10: hypothesis-loop forward log v1 (append-only, hash-chained, read-only DB).

    python -m scripts.research_forward_log run    --log-dir DIR
    python -m scripts.research_forward_log admit  --log-dir DIR --scan-dir SCAN --repo CLONE
    python -m scripts.research_forward_log status --log-dir DIR
    python -m scripts.research_forward_log verify --log-dir DIR

Rules: ``docs/paper_log/hypothesis-forward-v1-preregistration.md``.

* ``run`` (daily job): opens a NullPool engine with
  ``default_transaction_read_only=on`` and a statement timeout of at most 60 s,
  reads only through the latest-vintage adapter, appends due prediction,
  outcome and verdict records, then rewrites ``STATUS.md``. On an empty log it
  writes the header record only.
* ``admit`` (operator step): checks one scan output directory against the
  pre-registered eligibility rules and appends its candidates, all or none.
  ``--repo`` is a git clone used to prove the scan's code includes #661. No DB.
* ``status`` / ``verify``: no DB. ``verify`` exits 1 on a broken chain.

Commands that need no database never import ``config`` (which validates
``DB_PASSWORD`` at import).
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from analysis import research_forward_log as fl

REPO = Path(__file__).resolve().parent.parent


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m scripts.research_forward_log")
    parser.add_argument("command", choices=["run", "admit", "status", "verify"])
    parser.add_argument("--log-dir", required=True, type=Path)
    parser.add_argument("--scan-dir", type=Path, help="admit: scan output directory")
    parser.add_argument("--repo", type=Path, help="admit: git clone holding the scan's commit")
    parser.add_argument("--code-sha", default=None, help="override the recorded code_sha (dev)")
    parser.add_argument("--statement-timeout-s", type=int, default=60)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    log = fl.ForwardLog(args.log_dir)
    now = datetime.now(timezone.utc)

    if args.command == "verify":
        check = log.verify_chain()
        print(json.dumps(check, indent=2))
        return 0 if check["ok"] else 1

    if args.command == "status":
        print(fl.format_status(fl.status_report(log, now)), end="")
        return 0 if log.verify_chain()["ok"] else 1

    code_sha = fl.resolve_code_sha(REPO, args.code_sha)

    if args.command == "admit":
        if args.scan_dir is None or args.repo is None:
            parser.error("admit needs --scan-dir and --repo")
        try:
            written = fl.admit_scan(log, args.scan_dir, fl.GitRepo(args.repo), now, code_sha)
        except fl.Refused as refusal:
            print(f"REFUSED: {refusal}", file=sys.stderr)
            return 2
        fl.write_status(log, now)
        print(f"admitted {sum(r['kind'] == 'admission' for r in written)} candidate(s)")
        return 0

    from scripts.run_real_panel_scan import read_only_engine

    engine = read_only_engine(args.statement_timeout_s, "hypothesis_forward_log")
    try:
        with engine.connect() as conn:
            written = fl.run_forward(log, conn, now, code_sha)
    finally:
        engine.dispose()
    fl.write_status(log, now)
    counts: dict[str, int] = {}
    for record in written:
        counts[record["kind"]] = counts.get(record["kind"], 0) + 1
    print(json.dumps({"run_at": now.isoformat(), "written": counts}, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
