"""CLI: ``python -m alpha_research.edge_stream {genesis,cycle,status,verify}``.

Paths come from flags or the environment (the Hermes drop-in sets them):
``EDGE_STREAM_LEDGER_PATH``, ``EDGE_STREAM_ANCHOR_PATH``, ``EDGE_STREAM_DATA_DIR``.

* ``genesis``   creates the ledger + anchor + policy record (refuses existing files).
* ``cycle``     one cycle; ``--dry-run`` appends nothing and computes no outcome
                (with no ledger configured it uses an in-memory genesis).
* ``status``    prints the machine-readable status (no data read).
* ``verify``    re-walks the hash chain and the anchor and re-checks every stream rule.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from . import ledger as L
from .data import PolygonMinuteSource
from .runner import build_status, run_cycle


def _paths(args) -> tuple[str | None, str | None, Path]:
    led = args.ledger or os.environ.get("EDGE_STREAM_LEDGER_PATH")
    anc = args.anchor or os.environ.get("EDGE_STREAM_ANCHOR_PATH")
    data = Path(args.data_dir or os.environ.get("EDGE_STREAM_DATA_DIR") or "edge-stream-data")
    return led, anc, data


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m alpha_research.edge_stream")
    ap.add_argument("command", choices=("genesis", "cycle", "status", "verify"))
    ap.add_argument("--ledger")
    ap.add_argument("--anchor")
    ap.add_argument("--data-dir")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-register", action="store_true")
    ap.add_argument("--max-requests", type=int, default=400)
    args = ap.parse_args(argv)
    led, anc, data = _paths(args)

    if args.command == "genesis":
        if not (led and anc):
            ap.error("genesis needs --ledger and --anchor (or the env)")
        ledger = L.create(led, anc)
        print(json.dumps({"ledger_id": ledger.genesis["ledger_id"], "head": ledger.head,
                          "records": len(ledger.records), "policy_sha256": L.load_state(ledger).policy_sha256}))
        return 0

    if args.command == "cycle" and led and anc:
        source = PolygonMinuteSource(data / "bars.sqlite3", max_requests=args.max_requests)
        # opened inside the cycle lock, so a second process never works from a stale copy
        summary = run_cycle(lambda: L.open_ledger(led, anc), source, data, dry_run=args.dry_run,
                            register=not args.no_register)
        print(json.dumps(summary, indent=1, sort_keys=True, default=str))
        return 0
    if led and anc:
        ledger = L.open_ledger(led, anc)
    elif args.dry_run:
        ledger = L.memory()
    else:
        print("edge_stream: EDGE_STREAM_LEDGER_PATH / _ANCHOR_PATH not set; nothing written", file=sys.stderr)
        return 2

    if args.command == "verify":
        ledger.verify(full=True)
        state = L.load_state(ledger)
        bad = []
        for reg_sha, look in state.looks.items():  # every look's report still hashes to its record
            path = Path(look["report_path"])
            if not path.exists() or L.sha256_bytes(path.read_bytes()) != look["report_sha256"]:
                bad.append(reg_sha[:16])
        if bad:
            print(json.dumps({"ok": False, "reports_missing_or_changed": bad}))
            return 1
        print(json.dumps({"ok": True, "records": len(ledger.records), "head": ledger.head,
                          "registrations": len(state.registrations), "looks": len(state.looks),
                          "abandoned": len(state.abandoned)}))
        return 0
    if args.command == "status":
        status = build_status(ledger, L.load_state(ledger), datetime.now(timezone.utc))
        print(json.dumps(status, indent=1, sort_keys=True))
        return 0

    source = PolygonMinuteSource(data / "bars.sqlite3", max_requests=args.max_requests)
    summary = run_cycle(ledger, source, data, dry_run=args.dry_run, register=not args.no_register)
    print(json.dumps(summary, indent=1, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
