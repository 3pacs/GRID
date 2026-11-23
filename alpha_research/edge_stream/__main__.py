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
from . import sanity
from .data import EdgarEventSource, PolygonDailySource, PolygonMinuteSource
from .runner import build_status, run_cycle


def _extras(source: PolygonMinuteSource, data: Path) -> dict:
    """Sources of the multi-day event families: daily bars (same key and request cap as the
    minute bars) and SEC filings (needs ``SEC_USER_AGENT``; without it nothing is registered)."""
    return {"daily": PolygonDailySource(source), "events": EdgarEventSource(data / "bars.sqlite3")}


def _paths(args) -> tuple[str | None, str | None, Path]:
    led = args.ledger or os.environ.get("EDGE_STREAM_LEDGER_PATH")
    anc = args.anchor or os.environ.get("EDGE_STREAM_ANCHOR_PATH")
    data = Path(args.data_dir or os.environ.get("EDGE_STREAM_DATA_DIR") or "edge-stream-data")
    return led, anc, data


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m alpha_research.edge_stream")
    ap.add_argument("command", choices=("genesis", "cycle", "status", "verify", "unread-receipt"))
    ap.add_argument("--ledger")
    ap.add_argument("--anchor")
    ap.add_argument("--data-dir")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-register", action="store_true")
    ap.add_argument("--max-requests", type=int, default=400)
    ap.add_argument("--family", help="with --dry-run: preview this family's next queue item")
    ap.add_argument("--registration", help="unread-receipt: the abandoned registration's full sha256")
    ap.add_argument("--repo", help="unread-receipt: a git repository holding both releases' commits")
    ap.add_argument("--registered-release", help="unread-receipt: commit that sealed the registration")
    ap.add_argument("--abandoning-release", help="unread-receipt: commit that ran the abandoning cycle")
    ap.add_argument("--out", help="unread-receipt: the receipt file to create (0444, never overwritten)")
    ap.add_argument("--cache", action="append", default=[],
                    help="unread-receipt: one cache on the owner-approved list (repeat for each)")
    ap.add_argument("--writers-through", help="unread-receipt: the release whose history holds every cache writer")
    ap.add_argument("--writer-tree", action="append", default=[],
                    help="unread-receipt: a staged code tree a rehearsal ran against a listed cache (repeat)")
    args = ap.parse_args(argv)
    if args.family and not args.dry_run:
        ap.error("--family is a dry-run preview")
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
        sanity.set_feasibility_source(source)  # G11 reads pre-window bars through the same cache
        try:
            # opened inside the cycle lock, so a second process never works from a stale copy
            summary = run_cycle(lambda: L.open_ledger(led, anc), source, data, dry_run=args.dry_run,
                                register=not args.no_register, extras=_extras(source, data),
                                only_family=args.family)
        finally:
            sanity.set_feasibility_source(None)
        print(json.dumps(summary, indent=1, sort_keys=True, default=str))
        return 0
    if led and anc:
        ledger = L.open_ledger(led, anc)
    elif args.dry_run:
        ledger = L.memory()
    else:
        print("edge_stream: EDGE_STREAM_LEDGER_PATH / _ANCHOR_PATH not set; nothing written", file=sys.stderr)
        return 2

    if args.command == "unread-receipt":
        if not (args.registration and args.repo and args.registered_release and args.abandoning_release
                and args.out and data and args.cache and args.writers_through):
            ap.error("unread-receipt needs --registration, --repo, --registered-release, --abandoning-release, "
                     "--writers-through, --cache (one per listed cache), --out and --data-dir")
        ledger.verify(full=True)
        from . import receipt as RC

        # the cache list is the owner's; the policy's attestation must name the same list
        rc = RC.build(ledger, args.registration, Path(data), repo=Path(args.repo),
                      registered_release=args.registered_release, abandoning_release=args.abandoning_release,
                      caches=args.cache, writers_through=args.writers_through, writer_trees=args.writer_tree)
        sha = RC.write(rc, Path(args.out))
        print(json.dumps({"verdict": rc["verdict"], "failed": rc.get("failed"), "out": args.out, "sha256": sha,
                          "consumed_through_exclusive": rc.get("consumed_through_exclusive")}))
        return 0 if rc["verdict"] != "REFUSED" else 1
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
    sanity.set_feasibility_source(source)
    try:
        summary = run_cycle(ledger, source, data, dry_run=args.dry_run, register=not args.no_register,
                            extras=_extras(source, data), only_family=args.family)
    finally:
        sanity.set_feasibility_source(None)
    print(json.dumps(summary, indent=1, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
