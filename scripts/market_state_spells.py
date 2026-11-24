#!/usr/bin/env python3
"""Read-only: the market-state transition log and spells (``intelligence/regime/spells.py``, #1309).

Usage:
    python3 scripts/market_state_spells.py --as-of 2026-11-19          # stored and recomputed blocks at one session
    python3 scripts/market_state_spells.py --transitions --from 2026-01-02 --to 2026-11-19 [--name vix]
    python3 scripts/market_state_spells.py --transitions --stored      # only what the writer stored
    python3 scripts/market_state_spells.py --check                     # stored blocks vs a recomputation

Two sources, never mixed. **Stored** is what the decision writer saved in each record's ``spells`` block when it
wrote the record (``source: writer``); records written before spells existed have none. **Recomputed** folds the
rows of ``market_state_daily`` as stored now, selected by session date, not by when each row was written: a row
backfilled or replaced after a session's own record is folded in. A recomputed block is therefore a
reconstruction from today's store, not what was known that evening; its ``lineage`` counts the earlier rows written
after the session's own record. Every row also keeps its origin: ``daily_write`` or ``reconstruction`` (the
2026-11-13 backfill, ranges, explicit sessions, replacements).

``--transitions`` prints one JSON object per line, each with its ``source``; ``--as-of`` and ``--check`` print one
JSON object. Nothing is written: on Postgres every read runs in a READ ONLY transaction. ``--check`` exits 1 when a
stored block differs from a recomputation (a later ``--replace`` or backfill of an earlier session does that, and
it is worth seeing).
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

COMPARED = ("status", "variables", "named", "transitions")   # the parts of a block --check compares


def _d(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--as-of", type=_d, help="print the stored and the recomputed block at this session")
    mode.add_argument("--transitions", action="store_true", help="print transitions, one JSON object per line")
    mode.add_argument("--check", action="store_true", help="compare every stored block with a recomputation")
    p.add_argument("--stored", action="store_true",
                   help="--transitions: only the transitions the writer stored in each record's block "
                        "(default: recomputed from the current store)")
    p.add_argument("--from", dest="start", type=_d, default=None, help="--transitions: first session (inclusive)")
    p.add_argument("--to", dest="end", type=_d, default=None, help="--transitions: last session (inclusive)")
    p.add_argument("--name", action="append", default=None,
                   help="--transitions: only this variable or named spell (repeatable)")
    return p


def _absent(own) -> dict:
    if own is None:
        return {"status": "absent", "reason": "no stored record for this session"}
    return {"status": "absent", "reason": "the record was stored without a spells block (before #1309)"}


def main(argv: list[str] | None = None, engine=None) -> int:
    a = build_parser().parse_args(argv)
    if not a.transitions and (a.start or a.end or a.name or a.stored):
        print("market-state-spells: --stored, --from, --to and --name go with --transitions", file=sys.stderr)
        return 2
    if a.start and a.end and a.start > a.end:
        print("market-state-spells: --from is after --to", file=sys.stderr)
        return 2

    from intelligence.regime.market_state import xnys_sessions
    from intelligence.regime.spells import RECOMPUTED, fold, load_records, recompute_lineage, spells_block

    if engine is None:
        from db import get_engine

        engine = get_engine()
    now = datetime.now(timezone.utc)
    today = now.date()

    if a.as_of:
        records = load_records(engine, through=a.as_of)
        rows = [r.spell_row() for r in records]
        own = next((r for r in records if r.as_of == a.as_of), None)
        stored = own.record.get("spells") if own is not None else None
        out = {"as_of": a.as_of.isoformat(), "read_at": now.isoformat(),
               "stored": stored if isinstance(stored, dict) else _absent(own),
               "recomputed": {**spells_block(rows, a.as_of, source=RECOMPUTED),
                              "lineage": recompute_lineage(rows, a.as_of)}}
        print(json.dumps(out, sort_keys=True, default=str))
        return 0

    if a.transitions:
        names = set(a.name) if a.name else None

        def wanted(t: dict) -> bool:
            day = date.fromisoformat(t["as_of"])
            return not ((a.start and day < a.start) or (a.end and day > a.end)
                        or (names is not None and t["name"] not in names))

        records = load_records(engine, through=a.end or today)
        shown = 0
        if a.stored:
            blocks = [r.record.get("spells") for r in records]
            with_block = [b for b in blocks if isinstance(b, dict) and b.get("status") == "ok"]
            for b in with_block:
                for t in b.get("transitions") or []:
                    if wanted(t):
                        print(json.dumps({**t, "source": "stored"}, sort_keys=True, default=str))
                        shown += 1
            print(f"market-state-spells: {shown} stored transitions shown; {len(with_block)} of {len(records)} "
                  "records carry an ok spells block (the rest were stored before #1309 or with spells unavailable)",
                  file=sys.stderr)
            return 0
        f = fold([r.spell_row() for r in records], xnys_sessions()[2])
        for t in f.transitions:
            if wanted(t):
                print(json.dumps({**t, "source": RECOMPUTED}, sort_keys=True, default=str))
                shown += 1
        print(f"market-state-spells: {shown} transitions shown of {len(f.transitions)}, recomputed from the store "
              f"as read at {now.isoformat()} (by session date, not first availability); "
              f"{len(f.missing_sessions)} sessions without a row; "
              f"{len(f.ignored_rows)} rows on non-session days ignored", file=sys.stderr)
        return 0

    records = load_records(engine, through=today)
    rows = [r.spell_row() for r in records]
    checked, mismatched, unchecked = 0, [], 0
    for i, r in enumerate(records):
        stored = r.record.get("spells")
        if not isinstance(stored, dict):
            unchecked += 1
            continue
        derived = json.loads(json.dumps(spells_block(rows[: i + 1], r.as_of), default=str))  # as stored: JSON
        checked += 1
        diff = [k for k in COMPARED if stored.get(k) != derived.get(k)]
        if diff:
            later = recompute_lineage(rows[: i + 1], r.as_of)["earlier_rows_written_after_own_record"]
            mismatched.append({"as_of": r.as_of.isoformat(), "differs": diff,
                               "earlier_rows_written_after_block": later})
    print(json.dumps({"rows": len(records), "with_spells": checked, "without_spells": unchecked,
                      "mismatched": mismatched, "read_at": now.isoformat()}, sort_keys=True))
    return 1 if mismatched else 0


if __name__ == "__main__":
    sys.exit(main())
