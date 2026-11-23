#!/usr/bin/env python3
"""Read-only: the market-state transition log and spells (``intelligence/regime/spells.py``, #1309).

Usage:
    python3 scripts/market_state_spells.py --as-of 2026-11-19          # the spells block at one session (JSON)
    python3 scripts/market_state_spells.py --transitions --from 2026-01-02 --to 2026-11-19 [--name vix]
    python3 scripts/market_state_spells.py --check                     # stored blocks vs a fresh derivation

Everything is derived from the stored labels of ``market_state_daily`` on the pinned XNYS sessions, so it agrees with
them by construction. A transition read in a row the nightly writer did not store (the 2026-11-13 backfill, ranges,
explicit sessions, replacements) is labelled ``reconstruction``. ``--transitions`` prints one JSON object per line;
``--as-of`` and ``--check`` print one JSON object. Nothing is written: on Postgres every read runs in a READ ONLY
transaction. ``--check`` exits 1 when a stored block differs from a fresh derivation (a later ``--replace`` of an
earlier session does that, and it is worth seeing).
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
    mode.add_argument("--as-of", type=_d, help="print the spells block at this session")
    mode.add_argument("--transitions", action="store_true", help="print every transition, one JSON object per line")
    mode.add_argument("--check", action="store_true", help="compare every stored block with a fresh derivation")
    p.add_argument("--from", dest="start", type=_d, default=None, help="--transitions: first session (inclusive)")
    p.add_argument("--to", dest="end", type=_d, default=None, help="--transitions: last session (inclusive)")
    p.add_argument("--name", action="append", default=None,
                   help="--transitions: only this variable or named spell (repeatable)")
    return p


def main(argv: list[str] | None = None, engine=None) -> int:
    a = build_parser().parse_args(argv)
    if not a.transitions and (a.start or a.end or a.name):
        print("market-state-spells: --from, --to and --name go with --transitions", file=sys.stderr)
        return 2
    if a.start and a.end and a.start > a.end:
        print("market-state-spells: --from is after --to", file=sys.stderr)
        return 2

    from intelligence.regime.market_state import xnys_sessions
    from intelligence.regime.spells import (
        SpellRow,
        fold,
        load_records,
        load_rows,
        spells_block,
    )

    if engine is None:
        from db import get_engine

        engine = get_engine()
    today = datetime.now(timezone.utc).date()

    if a.as_of:
        print(json.dumps(spells_block(load_rows(engine, through=a.as_of), a.as_of), sort_keys=True, default=str))
        return 0

    if a.transitions:
        f = fold(load_rows(engine, through=a.end or today), xnys_sessions()[2])
        names = set(a.name) if a.name else None
        shown = 0
        for t in f.transitions:
            day = date.fromisoformat(t["as_of"])
            if (a.start and day < a.start) or (names is not None and t["name"] not in names):
                continue
            print(json.dumps(t, sort_keys=True, default=str))
            shown += 1
        print(f"market-state-spells: {shown} transitions shown of {len(f.transitions)}; "
              f"{len(f.missing_sessions)} sessions without a row; "
              f"{len(f.ignored_rows)} rows on non-session days ignored", file=sys.stderr)
        return 0

    records = load_records(engine, through=today)
    rows = [SpellRow.from_record(d, rule, rec) for d, rule, rec in records]
    checked, mismatched, unchecked = 0, [], 0
    for i, (d, _rule, rec) in enumerate(records):
        stored = rec.get("spells")
        if not isinstance(stored, dict):
            unchecked += 1
            continue
        derived = json.loads(json.dumps(spells_block(rows[: i + 1], d), default=str))  # as stored: JSON
        checked += 1
        diff = [k for k in COMPARED if stored.get(k) != derived.get(k)]
        if diff:
            mismatched.append({"as_of": d.isoformat(), "differs": diff})
    print(json.dumps({"rows": len(records), "with_spells": checked, "without_spells": unchecked,
                      "mismatched": mismatched}, sort_keys=True))
    return 1 if mismatched else 0


if __name__ == "__main__":
    sys.exit(main())
