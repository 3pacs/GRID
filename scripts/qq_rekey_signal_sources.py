"""Re-key legacy QuiverQuant ``signal_sources`` rows to their act identity.

Background
----------
``ingestion/altdata/quiverquant.py`` used to write ``source_id = "qq_<endpoint>"``
for every row of an endpoint. For the four endpoints that return one row per act
(insider, house, senate, lobbying) the writer now writes
``qq_<endpoint>:<act identity>`` (``ingestion/altdata/quiverquant_identity.py``).
Rows written before that keep the constant id. This script moves each legacy row
to the id the new writer would have given it, computed from the row's own stored
``signal_value``.

What it does
------------
For every legacy row (``source_id`` equals the endpoint's constant id) of the four
keyed ``source_type`` values it computes the new ``source_id`` and plans one of:

* ``move``      - the target key ``(source_type, new source_id, ticker,
                  signal_date, signal_type)`` is free: ``UPDATE ... SET source_id``;
* ``conflict``  - the target key already exists. This is the overlap window of the
                  QuiverQuant live endpoints: the new writer already re-pulled the
                  same act under its keyed id. The legacy row is left as it is and
                  reported;
* ``unreadable_payload`` / ``no_identity_fields`` - the payload cannot say who the
                  act belongs to; left as it is and reported.

It never deletes, never touches ``signal_value``, ``created_at``, ``outcome`` or any
other column, and never touches a row that is not a legacy row. The coordinator
decides what to do with the reported conflicts (they are exact duplicates of a
keyed row).

Safety
------
* Dry run by default: a read-only session (``default_transaction_read_only=on``),
  20 s statement timeout, 2 s lock timeout, nothing is written.
* Both the dry run and ``--apply`` refuse to start, and refuse to continue between
  batches, inside 03:30-10:30 UTC (nightly ``pg_dump`` window).
* ``--apply`` requires ``--audit-log`` (a new file): one JSON line per move with
  ``id``, ``old_source_id`` and ``new_source_id``. Each UPDATE re-checks that the
  target key is still free, so a concurrent writer cannot make it collide.
* ``--apply`` commits in batches (``--batch-size``); ``--max-moves`` stops early.

Side effect to know before applying
-----------------------------------
``intelligence/signal_extractor`` de-duplicates on the composite
``source_type:source_id:ticker`` + ``signal_date``. A re-keyed row inside the
extractor's look-back (45 days, ``GRID_EXTRACTOR_LOOKBACK_DAYS``) is therefore
extracted into ``signal_data`` once more under its new id. The report counts those
rows (``in_extractor_window``); use ``--before`` to leave them out of a run.

Revert (from the audit log)
---------------------------
    UPDATE signal_sources SET source_id = :old
     WHERE id = :id AND source_id = :new;

Usage (grid-svr; see the PR for the run procedure)
--------------------------------------------------
    cd /data/grid_v4/grid_release
    set -a; . /home/grid/grid_v4/grid_repo/.env; set +a
    /home/grid/grid_v4/venv/bin/python -m scripts.qq_rekey_signal_sources            # dry run
    /home/grid/grid_v4/venv/bin/python -m scripts.qq_rekey_signal_sources --apply \\
        --audit-log ~/research/qq_rekey_audit_YYYYMMDD.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from ingestion.altdata.quiverquant_identity import (
    IDENTITY_SEPARATOR,
    KEYED_SOURCE_TYPES,
    act_identity,
    legacy_source_id,
    parse_payload,
    source_id_for,
)
from scripts import qq_transition_common as common

DEFAULT_BATCH_SIZE = 500
EXTRACTOR_LOOKBACK_DAYS = int(os.environ.get("GRID_EXTRACTOR_LOOKBACK_DAYS", "45"))
SAMPLE_LIMIT = 10

Key = tuple[str, str, str, date, str]  # source_type, source_id, ticker, signal_date, signal_type


# ── planning (pure) ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Move:
    """One planned re-key."""

    id: int
    source_type: str
    ticker: str
    signal_date: date
    signal_type: str
    old_source_id: str
    new_source_id: str


@dataclass
class RekeyPlan:
    """Everything the planner decided for one ``source_type``."""

    source_type: str
    moves: list[Move] = field(default_factory=list)
    conflicts: list[Move] = field(default_factory=list)
    skipped: Counter = field(default_factory=Counter)
    legacy_rows: int = 0
    min_date: date | None = None
    max_date: date | None = None
    date_counts: Counter = field(default_factory=Counter)


def as_date(value: Any) -> date | None:
    """A DATE column value (``date``, ``datetime`` or ISO text) as a ``date``."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def target_source_id(source_type: str, payload: dict[str, Any]) -> str | None:
    """The id the writer would give this payload, or ``None`` when it cannot say.

    ``None`` when the source_type is not a keyed feed, or the payload carries none
    of the identity fields (every part of the identity is the unknown marker).
    """
    endpoint = KEYED_SOURCE_TYPES.get(source_type)
    if endpoint is None:
        return None
    identity = act_identity(endpoint, payload)
    if identity is None or all(part == "?" for part in identity.split(IDENTITY_SEPARATOR)):
        return None
    return source_id_for(endpoint, payload)


def plan_rekey(
    source_type: str,
    rows: Iterable[dict[str, Any]],
    existing_keys: set[Key],
) -> RekeyPlan:
    """Decide, row by row, what re-keying ``source_type``'s legacy rows would do.

    Parameters:
        source_type: One of ``KEYED_SOURCE_TYPES``.
        rows: Legacy rows: dicts with ``id``, ``ticker``, ``signal_date``,
            ``signal_type``, ``source_id`` and ``signal_value``.
        existing_keys: Every full key already present for rows whose ``source_id``
            is *not* the legacy constant (the new writer's rows).

    Returns:
        The plan; ``moves`` and ``conflicts`` are in input order.
    """
    plan = RekeyPlan(source_type=source_type)
    legacy = legacy_source_id(KEYED_SOURCE_TYPES[source_type])
    claimed: set[Key] = set()
    for row in rows:
        old_id = row.get("source_id")
        if old_id != legacy:
            plan.skipped["not_a_legacy_row"] += 1
            continue
        plan.legacy_rows += 1
        sdate = as_date(row.get("signal_date"))
        if sdate is not None:
            plan.min_date = sdate if plan.min_date is None else min(plan.min_date, sdate)
            plan.max_date = sdate if plan.max_date is None else max(plan.max_date, sdate)
            plan.date_counts[sdate] += 1
        payload = parse_payload(row.get("signal_value"))
        if not payload:
            plan.skipped["unreadable_payload"] += 1
            continue
        new_id = target_source_id(source_type, payload)
        if new_id is None or sdate is None:
            plan.skipped["no_identity_fields"] += 1
            continue
        move = Move(
            id=int(row["id"]), source_type=source_type, ticker=str(row["ticker"]),
            signal_date=sdate, signal_type=str(row["signal_type"]),
            old_source_id=str(old_id), new_source_id=new_id,
        )
        key: Key = (source_type, new_id, move.ticker, sdate, move.signal_type)
        if key in existing_keys or key in claimed:
            plan.conflicts.append(move)
            continue
        claimed.add(key)
        plan.moves.append(move)
    return plan


def summarize(plan: RekeyPlan, today: date) -> dict[str, Any]:
    """Counts only (no names): safe to paste into a ticket."""
    window_start = today - timedelta(days=EXTRACTOR_LOOKBACK_DAYS)
    in_window = sum(1 for m in plan.moves if m.signal_date >= window_start)
    conflicts_by_year: Counter = Counter(str(m.signal_date.year) for m in plan.conflicts)
    # How far back the legacy rows reach: the live endpoints only re-serve recent acts, so
    # the rows that can ever collide with a keyed row are the recent ones.
    age_buckets = {"<=7d": 0, "<=30d": 0, "<=90d": 0, ">90d": 0}
    for sdate, n in plan.date_counts.items():
        age = (today - sdate).days
        age_buckets["<=7d" if age <= 7 else "<=30d" if age <= 30 else "<=90d" if age <= 90 else ">90d"] += n
    return {
        "source_type": plan.source_type,
        "legacy_rows": plan.legacy_rows,
        "would_move": len(plan.moves),
        "conflicts_skipped": len(plan.conflicts),
        "skipped": dict(plan.skipped),
        "in_extractor_window": in_window,
        "legacy_rows_by_age": age_buckets,
        "legacy_date_range": [
            plan.min_date.isoformat() if plan.min_date else None,
            plan.max_date.isoformat() if plan.max_date else None,
        ],
        "conflicts_by_year": dict(sorted(conflicts_by_year.items())),
        "conflict_sample": [
            {"id": m.id, "ticker": m.ticker, "signal_date": m.signal_date.isoformat(), "signal_type": m.signal_type}
            for m in plan.conflicts[:SAMPLE_LIMIT]
        ],
    }


# ── database ───────────────────────────────────────────────────────────────

_LEGACY_ROWS_SQL = text(
    """
    SELECT id, source_id, ticker, signal_date, signal_type, signal_value
      FROM signal_sources
     WHERE source_type = :source_type AND source_id = :legacy
    """
)
_KEYED_KEYS_SQL = text(
    """
    SELECT source_id, ticker, signal_date, signal_type
      FROM signal_sources
     WHERE source_type = :source_type AND source_id <> :legacy
    """
)
# The NOT EXISTS makes the move atomic with the check: a row written after the
# plan was made cannot be collided with.
_MOVE_SQL = text(
    """
    UPDATE signal_sources
       SET source_id = :new_id
     WHERE id = :id
       AND source_type = :source_type
       AND source_id = :old_id
       AND NOT EXISTS (
            SELECT 1 FROM signal_sources t
             WHERE t.source_type = :source_type AND t.source_id = :new_id
               AND t.ticker = :ticker AND t.signal_date = :signal_date
               AND t.signal_type = :signal_type)
    """
)


def load_legacy_rows(conn: Connection, source_type: str) -> Iterator[dict[str, Any]]:
    """Stream the legacy rows of one source_type."""
    legacy = legacy_source_id(KEYED_SOURCE_TYPES[source_type])
    result = conn.execute(
        _LEGACY_ROWS_SQL, {"source_type": source_type, "legacy": legacy},
        execution_options={"stream_results": True},  # per statement: never leaks onto the connection
    )
    for row in result.mappings():
        yield dict(row)


def load_keyed_keys(conn: Connection, source_type: str) -> set[Key]:
    """The full keys of the source_type's rows that are not legacy rows."""
    legacy = legacy_source_id(KEYED_SOURCE_TYPES[source_type])
    keys: set[Key] = set()
    for source_id, ticker, signal_date, signal_type in conn.execute(
        _KEYED_KEYS_SQL, {"source_type": source_type, "legacy": legacy}
    ):
        sdate = as_date(signal_date)
        if sdate is not None:
            keys.add((source_type, str(source_id), str(ticker), sdate, str(signal_type)))
    return keys


def apply_moves(
    engine: Engine,
    moves: list[Move],
    *,
    batch_size: int,
    audit_path: Path,
    max_moves: int | None = None,
) -> dict[str, int]:
    """Execute planned moves in committed batches, appending each to the audit log."""
    moved = 0
    lost_race = 0
    todo = moves if max_moves is None else moves[:max_moves]
    with audit_path.open("x", encoding="utf-8") as audit:
        for start in range(0, len(todo), batch_size):
            common.check_window()
            batch = todo[start:start + batch_size]
            done: list[Move] = []
            with engine.begin() as conn:
                for m in batch:
                    res = conn.execute(_MOVE_SQL, {
                        "id": m.id, "source_type": m.source_type, "old_id": m.old_source_id,
                        "new_id": m.new_source_id, "ticker": m.ticker,
                        "signal_date": m.signal_date, "signal_type": m.signal_type,
                    })
                    if res.rowcount == 1:
                        done.append(m)
                    else:
                        lost_race += 1
            for m in done:  # written after the batch committed: the log never claims a move that did not happen
                audit.write(json.dumps({
                    "id": m.id, "source_type": m.source_type,
                    "old_source_id": m.old_source_id, "new_source_id": m.new_source_id,
                }) + "\n")
            audit.flush()
            moved += len(done)
    return {"moved": moved, "not_moved_target_taken_or_row_changed": lost_race}


def run(
    engine: Engine,
    *,
    source_types: list[str],
    apply: bool,
    audit_path: Path | None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_moves: int | None = None,
    before: date | None = None,
    today: date | None = None,
) -> dict[str, Any]:
    """Plan (and with ``apply`` execute) the re-key for each source_type."""
    today = today or datetime.now(timezone.utc).date()
    report: dict[str, Any] = {
        "mode": "apply" if apply else "dry_run",
        "run_at": datetime.now(timezone.utc).isoformat(),
        "before": before.isoformat() if before else None,
        "extractor_lookback_days": EXTRACTOR_LOOKBACK_DAYS,
        "source_types": {},
    }
    plans: dict[str, RekeyPlan] = {}
    with engine.connect() as conn:
        if not apply and engine.dialect.name == "postgresql":
            common.assert_read_only(conn)
        for st in source_types:
            common.check_window()
            rows = load_legacy_rows(conn, st)
            if before is not None:
                rows = (r for r in rows if (as_date(r.get("signal_date")) or date.min) < before)
            plans[st] = plan_rekey(st, rows, load_keyed_keys(conn, st))
            report["source_types"][st] = summarize(plans[st], today)
    totals: dict[str, int] = defaultdict(int)
    for summary in report["source_types"].values():
        for k in ("legacy_rows", "would_move", "conflicts_skipped", "in_extractor_window"):
            totals[k] += summary[k]
    report["totals"] = dict(totals)
    if apply:
        assert audit_path is not None
        all_moves = [m for st in source_types for m in plans[st].moves]
        report["applied"] = apply_moves(
            engine, all_moves, batch_size=batch_size, audit_path=audit_path, max_moves=max_moves
        )
    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write (default: dry run, read-only session)")
    ap.add_argument("--source-type", action="append", choices=sorted(KEYED_SOURCE_TYPES),
                    help="limit to one source_type (repeatable); default: all four keyed feeds")
    ap.add_argument("--audit-log", type=Path, help="new JSONL file recording every move (required with --apply)")
    ap.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    ap.add_argument("--max-moves", type=int, help="stop --apply after this many moves")
    ap.add_argument("--before", help="only re-key rows with signal_date before this ISO date")
    ap.add_argument("--db-url-env", help="env var holding the database URL (default: config.settings.DB_URL)")
    ap.add_argument("--out", type=Path, help="also write the JSON report here (must not exist)")
    args = ap.parse_args(argv)

    if args.apply and not args.audit_log:
        print("--apply requires --audit-log", file=sys.stderr)
        return 2
    if args.audit_log and args.audit_log.exists():
        print(f"refusing to overwrite {args.audit_log}", file=sys.stderr)
        return 2
    if args.out and args.out.exists():
        print(f"refusing to overwrite {args.out}", file=sys.stderr)
        return 2
    try:
        common.check_window()
    except common.WindowClosed as exc:
        print(str(exc), file=sys.stderr)
        return 3

    engine = common.open_engine(
        common.database_url(args.db_url_env), read_only=not args.apply,
        application_name="qq_rekey_signal_sources",
    )
    try:
        report = run(
            engine,
            source_types=args.source_type or sorted(KEYED_SOURCE_TYPES),
            apply=args.apply,
            audit_path=args.audit_log,
            batch_size=args.batch_size,
            max_moves=args.max_moves,
            before=date.fromisoformat(args.before) if args.before else None,
        )
    except common.WindowClosed as exc:
        print(str(exc), file=sys.stderr)
        return 3
    finally:
        engine.dispose()
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.out:
        args.out.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
