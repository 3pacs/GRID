"""Re-date post-#694 ``quiverquant:gov_contracts`` rows from calendar to fiscal quarter ends.

Background
----------
QuiverQuant's ``/live/govcontracts`` ``(Year, Qtr)`` is the US federal *fiscal*
quarter (FY Y Q1 = Oct-Dec of Y-1, Q2 = Jan-Mar Y, Q3 = Apr-Jun Y, Q4 = Jul-Sep Y).
GD-FIX (#694) stored ``signal_date = calendar_end(Year, Qtr)``, one quarter too
late (``max(signal_date)`` was the future 2026-12-31). The writer now stores
``fiscal_end(Year, Qtr)``. This script moves the rows #694 wrote.

The collision chain
-------------------
``calendar_end(Y, Q) == fiscal_end(Y, Q+1)`` (and ``calendar_end(Y-1, 4) ==
fiscal_end(Y, 1)``), so the post-fix row of quarter Q must move into the slot the
row of quarter Q-1 occupies, and that row is moving too. A naive re-date collides on
the unique key ``(source_type, source_id, ticker, signal_date, signal_type)``. Rows
are therefore processed in ascending ``signal_date`` per ticker: the earliest row
moves into an empty slot, which vacates the slot the next row moves into, and so on.
The plan simulates this occupancy exactly, so the apply never needs a deferred
constraint, a temporary date or a delete.

What it does
------------
A row is a post-#694 row when ``signal_date == calendar_end(payload Year, Qtr)``.
Each row of ``quiverquant:gov_contracts`` is classified:

* ``move``                 - post-#694 row whose fiscal slot is free (or just vacated);
* ``already_fiscal``       - ``signal_date == fiscal_end(payload Year, Qtr)``: the new
                             writer already overwrote or wrote it; nothing to do;
* ``conflict``             - post-#694 row whose fiscal slot is occupied and is not
                             vacated: left alone and reported. ``duplicate_of_fiscal_row``
                             means the occupant carries the same (Year, Qtr), i.e. this
                             row is a stale duplicate (typically the latest quarter's
                             old row, still dated in the future); ``target_held_by_other``
                             means another quarter's row, itself not moving, holds it;
* ``not_post_fix_row``     - dated neither at the calendar nor the fiscal end (pre-#694
                             daily snapshots dated the pull day): not touched;
* ``no_period``            - payload has no usable (Year, Qtr): not touched.

It never deletes and never touches ``signal_value`` or any column other than
``signal_date``.

Safety
------
* Dry run by default: read-only session, 20 s statement timeout, 2 s lock timeout.
* Both the dry run and ``--apply`` refuse to start, and refuse to continue between
  tickers, inside 03:30-10:30 UTC (nightly ``pg_dump`` window).
* ``--apply`` requires ``--audit-log`` (a new file): one JSON line per move. Each
  ticker's chain is one transaction and every UPDATE re-checks that its target slot
  is free and that the row still has the date the plan saw.
* ``--revert AUDIT_LOG`` undoes an apply, processing each ticker in *descending*
  date order for the same reason.

Run order matters
-----------------
Run it after the fiscal-quarter writer is deployed (the old writer would re-create
calendar-dated rows). After the first pull of the new writer most rows are already
fiscal-dated (it overwrites the calendar-dated rows in place, one slot down); what is
left is the latest quarter's stale future-dated row per ticker, reported as
``duplicate_of_fiscal_row``.

Usage (grid-svr; see the PR for the run procedure)
--------------------------------------------------
    cd /data/grid_v4/grid_release
    set -a; . /home/grid/grid_v4/grid_repo/.env; set +a
    /home/grid/grid_v4/venv/bin/python -m scripts.qq_gov_contracts_redate           # dry run
    /home/grid/grid_v4/venv/bin/python -m scripts.qq_gov_contracts_redate --apply \\
        --audit-log ~/research/qq_gov_redate_audit_YYYYMMDD.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from ingestion.altdata.quiverquant_identity import (
    calendar_quarter_end,
    fiscal_quarter_end,
    parse_payload,
    parse_year_qtr,
)
from scripts import qq_transition_common as common

SOURCE_TYPE = "quiverquant:gov_contracts"
SAMPLE_LIMIT = 10

Slot = tuple[str, str, str, date]  # source_id, ticker, signal_type, signal_date


# ── planning (pure) ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Redate:
    """One planned date move."""

    id: int
    source_id: str
    ticker: str
    signal_type: str
    year: int
    qtr: int
    old_date: date
    new_date: date


@dataclass
class RedatePlan:
    """The planner's decision for every row of the source_type."""

    moves: list[Redate] = field(default_factory=list)
    conflicts: list[tuple[Redate, str]] = field(default_factory=list)
    skipped: Counter = field(default_factory=Counter)
    total_rows: int = 0
    longest_chain: int = 0
    chain_count: int = 0


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


def plan_redate(rows: Iterable[dict[str, Any]]) -> RedatePlan:
    """Plan the calendar -> fiscal re-date for every ``quiverquant:gov_contracts`` row.

    Parameters:
        rows: Dicts with ``id``, ``source_id``, ``ticker``, ``signal_type``,
            ``signal_date`` and ``signal_value`` (dict, JSON text or None). Pass
            *every* row of the source_type: rows the plan does not move still occupy
            slots, and a move onto an occupied slot is a conflict.

    Returns:
        The plan. ``moves`` are ordered so that executing them in order never lands
        on an occupied slot: ascending ``signal_date`` within each
        (source_id, ticker, signal_type).
    """
    plan = RedatePlan()
    occupied: dict[Slot, tuple[int, tuple[int, int] | None]] = {}
    candidates: dict[tuple[str, str, str], list[Redate]] = defaultdict(list)

    for row in rows:
        plan.total_rows += 1
        sdate = as_date(row.get("signal_date"))
        if sdate is None:
            plan.skipped["no_signal_date"] += 1
            continue
        source_id, ticker, stype = str(row["source_id"]), str(row["ticker"]), str(row["signal_type"])
        year_qtr = parse_year_qtr(parse_payload(row.get("signal_value")))
        occupied[(source_id, ticker, stype, sdate)] = (int(row["id"]), year_qtr)
        if year_qtr is None:
            plan.skipped["no_period"] += 1
            continue
        fiscal = fiscal_quarter_end(*year_qtr)
        calendar = calendar_quarter_end(*year_qtr)
        if fiscal is None or calendar is None:
            plan.skipped["no_period"] += 1
        elif sdate == fiscal:
            plan.skipped["already_fiscal"] += 1
        elif sdate == calendar:
            candidates[(source_id, ticker, stype)].append(Redate(
                id=int(row["id"]), source_id=source_id, ticker=ticker, signal_type=stype,
                year=year_qtr[0], qtr=year_qtr[1], old_date=sdate, new_date=fiscal,
            ))
        else:
            plan.skipped["not_post_fix_row"] += 1

    for group in candidates.values():
        group.sort(key=lambda r: r.old_date)
        for item in group:
            target: Slot = (item.source_id, item.ticker, item.signal_type, item.new_date)
            holder = occupied.get(target)
            if holder is not None:
                same_quarter = holder[1] == (item.year, item.qtr)
                plan.conflicts.append((item, "duplicate_of_fiscal_row" if same_quarter else "target_held_by_other"))
                continue
            del occupied[(item.source_id, item.ticker, item.signal_type, item.old_date)]
            occupied[target] = (item.id, (item.year, item.qtr))
            plan.moves.append(item)
        lengths = _chain_lengths(group)
        plan.chain_count += len(lengths)
        plan.longest_chain = max([plan.longest_chain, *lengths])
    return plan


def _chain_lengths(group: list[Redate]) -> list[int]:
    """Lengths of the runs of consecutive moves where each lands in the previous one's slot.

    After the fiscal writer's first pull each run's top row (the latest quarter, still at
    its calendar end) is what remains as a stale duplicate, so the run count is also the
    expected number of ``duplicate_of_fiscal_row`` conflicts per ticker and signal type.
    """
    lengths: list[int] = []
    run = 0
    prev_old: date | None = None
    for item in group:
        if prev_old is not None and item.new_date == prev_old:
            run += 1
        else:
            if run:
                lengths.append(run)
            run = 1
        prev_old = item.old_date
    if run:
        lengths.append(run)
    return lengths


def summarize(plan: RedatePlan, today: date) -> dict[str, Any]:
    """Counts only: safe to paste into a ticket."""
    reasons: Counter = Counter(reason for _, reason in plan.conflicts)
    moved_future = sum(1 for m in plan.moves if m.old_date > today)
    still_future = sum(1 for m in plan.moves if m.new_date > today)
    return {
        "source_type": SOURCE_TYPE,
        "rows_examined": plan.total_rows,
        "would_move": len(plan.moves),
        "already_fiscal": plan.skipped.get("already_fiscal", 0),
        "conflicts_skipped": len(plan.conflicts),
        "conflicts_by_reason": dict(reasons),
        "not_post_fix_row": plan.skipped.get("not_post_fix_row", 0),
        "no_period": plan.skipped.get("no_period", 0),
        "no_signal_date": plan.skipped.get("no_signal_date", 0),
        "longest_collision_chain": plan.longest_chain,
        "collision_chains": plan.chain_count,
        "moves_by_quarter_shift": dict(sorted(Counter(
            f"Q{m.qtr}: {m.old_date.isoformat()[5:]} -> {m.new_date.isoformat()[5:]}" for m in plan.moves
        ).items())),
        "future_dated_moves_before": moved_future,
        "future_dated_moves_after": still_future,
        "conflict_sample": [
            {"id": c.id, "ticker": c.ticker, "year": c.year, "qtr": c.qtr,
             "signal_date": c.old_date.isoformat(), "reason": reason}
            for c, reason in plan.conflicts[:SAMPLE_LIMIT]
        ],
    }


# ── database ───────────────────────────────────────────────────────────────

_ROWS_SQL = text(
    """
    SELECT id, source_id, ticker, signal_date, signal_type, signal_value
      FROM signal_sources
     WHERE source_type = :source_type
    """
)
_MOVE_SQL = text(
    """
    UPDATE signal_sources
       SET signal_date = :new_date
     WHERE id = :id
       AND source_type = :source_type
       AND signal_date = :old_date
       AND NOT EXISTS (
            SELECT 1 FROM signal_sources t
             WHERE t.source_type = :source_type AND t.source_id = :source_id
               AND t.ticker = :ticker AND t.signal_date = :new_date
               AND t.signal_type = :signal_type)
    """
)


def load_rows(conn: Connection) -> Iterator[dict[str, Any]]:
    """Stream every row of the source_type (keyed by ``source_type``, its index's lead column)."""
    result = conn.execute(_ROWS_SQL, {"source_type": SOURCE_TYPE}, execution_options={"stream_results": True})
    for row in result.mappings():
        yield dict(row)


def _params(m: Redate, *, forward: bool) -> dict[str, Any]:
    return {
        "id": m.id, "source_type": SOURCE_TYPE, "source_id": m.source_id, "ticker": m.ticker,
        "signal_type": m.signal_type,
        "old_date": m.old_date if forward else m.new_date,
        "new_date": m.new_date if forward else m.old_date,
    }


def _by_ticker(moves: Iterable[Redate], *, descending: bool) -> list[list[Redate]]:
    groups: dict[tuple[str, str, str], list[Redate]] = defaultdict(list)
    for m in moves:
        groups[(m.source_id, m.ticker, m.signal_type)].append(m)
    return [sorted(g, key=lambda r: r.old_date, reverse=descending) for g in groups.values()]


def apply_moves(engine: Engine, moves: list[Redate], *, audit_path: Path, forward: bool = True) -> dict[str, int]:
    """Run each ticker's chain in one transaction, ascending (``forward``) or descending."""
    moved = blocked = 0
    with audit_path.open("x", encoding="utf-8") as audit:
        for chain in _by_ticker(moves, descending=not forward):
            common.check_window()
            done: list[Redate] = []
            with engine.begin() as conn:
                for m in chain:
                    res = conn.execute(_MOVE_SQL, _params(m, forward=forward))
                    if res.rowcount == 1:
                        done.append(m)
                    else:
                        blocked += 1
            for m in done:  # logged after the chain committed
                audit.write(json.dumps({
                    "id": m.id, "source_id": m.source_id, "ticker": m.ticker, "signal_type": m.signal_type,
                    "year": m.year, "qtr": m.qtr,
                    "old_date": m.old_date.isoformat(), "new_date": m.new_date.isoformat(),
                }) + "\n")
            audit.flush()
            moved += len(done)
    return {"moved": moved, "not_moved_slot_taken_or_row_changed": blocked}


def read_audit(path: Path) -> list[Redate]:
    """Moves recorded by a previous ``--apply``."""
    moves = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        moves.append(Redate(
            id=int(rec["id"]), source_id=rec["source_id"], ticker=rec["ticker"], signal_type=rec["signal_type"],
            year=int(rec["year"]), qtr=int(rec["qtr"]),
            old_date=date.fromisoformat(rec["old_date"]), new_date=date.fromisoformat(rec["new_date"]),
        ))
    return moves


def run(
    engine: Engine,
    *,
    apply: bool,
    audit_path: Path | None,
    today: date | None = None,
) -> dict[str, Any]:
    """Plan (and with ``apply`` execute) the re-date."""
    today = today or datetime.now(timezone.utc).date()
    common.check_window()
    with engine.connect() as conn:
        if not apply and engine.dialect.name == "postgresql":
            common.assert_read_only(conn)
        plan = plan_redate(load_rows(conn))
    report: dict[str, Any] = {
        "mode": "apply" if apply else "dry_run",
        "run_at": datetime.now(timezone.utc).isoformat(),
        "summary": summarize(plan, today),
    }
    if apply:
        assert audit_path is not None
        report["applied"] = apply_moves(engine, plan.moves, audit_path=audit_path)
    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write (default: dry run, read-only session)")
    ap.add_argument("--revert", type=Path, metavar="AUDIT_LOG", help="undo the moves recorded in an audit log")
    ap.add_argument("--audit-log", type=Path, help="new JSONL file recording every move (required with --apply/--revert)")
    ap.add_argument("--db-url-env", help="env var holding the database URL (default: config.settings.DB_URL)")
    ap.add_argument("--out", type=Path, help="also write the JSON report here (must not exist)")
    args = ap.parse_args(argv)

    if (args.apply or args.revert) and not args.audit_log:
        print("--apply and --revert require --audit-log", file=sys.stderr)
        return 2
    if args.apply and args.revert:
        print("--apply and --revert are exclusive", file=sys.stderr)
        return 2
    for path in (args.audit_log, args.out):
        if path and path.exists():
            print(f"refusing to overwrite {path}", file=sys.stderr)
            return 2
    try:
        common.check_window()
    except common.WindowClosed as exc:
        print(str(exc), file=sys.stderr)
        return 3

    writing = bool(args.apply or args.revert)
    engine = common.open_engine(
        common.database_url(args.db_url_env), read_only=not writing, application_name="qq_gov_contracts_redate",
    )
    try:
        if args.revert:
            report = {
                "mode": "revert",
                "applied": apply_moves(engine, read_audit(args.revert), audit_path=args.audit_log, forward=False),
            }
        else:
            report = run(engine, apply=args.apply, audit_path=args.audit_log)
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
