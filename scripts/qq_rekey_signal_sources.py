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

Run order (important)
---------------------
Run this **before the next QuiverQuant pull**, never after one. A pull between the
deploy and this script writes keyed rows next to the legacy rows (every one of them
then a conflict this script can only skip, and a duplicate that trust_scorer,
lever_pullers and the money-flow layer count twice). The writer therefore guards
itself: until the transition marker file exists (``~/.grid/quiverquant_transition_done``
or ``$GRID_QQ_TRANSITION_DONE_FILE``) the four act-keyed endpoints and gov_contracts
return SKIPPED (a warning in the log, no API call, no write), including the overdue pull
the scheduler fires on its first tick after a deploy restart. The procedure:

1. deploy; confirm the scheduler records ``quiverquant`` as skipped (marker absent);
2. dry run both transition scripts;
3. apply both: this one first with ``--before <today - 45d>``, then without;
4. ``mkdir -p ~/.grid && touch ~/.grid/quiverquant_transition_done`` to release the
   writer. (Re-key and re-date counts are 0 conflicts as long as no pull got through.)

The marker path is under ``$HOME`` by default: run the scripts as the scheduler's user
(``grid``), or set ``GRID_QQ_TRANSITION_DONE_FILE`` to the same absolute path for the
service and the scripts. (The held-pull warning in the service log prints the path it checks.)

``--apply`` and ``--revert`` refuse to run once the marker exists (``--no-guard-check``
overrides, if QuiverQuant is paused another way). A non-zero ``conflicts_skipped``
means a pull got through before the apply.

Safety
------
* Dry run by default: a read-only session (``default_transaction_read_only=on``),
  2 s statement timeout, 1 s lock timeout, nothing is written.
* Both the dry run and ``--apply`` refuse to start, and refuse to continue between
  writes and COMMIT, inside 03:30-10:30Z, 10:58-11:12Z and weekday 13:25-14:20Z.
  Writes also refuse within five seconds of a blackout or once the marker appears.
* ``--apply`` requires ``--audit-log`` (a new file): one JSON line per move with
  ``direction``, ``id``, ``old_source_id`` and ``new_source_id``. Each UPDATE
  re-checks that the target key is still free, so a concurrent writer cannot make it
  collide.
* ``--apply`` commits in batches of 1-50 (``--batch-size``); ``--max-moves`` stops early. A
  batch that hits the lock or statement timeout (a concurrent uncommitted write) is
  rolled back, counted in the report (``batches_skipped_timeout``, with the skipped
  row ids) and skipped; the run continues. Skips require operator review before retry.
  COMMIT/connection uncertainty and audit I/O errors stop without automatic replay;
  failures retain the acknowledged committed prefix, which can differ from the audit
  after an audit-device failure and never includes an uncertain COMMIT.
* ``--revert AUDIT_LOG`` undoes an apply from its audit log.

Identity limits to know
-----------------------
The key is the whole identity, so an act whose identity changes between pulls gets a
second key: House/Senate rows that gain or lose a ``BioGuideID`` (the key falls back to
the member name), an amended Form 4 (shares or price restated) and a lobbying filing
whose Amount is restated. The dry run counts the payloads at risk
(``without_bioguide``, ``partial_identity_rows``); ``--probe-duplicates`` (read-only,
run after pulls resume) counts House/Senate acts that now exist under two keys.

Side effect to know before applying
-----------------------------------
``intelligence/signal_extractor`` de-duplicates on the composite
``source_type:source_id:ticker`` + ``signal_date``. A re-keyed row inside the
extractor's look-back (45 days, ``GRID_EXTRACTOR_LOOKBACK_DAYS``) is therefore
extracted into ``signal_data`` once more under its new id; the writer change alone
does the same for any act it re-writes under a keyed id. The report counts the
re-keyed rows involved (``in_extractor_window``); ``--before`` leaves them out of a run.

Usage (grid-svr; see the PR for the run procedure)
--------------------------------------------------
    cd /data/grid_v4/grid_release
    set -a; . /home/grid/grid_v4/grid_repo/.env; set +a
    PY=/home/grid/grid_v4/venv/bin/python
    $PY -m scripts.qq_rekey_signal_sources                                    # dry run
    $PY -m scripts.qq_rekey_signal_sources --apply --before <today-45d> \\
        --audit-log ~/research/qq_rekey_audit_old.jsonl                       # older than 45 days
    $PY -m scripts.qq_rekey_signal_sources --apply \\
        --audit-log ~/research/qq_rekey_audit_rest.jsonl                      # the rest
    $PY -m scripts.qq_rekey_signal_sources --probe-duplicates                 # after pulls resume
    $PY -m scripts.qq_rekey_signal_sources --revert ~/research/qq_rekey_audit_old.jsonl \\
        --audit-log ~/research/qq_rekey_revert.jsonl
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
    has_bioguide,
    is_partial_identity,
    legacy_source_id,
    member_loose_key,
    parse_payload,
    source_id_for,
)
from scripts import qq_transition_common as common
from ingestion.altdata import quiverquant_transactions as tx

DEFAULT_BATCH_SIZE = tx.MAX_WRITE_ROWS
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
    partial_identity: int = 0
    without_bioguide: int = 0


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
        endpoint = KEYED_SOURCE_TYPES[source_type]
        plan.partial_identity += is_partial_identity(endpoint, payload)
        if endpoint in ("house_trading", "senate_trading") and not has_bioguide(payload):
            plan.without_bioguide += 1
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
        # identities with a missing field (a "?" part): still moved, but an act whose
        # payload later gains the field gets a second key
        "partial_identity_rows": plan.partial_identity,
        "without_bioguide": plan.without_bioguide if plan.source_type in (
            "quiverquant:house", "quiverquant:senate") else None,
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
       AND ticker = :ticker AND signal_date = :signal_date AND signal_type = :signal_type
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
    direction: str = "forward",
    guard_check: bool = True,
) -> dict[str, Any]:
    """Execute planned moves in batches of 1-50 rows, auditing acknowledged commits.

    A batch that hits ``lock_timeout`` / ``statement_timeout`` is rolled back whole,
    reported and skipped; any other database error propagates.
    """
    tx.validate_batch_size(batch_size)
    if max_moves is not None and max_moves < 0:
        raise ValueError("max_moves must be nonnegative")
    moved = 0
    lost_race = 0
    timeout_batches = 0
    skipped_ids: list[int] = []
    todo = moves if max_moves is None else moves[:max_moves]
    with audit_path.open("x", encoding="utf-8") as audit:
        for start in range(0, len(todo), batch_size):
            batch = todo[start:start + batch_size]
            done: list[Move] = []
            raced = 0
            try:
                with tx.write_transaction(engine, guard=lambda: common.write_guard(guard_check=guard_check)) as (conn, check):
                    for m in batch:
                        check()
                        res = conn.execute(_MOVE_SQL, {
                            "id": m.id, "source_type": m.source_type, "old_id": m.old_source_id,
                            "new_id": m.new_source_id, "ticker": m.ticker,
                            "signal_date": m.signal_date, "signal_type": m.signal_type,
                        })
                        if res.rowcount == 1:
                            done.append(m)
                        else:
                            raced += 1
            except Exception as exc:
                if tx.is_connection_error(exc) or not common.is_lock_or_timeout(exc):
                    common.preserve_committed(exc, moved)
                    raise
                timeout_batches += 1
                skipped_ids.extend(m.id for m in batch)
                continue
            lost_race += raced
            moved += len(done)  # acknowledged COMMIT, even if the audit device then fails
            try:
                common.append_audit(audit, [{
                    "direction": direction, "id": m.id, "source_type": m.source_type,
                    "old_source_id": m.old_source_id, "new_source_id": m.new_source_id,
                } for m in done])
            except Exception as exc:
                common.preserve_committed(exc, moved)
                raise
    return {
        "moved": moved,
        "not_moved_target_taken_or_row_changed": lost_race,
        "batches_skipped_timeout": timeout_batches,
        "skipped_timeout_rows": len(skipped_ids),
        "skipped_timeout_ids": skipped_ids[:SAMPLE_LIMIT * 10],
    }


def read_audit(path: Path) -> list[dict[str, Any]]:
    """Forward moves recorded by a previous ``--apply``."""
    records = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        if rec.get("direction", "forward") == "forward":
            records.append(rec)
    return records


_REVERT_FETCH_SQL = text(
    "SELECT ticker, signal_date, signal_type FROM signal_sources WHERE id = :id AND source_type = :source_type"
)


def revert_moves(engine: Engine, audit_in: Path, *, batch_size: int, audit_path: Path, guard_check: bool = True) -> dict[str, Any]:
    """Undo an apply: each row goes back to its old source_id (if its key there is free)."""
    tx.validate_batch_size(batch_size)
    reverse: list[Move] = []
    with engine.connect() as conn:
        for rec in read_audit(audit_in):
            common.check_window()
            row = conn.execute(_REVERT_FETCH_SQL, {"id": int(rec["id"]), "source_type": rec["source_type"]}).fetchone()
            sdate = as_date(row[1]) if row is not None else None
            if row is None or sdate is None:
                continue
            reverse.append(Move(
                id=int(rec["id"]), source_type=rec["source_type"], ticker=str(row[0]), signal_date=sdate,
                signal_type=str(row[2]), old_source_id=rec["new_source_id"], new_source_id=rec["old_source_id"],
            ))
    return apply_moves(engine, reverse, batch_size=batch_size, audit_path=audit_path, direction="revert", guard_check=guard_check)


_KEYED_ROWS_SQL = text(
    """
    SELECT id, source_id, ticker, signal_date, signal_type, signal_value
      FROM signal_sources
     WHERE source_type = :source_type AND source_id <> :legacy
    """
)


def probe_duplicates(conn: Connection) -> dict[str, Any]:
    """House/Senate acts stored under two keys because BioGuideID appeared or disappeared.

    Groups keyed rows by (ticker, date, signal_type, name + Transaction + Range) and counts
    groups holding more than one distinct ``source_id``. Read-only; counts and row ids only.
    """
    report: dict[str, Any] = {}
    for st in ("quiverquant:house", "quiverquant:senate"):
        endpoint = KEYED_SOURCE_TYPES[st]
        legacy = legacy_source_id(endpoint)
        groups: dict[tuple, dict[str, int]] = defaultdict(dict)
        rows = 0
        result = conn.execute(
            _KEYED_ROWS_SQL, {"source_type": st, "legacy": legacy}, execution_options={"stream_results": True}
        )
        for row in result.mappings():
            rows += 1
            payload = parse_payload(row["signal_value"])
            loose = member_loose_key(endpoint, payload)
            sdate = as_date(row["signal_date"])
            if loose is None or sdate is None:
                continue
            groups[(str(row["ticker"]), sdate, str(row["signal_type"]), loose)][str(row["source_id"])] = int(row["id"])
        multi = [g for g in groups.values() if len(g) > 1]
        report[st] = {
            "keyed_rows": rows,
            "acts_under_two_or_more_keys": len(multi),
            "rows_involved": sum(len(g) for g in multi),
            "sample_row_ids": [sorted(g.values()) for g in multi[:SAMPLE_LIMIT]],
        }
    return report


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
    guard_check: bool = True,
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
            engine, all_moves, batch_size=batch_size, audit_path=audit_path, max_moves=max_moves, guard_check=guard_check
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
    ap.add_argument("--revert", type=Path, metavar="AUDIT_LOG", help="undo the moves recorded in an audit log")
    ap.add_argument("--probe-duplicates", action="store_true",
                    help="read-only: count House/Senate acts stored under two keys (run after pulls resume)")
    ap.add_argument("--no-guard-check", action="store_true",
                    help="allow --apply/--revert although the transition marker exists (QuiverQuant paused another way)")
    ap.add_argument("--db-url-env", help="env var holding the database URL (default: config.settings.DB_URL)")
    ap.add_argument("--out", type=Path, help="also write the JSON report here (must not exist)")
    args = ap.parse_args(argv)

    try:
        tx.validate_batch_size(args.batch_size)
        if args.max_moves is not None and args.max_moves < 0:
            raise ValueError("--max-moves must be nonnegative")
        before = date.fromisoformat(args.before) if args.before else None
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    if (args.apply or args.revert) and not args.audit_log:
        print("--apply and --revert require --audit-log", file=sys.stderr)
        return 2
    if sum(bool(x) for x in (args.apply, args.revert, args.probe_duplicates)) > 1:
        print("--apply, --revert and --probe-duplicates are exclusive", file=sys.stderr)
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

    writing = bool(args.apply or args.revert)
    if writing and not args.no_guard_check:
        try:
            common.require_guard_closed()
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 4
    engine = common.open_engine(
        common.database_url(args.db_url_env), read_only=not writing,
        application_name="qq_rekey_signal_sources",
    )
    try:
        if args.revert:
            report = {"mode": "revert", "applied": revert_moves(
                engine, args.revert, batch_size=args.batch_size, audit_path=args.audit_log, guard_check=not args.no_guard_check)}
        elif args.probe_duplicates:
            with engine.connect() as conn:
                if engine.dialect.name == "postgresql":
                    common.assert_read_only(conn)
                report = {"mode": "probe_duplicates", "result": probe_duplicates(conn)}
        else:
            report = run(
                engine,
                source_types=args.source_type or sorted(KEYED_SOURCE_TYPES),
                apply=args.apply,
                audit_path=args.audit_log,
                batch_size=args.batch_size,
                max_moves=args.max_moves,
                before=before, guard_check=not args.no_guard_check,
            )
    except common.WindowClosed as exc:
        print(f"{exc}; acknowledged committed rows={getattr(exc, 'committed_rows', 0)}", file=sys.stderr)
        return 3
    except Exception as exc:
        print(json.dumps({"status": "ABORTED", "error_type": type(exc).__name__,
                          "reason": str(exc) if isinstance(exc, ValueError) else "database/audit failure; inspect private evidence",
                          "acknowledged_committed_rows": getattr(exc, "committed_rows", 0),
                          "commit_uncertain": getattr(exc, "commit_uncertain", False),
                          "action": "stop; reconcile database and audit before a separately reviewed retry"}), file=sys.stderr)
        return 5
    finally:
        engine.dispose()
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.out:
        args.out.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
