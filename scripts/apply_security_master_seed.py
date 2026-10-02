#!/usr/bin/env python3
"""Load a frozen all-issuers seed artifact into ``security_master`` / ``security_identifiers``.

Takes the artifact written by ``scripts/build_security_master_all_issuers.py``
(``security_master_seed.jsonl`` plus its ``receipt.json``) and INSERTs it.

* **Dry run is the default.** Without ``--db-url`` it only counts the artifact. With ``--db-url`` it also
  reads the current ``security_master`` / ``security_identifiers`` keys in a read-only transaction and
  reports exactly what an apply would insert and what it would skip. It writes nothing to any database.
* ``--apply`` writes, and needs ``--db-url`` (or ``config.settings.DB_URL`` from the environment).
* **No database connection is opened between 03:30 and 10:30 UTC** (the nightly ``pg_dump`` window),
  for a dry run or an apply. A long apply re-checks before every batch and stops cleanly if the
  window starts, leaving a partial but consistent state (every batch is its own transaction and the
  whole load is idempotent: run it again after 10:30Z and only the rest is inserted).
* Statements are ``INSERT ... ON CONFLICT DO NOTHING`` in batches, each with ``statement_timeout`` and
  ``lock_timeout``. **It never UPDATEs or DELETEs a row**, and never touches a table other than the
  two above (not ``security_sector_membership``).
* Existing rows are never rewritten: a CIK that already has an entity is skipped, and so is any
  ``(entity_id, id_scheme, id_value, valid_from)`` that already exists. A CIK identifier is also
  skipped when the entity already has that CIK value under another ``valid_from`` (the Technology seed
  dated its own at the seed day). A new ticker row that overlaps a DIFFERENT entity's ticker row already
  in the database is inserted with ``conflict_flag`` true and ``is_primary`` false and reported, so the
  existing row keeps winning ties.
* A run writes a receipt (``--receipt``, default next to the artifact). It is written when the run starts
  and rewritten when it ends.

Rollback (printed, never executed). Every row this loader adds has ``source = 'all_issuers_v1'`` (entities)
or ``source LIKE 'all_issuers_v1:%'`` (identifiers), so it can be removed without touching the
Technology seed::

    DELETE FROM security_identifiers WHERE source LIKE 'all_issuers_v1:%';
    DELETE FROM security_master      WHERE source = 'all_issuers_v1';

Run both outside 03:30-10:30 UTC and before ``people_events.security_id`` references any of the new
entities (that foreign key would block the delete by design).

Usage::

    python -m scripts.apply_security_master_seed --seed-dir <dir>                       # count only
    python -m scripts.apply_security_master_seed --seed-dir <dir> --db-url ...          # dry run with diff
    python -m scripts.apply_security_master_seed --seed-dir <dir> --db-url ... --apply \\
        --expect-output-sha256 <sha256 from receipt.json>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from sqlalchemy import text  # noqa: E402

from intelligence.people_events_pipeline.rules import normalize_ticker  # noqa: E402

SEED_FILE = "security_master_seed.jsonl"
ENTITY_SOURCE = "all_issuers_v1"
BLOCKED_START = time(3, 30)
BLOCKED_END = time(10, 30)
DEFAULT_BATCH = 2000
DEFAULT_STATEMENT_TIMEOUT_MS = 60_000
DEFAULT_LOCK_TIMEOUT_MS = 5_000

ROLLBACK_SQL = (
    "DELETE FROM security_identifiers WHERE source LIKE 'all_issuers_v1:%';\n"
    "DELETE FROM security_master WHERE source = 'all_issuers_v1';"
)

_INSERT_SM = """
INSERT INTO security_master
    (entity_id, cik, name, security_type, is_active, delisted_at, delisted_reason, delisted_basis,
     sic, source, provenance)
SELECT r.entity_id, r.cik, r.name, r.security_type, r.is_active, r.delisted_at, r.delisted_reason,
       r.delisted_basis, r.sic, r.source, COALESCE(r.provenance, '{}'::jsonb)
FROM jsonb_to_recordset(CAST(:payload AS jsonb)) AS r(
    entity_id text, cik integer, name text, security_type text, is_active boolean, delisted_at date,
    delisted_reason text, delisted_basis text, sic integer, source text, provenance jsonb)
ON CONFLICT DO NOTHING
"""

_INSERT_SI = """
INSERT INTO security_identifiers
    (entity_id, id_scheme, id_value, valid_from, valid_to, is_primary, source, conflict_flag, conflict_detail)
SELECT r.entity_id, r.id_scheme, r.id_value, r.valid_from, r.valid_to, r.is_primary, r.source,
       r.conflict_flag, r.conflict_detail
FROM jsonb_to_recordset(CAST(:payload AS jsonb)) AS r(
    entity_id text, id_scheme text, id_value text, valid_from date, valid_to date, is_primary boolean,
    source text, conflict_flag boolean, conflict_detail jsonb)
ON CONFLICT (entity_id, id_scheme, id_value, valid_from) DO NOTHING
"""

# ``set_config(..., true)`` is ``SET LOCAL`` with a bound value (no SQL string building).
_SET_LOCAL = text("SELECT set_config(:k, :v, true)")
_READ_ENTITIES = "SELECT entity_id, cik FROM security_master"
_READ_IDENTIFIERS = (
    "SELECT entity_id, id_scheme, id_value, valid_from, valid_to FROM security_identifiers "
    "WHERE id_scheme IN ('cik', 'ticker')"
)
_COUNTS = {
    "security_master": "SELECT count(*) FROM security_master",
    "security_identifiers": "SELECT count(*) FROM security_identifiers",
}


class WindowRefused(RuntimeError):
    """Raised instead of opening (or continuing to use) a connection inside the nightly backup window."""


def in_blocked_window(now: Optional[datetime] = None) -> bool:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return BLOCKED_START <= now.time().replace(tzinfo=None) < BLOCKED_END


def check_window(now: Optional[datetime] = None) -> None:
    if in_blocked_window(now):
        raise WindowRefused("refusing to use the database between 03:30 and 10:30 UTC (nightly backup window)")


# --- artifact -------------------------------------------------------------------------------


@dataclass
class Seed:
    security_master: list[dict[str, Any]]
    security_identifiers: list[dict[str, Any]]
    sha256: str
    receipt: Optional[dict[str, Any]] = None


def load_seed(seed_dir: Path, *, expect_sha256: Optional[str] = None, require_receipt: bool = False) -> Seed:
    """Read the artifact and verify it against its receipt (and ``expect_sha256`` when given)."""
    path = Path(seed_dir) / SEED_FILE
    receipt_path = Path(seed_dir) / "receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8")) if receipt_path.exists() else None
    if receipt is None and require_receipt:
        raise ValueError(f"{receipt_path} is missing; refusing to apply an artifact without its receipt")
    data = path.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if receipt is not None and receipt["output"]["sha256"] != digest:
        raise ValueError("artifact sha256 does not match receipt.json: the file was changed after it was built")
    if expect_sha256 is not None and expect_sha256 != digest:
        raise ValueError(f"artifact sha256 {digest} != --expect-output-sha256 {expect_sha256}")
    sm: list[dict[str, Any]] = []
    si: list[dict[str, Any]] = []
    for raw in data.splitlines():
        rec = json.loads(raw)
        kind = rec.pop("t")
        (sm if kind == "sm" else si).append(rec)
    return Seed(sm, si, digest, receipt)


# --- existing state + plan ------------------------------------------------------------------


@dataclass
class Existing:
    entities: set[str] = field(default_factory=set)
    cik_to_entity: dict[int, str] = field(default_factory=dict)
    identifier_keys: set[tuple[str, str, str, str]] = field(default_factory=set)
    entity_cik_values: set[tuple[str, str]] = field(default_factory=set)
    tickers: dict[str, list[tuple[str, str, Optional[str]]]] = field(default_factory=lambda: defaultdict(list))


def _iso(d: Any) -> Optional[str]:
    return None if d is None else (d.isoformat() if hasattr(d, "isoformat") else str(d))


def build_existing(entity_rows: Iterable[tuple[Any, Any]], identifier_rows: Iterable[tuple[Any, ...]]) -> Existing:
    ex = Existing()
    for entity_id, cik in entity_rows:
        ex.entities.add(entity_id)
        if cik is not None:
            ex.cik_to_entity[int(cik)] = entity_id
    for entity_id, scheme, value, vfrom, vto in identifier_rows:
        ex.identifier_keys.add((entity_id, scheme, value, _iso(vfrom) or ""))
        if scheme == "cik":
            ex.entity_cik_values.add((entity_id, str(value)))
        elif scheme == "ticker":
            norm = normalize_ticker(value)
            if norm:
                ex.tickers[norm].append((entity_id, _iso(vfrom) or "", _iso(vto)))
    return ex


@dataclass
class Plan:
    sm_inserts: list[dict[str, Any]] = field(default_factory=list)
    si_inserts: list[dict[str, Any]] = field(default_factory=list)
    skipped: Counter = field(default_factory=Counter)
    cik_collisions: list[dict[str, Any]] = field(default_factory=list)
    db_ticker_overlaps: list[dict[str, Any]] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        return {
            "security_master_to_insert": len(self.sm_inserts),
            "security_identifiers_to_insert": len(self.si_inserts),
            "skipped": dict(sorted(self.skipped.items())),
            "cik_collisions": len(self.cik_collisions),
            "cik_collision_examples": self.cik_collisions[:20],
            "new_ticker_rows_overlapping_existing_other_entity": len(self.db_ticker_overlaps),
            "overlap_examples": self.db_ticker_overlaps[:20],
        }


def _window_overlap(a_from: str, a_to: Optional[str], b_from: str, b_to: Optional[str]) -> bool:
    start = max(a_from, b_from)
    ends = [e for e in (a_to, b_to) if e is not None]
    return not ends or start <= min(ends)


def plan_inserts(seed: Seed, existing: Optional[Existing] = None) -> Plan:
    """Pure: which artifact rows an apply would insert given the database's current keys."""
    ex = existing or Existing()
    plan = Plan()
    new_entities: set[str] = set()
    blocked: set[str] = set()
    for row in seed.security_master:
        eid = row["entity_id"]
        if eid in ex.entities:
            plan.skipped["security_master:entity_exists"] += 1
            continue
        owner = ex.cik_to_entity.get(int(row["cik"])) if row.get("cik") is not None else None
        if owner is not None and owner != eid:
            plan.skipped["security_master:cik_held_by_other_entity"] += 1
            plan.cik_collisions.append({"entity_id": eid, "cik": row["cik"], "existing_entity_id": owner})
            blocked.add(eid)
            continue
        new_entities.add(eid)
        plan.sm_inserts.append(row)
    for row in seed.security_identifiers:
        eid = row["entity_id"]
        if eid in blocked:
            plan.skipped["security_identifiers:entity_blocked_by_cik_collision"] += 1
            continue
        if eid not in new_entities and eid not in ex.entities:
            plan.skipped["security_identifiers:no_entity"] += 1
            continue
        if (eid, row["id_scheme"], row["id_value"], row["valid_from"]) in ex.identifier_keys:
            plan.skipped["security_identifiers:exists"] += 1
            continue
        if row["id_scheme"] == "cik" and (eid, row["id_value"]) in ex.entity_cik_values:
            plan.skipped["security_identifiers:cik_already_identified"] += 1
            continue
        if row["id_scheme"] == "ticker":
            row = _flag_against_existing(row, ex, plan)
        plan.si_inserts.append(row)
    return plan


def _flag_against_existing(row: dict[str, Any], ex: Existing, plan: Plan) -> dict[str, Any]:
    norm = normalize_ticker(row["id_value"])
    others = sorted({
        eid for eid, vfrom, vto in ex.tickers.get(norm or "", [])
        if eid != row["entity_id"] and _window_overlap(row["valid_from"], row["valid_to"], vfrom, vto)
    })
    if not others:
        return row
    detail = dict(row.get("conflict_detail") or {})
    detail.update({"kind": detail.get("kind") or "overlap_with_existing_db_row", "existing_db_entities": others[:25]})
    plan.db_ticker_overlaps.append({"entity_id": row["entity_id"], "ticker": row["id_value"], "existing_entities": others[:5]})
    return {**row, "conflict_flag": True, "is_primary": False, "conflict_detail": detail}


# --- database -------------------------------------------------------------------------------


def _redacted_target(url: str) -> dict[str, Any]:
    from sqlalchemy.engine import make_url

    u = make_url(url)
    return {"host": u.host, "port": u.port, "database": u.database}


def read_existing(
    engine: Any,
    *,
    statement_timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS,
    now: Optional[datetime] = None,
) -> tuple[Existing, dict[str, int]]:
    """One read-only transaction: entity keys, cik/ticker identifier keys, and the two table counts."""
    check_window(now)
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text("SET TRANSACTION READ ONLY"))
            conn.execute(_SET_LOCAL, {"k": "statement_timeout", "v": f"{int(statement_timeout_ms)}ms"})
            entities = [tuple(r) for r in conn.execute(text(_READ_ENTITIES))]
            identifiers = [tuple(r) for r in conn.execute(text(_READ_IDENTIFIERS))]
            counts = {name: int(conn.execute(text(sql)).scalar() or 0) for name, sql in _COUNTS.items()}
    return build_existing(entities, identifiers), counts


def _batches(rows: list[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
    for i in range(0, len(rows), size):
        yield rows[i:i + size]


def apply_plan(
    engine: Any,
    plan: Plan,
    *,
    batch_size: int = DEFAULT_BATCH,
    statement_timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS,
    lock_timeout_ms: int = DEFAULT_LOCK_TIMEOUT_MS,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    on_progress: Optional[Callable[[dict[str, int]], None]] = None,
) -> dict[str, int]:
    """INSERT ... ON CONFLICT DO NOTHING in batches. Returns rows actually inserted per table."""
    inserted = {"security_master": 0, "security_identifiers": 0}
    for table, sql, rows in (
        ("security_master", _INSERT_SM, plan.sm_inserts),
        ("security_identifiers", _INSERT_SI, plan.si_inserts),
    ):
        for batch in _batches(rows, batch_size):
            check_window(now())
            with engine.begin() as conn:
                conn.execute(_SET_LOCAL, {"k": "statement_timeout", "v": f"{int(statement_timeout_ms)}ms"})
                conn.execute(_SET_LOCAL, {"k": "lock_timeout", "v": f"{int(lock_timeout_ms)}ms"})
                res = conn.execute(text(sql), {"payload": json.dumps(batch, separators=(",", ":"))})
                inserted[table] += int(res.rowcount or 0)
            if on_progress:
                on_progress(dict(inserted))
    return inserted


# --- receipt + CLI --------------------------------------------------------------------------


def write_receipt(path: Path, receipt: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(receipt, indent=1, sort_keys=True, default=str) + "\n", encoding="utf-8")
    tmp.replace(path)


def run(args: argparse.Namespace, *, now: Optional[Callable[[], datetime]] = None, engine_factory: Optional[Callable[[str], Any]] = None) -> dict[str, Any]:
    clock = now or (lambda: datetime.now(timezone.utc))
    seed = load_seed(args.seed_dir, expect_sha256=args.expect_output_sha256, require_receipt=args.apply)
    started = clock()
    receipt: dict[str, Any] = {
        "mode": "apply" if args.apply else "dry_run",
        "status": "started",
        "started_at": started.isoformat(),
        "artifact": {"dir": str(args.seed_dir), "sha256": seed.sha256, "rows": {
            "security_master": len(seed.security_master), "security_identifiers": len(seed.security_identifiers)}},
        "code_sha": (seed.receipt or {}).get("code"),
        "rollback_sql": ROLLBACK_SQL,
        "writes_to_database": False,
    }
    db_url = args.db_url
    if args.apply and not db_url:
        from config import settings

        db_url = settings.DB_URL
    receipt_path = Path(args.receipt) if args.receipt else Path(args.seed_dir) / (
        f"{receipt['mode']}_receipt_{started.strftime('%Y%m%dT%H%M%SZ')}.json")
    receipt["receipt_path"] = str(receipt_path)

    if db_url is None:
        plan = plan_inserts(seed, None)
        receipt.update(status="counted_artifact_only", plan=plan.summary(),
                       note="no --db-url: nothing was read from any database, so every row counts as new")
        write_receipt(receipt_path, receipt)
        return receipt

    check_window(clock())
    from sqlalchemy import create_engine

    engine = (engine_factory or create_engine)(db_url)
    receipt["target"] = _redacted_target(db_url)
    try:
        existing, before = read_existing(engine, statement_timeout_ms=args.statement_timeout_ms, now=clock())
        plan = plan_inserts(seed, existing)
        receipt.update(before_counts=before, plan=plan.summary())
        if not args.apply:
            receipt["status"] = "dry_run_complete"
            write_receipt(receipt_path, receipt)
            return receipt
        receipt["writes_to_database"] = True
        write_receipt(receipt_path, receipt)
        inserted = apply_plan(
            engine, plan, batch_size=args.batch_size, statement_timeout_ms=args.statement_timeout_ms,
            lock_timeout_ms=args.lock_timeout_ms, now=clock,
            on_progress=lambda p: receipt.update(inserted_so_far=p))
        _, after = read_existing(engine, statement_timeout_ms=args.statement_timeout_ms, now=clock())
        receipt.update(status="applied", inserted=inserted, after_counts=after, finished_at=clock().isoformat())
    except WindowRefused as exc:
        receipt.update(status="stopped_backup_window", error=str(exc), finished_at=clock().isoformat())
    except Exception as exc:  # noqa: BLE001 - the receipt must record any failure before it propagates
        receipt.update(status="failed", error=f"{type(exc).__name__}: {exc}", finished_at=clock().isoformat())
        write_receipt(receipt_path, receipt)
        raise
    finally:
        engine.dispose()
    write_receipt(receipt_path, receipt)
    return receipt


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed-dir", type=Path, required=True, help="directory holding security_master_seed.jsonl + receipt.json")
    ap.add_argument("--apply", action="store_true", help="write (default: dry run). Needs --db-url or the settings DB_URL")
    ap.add_argument("--db-url", help="SQLAlchemy URL. Without --apply it is only read, in a read-only transaction")
    ap.add_argument("--expect-output-sha256", help="refuse unless the artifact has this sha256 (from receipt.json)")
    ap.add_argument("--batch-size", type=int, default=DEFAULT_BATCH)
    ap.add_argument("--statement-timeout-ms", type=int, default=DEFAULT_STATEMENT_TIMEOUT_MS)
    ap.add_argument("--lock-timeout-ms", type=int, default=DEFAULT_LOCK_TIMEOUT_MS)
    ap.add_argument("--receipt", type=Path, help="receipt path (default: next to the artifact)")
    args = ap.parse_args(argv)
    try:
        receipt = run(args)
    except WindowRefused as exc:
        print(str(exc), file=sys.stderr)
        return 3
    print(json.dumps({k: receipt.get(k) for k in (
        "mode", "status", "artifact", "target", "before_counts", "plan", "inserted", "after_counts", "receipt_path")},
        indent=2, sort_keys=True, default=str))
    print("rollback (not executed):\n" + ROLLBACK_SQL)
    return 0 if receipt["status"] in ("counted_artifact_only", "dry_run_complete", "applied") else 4


if __name__ == "__main__":
    raise SystemExit(main())
