#!/usr/bin/env python3
"""Reviewed, reversible removal of ``dollar_flows`` rows that break the stored-row contract.

``intelligence.dollar_flows.STORED_ROW_CONTRACTS`` names, per source type, the
only shape the current normalizer writes (today: every ``13f`` row has
direction ``observed``). The daily writer rebuilds only the dates inside its
window, so rows an older rule wrote at earlier dates keep their old shape;
``scripts/run_dollar_flows.py`` reports them and exits 3, and the CHECK
constraint ``dollar_flows_13f_observed_only`` stops any writer adding more.
This script is the only way such rows are removed. Every step is explicit:

``export --out FILE``
    Read-only. Writes every row that breaks a contract, every column, to a
    new JSON manifest (never overwrites), and prints its row count, id list,
    amount sum and the sha256 of its rows. Review the manifest.

``apply --manifest FILE --expect-count N --expect-sha256 H [--dry-run]``
    One transaction, all or nothing. Takes ``SHARE ROW EXCLUSIVE`` on
    ``dollar_flows`` (the daily writer waits; readers do not). Refuses unless
    the CHECK ``dollar_flows_13f_observed_only`` exists with the expected
    definition (so it never runs before the release that adds it). Re-reads
    the rows that break a contract and refuses unless they are exactly the
    manifest's rows, column for column. Deletes by the manifest's explicit
    id list, refuses unless exactly N rows were deleted and none that break a
    contract remain, then validates the constraint, and only then commits.
    Any refusal or error (a lock timeout, a failed VALIDATE) rolls the whole
    transaction back: nothing is deleted and the same manifest can be applied
    again. ``--dry-run`` does all of it, VALIDATE included, then rolls back.
    Lock scope: the VALIDATE scan (one sequential read of the table) runs
    under the SHARE ROW EXCLUSIVE already held, so readers are never blocked
    and the writer waits for the scan.

``validate [--dry-run]``
    For a table with no rows that break a contract and a NOT VALID
    constraint (a database where the migration found nothing to remove):
    validates it under the same lock, or refuses if any row breaks it.

``restore --manifest FILE --expect-sha256 H [--dry-run]``
    One transaction. Re-inserts the manifest's rows with their original ids
    and ``created_at``; refuses if any id exists. While the constraint exists
    PostgreSQL rejects these rows, so restore refuses up front and prints the
    statement that drops it (a schema change: the operator's decision). The
    database keeps its alembic version, so no later ``alembic upgrade`` puts the
    constraint back; the report prints the statement that re-adds it NOT VALID.

Exit status: 0 done, 2 refused (nothing written). A database error ends in
a traceback (exit 1) with the transaction rolled back.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import text  # noqa: E402
from sqlalchemy.engine import Connection, Engine  # noqa: E402

from intelligence.dollar_flows import (  # noqa: E402
    DOLLAR_FLOWS_CONTRACT_CHECK_SQL,
    DOLLAR_FLOWS_CONTRACT_CONSTRAINT,
)

MANIFEST_KIND = "dollar_flows_stored_contract_repair"
MANIFEST_VERSION = 1
_COLUMNS = (
    "id", "source_type", "actor_name", "ticker", "amount_usd", "direction",
    "confidence", "evidence", "flow_date", "created_at",
)


EXIT_REFUSED = 2
VALIDATE_SQL = f"ALTER TABLE dollar_flows VALIDATE CONSTRAINT {DOLLAR_FLOWS_CONTRACT_CONSTRAINT}"
DROP_SQL = f"ALTER TABLE dollar_flows DROP CONSTRAINT {DOLLAR_FLOWS_CONTRACT_CONSTRAINT}"


def readd_sql(check_sql: str = DOLLAR_FLOWS_CONTRACT_CHECK_SQL) -> str:
    """ADD ... NOT VALID for ``check_sql``; parenthesised whole, so several
    contracts joined by AND stay one CHECK expression."""
    return (
        f"ALTER TABLE dollar_flows ADD CONSTRAINT {DOLLAR_FLOWS_CONTRACT_CONSTRAINT} "
        f"CHECK ({check_sql}) NOT VALID"
    )


READD_SQL = readd_sql()


class Refused(RuntimeError):
    """A precondition failed; nothing was written."""


def _canonical_row(r: Any) -> dict[str, Any]:
    m = dict(r._mapping) if hasattr(r, "_mapping") else dict(r)
    ev = m["evidence"]
    if isinstance(ev, str):
        ev = json.loads(ev)
    amt = m["amount_usd"]
    fd = m["flow_date"]
    ca = m["created_at"]
    if isinstance(ca, datetime):
        ca = (ca if ca.tzinfo else ca.replace(tzinfo=timezone.utc)).astimezone(timezone.utc).isoformat()
    return {
        "id": int(m["id"]),
        "source_type": m["source_type"],
        "actor_name": m["actor_name"],
        "ticker": m["ticker"],
        "amount_usd": str(amt) if isinstance(amt, Decimal) else (None if amt is None else str(Decimal(str(amt)))),
        "direction": m["direction"],
        "confidence": m["confidence"],
        "evidence": ev,
        "flow_date": fd.isoformat() if isinstance(fd, date) else fd,
        "created_at": ca,
    }


def rows_sha256(rows: list[dict[str, Any]]) -> str:
    blob = json.dumps(rows, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(blob.encode("ascii")).hexdigest()


def _read_violations(conn: Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        text(
            f"SELECT {', '.join(_COLUMNS)} FROM dollar_flows "
            f"WHERE NOT ({DOLLAR_FLOWS_CONTRACT_CHECK_SQL}) ORDER BY id"
        )
    ).fetchall()
    return [_canonical_row(r) for r in rows]


def _constraint_state(conn: Connection) -> bool | None:
    """``None`` if the constraint is absent, else whether it is validated."""
    row = conn.execute(
        text(
            "SELECT convalidated FROM pg_constraint "
            "WHERE conname = :c AND conrelid = to_regclass('dollar_flows')"
        ),
        {"c": DOLLAR_FLOWS_CONTRACT_CONSTRAINT},
    ).fetchone()
    return None if row is None else bool(row[0])


def _normalized_def(defn: str) -> str:
    """Compare-form of a CHECK definition: no casts, parentheses or whitespace.

    It equals for PostgreSQL 14 and 16 deparses of our CHECK and rejects other
    operators or operands. Dropping parentheses also drops grouping, which is
    unambiguous for today's single ``a OR b`` contract only; a second contract
    needs a grouping-aware comparison (pinned by a test that fails when one is
    added).
    """
    return re.sub(r"::[a-z_ ]+|[()\s]", "", defn.lower())


EXPECTED_CONSTRAINT_DEF = _normalized_def(f"CHECK ({DOLLAR_FLOWS_CONTRACT_CHECK_SQL})")


def _require_constraint(conn: Connection) -> bool:
    """Refuse unless the expected CHECK exists; return whether it is validated."""
    row = conn.execute(
        text(
            "SELECT contype, convalidated, pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conname = :c AND conrelid = to_regclass('dollar_flows')"
        ),
        {"c": DOLLAR_FLOWS_CONTRACT_CONSTRAINT},
    ).fetchone()
    if row is None:
        raise Refused(
            f"constraint {DOLLAR_FLOWS_CONTRACT_CONSTRAINT} is absent: run this after the release "
            "whose alembic upgrade adds it, so nothing can re-insert the rows"
        )
    got = _normalized_def(row[2]).removesuffix("notvalid")
    if row[0] != "c" or got != EXPECTED_CONSTRAINT_DEF:
        raise Refused(
            f"constraint {DOLLAR_FLOWS_CONTRACT_CONSTRAINT} is not the expected CHECK: "
            f"{row[2]!r} (expected CHECK ({DOLLAR_FLOWS_CONTRACT_CHECK_SQL}))"
        )
    return bool(row[1])


def _lock(conn: Connection) -> None:
    conn.execute(text("SET LOCAL lock_timeout = '5s'"))
    conn.execute(text("SET LOCAL statement_timeout = '60s'"))
    conn.execute(text("LOCK TABLE dollar_flows IN SHARE ROW EXCLUSIVE MODE"))


def build_manifest(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "kind": MANIFEST_KIND,
        "version": MANIFEST_VERSION,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "check_sql": DOLLAR_FLOWS_CONTRACT_CHECK_SQL,
        "count": len(rows),
        "ids": [r["id"] for r in rows],
        "amount_usd_sum": str(sum((Decimal(r["amount_usd"]) for r in rows), Decimal(0))),
        "rows_sha256": rows_sha256(rows),
        "rows": rows,
    }


def load_manifest(path: Path, expect_sha256: str) -> dict[str, Any]:
    man = json.loads(path.read_text())
    if man.get("kind") != MANIFEST_KIND or man.get("version") != MANIFEST_VERSION:
        raise Refused(f"{path} is not a v{MANIFEST_VERSION} {MANIFEST_KIND} manifest")
    rows = man.get("rows") or []
    digest = rows_sha256(rows)
    if digest != man.get("rows_sha256") or digest != expect_sha256:
        raise Refused(
            f"manifest rows sha256 {digest} does not match the manifest's "
            f"{man.get('rows_sha256')} and --expect-sha256 {expect_sha256}"
        )
    if [r["id"] for r in rows] != man.get("ids") or len(rows) != man.get("count"):
        raise Refused("manifest ids/count do not match its rows")
    if len(set(man["ids"])) != len(man["ids"]):
        raise Refused("manifest lists an id twice")
    return man


def export(engine: Engine, out: Path) -> dict[str, Any]:
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text("SET TRANSACTION READ ONLY"))
            rows = _read_violations(conn)
            state = _constraint_state(conn)
    man = build_manifest(rows)
    fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    with os.fdopen(fd, "w") as fh:
        json.dump(man, fh, sort_keys=True, indent=1)
        fh.write("\n")
    return {
        "action": "export",
        "manifest": str(out),
        "count": man["count"],
        "ids": man["ids"],
        "amount_usd_sum": man["amount_usd_sum"],
        "rows_sha256": man["rows_sha256"],
        "constraint": "absent" if state is None else ("validated" if state else "not_valid"),
    }


def apply(engine: Engine, man: dict[str, Any], *, expect_count: int, dry_run: bool) -> dict[str, Any]:
    ids = list(man["ids"])
    if expect_count != len(ids):
        raise Refused(f"--expect-count {expect_count} but the manifest lists {len(ids)} rows")
    if not ids:
        raise Refused("the manifest lists no rows")
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            _lock(conn)
            was_valid = _require_constraint(conn)
            current = _read_violations(conn)
            if current != man["rows"]:
                cur_ids = {r["id"] for r in current}
                raise Refused(
                    "the rows that break the contract now are not the manifest's rows "
                    f"(now {len(current)}, manifest {len(ids)}; only now: "
                    f"{sorted(cur_ids - set(ids))[:20]}, only in manifest: "
                    f"{sorted(set(ids) - cur_ids)[:20]}, or a column changed); re-export"
                )
            deleted = conn.execute(
                text(
                    f"DELETE FROM dollar_flows WHERE id = ANY(:ids) "
                    f"AND NOT ({DOLLAR_FLOWS_CONTRACT_CHECK_SQL})"
                ),
                {"ids": ids},
            ).rowcount
            if deleted != expect_count:
                raise Refused(f"DELETE removed {deleted} rows, expected {expect_count}")
            remaining = len(_read_violations(conn))
            if remaining:
                raise Refused(f"{remaining} rows still break the contract after the delete")
            if not was_valid:
                _validate_in_transaction(conn)
            validated = _require_constraint(conn)
            if not validated:
                raise Refused("the constraint is still NOT VALID after VALIDATE")
            if dry_run:
                trans.rollback()
            else:
                trans.commit()
        except BaseException:
            if trans.is_active:
                trans.rollback()
            raise
    return {
        "action": "apply",
        "dry_run": dry_run,
        "deleted": deleted,
        "remaining_violations": remaining,
        "ids": ids,
        "constraint": "validated",
    }


def _validate_in_transaction(conn: Connection) -> None:
    """VALIDATE inside the caller's locked transaction (a failure rolls it all back)."""
    conn.execute(text(VALIDATE_SQL))


def validate(engine: Engine, *, dry_run: bool) -> dict[str, Any]:
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            _lock(conn)
            was_valid = _require_constraint(conn)
            remaining = len(_read_violations(conn))
            if remaining:
                raise Refused(f"{remaining} rows break the contract; remove them with export/apply first")
            if not was_valid:
                _validate_in_transaction(conn)
            if not _require_constraint(conn):
                raise Refused("the constraint is still NOT VALID after VALIDATE")
            if dry_run:
                trans.rollback()
            else:
                trans.commit()
        except BaseException:
            if trans.is_active:
                trans.rollback()
            raise
    return {"action": "validate", "dry_run": dry_run, "was_validated": was_valid, "constraint": "validated"}


def restore(engine: Engine, man: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    rows = man["rows"]
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            conn.execute(text("SET LOCAL lock_timeout = '5s'"))
            if _constraint_state(conn) is not None:
                raise Refused(
                    f"constraint {DOLLAR_FLOWS_CONTRACT_CONSTRAINT} rejects these rows; restoring "
                    f"them needs it dropped first (a schema change): {DROP_SQL}"
                )
            present = conn.execute(
                text("SELECT id FROM dollar_flows WHERE id = ANY(:ids) ORDER BY id"),
                {"ids": man["ids"]},
            ).scalars().all()
            if present:
                raise Refused(f"{len(present)} manifest ids already exist, e.g. {present[:10]}")
            for r in rows:
                conn.execute(
                    text(
                        "INSERT INTO dollar_flows (id, source_type, actor_name, ticker, amount_usd, "
                        "direction, confidence, evidence, flow_date, created_at) VALUES "
                        "(:id, :source_type, :actor_name, :ticker, CAST(:amount_usd AS NUMERIC), "
                        ":direction, :confidence, CAST(:evidence AS JSONB), CAST(:flow_date AS DATE), "
                        "CAST(:created_at AS TIMESTAMPTZ))"
                    ),
                    {**r, "evidence": None if r["evidence"] is None else json.dumps(r["evidence"])},
                )
            back = [
                _canonical_row(x)
                for x in conn.execute(
                    text(f"SELECT {', '.join(_COLUMNS)} FROM dollar_flows WHERE id = ANY(:ids) ORDER BY id"),
                    {"ids": man["ids"]},
                ).fetchall()
            ]
            if back != rows:
                raise Refused("restored rows do not read back equal to the manifest")
            if dry_run:
                trans.rollback()
            else:
                trans.commit()
        except BaseException:
            if trans.is_active:
                trans.rollback()
            raise
    return {
        "action": "restore",
        "dry_run": dry_run,
        "restored": len(rows),
        "rows_sha256": rows_sha256(back),
        # alembic still records the revision, so nothing re-adds the constraint by itself.
        "re_add_constraint": READD_SQL,
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("export", help="read-only: write the manifest of rows that break a contract")
    e.add_argument("--out", type=Path, required=True)
    a = sub.add_parser("apply", help="delete exactly the manifest's rows in one transaction")
    a.add_argument("--manifest", type=Path, required=True)
    a.add_argument("--expect-count", type=int, required=True)
    a.add_argument("--expect-sha256", required=True)
    a.add_argument("--dry-run", action="store_true")
    v = sub.add_parser("validate", help="validate the constraint on a table with nothing to remove")
    v.add_argument("--dry-run", action="store_true")
    r = sub.add_parser("restore", help="re-insert the manifest's rows in one transaction")
    r.add_argument("--manifest", type=Path, required=True)
    r.add_argument("--expect-sha256", required=True)
    r.add_argument("--dry-run", action="store_true")
    return p


def main(argv: list[str] | None = None, engine: Engine | None = None) -> int:
    args = build_parser().parse_args(argv)
    if engine is None:
        from db import get_engine

        engine = get_engine()
    try:
        if args.cmd == "export":
            report = export(engine, args.out)
        elif args.cmd == "apply":
            report = apply(
                engine,
                load_manifest(args.manifest, args.expect_sha256),
                expect_count=args.expect_count,
                dry_run=args.dry_run,
            )
        elif args.cmd == "validate":
            report = validate(engine, dry_run=args.dry_run)
        else:
            report = restore(engine, load_manifest(args.manifest, args.expect_sha256), dry_run=args.dry_run)
    except Refused as exc:
        print(json.dumps({"action": args.cmd, "refused": str(exc)}, sort_keys=True))
        return EXIT_REFUSED
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
