#!/usr/bin/env python3
"""Reviewed, reversible re-derive of ``dollar_flows`` etf_flow rows stored by the old rule.

Before 2026-11-16 the normalizer stored every ETF row as direction 'inflow'
with ``evidence`` = {series_id, raw_value} and no ``direction_basis``. ETF
dollar volume is unsigned, so the current rule (``_normalize_etf_flows``)
writes the same row as 'neutral' with ``direction_basis`` 'unsigned_volume'
and the same amount (0.3% of the stored 5d dollar volume). The daily writer
rebuilds only its window, so the old rows stayed (693 on production,
2026-02-27..11-13); the flow aggregator's 30- and 60-day views counted them as
inflows. Owner decision 2026-11-28: re-derive them in place as neutral, amount
kept.

``created_at`` is kept. ``dollar_flows`` is a current view (each run deletes
and rewrites its window; no reader reconstructs a past date from it, see
``get_aggregate_flows``' past-date refusal), so after the re-derive
``created_at`` still records when the old rule first wrote the row, never when
the neutral value became available. It must not be read as proof of past
availability; the ``rederived`` marker says the row was rewritten.

``intelligence.dollar_flows.STORED_ROW_CONTRACTS["etf_flow"]`` is the
contract; alembic revision ``df_etf_flow_basis_20261128`` adds its CHECK
``dollar_flows_etf_flow_direction_basis`` NOT VALID. The 13F contract and its
delete repair (``scripts/repair_dollar_flows_stored_contract.py``) are not
touched; this script reuses that script's row canonicalisation, digest, lock
and refusal (``_canonical_row``, ``rows_sha256``, ``_lock``, ``Refused``).

``export --out FILE``
    Read-only, one snapshot. Writes every etf_flow row that breaks the
    contract, every column, to a new JSON manifest (never overwrites), with
    the row each becomes: direction 'neutral', ``evidence`` plus
    ``direction_basis`` 'unsigned_volume' and a ``rederived`` marker, every
    other column unchanged. Refuses if any such row is not exactly the old
    rule's shape and provenance (direction 'inflow'; evidence keys exactly
    series_id and raw_value; series ``ETF_FLOW:<ticker>:5d`` with that
    ticker; amount 0.3% of raw_value; flow_date and created_at inside the old
    rule's period): that row needs a person, not this script. Loading a
    manifest checks the same again.

``apply --manifest FILE --expect-count N --expect-sha256 H [--dry-run]``
    One transaction, all or nothing, under ``SHARE ROW EXCLUSIVE`` on
    ``dollar_flows`` (the daily writer waits; readers do not). Refuses unless
    the CHECK exists and behaves as the contract (name, text, and probe rows
    under a rolled-back savepoint). Re-reads the rows that break the
    contract and refuses unless they are exactly the manifest's rows. Updates
    each by id to its planned row (one row each), reads them back equal to
    the plan, refuses unless none break the contract, validates the
    constraint, then commits. ``--dry-run`` does all of it and rolls back.

``validate [--dry-run]``
    For a table with nothing to re-derive and a NOT VALID constraint.

``restore --manifest FILE --expect-sha256 H [--dry-run]``
    One transaction. Writes the manifest's original direction and evidence
    back, only to rows still exactly as ``apply`` left them; refuses if any
    is not. The originals break the contract, so restore refuses while the
    constraint exists and prints the statement that drops it (a schema
    change: the operator's decision); the report prints the one that re-adds
    it NOT VALID.

``status``
    Read-only counts: rows breaking the contract, rows in the re-derived
    shape, the constraint state.

Exit status: 0 done, 2 refused (nothing written). A database error ends in a
traceback (exit 1) with the transaction rolled back.
"""

from __future__ import annotations

import argparse
import copy
import json
import math
import os
import re
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import text  # noqa: E402
from sqlalchemy.engine import Connection, Engine  # noqa: E402
from sqlalchemy.exc import IntegrityError  # noqa: E402

from intelligence.dollar_flows import (  # noqa: E402
    ETF_FLOW_CONTRACT_CHECK_SQL,
    ETF_FLOW_CONTRACT_CONSTRAINT,
    UNSIGNED_VOLUME_BASIS,
    flow_contract_violation,
)
from scripts.repair_dollar_flows_stored_contract import (  # noqa: E402  (shared with the 13F repair)
    EXIT_REFUSED,
    Refused,
    _canonical_row,
    _lock,
    rows_sha256,
)

MANIFEST_KIND = "dollar_flows_etf_flow_rederive"
MANIFEST_VERSION = 1
REDERIVE_KIND = "dollar_flows_etf_flow_rederive"
AMOUNT_SHARE = Decimal("0.003")            # the writer's ETF magnitude proxy (0.3% of 5d dollar volume)
_COLUMNS = (
    "id", "source_type", "actor_name", "ticker", "amount_usd", "direction",
    "confidence", "evidence", "flow_date", "created_at",
)
_SERIES = re.compile(r"^ETF_FLOW:([A-Z0-9.\-]{1,10}):5d$")
# The old rule's provenance: the current rule wrote every etf_flow row from
# 2026-11-16 on; the last old-rule row on production was created 2026-11-20
# 11:45Z for flow_date 2026-11-13. A row of the old shape outside these bounds
# was written by something else and is refused.
OLD_RULE_LAST_FLOW_DATE = "2026-11-15"
OLD_RULE_CREATED_BEFORE = "2026-11-21T00:00:00+00:00"
VALIDATE_SQL = f"ALTER TABLE dollar_flows VALIDATE CONSTRAINT {ETF_FLOW_CONTRACT_CONSTRAINT}"
DROP_SQL = f"ALTER TABLE dollar_flows DROP CONSTRAINT {ETF_FLOW_CONTRACT_CONSTRAINT}"
READD_SQL = (f"ALTER TABLE dollar_flows ADD CONSTRAINT {ETF_FLOW_CONTRACT_CONSTRAINT} "
             f"CHECK {ETF_FLOW_CONTRACT_CHECK_SQL} NOT VALID")


# ── reading ──────────────────────────────────────────────────────────────


def _read_violations(conn: Connection) -> list[dict[str, Any]]:
    rows = conn.execute(text(
        f"SELECT {', '.join(_COLUMNS)} FROM dollar_flows "
        f"WHERE source_type = 'etf_flow' AND NOT {ETF_FLOW_CONTRACT_CHECK_SQL} ORDER BY id"
    )).fetchall()
    return [_canonical_row(r) for r in rows]


def _read_ids(conn: Connection, ids: list[int]) -> list[dict[str, Any]]:
    rows = conn.execute(
        text(f"SELECT {', '.join(_COLUMNS)} FROM dollar_flows WHERE id = ANY(:ids) ORDER BY id"),
        {"ids": ids},
    ).fetchall()
    return [_canonical_row(r) for r in rows]


def _constraint_state(conn: Connection) -> bool | None:
    """``None`` if the constraint is absent, else whether it is validated."""
    row = conn.execute(text(
        "SELECT convalidated FROM pg_constraint WHERE conname = :c AND conrelid = to_regclass('dollar_flows')"
    ), {"c": ETF_FLOW_CONTRACT_CONSTRAINT}).fetchone()
    return None if row is None else bool(row[0])


def _flat(defn: str) -> str:
    """Compare-form that ignores casts, whitespace and parentheses (operators and operands only)."""
    return re.sub(r"::[a-z_ ]+|[()\s]", "", defn.lower())


EXPECTED_FLAT_DEF = _flat(f"CHECK {ETF_FLOW_CONTRACT_CHECK_SQL}")

# Probe rows the contract's CHECK must accept (True) or reject (False). They
# pin the grouping the flat text comparison cannot see: a CHECK with the same
# operators grouped differently accepts or rejects at least one of them (the
# PostgreSQL tests check two regroupings). Each probe run consumes SERIAL ids
# (sequences are not rolled back), so ids after an apply are not contiguous.
CONSTRAINT_PROBES: tuple[tuple[str, str, Any, bool], ...] = (
    ("etf_flow", "inflow", {"series_id": "ETF_FLOW:SPY:5d", "raw_value": 1.0}, False),   # the old rule's row
    ("etf_flow", "inflow", {"direction_basis": UNSIGNED_VOLUME_BASIS}, False),
    ("etf_flow", "neutral", {"direction_basis": UNSIGNED_VOLUME_BASIS}, True),           # the current rule's row
    ("etf_flow", "inflow", {"direction_basis": "net_flow"}, True),                       # a signed measure
    ("etf_flow", "neutral", {}, False),
    ("etf_flow", "neutral", None, False),
    ("etf_flow", "neutral", {"direction_basis": ""}, False),
    ("etf_flow", "neutral", {"direction_basis": None}, False),                           # JSON null
    ("etf_flow", "inflow", {"direction_basis": 5}, False),                               # not a string
    ("etf_flow", "neutral", [1], False),                                                 # not an object
    ("insider", "inflow", None, True),                                                   # other sources untouched,
    ("insider", "inflow", {"direction_basis": UNSIGNED_VOLUME_BASIS}, True),             # whatever their evidence
    ("insider", "inflow", {}, True),
)


def _probe(conn: Connection) -> list[str]:
    """Insert each probe row under a savepoint that is always rolled back; the mismatches."""
    wrong = []
    for stype, direction, evidence, accept in CONSTRAINT_PROBES:
        sp = conn.begin_nested()
        try:
            conn.execute(text(
                "INSERT INTO dollar_flows (source_type, amount_usd, direction, evidence, flow_date) "
                "VALUES (:s, 1, :d, CAST(:e AS JSONB), DATE '2000-01-03')"
            ), {"s": stype, "d": direction, "e": None if evidence is None else json.dumps(evidence)})
            accepted = True
        except IntegrityError as exc:
            accepted = False
            name = getattr(getattr(exc.orig, "diag", None), "constraint_name", None)
            if name != ETF_FLOW_CONTRACT_CONSTRAINT:
                wrong.append(f"{stype}/{direction}/{evidence}: rejected by {name!r}, not the contract")
        finally:
            if sp.is_active:
                sp.rollback()
        if accepted != accept:
            wrong.append(f"{stype}/{direction}/{evidence}: {'accepted' if accepted else 'rejected'}, "
                         f"the contract {'accepts' if accept else 'rejects'} it")
    return wrong


def _require_constraint(conn: Connection) -> bool:
    """Refuse unless the contract's CHECK exists and behaves as the contract; return whether validated."""
    row = conn.execute(text(
        "SELECT contype, convalidated, pg_get_constraintdef(oid) FROM pg_constraint "
        "WHERE conname = :c AND conrelid = to_regclass('dollar_flows')"
    ), {"c": ETF_FLOW_CONTRACT_CONSTRAINT}).fetchone()
    if row is None:
        raise Refused(f"constraint {ETF_FLOW_CONTRACT_CONSTRAINT} is absent: run this after the release "
                      "whose alembic upgrade (df_etf_flow_basis_20261128) adds it")
    if row[0] != "c" or _flat(row[2]).removesuffix("notvalid") != EXPECTED_FLAT_DEF:
        raise Refused(f"constraint {ETF_FLOW_CONTRACT_CONSTRAINT} is not the expected CHECK: {row[2]!r} "
                      f"(expected CHECK {ETF_FLOW_CONTRACT_CHECK_SQL})")
    wrong = _probe(conn)
    if wrong:
        raise Refused(f"constraint {ETF_FLOW_CONTRACT_CONSTRAINT} does not behave as the contract: {wrong}")
    return bool(row[1])


# ── the plan ─────────────────────────────────────────────────────────────


def _not_old_rule(r: dict[str, Any]) -> str | None:
    """Why a contract-breaking row is not exactly the old rule's row, or None."""
    if r["direction"] != "inflow":
        return f"direction {r['direction']!r}"
    ev = r["evidence"]
    if not isinstance(ev, dict) or set(ev) != {"series_id", "raw_value"}:
        return f"evidence keys {sorted(ev) if isinstance(ev, dict) else type(ev).__name__}"
    m = _SERIES.match(str(ev["series_id"]))
    if not m or m.group(1) != r["ticker"]:
        return f"series {ev['series_id']!r} for ticker {r['ticker']!r}"
    try:
        raw = Decimal(str(ev["raw_value"]))
    except Exception:
        return f"raw_value {ev['raw_value']!r}"
    if not raw.is_finite() or raw <= 0:
        return f"raw_value {raw}"
    expect = raw * AMOUNT_SHARE
    if not math.isclose(float(Decimal(r["amount_usd"])), float(expect), rel_tol=1e-9):
        return f"amount {r['amount_usd']} is not 0.3% of raw_value {raw}"
    if str(r["flow_date"]) > OLD_RULE_LAST_FLOW_DATE:
        return f"flow_date {r['flow_date']} is after the old rule ({OLD_RULE_LAST_FLOW_DATE})"
    created = r["created_at"]
    if created is None or datetime.fromisoformat(created) >= datetime.fromisoformat(OLD_RULE_CREATED_BEFORE):
        return f"created_at {created} is not before {OLD_RULE_CREATED_BEFORE}"
    return None


def rederived(row: dict[str, Any], source_rows_sha256: str) -> dict[str, Any]:
    """The row as the current rule writes it: neutral, its basis named, everything else kept."""
    new = copy.deepcopy(row)
    new["direction"] = "neutral"
    new["evidence"] = {
        **row["evidence"],
        "direction_basis": UNSIGNED_VOLUME_BASIS,
        "rederived": {
            "kind": REDERIVE_KIND,
            "from_direction": row["direction"],
            "rule": "ETF dollar volume is unsigned (current _normalize_etf_flows)",
            "manifest_rows_sha256": source_rows_sha256,
        },
    }
    return new


def build_manifest(rows: list[dict[str, Any]]) -> dict[str, Any]:
    not_old = {r["id"]: why for r in rows if (why := _not_old_rule(r))}
    if not_old:
        raise Refused(f"{len(not_old)} contract-breaking etf_flow row(s) are not the old rule's shape and "
                      f"need a person: {dict(list(not_old.items())[:10])}")
    digest = rows_sha256(rows)
    after = [rederived(r, digest) for r in rows]
    for a in after:
        if flow_contract_violation(a) is not None:
            raise Refused(f"the re-derived row {a['id']} would still break the contract")
    return {
        "kind": MANIFEST_KIND,
        "version": MANIFEST_VERSION,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "check_sql": ETF_FLOW_CONTRACT_CHECK_SQL,
        "count": len(rows),
        "ids": [r["id"] for r in rows],
        "amount_usd_sum": str(sum((Decimal(r["amount_usd"]) for r in rows), Decimal(0))),
        "flow_dates": [min((r["flow_date"] for r in rows), default=None),
                       max((r["flow_date"] for r in rows), default=None)],
        "rows_sha256": digest,
        "after_sha256": rows_sha256(after),
        "rows": rows,
        "after": after,
    }


def load_manifest(path: Path, expect_sha256: str) -> dict[str, Any]:
    man = json.loads(Path(path).read_text())
    if man.get("kind") != MANIFEST_KIND or man.get("version") != MANIFEST_VERSION:
        raise Refused(f"{path} is not a v{MANIFEST_VERSION} {MANIFEST_KIND} manifest")
    rows = man.get("rows") or []
    digest = rows_sha256(rows)
    if digest != man.get("rows_sha256") or digest != expect_sha256:
        raise Refused(f"manifest rows sha256 {digest} does not match the manifest's {man.get('rows_sha256')} "
                      f"and --expect-sha256 {expect_sha256}")
    if [r["id"] for r in rows] != man.get("ids") or len(rows) != man.get("count"):
        raise Refused("manifest ids/count do not match its rows")
    if len(set(man["ids"])) != len(man["ids"]):
        raise Refused("manifest lists an id twice")
    not_old = {r["id"]: why for r in rows if (why := _not_old_rule(r))}
    if not_old:
        raise Refused(f"manifest rows are not the old rule's shape: {dict(list(not_old.items())[:10])}")
    if man.get("after") != [rederived(r, digest) for r in rows] or rows_sha256(man["after"]) != man.get("after_sha256"):
        raise Refused("manifest 'after' rows are not the re-derive of its rows (edited?)")
    return man


# ── actions ──────────────────────────────────────────────────────────────


def _read_only_snapshot(conn: Connection) -> None:
    conn.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))


def export(engine: Engine, out: Path) -> dict[str, Any]:
    with engine.connect() as conn:
        with conn.begin():
            _read_only_snapshot(conn)
            rows = _read_violations(conn)
            state = _constraint_state(conn)
    man = build_manifest(rows)
    fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    with os.fdopen(fd, "w") as fh:
        json.dump(man, fh, sort_keys=True, indent=1)
        fh.write("\n")
    return {
        "action": "export", "manifest": str(out), "count": man["count"],
        "amount_usd_sum": man["amount_usd_sum"], "flow_dates": man["flow_dates"],
        "rows_sha256": man["rows_sha256"], "after_sha256": man["after_sha256"],
        "constraint": "absent" if state is None else ("validated" if state else "not_valid"),
    }


def _validate(conn: Connection) -> None:
    conn.execute(text(VALIDATE_SQL))


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
                cur = {r["id"] for r in current}
                raise Refused(
                    "the etf_flow rows that break the contract now are not the manifest's rows "
                    f"(now {len(current)}, manifest {len(ids)}; only now: {sorted(cur - set(ids))[:20]}, "
                    f"only in manifest: {sorted(set(ids) - cur)[:20]}, or a column changed); re-export")
            for after in man["after"]:
                n = conn.execute(text(
                    "UPDATE dollar_flows SET direction = :d, evidence = CAST(:e AS JSONB) "
                    f"WHERE id = :id AND source_type = 'etf_flow' AND NOT {ETF_FLOW_CONTRACT_CHECK_SQL}"
                ), {"d": after["direction"], "e": json.dumps(after["evidence"]), "id": after["id"]}).rowcount
                if n != 1:
                    raise Refused(f"row {after['id']}: UPDATE matched {n} rows, expected 1")
            back = _read_ids(conn, ids)
            if back != man["after"]:
                raise Refused("the re-derived rows do not read back equal to the manifest's plan")
            remaining = len(_read_violations(conn))
            if remaining:
                raise Refused(f"{remaining} etf_flow rows still break the contract after the re-derive")
            if not was_valid:
                _validate(conn)
            if not _require_constraint(conn):
                raise Refused("the constraint is still NOT VALID after VALIDATE")
            trans.rollback() if dry_run else trans.commit()
        except BaseException:
            if trans.is_active:
                trans.rollback()
            raise
    return {"action": "apply", "dry_run": dry_run, "rederived": len(ids), "remaining_violations": remaining,
            "after_sha256": rows_sha256(back), "constraint": "validated"}


def validate(engine: Engine, *, dry_run: bool) -> dict[str, Any]:
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            _lock(conn)
            was_valid = _require_constraint(conn)
            remaining = len(_read_violations(conn))
            if remaining:
                raise Refused(f"{remaining} etf_flow rows break the contract; re-derive them with export/apply first")
            if not was_valid:
                _validate(conn)
            if not _require_constraint(conn):
                raise Refused("the constraint is still NOT VALID after VALIDATE")
            trans.rollback() if dry_run else trans.commit()
        except BaseException:
            if trans.is_active:
                trans.rollback()
            raise
    return {"action": "validate", "dry_run": dry_run, "was_validated": was_valid, "constraint": "validated"}


def restore(engine: Engine, man: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    ids = list(man["ids"])
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            _lock(conn)
            if _constraint_state(conn) is not None:
                raise Refused(f"constraint {ETF_FLOW_CONTRACT_CONSTRAINT} rejects the original rows; restoring "
                              f"them needs it dropped first (a schema change): {DROP_SQL}")
            now = _read_ids(conn, ids)
            if now != man["after"]:
                changed = sorted({a["id"] for a in man["after"]} - {r["id"] for r in now if r in man["after"]})
                raise Refused(f"{len(changed)} row(s) are no longer as apply left them (e.g. {changed[:10]}); "
                              "restore writes only over its own re-derive")
            for orig in man["rows"]:
                n = conn.execute(text(
                    "UPDATE dollar_flows SET direction = :d, evidence = CAST(:e AS JSONB) WHERE id = :id"
                ), {"d": orig["direction"], "e": json.dumps(orig["evidence"]), "id": orig["id"]}).rowcount
                if n != 1:
                    raise Refused(f"row {orig['id']}: UPDATE matched {n} rows, expected 1")
            back = _read_ids(conn, ids)
            if back != man["rows"]:
                raise Refused("restored rows do not read back equal to the manifest")
            trans.rollback() if dry_run else trans.commit()
        except BaseException:
            if trans.is_active:
                trans.rollback()
            raise
    return {"action": "restore", "dry_run": dry_run, "restored": len(ids), "rows_sha256": rows_sha256(back),
            "re_add_constraint": READD_SQL}


def status(engine: Engine) -> dict[str, Any]:
    with engine.connect() as conn:
        with conn.begin():
            _read_only_snapshot(conn)
            breaking = len(_read_violations(conn))
            rederived_rows = conn.execute(text(
                "SELECT COUNT(*) FROM dollar_flows WHERE source_type = 'etf_flow' "
                "AND evidence -> 'rederived' ->> 'kind' = :k"), {"k": REDERIVE_KIND}).scalar()
            state = _constraint_state(conn)
    return {"action": "status", "breaking_contract": breaking, "rederived": int(rederived_rows),
            "constraint": "absent" if state is None else ("validated" if state else "not_valid")}


# ── command line ─────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("export", help="read-only: write the manifest of rows to re-derive")
    e.add_argument("--out", type=Path, required=True)
    a = sub.add_parser("apply", help="re-derive the manifest's rows (one transaction)")
    a.add_argument("--manifest", type=Path, required=True)
    a.add_argument("--expect-count", type=int, required=True)
    a.add_argument("--expect-sha256", required=True)
    a.add_argument("--dry-run", action="store_true")
    v = sub.add_parser("validate", help="validate the constraint on a table with nothing to re-derive")
    v.add_argument("--dry-run", action="store_true")
    r = sub.add_parser("restore", help="write the manifest's original rows back (needs the constraint dropped)")
    r.add_argument("--manifest", type=Path, required=True)
    r.add_argument("--expect-sha256", required=True)
    r.add_argument("--dry-run", action="store_true")
    sub.add_parser("status", help="read-only counts")
    return p


def main(argv: list[str] | None = None, *, engine: Engine | None = None) -> int:
    args = build_parser().parse_args(argv)
    if engine is None:
        from db import get_engine
        engine = get_engine()
    try:
        if args.cmd == "export":
            if args.out.exists():
                raise Refused(f"{args.out} exists; manifests are not overwritten")
            out = export(engine, args.out)
        elif args.cmd == "apply":
            out = apply(engine, load_manifest(args.manifest, args.expect_sha256),
                        expect_count=args.expect_count, dry_run=args.dry_run)
        elif args.cmd == "validate":
            out = validate(engine, dry_run=args.dry_run)
        elif args.cmd == "restore":
            out = restore(engine, load_manifest(args.manifest, args.expect_sha256), dry_run=args.dry_run)
        else:
            out = status(engine)
    except Refused as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return EXIT_REFUSED
    print(json.dumps(out, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
