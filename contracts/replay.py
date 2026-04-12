"""Manual replay for dead-letter entries.

Usage (CLI)::

    python -m contracts.replay                       # replay all due entries
    python -m contracts.replay --contract PullLifecycle
    python -m contracts.replay --limit 10
    python -m contracts.replay --entry 42            # replay a single entry by id

API usage: ``replay_entry(engine, entry)`` is what the FastAPI replay
endpoint calls.
"""
from __future__ import annotations

import argparse
import json as _json
from typing import Iterable
from uuid import UUID

from loguru import logger as log
from sqlalchemy import text

from contracts.dead_letter import DeadLetterEntry, mark_resolved
from contracts.router import resolve_handler
from contracts.schemas import ALL_CONTRACTS


_CONTRACTS_BY_NAME: dict[str, type] = {cls.__name__: cls for cls in ALL_CONTRACTS}


def replay_entry(engine, entry: DeadLetterEntry) -> bool:
    """Re-run a single dead-letter entry. Returns True on success."""
    contract_cls = _CONTRACTS_BY_NAME.get(entry.contract_type)
    if contract_cls is None:
        log.warning("replay: unknown contract {ct}", ct=entry.contract_type)
        return False

    try:
        contract = contract_cls(**entry.payload)
        handler = resolve_handler(entry.consumer)
        handler(contract, engine=engine)
    except Exception as exc:
        log.info(
            "replay failed for entry {id}: {e}", id=entry.id, e=str(exc)
        )
        return False

    mark_resolved(engine, entry.id)
    return True


def replay_many(engine, entries: Iterable[DeadLetterEntry]) -> dict[str, int]:
    success = 0
    failed = 0
    for entry in entries:
        if replay_entry(engine, entry):
            success += 1
        else:
            failed += 1
    return {"success": success, "failed": failed}


def replay_filtered(
    engine, contract_type: str | None = None, limit: int = 100
) -> dict[str, int]:
    entries = _load_filtered(engine, contract_type=contract_type, limit=limit)
    return replay_many(engine, entries)


def _load_filtered(
    engine, contract_type: str | None, limit: int
) -> list[DeadLetterEntry]:
    sql = text(
        """
        SELECT id, event_id, contract_type, payload, consumer,
               error_type, error_detail, retry_count, next_retry_at,
               failed_at, correlation_id
        FROM contracts_dead_letter
        WHERE resolved_at IS NULL
          AND (:contract_type IS NULL OR contract_type = :contract_type)
        ORDER BY failed_at DESC
        LIMIT :limit
        """
    )
    out: list[DeadLetterEntry] = []
    with engine.begin() as conn:
        rows = conn.execute(
            sql.bindparams(contract_type=contract_type, limit=limit)
        ).fetchall()
    for r in rows:
        out.append(
            DeadLetterEntry(
                id=int(r[0]),
                event_id=UUID(str(r[1])),
                contract_type=str(r[2]),
                payload=r[3] if isinstance(r[3], dict) else _json.loads(r[3]),
                consumer=str(r[4]),
                error_type=str(r[5]),
                error_detail=str(r[6]),
                retry_count=int(r[7]),
                next_retry_at=r[8],
                failed_at=r[9],
                correlation_id=UUID(str(r[10])) if r[10] else None,
            )
        )
    return out


# ---------- CLI ----------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="contracts.replay",
        description="Replay dead-letter contract entries.",
    )
    p.add_argument("--contract", type=str, default=None)
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--entry", type=int, default=None, help="single entry id")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    from api.dependencies import get_db_engine
    engine = get_db_engine()

    if args.entry is not None:
        entries = _load_filtered(engine, contract_type=None, limit=1000)
        match = [e for e in entries if e.id == args.entry]
        report = replay_many(engine, match)
    else:
        report = replay_filtered(engine, args.contract, args.limit)
    print(f"replay complete: {report}")
    return 0 if report["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
