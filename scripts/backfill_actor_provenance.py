#!/usr/bin/env python3
"""Backfill actors.provenance / actors.provenance_as_of for the seed-list actors.

Standalone, operator-run tool -- deliberately NOT an Alembic migration and NOT wired
into deploy.yml or any automatic path. This is the direct fix for the #596 incident
(2026-09-22 02:42Z): the original backfill ran as an UPDATE inside the SAME Alembic
transaction as the schema ADD COLUMNs (migrations/versions/actors_provenance_20260917.py,
reverted by #597). A retry hit an unexplained 30-second statement_timeout mid-execution,
and because Alembic runs a whole upgrade() in one transaction, the entire revision rolled
back -- including the two harmless, already-succeeded ADD COLUMNs. See the bounded
remediation plan (00-Agent-Reports/2026-09-22/claude__ANIK__grid-596-remediation-plan.md)
for the full incident history and why the retry design changed.

Design, per the remediation plan's own preconditions:
  - Runs OUTSIDE Alembic's single-transaction model. Each chunk is its own transaction,
    committed independently -- a chunk that fails leaves every PRIOR chunk's commit
    intact, unlike the original one-shot UPDATE.
  - Idempotent and resumable: every UPDATE only touches rows where
    ``provenance IS DISTINCT FROM`` the target value (same guard the original migration
    used), so a rerun after a partial failure only touches rows the last attempt did not
    reach -- no chunk-position bookkeeping needed, the WHERE clause IS the resume point.
  - Small chunks (default 25 ids, vs. the original single UPDATE's 486): the point of
    chunking here is SHORT transactions the finite statement_timeout can comfortably
    complete, not (as in the original) staying under a SQL parameter-count limit.
  - Re-issues SET LOCAL lock_timeout/statement_timeout every chunk -- SET LOCAL only
    lasts until the next commit, so it must be set again per transaction, not once for
    the whole run.
  - --dry-run prints the classification each chunk WOULD apply without writing anything.
  - Exactly the same three-way classification the original migration used -- see
    intelligence/actors/provenance.py's module docstring for why "touched" is not
    "observed", and PROVENANCE_UNCONFIRMED's own reasoning. This script does not
    reinterpret or simplify that logic.

Usage:
    python3 scripts/backfill_actor_provenance.py --dry-run
    python3 scripts/backfill_actor_provenance.py --chunk-size 25
    python3 scripts/backfill_actor_provenance.py --chunk-size 25 --lock-timeout 5s --statement-timeout 30s

Exit status:
    0  completed (or dry-run completed) with no error
    1  a chunk failed -- see stderr for which one; prior chunks already committed remain
       committed, this is not an error requiring any corrective action beyond rerunning
       this same command later
"""

from __future__ import annotations

import argparse
import sys
import time

from loguru import logger as log
from sqlalchemy import text

from db import get_engine
from intelligence.actors.provenance import (
    PROVENANCE_SEED,
    PROVENANCE_UNCONFIRMED,
    SEED_ACTOR_IDS,
    SEED_VINTAGE,
    SEED_VINTAGE_TS,
)


def _chunks(items: list[str], size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def backfill_chunk(
    conn,
    chunk: list[str],
    *,
    lock_timeout: str,
    statement_timeout: str,
    dry_run: bool,
) -> tuple[int, int]:
    """Classify one chunk of seed-list ids. Returns (promoted_to_seed, marked_unconfirmed).

    Each call is meant to run inside its OWN transaction (the caller commits or rolls
    back around this), so SET LOCAL is re-issued every time.
    """
    if dry_run:
        seed_count = conn.execute(text(
            "SELECT count(*) FROM actors "
            " WHERE id = ANY(:ids) AND provenance IS DISTINCT FROM :seed AND updated_at <= :seed_ts"
        ), {"ids": chunk, "seed": PROVENANCE_SEED, "seed_ts": SEED_VINTAGE_TS}).scalar()
        unconfirmed_count = conn.execute(text(
            "SELECT count(*) FROM actors "
            " WHERE id = ANY(:ids) AND provenance IS DISTINCT FROM :unconfirmed AND updated_at > :seed_ts"
        ), {"ids": chunk, "unconfirmed": PROVENANCE_UNCONFIRMED, "seed_ts": SEED_VINTAGE_TS}).scalar()
        return int(seed_count or 0), int(unconfirmed_count or 0)

    conn.execute(text(f"SET LOCAL lock_timeout = '{lock_timeout}'"))
    conn.execute(text(f"SET LOCAL statement_timeout = '{statement_timeout}'"))

    seed_result = conn.execute(text(
        "UPDATE actors "
        "   SET provenance = :seed, provenance_as_of = :vintage "
        " WHERE id = ANY(:ids) "
        "   AND provenance IS DISTINCT FROM :seed "
        "   AND updated_at <= :seed_ts"
    ), {
        "seed": PROVENANCE_SEED,
        "vintage": SEED_VINTAGE,
        "seed_ts": SEED_VINTAGE_TS,
        "ids": chunk,
    })
    unconfirmed_result = conn.execute(text(
        "UPDATE actors "
        "   SET provenance = :unconfirmed, provenance_as_of = NULL "
        " WHERE id = ANY(:ids) "
        "   AND provenance IS DISTINCT FROM :unconfirmed "
        "   AND updated_at > :seed_ts"
    ), {
        "unconfirmed": PROVENANCE_UNCONFIRMED,
        "seed_ts": SEED_VINTAGE_TS,
        "ids": chunk,
    })
    return seed_result.rowcount, unconfirmed_result.rowcount


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--chunk-size", type=int, default=25, help="ids per transaction (default: 25)")
    parser.add_argument("--lock-timeout", default="5s", help="SET LOCAL lock_timeout per chunk (default: 5s)")
    parser.add_argument("--statement-timeout", default="30s", help="SET LOCAL statement_timeout per chunk (default: 30s)")
    parser.add_argument("--dry-run", action="store_true", help="report what would change, write nothing")
    parser.add_argument("--sleep-between-chunks", type=float, default=0.0, help="seconds to sleep between chunk commits (default: 0, no throttling)")
    args = parser.parse_args()

    seed_ids = sorted(SEED_ACTOR_IDS)
    if not seed_ids:
        log.warning("SEED_ACTOR_IDS is empty -- nothing to backfill")
        return 0

    engine = get_engine()
    total_chunks = (len(seed_ids) + args.chunk_size - 1) // args.chunk_size
    log.info(
        "backfill_actor_provenance: {n} seed ids, chunk_size={cs}, {tc} chunks, dry_run={dr}",
        n=len(seed_ids), cs=args.chunk_size, tc=total_chunks, dr=args.dry_run,
    )

    total_seed = 0
    total_unconfirmed = 0
    for i, chunk in enumerate(_chunks(seed_ids, args.chunk_size), start=1):
        try:
            with engine.begin() as conn:
                promoted, unconfirmed = backfill_chunk(
                    conn, chunk,
                    lock_timeout=args.lock_timeout,
                    statement_timeout=args.statement_timeout,
                    dry_run=args.dry_run,
                )
        except Exception:
            log.exception(
                "chunk {i}/{tc} FAILED (ids {first}..{last}) -- prior chunks already "
                "committed remain committed; rerun this same command to resume from "
                "here, no other action needed",
                i=i, tc=total_chunks, first=chunk[0], last=chunk[-1],
            )
            return 1
        total_seed += promoted
        total_unconfirmed += unconfirmed
        log.info(
            "chunk {i}/{tc}: {p} {verb} seed, {u} {verb} unconfirmed",
            i=i, tc=total_chunks, p=promoted, u=unconfirmed,
            verb="would be marked" if args.dry_run else "marked",
        )
        if args.sleep_between_chunks:
            time.sleep(args.sleep_between_chunks)

    log.info(
        "backfill_actor_provenance complete: {p} total {verb} seed, {u} total {verb} unconfirmed",
        p=total_seed, u=total_unconfirmed,
        verb="would be marked" if args.dry_run else "marked",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
