#!/usr/bin/env python3
"""Fold SEC-filer actor nodes into the ticker / insider actors they duplicate.

The curated Louvain run of 2026-09-10 (``actor_analytics_curated``, 13,226
nodes, 980 communities) had three of its twelve headline communities — 255,
245 and 197 nodes, all category ``corporation`` — built entirely out of SEC
EDGAR full-text-search display names::

    CENOVUS ENERGY INC.  (CVE, CVE-PB)  (CIK 0001071297)
    BlackRock Inc.  (BLK)  (CIK 0001364742)
    iSHARES TRUST  (CIK 0001100663)
    SCRIVNER DOUGLAS G  (CIK 0001234567)

They connect only to each other, while ``CVE Corp`` / ``BLK Corp`` — the same
companies — sit in the market clusters. ``intelligence/spider/discovery.py``
now resolves those names before creating a node; this script repairs the graph
that already exists:

1. Resolve every unfolded filer node to its canonical actor
   (``intelligence/actor_identity.py``: ticker in the name, CIK -> ticker via
   the SEC ``company_tickers.json`` cache, or person name -> insider actor).
2. Re-point ``actor_connections`` onto the canonical id, deduplicating on
   ``(actor_a, actor_b, relationship)`` and keeping the maximum strength.
   Edges that collapse to a self-loop are dropped.
3. Record the filer name in the canonical actor's ``metadata->'aliases'`` and
   set ``actors.merged_into`` on the filer row.

The ``actors`` row is never deleted — ``decision_journal``, ``wealth_flows``,
``actor_analytics`` and ``actor_news`` all reference ``actors(id)``. Folded
rows leave the curated graph because ``graph_analytics._CURATED_EDGE_SQL``
excludes ``merged_into IS NOT NULL``; the full-scope ``actor_analytics`` run
is untouched.

Usage::

    python3 scripts/fold_actor_aliases.py                # dry run, counts only
    python3 scripts/fold_actor_aliases.py --apply        # perform the fold
    python3 scripts/fold_actor_aliases.py --apply --batch-size 200
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

# Ensure project root is on sys.path so imports work when run standalone
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from loguru import logger as log

from db import get_connection
from intelligence.actor_identity import (
    build_person_index,
    ensure_merged_into_column,
    propose_canonical,
)

# Filer nodes worth examining: every SEC full-text display name carries a
# "(CIK …)" group, and the 13F filer nodes from actor_discovery use an
# inst_13f_<cik> id with the CIK in metadata.
DISPLAY_NAME_PATTERN = "%(CIK %"
FILER_ID_PATTERN = "inst_13f_%"

# Actor-id prefixes that hold natural persons (targets of the person rule).
PERSON_ID_PATTERNS: tuple[str, ...] = ("insider\\_%", "ins\\_%", "congress\\_%", "gov\\_%")

DEFAULT_BATCH_SIZE = 500


# ── SQL ───────────────────────────────────────────────────────────────────

_CANDIDATE_SQL = (
    "SELECT id, name, metadata->>'cik' AS cik "
    "FROM actors "
    "WHERE merged_into IS NULL "
    "  AND (name LIKE %s OR id LIKE %s) "
    "ORDER BY id"
)

_PERSON_INDEX_SQL = (
    "SELECT id, name FROM actors "
    "WHERE merged_into IS NULL AND id LIKE ANY(%s) AND name IS NOT NULL"
)

_EXISTING_SQL = (
    "SELECT id FROM actors WHERE id = ANY(%s) AND merged_into IS NULL"
)

_AFFECTED_EDGES_SQL = (
    "SELECT COUNT(*) FROM actor_connections "
    "WHERE actor_a = ANY(%s) OR actor_b = ANY(%s)"
)

# One statement per batch: re-point, dedupe on the unique key keeping the
# strongest edge, then drop the originals. Insert and delete read the same
# snapshot, and a folded row's key can never collide with a touched row's
# (touched rows always have an alias endpoint, folded rows never do), so the
# delete cannot remove an edge the insert just wrote.
_FOLD_EDGES_SQL = (
    "WITH m(alias_id, canonical_id) AS ("
    "    SELECT * FROM unnest(CAST(%s AS text[]), CAST(%s AS text[]))"
    "), touched AS ("
    "    SELECT c.id, c.relationship, c.strength, c.evidence,"
    "           COALESCE(ma.canonical_id, c.actor_a) AS new_a,"
    "           COALESCE(mb.canonical_id, c.actor_b) AS new_b"
    "    FROM actor_connections c"
    "    LEFT JOIN m ma ON ma.alias_id = c.actor_a"
    "    LEFT JOIN m mb ON mb.alias_id = c.actor_b"
    "    WHERE ma.alias_id IS NOT NULL OR mb.alias_id IS NOT NULL"
    "), folded AS ("
    "    SELECT new_a AS actor_a, new_b AS actor_b, relationship,"
    "           MAX(strength) AS strength,"
    "           (ARRAY_AGG(evidence ORDER BY strength DESC NULLS LAST))[1] AS evidence"
    "    FROM touched"
    "    WHERE new_a <> new_b"
    "    GROUP BY new_a, new_b, relationship"
    "), ins AS ("
    "    INSERT INTO actor_connections (actor_a, actor_b, relationship, strength, evidence)"
    "    SELECT actor_a, actor_b, relationship, strength, evidence FROM folded"
    "    ON CONFLICT (actor_a, actor_b, relationship) DO UPDATE"
    "        SET strength = GREATEST(actor_connections.strength, EXCLUDED.strength)"
    "    RETURNING 1"
    "), del AS ("
    "    DELETE FROM actor_connections WHERE id IN (SELECT id FROM touched) RETURNING 1"
    ") "
    "SELECT (SELECT COUNT(*) FROM ins)   AS repointed,"
    "       (SELECT COUNT(*) FROM del)   AS removed,"
    "       (SELECT COUNT(*) FROM touched WHERE new_a = new_b) AS self_loops"
)

_RECORD_ALIASES_SQL = (
    "UPDATE actors a "
    "SET metadata = COALESCE(a.metadata, '{}'::jsonb) || jsonb_build_object("
    "        'aliases',"
    "        (SELECT jsonb_agg(DISTINCT x) FROM jsonb_array_elements("
    "            CASE WHEN jsonb_typeof(a.metadata->'aliases') = 'array'"
    "                 THEN a.metadata->'aliases' ELSE '[]'::jsonb END"
    "            || m.alias_names"
    "        ) AS t(x))"
    "    ), "
    "    updated_at = NOW() "
    "FROM ("
    "    SELECT canonical_id, jsonb_agg(to_jsonb(alias_name)) AS alias_names"
    "    FROM unnest(CAST(%s AS text[]), CAST(%s AS text[])) AS u(canonical_id, alias_name)"
    "    GROUP BY canonical_id"
    ") m "
    "WHERE a.id = m.canonical_id"
)

_MARK_MERGED_SQL = (
    "UPDATE actors a "
    "SET merged_into = m.canonical_id, updated_at = NOW() "
    "FROM unnest(CAST(%s AS text[]), CAST(%s AS text[])) AS m(alias_id, canonical_id) "
    "WHERE a.id = m.alias_id AND a.merged_into IS NULL"
)


# ── Model ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class AliasFold:
    """One filer node and the canonical actor it folds into."""

    alias_id: str
    alias_name: str
    canonical_id: str
    rule: str


# ── Planning ──────────────────────────────────────────────────────────────


def plan_folds(
    candidates: list[tuple[str, str, str | None]],
    existing_ids: set[str],
    person_index: dict[str, str],
) -> list[AliasFold]:
    """Resolve candidate filer rows to canonical actors.

    Parameters:
        candidates: ``(actor_id, name, cik)`` rows from :data:`_CANDIDATE_SQL`.
        existing_ids: ids confirmed present and not themselves folded.
        person_index: from :func:`build_person_index`.

    Returns:
        The folds to apply. A candidate is skipped when no rule fires, when the
        canonical actor does not exist, when it resolves to itself, or when the
        canonical target is itself a candidate (no alias chains).
    """
    resolved: list[AliasFold] = []

    for actor_id, name, cik in candidates:
        proposal = propose_canonical(name, cik_hint=cik)
        if proposal is None:
            continue

        canonical = proposal.actor_id
        if canonical is None and proposal.person_key is not None:
            canonical = person_index.get(proposal.person_key)
        if not canonical or canonical == actor_id:
            continue
        if canonical not in existing_ids:
            continue

        resolved.append(AliasFold(
            alias_id=actor_id,
            alias_name=name,
            canonical_id=canonical,
            rule=proposal.rule,
        ))

    # No alias chains: a target that is itself being folded would strand the
    # edges one hop short of the real actor. Drop those rather than following
    # the chain — a canonical that needs folding is a resolution bug, not a
    # graph to walk.
    folding_ids = {f.alias_id for f in resolved}
    folds = []
    for fold in resolved:
        if fold.canonical_id in folding_ids:
            log.warning(
                "Skipping chained alias {a} -> {c} (the target is itself folding)",
                a=fold.alias_id, c=fold.canonical_id,
            )
            continue
        folds.append(fold)

    return folds


def proposed_canonical_ids(candidates: list[tuple[str, str, str | None]]) -> list[str]:
    """Every concrete canonical id the ticker rules propose, for one existence check."""
    ids: set[str] = set()
    for _actor_id, name, cik in candidates:
        proposal = propose_canonical(name, cik_hint=cik)
        if proposal is not None and proposal.actor_id:
            ids.add(proposal.actor_id)
    return sorted(ids)


# ── Execution ─────────────────────────────────────────────────────────────


def count_affected_edges(cur, aliases: list[str]) -> int:
    """Edges with at least one endpoint among ``aliases``."""
    if not aliases:
        return 0
    cur.execute(_AFFECTED_EDGES_SQL, (aliases, aliases))
    row = cur.fetchone()
    return int(row[0]) if row else 0


def fold_batch(cur, batch: list[AliasFold]) -> dict[str, int]:
    """Re-point, record and mark one batch. Returns per-step counts."""
    alias_ids = [f.alias_id for f in batch]
    canonical_ids = [f.canonical_id for f in batch]
    alias_names = [f.alias_name for f in batch]

    cur.execute(_FOLD_EDGES_SQL, (alias_ids, canonical_ids))
    row = cur.fetchone() or (0, 0, 0)
    repointed, removed, self_loops = int(row[0]), int(row[1]), int(row[2])

    cur.execute(_RECORD_ALIASES_SQL, (canonical_ids, alias_names))
    cur.execute(_MARK_MERGED_SQL, (alias_ids, canonical_ids))
    marked = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    return {
        "aliases": len(batch),
        "edges_repointed": repointed,
        "edges_removed": removed,
        "self_loops_dropped": self_loops,
        "actors_marked": marked,
    }


def run_fold(
    *,
    apply: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    limit: int | None = None,
) -> dict:
    """Plan (and optionally apply) the filer fold. Returns a summary dict."""
    ensure_merged_into_column()

    summary: dict = {
        "candidates": 0,
        "planned": 0,
        "by_rule": {},
        "edges_affected": 0,
        "applied": bool(apply),
    }

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_CANDIDATE_SQL, (DISPLAY_NAME_PATTERN, FILER_ID_PATTERN))
            candidates = [(r[0], r[1] or "", r[2]) for r in cur.fetchall()]
            summary["candidates"] = len(candidates)
            if not candidates:
                log.info("No unfolded filer candidates found — nothing to do")
                return summary

            cur.execute(_PERSON_INDEX_SQL, (list(PERSON_ID_PATTERNS),))
            person_index = build_person_index([(r[0], r[1] or "") for r in cur.fetchall()])

            proposed = proposed_canonical_ids(candidates)
            existing: set[str] = set()
            if proposed:
                cur.execute(_EXISTING_SQL, (proposed,))
                existing = {r[0] for r in cur.fetchall()}
            existing.update(person_index.values())

            folds = plan_folds(candidates, existing, person_index)
            if limit is not None:
                folds = folds[:limit]
            summary["planned"] = len(folds)
            for fold in folds:
                summary["by_rule"][fold.rule] = summary["by_rule"].get(fold.rule, 0) + 1

            summary["edges_affected"] = count_affected_edges(
                cur, [f.alias_id for f in folds]
            )

            log.info(
                "Filer fold plan: {c} candidates, {p} resolvable, {e} edges affected, rules={r}",
                c=len(candidates), p=len(folds),
                e=summary["edges_affected"], r=summary["by_rule"],
            )
            for fold in folds[:10]:
                log.info("  {a}  ->  {c}  [{r}]", a=fold.alias_id, c=fold.canonical_id, r=fold.rule)

            if not apply:
                log.info("Dry run — no changes written. Re-run with --apply to fold.")
                return summary

            totals = {
                "edges_repointed": 0, "edges_removed": 0,
                "self_loops_dropped": 0, "actors_marked": 0,
            }
            for start in range(0, len(folds), batch_size):
                batch = folds[start : start + batch_size]
                counts = fold_batch(cur, batch)
                for key in totals:
                    totals[key] += counts[key]
                log.info(
                    "Batch {n}/{t}: {c}",
                    n=start // batch_size + 1,
                    t=(len(folds) + batch_size - 1) // batch_size,
                    c=counts,
                )
            summary.update(totals)

    log.info("Filer fold complete: {s}", s=summary)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true",
        help=(
            "Write the fold. Without it the script only plans and prints counts "
            "(it still ensures the actors.merged_into column, which the plan query "
            "reads — that DDL is idempotent and changes no rows)."
        ),
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Explicitly plan only (the default when --apply is absent).",
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Cap the number of aliases folded (useful for a first small run).",
    )
    args = parser.parse_args()

    if args.apply and args.dry_run:
        parser.error("--apply and --dry-run are mutually exclusive")

    summary = run_fold(
        apply=args.apply, batch_size=args.batch_size, limit=args.limit,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
