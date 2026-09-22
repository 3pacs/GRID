"""
GRID Intelligence — Actor provenance labels.

The ``actors`` table mixes two populations that used to be indistinguishable
on the wire:

* **seed rows** written by :func:`intelligence.actors.db._seed_known_actors`
  from the hand-curated ``_KNOWN_ACTORS`` table in ``seed_data.py``. These are
  named real people and organizations whose ``influence_score``,
  ``net_worth_estimate`` and ``aum`` figures were typed in by hand on a single
  day and have not moved since.
* **observed rows** written from Form 4 / 13F / congressional-disclosure
  pullers, the spider, ICIJ bulk import, or operator injection.

Every actor node served by the API now carries ``source``:

* ``"curated_seed"``  — the figures on this node came out of ``seed_data.py``.
* ``"observed"``      — the row was written by an ingestion path.
* ``"sector_map_curated"`` — the node is not an ``actors`` row at all; it was
  synthesized from ``analysis/sector_map_data.yaml``.

``source_as_of`` carries the vintage of a curated node (``None`` for observed
rows, whose freshness is already reported by ``updated_at``).

Precise meaning of ``provenance = 'seed'``
-------------------------------------------
``'seed'`` is a claim about the row's *current* state: "nothing has confirmed
or updated this row since it was hand-typed." It is not a permanent record of
where the row was first created, and it is not sticky by default in the
direction that would let it re-assert itself — the moment a real observed
writer (:func:`intelligence.actors.db.save_actor`, which upserts by the same
``id`` on purpose so live data merges onto a seeded skeleton) touches a row,
that row's stored ``provenance`` becomes (and, per
``_seed_known_actors``'s own ``ON CONFLICT`` clause, stays) ``'observed'`` —
a later seed rerun for that same id must not relabel it ``'seed'`` again.
Seed-list (``SEED_ACTOR_IDS``) membership alone is therefore not sufficient
to classify a row ``'seed'``; both the one-time migration backfill
(``actors_provenance_20260917``) and the seeder's own upsert additionally
require evidence the row was never touched after the seed vintage (see each
one's own docstring for its exact check).

Resolution order
----------------
``resolve_provenance`` prefers the row's own ``provenance`` column (added by
alembic revision ``actors_provenance_20260917``) — this is the only source
that reflects the current-state definition above. Where that column is not
available to a call site — the read-only router queries that select a narrow
column list — membership of ``SEED_ACTOR_IDS`` is used instead, as a
documented, degraded approximation. The two usually agree, but can diverge
for a seed id that has since been re-sourced from a real observation: the
stored column would correctly read ``'observed'``, while the membership
fallback has no way to see that and still reports ``'seed'``. Callers that
need the current-state guarantee must select and pass the stored column;
callers using the bare functions below accept this known approximation.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from intelligence.actors.seed_data import _KNOWN_ACTORS

# ── Provenance values stored in actors.provenance ─────────────────────────

PROVENANCE_SEED = "seed"
PROVENANCE_OBSERVED = "observed"

# ── Wire labels exposed as the node's "source" field ──────────────────────

SOURCE_CURATED_SEED = "curated_seed"
SOURCE_OBSERVED = "observed"
SOURCE_SECTOR_MAP = "sector_map_curated"

# The vintage of the curated seed table. This is the last hand-edit of
# intelligence/actors/seed_data.py (``git log -1 --date=short``), i.e. the
# most recent date on which any of these figures could have been checked by
# the person who typed them in. It is deliberately NOT "today": a seeded row
# is not a fresh observation and must never report itself as one.
SEED_VINTAGE = "2026-04-07"
SEED_VINTAGE_DATE = date(2026, 4, 7)
SEED_VINTAGE_TS = datetime(2026, 4, 7, tzinfo=timezone.utc)

# Ids that _seed_known_actors writes. Used as the fallback provenance signal
# for call sites that do not select the provenance column.
SEED_ACTOR_IDS: frozenset[str] = frozenset(_KNOWN_ACTORS)


def resolve_provenance(actor_id: str, stored: str | None = None) -> str:
    """Return the provenance of an actor row.

    Parameters:
        actor_id: The ``actors.id`` value.
        stored: The row's ``provenance`` column, when the caller selected it.

    Returns:
        ``"seed"`` or ``"observed"``.
    """
    if stored:
        return str(stored)
    return PROVENANCE_SEED if actor_id in SEED_ACTOR_IDS else PROVENANCE_OBSERVED


def actor_source(actor_id: str, stored: str | None = None) -> str:
    """Return the wire ``source`` label for an actor node."""
    return (
        SOURCE_CURATED_SEED
        if resolve_provenance(actor_id, stored) == PROVENANCE_SEED
        else SOURCE_OBSERVED
    )


def source_as_of(actor_id: str, stored: str | None = None, vintage: str | None = None) -> str | None:
    """Return the curation vintage for an actor node, or ``None`` if observed.

    Parameters:
        actor_id: The ``actors.id`` value.
        stored: The row's ``provenance`` column, when selected.
        vintage: The row's ``provenance_as_of`` column, when selected.
    """
    if resolve_provenance(actor_id, stored) != PROVENANCE_SEED:
        return None
    return str(vintage) if vintage else SEED_VINTAGE


def stamp_actor_node(
    node: dict,
    actor_id: str,
    *,
    stored: str | None = None,
    vintage: str | None = None,
) -> dict:
    """Add ``source`` / ``source_as_of`` to an actor node dict in place."""
    node["source"] = actor_source(actor_id, stored)
    node["source_as_of"] = source_as_of(actor_id, stored, vintage)
    return node


__all__ = [
    "PROVENANCE_SEED",
    "PROVENANCE_OBSERVED",
    "SOURCE_CURATED_SEED",
    "SOURCE_OBSERVED",
    "SOURCE_SECTOR_MAP",
    "SEED_VINTAGE",
    "SEED_VINTAGE_DATE",
    "SEED_VINTAGE_TS",
    "SEED_ACTOR_IDS",
    "resolve_provenance",
    "actor_source",
    "source_as_of",
    "stamp_actor_node",
]
