"""
GRID Intelligence — Actor provenance labels.

The ``actors`` table mixes populations that used to be indistinguishable on
the wire:

* **seed rows** written by :func:`intelligence.actors.db._seed_known_actors`
  from the hand-curated ``_KNOWN_ACTORS`` table in ``seed_data.py``. These are
  named real people and organizations whose ``influence_score``,
  ``net_worth_estimate`` and ``aum`` figures were typed in by hand on a single
  day and have not moved since.
* **observed rows** written from Form 4 / 13F / congressional-disclosure
  pullers, the spider, ICIJ bulk import, or operator injection, where the
  writer's own contract is evidence enough of a real observation (see
  ``PROVENANCE_OBSERVED`` below).
* **unconfirmed rows**: a seed-list id whose row has been modified since the
  seed vintage by *something*, but not through a writer whose contract
  proves a real observation happened. See ``PROVENANCE_UNCONFIRMED`` below —
  this exists specifically so "we don't know" is never silently collapsed
  into either of the other two.

Every actor node served by the API now carries ``source``:

* ``"curated_seed"``  — the figures on this node came out of ``seed_data.py``
  and nothing has touched the row since.
* ``"observed"``      — the row was written or confirmed by an ingestion path
  whose contract is known to write real data.
* ``"unconfirmed"``   — the row started as a seed row and has since been
  modified by something outside that known-observing set; what is on it now
  is not guaranteed to still be the hand-typed seed figures, but nothing
  confirms it is a real measurement either.
* ``"sector_map_curated"`` — the node is not an ``actors`` row at all; it was
  synthesized from ``analysis/sector_map_data.yaml``.

``source_as_of`` carries the vintage of a curated node (``None`` for
``observed``/``unconfirmed`` rows — neither has a curated vintage to report;
an unconfirmed row's actual freshness, whatever it is, lives in
``updated_at``, not in this function).

Precise meaning of ``provenance = 'seed'`` — and why "touched" is not "observed"
---------------------------------------------------------------------------------
``'seed'`` is a claim about the row's *current* state: "nothing has confirmed
or updated this row since it was hand-typed." It is not a permanent record of
where the row was first created, and it does not reassert itself once lost —
see ``PROVENANCE_OBSERVED``/``PROVENANCE_UNCONFIRMED`` for what "lost" means.

**``updated_at`` moving is evidence of modification only, not of
observation.** A survey of every writer that touches ``actors`` found more
than a dozen call sites beyond the two provenance-aware ones —
``intelligence/actors/trial_bridge.py`` (connection merges),
``intelligence/actor_discovery.py`` (several), ``intelligence/actor_researcher.py``,
``intelligence/signal_backlinker.py``, ``scripts/fold_actor_aliases.py`` (alias
consolidation) — that stamp ``updated_at = NOW()`` for reasons that have
nothing to do with confirming a measurement. Treating "touched since the seed
vintage" as proof of "observed" would launder an alias-fold or a connection
merge into a confidence claim it never earned. Only
:func:`intelligence.actors.db.save_actor`'s contract is trusted as evidence of
a real observation — it always writes alongside real data (a merged
``data_sources`` list, a ``GREATEST``-combined ``influence_score``), not just
a timestamp. So:

* A seed-list row untouched since ``SEED_VINTAGE_TS`` → ``PROVENANCE_SEED``.
* A seed-list row confirmed by ``save_actor``'s contract → ``PROVENANCE_OBSERVED``.
* A seed-list row whose ``updated_at`` has moved but not through
  ``save_actor`` → ``PROVENANCE_UNCONFIRMED`` — modification is real,
  observation is not established, and the row must not claim either "still
  seed" or "confirmed observed".

Once a row is ``PROVENANCE_OBSERVED`` or ``PROVENANCE_UNCONFIRMED``, a later
seed rerun must not touch it at all — not the provenance columns, and not any
other seed-authored field (``influence_score``, ``name``, etc.). Overwriting
``influence_score`` with the hand-typed seed figure while the row's
``provenance`` still reads ``'observed'``/``'unconfirmed'`` would make that
label a lie for the overwritten field; see ``_seed_known_actors``'s own
``ON CONFLICT ... WHERE`` clause, which only refreshes a row that is still
exactly ``'seed'``.

Resolution order
----------------
``resolve_provenance`` prefers the row's own ``provenance`` column (added by
alembic revision ``actors_provenance_columns_0922``) — this is the only
source that can represent all three states above, including
``'unconfirmed'``. The fallback below only ever returns ``PROVENANCE_SEED``
or ``PROVENANCE_OBSERVED`` (a bare id carries no signal that would justify
guessing ``'unconfirmed'``), so a caller using it for a seed id that has
actually been modified would report stale ``'curated_seed'`` — call sites
that can select the column must do so rather than accept this approximation.
Until the separate backfill script (scripts/backfill_actor_provenance.py)
has run, every row's stored ``provenance`` is the column default
(``'observed'``), so callers that DO select it will correctly see
``'observed'`` for not-yet-backfilled seed rows too — this module makes no
claim about backfill status; it only resolves whatever is actually stored,
or falls back to seed-list membership when nothing is.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from intelligence.actors.seed_data import _KNOWN_ACTORS

# ── Provenance values stored in actors.provenance ─────────────────────────

PROVENANCE_SEED = "seed"
PROVENANCE_OBSERVED = "observed"
PROVENANCE_UNCONFIRMED = "unconfirmed"

# ── Wire labels exposed as the node's "source" field ──────────────────────

SOURCE_CURATED_SEED = "curated_seed"
SOURCE_OBSERVED = "observed"
SOURCE_UNCONFIRMED = "unconfirmed"
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
            Always prefer passing this — it is the only way to see
            ``PROVENANCE_UNCONFIRMED``. The fallback used when it is absent
            cannot represent that state (see module docstring).

    Returns:
        ``"seed"``, ``"observed"``, or ``"unconfirmed"`` when ``stored`` is
        given; otherwise ``"seed"`` or ``"observed"`` only.
    """
    if stored:
        return str(stored)
    return PROVENANCE_SEED if actor_id in SEED_ACTOR_IDS else PROVENANCE_OBSERVED


def actor_source(actor_id: str, stored: str | None = None) -> str:
    """Return the wire ``source`` label for an actor node."""
    provenance = resolve_provenance(actor_id, stored)
    if provenance == PROVENANCE_SEED:
        return SOURCE_CURATED_SEED
    if provenance == PROVENANCE_UNCONFIRMED:
        return SOURCE_UNCONFIRMED
    return SOURCE_OBSERVED


def source_as_of(actor_id: str, stored: str | None = None, vintage: str | None = None) -> str | None:
    """Return the curation vintage for an actor node, or ``None`` otherwise.

    Parameters:
        actor_id: The ``actors.id`` value.
        stored: The row's ``provenance`` column, when selected.
        vintage: The row's ``provenance_as_of`` column, when selected.

    ``None`` for both ``observed`` (freshness is ``updated_at``, not a
    curated vintage) and ``unconfirmed`` (there is no curated vintage left
    to report once modification is known but unconfirmed).
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
    "PROVENANCE_UNCONFIRMED",
    "SOURCE_CURATED_SEED",
    "SOURCE_OBSERVED",
    "SOURCE_UNCONFIRMED",
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
