"""Add actors.provenance / actors.provenance_as_of.

Revision ID: actors_provenance_20260917
Revises: earnings_pred_move_basis_0918 (re-parented 2026-09-18: this PR is stacked on #540, whose revision sits between; keeps the remediation chain linear so no merge revision is needed)
Create Date: 2026-09-17

Why
---
``intelligence/actors/db.py::_seed_known_actors`` is not a fixture loader: it
runs as a *live fallback* from ``build_actor_graph`` whenever the ``actors``
table comes back empty, and it writes 500+ hand-curated rows about named real
people and organizations -- ``influence_score``, ``net_worth_estimate`` and
``aum`` figures that were typed in by hand -- into the production table.
Nothing on the row said so, and the upsert stamped ``updated_at = NOW()``, so
a hand-entered net-worth literal reported itself as freshly refreshed.

This revision adds the column that lets a seeded row be told apart from an
observed one end to end. ``intelligence/actors/provenance.py`` maps it onto
the wire as ``"source": "curated_seed"`` / ``"observed"``.

Safety
------
Both columns are additive and nullable-or-defaulted, so the revision is safe
to run while the API is up:

* ``provenance`` is ``TEXT NOT NULL DEFAULT 'observed'``. Postgres 11+ stores
  a non-volatile column default in the catalog rather than rewriting the
  heap, so this is a metadata-only ALTER even on the full table. Defaulting
  to ``'observed'`` is the conservative choice for rows already present: the
  backfill below then promotes exactly the ids the seeder owns.
* ``provenance_as_of`` is a nullable ``DATE`` -- NULL for observed rows, whose
  freshness is already carried by ``updated_at``.

The backfill promotes exactly the ids the seeder owns, read from
``_KNOWN_ACTORS`` at revision time (a pure-data module with no imports of its
own). Rows the seeder does not own keep the default. ``_seed_known_actors``
sets both columns explicitly from then on, so the backfill only has to cover
the rows already in the table -- which matters, because the seeder runs only
when ``actors`` comes back *empty* and would therefore never re-stamp them.

No index is added: neither column is a filter in any query today (the API
resolves provenance in Python), and an index on a two-value column over a
table this size would not be used.

Precise meaning of ``provenance = 'seed'`` -- and why "touched" is not "observed"
-----------------------------------------------------------------------------------
This is a claim about the row's *current* state, not a permanent record of
where the row was first created: "as of this read, nothing has confirmed or
updated this row since it was hand-typed." Seed-list membership is therefore
**not sufficient on its own** to backfill a row -- an id can be in
``_KNOWN_ACTORS`` and still have been touched since.

``updated_at`` moving is evidence of *modification*, not of *observation*.
A row the real seeder created and nothing has touched since carries exactly
``SEED_VINTAGE_TS`` (a fixed historical constant, never ``NOW()`` -- see
``_seed_known_actors``). Anything that has moved it forward proves the row
is no longer pristine, but does not by itself prove a real measurement
happened: a repo-wide survey of writers touching ``actors`` found over a
dozen call sites beyond the two provenance-aware ones
(``intelligence/actors/trial_bridge.py``, ``intelligence/actor_discovery.py``,
``intelligence/actor_researcher.py``, ``intelligence/signal_backlinker.py``,
``scripts/fold_actor_aliases.py``) that stamp ``updated_at = NOW()`` for
reasons -- connection merges, alias consolidation -- that have nothing to do
with confirming a measurement. Only
``intelligence.actors.db.save_actor``'s contract is trusted as evidence of a
real observation: it always writes alongside real data (a merged
``data_sources`` list, a ``GREATEST``-combined ``influence_score``), never
just a timestamp, and it upserts by the same ``id`` on purpose so live data
merges onto a seeded skeleton. This revision has no way to attribute a given
``updated_at`` move to a specific writer after the fact, so it does not
claim ``'observed'`` for a touched row (that would launder an alias-fold
into a confidence claim it never earned); it explicitly marks the row
``PROVENANCE_UNCONFIRMED`` instead:

* ``updated_at <= SEED_VINTAGE_TS`` (never touched) -> promoted to ``'seed'``.
* ``updated_at > SEED_VINTAGE_TS`` (touched by something) -> explicitly
  stamped ``'unconfirmed'`` -- not silently left on the column's
  ``'observed'`` default, which would overclaim confidence this migration
  does not have.

Both backfills are restricted to ids in ``_KNOWN_ACTORS`` -- this revision
has no comparable pristine baseline for the rest of ``actors`` and makes no
claim about it. ``'unconfirmed'`` is a state the migration assigns during
this one-time historical classification; going forward, only
``_seed_known_actors``'s own upsert or a future writer contract change would
be able to introduce it again, and neither does today.

Callers that label an actor node without selecting the stored ``provenance``
column (see ``intelligence/actors/provenance.py``'s "Resolution order") fall
back to bare ``SEED_ACTOR_IDS`` membership, which cannot represent
``'unconfirmed'`` at all. Every current caller has been updated to select and
pass the stored column instead; the fallback remains a documented, last-resort
approximation for a call site with no row in hand.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Sequence, Union

from alembic import op
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision: str = "actors_provenance_20260917"
down_revision: Union[str, Sequence[str], None] = "earnings_pred_move_basis_0918"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Must stay in step with intelligence/actors/provenance.py.
PROVENANCE_SEED = "seed"
PROVENANCE_OBSERVED = "observed"
PROVENANCE_UNCONFIRMED = "unconfirmed"
SEED_VINTAGE = "2026-04-07"
SEED_VINTAGE_TS = datetime(2026, 4, 7, tzinfo=timezone.utc)

# SET LOCAL: scoped to this migration's own transaction only -- see
# oracle_pred_nullable_0918 for the precedent this follows. lock_timeout
# short (fail fast rather than block every other statement queued behind an
# ACCESS EXCLUSIVE wait on `actors`, a table the live API reads and writes);
# statement_timeout generous (the ADD COLUMNs are catalog-only and the
# backfill UPDATE covers at most len(_KNOWN_ACTORS) rows by primary key).
_LOCK_TIMEOUT = "5s"
_STATEMENT_TIMEOUT = "30s"


def _set_finite_timeouts() -> None:
    op.execute(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT}'")
    op.execute(f"SET LOCAL statement_timeout = '{_STATEMENT_TIMEOUT}'")


def upgrade() -> None:
    conn = op.get_bind()
    _set_finite_timeouts()
    conn.execute(text(
        "ALTER TABLE actors "
        "ADD COLUMN IF NOT EXISTS provenance TEXT NOT NULL "
        f"DEFAULT '{PROVENANCE_OBSERVED}'"
    ))
    conn.execute(text(
        "ALTER TABLE actors ADD COLUMN IF NOT EXISTS provenance_as_of DATE"
    ))

    seed_ids = _seed_actor_ids()
    if not seed_ids:
        return
    # Chunked so neither UPDATE ever builds a single multi-thousand-element
    # array parameter.
    for start in range(0, len(seed_ids), 500):
        chunk = seed_ids[start:start + 500]
        # Untouched since the real seeder's own timestamp -- still pristine.
        conn.execute(text(
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
        # Touched by *something* since seeding, but not through a writer
        # contract that proves a real observation -- explicitly unconfirmed,
        # never left to inherit the column's 'observed' default.
        conn.execute(text(
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


def _seed_actor_ids() -> list[str]:
    """Ids written by ``_seed_known_actors``, or ``[]`` if unreadable.

    Imported lazily so a missing application tree degrades to "no backfill"
    rather than failing the whole upgrade; the columns still land.
    """
    try:
        from intelligence.actors.seed_data import _KNOWN_ACTORS
    except Exception:  # pragma: no cover - defensive
        return []
    return sorted(_KNOWN_ACTORS)


def downgrade() -> None:
    conn = op.get_bind()
    _set_finite_timeouts()
    conn.execute(text("ALTER TABLE actors DROP COLUMN IF EXISTS provenance_as_of"))
    conn.execute(text("ALTER TABLE actors DROP COLUMN IF EXISTS provenance"))
