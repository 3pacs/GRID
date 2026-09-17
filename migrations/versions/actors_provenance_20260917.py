"""Add actors.provenance / actors.provenance_as_of.

Revision ID: actors_provenance_20260917
Revises: snapshot_actor_col_20260914
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
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision: str = "actors_provenance_20260917"
down_revision: Union[str, Sequence[str], None] = "snapshot_actor_col_20260914"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Must stay in step with intelligence/actors/provenance.py.
PROVENANCE_SEED = "seed"
PROVENANCE_OBSERVED = "observed"
SEED_VINTAGE = "2026-04-07"


def upgrade() -> None:
    conn = op.get_bind()
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
    # Chunked so the UPDATE never builds a single multi-thousand-element
    # array parameter.
    for start in range(0, len(seed_ids), 500):
        conn.execute(text(
            "UPDATE actors "
            "   SET provenance = :seed, provenance_as_of = :vintage "
            " WHERE id = ANY(:ids) "
            "   AND provenance IS DISTINCT FROM :seed"
        ), {
            "seed": PROVENANCE_SEED,
            "vintage": SEED_VINTAGE,
            "ids": seed_ids[start:start + 500],
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
    conn.execute(text("ALTER TABLE actors DROP COLUMN IF EXISTS provenance_as_of"))
    conn.execute(text("ALTER TABLE actors DROP COLUMN IF EXISTS provenance"))
