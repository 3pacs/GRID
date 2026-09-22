"""Add actors.provenance / actors.provenance_as_of -- SCHEMA ONLY, no backfill.

Revision ID: actors_provenance_columns_0922
Revises: oracle_pred_nullable_0918
Create Date: 2026-09-22

Why a separate migration from the backfill
-------------------------------------------
The original attempt (#596, merge commit adeceba63f, 2026-09-22 ~02:42Z) combined this
same ADD COLUMN pair with a backfill UPDATE covering ~486 seed-list actor rows in ONE
Alembic revision. Alembic runs a whole `upgrade()` call in a single transaction by
default, so when the backfill's second UPDATE statement hit an unexplained 30-second
`statement_timeout` mid-execution during a retry, the ENTIRE revision rolled back --
including the two ADD COLUMNs, which had nothing wrong with them and had already
succeeded on that same attempt. See the incident record
(00-Agent-Reports/2026-09-22/claude__ANIK__grid-sonnet-596-merge-deploy-incident.md and
the recovery/closure reports that follow it) and the bounded remediation plan
(00-Agent-Reports/2026-09-22/claude__ANIK__grid-596-remediation-plan.md).

This revision deliberately does ONLY the schema change. The backfill is a separate,
standalone, resumable script (scripts/backfill_actor_provenance.py) that commits
per-chunk -- explicitly NOT another Alembic revision, because a chunked loop inside a
single migration's upgrade() still holds locks for the full cumulative duration
regardless of chunking (the exact gap the remediation plan identified: chunking without
independent commits gives none of the intended benefit).

Safety
------
Both columns are additive and nullable-or-defaulted:

* ``provenance`` is ``TEXT NOT NULL DEFAULT 'unknown'``. Postgres 11+ stores a
  non-volatile column default in the catalog rather than rewriting the heap, so this is
  a metadata-only ALTER even on the full table.
* ``provenance_as_of`` is a nullable ``DATE``.

**Amended from the original design's ``DEFAULT 'observed'``.** ``'observed'`` is a
specific, earned claim -- confirmed by a real writer's evidence contract (see
``intelligence/actors/provenance.py``'s module docstring and
``intelligence/actors/db.py::save_actor``) -- not a safe thing for a column default to
assert about a row nothing has actually classified yet. A default of ``'observed'``
meant every one of the 486 seed-list rows, and every other row in the table, read as
"observed" the instant this migration ran, before any evidence existed either way --
and made the schema-default value and the genuinely-confirmed value literally
indistinguishable by looking at the column alone, which is also what let a backfill
attempt incorrectly reclassify an already-confirmed row (see
scripts/backfill_actor_provenance.py's module docstring for the concurrency-adjacent
version of this same bug, found and fixed alongside this default change).

``'unknown'`` is honest about what schema deployment alone establishes: nothing. No
row -- including the 486 seed-list ids -- is ``'seed'``, ``'observed'``, or
``'unconfirmed'`` until something with real evidence says so: the separate backfill
script (classifies the seed-list ids from their pre-existing ``updated_at``),
``_seed_known_actors`` (seeds/reseeds a row still at ``'unknown'`` or already
``'seed'``), or ``save_actor`` (stamps ``'observed'`` only alongside its own evidence
contract -- see that function). ``intelligence/actors/db.py::_seed_known_actors``'s
``WHERE actors.provenance IN ('seed', 'unknown')`` reseed guard (added alongside this
migration) treats ``'unknown'`` as fair game to seed, exactly like a row that does not
exist yet -- legitimate seeding is unaffected by this default. What the guard still
refuses, unconditionally, is overwriting a row already ``'observed'`` or
``'unconfirmed'`` -- a real classification decision, never a default.

No index is added: neither column is a filter in any query today (the API resolves
provenance in Python), and an index on a two-value-mostly column over a table this size
would not be used.

Must stay in step with intelligence/actors/provenance.py and
scripts/backfill_actor_provenance.py, both of which import these same value constants
independently rather than from this file (migrations are not meant to be imported by
application code).
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "actors_provenance_columns_0922"
down_revision: Union[str, Sequence[str], None] = "oracle_pred_nullable_0918"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# SET LOCAL: scoped to this migration's own transaction only -- see
# oracle_pred_nullable_0918 for the precedent this follows. Both statements here are
# catalog-only ALTERs (no table rewrite, no row scan), so a short statement_timeout is
# fine too -- there is no backfill UPDATE in this revision to need a generous one for.
_LOCK_TIMEOUT = "5s"
_STATEMENT_TIMEOUT = "5s"


def _set_finite_timeouts() -> None:
    op.execute(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT}'")
    op.execute(f"SET LOCAL statement_timeout = '{_STATEMENT_TIMEOUT}'")


def upgrade() -> None:
    conn = op.get_bind()
    _set_finite_timeouts()
    conn.execute(text(
        "ALTER TABLE actors "
        "ADD COLUMN IF NOT EXISTS provenance TEXT NOT NULL DEFAULT 'unknown'"
    ))
    conn.execute(text(
        "ALTER TABLE actors ADD COLUMN IF NOT EXISTS provenance_as_of DATE"
    ))


def downgrade() -> None:
    conn = op.get_bind()
    _set_finite_timeouts()
    conn.execute(text("ALTER TABLE actors DROP COLUMN IF EXISTS provenance_as_of"))
    conn.execute(text("ALTER TABLE actors DROP COLUMN IF EXISTS provenance"))
