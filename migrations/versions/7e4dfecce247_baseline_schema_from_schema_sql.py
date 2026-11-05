"""baseline schema from schema.sql

Revision ID: 7e4dfecce247
Revises:
Create Date: 2026-03-23 01:51:24.995660

This is the baseline migration. It stamps the database as being at the
initial schema version without running any DDL — the assumption is that
``schema.sql`` has already been applied.

New deployment (fresh database), from the repository root::

    psql -v ON_ERROR_STOP=1 -f schema.sql
    alembic stamp 7e4dfecce247
    alembic upgrade head

Stamp the *baseline*, not ``head``: ``alembic stamp head`` would record every
later revision as applied without running it, so anything those revisions
create that ``schema.sql`` does not (feature_registry rows, FTS columns and
triggers, the intelligence_search view, ...) would silently never exist. The
later revisions are written to be no-ops where ``schema.sql`` already built
their end state; the ``migration-replay`` CI job
(``.github/workflows/migration-replay.yml``) replays exactly this sequence on
a throwaway PostgreSQL 15 to keep it that way (issue #523).

Existing deployment that predates Alembic::

    alembic stamp 7e4dfecce247
    alembic upgrade head
"""

from typing import Sequence, Union



# revision identifiers, used by Alembic.
revision: str = '7e4dfecce247'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Baseline — no DDL.  Schema is managed by schema.sql for the initial version."""
    pass


def downgrade() -> None:
    """Cannot downgrade past the baseline."""
    raise RuntimeError(
        "Cannot downgrade past the baseline migration. "
        "The initial schema is managed by schema.sql."
    )
