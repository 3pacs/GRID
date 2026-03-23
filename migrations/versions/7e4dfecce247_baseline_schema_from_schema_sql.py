"""baseline schema from schema.sql

Revision ID: 7e4dfecce247
Revises:
Create Date: 2026-03-23 01:51:24.995660

This is the baseline migration. It stamps the database as being at the
initial schema version without running any DDL — the assumption is that
``schema.sql`` has already been applied.

For new deployments, run ``schema.sql`` first, then::

    cd grid && alembic stamp head

For existing deployments that predate Alembic::

    cd grid && alembic stamp 7e4dfecce247
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


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
