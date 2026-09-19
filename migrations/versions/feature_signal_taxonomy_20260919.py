"""Register production's feature_registry.signal_domain / signal_subtype in tracked migrations.

Revision ID: feature_signal_taxonomy_20260919
Revises: god_view_market_tables_20260918
Create Date: 2026-09-19 00:00:00.000000

Note on the id: the natural name for this revision is 41 characters
("feature_registry_signal_taxonomy_20260919"), which exceeds the 32-char
ceiling tests/test_alembic_single_head.py::test_revision_ids_fit_the_version_column
enforces (alembic_version.version_num is VARCHAR(32); see that revision's
own history in raw_sql_port_20260914.py for why this ceiling exists and
what crossing it does). Shortened to "feature_signal_taxonomy_20260919"
(32 chars, exactly at the limit).

A read-only catalog query against griddb on 2026-09-19 (``SELECT column_name
FROM information_schema.columns WHERE table_name = 'feature_registry' ORDER
BY ordinal_position``) confirmed production has ``signal_domain`` and
``signal_subtype`` columns on ``feature_registry``, and has no ``subfamily``
column. Neither fact was ever recorded in the tracked migration chain:
``signal_domain``/``signal_subtype`` were added out-of-band by
``scripts/signal_taxonomy.py``'s own runtime DDL (an ``ALTER TABLE
feature_registry ADD COLUMN ... TEXT`` inside a ``DO`` block, plus
``CREATE INDEX IF NOT EXISTS idx_feature_domain ON feature_registry
(signal_domain)`` and ``idx_feature_subtype ON feature_registry
(signal_subtype)``), run by hand outside ``alembic upgrade head`` the same
way ``migrations/0062_...`` / ``0063_...`` were before
``raw_sql_port_20260914`` ported them into the chain (see that revision's
docstring for the same "a migration mechanism whose apply step is a human
remembering is the defect" framing — this is the same defect, different
columns).

This revision ports that DDL into alembic so a fresh install ends up with
the same schema production actually has, and so a future rebuild does not
require re-discovering that ``scripts/signal_taxonomy.py`` needs to be run
by hand. ``schema.sql`` was updated in the same change to declare both
columns and both indexes directly, for the same reason
``raw_sql_port_20260914`` exists: don't let the tracked chain lie about
what a fresh install ends up with.

Idempotent and non-destructive by construction
------------------------------------------------
Every statement in ``TAXONOMY_DDL`` uses ``ADD COLUMN IF NOT EXISTS`` /
``CREATE INDEX IF NOT EXISTS``, so running this against griddb (where
``scripts/signal_taxonomy.py`` already created both columns and both
indexes) is expected to be a no-op — ``upgrade()`` logs a before -> after
line, exactly as ``raw_sql_port_20260914`` does, so that expected no-op is
a *stated* no-op rather than a silent one. Running it against a
fresh-install database (schema.sql applied, no signal_taxonomy.py run) adds
both columns and both indexes for the first time.

``downgrade()`` deliberately does **not** drop the columns: production has
real, currently-in-use data in them (whatever
``scripts/signal_taxonomy.py`` last wrote), and dropping on downgrade would
destroy it for the sake of a revert this revision never needs anyone to
run. It logs why and returns, the same non-destructive-downgrade pattern
``raw_sql_port_20260914`` uses for its own INSERTed rows (there, "leave
resolved_series alone"; here, "leave the columns and their data alone").

No size gate, no GRANT footer
------------------------------
``feature_registry`` is a few hundred rows — no
``pg_total_relation_size`` gate is needed the way
``snapshot_actor_index_20260912`` needs one for a 511 GB table. This
revision creates no new table, so it needs no GRANT footer either
(``grid`` already has privileges on ``feature_registry``).
"""

import logging
from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "feature_signal_taxonomy_20260919"
down_revision: Union[str, Sequence[str], None] = "god_view_market_tables_20260918"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

log = logging.getLogger("alembic.runtime.migration")

# Verbatim (modulo formatting) from scripts/signal_taxonomy.py's runtime DDL.
# Exposed as a module-level tuple so a test can apply these statements
# directly, without going through alembic, against a scratch schema.
TAXONOMY_DDL: tuple[str, ...] = (
    "ALTER TABLE feature_registry ADD COLUMN IF NOT EXISTS signal_domain TEXT",
    "ALTER TABLE feature_registry ADD COLUMN IF NOT EXISTS signal_subtype TEXT",
    "CREATE INDEX IF NOT EXISTS idx_feature_domain ON feature_registry (signal_domain)",
    "CREATE INDEX IF NOT EXISTS idx_feature_subtype ON feature_registry (signal_subtype)",
)


def _taxonomy_columns_present(conn) -> int:
    """How many of the two taxonomy columns feature_registry currently has."""
    return conn.execute(
        text(
            "SELECT count(*) FROM information_schema.columns "
            "WHERE table_name = 'feature_registry' "
            "AND column_name IN ('signal_domain', 'signal_subtype')"
        )
    ).scalar()


def upgrade() -> None:
    """Add signal_domain/signal_subtype + their indexes, then say what changed.

    Every statement is individually idempotent, so on griddb (where
    scripts/signal_taxonomy.py already ran) this is expected to add 0 of 2
    columns. The before -> after line makes that expected no-op a stated
    one, exactly like raw_sql_port_20260914's before -> after count for
    feature_registry rows.
    """
    conn = op.get_bind()
    before = _taxonomy_columns_present(conn)

    for statement in TAXONOMY_DDL:
        op.execute(statement)

    after = _taxonomy_columns_present(conn)
    log.warning(
        "feature_registry signal taxonomy columns: %d of 2 present before, "
        "%d after (%d added). 2 before means this database already had them "
        "-- scripts/signal_taxonomy.py's runtime DDL (or a prior run of this "
        "revision) already applied -- which is the expected outcome on "
        "griddb. 0 before means a fresh-install database that had never run "
        "signal_taxonomy.py.",
        before, after, after - before,
    )


def downgrade() -> None:
    """Intentionally a no-op: do NOT drop signal_domain/signal_subtype.

    Production data written by scripts/signal_taxonomy.py (or by whatever
    autoresearch/taxonomy_fix runs have populated since) lives in these
    columns. Dropping them on downgrade would destroy that data to undo a
    revision whose whole purpose was to stop losing track of columns
    production already depends on. Log and return, same as this file's
    upgrade() logs rather than silently doing nothing.
    """
    log.warning(
        "feature_signal_taxonomy_20260919.downgrade() is a "
        "deliberate no-op: signal_domain/signal_subtype are left in place "
        "to avoid destroying production data written by "
        "scripts/signal_taxonomy.py. Reverting this revision does not "
        "remove the columns."
    )
