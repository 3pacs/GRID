"""market_state_provisional: the display-only market-state record computed at a session's close (#1312).

Revision ID: market_state_prov_20261122
Revises: regime_history_revs_20261122
Create Date: 2026-11-22 00:00:00.000000

Owner's decision (bring-up plan rev 100): #1191's previous-session rule stays for anything that decides. A record
computed at the close is display only, labelled provisional, and kept in this table, never in
``market_state_daily``. The writer is ``scripts/run_market_state.py --provisional``; the only reader is the post-close
desk brief (``analysis/desk_brief_sources.py``).

* ``provisional BOOLEAN NOT NULL DEFAULT TRUE CHECK (provisional)``: a non-provisional row cannot exist here.
* ``record`` JSONB, ``computed_at`` TIMESTAMPTZ (the insert or replace clock, read the same in any session TimeZone).
* Insert-only per session from the writer; ``--replace`` rewrites a row, so there is no insert-only trigger.

Production never creates this table at runtime: ``intelligence/regime/market_state.py`` creates it on SQLite (tests)
only, and on Postgres its store refuses to write while the table is missing, naming this revision.
``schema.sql`` declares the same table (the migration replay applies schema.sql first, so the statement is
idempotent); ``tests/test_market_state_provisional_migration.py`` keeps the two identical.
Downgrade refuses while any row exists.
"""

from alembic import op

# revision identifiers, used by Alembic (version_num is VARCHAR(32), so the id is shorter than the table's name).
revision = "market_state_prov_20261122"
down_revision = "regime_history_revs_20261122"
branch_labels = None
depends_on = None


# Also verbatim in schema.sql.
TABLE = """
CREATE TABLE IF NOT EXISTS market_state_provisional (
    as_of_date   DATE PRIMARY KEY,
    rule         TEXT NOT NULL,
    record       JSONB NOT NULL,
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    provisional  BOOLEAN NOT NULL DEFAULT TRUE CHECK (provisional)
)
"""

# GRANT footer (migrations/_TEMPLATE.sql). The date primary key has no sequence. On griddb the migration runs as the
# application role, which then owns the table; the grant matters where a superuser applies migrations.
GRANTS = """
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
        EXECUTE 'GRANT ALL ON market_state_provisional TO grid';
    END IF;
END
$$
"""


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    op.execute(TABLE)
    op.execute(GRANTS)


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM market_state_provisional) THEN
                RAISE EXCEPTION 'market_state_provisional holds records; refusing to drop it';
            END IF;
        END
        $$
        """
    )
    op.execute("DROP TABLE IF EXISTS market_state_provisional")
