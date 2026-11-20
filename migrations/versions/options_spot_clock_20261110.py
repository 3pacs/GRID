"""Keep exact first-quote spot clocks and provider for future options captures.

Existing immutable batches remain NULL: neither contract-row clocks nor
capture completion can reconstruct their first response's quote timestamp.
"""

from alembic import op

revision = "options_spot_clock_20261110"
down_revision = "oracle_forward_v2_20261110"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    op.execute("ALTER TABLE options_capture_batches ADD COLUMN IF NOT EXISTS "
               "provider_regular_market_at TIMESTAMPTZ")
    op.execute("ALTER TABLE options_capture_batches ADD COLUMN IF NOT EXISTS spot_provider TEXT")
    # Existing table grants cover added columns; no new table or sequence.
    op.execute("""DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
            GRANT SELECT, INSERT ON options_capture_batches TO grid;
        END IF;
    END $$""")


def downgrade() -> None:
    raise RuntimeError("Refusing to erase frozen options quote provenance")
