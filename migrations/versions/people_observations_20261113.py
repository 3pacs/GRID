"""Append-only bounded observations of committed canonical people-event state.

No backfill, scheduler, or existing reader activation. Downgrade refuses data loss.
"""
from alembic import op

revision = 'people_observations_20261113'
down_revision = 'finra_dix_feature_20261113'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
    CREATE TABLE IF NOT EXISTS people_event_observations (
        request_id TEXT PRIMARY KEY CHECK (length(request_id) BETWEEN 1 AND 128),
        scope_sha256 TEXT NOT NULL CHECK (scope_sha256 ~ '^[0-9a-f]{64}$'),
        observation_contract TEXT NOT NULL,
        observed_at TIMESTAMPTZ NOT NULL,
        payload_text TEXT NOT NULL,
        payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
        receipt_sha256 TEXT NOT NULL CHECK (receipt_sha256 ~ '^[0-9a-f]{64}$')
    );
    CREATE OR REPLACE FUNCTION people_observations_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN RAISE EXCEPTION 'people_event_observations is append-only'; END $$;
    DROP TRIGGER IF EXISTS people_observations_no_mutation ON people_event_observations;
    CREATE TRIGGER people_observations_no_mutation BEFORE UPDATE OR DELETE OR TRUNCATE
      ON people_event_observations FOR EACH STATEMENT EXECUTE FUNCTION people_observations_immutable();
    """)


def downgrade():
    op.execute("""
    LOCK TABLE people_event_observations IN ACCESS EXCLUSIVE MODE;
    DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM people_event_observations) THEN
        RAISE EXCEPTION 'refusing to drop durable observation receipts';
      END IF;
    END $$;
    DROP TABLE people_event_observations;
    DROP FUNCTION people_observations_immutable();
    """)
