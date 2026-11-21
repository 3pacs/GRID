"""Versioned SEC financial source snapshots, with a separate current pointer.

No historical capital_flows rows are rewritten. Original compressed source
bytes and normalized outputs are append-only through the application writer.
"""
from alembic import op

revision = 'sec_financial_snap_20261107'
down_revision = 'sleuth_intersections_20261107'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    op.execute('''CREATE TABLE IF NOT EXISTS sec_financial_snapshots (
        snapshot_id TEXT PRIMARY KEY CHECK (length(snapshot_id)=64),
        actor_id TEXT NOT NULL, cik TEXT NOT NULL, source_url TEXT NOT NULL,
        source_sha256 TEXT NOT NULL CHECK (length(source_sha256)=64),
        normalized_sha256 TEXT NOT NULL CHECK (length(normalized_sha256)=64),
        normalizer_version TEXT NOT NULL, source_gzip BYTEA NOT NULL,
        normalized_rows JSONB NOT NULL CHECK (jsonb_typeof(normalized_rows)='array'),
        fx_inputs JSONB NOT NULL CHECK (jsonb_typeof(fx_inputs)='object'),
        first_request_started_at TIMESTAMPTZ NOT NULL,
        first_received_at TIMESTAMPTZ NOT NULL,
        archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CHECK (first_request_started_at <= first_received_at),
        UNIQUE (actor_id, snapshot_id)
    )''')
    op.execute('''CREATE TABLE IF NOT EXISTS sec_financial_snapshot_heads (
        actor_id TEXT PRIMARY KEY, snapshot_id TEXT NOT NULL,
        request_started_at TIMESTAMPTZ NOT NULL, received_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CHECK (request_started_at <= received_at),
        FOREIGN KEY (actor_id, snapshot_id) REFERENCES sec_financial_snapshots (actor_id, snapshot_id)
    )''')
    op.execute('''DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='grid') THEN
        EXECUTE 'GRANT SELECT, INSERT ON sec_financial_snapshots TO grid';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE ON sec_financial_snapshot_heads TO grid';
      END IF;
    END $$''')


def downgrade():
    # Retain immutable evidence and current pointers; destructive removal is
    # deliberately not a deployment rollback side effect.
    pass
