"""causal_links: point-in-time provenance columns + causal_link_runs (slice N2).

``causal_links`` was created at runtime by ``intelligence.causation_core.
ensure_table`` and has 0 rows in production (checked 2026-09-27). The new
writer (``scripts/run_causal_links.py`` -> ``intelligence.causal_links``)
needs, per edge: a stable ``edge_key`` for idempotent upserts, the known_at
of the trade, of the event and of the edge, and the run id / code sha that
produced it. ``causal_link_runs`` records each run, so the views can label
what they show "as of" a finished run.

Additive only: CREATE TABLE IF NOT EXISTS for the legacy shape (fresh
databases), ADD COLUMN IF NOT EXISTS for the new columns (all nullable, so
legacy rows stay valid and are simply not shown — readers require
``edge_key IS NOT NULL``), one unique index on ``edge_key`` (NULLs do not
collide), and the run table. The table is empty in production, so the
index builds instantly.

Downgrade drops the run table, the indexes and the added columns.
"""

from alembic import op

revision = "causal_links_provenance_20260927"
down_revision = "raw_series_quarantined_20260926"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS causal_links (
            id              SERIAL PRIMARY KEY,
            signal_id       INT,
            actor           TEXT,
            ticker          TEXT,
            action_date     DATE,
            cause_type      TEXT,
            probable_cause  TEXT,
            evidence        JSONB,
            probability     NUMERIC,
            created_at      TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    op.execute("""
        ALTER TABLE causal_links
            ADD COLUMN IF NOT EXISTS edge_key               TEXT,
            ADD COLUMN IF NOT EXISTS action                 TEXT,
            ADD COLUMN IF NOT EXISTS action_channel         TEXT,
            ADD COLUMN IF NOT EXISTS action_known_at        TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS action_known_at_basis  TEXT,
            ADD COLUMN IF NOT EXISTS event_kind             TEXT,
            ADD COLUMN IF NOT EXISTS event_key              TEXT,
            ADD COLUMN IF NOT EXISTS event_date             DATE,
            ADD COLUMN IF NOT EXISTS event_known_at         TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS event_known_at_basis   TEXT,
            ADD COLUMN IF NOT EXISTS known_at               TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS lead_time_days         NUMERIC,
            ADD COLUMN IF NOT EXISTS score_method           TEXT,
            ADD COLUMN IF NOT EXISTS run_id                 TEXT,
            ADD COLUMN IF NOT EXISTS first_run_id           TEXT,
            ADD COLUMN IF NOT EXISTS code_sha               TEXT,
            ADD COLUMN IF NOT EXISTS computed_at            TIMESTAMPTZ
    """)
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_causal_links_edge_key "
        "ON causal_links (edge_key)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_causal_links_ticker_action "
        "ON causal_links (ticker, action_date DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_causal_links_known_at "
        "ON causal_links (known_at DESC)"
    )
    op.execute("""
        CREATE TABLE IF NOT EXISTS causal_link_runs (
            run_id             TEXT PRIMARY KEY,
            started_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            finished_at        TIMESTAMPTZ,
            as_of              TIMESTAMPTZ NOT NULL,
            code_sha           TEXT NOT NULL,
            params             JSONB,
            status             TEXT NOT NULL
                               CHECK (status IN ('running', 'succeeded', 'failed')),
            tickers_processed  INT NOT NULL DEFAULT 0,
            actions_processed  INT NOT NULL DEFAULT 0,
            edges_found        INT NOT NULL DEFAULT 0,
            edges_written      INT NOT NULL DEFAULT 0,
            error              TEXT
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_causal_link_runs_finished "
        "ON causal_link_runs (status, finished_at DESC)"
    )

    # GRANT footer (migrations/_TEMPLATE.sql). Alembic runs as the owner role;
    # the API and the scheduler connect as `grid`.
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
                EXECUTE 'GRANT ALL ON causal_links TO grid';
                EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE causal_links_id_seq TO grid';
                EXECUTE 'GRANT ALL ON causal_link_runs TO grid';
            END IF;
        END
        $$
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")
    op.execute("DROP TABLE IF EXISTS causal_link_runs")
    op.execute("DROP INDEX IF EXISTS idx_causal_links_known_at")
    op.execute("DROP INDEX IF EXISTS idx_causal_links_ticker_action")
    op.execute("DROP INDEX IF EXISTS uq_causal_links_edge_key")
    op.execute("""
        ALTER TABLE causal_links
            DROP COLUMN IF EXISTS computed_at,
            DROP COLUMN IF EXISTS code_sha,
            DROP COLUMN IF EXISTS first_run_id,
            DROP COLUMN IF EXISTS run_id,
            DROP COLUMN IF EXISTS score_method,
            DROP COLUMN IF EXISTS lead_time_days,
            DROP COLUMN IF EXISTS known_at,
            DROP COLUMN IF EXISTS event_known_at_basis,
            DROP COLUMN IF EXISTS event_known_at,
            DROP COLUMN IF EXISTS event_date,
            DROP COLUMN IF EXISTS event_key,
            DROP COLUMN IF EXISTS event_kind,
            DROP COLUMN IF EXISTS action_known_at_basis,
            DROP COLUMN IF EXISTS action_known_at,
            DROP COLUMN IF EXISTS action_channel,
            DROP COLUMN IF EXISTS action
    """)
