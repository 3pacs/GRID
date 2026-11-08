"""Machine-readable daily review: runs + per-claim verdicts (append-only).

Revision ID: pub_review_ingest_20261108
Revises: sleuth_intersections_20261107

The daily review of GRID's own publications (hourly briefings, thesis
snapshots) used to end as a document nobody read and nothing consumed. From
2026-11-08 its deliverable is ``review.jsonl`` (schema
``publication-review-verdicts-v1``: one header, one ``claim_verdict`` row per
graded claim, every row bound to the publication packet by hash and to the
publication text by a verbatim span). ``scripts/ingest_publication_review.py``
verifies a day's file against its packet and writes it here:

* ``publication_review_runs``    -- one row per ingested file (``review_sha256``
  UNIQUE: a re-run of the same file is a no-op), with the grader identity, the
  counts the header declared versus what the ingest accepted, and
  ``supersedes_run_id`` pointing at the day's previous run when a corrected file
  is ingested (analytics take the latest run per day).
* ``publication_claim_verdicts`` -- one row per accepted claim, both channels
  (``market_briefings`` and ``thesis_snapshots``). This is the single machine
  record. Nothing is projected into ``thesis_postmortems``: that table is
  ``intelligence/thesis_tracker.py``'s wrong-thesis analysis (enum root causes,
  under its scoring hold) and a grader row there would both pollute it and block
  the tracker's own postmortem for the snapshot. Consumers join on
  ``(channel, source_row_id)`` instead.

Both tables reject UPDATE, DELETE and TRUNCATE: a correction is a new review
file (new ``review_sha256``), never an edit. GRANT footer per
``migrations/_TEMPLATE.sql``: ``grid`` gets SELECT, INSERT (and sequence usage).
"""

from alembic import op

revision = "pub_review_ingest_20261108"
down_revision = "sleuth_intersections_20261107"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS publication_review_runs (
            id                  BIGSERIAL PRIMARY KEY,
            publication_day     DATE NOT NULL,
            review_sha256       TEXT NOT NULL UNIQUE CHECK (review_sha256 ~ '^[0-9a-f]{64}$'),
            packet_sha256       TEXT NOT NULL CHECK (packet_sha256 ~ '^[0-9a-f]{64}$'),
            schema_version      TEXT NOT NULL,
            grader              JSONB NOT NULL DEFAULT '{}'::jsonb,
            adjudicator         JSONB,
            graded_at           TIMESTAMPTZ NOT NULL,
            declared_total      INTEGER,
            declared_graded     INTEGER,
            declared_unscorable INTEGER,
            accepted_rows       INTEGER NOT NULL,
            rejected_rows       INTEGER NOT NULL,
            supersedes_run_id   BIGINT REFERENCES publication_review_runs(id),
            source_path         TEXT,
            ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_publication_review_runs_day
        ON publication_review_runs (publication_day, ingested_at DESC)
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS publication_claim_verdicts (
            id                    BIGSERIAL PRIMARY KEY,
            run_id                BIGINT NOT NULL REFERENCES publication_review_runs(id),
            publication_day       DATE NOT NULL,
            channel               TEXT NOT NULL
                                  CHECK (channel IN ('market_briefings', 'thesis_snapshots', 'market_diary')),
            publication_id        TEXT NOT NULL,
            source_row_id         INTEGER NOT NULL,
            publication_sha256    TEXT NOT NULL CHECK (publication_sha256 ~ '^[0-9a-f]{64}$'),
            stated_at             TIMESTAMPTZ NOT NULL,
            claim_span            TEXT NOT NULL CHECK (length(claim_span) BETWEEN 1 AND 300),
            claim_sha256          TEXT NOT NULL CHECK (claim_sha256 ~ '^[0-9a-f]{64}$'),
            claim_type            TEXT NOT NULL
                                  CHECK (claim_type IN ('direction', 'level', 'event', 'causal', 'data_quality')),
            instrument            TEXT,
            horizon_end           TIMESTAMPTZ,
            verdict               TEXT NOT NULL
                                  CHECK (verdict IN ('confirmed', 'refuted', 'partially_confirmed',
                                                     'unresolved', 'unscorable')),
            evidence              JSONB NOT NULL DEFAULT '[]'::jsonb,
            knowable_at_stated_at BOOLEAN,
            thesis_direction      TEXT CHECK (thesis_direction IN ('bullish', 'bearish', 'neutral')),
            actual_direction      TEXT CHECK (actual_direction IN ('bullish', 'bearish', 'neutral')),
            root_cause            TEXT,
            lesson                TEXT,
            grader_confidence     DOUBLE PRECISION
                                  CHECK (grader_confidence IS NULL OR grader_confidence BETWEEN 0 AND 1),
            created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (run_id, publication_id, claim_sha256)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_publication_claim_verdicts_day_verdict
        ON publication_claim_verdicts (publication_day, verdict)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_publication_claim_verdicts_publication
        ON publication_claim_verdicts (channel, source_row_id)
    """)
    op.execute("""
        CREATE OR REPLACE FUNCTION grid_reject_publication_review_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION '% is append-only: % refused (ingest a new review file instead)',
                TG_TABLE_NAME, TG_OP USING ERRCODE = 'restrict_violation';
        END
        $$
    """)
    op.execute("DROP TRIGGER IF EXISTS trg_publication_review_runs_append_only ON publication_review_runs")
    op.execute("""
        CREATE TRIGGER trg_publication_review_runs_append_only
        BEFORE UPDATE OR DELETE ON publication_review_runs
        FOR EACH ROW EXECUTE FUNCTION grid_reject_publication_review_mutation()
    """)
    op.execute("DROP TRIGGER IF EXISTS trg_publication_review_runs_no_truncate ON publication_review_runs")
    op.execute("""
        CREATE TRIGGER trg_publication_review_runs_no_truncate
        BEFORE TRUNCATE ON publication_review_runs
        FOR EACH STATEMENT EXECUTE FUNCTION grid_reject_publication_review_mutation()
    """)
    op.execute("DROP TRIGGER IF EXISTS trg_publication_claim_verdicts_append_only ON publication_claim_verdicts")
    op.execute("""
        CREATE TRIGGER trg_publication_claim_verdicts_append_only
        BEFORE UPDATE OR DELETE ON publication_claim_verdicts
        FOR EACH ROW EXECUTE FUNCTION grid_reject_publication_review_mutation()
    """)
    op.execute("DROP TRIGGER IF EXISTS trg_publication_claim_verdicts_no_truncate ON publication_claim_verdicts")
    op.execute("""
        CREATE TRIGGER trg_publication_claim_verdicts_no_truncate
        BEFORE TRUNCATE ON publication_claim_verdicts
        FOR EACH STATEMENT EXECUTE FUNCTION grid_reject_publication_review_mutation()
    """)
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
                EXECUTE 'GRANT SELECT, INSERT ON publication_review_runs TO grid';
                EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE publication_review_runs_id_seq TO grid';
                EXECUTE 'GRANT SELECT, INSERT ON publication_claim_verdicts TO grid';
                EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE publication_claim_verdicts_id_seq TO grid';
            END IF;
        END
        $$
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP TABLE IF EXISTS publication_claim_verdicts")
    op.execute("DROP TABLE IF EXISTS publication_review_runs")
    op.execute("DROP FUNCTION IF EXISTS grid_reject_publication_review_mutation()")
