"""Ledger of every options call the 100x digest emails (options_digest_calls).

Before this revision nothing ``alerts/hundredx_digest.py`` put in an email
was recorded anywhere, so no emailed call could be scored and the email
could not show a track record. The digest now writes one row per call
BEFORE it hands the email to the sender, and sends nothing when this insert
(or the ledger read) fails.

Rows are immutable, like the decision journal: UPDATE, DELETE and TRUNCATE
raise for every role, and ``grid`` is granted only SELECT and INSERT.
Outcomes are never written back; ``trading/options_tracker.py``
(``load_digest_track_record``) scores each row on read from prices dated at
or before the scoring date.

Revision ID: options_digest_calls_20261104
Revises: merge_yf_finra_20261023
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "options_digest_calls_20261104"
down_revision = "merge_yf_finra_20261023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
    CREATE TABLE IF NOT EXISTS options_digest_calls (
        id                    BIGSERIAL PRIMARY KEY,
        digest_run_id         TEXT NOT NULL,
        emailed_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        ticker                TEXT NOT NULL,
        direction             TEXT NOT NULL CHECK (direction IN ('CALL', 'PUT')),
        expiry                DATE NOT NULL,
        strike                DOUBLE PRECISION NOT NULL CHECK (strike > 0),
        bid                   DOUBLE PRECISION NOT NULL CHECK (bid > 0),
        ask                   DOUBLE PRECISION NOT NULL CHECK (ask >= bid),
        quote_at              TIMESTAMPTZ NOT NULL,
        quote_source          TEXT NOT NULL,
        contract_iv           DOUBLE PRECISION,
        contract_oi           BIGINT,
        underlying_spot       DOUBLE PRECISION NOT NULL CHECK (underlying_spot > 0),
        spot_source           TEXT NOT NULL,
        breakeven             DOUBLE PRECISION NOT NULL,
        prob_beyond_breakeven DOUBLE PRECISION
            CHECK (prob_beyond_breakeven IS NULL
                   OR (prob_beyond_breakeven >= 0 AND prob_beyond_breakeven <= 1)),
        invalidation_level    DOUBLE PRECISION,
        score                 DOUBLE PRECISION NOT NULL,
        signal_date           DATE,
        signals               JSONB NOT NULL DEFAULT '{}'::jsonb,
        catalyst              JSONB,
        llm_verdict           TEXT,
        provenance            JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """)
    # Hot paths: the track record (latest N calls) and the re-send check.
    op.execute("""
    CREATE INDEX IF NOT EXISTS idx_options_digest_calls_emailed
        ON options_digest_calls (emailed_at DESC)
    """)
    op.execute("""
    CREATE INDEX IF NOT EXISTS idx_options_digest_calls_contract
        ON options_digest_calls (ticker, direction, expiry, strike, emailed_at DESC)
    """)
    op.execute("""
    CREATE OR REPLACE FUNCTION options_digest_calls_append_only_guard() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
        RAISE EXCEPTION 'options_digest_calls is append-only: % on % refused',
            TG_OP, TG_TABLE_NAME USING ERRCODE = 'restrict_violation';
    END
    $$;
    DROP TRIGGER IF EXISTS options_digest_calls_no_row_mutation ON options_digest_calls;
    CREATE TRIGGER options_digest_calls_no_row_mutation
        BEFORE UPDATE OR DELETE ON options_digest_calls
        FOR EACH ROW EXECUTE FUNCTION options_digest_calls_append_only_guard();
    DROP TRIGGER IF EXISTS options_digest_calls_no_truncate ON options_digest_calls;
    CREATE TRIGGER options_digest_calls_no_truncate
        BEFORE TRUNCATE ON options_digest_calls
        FOR EACH STATEMENT EXECUTE FUNCTION options_digest_calls_append_only_guard();
    """)

    # ====== GRANT FOOTER (REQUIRED) ======
    # Migrations run as `postgres`; the digest connects as `grid`. Append-only
    # is also a privilege: no UPDATE / DELETE / TRUNCATE for grid.
    op.execute("""
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
            GRANT SELECT, INSERT ON options_digest_calls TO grid;
            GRANT USAGE, SELECT ON SEQUENCE options_digest_calls_id_seq TO grid;
        END IF;
    END $$;
    """)


def downgrade() -> None:
    bind = op.get_bind()
    rows = bind.execute(sa.text("SELECT count(*) FROM options_digest_calls")).scalar()
    if rows:
        raise RuntimeError(
            f"options_digest_calls holds {rows} emailed calls; they are the digest's "
            "track record and are append-only. Refusing to drop them."
        )
    op.execute("DROP TABLE options_digest_calls")
    op.execute("DROP FUNCTION IF EXISTS options_digest_calls_append_only_guard()")
