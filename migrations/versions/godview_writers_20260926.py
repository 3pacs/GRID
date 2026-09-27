"""God view G2: provenance and point-in-time columns for the three pillar tables.

Revision ID: godview_writers_20260926
Revises: robinhood_guards_20260924

Slice G2 of the god-view materialization plan
(GRID-GODVIEW-MATERIALIZATION-PLAN-20260926.md, sections 2, 3, 4 and 6).
Schema only. Nothing here writes, rewrites, archives or deletes a row.

What it adds
------------
* ``godview_runs``: the run ledger every pillar writer (G3-G5) records into,
  in the same transaction as its rows. ``status`` is a closed domain
  (``GODVIEW_RUN_STATUSES``), including ``partial_blocked_by_legacy`` for a
  run that skipped keys still held by legacy rows (cleared by the A2 archive). The API's never-run / failed / stale
  states come from this table, not from guessing.
* On ``cftc_positioning_daily``, ``fed_net_liquidity_daily`` and
  ``dealer_gex_daily``: ``release_at``, ``available_at``,
  ``availability_basis``, ``provenance``, ``source_ref`` (jsonb: input series
  ids, per-input obs_date and pull_timestamp, batch ids), ``run_id``,
  ``code_sha``, ``updated_at``, ``coverage_fraction``.
* CFTC: ``cftc_market_code``, ``market_name`` (one CFTC market per row, never
  a name substring match).
* Fed: ``delta_1w_m``, ``delta_4w_m`` (exact prior H.4.1 Wednesdays). The
  legacy ``delta_5d_m`` / ``delta_30d_m`` stay as they are and new rows leave
  them NULL.
* GEX: the columns ``physics.dealer_gamma.DealerGammaEngine`` actually emits
  (``gex_aggregate``, ``gex_normalized``, ``gamma_flip``,
  ``gamma_flip_crossings``, ``regime``, walls, ``dealer_delta``,
  ``model_basis``, ``sign_convention``) plus chain and spot provenance.

What it relaxes
---------------
NOT NULL is dropped only where it would force a writer to invent a value:
``crowding_regime`` (NULL while the z-score window is short),
``liquidity_regime`` (NULL without enough history) and every legacy GEX value
column (the engine does not produce them in these units, so new rows leave
them NULL rather than converting by guesswork). The raw-input columns that a
writer must have to produce a row at all (CFTC open interest and legs, the
three fed legs and net liquidity) stay NOT NULL: a missing leg means no row.

Legacy rows
-----------
Every existing row keeps its values and reads ``provenance IS NULL``, which
the G6 view treats as excluded. Archiving or deleting them is a separate,
owner-approved step (A2 / A5).

Row-level guards
----------------
* ``provenance`` / ``availability_basis`` / GEX ``regime`` are closed domains.
* A row with a provenance must carry the whole receipt: run_id, code_sha,
  source_ref (a JSON object), release_at, available_at, availability_basis.
  CFTC rows must name their market code; GEX rows their capture batch and
  spot receipt.
* ``dealer_gex_daily.provenance`` can only be ``'modeled'``.
* ``run_id`` references ``godview_runs``; the check is DEFERRABLE INITIALLY
  DEFERRED so a writer may insert rows before its ledger row inside the same
  transaction.

Locks
-----
One ``ALTER TABLE`` per table, so each takes ACCESS EXCLUSIVE once, held
until commit. ``ADD COLUMN`` without a default and ``DROP NOT NULL`` are
catalog-only. The new CHECK and FOREIGN KEY constraints are validated by one
scan per table: 6,405 / 89 / 93 rows in production on 2026-09-26, i.e.
milliseconds. ``lock_timeout = '5s'`` makes the upgrade fail fast (and roll
back as a whole) instead of queueing behind a long reader.
``market_god_view_daily`` depends on all three tables; none of these
subcommands conflicts with that dependency, and the matview is not touched
(G6 decides view vs matview).

Downgrade
---------
Refuses once any writer has used the new schema (any row with a provenance,
or any ``godview_runs`` row): dropping the columns then would leave written
rows indistinguishable from legacy ones. Archive and remove those first.
Otherwise it drops the new constraints and columns, restores the NOT NULL
constraints and drops ``godview_runs``.
"""

from alembic import op

revision = "godview_writers_20260926"
down_revision = "robinhood_guards_20260924"
branch_labels = None
depends_on = None

_LOCK_TIMEOUT = "5s"
_STATEMENT_TIMEOUT = "30s"

# godview_runs.status domain (must match the CHECK in upgrade()).
# 'partial_blocked_by_legacy': the run wrote what it could but skipped keys
# still occupied by legacy (NULL-provenance) rows; the A2 archive clears them.
GODVIEW_RUN_STATUSES = (
    "running",
    "complete",
    "failed",
    "noop",
    "inputs_missing",
    "inputs_stale",
    "non_session",
    "no_completed_capture",
    "no_verified_spot",
    "partial_blocked_by_legacy",
)

# Legacy dealer_gex_daily value columns whose NOT NULL is dropped. obs_date and
# ticker (the natural key) stay NOT NULL.
GEX_LEGACY_VALUE_COLUMNS = (
    "spot_price",
    "net_gex_usd_m",
    "call_gex_usd_m",
    "put_gex_usd_m",
    "gamma_flip_strike",
    "spot_to_flip_pct",
    "gex_regime",
    "max_pain_strike",
    "put_call_oi_ratio",
    "atm_iv",
)


def _set_timeouts() -> None:
    # Literals, not f-strings (repo SQL rule); the PG test asserts they match
    # _LOCK_TIMEOUT / _STATEMENT_TIMEOUT.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")


def upgrade() -> None:
    _set_timeouts()

    # ------------------------------------------------------------------ ledger
    op.execute("""
        CREATE TABLE IF NOT EXISTS godview_runs (
            run_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            pillar            TEXT NOT NULL,
            started_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            finished_at       TIMESTAMPTZ,
            status            TEXT NOT NULL DEFAULT 'running',
            rows_written      INTEGER NOT NULL DEFAULT 0,
            rows_skipped      INTEGER NOT NULL DEFAULT 0,
            reasons           JSONB,
            input_watermarks  JSONB,
            error             TEXT,
            code_sha          TEXT NOT NULL,
            CONSTRAINT godview_runs_pillar_chk CHECK (
                pillar IN ('fed_liquidity', 'cftc', 'dealer_gex', 'view_refresh')
            ),
            CONSTRAINT godview_runs_status_chk CHECK (
                status IN (
                    'running', 'complete', 'failed', 'noop',
                    'inputs_missing', 'inputs_stale', 'non_session',
                    'no_completed_capture', 'no_verified_spot',
                    'partial_blocked_by_legacy'
                )
            ),
            CONSTRAINT godview_runs_counts_chk CHECK (
                rows_written >= 0 AND rows_skipped >= 0
            ),
            CONSTRAINT godview_runs_finished_chk CHECK (
                (status = 'running') = (finished_at IS NULL)
                AND (finished_at IS NULL OR finished_at >= started_at)
            ),
            CONSTRAINT godview_runs_code_sha_chk CHECK (length(code_sha) > 0)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_godview_runs_pillar_started
        ON godview_runs (pillar, started_at DESC)
    """)

    # -------------------------------------------------------------------- CFTC
    op.execute("""
        ALTER TABLE cftc_positioning_daily
            ADD COLUMN IF NOT EXISTS release_at         TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS available_at       TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS availability_basis TEXT,
            ADD COLUMN IF NOT EXISTS provenance         TEXT,
            ADD COLUMN IF NOT EXISTS source_ref         JSONB,
            ADD COLUMN IF NOT EXISTS run_id             UUID,
            ADD COLUMN IF NOT EXISTS code_sha           TEXT,
            ADD COLUMN IF NOT EXISTS updated_at         TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS coverage_fraction  DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS cftc_market_code   TEXT,
            ADD COLUMN IF NOT EXISTS market_name        TEXT,
            ALTER COLUMN crowding_regime DROP NOT NULL,
            DROP CONSTRAINT IF EXISTS cftc_positioning_daily_gv_domain_chk,
            ADD CONSTRAINT cftc_positioning_daily_gv_domain_chk CHECK (
                (provenance IS NULL OR provenance IN ('measured', 'derived', 'modeled'))
                AND (availability_basis IS NULL OR availability_basis IN
                     ('observed_acquisition', 'inferred_schedule', 'unknown'))
                AND (coverage_fraction IS NULL OR coverage_fraction BETWEEN 0 AND 1)
                AND (source_ref IS NULL OR jsonb_typeof(source_ref) = 'object')
            ),
            DROP CONSTRAINT IF EXISTS cftc_positioning_daily_gv_receipt_chk,
            ADD CONSTRAINT cftc_positioning_daily_gv_receipt_chk CHECK (
                provenance IS NULL OR (
                    run_id IS NOT NULL AND code_sha IS NOT NULL AND source_ref IS NOT NULL
                    AND release_at IS NOT NULL AND available_at IS NOT NULL
                    AND availability_basis IS NOT NULL AND cftc_market_code IS NOT NULL
                )
            ),
            DROP CONSTRAINT IF EXISTS cftc_positioning_daily_gv_run_fk,
            ADD CONSTRAINT cftc_positioning_daily_gv_run_fk FOREIGN KEY (run_id)
                REFERENCES godview_runs (run_id) DEFERRABLE INITIALLY DEFERRED
    """)

    # --------------------------------------------------------------------- Fed
    op.execute("""
        ALTER TABLE fed_net_liquidity_daily
            ADD COLUMN IF NOT EXISTS release_at         TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS available_at       TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS availability_basis TEXT,
            ADD COLUMN IF NOT EXISTS provenance         TEXT,
            ADD COLUMN IF NOT EXISTS source_ref         JSONB,
            ADD COLUMN IF NOT EXISTS run_id             UUID,
            ADD COLUMN IF NOT EXISTS code_sha           TEXT,
            ADD COLUMN IF NOT EXISTS updated_at         TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS coverage_fraction  DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS delta_1w_m         DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS delta_4w_m         DOUBLE PRECISION,
            ALTER COLUMN liquidity_regime DROP NOT NULL,
            DROP CONSTRAINT IF EXISTS fed_net_liquidity_daily_gv_domain_chk,
            ADD CONSTRAINT fed_net_liquidity_daily_gv_domain_chk CHECK (
                (provenance IS NULL OR provenance IN ('measured', 'derived', 'modeled'))
                AND (availability_basis IS NULL OR availability_basis IN
                     ('observed_acquisition', 'inferred_schedule', 'unknown'))
                AND (coverage_fraction IS NULL OR coverage_fraction BETWEEN 0 AND 1)
                AND (source_ref IS NULL OR jsonb_typeof(source_ref) = 'object')
            ),
            DROP CONSTRAINT IF EXISTS fed_net_liquidity_daily_gv_receipt_chk,
            ADD CONSTRAINT fed_net_liquidity_daily_gv_receipt_chk CHECK (
                provenance IS NULL OR (
                    run_id IS NOT NULL AND code_sha IS NOT NULL AND source_ref IS NOT NULL
                    AND release_at IS NOT NULL AND available_at IS NOT NULL
                    AND availability_basis IS NOT NULL
                )
            ),
            DROP CONSTRAINT IF EXISTS fed_net_liquidity_daily_gv_run_fk,
            ADD CONSTRAINT fed_net_liquidity_daily_gv_run_fk FOREIGN KEY (run_id)
                REFERENCES godview_runs (run_id) DEFERRABLE INITIALLY DEFERRED
    """)

    # --------------------------------------------------------------------- GEX
    op.execute("""
        ALTER TABLE dealer_gex_daily
            ADD COLUMN IF NOT EXISTS release_at                 TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS available_at               TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS availability_basis         TEXT,
            ADD COLUMN IF NOT EXISTS provenance                 TEXT,
            ADD COLUMN IF NOT EXISTS source_ref                 JSONB,
            ADD COLUMN IF NOT EXISTS run_id                     UUID,
            ADD COLUMN IF NOT EXISTS code_sha                   TEXT,
            ADD COLUMN IF NOT EXISTS updated_at                 TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS coverage_fraction          DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS spot                       DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS gex_aggregate              DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS gex_normalized             DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS gamma_flip                 DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS gamma_flip_crossings       INTEGER,
            ADD COLUMN IF NOT EXISTS regime                     TEXT,
            ADD COLUMN IF NOT EXISTS gamma_wall                 DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS put_wall                   DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS call_wall                  DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS dealer_delta               DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS model_basis                TEXT,
            ADD COLUMN IF NOT EXISTS sign_convention            TEXT,
            ADD COLUMN IF NOT EXISTS chain_capture_batch_id     TEXT,
            ADD COLUMN IF NOT EXISTS chain_capture_ordinal      BIGINT,
            ADD COLUMN IF NOT EXISTS chain_capture_started_at   TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS chain_capture_completed_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS spot_source                TEXT,
            ADD COLUMN IF NOT EXISTS spot_basis                 TEXT,
            ADD COLUMN IF NOT EXISTS spot_obs_date              DATE,
            ADD COLUMN IF NOT EXISTS spot_available_at          TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS spot_receipt_id            BIGINT,
            ALTER COLUMN spot_price        DROP NOT NULL,
            ALTER COLUMN net_gex_usd_m     DROP NOT NULL,
            ALTER COLUMN call_gex_usd_m    DROP NOT NULL,
            ALTER COLUMN put_gex_usd_m     DROP NOT NULL,
            ALTER COLUMN gamma_flip_strike DROP NOT NULL,
            ALTER COLUMN spot_to_flip_pct  DROP NOT NULL,
            ALTER COLUMN gex_regime        DROP NOT NULL,
            ALTER COLUMN max_pain_strike   DROP NOT NULL,
            ALTER COLUMN put_call_oi_ratio DROP NOT NULL,
            ALTER COLUMN atm_iv            DROP NOT NULL,
            DROP CONSTRAINT IF EXISTS dealer_gex_daily_gv_domain_chk,
            ADD CONSTRAINT dealer_gex_daily_gv_domain_chk CHECK (
                (provenance IS NULL OR provenance = 'modeled')
                AND (availability_basis IS NULL OR availability_basis IN
                     ('observed_acquisition', 'inferred_schedule', 'unknown'))
                AND (coverage_fraction IS NULL OR coverage_fraction BETWEEN 0 AND 1)
                AND (source_ref IS NULL OR jsonb_typeof(source_ref) = 'object')
                AND (regime IS NULL OR regime IN ('LONG_GAMMA', 'SHORT_GAMMA', 'NEUTRAL'))
                AND (gamma_flip_crossings IS NULL OR gamma_flip_crossings >= 0)
            ),
            DROP CONSTRAINT IF EXISTS dealer_gex_daily_gv_receipt_chk,
            ADD CONSTRAINT dealer_gex_daily_gv_receipt_chk CHECK (
                provenance IS NULL OR (
                    run_id IS NOT NULL AND code_sha IS NOT NULL AND source_ref IS NOT NULL
                    AND release_at IS NOT NULL AND available_at IS NOT NULL
                    AND availability_basis IS NOT NULL
                    AND chain_capture_batch_id IS NOT NULL
                    AND chain_capture_completed_at IS NOT NULL
                    AND spot_receipt_id IS NOT NULL
                )
            ),
            DROP CONSTRAINT IF EXISTS dealer_gex_daily_gv_run_fk,
            ADD CONSTRAINT dealer_gex_daily_gv_run_fk FOREIGN KEY (run_id)
                REFERENCES godview_runs (run_id) DEFERRABLE INITIALLY DEFERRED
    """)

    op.execute("""
        COMMENT ON TABLE godview_runs IS
        'God-view pillar writer run ledger (G2). One row per writer run, written in the same transaction as its rows.'
    """)
    op.execute("""
        COMMENT ON COLUMN cftc_positioning_daily.provenance IS
        'NULL = legacy/unverified row, excluded from the god view. Set only by the godview writers.'
    """)
    op.execute("""
        COMMENT ON COLUMN fed_net_liquidity_daily.provenance IS
        'NULL = legacy/unverified row, excluded from the god view. Set only by the godview writers.'
    """)
    op.execute("""
        COMMENT ON COLUMN dealer_gex_daily.provenance IS
        'NULL = legacy/unverified row, excluded from the god view. Otherwise always ''modeled'': dealer side assumed, not measured.'
    """)

    # GRANT footer (migrations/_TEMPLATE.sql). Production runs alembic as `grid`,
    # which already owns the three altered tables; the grant covers a deploy that
    # runs as another role. godview_runs has a UUID key, so there is no sequence.
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
                EXECUTE 'GRANT ALL ON godview_runs TO grid';
            END IF;
        END
        $$
    """)


def downgrade() -> None:
    _set_timeouts()

    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM cftc_positioning_daily WHERE provenance IS NOT NULL)
               OR EXISTS (SELECT 1 FROM fed_net_liquidity_daily WHERE provenance IS NOT NULL)
               OR EXISTS (SELECT 1 FROM dealer_gex_daily WHERE provenance IS NOT NULL)
               OR EXISTS (SELECT 1 FROM godview_runs)
            THEN
                RAISE EXCEPTION 'godview_writers_20260926 downgrade refused: god-view writer rows exist (provenance IS NOT NULL or godview_runs non-empty). Archive and remove them first; dropping the columns would make them indistinguishable from legacy rows.';
            END IF;
        END
        $$
    """)

    op.execute("""
        ALTER TABLE dealer_gex_daily
            DROP CONSTRAINT IF EXISTS dealer_gex_daily_gv_run_fk,
            DROP CONSTRAINT IF EXISTS dealer_gex_daily_gv_receipt_chk,
            DROP CONSTRAINT IF EXISTS dealer_gex_daily_gv_domain_chk,
            DROP COLUMN IF EXISTS spot_receipt_id,
            DROP COLUMN IF EXISTS spot_available_at,
            DROP COLUMN IF EXISTS spot_obs_date,
            DROP COLUMN IF EXISTS spot_basis,
            DROP COLUMN IF EXISTS spot_source,
            DROP COLUMN IF EXISTS chain_capture_completed_at,
            DROP COLUMN IF EXISTS chain_capture_started_at,
            DROP COLUMN IF EXISTS chain_capture_ordinal,
            DROP COLUMN IF EXISTS chain_capture_batch_id,
            DROP COLUMN IF EXISTS sign_convention,
            DROP COLUMN IF EXISTS model_basis,
            DROP COLUMN IF EXISTS dealer_delta,
            DROP COLUMN IF EXISTS call_wall,
            DROP COLUMN IF EXISTS put_wall,
            DROP COLUMN IF EXISTS gamma_wall,
            DROP COLUMN IF EXISTS regime,
            DROP COLUMN IF EXISTS gamma_flip_crossings,
            DROP COLUMN IF EXISTS gamma_flip,
            DROP COLUMN IF EXISTS gex_normalized,
            DROP COLUMN IF EXISTS gex_aggregate,
            DROP COLUMN IF EXISTS spot,
            DROP COLUMN IF EXISTS coverage_fraction,
            DROP COLUMN IF EXISTS updated_at,
            DROP COLUMN IF EXISTS code_sha,
            DROP COLUMN IF EXISTS run_id,
            DROP COLUMN IF EXISTS source_ref,
            DROP COLUMN IF EXISTS provenance,
            DROP COLUMN IF EXISTS availability_basis,
            DROP COLUMN IF EXISTS available_at,
            DROP COLUMN IF EXISTS release_at,
            ALTER COLUMN spot_price        SET NOT NULL,
            ALTER COLUMN net_gex_usd_m     SET NOT NULL,
            ALTER COLUMN call_gex_usd_m    SET NOT NULL,
            ALTER COLUMN put_gex_usd_m     SET NOT NULL,
            ALTER COLUMN gamma_flip_strike SET NOT NULL,
            ALTER COLUMN spot_to_flip_pct  SET NOT NULL,
            ALTER COLUMN gex_regime        SET NOT NULL,
            ALTER COLUMN max_pain_strike   SET NOT NULL,
            ALTER COLUMN put_call_oi_ratio SET NOT NULL,
            ALTER COLUMN atm_iv            SET NOT NULL
    """)

    op.execute("""
        ALTER TABLE fed_net_liquidity_daily
            DROP CONSTRAINT IF EXISTS fed_net_liquidity_daily_gv_run_fk,
            DROP CONSTRAINT IF EXISTS fed_net_liquidity_daily_gv_receipt_chk,
            DROP CONSTRAINT IF EXISTS fed_net_liquidity_daily_gv_domain_chk,
            DROP COLUMN IF EXISTS delta_4w_m,
            DROP COLUMN IF EXISTS delta_1w_m,
            DROP COLUMN IF EXISTS coverage_fraction,
            DROP COLUMN IF EXISTS updated_at,
            DROP COLUMN IF EXISTS code_sha,
            DROP COLUMN IF EXISTS run_id,
            DROP COLUMN IF EXISTS source_ref,
            DROP COLUMN IF EXISTS provenance,
            DROP COLUMN IF EXISTS availability_basis,
            DROP COLUMN IF EXISTS available_at,
            DROP COLUMN IF EXISTS release_at,
            ALTER COLUMN liquidity_regime SET NOT NULL
    """)

    op.execute("""
        ALTER TABLE cftc_positioning_daily
            DROP CONSTRAINT IF EXISTS cftc_positioning_daily_gv_run_fk,
            DROP CONSTRAINT IF EXISTS cftc_positioning_daily_gv_receipt_chk,
            DROP CONSTRAINT IF EXISTS cftc_positioning_daily_gv_domain_chk,
            DROP COLUMN IF EXISTS market_name,
            DROP COLUMN IF EXISTS cftc_market_code,
            DROP COLUMN IF EXISTS coverage_fraction,
            DROP COLUMN IF EXISTS updated_at,
            DROP COLUMN IF EXISTS code_sha,
            DROP COLUMN IF EXISTS run_id,
            DROP COLUMN IF EXISTS source_ref,
            DROP COLUMN IF EXISTS provenance,
            DROP COLUMN IF EXISTS availability_basis,
            DROP COLUMN IF EXISTS available_at,
            DROP COLUMN IF EXISTS release_at,
            ALTER COLUMN crowding_regime SET NOT NULL
    """)

    op.execute("DROP TABLE IF EXISTS godview_runs")
