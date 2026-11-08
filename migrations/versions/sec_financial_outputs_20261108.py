"""Bind persisted financial outputs to SEC/TTM source generations.

Old report rows are retained unchanged and excluded from current views until
recomputed with a verified generation. No historical financial repair occurs.
"""
from alembic import op

revision = 'sec_financial_outputs_20261108'
down_revision = 'sec_financial_current_20261108'
branch_labels = None
depends_on = None

GENERATION_SQL = """
CREATE OR REPLACE VIEW sec_financial_generation AS
SELECT md5(jsonb_build_object(
    'heads', COALESCE((SELECT jsonb_agg(jsonb_build_array(
        actor_id, snapshot_id, request_started_at AT TIME ZONE 'UTC',
        received_at AT TIME ZONE 'UTC') ORDER BY actor_id)
        FROM sec_financial_snapshot_heads), '[]'::jsonb),
    'ttm', COALESCE((SELECT jsonb_agg(jsonb_build_array(
        actor_id, sec_snapshot_id, quarter_fingerprint, computed_at AT TIME ZONE 'UTC')
        ORDER BY actor_id) FROM capital_flows_ttm_state), '[]'::jsonb)
)::text) AS generation
"""

EMPTY_REPORTS_SQL = """
DO $$ BEGIN
  IF to_regclass('public.fundamental_divergence') IS NULL THEN
    CREATE TABLE public.fundamental_divergence (
      id SERIAL PRIMARY KEY, ticker TEXT NOT NULL, as_of DATE NOT NULL, sector TEXT,
      fundamental_score NUMERIC, price_score NUMERIC, divergence NUMERIC,
      classification TEXT, narrative TEXT, UNIQUE(ticker, as_of));
    CREATE INDEX idx_fund_div_class ON public.fundamental_divergence(classification);
    CREATE INDEX idx_fund_div_divergence ON public.fundamental_divergence(divergence DESC);
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='grid') THEN
      GRANT SELECT, INSERT, UPDATE, DELETE ON public.fundamental_divergence TO grid;
      GRANT USAGE, SELECT ON SEQUENCE public.fundamental_divergence_id_seq TO grid;
    END IF;
  END IF;
  IF to_regclass('public.sector_health_snapshots') IS NULL THEN
    CREATE TABLE public.sector_health_snapshots (
      id SERIAL PRIMARY KEY, sector_name TEXT NOT NULL, score NUMERIC, components JSONB,
      snapshot_date DATE NOT NULL, as_of TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(sector_name, snapshot_date));
    CREATE INDEX idx_sector_health_date ON public.sector_health_snapshots(snapshot_date DESC);
    CREATE INDEX idx_sector_health_sector_date ON public.sector_health_snapshots(sector_name,snapshot_date DESC);
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='grid') THEN
      GRANT SELECT, INSERT, UPDATE, DELETE ON public.sector_health_snapshots TO grid;
      GRANT USAGE, SELECT ON SEQUENCE public.sector_health_snapshots_id_seq TO grid;
    END IF;
  END IF;
END $$
"""


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    op.execute(EMPTY_REPORTS_SQL)
    op.execute("ALTER TABLE fundamental_divergence ADD COLUMN IF NOT EXISTS sec_generation TEXT")
    op.execute("ALTER TABLE sector_health_snapshots ADD COLUMN IF NOT EXISTS sec_generation TEXT")
    op.execute(GENERATION_SQL)
    op.execute("""CREATE OR REPLACE VIEW fundamental_divergence_current AS
        SELECT d.* FROM fundamental_divergence d
        CROSS JOIN sec_financial_generation g WHERE d.sec_generation=g.generation""")
    op.execute("""CREATE OR REPLACE VIEW sector_health_snapshots_current AS
        SELECT s.* FROM sector_health_snapshots s
        CROSS JOIN sec_financial_generation g WHERE s.sec_generation=g.generation""")
    op.execute("CREATE INDEX IF NOT EXISTS idx_fund_div_generation ON fundamental_divergence(sec_generation,as_of DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_sector_health_generation ON sector_health_snapshots(sec_generation,snapshot_date DESC)")
    op.execute("""DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='grid') THEN
        GRANT SELECT ON sec_financial_generation, fundamental_divergence_current,
            sector_health_snapshots_current TO grid;
      END IF; END $$""")


def downgrade():
    # Keep original report rows and their binding evidence; coordinated code
    # rollback is required, rather than silently serving unqualified raw rows.
    op.execute("DROP VIEW IF EXISTS fundamental_divergence_current")
    op.execute("DROP VIEW IF EXISTS sector_health_snapshots_current")
    op.execute("DROP VIEW IF EXISTS sec_financial_generation")
