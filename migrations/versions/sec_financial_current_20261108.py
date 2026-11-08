"""Snapshot-aware financial read projection; retain all original ledger rows.

A snapshot head is authoritative for supported direct annual/quarter flows.
Missing means unavailable, never a fallback to a legacy or seeded value.
TTM remains hidden until recomputed against that same snapshot identity.
"""
from alembic import op

revision = 'sec_financial_current_20261108'
down_revision = 'sec_financial_snap_20261107'
branch_labels = None
depends_on = None

# Frozen migration SQL, deliberately independent of mutable application imports.
VIEW_SQL = """
CREATE VIEW capital_flows_current AS
WITH qualified AS (
    SELECT h.actor_id, n.*
    FROM sec_financial_snapshot_heads h
    JOIN sec_financial_snapshots s ON s.snapshot_id = h.snapshot_id
    CROSS JOIN LATERAL jsonb_to_recordset(s.normalized_rows) AS n(
        fiscal_period DATE, period_type TEXT, flow_type TEXT, direction TEXT,
        amount_usd NUMERIC, currency TEXT, confidence TEXT, source_filing TEXT)
), eligible AS (
    SELECT cf.*,
        (cf.period_type IN ('annual', 'quarter')
         AND NULLIF(cf.counterparty_id, '') IS NULL
         AND cf.flow_type IN ('revenue','cogs','opex','r_and_d','capex',
             'interest_paid','tax','dividends','buybacks','acquisitions','debt_issuance','equity_issuance')) AS direct_financial
    FROM capital_flows cf
    LEFT JOIN sec_financial_snapshot_heads h ON h.actor_id = lower(cf.actor_id)
    LEFT JOIN qualified q ON q.actor_id = cf.actor_id
        AND q.fiscal_period = cf.fiscal_period AND q.period_type = cf.period_type
        AND q.flow_type = cf.flow_type AND q.direction = cf.direction
        AND q.amount_usd = cf.amount_usd AND q.currency = cf.currency
        AND q.confidence = cf.confidence AND q.source_filing = cf.source_filing
    LEFT JOIN capital_flows_ttm_state ts ON ts.actor_id = h.actor_id
    WHERE h.actor_id IS NULL
       OR NULLIF(cf.counterparty_id, '') IS NOT NULL
       OR cf.flow_type NOT IN ('revenue','cogs','opex','r_and_d','capex',
             'interest_paid','tax','dividends','buybacks','acquisitions','debt_issuance','equity_issuance')
       OR cf.period_type NOT IN ('annual', 'quarter', 'ttm')
       OR (cf.period_type IN ('annual', 'quarter') AND q.actor_id IS NOT NULL)
       OR (cf.actor_id = h.actor_id AND cf.period_type = 'ttm'
           AND cf.source_filing = 'ttm_rollup'
           AND ts.sec_snapshot_id = h.snapshot_id)
), ranked AS (
    SELECT eligible.*, ROW_NUMBER() OVER (
        PARTITION BY actor_id, fiscal_period, period_type, flow_type, direction,
                     COALESCE(NULLIF(counterparty_id, ''), '__none__')
        ORDER BY CASE
            WHEN source_filing LIKE '10-%' THEN 1
            WHEN source_filing LIKE '20-%' THEN 2
            WHEN source_filing LIKE '8-%' THEN 3
            WHEN source_filing LIKE 'seed%' THEN 5 ELSE 4 END,
            CASE confidence WHEN 'confirmed' THEN 1 WHEN 'derived' THEN 2
                 WHEN 'estimated' THEN 3 WHEN 'rumored' THEN 4
                 WHEN 'inferred' THEN 5 ELSE 6 END,
            as_of DESC NULLS LAST, id DESC
    ) AS current_rank FROM eligible
)
SELECT id, actor_id, fiscal_period, period_type, flow_type, direction,
       amount_usd, counterparty_id, source_filing, confidence, as_of, currency
FROM ranked WHERE NOT direct_financial OR current_rank = 1
"""


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    op.execute("ALTER TABLE capital_flows_ttm_state ADD COLUMN sec_snapshot_id TEXT")
    op.execute(VIEW_SQL)
    op.execute("""DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='grid') THEN
        GRANT SELECT ON capital_flows_current TO grid;
      END IF;
    END $$""")


def downgrade():
    op.execute("DROP VIEW capital_flows_current")
    op.execute("ALTER TABLE capital_flows_ttm_state DROP COLUMN sec_snapshot_id")
