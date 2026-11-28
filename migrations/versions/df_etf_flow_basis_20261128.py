"""dollar_flows: every stored etf_flow row names its direction basis.

ETF dollar volume is unsigned. The current normalizer (``_normalize_etf_flows``)
writes an ``etf_flow`` row with ``evidence.direction_basis`` and direction
'neutral' unless a signed measure exists. An older rule stored every ETF row
as 'inflow' with no basis; the daily writer rebuilds only its own window, so
693 such rows stayed on production (2026-02-27..11-13), counted as inflows by
the 30- and 60-day flow aggregator views.

This revision adds the CHECK ``dollar_flows_etf_flow_direction_basis`` NOT
VALID, so no writer can store that shape again, and changes no row. It never
validates, for the reason ``df_13f_observed_20261127`` gives: a VALIDATE in
this transaction would scan the table under ACCESS EXCLUSIVE. Validation is
the owned re-derive step (scripts/rederive_dollar_flows_etf_legacy.py
``apply``, or ``validate`` on a table with nothing to re-derive), under SHARE
ROW EXCLUSIVE. The 13F constraint is not touched.

Running the upgrade again is a no-op. A database without ``dollar_flows`` is
left alone (the writer's DDL creates the table with the constraint).
"""
from alembic import op

revision = 'df_etf_flow_basis_20261128'
down_revision = 'df_13f_observed_20261127'
branch_labels = None
depends_on = None

CONSTRAINT = 'dollar_flows_etf_flow_direction_basis'
CHECK_SQL = (
    "(source_type <> 'etf_flow' OR (COALESCE(jsonb_typeof(evidence -> 'direction_basis'), '') = 'string' "
    "AND evidence ->> 'direction_basis' <> '' "
    "AND (direction = 'neutral' OR evidence ->> 'direction_basis' <> 'unsigned_volume')))"
)


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    op.execute("""DO $$
    BEGIN
      IF to_regclass('dollar_flows') IS NULL THEN
        RETURN;
      END IF;
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = '""" + CONSTRAINT + """' AND conrelid = to_regclass('dollar_flows')
      ) THEN
        ALTER TABLE dollar_flows ADD CONSTRAINT """ + CONSTRAINT + """ CHECK """ + CHECK_SQL + """ NOT VALID;
      END IF;
    END $$""")


def downgrade():
    # A constraint holds no data; dropping it loses nothing.
    op.execute("ALTER TABLE IF EXISTS dollar_flows DROP CONSTRAINT IF EXISTS " + CONSTRAINT)
