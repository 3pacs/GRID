"""dollar_flows: every stored 13F row is an observation (direction 'observed').

Owner decision 2026-11-23 ("Yes, non-directional"): no 13F row is a
directional flow. The normalizer writes 13F rows with direction 'observed',
but it rebuilds only the dates inside its run window, so rows an older rule
wrote stay in their old shape at every earlier date. On 2026-11-27 production
held 49 13F portfolio summaries stored as 'inflow' (2026-11-06..11-13).

This revision adds the CHECK ``dollar_flows_13f_observed_only`` NOT VALID, so
no writer can store that shape again, and changes no row. It never validates:
adding a NOT VALID CHECK holds ACCESS EXCLUSIVE only for the catalog change,
while a VALIDATE in the same transaction would scan the table under that lock
and block every reader. Validation is the owned repair step
(scripts/repair_dollar_flows_stored_contract.py ``apply``, or ``validate`` on a
table with nothing to remove), which scans under SHARE ROW EXCLUSIVE.

Running the upgrade again is a no-op. A database without ``dollar_flows`` is
left alone (the writer's DDL creates the table with the constraint).
"""
from alembic import op

revision = 'df_13f_observed_20261127'
down_revision = 'sec_fin_report_dates_20261127'
branch_labels = None
depends_on = None

CONSTRAINT = 'dollar_flows_13f_observed_only'
CHECK_SQL = "(source_type <> '13f' OR direction = 'observed')"


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
