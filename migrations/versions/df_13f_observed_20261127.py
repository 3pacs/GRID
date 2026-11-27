"""dollar_flows: every stored 13F row is an observation (direction 'observed').

Owner decision 2026-11-23 ("Yes, non-directional"): no 13F row is a
directional flow. The normalizer writes 13F rows with direction 'observed',
but it rebuilds only the dates inside its run window, so rows an older rule
wrote stay in their old shape at every earlier date. On 2026-11-27 production
held 49 13F portfolio summaries stored as 'inflow' (2026-11-06..11-13).

This revision adds the CHECK ``dollar_flows_13f_observed_only`` so no writer
can store that shape again. Existing rows are not touched:

* the constraint is added NOT VALID, so it is enforced for every new or
  updated row at once and the upgrade cannot fail on the old rows;
* it is validated in the same upgrade only when no stored row breaks it;
  otherwise a NOTICE says so, and the reviewed repair
  (scripts/repair_dollar_flows_stored_contract.py) validates it after it
  removes the rows.

Running the upgrade again is a no-op except that it validates a NOT VALID
constraint once the table is clean. A database without ``dollar_flows`` is
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
    DECLARE
      n_bad bigint;
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
      IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = '""" + CONSTRAINT + """' AND conrelid = to_regclass('dollar_flows')
          AND NOT convalidated
      ) THEN
        SELECT count(*) INTO n_bad FROM dollar_flows WHERE NOT """ + CHECK_SQL + """;
        IF n_bad = 0 THEN
          ALTER TABLE dollar_flows VALIDATE CONSTRAINT """ + CONSTRAINT + """;
        ELSE
          RAISE NOTICE '""" + CONSTRAINT + """ left NOT VALID: % stored row(s) break it; '
            'remove them with scripts/repair_dollar_flows_stored_contract.py', n_bad;
        END IF;
      END IF;
    END $$""")


def downgrade():
    # A constraint holds no data; dropping it loses nothing.
    op.execute("ALTER TABLE IF EXISTS dollar_flows DROP CONSTRAINT IF EXISTS " + CONSTRAINT)
