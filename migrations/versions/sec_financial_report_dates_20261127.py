"""Durable EDGAR reportDate evidence on the SEC financial snapshot archive.

A snapshot whose admission depended on EDGAR submissions reportDate evidence
(a start-less mapped-flow point tolerated as an event) cannot be reproduced from
Company Facts bytes, normalized rows and FX inputs alone. This nullable JSONB
column records the canonical accession -> reportDate mapping that was used and a
reference to the EDGAR submissions response it came from (url, sha256, receipt
time). Additive and nullable: every existing row and every ordinary snapshot
stays NULL; the application writer still only inserts.
"""
from alembic import op

revision = 'sec_fin_report_dates_20261127'
down_revision = 'market_state_revs_20261125'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    op.execute('''ALTER TABLE sec_financial_snapshots
        ADD COLUMN IF NOT EXISTS report_date_evidence JSONB
        CHECK (report_date_evidence IS NULL OR jsonb_typeof(report_date_evidence) = 'object')''')


def downgrade():
    # Retain immutable evidence; a column drop is not a deployment rollback side effect.
    pass
