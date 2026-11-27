"""Durable EDGAR reportDate evidence on the SEC financial snapshot archive.

A snapshot whose admission depended on EDGAR submissions reportDate evidence
(a start-less mapped-flow point tolerated as an event) cannot be reproduced from
Company Facts bytes, normalized rows and FX inputs alone. Two additive nullable
columns record, for such snapshots only:

* report_date_evidence JSONB: the canonical accession -> reportDate mapping that
  was used and the reference to the EDGAR submissions response it came from
  (matched SEC CIK url, lowercase sha256, timezone-aware receipt time);
* report_date_source_gzip BYTEA: that response itself, so the pairs can be
  re-derived independently of the puller.

CHECKs make the shape a validation, not only an object test (a missing key
fails rather than passing as NULL), and require the two columns to be present
together or not at all. Every existing row and every
ordinary snapshot stays NULL; the application writer still only inserts.
"""
from alembic import op

revision = 'sec_fin_report_dates_20261127'
down_revision = 'market_state_revs_20261125'
branch_labels = None
depends_on = None

_EVIDENCE_OK = """(report_date_evidence IS NULL OR COALESCE((
        jsonb_typeof(report_date_evidence) = 'object'
        AND jsonb_typeof(report_date_evidence -> 'report_dates') = 'object'
        AND report_date_evidence -> 'report_dates' <> CAST('{}' AS JSONB)
        AND jsonb_typeof(report_date_evidence -> 'source') = 'object'
        AND (report_date_evidence -> 'source' ->> 'url') ~ '^https://data[.]sec[.]gov/submissions/CIK[0-9]{10}[.]json$'
        AND (report_date_evidence -> 'source' ->> 'sha256') ~ '^[0-9a-f]{64}$'
        AND (report_date_evidence -> 'source' ->> 'received_at')
            ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}[:][0-9]{2}[:][0-9]{2}([.][0-9]+)?([+-][0-9]{2}[:][0-9]{2}|Z)$'
    ), FALSE))"""


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    op.execute("ALTER TABLE sec_financial_snapshots ADD COLUMN IF NOT EXISTS report_date_evidence JSONB")
    op.execute("ALTER TABLE sec_financial_snapshots ADD COLUMN IF NOT EXISTS report_date_source_gzip BYTEA")
    op.execute("""DO $$ BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'sec_fin_snapshots_report_evidence_ok') THEN
        ALTER TABLE sec_financial_snapshots ADD CONSTRAINT sec_fin_snapshots_report_evidence_ok CHECK """ + _EVIDENCE_OK + """;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'sec_fin_snapshots_report_evidence_pair') THEN
        ALTER TABLE sec_financial_snapshots ADD CONSTRAINT sec_fin_snapshots_report_evidence_pair
          CHECK ((report_date_evidence IS NULL) = (report_date_source_gzip IS NULL));
      END IF;
    END $$""")


def downgrade():
    # Retain immutable evidence; a column drop is not a deployment rollback side effect.
    pass
