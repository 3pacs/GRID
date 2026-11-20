"""GDELT 2.0 bulk event loader: per-file ledger and publication clocks.

``scripts/parse_gdelt.py`` created ``gdelt_events`` and ``gdelt_daily_summary``
with ``CREATE TABLE IF NOT EXISTS`` during a one-off backfill (the live tables
stop at 2026-03-19). ``ingestion/altdata/gdelt_bulk.py`` now loads GDELT's
15-minute export files incrementally, so the tables come under migrations
here, unchanged where they already exist, plus:

* ``gdelt_export_files`` -- one row per 15-minute export file: loaded (with
  its publication instant, from the server's Last-Modified header, and its
  aggregates) or missing (a 404 old enough to be final). The loader resumes
  from it and never fetches a recorded file twice.
* ``gdelt_events.date_added`` -- the file's DATEADDED stamp. NULL for the
  legacy rows, whose files were never recorded.
* ``gdelt_daily_summary.published_at`` / ``day_basis`` / ``files_loaded`` /
  ``files_missing`` -- when the day's last file was published and what the
  day groups by. NULL for the legacy rows (grouped by SQLDATE).

All column additions are nullable without defaults: catalogue-only changes,
no table rewrite on the 88M-row events table. No backfill.
"""

from alembic import op

revision = "gdelt_bulk_loader_20261116"
down_revision = "options_contract_clocks_20261114"
branch_labels = None
depends_on = None

#: Executed in order by upgrade(); tests/test_gdelt_bulk.py applies the same list.
DDL: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS gdelt_events (
        global_event_id    BIGINT PRIMARY KEY,
        event_date         DATE NOT NULL,
        actor1_country     TEXT,
        actor2_country     TEXT,
        event_root_code    TEXT,
        quad_class         INTEGER,
        goldstein_scale    DOUBLE PRECISION,
        num_mentions       INTEGER,
        num_sources        INTEGER,
        num_articles       INTEGER,
        avg_tone           DOUBLE PRECISION
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_gdelt_date ON gdelt_events (event_date)",
    "CREATE INDEX IF NOT EXISTS idx_gdelt_quad ON gdelt_events (quad_class)",
    "CREATE INDEX IF NOT EXISTS idx_gdelt_actor1 ON gdelt_events (actor1_country)",
    "ALTER TABLE gdelt_events ADD COLUMN IF NOT EXISTS date_added TIMESTAMPTZ",
    """
    CREATE TABLE IF NOT EXISTS gdelt_daily_summary (
        summary_date         DATE PRIMARY KEY,
        total_events         INTEGER,
        conflict_count       INTEGER,
        cooperation_count    INTEGER,
        avg_goldstein        DOUBLE PRECISION,
        avg_tone             DOUBLE PRECISION,
        total_mentions       BIGINT,
        country_events       JSONB
    )
    """,
    "ALTER TABLE gdelt_daily_summary ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ",
    "ALTER TABLE gdelt_daily_summary ADD COLUMN IF NOT EXISTS day_basis TEXT",
    "ALTER TABLE gdelt_daily_summary ADD COLUMN IF NOT EXISTS files_loaded INTEGER",
    "ALTER TABLE gdelt_daily_summary ADD COLUMN IF NOT EXISTS files_missing INTEGER",
    """
    CREATE TABLE IF NOT EXISTS gdelt_export_files (
        file_stamp      TIMESTAMPTZ PRIMARY KEY,
        url             TEXT NOT NULL,
        status          TEXT NOT NULL CHECK (status IN ('loaded', 'missing')),
        published_at    TIMESTAMPTZ,
        published_basis TEXT CHECK (published_basis IN ('last_modified', 'fetched_at')),
        fetched_at      TIMESTAMPTZ NOT NULL,
        md5             TEXT,
        size_bytes      INTEGER,
        rows_parsed     INTEGER,
        rows_rejected   INTEGER,
        rows_inserted   INTEGER,
        aggregates      JSONB,
        CHECK (status = 'missing' OR (published_at IS NOT NULL AND aggregates IS NOT NULL))
    )
    """,
    ("COMMENT ON TABLE gdelt_export_files IS "
     "'One row per GDELT 2.0 15-minute export file read by ingestion/altdata/gdelt_bulk.py'"),
    ("COMMENT ON COLUMN gdelt_export_files.published_at IS "
     "'When GDELT published the file: HTTP Last-Modified, else the fetch instant (later, so PIT-safe)'"),
    ("COMMENT ON COLUMN gdelt_events.date_added IS "
     "'GDELT DATEADDED (the export file stamp); NULL for rows loaded before gdelt_export_files existed'"),
    ("COMMENT ON COLUMN gdelt_daily_summary.day_basis IS "
     "'dateadded_v2: events in the 96 export files stamped on that UTC day; NULL: legacy SQLDATE grouping'"),
)


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    for statement in DDL:
        op.execute(statement)


def downgrade():
    raise RuntimeError("Refusing to drop the GDELT file ledger and publication clocks")
