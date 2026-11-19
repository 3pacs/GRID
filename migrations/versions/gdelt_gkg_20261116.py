"""GDELT GKG files in the bulk loader's ledger: a ``kind`` column and a GKG day table.

Slice 3 of moving GDELT onto its bulk files (design:
docs/specs/2026-11-16-gdelt-gkg-design.md, 3pacs/GRID#1234, agreed by Anik
2026-11-16). ``ingestion/altdata/gdelt_bulk.py`` reads GDELT 2.0's GKG files
(``<stamp>.gkg.csv.zip``) next to the export files, through the same ledger.

* ``gdelt_export_files.kind`` -- ``'export'`` (every existing row) or
  ``'gkg'``; the primary key becomes ``(kind, file_stamp)``, since both
  kinds share a stamp.
* ``gdelt_gkg_daily`` -- one row per finalised UTC day of GKG files: the
  day's summed per-file aggregates, how many of the 96 files loaded, the
  latest publication instant and the release date given to the day's
  ``gkg_*`` feature points (the later of D+1 and that instant's UTC date).

Raw GKG documents are not stored. Additive apart from the primary key
swap, which rewrites no rows. Downgrade refuses.
"""

from alembic import op

revision = "gdelt_gkg_20261116"
down_revision = "gdelt_bulk_loader_20261116"
branch_labels = None
depends_on = None

DDL = (
    "ALTER TABLE gdelt_export_files ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'export'",
    """
    DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'gdelt_export_files_kind_check') THEN
            ALTER TABLE gdelt_export_files ADD CONSTRAINT gdelt_export_files_kind_check
                CHECK (kind IN ('export', 'gkg'));
        END IF;
        IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'gdelt_export_files_pkey'
                   AND conrelid = 'gdelt_export_files'::regclass
                   AND array_length(conkey, 1) = 1) THEN
            ALTER TABLE gdelt_export_files DROP CONSTRAINT gdelt_export_files_pkey;
            ALTER TABLE gdelt_export_files ADD CONSTRAINT gdelt_export_files_pkey PRIMARY KEY (kind, file_stamp);
        END IF;
    END $$
    """,
    """
    CREATE TABLE IF NOT EXISTS gdelt_gkg_daily (
        summary_date  DATE PRIMARY KEY,
        docs          INTEGER,
        aggregates    JSONB,
        files_loaded  INTEGER NOT NULL,
        files_missing INTEGER NOT NULL,
        published_at  TIMESTAMPTZ,
        release_date  DATE,
        finalised_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        CHECK (release_date IS NULL OR release_date > summary_date)
    )
    """,
    ("COMMENT ON COLUMN gdelt_export_files.kind IS "
     "'export: GDELT 2.0 export file; gkg: GDELT 2.0 GKG 2.1 file (aggregates only, documents not stored)'"),
    ("COMMENT ON TABLE gdelt_gkg_daily IS "
     "'One row per UTC day of the 96 GKG files stamped on it, finalised by ingestion/altdata/gdelt_bulk.py'"),
)


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    for statement in DDL:
        op.execute(statement)


def downgrade():
    raise RuntimeError("Refusing to drop the GDELT GKG ledger kind and day table")
