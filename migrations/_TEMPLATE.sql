-- Migration: 00XX_<description>.sql
-- Author: <agent>
-- Applies via: sudo -u postgres psql griddb -f path/to/this/file.sql
--
-- GRANT FOOTER: every migration MUST grant table + sequence privileges to the
-- `grid` role, or the API and ingestors will get permission denied at runtime.
-- The lint script `scripts/lint_migrations.py` enforces this — if a migration
-- creates a new table without the matching `GRANT ALL ON <table> TO grid;`
-- the lint will fail and CI/pre-commit will block the commit.
--
-- Why this exists: migrations run as the `postgres` superuser (so they can
-- CREATE EXTENSION, ALTER SCHEMA, etc.), but the API and every ingestor
-- connect as the unprivileged `grid` role. New tables created by `postgres`
-- are owned by `postgres` and produce `permission denied for table X` the
-- first time the API tries to read them — unless explicit grants are given.
--
-- Idempotency: every CREATE / GRANT statement uses IF NOT EXISTS / re-issuing
-- the GRANT is a no-op, so this template is safe on re-run.

-- ====== SCHEMA CHANGES ======

CREATE TABLE IF NOT EXISTS your_table_here (
    id SERIAL PRIMARY KEY
    -- ... add columns here ...
);

CREATE INDEX IF NOT EXISTS idx_your_table_col ON your_table_here(id);

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- Without these grants the `grid` user will see `permission denied for
-- table your_table_here` the first time the API tries to read it.
--
-- For each new table:
--   GRANT ALL ON <table> TO grid;
-- For each new SERIAL / BIGSERIAL primary key (auto-creates a sequence):
--   GRANT USAGE, SELECT ON SEQUENCE <table>_<col>_seq TO grid;
-- For tables created in a non-public schema:
--   GRANT USAGE ON SCHEMA <schema> TO grid;
--   GRANT ALL ON <schema>.<table> TO grid;

GRANT ALL ON your_table_here TO grid;
GRANT USAGE, SELECT ON SEQUENCE your_table_here_id_seq TO grid;
