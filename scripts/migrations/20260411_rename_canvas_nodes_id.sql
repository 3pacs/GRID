-- Rename canvas_nodes.id → canvas_nodes.node_id
--
-- Production router code (6 files, 163 references) writes INSERTs,
-- UPDATEs and SELECTs against `node_id`, but the original Alembic
-- migration created the column as `id`. The mismatch has silently made
-- all canvas_predict / canvas_expand / canvas_investigate write paths
-- fail at runtime. Aligning the schema with the majority of the code is
-- the smallest safe fix.
--
-- PostgreSQL RENAME COLUMN is atomic and does not break foreign keys:
-- canvas_edges.source_node_id and canvas_edges.target_node_id reference
-- the column by OID, not by name, so the FK constraints follow the rename
-- automatically.
--
-- Idempotent: checks the column name before renaming so the migration
-- can be re-applied without error.

BEGIN;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'canvas_nodes'
          AND column_name = 'id'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'canvas_nodes'
          AND column_name = 'node_id'
    ) THEN
        EXECUTE 'ALTER TABLE canvas_nodes RENAME COLUMN id TO node_id';
    END IF;
END $$;

COMMIT;
