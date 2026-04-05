-- schema_obsidian.sql
-- Obsidian Bridge tables — run after schema.sql
-- Execute: psql -U grid_user -d grid -f schema_obsidian.sql

CREATE TABLE IF NOT EXISTS obsidian_notes (
    id              SERIAL PRIMARY KEY,
    vault_path      TEXT NOT NULL UNIQUE,
    domain          TEXT NOT NULL DEFAULT 'grid',
    status          TEXT NOT NULL DEFAULT 'inbox',
    title           TEXT NOT NULL DEFAULT '',
    content_hash    TEXT NOT NULL DEFAULT '',
    frontmatter     JSONB NOT NULL DEFAULT '{}',
    body            TEXT NOT NULL DEFAULT '',
    body_tsvector   TSVECTOR,
    agent_flags     JSONB NOT NULL DEFAULT '{}',
    synced_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_obsidian_notes_domain ON obsidian_notes(domain);
CREATE INDEX IF NOT EXISTS idx_obsidian_notes_status ON obsidian_notes(status);
CREATE INDEX IF NOT EXISTS idx_obsidian_notes_fts ON obsidian_notes USING gin(body_tsvector);
CREATE INDEX IF NOT EXISTS idx_obsidian_notes_agent_flags ON obsidian_notes USING gin(agent_flags);

-- Auto-update tsvector on insert/update
CREATE OR REPLACE FUNCTION obsidian_notes_tsvector_update() RETURNS trigger AS $$
BEGIN
    NEW.body_tsvector := to_tsvector('english', COALESCE(NEW.title, '') || ' ' || COALESCE(NEW.body, ''));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_obsidian_notes_tsvector ON obsidian_notes;
CREATE TRIGGER trg_obsidian_notes_tsvector
    BEFORE INSERT OR UPDATE OF title, body ON obsidian_notes
    FOR EACH ROW EXECUTE FUNCTION obsidian_notes_tsvector_update();

CREATE TABLE IF NOT EXISTS obsidian_actions (
    id              SERIAL PRIMARY KEY,
    note_id         INTEGER NOT NULL REFERENCES obsidian_notes(id) ON DELETE CASCADE,
    actor           TEXT NOT NULL,
    action          TEXT NOT NULL,
    detail          JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_obsidian_actions_note_id ON obsidian_actions(note_id);
CREATE INDEX IF NOT EXISTS idx_obsidian_actions_created ON obsidian_actions(created_at DESC);
