-- pgvector + RAG embeddings table
-- Run: psql -U grid_user -d grid -f migrations/add_pgvector_rag.sql

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS embeddings (
    id              BIGSERIAL PRIMARY KEY,
    source_type     TEXT NOT NULL,          -- knowledge, actor, briefing, filing, news
    source_id       TEXT NOT NULL,          -- unique doc identifier
    chunk_text      TEXT NOT NULL,
    embedding       vector(768),            -- nomic-embed-text dimension
    metadata        JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- HNSW index for fast cosine similarity search
CREATE INDEX IF NOT EXISTS idx_embeddings_hnsw
    ON embeddings USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

CREATE INDEX IF NOT EXISTS idx_embeddings_source
    ON embeddings (source_type, source_id);

-- ICIJ tables
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS icij_entities (
    id              BIGSERIAL PRIMARY KEY,
    node_id         BIGINT UNIQUE NOT NULL,
    name            TEXT NOT NULL,
    jurisdiction    TEXT,
    country_codes   TEXT,
    incorporation_date TEXT,
    inactivation_date TEXT,
    status          TEXT,
    source_dataset  TEXT NOT NULL,          -- panama, paradise, pandora, bahamas, offshore
    service_provider TEXT,
    address         TEXT,
    note            TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS icij_officers (
    id              BIGSERIAL PRIMARY KEY,
    node_id         BIGINT UNIQUE NOT NULL,
    name            TEXT NOT NULL,
    country_codes   TEXT,
    source_dataset  TEXT NOT NULL,
    valid_until     TEXT,
    note            TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS icij_intermediaries (
    id              BIGSERIAL PRIMARY KEY,
    node_id         BIGINT UNIQUE NOT NULL,
    name            TEXT NOT NULL,
    country_codes   TEXT,
    source_dataset  TEXT NOT NULL,
    status          TEXT,
    address         TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS icij_addresses (
    id              BIGSERIAL PRIMARY KEY,
    node_id         BIGINT UNIQUE NOT NULL,
    address         TEXT NOT NULL,
    country_codes   TEXT,
    source_dataset  TEXT NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS icij_relationships (
    id              BIGSERIAL PRIMARY KEY,
    from_node       BIGINT NOT NULL,
    to_node         BIGINT NOT NULL,
    rel_type        TEXT NOT NULL,
    source_dataset  TEXT NOT NULL,
    start_date      TEXT,
    end_date        TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_icij_rel_from ON icij_relationships (from_node);
CREATE INDEX IF NOT EXISTS idx_icij_rel_to ON icij_relationships (to_node);

-- Trigram indexes for fuzzy matching
CREATE INDEX IF NOT EXISTS idx_icij_entities_name_trgm ON icij_entities USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_icij_officers_name_trgm ON icij_officers USING gin (name gin_trgm_ops);

CREATE TABLE IF NOT EXISTS icij_actor_matches (
    id              BIGSERIAL PRIMARY KEY,
    icij_node_id    BIGINT NOT NULL,
    icij_node_type  TEXT NOT NULL,          -- entity, officer, intermediary
    actor_name      TEXT NOT NULL,
    match_type      TEXT NOT NULL,          -- exact, fuzzy, alias
    similarity      REAL NOT NULL,          -- 0-1 match confidence
    confirmed       BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Actors table — matches server schema exactly
-- Only create if not exists (server already has it with seed data)
CREATE TABLE IF NOT EXISTS actors (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    tier            TEXT NOT NULL,
    category        TEXT NOT NULL,
    title           TEXT,
    net_worth_estimate NUMERIC,
    aum             NUMERIC,
    influence_score NUMERIC DEFAULT 0.5,
    trust_score     NUMERIC DEFAULT 0.5,
    motivation_model TEXT DEFAULT 'unknown',
    connections     JSONB DEFAULT '[]',
    known_positions JSONB DEFAULT '[]',
    board_seats     JSONB DEFAULT '[]',
    political_affiliations JSONB DEFAULT '[]',
    data_sources    JSONB DEFAULT '[]',
    credibility     TEXT DEFAULT 'inferred',
    metadata        JSONB DEFAULT '{}',
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source          TEXT DEFAULT 'unknown',
    degree          INTEGER DEFAULT 0,
    icij_node_id    TEXT
);

CREATE INDEX IF NOT EXISTS idx_actors_name_trgm ON actors USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_actors_tier ON actors (tier);
CREATE INDEX IF NOT EXISTS idx_actors_category ON actors (category);
CREATE INDEX IF NOT EXISTS idx_actors_source ON actors (source);
CREATE INDEX IF NOT EXISTS idx_actors_influence ON actors (influence_score DESC);
CREATE INDEX IF NOT EXISTS idx_actors_name ON actors (name);
CREATE INDEX IF NOT EXISTS idx_actors_updated ON actors (updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_actors_degree ON actors (degree);
CREATE INDEX IF NOT EXISTS idx_actors_icij_node ON actors (icij_node_id) WHERE icij_node_id IS NOT NULL;

-- Attention anomaly tracking
CREATE TABLE IF NOT EXISTS attention_anomaly (
    id              BIGSERIAL PRIMARY KEY,
    entity_name     TEXT NOT NULL,
    ticker          TEXT,
    anomaly_date    DATE NOT NULL,
    wikipedia_zscore REAL,
    trends_breakout  REAL,
    combined_score  REAL NOT NULL,          -- 0-100
    price_move_5d   REAL,
    source          TEXT NOT NULL,          -- wikipedia, trends, combined
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_attention_anomaly_date ON attention_anomaly (anomaly_date DESC);
CREATE INDEX IF NOT EXISTS idx_attention_anomaly_entity ON attention_anomaly (entity_name, anomaly_date DESC);
