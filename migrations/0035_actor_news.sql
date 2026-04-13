-- Migration: 0035_actor_news
-- Purpose: Store free-source news mentions and biographical context for
-- every actor in analysis/sector_map.py — especially the lesser-known
-- non-ticker actors (activists, family offices, CEOs, trade groups,
-- private mega-caps, regulators). Traditional per-ticker news ingestion
-- returns nothing for these; we need name-based search across Google
-- News RSS, GDELT DOC, Wikipedia, SEC EDGAR full-text, and CrossRef.
--
-- actor_news stores individual article mentions with extracted stance
-- markers and loyalty signals. actor_bio stores a Wikipedia-derived
-- biographical snapshot with affiliations and positions held.
--
-- Idempotent via UNIQUE (actor_id, source, url). Hermes will refresh
-- daily for top 200 actors and weekly for the remainder. Runs in
-- ingestion/altdata/actor_news_puller.py + scripts/run_actor_news.py.

CREATE TABLE IF NOT EXISTS actor_news (
    id              SERIAL PRIMARY KEY,
    actor_id        TEXT NOT NULL,
    source          TEXT NOT NULL,
    url             TEXT,
    title           TEXT,
    snippet         TEXT,
    published_at    TIMESTAMPTZ,
    sentiment       NUMERIC,
    stance_markers  TEXT[],
    loyalty_signals TEXT[],
    raw_content     TEXT,
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (actor_id, source, url)
);

CREATE INDEX IF NOT EXISTS idx_actor_news_actor
    ON actor_news(actor_id, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_actor_news_source
    ON actor_news(source);
CREATE INDEX IF NOT EXISTS idx_actor_news_ingested
    ON actor_news(ingested_at DESC);

CREATE TABLE IF NOT EXISTS actor_bio (
    actor_id          TEXT PRIMARY KEY,
    wikipedia_url     TEXT,
    wikipedia_extract TEXT,
    affiliations      TEXT[],
    positions         TEXT[],
    net_worth_usd     NUMERIC,
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- Migrations run as `postgres`; the API and ingestors connect as `grid`.
-- Without these grants the `grid` user gets `permission denied for table`.
GRANT ALL ON actor_news, actor_bio TO grid;
GRANT USAGE, SELECT ON SEQUENCE actor_news_id_seq TO grid;
