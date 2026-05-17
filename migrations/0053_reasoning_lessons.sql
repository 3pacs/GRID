-- ReasoningBank-style memory layer for distilled trade/prediction lessons.
--
-- Stores compact, immutable strategy items derived from BOTH success and
-- failure paths (postmortems, oracle scoring, hypothesis kills). Lessons
-- are keyed on a condition fingerprint (regime / fci / vol / horizon /
-- ticker / direction) and retrieved at decision time to inject prior
-- experience into new predictions.
--
-- Inspired by Google's ReasoningBank (2025). Insert-only, like the
-- decision journal — no UPDATE / DELETE in normal operation.

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS reasoning_lessons (
    id                    BIGSERIAL PRIMARY KEY,
    title                 TEXT NOT NULL,
    description           TEXT NOT NULL,
    content               TEXT NOT NULL,
    outcome_class         TEXT NOT NULL
                          CHECK (outcome_class IN ('success', 'failure', 'neutral')),
    condition_fingerprint JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_type           TEXT NOT NULL,
    source_id             TEXT,
    embedding             vector(768),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- HNSW cosine index for embedding similarity (mirrors add_pgvector_rag.sql).
-- Partial — only index rows that actually have an embedding.
CREATE INDEX IF NOT EXISTS idx_reasoning_lessons_embedding_hnsw
    ON reasoning_lessons USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64)
    WHERE embedding IS NOT NULL;

-- Hot path: list recent lessons by outcome class.
CREATE INDEX IF NOT EXISTS idx_reasoning_lessons_outcome_created
    ON reasoning_lessons (outcome_class, created_at DESC);

-- Trace back from a postmortem / prediction to its derived lesson(s).
CREATE INDEX IF NOT EXISTS idx_reasoning_lessons_source
    ON reasoning_lessons (source_type, source_id);

-- Fast containment lookups against fingerprint keys (regime, fci_bucket,
-- vol_bucket, horizon_bucket, ticker, direction).
CREATE INDEX IF NOT EXISTS idx_reasoning_lessons_fingerprint
    ON reasoning_lessons USING gin (condition_fingerprint jsonb_path_ops);

GRANT ALL ON TABLE reasoning_lessons TO grid;
GRANT USAGE, SELECT ON SEQUENCE reasoning_lessons_id_seq TO grid;
