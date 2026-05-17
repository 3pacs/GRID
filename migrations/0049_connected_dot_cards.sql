-- Persist connected-dot synthesis outputs as first-class Surfacer candidates.

CREATE TABLE IF NOT EXISTS connected_dot_cards (
    id BIGSERIAL PRIMARY KEY,
    dot_key TEXT NOT NULL UNIQUE,
    dot_type TEXT NOT NULL,
    ticker TEXT,
    direction TEXT NOT NULL DEFAULT 'watch',
    horizon TEXT NOT NULL DEFAULT 'watch',
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    catalyst TEXT NOT NULL,
    evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
    invalidation TEXT NOT NULL,
    confidence DOUBLE PRECISION NOT NULL DEFAULT 0.35,
    next_check_at TIMESTAMPTZ,
    state TEXT NOT NULL DEFAULT 'new',
    previous_state TEXT,
    state_signature TEXT NOT NULL,
    state_changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    validation JSONB NOT NULL DEFAULT '{}'::jsonb,
    quality JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_connected_dot_cards_type
    ON connected_dot_cards(dot_type);

CREATE INDEX IF NOT EXISTS idx_connected_dot_cards_ticker
    ON connected_dot_cards(ticker);

CREATE INDEX IF NOT EXISTS idx_connected_dot_cards_updated
    ON connected_dot_cards(updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_connected_dot_cards_state
    ON connected_dot_cards(state, state_changed_at DESC);
