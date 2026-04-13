-- Migration: 0036_user_intel
-- Purpose: Cooperative intel contribution system.
--
-- Users act as "tentacles" — they can submit biographical facts, connections,
-- loyalties, stances, rumors, or tips about any actor directly in the app.
-- Other users upvote/downvote/flag. Admins verify. Verified intel boosts trust.
--
-- Two tables:
--   user_intel       — one row per submission with vote aggregates
--   user_intel_votes — per-user vote ledger (UNIQUE prevents double voting)

CREATE TABLE IF NOT EXISTS user_intel (
    id SERIAL PRIMARY KEY,
    actor_id TEXT NOT NULL,
    intel_type TEXT NOT NULL,  -- biography | connection | loyalty | stance | rumor | tip | fact
    note TEXT NOT NULL,
    source_url TEXT,
    confidence TEXT,  -- user-declared: high | medium | low
    submitted_by TEXT NOT NULL,  -- user id
    submitted_at TIMESTAMPTZ DEFAULT NOW(),
    verified_by TEXT,
    verified_at TIMESTAMPTZ,
    verification_status TEXT DEFAULT 'pending',  -- pending | verified | rejected
    upvotes INT DEFAULT 0,
    downvotes INT DEFAULT 0,
    flags INT DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_user_intel_actor ON user_intel(actor_id, submitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_user_intel_status ON user_intel(verification_status);

CREATE TABLE IF NOT EXISTS user_intel_votes (
    id SERIAL PRIMARY KEY,
    intel_id INT REFERENCES user_intel(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL,
    vote INT NOT NULL,  -- -1 or 1
    voted_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (intel_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_user_intel_votes_intel ON user_intel_votes(intel_id);

GRANT ALL ON user_intel, user_intel_votes TO grid;
GRANT USAGE, SELECT ON SEQUENCE user_intel_id_seq, user_intel_votes_id_seq TO grid;
