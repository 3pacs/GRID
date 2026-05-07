-- Migration: 0050_agent_reports
-- Purpose: Centralized tailnet agent report ingest table.
-- Applies via: sudo -u postgres psql griddb -f migrations/0050_agent_reports.sql

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS agent_reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    date DATE NOT NULL,
    agent TEXT NOT NULL,
    host TEXT NOT NULL,
    title TEXT NOT NULL,
    body_md TEXT NOT NULL,
    body_json JSONB,
    tags TEXT[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    report_uri TEXT,
    idempotency_key TEXT NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_agent_reports_date_agent
    ON agent_reports (date, agent);

CREATE INDEX IF NOT EXISTS idx_agent_reports_tags
    ON agent_reports USING GIN (tags);

GRANT ALL ON TABLE agent_reports TO grid;

