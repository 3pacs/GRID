-- Contracts infrastructure: audit trail + dead-letter store
-- Spec: docs/superpowers/specs/2026-04-11-information-flow-optimization-design.md
-- Phase 1 of information flow optimization.

BEGIN;

CREATE TABLE IF NOT EXISTS contracts_audit (
    id               BIGSERIAL PRIMARY KEY,
    event_id         UUID NOT NULL,
    contract_type    TEXT NOT NULL,
    producer_module  TEXT NOT NULL,
    correlation_id   UUID NOT NULL,
    emitted_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dispatched_to    TEXT[] NOT NULL DEFAULT '{}',
    payload_hash     TEXT NOT NULL,
    schema_version   INT NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS contracts_audit_correlation
    ON contracts_audit (correlation_id);

CREATE INDEX IF NOT EXISTS contracts_audit_type_time
    ON contracts_audit (contract_type, emitted_at DESC);

CREATE INDEX IF NOT EXISTS contracts_audit_event_id
    ON contracts_audit (event_id);

CREATE TABLE IF NOT EXISTS contracts_dead_letter (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID NOT NULL,
    contract_type   TEXT NOT NULL,
    payload         JSONB NOT NULL,
    consumer        TEXT NOT NULL,
    error_type      TEXT NOT NULL,
    error_detail    TEXT NOT NULL,
    retry_count     INT NOT NULL DEFAULT 0,
    next_retry_at   TIMESTAMPTZ,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ,
    correlation_id  UUID
);

CREATE INDEX IF NOT EXISTS contracts_dead_letter_retry
    ON contracts_dead_letter (next_retry_at)
    WHERE resolved_at IS NULL;

CREATE INDEX IF NOT EXISTS contracts_dead_letter_type_unresolved
    ON contracts_dead_letter (contract_type, failed_at DESC)
    WHERE resolved_at IS NULL;

COMMIT;
