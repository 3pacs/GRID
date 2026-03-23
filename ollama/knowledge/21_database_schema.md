# Database Schema — Canonical Data Model

## Overview

GRID uses PostgreSQL 15 with TimescaleDB extension. The schema enforces PIT
correctness, model governance, feature registry, and decision journal immutability
through constraints, triggers, and indexes.

## Core Tables

### Data Pipeline

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `source_catalog` | External data sources | name, trust_score, latency_class, pit_available |
| `raw_series` | Raw ingested data | series_id, source_id, obs_date, pull_timestamp, value |
| `feature_registry` | Canonical features | name, family, transformation, normalization, model_eligible |
| `resolved_series` | PIT-resolved data | feature_id, obs_date, vintage_date, value, conflict_flag |

### Intelligence Pipeline

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `hypothesis_registry` | ML hypotheses | statement, layer, feature_ids, state, kill_reason |
| `model_registry` | Model versions | name, layer, version, state, feature_set, parameters |
| `validation_results` | Backtest results | hypothesis_id, era_results, verdict, cost_bps |
| `feature_importance_log` | Feature scores | model_version_id, feature_id, importance_score, method |

### Decision & Audit

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `decision_journal` | Immutable decision log | inferred_state, confidence, recommendation, verdict |
| `agent_runs` | Multi-agent deliberation | ticker, analyst_reports, debate, decision, reasoning |

## Key Constraints

### PIT Correctness
- Composite unique index on `(feature_id, obs_date, vintage_date)` in resolved_series
- Queries enforce `release_date <= as_of_date` and `obs_date <= as_of_date`

### Model Governance
- Unique partial index: one PRODUCTION model per layer
- State CHECK constraint: CANDIDATE|SHADOW|STAGING|PRODUCTION|FLAGGED|RETIRED

### Immutability
- `enforce_journal_immutability` trigger on decision_journal
- Blocks updates to core decision fields (only outcome fields updatable)

### Feature Families
- CHECK constraint on family: rates|credit|breadth|vol|fx|commodity|sentiment|macro|earnings|crypto

## Key Indexes

- `idx_resolved_pit_lookup` — (feature_id, obs_date, vintage_date) for PIT queries
- `idx_raw_series_source_obs` — (source_id, obs_date) for ingestion dedup
- `idx_hypothesis_state` — (state) for filtering hypotheses
- `idx_model_registry_production` — Unique partial index WHERE state='PRODUCTION'
- `idx_decision_journal_timestamp` — (decision_timestamp DESC)
- `idx_agent_runs_ticker_date` — (ticker, as_of_date)

## Migrations

Alembic is configured for schema migrations:
- `alembic.ini` — Configuration
- `migrations/env.py` — Migration environment
- `migrations/versions/` — Version scripts

## Key Files

- `schema.sql` — Complete schema definition
- `db.py` — Engine creation, schema application, health checks
- `docker-compose.yml` — PostgreSQL 15 + TimescaleDB container
