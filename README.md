# GRID — Private Trading Intelligence Engine

GRID is a systematic trading intelligence platform that ingests macroeconomic and market data from multiple sources, resolves conflicts using point-in-time (PIT) correct methodology, engineers features, discovers market regimes via unsupervised clustering, validates hypotheses through rigorous walk-forward backtesting, and maintains an immutable decision journal for performance review. Every query enforces strict no-lookahead constraints to prevent data leakage.

## Prerequisites

- **Python 3.11+**
- **Docker** and Docker Compose
- **PostgreSQL 15** with TimescaleDB extension (provided via Docker)
- A **FRED API key** (free from https://fred.stlouisfed.org/docs/api/api_key.html)

## Setup

### 1. Clone / copy project

```bash
cd grid
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env and set your FRED_API_KEY
```

### 3. Start the database

```bash
docker-compose up -d
```

Wait for the health check to pass (~30 seconds).

### 4. Apply the schema

```bash
python db.py
```

This creates all tables, indexes, triggers, and seed data.

### 5. Run initial data pull

```bash
# Historical pull (1990–present, takes a while)
python -m ingestion.scheduler --historical

# Or start the ongoing scheduler
python -m ingestion.scheduler
```

### 6. Run the orthogonality audit

```bash
python -m discovery.orthogonality
```

Outputs are saved to `outputs/orthogonality/`.

## Running Tests

```bash
pytest tests/ -v
```

Tests require a running PostgreSQL instance (via Docker). The PIT store tests are the most critical — they verify no future data leaks into historical queries.

## Week 1 Goals

By end of week 1, the system should:

1. Have all FRED, yfinance, and BLS data pulled from 1990 to present
2. Produce a complete orthogonality audit with correlation heatmaps and PCA analysis
3. Identify the true dimensionality of the feature set
4. Run initial cluster discovery to find candidate regime count
5. Have all tests passing

## Hyperspace Integration

GRID uses [Hyperspace](https://agents.hyper.space) as a local compute
and inference layer. The Hyperspace node runs alongside GRID, providing:

- **Local LLM inference** via OpenAI-compatible API at localhost:8080
- **Semantic embeddings** for feature similarity analysis
- **LLM-assisted reasoning** for hypothesis interpretation
- **Passive compute earnings** while the system is idle

### Setup

```bash
# Install and start the node
./hyperspace_setup/install.sh
./hyperspace_setup/start_node.sh

# Check status
./hyperspace_setup/status.sh

# Monitor from Python
python -m hyperspace.monitor
```

### Privacy Boundary

GRID's signal logic is never sent to the Hyperspace network.
The integration uses only:

- Local inference (model runs on your machine)
- Semantic embeddings for feature descriptions (public concepts only)
- Generic economic reasoning (no feature values, no discovered clusters)

### Graceful Degradation

Every Hyperspace call in GRID returns None if the node is offline.
GRID operates fully without Hyperspace — it is an enhancement layer,
not a dependency.

## Architecture Overview

| Module | Purpose |
|---|---|
| **config** | Central configuration from environment variables |
| **db** | Database connection management and schema application |
| **ingestion** | Pulls raw data from FRED, yfinance, and BLS APIs |
| **normalization** | Maps raw series to canonical features and resolves multi-source conflicts |
| **store** | Point-in-time query engine enforcing no-lookahead constraints |
| **features** | Feature transformation engine (z-score, rolling slope, ratios, spreads) |
| **discovery** | Orthogonality audits, PCA analysis, and unsupervised regime clustering |
| **validation** | Walk-forward backtesting and promotion gate enforcement |
| **inference** | Live inference using production models and latest PIT data |
| **journal** | Immutable decision log with outcome tracking |
| **governance** | Model lifecycle state machine (CANDIDATE → SHADOW → STAGING → PRODUCTION) |
| **hyperspace** | Local LLM inference, semantic embeddings, and reasoning via Hyperspace P2P node |
