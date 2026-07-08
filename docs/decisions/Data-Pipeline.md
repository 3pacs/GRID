---
source: /Users/anikdang/grid_obsidian/Architecture/Data-Pipeline.md
promoted_at: 2026-04-13
promoted_via: OBSIDIAN-2 (task #76)
---
# Data Pipeline

How data flows through GRID: from external sources to actionable intelligence.

## Pipeline Stages

### 1. Ingestion (118 files)

External data is pulled by source-specific puller classes, each implementing a standard interface: `pull()` -> `save_to_db()`.

**Source Groups:**
- [[Ingestion-AltData]] — 56 files: AAII sentiment, CBOE, [[CFTC COT]], [[Dark Pool|dark pools]], [[GDELT]], [[Insider Filings|insider filings]], prediction markets, etc.
- [[Ingestion-International]] — 14 files: [[ECB]], BCB, BIS, IMF, OECD, Eurostat, etc.
- [[Ingestion-Physical]] — 7 files: [[USDA]], VIIRS nightlights, patents, OFR
- [[Ingestion-Celestial]] — 6 files: planetary aspects, lunar cycles, solar activity, Vedic astrology, Chinese calendar
- [[Ingestion-Trade]] — 5 files: Comtrade, CEPII, WIOD, Atlas ECI
- [[Ingestion-ML]] — 2 files: FinBERT scoring
- [[Ingestion-Core]] — 15 files: [[FRED]], yfinance, [[BLS]], Tiingo, [[CoinGecko]], [[EDGAR]], options, web scraper

**Scheduling:** Managed by `ingestion/scheduler.py` (unified domestic + international) and the [[Hermes-Operator]] daemon.

### 2. Entity Mapping

Raw series identifiers (e.g., `T10Y2Y`, `YF:^GSPC:close`) are mapped to canonical feature names via [[Entity-Map]].

- `normalization/entity_map.py` — `SEED_MAPPINGS` dict + V2 mappings
- `normalization/resolver.py` — [[Conflict Resolution|conflict resolution]] with per-family thresholds
- Writes to `raw_series` table first, then resolves to `resolved_series`

### 3. Resolution

The `Resolver` class selects the highest-priority source per feature per date, detecting conflicts when multiple sources disagree by more than threshold (0.5% default, per-family overrides up to 5% for alt data).

**Conflict thresholds:**
| Family | Threshold |
|--------|-----------|
| equity | 1% |
| crypto | 3% |
| commodity | 1.5% |
| vol | 2% |
| alternative | 5% |

### 4. Feature Store

Features live in `feature_registry` (1,281 entries) with metadata: family, model eligibility, source priority.

- `features/registry.py` — `FeatureRegistry` query interface
- `features/lab.py` — `FeatureLab` computes derived features (z-scores, momentum, etc.)
- `features/alpha101.py` — WorldQuant Alpha101 factor implementations
- `features/importance.py` — feature importance scoring

### 5. Point-in-Time (PIT) Store

**The most critical correctness component.** Guarantees no look-ahead bias.

- `store/pit.py` — `PITStore` enforces `release_date <= as_of_date`
- Supports `FIRST_RELEASE` and `LATEST_AS_OF` vintage policies
- Used by both backtesting and [[Live Inference|live inference]]

### 6. Analytical Snapshots

Every analytical output is persisted for historical comparison.

- `store/snapshots.py` — saves clustering, regime, options scans with full provenance
- `store/astrogrid.py` — [[AstroGrid]]-specific persistence

## Database Tables (Data Pipeline)

| Table | Role |
|-------|------|
| `feature_registry` | Canonical feature definitions |
| `raw_series` | Raw ingested observations with source attribution |
| `resolved_series` | Conflict-resolved canonical values |
| `analytical_snapshots` | Historical analytical outputs |

## Key Scripts

- `scripts/run_pipeline.py` — Full ingest pipeline
- `scripts/bulk_resolve.py` — Fast bulk resolver for initial population
- `scripts/fill_missing_features.py` — Gap filling
- `scripts/compute_derived_features.py` — Derived feature computation

## Known Issues

- Some altdata pullers lack proper docstrings (most have empty `"""` blocks)
- Price fallback runs every 6h to catch stale equity/crypto prices
- `ingestion/smart_scheduler.py` exists but the main scheduler is `scheduler.py`
