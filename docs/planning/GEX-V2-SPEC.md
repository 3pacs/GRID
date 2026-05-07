---
source: /Users/anikdang/grid_obsidian/GEX GPT notes.md
promoted_at: 2026-04-13
promoted_via: OBSIDIAN-2 (task #76)
---
## Options-dealer-flow-alpha-layer

# Open-Source Alpha Layer for Options Dealer Flow

**V2 Spec — Production-Oriented Research and Implementation Blueprint**

**Version**: V2 Spec (April 2026)  
**Purpose**: Research-to-production design for a dealer-flow engine using open and low-cost data paths first, with clean upgrade paths to paid institutional feeds later.  
**Status**: Specification document, not a claim of validated live alpha.  
**License Suggestion**: MIT for code, separate disclaimer for trading use.

---

## 1. Executive Summary

This project is an **options dealer-flow alpha layer** that converts raw option chain data into a normalized structural map of likely hedging pressure.

The engine is designed to estimate and aggregate:

- **Gamma Exposure ([[Dealer Gamma|GEX]])**
    
- **Gamma Flip**
    
- **Call / Put Walls**
    
- **Charm Exposure (CEX)**
    
- **Vanna Exposure (VEX)**
    
- **Vomma Exposure (VOEX)**
    
- **Color Exposure (COLEX)**
    
- **Zomma Exposure (ZEX)**
    
- **Speed Exposure (SPEEDEX)**
    

The goal is **not** to predict price directly from theory alone. The goal is to produce a high-quality, testable feature layer that downstream rule systems or ML models can use.

This V2 spec fixes the biggest weaknesses of the original draft by making the project:

- schema-driven
    
- venue-aware
    
- contract-size aware
    
- testable
    
- vectorized
    
- confidence-scored
    
- explicit about uncertainty
    

---

## 2. Design Principles

### 2.1 Core design principles

1. **Normalize first, infer second.** Never compute exposures from raw exchange responses directly.
    
2. **Make units explicit.** Every Greek and every exposure must carry a documented unit convention.
    
3. **Separate data, exposure, and alpha layers.** No mixed responsibilities.
    
4. **Fail loudly on bad data.** Silent assumptions kill trading systems.
    
5. **Treat dealer positioning as an inferred latent model.** Never present it as ground truth.
    
6. **Prefer robust repeatability over elegant theory.**
    
7. **Every output must be confidence-scored.**
    

### 2.2 Non-goals

This project is not, by itself:

- a complete execution engine
    
- a guaranteed predictive model
    
- a substitute for risk management
    
- a proof that any one Greek exposure always dominates price action
    

---

## 3. System Scope

### 3.1 In-scope

- Crypto options first: Deribit, OKX, Bybit
    
- Public and low-cost ingestion path first
    
- Vectorized Black-Scholes Greek estimation where missing
    
- Exposure aggregation by strike, expiry, and venue
    
- Signal packaging for downstream systems
    
- Confidence scoring and validation metrics
    
- Historical snapshot storage for research and backtests
    

### 3.2 Future scope

- US equity / index options
    
- surface-aware Greeks with better vol interpolation
    
- order-book / trade-flow integration
    
- regime-conditioned ML
    
- execution-aware alpha decay modeling
    

---

## 4. High-Level Architecture

[Exchange APIs / Files / Paid Feeds]

│

▼

[Venue Adapters]

│

▼

[Normalized Contract Schema]

│

▼

[Validation Pipeline]

│

▼

[Greek Completion / Recalculation Layer]

│

▼

[Exposure Aggregation Engine]

│

▼

[Structural Outputs: walls / flips / profiles]

│

▼

[Alpha Layer: rules / ML features / confidence]

│

▼

[Backtest / Dashboard / Live Strategy]

### 4.1 Layer separation

#### Layer A — Ingestion

Responsibilities:

- connect to venue APIs
    
- fetch instruments and quotes
    
- batch-fetch OI / mark / IV / greeks if available
    
- normalize symbols and metadata
    
- stamp source timestamps
    

#### Layer B — Validation and normalization

Responsibilities:

- enforce schema
    
- coerce units
    
- normalize IV format
    
- reject malformed contracts
    
- attach contract size and settlement metadata
    

#### Layer C — Exposure engine

Responsibilities:

- recompute missing Greeks
    
- aggregate exposures by strike / expiry / venue
    
- compute flips, walls, concentration, decay
    

#### Layer D — Alpha layer

Responsibilities:

- derive directional or mean-reversion features
    
- compute confidence score
    
- combine with other features
    
- output machine-readable signal payloads
    

#### Layer E — Research / ops

Responsibilities:

- snapshotting
    
- metrics
    
- monitoring
    
- backtesting
    
- anomaly detection
    

---

## 5. Canonical Normalized Schema

All venue adapters must map raw contracts into the following schema before any exposure computation.

### 5.1 Contract-level fields

|Field|Type|Required|Description|
|---|---|---|---|
|venue|string|yes|deribit, okx, bybit, etc.|
|symbol|string|yes|venue-native option symbol|
|underlying|string|yes|BTC, ETH, etc.|
|expiry_ts_utc|int64|yes|expiration timestamp in UTC ms|
|strike|float64|yes|strike price|
|option_type|string|yes|call or put|
|contract_size|float64|yes|underlying or payout contract size|
|settlement_currency|string|yes|e.g. BTC, ETH, USDC, USD|
|quote_currency|string|yes|quote denomination|
|mark_price|float64|no|normalized mark price|
|bid|float64|no|best bid|
|ask|float64|no|best ask|
|mid|float64|no|midpoint if available or derived|
|oi_contracts|float64|yes|open interest in contracts|
|oi_underlying_units|float64|no|OI mapped to underlying units|
|volume_24h|float64|no|recent volume|
|underlying_price|float64|yes|normalized spot / index price|
|iv_decimal|float64|no|IV as decimal, e.g. 0.65|
|delta|float64|no|normalized per-contract delta|
|gamma|float64|no|normalized per-contract gamma|
|vanna|float64|no|normalized per-contract vanna|
|charm|float64|no|normalized per-contract charm|
|vomma|float64|no|normalized per-contract vomma|
|color|float64|no|normalized per-contract color|
|zomma|float64|no|normalized per-contract zomma|
|speed|float64|no|normalized per-contract speed|
|source_ts_utc|int64|yes|exchange timestamp or fetch timestamp|
|ingest_ts_utc|int64|yes|local ingest timestamp|
|is_expired|bool|yes|derived validation field|
|data_quality_flags|object/list|yes|validation annotations|

### 5.2 Derived fields

The engine may derive and attach:

- time_to_expiry_years
    
- dte_days
    
- distance_from_spot_pct
    
- spread_bps
    
- quote_age_ms
    
- greek_source (`exchange`, `recomputed`, `mixed`)
    
- row_confidence
    

---

## 6. Exposure Definitions and Unit Conventions

The V2 engine must define all exposures in explicit units.

### 6.1 Required conventions

Every exposure output must document:

- per-contract Greek basis
    
- aggregation multiplier
    
- whether it is signed or absolute
    
- whether it is scaled for a 1% spot move, 1 vol-point move, or 1 day time decay
    
- final unit: USD-equivalent, underlying-equivalent, or dimensionless score
    

### 6.2 Recommended default conventions

#### Gamma exposure (GEX)

Recommended default:

- compute from **per-contract gamma**
    
- aggregate as a **delta-change estimate for a 1% underlying move**
    
- include contract size explicitly
    
- sign convention must be documented in one place only
    

Example conceptual form:

gex = gamma_per_contract × oi_contracts × contract_size × spot^2 × 0.01 × sign_convention

#### Charm exposure (CEX)

Recommended default:

- delta drift from passage of one calendar day
    
- annualized charm converted to daily change explicitly
    

Example conceptual form:

cex = charm_per_contract × oi_contracts × contract_size × day_fraction × sign_convention

#### Vanna exposure (VEX)

Recommended default:

- delta sensitivity to a 1 vol-point move
    
- define whether 1 vol-point means `0.01` in decimal IV terms
    

#### Vomma exposure (VOEX)

Recommended default:

- vega convexity exposure to vol shock
    
- document whether it is mostly diagnostic or tradable
    

#### Color exposure (COLEX)

Recommended default:

- gamma decay per day
    
- primarily a persistence / decay feature rather than standalone directional signal
    

#### Zomma / Speed exposures

Recommended default:

- secondary diagnostics unless backtests prove independent value
    

### 6.3 Critical rule

A bulletproof implementation must **never** mix exposure conventions across venues or features.

---

## 7. Mathematical Layer

### 7.1 Baseline model

Use Black-Scholes as the default fallback for missing Greeks, with parameters:

- `S`: normalized spot or forward proxy
    
- `K`: strike
    
- `T`: time to expiry in years
    
- `r`: risk-free rate assumption
    
- `q`: carry/dividend placeholder, default 0 for crypto unless otherwise modeled
    
- `sigma`: IV in decimal units
    

### 7.2 Model caveats

Black-Scholes is an approximation. In production research, Greek recomputation should be treated as:

- acceptable for bootstrap systems
    
- decent for short-dated liquid options
    
- imperfect in skewed / discontinuous / event-driven surfaces
    

### 7.3 Upgrade path

Future versions should support:

- surface-interpolated IV
    
- forward-based pricing
    
- SABR / SVI-inspired approximations where helpful
    
- exchange-supplied Greeks as primary with recalculated parity checks
    

---

## 8. Venue Adapter Specification

Each venue must have its own adapter class.

### 8.1 Required interface

class BaseOptionsVenueAdapter:

def fetch_instruments(self, underlying: str) -> list[dict]:

...

  

def fetch_chain_snapshot(self, underlying: str, max_dte_days: int):

...

  

def normalize_snapshot(self, raw_snapshot) -> "pd.DataFrame":

...

  

def validate_snapshot(self, df):

...

### 8.2 Required adapters

- `DeribitAdapter`
    
- `OKXAdapter`
    
- `BybitAdapter`
    

### 8.3 Adapter responsibilities

Each adapter must:

- fetch instrument metadata in bulk
    
- fetch quotes / greeks / OI in the fewest possible calls
    
- normalize field names
    
- normalize timestamps to UTC ms
    
- normalize IV into decimal form
    
- attach contract size correctly
    
- annotate missing fields instead of guessing silently
    

### 8.4 Adapter failure policy

If a venue fails:

- retry with exponential backoff
    
- emit structured error log
    
- degrade confidence score
    
- optionally continue with remaining venues
    
- never fabricate missing rows
    

---

## 9. Validation Pipeline

No row reaches the exposure engine without validation.

### 9.1 Hard validation rejects

Reject or quarantine rows if:

- `strike <= 0`
    
- `underlying_price <= 0`
    
- `contract_size <= 0`
    
- `oi_contracts < 0`
    
- `time_to_expiry_years <= 0`
    
- `option_type` not in `{call, put}`
    
- IV exists and is outside configured sanity bounds
    
- duplicate primary key rows exist for the same snapshot
    

### 9.2 Soft validation warnings

Warn but optionally keep row if:

- spread too wide
    
- IV missing but recomputable from mark later
    
- stale quote timestamp
    
- zero volume with nonzero OI
    
- missing exchange Greek values
    

### 9.3 Suggested sanity bounds

Configurable defaults:

- `0 < iv_decimal <= 5.0`
    
- `0 <= abs(delta) <= 1.05`
    
- `gamma >= 0` for vanilla options under standard assumptions
    
- `dte_days <= configured_max_dte`
    

### 9.4 Validation outputs

The pipeline must return:

- clean dataframe
    
- rejected rows dataframe
    
- validation summary metrics
    
- counts by error type
    

---

## 10. Greek Completion Layer

### 10.1 Philosophy

Use exchange Greeks when trustworthy, but verify them. Recompute when missing, stale, or clearly invalid.

### 10.2 Rules

For each row:

1. Prefer exchange-provided Greek if valid.
    
2. Recompute if missing or outside sanity bounds.
    
3. Mark each Greek with provenance.
    
4. Log percent of chain recomputed.
    

### 10.3 Vectorization requirement

All Greek completion must be vectorized using NumPy / pandas or equivalent array operations. No `iterrows()` in production paths.

### 10.4 Outputs

Add:

- `greek_source_gamma`
    
- `greek_source_delta`
    
- `greek_source_vanna`
    
- etc.
    
- `row_confidence`
    

---

## 11. Exposure Aggregation Engine

The aggregation engine computes structural maps from the normalized snapshot.

### 11.1 Required outputs

At minimum, for each snapshot:

- net GEX
    
- net CEX
    
- net VEX
    
- net VOEX
    
- net COLEX
    
- gamma flip estimate
    
- top call wall
    
- top put wall
    
- top charm wall(s)
    
- exposure concentration by strike
    
- exposure concentration by expiry
    
- per-venue exposure breakdown
    
- confidence score
    

### 11.2 Profiles

The engine must produce:

- strike profile
    
- expiry profile
    
- strike × expiry heatmap-ready matrix
    
- rolling time-series outputs for each metric
    

### 11.3 Gamma flip calculation

The gamma flip should be computed from a clearly documented signed cumulative gamma profile. The method should support:

- linear interpolation between sign-change strikes
    
- no-flip result when no sign change exists
    
- optional smoothing to reduce noisy single-strike flips
    

### 11.4 Wall detection

Wall detection should support more than “single max strike.”

Recommended wall candidates:

- absolute max exposure strike
    
- top N clustered strike zone
    
- persistence-weighted wall over recent snapshots
    
- expiry-specific wall
    

This prevents one noisy outlier row from becoming “the wall.”

---

## 12. Confidence Score Framework

Every snapshot and every signal must include confidence.

### 12.1 Confidence components

Suggested factors:

- chain completeness
    
- percent of OI covered
    
- percent of Greeks recomputed
    
- quote freshness
    
- cross-venue agreement
    
- spread quality
    
- concentration stability over last N snapshots
    
- missing-field rate
    
- expiry concentration quality
    

### 12.2 Example score layout

confidence_score = weighted_sum(

completeness_score,

freshness_score,

consistency_score,

spread_score,

venue_agreement_score,

persistence_score

)

### 12.3 Output requirement

Confidence must be attached to:

- full snapshot
    
- each major exposure metric
    
- each wall / flip signal
    

---

## 13. Alpha Layer Specification

The alpha layer sits on top of exposures and turns them into testable features.

### 13.1 Feature categories

#### Structural positioning features

- signed net GEX
    
- spot distance to gamma flip
    
- spot distance to major call wall
    
- spot distance to major put wall
    
- GEX concentration ratio
    
- expiry concentration ratio
    

#### Drift / decay features

- net CEX
    
- net COLEX
    
- wall migration rate
    
- flip migration rate
    

#### Vol-sensitivity features

- net VEX
    
- net VOEX
    
- VEX skew across strikes
    
- VEX concentration near ATM
    

#### Stability features

- signal persistence over last 3 / 5 / 10 snapshots
    
- cross-venue consensus
    
- data-quality confidence
    

### 13.2 Example rule outputs

The alpha layer may output labels like:

- `pinning_regime_candidate`
    
- `negative_gamma_breakout_candidate`
    
- `charm_drift_supportive`
    
- `vanna_acceleration_risk`
    
- `low_confidence_do_not_trade`
    

### 13.3 Critical rule

The alpha layer must not hardcode trading decisions as truth. It should emit:

- features
    
- interpretations
    
- confidence
    
- optional recommended regime label
    

---

## 14. Historical Storage and Snapshotting

A bulletproof system must preserve raw and normalized history.

### 14.1 Store at least three levels

1. raw venue payloads
    
2. normalized validated snapshots
    
3. aggregated exposure outputs
    

### 14.2 Why this matters

Without snapshots you cannot:

- reproduce backtests
    
- investigate anomalies
    
- compare formula revisions
    
- audit live-vs-research differences
    

### 14.3 Recommended storage layout

/data

/raw

/venue=deribit/date=YYYY-MM-DD/

/normalized

/underlying=BTC/date=YYYY-MM-DD/

/features

/underlying=BTC/date=YYYY-MM-DD/

/signals

/underlying=BTC/date=YYYY-MM-DD/

Preferred formats:

- Parquet for normalized data and features
    
- JSON for raw payload archival when needed
    

---

## 15. Testing Specification

This project is not production-worthy without a real test harness.

### 15.1 Unit tests

Must cover:

- Black-Scholes formulas
    
- sign conventions
    
- IV normalization
    
- expiry filtering
    
- contract-size scaling
    
- gamma flip interpolation
    
- wall detection logic
    

### 15.2 Snapshot tests

Use frozen chain snapshots and verify:

- net GEX
    
- top walls
    
- flip value
    
- confidence score stability
    
- recomputed vs supplied Greeks tolerance
    

### 15.3 Property tests

Useful checks:

- no expired row survives validation
    
- no negative contract size survives validation
    
- wall selection remains deterministic for same snapshot
    
- exposure outputs are reproducible
    

### 15.4 Regression tests

Every formula or schema change must rerun against a golden dataset.

### 15.5 Research validation

Backtests must include:

- normal sessions
    
- high-volatility sessions
    
- expiry sessions
    
- event-driven sessions
    
- venue-disagreement sessions
    

---

## 16. Monitoring and Observability

### 16.1 Required metrics

- ingest latency by venue
    
- row count by venue
    
- reject count by validation rule
    
- percent missing IV
    
- percent recomputed Greeks
    
- stale quote count
    
- net exposure drift between snapshots
    
- venue disagreement score
    

### 16.2 Required logs

Structured logs should include:

- snapshot id
    
- venue
    
- underlying
    
- request latency
    
- failure reason
    
- count of clean vs rejected rows
    
- final confidence score
    

### 16.3 Alerts

Alert when:

- row counts collapse unexpectedly
    
- one venue diverges sharply from others
    
- confidence score drops below threshold
    
- stale data persists
    
- wall / flip outputs jump beyond configured anomaly threshold
    

---

## 17. Security and Operational Safety

### 17.1 Operational safety

- never let failed data silently become valid features
    
- isolate API credentials if paid feeds are added later
    
- version all configs
    
- record code version with each snapshot
    

### 17.2 Trading safety

- all live usage should be gated by confidence and risk checks
    
- the output of this engine should be advisory unless explicitly validated within a broader system
    

---

## 18. Reference Output Payload

A live snapshot should emit a structured object like:

{

"snapshot_id": "btc_2026-04-12T19:30:00Z",

"underlying": "BTC",

"spot": 84250.0,

"venues": ["deribit", "okx"],

"max_dte_days": 7,

"net_gex": 1234567.89,

"net_cex": -3456.78,

"net_vex": 98765.43,

"net_voex": 4567.89,

"net_colex": -234.56,

"net_zex": 12.34,

"net_speedex": -9.87,

"gamma_flip": 83800.0,

"call_wall": 85000.0,

"put_wall": 82000.0,

"call_charm_wall": 84500.0,

"put_charm_wall": 82500.0,

"confidence_score": 0.84,

"regime_tags": ["positive_gamma", "charm_supportive"],

"data_quality": {

"row_count": 1942,

"rejected_rows": 21,

"recomputed_gamma_pct": 0.12,

"stale_quote_pct": 0.03,

"venue_agreement_score": 0.79

}

}

---

## 19. Implementation Roadmap

### Phase 1 — Correct prototype

Deliverables:

- strict normalized schema
    
- one adapter working end-to-end
    
- vectorized gamma/delta/charm
    
- clean GEX / CEX / wall / flip outputs
    
- snapshot persistence
    
- unit tests
    

### Phase 2 — Complete core engine

Deliverables:

- Deribit + OKX + Bybit adapters
    
- vanna / vomma / color / zomma / speed implementations
    
- confidence scoring
    
- monitoring metrics
    
- strike / expiry profiles
    

### Phase 3 — Research alpha layer

Deliverables:

- regime labels
    
- engineered features for ML
    
- rolling persistence metrics
    
- cross-venue consensus features
    
- [[Walk-Forward Backtesting|walk-forward]] backtests
    

### Phase 4 — Production hardening

Deliverables:

- alerting
    
- anomaly detection
    
- robust failover behavior
    
- config versioning
    
- packaged [[deployment]] targets
    

---

## 20. Minimal Recommended Repository Structure

options-dealer-flow/

├─ README.md

├─ pyproject.toml

├─ requirements.txt

├─ configs/

│ ├─ venues.yaml

│ ├─ validation.yaml

│ └─ exposures.yaml

├─ src/

│ ├─ adapters/

│ │ ├─ base.py

│ │ ├─ deribit.py

│ │ ├─ okx.py

│ │ └─ bybit.py

│ ├─ schemas/

│ │ ├─ contracts.py

│ │ └─ signals.py

│ ├─ validation/

│ │ └─ pipeline.py

│ ├─ greeks/

│ │ ├─ black_scholes.py

│ │ └─ completion.py

│ ├─ exposures/

│ │ ├─ gamma.py

│ │ ├─ charm.py

│ │ ├─ vanna.py

│ │ ├─ vomma.py

│ │ ├─ color.py

│ │ ├─ zomma.py

│ │ └─ speed.py

│ ├─ alpha/

│ │ ├─ features.py

│ │ ├─ scoring.py

│ │ └─ regimes.py

│ ├─ storage/

│ │ └─ snapshots.py

---

## 21. What Makes V2 Better Than the Original Draft

The original draft had good intuition but was still a prototype. V2 improves it by:

- replacing vague assumptions with a formal normalized schema
    
- making contract size mandatory instead of hardcoded
    
- separating ingestion from exposure math
    
- adding confidence scoring
    
- requiring vectorization and tests
    
- formalizing validation and failure handling
    
- reframing the engine as a research-grade feature layer rather than guaranteed alpha
    

---

## 22. Practical Guidance for the First Real Build

If you are building this now, the smartest order is:

1. Implement **Deribit only** first.
    
2. Build the normalized schema and validation pipeline.
    
3. Get **GEX, flip, call wall, put wall, CEX** working correctly.
    
4. Save every snapshot.
    
5. Build tests from frozen snapshots.
    
6. Then add VEX, VOEX, COLEX, ZEX, SPEEDEX.
    
7. Only after that, add ML or trading rules.
    

That order gives the highest chance of producing something real instead of a flashy but brittle dashboard.

---

## 23. Final Assessment

A bulletproof dealer-flow engine is not just a Greek calculator. It is:

- a data normalization system
    
- a validated exposure engine
    
- a confidence-scored feature factory
    
- a monitored research platform
    
- a reproducible backtestable pipeline
    

That is the standard this V2 spec is designed to reach.

---

## 24. Suggested Next Deliverables

After this spec, the next best artifacts are:

1. a **repository scaffold** matching the structure above
    
2. a **Pydantic schema layer** for normalized contracts and signals
    
3. a **Deribit adapter MVP**
    
4. a **vectorized gamma/charm exposure module**
    
5. a **test pack with frozen example snapshots**
    

Those five items would turn this from a document into a real build plan.