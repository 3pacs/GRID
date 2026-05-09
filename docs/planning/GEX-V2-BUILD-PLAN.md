# GEX V2 Build Plan — Sequenced Execution Roadmap

**Task**: #78 (DOCUMENT ONLY — follow-ups #79/#80/#81 execute)
**Author**: Claude Code (Opus 4.6)
**Date**: 2026-04-11
**Status**: Draft for execution
**Spec source**: [`docs/planning/GEX-V2-SPEC.md`](./GEX-V2-SPEC.md) (1,337 lines, V2 canonical)
**Reference impl**: `/Users/anikdang/grid_obsidian/Gex Grok MD.md` (276 lines, CCXT/Deribit/BS)
**Existing baseline**: `physics/dealer_gamma.py` (494 LOC, equity, vectorized 23x)

---

## 1. Intro — The Unlock

GRID currently has a production-grade **equity** dealer-gamma engine (`DealerGammaEngine` in `physics/dealer_gamma.py`) that computes [[Dealer Gamma|GEX]], gamma flip, call/put walls, [[Dealer Gamma|vanna]] and charm — vectorized, DB-backed, wired into `options_recommender.py`. What it does **not** have is: (a) coverage of **crypto options** (Deribit/OKX/Bybit), where dealer flow is the dominant intraday signal because expiries compress into hours and retail is concentrated; (b) the three higher-order Greeks the V2 spec requires — **vomma (VOEX), color (COLEX), zomma (ZEX), speed (SPEEDEX)**; (c) a **normalized contract schema** that abstracts across venues with explicit `contract_size`, `settlement_currency`, and provenance tracking; and (d) a **venue-adapter pattern** that lets us bolt on new exchanges without touching the exposure engine. GEX V2 closes all four gaps so that the `chain_contagion.py` → `oracle/engine.py` → `contagion_to_ticket.py` → `options_recommender.py` pipeline can emit **crypto dealer-flow-conditioned trade tickets** with identical semantics to the equity path. The endgame: Deribit 0DTE BTC chain hits the pipeline at T+30s, a gamma-flip breach shows up as a dealer-flow shock in `chain_contagion`, oracle tags it with `negative_gamma_breakout_candidate`, and `options_recommender` sizes a Kelly position against it — all in the same plumbing that already ships for SPY/QQQ/TSMC.

---

## 2. Current-State Inventory

Concept-by-concept mapping of V2 spec → existing GRID modules.

| V2 Concept | Spec § | Exists in GRID? | Where | Gap |
|---|---|---|---|---|
| Normalized contract schema | §5 | Partial | `options_snapshots` table | No `contract_size`, no `settlement_currency`, no venue field, no quote_age_ms |
| Venue adapters (base class) | §8 | No | — | Must create `physics/dealer_flow/adapters/base.py` |
| DeribitAdapter | §8.2 | No | — | Must create `physics/dealer_flow/adapters/deribit.py` (CCXT-based) |
| OKXAdapter, BybitAdapter | §8.2 | No | — | Out-of-scope for Wave 1-5 (future GEX-8) |
| Black-Scholes d1/d2 | §7 | Yes (scalar) | `physics/dealer_gamma.py:49-57` (`_d1`, `_d2`) | Needs vectorized numpy version in shared module |
| BS gamma | §7 | Yes | `physics/dealer_gamma.py:60` (`bs_gamma`) | Port to `physics/greeks/black_scholes.py` |
| BS delta (call/put) | §7 | Yes | `physics/dealer_gamma.py:68-75` | Same |
| BS vanna | §7 | Yes | `physics/dealer_gamma.py:78` | Same |
| BS charm | §7 | Yes | `physics/dealer_gamma.py:87` | Same |
| BS vomma | §7 | **NO** | — | Must add (from Grok MD line 83) |
| BS speed | §7 | **NO** | — | Must add (from Grok MD line 90) |
| BS color | §7 | **NO** | — | Must add (from Grok MD line 98) |
| BS zomma | §7 | **NO** | — | Must add (from Grok MD line 108) |
| Validation pipeline | §9 | Partial | Ad-hoc filters in `_load_chain` | Must create `physics/dealer_flow/pipeline.py` |
| Greek completion layer | §10 | Partial | `DealerGammaEngine` defaults IV=0.25 silently | Must add provenance tracking (`greek_source_*`) |
| Exposure aggregation | §11 | Partial (gamma only) | `compute_gex_profile` | Must add CEX/VEX/VOEX/COLEX aggregators |
| Confidence scoring | §12 | No | — | Must create `physics/dealer_flow/confidence.py` |
| Alpha layer (regime tags) | §13 | Partial | `regime: LONG_GAMMA/SHORT_GAMMA/NEUTRAL` | Extend to spec labels (`pinning_regime_candidate`, etc.) |
| Snapshot storage | §14 | Partial | `options_snapshots` raw DB table | No partitioned `option_exposures` table |
| Ingestion — equity | §3 | Yes | `ingestion/altdata/polygon_puller.py` (302 LOC, 20 mentions of greeks) | Keep as-is |
| Ingestion — crypto | §3.1 | **NO** | — | Deribit via CCXT in new adapter |
| `options_recommender.py` | integration | Yes (1,556 LOC) | `trading/options_recommender.py` | Must accept V2 dealer-flow payload as input, not bypass |
| `contagion_to_ticket.py` | integration | Yes | `trading/contagion_to_ticket.py` | Must call the new `physics/dealer_flow.pipeline.run()` |
| `oracle/engine.py` | integration | Yes | 5-model ensemble | Add `crypto_dealer_flow` signal type |
| `dealer_gamma.py` existing | refactor | Yes | 494 LOC | Refactor to import shared BS primitives, don't rewrite |

**Key finding**: ~60% of the spec's math already exists for equity. What's missing is (a) the four new Greeks (vomma/speed/color/zomma), (b) the venue-adapter abstraction, and (c) the normalized schema + confidence scoring framework. This is a **port + extend** project, not a rewrite.

---

## 3. Pre-Create Check Findings (embedded verbatim)

Run against `scripts/pre_create_check.py` on 2026-04-11.

### 3.1 `dealer_flow`
```
Coverage EXISTS. Extend ollama/dealer_flow_briefing.py instead of creating a new module.
Files (4):
  ollama/dealer_flow_briefing.py (595 LOC, 5 mentions) — narrative synthesis
  intelligence/scheduler.py (247 LOC, 5 mentions)
  api/routers/derivatives.py (994 LOC, 4 mentions) — Dealer Flow Intelligence endpoints
  trading/options_recommender.py (1,556 LOC, 4 mentions)
```
**Plan decision**: The existing `ollama/dealer_flow_briefing.py` is a **narrative/LLM** synthesis module — it consumes dealer-flow data and produces text. It does NOT compute exposures. We need a separate `physics/dealer_flow/` **subpackage** (not `ollama/`, not the same file) for the **math engine**. The pre-create check matched on keyword but the existing module is the downstream consumer, not a duplicate. **Proceed with `physics/dealer_flow/` as a new subpackage.** The briefing module will later import from it.

### 3.2 `black_scholes`
```
No existing coverage. Safe to create.
```
**Plan decision**: Create `physics/greeks/black_scholes.py` as a new module. Existing scalar helpers in `physics/dealer_gamma.py` (`_d1`, `_d2`, `bs_gamma`, etc.) will be **imported from the new module** after Wave 1 — no duplication.

### 3.3 `greeks`
```
Coverage EXISTS. Extend ingestion/altdata/polygon_puller.py instead of creating a new module.
Files (5):
  ingestion/altdata/polygon_puller.py (302 LOC, 20 mentions) — pulls greeks from Polygon API
  analysis/vol_surface.py (1,272 LOC, 10 mentions) — IV surface engine
  physics/dealer_gamma.py (494 LOC, 2 mentions)
  api/routers/search.py (255 LOC, 1 mention)
  ingestion/scheduler.py (1,518 LOC, 1 mention)
```
**Plan decision**: The polygon_puller **ingests** greeks from an external API — it does not compute them. `vol_surface.py` computes IV surfaces, not the Greeks themselves. Neither is a duplicate of the V2 BS completion layer. The new `physics/greeks/` subpackage is distinct: it's the mathematical completion layer that fills missing Greeks when the exchange doesn't provide them. **Proceed with `physics/greeks/` as a new subpackage.** Refactor `physics/dealer_gamma.py` to import from it in Wave 4.

### 3.4 `deribit`
```
No existing coverage. Safe to create.
```
**Plan decision**: Greenfield. Create `physics/dealer_flow/adapters/deribit.py` via CCXT (per Grok MD reference impl). No existing Deribit client anywhere in GRID.

### 3.5 `options_normalized`
```
No existing coverage. Safe to create.
```
**Plan decision**: Greenfield. New tables `option_contracts_normalized`, `option_snapshots_raw`, `option_exposures` added via migration `0037_options_v2_schema.sql`. These are **additive**; the existing `options_snapshots` table stays untouched to preserve the equity pipeline.

---

## 4. Wave-by-Wave Execution Plan

Five waves, topologically ordered. Each wave is a separate follow-up task. Waves 1-3 are already queued ([[Dealer Gamma|GEX]]-3/4/5); Waves 4-5 queue as new tasks.

### Wave 1 — Port the 7 BS Greeks to a shared module
**Task**: [[Dealer Gamma|GEX]]-3 (#79, already queued)
**Title**: Create `physics/greeks/black_scholes.py` with vectorized 7-Greek primitives
**Scope**:
- New file: `physics/greeks/__init__.py`
- New file: `physics/greeks/black_scholes.py` (~250 LOC)
- New file: `grid/tests/test_black_scholes.py` (~150 LOC)
**Dependencies**: None (pure math, numpy + scipy.stats already imported elsewhere).
**Deliverables** (specific function signatures):
```python
# physics/greeks/black_scholes.py
import numpy as np
from scipy.stats import norm

def d1(S, K, T, r, sigma, q=0.0) -> np.ndarray: ...
def d2(S, K, T, r, sigma, q=0.0) -> np.ndarray: ...
def bs_gamma(S, K, T, r, sigma, q=0.0) -> np.ndarray: ...
def bs_delta(S, K, T, r, sigma, q=0.0, option_type='call') -> np.ndarray: ...
def bs_vanna(S, K, T, r, sigma, q=0.0) -> np.ndarray: ...
def bs_charm(S, K, T, r, sigma, q=0.0, option_type='call') -> np.ndarray: ...
def bs_vomma(S, K, T, r, sigma, q=0.0) -> np.ndarray: ...          # NEW
def bs_speed(S, K, T, r, sigma, q=0.0) -> np.ndarray: ...          # NEW
def bs_color(S, K, T, r, sigma, q=0.0) -> np.ndarray: ...          # NEW
def bs_zomma(S, K, T, r, sigma, q=0.0) -> np.ndarray: ...          # NEW

# All functions accept scalars or numpy arrays (broadcast-safe).
# All handle T<=0 or sigma<=0 by returning 0 (no NaN propagation).
```
**Test criteria**:
- Parity against scalar `physics/dealer_gamma.bs_gamma/bs_delta/bs_vanna/bs_charm` within 1e-9 for 1000 random strikes.
- Vomma/speed/color/zomma reproduce published textbook values for a standard ATM call (S=100, K=100, T=0.25, r=0.05, sigma=0.2) within 1e-6.
- Vectorized form (S as ndarray) matches scalar form pointwise.
- `T=0` and `sigma=0` paths return exactly 0.0, not NaN.
- Unit convention comment block at top of file matches §6.2 of spec.
**LOC estimate**: 250 prod + 150 test = **400 LOC**
**Risk**: **LOW**. Pure math with closed-form references. The Grok MD file has working Python for all 7 — it's a direct port with vectorization added.
**Follow-up mapping**: This IS [[Dealer Gamma|GEX]]-3 (#79). No new task needed.

---

### Wave 2 — Scaffold the `physics/dealer_flow/` subpackage
**Task**: [[Dealer Gamma|GEX]]-4 (#80, already queued)
**Title**: Build `physics/dealer_flow/` with schemas, base adapter, Deribit adapter, pipeline, confidence
**Scope**:
- New dir: `physics/dealer_flow/`
- New file: `physics/dealer_flow/__init__.py` (public exports)
- New file: `physics/dealer_flow/schemas.py` (~200 LOC) — Pydantic models for `NormalizedContract`, `ExposureSnapshot`, `DealerFlowSignal`
- New file: `physics/dealer_flow/adapters/__init__.py`
- New file: `physics/dealer_flow/adapters/base.py` (~120 LOC) — `BaseOptionsVenueAdapter` protocol
- New file: `physics/dealer_flow/adapters/deribit.py` (~300 LOC) — CCXT-based, port from Grok MD `fetch_crypto_option_chain`
- New file: `physics/dealer_flow/pipeline.py` (~250 LOC) — orchestrator: adapter → validate → complete → aggregate → score
- New file: `physics/dealer_flow/exposures.py` (~350 LOC) — vectorized aggregation for all 7 Greeks → net_gex/cex/vex/voex/colex/zex/speedex + walls + flip
- New file: `physics/dealer_flow/confidence.py` (~150 LOC) — `confidence_score()` per §12
- New file: `physics/dealer_flow/validation.py` (~200 LOC) — §9 hard/soft rules
- New file: `grid/tests/test_dealer_flow.py` (~400 LOC) — frozen-snapshot tests
**Dependencies**: **Wave 1 must land first** (imports from `physics/greeks/black_scholes`). Also needs `ccxt` in `requirements.txt` (verify presence; if missing, add to wave).
**Deliverables** (key signatures):
```python
# physics/dealer_flow/adapters/base.py
from typing import Protocol
import pandas as pd

class BaseOptionsVenueAdapter(Protocol):
    venue: str
    def fetch_instruments(self, underlying: str) -> list[dict]: ...
    def fetch_chain_snapshot(self, underlying: str, max_dte_days: int) -> list[dict]: ...
    def normalize_snapshot(self, raw_snapshot) -> pd.DataFrame: ...
    def validate_snapshot(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]: ...

# physics/dealer_flow/adapters/deribit.py
class DeribitAdapter:
    venue = "deribit"
    def __init__(self, exchange: ccxt.deribit | None = None): ...
    # implements BaseOptionsVenueAdapter

# physics/dealer_flow/pipeline.py
def run_dealer_flow_pipeline(
    underlying: str,
    venues: list[str] = ["deribit"],
    max_dte_days: int = 7,
    db_engine: Engine | None = None,  # for persistence to option_exposures
) -> DealerFlowSignal: ...

# physics/dealer_flow/exposures.py
def compute_all_exposures(df: pd.DataFrame, spot: float, r: float = 0.05, q: float = 0.0) -> dict: ...
def find_gamma_flip(df: pd.DataFrame, spot: float) -> float | None: ...
def detect_walls(df: pd.DataFrame, top_n: int = 3) -> dict: ...

# physics/dealer_flow/confidence.py
def confidence_score(snapshot: pd.DataFrame, metrics: dict) -> float: ...  # returns 0.0-1.0
```
**Test criteria**:
- Pipeline runs end-to-end on a **frozen Deribit JSON fixture** (checked into repo at `grid/tests/fixtures/deribit_btc_2026-04-11.json`) without hitting network.
- Frozen snapshot produces deterministic `net_gex`, `call_wall`, `put_wall`, `gamma_flip` values (recorded as golden values in test).
- Validation rejects rows with `strike<=0`, `contract_size<=0`, expired, `iv>5.0` (§9.1 hard rules).
- Confidence score returns value in `[0, 1]` for both happy path and degraded (all-stale) snapshot.
- Mock CCXT client for adapter tests; no network calls.
**LOC estimate**: 1,470 prod + 400 test = **~1,870 LOC**
**Risk**: **MED**. CCXT Deribit public endpoints are stable but rate-limited; normalized schema requires careful unit handling (Deribit delivers Greeks per-BTC not per-contract — must multiply by `contract_size`); Pydantic v2 vs v1 mismatch risk with existing `schemas.py` files in GRID.
**Follow-up mapping**: This IS [[Dealer Gamma|GEX]]-4 (#80). No new task needed. **Split into two PRs if >1,500 LOC** (Wave 2a = schemas + base adapter + Deribit adapter; Wave 2b = pipeline + exposures + confidence).

---

### Wave 3 — Schema migration for V2 tables
**Task**: [[Dealer Gamma|GEX]]-5 (#81, already queued)
**Title**: Migration `migrations/0037_options_v2_schema.sql`
**Scope**:
- New file: `migrations/versions/0037_options_v2_schema.sql`
- Three tables: `option_contracts_normalized`, `option_snapshots_raw`, `option_exposures`
- GRANT footer for `grid` role
**Dependencies**: **None from code** (migration is pure SQL), but code from Wave 2 must match the column names. Recommend landing Wave 2 as a dry-run (no DB writes) first, then applying Wave 3, then wiring persistence.
**Deliverables**: See §6 below for full column list.
**Test criteria**:
- Migration applies cleanly on a throwaway Postgres 15 container with `sqlalchemy.create_engine` reflection.
- Reverse migration (`DROP TABLE ... CASCADE`) included in a sibling `.down.sql` (or inline comment).
- [[TimescaleDB]] hypertable on `option_snapshots_raw` partitioned by `source_ts_utc` (1-day chunks).
- `option_exposures` primary key: `(venue, underlying, snapshot_id)`.
- Indexes on `(underlying, source_ts_utc DESC)` for all 3 tables.
- **No `DROP` of existing `options_snapshots`** — V2 tables are additive, equity path keeps working.
**LOC estimate**: 150 LOC SQL
**Risk**: **MED-HIGH**. Production already has `options_snapshots` (equity) with live data flowing through `DealerGammaEngine`. A bad migration could break the existing equity [[Dealer Gamma|GEX]] pipeline. Mitigations: (a) V2 tables are **additive**, never touching existing ones; (b) dry-run on server with `psql --set ON_ERROR_STOP=1` before commit; (c) GRANT footer must include `grid_read` for the API service account.
**Follow-up mapping**: This IS [[Dealer Gamma|GEX]]-5 (#81). No new task needed.

---

### Wave 4 — Refactor `physics/dealer_gamma.py` to share primitives
**Task**: **NEW** — [[Dealer Gamma|GEX]]-6 (queue after Wave 3 lands)
**Title**: Refactor `DealerGammaEngine` to import shared BS + exposures from `physics/greeks/` and `physics/dealer_flow/`
**Scope**:
- Modify `physics/dealer_gamma.py` (~494 → ~350 LOC after dedup)
- No behavior change; pure refactor
- Update `grid/tests/test_dealer_gamma.py` (if exists) to ensure parity
**Dependencies**: Waves 1 AND 2 must be merged. This is the **integration** wave that retires duplicated math.
**Deliverables**:
```python
# Before (physics/dealer_gamma.py):
def bs_gamma(S, K, T, r, sigma) -> float: ...  # scalar, local

# After:
from physics.greeks.black_scholes import bs_gamma, bs_delta, bs_vanna, bs_charm
# scalar wrappers deleted, all call sites updated
```
- Keep `DealerGammaEngine` class as-is (equity DB-backed, still the SPY/QQQ path).
- Delete local `_d1`/`_d2`/`bs_*` helpers after verifying all imports resolve.
- `_compute_per_strike` and `_gex_at_spots_vectorized` now delegate to `physics.dealer_flow.exposures.compute_all_exposures` with a `venue='equity_db'` flag.
- Retain `_load_chain` and `_get_spot` (DB I/O stays in this module).
**Test criteria**:
- All existing tests for `DealerGammaEngine` still pass (parity is the bar).
- `compute_gex_profile('SPY')` output byte-identical (within 1e-9) vs. pre-refactor baseline for a frozen snap_date.
- `grid/tests/test_dealer_gamma.py` regenerated from a pinned baseline before the refactor, asserted against after.
**LOC estimate**: -150 LOC (net reduction from dedup)
**Risk**: **MED**. The equity pipeline is live and consumed by `options_recommender.py`. Any numerical drift breaks trade tickets. Mitigation: snapshot the output of `compute_gex_profile('SPY', date(2026,4,10))` pre-refactor, diff post-refactor, block merge on any delta > 1e-6.
**Follow-up mapping**: **Queue as new task [[Dealer Gamma|GEX]]-6** after GEX-5 merges. Not in the existing queue.

---

### Wave 5 — Wire `physics/dealer_flow.pipeline` into the trade-ticket path
**Task**: **NEW** — [[Dealer Gamma|GEX]]-7 (queue after Wave 4 lands)
**Title**: Integrate V2 dealer-flow signal into `contagion_to_ticket.py` via `options_recommender.py`
**Scope**:
- Modify `trading/contagion_to_ticket.py` — add `crypto_dealer_flow` as a new input signal
- Modify `trading/options_recommender.py` — accept `DealerFlowSignal` payload for crypto underlyings
- Modify `oracle/engine.py` — register `crypto_dealer_flow` as a 6th model in the ensemble (or route to existing `dealer_gamma` slot with venue flag)
- Modify `intelligence/chain_contagion.py` — emit shock events on gamma-flip breaches from V2 pipeline
- New file: `pwa/src/views/canvas_lenses/CapitalLens.jsx` **patch only** — add a "Dealer Flow Crypto" sub-lens that reads from the new `/api/derivatives/v2/dealer_flow` endpoint
- New file: `api/routers/derivatives.py` **patch** — add `GET /v2/dealer_flow/{underlying}` endpoint
**Dependencies**: Waves 1-4 merged. Migration 0037 applied in production. The Wave 2 pipeline must be running on a scheduler (add to `ingestion/scheduler.py` as a new puller).
**Deliverables**:
```python
# trading/contagion_to_ticket.py — new branch
def build_ticket_from_contagion(event: ContagionEvent) -> TradeTicket:
    if event.asset_class == "crypto" and event.dealer_flow_signal:
        # use V2 path
        return _build_from_v2_dealer_flow(event)
    return _build_from_equity_gex(event)  # existing path

# oracle/engine.py — add signal type
SIGNAL_TYPES = [..., "crypto_dealer_flow"]  # evaluation window: 1h
TRUST_WEIGHTS = {..., "crypto_dealer_flow": 0.8}  # starting prior

# api/routers/derivatives.py — new route
@router.get("/v2/dealer_flow/{underlying}")
def get_v2_dealer_flow(underlying: str, venue: str = "deribit") -> DealerFlowSignal: ...

# pwa/src/views/canvas_lenses/CapitalLens.jsx — new lens tab
// Subscribe to /api/derivatives/v2/dealer_flow/BTC via useAsyncData
// Render walls + flip on the Canvas node overlay
```
**Test criteria**:
- End-to-end integration test: feed a mock Deribit chain → `chain_contagion` emits shock → `oracle/engine` scores → `contagion_to_ticket` generates ticket with `underlying=BTC`, `strike`, `entry`, `stop`, `kelly_fraction`.
- Ticket fields are all non-None (no silent defaults).
- `pwa` Canvas renders dealer-flow walls without console errors (manual smoke).
- `/api/derivatives/v2/dealer_flow/BTC` returns valid `DealerFlowSignal` JSON under 500ms p95.
**LOC estimate**: 400 LOC across 6 files
**Risk**: **HIGH**. This is the integration wave that exposes V2 to live money. Mitigations:
- Gate by `confidence_score >= 0.7` hardcoded in Wave 5 v1; relax in later iteration.
- Wave 5 must ship with a **kill switch** feature flag (`ENABLE_V2_CRYPTO_TICKETS=false` default) so we can toggle off without redeploy.
- All V2 tickets flagged in `decision_journal` with `source='gex_v2'` for [[Postmortem|post-mortem]].
- Do NOT wire to `scripts/live_trader.py` in this wave — Wave 5 produces tickets, does not execute them.
**Follow-up mapping**: **Queue as new task [[Dealer Gamma|GEX]]-7** after GEX-6 merges. Not in the existing queue.

---

## 5. Integration Points

### 5.1 Downstream: `trading/contagion_to_ticket.py`
V2 pipeline emits `DealerFlowSignal(underlying, venue, net_gex, flip, walls, regime_tags, confidence)`. `contagion_to_ticket` branches on `asset_class`:
- `crypto` → V2 path (new code in Wave 5)
- `equity` → existing `DealerGammaEngine.compute_gex_profile` (unchanged)
The V2 signal is NOT bypassing `options_recommender`; it feeds it via the same ticket-building interface.

### 5.2 Sibling: `oracle/engine.py` (via SYNTHESIS_WIRING_PLAN, task #91)
V2 registers `crypto_dealer_flow` as a new signal type in `oracle/engine.py`. Evaluation window **1 hour** (shorter than equity's 1-day window because crypto expiries are daily/weekly and intraday rehedging is dominant). Starting trust prior: 0.8 (high — dealer flow is mechanical, not discretionary). Trust decays per `intelligence/trust_scorer.py` (90-day half-life).

### 5.3 Upstream: `intelligence/chain_contagion.py`
V2 pipeline is an **upstream producer** of shock events for `chain_contagion`. When `net_gex` flips sign between consecutive snapshots, or when spot breaches the `gamma_flip`, `chain_contagion` ingests a `DealerFlowShock` event (new event type in Wave 5). Downstream, `chain_contagion` can [[Cross Reference|cross-reference]] this with other signals (funding rate spikes, whale flows) to raise conviction.

### 5.4 Frontend: `pwa/src/views/canvas_lenses/CapitalLens.jsx`
V2 surfaces on the Canvas as a new **"Dealer Flow — Crypto"** sub-lens inside `CapitalLens`. Visualization:
- Node overlay rendering call/put walls as horizontal lines on the price axis
- Gamma flip as a dashed line
- Regime tag badge (`negative_gamma_breakout_candidate`, etc.) next to the underlying
- Confidence score as the node border opacity
Read path: `useAsyncData('/api/derivatives/v2/dealer_flow/BTC')`. Refresh every 60s via existing event bus. No new state management — reuse existing [[Zustand]] store.

---

## 6. Schema Migration Plan — `migrations/0037_options_v2_schema.sql`

Next migration number confirmed by listing `migrations/versions/`: latest is `0036_user_intel.sql`.
**Full SQL body below. DO NOT [[DEPLOY]] — Wave 3/[[Dealer Gamma|GEX]]-5 executes this.**

```sql
-- migrations/versions/0037_options_v2_schema.sql
-- GEX V2 — Options Dealer Flow normalized schema
-- Additive: existing options_snapshots table is untouched.
-- Dependencies: TimescaleDB extension (already installed).

BEGIN;

-- ============================================================================
-- 1. option_contracts_normalized — canonical cross-venue contract identity
-- ============================================================================
CREATE TABLE IF NOT EXISTS option_contracts_normalized (
    id                   BIGSERIAL PRIMARY KEY,
    venue                TEXT NOT NULL,                 -- 'deribit', 'okx', 'bybit', 'polygon_equity'
    symbol               TEXT NOT NULL,                 -- venue-native option symbol
    underlying           TEXT NOT NULL,                 -- 'BTC', 'ETH', 'SPY', 'QQQ'
    expiry_ts_utc        BIGINT NOT NULL,               -- UTC milliseconds
    strike               DOUBLE PRECISION NOT NULL CHECK (strike > 0),
    option_type          TEXT NOT NULL CHECK (option_type IN ('call', 'put')),
    contract_size        DOUBLE PRECISION NOT NULL CHECK (contract_size > 0),
    settlement_currency  TEXT NOT NULL,                 -- 'BTC', 'ETH', 'USDC', 'USD'
    quote_currency       TEXT NOT NULL,
    first_seen_utc       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_utc        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_expired           BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (venue, symbol)
);
CREATE INDEX IF NOT EXISTS idx_ocn_underlying_expiry
    ON option_contracts_normalized (underlying, expiry_ts_utc DESC);
CREATE INDEX IF NOT EXISTS idx_ocn_venue_symbol
    ON option_contracts_normalized (venue, symbol);

-- ============================================================================
-- 2. option_snapshots_raw — per-tick normalized market state (hypertable)
-- ============================================================================
CREATE TABLE IF NOT EXISTS option_snapshots_raw (
    id                   BIGSERIAL,
    contract_id          BIGINT NOT NULL REFERENCES option_contracts_normalized(id),
    venue                TEXT NOT NULL,
    underlying           TEXT NOT NULL,
    strike               DOUBLE PRECISION NOT NULL,
    option_type          TEXT NOT NULL,
    mark_price           DOUBLE PRECISION,
    bid                  DOUBLE PRECISION,
    ask                  DOUBLE PRECISION,
    mid                  DOUBLE PRECISION,
    oi_contracts         DOUBLE PRECISION,
    oi_underlying_units  DOUBLE PRECISION,
    volume_24h           DOUBLE PRECISION,
    underlying_price     DOUBLE PRECISION NOT NULL,
    iv_decimal           DOUBLE PRECISION,
    delta                DOUBLE PRECISION,
    gamma                DOUBLE PRECISION,
    vanna                DOUBLE PRECISION,
    charm                DOUBLE PRECISION,
    vomma                DOUBLE PRECISION,
    color                DOUBLE PRECISION,
    zomma                DOUBLE PRECISION,
    speed                DOUBLE PRECISION,
    greek_source_gamma   TEXT CHECK (greek_source_gamma IN ('exchange','recomputed','mixed')),
    greek_source_delta   TEXT,
    source_ts_utc        BIGINT NOT NULL,
    ingest_ts_utc        BIGINT NOT NULL,
    quote_age_ms         INTEGER,
    spread_bps           DOUBLE PRECISION,
    data_quality_flags   JSONB DEFAULT '{}'::jsonb,
    row_confidence       DOUBLE PRECISION,
    PRIMARY KEY (id, source_ts_utc)
);
-- TimescaleDB hypertable, 1-day chunks
SELECT create_hypertable(
    'option_snapshots_raw',
    'source_ts_utc',
    chunk_time_interval => 86400000,  -- 1 day in ms
    if_not_exists => TRUE
);
CREATE INDEX IF NOT EXISTS idx_osr_underlying_ts
    ON option_snapshots_raw (underlying, source_ts_utc DESC);
CREATE INDEX IF NOT EXISTS idx_osr_contract_ts
    ON option_snapshots_raw (contract_id, source_ts_utc DESC);

-- ============================================================================
-- 3. option_exposures — aggregated per-snapshot dealer-flow output
-- ============================================================================
CREATE TABLE IF NOT EXISTS option_exposures (
    snapshot_id          TEXT NOT NULL,                 -- e.g. 'btc_2026-04-11T19:30:00Z'
    venue                TEXT NOT NULL,                 -- or 'multi' for aggregation
    underlying           TEXT NOT NULL,
    spot                 DOUBLE PRECISION NOT NULL,
    max_dte_days         INTEGER NOT NULL,
    net_gex              DOUBLE PRECISION,
    net_cex              DOUBLE PRECISION,
    net_vex              DOUBLE PRECISION,
    net_voex             DOUBLE PRECISION,
    net_colex            DOUBLE PRECISION,
    net_zex              DOUBLE PRECISION,
    net_speedex          DOUBLE PRECISION,
    gamma_flip           DOUBLE PRECISION,
    call_wall            DOUBLE PRECISION,
    put_wall             DOUBLE PRECISION,
    call_charm_wall      DOUBLE PRECISION,
    put_charm_wall       DOUBLE PRECISION,
    confidence_score     DOUBLE PRECISION CHECK (confidence_score BETWEEN 0 AND 1),
    regime_tags          TEXT[],                        -- ['positive_gamma','charm_supportive']
    row_count            INTEGER,
    rejected_rows        INTEGER,
    recomputed_gamma_pct DOUBLE PRECISION,
    stale_quote_pct      DOUBLE PRECISION,
    venue_agreement_score DOUBLE PRECISION,
    computed_at_utc      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    code_version         TEXT,                          -- git sha of the computing code
    PRIMARY KEY (venue, underlying, snapshot_id)
);
CREATE INDEX IF NOT EXISTS idx_oe_underlying_time
    ON option_exposures (underlying, computed_at_utc DESC);
CREATE INDEX IF NOT EXISTS idx_oe_regime_tags
    ON option_exposures USING GIN (regime_tags);

-- ============================================================================
-- GRANTs (footer — REQUIRED for GRID deployment pattern)
-- ============================================================================
GRANT SELECT, INSERT, UPDATE ON option_contracts_normalized TO grid;
GRANT USAGE, SELECT ON SEQUENCE option_contracts_normalized_id_seq TO grid;
GRANT SELECT, INSERT ON option_snapshots_raw TO grid;
GRANT USAGE, SELECT ON SEQUENCE option_snapshots_raw_id_seq TO grid;
GRANT SELECT, INSERT, UPDATE ON option_exposures TO grid;
GRANT SELECT ON option_contracts_normalized TO grid_read;
GRANT SELECT ON option_snapshots_raw TO grid_read;
GRANT SELECT ON option_exposures TO grid_read;

COMMIT;
```

---

## 7. Risk Register — Top 5

| # | Risk | Severity | Waves | Mitigation |
|---|---|---|---|---|
| 1 | **CCXT rate limits on Deribit public endpoints**. Deribit limits unauthenticated requests to ~20/s; a full BTC chain is ~1,500 instruments requiring sequential `fetch_ticker` calls — that's 75+ seconds for one snapshot, unusable for live flow. | HIGH | 2 | Use Deribit's `public/get_book_summary_by_currency` which returns the full chain in one call. Rate-limit the puller to 1 snapshot/60s per underlying. Add `enableRateLimit: True` in CCXT init. Cache instrument metadata for 1 hour. |
| 2 | **Deribit auth required for full Greeks**. The public `get_book_summary_by_currency` returns mark_iv but NOT greeks. We must recompute Greeks via BS (§10). This is fine for gamma/delta but introduces IV-surface error for charm and vanna. | MED | 2 | Cross-check Greek recomputation against Deribit's public `public/get_order_book` (which does return some Greeks under `greeks.*` path). Pin a tolerance: recomputed vs exchange-provided delta must agree within 5%, else flag `greek_source='mixed'` and lower confidence. Full authenticated path is Phase 2 (future GEX-8). |
| 3 | **BS numerical precision for short-dated OTM options**. When T → 0 and the option is far OTM, `d1` and `d2` diverge, `norm.pdf(d1)` underflows to 0, and gamma/vanna/vomma all return 0 — invisible rows. Crypto 0DTE (the primary use case) hits this constantly. | MED | 1,4 | Wave 1 unit tests include a 0DTE ATM grid (T=1/365, S/K ratio in [0.7, 1.3]) and assert gamma > 0 for all in-bounds rows. Add a `clip_T_minimum=1e-5` parameter on all BS functions. Fall back to finite-difference gamma for T<1/252 if closed-form returns 0. |
| 4 | **Schema migration on production `options_snapshots`**. The existing `options_snapshots` table has live equity data flowing through `DealerGammaEngine`. If migration 0037 even *touches* it (rename, alter constraint), the equity pipeline breaks and SPY/QQQ trade tickets stop. | HIGH | 3 | Migration is **additive only** — creates `option_contracts_normalized`, `option_snapshots_raw`, `option_exposures`. Does NOT modify `options_snapshots`. Dry-run on server with `BEGIN; <migration>; ROLLBACK;` before committing. Coordinate deploy with a SPY trade-ticket smoke test immediately after. |
| 5 | **Refactor of `physics/dealer_gamma.py` breaks equity pipeline (TSMC/ASML/SPY)**. Wave 4 refactors an actively-consumed module. Even a 1e-6 numerical drift changes trade tickets consumed by `options_recommender` Kelly sizing, causing silent live-money impact. | HIGH | 4 | Pre-refactor: snapshot `compute_gex_profile('SPY', date(2026,4,10))` to a golden-value JSON file. Post-refactor: assert byte-identical output (within 1e-9) in a regression test. Block merge on any drift. Stage-gate the rollout: deploy with a `USE_V2_PRIMITIVES=false` feature flag, flip to true only after 24h of parallel computation. |

---

## 8. Order-of-Operations Checklist

Topologically sorted — do NOT reorder. Each step shows the exact command to run.

**Pre-flight (one-time, already done by task #78)**:
- [x] `python3 scripts/pre_create_check.py dealer_flow --verbose`
- [x] `python3 scripts/pre_create_check.py black_scholes --verbose`
- [x] `python3 scripts/pre_create_check.py greeks --verbose`
- [x] `python3 scripts/pre_create_check.py deribit --verbose`
- [x] `python3 scripts/pre_create_check.py options_normalized --verbose`

**Wave 1 — [[Dealer Gamma|GEX]]-3 (#79)**:
- [ ] Dispatch agent with task #79 body referencing this plan §4.Wave1
- [ ] Agent creates `physics/greeks/black_scholes.py` + test file
- [ ] `cd grid && python -m pytest tests/test_black_scholes.py -v` — all pass
- [ ] `python3 scripts/deploy.py --snapshot physics/greeks/black_scholes.py grid/tests/test_black_scholes.py`
- [ ] Verify `physics/dealer_gamma.py` still works (no regressions yet — imports unchanged)

**Wave 2 — [[Dealer Gamma|GEX]]-4 (#80)**:
- [ ] Dispatch agent with task #80 body referencing this plan §4.Wave2
- [ ] Add `ccxt>=4.0` to `requirements.txt` if not present
- [ ] Agent creates `physics/dealer_flow/` subpackage (9 files)
- [ ] Agent creates `grid/tests/fixtures/deribit_btc_2026-04-11.json` frozen fixture
- [ ] `cd grid && python -m pytest tests/test_dealer_flow.py -v` — all pass
- [ ] Smoke-test against live Deribit public endpoint (1 call only, rate-limited):
      `python3 -c "from physics.dealer_flow.adapters.deribit import DeribitAdapter; print(DeribitAdapter().fetch_chain_snapshot('BTC', 1))"`
- [ ] `python3 scripts/deploy.py --snapshot physics/dealer_flow/`

**Wave 3 — [[Dealer Gamma|GEX]]-5 (#81)**:
- [ ] Dispatch agent with task #81 body referencing this plan §4.Wave3 and §6
- [ ] Agent creates `migrations/versions/0037_options_v2_schema.sql`
- [ ] Dry-run on throwaway container:
      `docker run --rm -e POSTGRES_PASSWORD=test -p 5433:5432 -d timescale/timescaledb:latest-pg15`
      `psql -h localhost -p 5433 -U postgres -f migrations/versions/0037_options_v2_schema.sql`
- [ ] Commit migration; deploy applies via alembic in the standard deploy script
- [ ] Post-deploy verify on grid-svr: `\dt option_*` shows 3 tables, `\d option_snapshots_raw` shows hypertable
- [ ] Smoke: `SELECT COUNT(*) FROM options_snapshots;` — existing equity count unchanged

**Wave 4 — [[Dealer Gamma|GEX]]-6 (NEW, queue now)**:
- [ ] Queue task [[Dealer Gamma|GEX]]-6 in task tracker with body pointing at this plan §4.Wave4
- [ ] Pre-refactor: `python3 -c "from physics.dealer_gamma import DealerGammaEngine; import json; ...; json.dump(result, open('/tmp/spy_golden.json','w'))"`
- [ ] Agent refactors `physics/dealer_gamma.py` to import from `physics/greeks/`
- [ ] Post-refactor parity test: compare vs `/tmp/spy_golden.json`, abs diff < 1e-9
- [ ] `cd grid && python -m pytest tests/test_dealer_gamma.py tests/test_black_scholes.py tests/test_dealer_flow.py -v`
- [ ] `python3 scripts/deploy.py --snapshot physics/dealer_gamma.py`
- [ ] Post-deploy: call `/api/derivatives/gex/SPY` and confirm identical output

**Wave 5 — [[Dealer Gamma|GEX]]-7 (NEW, queue after Wave 4)**:
- [ ] Queue task [[Dealer Gamma|GEX]]-7 in task tracker with body pointing at this plan §4.Wave5
- [ ] Agent adds `crypto_dealer_flow` signal type to `oracle/engine.py`
- [ ] Agent adds `/v2/dealer_flow/{underlying}` endpoint to `api/routers/derivatives.py`
- [ ] Agent patches `trading/contagion_to_ticket.py` branch for `asset_class=='crypto'`
- [ ] Agent patches `pwa/src/views/canvas_lenses/CapitalLens.jsx` sub-lens
- [ ] Feature flag `ENABLE_V2_CRYPTO_TICKETS=false` in `config.py` (default OFF)
- [ ] End-to-end integration test with mock Deribit fixture
- [ ] `python3 scripts/deploy.py --snapshot ...`
- [ ] Post-deploy: flip feature flag to `true` **only after 24h parallel-run validation**

**Post-Wave-5 hardening (not in this plan — queue as [[Dealer Gamma|GEX]]-8)**:
- [ ] Add OKXAdapter
- [ ] Add BybitAdapter
- [ ] Cross-venue consensus scoring
- [ ] Wire to `scripts/live_trader.py` for real execution
- [ ] Backtest on `option_snapshots_raw` history once enough data accumulates

---

## 9. Task Queue Summary

| Wave | Task ID | Status | Blocks On | LOC | Risk |
|---|---|---|---|---|---|
| 1 | GEX-3 (#79) | Queued | — | 400 | LOW |
| 2 | GEX-4 (#80) | Queued | Wave 1 | 1,870 | MED |
| 3 | GEX-5 (#81) | Queued | — (parallel with 2) | 150 | MED-HIGH |
| 4 | **GEX-6** (new) | **To queue** | Waves 1+2+3 | -150 | MED |
| 5 | **GEX-7** (new) | **To queue** | Wave 4 | 400 | HIGH |
| 6 | **GEX-8** (new, future) | **Backlog** | Wave 5 | TBD | TBD |

Total net LOC across V2 build: **~2,670** (ignoring the Wave 4 dedup reduction).

---

## 10. Non-Goals (explicit)

- **No execution code.** V2 produces signals and trade tickets; it does NOT place orders. Wave 5 stops at ticket emission; execution is gated by the existing `trading/signal_executor.py` which stays behind `ENABLE_V2_CRYPTO_TICKETS=false` until [[Postmortem|post-mortem]] data exists.
- **No OKX / Bybit adapters in this plan.** Deribit-only through Wave 5; other venues are [[Dealer Gamma|GEX]]-8 backlog.
- **No ML / [[Walk-Forward Backtesting|walk-forward]] backtests.** Phase 3 of the spec (§19 "Research alpha layer") is out of scope for [[Dealer Gamma|GEX]] V2 build. V2 ships the feature factory; ML is a separate follow-up.
- **No retirement of `options_snapshots`**. The existing equity table and its pipeline stay live forever. V2 is purely additive.

---

**End of plan.** Execute in order. Every wave is a separate agent dispatch. Every wave ends with a `python3 scripts/deploy.py` call.
