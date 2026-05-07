# Obsidian Vault Audit — 2026-04-13

**Vault:** `/Users/anikdang/grid_obsidian`
**Auditor:** Claude (read-only first pass)
**Purpose:** Inform the sync strategy before any vault <-> GRID automation is built.
**Constraints honored:** No vault files modified. No GRID code written. Single output = this file.

---

## Summary

- **163** total `.md` files across 8 top-level directories + 5 orphan root files
- Hottest folder: **`Modules/Intelligence/`** with **59** notes (36% of the vault), mostly thin stubs
- Second hottest: **`Modules/`** umbrella with ~96 notes total across 16 subfolders
- Real content weight is concentrated in **3 top-level [[Dealer Gamma|GEX]] files** (3,323 lines) and **`version 5 - Stack & Visualization Architecture.md`** (the Palantir/Gotham master thesis)
- Oldest meaningful content: 2026-04-04 (S19, Audit-2026-04-04, 120B dead-end, LLM decision)
- Newest: Session 22 (2026-04-07), `TOP ASAP FIXES.md` (undated but references Phase 5 dedupe)

### File count by directory

| Directory | Notes | Notes |
|---|---:|---|
| `Modules/Intelligence/` | 59 | Mostly thin re-listings of intelligence modules; biggest folder by count, lowest content density |
| `Modules/` (other subfolders) | 37 | Ingestion, API, Frontend, Oracle, Trading, etc. — one note per module subpackage |
| `Architecture/` | 15 | High-level system maps, security, cron, ML, orchestration |
| `Infrastructure/` | 5 | LLM stack, servers, services, sanity checks, TTS |
| `Sessions/` | 4 | S19 / S20 / S21 / S22, 2026-04-04 through 2026-04-07 |
| `Workstreams/` | 6 | Gemma fine-tuning, Oracle pipeline, prompt pruning, publishing firewall, security hardening, signal feed |
| `Data-Integrity/` | 4 | Audit 2026-04-04, Oracle scoring S20, Report-audit criticals, sentinel cleanup |
| `Data-Model/` | 3 | Database schema (117 tables), entity map, feature registry (1,281 features) |
| `Dead-Ends/` | 2 | 120B-CPU-Inference, TimesFM pair trading |
| `Decisions/` | 1 | LLM model selection (Nemotron-49B, superseded by Gemma 4 31B in S21) |
| Root orphans | 7 | **GEX Plan.md**, **GEX GPT notes.md**, **Gex Grok MD.md**, 00-Dashboard, 00-Session-Start, TOP ASAP FIXES, version 5 - Stack & Visualization Architecture, META-Agent-Brief-Template |

---

## GEX / dealer gamma work (user's top priority)

### Notes found: 31 files mention GEX/dealer-gamma keywords

Of those, **only 3 are actual GEX design documents** — the rest are one-line drive-by references inside module README stubs, session logs, or security docs.

The 3 real design documents are all at the vault root:

1. `/Users/anikdang/grid_obsidian/GEX Plan.md` — **1,710 lines**
2. `/Users/anikdang/grid_obsidian/GEX GPT notes.md` — **1,337 lines**
3. `/Users/anikdang/grid_obsidian/Gex Grok MD.md` — **276 lines**

Total active GEX design surface in the vault: **3,323 lines of Markdown**.

#### 1. `GEX Plan.md` (1,710 lines) — "V1 raw research dump"

- **Origin:** Looks like an LLM-generated deep-dive (ChatGPT or similar), pasted in raw with `\(...\)` LaTeX artifacts.
- **Concepts covered (in order):**
  1. Open-source stack shopping list (Polygon.io, fintools-ai/mcp-options-order-flow-server, KaranChavan21/GEX_Dashboard, NavnoorBawa/Options-Flow-Predictor, py_vollib, QuantLib, Lumibot)
  2. Full GEX formula + Black-Scholes gamma + Python sketch (`calculate_gex`)
  3. Gamma flip point: simple linear interpolation method AND full simulation method, with code
  4. Call walls / put walls (`compute_call_put_walls`)
  5. Vanna Exposure (VEX) — formula, dealer sign convention, `compute_vex_and_vanna_walls`
  6. Charm Exposure (CEX) — `black_scholes_charm`, `compute_cex_and_charm_walls`
  7. Vomma / Volga Exposure (VOEX) — `black_scholes_vomma`, `compute_voex_and_vomma_walls`
  8. Speed Exposure (SPEX = DGammaDSpot) — `black_scholes_speed`, `compute_spex_and_speed_walls`
  9. Color Exposure (COLEX = gamma decay) — `black_scholes_color`, `compute_colex_and_color_walls`
  10. Zomma Exposure (ZOEX = DGammaDVol) — `black_scholes_zomma`, `compute_zoex_and_zomma_walls`
  11. Standalone essays on charm walls dynamics, charm-vs-color comparison, charm in crypto options (Deribit/Bybit/OKX)
- **Concrete designs the user has captured:**
  - `full_dealer_exposure(chain_df, spot, r, q)` — one-shot orchestration function combining all 7 exposure engines plus `combined_dealer_flow` aggregate
  - Per-strike schema (`strike`, `option_type`, `oi`, `gamma`, `iv`, `time_to_expiry`)
  - Sign convention: dealers net short options → call contributions negated, put contributions kept positive
- **Completeness:** `Gamma`, `Vanna`, `Charm`, `Vomma`, `Speed`, `Color`, `Zomma` code snippets all present and drop-in-ready. Zomma section is **duplicated twice** in the file — the LLM output repeats itself. No tests. No storage layer. No validation. No [[architecture]].
- **Status:** Raw research, not production-ready, but the math is essentially complete.

#### 2. `GEX GPT notes.md` (1,337 lines) — "V2 production spec" (THIS is the new GEX build)

- **Origin:** A much more rigorous follow-on document titled "Open-Source Alpha Layer for Options Dealer Flow — V2 Spec — Production-Oriented Research and Implementation Blueprint". Version header says **April 2026** — this is the current target.
- **This is the document the user was referring to when he said "much of it is done."**
- **Not "much of the code is done"** — much of the **DESIGN** is done. The V2 spec is essentially complete at the architecture level.
- **Structure (24 sections):**
  1. Executive summary — confidence-scored feature factory, not a prediction claim
  2. Design principles — normalize first, infer second; make units explicit; fail loudly; dealer positioning is a latent model
  3. System scope — **crypto first (Deribit / OKX / Bybit)**, US equity later
  4. High-level architecture — 5-layer pipeline (Ingestion -> Validation -> Greek Completion -> Exposure Aggregation -> Alpha Layer)
  5. **Canonical normalized schema** — 30+ fields per contract (venue, symbol, underlying, expiry_ts_utc, strike, option_type, contract_size, settlement_currency, oi_contracts, iv_decimal, delta, gamma, vanna, charm, vomma, color, zomma, speed, source_ts_utc, ingest_ts_utc, data_quality_flags)
  6. Exposure definitions with **explicit unit conventions** (per-contract, per-1%-move, signed/absolute)
  7. Mathematical layer — Black-Scholes as fallback, upgrade path to SABR/SVI
  8. **Venue adapter spec** — `BaseOptionsVenueAdapter` abstract class + `DeribitAdapter`, `OKXAdapter`, `BybitAdapter`
  9. **Validation pipeline** — hard rejects, soft warnings, sanity bounds, data_quality_flags output
  10. **Greek completion layer** — exchange Greeks preferred, recompute missing/invalid, vectorized, provenance-tagged
  11. **Exposure aggregation engine** — net GEX/CEX/VEX/VOEX/COLEX, gamma flip, walls, concentration, profiles
  12. **Confidence score framework** — completeness, freshness, venue agreement, persistence, spread quality
  13. Alpha layer spec — feature categories (structural, drift, vol-sensitivity, stability)
  14. Historical storage — raw payloads / normalized snapshots / features / signals, partitioned by venue + date, Parquet
  15. **Testing spec** — unit tests for formulas, snapshot tests on frozen chains, property tests, regression tests on golden datasets
  16. Monitoring spec — ingest latency, reject counts, percent missing IV, percent recomputed Greeks, venue disagreement score
  17-18. [[Security]] and reference output payload (complete JSON schema example for a live snapshot)
  19. **Implementation roadmap — 4 phases** (Correct prototype -> Complete core engine -> Research alpha layer -> Production hardening)
  20. **Minimal repository structure** — full directory tree: `adapters/`, `schemas/`, `validation/`, `greeks/`, `exposures/`, `alpha/`, `storage/`, `configs/`
  21-24. What makes V2 better, first-build practical guidance, final assessment, suggested next deliverables
- **Completeness:** 95% as a spec. 0% as code. No repository has been scaffolded. No adapter has been written. No test has been authored.
- **Status:** Spec complete. Implementation has not started in this tree.

#### 3. `Gex Grok MD.md` (276 lines) — "V1.5 single-file reference build (Grok version)"

- **Origin:** Labeled "built from the complete conversation" by Grok. Contains a **single-file, working Python implementation** (`dealer_flow_engine.py`) using CCXT + Deribit + on-the-fly Black-Scholes.
- **Concrete deliverable:** A ~200-line Python script with:
  - Every Black-Scholes helper (gamma, delta, vanna, charm, vomma, speed, color, zomma)
  - `compute_missing_greeks` fallback path
  - `compute_gamma_flip`, `compute_call_put_walls`, `compute_cex_and_charm_walls`
  - `full_dealer_exposure` one-shot orchestrator
  - `fetch_crypto_option_chain` CCXT adapter for Deribit
  - `__main__` example run on BTC 0DTE-style chain
- **Positioning:** Bridges the gap between V1 math dump and V2 spec. Not the authoritative design — V2 is.
- **Status:** This is the closest thing to "reference code" that exists in the vault. **It does not exist anywhere in the GRID tree.**

### Summary of GEX design (synthesis)

**What the user has designed, end-to-end:**

- A crypto-first (Deribit/OKX/Bybit via CCXT) options dealer-flow engine
- A normalized per-contract schema with unit-explicit exposures
- Full higher-order Greeks suite: **Gamma, Vanna, Charm, Vomma, Speed, Color, Zomma** — 7 exposures, each with walls + flips + per-strike profiles
- A 5-layer pipeline (Ingest -> Validate -> Complete Greeks -> Aggregate -> Alpha)
- Confidence scoring on every snapshot, every metric, every signal
- Three-tier historical storage (raw payloads / normalized snapshots / aggregated features) in Parquet
- Venue-agreement cross-checks and persistence-weighted wall selection (not "single max strike")
- Phase 4 roadmap with production hardening (alerting, anomaly detection, config versioning)

**Big picture:** The vault contains a complete production-grade V2 specification for a crypto options dealer-flow engine that does **not yet exist in any form inside the GRID repo**. The closest GRID analog is `physics/dealer_gamma.py` (494 lines, equity-only, single-venue, no higher-order Greeks beyond vanna+charm, no normalized schema, no validation pipeline, no confidence scoring, no snapshot storage).

**What's done vs missing vs stub:**

| Artifact | Vault | GRID code |
|---|---|---|
| GEX + gamma flip + walls (equity) | Spec + reference code | **EXISTS** (`physics/dealer_gamma.py`, 494 LOC, vectorized in S22) |
| Vanna + Charm (equity) | Spec + reference code | **EXISTS** (`physics/dealer_gamma.py` has `bs_vanna`, `bs_charm`) |
| Vomma / Volga | Spec + reference code | **MISSING** |
| Speed (DGammaDSpot) | Spec + reference code | **MISSING** |
| Color (gamma decay) | Spec + reference code | **MISSING** |
| Zomma (DGammaDVol) | Spec + reference code | **MISSING** |
| Normalized contract schema | Spec (30+ fields) | **MISSING** — GRID uses ad-hoc columns in `options_snapshots` (14 cols, **0 rows**) |
| Venue adapters (Deribit/OKX/Bybit) | Spec + reference code (Deribit via CCXT) | **MISSING** — zero files match `*deribit*`, zero mentions of CCXT |
| Validation pipeline (hard/soft/bounds) | Spec | **MISSING** as a dedicated layer |
| Greek completion with provenance tags | Spec | **MISSING** — no `greek_source_*` columns |
| Confidence score framework | Spec | **MISSING** — no per-snapshot confidence |
| Snapshot storage (raw / normalized / features) | Spec (Parquet, partitioned) | **PARTIAL** — `options_snapshots` table exists but empty; no Parquet layer |
| Persistence-weighted wall selection | Spec | **MISSING** — GRID uses single-max-strike |
| Higher-order walls (vomma/speed/color/zomma) | Spec + code | **MISSING** |
| Test harness on frozen snapshots | Spec | **MISSING** |
| Monitoring/alerting (venue disagreement, stale %) | Spec | **MISSING** |
| Crypto support (at all) | Primary scope | **MISSING** — GRID options stack is equity/index-only |

**GRID modules that would need to change to implement V2:**

- **NEW:** `physics/dealer_flow/` subpackage (or new `derivatives_engine/` top-level) containing `adapters/`, `schemas/`, `validation/`, `greeks/` (extend existing BS helpers), `exposures/` (7 files, one per Greek), `alpha/`, `storage/`
- **EXTEND:** `physics/dealer_gamma.py` — add vomma, speed, color, zomma; migrate to vectorized completion pipeline; adopt normalized schema
- **NEW:** `ingestion/altdata/deribit.py`, `okx.py`, `bybit.py` (or a single `ingestion/crypto_options/` subpackage)
- **NEW DB tables:** `option_contracts_normalized`, `option_snapshots_raw`, `option_exposures`, `option_confidence_scores` — current `options_snapshots` (14 cols, 0 rows) should be migrated or deprecated
- **EXTEND:** `api/routers/derivatives.py` — add `/gex/crypto/{underlying}`, `/confidence/{underlying}`, venue-breakdown endpoints
- **NEW:** `trading/options_recommender.py` — extend or replace equity-only logic with crypto dealer-flow regime features
- **NEW planning doc:** replace/supersede `docs/planning/GSD-OPTIONS-EDGE.md` and `docs/planning/DERIVATIVESGRID-PLAN.md` — these predate V2 and don't mention Deribit, CCXT, vomma, zomma, speed, color, confidence scoring, venue adapters, or normalized schemas

---

## Architecture decisions

15 notes in `/Architecture/`. Biggest ones:

- `Overview.md` — 8-layer system map, module counts (ingestion 118, intelligence 89, scripts 82, api 69)
- `Project-Structure.md` — referenced as canonical directory tree; not read in this pass
- `Security.md` — referenced; not read in this pass
- `Data-Pipeline.md` — Pull -> [[Raw Series Table|raw_series]] -> Resolver -> [[Resolved Series Table|resolved_series]] -> PIT -> features -> models
- `API-Layer.md` — 40 [[FastAPI]] routers
- `Intelligence-Layer.md` — 89 intelligence modules
- `Analysis-Layer.md`, `Trading-Layer.md`, `ML-Inference.md`, `Orchestration-Layer.md`, `Cron-Schedule.md`, `Planning-Docs.md`, `Module-Sizes.md`, `Config-Map.md`

Most architecture notes are **documentation stubs** summarizing code that already exists. They are reference material, not proposals.

---

## Dead-ends

2 notes in `/Dead-Ends/`:

- `120B-CPU-Inference.md` — Nemotron-3-Super-120B on 503GB RAM = 0.18 tok/s. MoE weights crossing CPU-GPU boundary 78 times per token. Don't retry without 2x A6000 or IQ2 quantization. Clean negative result.
- `TimesFM-Pair-Trading.md` — 49.9% directional accuracy across 16K PYPL->XLK runs (random). Useless for cross-asset. Still marginally OK (~56%) for single-asset [[Walk-Forward Backtesting|walk-forward]]. **Most interesting dead end** because the raw number is so clean — 16K runs, dead-flat 49.9%, across 3 different context window sizes.

Both are short, clear, and well-referenced. This is a healthy pattern worth preserving.

---

## Data model notes

3 notes in `/Data-Model/`:

- `Database-Schema.md` — **117 tables**, ~9.6M raw rows, ~6M resolved rows, 1.6M actors, 4.9M actor connections. Key finding for this audit: **`options_snapshots` has 14 columns and 0 rows.** The table exists but has never been populated. `options_daily_signals` has 11,175 rows, `options_recommendations` has 156 rows.
- `Entity-Map.md` — 772 mappings (201 WORKING, 223 BROKEN_NO_REGISTRY, 107 BROKEN_NO_RAW, 230 DUPLICATE, 11 CONFLICT). Matches the S19 audit.
- `Feature-Registry.md` — 1,281 features, 85% HEALTHY.

---

## Data integrity notes

4 notes in `/Data-Integrity/`. All from 2026-04-04:

- `Audit-2026-04-04.md` — 1,281 feature audit. 149 stale, 28 ORPHAN_NO_PULLER. Most pullers write direct to `resolved_series` bypassing `pull_log`.
- `Report-Audit-Criticals.md` — **7 CRITICAL hallucinations** found in outputs: SPX shown as "3 billion" (volume vs close confusion), duplicate conflicting GSPC values (2518 vs 6582 from same source), LLM hallucinating PYPL at $200-250 (actual $44, 5x overstatement), CMCSA 2x overstated, regime taxonomy mismatch (GROWTH vs risk_on), zero oracle predictions scored, backtest Sharpe 0.16 vs SPY 0.44.
- `Oracle-Scoring-S20.md` — 12,656 pending predictions backfilled to 9,803 ready-to-score.
- `Sentinel-Cleanup-S20.md` — 815K corrupt rows deleted from `resolved_series` (ephemeris future/past sentinels).

These are **[[Postmortem|post-mortem]] artifacts**, not proposals. The corresponding code fixes mostly shipped in S20.

---

## Sessions

4 notes in `/Sessions/`:

| Session | Date | Vault content | Memory/handoff in `~/.claude/projects/.../memory/` |
|---|---|---|---|
| S19 | 2026-04-04 | OpenRouter crisis, Nemotron-49B landed, TTS deployed, sanity checks | `session_2026_04_05.md` and later overlap partially |
| S20 | 2026-04-04 | 21/21 HIGH security bugs, 9,803 predictions, Gemma 27B downloaded, 815K row cleanup | Partial overlap with `session_2026_04_05.md` |
| S21 | 2026-04-06 | Hallucination guard, Gemma 4 31B migration, 67-entry freshness guardian, PR #9-#13 merged | `session_2026_04_06.md` and `handoff_2026_04_06.md` have similar content |
| S22 | 2026-04-07 | 46 commits, thesis 10->14 models, actor network 55x faster, **GEX vectorized (37s -> 1.6s)**, flow page fixed | `session_2026_04_06b.md` and `handoff_2026_04_07.md` partially overlap |

**Overlap verdict:** Vault sessions are thinner but **sharper** — ~30-90 lines each, structured with clear sections (Summary / Key Changes / Open Items). The `.claude` memory files are ~10x longer and more detailed. For session logs the memory files win on completeness, but the vault wins on readability. Neither is clearly newer; they were written in the same sessions.

Vault sessions stop at S22 (2026-04-07). The `.claude` memory has handoffs through 2026-04-11. **The vault has gone silent for ~4 days.**

---

## Modules/* notes vs GRID code

### Documented & exists (note points at live code, helpful reference)

- `Modules/Trading/Options-Analytics.md` -> `physics/dealer_gamma.py`, `analysis/vol_surface.py`, `trading/options_recommender.py`, `trading/options_tracker.py`, `discovery/options_scanner.py`, `derivatives/` frontend — all present
- `Modules/Trading/Trading-Overview.md` -> accurate file-by-file inventory of 11 files in `trading/` with line counts
- `Modules/Oracle/Oracle-Engine.md` -> `oracle/engine.py` etc. — present
- `Modules/Intelligence/*.md` (59 notes) -> most correspond to files under `intelligence/` (143 modules per [[MODULE_INVENTORY]].md). Not spot-checked 1:1 but scale matches.
- `Modules/Ingestion/*.md` (7 notes) -> broad categories (Core/AltData/International/Physical/Celestial/Trade/ML) — match `ingestion/` subdir layout
- `Modules/API/*.md` (9 notes) -> broad router categories — match `api/routers/`

### Documented but CODE DOESN'T EXIST (proposals)

**This is where the signal is.** These are things the user designed in the vault but never built:

1. **The entire V2 GEX engine** (`GEX GPT notes.md`) — 0% of spec exists as code. **Biggest proposal in the vault.**
2. **The single-file Deribit/CCXT reference build** (`Gex Grok MD.md`) — 0 files in GRID match `*deribit*`. No CCXT usage anywhere.
3. **`version 5 - Stack & Visualization Architecture.md`** — Proposes Apache AGE (graph extension), Elasticsearch (document store), Redpanda (event stream), MinIO (blob store), Temporal.io (durable orchestration), deck.gl geo layer, React Flow canvas. Per the `.claude` memory, MinIO + Redpanda + Prefect were "shipped" in V5 Phase 4 (handoff 2026-04-08d), so this one is PARTIALLY built. Canvas view is also built. But Apache AGE, Elasticsearch, and Temporal are not confirmed live in the latest memory snapshots.
4. **Palantir-style Canvas View** — called out as "THE killer feature" in the version 5 doc. Per memory this shipped as `Gotham Canvas` in handoff 2026-04-09c (6 commits, 12 intelligence tables wired). **Vault is stale on this — still describes it as "Missing".**
5. **`Modules/Intelligence/Postmortem.md`, `Causation.md`, `Hypothesis Engine.md`** — these also have corresponding code files per `intelligence/postmortem.py` (1344 LOC), `intelligence/causation*.py`, `intelligence/hypothesis_engine.py` (2137 LOC). The notes exist but are 1-line stubs; all the content is in code.

### CODE EXISTS but no note (undocumented modules)

Based on `docs/MODULE_INVENTORY.md` (649 modules in 30 directories) vs the ~96 module notes in the vault, **roughly 550+ GRID modules have no vault note**. Most of these are internal implementation files (feature helpers, specific ingestion pullers, API sub-routers). The vault notes are organized at the **subpackage** level, not the module level — which is reasonable — but it means the vault has systematic blind spots on:

- Most of `features/`, `discovery/`, `inference/`, `governance/`, `validation/`, `normalization/` (each has 1-3 vault notes at most; code has 5-21 files each)
- The entire `intelligence/actors/` subpackage (after the [[Actor Network|actor_network.py]] refactor — vault still references the monolith)
- The `a2a/`, `contracts/`, `autoagent/`, `bitnet/`, `events/`, `hyperspace/` top-level dirs (no vault notes)
- `oracle/` has 14 files but only 1 vault note (`Oracle-Engine.md`)
- **Critically:** `physics/dealer_gamma.py` has no dedicated vault note. The only reference is inside `Modules/Trading/Options-Analytics.md`.

### CONTRADICTION — note says X, code does Y

Top drift items (vault says one thing, code does another):

1. **LLM stack** — `Decisions/LLM-Model-Selection.md` says "FINAL — Nemotron-Super-49B v1.5, 60/81 GPU, ~1 tok/s. Don't revisit unless hardware changes." The decision file is marked `status: FINAL`. Reality (per S21 and the `.claude` CLAUDE.md): **Gemma 4 31B is the sole local LLM as of 2026-04-06.** The Decisions file is stale by at least one major migration.
2. **`00-Dashboard.md` "System Status" table** — Lists "LLM (31B) ONLINE | [[Hermes Scheduler|Hermes]] 8B -> Gemma 4 31B". That's half right but says `Gemma 4` in one column and `Hermes` in another; also lists `Nemotron-Super-49B` as live in `Infrastructure/LLM-Stack` row. Three different LLMs claimed as active in the same dashboard.
3. **`Modules/Trading/Options-Analytics.md`** — Lists `physics/dealer_gamma.py` as "Dealer gamma exposure calculation (GEX)" without mentioning vanna, charm, gamma flip, walls, dealer_delta, profile curves, or the vectorized refactor from S22. The note is 60 lines covering ~6 files; the actual `dealer_gamma.py` alone is 494 lines. **Massive undersell.**
4. **Canvas / Gotham view** — version 5 doc says "Investigation canvas: Missing". Per `.claude` memory handoff 2026-04-09c, Gotham Canvas shipped with 6 commits, 12 intelligence tables wired, dedup, tiered depth, auto-seed, intel feed. Vault is >2 sessions behind.
5. **`Modules/Intelligence/Actor Network.md`** references the old monolith. Per GRID CLAUDE.md: "`intelligence/actor_network.py` (153 LOC facade) — thin re-export shim; the real actor network now lives in the `intelligence/actors/` subpackage". Note is out of date.

Full drift audit would take another ~2 hours. These 5 are the most load-bearing.

---

## Orphan root files (worth listing individually)

- **`GEX Plan.md`** (1710 lines) — V1 raw research dump, all 7 Greeks, duplicated Zomma section
- **`GEX GPT notes.md`** (1337 lines) — **V2 production spec. This is the canonical GEX build plan.**
- **`Gex Grok MD.md`** (276 lines) — Single-file reference build with working CCXT/Deribit code
- **`version 5 - Stack & Visualization Architecture.md`** (173 lines) — Palantir/Gotham thesis. The "Five Views That Matter" + Canvas as killer feature. Proposes Apache AGE, Elasticsearch, Redpanda, MinIO, Temporal, React Flow, deck.gl. Partially shipped as V5 Phase 4+ per memory.
- **`00-Dashboard.md`** — Command center index page with stale LLM info (see drift #2 above)
- **`00-Session-Start.md`** — Not read this pass
- **`TOP ASAP FIXES.md`** — 10-item post-Phase-5-dedupe todo list (dual-deploy tree, agent prompt templates, parallel agent file conflicts, MODULE_INVENTORY staleness gate, smoke test gate, LOC as metric, agent JSON contract, backup policy, pre-commit grep check, synthesis tasks INDEX-4). **This is fresh and actionable.**
- **`META-Agent-Brief-Template.md`** — Not read this pass

---

## Sync strategy recommendation

**Recommendation: Metadata-only with a vault -> GRID one-way promotion lane for high-value docs.**

Concretely:

1. **Vault stays as the planning surface** — the V2 GEX spec, the version 5 stack doc, and the TOP ASAP FIXES list are the right kind of content for an Obsidian vault. Wikilinks between notes are already heavily used. Don't force this into GRID's doc tree.

2. **GRID stays as the execution surface** — code + machine-verified inventories (`docs/MODULE_INVENTORY.md`, `docs/CODEBASE_INDEX.md`, migration files, schema.sql). Don't pollute this with prose design docs.

3. **Build a one-way promotion lane** for 3-5 specific artifact types only:
    - **Design specs** (like `GEX GPT notes.md`) get promoted to `docs/planning/<slug>.md` **when they're ready to execute**. The vault version stays as the living doc; the GRID copy is a frozen snapshot with a front-matter `source_vault_note:` reference. This is how `GEX GPT notes.md` should flow into `docs/planning/GEX-V2-SPEC.md` (superseding the stale `GSD-OPTIONS-EDGE.md` and `DERIVATIVESGRID-PLAN.md`).
    - **Architecture decision records** (`Decisions/*.md`) get promoted to `docs/decisions/<slug>.md` with date stamps. Makes them discoverable to agents that don't read the vault.
    - **Dead-end notes** promoted to `docs/dead-ends/` so agents stop rediscovering 49.9% TimesFM.
    - **Session logs** stay vault-only. The `.claude` memory is the authoritative session record; vault sessions are a thinner second copy and should not be the source of truth.
    - **Module notes** (`Modules/**/*.md`) stay vault-only. They systematically drift, they're at the wrong granularity, and `docs/MODULE_INVENTORY.md` already does this job better with code generation.

4. **Reverse direction** — GRID -> vault: only auto-sync `docs/MODULE_INVENTORY.md` and `docs/CODEBASE_INDEX.md` into a read-only `Modules/_Generated/` folder in the vault, replacing the hand-written module stubs over time. These are the only GRID docs the vault genuinely benefits from.

5. **Do NOT attempt bidirectional sync.** The failure modes ([[Conflict Resolution|conflict resolution]], prose reformatting, wikilink translation) are not worth the complexity for <200 files.

**Tradeoffs:**

- **Pro:** Each surface plays to its strength. Claude Code can read `docs/planning/*.md` without needing vault access. The user can still write freeform in Obsidian.
- **Pro:** One-way promotion makes the vault -> GRID transition explicit and reviewable (git diff on the promoted file).
- **Con:** Design docs will diverge between vault and GRID once promoted. Accept that — the vault version evolves, the GRID version is a [[PIT Store|point-in-time]] plan.
- **Con:** No automatic backfill of the existing 96 module notes. That's fine; most of them are wrong or stale anyway.

---

## Next actions (top priority)

1. **Promote `GEX GPT notes.md` to `docs/planning/GEX-V2-SPEC.md` as the canonical GEX build plan.** Mark `docs/planning/GSD-OPTIONS-EDGE.md` and `docs/planning/DERIVATIVESGRID-PLAN.md` as SUPERSEDED. This is the "sync the obsidian and superplan the new GEX build" work the user asked for. The V2 spec is 95% complete as design — turn it into a scaffold + phase 1 task list.

2. **Scaffold `physics/dealer_flow/` subpackage** matching the V2 spec repository structure (`adapters/`, `schemas/`, `validation/`, `greeks/`, `exposures/`, `alpha/`, `storage/`). Port the 7 Black-Scholes helpers from `Gex Grok MD.md` into `greeks/black_scholes.py` (extending, not replacing, the existing `physics/dealer_gamma.py` helpers). This unblocks vomma/speed/color/zomma which are missing from GRID today.

3. **Build the `DeribitAdapter` first, end-to-end.** The V2 spec explicitly recommends starting with Deribit-only, getting GEX + flip + call wall + put wall + CEX working, saving every snapshot, then building tests from frozen snapshots. No venue-agnostic abstraction until you have one working adapter. New module: `ingestion/altdata/crypto_options/deribit.py`.

4. **Migrate or deprecate `options_snapshots` (0 rows).** The 14-column equity table doesn't match the V2 normalized schema (30+ fields, venue-aware, contract-size aware). Create `option_contracts_normalized` + `option_snapshots_raw` + `option_exposures` tables per V2 spec section 14. Add migration file.

5. **Reconcile vault drift on LLM stack.** Update `Decisions/LLM-Model-Selection.md` to mark the Nemotron-49B decision as SUPERSEDED-BY Gemma 4 31B, or write a new Decision note. The current `status: FINAL` is actively misleading.

6. **Reconcile vault drift on Canvas.** Update `version 5 - Stack & Visualization Architecture.md` "Gap" column — Canvas is no longer Missing per memory handoff 2026-04-09c (Gotham Canvas shipped).

7. **Sync TOP ASAP FIXES into GRID task tracker.** The 10 items are still valid and not yet addressed per the memory session log. Items #1 (dual-deploy tree), #2 (agent prompt template enforcement), and #3 (parallel agent file conflicts) are especially urgent.

---

## Appendix: "Holy shit" findings

Three things the main agent should know immediately:

1. **The new GEX build is 95% designed and 0% coded.** `GEX GPT notes.md` is a 1,337-line production spec for a crypto-first dealer-flow engine with 7 higher-order Greeks, normalized schema, venue adapters, confidence scoring, and a 4-phase roadmap. None of it exists in GRID. `options_snapshots` (the only table the spec would write to) has 0 rows. `docs/planning/GSD-OPTIONS-EDGE.md` predates V2 and does not mention Deribit, CCXT, vomma, zomma, speed, color, confidence scoring, venue adapters, or normalized schemas. **This is the build the user was asking about.**

2. **There is a working single-file reference implementation in `Gex Grok MD.md`** (276 lines) that uses CCXT -> Deribit -> on-the-fly Black-Scholes to compute GEX + gamma flip + charm walls on BTC 0DTE chains. It won't ship as-is, but it's a usable skeleton — all 7 Black-Scholes helpers are implemented. Port these into `physics/greeks/black_scholes.py` on day 1 and you've closed the "missing 5 Greeks" gap in GRID immediately.

3. **`physics/dealer_gamma.py` (494 LOC, GRID) is load-bearing, well-built, and completely undocumented in the vault.** The module has `DealerGammaEngine` with vectorized GEX profile, gamma flip via bisection, walls, vanna, charm, dealer delta, regime classification (LONG_GAMMA / SHORT_GAMMA / NEUTRAL). S22 vectorized it (37s -> 1.6s per ticker, logged in the session note as a one-liner). The vault module note (`Modules/Trading/Options-Analytics.md`) describes it as one bullet point: "Dealer gamma exposure calculation (GEX)". **The existing GRID code is ~60% of the way to the V2 spec already** for equity. The V2 build doesn't need to start from scratch — it needs to be refactored into the new normalized-schema + crypto-first shape, with the missing 5 Greeks bolted on.
