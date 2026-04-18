# AI Finance Tools — Integration Research (2026-04-18)

Research pass across 11 trending open-source AI-finance projects, mapped against
GRID's existing 702-module codebase. Goal: identify what's already covered,
what's a duplicate, and where a concrete bolt-on would be net-new capability.

Read alongside `docs/MODULE_INVENTORY.md` before acting on any recommendation —
the pre-build checklist in `CLAUDE.md` still applies.

## TL;DR

| Project | Status in GRID | Verdict |
|---|---|---|
| shiyu-coder/Kronos | Not integrated | **ADOPT** — wrap as second foundation model alongside TimesFM |
| virattt/ai-hedge-fund | Partial (5 archetypes, no named investors) | **EXTEND** — add named-investor personas |
| TauricResearch/TradingAgents | Already built (`agents/`) | **SKIP** — GRID's agents/ IS this pattern |
| ZhuLinsen/daily_stock_analysis | Covered by `oracle/report.py` + Hermes | **SKIP** |
| hsliuping/TradingAgents-CN | A-share specific | **SKIP** — not our market |
| OpenBB-finance/OpenBB | Already integrated (`ingestion/openbb_pipeline.py`) | **EXPAND** if coverage gaps |
| freqtrade/freqtrade | Covered (`trading/hyperliquid.py`, `validation/gates.py`) | **SKIP** |
| AI4Finance-Foundation/FinGPT | Not integrated; Nemotron currently generic | **EVALUATE** — specialty sentiment model |
| juspay/hyperswitch | Payments router, unrelated | **SKIP** |
| microsoft/qlib | Not integrated | **PORT SELECTIVELY** — Alpha158/360 factor zoo → `features/lab.py` |
| Fincept-Corporation/FinceptTerminal | Not integrated (C++/Qt desktop) | **MINE FOR GAPS** — use their 100+ data connector list as a checklist |

## Concrete recommendations (ranked)

### 1. Kronos foundation model — `timeseries/kronos_forecaster.py`

**Why:** Kronos is an OHLCV-tokenizing transformer trained on 45+ exchanges, while
our existing `timeseries/timesfm_forecaster.py` wraps Google TimesFM (generic
univariate series). Kronos is natively multivariate (O,H,L,C,V), which matters
for options and dealer-gamma signals where intraday range structure drives edge.

**Integration surface:**
- New wrapper in `timeseries/kronos_forecaster.py` mirroring `TimesFMForecaster` API.
- Adapter in `oracle/forecaster_adapter.py` that emits Kronos-derived `Signal`
  objects (e.g. `kronos_range_expansion`, `kronos_volume_pulse`) alongside TimesFM.
- Register as a 6th competing model in `oracle/engine.py` so it joins the
  Shapley / disagreement conviction adjusters automatically.
- Fits the 13-layer conviction stack without changes — Kronos signals become just
  another contributor to the `disagreement` and `fragility` layers.

**Risks:** Model weights + deps may be heavy; run via llamacpp-style lazy import
the way `openbb_pipeline.py` does, so server startup is unaffected.

### 2. Named-investor personas — extend `agents/personas.py`

**Why:** The current 5 archetypes (balanced, value_investor, momentum_trader,
macro_strategist, contrarian) are generic. ai-hedge-fund popularized specific
named personas (Buffett/Munger/Ackman/Wood/Burry/Dalio) that map cleanly onto
different subsets of GRID's signal weights. Low-risk additive change — no
schema migration, no new deps.

**Integration surface:**
- 6–8 new `InvestorPersona` entries in `agents/personas.py`.
- Each sets `signal_weights` distinctively (e.g. Wood over-weights
  smart_money+scanner, Dalio over-weights global_levers+foreign_lobbying).
- Expose via the existing `/api/agents/personas` endpoint automatically
  because `_PERSONAS` is the registry.

**Risks:** Persona weights are untuned. Ship behind a validation window (shadow
mode before a new persona counts toward live predictions).

### 3. qlib Alpha158/360 factor zoo — selective port into `features/lab.py`

**Why:** qlib's factor library codifies ~158–360 well-known price/volume alpha
factors (momentum, reversal, Kbar, rolling stats). `features/lab.py` today is
driven by GRID's own signal domain and has 1,188 eligible features, but the qlib
factor set is a battle-tested zoo for intraday/equity alpha that we can port
selectively. Not a wholesale adopt — we don't want qlib's pipeline, just the
factor recipes.

**Integration surface:**
- Evaluate qlib's `qlib.contrib.data.handler.Alpha158` factor list.
- Cherry-pick ~20 factors that are orthogonal to our existing `discovery/orthogonality.py` registry.
- Port as pure pandas implementations into `features/lab.py` under a
  `qlib_alpha_*` namespace so they're trivially greppable.
- Route through the normal orthogonality audit before promotion.

**Risks:** Orthogonality check is mandatory — otherwise we balloon the feature
set with correlated redundancy. Respect NaN strategy per the `data-integrity.md` rule.

### 4. FinGPT — evaluate as specialty sentiment LLM

**Why:** Our Nemotron-Cascade-2 and Nemotron-3-Super are generic instruction
models. FinGPT fine-tunes on financial news/filings/earnings and benchmarks
materially better on FinBERT-style sentiment tasks. Current consumers:
`intelligence/earnings_transcript_analyzer.py`, `intelligence/smart_money.py`,
`ingestion/altdata/smart_money.py`.

**Integration surface:**
- Add as a 3rd route tier in `llm/router.py` (LOCAL / REASON / ORACLE / **SENTIMENT**).
- Expose via a new llamacpp sidecar process if the quantized weights fit, or
  fall back to HuggingFace inference.
- Benchmark against current sentiment paths on a held-out set before wiring live.

**Risks:** Adds a 4th LLM process on the server — check GPU/CPU headroom first.
Lower priority than Kronos/qlib until we quantify the sentiment-accuracy gap.

### 5. FinceptTerminal data-connector audit

**Why:** Not an adoption candidate (they're a C++/Qt desktop app), but their
100+ data-connector list is a free gap-analysis against our 48 pullers.
Specifically interesting: **DBnomics** (2.6B+ series, ~300 providers) and
**AkShare** (Chinese+global data) are both free and not yet in `ingestion/`.

**Integration surface:**
- New pullers `ingestion/international/dbnomics.py` and `ingestion/international/akshare.py`.
- Each follows the standard `_resolve_source_id()` + Hermes scheduler pattern.
- DBnomics is high-value because it aggregates FRED, ECB, BIS, IMF, etc. — could
  consolidate several existing pullers.

**Risks:** Watch for overlap with existing sources (FRED, ECB). Entity-map
duplicate detection in `normalization/resolver.py` must be working before this
lands or we double-count signals.

## Explicit skips

- **TradingAgents (tauric)** — already built. `agents/runner.py`,
  `agents/personas.py`, `agents/backtest.py` are our equivalent. Extending
  personas (item #2) is the only useful addition.
- **TradingAgents-CN** — A-share / Shanghai & Shenzhen specific. Outside scope.
- **daily_stock_analysis** — the value-add is the GH Actions scheduler. We have
  Hermes (`ingestion/scheduler.py` + `hermes_operator.py`), which is strictly
  more capable. `oracle/report.py` already handles the daily-digest role.
- **freqtrade** — crypto-only bot + backtester. We have
  `trading/hyperliquid.py` (exchange) and `validation/gates.py` (backtester).
  Their Telegram control UX is a nice-to-have but not worth the dep.
- **hyperswitch** — payments router. Unrelated to trading intel.

## Cross-cutting notes

- **Conviction stack compatibility:** Any new model (Kronos, FinGPT) must be
  defensive per `CLAUDE.md`'s 13-layer table — wrap DB calls in try/except and
  return neutral `1.0` on failure, so a missing upstream never breaks the live
  path.
- **PIT correctness:** Every factor ported from qlib must route through
  `store/pit.py`. qlib's native pipeline is not PIT-aware; do NOT copy its
  dataset handler wholesale.
- **Shadow before live:** New personas and new models go through
  CANDIDATE → SHADOW → STAGING → PRODUCTION in `governance/registry.py`. No
  exceptions, especially for the persona additions where weights are untuned.
- **Dedup audit:** Before starting any of the above, re-run
  `/grid-check-exists <keyword>` per the pre-build checklist. This doc is a
  snapshot; the authoritative inventory remains `docs/MODULE_INVENTORY.md`.

## Suggested sequencing

1. **Week 1 (fastest wins):** Named-investor personas (item #2). ~1 day, no
   deps, purely additive.
2. **Week 1–2:** DBnomics puller (item #5 partial). Unblocks broader macro
   coverage with a single source.
3. **Week 2–3:** Kronos wrapper + oracle adapter (item #1). Biggest single
   capability add. Ship in SHADOW for at least 2 weeks before counting toward
   live predictions.
4. **Week 3–4:** qlib factor port (item #3). Gated behind orthogonality audit.
5. **Deferred:** FinGPT evaluation (item #4) pending GPU headroom measurement.
