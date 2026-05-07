# GRID Self-Learning Sprints — 3-Agent Parallel Execution Plan

**Created:** 2026-04-14
**Scope:** Turn GRID from "80 modules with bespoke loops" into "every
scorable module and every LLM call self-improves on a shared cadence,
observed by one orchestrator, plugged into [[Hermes Scheduler|Hermes]]."
**Agents:** Assume **3 frontier-model agents** (Agent-A, Agent-B,
Agent-C) can run in parallel. Every sprint is partitioned so the three
agents never touch overlapping files — no merge collisions, no races.

## The state at sprint kickoff (2026-04-14)

Already on disk and working (uncommitted):

- `intelligence/self_learning_loop.py` — shared primitive (emit → score → update → persist)
- `intelligence/grand_orchestrator.py` — meta-layer, cadence auto-tuner
- `intelligence/llm_harness.py` — self-learning wrapper for any LLM client
- `scripts/audit_self_learning.py` — classifier (HAS_LOOP / NEEDS_LOOP / NO_LOOP_NEEDED)
- Orphan adoption completed: 4 deleted files, 9 new hermes registry slots
- Dark-puller count: 15 → 5; orphans: 33 → 21

Current audit numbers:
- 741 → 738 modules
- 80 HAS_LOOP (10.8%)
- 12 NEEDS_LOOP (1.6%) — target list below
- 649 NO_LOOP_NEEDED (87.6%)

The 12 NEEDS_LOOP modules:
```
features/alpha101
inference/calibration
inference/trained_models
inference/tuning
intelligence/deal_detector
intelligence/earnings_transcript_analyzer
intelligence/news_impact
intelligence/news_momentum
intelligence/spider/priority_queue
physics/momentum
trading/trade_ticket_generator
(+ scripts/lint_module_inventory — false positive, ignore)
```

---

## Partitioning rule (how we avoid agent collisions)

Every sprint assigns each agent a **disjoint file set** — three
non-overlapping path prefixes. The only shared files are (a) the
primitives `self_learning_loop.py`/`grand_orchestrator.py`/`llm_harness.py`
which are **frozen** after sprint 1 and (b) `scripts/audit_self_learning.py`
which only Agent-A touches. No agent ever edits a file another agent is
holding in the same sprint. Sprints are sequenced: all three agents must
report done before the next sprint starts.

Conflict matrix convention: Agent-A owns `intelligence/`, Agent-B owns
`inference/` + `features/` + `physics/`, Agent-C owns `trading/` +
`oracle/` + `api/`. Sprint 1 breaks this rule for the orchestrator + DB
migration, but sprints 2–7 honor it.

---

## Sprint 1 — Land the primitives (1 day)

**Goal:** Commit + deploy the already-written scaffolding. After this
sprint the primitives are frozen APIs that sprints 2–7 depend on.

| Agent | Owns | Tasks |
|-------|------|-------|
| Agent-A | `intelligence/self_learning_loop.py`, `intelligence/grand_orchestrator.py`, `intelligence/llm_harness.py`, `scripts/audit_self_learning.py` | (a) code review + type-tighten, (b) write `tests/test_self_learning_loop.py` (emission round-trip, update clamp, defensive failure), (c) write `tests/test_grand_orchestrator.py` (cadence gating, auto-demote, registry), (d) write `tests/test_llm_harness.py` (fake client, escalation, template lookup, temperature updater). |
| Agent-B | `scripts/migrations/add_self_learning_tables.sql` (NEW), `scripts/bootstrap_self_learning.py` (NEW) | Write the DDL migration for `self_learning_emissions`, `self_learning_state`, `grand_orchestrator_log`. Add a bootstrap script that creates the tables idempotently and seeds `grand_orchestrator_log` with one row per registered module. Run it locally + against `griddb` on grid-svr. |
| Agent-C | `api/routers/self_learning.py` (NEW), `api/main.py` (1-line mount) | Thin observability router: `GET /api/self-learning/modules`, `GET /api/self-learning/log?limit=50`, `GET /api/self-learning/module/{name}`. Each endpoint proxies to `grand_orchestrator.get_*` helpers. Unit test against a fake engine. |

**Exit gate:** `python3 -m scripts.audit_self_learning` still reports 80
HAS_LOOP. `pytest tests/test_self_learning_loop.py tests/test_grand_orchestrator.py tests/test_llm_harness.py -v` is green. Migration applied on server. Router responds at `/api/self-learning/modules`.

**Blast radius if this sprint fails:** zero. No existing module depends
on the new primitives yet.

---

## Sprint 2 — Wire the 12 NEEDS_LOOP modules (2 days)

**Goal:** Every module the audit flagged as having a scorable surface
but no learning path gets a SelfLearningLoop. Each wiring is ~30 LOC:
import, instantiate in module init, emit at call time, add an
`update_fn` tailored to the module's outcome.

Partitioned strictly by directory so no two agents touch the same file:

| Agent | Owns (exact files) | Modules to wire |
|-------|--------------------|-----------------|
| Agent-A | `intelligence/deal_detector.py`, `intelligence/earnings_transcript_analyzer.py`, `intelligence/news_impact.py`, `intelligence/news_momentum.py`, `intelligence/spider/priority_queue.py` | 5 modules. All use outcome = forward 5d return alignment with prediction direction. |
| Agent-B | `features/alpha101.py`, `inference/calibration.py`, `inference/trained_models.py`, `inference/tuning.py`, `physics/momentum.py` | 5 modules. Outcome = model-specific (Brier for calibration, RMSE for tuning, information coefficient for alpha101/momentum). |
| Agent-C | `trading/trade_ticket_generator.py` (EXTEND — already gated by circuit breaker), plus audit any remaining holes in `oracle/` that the audit missed | 1 module + a re-audit pass. Outcome = realized P&L per ticket, scaled to [0,1]. |

**Exit gate:** Re-run `audit_self_learning.py`. HAS_LOOP bumps from 80 →
92 (80 + 12). NEEDS_LOOP drops to 0 (excluding the lint-module false
positive). Each agent writes one integration test per module proving
emit → score round-trip works against SQLite.

**Collision prevention:** each agent's file list is disjoint. The
shared `scripts/audit_self_learning.py` is read-only for this sprint.

---

## Sprint 3 — LLM harness on top of 3 production call sites (1.5 days)

**Goal:** Put the `LLMHarness` between live code and 3 LLM call sites.
These 3 are chosen because they already have scorable outcomes: oracle
arena has [[Oracle Calibration|brier score]]s, news_impact has forward returns,
earnings_transcript_analyzer has EPS surprise numbers.

| Agent | Owns | Call site |
|-------|------|-----------|
| Agent-A | `oracle/agent_arena.py` + `oracle/engine.py` (call site injection only, no logic change) | Wrap the per-agent debate `.chat()` calls. One harness per agent role. update_fn = brier score of the agent's directional vote on the prediction's 21d outcome. |
| Agent-B | `intelligence/news_impact.py` (harness at the headline→magnitude call site, separate from the self-learning-loop wiring that Agent-A did in sprint 2) | One harness. update_fn = absolute error between predicted pct move and realized pct move over 5d. **Coordination note:** Agent-A touched this file in sprint 2, but sprint 2 must complete before sprint 3 starts — so no lock contention. |
| Agent-C | `intelligence/earnings_transcript_analyzer.py` (harness only, same temporal ordering) | One harness. update_fn = sign of (guidance_sentiment × post-earnings 2d return). |

**Exit gate:** Three harnesses live. Each has logged ≥ 5 emissions.
Each has at least one scored outcome pulled from existing journal data.
Run `auto_register_self_learning_modules(engine)` and confirm the
orchestrator discovers all three.

**Collision prevention:** the three target files are disjoint. Sprint 3
is sequenced *after* sprint 2 so each file is only held by one agent at
a time across the combined timeline.

---

## Sprint 4 — Cadence auto-tuner validation (1 day)

**Goal:** Prove the grand orchestrator's cadence adjustments actually
move cadence in response to improvement. This is the one sprint where
the three agents work on the same system but *different layers*: logic,
simulation harness, observability.

| Agent | Owns | Tasks |
|-------|------|-------|
| Agent-A | `intelligence/grand_orchestrator.py` (logic tweaks only), `tests/test_grand_orchestrator.py` | Add `simulate_cycles()` helper that lets a test feed synthetic `LearningCycleResult` objects at a fixed cadence; assert cadence halves on sustained improvement and auto-demotes to weekly after 5 negative cycles. |
| Agent-B | `scripts/simulate_self_learning.py` (NEW) | Offline driver: pick 3 registered modules, generate 100 synthetic cycles with seeded improvement curves, run through `run_due_cycles()` in a tight loop, write a CSV of cadence vs. cycle number. This is a **new file** — does not touch Agent-A's module. |
| Agent-C | `pwa/src/views/SelfLearning.jsx` (NEW), `pwa/src/store/selfLearning.js` (NEW) | Minimal dashboard: table of registered modules, sparkline of last 20 cycles' primary_metric per module, cadence badge. Uses the `/api/self-learning/*` endpoints from sprint 1. |

**Exit gate:** Simulation script prints the expected cadence trajectory
(sub-1h → 10min on winners, 1h → 7d on losers). Dashboard renders on
localhost. Zero changes to the primitive's public API.

**Collision prevention:** Agent-A owns only `grand_orchestrator.py` and
its test. Agent-B creates a brand-new `scripts/` file. Agent-C creates
two brand-new frontend files. No shared ownership.

---

## Sprint 5 — Register the orchestrator as a Hermes slot (0.5 day)

**Goal:** One agent only — this sprint is too small to parallelize.
Running it as **Agent-A** keeps Agents B and C free to start sprint 6
work that doesn't depend on Hermes.

| Agent | Owns | Tasks |
|-------|------|-------|
| Agent-A | `scripts/hermes_operator.py` (1 registry entry), `intelligence/grand_orchestrator_runner.py` (NEW) | New runner module exposes `pull_all(engine)` that calls `auto_register_self_learning_modules(engine)` then `run_due_cycles(engine)`. Register it in `_SOURCE_REGISTRY` with `interval_h: 1`. Smoke test via `python3 -m scripts.hermes_operator --once --source grand_orchestrator`. |
| Agent-B | `features/` sprint 6 prep — read-only | Drafts the sprint 6 plan for features/ batch. No writes. |
| Agent-C | `trading/` sprint 6 prep — read-only | Drafts the sprint 6 plan for trading/. No writes. |

**Exit gate:** `systemctl status grid-hermes` on grid-svr shows the
orchestrator slot firing every hour. `grand_orchestrator_log` on the
server has at least one row per registered module.

---

## Sprint 6 — Scale to the 80 existing HAS_LOOP modules (3 days)

**Goal:** Every one of the 80 pre-existing legacy-learning modules
(`per_signal_brier`, `trust_scorer`, `forensics`, etc.) gets registered
with the grand orchestrator. Most already have the "update" path —
they just need to expose a `run_cycle` callable and be registered.

Partition the 80 modules by top-level directory so no two agents write
to the same file:

| Agent | Owns (top-level dirs) | Approx module count |
|-------|----------------------|---------------------|
| Agent-A | `intelligence/*` | ~35 modules (trust_scorer, cooccurrence, confidence_bucket, historical_scenario, null_hypothesis, contra_indicator, etc.) |
| Agent-B | `features/*` + `physics/*` + `discovery/*` + `analysis/*` | ~25 modules (feature_importance_log, dealer_gamma, clustering, flow_aggregator, etc.) |
| Agent-C | `oracle/*` + `trading/*` + `governance/*` + `validation/*` + `alpha_research/*` | ~20 modules (oracle engine, options_tracker, model_registry, walk-forward gates, alpha_research signals) |

The wiring per module is: (a) define an `update_fn` that reads the
module's existing history table and returns a new param dict, (b) add
one-line `register_learning_module(name, update_fn)` in module init,
(c) delete any now-duplicate cadence logic in the module itself.

**Exit gate:** `grand_orchestrator.get_all_registered()` returns ≥ 92
modules (12 new + 80 legacy). `scripts/audit_self_learning.py` reports
HAS_LOOP ≥ 92 still. No regressions in the existing legacy tests.

**Collision prevention:** three top-level directory partitions, all
disjoint. Any module that genuinely spans two directories stays with
whichever agent owns the primary file.

---

## Sprint 7 — Meta-learning the update rules themselves (2 days)

**Goal:** The orchestrator currently runs fixed `update_fn`s. Sprint 7
makes the orchestrator observe which update rules *actually improve*
primary metrics over 100+ cycles, and demote the ones that don't. This
is the self-improving layer on top of the self-improving layer.

| Agent | Owns | Tasks |
|-------|------|-------|
| Agent-A | `intelligence/meta_orchestrator.py` (NEW) | New module: reads `grand_orchestrator_log`, computes per-module improvement slope over the last 100 cycles, flags modules whose update_fn is net-negative. Exposes `suggest_demotions() -> list[str]`. Read-only wrt existing files. |
| Agent-B | `tests/test_meta_orchestrator.py` (NEW), synthetic fixture data | Integration test: seed the log with 3 winning modules + 2 losing modules, run meta_orchestrator, assert losers show up in suggestions. |
| Agent-C | `pwa/src/views/SelfLearning.jsx` (EXTEND the file Agent-C created in sprint 4 — no other agent touches it) | Add a "flagged" tab that lists meta_orchestrator suggestions with a disable toggle (no-op write for now). |

**Exit gate:** Seeded simulation produces the expected demotion list.
Dashboard renders the flagged tab. Zero writes from the meta layer to
production module files — demotions are advisory only this sprint.

---

## Sprint 8 (stretch) — One-click demote/promote (1 day)

**Goal:** Close the loop. Admin can toggle a module's orchestration
state from the dashboard. State is persisted in `self_learning_state`
via a new `enabled` field.

This is a stretch sprint — it's a nice-to-have after the core
machinery is live. Partitioned same as sprint 4 (A=logic, B=tests,
C=frontend), same disjoint file set.

---

## Execution order + dependencies

```
Sprint 1 ──▶ Sprint 2 ──▶ Sprint 3 ──▶ Sprint 4 ──▶ Sprint 5 ──▶ Sprint 6 ──▶ Sprint 7 ──▶ Sprint 8
(1d)        (2d)         (1.5d)       (1d)         (0.5d)       (3d)         (2d)         (1d stretch)
```

Sprints 1–5 are the critical path. Sprint 6 is the bulk of the wiring
but fully parallelizable. Sprint 7 is the meta layer. Total: ~10.5
days wall-clock with 3 agents, sequential sprints. If we relaxed the
"no-collisions within a sprint" rule we could overlap sprints 3+4 and
sprints 5+6, saving ~2 days — but at the cost of merge conflicts.

## Hard guardrails

1. **No new primitives after sprint 1.** `self_learning_loop.py`,
   `grand_orchestrator.py`, and `llm_harness.py` are frozen APIs. Bug
   fixes only; no new public functions without a new sprint.
2. **Every wired module must keep working if the orchestrator is
   offline.** The grand_orchestrator loop is a *consumer* of learning
   signals, not a *producer*. Modules never block on orchestrator
   calls.
3. **Every update_fn must be defensive.** Return current_params on any
   error, never raise. The loop primitive already wraps them in
   try/except, but belt + suspenders here.
4. **Never wire a loop to a module without a scorable outcome.** If
   there's no ground truth, the audit should say NO_LOOP_NEEDED. If
   the audit disagrees with a human read, update the audit first.
5. **Agents commit to their own branches.** Sprint 1 lands on `main`.
   Sprints 2–7 land on `sprint-N-agent-X` branches that merge sequentially.

## Success criteria for "self-learning is live"

- [ ] 92+ modules registered with `grand_orchestrator`
- [ ] Cadence auto-tuner has promoted at least 1 module and demoted at
      least 1 module on real (not simulated) metrics
- [ ] 3 LLM call sites running through `LLMHarness` with ≥ 100 scored
      emissions each
- [ ] `/api/self-learning/*` endpoints serving the dashboard
- [ ] Dashboard rendered at `https://grid.stepdad.finance/self-learning`
- [ ] `grid-hermes` running the orchestrator every hour on grid-svr
- [ ] `audit_self_learning.py` reports 0 NEEDS_LOOP (false positives
      excluded)
- [ ] Meta-orchestrator has flagged at least 1 underperforming
      update_fn over a 2-week window

When every box is ticked, the platform is genuinely self-improving end
to end: every scorable module records → scores → updates on cadence,
every LLM call site recorded the same way, the cadence of each module
is tuned by observed improvement, and the update rules themselves are
observed for net contribution. That's the harness the user asked for.
