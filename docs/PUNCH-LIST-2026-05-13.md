# GRID Punch List — 2026-05-13

This file is an append-only feed produced by the GRID Code Auditor for the
GRID Backlog Orchestrator to pick up via its TIER 4 path. Each item is a
single-PR-sized task (< 200 LOC, < 5 files).

Priority legend:
- `[P0]` — security / correctness bug actively affecting production. Next pick.
- `[P1]` — correctness or coverage gap likely affecting accuracy/reliability.
- `[P2]` — hygiene: dead code, long functions, stale TODOs, missing types.

---

## Auditor 2026-05-13 — oracle/

Metrics: 29 py files, 10,651 total LOC, 3 commits in last 30d.
Checks run: A B C D E F G H I J

> **2026-06-09 reconciliation note:** 11 of the 13 items in this section landed
> via the hermes-operator commit `8de99c4e` (2026-05-30) and are marked `[x]`
> below. The two remaining open items are `[P2]` engine-refactor splits that
> are too large for a single routine PR (`oracle/engine.py` is 2,877 LOC).

- [x] [P0] De-duplicate horizon helpers in `engine.py` (one canonical copy; remove the other) — `oracle/engine.py:136` and `oracle/engine.py:2158` — `_default_horizon_buckets` / `_parse_horizon_buckets` / `_horizon_key` are defined twice in the same module with divergent bodies (second `_horizon_key` adds a `canonical[days]` shortcut absent in the first); Python's module-load order means the second silently wins, masking the first definition's logic. **RESOLVED 2026-05-30 (commit 8de99c4e):** single canonical definitions remain at `oracle/engine.py:137,152,185`; the second copy was removed.
- [x] [P0] Collapse the two `publish_astrogrid_prediction` implementations to a single source of truth — `oracle/publish.py:51` and `oracle/publisher_gate.py:195` — `publish.py` enriches signals via `build_prediction_context` + `enrich_signals_payload` (regime / fci_regime / vix_level / signal_contributions), but `publisher_gate.py` does the un-enriched insert; `api/routers/oracle.py:18` calls the enriched version while `api/routers/astrogrid_helpers.py:77` calls the un-enriched one, so astrogrid predictions written through `astrogrid_helpers` skip the conviction-stack context the calibrators need. **RESOLVED 2026-05-30 (commit 8de99c4e):** `oracle/publisher_gate.py` is now a 161-line re-export shim that imports `publish_astrogrid_prediction` from `oracle.publish` (single enriched source of truth).
- [x] [P1] Rename or merge the duplicate `CalibrationReport` dataclass — `oracle/calibration.py:34` and `inference/calibration.py:57` — same class name, different field shapes across two modules; an accidental cross-import will type-check but produce wrong attribute access at runtime. **RESOLVED 2026-05-30 (commit 8de99c4e):** `oracle/calibration.py:34` renamed to `OracleCalibrationReport`; collision-prevention regression test at `tests/test_oracle_calibration_report_naming.py`.
- [x] [P1] Add unit tests for the chat publishing firewall pipeline — `oracle/firewall.py:42` — `verify_output` is the single entry point in `api/routers/chat.py:1409` (LLM output gate) and has zero tests; covers claim extraction → verification → sanity → gate → audit write. **RESOLVED 2026-05-30 (commit 8de99c4e):** `tests/test_firewall.py` (473 LOC, 27 test cases) covers `verify_output`.
- [x] [P1] Add unit tests for `gate_decision` publish/review/reject thresholds — `oracle/publisher_gate.py:42` — feeds both chat firewall and astrogrid publish path, no tests covering the auto-publish (>0.85 confidence), reject (contradicted/critical-fail), and review (>30% flagged) branches. **RESOLVED 2026-05-30 (commit 8de99c4e):** `tests/test_publisher_gate.py` (250 LOC, 22 test cases) covers `gate_decision`.
- [x] [P1] Add unit tests for `oracle/claim_extractor.py` price/percent/direction regexes — `oracle/claim_extractor.py:127` — `extract_claims` parses LLM text into structured claims for the firewall; zero tests, regex changes could silently drop or mis-tag claims. **RESOLVED 2026-05-30 (commit 8de99c4e):** `tests/test_claim_extractor.py` (229 LOC, 19 test cases) covers `extract_claims`.
- [x] [P1] Add unit tests for `oracle/claim_verifier.py` DB-evidence verdicts — `oracle/claim_verifier.py:17` — verifies parsed claims against the live store; supports the supported / contradicted / insufficient verdict path used by `gate_decision`. **RESOLVED 2026-05-30 (commit 8de99c4e):** `tests/test_claim_verifier.py` (380 LOC, 34 test cases) covers `verify_claim` verdict branches.
- [x] [P1] Add unit tests for `oracle/sanity_checker.py` deterministic checks — `oracle/sanity_checker.py:260` — `run_sanity_checks` runs price-range / pct-math / direction-consistency / date / unit / cross-claim checks; zero coverage despite gating publish decisions. **RESOLVED 2026-05-30 (commit 8de99c4e):** `tests/test_sanity_checker.py` (367 LOC, 41 test cases) covers `run_sanity_checks`.
- [x] [P1] Fix Signal positional-arg mismatch in `_gather_signals_from_registry` — `oracle/engine.py:812` — `Signal(name, family, z, 0, sig_dir, conf, 0)` stores the z-score in the `value` field and sets `z_score=0`; downstream `s.z_score * s.weight` at `oracle/engine.py:1371` then contributes zero, neutralizing registry-sourced signals whenever `GRID_SIGNAL_REGISTRY=1` is enabled. **RESOLVED 2026-05-30 (commit 8de99c4e):** call at `oracle/engine.py:835` now passes `Signal(name, family, z, z, sig_dir, conf, 0)` so `z_score` carries the registry z; regression test at `tests/test_oracle_registry_signals.py`.
- [ ] [P2] Split `oracle/engine.py` (2,793 LOC) into focused modules — `oracle/engine.py:1` — file is well past the 1,500-LOC threshold; obvious carve-outs are the `EnsemblePrediction` + `EnsemblePredictor` block (line 2229+) into `oracle/ensemble.py` and the second copy of horizon helpers (line 2158+) into a shared `oracle/horizons.py`.
- [ ] [P2] Refactor `OracleEngine.predict` (405 lines) into smaller passes — `oracle/engine.py:2274` — one of three over-100-line methods in the file; current shape blends regime fetch, FCI compute, shapley, crowdedness, market-implied prob, and journal-bias adjustment in one function.
- [ ] [P2] Refactor `OracleEngine._oracle_one_ticker` (299 lines) — `oracle/engine.py:1189` — per-ticker signal gather + anti-signal scan + convergence boost + confidence normalization in a single function; extract `_apply_credit_family_boost`, `_apply_actor_signal_enrichment`, `_apply_convergence_boost` helpers.
- [x] [P2] Add unit tests for `oracle/citation_extractor.py` feature-mention matching — `oracle/citation_extractor.py:50` — `extract_citations` is called from `api/routers/chat.py:1450` to record which features the LLM cited; covered behavior is alias/family normalization, no tests exist. **RESOLVED 2026-05-30 (commit 8de99c4e):** `tests/test_citation_extractor.py` (218 LOC, 28 test cases) covers `extract_citations`.
- [x] [P2] Add unit tests for `oracle/psi_model.py` PSI+VIX gating thresholds — `oracle/psi_model.py:137` — `evaluate_psi_signals` is the entry point used by `scripts/run_psi_oracle.py:26`; the hardcoded Sharpe-2.59 GLD config and Sharpe-2.01 QQQ config thresholds have no regression coverage. **RESOLVED 2026-05-30 (commit 8de99c4e):** `tests/test_psi_model.py` (385 LOC, 25 test cases) covers `evaluate_psi_signals`.

## Auditor 2026-05-17 — api/

Metrics: 107 py files, 48,162 total LOC, 4 commits in last 30d.
Checks run: A B C D E F G H I J

> **2026-06-09 reconciliation note:** the `has_more` pagination items, the
> `system.py` test-coverage item, the `prediction_backtest` engine-import
> item, the `clear_singletons()` fix, and the `viz` smoke test are each
> covered by an open routine PR (see PRs #239, #240, #255, #256, #265, #275,
> #277, #292, #297). They are NOT marked `[x]` here until those PRs merge.

- [ ] [P1] Add `has_more` to `search_intelligence` paginated response — `api/routers/intelligence_search.py:131` — endpoint takes `limit`/`offset` Query params but the return shape is `{results, total, query}` only; `.claude/rules/security.md` requires list endpoints to return `total` + `limit` + `offset` + `has_more` so clients can paginate without re-computing.
- [ ] [P1] Add `has_more` to `oracle.get_predictions` response — `api/routers/oracle.py:168` — returns `total/limit/offset` but not `has_more`; same security-rule pagination contract as the journal endpoint at `api/routers/journal.py:78` which is the canonical pattern to copy.
- [ ] [P1] Add `has_more` to `models.get_all` response — `api/routers/models.py:67` — same `total/limit/offset` without `has_more` gap; this is the model-registry list, often paged by the PWA model browser.
- [ ] [P1] Add pagination metadata to `intel.intel_search` — `api/routers/intel.py:252` — accepts `limit`/`offset` but the `_ok(...)` envelope returns no `total` / `limit` / `offset` / `has_more`; client gets a results array with no signal that more pages exist.
- [ ] [P1] Add `has_more` to `intel.intel_predictions_active` meta — `api/routers/intel.py:1384` — currently emits `meta={"total": total, "limit": limit, "offset": offset}` but no `has_more`; small uniformity fix on a hot intel endpoint.
- [ ] [P1] Add direct test coverage for `api/routers/system.py` — `api/routers/system.py:64` — 1,686-LOC router holds `/health`, `/status`, `/freshness`, `/pipeline-health`, `/hermes-status`, `/services`; only `/health` and `/status` are touched by `tests/test_api.py:47`; create `tests/test_system_router.py` exercising at least `/freshness`, `/pipeline-health`, `/hermes-status` happy paths.
- [ ] [P1] Stop importing `get_engine` from `api.dependencies` in `prediction_backtest.py` — `api/routers/prediction_backtest.py:20` — re-export only works because the name is in module scope; the other 9 routers use `get_db_engine` (the clearable wrapper). Switch to `get_db_engine` for consistency with the singleton-clearing contract documented in `.claude/rules/security.md`.
- [ ] [P2] Fix `clear_singletons()` so it actually replaces the engine — `api/dependencies.py:67` — function disposes `_db_engine` and sets api-level pointer to None, but the underlying `db._engine` singleton at `db.py:46` is not cleared, so the next `get_db_engine()` returns a *disposed* engine. Add `import db; db._engine = None` (or expose a `db.clear_engine()` helper) so the contract advertised in the docstring holds.
- [ ] [P2] Replace f-string SQL in `prediction_backtest.dataset_stats` with parameterized table-validated query — `api/routers/prediction_backtest.py:116` — `text(f"SELECT COUNT(*) FROM {table}")` interpolates a table name into SQL; today the values come from a hardcoded list literal so it's safe, but the pattern violates `.claude/rules/security.md` ("NEVER use f-strings ... for SQL queries"). Switch to explicit per-table queries or an allowlist+validate helper like the one at `api/routers/config.py:27`.
- [ ] [P2] Split `canvas_expand.expand_node` (737 LOC) into stage helpers — `api/routers/canvas_expand.py:219` — single function blends actor lookup, signal fetch, evidence rollup, layout, and serialization; extract `_collect_neighbor_actors`, `_attach_signals`, `_attach_evidence`, `_build_layout` as separate functions in the same file (no new modules).
- [ ] [P2] Split `intel.intel_briefing` (509 LOC) into per-section helpers — `api/routers/intel.py:1650` — function returns a multi-section briefing payload; each section (macro, sectors, tickers, news, etc.) is an obvious extraction target with no shared mutable state.
- [ ] [P2] Split `intelligence_risk._build_risk_map` (448 LOC) — `api/routers/intelligence_risk.py:43` — pre-DI bug magnet; pull per-region risk computation and the final assembly step into named helpers so the route handler stays under 100 LOC.
- [ ] [P2] Split `flows._build_sector_connections` (464 LOC) — `api/routers/flows.py:877` — sector graph construction; extract `_score_edges`, `_normalize_weights`, `_label_communities` helpers to bring the body under the 100-LOC review threshold from `.claude/rules/common/coding-style.md`.
- [ ] [P2] Add a smoke test for the 9 unauthenticated `viz` routes — `api/routers/viz.py:28` — `/api/v1/viz/recommend`, `/rules`, `/weights`, `/spec/*` are mounted without `require_auth`. They return canned VizSpec payloads (no DB writes, no PII), but the no-auth surface should be locked in with a test so a future edit can't accidentally widen exposure (a regression check at minimum: GET `/api/v1/viz/rules` returns 200 without a token).
