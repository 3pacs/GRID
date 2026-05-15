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

- [ ] [P0] De-duplicate horizon helpers in `engine.py` (one canonical copy; remove the other) — `oracle/engine.py:136` and `oracle/engine.py:2158` — `_default_horizon_buckets` / `_parse_horizon_buckets` / `_horizon_key` are defined twice in the same module with divergent bodies (second `_horizon_key` adds a `canonical[days]` shortcut absent in the first); Python's module-load order means the second silently wins, masking the first definition's logic.
- [ ] [P0] Collapse the two `publish_astrogrid_prediction` implementations to a single source of truth — `oracle/publish.py:51` and `oracle/publisher_gate.py:195` — `publish.py` enriches signals via `build_prediction_context` + `enrich_signals_payload` (regime / fci_regime / vix_level / signal_contributions), but `publisher_gate.py` does the un-enriched insert; `api/routers/oracle.py:18` calls the enriched version while `api/routers/astrogrid_helpers.py:77` calls the un-enriched one, so astrogrid predictions written through `astrogrid_helpers` skip the conviction-stack context the calibrators need.
- [ ] [P1] Rename or merge the duplicate `CalibrationReport` dataclass — `oracle/calibration.py:34` and `inference/calibration.py:57` — same class name, different field shapes across two modules; an accidental cross-import will type-check but produce wrong attribute access at runtime.
- [ ] [P1] Add unit tests for the chat publishing firewall pipeline — `oracle/firewall.py:42` — `verify_output` is the single entry point in `api/routers/chat.py:1409` (LLM output gate) and has zero tests; covers claim extraction → verification → sanity → gate → audit write.
- [ ] [P1] Add unit tests for `gate_decision` publish/review/reject thresholds — `oracle/publisher_gate.py:42` — feeds both chat firewall and astrogrid publish path, no tests covering the auto-publish (>0.85 confidence), reject (contradicted/critical-fail), and review (>30% flagged) branches.
- [ ] [P1] Add unit tests for `oracle/claim_extractor.py` price/percent/direction regexes — `oracle/claim_extractor.py:127` — `extract_claims` parses LLM text into structured claims for the firewall; zero tests, regex changes could silently drop or mis-tag claims.
- [ ] [P1] Add unit tests for `oracle/claim_verifier.py` DB-evidence verdicts — `oracle/claim_verifier.py:17` — verifies parsed claims against the live store; supports the supported / contradicted / insufficient verdict path used by `gate_decision`.
- [ ] [P1] Add unit tests for `oracle/sanity_checker.py` deterministic checks — `oracle/sanity_checker.py:260` — `run_sanity_checks` runs price-range / pct-math / direction-consistency / date / unit / cross-claim checks; zero coverage despite gating publish decisions.
- [ ] [P1] Fix Signal positional-arg mismatch in `_gather_signals_from_registry` — `oracle/engine.py:812` — `Signal(name, family, z, 0, sig_dir, conf, 0)` stores the z-score in the `value` field and sets `z_score=0`; downstream `s.z_score * s.weight` at `oracle/engine.py:1371` then contributes zero, neutralizing registry-sourced signals whenever `GRID_SIGNAL_REGISTRY=1` is enabled.
- [ ] [P2] Split `oracle/engine.py` (2,793 LOC) into focused modules — `oracle/engine.py:1` — file is well past the 1,500-LOC threshold; obvious carve-outs are the `EnsemblePrediction` + `EnsemblePredictor` block (line 2229+) into `oracle/ensemble.py` and the second copy of horizon helpers (line 2158+) into a shared `oracle/horizons.py`.
- [ ] [P2] Refactor `OracleEngine.predict` (405 lines) into smaller passes — `oracle/engine.py:2274` — one of three over-100-line methods in the file; current shape blends regime fetch, FCI compute, shapley, crowdedness, market-implied prob, and journal-bias adjustment in one function.
- [ ] [P2] Refactor `OracleEngine._oracle_one_ticker` (299 lines) — `oracle/engine.py:1189` — per-ticker signal gather + anti-signal scan + convergence boost + confidence normalization in a single function; extract `_apply_credit_family_boost`, `_apply_actor_signal_enrichment`, `_apply_convergence_boost` helpers.
- [ ] [P2] Add unit tests for `oracle/citation_extractor.py` feature-mention matching — `oracle/citation_extractor.py:50` — `extract_citations` is called from `api/routers/chat.py:1450` to record which features the LLM cited; covered behavior is alias/family normalization, no tests exist.
- [ ] [P2] Add unit tests for `oracle/psi_model.py` PSI+VIX gating thresholds — `oracle/psi_model.py:137` — `evaluate_psi_signals` is the entry point used by `scripts/run_psi_oracle.py:26`; the hardcoded Sharpe-2.59 GLD config and Sharpe-2.01 QQQ config thresholds have no regression coverage.
