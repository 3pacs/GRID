## 2026-05-14 05:25 UTC — 2026-05-14-0510
**Why this matters next run:** PR #164 closes [P1] item 3 (`CalibrationReport` dataclass-name collision) — the LAST non-blocked PUNCH-LIST-2026-05-13 item. The routine queue is now genuinely empty/stuck: all remaining oracle/ items are blocked behind un-merged agent PRs or need operator input. Next agent should expect to log a no-work entry unless #156/#157 have merged.

Fix: renamed `oracle.calibration.CalibrationReport` → `OracleCalibrationReport`. The prior handoff flagged this as "needs operator input on which name wins" — it does NOT. Merging was off the table (the two dataclasses have genuinely different field shapes), and the oracle copy had **zero external importers** (verified `grep -rn 'CalibrationReport'`), so renaming it is a fully-contained 1-file change with no decision required. `inference.calibration.CalibrationReport` is left as the sole `CalibrationReport`. Added `tests/test_oracle_calibration_report_naming.py` (5 tests, `pytest --noconftest` green, ruff clean).

**PUNCH-LIST-2026-05-13 status — queue is now exhausted of unblocked work:**
- [P0] items 1, 2 → PRs #156, #157 still open (file claims on `oracle/engine.py`, `oracle/publisher_gate.py`).
- [P1] item 5 (`gate_decision` tests) → blocked on #157 merge.
- [P1] item 9 (Signal positional-arg bug, `oracle/engine.py:812`) → blocked on #156 merge.
- [P2] items 10/11/12 (split/refactor `oracle/engine.py`) → architectural, need operator + blocked on #156.
- Everything else (3/4/6/7/8/13/14) → DONE (PRs #158-164).

**If #156/#157 still open next run:** the queue is stuck. Per the standing fallback, either pick a DEV-NOTES H10 non-CLI `print()` → `log` conversion in an unclaimed module (`grep -nE '^\s+print\(' intelligence/*.py`, excluding `__main__` blocks — see 2026-05-14-0413 handoff for the leave-alone list), or log a clean no-work entry. Do not thrash on the blocked items.

**Env note (carried forward):** this box ships with NO python deps preinstalled — had to `pip install ruff numpy loguru sqlalchemy pytest pandas` to import `oracle.calibration` + `inference.calibration` and run the regression test. Budget for ~2 min of pip installs if your test needs to import SUT modules. `gh` CLI absent — MCP `mcp__github__*` only. `git push origin routine-bookkeeping` → 403, use MCP `create_or_update_file`.

## 2026-05-14 04:48 UTC — 2026-05-14-0413
**Why this matters next run:** PR #163 closes [P1] item 4 (`oracle/firewall.py::verify_output` end-to-end pipeline tests). The 2026-05-14-0306 handoff explicitly said wait until #157 + item 5 landed before tackling item 4 — I deviated because #157 has been open 24h+ without merging and item 5 is blocked behind it on the same file claim, so the test-gap chain was indefinitely stuck. The deviation cost is small: firewall tests patch `gate_decision` at the import site, so they don't care about its internals; a future item 5 PR can pick whatever `gate_decision` mocking convention it likes without breaking these tests.

PR #163 adds 32 test cases (473 LOC, tests-only) across 7 test classes. Mocks `extract_claims` / `verify_claims` / `run_sanity_checks` / `gate_decision` at the `oracle.firewall.*` import site; `_audit_claims` uses a MagicMock engine with the `connect().__enter__().execute()` chain established in PR #161/162. Smoke-run: `pytest --noconftest tests/test_firewall.py` → 32 passed in 0.34s. Ruff clean.

Behavior locked in by tests worth noting for any future refactor:

- **`flagged_count = contradicted + critical_fail`** (line 77) is a DIFFERENT predicate than the `_mark_unverified` flagging set (which also flags `ambiguous` and any `warn` sanity result). The two diverge on `ambiguous` verdicts and on `warn`-flagged-but-supported claims. Pinned by `TestFlaggedCountSemantics` + `TestMarkUnverified`. If you ever align them, update BOTH classes in the same PR.
- **`_mark_unverified` reverse-span ordering** — flagged claims sorted by `source_span[0]` DESC so later-position inserts don't shift earlier spans. Pinned by `test_multiple_flagged_inserted_in_reverse_order_preserves_spans`. A future LTR insertion would silently corrupt second-and-onwards marker positions.
- **`_mark_unverified` silently skips `span_start > len(text)`** (the `0 < span_start <= len(result)` guard at line 110). Pinned by `test_span_start_past_end_of_text_silently_skipped`. A future strict-mode would need to update this test.
- **`_audit_claims` is non-blocking** — `engine.connect()` raising is logged at `warning` and swallowed. The chat firewall must stay up even if `claim_audit` is unreachable. Pinned by `test_db_failure_is_swallowed_and_logged`.
- **Empty-claims short-circuit at line 54** skips `verify_claims` / `run_sanity_checks` / `_audit_claims` entirely — `gate_decision([])` is called but the four import-site patches confirm zero calls to the downstream three. Pinned by `test_no_claims_skips_verify_sanity_and_audit`. A future "always audit" change must update this test.
- **Materiality table** — `claim_type in ("price", "percentage")` → `"high"`, else `"medium"`. Adding a new claim_type silently defaults to `"medium"`. Pinned via parametrise across all 6 `ClaimType` values.
- **`claim_text` truncated at 500 chars** before insert. Defensive cap for the `claim_audit.claim_text` column. Pinned by `test_claim_text_truncated_at_500_chars`.

**Remaining unclaimed punch-list items (smallest-first ordering):**

- **[P1] item 5**: `oracle/publisher_gate.py::gate_decision` (line 42) — **PR #157 still open on this file**. Wait for #157 to merge, then write ~120 LOC of tests for the auto-publish (>0.85 confidence), reject (contradicted/critical-fail), and review (>30% flagged) branches. **The PR #163 firewall tests now pin `gate_decision`'s call shape at the boundary** (`list[CheckedClaim]` → `PublishDecision(decision, score, claims, reasons)`), so item 5 can use that contract as its starting point.
- **[P1] item 3**: DONE in PR #164 (2026-05-14-0510). Renamed `oracle.calibration.CalibrationReport` → `OracleCalibrationReport`.
- **[P1] item 9**: Signal positional-arg mismatch in `_gather_signals_from_registry` (`oracle/engine.py:812`). Real bug — z-score stored in `value` field, `z_score=0` → registry-sourced signals contribute zero downstream at line 1371. **Still blocked on PR #156 merge** (file claim on `oracle/engine.py`).
- **[P2] items 10-12**: Splits/refactors of `oracle/engine.py`. Architectural; coordinate with operator.

With items 4/6/7/8/13/14 all done, the **oracle test-gap chain is effectively closed for now**. Item 5 (gate_decision) is the only outstanding test-gap item and it's blocked on #157.

**If both blocked items stay closed to the next agent, the routine queue is empty. Options:**
- Pick a DEV-NOTES H10 print() → log conversion in a NON-claimed module. Most `__main__` block prints are legitimate CLI output and should NOT be converted (intelligence/sleuth.py:1261-1277, intelligence/source_audit.py:969-979, intelligence/trust_scorer.py:1776-1779, intelligence/cross_reference.py:1808-1832, intelligence/market_diary.py:801-809, intelligence/actors/trial_bridge.py:454-456 are all CLI-output, leave them). The non-CLI prints to look for live INSIDE function bodies — `grep -nE '^\s+print\(' intelligence/*.py` after excluding `__main__` blocks.
- Log a no-work entry. The chain is genuinely stuck on operator-merge of #156 + #157.

**File claims to avoid this cycle (last-touched in still-open agent PRs):**
- `oracle/calibration.py` + `tests/test_oracle_calibration_report_naming.py` (#164)
- `tests/test_firewall.py` (#163)
- `tests/test_claim_verifier.py` (#162)
- `tests/test_psi_model.py` (#161)
- `tests/test_citation_extractor.py` (#160)
- `tests/test_sanity_checker.py` (#159)
- `tests/test_claim_extractor.py` (#158)
- `oracle/publisher_gate.py` + `tests/test_publish_astrogrid_canonical.py` (#157)
- `oracle/engine.py` (#156)
- `intelligence/hypothesis_engine.py` + `tests/test_hypothesis_engine_intelligence_kills_logging.py` (#155)
- `ingestion/altdata/indeed_hiring_puller.py` + `tests/test_indeed_hiring_puller.py` (#154)
- `ingestion/altdata/redfin_puller.py` + `tests/test_redfin_puller.py` (#153)
- `intelligence/universe_ranker.py` + `tests/test_universe_ranker.py` (#152)

**Env quirks (carried forward, unchanged):**
- `git push origin routine-bookkeeping` returns HTTP 403. Use MCP `create_or_update_file` for both `.grid_backups/routine_log.jsonl` and this file. Work-branch pushes work fine.
- `python3 -m pytest` fails at conftest collection (pandas + psycopg2 + python-jose imported at module top). Use `pytest tests/test_X.py --noconftest` for pure-function test files. Needs `pip install pytest loguru sqlalchemy` (sqlalchemy only when the SUT imports it). `ruff check` and `py_compile` work after `pip install ruff`.
- `gh` CLI is not present on this box — use MCP `mcp__github__*` tools for all GitHub operations.
- `mcp__github__list_pull_requests` returns >220KB which exceeds tool-result limits; slice with python via the saved tool-result file, or use `search_pull_requests` with a tighter query.
- `pytest --noconftest` is the cleanest way to bypass the conftest pandas import — `--rootdir=/tmp` and `cd /tmp` are not enough; pytest still walks up to find conftest.py.
- **The 25-min routine budget is tight when discovery is needed.** This run took ~37 min total because the prior handoff's "wait" instruction made me re-scope twice before deciding to deviate. If the next agent inherits a "queue stuck" state, scope down faster — pick a small no-work or a single doc-cleanup rather than thrashing on the blocked items.

---
## 2026-05-14 03:25 UTC — 2026-05-14-0306
**Why this matters next run:** PR #162 closes [P1] item 7 (`oracle/claim_verifier.py` DB-evidence verdict tests). With items 6/7/8/13/14 done, the **remaining test-gap item is [P1] item 4** (`oracle/firewall.py::verify_output` end-to-end pipeline) — but **wait until [P1] item 5 lands** (`oracle/publisher_gate.py::gate_decision`, PR #157 still open on this file). The firewall composes claim_extractor → claim_verifier → sanity_checker → gate_decision → audit write, so testing it before #157 merges risks re-doing the gate-decision mocking from scratch. After #157 lands, item 5 (gate_decision unit tests, ~120 LOC) becomes the safer next pick — then item 4 (firewall composition, ~150 LOC).

PR #162 adds 34 test cases (380 LOC, tests-only) covering every branch of `oracle/claim_verifier.py`: each candidate-name fall-through (`<ticker>` → `<ticker>_full` → `<ticker>_usd_full`) for `_lookup_latest_value` + `_lookup_price_change`; NULL-value filter, engine-exception → None-triple/pair fallthrough; `_verify_price` insufficient/supported/contradicted at the 5% boundary; `_verify_percentage` insufficient/supported/contradicted at the 3pp boundary; `_verify_direction` both-up / both-down / mismatch; `_verify_generic` ambiguous fallback; `verify_claims` dispatcher (known-type routing, unknown-type → generic, order/length preservation). Reuses the PR #161 `MagicMock(engine.connect().__enter__().execute())` chain, with separate `side_effect` builders for `.fetchone()` (single-row lookups) vs `.fetchall()` (price-change window). Smoke-run: `pytest --noconftest tests/test_claim_verifier.py` → 34 passed in 0.33s. Ruff clean.

Behavior locked in by tests worth noting for any future refactor:

- **`actual=0.0` short-circuits to `verdict="supported"`** — `_lookup_latest_value`'s guard is `row[0] is not None`, not truthy. A stored 0.0 passes through; `_verify_price`'s `if actual != 0 else 0` ternary then sets `pct_diff=0`, satisfying `0 <= 5.0`. Pinned by `test_verify_price_zero_actual_short_circuits_to_supported`. A future "0.0 means missing data" guard MUST update this test in the same PR.
- **Tolerances are inclusive `<=`** — claim 105 vs actual 100 (exactly 5% diff) is **supported**, not contradicted; claim 8% vs actual 5% (exactly 3pp diff) is also supported. Pinned by `test_verify_price_boundary_at_5_percent_is_supported` and `test_verify_percentage_boundary_at_3pp_is_supported`. A future tightening to strict `<` must update both.
- **`_lookup_price_change` returns `rows[0]` vs `rows[-1]`** — i.e. latest vs OLDEST in the DESC window, not latest vs second-newest. With `periods=2` (the default for `_verify_percentage`/`_verify_direction`) this is latest-vs-previous, but if a future caller passes `periods=5` the "previous" silently becomes 5 days ago. Pinned by `test_lookup_price_change_returns_latest_and_oldest_when_enough_rows`. If you ever extend the window, consider renaming or adding a separate `_lookup_price_window` helper instead.
- **`_verify_generic` is the catch-all for any unknown `claim_type`** — adding a new `claim_type` to `oracle/claim_extractor.ClaimType` without adding an entry to `_VERIFIERS` will silently route to `_verify_generic` → "ambiguous". Pinned by `test_verify_claims_unknown_type_falls_through_to_generic`.

**Env quirks (carried forward, unchanged):**
- `git push origin routine-bookkeeping` returns HTTP 403. Use MCP `create_or_update_file` for both `.grid_backups/routine_log.jsonl` and this file. Work-branch pushes work fine.
- `python3 -m pytest` fails at conftest collection (pandas + psycopg2 + python-jose imported at module top). Use `pytest tests/test_X.py --noconftest` for pure-function test files. Needs `pip install pytest loguru sqlalchemy`. `ruff check` and `py_compile` work after `pip install ruff`.
- `gh` CLI is not present on this box — use MCP `mcp__github__*` tools for all GitHub operations.
- `mcp__github__list_pull_requests` returns >220KB which exceeds tool-result limits; slice with python via the saved tool-result file, or use `search_pull_requests` with a tighter query.
- `pytest --noconftest` is the cleanest way to bypass the conftest pandas import — `--rootdir=/tmp` and `cd /tmp` are not enough; pytest still walks up to find conftest.py.

---
## 2026-05-14 02:14 UTC — 2026-05-14-0210
**Why this matters next run:** PR #161 closes [P2] item 14 (`oracle/psi_model.py` PSI+VIX gating tests). With items 13 & 14 done, the remaining test-gap items are **[P1] item 7** (`oracle/claim_verifier.py` — DB-mock-required, heaviest) and **[P1] item 4** (`oracle/firewall.py::verify_output` — best done LAST after 5/7 land). [P1] item 5 (`oracle/publisher_gate.py`) is still file-claimed by PR #157.

PR #161 adds 33 test cases (385 LOC, tests-only) covering every behavior of `oracle/psi_model.py`: `_check_psi_condition` lt/gt strict-comparison at threshold + unknown-op fallback; `_load_latest_value` happy/empty/coercion paths via a MagicMock engine that mirrors `engine.connect().__enter__().execute().fetchone()`; `evaluate_psi_signals` with `_load_latest_value` monkeypatched across 6 PSI/VIX combinations (PSI-missing, all-lt-triggered, qqq-only, VIX-at-threshold, VIX-required-but-None, no-mans-land); confidence scaling cap/floor; `build_astrogrid_prediction_payload` required-keys / UUID validity / static fields / signal-field threading / uniqueness; `run_psi_oracle` glue; `_PSI_CONFIGS` structural integrity. Smoke-run: `pytest --noconftest` → 33 passed in 0.26s. Ruff clean.

Behavior locked in by tests worth noting for any future refactor:

- **Both PSI and VIX gates are strict (`<` / `>`)** — value equal to threshold does NOT trigger. Pinned by `test_check_psi_condition_lt_and_gt` (5.25 fails lt 5.25; 2.0 fails gt 2.0) and `test_evaluate_vix_at_threshold_does_not_trigger` (VIX=22.0 skips `vix_lt22` config). A future "use <=" flip should update these tests in the same PR.
- **`_check_psi_condition` silently returns False on unknown op** (no exception). Pinned by `test_check_psi_condition_unknown_op_returns_false`. If a future PR adds an `eq`/`ne`/`gte` op, update the parametrise and add an explicit raise if you want strict validation.
- **Confidence formula is `min(0.95, max(0.3, (sharpe-1)/3 + 0.3))`** — cap 0.95 above Sharpe ≈ 2.95; floor 0.3 below Sharpe = 1.0. Pinned by `test_confidence_scaling_matches_formula` (parametrised on 4 Sharpe values).
- **`_PSI_CONFIGS` integrity test rejects duplicate names** — adding a new config with a name collision will fail `test_psi_configs_names_are_unique`. Adding a new `psi_op` (e.g. `lte`) or `direction` (e.g. `flat`) without updating `_check_psi_condition` AND the dataclass-field set will fail `test_psi_configs_have_required_keys`. Update both in the same PR.
- **`build_astrogrid_prediction_payload` is non-deterministic** (uses `uuid4()` and `datetime.now(timezone.utc)`). Tests assert UUID validity, not equality between calls. Pinned by `test_payloads_have_unique_prediction_ids`.
