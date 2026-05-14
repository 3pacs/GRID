## 2026-05-14 03:25 UTC — 2026-05-14-0306
**Why this matters next run:** PR #162 closes [P1] item 7 (`oracle/claim_verifier.py` DB-evidence verdict tests). With items 6/7/8/13/14 done, the **remaining test-gap item is [P1] item 4** (`oracle/firewall.py::verify_output` end-to-end pipeline) — but **wait until [P1] item 5 lands** (`oracle/publisher_gate.py::gate_decision`, PR #157 still open on this file). The firewall composes claim_extractor → claim_verifier → sanity_checker → gate_decision → audit write, so testing it before #157 merges risks re-doing the gate-decision mocking from scratch. After #157 lands, item 5 (gate_decision unit tests, ~120 LOC) becomes the safer next pick — then item 4 (firewall composition, ~150 LOC).

PR #162 adds 34 test cases (380 LOC, tests-only) covering every branch of `oracle/claim_verifier.py`: each candidate-name fall-through (`<ticker>` → `<ticker>_full` → `<ticker>_usd_full`) for `_lookup_latest_value` + `_lookup_price_change`; NULL-value filter, engine-exception → None-triple/pair fallthrough; `_verify_price` insufficient/supported/contradicted at the 5% boundary; `_verify_percentage` insufficient/supported/contradicted at the 3pp boundary; `_verify_direction` both-up / both-down / mismatch; `_verify_generic` ambiguous fallback; `verify_claims` dispatcher (known-type routing, unknown-type → generic, order/length preservation). Reuses the PR #161 `MagicMock(engine.connect().__enter__().execute())` chain, with separate `side_effect` builders for `.fetchone()` (single-row lookups) vs `.fetchall()` (price-change window). Smoke-run: `pytest --noconftest tests/test_claim_verifier.py` → 34 passed in 0.33s. Ruff clean.

Behavior locked in by tests worth noting for any future refactor:

- **`actual=0.0` short-circuits to `verdict="supported"`** — `_lookup_latest_value`'s guard is `row[0] is not None`, not truthy. A stored 0.0 passes through; `_verify_price`'s `if actual != 0 else 0` ternary then sets `pct_diff=0`, satisfying `0 <= 5.0`. Pinned by `test_verify_price_zero_actual_short_circuits_to_supported`. A future "0.0 means missing data" guard MUST update this test in the same PR.
- **Tolerances are inclusive `<=`** — claim 105 vs actual 100 (exactly 5% diff) is **supported**, not contradicted; claim 8% vs actual 5% (exactly 3pp diff) is also supported. Pinned by `test_verify_price_boundary_at_5_percent_is_supported` and `test_verify_percentage_boundary_at_3pp_is_supported`. A future tightening to strict `<` must update both.
- **`_lookup_price_change` returns `rows[0]` vs `rows[-1]`** — i.e. latest vs OLDEST in the DESC window, not latest vs second-newest. With `periods=2` (the default for `_verify_percentage`/`_verify_direction`) this is latest-vs-previous, but if a future caller passes `periods=5` the "previous" silently becomes 5 days ago. Pinned by `test_lookup_price_change_returns_latest_and_oldest_when_enough_rows`. If you ever extend the window, consider renaming or adding a separate `_lookup_price_window` helper instead.
- **`_verify_generic` is the catch-all for any unknown `claim_type`** — adding a new `claim_type` to `oracle/claim_extractor.ClaimType` without adding an entry to `_VERIFIERS` will silently route to `_verify_generic` → "ambiguous". Pinned by `test_verify_claims_unknown_type_falls_through_to_generic`.

**Remaining unclaimed punch-list items (smallest-first ordering):**

- **[P1] item 5**: `oracle/publisher_gate.py::gate_decision` (line 42) — **PR #157 still open on this file**. Wait for #157 to merge, then write ~120 LOC of tests for the auto-publish (>0.85 confidence), reject (contradicted/critical-fail), and review (>30% flagged) branches. Next safe pick once #157 lands.
- **[P1] item 4**: `oracle/firewall.py::verify_output` — end-to-end pipeline (claim_extractor → claim_verifier → sanity_checker → gate_decision → audit write). **Best done LAST** in the test-gap series — after both #157 and the item-5 PR land, so the mocking shape can be copied wholesale.
- **[P1] item 3**: `CalibrationReport` dataclass rename across `oracle/calibration.py:34` and `inference/calibration.py:57`. Still untouched. Real next-architectural-decision PR target — needs operator input on which name wins. Run `grep -rn 'CalibrationReport' --include='*.py'` to inventory callers before renaming.
- **[P1] item 9**: Signal positional-arg mismatch in `_gather_signals_from_registry` (`oracle/engine.py:812`). Real bug — z-score stored in `value` field, `z_score=0` → registry-sourced signals contribute zero downstream at line 1371. **Still blocked on PR #156 merge** (file claim on `oracle/engine.py`).
- **[P2] items 10-12**: Splits/refactors of `oracle/engine.py`. Architectural; coordinate with operator.

**File claims to avoid this cycle (last-touched in still-open agent PRs):**
- `tests/test_claim_verifier.py` (#162 — this PR)
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
- `python3 -m pytest` fails at conftest collection (pandas + psycopg2 + python-jose imported at module top). Use `pytest tests/test_X.py --noconftest` for pure-function test files. Needs `pip install pytest loguru sqlalchemy` (sqlalchemy only when the SUT imports it — `claim_verifier.py` does, via `from sqlalchemy.engine import Engine` type hint and `sql_text`). `ruff check` and `py_compile` work after `pip install ruff`.
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
