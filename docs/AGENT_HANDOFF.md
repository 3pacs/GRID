## 2026-05-14 02:14 UTC — 2026-05-14-0210
**Why this matters next run:** PR #161 closes [P2] item 14 (`oracle/psi_model.py` PSI+VIX gating tests). With items 13 & 14 done, the remaining test-gap items are **[P1] item 7** (`oracle/claim_verifier.py` — DB-mock-required, heaviest) and **[P1] item 4** (`oracle/firewall.py::verify_output` — best done LAST after 5/7 land). [P1] item 5 (`oracle/publisher_gate.py`) is still file-claimed by PR #157.

PR #161 adds 33 test cases (385 LOC, tests-only) covering every behavior of `oracle/psi_model.py`: `_check_psi_condition` lt/gt strict-comparison at threshold + unknown-op fallback; `_load_latest_value` happy/empty/coercion paths via a MagicMock engine that mirrors `engine.connect().__enter__().execute().fetchone()`; `evaluate_psi_signals` with `_load_latest_value` monkeypatched across 6 PSI/VIX combinations (PSI-missing, all-lt-triggered, qqq-only, VIX-at-threshold, VIX-required-but-None, no-mans-land); confidence scaling cap/floor; `build_astrogrid_prediction_payload` required-keys / UUID validity / static fields / signal-field threading / uniqueness; `run_psi_oracle` glue; `_PSI_CONFIGS` structural integrity. Smoke-run: `pytest --noconftest` → 33 passed in 0.26s. Ruff clean.

Behavior locked in by tests worth noting for any future refactor:

- **Both PSI and VIX gates are strict (`<` / `>`)** — value equal to threshold does NOT trigger. Pinned by `test_check_psi_condition_lt_and_gt` (5.25 fails lt 5.25; 2.0 fails gt 2.0) and `test_evaluate_vix_at_threshold_does_not_trigger` (VIX=22.0 skips `vix_lt22` config). A future "use <=" flip should update these tests in the same PR.
- **`_check_psi_condition` silently returns False on unknown op** (no exception). Pinned by `test_check_psi_condition_unknown_op_returns_false`. If a future PR adds an `eq`/`ne`/`gte` op, update the parametrise and add an explicit raise if you want strict validation.
- **Confidence formula is `min(0.95, max(0.3, (sharpe-1)/3 + 0.3))`** — cap 0.95 above Sharpe ≈ 2.95; floor 0.3 below Sharpe = 1.0. Pinned by `test_confidence_scaling_matches_formula` (parametrised on 4 Sharpe values).
- **`_PSI_CONFIGS` integrity test rejects duplicate names** — adding a new config with a name collision will fail `test_psi_configs_names_are_unique`. Adding a new `psi_op` (e.g. `lte`) or `direction` (e.g. `flat`) without updating `_check_psi_condition` AND the dataclass-field set will fail `test_psi_configs_have_required_keys`. Update both in the same PR.
- **`build_astrogrid_prediction_payload` is non-deterministic** (uses `uuid4()` and `datetime.now(timezone.utc)`). Tests assert UUID validity, not equality between calls. Pinned by `test_payloads_have_unique_prediction_ids`.

**Remaining unclaimed punch-list items (smallest-first ordering):**

- **[P1] item 7**: `oracle/claim_verifier.py` (DB-evidence verdicts). Needs DB-engine `MagicMock` per #161 pattern (`engine.connect().__enter__().execute()` chain, `side_effect` keyed on `params["name"]`). ~150 LOC. Heaviest remaining test item.
- **[P1] item 4**: `oracle/firewall.py::verify_output` — end-to-end pipeline. Composes claim_extractor → claim_verifier → sanity_checker → gate_decision → audit write. **Best done last** — wait for items 5/7 to land first.
- **[P1] item 5**: `oracle/publisher_gate.py::gate_decision` (line 42) — **PR #157 still open on this file**. Wait for #157 to merge before touching `publisher_gate.py` to avoid file-claim overlap.
- **[P1] item 3**: `CalibrationReport` dataclass rename across `oracle/calibration.py:34` and `inference/calibration.py:57`. **Still untouched through PRs #156-161.** Real next-architectural-decision PR target — needs operator input on which name wins. Run `grep -rn 'CalibrationReport' --include='*.py'` to inventory callers before renaming.
- **[P1] item 9**: Signal positional-arg mismatch in `_gather_signals_from_registry` (`oracle/engine.py:812`). Real bug — z-score stored in `value` field, `z_score=0` → registry-sourced signals contribute zero downstream at line 1371. **Still blocked on PR #156 merge** (file claim on `oracle/engine.py`).
- **[P2] items 10-12**: Splits/refactors of `oracle/engine.py`. Architectural; coordinate with operator.

**File claims to avoid this cycle (last-touched in still-open agent PRs):**
- `tests/test_psi_model.py` (#161 — this PR)
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
- `python3 -m pytest` fails at conftest collection (pandas + psycopg2 + python-jose imported at module top). Use `pytest tests/test_X.py --noconftest` for pure-function test files. Needs `pip install pytest loguru sqlalchemy` (sqlalchemy only if the module imports it — `psi_model.py` does). `ruff check` and `py_compile` work after `pip install ruff`.
- `gh` CLI is not present on this box — use MCP `mcp__github__*` tools for all GitHub operations.
- `mcp__github__list_pull_requests` returns >220KB which exceeds tool-result limits; slice with python via the saved tool-result file, or use `search_pull_requests` with a tighter query.
- `pytest --noconftest` is the cleanest way to bypass the conftest pandas import — `--rootdir=/tmp` and `cd /tmp` are not enough; pytest still walks up to find conftest.py.

---
## 2026-05-14 01:15 UTC — 2026-05-14-0109
**Why this matters next run:** PR #160 closes [P2] item 13 of the auditor punch list (`oracle/citation_extractor.py` alias/family tests). Next-smallest unclaimed punch-list item is **[P2] item 14** (`oracle/psi_model.py::evaluate_psi_signals` PSI+VIX gating thresholds, ~100 LOC, pure-function). After that, the remaining test-gap is [P1] item 7 (`oracle/claim_verifier.py` — heavier, needs DB-engine `MagicMock` per #158 handoff).

PR #160 adds 30 test cases (218 LOC, tests-only) covering every behavior in `oracle/citation_extractor.py`: empty/boundary inputs, exact + underscore-spaced name match (case-insensitive), alias match across all 17 `_ALIASES` keys via representative samples, family-level match (gated on truthy `feature_families`), dedup + sorted-output, and both `compute_citation_ratio` edges. Pure-function tests; smoke-ran via `python3 -c '...'` outside conftest (only `loguru` needed to be pip-installed; the module has no pandas/sqlalchemy/psycopg2 imports).

Behavior locked in by tests worth noting for any future refactor:

- **Alias matching is substring with no word boundary.** `"vixflavor of the day"` cites `vix` because alias `"vix"` is a substring of the text. Pinned by `test_alias_uses_substring_not_word_boundary`. A future tightening (e.g. `\bvix\b`) should update this test in the same PR — the auditor flagged this risk class as "alias changes could silently mis-tag citations".
- **Family branch is gated on truthy `feature_families`.** Both `None` (default) and `{}` skip the branch entirely. Pinned by two tests so a future "feature_families is not None" guard doesn't accidentally trigger the branch on an empty dict.
- **`"gold"` is in `_ALIASES["gld"]` but NOT in `_FAMILY_ALIASES["commodity"]`.** Family-level commodity match requires `"commodities"` / `"oil"` / `"crude"` / `"metals"` in text. The combined test (`test_combined_exact_alias_and_family`) uses `"commodities"` to exercise the family branch; using `"gold"` cites only `gld` via the alias path.
- **Output is `sorted(set(cited))`, not insertion-order.** Pinned by `test_multiple_features_returns_sorted_list`.

**Remaining unclaimed punch-list items (smallest-first ordering):**

- **[P2] item 14**: `oracle/psi_model.py::evaluate_psi_signals` (line 137) — hardcoded Sharpe-2.59 GLD config and Sharpe-2.01 QQQ config thresholds. Pure-function entry point used by `scripts/run_psi_oracle.py:26`. ~100 LOC of tests. **Best next pick** (same shape as #158/#159/#160).
- **[P1] item 7**: `oracle/claim_verifier.py` (DB-evidence verdicts) — needs DB-engine mocking via `sqlalchemy` `MagicMock`. Pattern: assemble a `MagicMock` whose `.connect().__enter__().execute()` returns parametrised rows; do NOT rely on `tests/conftest.py::mock_engine` (conftest is unimportable on this box due to pandas/psycopg2). ~150 LOC. Heaviest remaining test item.
- **[P1] item 4**: `oracle/firewall.py::verify_output` — end-to-end pipeline. Composes claim_extractor → claim_verifier → sanity_checker → gate_decision → audit write. **Best done last** — wait for items 5/7 to land first.
- **[P1] item 5**: `oracle/publisher_gate.py::gate_decision` (line 42) — **PR #157 still open on this file**. Wait for #157 to merge before touching `publisher_gate.py` to avoid file-claim overlap.
- **[P1] item 3**: `CalibrationReport` dataclass rename across `oracle/calibration.py:34` and `inference/calibration.py:57`. **Still untouched through PRs #156/#157/#158/#159/#160.** Real next-architectural-decision PR target — needs operator input on which name wins. Run `grep -rn 'CalibrationReport' --include='*.py'` to inventory callers before renaming.
- **[P1] item 9**: Signal positional-arg mismatch in `_gather_signals_from_registry` (`oracle/engine.py:812`). Real bug — z-score stored in `value` field, `z_score=0` → registry-sourced signals contribute zero downstream at line 1371. **Still blocked on PR #156 merge** (file claim on `oracle/engine.py`).
- **[P2] items 10-12**: Splits/refactors of `oracle/engine.py`. Architectural; coordinate with operator.

**File claims to avoid this cycle (last-touched in still-open agent PRs):**
- `tests/test_citation_extractor.py` (#160 — this PR)
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
- `python3 -m pytest` can't collect (conftest pulls pandas + psycopg2 + python-jose at module top). For pure-function tests, `python3 -c '...'` smoke scripts that bypass conftest work fine. `ruff check` and `py_compile` work after `pip install ruff loguru pytest`.
- `gh` CLI is not present on this box — use MCP `mcp__github__*` tools for all GitHub operations.
- `mcp__github__list_pull_requests` returns >220KB which exceeds tool-result limits; slice with python via the saved tool-result file (see this run's invocation) or use `search_pull_requests` with a tighter query.

---
## 2026-05-14 00:22 UTC — 2026-05-14-0016
**Why this matters next run:** PR #159 closes [P1] item 8 of the auditor punch list (`oracle/sanity_checker.py` deterministic checks). 3 sibling test-gap items remain — see ordering below.

PR #159 adds 41 test cases (367 LOC, tests-only) covering every check in `oracle/sanity_checker.py`: price_range, pct_math, direction_consistency, date_sanity, unit_sanity, cross_claim, plus the `run_sanity_checks` composer (input order, critical_fail flag, warn-vs-fail handling). Pure-function tests; smoke-ran via `python3 -c '...'` outside conftest (numpy/pandas/psycopg2 not installed, loguru + sqlalchemy were pip-installed for the smoke run only).

Behavior locked in by tests worth noting for any future refactor:

- **Direction `verdict="ambiguous"` is currently un-asserted**: `_check_direction_consistency` returns `pass` for everything except `"contradicted"`. Tested with `supported` and `insufficient`; `ambiguous` (the 4th `Verdict` literal) isn't covered because the current code-shape doesn't branch on it — a refactor that adds an `ambiguous` branch should update the test in the same PR.
- **`_check_pct_math` tolerance is ±3 percentage-points**, not ±3% relative. Test `test_pct_math_tolerates_within_three_points` pins this (claimed 12% vs actual 10% → diff 2.0 ≤ 3.0 → pass). If the tolerance is ever tightened, this test will fail loudly.
- **Cross-claim warn does NOT mark `critical_fail`** — only `fail` does. Test `test_run_sanity_checks_warn_does_not_mark_critical` pins this. Important if the gate ever wants warns to gate publishing.

**Remaining test-gap items from the punch list (smallest-first ordering):**
- **[P1] item 7**: `oracle/claim_verifier.py` (DB-evidence verdicts) — needs DB-engine mocking via `sqlalchemy` `MagicMock` (use the `mock_engine` pattern from `tests/conftest.py` if present, else assemble a `MagicMock` whose `.connect().__enter__().execute()` returns parametrised rows). ~150 LOC of tests. This is the heaviest remaining item.
- **[P1] item 4**: `oracle/firewall.py::verify_output` — the end-to-end pipeline. Composes claim_extractor → claim_verifier → sanity_checker → gate_decision → audit write. **Best done last** — once items 5/7 land, this one wires them together.
- **[P1] item 5**: `oracle/publisher_gate.py::gate_decision` (line 42) — **PR #157 collapse-duplicate is still open**; wait for it to merge before touching `publisher_gate.py` to avoid file-claim overlap.
- **[P2] item 13**: `oracle/citation_extractor.py` — alias/family normalization. Pure-function, no DB. Same shape as #158 + #159 (regex-style + small composer). Best warm-up if claim_verifier feels heavy.
- **[P2] item 14**: `oracle/psi_model.py::evaluate_psi_signals` (line 137) — hardcoded Sharpe-2.59 GLD / Sharpe-2.01 QQQ thresholds. ~100 LOC.

**[P1] item 3** (`CalibrationReport` dataclass rename across `oracle/calibration.py:34` and `inference/calibration.py:57`) is **still untouched** through PRs #156/#157/#158/#159. Real next-architectural-decision PR target — needs operator input on which name wins. Run `grep -rn 'CalibrationReport' --include='*.py'` to inventory callers before renaming.

**File claims to avoid this cycle (last-touched in still-open agent PRs):**
- `tests/test_sanity_checker.py` (#159 — this PR)
- `tests/test_claim_extractor.py` (#158)
- `oracle/publisher_gate.py` + `tests/test_publish_astrogrid_canonical.py` (#157)
- `oracle/engine.py` (#156)
- `intelligence/hypothesis_engine.py` + `tests/test_hypothesis_engine_intelligence_kills_logging.py` (#155)
- `ingestion/altdata/indeed_hiring_puller.py` + `tests/test_indeed_hiring_puller.py` (#154)
- `ingestion/altdata/redfin_puller.py` + `tests/test_redfin_puller.py` (#153)
- `intelligence/universe_ranker.py` + `tests/test_universe_ranker.py` (#152)

**Env quirks (carried forward, unchanged):**
- `git push origin routine-bookkeeping` returns HTTP 403. Use MCP `create_or_update_file` for both `.grid_backups/routine_log.jsonl` and this file. Work-branch pushes work fine.
- `python3 -m pytest` can't collect (conftest pulls pandas + psycopg2 + python-jose at module top). For pure-function tests, `python3 -c '...'` smoke scripts that bypass conftest work fine (this PR's tests were validated this way — `pip install loguru sqlalchemy` was needed; pandas/psycopg2 were not since sanity_checker has no DB calls). `ruff check` and `py_compile` work without deps.
- `gh` CLI is not present on this box — use MCP `mcp__github__*` tools for all GitHub operations.

---
## 2026-05-13 23:13 UTC — 2026-05-13-2308
**Why this matters next run:** PR #158 closes [P1] item 6 of the auditor punch list (`oracle/claim_extractor.py` regex tests). 4 sibling test-gap items remain — pick the next-smallest module first.

PR #158 adds 29 test cases (229 LOC, tests-only) covering price/percentage/direction/date extraction in `oracle/claim_extractor.py`. Locked-in behavior worth noting:

- **`_nearest_ticker` is misnamed** — it returns the **first** known ticker found by `_TICKER_MENTION_RE.finditer(window)` inside the ±80-char window, not the strictly-closest. In a multi-ticker paragraph the first ticker wins for every later claim. The auditor flagged this risk verbatim ("regex changes could silently drop or mis-tag claims") but did NOT mark it [P0]/[P1] for a behavior fix — my tests pin the current behavior. The `test_mixed_claims_in_one_paragraph` test was deliberately scoped to a single ticker. **If a future PR fixes `_nearest_ticker` to actually find the closest match, update `tests/test_claim_extractor.py` in the same PR — don't fight the existing tests.**

**Remaining test-gap items from the punch list (smallest-first ordering):**
- **[P1] item 7**: `oracle/claim_verifier.py` (DB-evidence verdicts) — small module, but needs DB-engine mocking via `sqlalchemy` `MagicMock` (use the `mock_engine` pattern from `tests/conftest.py`). ~150 LOC of tests.
- **[P1] item 8**: `oracle/sanity_checker.py::run_sanity_checks` (line 260) — deterministic checks (price-range, pct-math, direction-consistency, date, unit, cross-claim). Pure-function, no DB. Similar shape to today's PR but more branches. ~200 LOC.
- **[P1] item 4**: `oracle/firewall.py::verify_output` — the end-to-end pipeline. Composes claim_extractor → claim_verifier → sanity_checker → gate_decision → audit write. **Best done last** — once items 5/7/8 land, this one wires them together.
- **[P1] item 5**: `oracle/publisher_gate.py::gate_decision` (line 42) — but **PR #157 is still open on this file**. Wait for #157 to merge before touching.
- **[P2] item 13**: `oracle/citation_extractor.py` — alias/family normalization. Pure-function, no DB. Good warm-up if items 7/8 feel heavy.
- **[P2] item 14**: `oracle/psi_model.py::evaluate_psi_signals` (line 137) — hardcoded Sharpe-2.59 GLD / Sharpe-2.01 QQQ thresholds. ~100 LOC.

**[P1] item 3** (`CalibrationReport` dataclass rename across `oracle/calibration.py:34` and `inference/calibration.py:57`) is **still untouched** — neither PR #156/#157/#158 touched either file. Real next-architectural-decision PR target. Run `grep -rn 'CalibrationReport' --include='*.py'` to inventory callers before renaming.

**File claims to avoid this cycle (last-touched in still-open agent PRs):**
- `tests/test_claim_extractor.py` (#158 — this PR)
- `oracle/publisher_gate.py` + `tests/test_publish_astrogrid_canonical.py` (#157)
- `oracle/engine.py` (#156)
- `intelligence/hypothesis_engine.py` + `tests/test_hypothesis_engine_intelligence_kills_logging.py` (#155)
- `ingestion/altdata/indeed_hiring_puller.py` + `tests/test_indeed_hiring_puller.py` (#154)
- `ingestion/altdata/redfin_puller.py` + `tests/test_redfin_puller.py` (#153)
- `intelligence/universe_ranker.py` + `tests/test_universe_ranker.py` (#152)

**Env quirks (carried forward, unchanged):**
- `git push origin routine-bookkeeping` returns HTTP 403. Use MCP `create_or_update_file` for both `.grid_backups/routine_log.jsonl` and this file. Work-branch pushes work fine.
- `python3 -m pytest` can't collect (conftest pulls pandas + psycopg2 + python-jose at module top). For pure-function tests, `python3 -c '...'` smoke scripts that bypass conftest work fine (this PR's tests were validated this way). `ruff check` and `py_compile` work without deps.
- `gh` CLI is not present on this box — use MCP `mcp__github__*` tools for all GitHub operations.
