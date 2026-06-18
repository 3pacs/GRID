## 2026-06-18 23:10 UTC — 2026-06-18-2304
**Why this matters next run:** Two items from the 2026-06-15 handoff "Remaining unworked alpha_research items from PR #299" list are now shipped — strike them off before re-picking:
- **[P1] split_adjuster.py tests** → shipped as **PR #339** (`tests/test_split_adjuster.py`, 31 cases, 393 LOC, ruff clean, 0.48s).
- **[P2] heartbeat.py `except: pass` → log.warning** → already shipped yesterday as **PR #338** (the 2026-06-15 handoff was written before that pick landed).

Next pickable from alpha_research is **[P1] conviction_scorer.py 7 layer scorer tests** or the larger **[P1] conviction_scorer.py PIT plumbing** (caveat: changes public signatures wired through `api/routers/signals.py:194,226` — needs default `as_of_date=date.today()` to stay backward-compat). The physics/ PR #305 list from 2026-06-15 is fully untouched and the top item (`physics/verify.py:354` N+1 batch fix) is the highest-leverage single-PR pick on the queue.

One small test-data gotcha worth recording: when fabricating pre-split price history for `split_adjuster` tests, the older prices must NOT drop >40% relative to the next bar with a ratio ≥ 2 to the next bar, or `detect_splits` will catch a phantom 3rd split. Run `detect_splits(prices)` as a sanity assertion before any compounding assertion (the TSLA test in PR #339 does this).

## 2026-06-15 23:35 UTC — 2026-06-15-2305
**Why this matters next run:** A second auditor-feed PR (#305, physics/, opened 2026-06-14) is now on the queue alongside still-open PR #299 (alpha_research/). Same posture as #299: the PR only modifies `docs/PUNCH-LIST-2026-05-13.md`, so picking from its 11 underlying findings directly is unblocked — verify via `mcp__github__pull_request_read get_files` (this run did exactly that).

- Shipped the **compute_credit_cycle / compute_vix_exposure_scalar tests** named in the 2026-06-11 handoff as **PR #306** (`tests/test_alpha_research_signals.py`, 17 tests, ruff clean, 0.14s). Strike that item from the pickable list below.
- **NEW backlog source: PR #305 (auditor-feed physics/, 2026-06-14).** 11 items total. In rough leverage order:
   - **[P1]** `physics/verify.py:354` — replace the N+1 in `check_dimensional_consistency` (one `engine.connect()` + JOIN per feature, ~2.4k roundtrips per `verify_all`) with one CTE returning name→latest_value for all `model_eligible` features. High value (live-path perf fix). Caveat: must preserve the `_get_latest_value(name, ...)` signature for the few callers that still call it individually.
   - **[P1]** `physics/transforms.py:627` vs `analysis/transfer_entropy.py:135` — `transfer_entropy` exists in both with divergent bodies; only the `analysis/` copy has callers. Two clean options: delete the `physics/` body, OR re-export from `analysis.transfer_entropy`. Mechanical, ~10 LOC.
   - **[P1]** Tests for `physics/transforms.py` energy + OU helpers (`kinetic_energy`, `potential_energy`, `total_energy`, `market_temperature`, `estimate_ou_parameters`, `hurst_exponent` — the 6 actually wired into prod via `features/lab.py:374-412` + `api/routers/physics.py:140-293`). Pattern: reference-value tests on deterministic series.
   - **[P1]** Tests for `physics/conventions.py:197` validators (`validate_convention`, `validate_feature_set`, `check_unit_compatibility`). Called per-feature from `physics/verify.py:381` — uncovered range/heuristic branches.
   - **[P1]** Tests for `physics/waves.py:41` `build_execution_waves` — topo-sort used by `cli.py:312` + `api/routers/workflows.py:124`. Cover cycle detection, isolated-task wave assignment, `WaveTask` defaults.
   - **[P1]** Direct tests for `physics/dealer_gamma.py:118` `DealerGammaEngine.compute_gex_profile`. Today it's only mocked with a pre-computed dict — actual per-strike GEX aggregation, `_find_gamma_flip` interpolation, gamma/put/call wall selection have zero direct coverage.
   - **[P1]** Tests for `physics/verify.py:322` `check_dimensional_consistency` / `check_regime_boundaries` / `check_stationarity`. `verify_all` swallows their exceptions at line 96-108 so silent regressions don't surface in CI.
   - **[P2]** Delete 9 unused public functions from `physics/transforms.py` (`entropy_rate`/`phase_velocity`/`ou_mean_reversion_signal`/`ou_displacement`/`langevin_drift`/`langevin_diffusion`/`fokker_planck_density`/`relaxation_time`/`rolling_hurst`). Caveat: rerun `grep -rn` first (auditor noted string-import edge case).
   - **[P2]** Delete `execute_waves` + `build_grid_pipeline_waves` from `physics/waves.py:107,227` (~145 LOC + unused `concurrent.futures` import).
   - **[P2]** Split `physics/news_energy.py:320` `NewsEnergyEngine.analyze` (185 LOC).
   - **[P2]** Split `physics/verify.py:404` `check_regime_boundaries` (126 LOC).
- **Remaining unworked alpha_research items from PR #299** (one struck off this run):
   - **[P1]** `alpha_research/conviction_scorer.py:54` — add `as_of_date` + `release_date <= :as_of` PIT plumbing. Public-signature change → API-router-aware.
   - **[P1]** Add unit tests for `conviction_scorer.py` 7 layer scorers + `score_ticker` / `scan_all`.
   - **[P1]** Add unit tests for `signal_adapter.py` publish functions.
   - **[P1]** Add unit tests for `rotation_variant_backtest.backtest_rotation_variant`.
   - **[P1]** Add unit tests for `data/split_adjuster.py` (TSLA 5:1→3:1 compounding case).
   - **[P2]** Delete dead module `alpha_research/data/shares_tracker.py` (183 LOC).
   - **[P2]** Delete dead helpers `compute_vix_exposure_series` + `build_returns_panel`.
   - **[P2]** `alpha_research/heartbeat.py:111,137` — `except: pass` → `log.warning(...)`.
   - **[P2]** `alpha_research/strategies/rotation_variant_backtest.py:153` — log skipped rebalances.
   - **[P2]** `alpha_research/adapters/signal_adapter.py:174` — dangling `docs/TODO-REGIME-SIGNAL-USAGE.md` reference.
- **TIER 0 status:** `main` CI **GREEN** on HEAD `e5a0eafe` (GRID Tests success). Deploy-to-Grid-Server still red, operator-side.
- **TIER 1 status:** zero codex-authored PRs on the open list. All 30 open PRs are 3pacs (24) or dependabot (6). Don't churn on dependabot PRs.
- **Env note:** Fresh container needs `pip install pytest pandas sqlalchemy numpy ruff` (~10s). The new test file (`tests/test_alpha_research_signals.py`) imports `alpha_research.signals.*` directly — does NOT require the heavy `tests/conftest.py` chain.
- **`mcp__github__list_pull_requests` and `mcp__github__actions_list` still truncate** at 167k / 346k chars. Save-to-file + python slice / json.load is the canonical workaround.

## 2026-06-11 23:25 UTC — 2026-06-11-2305
**Why this matters next run:** the "don't reach across PR #299"
guidance from the 2026-06-09 handoff was overly broad. PR #299 only
modifies `docs/PUNCH-LIST-2026-05-13.md` — fixing the underlying
code-bugs it documents is **independent and unblocked**. This run
shipped the P0 (synthetic-random VIX in `adaptive_rotation.py:321`) as
**PR #302** without conflicting with PR #299.

- **Pickable alpha_research findings still on the table from PR #299** —
  all reference code already on `main`, all small enough for a single
  routine PR. In rough order of leverage:
  - **[P1]** `alpha_research/conviction_scorer.py:54` — add `as_of_date`
    + `release_date <= :as_of` to `_load_latest`, `_load_raw_latest`,
    `_load_raw_series`, `_load_price`. PIT-fix prerequisite to using
    the scorer in any backtest. **Caveat: changes the public signatures
    of `score_ticker` / `scan_all`, which are wired through
    `api/routers/signals.py:194,226` — needs a default `as_of_date=date.today()`
    to stay backward-compatible, otherwise the API breaks.**
  - **[P1]** `alpha_research/validation/gauntlet.py:64,136` — seed
    `np.random.default_rng(seed)` for `permutation_test` and
    `subsample_stability`. Thread `seed: int = 42` through
    `run_gauntlet`. Same determinism class as this PR.
  - **[P1]** Add unit tests for `conviction_scorer.py` 7 layer scorers
    (`score_setup`/`score_company`/`score_smart_money`/`score_crowd`/
    `score_narrative`/`score_flow`/`score_confirmation`/`score_ticker`/
    `scan_all`). Mock `conn.execute(...).fetchone/fetchall`. Same
    pattern as the oracle test PRs (#158/#159/#160).
  - **[P1]** Add unit tests for `signal_adapter.py` publish functions
    (`publish_factor_signals`/`publish_regime_signal`/
    `publish_all_alpha_signals`). The bug-fix postmortem at line 26-30
    is the regression target.
  - **[P1]** Add unit tests for
    `rotation_variant_backtest.backtest_rotation_variant`. Writes to
    `astrogrid.backtest_run`/`backtest_result`; lock the metric-payload
    schema.
  - **[P1]** Add unit tests for `credit_cycle.compute_credit_cycle` and
    `exposure_scaler.compute_vix_exposure_scalar`. Both drive
    `oracle/engine.py::_get_credit_cycle_routing` weights.
  - **[P1]** Add unit tests for `data/split_adjuster.py` —
    `detect_splits`/`adjust_splits`/`adjust_panel`/
    `compute_real_drawdown`. TSLA 5:1→3:1 compounding case is the
    docstring's named target.
  - **[P2]** Delete dead module `alpha_research/data/shares_tracker.py`
    (183 LOC, zero callers). Verify with `grep -r` first.
  - **[P2]** Delete dead helpers `compute_vix_exposure_series` and
    `build_returns_panel` — but **NOTE this PR's fix uses
    `panel_builder.get_vix_series` not `compute_vix_exposure_series`,
    so the auditor's "zero callers" claim for the latter still holds**.
    `get_vix_series` is now actively called by `adaptive_rotation.run_rotation`.
  - **[P2]** `alpha_research/heartbeat.py:111,137` — convert
    `except: pass` to `log.warning(...)` per CLAUDE.md log-level rule.
  - **[P2]** `alpha_research/strategies/rotation_variant_backtest.py:153`
    — log skipped rebalances. One-liner.
  - **[P2]** `alpha_research/adapters/signal_adapter.py:174` — fix or
    delete the dangling `docs/TODO-REGIME-SIGNAL-USAGE.md` reference.
- **`alpha_research/strategies/adaptive_rotation.py` is now file-blocked
  by PR #302.** `tests/test_adaptive_rotation.py` likewise. The 11 items
  above don't touch either file, so unblocked.
- **TIER 0 status:** `main` CI is **GREEN** on HEAD `0a41753a` (GRID
  Tests success). The 13-day red streak from the 2026-06-08 handoff is
  resolved — PR #291 must have landed. Deploy-to-Grid-Server still
  fails but that's operator-side (no SSH).
- **Env note:** `pip install pytest pandas numpy sqlalchemy loguru ruff`
  + the file uses no DB connection (engine is mocked via monkeypatch).
  15 tests in 0.65s. No cryptography/passlib needed.
- **`mcp__github__list_pull_requests` and `mcp__github__actions_list`
  still truncate** at 167k / 346k chars. Save-to-file + python slice or
  json.load is the workaround.

## 2026-06-10 23:08 UTC — 2026-06-10-2307
**Why this matters next run:** queue was bone-dry, logged no-work. See
the 2026-06-11 entry above — the "don't reach across PR #299" guard
that informed this no-work decision was overly broad. Future runs
should distinguish "fixing the code PR #299 documents" (always
unblocked) from "adding more items to the queue from PR #299" (blocked
until #299 lands).

## 2026-06-09 23:15 UTC — 2026-06-09-2303
**Why this matters next run:** the `oracle/` section of
`docs/PUNCH-LIST-2026-05-13.md` is now annotated with `[x]` on the 11
items that were already shipped. Don't re-investigate them. This was a
pure-docs PR (PR #301, single-file markdown change, ~22 LOC).

- **PUNCH-LIST is now structurally exhausted for the routine flow**
  (matches the 2026-06-08 entry's prediction). Oracle/ section has only
  two unresolved `[P2]` engine-split items left (`oracle/engine.py`
  2,877 LOC; `OracleEngine.predict` 405 lines;
  `OracleEngine._oracle_one_ticker` 299 lines) — all too large for a
  single 25-min routine PR. API/ section has the 4 same-size refactor
  items (`canvas_expand.expand_node` 737 LOC, `intel.intel_briefing`
  509 LOC, `intelligence_risk._build_risk_map` 448 LOC,
  `flows._build_sector_connections` 464 LOC) — also too big. The one
  P2 small item (`f-string SQL in prediction_backtest.dataset_stats`)
  is file-blocked by open PR #275.
- **The next free queue is PR #299 (alpha_research auditor feed)
  once it merges.** It appends 14 items to `docs/PUNCH-LIST-2026-05-13.md`
  including one `[P0]` (synthetic-random VIX history feeding live
  regime detection — `alpha_research/strategies/adaptive_rotation.py:321`)
  and 7 `[P1]`s. Most look routine-budget-sized. Don't reach across
  the PR to pick from items not yet on main.
- **TIER 0 status (carried for the 13th day):** `main` CI still red
  unless something has changed since 2026-06-08. PR #291 is still the
  one fix; do NOT open a duplicate.
- **File-block list (no change this run):** see the 2026-06-08 entry
  for the full list. This PR only touched `docs/PUNCH-LIST-2026-05-13.md`,
  which is not in any other PR's diff.
- **Env note:** no installs needed (docs-only PR, no tests run).
  Verified each resolution claim by spot-grep + `wc -l` on the
  corresponding test file. The `mcp__github__list_pull_requests`
  truncation workaround (save-to-file + python slice) is still
  required as of this run.

## 2026-06-08 23:15 UTC — 2026-06-08-2305
**Why this matters next run:** Both **PR #276 HIGH findings #2 and #3 are now DONE** in **PR #300** (10-day-old review queue item shipped). **Stop suggesting them.** With this, every concrete clean-pick named in the 2026-06-04/05 handoffs is closed. The routine queue is now genuinely bone-dry — no PUNCH-LIST P1 items remain in budget, the PR #276 review backlog is cleared, and Tier 0 (PR #291) has now been unmerged for **12 days**. Next run will most likely be a no-work entry unless something new lands in PUNCH-LIST, codex authors a new PR, or main CI breaks again. Don't fabricate work.

- Shipped PR #276 HIGH #2 (no try/except on `/export.xlsx` GET) + HIGH #3 (raw `str(exc)` leak on `/weekly` GET) as **PR #300**. Single file `api/routers/ten_year_portfolio.py` + new test file `tests/test_ten_year_portfolio_error_handling.py` (4 cases, all pass, ruff clean). Created the test file fresh rather than extending `tests/test_ten_year_portfolio.py` because that file is **still file-blocked by open PR #291**.
- **TIER 0 status (carried for the 12th day):** `main` CI still red. **PR #291** fixes it; its own CI is green. **Operator-merge blocker, not code.** Do NOT open a duplicate fix. This is the single biggest operational drag on the routine — every day it sits unmerged, file `tests/test_ten_year_portfolio.py` stays blocked, which forces any router-error-path test against ten_year_portfolio.py into a separate file (as this run did).
- **Files still file-blocked by open PRs** (use line-range check per 2026-06-05 handoff correction, not blanket file-block):
   - `api/routers/intel.py::intel_predictions_active` (#256)
   - `api/routers/intel.py::intel_search` (#297, this-week's run)
   - `api/routers/oracle.py` (#240)
   - `api/routers/intelligence_search.py` (#239)
   - `api/routers/models.py` (#255)
   - `api/routers/prediction_backtest.py` (#275)
   - `api/dependencies.py` + `db.py` (#277)
   - `tests/test_ten_year_portfolio.py` (#291)
   - `tests/test_system_router.py` (#265)
   - `api/routers/system.py` + `api/schemas/system.py` + `tests/test_freshness_stale_sources.py` (#293)
   - `requirements.txt` + `tests/test_requirements.py` (#294)
   - **NEW this run:** `api/routers/ten_year_portfolio.py::weekly_ten_year_portfolio` + `::export_current_model_workbook` + `tests/test_ten_year_portfolio_error_handling.py` (#300)
- **TIER 1 status:** Zero unreviewed codex PRs. The 4 codex/* branches with open PRs are #233 (reviewed 2026-05-25), #276 (reviewed 2026-05-29, the HIGHs are now ALL shipped: #294 + #300), and #242/#243 (intentional parked drafts — leave alone).
- **Remaining unclaimed PUNCH-LIST items** — all P2 refactors >400 LOC, exceed routine budget. **The 2026-05-13 punch list is structurally exhausted for the routine flow.**
   - line 48: `canvas_expand.expand_node` (737 LOC) — `api/routers/canvas_expand.py`
   - line 49: `intel.intel_briefing` (509 LOC) — `api/routers/intel.py::intel_briefing` at ~line 1650, NOT overlapping #256 or #297
   - line 50: `intelligence_risk._build_risk_map` (448 LOC) — `api/routers/intelligence_risk.py`
   - line 51: `flows._build_sector_connections` (464 LOC) — `api/routers/flows.py`
- **Speculative next picks if the queue stays dry (in priority order):**
   1. **`docs/PUNCH-LIST-2026-05-13.md` staleness sweep, lines 28–41 / 47.** The 2026-06-02 entry verified lines 19/20/21/22–26/27 as STALE. Lines 28–32 are oracle P2 splits that mostly predate the engine.py de-dup work in PR #156 — high chance they're stale. Line 47 (f-string SQL in `prediction_backtest.py:116`) — `api/routers/prediction_backtest.py` is still file-blocked by #275, so just verify on disk and either annotate or skip. Pure doc PR, low risk, advances no code.
   2. **`tests/test_requirements.py` extension** — verify whether `xgboost` / `kafka-python-ng` / `prefect` / `minio` get reached via a top-level import like `openpyxl` did. If yes, add a regression guard. **`tests/test_requirements.py` is file-blocked by #294** — would have to wait for #294 to merge or create a sibling test file.
   3. **`docs/PUNCH-LIST` refresh PR.** The 2026-05-13 punch list is structurally exhausted for routine-budget items. The auditor produces these; ping the operator to re-run the api/ auditor and produce a new list of small-budget items.
- **`mcp__github__list_pull_requests` still truncates** (167k chars for default page) — save-to-file + `python3 -c "import json; ...json.load(open(...))"` is the canonical workaround. Saved-file path: `/root/.claude/projects/<sid>/tool-results/mcp-github-list_pull_requests-*.txt`.
- **Env note (this run):** Full bootstrap was `pip install pytest fastapi pydantic pydantic-settings sqlalchemy loguru python-dotenv psycopg2-binary pandas numpy passlib bcrypt python-jose python-multipart openpyxl ruff && pip install --upgrade --ignore-installed cryptography cffi` — total ~90s. The cryptography/cffi `--upgrade --ignore-installed` is the 2026-06-02 fix; still required as of today. 4 tests pass in 1.39s.

---

## 2026-06-05 23:10 UTC — 2026-06-05-2305
**Why this matters next run:** PUNCH-LIST line 42 (intel.intel_search pagination) is now done — **PR #297**. The 2026-06-02 handoff incorrectly added `api/routers/intel.py` to the "file-blocked" list because PR #256 touches it; that's overly conservative. Different functions in the same file do NOT conflict in a 3-way merge — verify with `git log -p PR-#NNN -- <file>` before adding a file to the blocked list. Routine queue is now genuinely down to P2 refactors > 400 LOC (out of budget) or the still-open PR #276 ten_year_portfolio HIGH findings.

- Shipped PUNCH-LIST line 42 [P1] — **PR #297**. `api/routers/intel.py::intel_search` now returns `total/limit/offset/has_more` in `meta`. Implementation: 3 per-category `COUNT(*)` queries (actor/entity/ticker) summed; `has_more = (offset+limit) < total`. Empty-results envelope augmented too. 45 LOC + 147 LOC test (`tests/test_intel_search_pagination.py`, 4 cases). SQL-injection regression (9 tests) still passes. Ruff clean.
- **File-blocking correction:** the rule should be "different *function* in the same file is fine if your edits and the open PR's edits don't overlap by line range" — not "same file is blocked." Verify with `mcp__github__pull_request_read get_files` against the suspected blocker PR. PR #256 modifies `intel_predictions_active` (~line 1383) and creates `tests/test_intel_predictions_active_pagination.py`. PR #297 (this run) modifies `intel_search` (~line 144) and creates `tests/test_intel_search_pagination.py`. Zero overlap.
- **TIER 0 status (carried for the 9th day):** `main` CI still red — `GRID Tests` last failed on `9cd73faa`. **PR #291 fixes it; its own CI is green.** Operator-merge blocker, not code. Do NOT open a duplicate fix.
- **Files still file-blocked by open PRs (use line-range check, not whole-file block):** `api/routers/intel.py` (#256 — only `intel_predictions_active`), `api/routers/oracle.py` (#240), `api/routers/intelligence_search.py` (#239), `api/routers/models.py` (#255), `api/routers/prediction_backtest.py` (#275), `api/dependencies.py` + `db.py` (#277), `tests/test_ten_year_portfolio.py` (#291), `tests/test_system_router.py` (#265), `api/routers/system.py` + `api/schemas/system.py` + `tests/test_freshness_stale_sources.py` (#293), `requirements.txt` + `tests/test_requirements.py` (#294), and now `api/routers/intel.py::intel_search` + `tests/test_intel_search_pagination.py` (#297).
- **Remaining unclaimed PUNCH-LIST items** — all P2 refactors > 400 LOC, exceed the 25-min / 200-LOC budget: line 48 `canvas_expand.expand_node` (737 LOC), line 49 `intel.intel_briefing` (also overlaps #256 area but at line 1650), line 50 `intelligence_risk._build_risk_map` (448 LOC), line 51 `flows._build_sector_connections` (464 LOC). Tier 4 single-PR queue is exhausted.
- **Next clean picks if PUNCH-LIST stays dry:**
   1. **PR #276 HIGH findings #2 and #3 (still open after 7 days):** `api/routers/ten_year_portfolio.py:211-269` `/export.xlsx` lacks try/except (sibling POST has one); `:183-185` `/weekly` leaks `str(exc)` to clients (violates `.claude/rules/security.md`). Both mechanical, single-file. Verify the file isn't currently file-blocked first.
   2. **`docs/PUNCH-LIST-2026-05-13.md` staleness sweep:** 2026-06-02 verified lines 19/20/21/22-26/27 STALE; verify and mark lines 28-32 (oracle P2 splits — most look like they predate the engine.py de-dup). Pure doc PR.
   3. **Adding more guards to `tests/test_requirements.py`:** the pattern is established — `parse non-comment lines + grep for prefix`. Candidates: `xgboost`, `kafka-python-ng`. Only worth picking if one is reachable via a top-level import the way `openpyxl` was. Otherwise skip.
- **Env note (this run):** Fresh container, full bootstrap was: `pip install pytest pandas fastapi sqlalchemy pydantic loguru ruff`. 22 tests pass + 8 skip in 0.48s. Did NOT need cryptography/passlib/python-multipart for this PR — the new test stubs `api.auth` and `api.dependencies` at import-time so the heavy transitive deps never load.
- **`mcp__github__list_pull_requests` and `mcp__github__actions_list` still truncate** at 167k and 347k chars respectively for default page sizes. Save-to-file + python slice is the workaround (this run's results landed at `/root/.claude/projects/.../tool-results/`).

---

## 2026-06-04 23:08 UTC — 2026-06-04-2308
**Why this matters next run:** Both quick-picks from the 2026-06-03 handoff are now resolved — `openpyxl` shipped in PR #294, and the `astrogrid_api/dependencies.py:59` "same disposed-engine bug" suggestion is a **FALSE POSITIVE** (verified below). PR #291 (TIER 0 CI fix) is STILL unmerged after 8 days; `main` CI is still red. The routine queue is genuinely thin — see the new-pick options below before walking the punch list again.

- Shipped `openpyxl>=3.1.0` declaration in `requirements.txt` — **PR #294**. 20 LOC / 2 files: requirements.txt + new `tests/test_requirements.py::test_base_requirements_declare_openpyxl` regression guard (passes alongside the existing `test_base_requirements_do_not_mix_edgar_and_patent_client`). Ruff clean. The package was being silently relied on by `strategy/portfolio_workbook_plan.py:17-18` (top-level import, hit on FastAPI startup via `api/routers/ten_year_portfolio.py`) and by three pandas-engine call sites in `ingestion/altdata/{aaii_sentiment,nyfed_gscpi}.py` + `ingestion/trade/wiod.py`.
- **`astrogrid_api/dependencies.py:59` is NOT the same bug as PR #277 fixed in `api/dependencies.py`.** Reviewed the source directly: it's a standalone module with its own `_astrogrid_db_engine` + `create_engine()` call (lines 19, 35), and `clear_singletons()` correctly disposes the engine and nulls both globals (lines 60-64). PR #277 only mattered because `api/dependencies.py:get_db_engine()` was a *wrapper around* `db.get_engine()` and didn't cascade clears to `db._engine`. No equivalent cascade path exists in `astrogrid_api`. Stop suggesting this — it's been carried forward in 3 handoffs and is wrong.
- **TIER 0 status (carried for the 8th day):** `main` CI still red — `GRID Tests` workflow last failed on `9cd73faa` (psycopg2 `_FakeEngine` issue). **PR #291 fixes it; its own CI is green.** Operator-merge blocker, not a code problem. Do NOT open a duplicate fix.
- **Files still file-blocked by open PRs** (don't edit, will merge-conflict): `api/routers/intel.py` (#256), `api/routers/oracle.py` (#240), `api/routers/intelligence_search.py` (#239), `api/routers/models.py` (#255), `api/routers/prediction_backtest.py` (#275), `api/dependencies.py` + `db.py` (#277), `tests/test_ten_year_portfolio.py` (#291), `tests/test_system_router.py` (#265), `api/routers/system.py` + `api/schemas/system.py` + `tests/test_freshness_stale_sources.py` (#293), **and now** `requirements.txt` + `tests/test_requirements.py` (#294).
- **Remaining unclaimed PUNCH-LIST items** (unchanged from 2026-06-03 — all P2 refactors, 400–700 LOC, exceed routine budget): line 48 `canvas_expand.expand_node` (737 LOC), line 49 `intel.intel_briefing` (blocked by #256), line 50 `intelligence_risk._build_risk_map` (448 LOC), line 51 `flows._build_sector_connections` (464 LOC). Tier 4 is essentially exhausted for routine-budget items.
- **Best clean next picks (queue is genuinely thin — these are speculative):**
   1. **More packaging guards in `tests/test_requirements.py`** — the file's pattern is `parse non-comment lines + grep for prefix`. Easy to add similar guards for other top-level deps that get pulled in transitively today. Candidates to verify before adding: `xgboost` (declared, but worth a guard), `kafka-python-ng` (declared), `prefect`, `minio`. Worth doing IF you can show at least one is reachable via a top-level import like `openpyxl` was. Skip if all matches resolve to lazy/optional imports.
   2. **HIGH findings from PR #276 review (2026-05-29) still open** — beyond the openpyxl one shipped this run, the review flagged: (2) `api/routers/ten_year_portfolio.py:211-269` `/export.xlsx` GET handler has no `try/except` while sibling POST at :271 does; (3) `:183-185` `/weekly` returns raw `str(exc)` to client (violates `.claude/rules/security.md`). Both are small, mechanical, single-file changes. `api/routers/ten_year_portfolio.py` is NOT in the file-blocked list above; verify with `git log --oneline -5 api/routers/ten_year_portfolio.py` before picking — if there's been activity since 2026-05-29, re-confirm the findings still apply.
   3. **`docs/PUNCH-LIST-2026-05-13.md` staleness sweep** — the 2026-06-02 entry already verified lines 19/20/21/22-26/27 as STALE. Worth doing another sweep of lines 28-41 and marking the doc accordingly (similar to PR #150). Pure doc PR, low risk.
- **Env note (this run):** Only needed `pip install pytest ruff` to run `tests/test_requirements.py --noconftest` (it's pure-stdlib `pathlib`). 2 tests pass in 0.06s. Skipped the conftest.py + pandas dance entirely.

---

## 2026-06-03 23:30 UTC — 2026-06-03-2308
**Why this matters next run:** The standing latent bug from 2026-05-26 (`system.py::freshness()` stripping `stale_sources`) is now DONE in PR #293 — stop carrying it forward. Also: PR #291 (TIER 0 CI fix) is STILL open and unmerged after a full week. CI on `main` is still red against HEAD `9cd73faa` for the same `_FakeEngine` bug that #291 already fixes — DO NOT re-diagnose; ping the operator instead.

- Shipped the named-target latent bug — **PR #293**. Added `StaleSource` model + `stale_sources: list[StaleSource] = []` to `FreshnessResponse` in `api/schemas/system.py`; refactored `freshness()` in `api/routers/system.py` to construct the typed model directly (dropped the `resp.dict()` attach pattern). New `tests/test_freshness_stale_sources.py` (2 tests, both pass: empty-default + populated-rows shape). 99 ins / 13 del across 3 files. Ruff clean.
- **TIER 0 status:** `main` CI is still red — `GRID Tests` workflow run `26750529072` on `9cd73faa` failed with the same psycopg2 `_FakeEngine` shape issue PR #291 fixes. **PR #291 CI itself is green** (Backend Tests + Lint + Frontend Build + claude-review all success). The blocker is operator merge, not code. Do NOT open a duplicate fix.
- **TIER 1 status:** Zero unreviewed codex PRs. `#233`, `#276` already reviewed; `#242`, `#243` are intentional parked drafts (don't touch). The only `codex/*` branches still open as PRs are those three.
- **Files still blocked by open PRs** (don't edit, will merge-conflict): `api/routers/intel.py` (#256), `api/routers/oracle.py` (#240), `api/routers/intelligence_search.py` (#239), `api/routers/models.py` (#255), `api/routers/prediction_backtest.py` (#275), `api/dependencies.py` + `db.py` (#277), `tests/test_ten_year_portfolio.py` (#291), `tests/test_system_router.py` (#265), and **now** `api/routers/system.py` + `api/schemas/system.py` + `tests/test_freshness_stale_sources.py` (#293).
- **Best remaining quick-pick after #293**:
   - `astrogrid_api/dependencies.py:59` `clear_singletons()` — same disposed-engine bug PR #277 fixes in `api/dependencies.py`. ~2 LOC + a test. **CAVEAT (still unresolved from 2026-06-01 handoff):** verify `astrogrid_api/` isn't being retired before picking — the 2026-05-29 codex review of PR #276 noted "panda-node retirement cleanup." Run `git log --oneline -5 astrogrid_api/` first.
   - `requirements.txt` 1-line addition for `openpyxl` — PR #276 HIGH finding from 2026-05-29 still open as of this run; module is top-level imported in `strategy/portfolio_workbook_plan.py:18`, ModuleNotFoundError on fresh deploy. Verify it hasn't landed via another PR before opening.
- **Remaining unclaimed PUNCH-LIST items** (all P2 refactors, 400–700 LOC — exceed routine budget): line 48 `canvas_expand.expand_node` (737 LOC), line 49 `intel.intel_briefing` (also blocked by #256), line 50 `intelligence_risk._build_risk_map` (448 LOC), line 51 `flows._build_sector_connections` (464 LOC).
- **Env note (this run):** `pip install pytest fastapi httpx pydantic-settings sqlalchemy loguru python-dotenv passlib bcrypt pandas psycopg2-binary python-jose python-multipart ruff && pip install --upgrade --ignore-installed cryptography` was sufficient to run the new `tests/test_freshness_stale_sources.py` (2 passing, ~5s). `tests/test_api.py` still fails collection with `passlib` bcrypt panic (`password cannot be longer than 72 bytes`) on import — pre-existing, NOT touched by this PR; new API tests must NOT hash a password at import-time (use a static `GRID_MASTER_PASSWORD_HASH` literal + `create_token`).

---

## 2026-06-02 23:30 UTC — 2026-06-02-2308
**Why this matters next run:** PUNCH-LIST-2026-05-13.md is heavily stale — multiple P0/P1 items are already fixed in main. Skip them before walking the punch list. PR #291 (TIER 0 CI fix) is still open and unmerged — leave alone unless you have new info.

- Shipped PUNCH-LIST line 52 [P2] — `tests/test_viz_router.py` smoke tests for the 9 unauthenticated `/api/v1/viz/*` routes (12 tests, all pass, ruff clean). **PR #292.**
- **Verified STALE in `docs/PUNCH-LIST-2026-05-13.md` (no code change needed — confirmed against main HEAD `9cd73faa`):**
   - Line 19 [P0] `oracle/engine.py` horizon helpers duplication — only one definition of each at lines 137 / 152 / 185. No second copy at line 2158.
   - Line 20 [P0] Two `publish_astrogrid_prediction` implementations — `oracle/publisher_gate.py` is now a pure re-export shim (`from oracle.publish import publish_astrogrid_prediction`).
   - Line 21 [P1] Duplicate `CalibrationReport` dataclass — only defined in `inference/calibration.py:57`; `oracle/calibration.py` no longer defines it.
   - Lines 22–26 [P1] Missing tests for `oracle/firewall.py`, `oracle/publisher_gate.py`, `oracle/claim_extractor.py`, `oracle/claim_verifier.py`, `oracle/sanity_checker.py` — **all 5 test files exist** in `tests/`.
   - Line 27 [P1] `Signal(name, family, z, 0, ...)` z_score=0 bug — `oracle/engine.py:835` now correctly passes `z` for both `value` AND `z_score`.
- **Files still blocked by open PRs (don't edit this run, you'll merge-conflict yourself):** `api/routers/intel.py` (#256), `api/routers/oracle.py` (#240), `api/routers/intelligence_search.py` (#239), `api/routers/models.py` (#255), `api/routers/prediction_backtest.py` (#275), `api/dependencies.py` + `db.py` (#277), `tests/test_ten_year_portfolio.py` (#291), `tests/test_system_router.py` (#265).
- **Remaining unclaimed punch-list items** (all P2 refactors, all 400–700 LOC — likely exceed the 25-min and 200-LOC budgets):
   - Line 48: Split `canvas_expand.expand_node` (737 LOC) — `api/routers/canvas_expand.py`
   - Line 49: Split `intel.intel_briefing` (509 LOC) — blocked by #256 anyway
   - Line 50: Split `intelligence_risk._build_risk_map` (448 LOC) — `api/routers/intelligence_risk.py`
   - Line 51: Split `flows._build_sector_connections` (464 LOC) — `api/routers/flows.py`
- **TODO docs (`TODO-DATA-AUDIT.md`, `TODO-DUP-WRITES.md`) are multi-PR campaigns**, not single-PR items — skip for the routine flow.
- **Latent bug still standing from 2026-05-26 handoff** (verified unfixed): `system.py::freshness()` (~line 422-426) attaches `stale_sources` to `resp.dict()` but `response_model=FreshnessResponse` strips it before serialization. ~5 LOC follow-up PR. `api/routers/system.py` is NOT currently file-claimed by any open PR.
- **Env note:** This run's full bootstrap that worked for the viz smoke test: `pip install pytest fastapi httpx pydantic-settings sqlalchemy loguru python-dotenv passlib bcrypt pandas psycopg2-binary python-jose ruff && pip install --upgrade --ignore-installed cryptography` (the system `cryptography` 41 is debian-managed; `--ignore-installed` bypasses the "RECORD file not found" uninstall failure). Total ~90s. `tests/conftest.py` does load (pandas required) — tests collect after the cryptography upgrade.

---

## 2026-06-01 23:10 UTC — 2026-06-01-2310
**Why this matters next run:** main CI has been red since 2026-05-30 (PR #282 perf refactor broke `tests/test_ten_year_portfolio.py`). PR #291 fixes it — verify CI on the PR is green and request merge before doing anything else, then the alternative-pick list from 2026-05-29 is still the best queue.

- **TIER 0 done — PR #291:** `_load_price_history` was refactored in PR #282 (#2707648b) from a single `engine.connect()` to `engine.url.render_as_string()` + two parallel `psycopg2.connect(dsn)` workers via `ThreadPoolExecutor`. The test fake (`_FakeEngine`) didn't have `.url` and used the old SQLAlchemy `connect/execute` shape, so every commit since 2026-05-30 failed with `AttributeError: '_FakeEngine' object has no attribute 'url'`. Updated the fake to psycopg2 shape (`.cursor() -> _FakeCursor`, `.rollback()`, `.close()`) + added `_FakeURL.render_as_string()`, `monkeypatch.setattr(mod.psycopg2, "connect", ...)`, and corrected the assertions: AAPL/QQQ are no longer in `raw_series` (they hit `resolved_series` as `aapl_full`/`qqq_full`), only the 11 `FRONTIER_RAW_HISTORY_TICKERS` (CCJ, HPE, ANET, ASML, BHP, CEG, DELL, ETN, FCX, MU, NUE) are in the `raw_series` query.
- **Watch for: PR #291 itself** — if a reviewer asks why we don't just mock `_fetch_resolved_rows` / `_fetch_raw_rows` directly, the reason is the test's stated intent is to assert the SQL targets / params, which only the psycopg2-cursor mock surfaces. That said, the alternative (patch the two helpers) is ~3 fewer LOC and also valid; happy to swap if preferred.
- **The 2026-05-29 alternative-pick list is still good and unclaimed** (verified this run, none touched since):
   - `astrogrid_api/dependencies.py:59` `clear_singletons()` — same disposed-engine bug PR #277 fixed in `api/dependencies.py`. ~2 LOC + a matching test. **NEW CAVEAT:** before picking this, confirm `astrogrid_api/` isn't being deleted/retired — the 2026-05-29 codex review noted "panda-node retirement cleanup" was happening in PR #276, and astrogrid may be on the same chopping block. `git log --oneline -5 astrogrid_api/` and check.
   - `system.py::freshness()` (~line 422-426) `stale_sources` stripped by `response_model=FreshnessResponse`. Real bug, ~5 LOC fix (add field to model or drop `response_model`).
   - PUNCH-LIST line 52 [P2] — `viz.py` smoke tests (9 unauth routes), brand new test file, tiny.
- **PUNCH-LIST line 47 [P2]** (`prediction_backtest.py:116` f-string SQL) — `api/routers/prediction_backtest.py` is no longer file-blocked by PR #275 if it merged (check `git log --oneline -5 api/routers/prediction_backtest.py`). The 2026-05-29 handoff said #275 was still open then; verify status before picking.
- **Env note (revised this run — copy this prelude exactly):** the cffi-panic fix from 2026-05-19 still applies AND **`python-multipart` is now also required** to import any router from `api/routers/` (FastAPI `analyze_param` raises `RuntimeError: Form data requires python-multipart` on import). Full bootstrap that worked: `pip install --quiet pytest psycopg2-binary sqlalchemy loguru fastapi pydantic pydantic-settings pandas numpy python-jose passlib openpyxl python-multipart && pip install --quiet --force-reinstall cffi`. Total ~60s. The `openpyxl` install is what the 2026-05-29 HIGH finding on PR #276 flagged as a deploy bug — it's still missing from `requirements.txt` on main as of HEAD `9cd73faa`. **If that HIGH hasn't been actioned, that's a clean follow-up PR: 1-line addition to `requirements.txt`.**
- **`mcp__github__list_pull_requests` still truncates** (156k chars for 27 open PRs as of this run); save-to-file + python slice still works. **`mcp__github__actions_list list_workflow_runs` ALSO truncates now** (347k chars for the default page); save-to-file + json.load + iterate is the workaround. New `actions_list` + `get_job_logs` is the canonical way to grab failing-CI tracebacks; works fine — pass `tail_lines: 200` to stay under the cap.

---
_Entries older than 14 days were trimmed per the routine rules. The
2026-05-25 entry's standing insight — codex PRs in this repo are
authored by login `3pacs` on `codex/*` branches (not `app/openai-codex`),
so filter TIER 1 by `head.ref` starting `codex/` not by author — is
preserved here as a permanent note._

<!-- TRIMMED 2026-05-29 23:10 UTC — 2026-05-29-2310
**Why this matters next run:** TIER 1 burned on codex PR #276 review — don't re-review it. The standing PUNCH-LIST line 47 [P2] is still the best clean code pick, BUT the stale "PR #275 now merged" note in the 2026-05-28 handoff is wrong — #275 is still OPEN, so `prediction_backtest.py` remains file-claimed.

- Posted COMMENT review on **PR #276** (`codex/ten-year-portfolio-landing-20260527`, opened 2026-05-28, 24 files +3141/-51, zero prior reviews). 0 CRITICAL, 3 HIGH, 3 MEDIUM, 3 LOW. The 3 HIGH are operator-actionable one-liners: (1) `openpyxl` missing from `requirements.txt` despite top-level import in `strategy/portfolio_workbook_plan.py:18` — fresh-deploy `ModuleNotFoundError`; (2) `api/routers/ten_year_portfolio.py:211-269` `/export.xlsx` GET handler has no `try/except` while sibling POST at :271 does; (3) `:183-185` `/weekly` returns raw `str(exc)` to client (violates `.claude/rules/security.md`). MEDIUM #4 is a PIT bypass to flag for the operator: `_load_price_history` reads `raw_series`/`resolved_series` directly anchored on `CURRENT_DATE` — fine for live UI, lookahead landmine if ever reused in a backtest.
- **PR #275 status: still OPEN as of this run** (the 2026-05-28 handoff said "now merged" but it isn't). So PUNCH-LIST **line 47 [P2]** (f-string SQL in `api/routers/prediction_backtest.py:116`) is STILL file-blocked by #275's open diff. Either wait for #275 to merge, or pick a different item. If you do pick line 47, the conflict is mechanical (your edit hits the function body; #275 only touches `Depends()` signatures) — but the file-claim rule is conservative for a reason.
- **Alternative clean picks (no file conflicts) if #275 hasn't merged:**
   - **PUNCH-LIST line 52 [P2]** — viz smoke test (`api/routers/viz.py`, 9 unauth routes). Brand new test file, no existing file touched, tiny diff. Easiest available item.
   - **Companion bug from 2026-05-28 handoff** — `astrogrid_api/dependencies.py:59` `clear_singletons()` has the same disposed-engine bug PR #277 just fixed in `api/dependencies.py`. ~2 LOC + matching test. Verify `astrogrid_api/` isn't touched by another open branch first.
   - **2026-05-26 latent bug** — `system.py::freshness()` (~line 422-426) attaches `stale_sources` to `resp.dict()` but `response_model=FreshnessResponse` strips it before serialization. Add the field to the model or drop `response_model`. ~5 LOC.
- **PRs #242, #243 are still parked drafts** — don't review (confirmed by 2026-05-25 handoff). Other codex/* branches (#233 already reviewed, #276 reviewed this run) are the only codex PRs visible in TIER 1.
- **Env note (verified this run):** Direct `git push origin routine-bookkeeping` still fails (HTTP 403) — `mcp__github__push_files` against branch `routine-bookkeeping` works fine. `mcp__github__list_pull_requests` still exceeds tool-result token cap (26 open PRs → 154k chars); save-to-file + slice via python is the standard workaround.

---
## 2026-05-28 23:08 UTC — 2026-05-28-2308
**Why this matters next run:** Line 46 is now DONE (PR #277). The 2026-05-17 api/ audit is down to ONE remaining unclaimed item — line 47 [P2] — plus the stale latent bug from 2026-05-26 still standing as an easy follow-up.

- Shipped PUNCH-LIST [P2] line 46 (`clear_singletons()` cascade to `db._engine`) as **PR #277**. Added `db.clear_engine()` helper + routed `api.dependencies.clear_singletons()` through it. 5 regression tests in `tests/test_dependencies_clear_singletons.py` cover db._engine reset, dispose count, fresh-engine-on-next-call, dependent-store reset, and cold-state idempotency. Tests pass via `pytest --noconftest`; ruff clean.
- **Companion bug spotted, NOT in this PR:** `astrogrid_api/dependencies.py:59` has its own `clear_singletons()` with the same shape — almost certainly has the same disposed-engine bug. Skipped because the PUNCH-LIST item names `api/dependencies.py:67` specifically and bundling violates the one-PR rule. Good standalone follow-up (~2 LOC, same pattern: `import db; db.clear_engine()`).
- **Best clean next pick: line 47 [P2]** — f-string SQL in `api/routers/prediction_backtest.py:116` (`text(f"SELECT COUNT(*) FROM {table}")`). Currently safe because `table` comes from a hardcoded literal list, but rule-violation (`.claude/rules/security.md`). Switch to explicit per-table queries or an allowlist+validate helper like `api/routers/config.py:27`. Same file as PR #275 (now merged) but a separate function — no file-claim conflict.
- **Watch for: PR #277 itself** — if a reviewer says the `import db` shadows the existing `from db import get_engine`, that's expected (and intentional — needed to reach `db.clear_engine`). It's a stylistic call; can be reworked to `from db import clear_engine as _clear_db_engine` if desired, but the current form is clearer about the cross-module coupling.
- **Latent bug carried forward from 2026-05-26** (still unfixed): `system.py::freshness()` (~line 422-426) attaches `stale_sources` to `resp.dict()` but the `response_model=FreshnessResponse` filters it out before serialization — frontend never sees `stale_sources`. Real fix: add the field to the model or drop `response_model`. Still a good ~5 LOC follow-up PR.
- **Env note (confirmed this run):** `pip install --user sqlalchemy loguru pydantic pydantic-settings psycopg2-binary pandas numpy pytest ruff` is enough to run a dep-injection test like `test_dependencies_clear_singletons.py` via `pytest --noconftest` (`tests/conftest.py` pulls in too much). Total install ~30s. No `cffi` panic this run — only the cryptography panic hits when `api.auth` is imported, which this test avoids.

---
## 2026-05-27 23:23 UTC — 2026-05-27-2309
**Why this matters next run:** line 45 is now DONE; the next top-down P1 (line 42 `intel_search`) is double-blocked — don't pick it.
- Shipped PUNCH-LIST line 45 [P1] (`prediction_backtest.py` `get_engine`→`get_db_engine`, all 4 Depends sites) as **PR #275**, plus a static wiring regression `tests/test_prediction_backtest_engine_dep.py` (3 tests, pass via `pytest --noconftest`; ruff clean).
- **Line 42 (`intel.intel_search` pagination) is now blocked two ways:** (1) the per-source LIMIT/OFFSET design problem documented in the 2026-05-21 entry below, AND (2) `api/routers/intel.py` is currently modified by **open PR #256** — editing it now risks a conflict. Skip until #256 merges and the operator decides pagination semantics.
- **Best clean next pick: line 46 [P2]** — `clear_singletons()` (`api/dependencies.py:67`) disposes/None's the api-level `_db_engine` but never resets `db._engine`, so the next `get_db_engine()` returns a *disposed* engine. This is the underlying bug that makes PR #275 fully effective. Isolated ~2-line fix (`import db; db._engine = None`, or add a `db.clear_engine()` helper) + a small regression test. Not file-blocked.
- **Env note:** this sandbox lacks `fastapi`/`pandas`; the repo `tests/conftest.py` imports pandas so router-import tests can't collect here. For pure-stdlib static tests use `pytest --noconftest`; otherwise rely on CI (full deps present). `pytest`/`ruff` installed via `pip install --user`.

---
## 2026-05-26 23:35 UTC — 2026-05-26-2303
**Why this matters next run:** Line 44 (system.py tests) is now done; the next clean api/ items are 45/46/47, and I left a real latent bug flagged below.

- Picked **PUNCH-LIST line 44** [P1] — created `tests/test_system_router.py` (PR #265): auth-required + happy-path smoke tests for `/freshness`, `/pipeline-health`, `/hermes-status` via the `mock_engine` empty-DB graceful-degradation path. 6 tests pass, ruff clean.
- **Next clean routine-claimable items** (same 2026-05-17 api/ audit, all still unclaimed): line 45 [P1] `prediction_backtest.py:20` `get_engine`→`get_db_engine`; line 46 [P2] `clear_singletons()` doesn't clear `db._engine`; line 47 [P2] f-string SQL in `prediction_backtest.py:116` (same file as 45 — could pair). Line 42 (`intel_search` pagination) remains a design call — do NOT sum-and-ship (see 2026-05-21 entry).
- **Latent bug found, left unfixed (out of scope):** `system.py::freshness()` (line ~422-426) builds `resp_dict = resp.dict(); resp_dict["stale_sources"] = stale_sources` and the comment claims "FastAPI will include it." It does NOT — the route declares `response_model=FreshnessResponse`, so FastAPI filters the response through that model and **strips `stale_sources` entirely**. The frontend staleness-indicator data never reaches the client. Real fix = add `stale_sources` to the `FreshnessResponse` model (or drop `response_model`). Good small follow-up PR.
- **Env note (confirms 2026-05-21):** the test suite CAN run in this container after `pip install fastapi sqlalchemy pandas pydantic loguru psycopg2-binary 'python-jose[cryptography]' pytest ruff`. Two gotchas: (1) the installed **passlib bcrypt backend panics** on its self-test (`password cannot be longer than 72 bytes`) the moment you call `_pwd_ctx.hash()` — so in new API tests DON'T hash a password at import like `test_api.py` does; set a **static** `GRID_MASTER_PASSWORD_HASH` literal and mint auth via `create_token` (only needs `GRID_JWT_SECRET`). (2) system `cryptography` 41 is debian-managed and can't be force-reinstalled, but `python-jose[cryptography]` still imported fine for me this run.
-->

