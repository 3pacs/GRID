## 2026-06-30 23:05 UTC — 2026-06-30-2305
**Why this matters next run:** PUNCH-LIST line 116 (alpha_research log-skipped-rebalances) shipped as PR #358. Same pattern as PR #338 (heartbeat) — `from loguru import logger as log` + `log.warning` on the exception path + 2 regression cases via `monkeypatch.setattr(module.log, "warning", capture)`. Strike line 116 from any unchecked-item walk.

- **PUNCH-LIST staleness is real and worth a separate cleanup PR.** I verified mid-walk that lines 88, 108, 109, 110, 127, 128, 130, 131 all already have test files (listed in the prior 2026-06-29 + 2026-06-26 + 2026-06-25 handoffs); they're still showing `[ ]` because nobody reconciled. A pure-docs PR that flips them `[x]` with PR/file evidence would save every future routine ~5 minutes of grep work. Did NOT bundle that into PR #358 (scope guard — one fix, one PR).
- **Still pickable from prior handoffs' "small items" lists (in priority order):**
   - line 95 [P2] — `contracts/handlers/__init__.py:1` stale "empty in Phase 1" docstring (~10 LOC + 1 test).
   - line 117 [P2] — drop dangling `docs/TODO-REGIME-SIGNAL-USAGE.md` ref at `alpha_research/adapters/signal_adapter.py:174` (~3 LOC).
   - line 151 [P2] — `analysis/viz_intelligence.py:517,535` `except Exception: pass` → `log.warning` (same pattern as PR #358 / #338).
   - line 152 [P2] — `analysis/money_flow_engine/layer_credit.py:162` same `except: pass` → `log.warning`.
   - line 94 [P2] — preserve Decimal precision in `oracle_anti_signals.on_cross_reference_anomaly` (`float()` truncates NUMERIC binding).
   - line 96 [P2] — drop dead defensive branch in `oracle_anti_signals` severity check (Literal already rejects the path).
   - line 118 [P2] — drop unnecessary `object.__setattr__` workaround on `PositionState` (dataclass is mutable, not frozen).
- **Env note (worked this run):** pytest is uv-managed at `/root/.local/share/uv/tools/pytest/bin/python` (no pip). Install deps via `uv pip install --python /root/.local/share/uv/tools/pytest/bin/python numpy pandas sqlalchemy loguru` — finishes in <10s. Then run `/root/.local/bin/pytest tests/<file> -v --noconftest`. The repo's top-level `tests/conftest.py` requires more deps; `--noconftest` is the fast escape hatch when only the new test file matters.
- **TIER 0 status:** main CI **GREEN** on HEAD `44d8db41` (latest commit `Merge pull request #357 from 3pacs/coord/grid-w1-final-proof-20260630`). Both GRID Tests and Deploy success.
- **TIER 1 status:** zero codex-authored PRs (`mcp__github__search_pull_requests author:app/openai-codex` returns 0). 15 open PRs total: 5 claude-routine (#351-355) + 1 claude/* (#353) + 9 dependabot. None reviewable as routine items.
- **Tool-result truncation still hits** `mcp__github__list_pull_requests` (160k chars) and `mcp__github__actions_list` (335k chars). Save-to-file + `python3 -c "json.loads(open(p).read())"` is the canonical workaround — same as prior runs.
- **Pre-existing lint debt** in `alpha_research/strategies/rotation_variant_backtest.py`: unused `dataclasses.field` import + unused `pandas as pd` import + unused `excess` local (F401/F401/F841). I did NOT fix in PR #358 (out of scope). If a future agent picks anything else in this file, fix these in the same PR.

## 2026-06-29 23:05 UTC — 2026-06-29-2305
**Why this matters next run:** the 2026-06-26 handoff's next-pickable list named PUNCH-LIST line 150 (`LeadLagBacktest` docstring) — that's **shipped as PR #355**. Strike it before re-picking. The fix was pure docstring + 2 regression tests; the 16-line example was rewritten to `run_walk_forward(...)` with required `leader_name`/`follower_name` kwargs, and new `TestPublicSurface` locks the example to the public API so a future rename can't silently re-orphan the docs. Total 3 files +37/-5 LOC.

- **Still pickable from the 2026-06-26 list (in rough leverage order):**
   - PUNCH-LIST line 95 [P2] — one-line stale-docstring fix in `contracts/handlers/__init__.py:1` ("Phase 2 contract handlers — empty in Phase 1"). 11 handler modules now ship; just rewrite to match reality. ~10 LOC + 1 regression test.
   - PUNCH-LIST line 117 [P2] — drop dangling `docs/TODO-REGIME-SIGNAL-USAGE.md` reference at `alpha_research/adapters/signal_adapter.py:174`. ~3 LOC.
   - PUNCH-LIST line 88 [P1] **VERIFY FIRST**: `tests/contracts/test_alerts_handler.py` already exists (195 LOC per 2026-06-25 handoff). If it covers both `on_cross_reference_anomaly` and `on_regime_transition`, just flip the punch-list item `[x]` — that alone is a clean docs-only PR.
- **Stale-PUNCH-LIST sweep is still on the table.** Same list as 2026-06-26 handoff (lines 69, 70, 88, 91, 92, 108, 109, 110, 127, 128, 130, 131 all marked `[ ]` but shipped via PRs #348/#349/#350). Pure-docs cleanup PR, no PYTHONPATH dance.
- **Env note (worked this run):** `pip install --user ruff numpy` then `PYTHONPATH=/root/.local/lib/python3.11/site-packages:/usr/lib/python3/dist-packages:/home/user/GRID /root/.local/bin/pytest tests/<file> --noconftest`. Pure-math tests with only numpy don't need scipy/pandas/sqlalchemy.
- **TIER 0 status:** main CI **GREEN** on HEAD `840cdc28` — same as 2026-06-26 (no new main commits in 3 days; routine PRs piling up).
- **TIER 1 status:** zero codex-authored PRs. 14 open PRs at run start (counting): 4 claude-routine (#351, #352, #353, #354) + 10 dependabot (#322-331). PR #354 is an auditor-feed for `tests/` findings 2026-06-28 — fresh backlog source the next agent should scan for picks.
- **PR #354 is new** (auditor-feed/2026-06-28-tests, opened 2026-06-28). If the underlying findings file is only `docs/PUNCH-LIST-2026-05-13.md` or a new auditor doc, picking from its items directly is unblocked the same way prior auditor-feeds (#299/#305) were — verify via `mcp__github__pull_request_read get_files` before reaching across.
- `mcp__github__list_pull_requests` and `mcp__github__actions_list` still truncate (157k / 334k chars for current repo size). Same save-to-file + python slice / json.load workaround as prior runs.

## 2026-06-26 23:15 UTC — 2026-06-26-2304
**Why this matters next run:** the 2026-06-25 handoff's #1 next-pickable item (PUNCH-LIST line 148 — `analysis/ephemeris.py` core solver tests) is **shipped** as **PR #352**. Strike it before re-picking. The pure-math tests dodged the PYTHONPATH/scipy dance that prior vol_surface work needed — no third-party deps so `pytest --noconftest` worked straight up (the top-level `tests/conftest.py` still imports pandas which isn't in the routine env).

- Remaining "next pickable small items" from prior handoff are unchanged: PUNCH-LIST line 95 (`contracts/handlers/__init__.py:1` stale docstring), line 117 (drop `docs/TODO-REGIME-SIGNAL-USAGE.md` ref in `alpha_research/adapters/signal_adapter.py:174`), line 150 (rewrite stale `LeadLagBacktest` docstring), line 88 verify-first (alerts handler tests likely already in `tests/contracts/test_alerts_handler.py`).
- **Stale-PUNCH-LIST sweep PR is still a great no-test-deps pick.** Items shipped via PRs #348/#349/#350 still show `[ ]` in `docs/PUNCH-LIST-2026-05-13.md`: lines 69 (global_levers), 70 (deep_graph), 88 (alerts), 91 (trust.on_signal_fired), 92 (journal.on_prediction_scored), 108 (conviction_scorer), 109 (signal_adapter), 110 (rotation_variant_backtest), 127 (physics/transforms), 128 (physics/conventions), 130 (DealerGammaEngine), 131 (check_dimensional_consistency). Verify each test file exists in `tests/` before flipping. Cleanup PR is pure docs — no PYTHONPATH dance needed.
- **TIER 0 status:** main CI **GREEN** on HEAD `840cdc28` (no new merges since the 2026-06-25 run — 35h staleness; same head as yesterday).
- **TIER 1 status:** zero codex-authored PRs. 11 open PRs: 1 claude-routine (#351 vol_surface tests, my #352 just opened makes 2), 9 dependabot.

## 2026-06-25 23:30 UTC — 2026-06-25-2304
**Why this matters next run:** the pytest interpreter is uv-managed and
doesn't share site-packages with `python3 -m pip install --user`. Also `scipy`
is now needed for any `analysis/vol_surface.py` import (transitively pulled
through `physics.dealer_gamma`). The combination that worked this run was:

```bash
python3 -m pip install --user scipy numpy pandas sqlalchemy loguru
PYTHONPATH=/root/.local/lib/python3.11/site-packages:/usr/lib/python3/dist-packages:/home/user/GRID \
    /root/.local/bin/pytest tests/<file> -v --noconftest
```

The `/usr/lib/python3/dist-packages` segment matters for `six` (Debian
package, needed transitively by `python-dateutil` → pandas tz init). Don't
try to `pip install` into the uv venv directly — it ships without `pip`.

- Shipped PUNCH-LIST line 147 [P1] (analysis/vol_surface.py Black-Scholes
  helpers) as **PR #351**. New `tests/test_vol_surface_bs_helpers.py`,
  13 tests, 112 LOC; covers put-call parity, vega/theta/volga arithmetic
  identities, T=0 and sigma=0 intrinsic-value collapse, and 5 parametrised
  degenerate-input guards. Tests-only, ruff clean.
- **Next pickable small items in priority order** (all under routine budget):
   - PUNCH-LIST line 148 [P1] — `analysis/ephemeris.py` Kepler/ecliptic core
     solver reference-value tests (`_solve_kepler`, `_ecliptic_to_equatorial`,
     `_normalize_angle`, `_angular_separation`); auditor estimate ~50 LOC.
   - PUNCH-LIST line 95 [P2] — one-line stale-docstring fix in
     `contracts/handlers/__init__.py:1` (says "empty in Phase 1" but ships
     11 handler modules).
   - PUNCH-LIST line 117 [P2] — drop dangling `docs/TODO-REGIME-SIGNAL-USAGE.md`
     reference at `alpha_research/adapters/signal_adapter.py:174`.
   - PUNCH-LIST line 150 [P1] — rewrite stale `LeadLagBacktest` docstring in
     `analysis/lead_lag_backtest.py:18` (class doesn't exist; should reference
     `run_walk_forward(...)`).
   - PUNCH-LIST line 88 [P1] — `contracts/alerts` handler tests; **VERIFY
     FIRST**: `tests/contracts/test_alerts_handler.py` already exists with 195
     LOC. If it covers both `on_cross_reference_anomaly` and
     `on_regime_transition`, just mark the punch-list item `[x]`.
- **Stale-PUNCH-LIST sweep candidate:** several `[ ]` items shipped via the
  test-blitz PRs #348/#349/#350 and `[ ]` is now wrong. Specifically:
  lines 69 (global_levers), 70 (deep_graph), 88 (alerts), 91 (trust.on_signal_fired),
  92 (journal.on_prediction_scored), 108 (conviction_scorer), 109 (signal_adapter),
  110 (rotation_variant_backtest), 127 (physics/transforms), 128 (physics/conventions),
  130 (DealerGammaEngine), 131 (check_dimensional_consistency). A pure-docs
  cleanup PR that flips `[ ]` → `[x]` with PR-evidence is a good no-test-deps
  pick if the PYTHONPATH dance breaks for some reason.
- **TIER 0 status:** main CI **GREEN** on HEAD `840cdc28` (latest commit
  `test: cover conviction scorer and physics verification (#350)`). Both
  GRID Tests AND Deploy to Grid Server reported success on the top 10 runs.
- **TIER 1 status:** zero codex-authored PRs (search_pull_requests
  `author:app/openai-codex` returns 0). 10 open PRs total, all dependabot.
- **Tool-result truncation still hits** `mcp__github__list_pull_requests`
  (144k chars for 10 open PRs) and `mcp__github__actions_list` (334k chars).
  Same save-to-file + python slice / json.load workaround as prior runs.

## 2026-06-19 23:10 UTC — 2026-06-19-2305
**Why this matters next run:** the `transfer_entropy` drift item from the 2026-06-15 handoff's PR #305 physics/ list is **shipped** — strike it before picking. Down to 10 items on that list now.
- **[P1] `physics/transforms.py:627` vs `analysis/transfer_entropy.py:135` duplicate** → shipped as **PR #340** (deleted the physics/ body, added `tests/test_transfer_entropy_canonical.py` 2-test invariant guard, marked PUNCH-LIST line 126 `[x]`, updated `docs/MODULE_INVENTORY.md` LOC + function list). +52 / −61.
- Pickable PR #305 physics/ items unchanged otherwise. Next highest-leverage single-PR: **[P1] `physics/verify.py:354` N+1 → CTE** (preserve `_get_latest_value(name, ...)` signature) OR the **[P1] reference-value tests for the 6 prod-wired energy/OU helpers** (`kinetic_energy`, `potential_energy`, `total_energy`, `market_temperature`, `estimate_ou_parameters`, `hurst_exponent`) — tests-only, no signature risk.
- **Caveat for the next `physics/transforms.py` deletion pick ([P2] 9 unused functions):** the line numbers in that handoff item (`entropy_rate:121, phase_velocity:155, ...`) are unchanged — my deletion only removed the file tail (lines 622-679), so every targeted function's line is still accurate. Re-run `grep -rn` per the auditor's string-import note before deleting.
- **TIER 0 status:** main CI green on HEAD `7f7c5f3f` (latest run 2 days ago — last commit `docs: mark second punch-list test wave resolved (#337)`).
- **TIER 1 status:** zero codex-authored PRs. 12 open PRs total: 2 claude-routine (#338 heartbeat, #339 split_adjuster tests), 10 dependabot. None reviewable as routine items.

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

---
_Entries older than 14 days were trimmed per the routine rules. The
2026-05-25 entry's standing insight — codex PRs in this repo are
authored by login `3pacs` on `codex/*` branches (not `app/openai-codex`),
so filter TIER 1 by `head.ref` starting `codex/` not by author — is
preserved here as a permanent note._
