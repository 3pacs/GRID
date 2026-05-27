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

---
## 2026-05-25 23:12 UTC — 2026-05-25-2308
**Why this matters next run:** TIER 1 has been silently mis-scoped by prior runs — there ARE reviewable codex PRs; they just aren't authored by `app/openai-codex`.

- Codex PRs in this repo are authored by login `3pacs` on `codex/*` branches (not `app/openai-codex`). The old `search_pull_requests author:app/openai-codex → 0` check in the 2026-05-14/15 entries was a false negative. To find TIER 1 work, filter open PRs by `head.ref` starting `codex/`, not by author.
- This run reviewed **PR #233** (`codex/hermes-finetune-fleet-20260518`, open since 2026-05-18, zero prior reviews) — posted a COMMENT review (3 MEDIUM hardening items on the iMessage bridge + SFT scrubber; no blocker). It now has a claude review, so skip it next run unless it gets new commits.
- **PRs #242 and #243 are NOT review targets** — they are intentional draft "Park" branches (`[codex] Park ...`), explicitly "intentionally draft until rebased/retested against current main." Don't waste a TIER 1 slot reviewing them; the operator parked them on purpose.
- Watch for over-eager subagent findings: the code-reviewer subagent flagged a "CRITICAL osascript injection" in `hermes_imessage_bridge.py:1156` that was a false positive — `recipient`/`text` are passed as `argv` items to a static AppleScript body via `subprocess.run([...])` (no shell, no interpolation). Always verify a CRITICAL against the actual source before posting it publicly.

---
## 2026-05-21 23:18 UTC — 2026-05-21-2313
**Why this matters next run:** The has_more sibling series is now down to ONE remaining item, and it is NOT a simple one-key add — don't treat it as such.

Status of the 2026-05-17 audit has_more group:
- line 39 `search_intelligence` → PR #239 (done)
- line 40 `oracle.get_predictions` → PR #240 (done)
- line 41 `models.get_all` → PR #255 (done, opened 2026-05-20)
- line 43 `intel.intel_predictions_active` → **PR #256 (this run, done)** — was a clean one-key add; `total/limit/offset` already present, added `has_more: (offset+limit)<total` matching `journal.py:77`.
- line 42 `intel.intel_search` (`api/routers/intel.py:252`) → **STILL OPEN, but it's a design decision, not a quick add.** I deliberately skipped it. Reason: `intel_search` is a UNION of up to 3 independent sub-queries (actors / icij_relationships / oracle_predictions), and **each sub-query applies the SAME `LIMIT :lim OFFSET :off` separately** (lines 175, 204, 231). So with `type="all"` the result list can hold up to 3×limit rows, and there is no single coherent `total` — summing three COUNT(*)s doesn't match how offset is applied per-source. Adding `total`/`has_more` here requires picking a pagination semantics (per-type totals? cap type="all"? paginate only single-type queries?) — that's an operator/product call, not a mechanical fix. If you pick it up, propose the semantics in the PR body and flag it for operator review; do NOT just sum-and-ship a misleading `total`.

Next clean routine-claimable items on the same 2026-05-17 audit list (all unclaimed):
- line 45 [P1] — switch `prediction_backtest.py:20` import `get_engine` → `get_db_engine` (small, clean).
- line 44 [P1] — direct test coverage for `api/routers/system.py` (tests-only, bigger lift).
- line 46 [P2] — `clear_singletons()` doesn't clear `db._engine` (`api/dependencies.py:67`) — real bug, dep-injection plumbing.
- line 47 [P2] — f-string SQL in `prediction_backtest.py:116` (same file as line 45).

**Env note (this container):** no python deps preinstalled and **even after `pip install` the full test suite won't collect** — `tests/conftest.py` imports `pandas` and routers import `fastapi`; this routine box had neither and installing the whole chain wasn't worth it for a 6-line diff. I verified the change via `py_compile` + `ruff check` (clean) and confirmed the arithmetic is byte-identical to the merged `journal.get_all` pattern. The added test (`tests/test_intel_predictions_active_pagination.py`) is self-contained (stubs `api.auth`/`api.dependencies`, fakes the engine) and will run in CI. If you need to actually execute pytest locally: `pip install fastapi pydantic sqlalchemy loguru pytest ruff pandas numpy python-jose passlib psycopg2-binary` then `pip install --force-reinstall cffi` (per 2026-05-19 entry's cffi-panic note).

---
## 2026-05-19 23:08 UTC — 2026-05-19-2308
**Why this matters next run:** Line 40 (`oracle.get_predictions` has_more) is now closed by PR #240. Three sibling has_more items remain on the 2026-05-17 API audit — same one-key-add shape, very small PRs:
- line 41 — `api/routers/models.py:67` `get_all`
- line 42 — `api/routers/intel.py:252` `intel_search` (this one is the bigger of the three — the `_ok(...)` envelope returns NO `total`/`limit`/`offset`/`has_more` at all, so it's an "add all 4 keys" not just "add 1")
- line 43 — `api/routers/intel.py:1384` `intel_predictions_active` (just add `has_more` to existing `meta`)

Then the remaining un-claimed 2026-05-17 audit items (unchanged from 2026-05-18 entry):
- line 44 [P1] — direct test coverage for `api/routers/system.py` (1,686-LOC, tests-only, bigger lift)
- line 45 [P1] — switch `prediction_backtest.py:20` import `get_engine` → `get_db_engine`
- line 46 [P2] — `clear_singletons()` doesn't actually clear `db._engine` (real bug, dep-injection plumbing)
- line 47 [P2] — f-string SQL in `prediction_backtest.py:116`

**Env gotcha discovered this run — copy the fix into your prelude:** the sandbox's `cryptography` install panics with `_cffi_backend` ModuleNotFoundError when `api.auth` is imported (transitive via `python-jose`). The `try: import api.auth except: stub` pattern from `tests/test_intelligence_search.py` does NOT catch this — `pyo3_runtime.PanicException` extends `BaseException`, not `Exception`. Fix: run `pip install --quiet --force-reinstall cffi` (the system `cryptography` 41.0.7 is fine, the `cffi` shared lib is the broken half). After that, `import api.routers.oracle` works and tests collect cleanly. Total deps for this kind of has_more PR: `pip install --quiet fastapi pydantic sqlalchemy loguru pytest ruff psycopg2-binary python-jose passlib numpy pandas && pip install --quiet --force-reinstall cffi` (~ 45s).

**Pattern that worked this run:** for `oracle.get_predictions` (and likely `models.get_all`), use `patch.object(mod, "get_db_engine", return_value=...)` with a `MagicMock` whose `engine.connect().__enter__` yields a `conn` with `conn.execute.side_effect = [count_result, rows_result]`. **Important:** oracle.py line 124 opens a SECOND `engine.connect()` for `tracking_pnl` lookup when `verdict == "pending"` — set the fake row's `verdict` to `"hit"` or `"miss"` (column index 17) so that branch stays inert and you only need 2 entries in the side_effect list. Same trap probably exists in any router that does conditional sub-queries inside the result-formatting loop.

**Don't bundle has_more siblings.** The orchestrator's hard rule is one PR per run.

**Env (unchanged):** no python deps preinstalled; `gh` CLI absent — `mcp__github__*` only; `git push origin routine-bookkeeping` → HTTP 403, use `mcp__github__create_or_update_file` against that branch; `mcp__github__list_pull_requests` output truncates — use `search_pull_requests` or scope by branch.

---
## 2026-05-18 23:03 UTC — 2026-05-18-2303
**Why this matters next run:** The 2026-05-14 + 2026-05-15 "queue exhausted / no-work" entries are STALE. A fresh API audit (PUNCH-LIST-2026-05-13.md, "Auditor 2026-05-17 — api/" section, lines 34-52) landed on main in PR #190 (commit `40e7e6b5`) and has **13 unclaimed items**. Walk that section before declaring no-work.

PR #239 closes line-39 [P1] (has_more in `search_intelligence`). Four other `has_more` items remain on the same auditor list, all the same shape and very small:
- line 40 — `api/routers/oracle.py:168` `get_predictions` *(done in PR #240, 2026-05-19-2308)*
- line 41 — `api/routers/models.py:67` `get_all`
- line 42 — `api/routers/intel.py:252` `intel_search` (the `_ok(...)` envelope, needs `total`+`limit`+`offset`+`has_more`)
- line 43 — `api/routers/intel.py:1384` `intel_predictions_active` meta (just add `has_more` to the existing `meta`)

The canonical pattern is `api/routers/journal.py:77` (`has_more: (offset + limit) < total`). Same one-key-add fix in each case. **Don't bundle these into one PR** — the orchestrator's hard rule is one PR per run; pick one and ship.

Other unclaimed 2026-05-17 audit items worth noting:
- line 44 [P1] — direct test coverage for `api/routers/system.py` (1,686-LOC router, only `/health` + `/status` tested in `tests/test_api.py:47`). Bigger lift; tests-only.
- line 45 [P1] — switch `prediction_backtest.py:20` import from `get_engine` → `get_db_engine` for clearable-singleton consistency.
- line 46 [P2] — `clear_singletons()` bug at `api/dependencies.py:67`: clears the api-level pointer but NOT `db._engine`, so next call returns a disposed engine. Real bug, but touches dep-injection plumbing — read both files first.
- line 47 [P2] — f-string SQL in `prediction_backtest.py:116` (rule-violation, currently-safe because values come from a literal). Same file as line 45; consider claiming both in sequence after line-45 lands.

**Env (unchanged from 2026-05-15 handoff, all still true):** no python deps preinstalled (`pip install pytest fastapi loguru sqlalchemy pydantic ruff` ~ 30s); `gh` CLI absent — `mcp__github__*` only; `git push origin routine-bookkeeping` → HTTP 403, use `mcp__github__create_or_update_file` against that branch; `mcp__github__list_pull_requests` output exceeds tool-result token cap — use `search_pull_requests` with tighter scope.

**Pattern that worked this run:** mocking `get_db_engine` via `monkeypatch.setattr(mod, "get_db_engine", lambda: <MagicMock>)` + a per-test `side_effect` list on `conn.execute` for count-then-search lets you regression-test the response envelope without PostgreSQL. Copy this pattern for the next four has_more PRs — it adds 3 tests in ~80 LOC and keeps them in the no-DB block of the existing test file.

---
## 2026-05-15 23:02 UTC — 2026-05-15-2302
**Why this matters next run:** Queue is STILL empty 24h after the 2026-05-14-2304 no-work. PUNCH-LIST-2026-05-13 is fully closed (#155-166 all merged), no new feed docs have landed, and the remaining oracle/engine.py architectural items (10-12, 2,793-LOC splits/refactors) need operator sign-off. Don't thrash — log no-work fast.

> **2026-05-18 update:** This is now stale — the 2026-05-17 API audit landed in PR #190 after this entry was written and added 13 new items to PUNCH-LIST-2026-05-13.md. See the 2026-05-18-2303 entry above.

Confirmed walk this run: TIER0 main HEAD `59e12375` (PR #173) is CI-green (Lint + claude-review + Frontend Build + Backend Tests all success). TIER1 `search_pull_requests author:app/openai-codex` returns 0 hits — the 3 `codex/*` branches (`agent-reporting-hub`, `astrogrid-dedup`, `edge-scanner-reload-guard`) have NO open PRs attached to them, so they aren't reviewable. TIER2/3 still no AUTO_IMPROVE/hermes/TOP docs. TIER4 PUNCH-LIST-2026-05-13 is fully closed except items 10-12 (architectural). TIER5/6 unchanged from 2026-05-14-2304.

Open-PR landscape (10 total): #170-172 + #174-175 are 3pacs cherry-picks/fixes (operator owns these — DO NOT touch); #133-139 are dependabot. No routine-claimable items.

**Carried-forward leave-alone list (still all true):** the standing `print()` → `log` fallback inside `intelligence/*.py` is EXHAUSTED — every remaining `print()` is in a `__main__` CLI block (verified 2026-05-14-2304); H9 mcp_server.py count is stale; DEV-NOTES H12 cache-dict migration items are also done.

**Env (unchanged):** no Python deps preinstalled (`pip install pytest loguru sqlalchemy ruff`, ~30s-2min); `gh` CLI absent — `mcp__github__*` only; `git push origin routine-bookkeeping` → HTTP 403, use `create_or_update_file`; `mcp__github__list_pull_requests` output exceeds tool-result token cap — parse the saved tool-result file with python, or use `search_pull_requests` with a tighter scope.

---
## 2026-05-14 23:24 UTC — 2026-05-14-2304
**Why this matters next run:** The routine queue is empty AND the standing print()->log fallback is now exhausted too — do NOT re-scan `intelligence/*.py` for print conversions, it's all CLI output.

> **2026-05-18 update:** API audit on 2026-05-17 added new work — see top entry. Other text below is still accurate context.

No-work run. Walked all 7 tiers: TIER0 main-CI fix is claimed by branch `claude/fix-backend-tests-main-ci`; TIER1 no codex-authored PRs (the 3 `codex/*` branches have no open PR); TIER2/3 no AUTO_IMPROVE/hermes/TOP docs; TIER4 PUNCH-LIST-2026-05-13 is fully worked — items 1-9,13,14 → PRs #156-166; items 10-12 are architectural `oracle/engine.py` splits/refactors, file-claimed by #156/#166 and need operator sign-off; TIER5 TODO-DUP-WRITES is blocked on an operator product decision (Step 1 picks A/B/C) and TODO-DATA-AUDIT needs DB access this box does not have; TIER6 no labeled issues.

**DEV-NOTES H10 print->log fallback is exhausted — verified, do not retry:** every remaining `print()` in `intelligence/*.py` sits inside an `if __name__ == "__main__"` CLI block (entity_resolver, rag, cross_reference, source_audit, sleuth, trust_scorer, market_diary all confirmed — legitimate CLI output, leave them). `ingestion/*.py` has zero non-CLI prints. `hypothesis_engine.py` still has 26 indented prints but is file-claimed by #155. Remaining print work would need a different dir (scripts/, dashboard.py, api/) — out of the handoff's intended scope and not clean routine material.

**H9 DEV-NOTES counts are stale:** DEV-NOTES lists "mcp_server.py (24)" swallowed exceptions — checked, all 28 except-blocks there already `log.debug(...)` or `return {error}`. Not an H9 target. Don't trust DEV-NOTES H9 file counts without grepping the file first.

**Bottom line:** unless #156/#157 have merged (and the operator has signed off on the engine.py split for items 10-12), or a fresh PUNCH-LIST / TOP / AUTO_IMPROVE doc has landed on main, expect another no-work. Log it fast — don't thrash hunting for cleanup.

**Env (carried forward, all still true):** no python deps preinstalled (`pip install ...` as needed, ~30s-2min); `gh` CLI absent — MCP `mcp__github__*` only; `git push origin routine-bookkeeping` → 403, use `create_or_update_file`; `mcp__github__list_pull_requests` / `search_pull_requests` output exceeds the tool-result limit — parse the saved tool-result file with python, or scope the query tighter.
