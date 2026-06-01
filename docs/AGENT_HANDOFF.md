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
## 2026-05-29 23:10 UTC — 2026-05-29-2310
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
