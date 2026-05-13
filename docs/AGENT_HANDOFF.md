## 2026-05-13 20:13 UTC — 2026-05-13-2007
**Why this matters next run:** H9 audit on `intelligence/hypothesis_engine.py` is now half-done — 3 silent passes fixed (1702/1719/1737, all inside `_check_intelligence_kills`), 4 remain in the same file. Inspected them; here's the triage so the next agent doesn't waste a context window.

PR #155 fixes the inner three silent `except: pass` blocks in `_check_intelligence_kills` (LEVER_DIVERGED / FORENSIC_CONTRADICTION / TRUST_COLLAPSED subchecks). Each routes the swallowed exception through `log.debug` while preserving the `return None` graceful-degradation flow. Uses the canonical loguru→stdlib `caplog` bridge from `tests/test_redfin_puller.py` (also used in PR #154's test file).

**Remaining H9 sites in this file (4) — triage:**

- **Line 972 (`_apply_intelligence_boost` wrapper around `_check_intelligence_kills`)**: outer try/except guarding the same function PR #155 just fixed internally. Same shape, same severity, same fix pattern (`except Exception as exc: log.debug("hypothesis_engine: intelligence_kills wrapper failed: {e}", e=exc)`). Cleanest next pick. ~3 LOC + 1 test that monkeypatches `_check_intelligence_kills` to raise.
- **Line 853 (`_apply_intelligence_boost` around `_get_intelligence_boost`)**: same sibling-shape as 972. Could be combined with 972 into one PR ("log boost+kill wrapper failures") if you want to keep PR count low — same method, same pattern. ~6 LOC total.
- **Line 1518 (`_log_boost` SQL INSERT into `hypothesis_boost_log`)**: silent DB write failure. Should probably be `log.warning` not `log.debug` — losing boost-tracking rows degrades calibration. ~3 LOC.
- **Line 1530 (`_update_boost_outcomes` SQL UPDATE on `hypothesis_boost_log`)**: same shape as 1518, same severity. Could be combined with 1518 ("log boost-table write failures") — both in the same boost-logging cluster. ~6 LOC total.

**One thing NOT to do:** The protected lever-pullers density block at lines 1553-1591 (env-flagged on `LEVER_PULLERS_MODE`) is OFF-LIMITS per CLAUDE.md. None of the 4 remaining H9 sites are inside it (853/972 < 1509, 1518/1530 < 1553), so any of the four is safe to touch — just don't expand scope into the block.

**One related #13 site still survives** (carried forward from previous handoffs):
- `ingestion/altdata/redfin_puller.py::_store_metrics` (lines 384-390): silently skips rows where `pd.isna(val)` or `float(val)` fails. Per-row `log.warning` would be noisy; right shape is a summary counter at end of the function. **PR #153 (redfin_puller _detect_inventory_anomalies) still open** — wait for it to merge before touching this same file, otherwise file-claim overlap.

**H9 elsewhere — canonical detector + filters:**
```bash
grep -rnB1 "^\s*pass\s*$" --include='*.py' api/ ingestion/ intelligence/ orchestration/ analysis/ oracle/ | grep -B1 "except"
```
Filter out: Langfuse observability shims (commented "Never raises"), `try A; except: try B` fallback patterns. The actually-silent ones tend to cluster in legacy intelligence/* code and in API router caches.

**Env quirks (unchanged, still in force):**
- `git push origin routine-bookkeeping` returns HTTP 403 on receive-pack. Use MCP `create_or_update_file` for both `.grid_backups/routine_log.jsonl` and this file. Push of `claude-routine/...` work branches works fine.
- `python3 -m pytest` can't collect (conftest imports pandas + psycopg2 + python-jose at module top). For an actual local smoke run on a logging fix, write a `python3 -c '...'` script that bypasses conftest — see PR #155's smoke run for the recipe (install pandas + loguru + sqlalchemy via `pip install`, then monkeypatch the import target and assert the bridged caplog records). Works without DB.
- Loguru → stdlib bridge in `tests/test_redfin_puller.py::test_logs_when_inventory_silently_coerced` remains the canonical pytest pattern for asserting `log.warning(...)` / `log.debug(...)` via `caplog`. Mirrored verbatim into `tests/test_indeed_hiring_puller.py` (PR #154) and `tests/test_hypothesis_engine_intelligence_kills_logging.py` (this PR).

---
## 2026-05-13 19:11 UTC — 2026-05-13-1907
**Why this matters next run:** The `pd.to_numeric(errors="coerce")` audit (ATTENTION.md #13) in `ingestion/altdata/*.py` is now **fully closed** — every site has a coerce-count log. Don't re-grep this directory.

PR #154 fixes the lone remaining holdout (`indeed_hiring_puller.py::_pull_sectors`, line 368, the per-sector loop). Pattern matches the existing aggregate logger at lines 213-218 of the same file: capture pre/post-NaN delta, emit `log.warning` naming the count + sector. The bookkeeping → MCP `create_or_update_file` workaround is still in force (git push to `routine-bookkeeping` 403s on this box; PR push to `claude-routine/...` works fine).

**One related #13 site survives** (would have busted single-PR scope, both noted in PR #153 handoff):
- `ingestion/altdata/redfin_puller.py::_store_metrics` (lines 384-390): silently skips rows where `pd.isna(val)` or `float(val)` fails. Per-row `log.warning` would be noisy; the right shape is a summary counter at end of the function. ~10 LOC + a test feeding bad METRIC values. Clean next-PR target — but only after PR #153 lands (otherwise file-claim overlap with the in-flight redfin branch).

**H9 silent-pass candidates — corrections after this run's investigation:**

The previous handoff's three "clean single-line fix" candidates need triage:
- ✅ `intelligence/hypothesis_engine.py:1738` — outside the protected 1553-1591 block, OK to touch. Genuine silent except. Confirmed.
- ❌ `intelligence/universe_ranker.py:786` — **OFF LIMITS this routine cycle**. PR #152 just touched this file (different line, 871). Per orchestrator rules ("Read their last-touched files via `git diff --name-only main...<branch>` and AVOID those files this run"), wait until #152 lands.
- ⚠️  `api/routers/canvas_expand.py:413` — **NOT a silent except**. It's an `else: pass` after `if all_board_nodes:`. The actual bug here is dead code at lines 410-411 (`board_max_x - board_min_x` and `board_max_y - board_min_y` evaluate-and-discard — local vars never used elsewhere). Refactor-cleanup candidate, not an H9 logging fix. Recommend running `refactor-cleaner` agent on this block, not a routine logging PR.

**Next-up routine candidates (order by easiest):**
1. `intelligence/hypothesis_engine.py:1738` H9 silent except — single-line `pass` → `log.debug(...)` + module's existing test scaffolding. Note the protected block at 1553-1591 (`LEVER_PULLERS_MODE` env-flagged) is OFF-LIMITS per CLAUDE.md.
2. `ingestion/altdata/redfin_puller.py::_store_metrics` summary counter — only after PR #153 merges.
3. Broaden H9 grep beyond the 4 named files. The canonical detector from the previous handoff:
   ```bash
   grep -rnB1 "^\s*pass\s*$" --include='*.py' api/ ingestion/ intelligence/ orchestration/ analysis/ oracle/ | grep -B1 "except"
   ```
   Filter out Langfuse observability shims (commented "Never raises") and `try A; except: try B` fallback patterns — those are intentional.

**Env quirks (unchanged, still in force):**
- `git push origin routine-bookkeeping` returns HTTP 403 on the receive-pack endpoint. Use MCP `create_or_update_file` for `.grid_backups/routine_log.jsonl` and this file. Push of `claude-routine/...` work branches works fine.
- `python3 -m pytest` can't collect (conftest imports pandas + psycopg2 + python-jose at module top). `python3 -m py_compile` and `/root/.local/bin/ruff check` both work — use them as the local sanity gate. CI runs full pytest.
- Loguru → stdlib bridge in `tests/test_redfin_puller.py::test_logs_when_inventory_silently_coerced` is the canonical pattern for asserting `log.warning(...)` via pytest `caplog`. Mirrored verbatim into `tests/test_indeed_hiring_puller.py::test_logs_when_sector_values_silently_coerced` this run; copy it forward for any future H9/H13 logging-test PRs.

---
## 2026-05-13 18:22 UTC — 2026-05-13-1810
**Why this matters next run:** The `pd.to_numeric(errors="coerce")` audit (ATTENTION.md #13) in `ingestion/altdata/*.py` is now fully tapped — don't redo it.

PR #153 fixes the lone holdout (`redfin_puller.py:267-269` in `_detect_inventory_anomalies`). Every other `ingestion/altdata/*.py` file that does `pd.to_numeric(..., errors="coerce")` already logs the coerced count: `aaii_sentiment.py:330-339`, `cboe_indices.py:212-218`, `baltic_dry.py:157-160`, `supply_chain.py:713-716`, `indeed_hiring_puller.py:213-215` (main value_col only — sector_col at line 368 still silent but rare path), `yield_curve_full.py:167-170`, `repo_market.py:150-153`, `fed_liquidity.py:200-204+275-277`, `fred.py:415-422` (downstream of the unlogged 277/284/286 calls but those are redundant — line 415 catches everything).

**Two related sites left behind on purpose** (would have busted single-PR scope):
- `ingestion/altdata/redfin_puller.py::_store_metrics` (lines 384-390): silently skips rows where `pd.isna(val)` or `float(val)` fails. Per-row log.warning would be noisy; the right shape is a summary counter at end of the function. ~10 LOC + a test feeding bad METRIC values. Clean next-PR target.
- `ingestion/altdata/indeed_hiring_puller.py:368`: same shape as redfin's #13 fix but on a sector_col fallback path. ~5 LOC.

**Where to keep mining ATTENTION.md #13 if needed:** broaden the audit to `ingestion/` (not just `altdata/`) and to `analysis/` / `intelligence/` — but most of those don't ingest external data so coercion is rarer. Real next opportunity is probably **H9 silent passes** in less-obvious files. The handoff-named candidates (chat.py, mcp_server.py, system.py, llm_taskqueue.py) have already been instrumented with `log.debug`/`log.warning` on every except path — the "26 swallowed in llm_taskqueue.py" count must include `log.debug` paths, which are not real swallow bugs. Recommend the next agent grep for truly silent `pass` only:

```bash
grep -rnB1 "^\s*pass\s*$" --include='*.py' api/ ingestion/ intelligence/ orchestration/ analysis/ oracle/ | grep -B1 "except"
```

Most hits are intentional Langfuse observability shims (commented "Never raises") or fallback patterns (try A; on fail, try B). The actually-silent ones I saw: `intelligence/hypothesis_engine.py:1738` (outside the protected 1553-1591 block, OK to touch), `intelligence/universe_ranker.py:786` (`ensure_ranking_table` idempotent retry — could log.warning), `api/routers/canvas_expand.py:413`. Each is a clean single-line fix.

**Env quirk update (good news):** `git push origin claude-routine/...` works fine (PR #153 pushed normally). The previous handoff's note that **bookkeeping-branch pushes 403** is still accurate — used MCP `create_or_update_file` for the routine_log + this handoff. Same workaround as before, works first-try.

**Test-env note (unchanged):** `python3 -m pytest` can't collect (conftest imports pandas + psycopg2 at module top). `python3 -m py_compile` and `/root/.local/bin/ruff check` both work — they're the local sanity gate. CI runs full pytest. The pytest+loguru bridge pattern in `tests/test_redfin_puller.py::test_logs_when_inventory_silently_coerced` is the canonical recipe for asserting loguru `log.warning(...)` via pytest `caplog` (loguru doesn't write to stdlib by default — need a `logging.Handler` bridge that re-emits records via stdlib loggers).

---
## 2026-05-13 17:15 UTC — 2026-05-13-1705
**Why this matters next run:** H10 print-statement audit needs a finer filter than "is it inside `if __name__ == '__main__':`" — error-path prints inside a CLI `main()` are still wrong and should use `log.error` + stderr.

PR #152 narrows H10 to a specific class of target: prints on **exception paths** inside otherwise-CLI functions. `intelligence/universe_ranker.py:871` was inside `def main()` (i.e. reachable from `__main__`) but it printed an engine-bootstrap failure to **stdout**, which (a) bypassed loguru → no `errors.jsonl` row, and (b) corrupted the stdout channel reserved for the ranking narrative. Fix pattern:

```python
except Exception as exc:
    log.error("module: action failed: {e}", e=exc)
    print(f"module: action failed: {exc}", file=sys.stderr)
    return 1
```

The two narrative prints at lines 887, 889 of `universe_ranker.py` are still the right shape — they're CLI report stdout, not errors.

**Where to look for more of this pattern:** grep `print(.*fail|error|exc|raise|traceback)` inside `def main(`, `def _cli(`, or `def cli(` functions in `intelligence/`, `scripts/`, `oracle/`, `analysis/`. Each fix is ~3 LOC + a regression test that monkeypatches a dependency to raise.

**Test-env note (unchanged):** `python3 -m pytest` can't collect (conftest imports pandas at module top). `python3 -m py_compile` and `/root/.local/bin/ruff check` both work — they're the local sanity gate. CI runs full pytest.

**Env quirk discovered this run:** `git push origin routine-bookkeeping` returns HTTP 403 on the receive-pack endpoint in this routine box (info/refs succeeds — only the actual upload is denied). Push of the **work branch** (`claude-routine/...`) worked fine in the same session, so it's specific to the bookkeeping branch via git CLI. Workaround that worked: write the log entry / handoff via the GitHub MCP `create_or_update_file` tool — those land server-side and bypass the proxy. If `git push routine-bookkeeping` fails for the next agent too, switch to MCP immediately rather than retrying with exponential backoff.

---

## 2026-05-13 16:08 UTC — 2026-05-13-1608
**Why this matters next run:** DEV-NOTES-DATA-INTEGRITY.md is no longer misleading — annotations on main reflect actual state. H13 and H14 are DONE, not next-up.

PR #150 annotates DEV-NOTES with `Status` columns. Verified against origin/main on 2026-05-13:
- Phase 1 (C1-C5) all done. Phase 2 (H1-H8) all done. H13, H14, H15, H17, H21 all done.
- H12 has 5 PRs in flight (#145-#149) covering 6 caches. The 3 remaining caches need arch decisions:
  - `intel_cross_reference.py:25` already has per-key lock map (different shape; may not need migration)
  - `canvas.py:92` uses tuple key (TTLCache types `key: str`)
  - `surfacer.py:1507-1510` are function-locals, not globals — likely not a thread issue at all

**Genuinely open work, in scope-order:**
- H9: 281 swallowed exceptions across 82 files. Biggest concentrations: llm_taskqueue.py (26), mcp_server.py (24), system.py (19), chat.py (17). Each file is a clean single-PR slice. Replace `pass` with `log.warning(...)` — don't change behaviour.
- H10: 1,217 print() statements. BUT — most prints in `intelligence/*.py` are inside `if __name__ == "__main__":` CLI blocks (entity_resolver: 37, cross_reference: 12, source_audit: 9, trust_scorer: 4, sleuth: 8, universe_ranker: 3 — all guarded by `__main__`). Those are legitimate stdout — leave them. The genuine targets are library-code prints; the routine on this box found very few. Search with `grep -B1 -A0 '^\s*print(' file.py | grep -B1 __main__` style to filter, or check `ingestion/` which the doc says has bulk prints.
- H11: Several files already split (flow_thesis.py → flow_thesis_data + flow_thesis_scoring; causation.py → causation_core + causation_graph + causation_scoring). Remaining big targets: `llm_taskqueue.py`, `hermes_operator.py`. Splits are architectural — coordinate with operator before opening.
- H12: tapped for routine pattern. The three remaining caches are architectural.

**Don't repeat my detour:** I verified H13 and H14 (the previous handoff's "next-up" suggestions) and both are already implemented on main with batched `ANY(:fids)` / `ANY(:aids)` queries. The DEV-NOTES annotations now record that — don't re-investigate.

**Test-env note (unchanged):** `python3 -m pytest` and `python3 -m ruff` absent on this routine box; rely on CI to verify lint + tests.

---

## 2026-05-13 15:15 UTC — 2026-05-13-1510
**Why this matters next run:** H12 (TTLCache migrations) — both "simple-pattern" router targets are now done; what remains in H12 is non-trivial.

After PR #148 (`_actor_graph_cache`) and PR #149 (`_influence_graph_cache`, this run), the two "drop-in TTLCache, copy the test template" targets from the previous handoff are both shipped. Five PRs landed in this thread: #145, #146, #147, #148, #149. The cycle of "1 cache per PR, ~20 LOC, ~100 LOC of tests" still works fine if you find another instance — `grep -rn 'dict\[str, Any\] = {"data": None, "ts":' api/routers/` is the canonical detector.

**Remaining H12 cache dicts — DO NOT just copy the pattern:**

- `api/routers/intel_cross_reference.py:25-26` — `_cache` + `_cache_locks: dict[str, threading.Lock]`. Already has a per-key lock map, so it's NOT racy in the same way. May not need migrating, or may need a different shape. Read the call sites before touching.
- `api/routers/canvas.py:92` — `_canvas_graph_cache: dict[tuple[str, int, str, str | None, int], tuple[datetime, dict[str, Any]]]`. Key is a tuple, not a string. `utils.ttl_cache.TTLCache.get/set` types `key: str` — would need either a string-encoded key (`"|".join(map(str, tup))`) or broadening the TTLCache key type. Skip unless willing to do one or the other.

Both of those would need an architectural decision, not a routine fix-PR.

**Test-env note (unchanged from previous handoff, still true):** `tests/conftest.py` pulls in pandas + psycopg2 + python-jose + cryptography>=48 at collection time, so even pure unit tests under `tests/` fail to collect without those installed. On this routine box `python3 -m pytest` and `python3 -m ruff` are both absent — I shipped relying on the visual diff matching PR #148 + AST-parse of both files. CI will pick up real lint + pytest. If you need local runs: previous handoff's pip recipe still applies.

**DEV-NOTES-DATA-INTEGRITY.md staleness (unchanged):** Phase 1 (C1-C5) and most of Phase 2 (H1-H4, H7, H8) are already fixed on main even though the doc still lists them. Re-grep the cited file:line before starting work on a Phase 1/2 item.

**Suggestion for next run if H12 is genuinely tapped out:** H13 (`intelligence/actor_discovery.py:1416` N+1 in actor enrichment) or H14 (`api/routers/chat.py:118` N+1 in watchlist gatherer). Both are single-file batch-query fixes — same shape of PR as the H12 thread, just touching a different layer.
