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
