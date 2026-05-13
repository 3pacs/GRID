## 2026-05-13 12:06 UTC — 2026-05-13-1206
**Why this matters next run:** H12 (TTLCache migrations) — easy follow-ups available, plus one subtle non-trivial case.

After PR #145 (`_thesis_cache`) and PR #146 (`_lever_cache`), the remaining `dict[str, Any] = {"data": None, "ts": ...}` style cache dicts in `api/routers/` that follow the same simple shape and can use the PR #145/#146 pattern verbatim:

- `api/routers/intelligence_risk.py:20` — `_risk_map_cache`
- `api/routers/intelligence_risk.py:788` — `_globe_cache` (same file, two caches — do them in one PR)
- `api/routers/intelligence_forensics.py:299` — `_influence_graph_cache`
- `api/routers/intelligence_actors.py:24` — `_actor_graph_cache`

Non-trivial cases (do NOT just copy the pattern — verify first):

- `api/routers/intel_cross_reference.py:25-26` — `_cache` + `_cache_locks: dict[str, threading.Lock]`. This file already has a per-key lock map, so it's NOT racy in the same way as the others. May not need migrating, or may need a different shape. Read the call sites before touching.
- `api/routers/canvas.py:92` — `_canvas_graph_cache: dict[tuple[str, int, str, str | None, int], tuple[datetime, dict[str, Any]]]`. Key is a tuple, not a string. `utils.ttl_cache.TTLCache.get/set` types `key: str` — would need either a string-encoded key or a small `TTLCache` generic. Skip unless willing to broaden the cache's key type.

DEV-NOTES-DATA-INTEGRITY.md Phase 1 (C1-C5) and most of Phase 2 (H1-H4, H7, H8) are already fixed on main even though the doc still lists them. Re-grep the cited file:line before starting work on a Phase 1/2 item — the line numbers are stale and the fix is usually already there. The doc itself is overdue for a refresh, but that requires reading each callout against current code and is a bigger task than a single routine PR.
