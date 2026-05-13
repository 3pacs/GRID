## 2026-05-13 13:04 UTC — 2026-05-13-1304
**Why this matters next run:** H12 (TTLCache migrations) — two simple targets remain after PR #147.

After PR #145 (`_thesis_cache`), PR #146 (`_lever_cache`), and PR #147 (`_risk_map_cache` + `_globe_cache`), the remaining simple `dict[str, Any] = {"data": None, "ts": ...}` cache dicts in `api/routers/` that can use the same TTLCache pattern verbatim:

- `api/routers/intelligence_forensics.py:299` — `_influence_graph_cache`
- `api/routers/intelligence_actors.py:24` — `_actor_graph_cache`

Each is one PR, ~20 LOC delta. Mirror PR #147's regression-test layout — `tests/test_intelligence_risk_cache.py` is the closest template (single-key cache, no DB calls in the test). The pattern: declare the TTL constant first, then the TTLCache with `max_size=1`, then a `_*_CACHE_KEY = "default"` (or domain-appropriate) constant; replace the inline `from datetime import...` + `(now - ts).total_seconds() < TTL` logic with `_cache.get(KEY)` / `_cache.set(KEY, result)`.

Non-trivial cases (still untouched — do NOT just copy the pattern — verify first):

- `api/routers/intel_cross_reference.py:25-26` — `_cache` + `_cache_locks: dict[str, threading.Lock]`. This file already has a per-key lock map, so it's NOT racy in the same way as the others. May not need migrating, or may need a different shape. Read the call sites before touching.
- `api/routers/canvas.py:92` — `_canvas_graph_cache: dict[tuple[str, int, str, str | None, int], tuple[datetime, dict[str, Any]]]`. Key is a tuple, not a string. `utils.ttl_cache.TTLCache.get/set` types `key: str` — would need either a string-encoded key or a small `TTLCache` generic. Skip unless willing to broaden the cache's key type.

Test-env note: `tests/conftest.py` requires pandas + psycopg2 + python-jose + cryptography>=48 at collection time, so even pure unit tests under `tests/` fail to collect without those installed. If your routine box has `python3 -m pip` available, `pip install pandas pytest fastapi loguru sqlalchemy pydantic-settings psycopg2-binary python-jose passlib bcrypt` plus `pip install --ignore-installed cffi cryptography` was enough to run the new `test_intelligence_risk_cache.py` (the existing debian-shipped cryptography 41.0.7 is too old for `python-jose`).

DEV-NOTES-DATA-INTEGRITY.md Phase 1 (C1-C5) and most of Phase 2 (H1-H4, H7, H8) are already fixed on main even though the doc still lists them. Re-grep the cited file:line before starting work on a Phase 1/2 item — the line numbers are stale and the fix is usually already there. The doc itself is overdue for a refresh, but that requires reading each callout against current code and is a bigger task than a single routine PR.
