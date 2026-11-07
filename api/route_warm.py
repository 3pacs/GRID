"""Build FastAPI's per-route request contexts before the first request.

FastAPI 0.139 builds each included router's "effective route contexts"
(dependency analysis, pydantic TypeAdapters) lazily, the first time a request
is matched against that router, and it does so on the event loop. With ~100
routers and ~1,500 routes, the first request to reach a cold router stalls
every request for 2-6 s: 35 of the 36 event-loop stalls grid-api's watchdog
recorded over 48 h to 2026-11-07 had this stack
(``_IncludedRouter.effective_candidates`` -> ``_build_effective_context``).

:func:`warm_route_contexts` walks the include tree once and asks each
included router for its candidates, which FastAPI caches (keyed by the
routes version) on the same objects request matching later uses. Run it off
the event loop at startup. It touches private FastAPI API, so it is written to
do nothing, not raise, if those internals change.
"""

from __future__ import annotations

from typing import Any

from loguru import logger as log


def warm_route_contexts(router: Any) -> int:
    """Pre-build every included router's route contexts; return how many were built."""
    try:
        from fastapi import routing as fastapi_routing
    except Exception:  # noqa: BLE001
        return 0
    included_cls = getattr(fastapi_routing, "_IncludedRouter", None)
    if included_cls is None:
        return 0

    built = 0
    seen: set[int] = set()
    stack = [r for r in getattr(router, "routes", []) if isinstance(r, included_cls)]
    while stack:
        included = stack.pop()
        if id(included) in seen:
            continue
        seen.add(id(included))
        try:
            candidates = included.effective_candidates()
            included.effective_low_priority_routes()
        except Exception as exc:  # noqa: BLE001 -- warming is an optimisation only
            log.warning("route context warm-up stopped: {e}", e=str(exc))
            return built
        for candidate in candidates:
            if isinstance(candidate, included_cls):
                stack.append(candidate)
            else:
                built += 1
    return built
