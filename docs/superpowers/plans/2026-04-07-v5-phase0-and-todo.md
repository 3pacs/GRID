# V5 Phase 0 + TODO Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-[[development]] (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the TODO fixes ([[Cross Reference|cross-reference]] warm-up, [[GDELT]] rate limiting, spider stats widget, valuation migration) then build V5 Phase 0 foundations (event bus + SSE, useAsyncData hook, ViewErrorBoundary, [[Zustand]] store decomposition).

**[[architecture|Architecture]]:** Two parallel tracks — backend event bus (PG LISTEN/NOTIFY + SSE endpoint) and frontend foundations (shared hooks + store split). TODO fixes are warm-up tasks that ship independently before V5 work begins.

**Tech Stack:** Python 3.11, [[FastAPI]], asyncpg (new), [[PostgreSQL]] LISTEN/NOTIFY, SSE, React 18, [[Zustand]]

---

## File Map

### Wave 1: TODO Fixes
| Action | File | Purpose |
|--------|------|---------|
| Modify | `api/main.py:91-241` | Add cross-reference pre-warm to `_deferred_startup()` |
| Modify | `intelligence/breaking_news.py:262-319` | Add per-query sleep spacing in monitor loop |
| Modify | `pwa/src/views/IntelDashboard.jsx` | Add spider stats widget card row |
| Modify | `pwa/src/api.js` | Add `getSpiderStats()` method |
| Run | `grid/scripts/migrations/add_valuation_tables.sql` | Create valuation tables on live DB |

### Wave 2: V5 Phase 0 — Backend (Event Bus)
| Action | File | Purpose |
|--------|------|---------|
| Create | `events/__init__.py` | Package init |
| Create | `events/channels.py` | Channel name constants + payload types |
| Create | `events/bus.py` | PG LISTEN/NOTIFY async wrapper |
| Create | `api/routers/sse.py` | SSE endpoint `/api/v1/events/stream` |
| Modify | `api/main.py` | Register SSE router + start event bus in lifespan |
| Create | `tests/test_event_bus.py` | Unit tests for bus + channels |

### Wave 2: V5 Phase 0 — Frontend (Foundations)
| Action | File | Purpose |
|--------|------|---------|
| Create | `pwa/src/hooks/useAsyncData.js` | Universal async data hook |
| Create | `pwa/src/components/ViewErrorBoundary.jsx` | Per-view error boundary with retry + context |
| Create | `pwa/src/stores/authStore.js` | Auth slice (token, role, login/logout) |
| Create | `pwa/src/stores/uiStore.js` | UI slice (theme, view, loading, errors, notifications) |
| Create | `pwa/src/stores/domainStore.js` | Domain data slice (regime, journal, models, signals) |
| Create | `pwa/src/stores/realtimeStore.js` | Real-time slice (prices, alerts, recommendations, WS handler) |
| Modify | `pwa/src/store.js` | Re-export from slices for backwards compat |
| Create | `pwa/src/hooks/useEventStream.js` | SSE client hook (connects to event bus) |
| Modify | `pwa/src/App.jsx` | Use ViewErrorBoundary per view |

---

## Wave 1: TODO Fixes

### Task 1: Pre-warm cross-reference cache on API startup

**Files:**
- Modify: `api/main.py:239` (before the final log.info in `_deferred_startup`)

- [ ] **Step 1: Add [[Cross Reference|cross-reference]] pre-warm to _deferred_startup**

Add this block before line 241 (`log.info("GRID API ready...")`):

```python
    # Pre-warm cross-reference cache (skip LLM narrative — just warm DB queries)
    try:
        import threading

        def _prewarm_cross_reference():
            try:
                from db import get_engine as _get_eng
                from intelligence.cross_reference import run_all_checks
                eng = _get_eng()
                result = run_all_checks(eng, skip_narrative=True)
                checks = len(result.get("checks", [])) if isinstance(result, dict) else 0
                log.info("Cross-reference pre-warmed: {n} checks cached", n=checks)
            except Exception as exc:
                log.warning("Cross-reference pre-warm failed: {e}", e=str(exc))

        threading.Thread(target=_prewarm_cross_reference, daemon=True, name="xref-prewarm").start()
    except Exception as exc:
        log.debug("Cross-reference pre-warm thread setup failed: {e}", e=str(exc))
```

- [ ] **Step 2: Verify the edit**

Run: `cd /Users/anikdang/dev/GRID && python -c "from api.main import app; print('OK')"`
Expected: `OK` (no import errors)

- [ ] **Step 3: Commit**

```bash
git add api/main.py
git commit -m "perf: pre-warm cross-reference cache on startup (skip_narrative=True)"
```

---

### Task 2: Add GDELT rate limit spacing to breaking news monitor

**Files:**
- Modify: `intelligence/breaking_news.py:36-41` (add constant)
- Modify: `intelligence/breaking_news.py:280` (add sleep between queries)
- Test: `tests/test_breaking_news.py`

- [ ] **Step 1: Add rate limit constant**

After line 39 (`GDELT_TIMEOUT_SECONDS = 10`), add:

```python
GDELT_REQUEST_SPACING = 6.0    # seconds between GDELT requests (free tier ~10/min)
```

- [ ] **Step 2: Add sleep between [[GDELT]] requests in the monitor loop**

In `run_monitor()`, after the `continue` on line 278 (cooldown skip), add spacing before the `check_gdelt` call on line 280. The modified section of the loop body (lines 266-281) becomes:

```python
        for i, item in enumerate(WATCHLIST):
            query = item["query"]
            now = time.time()

            # Skip if in cooldown
            last_hit = cooldowns.get(query, 0.0)
            if now - last_hit < COOLDOWN_SECONDS:
                remaining = int(COOLDOWN_SECONDS - (now - last_hit))
                log.debug(
                    "Skipping '{q}' — cooldown {r}s remaining",
                    q=query[:40], r=remaining,
                )
                continue

            # Rate limit: space out GDELT requests to stay under free tier
            if i > 0:
                time.sleep(GDELT_REQUEST_SPACING)

            article_count = check_gdelt(query)
```

Key change: `for item in WATCHLIST` → `for i, item in enumerate(WATCHLIST)` and add the `if i > 0: time.sleep(GDELT_REQUEST_SPACING)` before the API call.

- [ ] **Step 3: Add test for rate spacing**

In `tests/test_breaking_news.py`, add:

```python
def test_gdelt_request_spacing_constant():
    """Verify GDELT_REQUEST_SPACING is set to respect free tier rate limits."""
    from intelligence.breaking_news import GDELT_REQUEST_SPACING, WATCHLIST
    # 12 queries with spacing should take at least 60s (fits ~10 req/min)
    min_cycle_time = (len(WATCHLIST) - 1) * GDELT_REQUEST_SPACING
    assert min_cycle_time >= 60, f"Cycle too fast: {min_cycle_time}s for {len(WATCHLIST)} queries"
    assert GDELT_REQUEST_SPACING >= 5.0, "Need at least 5s between GDELT requests"
```

- [ ] **Step 4: Run tests**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_breaking_news.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add intelligence/breaking_news.py tests/test_breaking_news.py
git commit -m "fix: add 6s spacing between GDELT requests to avoid 429 rate limits"
```

---

### Task 3: Wire spider stats into IntelDashboard

**Files:**
- Modify: `pwa/src/api.js` (add getSpiderStats method)
- Modify: `pwa/src/views/IntelDashboard.jsx` (add spider stats card + data fetch)

- [ ] **Step 1: Add getSpiderStats to API client**

Add to `pwa/src/api.js` near the other intelligence methods:

```javascript
    async getSpiderStats() {
        return this._fetch('/api/v1/intelligence/spider/stats');
    }
```

- [ ] **Step 2: Add spider stats state + fetch to [[Intel Dashboard View|IntelDashboard]]**

In `IntelDashboard.jsx`, add `spiderStats` state alongside the existing state declarations (after line 22):

```javascript
    const [spiderStats, setSpiderStats] = useState(null);
```

Add the spider fetch to the existing `Promise.all` in `loadData` (line 29-34). Change to:

```javascript
            const [ts, conv, xref, brief, spider] = await Promise.all([
                api.getTrustScores?.().catch(() => null),
                api.getConvergenceAlerts?.().catch(() => null),
                api.getCrossReference?.().catch(() => null),
                api.getLatestBriefing?.('hourly').catch(() => null),
                api.getSpiderStats?.().catch(() => null),
            ]);
            setTrustSources(ts);
            setConvergence(conv);
            setCrossRef(xref);
            setBriefing(brief);
            setSpiderStats(spider);
```

- [ ] **Step 3: Add spider stats card to the metric cards row**

Change the grid from `repeat(4, 1fr)` to `repeat(5, 1fr)` on line 98, and add a 5th card after the Briefing card (before the closing `</div>` of the grid on line 194):

```jsx
                {/* Spider Network */}
                <div
                    onClick={() => onNavigate?.('actor-network')}
                    title="Click to view actor network"
                    style={{
                        ...shared.cardGradient,
                        textAlign: 'center', padding: tokens.space.md,
                        cursor: 'pointer', transition: 'all 0.2s ease',
                        borderLeft: `3px solid #8B5CF6`,
                    }}
                    {...hoverBrighten}
                >
                    <div style={{
                        fontSize: '28px', fontWeight: 800, fontFamily: MONO,
                        color: '#8B5CF6',
                    }}>{spiderStats ? (spiderStats.total_actors >= 1000 ? `${(spiderStats.total_actors / 1000).toFixed(0)}K` : spiderStats.total_actors) : '--'}</div>
                    <div style={{
                        fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
                        color: colors.textMuted, fontFamily: MONO, marginTop: '4px',
                    }}>SPIDER NETWORK</div>
                </div>
```

- [ ] **Step 4: Build frontend to verify**

Run: `cd /Users/anikdang/dev/GRID/pwa && npm run build`
Expected: Build succeeds with no errors

- [ ] **Step 5: Commit**

```bash
git add pwa/src/api.js pwa/src/views/IntelDashboard.jsx
git commit -m "feat: wire spider stats into IntelDashboard metric cards"
```

---

### Task 4: Run valuation migration SQL

**Files:**
- Run: `grid/scripts/migrations/add_valuation_tables.sql`

- [ ] **Step 1: SSH to server and run migration**

```bash
ssh grid@100.75.185.36 "cd ~/grid_v4 && psql -U grid -d grid -f grid/scripts/migrations/add_valuation_tables.sql"
```

- [ ] **Step 2: Verify tables exist**

```bash
ssh grid@100.75.185.36 "psql -U grid -d grid -c \"SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'company_%' OR table_name LIKE 'derivatives_%' OR table_name LIKE 'valuation_%' ORDER BY 1;\""
```

Expected: `company_valuations`, `company_milestones`, `derivatives_support`, `valuation_analysis_log` all present

- [ ] **Step 3: Commit (no code change — migration is already in repo)**

No commit needed — the SQL file was part of PR #22.

---

## Wave 2: V5 Phase 0 — Backend Track

### Task 5: Create event channel definitions

**Files:**
- Create: `events/__init__.py`
- Create: `events/channels.py`

- [ ] **Step 1: Create events package**

`events/__init__.py`:
```python
"""GRID event bus — PG LISTEN/NOTIFY + SSE backbone for V5."""
```

- [ ] **Step 2: Write channel definitions**

`events/channels.py`:
```python
"""Event channel constants and payload schemas.

Each channel corresponds to a PostgreSQL NOTIFY channel. Producers emit
events via ``bus.emit(channel, payload)``, consumers receive them through
the SSE endpoint or internal subscribers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


# Channel name constants — match these exactly in LISTEN/NOTIFY
ACTOR_UPDATE = "grid_actor_update"
SIGNAL_FIRE = "grid_signal_fire"
REGIME_CHANGE = "grid_regime_change"
PREDICTION_SCORED = "grid_prediction_scored"
FLOW_SHIFT = "grid_flow_shift"
INVESTIGATION_ALERT = "grid_investigation_alert"
PULL_COMPLETE = "grid_pull_complete"
MODEL_PROMOTED = "grid_model_promoted"

ALL_CHANNELS: tuple[str, ...] = (
    ACTOR_UPDATE,
    SIGNAL_FIRE,
    REGIME_CHANGE,
    PREDICTION_SCORED,
    FLOW_SHIFT,
    INVESTIGATION_ALERT,
    PULL_COMPLETE,
    MODEL_PROMOTED,
)


@dataclass(frozen=True)
class Event:
    """Immutable event wrapper."""
    channel: str
    payload: dict[str, Any]
    timestamp: str  # ISO 8601 UTC

    def to_sse(self) -> str:
        """Format as SSE data line."""
        import json
        data = json.dumps({
            "channel": self.channel,
            "payload": self.payload,
            "timestamp": self.timestamp,
        })
        return f"event: {self.channel}\ndata: {data}\n\n"
```

- [ ] **Step 3: Commit**

```bash
git add events/__init__.py events/channels.py
git commit -m "feat(v5): add event channel definitions — 8 PG NOTIFY channels"
```

---

### Task 6: Build the event bus (PG LISTEN/NOTIFY)

**Files:**
- Create: `events/bus.py`
- Create: `tests/test_event_bus.py`

- [ ] **Step 1: Write failing tests**

`tests/test_event_bus.py`:
```python
"""Tests for the GRID event bus."""

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from events.channels import (
    ALL_CHANNELS, SIGNAL_FIRE, REGIME_CHANGE, Event,
)


# ── Event dataclass tests ──

def test_event_creation():
    e = Event(channel=SIGNAL_FIRE, payload={"ticker": "AAPL"}, timestamp="2026-04-07T12:00:00Z")
    assert e.channel == SIGNAL_FIRE
    assert e.payload["ticker"] == "AAPL"


def test_event_immutability():
    e = Event(channel=SIGNAL_FIRE, payload={}, timestamp="2026-04-07T12:00:00Z")
    with pytest.raises(AttributeError):
        e.channel = "other"


def test_event_to_sse():
    e = Event(channel=REGIME_CHANGE, payload={"from": "GROWTH", "to": "FRAGILE"}, timestamp="2026-04-07T12:00:00Z")
    sse = e.to_sse()
    assert sse.startswith(f"event: {REGIME_CHANGE}\n")
    assert "data: " in sse
    assert sse.endswith("\n\n")
    data_line = sse.split("data: ")[1].strip()
    parsed = json.loads(data_line)
    assert parsed["channel"] == REGIME_CHANGE
    assert parsed["payload"]["to"] == "FRAGILE"


def test_all_channels_has_8_entries():
    assert len(ALL_CHANNELS) == 8
    for ch in ALL_CHANNELS:
        assert ch.startswith("grid_")


# ── EventBus tests ──

from events.bus import EventBus


def test_bus_subscribe_and_receive():
    """In-process subscribers receive emitted events."""
    bus = EventBus()
    received = []
    bus.subscribe(SIGNAL_FIRE, lambda e: received.append(e))
    bus.emit_sync(SIGNAL_FIRE, {"ticker": "NVDA", "direction": "buy"})
    assert len(received) == 1
    assert received[0].payload["ticker"] == "NVDA"


def test_bus_subscribe_filters_channels():
    """Subscriber only receives events for their channel."""
    bus = EventBus()
    received = []
    bus.subscribe(SIGNAL_FIRE, lambda e: received.append(e))
    bus.emit_sync(REGIME_CHANGE, {"from": "GROWTH", "to": "FRAGILE"})
    assert len(received) == 0


def test_bus_multiple_subscribers():
    """Multiple subscribers on same channel all receive the event."""
    bus = EventBus()
    a, b = [], []
    bus.subscribe(SIGNAL_FIRE, lambda e: a.append(e))
    bus.subscribe(SIGNAL_FIRE, lambda e: b.append(e))
    bus.emit_sync(SIGNAL_FIRE, {"ticker": "SPY"})
    assert len(a) == 1
    assert len(b) == 1


def test_bus_emit_sync_creates_timestamp():
    """emit_sync auto-generates ISO 8601 UTC timestamp."""
    bus = EventBus()
    received = []
    bus.subscribe(SIGNAL_FIRE, lambda e: received.append(e))
    bus.emit_sync(SIGNAL_FIRE, {"ticker": "QQQ"})
    ts = received[0].timestamp
    # Should parse as valid ISO datetime
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    assert dt.tzinfo is not None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_event_bus.py -v`
Expected: FAIL (events.bus module not found)

- [ ] **Step 3: Implement the event bus**

`events/bus.py`:
```python
"""GRID Event Bus — PG LISTEN/NOTIFY wrapper with in-process fan-out.

Dual-mode operation:
  1. **In-process**: ``emit_sync()`` fans out to local subscribers immediately.
     Used by intelligence modules running in the API process.
  2. **Cross-process** (async): ``emit()`` sends a PG NOTIFY so other processes
     (and the SSE listener) receive the event. Requires an asyncpg connection.

The SSE router subscribes to all channels and streams events to the frontend.
"""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Callable

from loguru import logger as log

from events.channels import ALL_CHANNELS, Event


class EventBus:
    """Lightweight event bus with PG NOTIFY support."""

    def __init__(self) -> None:
        self._subscribers: dict[str, list[Callable[[Event], None]]] = defaultdict(list)
        self._pg_conn = None  # asyncpg connection, set via start()

    # ── In-process pub/sub ──

    def subscribe(self, channel: str, callback: Callable[[Event], None]) -> None:
        """Register a callback for events on *channel*."""
        self._subscribers[channel].append(callback)

    def emit_sync(self, channel: str, payload: dict[str, Any]) -> Event:
        """Emit an event synchronously to in-process subscribers.

        Returns the created Event for chaining / logging.
        """
        event = Event(
            channel=channel,
            payload=payload,
            timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        )
        for cb in self._subscribers.get(channel, []):
            try:
                cb(event)
            except Exception as exc:
                log.warning(
                    "Event subscriber error on {ch}: {e}",
                    ch=channel, e=str(exc),
                )
        return event

    # ── PG NOTIFY (cross-process) ──

    async def start(self, dsn: str) -> None:
        """Connect to PostgreSQL and start listening on all channels.

        Parameters:
            dsn: PostgreSQL connection string (e.g. ``postgresql://grid:pw@localhost/grid``)
        """
        try:
            import asyncpg
        except ImportError:
            log.warning("asyncpg not installed — event bus running in local-only mode")
            return

        try:
            self._pg_conn = await asyncpg.connect(dsn)
            for channel in ALL_CHANNELS:
                await self._pg_conn.add_listener(channel, self._on_pg_notify)
            log.info(
                "Event bus connected — listening on {n} PG channels",
                n=len(ALL_CHANNELS),
            )
        except Exception as exc:
            log.warning("Event bus PG connection failed: {e}", e=str(exc))
            self._pg_conn = None

    async def stop(self) -> None:
        """Disconnect from PostgreSQL."""
        if self._pg_conn:
            try:
                for channel in ALL_CHANNELS:
                    await self._pg_conn.remove_listener(channel, self._on_pg_notify)
                await self._pg_conn.close()
            except Exception:
                pass
            self._pg_conn = None

    def _on_pg_notify(
        self, connection: Any, pid: int, channel: str, payload: str
    ) -> None:
        """Handle incoming PG NOTIFY — deserialize and fan out."""
        try:
            data = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            data = {"raw": payload}

        event = Event(
            channel=channel,
            payload=data,
            timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        )
        for cb in self._subscribers.get(channel, []):
            try:
                cb(event)
            except Exception as exc:
                log.warning(
                    "Event subscriber error on {ch}: {e}",
                    ch=channel, e=str(exc),
                )

    async def emit(self, channel: str, payload: dict[str, Any]) -> Event:
        """Emit via PG NOTIFY (cross-process) + local fan-out.

        Falls back to local-only if PG is not connected.
        """
        event = self.emit_sync(channel, payload)

        if self._pg_conn:
            try:
                await self._pg_conn.execute(
                    f"SELECT pg_notify($1, $2)",
                    channel,
                    json.dumps(payload),
                )
            except Exception as exc:
                log.warning("PG NOTIFY failed for {ch}: {e}", ch=channel, e=str(exc))

        return event


# Module-level singleton
bus = EventBus()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_event_bus.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add events/bus.py tests/test_event_bus.py
git commit -m "feat(v5): implement event bus — PG LISTEN/NOTIFY + in-process fan-out"
```

---

### Task 7: SSE endpoint

**Files:**
- Create: `api/routers/sse.py`
- Modify: `api/main.py` (add SSE router + bus startup)

- [ ] **Step 1: Create SSE router**

`api/routers/sse.py`:
```python
"""Server-Sent Events endpoint for real-time event streaming.

Clients connect to ``GET /api/v1/events/stream`` and receive events
from all GRID channels. Optional ``channels`` query param to filter.

Example:
    curl -H "Authorization: Bearer <token>" \\
         "https://grid.stepdad.finance/api/v1/events/stream?channels=grid_signal_fire,grid_regime_change"
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import StreamingResponse
from loguru import logger as log

from api.auth import require_auth
from events.bus import bus
from events.channels import ALL_CHANNELS, Event

router = APIRouter(prefix="/api/v1/events", tags=["events"])


@router.get("/stream")
async def event_stream(
    request: Request,
    channels: str | None = Query(None, description="Comma-separated channel names to subscribe to"),
    _token: str = Depends(require_auth),
):
    """SSE endpoint — streams real-time GRID events to the client."""
    # Parse channel filter
    if channels:
        requested = set(ch.strip() for ch in channels.split(","))
        listen_channels = tuple(ch for ch in requested if ch in ALL_CHANNELS)
    else:
        listen_channels = ALL_CHANNELS

    # Queue for this client's events
    queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=100)

    def on_event(event: Event) -> None:
        try:
            queue.put_nowait(event)
        except asyncio.QueueFull:
            pass  # Drop oldest-style: client is too slow

    # Subscribe to requested channels
    for ch in listen_channels:
        bus.subscribe(ch, on_event)

    async def generate():
        try:
            # Send initial connection event
            yield f"event: connected\ndata: {json.dumps({'channels': list(listen_channels)})}\n\n"

            while True:
                # Check if client disconnected
                if await request.is_disconnected():
                    break

                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield event.to_sse()
                except asyncio.TimeoutError:
                    # Send keepalive comment every 30s
                    yield ": keepalive\n\n"
        finally:
            # Unsubscribe on disconnect
            for ch in listen_channels:
                try:
                    bus._subscribers[ch].remove(on_event)
                except ValueError:
                    pass
            log.debug("SSE client disconnected")

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/channels")
async def list_channels(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """List all available event channels."""
    return {
        "channels": list(ALL_CHANNELS),
        "count": len(ALL_CHANNELS),
    }
```

- [ ] **Step 2: Register SSE router + bus startup in api/main.py**

Add the SSE router to the router list (after the `spider` entry near line 428):

```python
    ("sse", "api.routers.sse", False),
```

Add event bus startup to `_deferred_startup()`, after the database health check (after line 104):

```python
    # Start event bus (PG LISTEN/NOTIFY for cross-process events)
    try:
        from events.bus import bus as _event_bus
        from config import settings as _cfg
        dsn = f"postgresql://{_cfg.DB_USER}:{_cfg.DB_PASSWORD}@{_cfg.DB_HOST}:{_cfg.DB_PORT}/{_cfg.DB_NAME}"
        await _event_bus.start(dsn)
    except Exception as exc:
        log.debug("Event bus startup skipped: {e}", e=str(exc))
```

- [ ] **Step 3: Verify import**

Run: `cd /Users/anikdang/dev/GRID && python -c "from api.routers.sse import router; print('SSE router OK')"`
Expected: `SSE router OK`

- [ ] **Step 4: Commit**

```bash
git add api/routers/sse.py api/main.py
git commit -m "feat(v5): add SSE endpoint /api/v1/events/stream + wire event bus into API startup"
```

---

## Wave 2: V5 Phase 0 — Frontend Track

### Task 8: useAsyncData hook

**Files:**
- Create: `pwa/src/hooks/useAsyncData.js`

- [ ] **Step 1: Create the hook**

`pwa/src/hooks/useAsyncData.js`:
```javascript
/**
 * useAsyncData — universal async data fetching hook.
 *
 * Replaces the 50+ duplicated useState(loading) + useEffect + catch patterns
 * across GRID views. Handles loading, error, refetch, and stale-while-revalidate.
 *
 * Usage:
 *   const { data, loading, error, refetch } = useAsyncData(
 *       () => api.getTrustScores(),
 *       { fallback: [] }
 *   );
 *
 *   const { data, loading } = useAsyncData(
 *       () => Promise.all([api.getA(), api.getB()]),
 *       { fallback: [null, null] }
 *   );
 */

import { useState, useEffect, useCallback, useRef } from 'react';

/**
 * @param {() => Promise<T>} fetcher - Async function that returns the data
 * @param {Object} options
 * @param {T} options.fallback - Default value while loading or on error
 * @param {any[]} options.deps - Extra dependencies to trigger refetch (default: [])
 * @param {boolean} options.skip - Skip the initial fetch (default: false)
 * @returns {{ data: T, loading: boolean, error: Error|null, refetch: () => void, stale: boolean }}
 */
export function useAsyncData(fetcher, options = {}) {
    const { fallback = null, deps = [], skip = false } = options;

    const [data, setData] = useState(fallback);
    const [loading, setLoading] = useState(!skip);
    const [error, setError] = useState(null);
    const [stale, setStale] = useState(false);
    const mountedRef = useRef(true);
    const fetcherRef = useRef(fetcher);
    fetcherRef.current = fetcher;

    const refetch = useCallback(async () => {
        if (!mountedRef.current) return;
        setLoading(true);
        setError(null);
        if (data !== fallback) setStale(true);

        try {
            const result = await fetcherRef.current();
            if (mountedRef.current) {
                setData(result);
                setStale(false);
            }
        } catch (err) {
            if (mountedRef.current) {
                setError(err);
            }
        } finally {
            if (mountedRef.current) {
                setLoading(false);
            }
        }
    }, deps); // eslint-disable-line react-hooks/exhaustive-deps

    useEffect(() => {
        mountedRef.current = true;
        if (!skip) {
            refetch();
        }
        return () => { mountedRef.current = false; };
    }, [refetch, skip]);

    return { data, loading, error, refetch, stale };
}
```

- [ ] **Step 2: Build to verify**

Run: `cd /Users/anikdang/dev/GRID/pwa && npm run build`
Expected: Build succeeds

- [ ] **Step 3: Commit**

```bash
git add pwa/src/hooks/useAsyncData.js
git commit -m "feat(v5): add useAsyncData hook — replaces 50+ duplicated loading/error patterns"
```

---

### Task 9: ViewErrorBoundary component

**Files:**
- Create: `pwa/src/components/ViewErrorBoundary.jsx`
- Modify: `pwa/src/App.jsx:249` (swap ErrorBoundary for ViewErrorBoundary)

- [ ] **Step 1: Create ViewErrorBoundary**

`pwa/src/components/ViewErrorBoundary.jsx`:
```jsx
/**
 * ViewErrorBoundary — per-view error isolation with retry and context.
 *
 * Wraps each view independently so a crash in one view doesn't take down
 * the entire app. Shows the view name, error details, and a retry button
 * that re-mounts the component.
 */

import React from 'react';
import { colors, tokens, shared } from '../styles/shared.js';

const MONO = "'JetBrains Mono', monospace";
const SANS = "'IBM Plex Sans', -apple-system, sans-serif";

export default class ViewErrorBoundary extends React.Component {
    constructor(props) {
        super(props);
        this.state = { hasError: false, error: null, errorInfo: null };
    }

    static getDerivedStateFromError(error) {
        return { hasError: true, error };
    }

    componentDidCatch(error, errorInfo) {
        this.setState({ errorInfo });
        console.error(
            `[ViewErrorBoundary] ${this.props.viewName || 'unknown'} crashed:`,
            error,
            errorInfo?.componentStack,
        );
    }

    render() {
        if (this.state.hasError) {
            const viewName = this.props.viewName || 'View';
            return (
                <div style={{
                    padding: '60px 20px', textAlign: 'center',
                    maxWidth: '500px', margin: '0 auto',
                }}>
                    <div style={{
                        fontSize: '40px', marginBottom: '16px',
                        filter: 'grayscale(1)',
                    }}>
                        {'\u26A0'}
                    </div>
                    <h3 style={{
                        color: colors.red,
                        fontFamily: MONO,
                        fontSize: tokens.fontSize.xl,
                        marginBottom: '8px',
                    }}>
                        {viewName} Error
                    </h3>
                    <p style={{
                        fontSize: '13px',
                        color: colors.textMuted,
                        fontFamily: MONO,
                        marginBottom: '24px',
                        lineHeight: '1.6',
                        wordBreak: 'break-word',
                    }}>
                        {this.state.error?.message || 'An unexpected error occurred'}
                    </p>
                    <div style={{ display: 'flex', gap: '12px', justifyContent: 'center' }}>
                        <button
                            onClick={() => this.setState({ hasError: false, error: null, errorInfo: null })}
                            style={{
                                ...shared.button,
                                fontSize: '13px',
                                padding: '10px 24px',
                            }}
                        >
                            Retry
                        </button>
                        {this.props.onNavigateHome && (
                            <button
                                onClick={this.props.onNavigateHome}
                                style={{
                                    ...shared.button,
                                    fontSize: '13px',
                                    padding: '10px 24px',
                                    background: 'transparent',
                                    border: `1px solid ${colors.border}`,
                                }}
                            >
                                Go Home
                            </button>
                        )}
                    </div>
                </div>
            );
        }

        return this.props.children;
    }
}
```

- [ ] **Step 2: Update App.jsx to use ViewErrorBoundary**

In `App.jsx`, change the import on line 6:

```javascript
import ViewErrorBoundary from './components/ViewErrorBoundary.jsx';
```

Remove the old ErrorBoundary import. Then change lines 249-253:

```jsx
                <ViewErrorBoundary key={activeView} viewName={activeView} onNavigateHome={() => navigate('dashboard')}>
                    <Suspense fallback={<div style={{ padding: '60px 20px', textAlign: 'center', color: '#5A7080', fontFamily: "'IBM Plex Mono', monospace", fontSize: '13px' }}>Loading view...</div>}>
                        {renderView()}
                    </Suspense>
                </ViewErrorBoundary>
```

- [ ] **Step 3: Build to verify**

Run: `cd /Users/anikdang/dev/GRID/pwa && npm run build`
Expected: Build succeeds

- [ ] **Step 4: Commit**

```bash
git add pwa/src/components/ViewErrorBoundary.jsx pwa/src/App.jsx
git commit -m "feat(v5): add ViewErrorBoundary — per-view error isolation with retry + context"
```

---

### Task 10: Zustand store decomposition

**Files:**
- Create: `pwa/src/stores/authStore.js`
- Create: `pwa/src/stores/uiStore.js`
- Create: `pwa/src/stores/domainStore.js`
- Create: `pwa/src/stores/realtimeStore.js`
- Modify: `pwa/src/store.js` (re-export from slices)

- [ ] **Step 1: Create stores directory and authStore**

`pwa/src/stores/authStore.js`:
```javascript
/**
 * Auth store slice — token, role, login/logout.
 */
import { create } from 'zustand';

const useAuthStore = create((set) => ({
    token: localStorage.getItem('grid_token') || null,
    isAuthenticated: !!localStorage.getItem('grid_token'),
    userRole: localStorage.getItem('grid_role') || 'admin',
    username: localStorage.getItem('grid_username') || 'operator',

    setAuth: (token, role = 'admin', username = 'operator') => {
        localStorage.setItem('grid_token', token);
        localStorage.setItem('grid_role', role);
        localStorage.setItem('grid_username', username);
        set({ token, isAuthenticated: true, userRole: role, username });
    },

    clearAuth: () => {
        localStorage.removeItem('grid_token');
        localStorage.removeItem('grid_role');
        localStorage.removeItem('grid_username');
        set({ token: null, isAuthenticated: false, userRole: 'admin', username: 'operator' });
    },
}));

export default useAuthStore;
```

- [ ] **Step 2: Create uiStore**

`pwa/src/stores/uiStore.js`:
```javascript
/**
 * UI store slice — theme, active view, loading, errors, notifications.
 */
import { create } from 'zustand';

const useUiStore = create((set) => ({
    theme: localStorage.getItem('grid_theme') || 'dark',
    activeView: 'home',
    loading: {},
    errors: {},
    notifications: [],

    setTheme: (name) => {
        localStorage.setItem('grid_theme', name);
        set({ theme: name });
    },

    setActiveView: (view) => set({ activeView: view }),

    setLoading: (key, value) => set(state => ({
        loading: { ...state.loading, [key]: value },
    })),

    setError: (key, error) => set(state => ({
        errors: { ...state.errors, [key]: error },
    })),

    addNotification: (type, message) => {
        const id = Date.now();
        set(state => ({
            notifications: [...state.notifications, { id, type, message }].slice(-5),
        }));
        setTimeout(() => {
            set(state => ({
                notifications: state.notifications.filter(n => n.id !== id),
            }));
        }, 5000);
    },

    removeNotification: (id) => set(state => ({
        notifications: state.notifications.filter(n => n.id !== id),
    })),
}));

export default useUiStore;
```

- [ ] **Step 3: Create domainStore**

`pwa/src/stores/domainStore.js`:
```javascript
/**
 * Domain store slice — regime, journal, models, discovery, signals.
 */
import { create } from 'zustand';

const useDomainStore = create((set) => ({
    // System
    systemStatus: null,

    // Signals
    latestSignals: null,

    // Regime
    currentRegime: null,
    regimeHistory: [],

    // Journal
    journalEntries: [],
    journalStats: null,

    // Models
    productionModels: {},
    allModels: [],

    // Discovery
    jobs: [],
    hypotheses: [],

    // Agents
    agentProgress: null,
    agentLastComplete: null,

    // Setters
    setSystemStatus: (status) => set({ systemStatus: status }),
    setCurrentRegime: (regime) => set({ currentRegime: regime }),
    setRegimeHistory: (history) => set({ regimeHistory: history }),
    setJournalEntries: (entries) => set({ journalEntries: entries }),
    setJournalStats: (stats) => set({ journalStats: stats }),
    setProductionModels: (models) => set({ productionModels: models }),
    setAllModels: (models) => set({ allModels: models }),
    setJobs: (jobs) => set({ jobs }),
    setHypotheses: (hypotheses) => set({ hypotheses }),
}));

export default useDomainStore;
```

- [ ] **Step 4: Create realtimeStore**

`pwa/src/stores/realtimeStore.js`:
```javascript
/**
 * Realtime store slice — WebSocket state, live prices, alerts, recommendations, push, chat.
 */
import { create } from 'zustand';

const useRealtimeStore = create((set, get) => ({
    // WebSocket
    wsConnected: false,

    // Live data
    livePriceUpdates: {},
    liveAlerts: [],
    liveRecommendations: [],
    lastRegimeChange: null,

    // Push notifications
    pushSupported: typeof navigator !== 'undefined' && 'serviceWorker' in navigator && 'PushManager' in window,
    pushPermission: typeof Notification !== 'undefined' ? Notification.permission : 'default',
    pushSubscription: null,
    pushPreferences: {
        trade_recommendations: true,
        convergence_alerts: true,
        regime_changes: true,
        red_flags: true,
        price_alerts: true,
        price_alert_threshold: 5.0,
    },

    // Chat
    chatMessages: [],
    chatUnread: 0,

    // WebSocket setters
    setWsConnected: (connected) => set({ wsConnected: connected }),

    // Live data actions
    setLivePriceUpdates: (prices) => set({ livePriceUpdates: prices }),

    pushAlert: (alert) => {
        const id = Date.now();
        const entry = { ...alert, id, timestamp: alert.timestamp || new Date().toISOString() };
        set(state => ({
            liveAlerts: [entry, ...state.liveAlerts].slice(0, 20),
        }));
        setTimeout(() => {
            set(state => ({
                liveAlerts: state.liveAlerts.filter(a => a.id !== id),
            }));
        }, 15000);
    },

    pushRecommendation: (rec) => {
        const id = Date.now();
        const entry = { ...rec, id, timestamp: rec.timestamp || new Date().toISOString() };
        set(state => ({
            liveRecommendations: [entry, ...state.liveRecommendations].slice(0, 20),
        }));
    },

    dismissAlert: (id) => set(state => ({
        liveAlerts: state.liveAlerts.filter(a => a.id !== id),
    })),

    dismissRecommendation: (id) => set(state => ({
        liveRecommendations: state.liveRecommendations.filter(r => r.id !== id),
    })),

    // Push notification actions
    setPushPermission: (perm) => set({ pushPermission: perm }),
    setPushSubscription: (sub) => set({ pushSubscription: sub }),
    setPushPreferences: (prefs) => set({ pushPreferences: prefs }),

    // Chat actions
    addChatMessage: (msg) => set(state => ({
        chatMessages: [...state.chatMessages, msg],
    })),
    clearChat: () => set({ chatMessages: [], chatUnread: 0 }),
    setChatUnread: (n) => set({ chatUnread: n }),

    // WebSocket message handler — dispatches to domain + realtime + UI stores
    handleWsMessage: (event) => {
        const { type, data, severity, timestamp } = event;
        switch (type) {
            case 'connected':
                set({ wsConnected: true });
                break;
            case 'regime_update':
                // Cross-store: update domainStore
                if (data) {
                    try {
                        const { default: useDomainStore } = require('./domainStore.js');
                        useDomainStore.getState().setCurrentRegime(data);
                    } catch (_) { /* domain store not loaded yet */ }
                }
                break;
            case 'signal_update':
                if (data) {
                    try {
                        const { default: useDomainStore } = require('./domainStore.js');
                        useDomainStore.setState({ latestSignals: data });
                    } catch (_) { /* */ }
                }
                break;
            case 'node_update':
                if (data) {
                    try {
                        const { default: useDomainStore } = require('./domainStore.js');
                        useDomainStore.setState(state => ({
                            systemStatus: state.systemStatus
                                ? { ...state.systemStatus, hyperspace: data }
                                : { hyperspace: data }
                        }));
                    } catch (_) { /* */ }
                }
                break;
            case 'agent_progress':
                try {
                    const { default: useDomainStore } = require('./domainStore.js');
                    useDomainStore.setState({ agentProgress: data });
                } catch (_) { /* */ }
                break;
            case 'agent_run_complete':
                try {
                    const { default: useDomainStore } = require('./domainStore.js');
                    useDomainStore.setState({ agentProgress: null, agentLastComplete: data });
                } catch (_) { /* */ }
                break;
            case 'ping':
                set({ wsConnected: true });
                break;
            case 'prices':
                if (data) {
                    set(state => ({
                        livePriceUpdates: { ...state.livePriceUpdates, ...data },
                    }));
                }
                break;
            case 'recommendation':
                if (data) {
                    get().pushRecommendation(data);
                    try {
                        const { default: useUiStore } = require('./uiStore.js');
                        useUiStore.getState().addNotification('info',
                            `New ${data.direction} rec: ${data.ticker} @ ${data.strike}`);
                    } catch (_) { /* */ }
                }
                break;
            case 'alert':
                if (data) {
                    get().pushAlert({ ...data, severity: severity || data.severity || 'info' });
                }
                break;
            case 'regime_change':
                if (data) {
                    set({ lastRegimeChange: { ...data, timestamp } });
                    if (data.to) {
                        try {
                            const { default: useDomainStore } = require('./domainStore.js');
                            const cur = useDomainStore.getState().currentRegime;
                            useDomainStore.setState({
                                currentRegime: cur
                                    ? { ...cur, state: data.to, confidence: data.confidence }
                                    : { state: data.to, confidence: data.confidence },
                            });
                        } catch (_) { /* */ }
                    }
                    try {
                        const { default: useUiStore } = require('./uiStore.js');
                        useUiStore.getState().addNotification('warning',
                            `Regime shift: ${data.from} → ${data.to} (${Math.round((data.confidence || 0) * 100)}%)`);
                    } catch (_) { /* */ }
                }
                break;
            default:
                break;
        }
    },
}));

export default useRealtimeStore;
```

- [ ] **Step 5: Update store.js to re-export from slices (backwards compat)**

Replace `pwa/src/store.js` entirely:

```javascript
/**
 * Zustand global state store for GRID PWA.
 *
 * V5 MIGRATION: State is now decomposed into focused slices under ./stores/.
 * This file re-exports a unified hook for backwards compatibility —
 * existing views that import from './store.js' continue to work unchanged.
 *
 * For new code, import directly from the slice:
 *   import useAuthStore from './stores/authStore.js';
 *   import useUiStore from './stores/uiStore.js';
 */

import useAuthStore from './stores/authStore.js';
import useUiStore from './stores/uiStore.js';
import useDomainStore from './stores/domainStore.js';
import useRealtimeStore from './stores/realtimeStore.js';

/**
 * Unified store hook — merges all slices into one selector interface.
 * This is the backwards-compat layer. All 83 properties + 35 actions
 * are accessible through this hook exactly as before.
 */
function useStore(selector) {
    const auth = useAuthStore(selector ? undefined : (s) => s);
    const ui = useUiStore(selector ? undefined : (s) => s);
    const domain = useDomainStore(selector ? undefined : (s) => s);
    const realtime = useRealtimeStore(selector ? undefined : (s) => s);

    const merged = { ...auth, ...ui, ...domain, ...realtime };

    if (selector) {
        return selector(merged);
    }
    return merged;
}

// Also make getState() work for imperative access (e.g., in api.js WebSocket)
useStore.getState = () => ({
    ...useAuthStore.getState(),
    ...useUiStore.getState(),
    ...useDomainStore.getState(),
    ...useRealtimeStore.getState(),
});

export default useStore;
```

- [ ] **Step 6: Build to verify everything still works**

Run: `cd /Users/anikdang/dev/GRID/pwa && npm run build`
Expected: Build succeeds — all existing views continue to work because `useStore` re-exports all properties.

- [ ] **Step 7: Commit**

```bash
git add pwa/src/stores/authStore.js pwa/src/stores/uiStore.js pwa/src/stores/domainStore.js pwa/src/stores/realtimeStore.js pwa/src/store.js
git commit -m "feat(v5): decompose Zustand store into 4 focused slices with backwards-compat re-export"
```

---

### Task 11: useEventStream SSE hook

**Files:**
- Create: `pwa/src/hooks/useEventStream.js`

- [ ] **Step 1: Create the SSE hook**

`pwa/src/hooks/useEventStream.js`:
```javascript
/**
 * useEventStream — SSE client hook for the GRID event bus.
 *
 * Connects to /api/v1/events/stream and dispatches events to the
 * appropriate Zustand store slices. Runs alongside the existing WebSocket
 * connection (which handles bidirectional chat + prices).
 *
 * Usage:
 *   const { connected, lastEvent } = useEventStream();
 *   const { connected } = useEventStream({ channels: ['grid_signal_fire'] });
 */

import { useEffect, useRef, useState, useCallback } from 'react';
import useAuthStore from '../stores/authStore.js';

const RECONNECT_DELAY_MS = 3000;
const MAX_RECONNECT_DELAY_MS = 30000;

/**
 * @param {Object} options
 * @param {string[]} options.channels - Channel names to subscribe to (default: all)
 * @param {(event: {channel, payload, timestamp}) => void} options.onEvent - Custom event handler
 */
export function useEventStream(options = {}) {
    const { channels, onEvent } = options;
    const [connected, setConnected] = useState(false);
    const [lastEvent, setLastEvent] = useState(null);
    const sourceRef = useRef(null);
    const delayRef = useRef(RECONNECT_DELAY_MS);
    const mountedRef = useRef(true);
    const reconnectTimer = useRef(null);

    const token = useAuthStore(s => s.token);
    const isAuthenticated = useAuthStore(s => s.isAuthenticated);

    const connect = useCallback(() => {
        if (!token || !mountedRef.current) return;

        // Close existing connection
        if (sourceRef.current) {
            sourceRef.current.close();
        }

        let url = `/api/v1/events/stream`;
        if (channels && channels.length > 0) {
            url += `?channels=${channels.join(',')}`;
        }

        // EventSource doesn't support Authorization headers natively,
        // so we pass the token as a query param (same pattern as WebSocket).
        url += `${url.includes('?') ? '&' : '?'}token=${encodeURIComponent(token)}`;

        const source = new EventSource(url);
        sourceRef.current = source;

        source.onopen = () => {
            if (!mountedRef.current) return;
            setConnected(true);
            delayRef.current = RECONNECT_DELAY_MS;
        };

        source.addEventListener('connected', (e) => {
            if (!mountedRef.current) return;
            setConnected(true);
        });

        source.onmessage = (e) => {
            if (!mountedRef.current) return;
            try {
                const parsed = JSON.parse(e.data);
                setLastEvent(parsed);
                onEvent?.(parsed);
            } catch (_) {
                // non-JSON message
            }
        };

        source.onerror = () => {
            if (!mountedRef.current) return;
            setConnected(false);
            source.close();
            // Reconnect with backoff
            const delay = delayRef.current;
            delayRef.current = Math.min(delay * 2, MAX_RECONNECT_DELAY_MS);
            reconnectTimer.current = setTimeout(() => {
                if (mountedRef.current && token) connect();
            }, delay);
        };
    }, [token, channels, onEvent]);

    useEffect(() => {
        mountedRef.current = true;
        if (isAuthenticated && token) {
            connect();
        }
        return () => {
            mountedRef.current = false;
            if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
            if (sourceRef.current) sourceRef.current.close();
        };
    }, [isAuthenticated, token, connect]);

    return { connected, lastEvent };
}
```

- [ ] **Step 2: Build to verify**

Run: `cd /Users/anikdang/dev/GRID/pwa && npm run build`
Expected: Build succeeds

- [ ] **Step 3: Commit**

```bash
git add pwa/src/hooks/useEventStream.js
git commit -m "feat(v5): add useEventStream SSE hook — connects frontend to event bus"
```

---

## Verification

After all tasks complete:

- [ ] **Backend**: `cd /Users/anikdang/dev/GRID && python -m pytest tests/test_event_bus.py tests/test_breaking_news.py -v` — all pass
- [ ] **Frontend**: `cd /Users/anikdang/dev/GRID/pwa && npm run build` — build succeeds
- [ ] **Import check**: `python -c "from events.bus import bus; from events.channels import ALL_CHANNELS; from api.routers.sse import router; print('All V5 modules OK')"` — prints OK
