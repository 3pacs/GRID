"""Bounds and backgrounds grid-realtime's raw DB writes.

Three call sites open a raw ``psycopg2`` connection via ``db.get_connection()``:
``dex_scanner.py``'s spike-write, ``flusher.py``'s periodic write, and
``ws_listener.py``'s shutdown final flush. Raw connections sit outside
SQLAlchemy's pool, so ``GRID_DB_POOL_SIZE``/``GRID_DB_MAX_OVERFLOW`` do not
bound them at all.

The task graph alone already limits the natural ceiling to 2 simultaneous
writes: ``dex_scanner``'s 60s poll and ``flusher``'s 300s poll are
independent tasks that could, in the worst case, both be mid-write at the
same instant; the shutdown flush can never overlap either, because it only
runs after both of those tasks have already been cancelled and awaited (see
``ws_listener.py::main``). ``_WRITE_SEMAPHORE`` makes that a stated,
enforced contract instead of an emergent property of today's two poll
intervals, so a future change to either interval -- or a new write site --
cannot silently raise it without this module's owner noticing.

Every write also runs on a worker thread via ``run_in_executor`` rather than
directly on the event loop. Before this, both ``flusher.py``'s periodic
write and ``dex_scanner.py``'s spike write were direct, unwrapped
synchronous calls inside their coroutines: a slow or hung write (lock
contention, a stalled connection, anything short of a fast round trip)
blocked the entire single-threaded event loop -- including the callback
``asyncio.loop.add_signal_handler()`` registers for SIGTERM. A blocked loop
cannot run that callback until the blocking call returns on its own, so a
hung periodic write could silently delay the whole graceful-shutdown
sequence (task cancellation, the final candle flush) past systemd's
``TimeoutStopUSec``, risking a SIGKILL that skips the final flush entirely.

This is a different, narrower failure path than an executor-wrapped call
going slow: traced empirically, a slow ``run_in_executor`` call does NOT
block ``asyncio.gather()`` or delay code that runs after it --
``task.cancel()`` still delivers ``CancelledError`` promptly even though the
underlying thread keeps running orphaned in the background. The only place
that background thread still matters is ``asyncio.run()``'s own cleanup
(``loop.shutdown_default_executor()``), which does wait for it before the
process can fully exit -- by which point any executor-wrapped write has
already had its chance to run.

## Why the semaphore is a ``threading.Semaphore``, not ``asyncio.Semaphore``

An earlier version of this module used ``asyncio.Semaphore``, acquired
around the whole ``async with ...: await run_in_executor(...)`` block in the
*coroutine*. That has a real hole: if the coroutine awaiting the executor
call gets cancelled, ``async with``'s ``__aexit__`` releases the semaphore
as part of unwinding the cancellation -- immediately, on the event loop --
even though the underlying *thread* is still running and still holds an open
database connection (threads can't be forcibly cancelled; see the module
docstring above). A third write can then acquire the now-"free" slot and
open a new connection while the orphaned thread's connection is still open,
exceeding the claimed ceiling of 2. Proven empirically: with the
``asyncio.Semaphore`` version, cancelling one of two concurrent slow writes
and immediately launching a third produced 3 simultaneous
connection-holding threads, not 2 (see this module's PR description for the
exact repro).

The fix: acquire and release ``_WRITE_SEMAPHORE`` *inside* the worker
thread, wrapping the actual write call, using a plain ``threading.Semaphore``
-- not an ``asyncio`` primitive, which isn't meant to be touched from a
non-event-loop thread. Its release is now tied to the write actually
finishing (success or exception), never to the awaiting coroutine's
cancellation state, because that coroutine's cancellation can't reach into
a thread that's already running. Verified with the same repro: cancelling
one of two concurrent slow writes and launching a third still shows only 2
threads holding a connection at once -- the third blocks on the semaphore
until one of the first two actually finishes.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any, Callable

# Matches the task graph's own proven ceiling (see module docstring) --
# deliberately tight, not a round number picked for headroom. A plain
# threading.Semaphore, not asyncio.Semaphore -- see the module docstring for
# why that distinction is load-bearing, not stylistic.
_WRITE_SEMAPHORE = threading.Semaphore(2)


def _run_bounded(write_fn: Callable[..., Any], args: tuple[Any, ...]) -> None:
    """Runs entirely on the executor's worker thread.

    The semaphore acquire/release happens here, in the same thread that
    actually opens and closes the DB connection -- so it's held for exactly
    the connection's real lifetime, regardless of what happens to the
    coroutine that submitted this work.
    """
    with _WRITE_SEMAPHORE:
        write_fn(*args)


async def bounded_write(write_fn: Callable[..., Any], *args: Any) -> None:
    """Run a synchronous DB write off the event loop, capped at 2 concurrent
    -- a cap that holds even if the awaiting coroutine is cancelled while
    the write is still in flight (see module docstring).

    ``write_fn`` must be a plain synchronous callable (typically one that
    opens its own ``db.get_connection()`` and closes it before returning) --
    it runs in the default ``ThreadPoolExecutor``, never on the event loop.
    """
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _run_bounded, write_fn, args)
