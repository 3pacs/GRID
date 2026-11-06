"""Single-flight wrapper for sync route handlers that build a cache on a miss.

While grid-api ran these handlers as ``async def`` on its one event loop,
two identical requests could never build the same cache entry at once. As
plain ``def`` handlers in the threadpool they can, so a cold cache would be
built N times in parallel by N identical requests. ``@single_flight``
restores the old one-at-a-time behaviour per argument set: the first caller
builds, identical concurrent callers wait and then hit the cache the first
one filled.

Locks are striped (a fixed pool keyed by a hash of the arguments) so the
lock table never grows with the space of query parameters; two different
argument sets that share a stripe merely wait for each other.
"""

from __future__ import annotations

import functools
import inspect
import threading
from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])

_STRIPES = 64

# Auth/session arguments carry no cache identity.
_IGNORED_KWARGS = frozenset({"_token", "token", "_", "_key", "request"})


def single_flight(fn: F) -> F:
    """Serialize concurrent calls of *fn* that have the same arguments."""
    stripes = [threading.Lock() for _ in range(_STRIPES)]

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        ident = (
            tuple(repr(a) for a in args),
            tuple(sorted((k, repr(v)) for k, v in kwargs.items() if k not in _IGNORED_KWARGS)),
        )
        with stripes[hash(ident) % _STRIPES]:
            return fn(*args, **kwargs)

    preserve_signature(wrapper, fn)
    return wrapper  # type: ignore[return-value]


def preserve_signature(wrapper: Callable[..., Any], fn: Callable[..., Any]) -> None:
    """Give *wrapper* *fn*'s signature with string annotations evaluated.

    FastAPI resolves ``from __future__ import annotations`` strings against
    the *wrapper's* module globals, where the route module's types (request
    models, ``Engine``) don't exist. Evaluating them here, in *fn*'s own
    module, keeps the route's parameters intact.
    """
    try:
        wrapper.__signature__ = inspect.signature(fn, eval_str=True)  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001 -- unresolvable names: fall back to __wrapped__
        pass
