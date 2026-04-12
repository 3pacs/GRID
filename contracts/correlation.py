"""Correlation id propagation for the contracts layer.

Uses a ``ContextVar`` so that any code running under a ``correlation_scope()``
sees the same id — no need to thread an argument through every function call.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator
from uuid import UUID, uuid4


_current_cid: ContextVar[UUID | None] = ContextVar(
    "contracts_correlation_id", default=None
)


def new_correlation_id() -> UUID:
    """Return a fresh correlation id."""
    return uuid4()


def get_current_correlation_id() -> UUID | None:
    """Return the current correlation id, or None if not inside a scope."""
    return _current_cid.get()


@contextmanager
def correlation_scope(cid: UUID | None = None) -> Iterator[UUID]:
    """Bind a correlation id for the duration of the ``with`` block.

    If *cid* is None a new id is generated.
    """
    if cid is None:
        cid = new_correlation_id()
    token = _current_cid.set(cid)
    try:
        yield cid
    finally:
        _current_cid.reset(token)
