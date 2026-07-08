"""Pull lifecycle contract handler.

The emit path already audits every contract. This handler exists so puller
STARTED / COMPLETED / FAILED events have a routed consumer and the dispatcher
can treat scraper lifecycle telemetry like every other contract type.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger as log

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from contracts.schemas import PullLifecycle


def on_pull_lifecycle(evt: "PullLifecycle", *, engine: "Engine") -> None:
    """Observe puller lifecycle events without blocking the puller path."""
    state = str(getattr(evt, "state", "") or "").upper()
    puller = getattr(evt, "puller_name", "unknown")
    row_count = getattr(evt, "row_count", None)
    if state == "FAILED":
        log.warning(
            "pull_lifecycle: {puller} failed row_count={row_count} error={error}",
            puller=puller,
            row_count=row_count,
            error=getattr(evt, "error", None),
        )
    else:
        log.debug(
            "pull_lifecycle: {puller} state={state} row_count={row_count}",
            puller=puller,
            state=state,
            row_count=row_count,
        )
