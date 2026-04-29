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
    status = str(getattr(evt, "status", "") or "").upper()
    puller = getattr(evt, "puller_name", "unknown")
    rows = getattr(evt, "rows", None)
    if status == "FAILED":
        log.warning(
            "pull_lifecycle: {puller} failed rows={rows} error={error}",
            puller=puller,
            rows=rows,
            error=getattr(evt, "error", None),
        )
    else:
        log.debug(
            "pull_lifecycle: {puller} status={status} rows={rows}",
            puller=puller,
            status=status,
            rows=rows,
        )
