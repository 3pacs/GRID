"""Source adapter protocol for the connection mapping spider."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from intelligence.spider.models import DiscoveredConnection


@runtime_checkable
class BaseSourceAdapter(Protocol):
    """Protocol that all source adapters must implement."""

    name: str

    def discover(
        self, actor_name: str, actor_hint: dict[str, Any]
    ) -> list[DiscoveredConnection]:
        ...
