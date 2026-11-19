"""The registry-divergence warning reported 33 "silent orphans" every
SmartScheduler start (2026-11-07); every one was a source_catalog-name alias
that hermes_operator._build_source_registry derives from
REGISTRY_CATALOG_NAMES for a registered puller."""

from __future__ import annotations

from loguru import logger

from ingestion.smart_scheduler import PULLER_REGISTRY, REGISTRY_CATALOG_NAMES, SmartScheduler


def test_catalog_aliases_are_not_reported_as_orphans() -> None:
    reg = {p["name"] for p in PULLER_REGISTRY}
    assert any(n in reg for n in REGISTRY_CATALOG_NAMES), "fixture needs at least one catalog alias"

    messages: list[str] = []
    sink = logger.add(lambda m: messages.append(m.record["message"]), level="WARNING")
    try:
        SmartScheduler._warn_registry_divergence(object.__new__(SmartScheduler))
    finally:
        logger.remove(sink)
    assert not [m for m in messages if "registry divergence" in m], messages
