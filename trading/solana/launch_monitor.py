"""
Real-time launch monitor.

Consumes two kinds of input:

  * **Helius webhooks** — POST handler deserialises a payload into a
    list of :class:`WebhookEvent` and fans them out to the monitor
  * **Polling** — for environments without a public webhook URL, the
    monitor can call ``HeliusClient`` directly on a timer

Both paths normalise into a :class:`LaunchEvent` and fire every
registered handler. The monitor itself doesn't decide anything — it
just publishes events. Decisions happen downstream in
:mod:`trading.solana.fast_entry`.

Stateful behaviour:
  * de-duplicates events by ``signature`` (recent LRU set, bounded)
  * enriches each event with early buyers before dispatch, when a
    provider is attached — a single extra call per launch, bounded by
    ``enrichment_window_seconds``

Design rules:
  * **Errors in one handler don't prevent others from running.**
  * **Errors in enrichment don't block dispatch** — the event still
    fires with ``early_buyers=()``, better than dropping it.
"""

from __future__ import annotations

import collections
from dataclasses import dataclass
from typing import Any, Callable, Iterable

from loguru import logger as log

from trading.solana.cross_ref import LaunchEvent
from trading.solana.helius_client import (
    WEBHOOK_CREATE_POOL,
    WEBHOOK_TOKEN_MINT,
    DeployInfoProvider,
    WebhookEvent,
    parse_webhook_payload,
)


LaunchHandler = Callable[[LaunchEvent], None]

_DEFAULT_DEDUP_SIZE = 1024
_DEFAULT_ENRICH_WINDOW = 60  # seconds


# ----------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------
@dataclass
class IngestSummary:
    events_seen: int = 0
    duplicates_dropped: int = 0
    events_dispatched: int = 0
    handlers_invoked: int = 0
    handler_errors: int = 0


# ----------------------------------------------------------------------
# Monitor
# ----------------------------------------------------------------------
class LaunchMonitor:
    """Pub/sub hub for Solana launch events."""

    def __init__(
        self,
        provider: DeployInfoProvider | None = None,
        dedup_size: int = _DEFAULT_DEDUP_SIZE,
        enrichment_window_seconds: int = _DEFAULT_ENRICH_WINDOW,
        enrich_early_buyers: bool = True,
    ) -> None:
        self.provider = provider
        self.enrichment_window_seconds = enrichment_window_seconds
        self.enrich_early_buyers = enrich_early_buyers
        self._handlers: list[LaunchHandler] = []
        self._seen: collections.deque[str] = collections.deque(maxlen=dedup_size)
        self._seen_set: set[str] = set()

    # ------------------------------------------------------------------
    # Handler registration
    # ------------------------------------------------------------------
    def on_launch(self, handler: LaunchHandler) -> None:
        """Register a handler for every dispatched launch event."""
        self._handlers.append(handler)

    def clear_handlers(self) -> None:
        self._handlers.clear()

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------
    def ingest_webhook(self, payload: Any) -> IngestSummary:
        """Process a raw Helius webhook body.

        Returns a summary of how many events were seen, dropped as
        duplicates, and dispatched to handlers.
        """
        events = parse_webhook_payload(payload)
        return self._ingest_events(events)

    def ingest_events(self, events: Iterable[WebhookEvent]) -> IngestSummary:
        """Ingest already-parsed events (used by polling and tests)."""
        return self._ingest_events(list(events))

    def _ingest_events(self, events: list[WebhookEvent]) -> IngestSummary:
        summary = IngestSummary()
        for event in events:
            summary.events_seen += 1
            if event.signature in self._seen_set:
                summary.duplicates_dropped += 1
                continue
            self._mark_seen(event.signature)

            # Only certain webhook types are interesting for new-token
            # detection. Anything else is dropped silently — the monitor
            # can be subscribed to a broad stream.
            if not self._is_launch_event(event):
                continue

            launch = self._to_launch_event(event)
            launch = self._enrich(launch)
            self._dispatch(launch, summary)
            summary.events_dispatched += 1

        return summary

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------
    def _is_launch_event(self, event: WebhookEvent) -> bool:
        if event.event_type == WEBHOOK_CREATE_POOL:
            return True
        # A token mint with no pool yet isn't tradeable, but some
        # webhooks emit both events in one batch — we accept TOKEN_MINT
        # as a launch signal and let the enrichment step pick up the
        # pool if it exists.
        if event.event_type == WEBHOOK_TOKEN_MINT and event.mints:
            return True
        return False

    def _to_launch_event(self, event: WebhookEvent) -> LaunchEvent:
        mint = event.mints[0] if event.mints else ""
        if not mint:
            log.debug(
                "Launch event without a mint — skipping: sig={s}",
                s=event.signature,
            )
            return LaunchEvent(mint="", source="helius_webhook", raw=event.raw)
        return LaunchEvent(
            mint=mint,
            deployer=event.source_wallet,
            symbol=_extract_symbol(event.raw),
            name=_extract_name(event.raw),
            early_buyers=(),
            initial_liquidity_usd=_extract_liquidity(event.raw),
            observed_at=event.timestamp,
            source="helius_webhook",
            pool_address=event.pool_address,
            raw=event.raw,
        )

    # ------------------------------------------------------------------
    # Enrichment — pull early buyers from the provider
    # ------------------------------------------------------------------
    def _enrich(self, launch: LaunchEvent) -> LaunchEvent:
        if not launch.mint:
            return launch
        if not self.enrich_early_buyers or self.provider is None:
            return launch
        try:
            early = self.provider.get_early_buyers(
                launch.mint,
                window_seconds=self.enrichment_window_seconds,
            )
        except Exception as exc:  # noqa: BLE001 — don't block dispatch
            log.warning(
                "Enrichment failed for {m}: {e}",
                m=launch.mint, e=str(exc),
            )
            return launch
        wallets = tuple(eb.wallet for eb in early)
        # dataclass is frozen — rebuild via replace to keep immutability
        return _replace_launch(launch, early_buyers=wallets)

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------
    def _dispatch(self, launch: LaunchEvent, summary: IngestSummary) -> None:
        if not launch.mint:
            return
        for handler in self._handlers:
            summary.handlers_invoked += 1
            try:
                handler(launch)
            except Exception as exc:  # noqa: BLE001 — handler isolation
                summary.handler_errors += 1
                log.warning(
                    "Launch handler {h} raised: {e}",
                    h=getattr(handler, "__name__", repr(handler)),
                    e=str(exc),
                )

    # ------------------------------------------------------------------
    # Dedup
    # ------------------------------------------------------------------
    def _mark_seen(self, signature: str) -> None:
        if len(self._seen) == self._seen.maxlen:
            oldest = self._seen[0]
            self._seen_set.discard(oldest)
        self._seen.append(signature)
        self._seen_set.add(signature)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _extract_symbol(raw: dict[str, Any]) -> str | None:
    return _first_string(raw, ("symbol", "tokenSymbol"))


def _extract_name(raw: dict[str, Any]) -> str | None:
    return _first_string(raw, ("name", "tokenName"))


def _extract_liquidity(raw: dict[str, Any]) -> float | None:
    events = raw.get("events") or {}
    if isinstance(events, dict):
        pool = events.get("createPool") or events.get("create_pool") or {}
        if isinstance(pool, dict):
            value = pool.get("initialLiquidityUsd") or pool.get("liquidity_usd")
            try:
                return float(value) if value is not None else None
            except (TypeError, ValueError):
                return None
    return None


def _first_string(blob: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for k in keys:
        v = blob.get(k)
        if isinstance(v, str) and v:
            return v
    # Look one level deeper into events.
    events = blob.get("events") or {}
    if isinstance(events, dict):
        for sub in events.values():
            if isinstance(sub, dict):
                for k in keys:
                    v = sub.get(k)
                    if isinstance(v, str) and v:
                        return v
    return None


def _replace_launch(launch: LaunchEvent, **changes: Any) -> LaunchEvent:
    """Immutable replace — dataclasses.replace doesn't play well with
    dict-typed fields that default-factory, so we spell it out.
    """
    return LaunchEvent(
        mint=changes.get("mint", launch.mint),
        deployer=changes.get("deployer", launch.deployer),
        symbol=changes.get("symbol", launch.symbol),
        name=changes.get("name", launch.name),
        early_buyers=changes.get("early_buyers", launch.early_buyers),
        initial_liquidity_usd=changes.get(
            "initial_liquidity_usd", launch.initial_liquidity_usd
        ),
        observed_at=changes.get("observed_at", launch.observed_at),
        source=changes.get("source", launch.source),
        pool_address=changes.get("pool_address", launch.pool_address),
        raw=changes.get("raw", launch.raw),
    )
