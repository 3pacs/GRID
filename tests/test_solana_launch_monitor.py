"""
Tests for trading/solana/launch_monitor.py.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from trading.solana.helius_client import (
    WEBHOOK_CREATE_POOL,
    WEBHOOK_SWAP,
    WEBHOOK_TOKEN_MINT,
    EarlyBuyer,
    WebhookEvent,
)
from trading.solana.launch_monitor import LaunchMonitor


NOW = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)


def _create_pool_event(signature: str = "S1", mint: str = "MINT1") -> WebhookEvent:
    return WebhookEvent(
        event_type=WEBHOOK_CREATE_POOL,
        signature=signature,
        timestamp=NOW,
        source_wallet="DEPLOYER1",
        mints=(mint,),
        pool_address="POOL1",
        raw={"signature": signature, "tokenTransfers": [{"mint": mint}]},
    )


# ----------------------------------------------------------------------
# Ingestion / dispatch
# ----------------------------------------------------------------------
def test_ingest_events_dispatches_to_handler():
    monitor = LaunchMonitor(provider=None, enrich_early_buyers=False)
    received = []
    monitor.on_launch(lambda ev: received.append(ev))

    summary = monitor.ingest_events([_create_pool_event()])
    assert summary.events_dispatched == 1
    assert summary.handlers_invoked == 1
    assert len(received) == 1
    assert received[0].mint == "MINT1"
    assert received[0].deployer == "DEPLOYER1"
    assert received[0].pool_address == "POOL1"


def test_ingest_events_dedups_by_signature():
    monitor = LaunchMonitor(provider=None, enrich_early_buyers=False)
    received = []
    monitor.on_launch(lambda ev: received.append(ev))

    summary = monitor.ingest_events(
        [_create_pool_event("S1"), _create_pool_event("S1")]
    )
    assert summary.events_seen == 2
    assert summary.duplicates_dropped == 1
    assert summary.events_dispatched == 1
    assert len(received) == 1


def test_ingest_events_ignores_unrelated_types():
    monitor = LaunchMonitor(provider=None, enrich_early_buyers=False)
    received = []
    monitor.on_launch(lambda ev: received.append(ev))

    swap = WebhookEvent(
        event_type=WEBHOOK_SWAP,
        signature="S_SWAP",
        timestamp=NOW,
        source_wallet="W",
        mints=("MINT",),
    )
    summary = monitor.ingest_events([swap])
    assert summary.events_dispatched == 0
    assert received == []


def test_ingest_events_handles_token_mint_as_launch():
    monitor = LaunchMonitor(provider=None, enrich_early_buyers=False)
    received = []
    monitor.on_launch(lambda ev: received.append(ev))

    mint_event = WebhookEvent(
        event_type=WEBHOOK_TOKEN_MINT,
        signature="S_MINT",
        timestamp=NOW,
        source_wallet="DEPLOYER1",
        mints=("NEWMINT",),
    )
    summary = monitor.ingest_events([mint_event])
    assert summary.events_dispatched == 1
    assert received[0].mint == "NEWMINT"


def test_ingest_events_multiple_handlers():
    monitor = LaunchMonitor(provider=None, enrich_early_buyers=False)
    calls_a, calls_b = [], []
    monitor.on_launch(lambda ev: calls_a.append(ev))
    monitor.on_launch(lambda ev: calls_b.append(ev))

    monitor.ingest_events([_create_pool_event()])
    assert len(calls_a) == 1
    assert len(calls_b) == 1


def test_ingest_events_handler_errors_are_isolated():
    monitor = LaunchMonitor(provider=None, enrich_early_buyers=False)

    def bad_handler(ev):
        raise RuntimeError("handler boom")

    good_calls = []
    monitor.on_launch(bad_handler)
    monitor.on_launch(lambda ev: good_calls.append(ev))

    summary = monitor.ingest_events([_create_pool_event()])
    assert summary.handler_errors == 1
    assert summary.events_dispatched == 1
    assert len(good_calls) == 1


# ----------------------------------------------------------------------
# Enrichment
# ----------------------------------------------------------------------
def test_enrichment_populates_early_buyers():
    provider = MagicMock()
    provider.get_early_buyers.return_value = [
        EarlyBuyer("BUYER1", "MINT1", NOW),
        EarlyBuyer("BUYER2", "MINT1", NOW),
    ]
    monitor = LaunchMonitor(provider=provider, enrich_early_buyers=True)
    received = []
    monitor.on_launch(lambda ev: received.append(ev))

    monitor.ingest_events([_create_pool_event()])
    assert received[0].early_buyers == ("BUYER1", "BUYER2")
    provider.get_early_buyers.assert_called_once()


def test_enrichment_failure_still_dispatches():
    provider = MagicMock()
    provider.get_early_buyers.side_effect = RuntimeError("upstream down")
    monitor = LaunchMonitor(provider=provider, enrich_early_buyers=True)
    received = []
    monitor.on_launch(lambda ev: received.append(ev))

    summary = monitor.ingest_events([_create_pool_event()])
    # Event still fires; early_buyers empty.
    assert summary.events_dispatched == 1
    assert received[0].early_buyers == ()


def test_enrichment_can_be_disabled():
    provider = MagicMock()
    monitor = LaunchMonitor(provider=provider, enrich_early_buyers=False)
    monitor.on_launch(lambda ev: None)
    monitor.ingest_events([_create_pool_event()])
    provider.get_early_buyers.assert_not_called()


# ----------------------------------------------------------------------
# Webhook path
# ----------------------------------------------------------------------
def test_ingest_webhook_end_to_end():
    monitor = LaunchMonitor(provider=None, enrich_early_buyers=False)
    received = []
    monitor.on_launch(lambda ev: received.append(ev))

    webhook_body = [
        {
            "type": WEBHOOK_CREATE_POOL,
            "signature": "SIG1",
            "timestamp": int(NOW.timestamp()),
            "feePayer": "DEPLOYER1",
            "tokenTransfers": [{"mint": "NEWCOIN"}],
            "events": {"swap": {"poolId": "POOL_ABC"}},
        }
    ]
    summary = monitor.ingest_webhook(webhook_body)
    assert summary.events_dispatched == 1
    assert received[0].mint == "NEWCOIN"
    assert received[0].pool_address == "POOL_ABC"


def test_ingest_webhook_non_list_body():
    monitor = LaunchMonitor(provider=None, enrich_early_buyers=False)
    monitor.on_launch(lambda ev: None)
    summary = monitor.ingest_webhook({"oops": "not a list"})
    assert summary.events_seen == 0


# ----------------------------------------------------------------------
# Dedup bounded memory
# ----------------------------------------------------------------------
def test_dedup_set_is_bounded():
    monitor = LaunchMonitor(provider=None, enrich_early_buyers=False, dedup_size=3)
    monitor.on_launch(lambda ev: None)

    events = [_create_pool_event(f"S{i}", f"M{i}") for i in range(5)]
    summary = monitor.ingest_events(events)
    assert summary.events_dispatched == 5

    # Re-ingesting the oldest two signatures should go through because
    # they've been evicted from the dedup window.
    summary = monitor.ingest_events(
        [_create_pool_event("S0", "M0"), _create_pool_event("S1", "M1")]
    )
    # Both S0 and S1 should have been evicted (maxlen=3, saw S0..S4).
    assert summary.duplicates_dropped == 0
    assert summary.events_dispatched == 2


# ----------------------------------------------------------------------
# clear_handlers
# ----------------------------------------------------------------------
def test_clear_handlers():
    monitor = LaunchMonitor(provider=None, enrich_early_buyers=False)
    calls = []
    monitor.on_launch(lambda ev: calls.append(ev))
    monitor.clear_handlers()
    monitor.ingest_events([_create_pool_event()])
    assert calls == []
