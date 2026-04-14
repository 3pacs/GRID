"""
Tests for trading/solana/helius_client.py.

HTTP traffic is mocked — the real Helius API is never hit.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import httpx
import pytest

from trading.solana.helius_client import (
    WEBHOOK_CREATE_POOL,
    WEBHOOK_SWAP,
    WEBHOOK_TOKEN_MINT,
    HeliusClient,
    HeliusError,
    parse_webhook_payload,
)


def _make_response(json_data, status=200):
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


@pytest.fixture()
def mock_http():
    return MagicMock(spec=httpx.Client)


# ----------------------------------------------------------------------
# get_wallet_transactions
# ----------------------------------------------------------------------
def test_get_wallet_transactions_happy_path(mock_http):
    mock_http.get.return_value = _make_response([{"signature": "S1", "type": "SWAP"}])
    client = HeliusClient(api_key="KEY", client=mock_http)
    txs = client.get_wallet_transactions("WALLET1", limit=10)
    assert txs == [{"signature": "S1", "type": "SWAP"}]
    call = mock_http.get.call_args
    assert "WALLET1" in call.args[0]
    assert call.kwargs["params"]["api-key"] == "KEY"
    assert call.kwargs["params"]["limit"] == 10


def test_get_wallet_transactions_pagination_cursor(mock_http):
    mock_http.get.return_value = _make_response([])
    client = HeliusClient(client=mock_http)
    client.get_wallet_transactions("W", before="CURSOR1")
    assert mock_http.get.call_args.kwargs["params"]["before"] == "CURSOR1"


def test_get_wallet_transactions_validates(mock_http):
    client = HeliusClient(client=mock_http)
    with pytest.raises(ValueError):
        client.get_wallet_transactions("")


def test_get_wallet_transactions_http_error(mock_http):
    mock_http.get.side_effect = httpx.ConnectError("down")
    client = HeliusClient(client=mock_http)
    with pytest.raises(HeliusError):
        client.get_wallet_transactions("WALLET")


def test_get_wallet_transactions_bad_shape(mock_http):
    mock_http.get.return_value = _make_response({"error": "nope"})
    client = HeliusClient(client=mock_http)
    with pytest.raises(HeliusError, match="unexpected transactions shape"):
        client.get_wallet_transactions("WALLET")


# ----------------------------------------------------------------------
# list_wallet_deploys
# ----------------------------------------------------------------------
def test_list_wallet_deploys_filters_token_mints(mock_http):
    now = int(datetime.now(timezone.utc).timestamp())
    txs = [
        {
            "type": WEBHOOK_TOKEN_MINT,
            "signature": "S1",
            "timestamp": now - 3600,
            "tokenTransfers": [{"mint": "MINT_A"}],
        },
        {
            "type": "SWAP",
            "signature": "S2",
            "timestamp": now - 1800,
            "tokenTransfers": [{"mint": "MINT_B"}],
        },
        {
            "type": WEBHOOK_TOKEN_MINT,
            "signature": "S3",
            "timestamp": now - 900,
            "tokenTransfers": [{"mint": "MINT_C"}],
        },
    ]
    mock_http.get.return_value = _make_response(txs)
    client = HeliusClient(client=mock_http)
    deploys = client.list_wallet_deploys("WALLET1", lookback_days=7)

    mints = [d.mint for d in deploys]
    assert mints == ["MINT_A", "MINT_C"]


def test_list_wallet_deploys_skips_old_entries(mock_http):
    old = int((datetime.now(timezone.utc).timestamp())) - 86400 * 400
    mock_http.get.return_value = _make_response(
        [
            {
                "type": WEBHOOK_TOKEN_MINT,
                "signature": "S1",
                "timestamp": old,
                "tokenTransfers": [{"mint": "OLD_MINT"}],
            }
        ]
    )
    client = HeliusClient(client=mock_http)
    deploys = client.list_wallet_deploys("W", lookback_days=180)
    assert deploys == []


# ----------------------------------------------------------------------
# get_early_buyers
# ----------------------------------------------------------------------
def test_get_early_buyers_within_window(mock_http):
    base_ts = int(datetime.now(timezone.utc).timestamp())
    swaps = [
        {
            "type": WEBHOOK_SWAP,
            "signature": "S1",
            "timestamp": base_ts,
            "tokenTransfers": [
                {"mint": "MINT_X", "toUserAccount": "BUYER_1"}
            ],
            "events": {"swap": {"amountUsd": 250.0}},
        },
        {
            "type": WEBHOOK_SWAP,
            "signature": "S2",
            "timestamp": base_ts + 30,
            "tokenTransfers": [
                {"mint": "MINT_X", "toUserAccount": "BUYER_2"}
            ],
            "events": {"swap": {"amountUsd": 500.0}},
        },
        {
            "type": WEBHOOK_SWAP,
            "signature": "S3",
            "timestamp": base_ts + 120,  # outside 60s window
            "tokenTransfers": [
                {"mint": "MINT_X", "toUserAccount": "BUYER_3"}
            ],
            "events": {"swap": {"amountUsd": 100.0}},
        },
    ]
    mock_http.get.return_value = _make_response(swaps)
    client = HeliusClient(client=mock_http)
    buyers = client.get_early_buyers("MINT_X", window_seconds=60)
    wallets = [b.wallet for b in buyers]
    assert wallets == ["BUYER_1", "BUYER_2"]
    assert buyers[1].amount_usd == 500.0


def test_get_early_buyers_empty(mock_http):
    mock_http.get.return_value = _make_response([])
    client = HeliusClient(client=mock_http)
    assert client.get_early_buyers("MINT_X") == []


def test_get_early_buyers_validates(mock_http):
    client = HeliusClient(client=mock_http)
    with pytest.raises(ValueError):
        client.get_early_buyers("")


# ----------------------------------------------------------------------
# Webhook parsing
# ----------------------------------------------------------------------
def test_parse_webhook_payload_happy_path():
    now = int(datetime.now(timezone.utc).timestamp())
    raw = [
        {
            "type": WEBHOOK_CREATE_POOL,
            "signature": "SIG1",
            "timestamp": now,
            "feePayer": "DEPLOYER1",
            "tokenTransfers": [{"mint": "NEWMINT"}],
            "events": {"swap": {"poolId": "POOL1"}},
        }
    ]
    events = parse_webhook_payload(raw)
    assert len(events) == 1
    ev = events[0]
    assert ev.event_type == WEBHOOK_CREATE_POOL
    assert ev.signature == "SIG1"
    assert ev.source_wallet == "DEPLOYER1"
    assert ev.mints == ("NEWMINT",)
    assert ev.pool_address == "POOL1"


def test_parse_webhook_payload_drops_malformed_rows():
    raw = [
        "not a dict",
        {"type": "SWAP"},  # missing signature
        {"type": "SWAP", "signature": "OK"},
    ]
    events = parse_webhook_payload(raw)
    assert len(events) == 1
    assert events[0].signature == "OK"


def test_parse_webhook_payload_non_list():
    assert parse_webhook_payload({"not": "a list"}) == []


def test_parse_webhook_payload_extracts_multiple_mints():
    raw = [
        {
            "type": WEBHOOK_TOKEN_MINT,
            "signature": "S1",
            "timestamp": 1000,
            "tokenTransfers": [
                {"mint": "MINT_A"},
                {"mint": "MINT_B"},
                {"mint": "MINT_A"},  # dedup
            ],
        }
    ]
    events = parse_webhook_payload(raw)
    assert events[0].mints == ("MINT_A", "MINT_B")


# ----------------------------------------------------------------------
# get_mint_deployer
# ----------------------------------------------------------------------
def test_get_mint_deployer_returns_fee_payer_of_earliest_mint(mock_http):
    base_ts = int(datetime.now(timezone.utc).timestamp())
    txs = [
        {
            "type": "SWAP",
            "signature": "S1",
            "timestamp": base_ts + 1000,
            "feePayer": "LATER_TRADER",
        },
        {
            "type": WEBHOOK_TOKEN_MINT,
            "signature": "S2",
            "timestamp": base_ts + 500,
            "feePayer": "DEPLOYER_WALLET",
            "tokenTransfers": [{"mint": "MINT_X"}],
        },
        {
            "type": WEBHOOK_TOKEN_MINT,
            "signature": "S3",
            "timestamp": base_ts + 100,  # earliest mint tx — this is the one
            "feePayer": "TRUE_DEPLOYER",
            "tokenTransfers": [{"mint": "MINT_X"}],
        },
    ]
    mock_http.get.return_value = _make_response(txs)
    client = HeliusClient(client=mock_http)
    assert client.get_mint_deployer("MINT_X") == "TRUE_DEPLOYER"


def test_get_mint_deployer_returns_none_when_no_mint_events(mock_http):
    mock_http.get.return_value = _make_response(
        [{"type": "SWAP", "signature": "S1", "timestamp": 1000}]
    )
    client = HeliusClient(client=mock_http)
    assert client.get_mint_deployer("MINT_X") is None


def test_get_mint_deployer_falls_back_to_signers(mock_http):
    mock_http.get.return_value = _make_response(
        [
            {
                "type": WEBHOOK_TOKEN_MINT,
                "signature": "S1",
                "timestamp": 1000,
                "signers": ["SIGNER1"],
            }
        ]
    )
    client = HeliusClient(client=mock_http)
    assert client.get_mint_deployer("MINT_X") == "SIGNER1"


def test_get_mint_deployer_validates(mock_http):
    client = HeliusClient(client=mock_http)
    with pytest.raises(ValueError):
        client.get_mint_deployer("")


def test_get_mint_deployer_returns_none_on_http_error(mock_http):
    mock_http.get.side_effect = httpx.ConnectError("down")
    client = HeliusClient(client=mock_http)
    assert client.get_mint_deployer("MINT_X") is None
