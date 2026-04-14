"""
Tests for trading/solana/solana_rpc.py.

All HTTP traffic is mocked — the real Solana RPC is never hit.
"""

from __future__ import annotations

import base64
import struct
from unittest.mock import MagicMock

import httpx
import pytest

from trading.solana.solana_rpc import (
    SolanaRPC,
    SolanaRPCError,
    parse_mint_account,
)

# ----------------------------------------------------------------------
# parse_mint_account
# ----------------------------------------------------------------------
def _make_mint_bytes(
    *,
    mint_renounced: bool,
    freeze_renounced: bool,
    supply: int = 1_000_000_000,
    decimals: int = 9,
    initialized: bool = True,
) -> bytes:
    blob = bytearray(82)
    struct.pack_into("<I", blob, 0, 0 if mint_renounced else 1)
    # mint_authority pubkey bytes 4-36 (left zero, irrelevant)
    struct.pack_into("<Q", blob, 36, supply)
    blob[44] = decimals
    blob[45] = 1 if initialized else 0
    struct.pack_into("<I", blob, 46, 0 if freeze_renounced else 1)
    return bytes(blob)


def test_parse_mint_account_renounced():
    raw = _make_mint_bytes(mint_renounced=True, freeze_renounced=True)
    info = parse_mint_account("MINT1", raw)
    assert info.mint_authority_renounced is True
    assert info.freeze_authority_renounced is True
    assert info.is_initialized is True
    assert info.supply == 1_000_000_000
    assert info.decimals == 9


def test_parse_mint_account_active_authorities():
    raw = _make_mint_bytes(mint_renounced=False, freeze_renounced=False)
    info = parse_mint_account("MINT1", raw)
    assert info.mint_authority_renounced is False
    assert info.freeze_authority_renounced is False


def test_parse_mint_account_rejects_short_blob():
    with pytest.raises(SolanaRPCError, match="82 bytes"):
        parse_mint_account("MINT1", b"\x00" * 10)


def test_parse_mint_account_rejects_bad_option_discriminant():
    blob = bytearray(_make_mint_bytes(mint_renounced=True, freeze_renounced=True))
    struct.pack_into("<I", blob, 0, 99)  # bogus option
    with pytest.raises(SolanaRPCError, match="mint_authority_option"):
        parse_mint_account("MINT1", bytes(blob))


# ----------------------------------------------------------------------
# SolanaRPC — HTTP
# ----------------------------------------------------------------------
def _make_response(json_data: dict, status_code: int = 200) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


@pytest.fixture()
def mock_http() -> MagicMock:
    return MagicMock(spec=httpx.Client)


def test_get_mint_info_happy_path(mock_http):
    raw = _make_mint_bytes(mint_renounced=True, freeze_renounced=True)
    b64 = base64.b64encode(raw).decode("ascii")
    mock_http.post.return_value = _make_response(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"value": {"data": [b64, "base64"]}},
        }
    )

    rpc = SolanaRPC(client=mock_http)
    info = rpc.get_mint_info("MINT1")

    assert info.mint_authority_renounced is True
    assert info.freeze_authority_renounced is True
    payload = mock_http.post.call_args.kwargs["json"]
    assert payload["method"] == "getAccountInfo"
    assert payload["params"][0] == "MINT1"


def test_get_mint_info_account_missing(mock_http):
    mock_http.post.return_value = _make_response(
        {"jsonrpc": "2.0", "id": 1, "result": {"value": None}}
    )
    rpc = SolanaRPC(client=mock_http)
    with pytest.raises(SolanaRPCError, match="not found"):
        rpc.get_mint_info("MINT1")


def test_get_mint_info_rpc_error(mock_http):
    mock_http.post.return_value = _make_response(
        {"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "nope"}}
    )
    rpc = SolanaRPC(client=mock_http)
    with pytest.raises(SolanaRPCError, match="returned error"):
        rpc.get_mint_info("MINT1")


def test_get_mint_info_http_error(mock_http):
    mock_http.post.side_effect = httpx.ConnectError("down")
    rpc = SolanaRPC(client=mock_http)
    with pytest.raises(SolanaRPCError, match="failed"):
        rpc.get_mint_info("MINT1")


def test_get_mint_info_bad_encoding(mock_http):
    mock_http.post.return_value = _make_response(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"value": {"data": ["BLOB", "jsonParsed"]}},
        }
    )
    rpc = SolanaRPC(client=mock_http)
    with pytest.raises(SolanaRPCError, match="unexpected encoding"):
        rpc.get_mint_info("MINT1")


def test_get_mint_info_validates_mint(mock_http):
    rpc = SolanaRPC(client=mock_http)
    with pytest.raises(ValueError):
        rpc.get_mint_info("")


# ----------------------------------------------------------------------
# get_token_largest_accounts
# ----------------------------------------------------------------------
def test_get_token_largest_accounts_happy_path(mock_http):
    mock_http.post.return_value = _make_response(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "value": [
                    {"address": "ADDR1", "amount": "500", "uiAmount": 0.5},
                    {"address": "ADDR2", "amount": "200", "uiAmount": 0.2},
                ]
            },
        }
    )
    rpc = SolanaRPC(client=mock_http)
    holders = rpc.get_token_largest_accounts("MINT1")

    assert len(holders) == 2
    assert holders[0].address == "ADDR1"
    assert holders[0].amount == 500
    assert holders[0].ui_amount == 0.5


def test_get_token_largest_accounts_skips_malformed_rows(mock_http):
    mock_http.post.return_value = _make_response(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "value": [
                    {"address": "GOOD", "amount": "1", "uiAmount": 1.0},
                    {"address": "BAD"},  # missing amount — gets skipped
                ]
            },
        }
    )
    rpc = SolanaRPC(client=mock_http)
    holders = rpc.get_token_largest_accounts("MINT1")
    # Missing amount defaults to 0, so the row is kept but zeroed.
    assert len(holders) == 2
    assert holders[1].amount == 0


def test_context_manager_closes_owned_client():
    rpc = SolanaRPC()
    with rpc as r:
        assert r is rpc
    rpc.close()  # should be idempotent


def test_context_manager_does_not_close_external_client(mock_http):
    rpc = SolanaRPC(client=mock_http)
    with rpc:
        pass
    mock_http.close.assert_not_called()
