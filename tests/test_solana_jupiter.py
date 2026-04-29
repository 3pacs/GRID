"""
Tests for trading/solana/jupiter_client.py.

All HTTP traffic is mocked — the real Jupiter API is never hit.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest

from trading.solana.jupiter_client import (
    JupiterClient,
    JupiterError,
    SOL_MINT,
    USDC_MINT,
)


def _make_response(json_data: dict, status_code: int = 200) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


@pytest.fixture()
def mock_http() -> MagicMock:
    return MagicMock(spec=httpx.Client)


# ----------------------------------------------------------------------
# get_token_price
# ----------------------------------------------------------------------
def test_get_token_price_single(mock_http):
    mock_http.get.return_value = _make_response(
        {SOL_MINT: {"usdPrice": 123.45, "decimals": 9}}
    )
    client = JupiterClient(client=mock_http)

    result = client.get_token_price(SOL_MINT)

    assert result[SOL_MINT]["usdPrice"] == 123.45
    call = mock_http.get.call_args
    assert call.args[0] == "https://api.jup.ag/price/v3"
    assert call.kwargs["params"] == {"ids": SOL_MINT}


def test_get_token_price_multi(mock_http):
    mock_http.get.return_value = _make_response({SOL_MINT: {}, USDC_MINT: {}})
    client = JupiterClient(client=mock_http)

    client.get_token_price([SOL_MINT, USDC_MINT])

    params = mock_http.get.call_args.kwargs["params"]
    assert params["ids"] == f"{SOL_MINT},{USDC_MINT}"


def test_get_token_price_empty_returns_empty_dict(mock_http):
    client = JupiterClient(client=mock_http)
    assert client.get_token_price([]) == {}
    assert client.get_token_price("") == {}
    mock_http.get.assert_not_called()


def test_get_token_price_api_key_header(mock_http):
    mock_http.get.return_value = _make_response({})
    client = JupiterClient(api_key="secret-key", client=mock_http)
    client.get_token_price(SOL_MINT)
    assert mock_http.get.call_args.kwargs["headers"] == {"x-api-key": "secret-key"}


def test_get_token_price_no_api_key_means_no_header(mock_http):
    mock_http.get.return_value = _make_response({})
    client = JupiterClient(client=mock_http)
    client.get_token_price(SOL_MINT)
    assert mock_http.get.call_args.kwargs["headers"] == {}


def test_get_token_price_http_error_wraps(mock_http):
    mock_http.get.side_effect = httpx.ConnectError("boom")
    client = JupiterClient(client=mock_http)
    with pytest.raises(JupiterError, match="price query failed"):
        client.get_token_price(SOL_MINT)


# ----------------------------------------------------------------------
# get_order
# ----------------------------------------------------------------------
def test_get_order_happy_path(mock_http):
    mock_http.get.return_value = _make_response(
        {
            "requestId": "req-1",
            "transaction": "BASE64TX",
            "inputMint": USDC_MINT,
            "outputMint": SOL_MINT,
            "inAmount": "1000000",
            "outAmount": "8500",
        }
    )
    client = JupiterClient(client=mock_http)

    order = client.get_order(
        input_mint=USDC_MINT,
        output_mint=SOL_MINT,
        amount=1_000_000,
        taker="WALLET123",
    )

    assert order.request_id == "req-1"
    assert order.transaction == "BASE64TX"
    assert order.in_amount == 1_000_000
    assert order.out_amount == 8500
    params = mock_http.get.call_args.kwargs["params"]
    assert params == {
        "inputMint": USDC_MINT,
        "outputMint": SOL_MINT,
        "amount": "1000000",
        "taker": "WALLET123",
    }


def test_get_order_validates_inputs(mock_http):
    client = JupiterClient(client=mock_http)
    with pytest.raises(ValueError, match="input_mint"):
        client.get_order("", SOL_MINT, 1, "w")
    with pytest.raises(ValueError, match="amount"):
        client.get_order(USDC_MINT, SOL_MINT, 0, "w")
    with pytest.raises(ValueError, match="taker"):
        client.get_order(USDC_MINT, SOL_MINT, 1, "")


def test_get_order_missing_fields_raises(mock_http):
    mock_http.get.return_value = _make_response({"requestId": "r"})
    client = JupiterClient(client=mock_http)
    with pytest.raises(JupiterError, match="missing keys"):
        client.get_order(USDC_MINT, SOL_MINT, 1, "wallet")


# ----------------------------------------------------------------------
# execute_swap
# ----------------------------------------------------------------------
def test_execute_swap_requires_wallet_instance(mock_http):
    client = JupiterClient(client=mock_http)
    with pytest.raises(ValueError, match="SolanaWallet"):
        client.execute_swap(MagicMock(), wallet="not-a-wallet")


def test_execute_swap_posts_signed_transaction(mock_http):
    from trading.solana.jupiter_client import SwapOrder
    from trading.solana.wallet import SolanaWallet

    mock_http.post.return_value = _make_response({"signature": "sig-1"})
    client = JupiterClient(client=mock_http)

    fake_wallet = MagicMock(spec=SolanaWallet)
    fake_wallet.sign_base64_transaction.return_value = "SIGNED_BASE64"

    order = SwapOrder(
        request_id="req-1",
        transaction="UNSIGNED_BASE64",
        input_mint=USDC_MINT,
        output_mint=SOL_MINT,
        in_amount=1,
        out_amount=1,
        raw={},
    )

    result = client.execute_swap(order, fake_wallet)

    fake_wallet.sign_base64_transaction.assert_called_once_with("UNSIGNED_BASE64")
    body = mock_http.post.call_args.kwargs["json"]
    assert body == {"signedTransaction": "SIGNED_BASE64", "requestId": "req-1"}
    assert result == {"signature": "sig-1"}


# ----------------------------------------------------------------------
# get_holdings
# ----------------------------------------------------------------------
def test_get_holdings_happy_path(mock_http):
    mock_http.get.return_value = _make_response(
        {"sol": {"amount": 1_000_000_000}, "tokens": {}}
    )
    client = JupiterClient(client=mock_http)

    result = client.get_holdings("WALLET123")

    assert result["sol"]["amount"] == 1_000_000_000
    url = mock_http.get.call_args.args[0]
    assert url.endswith("/holdings/WALLET123")


def test_get_holdings_validates_address(mock_http):
    client = JupiterClient(client=mock_http)
    with pytest.raises(ValueError):
        client.get_holdings("")


# ----------------------------------------------------------------------
# Context manager
# ----------------------------------------------------------------------
def test_context_manager_closes_owned_client():
    client = JupiterClient()
    with client as c:
        assert c is client
    # Second close should be a no-op
    client.close()


def test_context_manager_does_not_close_external_client(mock_http):
    client = JupiterClient(client=mock_http)
    with client:
        pass
    mock_http.close.assert_not_called()
